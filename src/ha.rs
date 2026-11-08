//! Home Assistant as a backend (#20): its WebSocket API, as pure translation
//! plus one connection.
//!
//! HA is an on-prem hub here. The bridge talks to it over the LAN; what HA
//! uses to reach a lock is HA's business. The protocol is small:
//! `auth_required` → `auth` → `auth_ok`, then numbered commands
//! (`subscribe_events`, `get_states`, `call_service`, `ping`) each answered
//! by a `result` with the same `id`, and `event` frames for the subscription.
//!
//! ## What a state means
//!
//! One rule per domain, in [`entity_incoming`]. For a `lock`, **unlocked is
//! on**: the role a lock plays is `strike`, and an energised strike is a door
//! that opens. `locking`/`unlocking` are transitional and are NOT reports --
//! the previous known state stands until the lock settles. `jammed`, and HA's
//! own `unavailable`/`unknown`, read as the entity being unreachable, which
//! the core turns into `Unknown` and, under a live lease, a stop.
//!
//! ## The state on connect is live
//!
//! Issue #20 first proposed treating `get_states` on connect like a retained
//! MQTT message -- Unknown until an event arrives. That does not work for a
//! lock at rest: HA emits `state_changed` only on change, so a resting lock
//! would stay Unknown for as long as nobody touched it, and a leased one
//! would be stopped for it. The staleness a retained message hides is, in
//! HA, tracked explicitly: an integration that loses its device marks the
//! entity `unavailable`. So the snapshot is taken as HA's current belief,
//! `unavailable` included, and `Report.retained` is never set here. The
//! caveat, for the README: how quickly an integration notices a lost device
//! is the integration's (ZHA defaults to hours for mains-powered devices).
//!
//! Losing the connection to HA reads as every configured entity becoming
//! unavailable. The bridge cannot see them; it must not pretend otherwise.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::backend::Incoming;
use crate::core::Report;

// ---------------------------------------------------------------- meaning

/// The domain of an entity id: `lock.front_door` → `lock`.
pub fn domain(entity_id: &str) -> &str {
    entity_id.split('.').next().unwrap_or("")
}

/// What an entity's state means in the core's terms. `None` for a transitional
/// state that is not yet a fact.
pub fn entity_incoming(entity_id: &str, state: &str) -> Option<Incoming> {
    // HA's own words for "I cannot see it", any domain.
    if state == "unavailable" || state == "unknown" {
        return Some(Incoming::Availability { online: false });
    }
    let on_off = |on: bool| {
        Some(Incoming::State(Report {
            on,
            retained: false,
        }))
    };
    match domain(entity_id) {
        "lock" => match state {
            // Unlocked is on: the strike is energised, the door opens.
            "unlocked" | "open" => on_off(true),
            "locked" => on_off(false),
            // In motion. Not a fact yet; the previous known state stands.
            "locking" | "unlocking" | "opening" => None,
            // A jammed lock is one whose state nobody can vouch for.
            "jammed" => Some(Incoming::Availability { online: false }),
            _ => Some(Incoming::Unrecognised),
        },
        "switch" | "light" | "input_boolean" | "binary_sensor" => match state {
            "on" => on_off(true),
            "off" => on_off(false),
            _ => Some(Incoming::Unrecognised),
        },
        _ => Some(Incoming::Unrecognised),
    }
}

/// The service that drives an entity to `on` or `off`: `(domain, service)`.
/// `None` for a domain that is input-only -- a `binary_sensor` cannot be set,
/// and a lease on one is a configuration error the config layer refuses.
pub fn service_for(entity_id: &str, on: bool) -> Option<(&'static str, &'static str)> {
    match (domain(entity_id), on) {
        ("lock", true) => Some(("lock", "unlock")),
        ("lock", false) => Some(("lock", "lock")),
        ("switch", true) => Some(("switch", "turn_on")),
        ("switch", false) => Some(("switch", "turn_off")),
        ("light", true) => Some(("light", "turn_on")),
        ("light", false) => Some(("light", "turn_off")),
        ("input_boolean", true) => Some(("input_boolean", "turn_on")),
        ("input_boolean", false) => Some(("input_boolean", "turn_off")),
        _ => None,
    }
}

// --------------------------------------------------------------- protocol

/// A frame from HA, reduced to what the bridge acts on.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerMsg {
    AuthRequired,
    AuthOk,
    AuthInvalid(String),
    /// The answer to a numbered command.
    Result {
        id: u64,
        success: bool,
        error: Option<String>,
        result: Value,
    },
    /// A `state_changed` event. `state` is `None` when the entity was removed.
    StateChanged {
        entity_id: String,
        state: Option<String>,
    },
    Pong(u64),
    /// Anything else: ignored, logged at debug.
    Other(String),
}

/// Never errors: a frame that is not understood is `Other`, so an HA version
/// that adds a message type does not take the connection down.
pub fn parse_server(text: &str) -> ServerMsg {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return ServerMsg::Other(text.chars().take(80).collect());
    };
    let ty = v.get("type").and_then(Value::as_str).unwrap_or("");
    match ty {
        "auth_required" => ServerMsg::AuthRequired,
        "auth_ok" => ServerMsg::AuthOk,
        "auth_invalid" => ServerMsg::AuthInvalid(
            v.get("message")
                .and_then(Value::as_str)
                .unwrap_or("auth_invalid")
                .to_string(),
        ),
        "result" => ServerMsg::Result {
            id: v.get("id").and_then(Value::as_u64).unwrap_or(0),
            success: v.get("success").and_then(Value::as_bool).unwrap_or(false),
            error: v
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string),
            result: v.get("result").cloned().unwrap_or(Value::Null),
        },
        "pong" => ServerMsg::Pong(v.get("id").and_then(Value::as_u64).unwrap_or(0)),
        "event" => {
            let ev = v.get("event").cloned().unwrap_or(Value::Null);
            if ev.get("event_type").and_then(Value::as_str) != Some("state_changed") {
                return ServerMsg::Other(format!(
                    "event {}",
                    ev.get("event_type").unwrap_or(&Value::Null)
                ));
            }
            let data = ev.get("data").cloned().unwrap_or(Value::Null);
            let entity_id = data
                .get("entity_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let state = data
                .get("new_state")
                .and_then(|s| s.get("state"))
                .and_then(Value::as_str)
                .map(str::to_string);
            ServerMsg::StateChanged { entity_id, state }
        }
        other => ServerMsg::Other(other.to_string()),
    }
}

pub fn auth_msg(token: &str) -> String {
    json!({ "type": "auth", "access_token": token }).to_string()
}
pub fn subscribe_msg(id: u64) -> String {
    json!({ "id": id, "type": "subscribe_events", "event_type": "state_changed" }).to_string()
}
pub fn get_states_msg(id: u64) -> String {
    json!({ "id": id, "type": "get_states" }).to_string()
}
pub fn call_service_msg(id: u64, domain: &str, service: &str, entity_id: &str) -> String {
    json!({
        "id": id,
        "type": "call_service",
        "domain": domain,
        "service": service,
        "target": { "entity_id": entity_id },
    })
    .to_string()
}
pub fn ping_msg(id: u64) -> String {
    json!({ "id": id, "type": "ping" }).to_string()
}

/// `(entity_id, state)` for every entry of a `get_states` result.
pub fn states_from(result: &Value) -> Vec<(String, String)> {
    result
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| {
                    Some((
                        s.get("entity_id")?.as_str()?.to_string(),
                        s.get("state")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

// ------------------------------------------------------------- connection

/// How long a `call_service` waits for HA's `result` before it is an error.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// A ping every so often, so a half-open TCP connection is noticed within a
/// minute rather than at the next command.
const PING_EVERY: Duration = Duration::from_secs(30);
/// Reconnect backoff: doubling from here, capped below. A rejected token
/// backs off straight to the cap: it will not fix itself.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

struct Outbound {
    domain: String,
    service: String,
    entity_id: String,
    reply: oneshot::Sender<Result<()>>,
}

/// One connection to one Home Assistant, shared by every device it owns.
/// Events for the configured entities arrive on `events` as
/// `(entity_id, Incoming)`; commands go through [`Connection::call_service`].
pub struct Connection {
    out: mpsc::Sender<Outbound>,
}

impl Connection {
    /// Connect in the background and keep connecting. Returns at once.
    pub fn start(
        url: String,
        token: String,
        entities: Vec<String>,
        events: mpsc::Sender<(String, Incoming)>,
    ) -> Self {
        let (out, out_rx) = mpsc::channel::<Outbound>(64);
        tokio::spawn(supervise(url, token, entities, events, out_rx));
        Self { out }
    }

    /// Call a service on one entity and wait for HA to answer. An `Err` is
    /// HA saying no, HA not answering within [`CALL_TIMEOUT`], or HA not
    /// being connected at all.
    pub async fn call_service(&self, domain: &str, service: &str, entity_id: &str) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.out
            .send(Outbound {
                domain: domain.to_string(),
                service: service.to_string(),
                entity_id: entity_id.to_string(),
                reply,
            })
            .await
            .map_err(|_| anyhow!("the Home Assistant connection task is gone"))?;
        match tokio::time::timeout(CALL_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(anyhow!("Home Assistant connection lost before it answered")),
            Err(_) => Err(anyhow!(
                "Home Assistant did not answer {domain}.{service} within {} s",
                CALL_TIMEOUT.as_secs()
            )),
        }
    }

    /// Ask the integration to refresh an entity from its device. Best effort:
    /// integrations that push state do nothing with it, and a refresh that
    /// changes nothing produces no event.
    pub async fn update_entity(&self, entity_id: &str) -> Result<()> {
        self.call_service("homeassistant", "update_entity", entity_id)
            .await
    }
}

/// The drive half for one entity: the core's Set/Get as service calls.
pub struct Handle {
    conn: Arc<Connection>,
    entity_id: String,
}

impl Handle {
    pub fn new(conn: Arc<Connection>, entity_id: &str) -> Self {
        Self {
            conn,
            entity_id: entity_id.to_string(),
        }
    }

    /// `on_time_s` is ignored: no HA service takes a countdown, and the config
    /// layer refuses `countdown` for this backend so the core never sends one.
    pub async fn set(&self, on: bool, _on_time_s: Option<u32>) -> Result<()> {
        let (domain, service) = service_for(&self.entity_id, on).with_context(|| {
            format!(
                "{}: nothing switches a {} entity",
                self.entity_id,
                domain(&self.entity_id)
            )
        })?;
        self.conn
            .call_service(domain, service, &self.entity_id)
            .await
    }

    pub async fn get(&self) -> Result<()> {
        self.conn.update_entity(&self.entity_id).await
    }

    pub fn describe(&self) -> String {
        format!("homeassistant {}", self.entity_id)
    }
}

async fn supervise(
    url: String,
    token: String,
    entities: Vec<String>,
    events: mpsc::Sender<(String, Incoming)>,
    mut out_rx: mpsc::Receiver<Outbound>,
) {
    let watched: HashSet<String> = entities.iter().cloned().collect();
    let mut backoff = BACKOFF_MIN;
    loop {
        match session(&url, &token, &watched, &events, &mut out_rx).await {
            Ok(()) => {
                // A session only returns Ok when the command channel closed:
                // the bridge is shutting down.
                return;
            }
            Err(SessionEnd::AuthInvalid(why)) => {
                error!(
                    "ha: token rejected ({why}); retrying in {} s",
                    BACKOFF_MAX.as_secs()
                );
                backoff = BACKOFF_MAX;
            }
            Err(SessionEnd::Lost(e)) => {
                warn!(
                    "ha: connection lost ({e:#}); retrying in {} s",
                    backoff.as_secs()
                );
            }
        }
        // Whatever ended the session, the bridge can no longer see the
        // entities. Say so; the core decides what that means for a lease.
        for e in &watched {
            let _ = events
                .send((e.clone(), Incoming::Availability { online: false }))
                .await;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

enum SessionEnd {
    AuthInvalid(String),
    Lost(anyhow::Error),
}

impl From<anyhow::Error> for SessionEnd {
    fn from(e: anyhow::Error) -> Self {
        SessionEnd::Lost(e)
    }
}

/// One connected session, from TCP connect to whatever ends it.
async fn session(
    url: &str,
    token: &str,
    watched: &HashSet<String>,
    events: &mpsc::Sender<(String, Incoming)>,
    out_rx: &mut mpsc::Receiver<Outbound>,
) -> std::result::Result<(), SessionEnd> {
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("connecting to {url}"))?;

    // --- auth ----------------------------------------------------------
    match next_server(&mut ws).await? {
        ServerMsg::AuthRequired => {}
        other => return Err(anyhow!("expected auth_required, got {other:?}").into()),
    }
    ws.send(Message::text(auth_msg(token)))
        .await
        .context("sending auth")?;
    match next_server(&mut ws).await? {
        ServerMsg::AuthOk => info!("ha: connected and authenticated"),
        ServerMsg::AuthInvalid(why) => return Err(SessionEnd::AuthInvalid(why)),
        other => return Err(anyhow!("expected auth_ok, got {other:?}").into()),
    }

    // --- subscribe, then the snapshot -----------------------------------
    let mut next_id: u64 = 1;
    let mut pending: HashMap<u64, oneshot::Sender<Result<()>>> = HashMap::new();

    let sub_id = next_id;
    next_id += 1;
    ws.send(Message::text(subscribe_msg(sub_id)))
        .await
        .context("subscribing")?;
    expect_ok(&mut ws, sub_id, "subscribe_events").await?;

    let snap_id = next_id;
    next_id += 1;
    ws.send(Message::text(get_states_msg(snap_id)))
        .await
        .context("get_states")?;
    let snapshot = loop {
        match next_server(&mut ws).await? {
            ServerMsg::Result {
                id,
                success,
                error,
                result,
            } if id == snap_id => {
                if !success {
                    return Err(anyhow!("get_states refused: {}", error.unwrap_or_default()).into());
                }
                break result;
            }
            ServerMsg::StateChanged { .. } => {
                // An event between subscribe and snapshot: the snapshot that
                // follows is newer, so this is dropped deliberately.
            }
            other => debug!("ha: while waiting for the snapshot: {other:?}"),
        }
    };
    let mut seen = 0;
    for (entity_id, state) in states_from(&snapshot) {
        if !watched.contains(&entity_id) {
            continue;
        }
        seen += 1;
        emit(events, &entity_id, &state).await;
    }
    if seen < watched.len() {
        warn!(
            "ha: {} of {} configured entities are not in Home Assistant's state list",
            watched.len() - seen,
            watched.len()
        );
    }

    // --- steady state -----------------------------------------------------
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await; // the first tick is immediate; skip it
    loop {
        tokio::select! {
            frame = ws.next() => {
                let text = match frame {
                    Some(Ok(Message::Text(t))) => t.to_string(),
                    Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await.ok(); continue; }
                    Some(Ok(Message::Close(_))) | None => return Err(anyhow!("Home Assistant closed the connection").into()),
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Err(anyhow!(e).context("reading from Home Assistant").into()),
                };
                match parse_server(&text) {
                    ServerMsg::StateChanged { entity_id, state } => {
                        if watched.contains(&entity_id) {
                            match state {
                                Some(s) => emit(events, &entity_id, &s).await,
                                None => {
                                    warn!("ha: {entity_id} was removed from Home Assistant");
                                    let _ = events.send((entity_id, Incoming::Availability { online: false })).await;
                                }
                            }
                        }
                    }
                    ServerMsg::Result { id, success, error, .. } => {
                        if let Some(reply) = pending.remove(&id) {
                            let _ = reply.send(if success {
                                Ok(())
                            } else {
                                Err(anyhow!("Home Assistant refused: {}", error.unwrap_or_else(|| "no reason given".into())))
                            });
                        }
                    }
                    ServerMsg::Pong(_) => {}
                    other => debug!("ha: ignoring {other:?}"),
                }
            }
            cmd = out_rx.recv() => {
                let Some(Outbound { domain, service, entity_id, reply }) = cmd else {
                    return Ok(());
                };
                let id = next_id;
                next_id += 1;
                pending.insert(id, reply);
                if let Err(e) = ws.send(Message::text(call_service_msg(id, &domain, &service, &entity_id))).await {
                    if let Some(reply) = pending.remove(&id) {
                        let _ = reply.send(Err(anyhow!("sending {domain}.{service}: {e}")));
                    }
                    return Err(anyhow!(e).context("sending a service call").into());
                }
            }
            _ = ping.tick() => {
                let id = next_id;
                next_id += 1;
                ws.send(Message::text(ping_msg(id))).await.context("ping")?;
            }
        }
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_server(ws: &mut Ws) -> Result<ServerMsg> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => return Ok(parse_server(&t)),
            Some(Ok(Message::Ping(p))) => {
                ws.send(Message::Pong(p)).await.ok();
            }
            Some(Ok(Message::Close(_))) | None => {
                return Err(anyhow!("Home Assistant closed the connection"))
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(anyhow!(e).context("reading from Home Assistant")),
        }
    }
}

async fn expect_ok(ws: &mut Ws, id: u64, what: &str) -> Result<()> {
    loop {
        match next_server(ws).await? {
            ServerMsg::Result {
                id: got,
                success,
                error,
                ..
            } if got == id => {
                anyhow::ensure!(success, "{what} refused: {}", error.unwrap_or_default());
                return Ok(());
            }
            other => debug!("ha: while waiting for {what}: {other:?}"),
        }
    }
}

async fn emit(events: &mpsc::Sender<(String, Incoming)>, entity_id: &str, state: &str) {
    match entity_incoming(entity_id, state) {
        Some(Incoming::Unrecognised) => warn!("ha: {entity_id}: state {state:?} is not understood"),
        Some(i) => {
            let _ = events.send((entity_id.to_string(), i)).await;
        }
        None => debug!("ha: {entity_id}: {state} (transitional, not a report)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn on(b: bool) -> Option<Incoming> {
        Some(Incoming::State(Report {
            on: b,
            retained: false,
        }))
    }
    const GONE: Option<Incoming> = Some(Incoming::Availability { online: false });

    #[test]
    fn a_locks_states_mean_what_the_strike_role_needs() {
        // Unlocked is ON. The mutant is the obvious inversion -- "locked is
        // the active state" -- which would report a shut door as an open
        // strike and energise the strike on lease withdrawal.
        assert_eq!(entity_incoming("lock.door", "unlocked"), on(true));
        assert_eq!(entity_incoming("lock.door", "open"), on(true));
        assert_eq!(entity_incoming("lock.door", "locked"), on(false));
        // In motion is not a fact.
        assert_eq!(entity_incoming("lock.door", "unlocking"), None);
        assert_eq!(entity_incoming("lock.door", "locking"), None);
        // Nobody can vouch for a jammed lock, or one HA cannot see.
        assert_eq!(entity_incoming("lock.door", "jammed"), GONE);
        assert_eq!(entity_incoming("lock.door", "unavailable"), GONE);
        assert_eq!(entity_incoming("switch.x", "unknown"), GONE);
        assert_eq!(
            entity_incoming("lock.door", "ajar"),
            Some(Incoming::Unrecognised)
        );
    }

    #[test]
    fn switches_and_sensors_are_on_or_off_and_other_domains_are_not_ours() {
        assert_eq!(entity_incoming("switch.heater", "on"), on(true));
        assert_eq!(
            entity_incoming("binary_sensor.door_contact", "off"),
            on(false)
        );
        assert_eq!(
            entity_incoming("climate.room", "heat"),
            Some(Incoming::Unrecognised)
        );
    }

    #[test]
    fn a_lease_drives_a_lock_with_unlock_and_a_sensor_cannot_be_driven() {
        // The mutant is swapping lock/unlock: a granted lease would then
        // lock the door and a withdrawn one open it.
        assert_eq!(service_for("lock.door", true), Some(("lock", "unlock")));
        assert_eq!(service_for("lock.door", false), Some(("lock", "lock")));
        assert_eq!(
            service_for("switch.heater", true),
            Some(("switch", "turn_on"))
        );
        assert_eq!(
            service_for("switch.heater", false),
            Some(("switch", "turn_off"))
        );
        assert_eq!(service_for("binary_sensor.door", true), None);
    }

    #[test]
    fn the_frames_ha_sends_are_read_as_they_are_documented() {
        assert_eq!(
            parse_server(r#"{"type":"auth_required","ha_version":"2026.10.1"}"#),
            ServerMsg::AuthRequired
        );
        assert_eq!(
            parse_server(r#"{"type":"auth_invalid","message":"Invalid access token"}"#),
            ServerMsg::AuthInvalid("Invalid access token".into())
        );
        assert_eq!(
            parse_server(
                r#"{"id":7,"type":"result","success":false,"error":{"code":"not_found","message":"no such entity"}}"#
            ),
            ServerMsg::Result {
                id: 7,
                success: false,
                error: Some("no such entity".into()),
                result: Value::Null
            }
        );
        let ev = r#"{"id":1,"type":"event","event":{"event_type":"state_changed","data":{"entity_id":"lock.door","old_state":{"state":"locked"},"new_state":{"entity_id":"lock.door","state":"unlocked","attributes":{}}}}}"#;
        assert_eq!(
            parse_server(ev),
            ServerMsg::StateChanged {
                entity_id: "lock.door".into(),
                state: Some("unlocked".into())
            }
        );
        // A removed entity has a null new_state.
        let gone = r#"{"id":1,"type":"event","event":{"event_type":"state_changed","data":{"entity_id":"lock.door","old_state":{"state":"locked"},"new_state":null}}}"#;
        assert_eq!(
            parse_server(gone),
            ServerMsg::StateChanged {
                entity_id: "lock.door".into(),
                state: None
            }
        );
        // Another event type, or garbage, is Other and never an error.
        assert!(matches!(
            parse_server(
                r#"{"id":1,"type":"event","event":{"event_type":"call_service","data":{}}}"#
            ),
            ServerMsg::Other(_)
        ));
        assert!(matches!(parse_server("not json"), ServerMsg::Other(_)));
    }

    #[test]
    fn the_frames_the_bridge_sends_are_what_ha_documents() {
        // call_service targets by entity_id; `service_data` with entity_id is
        // the deprecated form and would be the natural mutant.
        let v: Value =
            serde_json::from_str(&call_service_msg(3, "lock", "unlock", "lock.door")).unwrap();
        assert_eq!(v["id"], 3);
        assert_eq!(v["type"], "call_service");
        assert_eq!(v["domain"], "lock");
        assert_eq!(v["service"], "unlock");
        assert_eq!(v["target"]["entity_id"], "lock.door");
        assert!(v.get("service_data").is_none());
        let s: Value = serde_json::from_str(&subscribe_msg(1)).unwrap();
        assert_eq!(s["event_type"], "state_changed");
        let a: Value = serde_json::from_str(&auth_msg("tok")).unwrap();
        assert_eq!(a["access_token"], "tok");
        assert!(a.get("id").is_none(), "auth carries no id");
    }

    #[test]
    fn a_snapshot_is_read_entity_by_entity() {
        let r = json!([
            {"entity_id": "lock.door", "state": "locked", "attributes": {}},
            {"entity_id": "sun.sun", "state": "above_horizon"},
            {"no_entity_id": true}
        ]);
        assert_eq!(
            states_from(&r),
            vec![
                ("lock.door".to_string(), "locked".to_string()),
                ("sun.sun".to_string(), "above_horizon".to_string())
            ]
        );
        assert!(states_from(&Value::Null).is_empty());
    }
}

/// Against a fake Home Assistant: a real WebSocket server speaking the
/// half-dozen frames the bridge uses, in-process, on an ephemeral port.
#[cfg(test)]
mod live_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    /// `(entity_id, new state)`; `None` is a removed entity.
    type Pushed = Vec<(String, Option<String>)>;

    /// What the fake does once a client has authenticated.
    #[derive(Clone)]
    struct Fake {
        token: &'static str,
        snapshot: Value,
        /// Every call_service frame the client sent, as received.
        calls: Arc<Mutex<Vec<Value>>>,
        /// Events the test wants pushed to the client, after the snapshot.
        push: Arc<Mutex<Pushed>>,
    }

    async fn serve(fake: Fake) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/api/websocket", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let fake = fake.clone();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    ws.send(Message::text(
                        r#"{"type":"auth_required","ha_version":"fake"}"#,
                    ))
                    .await
                    .unwrap();
                    let Some(Ok(Message::Text(t))) = ws.next().await else {
                        return;
                    };
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["access_token"] != fake.token {
                        ws.send(Message::text(
                            r#"{"type":"auth_invalid","message":"Invalid access token"}"#,
                        ))
                        .await
                        .ok();
                        return;
                    }
                    ws.send(Message::text(r#"{"type":"auth_ok","ha_version":"fake"}"#))
                        .await
                        .unwrap();
                    let mut sub_id = None;
                    while let Some(Ok(Message::Text(t))) = ws.next().await {
                        let v: Value = serde_json::from_str(&t).unwrap();
                        let id = v["id"].as_u64().unwrap();
                        match v["type"].as_str().unwrap() {
                            "subscribe_events" => {
                                sub_id = Some(id);
                                ws.send(Message::text(json!({"id": id, "type": "result", "success": true, "result": null}).to_string())).await.unwrap();
                            }
                            "get_states" => {
                                ws.send(Message::text(json!({"id": id, "type": "result", "success": true, "result": fake.snapshot}).to_string())).await.unwrap();
                                // The snapshot is out; now anything the test queued.
                                let queued: Vec<_> =
                                    std::mem::take(&mut *fake.push.lock().unwrap());
                                for (entity, state) in queued {
                                    let new_state = state.map(|s| json!({"entity_id": entity, "state": s, "attributes": {}}));
                                    let ev = json!({"id": sub_id.unwrap_or(1), "type": "event", "event": {"event_type": "state_changed", "data": {"entity_id": entity, "old_state": null, "new_state": new_state}}});
                                    ws.send(Message::text(ev.to_string())).await.unwrap();
                                }
                            }
                            "call_service" => {
                                fake.calls.lock().unwrap().push(v.clone());
                                let ok = v["domain"] != "lock"
                                    || v["target"]["entity_id"] != "lock.jammed";
                                let reply = if ok {
                                    json!({"id": id, "type": "result", "success": true, "result": null})
                                } else {
                                    json!({"id": id, "type": "result", "success": false, "error": {"code": "fail", "message": "the lock is jammed"}})
                                };
                                ws.send(Message::text(reply.to_string())).await.unwrap();
                                if v["service"] == "disconnect_me" {
                                    return; // drops the connection
                                }
                            }
                            "ping" => {
                                ws.send(Message::text(
                                    json!({"id": id, "type": "pong"}).to_string(),
                                ))
                                .await
                                .unwrap();
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        url
    }

    fn fake(snapshot: Value) -> Fake {
        Fake {
            token: "good-token",
            snapshot,
            calls: Arc::new(Mutex::new(vec![])),
            push: Arc::new(Mutex::new(vec![])),
        }
    }

    async fn recv(rx: &mut mpsc::Receiver<(String, Incoming)>) -> Option<(String, Incoming)> {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn the_snapshot_on_connect_is_live_and_unavailable_is_said_so() {
        // The design decision in the module docs: HA's current belief is a
        // report, not a replay. The mutant is `retained: true` in
        // entity_incoming, which would leave a resting lock Unknown forever
        // and stop a leased one.
        let f = fake(json!([
            {"entity_id": "lock.door", "state": "unlocked"},
            {"entity_id": "switch.heater", "state": "unavailable"},
            {"entity_id": "lock.other", "state": "locked"}
        ]));
        let url = serve(f).await;
        let (tx, mut rx) = mpsc::channel(16);
        let _c = Connection::start(
            url,
            "good-token".into(),
            vec!["lock.door".into(), "switch.heater".into()],
            tx,
        );
        let mut got = vec![recv(&mut rx).await.unwrap(), recv(&mut rx).await.unwrap()];
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            got,
            vec![
                (
                    "lock.door".to_string(),
                    Incoming::State(Report {
                        on: true,
                        retained: false
                    })
                ),
                (
                    "switch.heater".to_string(),
                    Incoming::Availability { online: false }
                ),
            ]
        );
        // lock.other is not configured and is not reported.
        assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_state_change_is_a_report_and_a_transition_is_not() {
        let f = fake(json!([{"entity_id": "lock.door", "state": "locked"}]));
        f.push.lock().unwrap().extend([
            ("lock.door".to_string(), Some("unlocking".to_string())),
            ("lock.door".to_string(), Some("unlocked".to_string())),
            ("lock.other".to_string(), Some("unlocked".to_string())),
        ]);
        let url = serve(f).await;
        let (tx, mut rx) = mpsc::channel(16);
        let _c = Connection::start(url, "good-token".into(), vec!["lock.door".into()], tx);
        assert_eq!(
            recv(&mut rx).await.unwrap().1,
            Incoming::State(Report {
                on: false,
                retained: false
            })
        );
        // `unlocking` produced nothing; the next thing heard is `unlocked`.
        assert_eq!(
            recv(&mut rx).await.unwrap(),
            (
                "lock.door".to_string(),
                Incoming::State(Report {
                    on: true,
                    retained: false
                })
            )
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "lock.other is not ours"
        );
    }

    #[tokio::test]
    async fn a_service_call_reaches_ha_in_the_documented_shape_and_a_refusal_is_an_error() {
        let f = fake(
            json!([{"entity_id": "lock.door", "state": "locked"}, {"entity_id": "lock.jammed", "state": "locked"}]),
        );
        let calls = f.calls.clone();
        let url = serve(f).await;
        let (tx, mut rx) = mpsc::channel(16);
        let c = Connection::start(
            url,
            "good-token".into(),
            vec!["lock.door".into(), "lock.jammed".into()],
            tx,
        );
        recv(&mut rx).await.unwrap();
        recv(&mut rx).await.unwrap();
        c.call_service("lock", "unlock", "lock.door")
            .await
            .expect("HA said yes");
        let err = c
            .call_service("lock", "unlock", "lock.jammed")
            .await
            .expect_err("HA said no");
        assert!(err.to_string().contains("jammed"), "{err}");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["domain"], "lock");
        assert_eq!(calls[0]["service"], "unlock");
        assert_eq!(calls[0]["target"]["entity_id"], "lock.door");
        // ids are distinct and increasing: the second call did not reuse the first's.
        assert!(calls[1]["id"].as_u64().unwrap() > calls[0]["id"].as_u64().unwrap());
    }

    #[tokio::test]
    async fn a_rejected_token_reads_as_every_entity_unavailable_and_does_not_panic() {
        let f = fake(json!([]));
        let url = serve(f).await;
        let (tx, mut rx) = mpsc::channel(16);
        let c = Connection::start(url, "bad-token".into(), vec!["lock.door".into()], tx);
        assert_eq!(
            recv(&mut rx).await.unwrap(),
            (
                "lock.door".to_string(),
                Incoming::Availability { online: false }
            )
        );
        // And a command in that state is an error, not a hang past the timeout.
        let t0 = std::time::Instant::now();
        let err = c
            .call_service("lock", "unlock", "lock.door")
            .await
            .expect_err("not connected");
        assert!(
            t0.elapsed() <= CALL_TIMEOUT + Duration::from_secs(1),
            "{err}"
        );
    }

    #[tokio::test]
    async fn losing_ha_reads_as_every_entity_unavailable() {
        // The oracle for "the bridge must not pretend to see what it cannot":
        // when the socket drops, every watched entity is reported gone.
        let f = fake(json!([{"entity_id": "lock.door", "state": "unlocked"}]));
        let url = serve(f).await;
        let (tx, mut rx) = mpsc::channel(16);
        let c = Connection::start(url, "good-token".into(), vec!["lock.door".into()], tx);
        assert_eq!(
            recv(&mut rx).await.unwrap().1,
            Incoming::State(Report {
                on: true,
                retained: false
            })
        );
        // The fake drops the connection after answering this one.
        let _ = c.call_service("lock", "disconnect_me", "lock.door").await;
        assert_eq!(
            recv(&mut rx).await.unwrap(),
            (
                "lock.door".to_string(),
                Incoming::Availability { online: false }
            )
        );
        // ...and it reconnects: the snapshot comes round again.
        assert_eq!(
            recv(&mut rx).await.unwrap().1,
            Incoming::State(Report {
                on: true,
                retained: false
            })
        );
    }
}
