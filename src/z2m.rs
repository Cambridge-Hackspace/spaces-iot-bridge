//! Zigbee2MQTT as a backend: topics and payloads in both directions as pure
//! translation, plus the two thin pieces that touch the broker -- a [`Router`]
//! that says which device a delivered message belongs to and what it means,
//! and a [`Handle`] that carries out the core's commands for one device.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use crate::backend::Incoming;
use crate::config::DeviceEntry;
use crate::core::Report;
use crate::mqtt::{Connection, Delivered};

/// `<base>/<friendly>` -- the device's state topic.
pub fn state_topic(base: &str, friendly: &str) -> String {
    format!("{base}/{friendly}")
}
pub fn availability_topic(base: &str, friendly: &str) -> String {
    format!("{base}/{friendly}/availability")
}
pub fn set_topic(base: &str, friendly: &str) -> String {
    format!("{base}/{friendly}/set")
}
pub fn get_topic(base: &str, friendly: &str) -> String {
    format!("{base}/{friendly}/get")
}

/// Classify a message for a device, given its base and friendly name.
/// `retained` is the broker's flag on the delivered packet.
pub fn classify(
    base: &str,
    friendly: &str,
    topic: &str,
    payload: &[u8],
    retained: bool,
) -> Option<Incoming> {
    if topic == availability_topic(base, friendly) {
        return Some(
            parse_availability(payload).map_or(Incoming::Unrecognised, |online| {
                Incoming::Availability { online }
            }),
        );
    }
    if topic == state_topic(base, friendly) {
        if payload.is_empty() {
            return Some(Incoming::Cleared);
        }
        return Some(parse_state(payload).map_or(Incoming::Unrecognised, |on| {
            Incoming::State(Report { on, retained })
        }));
    }
    None
}

/// `{"state":"ON", ...}` -> true. Anything else is not a state we recognise.
fn parse_state(payload: &[u8]) -> Option<bool> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    match v.get("state")?.as_str()? {
        "ON" => Some(true),
        "OFF" => Some(false),
        _ => None,
    }
}

/// Z2M has published availability two ways across versions: a bare
/// `online`/`offline` string, and `{"state":"online"}`. Both are accepted,
/// because which one jarvis runs is a #9 question and refusing the other
/// would read as the plug being gone.
fn parse_availability(payload: &[u8]) -> Option<bool> {
    let text = std::str::from_utf8(payload).ok()?.trim();
    let word = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(v) => v.get("state")?.as_str()?.to_string(),
        Err(_) => text.to_string(),
    };
    match word.as_str() {
        "online" => Some(true),
        "offline" => Some(false),
        _ => None,
    }
}

/// The `/set` payload. `on_time` only with ON and only when the plug is known
/// to honour it -- the core decides that, this just serialises it.
pub fn set_payload(on: bool, on_time_s: Option<u32>) -> String {
    let mut v = serde_json::json!({ "state": if on { "ON" } else { "OFF" } });
    if let (true, Some(t)) = (on, on_time_s) {
        v["on_time"] = serde_json::json!(t);
    }
    v.to_string()
}

/// The `/get` payload: ask for a live state report.
pub fn get_payload() -> &'static str {
    r#"{"state":""}"#
}

/// Which device a message on the IoT broker belongs to, and what it says.
/// Pure: built once from the config, consulted per message.
pub struct Router {
    base: String,
    /// state topic -> device name, and availability topic -> device name.
    by_topic: HashMap<String, String>,
    friendly: HashMap<String, String>,
}

impl Router {
    pub fn new(base: &str, devices: &[DeviceEntry]) -> Self {
        let mut by_topic = HashMap::new();
        let mut friendly = HashMap::new();
        for d in devices
            .iter()
            .filter(|d| d.backend == crate::config::BackendKind::Zigbee2Mqtt)
        {
            by_topic.insert(state_topic(base, d.friendly()), d.name.clone());
            by_topic.insert(availability_topic(base, d.friendly()), d.name.clone());
            friendly.insert(d.name.clone(), d.friendly().to_string());
        }
        Self {
            base: base.to_string(),
            by_topic,
            friendly,
        }
    }

    /// The topics the IoT connection must subscribe to for these devices.
    pub fn subscriptions(&self) -> Vec<String> {
        let mut t: Vec<String> = self.by_topic.keys().cloned().collect();
        t.sort();
        t
    }

    /// `(device name, what it said)`, or `None` for a message on no device's
    /// topic -- including our own `/set` and `/get` echoes.
    pub fn route(&self, m: &Delivered) -> Option<(&str, Incoming)> {
        let name = self.by_topic.get(&m.topic)?;
        let friendly = &self.friendly[name];
        classify(&self.base, friendly, &m.topic, &m.payload, m.retained).map(|i| (name.as_str(), i))
    }
}

/// The drive half for one device: `/set` and `/get` on the IoT connection.
pub struct Handle {
    conn: Arc<Connection>,
    set_topic: String,
    get_topic: String,
}

impl Handle {
    pub fn new(conn: Arc<Connection>, base: &str, friendly: &str) -> Self {
        Self {
            conn,
            set_topic: set_topic(base, friendly),
            get_topic: get_topic(base, friendly),
        }
    }

    pub async fn set(&self, on: bool, on_time_s: Option<u32>) -> Result<()> {
        self.conn
            .publish(&self.set_topic, &set_payload(on, on_time_s))
            .await
    }

    pub async fn get(&self) -> Result<()> {
        self.conn.publish(&self.get_topic, get_payload()).await
    }

    pub fn describe(&self) -> String {
        format!("zigbee2mqtt {}", self.set_topic.trim_end_matches("/set"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: &str = "zigbee2mqtt";
    const F: &str = "hackspace/south_wall/plug/filters";

    #[test]
    fn a_state_report_carries_the_brokers_retain_flag_through() {
        // The adapter half of core rule 2. The mutant is passing `false` for
        // retained in `classify`.
        let t = state_topic(B, F);
        assert_eq!(
            classify(B, F, &t, br#"{"state":"ON","power":12.3}"#, true),
            Some(Incoming::State(Report {
                on: true,
                retained: true
            }))
        );
        assert_eq!(
            classify(B, F, &t, br#"{"state":"OFF"}"#, false),
            Some(Incoming::State(Report {
                on: false,
                retained: false
            }))
        );
    }

    #[test]
    fn availability_is_read_in_both_shapes_z2m_has_used() {
        let t = availability_topic(B, F);
        assert_eq!(
            classify(B, F, &t, b"offline", false),
            Some(Incoming::Availability { online: false })
        );
        assert_eq!(
            classify(B, F, &t, br#"{"state":"online"}"#, false),
            Some(Incoming::Availability { online: true })
        );
        assert_eq!(
            classify(B, F, &t, b"maybe", false),
            Some(Incoming::Unrecognised)
        );
    }

    #[test]
    fn an_empty_state_payload_is_a_retained_clear_not_an_unrecognised_report() {
        // The CS:S e2e stage clears its retained ON with an empty publish and
        // the bridge WARNed "unrecognised payload" (#18). Empty is how a
        // retained message is forgotten; it is neither a report nor garbage.
        // The mutant is removing the is_empty arm: empty then falls to
        // parse_state, fails to parse, and is Unrecognised again.
        let t = state_topic(B, F);
        assert_eq!(classify(B, F, &t, b"", true), Some(Incoming::Cleared));
        assert_eq!(classify(B, F, &t, b"", false), Some(Incoming::Cleared));
        // Only on the state topic: an empty availability is still not a word.
        assert_eq!(
            classify(B, F, &availability_topic(B, F), b"", false),
            Some(Incoming::Unrecognised)
        );
    }

    #[test]
    fn another_devices_topic_is_not_ours() {
        assert_eq!(
            classify(B, F, "zigbee2mqtt/other/plug", br#"{"state":"ON"}"#, false),
            None
        );
        assert_eq!(
            classify(
                B,
                F,
                "zigbee2mqtt/hackspace/south_wall/plug/filters/set",
                b"{}",
                false
            ),
            None,
            "our own command echo is not a report"
        );
    }

    #[test]
    fn set_carries_on_time_only_with_on() {
        // The mutant is dropping the `on &&` from the condition: OFF would then
        // carry an on_time, which some plugs treat as "turn on in N".
        assert_eq!(
            set_payload(true, Some(60)),
            r#"{"on_time":60,"state":"ON"}"#
        );
        assert_eq!(set_payload(true, None), r#"{"state":"ON"}"#);
        assert_eq!(set_payload(false, Some(60)), r#"{"state":"OFF"}"#);
    }

    fn devices() -> Vec<DeviceEntry> {
        let mk = |name: &str, friendly: &str| DeviceEntry {
            name: name.into(),
            backend: crate::config::BackendKind::Zigbee2Mqtt,
            friendly: Some(friendly.into()),
            ieee: None,
            entity: None,
            role: "power".into(),
            resource: format!("{name}-tool"),
            identity: "/nonexistent".into(),
            countdown: false,
            on_time_s: 60,
            rearm_s: 15,
            stop_resource_on_loss: false,
        };
        vec![
            mk("extractor", "plug/extractor"),
            mk("heater", "plug/heater"),
        ]
    }

    fn delivered(topic: &str, payload: &[u8], retained: bool) -> Delivered {
        Delivered {
            topic: topic.into(),
            payload: payload.to_vec(),
            retained,
        }
    }

    #[test]
    fn the_router_hands_each_message_to_the_device_whose_topic_it_is() {
        // This replaces two HashMaps and a lookup that lived untested in
        // main.rs. The mutants: keying by friendly name alone (a different
        // base would then match), and returning the FIRST device for any
        // known topic.
        let r = Router::new(B, &devices());
        let (who, what) = r
            .route(&delivered(
                "zigbee2mqtt/plug/heater",
                br#"{"state":"OFF"}"#,
                false,
            ))
            .expect("the heater's state topic routes");
        assert_eq!(who, "heater");
        assert_eq!(
            what,
            Incoming::State(Report {
                on: false,
                retained: false
            })
        );
        let (who, what) = r
            .route(&delivered(
                "zigbee2mqtt/plug/extractor/availability",
                b"offline",
                false,
            ))
            .expect("the extractor's availability topic routes");
        assert_eq!(who, "extractor");
        assert_eq!(what, Incoming::Availability { online: false });
        // The retain flag survives the trip.
        let (_, what) = r
            .route(&delivered(
                "zigbee2mqtt/plug/heater",
                br#"{"state":"ON"}"#,
                true,
            ))
            .unwrap();
        assert_eq!(
            what,
            Incoming::State(Report {
                on: true,
                retained: true
            })
        );
    }

    #[test]
    fn the_router_takes_only_the_zigbee2mqtt_devices_from_a_mixed_list() {
        // The sample has an extractor on Zigbee2MQTT and a lock on Home
        // Assistant. The mutant is a router that does not filter by backend:
        // it then asks the lock for a friendly name it does not have.
        let cfg = crate::config::parse(include_str!("../config.sample.toml")).unwrap();
        let r = Router::new("zigbee2mqtt", &cfg.devices);
        assert_eq!(
            r.subscriptions(),
            vec![
                "zigbee2mqtt/hackspace/south_wall/plug/filters",
                "zigbee2mqtt/hackspace/south_wall/plug/filters/availability",
            ]
        );
    }

    #[test]
    fn the_router_ignores_what_is_nobodys_and_our_own_echoes() {
        let r = Router::new(B, &devices());
        assert!(r
            .route(&delivered("zigbee2mqtt/plug/other", b"{}", false))
            .is_none());
        assert!(
            r.route(&delivered("zigbee2mqtt/plug/heater/set", b"{}", false))
                .is_none(),
            "our own /set echo is not a report"
        );
        assert!(r
            .route(&delivered("other/plug/heater", br#"{"state":"ON"}"#, false))
            .is_none());
        // The base is the config's, not a constant: under another base the
        // same friendly name lives on another topic. The mutant is a router
        // that ignores `base` and assumes "zigbee2mqtt".
        let other = Router::new("z2m-east", &devices());
        assert_eq!(
            other
                .route(&delivered(
                    "z2m-east/plug/heater",
                    br#"{"state":"ON"}"#,
                    false
                ))
                .map(|(n, _)| n),
            Some("heater")
        );
        assert!(other
            .route(&delivered(
                "zigbee2mqtt/plug/heater",
                br#"{"state":"ON"}"#,
                false
            ))
            .is_none());
        // And it subscribes to exactly the state and availability topics.
        assert_eq!(
            r.subscriptions(),
            vec![
                "zigbee2mqtt/plug/extractor",
                "zigbee2mqtt/plug/extractor/availability",
                "zigbee2mqtt/plug/heater",
                "zigbee2mqtt/plug/heater/availability",
            ]
        );
    }
}
