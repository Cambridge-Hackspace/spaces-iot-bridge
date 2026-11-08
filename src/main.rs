//! The bridge. One task per device owning a [`core::Device`]; two broker
//! connections; commands applied as they come. Design: issue #1.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use spaces_device::wire::topic;
use spaces_iot_bridge::backend::{Backend, Incoming};
use spaces_iot_bridge::config::{self, BackendKind, DeviceEntry};
use spaces_iot_bridge::core::{Command, Device, DeviceConfig};
use spaces_iot_bridge::ha;
use spaces_iot_bridge::mqtt::{self, Connection, Delivered};
use spaces_iot_bridge::z2m;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(clap::Parser)]
#[command(name = "spaces-iot-bridge", version, about)]
struct Cli {
    /// The config file. Its shape is config.sample.toml.
    #[arg(long, default_value = "/config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Bridge every configured device until stopped.
    Run,
    /// Measure whether a plug honours on_time, and whether re-sending it
    /// while ON extends the deadline. Prints; writes nothing. Run with the
    /// bridge stopped, on a plug nothing is using.
    Probe {
        /// The device's `name` in the config.
        #[arg(long)]
        device: String,
    },
    /// Claim an invite for one device and persist its identity. Once.
    Register {
        /// The device's `name` in the config.
        #[arg(long)]
        device: String,
        /// The invite code from the platform (an administrator mints one).
        #[arg(long)]
        invite: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();
    // Before the one thing that builds a TLS config. rustls panics, not errors,
    // without a provider.
    spaces_iot_bridge::tls::install_crypto_provider();
    let cli = <Cli as clap::Parser>::parse();
    let cfg = config::load(&cli.config)?;
    if let Cmd::Probe { device } = &cli.cmd {
        let d = cfg
            .devices
            .iter()
            .find(|d| &d.name == device)
            .with_context(|| {
                format!("no [[device]] named {device:?} in {}", cli.config.display())
            })?;
        anyhow::ensure!(
            d.backend == BackendKind::Zigbee2Mqtt,
            "{}: `probe` measures a plug over Zigbee2MQTT; nothing measures a Home Assistant entity's countdown yet (#20)",
            d.name
        );
        let iot = cfg.iot.as_ref().context("probe needs an [iot] section")?;
        let creds = config::load_credentials(&iot.credentials)?;
        return probe_live(&iot.broker, &creds, &iot.base, d).await;
    }
    if let Cmd::Register { device, invite } = &cli.cmd {
        let d = cfg
            .devices
            .iter()
            .find(|d| &d.name == device)
            .with_context(|| {
                format!("no [[device]] named {device:?} in {}", cli.config.display())
            })?;
        let id = spaces_iot_bridge::register::register(&cfg.server.url, d, invite).await?;
        info!(
            "{}: registered as {}; identity written to {}",
            d.name,
            id.device_id,
            d.identity.display()
        );
        return Ok(());
    }
    info!(
        "spaces-iot-bridge {} with {} device(s)",
        env!("CARGO_PKG_VERSION"),
        cfg.devices.len()
    );
    anyhow::ensure!(!cfg.devices.is_empty(), "no [[device]] configured");

    let edge_creds = config::load_credentials(&cfg.edge.credentials)?;

    // Identities: registered by #5; refused here with the path, not guessed.
    let mut identities = HashMap::new();
    for d in &cfg.devices {
        identities.insert(
            d.name.clone(),
            config::load_identity(&d.identity).with_context(|| d.name.clone())?,
        );
    }

    // The Zigbee2MQTT side: one connection and a router that says which device
    // a message is for. Only when a device needs it -- a Home Assistant-only
    // bridge never touches the IoT broker. The sender is kept alive either way
    // so the receive arm below pends rather than closing.
    let (iot_tx, mut iot_rx) = mpsc::channel::<Delivered>(256);
    let has_z2m = cfg
        .devices
        .iter()
        .any(|d| d.backend == BackendKind::Zigbee2Mqtt);
    let mut z2m_side: Option<(z2m::Router, Arc<Connection>, String)> = None;
    if has_z2m {
        let iot_cfg = cfg
            .iot
            .as_ref()
            .expect("parse requires [iot] for a zigbee2mqtt device");
        let iot_creds = config::load_credentials(&iot_cfg.credentials)?;
        let router = z2m::Router::new(&iot_cfg.base, &cfg.devices);
        let conn = Arc::new(Connection::start(
            "iot",
            &mqtt::client_id("iot"),
            &iot_cfg.broker,
            Some(&iot_creds),
            router.subscriptions(),
            iot_tx.clone(),
        )?);
        z2m_side = Some((router, conn, iot_cfg.base.clone()));
    }

    // The Home Assistant side (#20): one WebSocket to one HA, shared by every
    // device it owns; events arrive already classified, keyed by entity.
    let (ha_tx, mut ha_rx) = mpsc::channel::<(String, Incoming)>(256);
    let ha_devices: Vec<&DeviceEntry> = cfg
        .devices
        .iter()
        .filter(|d| d.backend == BackendKind::Homeassistant)
        .collect();
    let mut ha_conn: Option<Arc<ha::Connection>> = None;
    let mut by_entity: HashMap<String, String> = HashMap::new();
    if !ha_devices.is_empty() {
        let ha_cfg = cfg
            .homeassistant
            .as_ref()
            .expect("parse requires [homeassistant] for a homeassistant device");
        let token = config::load_ha_token(&ha_cfg.token_file)?;
        for d in &ha_devices {
            by_entity.insert(d.entity().to_string(), d.name.clone());
        }
        ha_conn = Some(Arc::new(ha::Connection::start(
            ha_cfg.url.clone(),
            token,
            by_entity.keys().cloned().collect(),
            ha_tx.clone(),
        )));
    }

    let (edge_tx, mut edge_rx) = mpsc::channel::<Delivered>(256);
    // One connection for every identity (#1 q4): the edge reads device_id from
    // the payload and every module sees every lease.
    let edge = Arc::new(Connection::start(
        "edge",
        &mqtt::client_id("edge"),
        &cfg.edge.broker,
        Some(&edge_creds),
        topic::SUBSCRIPTIONS.iter().map(|s| s.to_string()).collect(),
        edge_tx,
    )?);

    // One task per device. Everything it needs arrives on its own channel.
    let mut devices: HashMap<String, mpsc::Sender<Input>> = HashMap::new();
    for d in cfg.devices.clone() {
        let (tx, rx) = mpsc::channel::<Input>(64);
        devices.insert(d.name.clone(), tx);
        let identity = identities[&d.name].clone();
        let backend = match d.backend {
            BackendKind::Zigbee2Mqtt => {
                let (_, conn, base) = z2m_side
                    .as_ref()
                    .expect("a zigbee2mqtt device has the Zigbee2MQTT side");
                Backend::Zigbee2Mqtt(z2m::Handle::new(conn.clone(), base, d.friendly()))
            }
            BackendKind::Homeassistant => Backend::HomeAssistant(ha::Handle::new(
                ha_conn
                    .clone()
                    .expect("a homeassistant device has the Home Assistant side"),
                d.entity(),
            )),
        };
        tokio::spawn(run_device(d, identity.device_id, rx, backend, edge.clone()));
    }

    // Routing. IoT messages go to the one device whose topic it is, already
    // classified; every edge message goes to every device, because every
    // module sees every lease and each one filters by its own device_id (core,
    // two_devices test).
    let router = z2m_side.as_ref().map(|(r, _, _)| r);
    loop {
        tokio::select! {
            Some(m) = iot_rx.recv() => {
                let Some(router) = router else { continue };
                match router.route(&m) {
                    Some((name, Incoming::Unrecognised)) => warn!("{name}: unrecognised payload on {}", m.topic),
                    Some((name, Incoming::Cleared)) => debug!("{name}: retained state cleared on {}", m.topic),
                    Some((name, incoming)) => {
                        if let Some(tx) = devices.get(name) {
                            let _ = tx.send(Input::Device(incoming)).await;
                        }
                    }
                    None => warn!("iot: message on {} matches no device", m.topic),
                }
            }
            Some((entity, incoming)) = ha_rx.recv() => {
                match by_entity.get(&entity).and_then(|n| devices.get(n)) {
                    Some(tx) => { let _ = tx.send(Input::Device(incoming)).await; }
                    None => warn!("ha: event for {entity} matches no device"),
                }
            }
            Some(m) = edge_rx.recv() => {
                for tx in devices.values() {
                    let _ = tx.send(Input::Edge(m.clone())).await;
                }
            }
            _ = tokio::signal::ctrl_c() => { info!("shutting down"); return Ok(()); }
        }
    }
}

enum Input {
    /// From the device's backend, already in the core's terms.
    Device(Incoming),
    Edge(Delivered),
}

async fn run_device(
    d: DeviceEntry,
    device_id: String,
    mut rx: mpsc::Receiver<Input>,
    backend: Backend,
    edge: Arc<Connection>,
) {
    let mut dev = Device::new(DeviceConfig {
        device_id,
        resource: d.resource.clone(),
        countdown: d.countdown,
        on_time_s: d.on_time_s,
        rearm_s: d.rearm_s,
        stop_on_loss: d.stop_resource_on_loss,
    });
    let epoch = Instant::now();
    let now = || epoch.elapsed().as_millis() as u64;
    // The Module notices a lapsed lease only when called; every ~50 ms keeps
    // the cut well inside a lease's TTL (spaces-device's own guidance).
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    loop {
        let cmds = tokio::select! {
            _ = ticker.tick() => dev.tick(now()),
            Some(input) = rx.recv() => match input {
                Input::Device(Incoming::State(r)) => dev.report(r, now()),
                Input::Device(Incoming::Availability { online }) => dev.availability(online, now()),
                // The router logs these itself and does not forward them.
                Input::Device(Incoming::Cleared | Incoming::Unrecognised) => vec![],
                Input::Edge(m) => dev.edge_message(&m.topic, &m.payload, now()),
            },
        };
        for c in cmds {
            let r = match c {
                Command::Set { on, on_time_s } => {
                    info!(
                        "{}: set {}{} ({})",
                        d.name,
                        if on { "ON" } else { "OFF" },
                        on_time_s
                            .map(|t| format!(" on_time={t}"))
                            .unwrap_or_default(),
                        backend.describe()
                    );
                    backend.set(on, on_time_s).await
                }
                Command::Get => backend.get().await,
                Command::Publish { topic, payload } => edge.publish(topic, &payload).await,
                Command::Stop { resource } => {
                    warn!("{}: stopping {resource}", d.name);
                    edge.publish(
                        topic::STOP_REQUEST,
                        &serde_json::json!({ "resource": resource }).to_string(),
                    )
                    .await
                }
                Command::Log(s) => {
                    info!("{}: {s}", d.name);
                    Ok(())
                }
            };
            if let Err(e) = r {
                error!("{}: publish failed: {e}", d.name);
            }
        }
    }
}

/// The probe's driver: two halves, timestamps collected, verdicts printed.
async fn probe_live(
    broker: &str,
    creds: &config::Credentials,
    base: &str,
    d: &DeviceEntry,
) -> Result<()> {
    use spaces_iot_bridge::probe::{self, Verdict, ON_TIME_S};
    let state_topic = z2m::state_topic(base, d.friendly());
    let (tx, mut rx) = mpsc::channel::<Delivered>(64);
    let conn = Connection::start(
        "probe",
        &mqtt::client_id("probe"),
        broker,
        Some(creds),
        vec![state_topic.clone()],
        tx,
    )?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    // Drain whatever the broker replays on subscribe: a retained state is not
    // the plug speaking, and it would otherwise look like an instant ON.
    while let Ok(m) = rx.try_recv() {
        if m.retained {
            info!(
                "probe: discarded a retained {} on {}",
                String::from_utf8_lossy(&m.payload),
                m.topic
            );
        }
    }

    info!("probe: half 1 -- does the plug honour on_time={ON_TIME_S}?");
    let o1 = probe_half(&conn, &mut rx, base, d, None).await?;
    let v1 = probe::verdict_countdown(o1, ON_TIME_S);
    // Make sure it is OFF and settled before half 2.
    conn.publish(
        &z2m::set_topic(base, d.friendly()),
        &z2m::set_payload(false, None),
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    while rx.try_recv().is_ok() {}

    let rearm = Duration::from_millis(u64::from(ON_TIME_S) * 500);
    info!(
        "probe: half 2 -- does re-sending at {} ms extend the deadline?",
        rearm.as_millis()
    );
    let o2 = probe_half(&conn, &mut rx, base, d, Some(rearm)).await?;
    let v2 = probe::verdict_rearm(o2, ON_TIME_S, rearm.as_millis() as u64);
    conn.publish(
        &z2m::set_topic(base, d.friendly()),
        &z2m::set_payload(false, None),
    )
    .await?;

    let show = |name: &str, v: &Verdict| match v {
        Verdict::Pass { off_after_ms } => println!("  PASS  {name}: OFF at {off_after_ms} ms"),
        Verdict::Fail(why) => println!("  FAIL  {name}: {why}"),
    };
    println!("probe {}:", d.name);
    show("honours on_time", &v1);
    show("re-arm extends the deadline", &v2);
    if v1.passed() && v2.passed() {
        println!(
            "both halves pass: you may set `countdown = true` for {} in the config, by hand.",
            d.name
        );
    } else {
        println!("leave `countdown = false` for {}: the bridge's own watchdog will be its only fail-safe, and the tool will honestly report power_fails_safe = false.", d.name);
    }
    Ok(())
}

/// One half of the probe: send `ON on_time`, optionally re-send it at
/// `rearm_at`, and record when the plug reports ON and then OFF, in ms from
/// the first send. A free function rather than a closure: it borrows the
/// receiver mutably, and a closure returning a future cannot lend it twice.
async fn probe_half(
    conn: &Connection,
    rx: &mut mpsc::Receiver<Delivered>,
    base: &str,
    d: &DeviceEntry,
    rearm_at: Option<Duration>,
) -> Result<spaces_iot_bridge::probe::Observed> {
    use spaces_iot_bridge::probe::{self, Observed, ON_TIME_S};
    let expected_ms = rearm_at.map_or(0, |r| r.as_millis() as u64) + u64::from(ON_TIME_S) * 1_000;
    let budget = probe::wait_budget(expected_ms);
    let set_topic = z2m::set_topic(base, d.friendly());
    let t0 = Instant::now();
    conn.publish(&set_topic, &z2m::set_payload(true, Some(ON_TIME_S)))
        .await?;
    let mut on_at = None;
    let mut off_at = None;
    let mut resent = false;
    loop {
        let elapsed = t0.elapsed();
        if elapsed > budget {
            break;
        }
        if let Some(r) = rearm_at {
            if !resent && elapsed >= r {
                conn.publish(&set_topic, &z2m::set_payload(true, Some(ON_TIME_S)))
                    .await?;
                info!(
                    "probe: re-sent ON on_time={ON_TIME_S} at {} ms",
                    elapsed.as_millis()
                );
                resent = true;
            }
        }
        let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await else {
            continue;
        };
        if m.retained {
            continue; // a replay is not the plug speaking
        }
        if let Some(Incoming::State(rep)) =
            z2m::classify(base, d.friendly(), &m.topic, &m.payload, m.retained)
        {
            let at = t0.elapsed().as_millis() as u64;
            if rep.on && on_at.is_none() {
                on_at = Some(at);
                info!("probe: plug reports ON at {at} ms");
            } else if !rep.on && on_at.is_some() && off_at.is_none() {
                off_at = Some(at);
                info!("probe: plug reports OFF at {at} ms");
                break;
            }
        }
    }
    Ok(Observed {
        on_at_ms: on_at,
        off_at_ms: off_at,
    })
}
