//! The bridge. One task per device owning a [`core::Device`]; two broker
//! connections; commands applied as they come. Design: issue #1.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use spaces_device::wire::topic;
use spaces_iot_bridge::config::{self, DeviceEntry};
use spaces_iot_bridge::core::{Command, Device, DeviceConfig};
use spaces_iot_bridge::mqtt::{Connection, Delivered};
use spaces_iot_bridge::z2m;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/config.toml"));
    let cfg = config::load(&path)?;
    info!(
        "spaces-iot-bridge {} with {} device(s)",
        env!("CARGO_PKG_VERSION"),
        cfg.devices.len()
    );
    anyhow::ensure!(!cfg.devices.is_empty(), "no [[device]] configured");

    let iot_creds = config::load_credentials(&cfg.iot.credentials)?;
    let edge_creds = config::load_credentials(&cfg.edge.credentials)?;

    // Identities: registered by #5; refused here with the path, not guessed.
    let mut identities = HashMap::new();
    for d in &cfg.devices {
        identities.insert(
            d.name.clone(),
            config::load_identity(&d.identity).with_context(|| d.name.clone())?,
        );
    }

    let (iot_tx, mut iot_rx) = mpsc::channel::<Delivered>(256);
    let mut iot_topics = Vec::new();
    for d in &cfg.devices {
        iot_topics.push(z2m::state_topic(&cfg.iot.base, &d.friendly));
        iot_topics.push(z2m::availability_topic(&cfg.iot.base, &d.friendly));
    }
    let iot = Arc::new(Connection::start(
        "iot",
        "spaces-iot-bridge",
        &cfg.iot.broker,
        Some(&iot_creds),
        iot_topics,
        iot_tx,
    )?);

    let (edge_tx, mut edge_rx) = mpsc::channel::<Delivered>(256);
    // One connection for every identity (#1 q4): the edge reads device_id from
    // the payload and every module sees every lease.
    let edge = Arc::new(Connection::start(
        "edge",
        "spaces-iot-bridge",
        &cfg.edge.broker,
        Some(&edge_creds),
        topic::SUBSCRIPTIONS.iter().map(|s| s.to_string()).collect(),
        edge_tx,
    )?);

    // One task per device. Everything it needs arrives on its own channel.
    let mut devices: HashMap<String, mpsc::Sender<Input>> = HashMap::new();
    let mut by_state_topic: HashMap<String, String> = HashMap::new();
    let mut by_avail_topic: HashMap<String, String> = HashMap::new();
    for d in cfg.devices.clone() {
        let (tx, rx) = mpsc::channel::<Input>(64);
        by_state_topic.insert(z2m::state_topic(&cfg.iot.base, &d.friendly), d.name.clone());
        by_avail_topic.insert(
            z2m::availability_topic(&cfg.iot.base, &d.friendly),
            d.name.clone(),
        );
        devices.insert(d.name.clone(), tx);
        let identity = identities[&d.name].clone();
        tokio::spawn(run_device(
            d,
            cfg.iot.base.clone(),
            identity.device_id,
            rx,
            iot.clone(),
            edge.clone(),
        ));
    }

    // Routing. IoT messages go to the one device whose topic it is; every edge
    // message goes to every device, because every module sees every lease and
    // each one filters by its own device_id (core, two_devices test).
    let base = cfg.iot.base.clone();
    loop {
        tokio::select! {
            Some(m) = iot_rx.recv() => {
                let name = by_state_topic.get(&m.topic).or_else(|| by_avail_topic.get(&m.topic));
                match name.and_then(|n| devices.get(n)) {
                    Some(tx) => { let _ = tx.send(Input::Iot(m)).await; }
                    None => warn!("iot: message on {} matches no device (base {base})", m.topic),
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
    Iot(Delivered),
    Edge(Delivered),
}

async fn run_device(
    d: DeviceEntry,
    base: String,
    device_id: String,
    mut rx: mpsc::Receiver<Input>,
    iot: Arc<Connection>,
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
                Input::Iot(m) => match z2m::classify(&base, &d.friendly, &m.topic, &m.payload, m.retained) {
                    Some(z2m::Incoming::State(r)) => dev.report(r, now()),
                    Some(z2m::Incoming::Availability { online }) => dev.availability(online, now()),
                    Some(z2m::Incoming::Unrecognised) => { warn!("{}: unrecognised payload on {}", d.name, m.topic); vec![] }
                    None => vec![],
                },
                Input::Edge(m) => dev.edge_message(&m.topic, &m.payload, now()),
            },
        };
        for c in cmds {
            let r = match c {
                Command::Z2mSet { on, on_time_s } => {
                    info!(
                        "{}: set {}{}",
                        d.name,
                        if on { "ON" } else { "OFF" },
                        on_time_s
                            .map(|t| format!(" on_time={t}"))
                            .unwrap_or_default()
                    );
                    iot.publish(
                        &z2m::set_topic(&base, &d.friendly),
                        &z2m::set_payload(on, on_time_s),
                    )
                    .await
                }
                Command::Z2mGet => {
                    iot.publish(&z2m::get_topic(&base, &d.friendly), z2m::get_payload())
                        .await
                }
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
