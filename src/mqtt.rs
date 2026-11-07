//! One broker connection, as a task: it polls the event loop forever,
//! reconnecting with backoff, and hands every delivered publish -- with the
//! broker's retain flag intact -- to a channel. The same code serves the IoT
//! broker and the edge broker; what differs is who subscribes to what.

use std::time::Duration;

use anyhow::Result;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{broker_addr, Credentials};

/// A delivered message. `retained` is the broker's flag, which is the whole
/// point of carrying it: it is what tells a replay from the plug speaking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivered {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retained: bool,
}

/// The MQTT client id for one of this process's connections.
///
/// Two things have to be true of it, and the first run of the CS:S e2e stage
/// found out what happens when they are not. A broker allows ONE session per
/// client id: a second connection with the same id evicts the first, which
/// reconnects and evicts the second, and so on at the reconnect interval. The
/// bridge's `iot` and `edge` connections both carried the literal
/// "spaces-iot-bridge", which was invisible on two brokers and a one-second
/// flap on one -- and one broker for Zigbee2MQTT and the edge is an ordinary
/// hackspace deployment. So: distinct per ROLE within a process, and distinct
/// per HOST across processes (two bridges on one edge broker is also ordinary).
/// The host is named by the same MAC that names it at registration; a host
/// with no MAC falls back to its pid, which registration would have refused
/// anyway.
pub fn client_id(role: &str) -> String {
    let instance = crate::register::host_mac()
        .map(|m| m.replace(':', ""))
        .unwrap_or_else(|_| std::process::id().to_string());
    format!("bridge-{role}-{instance}")
}

pub struct Connection {
    pub client: AsyncClient,
}

impl Connection {
    /// Connect, subscribe to `topics`, and spawn the poll loop. Delivered
    /// messages go to `tx`. The loop never ends; a lost connection is retried
    /// with backoff, and subscriptions are re-sent on each reconnect because
    /// the session is not persistent.
    pub fn start(
        name: &'static str,
        client_id: &str,
        url: &str,
        creds: Option<&Credentials>,
        topics: Vec<String>,
        tx: mpsc::Sender<Delivered>,
    ) -> Result<Self> {
        let (host, port) = broker_addr(url)?;
        let mut opts = MqttOptions::new(client_id, host, port);
        opts.set_keep_alive(Duration::from_secs(15));
        opts.set_clean_session(true);
        if let Some(c) = creds {
            opts.set_credentials(c.username.clone(), c.password.clone());
        }
        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        let subscriber = client.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!("{name}: connected");
                        backoff = Duration::from_secs(1);
                        for t in &topics {
                            if let Err(e) = subscriber.subscribe(t.clone(), QoS::AtLeastOnce).await
                            {
                                warn!("{name}: subscribe {t} failed: {e}");
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let d = Delivered {
                            topic: p.topic.clone(),
                            payload: p.payload.to_vec(),
                            retained: p.retain,
                        };
                        if tx.send(d).await.is_err() {
                            warn!("{name}: receiver gone; stopping");
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!("{name}: connection error ({e}); retrying in {backoff:?}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        });
        Ok(Self { client })
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload.as_bytes())
            .await?;
        Ok(())
    }
}

/// The broker tier. Runs only when `BRIDGE_TEST_BROKER=host:port` names a
/// real broker; the tenant starts a mosquitto and sets it. `#[ignore]` is the
/// opt-in, not a skip: the unit suite is hermetic and this one is not.
#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn each_role_gets_its_own_client_id_and_one_host_gets_one_instance() {
        // The mutant is a constant id. The instance half must be the same for
        // both, or two roles on one host would look like two hosts.
        let iot = client_id("iot");
        let edge = client_id("edge");
        assert_ne!(iot, edge);
        assert!(iot.starts_with("bridge-iot-"), "{iot}");
        assert!(edge.starts_with("bridge-edge-"), "{edge}");
        assert_eq!(
            iot.trim_start_matches("bridge-iot-"),
            edge.trim_start_matches("bridge-edge-")
        );
        assert!(
            !iot.contains(':'),
            "a MAC is written without separators: {iot}"
        );
    }
}

#[cfg(test)]
mod broker_tests {
    use super::*;

    fn broker() -> Option<String> {
        std::env::var("BRIDGE_TEST_BROKER")
            .ok()
            .map(|b| format!("mqtt://{b}"))
    }

    #[tokio::test]
    #[ignore = "needs BRIDGE_TEST_BROKER=host:port"]
    async fn a_retained_message_arrives_flagged_retained_from_a_real_broker() {
        // The oracle for rule 2's adapter half: the flag the core relies on is
        // set by the BROKER on replay, and this proves a real one sets it and
        // rumqttc surfaces it. The mutant is `retained: false` in `start`.
        let url = broker().expect("BRIDGE_TEST_BROKER");
        let topic = format!("bridge-test/{}/state", std::process::id());
        // Publish retained from one connection, then subscribe from a NEW one:
        // what it receives is the replay.
        let (tx0, _rx0) = mpsc::channel(8);
        let writer =
            Connection::start("writer", "bridge-test-writer", &url, None, vec![], tx0).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        writer
            .client
            .publish(
                topic.clone(),
                QoS::AtLeastOnce,
                true,
                br#"{"state":"ON"}"#.as_slice(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (tx, mut rx) = mpsc::channel(8);
        let _reader = Connection::start(
            "reader",
            "bridge-test-reader",
            &url,
            None,
            vec![topic.clone()],
            tx,
        )
        .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a replay within 5 s")
            .expect("channel");
        assert_eq!(got.topic, topic);
        assert!(
            got.retained,
            "a replayed message must carry the broker's retain flag"
        );

        // And a live publish on the same topic is NOT flagged.
        writer.publish(&topic, r#"{"state":"OFF"}"#).await.unwrap();
        let live = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("live within 5 s")
            .expect("channel");
        assert!(!live.retained, "a live message is not a replay");
        // Leave nothing retained behind for the next run.
        writer
            .client
            .publish(topic, QoS::AtLeastOnce, true, "".as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "needs BRIDGE_TEST_BROKER=host:port"]
    async fn the_bridges_two_connections_coexist_on_one_broker() {
        // The defect the first e2e run found: both connections carried the same
        // client id, and on one broker each connect evicted the other every
        // second. Two subscribers with the ids the bridge really uses must BOTH
        // see every one of a run of live publishes; a flapping connection
        // misses some. The mutant is `client_id` returning the same string for
        // both roles.
        let url = broker().expect("BRIDGE_TEST_BROKER");
        let topic = format!("bridge-test/{}/coexist", std::process::id());
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        let id_a = client_id("iot");
        let id_b = client_id("edge");
        let _a = Connection::start("iot", &id_a, &url, None, vec![topic.clone()], tx_a).unwrap();
        let _b = Connection::start("edge", &id_b, &url, None, vec![topic.clone()], tx_b).unwrap();
        // Long enough for an eviction cycle (reconnect backoff is 1 s) to show.
        tokio::time::sleep(Duration::from_millis(2500)).await;

        let (tx0, _rx0) = mpsc::channel(8);
        let writer =
            Connection::start("writer", "bridge-test-writer2", &url, None, vec![], tx0).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        const N: usize = 6;
        for i in 0..N {
            writer.publish(&topic, &format!("{i}")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        let mut seen_a = Vec::new();
        let mut seen_b = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline && (seen_a.len() < N || seen_b.len() < N) {
            tokio::select! {
                Some(m) = rx_a.recv() => seen_a.push(String::from_utf8_lossy(&m.payload).to_string()),
                Some(m) = rx_b.recv() => seen_b.push(String::from_utf8_lossy(&m.payload).to_string()),
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
        assert_eq!(
            seen_a.len(),
            N,
            "iot connection missed publishes: {seen_a:?}"
        );
        assert_eq!(
            seen_b.len(),
            N,
            "edge connection missed publishes: {seen_b:?}"
        );
    }
}
