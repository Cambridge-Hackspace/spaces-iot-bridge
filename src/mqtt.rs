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
}
