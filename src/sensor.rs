//! A bridged sensor (#25): a device that reports one interlock condition and
//! switches nothing.
//!
//! Where a plug ([`crate::core::Device`]) is driven by the lease, a sensor is
//! read by the edge: it publishes `resource/request/condition` with the
//! plain truth of its condition -- `door_open: true` means the door is open
//! -- and `resource/request/alive`, and that is all. The edge (CS:S #213)
//! debounces, gates and trips on the reading; leases still arrive for a
//! sensor binding and are ignored here, because there is nothing to switch.
//!
//! Two rules, both about what is NOT said:
//!
//! 1. **An unknown sensor says nothing.** Not the condition and not alive.
//!    A condition the edge has never observed denies the tool, and a sensor
//!    that has gone quiet trips it, so silence is the safe shape of "I do not
//!    know" -- and it is the only shape the edge can act on. A bridge that
//!    kept pulsing alive for a contact Home Assistant had lost would hide the
//!    loss behind a healthy heartbeat.
//! 2. **A change is reported at once; a steady state on a timer.** The edge
//!    denies until the first observation, so a sensor that reported only on
//!    change would leave its tool denied until the door moved. The timer is
//!    what makes a resting sensor count; the immediate report is what makes a
//!    trip prompt.

use css_wire::seal::Sealed;
use css_wire_sign::{Peer, Refused};
use spaces_device::wire::{topic, AliveRequest, ConditionReport, Lease, LocalHello};

use crate::core::{Command, Report};

/// How often a known sensor pulses alive. The firmware heartbeat is 1 s.
pub const ALIVE_EVERY_MS: u64 = 1_000;
/// How often a steady reading is re-sent. Well inside the edge's offline
/// window, and short enough that a tool is not denied for long after the
/// bridge (re)connects.
pub const REPORT_EVERY_MS: u64 = 3_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensorConfig {
    pub device_id: String,
    pub resource: String,
    /// The condition this sensor is the reading for, from the server's
    /// vocabulary: `door_open`, `flow_ok`, ...
    pub condition: String,
    /// Home Assistant's `on` for a door contact means open, which for
    /// `door_open` is asserted. A contact wired the other way, or a
    /// requirement read from a sensor whose `on` means the bad state, inverts.
    pub invert: bool,
}

pub struct Sensor {
    cfg: SensorConfig,
    /// `Some(asserted)` once a live reading has been heard; `None` before,
    /// and again whenever the backend says the sensor is unreachable.
    known: Option<bool>,
    last_alive: Option<u64>,
    last_report: Option<u64>,
    /// The command key and this end of the wire, once [`Sensor::signing`].
    seals: Option<(Vec<u8>, Peer)>,
}

impl Sensor {
    pub fn new(cfg: SensorConfig) -> Self {
        Self {
            cfg,
            known: None,
            last_alive: None,
            last_report: None,
            seals: None,
        }
    }

    /// Seal every report with this device's command key (CS:S #249). The
    /// edge opens a sealed condition only when it names the edge's current
    /// nonce, which a sensor learns from what the edge seals for it -- its
    /// hello, and the leases a sensor binding is sent anyway.
    pub fn signing(mut self, key: &str) -> Self {
        self.seals = Some((key.as_bytes().to_vec(), Peer::new(crate::signing::nonce())));
        self
    }

    /// Whether this sensor signs its reports.
    pub fn signs(&self) -> bool {
        self.seals.is_some()
    }

    /// The condition as last known: `None` is unknown, and unknown is silent.
    pub fn known(&self) -> Option<bool> {
        self.known
    }

    /// A reading from the backend. A replayed one (`retained`) is not the
    /// sensor speaking and makes it unknown, as for a plug.
    pub fn report(&mut self, r: Report, now: u64) -> Vec<Command> {
        if r.retained {
            let was = self.known.take();
            return if was.is_some() {
                vec![Command::Log(
                    "retained reading ignored; silent until a live one".into(),
                )]
            } else {
                vec![]
            };
        }
        let asserted = r.on != self.cfg.invert;
        let changed = self.known != Some(asserted);
        self.known = Some(asserted);
        if changed {
            // At once, not at the next timer: a door opening is a trip.
            self.last_report = Some(now);
            vec![
                Command::Log(format!("{} = {asserted}; reporting", self.cfg.condition)),
                self.condition_report(asserted),
            ]
        } else {
            vec![]
        }
    }

    /// The backend's view of whether the sensor is reachable. Unreachable is
    /// unknown, and unknown is silent -- the edge does the rest.
    pub fn availability(&mut self, online: bool, now: u64) -> Vec<Command> {
        let _ = now;
        if !online && self.known.is_some() {
            self.known = None;
            return vec![Command::Log(
                "sensor unavailable; going quiet so the edge notices".into(),
            )];
        }
        vec![]
    }

    /// Leases arrive for a sensor binding too. There is nothing to switch, so
    /// for a sensor that signs they are only how it learns the edge's nonce,
    /// as its hello is. Once the edge is newly known the condition goes out
    /// again at once: one sealed before was stale to that edge and dropped,
    /// and a door left open must not wait for the timer to be heard.
    pub fn edge_message(&mut self, topic_name: &str, payload: &[u8], _now: u64) -> Vec<Command> {
        let Some((key, peer)) = self.seals.as_mut() else {
            return vec![];
        };
        let mine = self.cfg.device_id.as_str();
        let before = peer.theirs().map(str::to_owned);
        let opened = if topic_name == topic::HELLO {
            learn::<LocalHello>(key, peer, payload, |m| m.device_id == mine)
        } else if topic_name == topic::LEASE {
            learn::<Lease>(key, peer, payload, |m| m.device_id == mine)
        } else {
            return vec![];
        };
        let mut out = Vec::new();
        if let Some(why) = opened {
            out.push(Command::Log(format!("{topic_name} not believed: {why:?}")));
        }
        if peer.theirs().map(str::to_owned) != before {
            if let Some(asserted) = self.known {
                self.last_report = None;
                out.push(Command::Log("edge newly known; reporting again".into()));
                out.push(self.condition_report(asserted));
            }
        }
        out
    }

    pub fn tick(&mut self, now: u64) -> Vec<Command> {
        let Some(asserted) = self.known else {
            return vec![];
        };
        let mut out = Vec::new();
        if self
            .last_alive
            .is_none_or(|t| now.saturating_sub(t) >= ALIVE_EVERY_MS)
        {
            self.last_alive = Some(now);
            out.push(Command::Publish {
                topic: topic::ALIVE_REQUEST,
                payload: self.sealed(AliveRequest {
                    device_id: self.cfg.device_id.clone(),
                    seal: None,
                }),
            });
        }
        if self
            .last_report
            .is_none_or(|t| now.saturating_sub(t) >= REPORT_EVERY_MS)
        {
            self.last_report = Some(now);
            out.push(self.condition_report(asserted));
        }
        out
    }

    fn condition_report(&mut self, asserted: bool) -> Command {
        let report = ConditionReport {
            device_id: self.cfg.device_id.clone(),
            resource: self.cfg.resource.clone(),
            condition: self.cfg.condition.clone(),
            asserted,
            seal: None,
        };
        Command::Publish {
            topic: topic::CONDITION_REQUEST,
            payload: self.sealed(report),
        }
    }

    /// `msg` as JSON, sealed when this sensor signs.
    fn sealed<M: Sealed + serde::Serialize>(&mut self, mut msg: M) -> String {
        if let Some((key, peer)) = self.seals.as_mut() {
            peer.seal(key, &mut msg);
        }
        serde_json::to_string(&msg).expect("a local message serialises")
    }
}

/// Open one message from the edge to learn its nonce. `None` when there is
/// nothing to say: it is for another device (every device sees every lease),
/// it does not parse, it was believed, or it was stale -- which is how a first
/// message from an edge that has not yet heard this sensor arrives, and still
/// teaches its nonce. `Some` for a message to this sensor that is unsigned or
/// badly signed: a forgery, or an edge older than CS:S 0.6.1.
fn learn<M: Sealed + serde::de::DeserializeOwned>(
    key: &[u8],
    peer: &mut Peer,
    payload: &[u8],
    named: impl Fn(&M) -> bool,
) -> Option<Refused> {
    let msg: M = serde_json::from_slice(payload).ok()?;
    if !named(&msg) {
        return None;
    }
    match peer.open(key, &msg, crate::signing::nonce) {
        Ok(()) | Err(Refused::Stale) => None,
        Err(why) => Some(why),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(invert: bool) -> SensorConfig {
        SensorConfig {
            device_id: "dev-s".into(),
            resource: "mill".into(),
            condition: "door_open".into(),
            invert,
        }
    }

    fn live(on: bool) -> Report {
        Report {
            on,
            retained: false,
        }
    }

    fn published<'a>(cmds: &'a [Command], t: &str) -> Vec<&'a str> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::Publish { topic, payload } if *topic == t => Some(payload.as_str()),
                _ => None,
            })
            .collect()
    }

    fn asserted_in(payload: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(payload).unwrap()["asserted"]
            .as_bool()
            .unwrap()
    }

    /// Drive ticks every 50 ms from `from` to `to`, collecting everything.
    fn run(s: &mut Sensor, from: u64, to: u64) -> Vec<Command> {
        let mut out = Vec::new();
        let mut t = from;
        while t <= to {
            out.extend(s.tick(t));
            t += 50;
        }
        out
    }

    #[test]
    fn an_unknown_sensor_says_nothing_at_all() {
        // Rule 1. The mutant is pulsing alive while unknown: the edge would
        // then see a healthy sensor it has no reading from, forever.
        let mut s = Sensor::new(cfg(false));
        let out = run(&mut s, 0, 10_000);
        assert!(
            published(&out, topic::ALIVE_REQUEST).is_empty(),
            "no alive while unknown"
        );
        assert!(published(&out, topic::CONDITION_REQUEST).is_empty());
    }

    #[test]
    fn a_first_reading_is_reported_at_once_and_then_on_the_timer() {
        let mut s = Sensor::new(cfg(false));
        let first = s.report(live(true), 100);
        let reports = published(&first, topic::CONDITION_REQUEST);
        assert_eq!(reports.len(), 1, "{first:?}");
        // The literal, which is the bridge's half of the #213 contract.
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(reports[0]).unwrap(),
            serde_json::json!({ "device_id": "dev-s", "resource": "mill", "condition": "door_open", "asserted": true })
        );
        // Then steady: alive every second, the reading every three.
        let out = run(&mut s, 150, 6_100);
        let alive = published(&out, topic::ALIVE_REQUEST).len();
        let cond = published(&out, topic::CONDITION_REQUEST).len();
        assert!((6..=7).contains(&alive), "alive pulses in 6 s: {alive}");
        assert_eq!(cond, 2, "steady re-reports at 3.1 s and 6.1 s: {cond}");
    }

    #[test]
    fn invert_flips_the_meaning() {
        // The mutant is ignoring `invert`: a contact wired the other way
        // would report the door open while it is shut.
        let mut s = Sensor::new(cfg(true));
        let out = s.report(live(true), 0);
        assert!(!asserted_in(published(&out, topic::CONDITION_REQUEST)[0]));
        let mut s = Sensor::new(cfg(false));
        let out = s.report(live(true), 0);
        assert!(asserted_in(published(&out, topic::CONDITION_REQUEST)[0]));
    }

    #[test]
    fn a_change_is_reported_at_once_not_at_the_next_timer() {
        // Rule 2's first half. The mutant is reporting changes only on the
        // timer: a door opening would reach the edge up to 3 s late.
        let mut s = Sensor::new(cfg(false));
        s.report(live(false), 0);
        let out = s.report(live(true), 500);
        let r = published(&out, topic::CONDITION_REQUEST);
        assert_eq!(r.len(), 1, "{out:?}");
        assert!(asserted_in(r[0]));
        // And an unchanged reading is not a report.
        assert!(published(&s.report(live(true), 600), topic::CONDITION_REQUEST).is_empty());
    }

    #[test]
    fn unavailable_makes_it_go_quiet_and_a_live_reading_brings_it_back() {
        // Rule 1 again, on the way down. The mutant is staying known through
        // `unavailable`: the bridge would keep vouching for a contact HA lost.
        let mut s = Sensor::new(cfg(false));
        s.report(live(false), 0);
        let out = s.availability(false, 1_000);
        assert!(out.iter().any(|c| matches!(c, Command::Log(_))));
        assert_eq!(s.known(), None);
        let quiet = run(&mut s, 1_050, 11_000);
        assert!(published(&quiet, topic::ALIVE_REQUEST).is_empty());
        assert!(published(&quiet, topic::CONDITION_REQUEST).is_empty());
        let back = s.report(live(false), 11_100);
        assert_eq!(published(&back, topic::CONDITION_REQUEST).len(), 1);
        assert!(!published(&run(&mut s, 11_150, 12_200), topic::ALIVE_REQUEST).is_empty());
    }

    #[test]
    fn a_retained_reading_is_not_the_sensor_speaking() {
        let mut s = Sensor::new(cfg(false));
        s.report(live(true), 0);
        s.report(
            Report {
                on: false,
                retained: true,
            },
            100,
        );
        assert_eq!(s.known(), None);
        assert!(published(&run(&mut s, 150, 4_000), topic::CONDITION_REQUEST).is_empty());
    }

    #[test]
    fn leases_are_ignored_because_there_is_nothing_to_switch() {
        let mut s = Sensor::new(cfg(false));
        s.report(live(false), 0);
        let out = s.edge_message(
            topic::LEASE,
            br#"{"resource":"mill","device_id":"dev-s","grant":true,"ttl_ms":3000}"#,
            10,
        );
        assert!(out.is_empty(), "{out:?}");
    }

    // --- signing the local wire (CS:S #249) -----------------------------

    const KEY: &str = "sensor-command-key";

    fn condition(payload: &str) -> spaces_device::wire::ConditionReport {
        serde_json::from_str(payload).unwrap()
    }

    fn hello_for(edge: &mut Peer, device_id: &str) -> Vec<u8> {
        let mut h = LocalHello {
            device_id: device_id.into(),
            seal: None,
        };
        edge.seal(KEY.as_bytes(), &mut h);
        serde_json::to_vec(&h).unwrap()
    }

    #[test]
    fn a_signing_sensor_seals_and_reports_again_as_soon_as_it_knows_its_edge() {
        // The mutants: `sealed` not sealing (the edge drops every reading on a
        // signed resource and denies the tool); `edge_message` not learning
        // (every reading stays stale to the edge for good); and no immediate
        // re-report (an open door waits for the timer to be heard).
        let mut s = Sensor::new(cfg(false)).signing(KEY);
        assert!(s.signs());
        let mut edge = Peer::new("edge-n1".into());

        // Before the edge is known: sealed, signed, but stale to the edge --
        // which still teaches the edge this sensor's nonce.
        let first = s.report(live(true), 100);
        let report = condition(published(&first, topic::CONDITION_REQUEST)[0]);
        assert!(report.seal.is_some(), "the reading went out unsigned");
        assert_eq!(
            edge.open(KEY.as_bytes(), &report, || "edge-n2".into()),
            Err(Refused::Stale)
        );

        // The edge's hello for ANOTHER device teaches nothing and sends nothing.
        let out = s.edge_message(topic::HELLO, &hello_for(&mut edge, "dev-other"), 150);
        assert!(
            published(&out, topic::CONDITION_REQUEST).is_empty(),
            "{out:?}"
        );

        // Its hello for this one: the reading goes out again at once, fresh.
        let out = s.edge_message(topic::HELLO, &hello_for(&mut edge, "dev-s"), 200);
        let again = published(&out, topic::CONDITION_REQUEST);
        assert_eq!(again.len(), 1, "no immediate re-report: {out:?}");
        edge.open(KEY.as_bytes(), &condition(again[0]), || "edge-n2".into())
            .expect("the edge opens the reading sent once it was known");

        // And alive, on the timer, is sealed and fresh too.
        let out = run(&mut s, 250, 1_300);
        let alive: spaces_device::wire::AliveRequest =
            serde_json::from_str(published(&out, topic::ALIVE_REQUEST)[0]).unwrap();
        edge.open(KEY.as_bytes(), &alive, || "edge-n2".into())
            .expect("the edge opens the sealed alive");
    }

    #[test]
    fn an_unsigned_hello_is_logged_and_teaches_nothing() {
        let mut s = Sensor::new(cfg(false)).signing(KEY);
        s.report(live(true), 100);
        let forged = serde_json::to_vec(&LocalHello {
            device_id: "dev-s".into(),
            seal: None,
        })
        .unwrap();
        let out = s.edge_message(topic::HELLO, &forged, 150);
        assert!(
            published(&out, topic::CONDITION_REQUEST).is_empty(),
            "{out:?}"
        );
        assert!(
            out.iter()
                .any(|c| matches!(c, Command::Log(l) if l.contains("Unsigned"))),
            "a forged hello went unremarked: {out:?}"
        );
    }

    #[test]
    fn an_unsigned_sensor_reports_exactly_as_before() {
        // No key: no seal field at all, so an edge older than CS:S 0.6.1
        // reads what it always read.
        let mut s = Sensor::new(cfg(false));
        let first = s.report(live(true), 100);
        let v: serde_json::Value =
            serde_json::from_str(published(&first, topic::CONDITION_REQUEST)[0]).unwrap();
        assert!(v.get("seal").is_none(), "{v}");
    }
}
