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

use spaces_device::wire::{topic, AliveRequest, ConditionReport};

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
}

impl Sensor {
    pub fn new(cfg: SensorConfig) -> Self {
        Self {
            cfg,
            known: None,
            last_alive: None,
            last_report: None,
        }
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

    /// Leases arrive for a sensor binding too. There is nothing to switch.
    pub fn edge_message(&mut self, _topic: &str, _payload: &[u8], _now: u64) -> Vec<Command> {
        vec![]
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
                payload: serde_json::to_string(&AliveRequest {
                    device_id: self.cfg.device_id.clone(),
                })
                .expect("an AliveRequest serialises"),
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

    fn condition_report(&self, asserted: bool) -> Command {
        Command::Publish {
            topic: topic::CONDITION_REQUEST,
            payload: serde_json::to_string(&ConditionReport {
                device_id: self.cfg.device_id.clone(),
                resource: self.cfg.resource.clone(),
                condition: self.cfg.condition.clone(),
                asserted,
            })
            .expect("a ConditionReport serialises"),
        }
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
}
