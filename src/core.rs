//! The per-device state machine, as pure functions over a record. No clocks,
//! no sockets: time is a parameter, in milliseconds, as `spaces_device` takes
//! it.
//!
//! The lease handling is NOT written here. [`spaces_device::Module`], built as
//! a plug (`Config::power_only`), already matches leases by `device_id`, holds
//! the standing lease, cuts the relay when it lapses, and publishes the
//! heartbeat and the alive pulse. That is the watchdog FIRMWARE.md warns is
//! easiest to get wrong, and it is host-tested upstream. This file wraps it
//! with what a *bridged* plug needs that a real module does not:
//!
//! 1. **`relay_on` is only ever what the plug said.** By default a Module's
//!    heartbeat fills `relay_on` from its own commanded state -- firmware with a
//!    relay knows what it set. A bridge has no relay; it has a plug on another
//!    broker that may or may not have obeyed. So the Module is told its relay
//!    is remote, and is handed [`KnownState`] as an observation whenever it
//!    changes (spaces-firmware #9): it reports `Some` only when the plug has
//!    been heard from, `None` -- OMITTED on the wire -- when not.
//!    The edge's stage-0 confirmation for the laser turns on that field, so an
//!    optimistic `true` would fire the laser before extraction is running.
//! 2. **A retained report is not the plug speaking.** The broker replays the
//!    last thing a topic said to every new subscriber, of unknown age, looking
//!    fresh. It yields `Unknown`, and the backend is told to ask for a live
//!    report so a reply resolves it in under a second.
//! 3. **Re-arming is on the plug's clock, not the lease's.** The edge regrants
//!    every second; re-sending `on_time` on each grant is a Zigbee write per
//!    second per plug. The Module's `Set` is transition-only, so the wrapper
//!    schedules the countdown refresh itself, every `rearm_s`.
//! 4. **Stop-on-loss.** Stage confirmation in the edge is sticky, so an
//!    extractor failing mid-job is not caught by start stages. A leased plug
//!    that goes Unknown or Off for the debounce gets a `stop` on its resource.

use spaces_device::{Action, Config, Module, Observed, Output};

/// What the bridge currently knows about a plug's relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownState {
    /// Nothing trustworthy has been heard. `relay_on` is OMITTED on the wire --
    /// "cannot report", which the edge treats as distinct from `false`.
    Unknown,
    On,
    Off,
}

/// A state report as the IoT adapter hands it over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    pub on: bool,
    /// Set when the broker delivered this with the MQTT retain flag: a replay
    /// of the last thing the topic said, of unknown age, handed to every new
    /// subscriber as if fresh.
    pub retained: bool,
}

/// Fold a report into what is known. A retained report yields `Unknown`,
/// whatever it says.
pub fn observe(report: Report) -> KnownState {
    if report.retained {
        return KnownState::Unknown;
    }
    if report.on {
        KnownState::On
    } else {
        KnownState::Off
    }
}

/// The `relay_on` field for a power report: present only when known.
pub fn relay_on(known: KnownState) -> Option<bool> {
    match known {
        KnownState::Unknown => None,
        KnownState::On => Some(true),
        KnownState::Off => Some(false),
    }
}

/// What the adapters must do, in order. The bridge's own vocabulary; the
/// Module's [`Action`]s are translated into it and never leak out, and no
/// backend's wire appears in it either -- `Set` and `Get` are carried out by
/// whichever [`crate::backend::Backend`] owns the device.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Switch the device; `on_time_s` is a countdown for it to run itself,
    /// present only when the core has decided it may be trusted with one.
    Set { on: bool, on_time_s: Option<u32> },
    /// Ask the device for a live state report.
    Get,
    /// To the edge broker. `relay_on` in a power report is already rewritten.
    Publish {
        topic: &'static str,
        payload: String,
    },
    /// `resource/request/stop` for the resource this plug serves.
    Stop { resource: String },
    /// For the log. Wording is not a contract.
    Log(String),
}

/// The per-device settings from config that the core needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceConfig {
    pub device_id: String,
    pub resource: String,
    /// From `bridge probe`, by hand: the plug honours `on_time` AND re-sending
    /// it while ON extends the deadline.
    pub countdown: bool,
    pub on_time_s: u32,
    pub rearm_s: u32,
    pub stop_on_loss: bool,
}

/// How long after sending `ON` a report must arrive before the command is
/// treated as unanswered and the state as Unknown.
pub const CONFIRM_MS: u64 = 5_000;
/// How long a leased plug may be Unknown or Off before a `stop` goes out.
pub const LOSS_DEBOUNCE_MS: u64 = 10_000;

/// One bridged plug. Not `Debug`: `spaces_device::Module` is not, and a
/// device is not something to print.
pub struct Device {
    cfg: DeviceConfig,
    module: Module,
    known: KnownState,
    /// An `ON`/`OFF` was sent at this time and the plug has not reported since.
    expecting: Option<(bool, u64)>,
    last_rearm: Option<u64>,
    /// When a leased plug was first seen not-On. Cleared on On.
    loss_since: Option<u64>,
    stop_sent: bool,
}

impl Device {
    pub fn new(cfg: DeviceConfig) -> Self {
        let mut module_cfg = Config::power_only(cfg.device_id.clone(), cfg.resource.clone());
        // The extractor follows the laser's lease; the laser's own box meters.
        // A second report of the same minutes would accumulate server-side.
        module_cfg.reports_usage = false;
        // The relay is the plug's, on another broker, and may not have obeyed:
        // Module reports only what the plug said (spaces-firmware #9), which is
        // fed to it whenever `known` changes (`know`).
        module_cfg.relay_is_remote = true;
        Self {
            cfg,
            module: Module::new(module_cfg),
            known: KnownState::Unknown,
            expecting: None,
            last_rearm: None,
            loss_since: None,
            stop_sent: false,
        }
    }

    pub fn known(&self) -> KnownState {
        self.known
    }

    /// Whether the edge is currently leasing this plug on.
    pub fn leased(&self) -> bool {
        self.module.tool_is_on()
    }

    /// A message from the edge broker: a lease, or an ack.
    pub fn edge_message(&mut self, topic_name: &str, payload: &[u8], now: u64) -> Vec<Command> {
        let actions = self.module.message(topic_name, payload, now);
        let mut out = self.translate(actions, now);
        out.extend(self.housekeeping(now));
        out
    }

    /// A state report from the plug.
    pub fn report(&mut self, report: Report, now: u64) -> Vec<Command> {
        let mut out = Vec::new();
        let known = observe(report);
        if known == KnownState::Unknown {
            out.push(Command::Log(
                "retained state ignored; asking for a live report".into(),
            ));
            out.push(Command::Get);
        } else {
            // A live report answers whatever was outstanding, whether or not it
            // agrees with it. Disagreement is logged, not corrected: the plug
            // is the authority on its own relay.
            if let Some((wanted, _)) = self.expecting.take() {
                if wanted != report.on {
                    out.push(Command::Log(format!(
                        "plug reports {} after being told {}",
                        if report.on { "ON" } else { "OFF" },
                        if wanted { "ON" } else { "OFF" }
                    )));
                }
            }
        }
        self.know(known);
        out.extend(self.housekeeping(now));
        out
    }

    /// Z2M's availability for this plug.
    pub fn availability(&mut self, online: bool, now: u64) -> Vec<Command> {
        let mut out = Vec::new();
        if !online && self.known != KnownState::Unknown {
            out.push(Command::Log("plug unavailable; state unknown".into()));
            self.know(KnownState::Unknown);
            self.expecting = None;
        }
        out.extend(self.housekeeping(now));
        out
    }

    /// Time passed. Call often -- the Module notices a lapsed lease only here
    /// or on another entry point.
    pub fn tick(&mut self, now: u64) -> Vec<Command> {
        let actions = self.module.tick(now);
        let mut out = self.translate(actions, now);
        out.extend(self.housekeeping(now));
        out
    }

    // ------------------------------------------------------------- internal

    /// The Module's actions, in the bridge's vocabulary.
    fn translate(&mut self, actions: Vec<Action>, now: u64) -> Vec<Command> {
        let mut out = Vec::new();
        for action in actions {
            match action {
                Action::Set {
                    output: Output::Tool,
                    on,
                } => {
                    let on_time_s = (on && self.cfg.countdown).then_some(self.cfg.on_time_s);
                    out.push(Command::Set { on, on_time_s });
                    self.expecting = Some((on, now));
                    if on {
                        self.last_rearm = Some(now);
                    } else {
                        self.last_rearm = None;
                    }
                }
                // A bridged plug drives no running sense and no latch.
                Action::Set { .. } => {}
                Action::Publish { topic, payload } => out.push(Command::Publish { topic, payload }),
                Action::Note(s) => out.push(Command::Log(s)),
                // A plug has no panel. Outcomes are session events, and a plug
                // has no session.
                Action::Outcome(_) => {}
            }
        }
        out
    }

    /// Rule 1: `relay_on` is only ever what the plug said. Recorded here and
    /// handed to the Module, which reports exactly that and omits the field
    /// while it is unknown (spaces-firmware #9). No draw: the Module's two
    /// draw constants are stand-ins for a relay it owns, and a plug's real
    /// reading, where it has one, is a later adapter concern -- until then no
    /// claim is better than a made-up one, because the bypass detector
    /// believes a number.
    fn know(&mut self, known: KnownState) {
        self.known = known;
        self.module.observe(Observed {
            relay_on: relay_on(known),
            draw_now: None,
        });
    }

    /// Rules 3 and 4, plus the confirm timeout. Run after every entry point.
    fn housekeeping(&mut self, now: u64) -> Vec<Command> {
        let mut out = Vec::new();

        // Confirm timeout: told ON/OFF, nothing heard back.
        if let Some((wanted, at)) = self.expecting {
            if now.saturating_sub(at) >= CONFIRM_MS {
                self.expecting = None;
                if self.known != KnownState::Unknown {
                    out.push(Command::Log(format!(
                        "no report within {CONFIRM_MS} ms of {}; state unknown",
                        if wanted { "ON" } else { "OFF" }
                    )));
                }
                self.know(KnownState::Unknown);
            }
        }

        let leased = self.module.tool_is_on();

        // Re-arm: on the plug's clock, only while leased, only once the plug
        // has actually reported On, and never more often than rearm_s.
        if leased && self.cfg.countdown && self.known == KnownState::On {
            let due = self
                .last_rearm
                .is_none_or(|t| now.saturating_sub(t) >= u64::from(self.cfg.rearm_s) * 1_000);
            if due {
                self.last_rearm = Some(now);
                out.push(Command::Set {
                    on: true,
                    on_time_s: Some(self.cfg.on_time_s),
                });
            }
        }

        // Stop-on-loss: leased, but the plug is not known to be On.
        if leased && self.cfg.stop_on_loss {
            if self.known == KnownState::On {
                self.loss_since = None;
                self.stop_sent = false;
            } else {
                let since = *self.loss_since.get_or_insert(now);
                if !self.stop_sent && now.saturating_sub(since) >= LOSS_DEBOUNCE_MS {
                    self.stop_sent = true;
                    out.push(Command::Log(format!(
                        "leased but not On for {LOSS_DEBOUNCE_MS} ms; stopping {}",
                        self.cfg.resource
                    )));
                    out.push(Command::Stop {
                        resource: self.cfg.resource.clone(),
                    });
                }
            }
        } else {
            self.loss_since = None;
            self.stop_sent = false;
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spaces_device::wire::topic;

    fn cfg() -> DeviceConfig {
        DeviceConfig {
            device_id: "dev-1".into(),
            resource: "laser".into(),
            countdown: true,
            on_time_s: 60,
            rearm_s: 15,
            stop_on_loss: true,
        }
    }

    fn lease(dev: &mut Device, grant: bool, now: u64) -> Vec<Command> {
        let payload =
            format!(r#"{{"device_id":"dev-1","grant":{grant},"ttl_ms":3000,"resource":"laser"}}"#);
        dev.edge_message(topic::LEASE, payload.as_bytes(), now)
    }

    fn sets(cmds: &[Command]) -> Vec<(bool, Option<u32>)> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::Set { on, on_time_s } => Some((*on, *on_time_s)),
                _ => None,
            })
            .collect()
    }

    fn relay_field(cmds: &[Command]) -> Vec<Option<bool>> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::Publish { topic, payload } if *topic == topic::POWER_REPORT => {
                    let v: serde_json::Value = serde_json::from_str(payload).unwrap();
                    Some(v.get("relay_on").map(|b| b.as_bool().unwrap()))
                }
                _ => None,
            })
            .collect()
    }

    // --- rule 2, seeded with the scaffold -------------------------------

    #[test]
    fn a_retained_report_is_not_the_plug_speaking() {
        // The mutant is deleting the `retained` branch of `observe`.
        assert_eq!(
            observe(Report {
                on: true,
                retained: true
            }),
            KnownState::Unknown
        );
    }

    #[test]
    fn a_live_report_is_believed() {
        assert_eq!(
            observe(Report {
                on: true,
                retained: false
            }),
            KnownState::On
        );
    }

    #[test]
    fn unknown_is_omitted_not_false() {
        assert_eq!(relay_on(KnownState::Unknown), None);
        assert_eq!(relay_on(KnownState::Off), Some(false));
    }

    #[test]
    fn a_retained_report_asks_for_a_live_one() {
        // The mutant is dropping the Get push in `report`.
        let mut d = Device::new(cfg());
        let out = d.report(
            Report {
                on: true,
                retained: true,
            },
            0,
        );
        assert!(out.contains(&Command::Get), "{out:?}");
        assert_eq!(d.known(), KnownState::Unknown);
        // ...and the live answer resolves it.
        d.report(
            Report {
                on: true,
                retained: false,
            },
            300,
        );
        assert_eq!(d.known(), KnownState::On);
    }

    // --- rule 1 ----------------------------------------------------------

    #[test]
    fn relay_on_is_what_the_plug_said_not_what_was_commanded() {
        // A grant makes the Module believe its relay is on. The report that
        // goes out must not say so until the plug has.
        let mut d = Device::new(cfg());
        let out = lease(&mut d, true, 0);
        assert_eq!(sets(&out), vec![(true, Some(60))], "ON with on_time");
        assert!(d.leased());
        // Heartbeat after a grant, plug still silent: relay_on OMITTED.
        let out = d.tick(1_000);
        assert_eq!(relay_field(&out), vec![None], "{out:?}");
        // The plug confirms; now it may be reported.
        d.report(
            Report {
                on: true,
                retained: false,
            },
            1_100,
        );
        let out = d.tick(2_000);
        assert_eq!(relay_field(&out), vec![Some(true)]);
    }

    #[test]
    fn an_unanswered_command_times_out_to_unknown() {
        // The mutant is removing the CONFIRM_MS arm of housekeeping.
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        assert_eq!(d.known(), KnownState::On);
        // Lease withdrawn -> OFF sent -> expecting Off. Nothing answers.
        lease(&mut d, false, 200);
        assert_eq!(d.known(), KnownState::On, "still On until the plug speaks");
        d.tick(200 + CONFIRM_MS - 1);
        assert_eq!(d.known(), KnownState::On, "not yet");
        d.tick(200 + CONFIRM_MS);
        assert_eq!(d.known(), KnownState::Unknown);
    }

    #[test]
    fn age_alone_never_makes_the_state_unknown() {
        // Many plugs report only on change. Ten minutes of silence on a plug
        // that is On is a plug that is On.
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        for t in (1_000..=600_000).step_by(1_000) {
            lease(&mut d, true, t);
            d.tick(t + 50);
        }
        assert_eq!(d.known(), KnownState::On);
    }

    // --- rule 3 ----------------------------------------------------------

    #[test]
    fn regrants_every_second_do_not_rewrite_the_plug_every_second() {
        // The edge regrants every second. The mutant is deleting the
        // `>= rearm_s` guard: every grant then re-arms.
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        let mut writes = 0;
        for t in (1_000..=14_000).step_by(1_000) {
            writes += sets(&lease(&mut d, true, t)).len();
            writes += sets(&d.tick(t + 50)).len();
        }
        assert_eq!(writes, 0, "fourteen regrants inside rearm_s wrote nothing");
        let out = lease(&mut d, true, 15_000);
        assert_eq!(sets(&out), vec![(true, Some(60))], "re-armed at rearm_s");
    }

    #[test]
    fn the_lease_lapsing_switches_the_plug_off() {
        // The Module's watchdog, reaching the plug through the translation.
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        let out = d.tick(3_000);
        assert_eq!(sets(&out), vec![(false, None)], "{out:?}");
        assert!(!d.leased());
    }

    #[test]
    fn a_refused_lease_switches_off_at_once() {
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        let out = lease(&mut d, false, 500);
        assert_eq!(sets(&out), vec![(false, None)]);
    }

    #[test]
    fn without_countdown_no_on_time_is_sent_and_nothing_is_rearmed() {
        let mut d = Device::new(DeviceConfig {
            countdown: false,
            ..cfg()
        });
        let out = lease(&mut d, true, 0);
        assert_eq!(sets(&out), vec![(true, None)]);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        // The lease is held the way the edge holds it: a grant every second.
        // Across sixty of them, a plug with no countdown must never be
        // re-armed -- there is no countdown to re-arm.
        let mut writes = 0;
        for t in (1_000..=60_000).step_by(1_000) {
            writes += sets(&lease(&mut d, true, t)).len();
            writes += sets(&d.tick(t + 50)).len();
        }
        assert_eq!(writes, 0, "no on_time means nothing to refresh");
        assert!(d.leased(), "and the lease was never allowed to lapse");
    }

    // --- rule 4 ----------------------------------------------------------

    #[test]
    fn a_leased_plug_that_drops_out_stops_the_resource_after_the_debounce() {
        // The mutant is setting LOSS_DEBOUNCE_MS to 0: the "not before" half
        // then fails.
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        // The plug leaves the mesh.
        let out = d.availability(false, 1_000);
        assert!(
            !out.iter().any(|c| matches!(c, Command::Stop { .. })),
            "not at once"
        );
        let out = lease(&mut d, true, 1_000 + LOSS_DEBOUNCE_MS - 1);
        assert!(
            !out.iter().any(|c| matches!(c, Command::Stop { .. })),
            "not before"
        );
        let out = lease(&mut d, true, 1_000 + LOSS_DEBOUNCE_MS);
        assert!(
            out.contains(&Command::Stop {
                resource: "laser".into()
            }),
            "{out:?}"
        );
        // Once, not every tick.
        let out = lease(&mut d, true, 1_000 + LOSS_DEBOUNCE_MS + 1_000);
        assert!(
            !out.iter().any(|c| matches!(c, Command::Stop { .. })),
            "once"
        );
    }

    #[test]
    fn a_blip_does_not_stop_the_resource() {
        let mut d = Device::new(cfg());
        lease(&mut d, true, 0);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        d.availability(false, 1_000);
        d.report(
            Report {
                on: true,
                retained: false,
            },
            3_000,
        );
        let out = lease(&mut d, true, 20_000);
        assert!(
            !out.iter().any(|c| matches!(c, Command::Stop { .. })),
            "{out:?}"
        );
    }

    #[test]
    fn an_unleased_plug_never_stops_anything() {
        let mut d = Device::new(cfg());
        d.availability(false, 0);
        let out = d.tick(LOSS_DEBOUNCE_MS * 3);
        assert!(
            !out.iter().any(|c| matches!(c, Command::Stop { .. })),
            "{out:?}"
        );
    }

    #[test]
    fn two_devices_keep_their_own_clocks() {
        let mut a = Device::new(DeviceConfig {
            device_id: "a".into(),
            on_time_s: 60,
            rearm_s: 15,
            ..cfg()
        });
        let mut b = Device::new(DeviceConfig {
            device_id: "b".into(),
            on_time_s: 10,
            rearm_s: 3,
            ..cfg()
        });
        let grant = |id: &str| {
            format!(r#"{{"device_id":"{id}","grant":true,"ttl_ms":3000,"resource":"laser"}}"#)
        };
        a.edge_message(topic::LEASE, grant("a").as_bytes(), 0);
        b.edge_message(topic::LEASE, grant("b").as_bytes(), 0);
        // A lease for b reaches a's Module too -- every module sees every
        // lease -- and must do nothing to a.
        let out = a.edge_message(topic::LEASE, grant("b").as_bytes(), 10);
        assert!(sets(&out).is_empty());
        a.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        b.report(
            Report {
                on: true,
                retained: false,
            },
            100,
        );
        let mut a_writes = 0;
        let mut b_writes = 0;
        for t in (1_000..=12_000).step_by(1_000) {
            a_writes += sets(&a.edge_message(topic::LEASE, grant("a").as_bytes(), t)).len();
            b_writes += sets(&b.edge_message(topic::LEASE, grant("b").as_bytes(), t)).len();
        }
        assert_eq!(a_writes, 0, "a: inside its 15 s");
        assert_eq!(b_writes, 4, "b: re-armed at 3, 6, 9, 12");
    }
}
