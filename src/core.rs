//! The per-device state machine, as pure functions. No clocks, no sockets.
//!
//! This file holds the first rule because it is the one the whole design hinges
//! on: `relay_on` is only ever what the plug SAID, and a report the broker
//! replayed from its retained store is not the plug saying anything -- it is
//! the broker remembering. The rest of the machine (expecting, watchdog,
//! re-arm, stop-on-loss) lands in its own issue.

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

/// Fold a report into what is known.
///
/// A retained report yields `Unknown`, whatever it says. On (re)connect the
/// broker replays Z2M's last state and it looks current; trusting it is how a
/// laser gets a stage-0 confirmation from an extractor that stopped minutes
/// ago. The adapter follows a retained report with a `/get`, and the live reply
/// that answers it is not retained, so Unknown resolves in under a second.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retained_report_is_not_the_plug_speaking() {
        // The rule the design hinges on. The mutant is deleting the `retained`
        // branch of `observe`: this then reads On and the laser can confirm
        // extraction against a replay.
        assert_eq!(
            observe(Report {
                on: true,
                retained: true
            }),
            KnownState::Unknown
        );
        assert_eq!(
            observe(Report {
                on: false,
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
        assert_eq!(
            observe(Report {
                on: false,
                retained: false
            }),
            KnownState::Off
        );
    }

    #[test]
    fn unknown_is_omitted_not_false() {
        // "Cannot report" and "off" are different claims to the edge, and only
        // the second one is a claim about the plug.
        assert_eq!(relay_on(KnownState::Unknown), None);
        assert_eq!(relay_on(KnownState::On), Some(true));
        assert_eq!(relay_on(KnownState::Off), Some(false));
    }
}
