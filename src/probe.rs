//! `bridge probe`: the commissioning measurement that decides `countdown`.
//!
//! Two halves, and the second is the one the design depends on (#1 q1, #6):
//!
//! 1. `ON on_time=N`; the plug must report ON, then OFF about N seconds later.
//!    This proves the plug honours a countdown at all.
//! 2. `ON on_time=N`; at N/2, re-send it; OFF must land about N seconds after
//!    the RE-SEND, not the first send. This proves re-arming while already ON
//!    extends the deadline -- which is what lets the bridge ride the plug's
//!    own countdown as the fail-safe. A plug that passes 1 and fails 2 cannot
//!    be re-armed, and the design falls back to the bridge's watchdog alone.
//!
//! The verdict is pure and tested; the driver just collects timestamps. The
//! probe prints and writes nothing: `countdown = true` goes into the config by
//! a person's hand, so the claim has someone behind it.

use std::time::Duration;

/// Seconds for the countdown under test. Short, so a probe is quick; long
/// enough that MQTT round-trips do not dominate.
pub const ON_TIME_S: u32 = 10;
/// Tolerance either side of the expected OFF, in milliseconds. Zigbee plugs
/// tick in whole seconds and Z2M reporting adds latency.
pub const TOLERANCE_MS: u64 = 2_500;

/// What one half observed, in milliseconds from the FIRST send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    /// When the plug first reported ON. `None` if it never did.
    pub on_at_ms: Option<u64>,
    /// When the plug reported OFF after that. `None` if it never did.
    pub off_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass { off_after_ms: u64 },
    Fail(String),
}

impl Verdict {
    pub fn passed(&self) -> bool {
        matches!(self, Verdict::Pass { .. })
    }
}

/// Half 1: OFF about `on_time_s` after the send.
pub fn verdict_countdown(o: Observed, on_time_s: u32) -> Verdict {
    let expected = u64::from(on_time_s) * 1_000;
    judge(o, expected, "the plug honours on_time")
}

/// Half 2: re-sent at `rearm_at_ms`; OFF about `on_time_s` after the RE-SEND.
/// An OFF near `on_time_s` after the FIRST send means the re-send was ignored.
pub fn verdict_rearm(o: Observed, on_time_s: u32, rearm_at_ms: u64) -> Verdict {
    let expected = rearm_at_ms + u64::from(on_time_s) * 1_000;
    let original = u64::from(on_time_s) * 1_000;
    match (o.on_at_ms, o.off_at_ms) {
        (_, Some(off)) if off.abs_diff(original) <= TOLERANCE_MS && off.abs_diff(expected) > TOLERANCE_MS => {
            Verdict::Fail(format!(
                "OFF at {off} ms is the ORIGINAL deadline ({original} ms): the re-send at {rearm_at_ms} ms was ignored, so re-arming does not extend the countdown"
            ))
        }
        _ => judge(o, expected, "re-sending on_time while ON extends the deadline"),
    }
}

fn judge(o: Observed, expected_ms: u64, claim: &str) -> Verdict {
    let Some(on) = o.on_at_ms else {
        return Verdict::Fail(format!("{claim}: the plug never reported ON"));
    };
    let Some(off) = o.off_at_ms else {
        return Verdict::Fail(format!(
            "{claim}: the plug reported ON at {on} ms and never reported OFF"
        ));
    };
    if off <= on {
        return Verdict::Fail(format!(
            "{claim}: OFF at {off} ms is not after ON at {on} ms"
        ));
    }
    if off.abs_diff(expected_ms) > TOLERANCE_MS {
        return Verdict::Fail(format!(
            "{claim}: OFF at {off} ms, expected about {expected_ms} ms (tolerance {TOLERANCE_MS} ms)"
        ));
    }
    Verdict::Pass { off_after_ms: off }
}

/// How long to wait for the OFF before giving up: the deadline plus slack.
pub fn wait_budget(expected_ms: u64) -> Duration {
    Duration::from_millis(expected_ms + TOLERANCE_MS * 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(on: Option<u64>, off: Option<u64>) -> Observed {
        Observed {
            on_at_ms: on,
            off_at_ms: off,
        }
    }

    #[test]
    fn a_plug_that_switches_off_on_time_passes_half_one() {
        assert!(verdict_countdown(obs(Some(300), Some(10_200)), 10).passed());
    }

    #[test]
    fn a_plug_that_ignores_on_time_fails_half_one() {
        // The mutant is widening TOLERANCE_MS to swallow this.
        let v = verdict_countdown(obs(Some(300), None), 10);
        assert!(!v.passed(), "{v:?}");
        let v = verdict_countdown(obs(Some(300), Some(40_000)), 10);
        assert!(!v.passed(), "off far too late: {v:?}");
    }

    #[test]
    fn a_rearm_that_extends_the_deadline_passes_half_two() {
        // Re-sent at 5 s; OFF about 10 s after THAT, i.e. ~15 s.
        assert!(verdict_rearm(obs(Some(300), Some(15_100)), 10, 5_000).passed());
    }

    #[test]
    fn a_rearm_the_plug_ignored_fails_half_two_and_says_why() {
        // The one the design depends on. OFF at ~10 s is the ORIGINAL deadline:
        // the re-send changed nothing. The mutant is dropping the "original
        // deadline" arm of verdict_rearm, which then fails with a generic
        // timing message -- or, with a tolerance that overlaps, passes.
        let v = verdict_rearm(obs(Some(300), Some(10_100)), 10, 5_000);
        match v {
            Verdict::Fail(why) => assert!(why.contains("ORIGINAL deadline"), "{why}"),
            Verdict::Pass { .. } => panic!("an ignored re-send must not pass"),
        }
    }

    #[test]
    fn off_before_on_is_not_a_countdown() {
        // The OFF is WITHIN tolerance of the expected deadline, so the timing
        // check would accept it; only the ordering check can refuse it. The
        // first version of this test put OFF 8 s early, which the tolerance
        // refused on its own -- and the mutant disabling `off <= on` passed,
        // which is how the test was found to be deciding nothing. The mutant
        // is disabling that check.
        let v = verdict_countdown(obs(Some(11_000), Some(10_500)), 10);
        match v {
            Verdict::Fail(why) => assert!(why.contains("not after ON"), "{why}"),
            Verdict::Pass { .. } => panic!("OFF before ON passed as a countdown"),
        }
    }

    #[test]
    fn never_reporting_on_fails_both_halves() {
        assert!(!verdict_countdown(obs(None, Some(10_000)), 10).passed());
        assert!(!verdict_rearm(obs(None, Some(15_000)), 10, 5_000).passed());
    }
}
