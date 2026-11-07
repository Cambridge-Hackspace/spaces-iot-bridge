//! Zigbee2MQTT, as pure translation. Topics and payloads in both directions;
//! no sockets here.

use crate::core::Report;

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

/// What arrived on one of a device's topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    State(Report),
    Availability {
        online: bool,
    },
    /// An empty payload on the state topic: a broker forgetting a retained
    /// state (that is how one is cleared). Not a report and not malformed;
    /// nothing to act on and nothing to warn about.
    Cleared,
    /// On a topic of ours but not something we understand. Logged, not acted on.
    Unrecognised,
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
}
