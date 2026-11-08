//! The seam between the core and whatever owns the device.
//!
//! The core ([`crate::core`]) knows about leases, what a plug last said, and
//! when to stop a resource. It does not know how a plug is spoken to. A
//! *backend* does: Zigbee2MQTT today (`z2m`), Home Assistant next (#20). Each
//! one translates its own wire into [`Incoming`] on the way in, and carries
//! out the core's [`crate::core::Command::Set`] and
//! [`crate::core::Command::Get`] on the way out. Nothing on either side of
//! this module names the other's wire.
//!
//! An enum rather than a trait object: two variants, and `async fn` in a
//! trait is not object-safe without a boxing crate this does not need.

use anyhow::Result;

use crate::core::Report;

/// What a backend observed about one device, in the core's terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// The device said it is on or off. `retained` is set when the transport
    /// replayed this from memory rather than the device saying it now -- an
    /// MQTT retained message, or Home Assistant's state on connect.
    State(Report),
    /// The transport's own view of whether the device is reachable.
    Availability { online: bool },
    /// A stored state was forgotten (an empty retained publish). Not a report
    /// and not malformed; nothing to act on and nothing to warn about.
    Cleared,
    /// On a topic or entity of ours but not something we understand. Logged,
    /// not acted on.
    Unrecognised,
}

/// The drive half: how one device is told what to do.
pub enum Backend {
    Zigbee2Mqtt(crate::z2m::Handle),
    HomeAssistant(crate::ha::Handle),
}

impl Backend {
    /// Switch the device. `on_time_s` is a countdown the device itself runs,
    /// where the core has decided it may be trusted with one; a backend with
    /// no such notion ignores it.
    pub async fn set(&self, on: bool, on_time_s: Option<u32>) -> Result<()> {
        match self {
            Backend::Zigbee2Mqtt(h) => h.set(on, on_time_s).await,
            Backend::HomeAssistant(h) => h.set(on, on_time_s).await,
        }
    }

    /// Ask for a live state report, so a replayed one can be resolved.
    pub async fn get(&self) -> Result<()> {
        match self {
            Backend::Zigbee2Mqtt(h) => h.get().await,
            Backend::HomeAssistant(h) => h.get().await,
        }
    }

    /// For the log line that says what was done where.
    pub fn describe(&self) -> String {
        match self {
            Backend::Zigbee2Mqtt(h) => h.describe(),
            Backend::HomeAssistant(h) => h.describe(),
        }
    }
}
