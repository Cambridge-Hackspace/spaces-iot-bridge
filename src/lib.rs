//! spaces-iot-bridge: devices on an IoT broker, presented to a Spaces edge as
//! modules. The design and every decision's basis are in issue #1.
//!
//! Layout: `core` is pure -- per-device state as functions over a record, no
//! I/O -- and is where the safety lives; the adapters that feed it are thin.
pub mod core;
