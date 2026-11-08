//! The configuration file and the credential files it names. The shape is
//! `config.sample.toml`, which is committed and which a test here parses, so
//! the sample cannot drift from the code.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Config {
    pub server: Server,
    pub edge: Broker,
    /// Zigbee2MQTT's broker, for devices with `backend = "zigbee2mqtt"`.
    /// Optional since #20: a bridge with only Home Assistant devices never
    /// connects to it. Required by `parse` when such a device exists.
    pub iot: Option<IotBroker>,
    /// Home Assistant, for devices with `backend = "homeassistant"` (#20).
    /// Optional: a bridge with only Zigbee2MQTT devices never connects to it.
    pub homeassistant: Option<HomeAssistant>,
    #[serde(rename = "device", default)]
    pub devices: Vec<DeviceEntry>,
}

/// css-server, for registration. Everything else goes over the edge's broker.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Server {
    pub url: String,
}

/// A broker, as `mqtt://host:port`, and the file holding its login.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Broker {
    pub broker: String,
    pub credentials: PathBuf,
}

/// Home Assistant's WebSocket API and the long-lived access token for it, in
/// its own 0600 file holding the token and nothing else.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct HomeAssistant {
    /// `ws://host:8123/api/websocket`, or `wss://` behind a reverse proxy.
    pub url: String,
    pub token_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct IotBroker {
    pub broker: String,
    pub credentials: PathBuf,
    /// Zigbee2MQTT's base topic; `<base>/<friendly>` is a device's state.
    pub base: String,
}

/// Which backend owns a device. Serde refuses an unknown one by name, so the
/// old `ensure!` on the string is now the deserializer's job.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Zigbee2Mqtt,
    Homeassistant,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DeviceEntry {
    pub name: String,
    pub backend: BackendKind,
    /// Zigbee2MQTT's friendly name: `<base>/<friendly>` is the state topic.
    /// Required for that backend, meaningless for the other.
    pub friendly: Option<String>,
    pub ieee: Option<String>,
    /// Home Assistant's entity id, `lock.workshop_door`. Required for that
    /// backend, meaningless for the other.
    pub entity: Option<String>,
    pub role: String,
    pub resource: String,
    /// Written by registration (#5). Holds `device_id` and `auth_token`.
    pub identity: PathBuf,
    #[serde(default)]
    pub countdown: bool,
    #[serde(default = "default_on_time")]
    pub on_time_s: u32,
    #[serde(default = "default_rearm")]
    pub rearm_s: u32,
    #[serde(default)]
    pub stop_resource_on_loss: bool,
}

impl DeviceEntry {
    /// The Zigbee2MQTT friendly name. `parse` has refused a Zigbee2MQTT device
    /// without one, so after parsing this cannot fail for one; a caller asking
    /// it of a Home Assistant device has the wrong device.
    pub fn friendly(&self) -> &str {
        self.friendly
            .as_deref()
            .unwrap_or_else(|| panic!("{}: not a Zigbee2MQTT device", self.name))
    }

    /// The Home Assistant entity id, likewise.
    pub fn entity(&self) -> &str {
        self.entity
            .as_deref()
            .unwrap_or_else(|| panic!("{}: not a Home Assistant device", self.name))
    }
}

/// The roles a device registers with. One, the configured role, except a
/// Home Assistant lock, which is `power` AND `strike`: the edge leases a
/// door's power bindings for exactly the strike pulse, and a door with no
/// strike binding has a pulse of zero -- so the lock must carry the strike
/// binding for its own lease to have any length. Declaring it is honest: the
/// latch IS released for `pulse_ms` on a yes, through the lease rather than
/// the scan reply (#20).
pub fn declared_roles(d: &DeviceEntry) -> Vec<&str> {
    match (d.backend, d.entity.as_deref().map(crate::ha::domain)) {
        (BackendKind::Homeassistant, Some("lock")) if d.role == "power" => vec!["power", "strike"],
        _ => vec![d.role.as_str()],
    }
}

fn default_on_time() -> u32 {
    60
}
fn default_rearm() -> u32 {
    15
}

/// A broker login, in its own 0600 file: `username`, `password`.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

/// A registered identity, in its own 0600 file, written by registration.
#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq)]
pub struct Identity {
    pub device_id: String,
    pub auth_token: String,
}

/// `mqtt://host:port` -> (host, port). Only that scheme, only that shape:
/// anything else is refused by name rather than guessed at.
pub fn broker_addr(url: &str) -> Result<(String, u16)> {
    let rest = url.strip_prefix("mqtt://").with_context(|| {
        format!("{url:?}: only mqtt://host:port is supported (no TLS, see Cargo.toml)")
    })?;
    let (host, port) = rest
        .rsplit_once(':')
        .with_context(|| format!("{url:?}: missing :port"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("{url:?}: port is not a number"))?;
    anyhow::ensure!(!host.is_empty(), "{url:?}: empty host");
    Ok((host.to_string(), port))
}

pub fn load(path: &Path) -> Result<Config> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse(&text).with_context(|| format!("parsing {}", path.display()))
}

pub fn parse(text: &str) -> Result<Config> {
    let cfg: Config = toml::from_str(text)?;
    for d in &cfg.devices {
        match d.backend {
            BackendKind::Zigbee2Mqtt => {
                anyhow::ensure!(
                    d.friendly.is_some(),
                    "device {}: a zigbee2mqtt device needs `friendly`",
                    d.name
                );
                anyhow::ensure!(
                    cfg.iot.is_some(),
                    "device {}: a zigbee2mqtt device needs an [iot] section",
                    d.name
                );
            }
            BackendKind::Homeassistant => {
                let entity = d.entity.as_deref().with_context(|| {
                    format!("device {}: a homeassistant device needs `entity`", d.name)
                })?;
                anyhow::ensure!(
                    cfg.homeassistant.is_some(),
                    "device {}: a homeassistant device needs a [homeassistant] section",
                    d.name
                );
                // The role decides what the entity must be able to do. A
                // power module is driven, so its domain must have a service;
                // a `climate` or a `sensor` under role power would be leased
                // and never switch.
                anyhow::ensure!(
                    crate::ha::service_for(entity, true).is_some(),
                    "device {}: entity {entity:?} is in a domain the bridge cannot switch (lock, switch, light, input_boolean)",
                    d.name
                );
                // `bridge probe` measures a plug's countdown over Zigbee2MQTT and
                // nothing measures a Home Assistant entity's, so none may be
                // claimed: an unmeasured countdown is the fail-safe lie #209 is
                // about.
                anyhow::ensure!(
                    !d.countdown,
                    "device {}: `countdown` cannot be measured for a homeassistant device yet; leave it false",
                    d.name
                );
            }
        }
        anyhow::ensure!(
            d.role == "power",
            "device {}: role {:?} is not supported yet (#1: power first)",
            d.name,
            d.role
        );
    }
    Ok(cfg)
}

pub fn load_credentials(path: &Path) -> Result<Credentials> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading credentials {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing credentials {}", path.display()))
}

/// The Home Assistant token: the file's contents, trimmed. A long-lived
/// access token is one line; anything else in the file is a mistake worth
/// refusing rather than sending.
pub fn load_ha_token(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading the Home Assistant token {}", path.display()))?;
    let token = text.trim();
    anyhow::ensure!(
        !token.is_empty() && !token.contains(char::is_whitespace),
        "{}: expected one token and nothing else",
        path.display()
    );
    Ok(token.to_string())
}

pub fn load_identity(path: &Path) -> Result<Identity> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading identity {} (register first: #5)", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing identity {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_committed_sample_parses_and_means_what_issue_1_decided() {
        // The sample is the documentation of the shape. If it stops parsing,
        // the documentation is wrong, and this is where that is noticed.
        let cfg = parse(include_str!("../config.sample.toml")).expect("config.sample.toml");
        assert_eq!(cfg.server.url, "https://spaces.example.org");
        assert_eq!(cfg.iot.as_ref().unwrap().base, "zigbee2mqtt");
        assert_eq!(cfg.devices.len(), 2);
        let d = &cfg.devices[0];
        assert_eq!(d.name, "extractor");
        assert_eq!(d.backend, BackendKind::Zigbee2Mqtt);
        assert_eq!(d.friendly(), "hackspace/south_wall/plug/filters");
        assert_eq!(d.resource, "laser");
        assert!(d.countdown);
        assert_eq!((d.on_time_s, d.rearm_s), (60, 15));
        assert!(d.stop_resource_on_loss);
        // The second is the Home Assistant lock (#20).
        let l = &cfg.devices[1];
        assert_eq!(l.backend, BackendKind::Homeassistant);
        assert_eq!(l.entity(), "lock.workshop_door");
        assert_eq!(l.role, "power");
        assert!(!l.countdown, "unmeasurable, so never claimed");
        assert!(l.stop_resource_on_loss);
        let ha = cfg.homeassistant.as_ref().expect("[homeassistant]");
        assert_eq!(ha.url, "ws://ha.local:8123/api/websocket");
        assert_eq!(
            ha.token_file,
            PathBuf::from("/var/lib/spaces-iot-bridge/ha-token")
        );
    }

    #[test]
    fn a_homeassistant_device_is_refused_without_what_it_needs() {
        // Each `ensure!` in `parse` for the backend, driven one at a time. The
        // mutant for each is deleting it: the config then parses and the
        // device fails at runtime, or never switches, or lies about countdown.
        let sample = include_str!("../config.sample.toml");
        let without_section = sample.replace("[homeassistant]", "[homeassistant_disabled]");
        let err = parse(&without_section).expect_err("no [homeassistant] section");
        assert!(err.to_string().contains("[homeassistant]"), "{err}");

        let no_entity = sample.replace("entity    = \"lock.workshop_door\"", "");
        let err = parse(&no_entity).expect_err("no entity");
        assert!(err.to_string().contains("entity"), "{err}");

        let climate = sample.replace(
            "entity    = \"lock.workshop_door\"",
            "entity    = \"climate.workshop\"",
        );
        let err = parse(&climate).expect_err("a domain with no switch service");
        assert!(err.to_string().contains("cannot switch"), "{err}");

        let claimed = sample.replace(
            "entity    = \"lock.workshop_door\"",
            "entity    = \"lock.workshop_door\"\ncountdown = true",
        );
        let err = parse(&claimed).expect_err("countdown claimed for an HA device");
        assert!(err.to_string().contains("countdown"), "{err}");
    }

    #[test]
    fn a_zigbee2mqtt_device_needs_its_friendly_name_and_an_iot_section() {
        let sample = include_str!("../config.sample.toml");
        let no_friendly = sample.replace("friendly  = \"hackspace/south_wall/plug/filters\"", "");
        let err = parse(&no_friendly).expect_err("no friendly");
        assert!(err.to_string().contains("friendly"), "{err}");
        let no_iot = sample.replace("[iot]", "[iot_disabled]");
        let err = parse(&no_iot).expect_err("no [iot]");
        assert!(err.to_string().contains("[iot]"), "{err}");
        // And a Home Assistant-only bridge needs neither: drop the extractor
        // (everything from its [[device]] up to the [homeassistant] section).
        let start = sample
            .find("[[device]]\nname      = \"extractor\"")
            .unwrap();
        let end = sample.find("# ---").unwrap();
        let ha_only =
            format!("{}{}", &sample[..start], &sample[end..]).replace("[iot]", "[iot_disabled]");
        let cfg = parse(&ha_only).expect("an HA-only bridge has no [iot]");
        assert_eq!(cfg.devices.len(), 1);
        assert!(cfg.iot.is_none());
    }

    #[test]
    fn a_homeassistant_lock_registers_as_power_and_strike_and_nothing_else_does() {
        // The mutant is a lock declaring `power` alone: its door then has a
        // zero-length pulse and the lock is never leased (#20).
        let cfg = parse(include_str!("../config.sample.toml")).unwrap();
        assert_eq!(declared_roles(&cfg.devices[0]), vec!["power"]);
        assert_eq!(declared_roles(&cfg.devices[1]), vec!["power", "strike"]);
        let mut sw = cfg.devices[1].clone();
        sw.entity = Some("switch.heater".into());
        assert_eq!(
            declared_roles(&sw),
            vec!["power"],
            "a switch is not a latch"
        );
    }

    #[test]
    fn a_role_the_bridge_cannot_serve_yet_is_refused_by_name() {
        let text = include_str!("../config.sample.toml")
            .replace("role      = \"power\"", "role      = \"sensor\"");
        let err = parse(&text).expect_err("sensor is #1's 'later'");
        assert!(err.to_string().contains("sensor"), "{err}");
    }

    #[test]
    fn an_unknown_backend_is_refused_by_name() {
        // The string `ensure!` that did this became serde's job when the
        // backend became an enum (#20, rung 1). The mutant is a catch-all
        // variant: `#[serde(other)]` would accept anything and silently run
        // a device on the wrong backend.
        let text = include_str!("../config.sample.toml")
            .replace("backend   = \"zigbee2mqtt\"", "backend   = \"openhab\"");
        let err = parse(&text).expect_err("openhab is not a backend");
        let msg = err.to_string();
        assert!(
            msg.contains("openhab") && msg.contains("zigbee2mqtt") && msg.contains("homeassistant"),
            "the error should name what was given and every backend accepted: {err}"
        );
        assert_eq!(
            parse(include_str!("../config.sample.toml"))
                .unwrap()
                .devices[0]
                .backend,
            BackendKind::Zigbee2Mqtt
        );
    }

    #[test]
    fn a_zigbee_only_bridge_needs_no_homeassistant_section() {
        let sample = include_str!("../config.sample.toml");
        let z2m_only = &sample[..sample.find("[homeassistant]").unwrap()];
        let cfg = parse(z2m_only).expect("a Zigbee2MQTT-only bridge");
        assert_eq!(cfg.devices.len(), 1);
        assert!(cfg.homeassistant.is_none());
    }

    #[test]
    fn a_token_file_holds_one_token_and_nothing_else() {
        let dir = std::env::temp_dir().join(format!("bridge-ha-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("ok");
        std::fs::write(&ok, "eyJhbGciOi.payload.sig\n").unwrap();
        assert_eq!(load_ha_token(&ok).unwrap(), "eyJhbGciOi.payload.sig");
        let two = dir.join("two");
        std::fs::write(&two, "token-one\ntoken-two\n").unwrap();
        assert!(load_ha_token(&two).is_err(), "two lines is not one token");
        let empty = dir.join("empty");
        std::fs::write(&empty, "\n").unwrap();
        assert!(load_ha_token(&empty).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn broker_urls_are_only_the_one_shape() {
        assert_eq!(
            broker_addr("mqtt://jarvis:1883").unwrap(),
            ("jarvis".into(), 1883)
        );
        assert!(
            broker_addr("mqtts://jarvis:8883").is_err(),
            "TLS is not wired"
        );
        assert!(broker_addr("mqtt://jarvis").is_err(), "no port");
    }
}
