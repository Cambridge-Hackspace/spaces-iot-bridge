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
    pub iot: IotBroker,
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
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DeviceEntry {
    pub name: String,
    pub backend: BackendKind,
    pub friendly: String,
    pub ieee: Option<String>,
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
        assert_eq!(cfg.iot.base, "zigbee2mqtt");
        assert_eq!(cfg.devices.len(), 1);
        let d = &cfg.devices[0];
        assert_eq!(d.name, "extractor");
        assert_eq!(d.resource, "laser");
        assert!(d.countdown);
        assert_eq!((d.on_time_s, d.rearm_s), (60, 15));
        assert!(d.stop_resource_on_loss);
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
        let text = include_str!("../config.sample.toml").replace(
            "backend   = \"zigbee2mqtt\"",
            "backend   = \"homeassistant\"",
        );
        let err = parse(&text).expect_err("homeassistant is not wired yet");
        assert!(
            err.to_string().contains("homeassistant") && err.to_string().contains("zigbee2mqtt"),
            "the error should name both what was given and what is accepted: {err}"
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
    fn the_homeassistant_section_is_optional_and_parses_when_present() {
        // The sample has no [homeassistant]; a Zigbee2MQTT-only bridge must
        // keep parsing. And with the section present, both fields are read.
        let without = parse(include_str!("../config.sample.toml")).unwrap();
        assert_eq!(without.homeassistant, None);
        let with = include_str!("../config.sample.toml").replace(
            "[iot]",
            "[homeassistant]\nurl = \"ws://ha.local:8123/api/websocket\"\ntoken_file = \"/etc/spaces-iot-bridge/ha-token\"\n\n[iot]",
        );
        let cfg = parse(&with).unwrap();
        let ha = cfg.homeassistant.expect("[homeassistant] parsed");
        assert_eq!(ha.url, "ws://ha.local:8123/api/websocket");
        assert_eq!(
            ha.token_file,
            PathBuf::from("/etc/spaces-iot-bridge/ha-token")
        );
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
