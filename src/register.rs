//! Claiming an invite for one bridged device: `POST /api/devices/register`,
//! and the identity it returns, persisted where the config says.
//!
//! Built on `spaces_device::registration` for the request and the reply, with
//! two things that crate cannot say at v0.4.0 and this bridge must:
//! `platform` is `linux` (the crate hardcodes "other", an ESP32's honest
//! answer; the server validates against exactly windows|linux|macos|other),
//! and `capabilities` carries the enforcement descriptors CS:S models --
//! `countdown` and `holds_last_on_disconnect` -- which the crate's
//! `Capabilities` has no fields for. The request is serialised and those two
//! are set on the JSON, which is what the server reads. Both gaps are filed:
//! Cambridge-Hackspace/spaces-firmware#2 (the fields), and
//! Cambridge-Hackspace/cooperative-systems-spaces#209 (the server's default for
//! `holds_last_on_disconnect` reads as "fails safe", which is why this bridge
//! answers it explicitly rather than relying on the default).

use std::path::Path;

use anyhow::{Context, Result};
use spaces_device::registration::{parse_reply, RegisterRequest, PATH};

use crate::config::{DeviceEntry, Identity};

/// The host's own MAC, formatted as the 17-character form the platform's
/// column holds. `None` if the host has no interface with one -- refused by
/// the caller, never fabricated.
pub fn host_mac() -> Result<String> {
    let mac = mac_address::get_mac_address()
        .context("reading the host MAC")?
        .context("this host has no interface with a MAC address; registration needs one")?;
    Ok(mac.to_string())
}

/// The registration body for one device. Pure, so it is tested without a
/// server: what goes on the wire is exactly this.
pub fn request_body(d: &DeviceEntry, invite: &str, mac: &str, version: &str) -> serde_json::Value {
    let req = RegisterRequest::with_roles(&[d.role.as_str()], invite, &d.name, mac, version, None);
    let mut v = serde_json::to_value(&req).expect("a RegisterRequest serialises");
    v["platform"] = serde_json::json!("linux");
    v["capabilities"] = serde_json::json!({
        "roles": [d.role],
        // From `bridge probe`, by hand (#1 q1). Declared only when measured.
        "countdown": d.countdown,
        // A smart plug keeps its relay state when its link drops. That is the
        // physical truth of the device class, and it is what makes `countdown`
        // the thing that lets it fail safe at all.
        "holds_last_on_disconnect": true,
        // It cannot cut its own output on a local condition; it has no inputs.
        "local_inhibit": false,
        "local_inputs": [],
    });
    v
}

/// Claim the invite and persist the identity. Refuses to overwrite an
/// existing identity file: nothing re-registers on its own (#1), and a
/// registration that replaced a live identity would orphan every binding
/// made to the old `device_id`.
pub async fn register(server_url: &str, d: &DeviceEntry, invite: &str) -> Result<Identity> {
    anyhow::ensure!(
        !d.identity.exists(),
        "{} already has an identity at {}; delete it deliberately if this device must re-register",
        d.name,
        d.identity.display()
    );
    let mac = host_mac()?;
    let body = request_body(d, invite, &mac, env!("CARGO_PKG_VERSION"));
    let url = format!("{}{}", server_url.trim_end_matches('/'), PATH);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = resp.status().as_u16();
    let bytes = resp
        .bytes()
        .await
        .context("reading the registration reply")?;
    let registered = parse_reply(status, &bytes).map_err(anyhow::Error::msg)?;
    let identity = Identity {
        device_id: registered.device_id,
        auth_token: registered.auth_token,
    };
    persist(&d.identity, &identity)?;
    Ok(identity)
}

/// Write the identity file, 0600, creating its directory. Refuses an existing
/// file for the reason `register` gives.
pub fn persist(path: &Path, identity: &Identity) -> Result<()> {
    anyhow::ensure!(
        !path.exists(),
        "{} exists; not overwriting an identity",
        path.display()
    );
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = toml::to_string(identity).context("serialising the identity")?;
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("creating {}", path.display()))?;
        f.write_all(text.as_bytes())?;
    }
    #[cfg(not(unix))]
    std::fs::write(path, text)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extractor(dir: &Path) -> DeviceEntry {
        crate::config::parse(&include_str!("../config.sample.toml").replace(
            "/var/lib/spaces-iot-bridge/identities/extractor.toml",
            &dir.join("extractor.toml").display().to_string(),
        ))
        .unwrap()
        .devices
        .remove(0)
    }

    #[test]
    fn the_request_says_linux_and_declares_what_a_plug_is() {
        // Two things the firmware crate cannot say. The mutants are leaving
        // platform as the crate's "other", and not overwriting capabilities.
        let tmp = tempdir();
        let d = extractor(&tmp);
        let v = request_body(&d, "INVITE", "02:00:00:00:00:01", "0.1.0");
        assert_eq!(v["platform"], "linux", "the server validates this exactly");
        assert_eq!(v["capabilities"]["roles"], serde_json::json!(["power"]));
        assert_eq!(
            v["capabilities"]["countdown"], true,
            "from the sample, by hand"
        );
        assert_eq!(v["capabilities"]["holds_last_on_disconnect"], true);
        assert_eq!(v["capabilities"]["local_inhibit"], false);
        assert_eq!(v["device_code"], "INVITE");
        assert_eq!(v["mac_address"], "02:00:00:00:00:01");
        assert_eq!(v["name"], "extractor");
        assert!(
            v["mac_address"].as_str().unwrap().len() <= 17,
            "the column is VARCHAR(17)"
        );
    }

    #[test]
    fn an_identity_is_persisted_private_and_never_overwritten() {
        let tmp = tempdir();
        let path = tmp.join("ids").join("extractor.toml");
        let id = Identity {
            device_id: "dev-1".into(),
            auth_token: "tok".into(),
        };
        persist(&path, &id).unwrap();
        let back = crate::config::load_identity(&path).unwrap();
        assert_eq!(back, id);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "a token file readable by others is a leak");
        }
        // The mutant is dropping the exists() refusal: a second registration
        // would then silently orphan every binding to the first device_id.
        let other = Identity {
            device_id: "dev-2".into(),
            auth_token: "tok2".into(),
        };
        let err = persist(&path, &other).expect_err("must refuse");
        assert!(err.to_string().contains("not overwriting"), "{err}");
        assert_eq!(
            crate::config::load_identity(&path).unwrap(),
            id,
            "the first identity survives"
        );
    }

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "bridge-test-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }
    fn rand_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
