//! The process-wide rustls crypto provider, installed once before the one
//! HTTPS call this crate makes. The same ten lines as CS:S's edge and CLI, for
//! the same reason: rustls 0.23 panics rather than erroring when no provider
//! is installed, and an explicit install survives any future bump that adds a
//! second provider, where feature-based auto-detection would not.

use tracing::{debug, warn};

/// Idempotent: a second call is reported and ignored, because losing the race
/// means a provider is installed, which is all this is for.
pub fn install_crypto_provider() {
    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => debug!("rustls crypto provider installed (aws-lc-rs, post-quantum preferred)"),
        Err(_) => warn!("rustls crypto provider was already installed; leaving it alone"),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_tls_client_config_can_be_built_after_installing_the_provider() {
        // The mutant is making install_crypto_provider a no-op: get_default()
        // is then None.
        super::install_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        assert!(!config.crypto_provider().cipher_suites.is_empty());
    }
}
