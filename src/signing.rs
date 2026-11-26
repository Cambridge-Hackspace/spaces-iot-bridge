//! Signing the local wire (CS:S #249): the one thing every bridged device
//! needs for it that the protocol crates do not provide, a source of nonces.
//!
//! A plug signs through `spaces_device::Module::signing`; a sensor through
//! [`crate::sensor::Sensor::signing`]. Both draw their nonce here.

/// 16 bytes from the operating system's random source, as hex: a fresh nonce
/// for one end of the local wire. Drawn when a device starts, and again only
/// when the seal code asks for a new one.
pub fn nonce() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the OS random source is available");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_nonce_is_32_hex_and_never_the_same_twice() {
        // The mutant is a constant nonce: every restart would then reuse the
        // nonce the edge's seals already named, and replayed seals would open.
        let a = super::nonce();
        let b = super::nonce();
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b);
    }
}
