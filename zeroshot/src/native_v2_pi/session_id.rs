//! Bounded derivation of Pi session identifiers.
//!
//! Pi requires a session identifier to start and end with an alphanumeric character and to stay
//! within `[A-Za-z0-9._-]`. Zeroshot's node-instance and execution identities are bounded opaque
//! strings with no such guarantee, so they are never passed through unchanged: each is hashed into
//! the Pi charset. The derivation is stable, which is what lets a node-instance session survive a
//! loop revisit and reuse the same provider session.

use sha2::{Digest, Sha256};

/// Longest identity this module will hash. Anything larger is rejected rather than truncated, so
/// two distinct identities cannot collide by sharing a prefix.
const MAX_IDENTITY_BYTES: usize = 512;

/// Derives a Pi session identifier from one Zeroshot identity.
///
/// The `zs-` prefix plus 64 hex characters is 67 characters and always begins and ends
/// alphanumeric, which satisfies Pi's documented constraint.
pub(super) fn pi_session_id(identity: &str) -> Result<String, &'static str> {
    if identity.is_empty() {
        return Err("Pi session identity must not be empty");
    }
    if identity.len() > MAX_IDENTITY_BYTES {
        return Err("Pi session identity exceeds the supported length");
    }
    Ok(format!("zs-{}", hex_digest(identity.as_bytes())))
}

/// Session storage directory for one provider session home.
pub(super) fn session_directory(home: &std::path::Path) -> std::path::PathBuf {
    home.join("sessions")
}

/// Agent directory for one provider session home.
pub(super) fn agent_directory(home: &std::path::Path) -> std::path::PathBuf {
    home.to_owned()
}

/// Lowercase hex SHA-256 of one bounded identity. Used only as a charset adapter: the input is
/// already a unique private identity, never a secret, so this carries no security claim beyond the
/// digest itself.
fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Reads the identity Pi reports in its session header and checks it against the derived value.
///
/// A mismatch means Pi is not addressing the session Zeroshot derived, which would silently split
/// one logical node session across two provider sessions.
pub(super) fn observe_session(observed: Option<&str>, expected: &str) -> Result<(), &'static str> {
    let Some(observed) = observed else {
        return Ok(());
    };
    if observed.is_empty() || observed.contains('\0') {
        return Err("Pi output contained an invalid session identifier");
    }
    if observed == expected {
        Ok(())
    } else {
        Err("Pi output reported a different session than the admitted node session")
    }
}

#[cfg(test)]
mod tests {
    use super::{agent_directory, observe_session, pi_session_id, session_directory};
    use std::path::Path;

    #[test]
    fn derivation_is_stable_and_within_the_pi_charset() {
        let first = pi_session_id("node-instance-7").unwrap();
        assert_eq!(first, pi_session_id("node-instance-7").unwrap());
        assert_eq!(first.len(), 67);
        assert!(first.starts_with("zs-"));
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        );
        assert!(
            first
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
        );
    }

    #[test]
    fn distinct_identities_derive_distinct_sessions() {
        assert_ne!(
            pi_session_id("node-instance-7").unwrap(),
            pi_session_id("node-instance-8").unwrap()
        );
    }

    #[test]
    fn identity_outside_the_supported_shape_is_rejected() {
        assert!(pi_session_id("").is_err());
        assert!(pi_session_id(&"x".repeat(513)).is_err());
        assert!(pi_session_id(&"x".repeat(512)).is_ok());
    }

    #[test]
    fn digest_matches_the_published_sha256_vectors() {
        // Pins the charset adapter to standard SHA-256 so a future rewrite cannot silently shift
        // session identities.
        assert_eq!(
            pi_session_id("abc").unwrap(),
            "zs-ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            pi_session_id("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq").unwrap(),
            "zs-248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn session_directories_stay_inside_the_private_home() {
        let home = Path::new("/run/pi-home");
        assert_eq!(agent_directory(home), home);
        assert_eq!(session_directory(home), home.join("sessions"));
    }

    #[test]
    fn a_different_provider_session_is_a_provider_failure() {
        let expected = pi_session_id("node-instance-7").unwrap();
        assert!(observe_session(Some(&expected), &expected).is_ok());
        assert!(observe_session(None, &expected).is_ok());
        assert!(observe_session(Some("zs-other"), &expected).is_err());
        assert!(observe_session(Some(""), &expected).is_err());
    }
}
