//! Bounded derivation of Pi session identifiers.
//!
//! Pi requires a session identifier to start and end with an alphanumeric character and to stay
//! within `[A-Za-z0-9._-]`. Zeroshot's node-instance and execution identities are bounded opaque
//! strings with no such guarantee, so they are never passed through unchanged: each is hashed into
//! the Pi charset. The derivation is stable, which is what lets a node-instance session survive a
//! loop revisit and reuse the same provider session.

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

const INITIAL: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// FIPS 180-4 SHA-256. Used only as a bounded charset adapter: the input is already a unique
/// private identity, never a secret, so this carries no security claim.
fn hex_digest(bytes: &[u8]) -> String {
    // Standard padding: 0x80, zeros to 56 bytes, then the 64-bit big-endian bit length.
    let mut padded = Vec::with_capacity(bytes.len() + 72);
    padded.extend_from_slice(bytes);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&((bytes.len() as u64) * 8).to_be_bytes());

    let mut state = INITIAL;
    for chunk in padded.chunks_exact(64) {
        // The block becomes the first sixteen schedule words; the eight-word state carries across
        // every block.
        let mut schedule = [0_u32; 64];
        for (index, word) in chunk.chunks_exact(4).enumerate() {
            schedule[index] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for index in 16..64 {
            let previous = schedule[index - 15];
            let recent = schedule[index - 2];
            let s0 = previous.rotate_right(7) ^ previous.rotate_right(18) ^ (previous >> 3);
            let s1 = recent.rotate_right(17) ^ recent.rotate_right(19) ^ (recent >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(K[index])
                .wrapping_add(schedule[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut hex = String::with_capacity(64);
    for word in state {
        hex.push_str(&format!("{word:08x}"));
    }
    hex
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
        // Guards the local compression function against an accidental rewrite.
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
