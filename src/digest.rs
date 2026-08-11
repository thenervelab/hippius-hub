//! Binary SHA-256 digest newtype. Hex only at the FFI/display boundary.
//!
//! The download verify path compares one of these per carved chunk; a binary
//! 32-byte compare replaces the previous per-chunk `hex::encode` + `String`
//! equality (two allocations per chunk). `from_hex` is the only way to build
//! one from untrusted input, so a non-digest string can never masquerade as a
//! digest past the FFI boundary.

use sha2::{Digest, Sha256};

use crate::error::CoreError;

/// A raw 32-byte SHA-256 digest. `Copy` so the per-pack fan-out never clones
/// a heap string; hex appears only via `from_hex` (parse) and `Display` (render).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// SHA-256 of `data`. Exists because the digest-then-wrap incantation
    /// (`from(<[u8; 32]>::from(Sha256::digest(..)))`) hit rule-of-three across
    /// the verify path and fixtures, and Phase B's uploader needs it too.
    /// Deliberately no `From<sha2::digest::Output>`: that would couple the API
    /// surface to `GenericArray`, which sha2 0.11 replaces.
    pub fn of(data: &[u8]) -> Self {
        Self(Sha256::digest(data).into())
    }

    /// Parse `"<64 hex chars>"` (no `sha256:` prefix). Typed error so a
    /// non-digest string can never masquerade as one past this point.
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)
            .map_err(|e| CoreError::InvalidArgument(format!("bad sha256 hex {s:?}: {e}")))?;
        Ok(Self(out))
    }
}

impl From<[u8; 32]> for Sha256Digest {
    fn from(raw: [u8; 32]) -> Self {
        Self(raw)
    }
}

impl std::fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl std::fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sha256Digest({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known SHA-256 of the ASCII bytes `abc` (FIPS 180-2 test vector).
    const ABC_HEX: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn from_hex_display_round_trips() {
        let Ok(d) = Sha256Digest::from_hex(ABC_HEX) else {
            unreachable!("64 valid hex chars must parse")
        };
        assert_eq!(d.to_string(), ABC_HEX);
    }

    #[test]
    fn debug_wraps_the_hex_rendering() {
        let Ok(d) = Sha256Digest::from_hex(ABC_HEX) else {
            unreachable!("64 valid hex chars must parse")
        };
        assert_eq!(format!("{d:?}"), format!("Sha256Digest({ABC_HEX})"));
    }

    // Every reject path must be the typed InvalidArgument, never a panic or a
    // silently-truncated digest: short, odd-length, non-hex, and over-length
    // inputs are all unrepresentable as a Sha256Digest.
    #[test]
    fn from_hex_rejects_malformed_input() {
        let too_short = &ABC_HEX[..62];
        let odd_length = &ABC_HEX[..63];
        let non_hex = format!("{}zz", &ABC_HEX[..62]);
        let too_long = format!("{ABC_HEX}00");
        for bad in [too_short, odd_length, non_hex.as_str(), too_long.as_str()] {
            assert!(
                matches!(
                    Sha256Digest::from_hex(bad),
                    Err(CoreError::InvalidArgument(_))
                ),
                "{bad:?} must be rejected as InvalidArgument"
            );
        }
    }

    #[test]
    fn from_raw_bytes_matches_from_hex() {
        // The verify path compares a hashed-bytes digest against a from_hex-parsed
        // expectation; both raw-bytes construction routes (From<[u8; 32]> and `of`)
        // must meet the parsed known answer on the same value.
        let raw = Sha256Digest::from(<[u8; 32]>::from(Sha256::digest(b"abc")));
        let Ok(expected) = Sha256Digest::from_hex(ABC_HEX) else {
            unreachable!("64 valid hex chars must parse")
        };
        assert_eq!(raw, expected);
        assert_eq!(Sha256Digest::of(b"abc"), expected);
    }

    #[test]
    fn of_renders_the_known_answer_hex() {
        // `of` is the one-step route the hot path uses; pin it against the FIPS
        // vector end-to-end (hash + Display), independent of from_hex.
        assert_eq!(Sha256Digest::of(b"abc").to_string(), ABC_HEX);
    }

    #[test]
    fn distinct_digests_compare_unequal() {
        assert_ne!(Sha256Digest::of(b"abc"), Sha256Digest::of(b"abd"));
    }
}
