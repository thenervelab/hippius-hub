//! Shared exponential-backoff-with-jitter delay for the transport retry loops.
//!
//! The download (`chunked_downloader`, `chunk_fetcher`) and upload (`uploader`)
//! paths each retry a transient failure up to `MAX_RETRIES` times. They
//! previously slept a DETERMINISTIC `2^attempt * 100ms`, which turns a shared
//! backpressure signal into a retry storm: when a registry emits 429/503 to the
//! many chunk/pack tasks in flight at once, every task fails at ~the same instant
//! and — sleeping the identical delay — re-collides on the endpoint that just
//! asked them to back off (audit L-JITTER). Full jitter spreads each retry
//! uniformly across `[0, cap]` so the herd decorrelates. This mirrors the Python
//! control plane, which already jitters its manifest-PUT retry.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Backoff cap base: retry attempt `n` (1-indexed) waits within `[0, 2^n * 100ms]`,
/// i.e. caps of 200 / 400 / 800 / 1600 ms — the same schedule the deterministic
/// loops used, now as the *upper bound* of a jittered draw rather than the exact
/// sleep.
const BACKOFF_BASE_MS: u64 = 100;

/// Full-jitter backoff for retry `attempt` (1-indexed): a `Duration` uniformly in
/// `[0, 2^attempt * 100ms]` (the AWS "full jitter" schedule).
///
/// The entropy source is the wall clock's sub-millisecond component, not a PRNG
/// dependency: two tasks reaching this call even microseconds apart draw
/// different offsets, which is all the decorrelation a retry storm needs. This is
/// backoff timing, not a security-sensitive random, so a coarse, dependency-free
/// source is the right trade — it keeps `rand` out of the extension's attack
/// surface.
pub(crate) fn backoff_delay(attempt: u32) -> Duration {
    // `duration_since(UNIX_EPOCH)` can only error if the clock is before 1970;
    // treat that as zero jitter (a fixed, small delay) rather than propagate — a
    // degenerate clock must not break the retry path.
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    jittered_backoff(attempt, entropy)
}

/// Pure core of [`backoff_delay`], split out so the `[0, cap]` invariant is
/// testable without reading the clock. `saturating_*` keeps a pathological
/// `attempt` from overflowing the shift/multiply; `.max(1)` guards the modulus
/// against a zero cap (unreachable for `attempt >= 1`, but defends the primitive).
fn jittered_backoff(attempt: u32, entropy: u64) -> Duration {
    let cap_ms = BACKOFF_BASE_MS.saturating_mul(2u64.saturating_pow(attempt));
    Duration::from_millis(entropy % cap_ms.max(1))
}

#[cfg(test)]
mod tests {
    use super::{jittered_backoff, BACKOFF_BASE_MS};
    use std::time::Duration;

    #[test]
    fn cap_matches_legacy_schedule() {
        // The jitter upper bound must equal the old deterministic sleep so the
        // worst-case backoff is unchanged: 200/400/800/1600 ms for attempts 1..=4.
        for (attempt, cap) in [(1u32, 200u64), (2, 400), (3, 800), (4, 1600)] {
            // entropy == cap-1 yields the largest in-range draw.
            let d = jittered_backoff(attempt, cap - 1);
            assert!(d < Duration::from_millis(cap), "attempt {attempt} exceeded cap {cap}");
            assert_eq!(BACKOFF_BASE_MS * 2u64.pow(attempt), cap);
        }
    }

    #[test]
    fn extreme_attempt_does_not_panic() {
        // A pathological attempt count must saturate, not overflow the shift.
        let _ = jittered_backoff(1000, u64::MAX);
    }

    proptest::proptest! {
        // Invariant: the delay is always strictly below the attempt's cap, for any
        // clock entropy. The shrinker surfaces modulus/edge bugs a fixture misses.
        #[test]
        fn always_within_cap(attempt in 1u32..=8, entropy in proptest::prelude::any::<u64>()) {
            let cap_ms = BACKOFF_BASE_MS.saturating_mul(2u64.saturating_pow(attempt));
            let d = jittered_backoff(attempt, entropy);
            proptest::prop_assert!(d < Duration::from_millis(cap_ms));
        }
    }
}
