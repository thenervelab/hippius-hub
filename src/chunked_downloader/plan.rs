//! Pure chunk-tiling math (`num_chunks`/`chunk_bounds`) and the per-response
//! validation of what a Range GET came back with (status discipline and
//! `Content-Range` alignment).

use crate::error::CoreError;

/// Number of HTTP Range requests needed to cover `content_length` bytes when
/// each chunk is `chunk_size` bytes. Returns 0 for empty files (caller is
/// expected to handle that as a special case). Returns 0 for `chunk_size == 0`
/// to avoid a division-by-zero panic; `download` turns that degenerate case into
/// a loud `InvalidArgument` for a non-empty file rather than writing zeros (the
/// Python layer also rejects a non-positive `HIPPIUS_CHUNK_SIZE`, but the pyo3
/// entry point can bypass it - defense-in-depth).
pub(super) fn num_chunks(content_length: u64, chunk_size: u64) -> usize {
    if content_length == 0 || chunk_size == 0 {
        return 0;
    }
    // Integer ceiling division - `div_ceil` (stable since Rust 1.73) avoids
    // both the f64 round-trip the original code used AND the `+ chunk_size
    // - 1` overflow that the manual form would hit at `u64::MAX`.
    //
    // `try_into().unwrap_or(usize::MAX)` saturates the u64->usize conversion
    // on 32-bit targets. The downloader cannot realistically address more
    // than `usize::MAX` chunks (each chunk has its own `tokio::spawn`,
    // backing JoinHandle, and reqwest pool slot - saturating means "as
    // many chunks as the platform can spawn", not silent truncation).
    content_length
        .div_ceil(chunk_size)
        .try_into()
        .unwrap_or(usize::MAX)
}

/// Inclusive `(start, end)` byte range for chunk index `i` in a Range header.
/// The last chunk is truncated at `content_length - 1` rather than running
/// past EOF. Caller must ensure `i < num_chunks(content_length, chunk_size)`.
pub(super) fn chunk_bounds(content_length: u64, chunk_size: u64, i: usize) -> (u64, u64) {
    let start = i as u64 * chunk_size;
    let end = std::cmp::min(start + chunk_size - 1, content_length - 1);
    (start, end)
}

/// Parse a `Content-Range: bytes START-END/TOTAL` (or `.../*`) value into its
/// `(start, end)` byte bounds. Returns `None` for anything that is not a
/// well-formed byte range - wrong unit, missing `-` or `/`, or non-numeric
/// bounds. The TOTAL is intentionally not validated (a proxy may legitimately
/// send `*`); only the offsets matter for the alignment check below.
fn parse_content_range(value: &str) -> Option<(u64, u64)> {
    let range = value.trim().strip_prefix("bytes ")?.split('/').next()?;
    let (start, end) = range.split_once('-')?;
    Some((start.trim().parse().ok()?, end.trim().parse().ok()?))
}

/// Reject a 206 whose `Content-Range` does not cover exactly `bytes={start}-{end}`
/// (audit L1). A range-aliasing edge/proxy can return a length-correct 206 for the
/// WRONG offset; the chunk write then `seek(start)`s and lands the misplaced bytes,
/// and with hash verification off (the default) the corrupt file is cached forever
/// under the trusted content digest. A 206 MUST carry a matching `Content-Range`
/// (RFC 9110 section 15.3.7), so an absent, unparsable, or mismatched header is treated as
/// a retryable anomaly (`BadResponse`) that re-fetches rather than a silent write.
pub(super) fn require_content_range_matches(
    headers: &reqwest::header::HeaderMap,
    start: u64,
    end: u64,
) -> Result<(), CoreError> {
    let raw = headers
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok());
    if raw.and_then(parse_content_range) == Some((start, end)) {
        return Ok(());
    }
    Err(CoreError::BadResponse(format!(
        "chunk bytes={start}-{end}: 206 Content-Range {} does not cover the requested range",
        raw.unwrap_or("<absent>")
    )))
}

/// Verify that a chunk GET produced exactly HTTP 206 Partial Content.
///
/// Audit D2: a server that ignores `Range` and returns 200 + the FULL body would,
/// if `seek(start)`-written, overwrite everything past `end + 1` and corrupt the
/// file - so a 200 is rejected, its diagnostic naming the ignored range (distinct
/// from a "wrong bytes" error). This rejects a 200 even for a single-chunk
/// whole-file request.
///
/// Audit L5: the one accepted 200 is a single-chunk small-file download whose
/// range covers the WHOLE object (`start == 0 && end == content_length - 1`) - a
/// `200 OK` with the full body is then RFC 9110 section 15.3.7-legal and correct (the
/// over-length write guard already bounds a stray full body). A multi-chunk
/// download that got a range-ignored 200 still fails loudly, because its range is
/// not the whole object. A 200 carries no `Content-Range`, so the caller runs
/// [`require_content_range_matches`] only for a 206.
pub(super) fn require_acceptable_status(
    status: reqwest::StatusCode,
    start: u64,
    end: u64,
    content_length: u64,
) -> Result<(), CoreError> {
    use reqwest::StatusCode;
    match status {
        StatusCode::PARTIAL_CONTENT => Ok(()),
        StatusCode::OK if start == 0 && content_length > 0 && end == content_length - 1 => Ok(()),
        StatusCode::OK => Err(CoreError::ServerError(
            status.as_u16(),
            format!(
                "server ignored Range bytes={start}-{end} (returned 200 OK instead of 206); \
                 writing the full body at offset {start} would corrupt the file"
            ),
        )),
        other => Err(CoreError::ServerError(
            other.as_u16(),
            format!("Failed chunk bytes {start}-{end}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn num_chunks_empty_file_is_zero() {
        assert_eq!(num_chunks(0, 100), 0);
    }

    #[test]
    fn num_chunks_smaller_than_chunk_is_one() {
        assert_eq!(num_chunks(50, 100), 1);
        assert_eq!(num_chunks(1, 100), 1);
    }

    #[test]
    fn num_chunks_exact_multiple() {
        assert_eq!(num_chunks(100, 100), 1);
        assert_eq!(num_chunks(300, 100), 3);
    }

    #[test]
    fn num_chunks_with_remainder() {
        assert_eq!(num_chunks(101, 100), 2);
        assert_eq!(num_chunks(301, 100), 4);
    }

    #[test]
    fn num_chunks_zero_chunk_size_does_not_panic() {
        // Defense in depth: Python validates this, but keep the Rust side safe.
        assert_eq!(num_chunks(1000, 0), 0);
    }

    #[test]
    fn num_chunks_handles_default_size_at_default_proportions() {
        // 100 MiB chunk, 250 MiB file -> 3 chunks (100+100+50)
        let mib = 1024 * 1024;
        assert_eq!(num_chunks(250 * mib, 100 * mib), 3);
    }

    #[test]
    fn chunk_bounds_first_chunk_is_zero_based() {
        assert_eq!(chunk_bounds(1000, 100, 0), (0, 99));
    }

    #[test]
    fn chunk_bounds_middle_chunk_is_full_size() {
        assert_eq!(chunk_bounds(1000, 100, 5), (500, 599));
    }

    #[test]
    fn chunk_bounds_last_chunk_truncates_at_eof() {
        // 1024 bytes, 1000-byte chunks -> chunk 0 is 0-999, chunk 1 is 1000-1023.
        assert_eq!(chunk_bounds(1024, 1000, 0), (0, 999));
        assert_eq!(chunk_bounds(1024, 1000, 1), (1000, 1023));
    }

    #[test]
    fn chunk_bounds_exact_multiple_full_last_chunk() {
        // 300 bytes, 100-byte chunks -> final chunk fills exactly.
        assert_eq!(chunk_bounds(300, 100, 2), (200, 299));
    }

    #[test]
    fn chunk_bounds_off_by_one_at_boundary() {
        // The classic off-by-one: file size exactly equal to one chunk_size + 1.
        // Should produce 2 chunks: 0..=99 and 100..=100.
        assert_eq!(num_chunks(101, 100), 2);
        assert_eq!(chunk_bounds(101, 100, 0), (0, 99));
        assert_eq!(chunk_bounds(101, 100, 1), (100, 100));
    }

    #[test]
    fn chunk_bounds_one_byte_file_one_chunk() {
        assert_eq!(num_chunks(1, 100), 1);
        assert_eq!(chunk_bounds(1, 100, 0), (0, 0));
    }

    // Phase 7.1 backfill: hand-picked fixtures above cover the boundary cases
    // a human would think of (off-by-one, exact multiples, one-byte file). The
    // proptest block below pins the STRUCTURAL invariants over the full u64
    // input space - the shrinker surfaces edges the author didn't enumerate.
    // Five properties together specify what `num_chunks` + `chunk_bounds`
    // MUST produce for any valid input, independent of implementation:
    //   - coverage: chunk sizes sum to content_length
    //   - contiguity: no gaps, no overlaps between consecutive chunks
    //   - full span: first chunk starts at 0, last ends at content_length - 1
    //   - chunk_size == 0 -> 0 chunks (defense-in-depth no-panic guarantee)
    //   - content_length == 0 -> 0 chunks (empty-file guarantee)
    // Input bounds are deliberately small enough (<= 1 GB / 200 MB) to keep
    // the default 256-case run under a second while still spanning realistic
    // file sizes. `proptest::prop_assert!` / `prop_assert_eq!` are used in
    // place of `assert!` so the shrinker reports the minimal failing case
    // instead of aborting the test runner on the first failure.
    proptest::proptest! {
        /// Sum of `(end - start + 1)` across all chunks equals `content_length`.
        #[test]
        fn proptest_chunks_cover_exactly_content_length(
            content_length in 1u64..1_000_000_000,
            chunk_size in 1u64..200_000_000,
        ) {
            let n = num_chunks(content_length, chunk_size);
            proptest::prop_assert!(n > 0, "non-empty file must have >=1 chunk");
            let mut total = 0u64;
            for i in 0..n {
                let (s, e) = chunk_bounds(content_length, chunk_size, i);
                proptest::prop_assert!(s <= e, "chunk {} has start > end: {} > {}", i, s, e);
                total += e - s + 1;
            }
            proptest::prop_assert_eq!(total, content_length);
        }

        /// Consecutive chunks are disjoint and contiguous: chunk i ends one
        /// byte before chunk i+1 begins.
        #[test]
        fn proptest_chunks_are_contiguous(
            content_length in 1u64..1_000_000_000,
            chunk_size in 1u64..200_000_000,
        ) {
            let n = num_chunks(content_length, chunk_size);
            for i in 1..n {
                let (_, prev_end) = chunk_bounds(content_length, chunk_size, i - 1);
                let (cur_start, _) = chunk_bounds(content_length, chunk_size, i);
                proptest::prop_assert_eq!(
                    cur_start, prev_end + 1,
                    "gap or overlap between chunk {} and {}", i - 1, i
                );
            }
        }

        /// First chunk starts at byte 0; last chunk ends at `content_length - 1`.
        #[test]
        fn proptest_chunks_span_full_file(
            content_length in 1u64..1_000_000_000,
            chunk_size in 1u64..200_000_000,
        ) {
            let n = num_chunks(content_length, chunk_size);
            let (first_start, _) = chunk_bounds(content_length, chunk_size, 0);
            let (_, last_end) = chunk_bounds(content_length, chunk_size, n - 1);
            proptest::prop_assert_eq!(first_start, 0);
            proptest::prop_assert_eq!(last_end, content_length - 1);
        }

        /// `chunk_size == 0` returns 0 chunks (no panic via div-by-zero).
        #[test]
        fn proptest_num_chunks_handles_zero_chunk_size(
            content_length in 0u64..1_000_000_000,
        ) {
            proptest::prop_assert_eq!(num_chunks(content_length, 0), 0);
        }

        /// `content_length == 0` returns 0 chunks (empty file -> no Range GETs).
        #[test]
        fn proptest_num_chunks_handles_zero_content(
            chunk_size in 0u64..200_000_000,
        ) {
            proptest::prop_assert_eq!(num_chunks(0, chunk_size), 0);
        }
    }
}

// Kept separate from the chunk-math `tests` module so the two test
// categories don't bleed into each other: chunk math is pure-arithmetic,
// this module is about HTTP status discipline. Audit D2.
#[cfg(test)]
mod partial_content_tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn accepts_206() {
        assert!(require_acceptable_status(StatusCode::PARTIAL_CONTENT, 0, 99, 100).is_ok());
    }

    // Audit L5: a 200 OK is accepted ONLY when the request covers the whole object
    // (a single-chunk small-file download) - the full body written at offset 0 is
    // then correct and RFC 9110 section 15.3.7-legal.
    #[test]
    fn accepts_whole_file_200() {
        assert!(require_acceptable_status(StatusCode::OK, 0, 99, 100).is_ok());
    }

    // A range-ignored 200 on a MULTI-chunk download (the range is not the whole
    // object) still fails loudly, and the diagnostic names the ignored range - the
    // only signal distinguishing "server ignored Range" from "wrong bytes".
    // `let ... else { unreachable!() }` instead of `.unwrap_err()`/`panic!()`
    // because the project denies `unwrap_used` and `panic` cluster-wide.
    #[test]
    fn rejects_range_ignored_200_with_diagnostic() {
        // range 0-99 of a 1000-byte object is NOT the whole file -> must reject.
        let Err(err) = require_acceptable_status(StatusCode::OK, 0, 99, 1000) else {
            unreachable!("a non-whole-file 200 must be rejected")
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("ignored Range"),
            "diagnostic must name the ignored Range header, got: {msg}"
        );
    }

    #[test]
    fn rejects_other_4xx_5xx() {
        assert!(require_acceptable_status(StatusCode::NOT_FOUND, 0, 99, 100).is_err());
        assert!(require_acceptable_status(StatusCode::INTERNAL_SERVER_ERROR, 0, 99, 100).is_err());
    }

    fn cr_headers(value: &str) -> reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        // Test values are ASCII, so `from_str` succeeds; `if let` avoids a denied
        // `unwrap` while leaving the map empty on the (unreachable) error path.
        if let Ok(v) = reqwest::header::HeaderValue::from_str(value) {
            h.insert(reqwest::header::CONTENT_RANGE, v);
        }
        h
    }

    // Audit L1: the 206 Content-Range must cover exactly the requested bytes.
    #[test]
    fn content_range_matching_is_accepted() {
        assert!(require_content_range_matches(&cr_headers("bytes 100-199/500"), 100, 199).is_ok());
    }

    #[test]
    fn content_range_wrong_offset_is_rejected() {
        // Length-correct (100 bytes) but offset-wrong (0- not 100-): the exact
        // silent-corruption case L1 defends against. The rejection is the honest
        // `BadResponse` (a protocol-contract violation, not a local I/O failure)
        // and MUST stay retryable - an LB mid-rollout can emit one bad header,
        // and the pre-BadResponse `Io` shape retried too. This assertion is the
        // behavior pin for that classification.
        let res = require_content_range_matches(&cr_headers("bytes 0-99/500"), 100, 199);
        match res {
            Err(err @ CoreError::BadResponse(_)) => {
                assert!(
                    err.is_retryable(),
                    "Content-Range mismatch must stay retryable, got permanent: {err}"
                );
            }
            other => unreachable!("expected BadResponse, got {other:?}"),
        }
    }

    #[test]
    fn content_range_absent_is_rejected() {
        assert!(matches!(
            require_content_range_matches(&reqwest::header::HeaderMap::new(), 0, 99),
            Err(CoreError::BadResponse(_))
        ));
    }

    #[test]
    fn parse_content_range_reads_bounds() {
        assert_eq!(parse_content_range("bytes 5-17/42"), Some((5, 17)));
        assert_eq!(parse_content_range("bytes 0-0/*"), Some((0, 0)));
        assert_eq!(parse_content_range("bogus"), None);
        assert_eq!(parse_content_range("bytes 5/42"), None);
    }

    proptest::proptest! {
        // Round-trip: any (start, end) formatted as a byte Content-Range parses
        // back to the same bounds, so the parser agrees with the wire format the
        // alignment check relies on - for offsets a hand-picked fixture would miss.
        #[test]
        fn parse_content_range_round_trips(start in 0u64.., extra in 0u64..) {
            let end = start.saturating_add(extra);
            let header = format!("bytes {start}-{end}/{}", end.saturating_add(1));
            proptest::prop_assert_eq!(parse_content_range(&header), Some((start, end)));
        }
    }
}
