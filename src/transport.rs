//! Shared transport constants for both HTTP planes (Task D3).
//!
//! The upload and download clients themselves stay separate on purpose - their
//! timeout philosophies differ deliberately (no client-level timeout on the
//! upload path, per-phase watchdogs instead; per-request budgets on downloads -
//! see `uploader::client::upload_client` for the full argument). But the
//! NUMBERS the two sides happened to share were previously defined once per
//! side, each documented as "kept local so the two clients stay independently
//! tunable". That tunability was never exercised, and duplicated values drift
//! silently; this module deliberately reverses that decision and holds each
//! shared constant exactly once. Constants with a single consumer (e.g. the
//! download side's `HEAD_REQUEST_TIMEOUT`, `DOWNLOAD_READ_IDLE`,
//! `DOWNLOAD_POOL_MAX_IDLE`, the upload watchdog's stall windows) stay local
//! to their plane.
//!
//! Everything here is `pub(crate)`, so clippy's `dead_code` lint (promoted to
//! an error by `-D warnings`) still fails the build if a refactor removes a
//! constant's last production call site - the same enforcement each per-side
//! copy carried.

use std::time::Duration;

/// TCP/TLS handshake budget shared by both process-global clients:
/// `uploader::client::upload_client` (audit M-UPLOAD-CONNECT) sets it directly,
/// and the download side's `chunk_fetcher::TransportTimeouts::default` uses it
/// as the `connect` fallback when `HIPPIUS_CONNECT_TIMEOUT` is unset (audit
/// L9). This covers only the connect phase - what bounds the rest of a request
/// differs per plane and is documented at each client.
pub(crate) const CONNECT_TIMEOUT_SECS: u64 = 30;

/// Buffer size for every whole-file sequential read loop that feeds SHA-256:
/// `uploader::hash::hash_file_async`, the pack path's
/// `chunk_fetcher::scatter::compute_sha256`, the Range path's
/// `chunked_downloader` verify re-read, and `incremental_hash::incremental_hash`.
/// Formerly `HASH_READ_BUFFER` on the upload side and two `VERIFY_READ_BUFFER`
/// copies on the download side, all 8 MiB for the same reason (audit L16): the
/// original 64 KiB buffer did ~128x more `read(2)` syscalls (~82k vs ~640 for a
/// 5 GiB blob) for no benefit - these passes are bandwidth-bound, not
/// latency-bound.
pub(crate) const IO_READ_BUFFER: usize = 8 * 1024 * 1024;

/// Full-request budget for a single chunk GET, applied per request via
/// `RequestBuilder::timeout` on both download paths: the pack fetch
/// (`chunk_fetcher::assemble::fetch_pack`) and the legacy Range chunk GET
/// (`chunked_downloader`, audit D6). `connect_timeout` covers only the TCP/TLS
/// handshake; a slow-loris server that handshakes then dribbles bytes would
/// otherwise hold a connection open forever. Five minutes is generous for
/// either payload - a ~220 KB/s floor on a ~64 MiB pack, ~333 KB/s on the
/// 100 MB legacy `DEFAULT_CHUNK_SIZE` (rope enough for slow mobile uplinks) -
/// yet forecloses an indefinitely-stuck fetch. Per-request (reqwest 0.12:
/// `RequestBuilder::timeout` overrides any client-level value) so other uses of
/// the shared client, e.g. the size-probe HEAD, pick their own budget.
///
/// Held in a named const (not inline) as one layer of a three-layer guard:
/// clippy's `dead_code` lint enforces the const is USED at both call sites, and
/// the value-pin test below enforces the VALUE can't silently drift. An earlier
/// shape of the guard grepped the source via `include_str!` for the literal
/// timeout call - vacuous, because the assertion text itself contained the
/// substring it searched for; the named-const approach removes the
/// self-reference.
pub(crate) const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

#[cfg(test)]
mod tests {
    use super::*;

    // Replaces the two per-side value pins (previously in
    // `chunk_fetcher::assemble` and `chunked_downloader::download`): one test
    // pinning each shared constant, so a value edit forces a deliberate
    // two-place change. Clippy's dead-code lint separately enforces that each
    // const keeps its production call sites.
    #[test]
    fn shared_transport_constants_are_pinned() {
        assert_eq!(
            CHUNK_REQUEST_TIMEOUT,
            Duration::from_mins(5),
            "CHUNK_REQUEST_TIMEOUT must be 5 minutes; a slow-loris server \
             could otherwise hold a TCP connection open indefinitely after \
             a fast handshake (connect_timeout covers only the handshake).",
        );
        assert_eq!(
            CONNECT_TIMEOUT_SECS, 30,
            "handshake budget shared by the upload and download clients \
             (audit M-UPLOAD-CONNECT / L9)",
        );
        assert_eq!(
            IO_READ_BUFFER,
            8 * 1024 * 1024,
            "8 MiB keeps whole-file hash passes at ~640 read(2) syscalls per \
             5 GiB instead of ~82k (audit L16)",
        );
    }
}
