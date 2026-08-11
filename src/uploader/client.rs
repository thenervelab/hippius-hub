use reqwest::Client;
use std::sync::OnceLock;
use std::time::Duration;

use crate::error::CoreError;

/// TCP/TLS handshake budget for `upload_client`, matching
/// `chunk_fetcher::CONNECT_TIMEOUT_SECS` on the download side (audit
/// M-UPLOAD-CONNECT). Kept local rather than re-exported so the two clients stay
/// independently tunable.
const CONNECT_TIMEOUT_SECS: u64 = 30;

/// Single upload attempt. Extracted from `upload_blob_async` in audit
/// U3 (Phase 3.11) so the surrounding retry loop has a unit to call
/// repeatedly. Each call opens its own `File` handle, builds its own
/// `FramedRead` stream, and sends one PUT - so the retry loop above
/// gets a fresh body on every attempt (the previous `Body::wrap_stream`
/// is consumed once the request future completes or errors).
/// Process-global HTTP client for blob uploads.
///
/// Mirrors the downloader, which builds its `reqwest::Client` once in
/// `ChunkedDownloader::new` and reuses it across all chunks. Previously
/// `try_upload_blob_once` rebuilt a client on every call - once per blob and
/// once per retry - discarding the keep-alive connection pool and forcing a
/// fresh DNS+TCP+TLS handshake against the registry host the previous blob just
/// finished using (audit N-4 / RUST-3). The `OnceLock` hoists construction out
/// of the per-attempt path so warm connections survive between blobs.
///
/// Construction is fallible (`build()` errors if the TLS backend won't
/// initialize), so this returns `Result` rather than `expect`-ing inside a
/// `get_or_init` closure - the crate denies `panic`/`unwrap` and warns on
/// `expect`. On the rare init race the losing thread's freshly built client is
/// dropped unused (RAII); after first init `get()` returns the shared client
/// immediately. `OnceLock` is valid in statics and never poisoned on panic
/// (doc.rust-lang.org/std/sync/struct.OnceLock.html).
pub(crate) fn upload_client() -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // Force HTTP/1.1 for the same reason as the downloader: avoids h2 single-TCP
    // multiplexing, lets uploads spread across multiple connections if the caller
    // parallelizes.
    //
    // No client-level timeout of any kind on the upload path (audit H-UPLOAD-TIMEOUT).
    // reqwest's `.timeout()` is a wall-clock deadline over the WHOLE request including
    // the streamed body - a 1h cap silently bounded total transfer, and since a reqwest
    // timeout is `is_retryable()`, `upload_blob_async` re-streamed from byte 0 up to the
    // retry budget (~4x the wall). `.read_timeout()` is NOT a safe substitute here: in
    // reqwest 0.12 it is a single non-resetting deadline over the `PendingRequest` phase
    // (request-send + wait-for-response-head) and only becomes a per-read resetting idle
    // timeout for the RESPONSE body - so on an upload it is a fixed wall on the body
    // write, reintroducing the same re-stream failure. Progress is instead bounded per
    // phase in `send_put_watchdogged`: `WRITE_STALL_TIMEOUT` guards the body write (idle,
    // resets on each accepted frame) and `RESPONSE_WAIT_TIMEOUT` bounds the wait for the
    // registry's response after the body is fully sent - neither caps an honest transfer.
    // A dead peer's handshake is caught by `connect_timeout`.
    let built = Client::builder()
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .http1_only()
        .tcp_keepalive(Duration::from_secs(30))
        // Generous fixed idle cap (was 8) so raising HIPPIUS_MAX_INFLIGHT_PACKS /
        // HIPPIUS_UPLOAD_WORKERS above 8 doesn't force connections past the 8th to
        // re-handshake per pack. The real in-flight bound is the _pack_upload_gate,
        // not this idle cap; matches the download client's pool_max_idle.
        .pool_max_idle_per_host(32)
        .build()?;
    Ok(CLIENT.get_or_init(|| built))
}

#[cfg(test)]
mod tests {
    // RUST-3 (audit N-4): the upload client is a process-global singleton,
    // built once and reused across blobs/retries rather than rebuilt per
    // attempt. Two calls must hand back the SAME `&'static Client` (pointer
    // equality) - the same invariant `lib.rs::runtime_tests` pins for the
    // shared runtime. `unwrap`/`expect`/`panic!` are denied crate-wide, so we
    // assert via `is_ok` + `if let` instead of unwrapping the Result.
    #[test]
    fn upload_client_returns_same_instance() {
        let a = super::upload_client();
        let b = super::upload_client();
        assert!(a.is_ok() && b.is_ok(), "upload client must build");
        if let (Ok(a), Ok(b)) = (a, b) {
            assert!(
                std::ptr::eq(a, b),
                "upload_client must return one shared instance, not a fresh client per call"
            );
        }
    }
}
