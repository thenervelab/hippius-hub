//! Unified error type for the `hippius_core` crate.
//!
//! Why this module exists: the previous shape kept two parallel
//! `DownloadError` / `UploadError` enums, neither of which implemented
//! `std::error::Error`, `Display`, or `source()`. Python callers saw
//! `format!("{:?}", e)` Debug output for every failure, collapsing the
//! cause chain into a single line and losing the underlying `reqwest` /
//! `io` / `JoinError` context. This module replaces both enums with a
//! single thiserror-derived `CoreError` whose `source()` walk preserves
//! the chain, and the `core_err_to_py` helper in `lib.rs` renders that
//! chain into the `PyRuntimeError` message so Python's `__cause__` /
//! traceback can show every layer.
//!
//! All public fallible operations return `Result<T, CoreError>`. The
//! type alias `Result<T>` is re-exported from `lib.rs` for the
//! one-import calling pattern.

use std::io;
use tokio::task::JoinError;

/// Crate-wide result alias. See [`CoreError`] for the variants callers
/// may match on.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Errors raised by the `hippius_core` crate.
///
/// # Stability
///
/// `Reqwest`, `Io`, `ServerError`, `ChunkFailed`, `JoinFailed`, and
/// `MissingContentLength` are stable contracts — callers may match on
/// them. Future variants gated by `#[non_exhaustive]` may be added in
/// any release; do not match exhaustively without a wildcard arm if
/// you depend on forward compatibility.
///
/// # Source chain
///
/// `Reqwest`, `Io`, `ChunkFailed`, and `JoinFailed` preserve the
/// underlying cause via `std::error::Error::source()` (wired by
/// `thiserror`'s `#[from]` / `#[source]` attributes). Walk the chain
/// with `let mut cur = err.source(); while let Some(s) = cur { ...; cur
/// = s.source(); }` — exactly what `lib.rs::core_err_to_py` does at
/// the Python boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoreError {
    /// HTTP transport error from reqwest — connect failure, read
    /// timeout, TLS handshake, mid-stream error, etc. The reqwest
    /// `Error` is preserved as the cause so callers can downcast to
    /// inspect (`err.is_timeout()`, `err.status()`).
    #[error("HTTP transport error")]
    Reqwest(#[from] reqwest::Error),

    /// Local I/O error from the standard library — file open, read,
    /// write, seek, sync. The `io::Error` is preserved in the cause
    /// chain; check `io_err.kind()` for `ErrorKind::NotFound` and
    /// similar branch points.
    #[error("local I/O error")]
    Io(#[from] io::Error),

    /// HTTP request completed but the server returned an unsuccessful
    /// status. The tuple is `(status_code, diagnostic_message)`; the
    /// message is human-readable context (e.g. "server ignored Range
    /// header"), not the response body.
    #[error("server returned {0} ({1})")]
    ServerError(u16, String),

    /// A chunk download failed after the per-chunk retry loop
    /// exhausted its budget. `index` is the 0-based chunk index in
    /// the parent file; `source` carries the inner cause (typically
    /// `Reqwest`, `Io`, or a `ServerError`). Boxed because the
    /// recursive `CoreError -> CoreError` containment would otherwise
    /// be infinite-sized.
    #[error("chunk {index} failed")]
    ChunkFailed {
        /// 0-based index of the chunk in the parent file.
        index: usize,
        /// Inner cause — typically `Reqwest`, `Io`, or `ServerError`.
        /// Never another `ChunkFailed`: the orchestrator builds this
        /// variant once, on the first failure to escape its inner
        /// retry loop.
        #[source]
        source: Box<CoreError>,
    },

    /// A `tokio::spawn`'d chunk task panicked or was aborted before
    /// completion. `index` is `None` when the join error fired
    /// before the `(i, res)` tuple was constructed inside the task
    /// body — the chunk identity is then lost in the join layer, and
    /// no caller can recover it. `Some(i)` is unused today (the
    /// downloader only sees the chunkless case) but reserved for
    /// future failure modes where the spawn site has the index.
    /// Reserved instead of dropped so the field type encodes the
    /// "identity may be unknown" invariant rather than a sentinel
    /// like `usize::MAX` would (audit D1 follow-up).
    #[error("chunk task {index} failed", index = match .index {
        Some(i) => i.to_string(),
        None => "<unknown>".to_string(),
    })]
    JoinFailed {
        /// `Some(i)` if the spawn site preserved the chunk index;
        /// `None` if the join error fired before the chunk identity
        /// tuple was constructed.
        index: Option<usize>,
        /// The underlying `JoinError`. Inspect via
        /// `source.is_panic()` / `source.is_cancelled()` to branch.
        #[source]
        source: JoinError,
    },

    /// HEAD response omitted the `Content-Length` header. We refuse
    /// to proceed: a missing header is qualitatively different from
    /// `Content-Length: 0` (an explicitly empty blob), and silently
    /// treating it as zero would truncate the destination file to 0
    /// bytes — the audit D3 regression this variant was introduced
    /// to prevent. Unit variant: the failure IS the absence; there
    /// is no inspectable field a caller could use beyond the
    /// discriminant.
    #[error("server did not return Content-Length")]
    MissingContentLength,

    /// A downloaded chunk (or the assembled whole file) did not match its
    /// content-addressed digest or its declared byte length. Distinct from
    /// `ServerError`: transport succeeded and the bytes arrived, but they are
    /// the *wrong* bytes — so a content-addressed blob will serve the same
    /// wrong bytes on retry (see `is_retryable`, which classifies this
    /// permanent). The string carries the offending context (chunk offset /
    /// "assembled file") plus expected-vs-got so the Python side can surface a
    /// diagnosable message.
    #[error("integrity check failed: {0}")]
    Integrity(String),

    /// The request-body write stalled: reqwest stopped pulling body bytes for
    /// longer than the upload write-stall window while the body was not yet fully
    /// sent. This is an application-level watchdog on the *upload* write path
    /// (audit H1) covering a gap reqwest cannot: the peer completed TCP+TLS (so
    /// `connect_timeout` passed) and keeps the connection alive at the TCP layer
    /// (so `tcp_keepalive` never fires) but stopped draining the socket, for which
    /// reqwest has no per-operation timeout. The `Duration` is the observed idle
    /// gap. Retryable — a fresh attempt to a healthy replica (a rolling redeploy
    /// or a dead backend behind a live load balancer) is expected to succeed.
    #[error("upload write stalled: no progress for {0:?}")]
    Stall(std::time::Duration),

    /// A chunk-body READ stalled: no data arrived within the download idle-timeout
    /// window (audit M4). The download counterpart to [`CoreError::Stall`]: a peer
    /// that completed the handshake then dribbled or stopped mid-body, which
    /// `connect_timeout`/`tcp_keepalive` cannot see and reqwest's opt-in client
    /// `read_timeout` only catches when enabled. Default-on and reset on each
    /// successful read, so it bounds a slow-loris without capping an honest
    /// slow-but-steady transfer. The `Duration` is the idle window that elapsed.
    /// Retryable — a fresh attempt to a healthy replica should succeed.
    #[error("download read stalled: no data for {0:?}")]
    ReadStall(std::time::Duration),

    /// A caller/configuration input was invalid — e.g. a `FastCDC` average
    /// outside the splitter's accepted range, or a size that does not fit this
    /// platform's `usize`. Distinct from [`CoreError::Integrity`]: the bytes are
    /// not wrong, the *parameters* are. Permanent (see `is_retryable`) — the same
    /// bad input reproduces the same failure, so retrying only wastes backoff.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The server completed the request but returned a response that violates the
    /// protocol contract in a way that is plausibly transient — a missing/invalid
    /// `Location` on upload-init, or a body whose length disagrees with the
    /// declared size (short/over-send). Distinct from [`CoreError::ServerError`]
    /// (which carries a status code) and [`CoreError::Integrity`] (wrong bytes,
    /// permanent): a load balancer mid-rollout or a mangling proxy can emit these,
    /// so `is_retryable` classifies them retryable — a fresh attempt to a healthy
    /// replica is expected to return a well-formed response.
    #[error("malformed server response: {0}")]
    BadResponse(String),
}

impl CoreError {
    /// Returns `true` if retrying the operation that produced this error
    /// stands a chance of succeeding.
    ///
    /// Audit U3 (Phase 3.11): both the downloader's per-chunk retry loop
    /// and the uploader's whole-request retry loop need the same
    /// transient-vs-permanent classifier. Keeping it as an inherent
    /// method on `CoreError` is the single source of truth — the
    /// alternative (a free fn in one module that the other imports, or a
    /// duplicated copy) drifts as soon as either site adds a variant.
    ///
    /// Classification:
    ///
    /// * [`CoreError::Reqwest`] / [`CoreError::Io`] — transient
    ///   transport-layer blips (TCP reset, transient EIO, mid-stream
    ///   read), retryable.
    /// * [`CoreError::ServerError`] with `status ∈ 500..600` — RFC 9110
    ///   §15.6 server-error status codes, retryable.
    /// * [`CoreError::ServerError`] of 408 or 429 — the two retryable 4xx:
    ///   408 Request Timeout (RFC 9110 §15.5.9) and 429 Too Many Requests
    ///   (RFC 6585 §4). Harbor emits 429 under per-token rate limits, so
    ///   treating it as permanent would surface routine backpressure as a
    ///   terminal failure.
    /// * [`CoreError::ServerError`] with any other status — permanent (4xx
    ///   auth/format or any non-HTTP code), not retryable.
    /// * [`CoreError::ChunkFailed`] / [`CoreError::JoinFailed`] —
    ///   constructed by the orchestrator AFTER an inner retry loop has
    ///   already given up; retrying compounds backoff for failures
    ///   already declared terminal.
    /// * [`CoreError::MissingContentLength`] — HEAD-response shape
    ///   error, not transient.
    ///
    /// The match is intentionally exhaustive (no wildcard arm). The
    /// `#[non_exhaustive]` attribute on `CoreError` is for *external*
    /// callers; inside this crate the compiler still requires every
    /// variant to be named, so adding a future variant forces a
    /// deliberate classification decision instead of silently defaulting
    /// to one bucket.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            // Network/transport errors are retryable, and so is an upload
            // write-stall (audit H1) — the watchdog aborts a socket the peer
            // stopped draining; a fresh attempt to a healthy replica succeeds.
            // Network/transport blips, both stall watchdogs, and a
            // malformed-but-plausibly-transient response (missing Location, a
            // short/over-sent body) all clear on a fresh attempt to a healthy
            // replica — retryable.
            CoreError::Reqwest(_)
            | CoreError::Io(_)
            | CoreError::Stall(_)
            | CoreError::ReadStall(_)
            | CoreError::BadResponse(_) => true,
            // 5xx server errors are retryable, plus the two retryable 4xx
            // (408 Request Timeout, 429 Too Many Requests). Everything else
            // 4xx is permanent. `(500..600).contains(status)` operates on
            // `&u16` because the match is over `&CoreError`, so `status` binds
            // as `&u16`; `matches!(*status, ...)` derefs for the value pattern.
            CoreError::ServerError(status, _) => {
                matches!(*status, 408 | 429) || (500..600).contains(status)
            }
            // Three permanent variants:
            //   - ChunkFailed / JoinFailed are structured terminal errors
            //     produced after the per-chunk retry loop already did its
            //     work — retrying compounds backoff for failures the inner
            //     loop already declared unrecoverable.
            //   - MissingContentLength is a HEAD-response shape error,
            //     not a transient network condition.
            //   - Integrity is a wrong-bytes error on a content-addressed
            //     blob: the source serves the same bytes on retry, so it is
            //     permanent, not a transient blip to back off and re-attempt.
            //   - InvalidArgument is a bad caller/config input that reproduces
            //     identically on retry (a wrong FastCDC average, an oversize).
            CoreError::ChunkFailed { .. }
            | CoreError::JoinFailed { .. }
            | CoreError::MissingContentLength
            | CoreError::Integrity(_)
            | CoreError::InvalidArgument(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreError, Result};
    use std::error::Error;

    // Source-chain walk on a `ChunkFailed` wrapping a `ServerError`.
    // Pins two invariants: (1) the wrapper renders via its own
    // `Display` (chunk N failed), without flattening the inner cause
    // into its message — that is the anti-pattern axiom
    // rust_quality_57_error_source_chain calls out; (2) the inner
    // `ServerError` is reachable through `source()` so callers / the
    // Python boundary can render it as a `caused by:` tail. A future
    // refactor that collapsed the cause into a `format!` would fail
    // the second assertion immediately.
    #[test]
    fn chunk_failed_source_chain_walkable() {
        let inner = CoreError::ServerError(503, "transient".into());
        let outer = CoreError::ChunkFailed {
            index: 7,
            source: Box::new(inner),
        };

        let mut chain: Vec<String> = vec![outer.to_string()];
        let mut current: Option<&dyn Error> = outer.source();
        while let Some(s) = current {
            chain.push(s.to_string());
            current = s.source();
        }

        assert_eq!(
            chain.len(),
            2,
            "expected wrapper + inner ServerError in chain, got {chain:?}"
        );
        assert!(
            chain[0].contains("chunk 7 failed"),
            "wrapper display: {}",
            chain[0]
        );
        assert!(chain[1].contains("503"), "inner display: {}", chain[1]);
    }

    // Pin that the `JoinFailed { index: None, .. }` shape compiles
    // by constructing the constructor at a typed function-pointer
    // binding. We can't construct a real `JoinError` here without a
    // runtime (tokio's `JoinError` has no public constructor) and so
    // can't render the `Display` directly — the runtime-side test
    // lives in
    // `chunked_downloader::retry_classification_tests::join_failed_is_not_retryable`,
    // where a real aborted task produces a genuine `JoinError`. The
    // assertion below uses the function pointer once so the binding
    // is not flagged as `no_effect_underscore_binding`.
    #[test]
    fn join_failed_unknown_index_shape_compiles() {
        // Coercing the closure to a fully-typed `fn(...)` pointer is
        // the compile-time check: a field rename or type change
        // surfaces here, not just at faraway use sites.
        let ctor: fn(tokio::task::JoinError) -> CoreError = |source| CoreError::JoinFailed {
            index: None,
            source,
        };
        // Use `ctor` as a value so the binding has an observed
        // effect (clippy::no_effect_underscore_binding). Pointer
        // equality against itself is the smallest observation that
        // still keeps the function pointer alive.
        assert!(std::ptr::fn_addr_eq(ctor, ctor));
    }

    // The `#[from] io::Error` derive provides this conversion; the
    // test pins the wiring so a refactor that swapped the variant
    // for a manual `From` impl with the wrong arm would fail loudly.
    // An upload write-stall (audit H1) must classify retryable so the upload
    // retry loop re-attempts against a healthy replica rather than surfacing the
    // watchdog abort as terminal.
    #[test]
    fn stall_is_retryable() {
        assert!(CoreError::Stall(std::time::Duration::from_secs(30)).is_retryable());
    }

    // A download read-stall (audit M4) must also classify retryable so the per-chunk
    // retry loop re-fetches from a healthy replica instead of surfacing the idle-cut
    // as terminal — the download counterpart of the upload write-stall above.
    #[test]
    fn read_stall_is_retryable() {
        assert!(CoreError::ReadStall(std::time::Duration::from_secs(30)).is_retryable());
    }

    // A malformed-but-plausibly-transient response (missing Location on
    // upload-init, a short/over-sent body) must classify retryable so the
    // transport retry loop re-attempts against a healthy replica instead of
    // surfacing an LB-mid-rollout hiccup as terminal.
    #[test]
    fn bad_response_is_retryable() {
        assert!(CoreError::BadResponse("registry omitted Location".into()).is_retryable());
    }

    // An invalid caller/config argument reproduces on retry, so it must classify
    // permanent — retrying a bad FastCDC average only burns backoff.
    #[test]
    fn invalid_argument_is_not_retryable() {
        assert!(!CoreError::InvalidArgument("avg out of range".into()).is_retryable());
    }

    #[test]
    fn from_io_error_routes_to_io_variant() {
        let io_err = std::io::Error::other("test");
        let core_err: CoreError = io_err.into();
        assert!(
            matches!(core_err, CoreError::Io(_)),
            "expected Io variant, got {core_err:?}"
        );
    }

    // Verify the `Result<T>` alias is exported as a `Result<T,
    // CoreError>` — small but load-bearing because `lib.rs` and the
    // downloader rely on the alias resolving to the same error type
    // for `?` to compose.
    #[test]
    fn result_alias_resolves_to_core_error() {
        fn returns_core_err() -> Result<()> {
            Err(CoreError::MissingContentLength)
        }
        assert!(matches!(
            returns_core_err(),
            Err(CoreError::MissingContentLength)
        ));
    }
}
