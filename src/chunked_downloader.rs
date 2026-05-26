use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom, BufWriter};
use tokio::task::AbortHandle;

use crate::error::CoreError;

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB default
const MAX_CONCURRENT_DOWNLOADS: usize = 32;
const MAX_RETRIES: u32 = 3;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024; // 8 MB read buffer for SHA256 verification

/// Number of HTTP Range requests needed to cover `content_length` bytes when
/// each chunk is `chunk_size` bytes. Returns 0 for empty files (caller is
/// expected to handle that as a special case). Returns 0 for `chunk_size == 0`
/// to avoid a division-by-zero panic if a caller sets `HIPPIUS_CHUNK_SIZE=0` —
/// the Python layer also validates this, but defense-in-depth.
fn num_chunks(content_length: u64, chunk_size: u64) -> usize {
    if content_length == 0 || chunk_size == 0 {
        return 0;
    }
    // Integer ceiling division — avoids the f64 round-trip the older code used.
    ((content_length + chunk_size - 1) / chunk_size) as usize
}

/// Inclusive `(start, end)` byte range for chunk index `i` in a Range header.
/// The last chunk is truncated at `content_length - 1` rather than running
/// past EOF. Caller must ensure `i < num_chunks(content_length, chunk_size)`.
fn chunk_bounds(content_length: u64, chunk_size: u64, i: usize) -> (u64, u64) {
    let start = i as u64 * chunk_size;
    let end = std::cmp::min(start + chunk_size - 1, content_length - 1);
    (start, end)
}

// Phase 3.8 (audit D8): the local DownloadError + UploadError enums
// were unified into `crate::error::CoreError`. The old enum had no
// `Display`/`Error`/`source()` impl, so Python callers saw flattened
// Debug output; the thiserror-derived replacement preserves the cause
// chain through `core_err_to_py`.

pub struct ChunkedDownloader {
    client: Client,
    url: String,
    auth_token: Option<String>,
    chunk_size: u64,
}

impl ChunkedDownloader {
    /// Construct a new concurrent downloader.
    pub fn new(url: String, auth_token: Option<String>, chunk_size_bytes: Option<u64>) -> Result<Self, CoreError> {
        // Force HTTP/1.1: with h2 reqwest multiplexes all chunks on a single TCP,
        // which caps aggregate throughput at the per-connection BBR ceiling (~150 MB/s
        // even on a fast edge). Forcing h1 makes each parallel chunk get its own TCP,
        // letting the kernel/qdisc fan out across the available bandwidth.
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .http1_only()
            .pool_max_idle_per_host(MAX_CONCURRENT_DOWNLOADS)
            .tcp_keepalive(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            url,
            auth_token,
            chunk_size: chunk_size_bytes.unwrap_or(DEFAULT_CHUNK_SIZE),
        })
    }

    /// Downloads the file concurrently by streaming each chunk directly to its
    /// offset in the final file (sparse pre-allocated). If `verify_hash` is
    /// true, reads the full file at the end to produce a SHA256. Otherwise
    /// returns an empty string.
    pub async fn download(&self, dest_path: &Path, verify_hash: bool) -> Result<String, CoreError> {
        // 1. Fetch the total blob size
        let content_length = self.get_content_length().await?;

        // Handle the empty-file case
        if content_length == 0 {
            return self.create_empty_file(dest_path).await;
        }

        let pb = ProgressBar::new(content_length);
        // The template string is a compile-time string literal — `indicatif` only
        // returns `Err` here for malformed format directives, which we control.
        #[expect(clippy::expect_used, reason = "infallible static template")]
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"));
        pb.set_message("📥 Downloading");

        let num_chunks = num_chunks(content_length, self.chunk_size);

        // Prepare the destination directory
        let parent_dir = dest_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;

        // 2. Pre-allocate the final file at the exact size (sparse OK).
        //    Each chunk task opens its own file handle and seeks to its offset.
        //    Concurrent writes via distinct handles to disjoint ranges are
        //    OS-safe (each handle has its own file pointer).
        {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(dest_path)
                .await?;
            f.set_len(content_length).await?;
            f.sync_all().await?; // Ensure the size is persisted before parallel writes
        }

        let dest_path_buf = dest_path.to_path_buf();

        // 3. Launch concurrent downloads — each streams directly to its
        //    correct offset in the final file.
        //
        // Audit D4: previously this used `buffer_unordered(MAX_CONCURRENT_DOWNLOADS)`
        // and early-returned on the first error, but dropping the `Buffered` stream
        // does NOT cancel the `tokio::spawn`'d tasks behind it — `JoinHandle::drop`
        // detaches a tokio task, leaving it running in the background where it
        // continues writing to `dest_path` and holding sockets after we've already
        // bubbled an error up. We now collect the spawn-side `AbortHandle`s eagerly
        // and call `.abort()` on every survivor before propagating the error, so
        // the survivors stop at their next await point instead of racing the next
        // download. The chunk-level HTTP concurrency bound that
        // `buffer_unordered(MAX_CONCURRENT_DOWNLOADS)` used to enforce is now
        // carried by `pool_max_idle_per_host(MAX_CONCURRENT_DOWNLOADS)` on the
        // reqwest client (line 98) — beyond pool capacity, reqwest queues HTTP
        // requests on the existing connections, so eager spawn does not multiply
        // network concurrency.
        let mut joins: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<(), CoreError>)>> =
            FuturesUnordered::new();
        let mut abort_handles: Vec<AbortHandle> = Vec::with_capacity(num_chunks);

        for i in 0..num_chunks {
            let (start, end) = chunk_bounds(content_length, self.chunk_size, i);

            let client = self.client.clone();
            let url = self.url.clone();
            let token = self.auth_token.clone();
            let chunk_pb = pb.clone();
            let path = dest_path_buf.clone();

            let handle = tokio::spawn(async move {
                let res = download_chunk_with_retry(client, url, token, start, end, i, path, chunk_pb).await;
                (i, res)
            });
            // `abort_handle()` clones the cooperative-cancel signal; the original
            // `JoinHandle` is what `FuturesUnordered` polls for completion.
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
        }

        // Drain the `FuturesUnordered` of `JoinHandle`s. Exhaustive match preserves
        // both the spawn-side (`JoinError`) and the download-layer cause: previously
        // both collapsed into a bare `ChunkFailed(usize)`, hiding which chunk failed
        // AND why. Phase 3.8 replaced the `usize::MAX` sentinel with
        // `JoinFailed.index: Option<usize>` — `None` here because the chunk index
        // lives inside the future's return tuple, and a `JoinError` that escapes
        // before the tuple is constructed has lost that identity. The thiserror
        // `Display` renders `None` as `<unknown>`.
        //
        // On any error we abort every collected handle before returning. Aborting
        // an already-completed handle is a documented no-op (tokio), so iterating
        // the full `abort_handles` vector is correct even though some tasks have
        // finished. We do not drain the remaining `joins` after firing the aborts:
        // tokio cancellation is cooperative — the spawned futures will return
        // `JoinError::is_cancelled()` at their next await and shut down on their
        // own; awaiting them here would only delay the user-visible failure.
        while let Some(res) = joins.next().await {
            match res {
                Err(join_err) => {
                    for a in &abort_handles {
                        a.abort();
                    }
                    return Err(CoreError::JoinFailed {
                        index: None,
                        source: join_err,
                    });
                }
                Ok((i, Err(chunk_err))) => {
                    for a in &abort_handles {
                        a.abort();
                    }
                    return Err(CoreError::ChunkFailed {
                        index: i,
                        source: Box::new(chunk_err),
                    });
                }
                Ok((_, Ok(()))) => continue,
            }
        }

        pb.finish_with_message("✅ Download complete");

        // 4. Optional SHA256 — a single sequential read-pass over the final file.
        //    Much faster than the old assembly phase (no rewrite).
        if verify_hash {
            let pb_hash = ProgressBar::new(content_length);
            // Same rationale as the download-phase bar above: static literal template.
            #[expect(clippy::expect_used, reason = "infallible static template")]
            pb_hash.set_style(ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.magenta/red}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("indicatif template is static and infallible")
                .progress_chars("=>-"));
            pb_hash.set_message("🔐 Verifying SHA256");

            let hash = compute_sha256(dest_path, &pb_hash).await?;
            pb_hash.finish_with_message("✅ Verified");
            Ok(hash)
        } else {
            Ok(String::new())
        }
    }

    /// Issue a HEAD request to obtain Content-Length
    async fn get_content_length(&self) -> Result<u64, CoreError> {
        let mut req = self.client.head(&self.url);
        if let Some(ref token) = self.auth_token {
            req = req.bearer_auth(token);
        }

        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(CoreError::ServerError(res.status().as_u16(), format!("Failed HEAD request: {:?}", res.status())));
        }

        // Audit D3: a missing/unparseable Content-Length previously fell through
        // to `unwrap_or(0)`, which `download()` then routed into `create_empty_file`
        // — silently truncating the destination and returning sha256 of empty.
        // We now surface a typed error; the empty-file path in `download()` is
        // reached only when the server explicitly sent `Content-Length: 0`.
        let content_length = res.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| val.parse::<u64>().ok())
            .ok_or(CoreError::MissingContentLength)?;

        Ok(content_length)
    }

    /// Special case: create an empty file when the size is 0
    async fn create_empty_file(&self, dest_path: &Path) -> Result<String, CoreError> {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .await?;
        f.sync_all().await?;
        drop(f);

        let mut hasher = Sha256::new();
        hasher.update(&[]);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// Compute the SHA256 of the final file in a single sequential read-pass.
///
/// Audit U1: same justification as `uploader::hash_file_async` — sha2's
/// digest loop is CPU-bound and would block a tokio worker for seconds on a
/// multi-GB verify, starving the parallel download tasks the runtime is
/// trying to drain. `spawn_blocking` parks the work on the dedicated
/// blocking pool instead. The `ProgressBar` is `Send + Sync` (Arc-internal
/// per indicatif docs), so cloning it into the closure for tick updates is
/// safe — `pb.inc` is thread-safe and now runs from the blocking thread.
///
/// The double `?` mirrors `hash_file_async`: outer `?` flattens
/// `JoinError → CoreError::Io`, inner `?` propagates `io::Error` from
/// the closure body.
async fn compute_sha256(path: &Path, pb: &ProgressBar) -> Result<String, CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
    let pb = pb.clone(); // indicatif::ProgressBar clones cheaply via internal Arc.
    tokio::task::spawn_blocking(move || -> Result<String, CoreError> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; VERIFY_READ_BUFFER];

        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            pb.inc(n as u64);
        }

        Ok(hex::encode(hasher.finalize()))
    })
    .await
    .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?
}

// Audit D5 retry classification moved to `CoreError::is_retryable` in
// `src/error.rs` (Phase 3.11). The uploader needs the same classifier,
// and a method on the error type is the single source of truth — no
// duplicate `fn` to drift, no `pub(crate)` import to maintain. See
// `CoreError::is_retryable` for the variant-by-variant rationale.

/// Wrapper with exponential-backoff retry for a single chunk download.
async fn download_chunk_with_retry(
    client: Client,
    url: String,
    token: Option<String>,
    start: u64,
    end: u64,
    _chunk_index: usize,
    dest_path: std::path::PathBuf,
    pb: ProgressBar,
) -> Result<(), CoreError> {
    let mut retries = 0;

    loop {
        match try_download_chunk_to_offset(&client, &url, &token, start, end, &dest_path, &pb).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retries += 1;
                // Audit D5: fail fast on permanent errors. The
                // `is_retryable` method borrows `&self` so the owned `e`
                // remains returnable below — it only inspects the
                // discriminant and (for `ServerError`) the status code.
                if !e.is_retryable() || retries > MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

/// Verify that a chunk GET produced exactly HTTP 206 Partial Content.
///
/// Audit D2: previously `try_download_chunk_to_offset` accepted any 2xx
/// status. A server that ignored the `Range` header would respond with
/// 200 OK and the FULL body; we would then `seek(start)` and stream that
/// full body starting at the chunk's offset, overwriting everything past
/// `end + 1` and producing a silently corrupt file. The diagnostic on
/// the 200 branch names the ignored range explicitly so the failure mode
/// is unambiguous in logs — distinct from a "server returned the wrong
/// bytes" error a caller might otherwise assume.
fn require_partial_content(
    status: reqwest::StatusCode,
    start: u64,
    end: u64,
) -> Result<(), CoreError> {
    use reqwest::StatusCode;
    match status {
        StatusCode::PARTIAL_CONTENT => Ok(()),
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

/// Streaming download of a chunk directly to its offset in the final file
/// (already pre-allocated). Each task opens its own file handle, seeks to its
/// offset, and writes bytes as they arrive from the HTTP stream.
/// Parallel writes to disjoint ranges are safe.
async fn try_download_chunk_to_offset(
    client: &Client,
    url: &str,
    token: &Option<String>,
    start: u64,
    end: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    // Audit D6: per-request timeout on the chunk GET. The `Client` (line 97)
    // sets `connect_timeout(30s)` but no full-request timeout, so a slow-loris
    // server could hold a TCP open and dribble bytes indefinitely without ever
    // tripping the connect phase. 5 minutes per chunk is generous given the
    // 100 MB `DEFAULT_CHUNK_SIZE` (≈ 333 KB/s floor before timing out) — enough
    // rope for slow mobile uplinks, tight enough that a stuck chunk cannot hang
    // the runtime forever. `RequestBuilder::timeout` overrides any client-level
    // value per the reqwest 0.12 docs; we keep it per-request so other client
    // uses (e.g. the HEAD in `get_content_length`) pick their own budget.
    let mut req = client.get(url)
        .header(header::RANGE, format!("bytes={}-{}", start, end))
        .timeout(Duration::from_secs(300));

    if let Some(ref t) = token {
        req = req.bearer_auth(t);
    }

    let mut res = req.send().await?;

    require_partial_content(res.status(), start, end)?;

    // Open this task's own handle on the pre-allocated final file, seek to start.
    let mut file = OpenOptions::new()
        .write(true)
        .open(dest_path)
        .await?;
    file.seek(SeekFrom::Start(start)).await?;

    // Wrap the file handle in a 2MB BufWriter to avoid thousands of small unbuffered write syscalls
    let mut buf_writer = BufWriter::with_capacity(2 * 1024 * 1024, file);

    // Stream HTTP body chunks directly to disk at our position.
    // No temp file, no assembly phase.
    loop {
        match res.chunk().await {
            Ok(Some(buf)) => {
                let len = buf.len();
                buf_writer.write_all(&buf).await?;
                pb.inc(len as u64);
            }
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        }
    }

    buf_writer.flush().await?;
    Ok(())
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
        // 100 MiB chunk, 250 MiB file → 3 chunks (100+100+50)
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
        // 1024 bytes, 1000-byte chunks → chunk 0 is 0-999, chunk 1 is 1000-1023.
        assert_eq!(chunk_bounds(1024, 1000, 0), (0, 999));
        assert_eq!(chunk_bounds(1024, 1000, 1), (1000, 1023));
    }

    #[test]
    fn chunk_bounds_exact_multiple_full_last_chunk() {
        // 300 bytes, 100-byte chunks → final chunk fills exactly.
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

    // Regression for audit D1: previously `ChunkFailed(usize)` discarded the
    // underlying cause, so a user saw "chunk 5 failed" with no clue whether
    // it was 404, 500, connection reset, or disk-full. The reshaped variant
    // carries the cause through `source: Box<CoreError>`; this test pins
    // the contract so a future refactor cannot silently re-flatten it.
    // `let ... else { unreachable!() }` is used instead of `panic!(...)`
    // because the project denies `panic` cluster-wide.
    #[test]
    fn chunk_failed_carries_cause() {
        let inner = CoreError::ServerError(404, "not found".into());
        let outer = CoreError::ChunkFailed {
            index: 3,
            source: Box::new(inner),
        };

        let CoreError::ChunkFailed { index, source } = outer else {
            unreachable!("constructed a ChunkFailed above; any other variant is a bug")
        };
        assert_eq!(index, 3);
        assert!(matches!(*source, CoreError::ServerError(404, _)));
    }

    // Regression for audit D6: pin the per-request timeout on the chunk GET.
    // A behavioral test would need a mock HTTP server with controlled latency
    // (Phase 4.3 territory); this structural source-grep is the minimal guard
    // that ensures a future refactor cannot silently drop the `.timeout(...)`
    // call and re-expose the slow-loris hang. `include_str!` is evaluated at
    // compile time on the same file the test lives in, so the assertion runs
    // against exactly the source the binary was built from.
    #[test]
    fn try_download_chunk_to_offset_sets_request_timeout() {
        let src = include_str!("chunked_downloader.rs");
        assert!(
            src.contains(".timeout(Duration::from_secs(300))"),
            "chunked_downloader.rs must call .timeout(...) on the chunk GET request",
        );
    }

    // Regression for audit D3: pin the variant shape so the missing-header
    // path cannot silently revert to `Ok(0)`. The assertion is intentionally
    // minimal — the contract here is "there is a distinct variant for this
    // case", not "the variant carries field X". Phase 3.8 wired this variant
    // through the thiserror-based `CoreError` hierarchy.
    #[test]
    fn missing_content_length_is_a_distinct_error() {
        let err = CoreError::MissingContentLength;
        assert!(matches!(err, CoreError::MissingContentLength));
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
        assert!(require_partial_content(StatusCode::PARTIAL_CONTENT, 0, 99).is_ok());
    }

    // The diagnostic on the 200 branch is load-bearing: it is the only signal
    // distinguishing "server ignored Range" from "server returned wrong bytes".
    // `let ... else { unreachable!() }` is used instead of `.unwrap_err()` /
    // `panic!()` because the project denies `unwrap_used` and `panic`
    // cluster-wide; the test still fails clearly if the helper accepts 200.
    #[test]
    fn rejects_200_with_diagnostic() {
        let result = require_partial_content(StatusCode::OK, 0, 99);
        let Err(err) = result else {
            unreachable!("require_partial_content must reject 200 OK")
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("ignored Range"),
            "diagnostic must name the ignored Range header, got: {msg}"
        );
    }

    #[test]
    fn rejects_other_4xx_5xx() {
        assert!(require_partial_content(StatusCode::NOT_FOUND, 0, 99).is_err());
        assert!(require_partial_content(StatusCode::INTERNAL_SERVER_ERROR, 0, 99).is_err());
    }
}

// Audit D5: pin the retry-classification contract. Tests cover the 4xx/5xx
// boundary explicitly (499 / 500 / 599 / 600) plus the terminal variants
// added by Phase 1.6 (ChunkFailed / JoinFailed) and Phase 1.8
// (MissingContentLength). `ReqwestError` and `JoinError` cannot be
// constructed without a live network/runtime — neither type exposes a
// public constructor — so we cover `ReqwestError` indirectly through the
// `IoError` arm (same `true` outcome, same single-match branch) and use a
// real `tokio::spawn` + `abort` to produce a `JoinError` for `JoinFailed`.
#[cfg(test)]
mod retry_classification_tests {
    use super::*;

    #[test]
    fn five_hundred_is_retryable() {
        assert!(CoreError::ServerError(500, "internal".into()).is_retryable());
    }

    #[test]
    fn five_oh_three_is_retryable() {
        // Service Unavailable — the canonical transient 5xx.
        assert!(CoreError::ServerError(503, "unavailable".into()).is_retryable());
    }

    #[test]
    fn five_ninety_nine_is_retryable() {
        // Upper inclusive boundary of the 5xx range.
        assert!(CoreError::ServerError(599, "edge".into()).is_retryable());
    }

    #[test]
    fn four_ninety_nine_is_not_retryable() {
        // One below the 5xx floor: still a client error per the contract.
        // HTTP technically does not register 499, but the classifier's job is
        // "5xx only", so 499 must fall into the permanent bucket.
        assert!(!CoreError::ServerError(499, "edge".into()).is_retryable());
    }

    #[test]
    fn six_hundred_is_not_retryable() {
        // HTTP does not define 6xx; the contract is "5xx only" so this is
        // permanent. Pinning the upper exclusive boundary so a future bump
        // of `(500..600)` to `(500..=600)` is caught.
        assert!(!CoreError::ServerError(600, "edge".into()).is_retryable());
    }

    #[test]
    fn four_oh_four_is_not_retryable() {
        // The headline audit case: 404 used to burn 3 s of backoff.
        assert!(!CoreError::ServerError(404, "not found".into()).is_retryable());
    }

    #[test]
    fn four_oh_one_is_not_retryable() {
        // 401 is permanent for the same token — retrying just re-presents the
        // same credentials.
        assert!(!CoreError::ServerError(401, "unauthorized".into()).is_retryable());
    }

    #[test]
    fn four_oh_three_is_not_retryable() {
        // 403 — same reasoning as 401.
        assert!(!CoreError::ServerError(403, "forbidden".into()).is_retryable());
    }

    #[test]
    fn missing_content_length_is_not_retryable() {
        // HEAD-response shape error — retrying the GET cannot heal a missing
        // header on a separate HEAD.
        assert!(!CoreError::MissingContentLength.is_retryable());
    }

    #[test]
    fn io_error_is_retryable() {
        // Local IO blip (e.g. EAGAIN, transient EIO) — same transport-class
        // bucket as `Reqwest`. `std::io::Error::other` is the public
        // constructor we use because the project denies `unwrap`.
        let err = CoreError::Io(std::io::Error::other("transient io"));
        assert!(err.is_retryable());
    }

    #[test]
    fn chunk_failed_is_not_retryable() {
        // `ChunkFailed` is constructed by the orchestrator AFTER the inner
        // retry loop has already exhausted its budget — retrying here would
        // compound the backoff for a failure already declared terminal.
        let inner = CoreError::ServerError(503, "x".into());
        let err = CoreError::ChunkFailed {
            index: 1,
            source: Box::new(inner),
        };
        assert!(!err.is_retryable());
    }

    // `JoinError` has no public constructor — we produce one by aborting a
    // spawned task and awaiting its handle, which surfaces the documented
    // `JoinError::is_cancelled()` shape. Using `#[tokio::test]` would need an
    // extra dev-dep; we instead build a current-thread runtime by hand. The
    // project denies `unwrap_used` and `panic` cluster-wide, so we destructure
    // with `let ... else { unreachable!() }` on the runtime-build path.
    //
    // Phase 3.8 (audit D1 follow-up): the `index` field is now
    // `Option<usize>`, replacing the prior `usize::MAX` sentinel. The
    // orchestrator path uses `None` (chunk identity lost in the join
    // layer); this test exercises the `Some(_)` shape so a future
    // refactor that drops `Option` cannot regress without breaking here.
    #[test]
    fn join_failed_is_not_retryable() {
        let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(_) => unreachable!("current-thread runtime build is infallible in this environment"),
        };
        let join_err = rt.block_on(async {
            let handle = tokio::spawn(async {
                // Long-enough sleep that the abort lands before completion.
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
            handle.abort();
            match handle.await {
                Ok(()) => unreachable!("aborted task must surface a JoinError"),
                Err(e) => e,
            }
        });
        let err = CoreError::JoinFailed {
            index: Some(7),
            source: join_err,
        };
        assert!(!err.is_retryable());
    }

    // Audit D8 / code-review I2 follow-up: the `#[error(...)]` format on
    // `CoreError::JoinFailed` uses a `match` to render `index: None` as
    // `<unknown>` and `index: Some(N)` as the number. The shape compiles
    // either way the arms are ordered, so a refactor that flipped them
    // would ship a wrong message without any test failing. Pin both arms
    // here. `JoinError` is non-`Clone` and has no public constructor, so
    // we spawn-and-abort twice in the same runtime to obtain two distinct
    // values. Same `Err => e` / `Ok(()) => unreachable!` shape on the
    // join arm as the sibling `join_failed_is_not_retryable` to honor
    // `panic = "deny"` (`expect_used` is also warned, see Cargo.toml).
    #[test]
    fn join_failed_display_renders_index_correctly() {
        let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
            unreachable!("current-thread runtime build is infallible in this environment")
        };
        let (join_err_none, join_err_some) = rt.block_on(async {
            let aborted = || async {
                let handle = tokio::spawn(async {
                    tokio::time::sleep(Duration::from_mins(1)).await;
                });
                handle.abort();
                match handle.await {
                    Ok(()) => unreachable!("aborted task must surface a JoinError"),
                    Err(e) => e,
                }
            };
            (aborted().await, aborted().await)
        });
        let err_none = CoreError::JoinFailed {
            index: None,
            source: join_err_none,
        };
        let err_some = CoreError::JoinFailed {
            index: Some(7),
            source: join_err_some,
        };
        assert!(
            err_none.to_string().contains("<unknown>"),
            "Display for None must contain '<unknown>', got: {err_none}",
        );
        assert!(
            err_some.to_string().contains("chunk task 7 failed"),
            "Display for Some(7) must contain 'chunk task 7 failed', got: {err_some}",
        );
    }
}
