use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::OpenOptions;
// `AsyncReadExt` was used by the old in-tokio sha256 loop; Phase 2.8
// moved that work onto `spawn_blocking` with the sync `std::io::Read`
// trait inside `compute_sha256`, so the async-read trait is no longer
// needed at module scope.
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom, BufWriter};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::error::CoreError;

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB default
const MAX_RETRIES: u32 = 3;
/// In-flight cap for the legacy Range downloader's eager-spawned chunk tasks, so a
/// small caller-set `HIPPIUS_CHUNK_SIZE` on a huge file can't open O(file/chunk)
/// connections at once. 32 mirrors the pack path's default concurrency.
const MAX_INFLIGHT_CHUNKS: usize = 32;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024; // 8 MB read buffer for SHA256 verification

/// Per-chunk request timeout (audit D6).
///
/// The shared `chunk_fetcher::download_client`'s `connect_timeout(30s)` covers the
/// TCP handshake; this constant is the FULL-REQUEST budget applied
/// per chunk GET via `.timeout(...)`. A slow-loris server that
/// completes the handshake then dribbles bytes can hold a connection
/// open indefinitely without ever tripping `connect_timeout` — this
/// per-request limit forecloses that.
///
/// Held in a const (not inline) so the regression test below can
/// assert the VALUE — a self-referential `include_str!` match on
/// `.timeout(Duration::from_mins(5))` would trivially pass when the
/// assertion text itself is the only occurrence. With a const, the
/// test asserts the value; removing the const's only call site at
/// `try_download_chunk_to_offset` makes it `dead_code`, which
/// `cargo clippy -- -D warnings` already promotes to a CI failure.
/// Three-layer defense without self-reference.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

/// Read/total request budget for the size-probe HEAD (audit L-HEAD-TIMEOUT). The
/// shared `download_client` sets only `connect_timeout(30s)`, which covers the
/// handshake but not a peer that completes it then never sends response headers —
/// `req.send().await` on the HEAD would otherwise hang indefinitely. A HEAD has no
/// body, so a tight bound is safe.
const HEAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global cap on Range GETs in flight across ALL concurrent legacy
/// downloads (audit M-RANGE-GATE). The per-file `permits` semaphore below bounds
/// ONE file's chunks; without a global gate a snapshot `ThreadPoolExecutor`
/// running N files each opens up to `MAX_INFLIGHT_CHUNKS` connections, so
/// `N * 32` GETs hit the registry at once — FD/ephemeral-port pressure and per-IP
/// 429 storms — while `pool_max_idle_per_host(32)` retains only 32 for reuse.
/// First-caller-wins fixed sizing mirrors `chunk_fetcher::global_pack_gate`: a
/// single large file still gets the full 32; N files SHARE that budget instead of
/// multiplying it.
fn global_range_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(GATE.get_or_init(|| Arc::new(Semaphore::new(MAX_INFLIGHT_CHUNKS))))
}

/// Number of HTTP Range requests needed to cover `content_length` bytes when
/// each chunk is `chunk_size` bytes. Returns 0 for empty files (caller is
/// expected to handle that as a special case). Returns 0 for `chunk_size == 0`
/// to avoid a division-by-zero panic if a caller sets `HIPPIUS_CHUNK_SIZE=0` —
/// the Python layer also validates this, but defense-in-depth.
fn num_chunks(content_length: u64, chunk_size: u64) -> usize {
    if content_length == 0 || chunk_size == 0 {
        return 0;
    }
    // Integer ceiling division — `div_ceil` (stable since Rust 1.73) avoids
    // both the f64 round-trip the original code used AND the `+ chunk_size
    // - 1` overflow that the manual form would hit at `u64::MAX`.
    //
    // `try_into().unwrap_or(usize::MAX)` saturates the u64→usize conversion
    // on 32-bit targets. The downloader cannot realistically address more
    // than `usize::MAX` chunks (each chunk has its own `tokio::spawn`,
    // backing JoinHandle, and reqwest pool slot — saturating means "as
    // many chunks as the platform can spawn", not silent truncation).
    content_length.div_ceil(chunk_size).try_into().unwrap_or(usize::MAX)
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
    // Pre-known whole-file size from the OCI manifest layer descriptor
    // (byte-accurate == the blob's Content-Length), threaded from Python so the
    // plain-blob path can skip the HEAD it otherwise issues to learn the size.
    // `None` -> HEAD for Content-Length as before.
    content_length: Option<u64>,
}

impl ChunkedDownloader {
    /// Construct a new concurrent downloader.
    pub fn new(
        url: String,
        auth_token: Option<String>,
        chunk_size_bytes: Option<u64>,
        content_length: Option<u64>,
        timeouts: crate::chunk_fetcher::TransportTimeouts,
    ) -> Result<Self, CoreError> {
        // Clone the process-global download client (shared with the pack path)
        // rather than building a fresh client + empty pool per file, so connections
        // stay warm across back-to-back downloads. It is HTTP/1-only for the same
        // reason as before: with h2 reqwest multiplexes all chunks on a single TCP
        // and caps aggregate throughput at the per-connection ceiling, whereas h1
        // lets each parallel chunk get its own TCP and fan out across the available
        // bandwidth. See `chunk_fetcher::download_client`.
        let client = crate::chunk_fetcher::download_client(timeouts)?.clone();
        Ok(Self {
            client,
            url,
            auth_token,
            chunk_size: chunk_size_bytes.unwrap_or(DEFAULT_CHUNK_SIZE),
            content_length,
        })
    }

    /// Downloads the file concurrently by streaming each chunk directly to its
    /// offset in the final file (sparse pre-allocated). When `verify_hash` is
    /// true, reads the full file at the end and returns `Some(sha256_hex)`.
    /// When false, skips the verify pass and returns `None`.
    ///
    /// Audit L6 (Phase 3.12): previously this signature was
    /// `Result<String, CoreError>` and the no-verify path returned
    /// `String::new()` as an in-band sentinel. `Option<String>` makes
    /// "verification skipped" a value the type system carries — pyo3 maps
    /// it to Python `Optional[str]`, and callers dispatch on `is None`
    /// instead of comparing against the empty string. The empty-file
    /// branch still returns `Some(sha256_of_empty_bytes)` because the
    /// file exists and has a defined (non-skipped) digest.
    pub async fn download(&self, dest_path: &Path, verify_hash: bool) -> Result<Option<String>, CoreError> {
        // 1. Total blob size: use the manifest-supplied size when Python passed it
        //    (the common path), else HEAD for Content-Length. Skipping the HEAD
        //    removes one control-plane RTT per plain-file download — meaningful for
        //    the many small files in a snapshot.
        let content_length = match self.content_length {
            Some(n) => n,
            None => self.get_content_length().await?,
        };

        // Handle the empty-file case. `create_empty_file` keeps its
        // `Result<String, _>` shape because an empty file has a defined
        // sha256; the `Option` wrap lives at this orchestration layer only.
        if content_length == 0 {
            return Ok(Some(self.create_empty_file(dest_path).await?));
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
            // No `sync_all` (audit L15): the parallel chunk writers see the
            // `set_len` size through the page cache without forcing metadata to
            // disk. It only bought crash durability of the pre-allocation, which is
            // discarded anyway — a crash re-downloads (the dest opens `truncate`).
        }

        let dest_path_buf = dest_path.to_path_buf();

        // 3. Launch concurrent downloads — each streams directly to its
        //    correct offset in the final file.
        //
        // Audit D4: previously this used `buffer_unordered(32)`
        // and early-returned on the first error, but dropping the `Buffered` stream
        // does NOT cancel the `tokio::spawn`'d tasks behind it — `JoinHandle::drop`
        // detaches a tokio task, leaving it running in the background where it
        // continues writing to `dest_path` and holding sockets after we've already
        // bubbled an error up. We now collect the spawn-side `AbortHandle`s eagerly
        // and call `.abort()` on every survivor before propagating the error, so
        // the survivors stop at their next await point instead of racing the next
        // download. NOTE: this legacy path has no `Semaphore`, so it eager-spawns
        // one task per chunk and the actual concurrent-connection count IS
        // `num_chunks` — `pool_max_idle_per_host` on the shared `download_client`
        // caps only IDLE (retained) connections, NOT in-flight requests (reqwest/
        // hyper open a new connection rather than queueing an h1 request). At the
        // 100 MiB default chunk size that is a handful of tasks, but a small
        // caller-set `HIPPIUS_CHUNK_SIZE` on a huge file would open O(file/chunk)
        // connections — so the `permits` Semaphore below caps in-flight chunk GETs
        // at MAX_INFLIGHT_CHUNKS, matching the pack path's per-file bound.
        let mut joins: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<(), CoreError>)>> =
            FuturesUnordered::new();
        let mut abort_handles: Vec<AbortHandle> = Vec::with_capacity(num_chunks);
        let permits = Arc::new(Semaphore::new(MAX_INFLIGHT_CHUNKS));

        for i in 0..num_chunks {
            let (start, end) = chunk_bounds(content_length, self.chunk_size, i);

            let client = self.client.clone();
            let url = self.url.clone();
            let token = self.auth_token.clone();
            let chunk_pb = pb.clone();
            let path = dest_path_buf.clone();
            let permits = Arc::clone(&permits);

            let handle = tokio::spawn(async move {
                // Per-file permit bounds THIS file's in-flight chunks; the global
                // permit (audit M-RANGE-GATE) bounds TOTAL Range GETs across every
                // concurrent legacy download so a snapshot fan-out cannot open
                // workers × 32 connections. Both RAII-released on completion or abort.
                // Acquired per-file-then-global consistently, so no lock-order cycle.
                let _permit = match permits.acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let _global_permit = match global_range_gate().acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let res = download_chunk_with_retry(client, url, token, start, end, content_length, i, path, chunk_pb).await;
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
                Ok((_, Ok(()))) => {}
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
            Ok(Some(hash))
        } else {
            // Audit L6: typed "skipped" — was `Ok(String::new())` before
            // Phase 3.12. `None` is the discriminant, not a magic value.
            Ok(None)
        }
    }

    /// Issue a HEAD request to obtain Content-Length
    async fn get_content_length(&self) -> Result<u64, CoreError> {
        let mut req = self.client.head(&self.url).timeout(HEAD_REQUEST_TIMEOUT);
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
        // `update` accepts `impl AsRef<[u8]>`; passing the empty slice
        // directly is clearer than `&[]`.
        hasher.update([]);
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
///
/// The eight parameters are the data captured by `tokio::spawn` for one
/// chunk task: the reqwest client + URL + bearer token (cloned per chunk
/// so the spawn body is `'static`), the inclusive byte range, the
/// destination path (each chunk writes its own slice), the progress bar
/// handle, and a chunk index reserved for future error reporting.
/// Bundling into a struct would require an extra clone per chunk for no
/// readability gain.
#[expect(
    clippy::too_many_arguments,
    reason = "spawn-captured chunk state; bundling into a struct adds a clone per chunk"
)]
async fn download_chunk_with_retry(
    client: Client,
    url: String,
    token: Option<String>,
    start: u64,
    end: u64,
    content_length: u64,
    _chunk_index: usize,
    dest_path: std::path::PathBuf,
    pb: ProgressBar,
) -> Result<(), CoreError> {
    let mut retries = 0;

    loop {
        match try_download_chunk_to_offset(&client, &url, token.as_deref(), start, end, content_length, &dest_path, &pb).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                // Audit D5: fail fast on permanent errors. The
                // `is_retryable` method borrows `&self` so the owned `e`
                // remains returnable below — it only inspects the
                // discriminant and (for `ServerError`) the status code.
                if !e.is_retryable() || retries > MAX_RETRIES {
                    return Err(e);
                }
                // Full-jitter backoff (audit L-JITTER): decorrelates the concurrent
                // chunk retries so a registry 429/503 does not trigger a lockstep
                // storm. Shared helper across the four transport retry loops.
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

/// Parse a `Content-Range: bytes START-END/TOTAL` (or `.../*`) value into its
/// `(start, end)` byte bounds. Returns `None` for anything that is not a
/// well-formed byte range — wrong unit, missing `-` or `/`, or non-numeric
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
/// (RFC 9110 §15.3.7), so an absent, unparsable, or mismatched header is treated as
/// a retryable anomaly (`Io`) that re-fetches rather than a silent write.
fn require_content_range_matches(
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
    Err(CoreError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "chunk bytes={start}-{end}: 206 Content-Range {} does not cover the requested range",
            raw.unwrap_or("<absent>")
        ),
    )))
}

/// Verify that a chunk GET produced exactly HTTP 206 Partial Content.
///
/// Audit D2: a server that ignores `Range` and returns 200 + the FULL body would,
/// if `seek(start)`-written, overwrite everything past `end + 1` and corrupt the
/// file — so a 200 is rejected, its diagnostic naming the ignored range (distinct
/// from a "wrong bytes" error). This rejects a 200 even for a single-chunk
/// whole-file request.
///
/// Audit L5: the one accepted 200 is a single-chunk small-file download whose
/// range covers the WHOLE object (`start == 0 && end == content_length - 1`) — a
/// `200 OK` with the full body is then RFC 9110 §15.3.7-legal and correct (the
/// over-length write guard already bounds a stray full body). A multi-chunk
/// download that got a range-ignored 200 still fails loudly, because its range is
/// not the whole object. A 200 carries no `Content-Range`, so the caller runs
/// [`require_content_range_matches`] only for a 206.
fn require_acceptable_status(
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

/// Streaming download of a chunk directly to its offset in the final file
/// (already pre-allocated). Each task opens its own file handle, seeks to its
/// offset, and writes bytes as they arrive from the HTTP stream.
/// Parallel writes to disjoint ranges are safe.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct per-chunk download input; content_length is needed for the whole-file-200 check (audit L5), matching download_chunk_with_retry's own expect"
)]
async fn try_download_chunk_to_offset(
    client: &Client,
    url: &str,
    token: Option<&str>,
    start: u64,
    end: u64,
    content_length: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    // Audit D6: per-request timeout on the chunk GET. The shared
    // `chunk_fetcher::download_client` sets `connect_timeout(30s)` but no
    // full-request timeout, so a slow-loris
    // server could hold a TCP open and dribble bytes indefinitely without ever
    // tripping the connect phase. 5 minutes per chunk is generous given the
    // 100 MB `DEFAULT_CHUNK_SIZE` (≈ 333 KB/s floor before timing out) — enough
    // rope for slow mobile uplinks, tight enough that a stuck chunk cannot hang
    // the runtime forever. `RequestBuilder::timeout` overrides any client-level
    // value per the reqwest 0.12 docs; we keep it per-request so other client
    // uses (e.g. the HEAD in `get_content_length`) pick their own budget.
    let mut req = client.get(url)
        .header(header::RANGE, format!("bytes={start}-{end}"))
        .timeout(CHUNK_REQUEST_TIMEOUT);  // audit D6 — see const docs

    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let mut res = req.send().await?;

    let status = res.status();
    require_acceptable_status(status, start, end, content_length)?;
    // Audit L1: a 206 must cover exactly the requested range — a range-aliasing
    // proxy can return a length-correct 206 for the WRONG offset, silently
    // corrupting the file. A whole-file 200 (audit L5) carries no Content-Range,
    // so validate it only for a 206.
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        require_content_range_matches(res.headers(), start, end)?;
    }

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
    let expected = end - start + 1;
    let mut written: u64 = 0;
    let mut over_range = false;
    loop {
        match res.chunk().await {
            Ok(Some(buf)) => {
                // Bound each write to the bytes still owed for this range (audit
                // M-SHORT206 follow-up): a 206 whose body RUNS PAST the requested
                // range would otherwise spill its surplus into the adjacent chunk's
                // region — the concurrent tasks write disjoint ranges of one shared
                // pre-allocated file. Write at most the remaining bytes and flag the
                // over-send; the surplus is dropped, never written.
                let remaining = expected - written;
                // `remaining` (u64) may exceed usize on a 32-bit target; when it
                // does, `buf.len()` is certainly the smaller bound, so fall back to
                // it — no truncating cast either way.
                let take = usize::try_from(remaining).map_or(buf.len(), |r| buf.len().min(r));
                if take > 0 {
                    buf_writer.write_all(&buf[..take]).await?;
                    written += take as u64;
                    pb.inc(take as u64);
                }
                if buf.len() as u64 > remaining {
                    over_range = true;
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        }
    }

    buf_writer.flush().await?;

    // Audit M-SHORT206: a 206 whose body is SHORTER than the requested range (an
    // internally-consistent short sub-range: matching Content-Length + a sub-range
    // Content-Range, as a proxy/CDN can emit) reaches a clean EOF here — `chunk()`
    // returns `Ok(None)` and hyper raises no incomplete-body error because the body
    // matched its own advertised length. Without a length check the chunk's tail
    // stays as the file's pre-allocated `set_len` zeros: a silently truncated file
    // cached forever under the trusted content digest. An OVER-length body (bounded
    // above) is likewise anomalous. Both surface as a retryable `CoreError::Io` so a
    // transient anomaly re-fetches before failing hard. `require_acceptable_status`
    // already rejects a range-ignored 200; these close the short/long-206
    // cases it cannot see.
    if over_range {
        return Err(CoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("chunk bytes={start}-{end}: server sent more than the {expected}-byte range"),
        )));
    }
    if written != expected {
        return Err(CoreError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("chunk bytes={start}-{end}: received {written} bytes, expected {expected}"),
        )));
    }
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

    // Regression for audit D6: pin the per-chunk request timeout on the
    // chunk GET. Asserts the VALUE of the named const (300 seconds = 5
    // minutes). The const itself is enforced by `cargo clippy -D warnings`:
    // if a future refactor removes its only call site at
    // `try_download_chunk_to_offset`, the const becomes `dead_code` and CI
    // fails before this test even runs. Two complementary guards:
    //   1. clippy: const must be used → production code must reference it
    //   2. this test: const must equal 300s → value can't drift accidentally
    //
    // An earlier shape of this test used `include_str!` to grep the file
    // for the literal timeout call. That was vacuous: the assertion text
    // itself contained the substring it was searching for, so any source
    // file containing the test passed regardless of what production did.
    // The named-const approach removes the self-reference.
    #[test]
    fn try_download_chunk_to_offset_sets_request_timeout() {
        assert_eq!(
            CHUNK_REQUEST_TIMEOUT,
            Duration::from_mins(5),
            "CHUNK_REQUEST_TIMEOUT must be 5 minutes; a slow-loris server \
             could otherwise hold a TCP connection open indefinitely after \
             a fast handshake (connect_timeout covers only the handshake).",
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

    // Audit L6 (Phase 3.12): pin that `ChunkedDownloader::download` returns
    // `Result<Option<String>, CoreError>`, not `Result<String, CoreError>`.
    // The shape is the contract — a refactor that re-flattened it would
    // silently re-introduce the empty-string sentinel and the Python
    // caller's `is not None` dispatch would start routing every download
    // through the manifest-digest fallback. Binding the method as a typed
    // function pointer is the cheapest compile-time pin: a return-type
    // change here surfaces as a coercion error at the binding, not as
    // confused behaviour deep in the call stack. Same pattern as the
    // `JoinFailed` constructor pin in `error::tests` — coerce a closure
    // to a fully-typed `fn` pointer, then exercise it with `fn_addr_eq`
    // so clippy's `no_effect_underscore_binding` lint stays satisfied.
    #[test]
    fn download_returns_option_string() {
        type DownloadFut<'a> = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<String>, CoreError>> + Send + 'a>,
        >;
        // The coercion below is the assertion: if `download` ever stops
        // returning `Result<Option<String>, _>` (e.g. reverts to `String`),
        // the binding fails to typecheck and this test fails to build.
        let typed: for<'a> fn(&'a ChunkedDownloader, &'a Path, bool) -> DownloadFut<'a> =
            |d, p, v| Box::pin(d.download(p, v));
        // Use `typed` as a value so the binding has an observed effect.
        assert!(std::ptr::fn_addr_eq(typed, typed));
    }

    // Phase 7.1 backfill: hand-picked fixtures above cover the boundary cases
    // a human would think of (off-by-one, exact multiples, one-byte file). The
    // proptest block below pins the STRUCTURAL invariants over the full u64
    // input space — the shrinker surfaces edges the author didn't enumerate.
    // Five properties together specify what `num_chunks` + `chunk_bounds`
    // MUST produce for any valid input, independent of implementation:
    //   - coverage: chunk sizes sum to content_length
    //   - contiguity: no gaps, no overlaps between consecutive chunks
    //   - full span: first chunk starts at 0, last ends at content_length - 1
    //   - chunk_size == 0 → 0 chunks (defense-in-depth no-panic guarantee)
    //   - content_length == 0 → 0 chunks (empty-file guarantee)
    // Input bounds are deliberately small enough (≤ 1 GB / 200 MB) to keep
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
            proptest::prop_assert!(n > 0, "non-empty file must have ≥1 chunk");
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

        /// `content_length == 0` returns 0 chunks (empty file → no Range GETs).
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
    // (a single-chunk small-file download) — the full body written at offset 0 is
    // then correct and RFC 9110 §15.3.7-legal.
    #[test]
    fn accepts_whole_file_200() {
        assert!(require_acceptable_status(StatusCode::OK, 0, 99, 100).is_ok());
    }

    // A range-ignored 200 on a MULTI-chunk download (the range is not the whole
    // object) still fails loudly, and the diagnostic names the ignored range — the
    // only signal distinguishing "server ignored Range" from "wrong bytes".
    // `let ... else { unreachable!() }` instead of `.unwrap_err()`/`panic!()`
    // because the project denies `unwrap_used` and `panic` cluster-wide.
    #[test]
    fn rejects_range_ignored_200_with_diagnostic() {
        // range 0-99 of a 1000-byte object is NOT the whole file → must reject.
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
        // silent-corruption case L1 defends against.
        assert!(require_content_range_matches(&cr_headers("bytes 0-99/500"), 100, 199).is_err());
    }

    #[test]
    fn content_range_absent_is_rejected() {
        assert!(require_content_range_matches(&reqwest::header::HeaderMap::new(), 0, 99).is_err());
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
        // alignment check relies on — for offsets a hand-picked fixture would miss.
        #[test]
        fn parse_content_range_round_trips(start in 0u64.., extra in 0u64..) {
            let end = start.saturating_add(extra);
            let header = format!("bytes {start}-{end}/{}", end.saturating_add(1));
            proptest::prop_assert_eq!(parse_content_range(&header), Some((start, end)));
        }
    }
}

// Audit M-SHORT206: a 206 whose body is shorter than the requested range must be
// rejected (retryable), never written as a zero-padded truncation. Kept in its own
// module — it drives a real HTTP/1 socket, distinct from the pure status/arith tests.
#[cfg(test)]
mod short_206_tests {
    use super::*;
    // `AsyncWriteExt` is already in scope via `super::*`; only the read half is new.
    use tokio::io::AsyncReadExt as _;

    /// Serve exactly one `206 Partial Content` whose Content-Length (and body) is
    /// `body_len`, while the Content-Range claims the full `range_len`-byte range —
    /// the internally-consistent short sub-range a misbehaving proxy/CDN can emit.
    /// `connection: close` so there is no keep-alive framing to parse.
    async fn serve_short_206(range_len: u64, body_len: usize) -> std::io::Result<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut req = Vec::new();
                let mut tmp = [0u8; 1024];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => req.extend_from_slice(&tmp[..n]),
                    }
                }
                let body = vec![7u8; body_len];
                let resp = format!(
                    "HTTP/1.1 206 Partial Content\r\ncontent-length: {}\r\ncontent-range: bytes 0-{}/{}\r\nconnection: close\r\n\r\n",
                    body_len,
                    range_len.saturating_sub(1),
                    range_len
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.shutdown().await;
            }
        });
        Ok(format!("http://{addr}"))
    }

    async fn prealloc(size: u64) -> Option<std::path::PathBuf> {
        use std::sync::atomic::{AtomicU64, Ordering};
        // A DISTINCT path per call, keyed on a monotonic counter — not the size —
        // so two tests that pre-allocate the same size never share a temp file. The
        // per-size path raced under the parallel test runner (one test's final
        // remove_file unlinked the other's dest mid-download): a CI flake this fixes.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hippius_short206_{}_{seq}.bin",
            std::process::id(),
        ));
        let f = OpenOptions::new().create(true).write(true).truncate(true).open(&path).await.ok()?;
        f.set_len(size).await.ok()?;
        Some(path)
    }

    #[tokio::test]
    async fn short_206_is_rejected_not_silently_truncated() {
        // Request 100 bytes; the server sends a consistent 50-byte 206 (matching
        // Content-Length, so hyper sees a clean EOF and raises nothing). The
        // byte-count guard must surface a retryable Io error instead of leaving the
        // chunk's tail as pre-allocated zeros.
        let Some(base) = serve_short_206(100, 50).await.ok() else { return };
        let Ok(client) = crate::chunk_fetcher::download_client(crate::chunk_fetcher::TransportTimeouts::default()) else { return };
        let Some(dest) = prealloc(100).await else { return };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(client, &format!("{base}/blob"), None, 0, 99, 100, &dest, &pb).await;
        let _ = std::fs::remove_file(&dest);
        assert!(
            matches!(res, Err(CoreError::Io(_))),
            "short 206 must be a retryable Io error (silent truncation guard), got {res:?}"
        );
    }

    #[tokio::test]
    async fn full_length_206_is_accepted() {
        // Control: a 206 whose body fills the requested range must succeed, so the
        // guard rejects only genuine short reads.
        let Some(base) = serve_short_206(100, 100).await.ok() else { return };
        let Ok(client) = crate::chunk_fetcher::download_client(crate::chunk_fetcher::TransportTimeouts::default()) else { return };
        let Some(dest) = prealloc(100).await else { return };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(client, &format!("{base}/blob"), None, 0, 99, 100, &dest, &pb).await;
        let _ = std::fs::remove_file(&dest);
        assert!(res.is_ok(), "full-length 206 must be accepted, got {res:?}");
    }

    #[tokio::test]
    async fn over_length_206_is_rejected_without_clobbering() {
        // Server sends 150 bytes for a 100-byte range. The write must be bounded to
        // 100 bytes — bytes [100, 200) of the shared file (the next chunk's region)
        // must stay the pre-allocated zeros, not the surplus — and the over-send must
        // surface as an error rather than a silent cross-chunk corruption.
        let Some(base) = serve_short_206(100, 150).await.ok() else { return };
        let Ok(client) = crate::chunk_fetcher::download_client(crate::chunk_fetcher::TransportTimeouts::default()) else { return };
        let Some(dest) = prealloc(200).await else { return };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(client, &format!("{base}/blob"), None, 0, 99, 100, &dest, &pb).await;
        let tail_is_zero = std::fs::read(&dest)
            .is_ok_and(|b| b.len() == 200 && b[100..].iter().all(|&x| x == 0));
        let _ = std::fs::remove_file(&dest);
        assert!(matches!(res, Err(CoreError::Io(_))), "over-length 206 must error, got {res:?}");
        assert!(tail_is_zero, "surplus bytes must not clobber the neighbouring chunk region");
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
    fn four_oh_eight_is_retryable() {
        // 408 Request Timeout (RFC 9110 §15.5.9): the request didn't complete
        // in time; resending stands a chance, so it's retryable despite being
        // a 4xx.
        assert!(CoreError::ServerError(408, "request timeout".into()).is_retryable());
    }

    #[test]
    fn four_two_nine_is_retryable() {
        // 429 Too Many Requests (RFC 6585 §4): the canonical backpressure
        // signal Harbor emits under per-token rate limits. Backing off and
        // retrying is the correct response, not terminal failure.
        assert!(CoreError::ServerError(429, "too many requests".into()).is_retryable());
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
        let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
            unreachable!("current-thread runtime build is infallible in this environment")
        };
        let join_err = rt.block_on(async {
            let handle = tokio::spawn(async {
                // Long-enough sleep that the abort lands before completion.
                tokio::time::sleep(Duration::from_mins(1)).await;
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
