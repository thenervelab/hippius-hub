//! The `ChunkedDownloader` orchestration: content-length resolution, the
//! bounded chunk fan-out over Range GETs, and the per-chunk task machinery
//! (retry loop + streaming write-to-offset).

use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::fs::OpenOptions;
// `AsyncReadExt` was used by the old in-tokio sha256 loop; Phase 2.8
// moved that work onto `spawn_blocking` with the sync `std::io::Read`
// trait inside `compute_sha256`, so the async-read trait is no longer
// needed at module scope.
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio::sync::Semaphore;

use crate::chunked_downloader::plan::{
    chunk_bounds, num_chunks, require_acceptable_status, require_content_range_matches,
};
use crate::chunked_downloader::verify::{record_verify_route, resolve_verified_digest};
use crate::error::CoreError;
use crate::incremental_hash::{spawn_incremental_hasher, HashSignal};
use crate::transport::CHUNK_REQUEST_TIMEOUT;
use std::sync::mpsc::Sender;

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB default
const MAX_RETRIES: u32 = 3;
/// In-flight cap for the legacy Range downloader's eager-spawned chunk tasks, so a
/// small caller-set `HIPPIUS_CHUNK_SIZE` on a huge file can't open O(file/chunk)
/// connections at once. 32 mirrors the pack path's default concurrency.
const MAX_INFLIGHT_CHUNKS: usize = 32;

/// Read/total request budget for the size-probe HEAD (audit L-HEAD-TIMEOUT). The
/// shared `download_client` sets only `connect_timeout(30s)`, which covers the
/// handshake but not a peer that completes it then never sends response headers -
/// `req.send().await` on the HEAD would otherwise hang indefinitely. A HEAD has no
/// body, so a tight bound is safe.
const HEAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global cap on Range GETs in flight across ALL concurrent legacy
/// downloads (audit M-RANGE-GATE). The per-file `permits` semaphore below bounds
/// ONE file's chunks; without a global gate a snapshot `ThreadPoolExecutor`
/// running N files each opens up to `MAX_INFLIGHT_CHUNKS` connections, so
/// `N * 32` GETs hit the registry at once - FD/ephemeral-port pressure and per-IP
/// 429 storms - while `pool_max_idle_per_host(32)` retains only 32 for reuse.
/// First-caller-wins fixed sizing mirrors `chunk_fetcher::global_pack_gate`: a
/// single large file still gets the full 32; N files SHARE that budget instead of
/// multiplying it.
fn global_range_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(GATE.get_or_init(|| Arc::new(Semaphore::new(MAX_INFLIGHT_CHUNKS))))
}

// Phase 3.8 (audit D8): the local DownloadError + UploadError enums
// were unified into `crate::error::CoreError`. The old enum had no
// `Display`/`Error`/`source()` impl, so Python callers saw flattened
// Debug output; the thiserror-derived replacement preserves the cause
// chain through `core_err_to_py`.

pub struct ChunkedDownloader {
    client: Client,
    // `Arc<str>` (not `String`): the URL and token are captured by every spawned
    // chunk task, so per-chunk clones are pointer bumps instead of heap copies
    // (Task C2). Converted once from the constructor's owned `String`s.
    url: Arc<str>,
    auth_token: Option<Arc<str>>,
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
            url: url.into(),
            auth_token: auth_token.map(Arc::from),
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
    /// "verification skipped" a value the type system carries - pyo3 maps
    /// it to Python `Optional[str]`, and callers dispatch on `is None`
    /// instead of comparing against the empty string. The empty-file
    /// branch still returns `Some(sha256_of_empty_bytes)` because the
    /// file exists and has a defined (non-skipped) digest.
    pub async fn download(
        &self,
        dest_path: &Path,
        verify_hash: bool,
    ) -> Result<Option<String>, CoreError> {
        // 1. Total blob size: use the manifest-supplied size when Python passed it
        //    (the common path), else HEAD for Content-Length. Skipping the HEAD
        //    removes one control-plane RTT per plain-file download - meaningful for
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

        let pb = download_progress_bar(content_length);

        let num_chunks = num_chunks(content_length, self.chunk_size);

        // A non-empty file that yields zero chunks means chunk_size == 0 (num_chunks
        // returns 0 to dodge a div-by-zero). Left unguarded, the spawn loop below
        // runs zero times, the set_len pre-allocation stands, and this returns
        // Ok(None) over an all-zero file - which the Python layer would then rename
        // into the content-addressed cache under the trusted sha256 name. Fail loudly
        // instead: chunk_size == 0 is an invalid argument, not an empty download.
        if content_length > 0 && num_chunks == 0 {
            return Err(CoreError::InvalidArgument(format!(
                "chunk_size 0 yields no chunks for a {content_length}-byte file; \
                 a zero HIPPIUS_CHUNK_SIZE would silently write an all-zero file"
            )));
        }

        // 2. Prepare the destination directory and pre-allocate the final file.
        prepare_destination(dest_path, content_length).await?;

        // Overlap whole-file verification with the download (Task C1, mirroring the
        // pack path in `chunk_fetcher::PackAssembler::assemble`): the background
        // hasher folds each completed chunk's extent in offset order while later
        // chunks are still in flight, so the verify pass normally costs no second
        // sequential read. Best-effort by contract - it yields `None` if it cannot
        // cover the file, and `resolve_verified_digest` then falls back to the full
        // re-read, so correctness never depends on this fast path. `(None, None)`
        // when `verify_hash` is false, making the fan-out below uniform either way.
        let (hash_tx, hasher_task) =
            spawn_incremental_hasher(dest_path, content_length, verify_hash);

        // 3. Launch concurrent downloads - each streams directly to its
        //    correct offset in the final file. The shared per-task captures live
        //    in a `ChunkTaskContext`; `run_chunk_fanout` owns the bounded spawn
        //    window and the join-drain loop (audits L13 / D4 / M-RANGE-GATE).
        let ctx = ChunkTaskContext {
            client: self.client.clone(),
            url: Arc::clone(&self.url),
            token: self.auth_token.clone(),
            // `Arc<Path>` built once so every chunk task's capture is a pointer
            // bump, not a fresh `PathBuf` heap copy (Task C2).
            dest: Arc::from(dest_path),
            pb: pb.clone(),
            content_length,
            chunk_size: self.chunk_size,
        };
        run_chunk_fanout(&ctx, num_chunks, hash_tx.as_ref()).await?;

        // Close the hash channel so the hasher task finalizes (or bails with
        // `None`). On the error path the `?` above returns and `hash_tx` drops
        // with the stack frame, which likewise closes the channel: the blocking
        // hasher task (unabortable by design) then exits promptly instead of
        // leaking on `recv` - the same sender-drop lifecycle the pack path relies
        // on when its aborted pack tasks drop their sender clones.
        drop(hash_tx);

        pb.finish_with_message("Download complete");

        // 4. Optional SHA256: prefer the digest the background hasher folded
        //    together during the download; fall back to the single sequential
        //    read-pass when the hasher could not cover the file.
        if verify_hash {
            let (hash, route) =
                resolve_verified_digest(hasher_task, dest_path, content_length).await?;
            record_verify_route(route);
            Ok(Some(hash))
        } else {
            // Audit L6: typed "skipped" - was `Ok(String::new())` before
            // Phase 3.12. `None` is the discriminant, not a magic value.
            Ok(None)
        }
    }

    /// Issue a HEAD request to obtain Content-Length
    async fn get_content_length(&self) -> Result<u64, CoreError> {
        // `&*self.url` re-borrows the `Arc<str>` as the `&str` that `IntoUrl` wants.
        let mut req = self.client.head(&*self.url).timeout(HEAD_REQUEST_TIMEOUT);
        if let Some(ref token) = self.auth_token {
            req = req.bearer_auth(token);
        }

        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(CoreError::ServerError(
                res.status().as_u16(),
                format!("Failed HEAD request: {:?}", res.status()),
            ));
        }

        // Audit D3: a missing/unparseable Content-Length previously fell through
        // to `unwrap_or(0)`, which `download()` then routed into `create_empty_file`
        // - silently truncating the destination and returning sha256 of empty.
        // We now surface a typed error; the empty-file path in `download()` is
        // reached only when the server explicitly sent `Content-Length: 0`.
        let content_length = res
            .headers()
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

/// Progress bar for the download phase. The template string is a compile-time
/// string literal - `indicatif` only returns `Err` here for malformed format
/// directives, which we control.
fn download_progress_bar(content_length: u64) -> ProgressBar {
    let pb = ProgressBar::new(content_length);
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .expect("indicatif template is static and infallible")
        .progress_chars("#>-"));
    pb.set_message("Downloading");
    pb
}

/// Create the destination's parent directory and pre-allocate the final file at
/// the exact size (sparse OK). Each chunk task opens its own file handle and
/// seeks to its offset; concurrent writes via distinct handles to disjoint
/// ranges are OS-safe (each handle has its own file pointer).
async fn prepare_destination(dest_path: &Path, content_length: u64) -> Result<(), CoreError> {
    let parent_dir = dest_path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent_dir).await?;

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
    // discarded anyway - a crash re-downloads (the dest opens `truncate`).
    Ok(())
}

/// The per-download state every spawned chunk task captures, built once by
/// `download` so each spawn is a set of `Arc` pointer bumps (Task C2), not heap
/// copies (`Client`/`ProgressBar` are Arc-backed handles too).
struct ChunkTaskContext {
    client: Client,
    url: Arc<str>,
    token: Option<Arc<str>>,
    dest: Arc<Path>,
    pb: ProgressBar,
    content_length: u64,
    chunk_size: u64,
}

impl ChunkTaskContext {
    /// Spawn the download task for chunk `i` into `set`.
    fn spawn_chunk(
        &self,
        set: &mut tokio::task::JoinSet<(usize, Result<(), CoreError>)>,
        i: usize,
    ) {
        let (start, end) = chunk_bounds(self.content_length, self.chunk_size, i);
        let client = self.client.clone();
        let url = Arc::clone(&self.url);
        let token = self.token.clone();
        let chunk_pb = self.pb.clone();
        let path = Arc::clone(&self.dest);
        let content_length = self.content_length;
        set.spawn(async move {
            // Global permit (RAII-released on completion or abort) bounds TOTAL
            // in-flight Range GETs across every concurrent legacy download so a
            // snapshot fan-out cannot open workers x MAX_INFLIGHT_CHUNKS at once.
            let _global_permit = match global_range_gate().acquire_owned().await {
                Ok(p) => p,
                Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
            };
            let res = download_chunk_with_retry(
                client,
                url,
                token,
                start,
                end,
                content_length,
                i,
                path,
                chunk_pb,
            )
            .await;
            (i, res)
        });
    }
}

/// Bounded-window fan-out over all `num_chunks` chunk tasks (audit L13): keep at
/// most `MAX_INFLIGHT_CHUNKS` tasks live and spawn the next only as one lands,
/// instead of eager-spawning one task per chunk (which, for a huge file under a
/// small `HIPPIUS_CHUNK_SIZE`, meant `O(file/chunk)` live tasks plus an
/// `O(num_chunks)` abort-handle Vec - `pool_max_idle_per_host` caps only IDLE
/// connections, not in-flight GETs). A `JoinSet` owns the live tasks; dropping
/// it on an early error return ABORTS every survivor (audit D4 - a detached
/// `tokio::spawn` would keep writing to the destination and holding sockets
/// after we bubbled the error up), so no manual `AbortHandle` bookkeeping is
/// needed. The window is this file's in-flight cap; `global_range_gate`
/// (audit M-RANGE-GATE) still bounds TOTAL Range GETs across every concurrent
/// legacy download.
async fn run_chunk_fanout(
    ctx: &ChunkTaskContext,
    num_chunks: usize,
    hash_tx: Option<&Sender<HashSignal>>,
) -> Result<(), CoreError> {
    let mut set: tokio::task::JoinSet<(usize, Result<(), CoreError>)> = tokio::task::JoinSet::new();

    // Prime the window; the drain loop below refills it one-for-one.
    let mut next = 0usize;
    while next < num_chunks && set.len() < MAX_INFLIGHT_CHUNKS {
        ctx.spawn_chunk(&mut set, next);
        next += 1;
    }

    // Drain the JoinSet, refilling the window as each chunk lands. Exhaustive
    // match preserves both the spawn-side (`JoinError`) and the download-layer
    // cause: previously both collapsed into a bare `ChunkFailed(usize)`, hiding
    // which chunk failed AND why. `JoinFailed.index` is `None` because the chunk
    // index lives inside the future's return tuple, and a `JoinError` escaping
    // before the tuple is built has lost that identity (`Display` -> `<unknown>`).
    //
    // On any error we return immediately; dropping `set` aborts every still-
    // running task (audit D4) so no survivor keeps writing to the destination.
    while let Some(joined) = set.join_next().await {
        match joined {
            Err(join_err) => {
                return Err(CoreError::JoinFailed {
                    index: None,
                    source: join_err,
                });
            }
            Ok((i, Err(chunk_err))) => {
                return Err(CoreError::ChunkFailed {
                    index: i,
                    source: Box::new(chunk_err),
                });
            }
            Ok((i, Ok(()))) => {
                // Signal the chunk's extent to the hasher ONLY here, on final
                // success: the retry loop lives inside the task, so `Ok` means
                // the write+flush landed and the exact-length check passed -
                // a retried chunk can never double-count. Sent from this drain
                // loop rather than inside each task so ONE sender suffices (no
                // per-task clones) and the extent is recomputed from the same
                // bounds math that formed the Range request: `chunk_bounds`
                // tiles [0, content_length) exactly (coverage/contiguity/span
                // proptests in `plan`), which IS the hasher's coverage invariant.
                if let Some(tx) = hash_tx {
                    let (start, end) = chunk_bounds(ctx.content_length, ctx.chunk_size, i);
                    // A closed channel means the hasher already exited; losing
                    // the signal merely forgoes the fast path (re-read fallback).
                    let _ = tx.send(vec![(start, end - start + 1)]);
                }
                if next < num_chunks {
                    ctx.spawn_chunk(&mut set, next);
                    next += 1;
                }
            }
        }
    }
    Ok(())
}

// Audit D5 retry classification moved to `CoreError::is_retryable` in
// `src/error.rs` (Phase 3.11). The uploader needs the same classifier,
// and a method on the error type is the single source of truth - no
// duplicate `fn` to drift, no `pub(crate)` import to maintain. See
// `CoreError::is_retryable` for the variant-by-variant rationale.

/// Wrapper with exponential-backoff retry for a single chunk download.
///
/// The eight parameters are the data captured by `tokio::spawn` for one
/// chunk task: the reqwest client + URL + bearer token (`Arc`-shared per
/// chunk so the spawn body is `'static` without heap copies - Task C2),
/// the inclusive byte range, the destination path (each chunk writes its
/// own slice), the progress bar handle, and a chunk index reserved for
/// future error reporting. Bundling into a struct would require an extra
/// clone per chunk for no readability gain.
#[expect(
    clippy::too_many_arguments,
    reason = "spawn-captured chunk state; bundling into a struct adds a clone per chunk"
)]
async fn download_chunk_with_retry(
    client: Client,
    url: Arc<str>,
    token: Option<Arc<str>>,
    start: u64,
    end: u64,
    content_length: u64,
    _chunk_index: usize,
    dest_path: Arc<Path>,
    pb: ProgressBar,
) -> Result<(), CoreError> {
    let mut retries = 0;

    loop {
        match try_download_chunk_to_offset(
            &client,
            &url,
            token.as_deref(),
            start,
            end,
            content_length,
            &dest_path,
            &pb,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                // Audit D5: fail fast on permanent errors. The
                // `is_retryable` method borrows `&self` so the owned `e`
                // remains returnable below - it only inspects the
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
    // 100 MB `DEFAULT_CHUNK_SIZE` (~ 333 KB/s floor before timing out) - enough
    // rope for slow mobile uplinks, tight enough that a stuck chunk cannot hang
    // the runtime forever. `RequestBuilder::timeout` overrides any client-level
    // value per the reqwest 0.12 docs; we keep it per-request so other client
    // uses (e.g. the HEAD in `get_content_length`) pick their own budget.
    let mut req = client
        .get(url)
        .header(header::RANGE, format!("bytes={start}-{end}"))
        .timeout(CHUNK_REQUEST_TIMEOUT); // audit D6 - see `crate::transport`

    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let mut res = req.send().await?;

    let status = res.status();
    require_acceptable_status(status, start, end, content_length)?;
    // Audit L1: a 206 must cover exactly the requested range - a range-aliasing
    // proxy can return a length-correct 206 for the WRONG offset, silently
    // corrupting the file. A whole-file 200 (audit L5) carries no Content-Range,
    // so validate it only for a 206.
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        require_content_range_matches(res.headers(), start, end)?;
    }

    // Open this task's own handle on the pre-allocated final file, seek to start.
    let mut file = OpenOptions::new().write(true).open(dest_path).await?;
    file.seek(SeekFrom::Start(start)).await?;

    // Wrap the file handle in a 2MB BufWriter to avoid thousands of small unbuffered write syscalls
    let mut buf_writer = BufWriter::with_capacity(2 * 1024 * 1024, file);

    // Stream HTTP body chunks directly to disk at our position.
    // No temp file, no assembly phase.
    let expected = end - start + 1;
    let mut written: u64 = 0;
    let mut over_range = false;
    // Each body read is bounded by the default-on read-idle window (audit M4): a peer
    // that dribbles the head then stalls mid-body is cut as a retryable ReadStall,
    // rather than held open until the per-chunk 5-minute total timeout.
    loop {
        match crate::chunk_fetcher::read_chunk_bounded(
            &mut res,
            crate::chunk_fetcher::download_read_idle(),
        )
        .await
        {
            Ok(Some(buf)) => {
                // Bound each write to the bytes still owed for this range (audit
                // M-SHORT206 follow-up): a 206 whose body RUNS PAST the requested
                // range would otherwise spill its surplus into the adjacent chunk's
                // region - the concurrent tasks write disjoint ranges of one shared
                // pre-allocated file. Write at most the remaining bytes and flag the
                // over-send; the surplus is dropped, never written.
                let remaining = expected - written;
                // `remaining` (u64) may exceed usize on a 32-bit target; when it
                // does, `buf.len()` is certainly the smaller bound, so fall back to
                // it - no truncating cast either way.
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
            Err(e) => return Err(e),
        }
    }

    buf_writer.flush().await?;

    // Audit M-SHORT206: a 206 whose body is SHORTER than the requested range (an
    // internally-consistent short sub-range: matching Content-Length + a sub-range
    // Content-Range, as a proxy/CDN can emit) reaches a clean EOF here - `chunk()`
    // returns `Ok(None)` and hyper raises no incomplete-body error because the body
    // matched its own advertised length. Without a length check the chunk's tail
    // stays as the file's pre-allocated `set_len` zeros: a silently truncated file
    // cached forever under the trusted content digest. An OVER-length body (bounded
    // above) is likewise anomalous. Both surface retryable - the over-send as a
    // `BadResponse` (protocol-contract violation, matching the pack path's
    // over-send handling in `chunk_fetcher::fetch_pack`), the short body as an
    // `Io(UnexpectedEof)` - so a transient anomaly re-fetches before failing hard.
    // `require_acceptable_status` already rejects a range-ignored 200; these close
    // the short/long-206 cases it cannot see.
    if over_range {
        return Err(CoreError::BadResponse(format!(
            "chunk bytes={start}-{end}: server sent more than the {expected}-byte range"
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

    // Regression for audit D3: pin the variant shape so the missing-header
    // path cannot silently revert to `Ok(0)`. The assertion is intentionally
    // minimal - the contract here is "there is a distinct variant for this
    // case", not "the variant carries field X". Phase 3.8 wired this variant
    // through the thiserror-based `CoreError` hierarchy.
    #[test]
    fn missing_content_length_is_a_distinct_error() {
        let err = CoreError::MissingContentLength;
        assert!(matches!(err, CoreError::MissingContentLength));
    }

    // Audit L6 (Phase 3.12): pin that `ChunkedDownloader::download` returns
    // `Result<Option<String>, CoreError>`, not `Result<String, CoreError>`.
    // The shape is the contract - a refactor that re-flattened it would
    // silently re-introduce the empty-string sentinel and the Python
    // caller's `is not None` dispatch would start routing every download
    // through the manifest-digest fallback. Binding the method as a typed
    // function pointer is the cheapest compile-time pin: a return-type
    // change here surfaces as a coercion error at the binding, not as
    // confused behaviour deep in the call stack. Same pattern as the
    // `JoinFailed` constructor pin in `error::tests` - coerce a closure
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
}

// Audit M-SHORT206: a 206 whose body is shorter than the requested range must be
// rejected (retryable), never written as a zero-padded truncation. Kept in its own
// module - it drives a real HTTP/1 socket, distinct from the pure status/arith tests.
#[cfg(test)]
mod short_206_tests {
    use super::*;
    // `AsyncWriteExt` is already in scope via `super::*`; only the read half is new.
    use tokio::io::AsyncReadExt as _;

    /// Serve exactly one `206 Partial Content` whose Content-Length (and body) is
    /// `body_len`, while the Content-Range claims the full `range_len`-byte range -
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
        // A DISTINCT path per call, keyed on a monotonic counter - not the size -
        // so two tests that pre-allocate the same size never share a temp file. The
        // per-size path raced under the parallel test runner (one test's final
        // remove_file unlinked the other's dest mid-download): a CI flake this fixes.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("hippius_short206_{}_{seq}.bin", std::process::id()));
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .ok()?;
        f.set_len(size).await.ok()?;
        Some(path)
    }

    #[tokio::test]
    async fn short_206_is_rejected_not_silently_truncated() {
        // Request 100 bytes; the server sends a consistent 50-byte 206 (matching
        // Content-Length, so hyper sees a clean EOF and raises nothing). The
        // byte-count guard must surface a retryable Io error instead of leaving the
        // chunk's tail as pre-allocated zeros.
        let Some(base) = serve_short_206(100, 50).await.ok() else {
            return;
        };
        let Ok(client) = crate::chunk_fetcher::download_client(
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            return;
        };
        let Some(dest) = prealloc(100).await else {
            return;
        };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(
            client,
            &format!("{base}/blob"),
            None,
            0,
            99,
            100,
            &dest,
            &pb,
        )
        .await;
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
        let Some(base) = serve_short_206(100, 100).await.ok() else {
            return;
        };
        let Ok(client) = crate::chunk_fetcher::download_client(
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            return;
        };
        let Some(dest) = prealloc(100).await else {
            return;
        };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(
            client,
            &format!("{base}/blob"),
            None,
            0,
            99,
            100,
            &dest,
            &pb,
        )
        .await;
        let _ = std::fs::remove_file(&dest);
        assert!(res.is_ok(), "full-length 206 must be accepted, got {res:?}");
    }

    #[tokio::test]
    async fn over_length_206_is_rejected_without_clobbering() {
        // Server sends 150 bytes for a 100-byte range. The write must be bounded to
        // 100 bytes - bytes [100, 200) of the shared file (the next chunk's region)
        // must stay the pre-allocated zeros, not the surplus - and the over-send must
        // surface as an error rather than a silent cross-chunk corruption.
        let Some(base) = serve_short_206(100, 150).await.ok() else {
            return;
        };
        let Ok(client) = crate::chunk_fetcher::download_client(
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            return;
        };
        let Some(dest) = prealloc(200).await else {
            return;
        };
        let pb = ProgressBar::hidden();
        let res = try_download_chunk_to_offset(
            client,
            &format!("{base}/blob"),
            None,
            0,
            99,
            100,
            &dest,
            &pb,
        )
        .await;
        let tail_is_zero =
            std::fs::read(&dest).is_ok_and(|b| b.len() == 200 && b[100..].iter().all(|&x| x == 0));
        let _ = std::fs::remove_file(&dest);
        assert!(
            matches!(res, Err(CoreError::BadResponse(_))),
            "over-length 206 must error as retryable BadResponse, got {res:?}"
        );
        assert!(
            tail_is_zero,
            "surplus bytes must not clobber the neighbouring chunk region"
        );
    }
}

// Audit D5: pin the retry-classification contract. Tests cover the 4xx/5xx
// boundary explicitly (499 / 500 / 599 / 600) plus the terminal variants
// added by Phase 1.6 (ChunkFailed / JoinFailed) and Phase 1.8
// (MissingContentLength). `ReqwestError` and `JoinError` cannot be
// constructed without a live network/runtime - neither type exposes a
// public constructor - so we cover `ReqwestError` indirectly through the
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
        // Service Unavailable - the canonical transient 5xx.
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
        // 401 is permanent for the same token - retrying just re-presents the
        // same credentials.
        assert!(!CoreError::ServerError(401, "unauthorized".into()).is_retryable());
    }

    #[test]
    fn four_oh_three_is_not_retryable() {
        // 403 - same reasoning as 401.
        assert!(!CoreError::ServerError(403, "forbidden".into()).is_retryable());
    }

    #[test]
    fn four_oh_eight_is_retryable() {
        // 408 Request Timeout (RFC 9110 section 15.5.9): the request didn't complete
        // in time; resending stands a chance, so it's retryable despite being
        // a 4xx.
        assert!(CoreError::ServerError(408, "request timeout".into()).is_retryable());
    }

    #[test]
    fn four_two_nine_is_retryable() {
        // 429 Too Many Requests (RFC 6585 section 4): the canonical backpressure
        // signal Harbor emits under per-token rate limits. Backing off and
        // retrying is the correct response, not terminal failure.
        assert!(CoreError::ServerError(429, "too many requests".into()).is_retryable());
    }

    #[test]
    fn missing_content_length_is_not_retryable() {
        // HEAD-response shape error - retrying the GET cannot heal a missing
        // header on a separate HEAD.
        assert!(!CoreError::MissingContentLength.is_retryable());
    }

    #[test]
    fn io_error_is_retryable() {
        // Local IO blip (e.g. EAGAIN, transient EIO) - same transport-class
        // bucket as `Reqwest`. `std::io::Error::other` is the public
        // constructor we use because the project denies `unwrap`.
        let err = CoreError::Io(std::io::Error::other("transient io"));
        assert!(err.is_retryable());
    }

    #[test]
    fn chunk_failed_is_not_retryable() {
        // `ChunkFailed` is constructed by the orchestrator AFTER the inner
        // retry loop has already exhausted its budget - retrying here would
        // compound the backoff for a failure already declared terminal.
        let inner = CoreError::ServerError(503, "x".into());
        let err = CoreError::ChunkFailed {
            index: 1,
            source: Box::new(inner),
        };
        assert!(!err.is_retryable());
    }

    // `JoinError` has no public constructor - we produce one by aborting a
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
        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
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
        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
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

    #[tokio::test]
    async fn download_refills_window_beyond_max_inflight_chunks() {
        // Audit L13: a file with MORE than MAX_INFLIGHT_CHUNKS chunks must download
        // completely - the bounded-spawn window has to refill as chunks land, not
        // stop after the first MAX_INFLIGHT_CHUNKS. A broken refill would leave the
        // tail chunks never spawned (hang / short file); this asserts every byte
        // lands by driving 52 four-byte chunks (> the 32 window) through a real 206
        // Range server.
        let total: usize = (MAX_INFLIGHT_CHUNKS + 20) * 4; // 52 chunks of 4 bytes
        let body: Vec<u8> = (0..total)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();

        let Ok((base, server)) =
            crate::chunked_downloader::test_server::serve_ranges(body.clone()).await
        else {
            return;
        };

        let url = format!("{base}/blob");
        let Ok(dl) = ChunkedDownloader::new(
            url,
            None,
            Some(4),
            Some(total as u64),
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            unreachable!("downloader builds")
        };
        let dest = std::env::temp_dir().join(format!("hippius-l13-{}.bin", std::process::id()));
        let out = tokio::time::timeout(Duration::from_secs(20), dl.download(&dest, false)).await;
        server.abort();
        let Ok(Ok(_)) = out else {
            unreachable!("52-chunk download must complete, got {out:?}")
        };
        let Ok(got) = tokio::fs::read(&dest).await else {
            unreachable!("read dest")
        };
        assert_eq!(
            got, body,
            "every chunk past the window must have been refilled and written"
        );
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn download_rejects_zero_chunk_size_for_nonempty_file() {
        // A chunk_size of 0 for a non-empty file must fail loudly, never silently
        // produce the set_len'd all-zero file. content_length is supplied, so the
        // guard fires before any HTTP - the URL is never contacted.
        let Ok(dl) = ChunkedDownloader::new(
            "http://127.0.0.1:1/never-contacted".to_string(),
            None,
            Some(0),
            Some(4096),
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            unreachable!("downloader builds")
        };
        let dest =
            std::env::temp_dir().join(format!("hippius-zerochunk-{}.bin", std::process::id()));
        let res = dl.download(&dest, false).await;
        assert!(
            matches!(res, Err(CoreError::InvalidArgument(ref m)) if m.contains("all-zero")),
            "chunk_size 0 must reject, not write zeros; got {res:?}"
        );
        // The guard must return BEFORE the set_len pre-allocation runs, so no
        // zero-filled file is left behind.
        assert!(
            !dest.exists(),
            "no destination file may be created on the zero-chunk-size path"
        );
        let _ = std::fs::remove_file(&dest);
    }
}
