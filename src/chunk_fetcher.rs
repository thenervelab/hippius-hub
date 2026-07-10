//! Parallel pull + assemble of a chunked artifact's content-defined chunk blobs.
//!
//! The chunked-artifact layout (docs/plans/2026-07-09-chunked-artifact-layout.md)
//! stores a large file as K independent, content-addressed OCI blobs. Unlike
//! `chunked_downloader.rs` — which parallelises ONE whole-file blob via HTTP
//! `Range` requests (206 slices) and is kept for pre-chunking artifacts — this
//! module fetches each chunk as its own ordinary blob (a full `200 OK`) and
//! writes it to its offset in the pre-allocated destination. Two consequences:
//! no `Range`/206 dependency (the ATS-edge 206 fragility the plan retires), and
//! each chunk verifies against its own digest as it streams, so the assembled
//! file's integrity is proven chunk-by-chunk rather than trusted.

use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::error::CoreError;

const CONNECT_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024;

/// Full-request budget for a single chunk-blob GET.
///
/// `connect_timeout` (below) covers only the TCP/TLS handshake; a slow-loris
/// server that handshakes then dribbles bytes would otherwise hold a connection
/// open forever. 5 minutes per ~64 MiB chunk is generous (a ~220 KB/s floor)
/// yet forecloses an indefinitely-stuck fetch. Held in a named const so the
/// regression test can pin the value and clippy's dead-code lint enforces its
/// call site.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

/// Idle-connection cap for the shared download client. Bounds only *idle*
/// (kept-alive) connections, not in-flight requests — the per-file `Semaphore`
/// (`PackAssembler`) and spawn count (`ChunkedDownloader`) are the real concurrency
/// bounds, so a fixed value is safe regardless of a caller's `max_concurrent`. 32
/// matches the default `max_concurrent`. This does change the pack path's idle-pool
/// sizing (previously `pool_max_idle_per_host(max_concurrent)`) to a fixed cap; a
/// caller running `HIPPIUS_MAX_CONCURRENT` above 32 keeps up to 32 warm idle
/// connections rather than `max_concurrent`, which only affects idle reuse, not the
/// real (semaphore-bounded) concurrency.
const DOWNLOAD_POOL_MAX_IDLE: usize = 32;

/// Process-global HTTP/1 client shared by both download paths (pack assembly here
/// and the legacy Range downloader). Mirrors `uploader::upload_client`: building a
/// `Client` per native call starts with an empty pool and forces a fresh
/// DNS+TCP+TLS handshake to the registry host on every file; the `OnceLock` hoists
/// construction out of the per-file path so warm connections survive across files
/// (the win for many-small-file snapshots). Auth is applied per request, so the
/// shared client carries no per-file credential across origins.
///
/// HTTP/1-only for the same reason the per-call clients were: h2 would multiplex
/// every parallel chunk onto one TCP and cap aggregate throughput at the
/// per-connection ceiling; h1 lets each chunk claim its own connection.
///
/// Construction is fallible (the TLS backend may fail to init), so this returns
/// `Result` rather than `expect`-ing inside a `get_or_init` closure — the crate
/// denies `panic`/`unwrap`. On an init race the loser's freshly built client is
/// dropped unused (RAII); `OnceLock` is valid in statics and never poisoned.
pub(crate) fn download_client() -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let built = Client::builder()
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .http1_only()
        .pool_max_idle_per_host(DOWNLOAD_POOL_MAX_IDLE)
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;
    Ok(CLIENT.get_or_init(|| built))
}

/// Process-global cap on packs in flight across ALL concurrent downloads (every
/// file in a snapshot), so the nested snapshot-workers × per-file-concurrency
/// parallelism cannot multiply resident 64 MiB pack buffers into an OOM
/// (8 workers × 32 × 64 MiB ≈ 16 GB worst case). Sized from the FIRST call's
/// `max_concurrent` (first-caller-wins, like `download_client`): in a uniform
/// snapshot every file passes the same value, so the total in-flight budget equals
/// one file's concurrency — a single large file is never throttled, and N files
/// SHARE that budget rather than each getting the full amount. Mirrors the upload
/// path's `_pack_upload_gate`.
fn global_pack_gate(max_concurrent: usize) -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(GATE.get_or_init(|| Arc::new(Semaphore::new(max_concurrent))))
}

/// Extents `(file_offset, size)` one completed pack contributes to the whole-file
/// hasher — the payload of the incremental-hash channel. One byte of the file
/// belongs to exactly one extent, so the extents across all packs tile the file.
type HashSignal = Vec<(u64, u64)>;

/// The join handle for the background incremental hasher; yields the whole-file
/// digest, or `None` when the incremental pass could not cover the file (see
/// `incremental_hash`).
type HasherTask = tokio::task::JoinHandle<Option<String>>;

/// What `spawn_incremental_hasher` hands back: the sender each pack signals
/// completion on and the task handle to await, or `(None, None)` when the caller
/// requested no whole-file digest.
type IncrementalHash = (Option<Sender<HashSignal>>, Option<HasherTask>);

/// One chunk to carve out of a fetched pack: where it sits in the pack, its size,
/// where it lands in the assembled file, and its content digest (hex, no prefix).
pub struct PackChunkTarget {
    pub offset_in_pack: u64,
    pub size: u64,
    pub file_offset: u64,
    pub expected_sha256: String,
}

/// One pack blob to fetch (a full `200 OK`) and the chunks to slice out of it.
/// A chunked-v2 file's chunks may come from several packs (new packs it wrote,
/// plus old packs it reuses by range); each pack is fetched once and every chunk
/// it holds for this file is verified and scattered to its offset.
pub struct PackPlanEntry {
    pub url: String,
    pub size: u64,
    pub chunks: Vec<PackChunkTarget>,
}

/// Parallel pull + scatter of a chunked-v2 file's pack blobs. The unit of work is
/// a pack (fetched whole, then sliced into N chunks): a bounded semaphore caps
/// concurrency, the first error aborts the whole batch, and every carved chunk is
/// digest-verified as it lands at its file offset.
pub struct PackAssembler {
    client: Client,
    auth_token: Option<String>,
    max_concurrent: usize,
}

impl PackAssembler {
    /// Clones the shared process-global `download_client` (warm pool across files);
    /// the semaphore in `assemble` — not the client's fixed idle pool — is the real
    /// concurrency bound. Fallible only on the client's first-time build.
    pub fn new(auth_token: Option<String>, max_concurrent: usize) -> Result<Self, CoreError> {
        // Clone the process-global client (an Arc-backed handle sharing one pool)
        // instead of building a fresh client + empty pool per file. `max_concurrent`
        // still bounds real concurrency via the `Semaphore` in `assemble`.
        let client = download_client()?.clone();
        Ok(Self { client, auth_token, max_concurrent: max_concurrent.max(1) })
    }

    /// Fetch every pack into `dest` (pre-allocated to `total_size`), verifying each
    /// carved chunk's digest, then the whole-file digest. `expected_file_sha256`
    /// proves chunk *ordering* across packs (the only thing per-chunk digests can't).
    pub async fn assemble(
        &self,
        dest: &Path,
        packs: &[PackPlanEntry],
        expected_file_sha256: Option<&str>,
        total_size: u64,
    ) -> Result<Option<String>, CoreError> {
        let parent = dest.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent).await?;
        {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(dest)
                .await?;
            f.set_len(total_size).await?;
            f.sync_all().await?;
        }

        let pb = ProgressBar::new(total_size);
        #[expect(clippy::expect_used, reason = "infallible static template")]
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("indicatif template is static and infallible")
                .progress_chars("#>-"),
        );
        pb.set_message("📥 Downloading packs");

        // Verify the whole-file digest incrementally, overlapped with the fetch,
        // instead of a second full read afterwards (see `spawn_incremental_hasher` /
        // `incremental_hash`). Best-effort: it falls back to a full re-read if it
        // cannot cover the file in order, so correctness never depends on it.
        let (hash_tx, hasher_task) =
            spawn_incremental_hasher(dest, total_size, expected_file_sha256.is_some());

        let mut joins: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<(), CoreError>)>> =
            FuturesUnordered::new();
        let mut abort_handles: Vec<AbortHandle> = Vec::with_capacity(packs.len());
        let permits = Arc::new(Semaphore::new(self.max_concurrent));
        let global = global_pack_gate(self.max_concurrent);

        for (i, plan) in packs.iter().enumerate() {
            let client = self.client.clone();
            let token = self.auth_token.clone();
            let url = plan.url.clone();
            let pack_size = plan.size;
            let targets: Vec<(u64, u64, u64, String)> = plan
                .chunks
                .iter()
                .map(|c| (c.offset_in_pack, c.size, c.file_offset, c.expected_sha256.clone()))
                .collect();
            let path = dest.to_path_buf();
            let pack_pb = pb.clone();
            let permits = Arc::clone(&permits);
            let global = Arc::clone(&global);
            let hash_tx = hash_tx.clone();

            let handle = tokio::spawn(async move {
                // Per-file permit bounds THIS file's concurrency; the global permit
                // bounds TOTAL packs in flight across every concurrent file (the
                // snapshot memory ceiling). Held for the whole fetch, released on drop.
                let _permit = match permits.acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let _global_permit = match global.acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let res = fetch_pack_with_retry(
                    &client, &url, token.as_deref(), pack_size, &targets, &path, &pack_pb,
                )
                .await;
                if res.is_ok() {
                    if let Some(tx) = &hash_tx {
                        // Signal the file-offset extents this pack verified+wrote, once,
                        // only AFTER the retry loop succeeded — a retried pack must not
                        // double-count. A closed channel means the hasher task already
                        // exited (error/abort), so a dropped signal merely forgoes the
                        // incremental fast path; the whole-file check then re-reads.
                        let done: HashSignal = targets.iter().map(|t| (t.2, t.1)).collect();
                        let _ = tx.send(done);
                    }
                }
                (i, res)
            });
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
        }
        // Drop the original sender so the channel closes once every pack task has
        // finished (each task holds its own clone); that unblocks the hasher task's
        // final `recv` and lets it finalize (or fall back).
        drop(hash_tx);

        while let Some(res) = joins.next().await {
            match res {
                Err(join_err) => {
                    for a in &abort_handles {
                        a.abort();
                    }
                    return Err(CoreError::JoinFailed { index: None, source: join_err });
                }
                Ok((i, Err(pack_err))) => {
                    for a in &abort_handles {
                        a.abort();
                    }
                    return Err(CoreError::ChunkFailed { index: i, source: Box::new(pack_err) });
                }
                Ok((_, Ok(()))) => {}
            }
        }
        pb.finish_with_message("✅ Packs complete");

        if let Some(expected_file) = expected_file_sha256 {
            return Ok(Some(verify_file_digest(hasher_task, dest, expected_file).await?));
        }
        Ok(None)
    }
}

/// Spawn the background incremental hasher (see `incremental_hash`) when the caller
/// asked for whole-file verification, handing back the sender packs signal
/// completion on and the task handle to await. Returns `(None, None)` when no digest
/// was requested, so the fan-out and finalize paths stay uniform either way.
fn spawn_incremental_hasher(dest: &Path, total_size: u64, verify: bool) -> IncrementalHash {
    if !verify {
        return (None, None);
    }
    let (tx, rx) = std::sync::mpsc::channel::<HashSignal>();
    let hash_path = dest.to_path_buf();
    let task = tokio::task::spawn_blocking(move || incremental_hash(&rx, &hash_path, total_size));
    (Some(tx), Some(task))
}

/// Resolve the whole-file digest once every pack has landed: prefer the digest the
/// background hasher computed (its work overlapped the download), else fall back to
/// a full re-read when the incremental pass could not cover the file in order. Both
/// hash the same on-disk bytes, so the fallback is a slower route to an identical
/// answer. A `JoinError` means the hasher task panicked — surfaced rather than
/// masked (the fn is written not to panic, so it is effectively unreachable, but a
/// silent fallback would hide a real defect). Errors on a digest mismatch, which is
/// exactly the cross-pack ordering failure this whole-file check exists to catch.
async fn verify_file_digest(
    hasher_task: Option<HasherTask>,
    dest: &Path,
    expected_file: &str,
) -> Result<String, CoreError> {
    let got = match hasher_task {
        Some(task) => match task.await {
            Ok(Some(digest)) => digest,
            Ok(None) => compute_sha256(dest).await?,
            Err(join_err) => return Err(CoreError::Io(std::io::Error::other(join_err))),
        },
        None => compute_sha256(dest).await?,
    };
    if got != expected_file {
        return Err(CoreError::Integrity(format!(
            "assembled file: expected sha256 {expected_file}, got {got}"
        )));
    }
    Ok(got)
}

async fn fetch_pack_with_retry(
    client: &Client,
    url: &str,
    token: Option<&str>,
    pack_size: u64,
    targets: &[(u64, u64, u64, String)],
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    let mut retries = 0;
    loop {
        match fetch_pack(client, url, token, pack_size, targets, dest_path, pb).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > MAX_RETRIES {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_millis(2u64.pow(retries) * 100)).await;
            }
        }
    }
}

/// Fetch one pack blob whole, verify each carved chunk's sha256, and scatter each
/// slice to its file offset. Buffering the pack (~64 MiB) is bounded by the
/// semaphore; the length check rejects a server that over-sends before slicing.
async fn fetch_pack(
    client: &Client,
    url: &str,
    token: Option<&str>,
    pack_size: u64,
    targets: &[(u64, u64, u64, String)],
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    let mut req = client.get(url).timeout(CHUNK_REQUEST_TIMEOUT);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        return Err(CoreError::ServerError(res.status().as_u16(), format!("pack GET failed for {url}")));
    }
    let bytes = res.bytes().await?;
    if bytes.len() as u64 != pack_size {
        return Err(CoreError::Integrity(format!(
            "pack {url}: expected {pack_size} bytes, got {}",
            bytes.len()
        )));
    }
    let mut file = OpenOptions::new().write(true).open(dest_path).await?;
    for (offset_in_pack, size, file_offset, expected) in targets {
        let start = usize::try_from(*offset_in_pack)
            .map_err(|_| CoreError::Integrity(format!("pack offset {offset_in_pack} exceeds usize")))?;
        let end = start
            .checked_add(usize::try_from(*size).map_err(|_| {
                CoreError::Integrity(format!("chunk size {size} exceeds usize"))
            })?)
            .ok_or_else(|| CoreError::Integrity("chunk range overflow".to_string()))?;
        if end > bytes.len() {
            return Err(CoreError::Integrity(format!(
                "pack {url}: chunk range {start}..{end} exceeds pack length {}",
                bytes.len()
            )));
        }
        let slice = &bytes[start..end];
        let got = hex::encode(Sha256::digest(slice));
        if &got != expected {
            return Err(CoreError::Integrity(format!(
                "chunk at pack offset {offset_in_pack}: expected sha256 {expected}, got {got}"
            )));
        }
        file.seek(SeekFrom::Start(*file_offset)).await?;
        file.write_all(slice).await?;
        pb.inc(*size);
    }
    file.flush().await?;
    Ok(())
}

/// SHA-256 of the assembled file in one sequential read pass on the blocking
/// pool. Same rationale as `chunked_downloader::compute_sha256`: the digest
/// loop is CPU-bound and would starve the runtime's chunk tasks if run inline.
async fn compute_sha256(path: &Path) -> Result<String, CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
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
        }
        Ok(hex::encode(hasher.finalize()))
    })
    .await
    .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?
}

/// Whole-file SHA-256 folded together incrementally from packs as they land, used
/// to prove chunk *ordering* across packs (the one property per-chunk digests
/// cannot). Runs on the blocking pool for the same reason as `compute_sha256`: the
/// digest loop is CPU-bound and would starve the runtime's fetch tasks inline.
///
/// Each `recv` carries the `(file_offset, size)` extents one completed pack wrote.
/// `pending` (keyed by start offset) is the reorder buffer for out-of-order packs;
/// it holds only metadata — the bytes are already on disk — so it stays a few bytes
/// per chunk and can never grow to the file size the way an in-memory byte reorder
/// buffer would. `watermark` is the end of the region contiguously covered from
/// offset 0; it advances only when the extent starting exactly at the watermark has
/// arrived. Whenever the watermark moves past `hashed`, the newly-contiguous span is
/// read straight from the just-written (page-cache-warm) file and folded into the
/// hasher, in strict offset order. Reads and the concurrent pack writes never touch
/// the same bytes (each byte is written once and flushed before its extent is
/// signalled), so the separate read handle is page-cache-coherent without locking.
///
/// Best-effort by contract: returns `Some(digest)` ONLY after consuming exactly
/// `[0, total_size)` in order; any shortfall (channel closing early on an abort, a
/// coverage gap, or a read error) yields `None` so the caller re-reads. It therefore
/// hashes the identical bytes `compute_sha256` would and can only ever be a faster
/// route to the same digest — never a different verdict.
fn incremental_hash(rx: &Receiver<HashSignal>, path: &Path, total_size: u64) -> Option<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; VERIFY_READ_BUFFER];
    let mut pending: HashMap<u64, u64> = HashMap::new();
    // Invariant: `hashed` == the file read position == bytes folded into `hasher`,
    // and `hashed <= watermark <= total_size` holds throughout.
    let mut watermark: u64 = 0;
    let mut hashed: u64 = 0;

    while let Ok(extents) = rx.recv() {
        for (start, size) in extents {
            pending.insert(start, size);
        }
        while let Some(size) = pending.remove(&watermark) {
            watermark = watermark.checked_add(size)?;
        }
        while hashed < watermark {
            let want = usize::try_from((watermark - hashed).min(buf.len() as u64)).ok()?;
            let n = file.read(&mut buf[..want]).ok()?;
            if n == 0 {
                return None; // file shorter than the extents claimed — give up, re-read
            }
            hasher.update(&buf[..n]);
            hashed += n as u64;
        }
    }

    // Channel closed: every pack task has finished. A complete, correctly-ordered
    // set covers exactly the whole file; anything less falls back to the re-read.
    if hashed == total_size {
        Some(hex::encode(hasher.finalize()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pin the per-chunk request timeout value (same guard shape as the legacy
    // path): clippy's dead-code lint enforces the const is USED at the call
    // site; this test enforces its VALUE can't silently drift.
    #[test]
    fn chunk_request_timeout_is_five_minutes() {
        assert_eq!(CHUNK_REQUEST_TIMEOUT, Duration::from_mins(5));
    }

    // An oversized/short/mis-hashed chunk must surface as the permanent
    // Integrity variant, not a retryable transport error — otherwise a
    // corrupt content-addressed blob would burn the whole retry budget.
    #[test]
    fn integrity_error_is_permanent() {
        let err = CoreError::Integrity("chunk at offset 0: bad".into());
        assert!(!err.is_retryable());
    }

    // PackAssembler::new must succeed for a realistic concurrency and yield a
    // usable client (constructor is the only fallible setup step).
    #[test]
    fn assembler_new_builds() {
        let a = PackAssembler::new(Some("tok".into()), 16);
        assert!(a.is_ok());
    }

    // --- incremental_hash ---
    //
    // The contract under test: for ANY file content, ANY chunk tiling, and ANY
    // order the completion signals arrive in, `incremental_hash` yields the plain
    // sequential SHA-256 of the file — or `None` (never a wrong digest) when it
    // cannot cover the file. Tests avoid `unwrap`/`expect` (crate-wide `deny`) by
    // returning `io::Result` and using `?`.
    use std::io::Write as _;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    /// RAII guard: removes the scratch file on drop so a failing assertion (which
    /// returns early) still cleans up. Ignoring the remove error is intentional —
    /// a leftover temp file is harmless and there is nothing to recover.
    struct TempFileGuard(PathBuf);
    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Distinct scratch path per call (process id + monotonic counter) so parallel
    /// tests never collide.
    fn scratch_path(tag: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("hippius_ihash_{tag}_{}_{seq}.bin", std::process::id()))
    }

    /// A varied (non-constant) byte pattern so a mis-ordering across a chunk
    /// boundary changes the digest — a constant fill would hide reorder bugs.
    fn pattern(n: usize) -> Vec<u8> {
        let seed = b"HIPPIUS-hub-chunked-v2";
        (0..n).map(|i| seed[i % seed.len()]).collect()
    }

    fn reference(content: &[u8]) -> String {
        hex::encode(Sha256::digest(content))
    }

    /// Write `content`, push each message (a pack's extents) onto the channel, then
    /// run the hasher to completion. Messages are enqueued before the sender drops,
    /// so `recv` drains them in order and the buffered-then-closed channel models a
    /// batch of packs completing in the given order.
    fn drive(content: &[u8], messages: &[Vec<(u64, u64)>]) -> std::io::Result<Option<String>> {
        let path = scratch_path("unit");
        let _guard = TempFileGuard(path.clone());
        std::fs::File::create(&path)?.write_all(content)?;
        let (tx, rx) = std::sync::mpsc::channel::<Vec<(u64, u64)>>();
        for m in messages {
            let _ = tx.send(m.clone());
        }
        drop(tx);
        Ok(incremental_hash(&rx, &path, content.len() as u64))
    }

    // Tests return `()` and assert (the crate denies both `unwrap`/`expect` AND
    // `panic_in_result_fn`, so a `Result`-returning test with `assert_eq!` is
    // rejected): `drive` carries the fallible I/O and its `.ok()` folds a setup
    // failure into `None`, which the assertion then flags as a test failure.
    #[test]
    fn incremental_in_order_matches_reference() {
        let content = pattern(1000);
        let msgs = vec![vec![(0, 400)], vec![(400, 300)], vec![(700, 300)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(Some(reference(&content))));
    }

    #[test]
    fn incremental_reverse_order_matches_reference() {
        let content = pattern(1000);
        let msgs = vec![vec![(700, 300)], vec![(400, 300)], vec![(0, 400)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(Some(reference(&content))));
    }

    #[test]
    fn incremental_multichunk_pack_before_prefix() {
        // A pack carrying two non-leading extents arrives before the pack that
        // fills [0, 400): the watermark must stay parked until the gap closes.
        let content = pattern(1000);
        let msgs = vec![vec![(400, 300), (700, 300)], vec![(0, 400)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(Some(reference(&content))));
    }

    #[test]
    fn incremental_single_chunk_covers_whole_file() {
        let content = pattern(777);
        let msgs = vec![vec![(0, content.len() as u64)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(Some(reference(&content))));
    }

    #[test]
    fn incremental_incomplete_coverage_returns_none() {
        // The final [700, 1000) extent never arrives, so the file is never fully
        // covered: must yield None (→ caller re-reads), not a partial digest.
        let content = pattern(1000);
        let msgs = vec![vec![(0, 400)], vec![(400, 300)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(None));
    }

    #[test]
    fn incremental_empty_file_hashes_empty() {
        // total_size == 0: no extents, channel closes immediately, hashed == 0 ==
        // total_size → the SHA-256 of the empty input.
        let content: Vec<u8> = Vec::new();
        assert_eq!(drive(&content, &[]).ok(), Some(Some(reference(&content))));
    }

    #[tokio::test]
    async fn incremental_agrees_with_compute_sha256() {
        // Pin that the incremental path yields the byte-for-byte same digest as the
        // authoritative re-read it is allowed to replace.
        let content = pattern(5000);
        let path = scratch_path("cmp");
        let _guard = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        let authoritative = compute_sha256(&path).await.ok();
        assert!(authoritative.is_some());
        let (tx, rx) = std::sync::mpsc::channel::<Vec<(u64, u64)>>();
        let _ = tx.send(vec![(0, content.len() as u64)]);
        drop(tx);
        assert_eq!(incremental_hash(&rx, &path, content.len() as u64), authoritative);
    }

    proptest::proptest! {
        // For any content, any tiling into extents, and any completion order, the
        // incremental digest equals the sequential SHA-256. The shrinker surfaces
        // reorder-bookkeeping bugs a hand-picked fixture would miss (axiom 111).
        #[test]
        fn incremental_agrees_with_reference_under_any_completion_order(
            content in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096usize),
            raw_bounds in proptest::collection::vec(0..4096usize, 0..12usize),
            priorities in proptest::collection::vec(proptest::prelude::any::<u64>(), 0..16usize),
        ) {
            let len = content.len();
            // Build extents [start, size) tiling [0, len) from sorted unique interior bounds.
            let mut bounds: Vec<usize> = raw_bounds.into_iter().filter(|&b| b > 0 && b < len).collect();
            bounds.sort_unstable();
            bounds.dedup();
            let mut extents: Vec<(u64, u64)> = Vec::new();
            let mut prev = 0usize;
            for b in bounds {
                extents.push((prev as u64, (b - prev) as u64));
                prev = b;
            }
            if len > 0 {
                extents.push((prev as u64, (len - prev) as u64));
            }
            // Permute the completion order deterministically from `priorities` (an
            // unstable sort of 0..n by any key is always a valid permutation).
            let n = extents.len();
            let mut order: Vec<usize> = (0..n).collect();
            order.sort_unstable_by_key(|&i| priorities.get(i).copied().unwrap_or(0));

            let path = scratch_path("pt");
            let _guard = TempFileGuard(path.clone());
            proptest::prop_assert!(std::fs::write(&path, &content).is_ok());
            let (tx, rx) = std::sync::mpsc::channel::<Vec<(u64, u64)>>();
            for &i in &order {
                let _ = tx.send(vec![extents[i]]);
            }
            drop(tx);
            let got = incremental_hash(&rx, &path, len as u64);
            proptest::prop_assert_eq!(got, Some(reference(&content)));
        }
    }
}
