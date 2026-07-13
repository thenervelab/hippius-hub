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
use tokio::io::SeekFrom;
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

/// Default per-chunk-read idle timeout for downloads (audit M4). Bounds a peer that
/// completes the handshake then dribbles or stops mid-body: reset on each successful
/// read, so it fires only on genuine no-progress (a 30s gap with zero bytes), never
/// on a slow-but-steady transfer. Default-ON — unlike the opt-in client
/// `read_timeout` — and overridden by `HIPPIUS_READ_TIMEOUT` when set. Scoped per
/// chunk read (an app-level `tokio::time::timeout`), not a global client setting, so
/// it fixes the slow-loris the 5-minute total timeout would otherwise leave open for
/// minutes.
const DOWNLOAD_READ_IDLE: Duration = Duration::from_secs(30);

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

/// Absolute ceiling on a single pack blob's declared size, before any of its bytes
/// are read or reserved. A pack aggregates `FastCDC` chunks toward `HIPPIUS_PACK_SIZE`
/// (~64 MiB default; 16 MiB max chunk), so no legitimate pack approaches 1 GiB — the
/// cap exists solely to bound a hostile or corrupt manifest. Without it, the pack
/// size comes straight from a registry-controlled OCI layer descriptor: a declared
/// 1 TiB would make `fetch_pack` reserve 1 TiB up front (an uncatchable alloc abort)
/// and accept up to 1 TiB of body before the length check fires. Both the up-front
/// reservation and the streaming cap are clamped to this value.
const MAX_PACK_BYTES: u64 = 1024 * 1024 * 1024;

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
/// Connect + read timeouts for the shared download client. Resolved in Python
/// (`constants.resolve_connect_timeout` / `resolve_read_timeout`) and threaded
/// down so `HIPPIUS_CONNECT_TIMEOUT` / `HIPPIUS_READ_TIMEOUT` reach real
/// transfers, not only `hippius-hub diagnose` (audit L9). `connect` bounds the
/// handshake; `read` is a *stalled-read* bound (reset on each successful read).
///
/// `read` is `Option`: `None` leaves the shared client's *opt-in* `.read_timeout()`
/// off, so the client is byte-for-byte the pre-audit one. The DEFAULT-ON download
/// stall guard (audit M4) lives at the app level instead — [`read_chunk_bounded`]
/// bounds each `res.chunk()` read by [`download_read_idle`] (30s, or
/// `HIPPIUS_READ_TIMEOUT` when set), scoped per chunk rather than as a global client
/// setting. So a slow-loris is cut by default; setting `HIPPIUS_READ_TIMEOUT`
/// additionally arms the client's per-request `.read_timeout()` and lowers the
/// app-level window to the same value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransportTimeouts {
    pub connect: Duration,
    pub read: Option<Duration>,
}

impl Default for TransportTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(CONNECT_TIMEOUT_SECS),
            read: None,
        }
    }
}

impl TransportTimeouts {
    /// Build from optional per-operation seconds; `None` keeps the field default
    /// (`connect` -> 30s, `read` -> no client read timeout). Non-positive values
    /// are unrepresentable — Python's `_resolve_positive_int` rejects them first.
    pub(crate) fn from_secs(connect: Option<u64>, read: Option<u64>) -> Self {
        let d = Self::default();
        Self {
            connect: connect.map_or(d.connect, Duration::from_secs),
            read: read.map(Duration::from_secs),
        }
    }
}

/// Pure client builder, split from `download_client` so a test can assert the
/// read-timeout behavior on a fresh client without racing the process-global
/// singleton (whose first caller fixes its config for the whole process).
fn build_download_client(timeouts: TransportTimeouts) -> Result<Client, CoreError> {
    let mut builder = Client::builder()
        .connect_timeout(timeouts.connect)
        .http1_only()
        .pool_max_idle_per_host(DOWNLOAD_POOL_MAX_IDLE)
        .tcp_keepalive(Duration::from_secs(30));
    if let Some(read) = timeouts.read {
        // Opt-in only (see `TransportTimeouts`): fires on a stalled read (no byte
        // within the window, reset on each successful read), bounding a peer that
        // handshakes then dribbles/stops mid-body — which `connect_timeout` and
        // `tcp_keepalive` cannot see and the per-chunk 5-min total `.timeout()`
        // only catches after 5 minutes (audit M4).
        builder = builder.read_timeout(read);
    }
    Ok(builder.build()?)
}

/// The resolved per-chunk-read idle timeout (audit M4), fixed by the first
/// `download_client` caller (first-caller-wins, like the client itself): every file
/// in a snapshot passes the same env-derived value. `HIPPIUS_READ_TIMEOUT` overrides
/// the `DOWNLOAD_READ_IDLE` default. Read via [`download_read_idle`].
static READ_IDLE: OnceLock<Duration> = OnceLock::new();

/// The default-on download read-idle timeout (audit M4). Falls back to
/// `DOWNLOAD_READ_IDLE` if no download has started yet; in practice the read loops
/// only run after `download_client` has fixed it, so the fallback is belt-and-braces.
pub(crate) fn download_read_idle() -> Duration {
    READ_IDLE.get().copied().unwrap_or(DOWNLOAD_READ_IDLE)
}

/// One response-body read bounded by `idle` (audit M4): a `res.chunk()` yielding no
/// data within `idle` is a retryable [`CoreError::ReadStall`], so a peer that stops
/// mid-body is cut promptly instead of running out the per-chunk 5-minute total
/// timeout. `idle` applies per call (per successful read resets it), so a slow-but-
/// steady transfer is never tripped. Shared by both download read loops. `idle` is a
/// parameter (not read from the global) so tests can drive a short window.
pub(crate) async fn read_chunk_bounded(
    res: &mut reqwest::Response,
    idle: Duration,
) -> Result<Option<bytes::Bytes>, CoreError> {
    match tokio::time::timeout(idle, res.chunk()).await {
        Ok(chunk) => Ok(chunk?),
        Err(_elapsed) => Err(CoreError::ReadStall(idle)),
    }
}

pub(crate) fn download_client(timeouts: TransportTimeouts) -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // First-caller-wins (like `global_pack_gate`): the process-global client is
    // built once with the first download's resolved timeouts. Every file in a
    // snapshot passes the same env-derived values, so the winner is representative;
    // a later differing value is ignored — the documented tradeoff of one shared
    // pool. The loser of an init race drops its freshly built client (RAII).
    let built = build_download_client(timeouts)?;
    let client = CLIENT.get_or_init(|| built);
    // Fix the default-on read-idle window (audit M4) alongside the client, same
    // first-caller-wins discipline; HIPPIUS_READ_TIMEOUT overrides the default.
    let _ = READ_IDLE.get_or_init(|| timeouts.read.unwrap_or(DOWNLOAD_READ_IDLE));
    Ok(client)
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

/// Aborts every held task handle when dropped. Fires on BOTH `assemble`'s
/// early-return error path AND on cancellation (the whole `assemble` future dropped
/// when Ctrl-C interrupts the native call — audit M1). Without it, dropping the
/// `FuturesUnordered`/`Vec<AbortHandle>` would DETACH the spawned pack tasks (a
/// `JoinHandle` drop detaches, not aborts), leaving them writing to `dest` and
/// holding the pack gate after the caller moved on — the exact hazard the download
/// path's `JoinSet` avoids structurally (audit D4/L13).
struct AbortOnDrop(Vec<AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

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
    pub fn new(
        auth_token: Option<String>,
        max_concurrent: usize,
        timeouts: TransportTimeouts,
    ) -> Result<Self, CoreError> {
        // Clone the process-global client (an Arc-backed handle sharing one pool)
        // instead of building a fresh client + empty pool per file. `max_concurrent`
        // still bounds real concurrency via the `Semaphore` in `assemble`.
        let client = download_client(timeouts)?.clone();
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
        validate_pack_plan(packs, total_size)?;
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
            // No `sync_all` (audit L15): the parallel chunk writers and the
            // incremental hasher see the `set_len` size through the page cache
            // without forcing metadata to disk. `sync_all` only bought crash
            // durability of the pre-allocation, which is discarded anyway — a crash
            // re-downloads the whole file (the dest always opens with `truncate`).
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
        // Abort every pack task when this scope unwinds — on the early-return error
        // path below AND on cancellation (audit M1): a `_`-prefixed binding keeps the
        // guard alive to scope end (a bare `_` would drop it immediately). It is
        // declared after `joins`, so on unwind it drops FIRST and aborts the tasks
        // before `joins` detaches them.
        let _abort_guard = AbortOnDrop(abort_handles);

        // Drop the original sender so the channel closes once every pack task has
        // finished (each task holds its own clone); that unblocks the hasher task's
        // final `recv` and lets it finalize (or fall back). On cancellation the
        // aborted pack tasks drop their sender clones too, so the channel closes and
        // the hasher exits rather than leaking.
        drop(hash_tx);

        while let Some(res) = joins.next().await {
            match res {
                // On any error we return; `_abort_guard` drops and aborts every
                // still-running pack task (the old explicit abort loop, now unified
                // with the cancellation path).
                Err(join_err) => return Err(CoreError::JoinFailed { index: None, source: join_err }),
                Ok((i, Err(pack_err))) => return Err(CoreError::ChunkFailed { index: i, source: Box::new(pack_err) }),
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

/// Reject a pack plan whose chunk placement would write outside the declared file
/// length, before any byte is fetched. `fetch_pack` writes each chunk at its
/// `file_offset` with `seek`+`write_all`, which silently extends the file past
/// `total_size` (leaving a zero hole) for an out-of-bounds chunk; that file hashes
/// differently from `[0, total_size)`, so a malformed or adversarial plan could
/// otherwise append trailing bytes to an otherwise-verifying file. Catching it here
/// keeps the assembled file exactly `total_size` bytes, which is what both the
/// incremental hasher and the `compute_sha256` fallback assume. `checked_add` guards
/// a `file_offset + size` that would itself overflow `u64`.
fn validate_pack_plan(packs: &[PackPlanEntry], total_size: u64) -> Result<(), CoreError> {
    for pack in packs {
        // Reject an absurd declared pack size BEFORE fetch_pack reserves or streams
        // it (see MAX_PACK_BYTES): the size is registry-controlled, so an unbounded
        // value is a hostile-manifest DoS, not an integrity mismatch of real bytes.
        if pack.size > MAX_PACK_BYTES {
            return Err(CoreError::Integrity(format!(
                "pack {} declares {} bytes, over the {MAX_PACK_BYTES}-byte ceiling",
                pack.url, pack.size
            )));
        }
        for c in &pack.chunks {
            let end = c.file_offset.checked_add(c.size).ok_or_else(|| {
                CoreError::Integrity(format!("chunk at file offset {} size {} overflows u64", c.file_offset, c.size))
            })?;
            if end > total_size {
                return Err(CoreError::Integrity(format!(
                    "chunk at file offset {} size {} overruns file length {total_size}",
                    c.file_offset, c.size
                )));
            }
        }
    }
    Ok(())
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
                // Full-jitter backoff (audit L-JITTER): decorrelates the up-to-32
                // concurrent pack fetches so a registry 429/503 does not make them
                // retry in lockstep. Shared helper across the four transport loops.
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
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
    let mut res = req.send().await?;
    if !res.status().is_success() {
        return Err(CoreError::ServerError(res.status().as_u16(), format!("pack GET failed for {url}")));
    }
    // Audit L12: read the body under a running cap instead of `res.bytes()`, which
    // buffers an unbounded body BEFORE the length check — a chunked (no
    // Content-Length) response from a misbehaving/compromised registry could
    // balloon memory well past the intended ~pack_size ceiling (x32 concurrent
    // packs) before rejection. Abort the moment the accumulated body exceeds
    // `pack_size`, so peak memory stays bounded to one pack.
    // Clamp the up-front reservation to MAX_PACK_BYTES so a registry-declared
    // pack_size (validate_pack_plan rejects > MAX_PACK_BYTES up front, but this is
    // the defense-in-depth backstop) can never turn `with_capacity` into a
    // multi-TiB alloc abort. A larger-but-legal pack still grows the Vec on demand,
    // bounded by the `received > pack_size` check below.
    let reserve = pack_size.min(MAX_PACK_BYTES);
    let cap = usize::try_from(reserve).unwrap_or(usize::MAX);
    let mut bytes: Vec<u8> = Vec::with_capacity(cap);
    let mut received: u64 = 0;
    // Each body read is bounded by the default-on read-idle window (audit M4): a
    // registry that stops streaming mid-pack is cut as a retryable ReadStall instead
    // of holding the connection until the 5-minute total timeout.
    while let Some(chunk) = read_chunk_bounded(&mut res, download_read_idle()).await? {
        received = received.saturating_add(chunk.len() as u64);
        if received > pack_size {
            // Transport length anomaly, not a wrong-bytes integrity failure — a
            // proxy/CDN that over-sends a self-consistent body can clear on retry,
            // so classify it retryable (matches the Range path's short/over-length
            // handling in chunked_downloader). Bounded by pack_size so a runaway
            // stream is cut here, well under the MAX_PACK_BYTES ceiling.
            return Err(CoreError::BadResponse(format!(
                "pack {url}: body exceeds expected {pack_size} bytes (over-send)"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != pack_size {
        return Err(CoreError::BadResponse(format!(
            "pack {url}: expected {pack_size} bytes, got {}",
            bytes.len()
        )));
    }
    // Verify + scatter on the blocking pool (audit L14). The per-chunk sha256 is
    // CPU-bound and the scatter writes are local disk — neither is async work, so
    // running them inline on the runtime starves the other up-to-32 concurrent pack
    // fetches. `bytes` (the received pack) moves in; the metadata clones are cheap.
    let targets_owned = targets.to_vec();
    let dest = dest_path.to_path_buf();
    let url_owned = url.to_string();
    let pb_owned = pb.clone();
    tokio::task::spawn_blocking(move || verify_and_scatter(&url_owned, &bytes, &targets_owned, &dest, &pb_owned))
        .await
        .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?
}

/// Verify each carved chunk's sha256 against `bytes` and scatter its slice to the
/// file offset. Runs on the blocking pool (audit L14): hashing is CPU-bound and the
/// writes are local disk, so this does no async work and must not sit on the async
/// runtime. A digest mismatch or out-of-range chunk is a PERMANENT `Integrity` error
/// (a content-addressed blob serves the same wrong bytes on retry, and an
/// out-of-bounds range is a bad plan) — distinct from the transport length anomalies
/// in `fetch_pack`, which are the retryable `BadResponse`. A corrupt/mis-placed pack
/// must never be written past its bounds.
fn verify_and_scatter(
    url: &str,
    bytes: &[u8],
    targets: &[(u64, u64, u64, String)],
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    use std::io::{Seek, Write};

    let mut file = std::fs::OpenOptions::new().write(true).open(dest_path)?;
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
        file.seek(SeekFrom::Start(*file_offset))?;
        file.write_all(slice)?;
        pb.inc(*size);
    }
    file.flush()?;
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
/// `[0, total_size)` in order AND draining every signalled extent (so no pack wrote
/// past `total_size`); any shortfall — an abort closing the channel early, a coverage
/// gap, a read error, or an out-of-bounds extent left in `pending` — yields `None` so
/// the caller re-reads. It therefore hashes the identical bytes `compute_sha256`
/// would and can only ever be a faster route to the same digest, never a different
/// verdict.
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

    // Channel closed: every pack task has finished. Require BOTH that the hash
    // covered exactly `total_size` AND that `pending` drained. A leftover extent
    // means a pack wrote beyond the contiguous [0, total_size) region — an
    // out-of-bounds placement leaves the on-disk file longer than total_size — so
    // returning the prefix digest here would ACCEPT a file the full re-read rejects.
    // Any leftover instead forces None, and the caller re-reads to EOF and catches
    // it. (validate_pack_plan already rejects such plans up front; this is the
    // matching defense inside the hasher so the two paths can never disagree.)
    if hashed == total_size && pending.is_empty() {
        Some(hex::encode(hasher.finalize()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Pin the per-chunk request timeout value (same guard shape as the legacy
    // path): clippy's dead-code lint enforces the const is USED at the call
    // site; this test enforces its VALUE can't silently drift.
    #[test]
    fn chunk_request_timeout_is_five_minutes() {
        assert_eq!(CHUNK_REQUEST_TIMEOUT, Duration::from_mins(5));
    }

    #[test]
    fn verify_and_scatter_writes_chunks_and_rejects_bad_digest() {
        // L14: the verify+scatter loop (moved onto the blocking pool) must place
        // each chunk at its file offset only after its sha256 matches the expected
        // digest, and reject a mismatched chunk as a (retryable) Integrity error.
        use std::io::Read;
        let pb = ProgressBar::hidden();
        let (a, b) = (b"AAAA".as_slice(), b"BB".as_slice()); // 4 + 2 bytes
        let pack: Vec<u8> = [a, b].concat();
        let ha = hex::encode(Sha256::digest(a));
        let hb = hex::encode(Sha256::digest(b));
        // (offset_in_pack, size, file_offset, expected_hex): scatter A→0, B→4.
        let good = vec![(0u64, 4u64, 0u64, ha), (4u64, 2u64, 4u64, hb.clone())];

        let path = std::env::temp_dir().join(format!("hippius-vs-{}.bin", std::process::id()));
        let Ok(()) = std::fs::File::create(&path).and_then(|f| f.set_len(6)) else {
            unreachable!("temp file create")
        };
        let Ok(()) = verify_and_scatter("u", &pack, &good, &path, &pb) else {
            unreachable!("valid chunks must scatter")
        };
        let mut got = Vec::new();
        let Ok(_) = std::fs::File::open(&path).and_then(|mut f| f.read_to_end(&mut got)) else {
            unreachable!("read back")
        };
        assert_eq!(got, b"AAAABB");

        // A wrong expected digest (hb over the "AAAA" slice) is a permanent Integrity
        // error, so a corrupt/mis-placed pack is never accepted.
        let bad = vec![(0u64, 4u64, 0u64, hb)];
        assert!(matches!(
            verify_and_scatter("u", &pack, &bad, &path, &pb),
            Err(CoreError::Integrity(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn read_chunk_bounded_trips_readstall_on_a_stalled_body() {
        // Audit M4: a peer that sends the head + a few body bytes then stalls (no
        // more data, socket held open) must be cut by the app-level per-read idle
        // window as a retryable ReadStall — not left until the 5-minute total
        // timeout. The client here has NO client read_timeout (default), so the
        // app-level ReadStall is the sole guard, proving it is default-on.
        let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else { return };
        let Ok(addr) = listener.local_addr() else { return };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                // Advertise 1000 bytes, send 8, then stall (hold the socket open).
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\nABCDEFGH")
                    .await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/blob");
        let Ok(client) = build_download_client(TransportTimeouts::default()) else {
            unreachable!("client builds")
        };
        let Ok(mut res) = client.get(&url).send().await else { unreachable!("GET connects") };
        let idle = Duration::from_millis(200);
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match read_chunk_bounded(&mut res, idle).await {
                    Ok(Some(_)) => {}
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        })
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::ReadStall(_)))),
            "a stalled body read must abort as a retryable ReadStall, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn abort_on_drop_aborts_held_tasks() {
        // Audit M1: dropping the guard must abort every held pack task, so a cancelled
        // (Ctrl-C'd) assemble never leaves a task writing to `dest` after the caller
        // moved on. Spawn long-lived tasks, wrap their abort handles in AbortOnDrop,
        // drop it, then confirm each task was cancelled (never ran to completion).
        let mut joins = Vec::new();
        let mut aborts = Vec::new();
        for _ in 0..3 {
            let h = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
            aborts.push(h.abort_handle());
            joins.push(h);
        }

        drop(AbortOnDrop(aborts));

        for h in joins {
            match h.await {
                Ok(()) => unreachable!("the task must be aborted, not run to completion"),
                Err(e) => assert!(e.is_cancelled(), "AbortOnDrop must cancel the task, got {e:?}"),
            }
        }
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
        let a = PackAssembler::new(Some("tok".into()), 16, TransportTimeouts::default());
        assert!(a.is_ok());
    }

    #[tokio::test]
    async fn read_timeout_aborts_a_stalled_response_body() {
        // Audit M4: a peer that completes the handshake, sends response headers +
        // a few body bytes, then goes silent (an application-layer stall
        // `connect_timeout`/`tcp_keepalive` cannot see) must be cut by the client's
        // `read_timeout`. Without it the body read hangs until the caller's 5-min
        // total timeout; the download plane's whole point is to fail fast and retry.
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else { return };
        let Ok(addr) = listener.local_addr() else { return };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await; // consume the request line/headers
                // Promise 1_000_000 bytes, deliver 8, then stall without closing.
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nabcdefgh")
                    .await;
                let _ = sock.flush().await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let Ok(client) = build_download_client(TransportTimeouts {
            connect: Duration::from_secs(5),
            read: Some(Duration::from_secs(1)),
        }) else {
            server.abort();
            return;
        };
        let url = format!("http://{addr}/blob");
        // `Ok(Err(_))` = the inner future finished with a reqwest error (read_timeout
        // fired — correct). `Err(_)` = the test's own 8s bound elapsed, i.e. the read
        // hung because `read_timeout` was NOT honored (the regression this guards).
        let outcome = tokio::time::timeout(Duration::from_secs(8), async {
            let resp = client.get(&url).send().await?;
            resp.bytes().await
        })
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(_))),
            "a stalled body read must abort via read_timeout, got {outcome:?}"
        );
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

    // --- validate_pack_plan ---

    fn chunk_target(offset_in_pack: u64, size: u64, file_offset: u64, sha: String) -> PackChunkTarget {
        PackChunkTarget { offset_in_pack, size, file_offset, expected_sha256: sha }
    }

    #[test]
    fn validate_pack_plan_accepts_in_bounds_tiling() {
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 1000,
            chunks: vec![chunk_target(0, 400, 0, String::new()), chunk_target(400, 600, 400, String::new())],
        }];
        assert!(validate_pack_plan(&packs, 1000).is_ok());
    }

    #[test]
    fn validate_pack_plan_rejects_out_of_bounds_chunk() {
        // A chunk at file_offset 1005 in a 1000-byte file would extend the assembled
        // file past total_size — exactly the over-length false-accept the whole-file
        // digest must never miss. It must be rejected before any fetch.
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 1100,
            chunks: vec![chunk_target(0, 1000, 0, String::new()), chunk_target(1000, 100, 1005, String::new())],
        }];
        assert!(matches!(validate_pack_plan(&packs, 1000), Err(CoreError::Integrity(_))));
    }

    #[test]
    fn validate_pack_plan_rejects_offset_size_overflow() {
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 10,
            chunks: vec![chunk_target(0, u64::MAX, 1, String::new())],
        }];
        assert!(matches!(validate_pack_plan(&packs, u64::MAX), Err(CoreError::Integrity(_))));
    }

    #[test]
    fn validate_pack_plan_rejects_pack_size_over_ceiling() {
        // A registry-declared pack size above MAX_PACK_BYTES must be refused BEFORE
        // fetch_pack reserves or streams it — the hostile-manifest DoS the ceiling
        // exists to bound. The chunks are otherwise in-bounds, so only the declared
        // pack.size trips the guard.
        let packs = vec![PackPlanEntry {
            url: "reg/packHuge".to_string(),
            size: MAX_PACK_BYTES + 1,
            chunks: vec![chunk_target(0, 10, 0, String::new())],
        }];
        assert!(matches!(validate_pack_plan(&packs, 10), Err(CoreError::Integrity(ref m)) if m.contains("ceiling")));
        // Exactly at the ceiling is still accepted (boundary, not a hostile value).
        let ok = vec![PackPlanEntry {
            url: String::new(),
            size: MAX_PACK_BYTES,
            chunks: vec![chunk_target(0, 10, 0, String::new())],
        }];
        assert!(validate_pack_plan(&ok, 10).is_ok());
    }

    #[test]
    fn incremental_extent_beyond_total_size_returns_none() {
        // A stray extent past total_size (a pack that wrote beyond the file end) must
        // NOT be accepted via the [0, total_size) prefix: the leftover in `pending`
        // forces None so the caller re-reads to EOF and catches the over-length file.
        let content = pattern(1000);
        let msgs = vec![vec![(0, 1000)], vec![(1005, 100)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(None));
    }

    // --- verify_file_digest (fallback / mismatch / JoinError wiring) ---

    #[tokio::test]
    async fn verify_file_digest_prefers_incremental_without_reread() {
        // Some(correct) is returned directly; `missing` never exists, so a stray
        // fallback re-read would error and fail this assertion.
        let missing = scratch_path("verify_fast");
        let expected = reference(b"payload");
        let e = expected.clone();
        let task = tokio::spawn(async move { Some(e) });
        assert_eq!(verify_file_digest(Some(task), &missing, &expected).await.ok(), Some(expected));
    }

    #[tokio::test]
    async fn verify_file_digest_falls_back_to_reread_when_incremental_none() {
        let content = pattern(2048);
        let path = scratch_path("verify_fallback");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        let task = tokio::spawn(async { None });
        assert_eq!(verify_file_digest(Some(task), &path, &reference(&content)).await.ok(), Some(reference(&content)));
    }

    #[tokio::test]
    async fn verify_file_digest_none_task_reads_from_disk() {
        let content = pattern(2048);
        let path = scratch_path("verify_notask");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        assert_eq!(verify_file_digest(None, &path, &reference(&content)).await.ok(), Some(reference(&content)));
    }

    #[tokio::test]
    async fn verify_file_digest_rejects_mismatch() {
        // Incremental yields a digest that disagrees with `expected` -> Integrity, not
        // an accept. Guards the `got != expected_file` comparison against inversion.
        let missing = scratch_path("verify_mismatch");
        let task = tokio::spawn(async { Some("a".repeat(64)) });
        let expected = "b".repeat(64);
        assert!(matches!(verify_file_digest(Some(task), &missing, &expected).await, Err(CoreError::Integrity(_))));
    }

    #[tokio::test]
    async fn verify_file_digest_surfaces_hasher_join_error_as_io() {
        // A JoinError (task cancelled/panicked) surfaces as CoreError::Io rather than
        // being masked. Aborting a pending task yields the JoinError without a panic!
        // macro (which the crate denies).
        let missing = scratch_path("verify_join");
        let task: HasherTask = tokio::spawn(async { std::future::pending::<Option<String>>().await });
        task.abort();
        assert!(matches!(verify_file_digest(Some(task), &missing, &"c".repeat(64)).await, Err(CoreError::Io(_))));
    }

    // --- assemble (end-to-end orchestration over a local pack server) ---

    /// Minimal HTTP/1 server for tests: serves each registered path's bytes as a 200
    /// with Content-Length and `connection: close` (one request per connection, so
    /// there is no keep-alive framing to parse). Returns the base URL; the accept loop
    /// lives in a spawned task the test's runtime cancels on completion.
    async fn serve_packs(routes: HashMap<String, Vec<u8>>) -> std::io::Result<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut req = Vec::new();
                    let mut tmp = [0u8; 1024];
                    while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => req.extend_from_slice(&tmp[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&req);
                    let path = head.lines().next().and_then(|l| l.split(' ').nth(1)).unwrap_or("/");
                    let (status, body): (&str, &[u8]) = match routes.get(path) {
                        Some(b) => ("200 OK", b.as_slice()),
                        None => ("404 Not Found", b"".as_slice()),
                    };
                    let resp = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.write_all(body).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Ok(format!("http://{addr}"))
    }

    /// A 3000-byte file split into three 1000-byte chunks across two packs, with the
    /// leading and trailing chunks scattered into pack A (non-contiguous file offsets)
    /// and the middle chunk in pack B — so the plan exercises cross-pack scatter and
    /// out-of-order arrival, not a trivial single-pack copy.
    fn three_pack_plan(base: &str, content: &[u8]) -> Vec<PackPlanEntry> {
        vec![
            PackPlanEntry {
                url: format!("{base}/packA"),
                size: 2000,
                chunks: vec![
                    chunk_target(0, 1000, 0, reference(&content[0..1000])),
                    chunk_target(1000, 1000, 2000, reference(&content[2000..3000])),
                ],
            },
            PackPlanEntry {
                url: format!("{base}/packB"),
                size: 1000,
                chunks: vec![chunk_target(0, 1000, 1000, reference(&content[1000..2000]))],
            },
        ]
    }

    #[tokio::test]
    async fn fetch_pack_rejects_over_length_body() {
        // Audit L12: a body larger than the declared pack_size must be rejected
        // under a running cap, not buffered whole. Serve 2000 bytes but declare
        // pack_size=1000 — the over-send guard trips before the carve.
        let mut routes = HashMap::new();
        routes.insert("/pack".to_string(), vec![9u8; 2000]);
        let Some(base) = serve_packs(routes).await.ok() else { return };
        let Ok(client) = download_client(TransportTimeouts::default()) else { return };
        let pb = ProgressBar::hidden();
        let dest = scratch_path("l12_overlen");
        let _g = TempFileGuard(dest.clone());
        let res = fetch_pack(client, &format!("{base}/pack"), None, 1000, &[], &dest, &pb).await;
        assert!(
            matches!(res, Err(CoreError::BadResponse(ref m)) if m.contains("over-send")),
            "an over-length pack body must be rejected (bounded), got {res:?}"
        );
        // A transport length anomaly is a plausibly-transient BadResponse, so the
        // retry loop re-attempts it — distinct from a permanent Integrity mismatch.
        assert!(
            res.is_err_and(|e| e.is_retryable()),
            "an over-send is retryable so fetch_pack_with_retry re-attempts it"
        );
    }

    #[tokio::test]
    async fn assemble_reconstructs_scattered_packs_and_verifies() {
        let content = pattern(3000);
        let pack_a = [&content[0..1000], &content[2000..3000]].concat();
        let pack_b = content[1000..2000].to_vec();
        let mut routes = HashMap::new();
        routes.insert("/packA".to_string(), pack_a);
        routes.insert("/packB".to_string(), pack_b);
        let Some(base) = serve_packs(routes).await.ok() else { return };
        let dest = scratch_path("asm_ok");
        let _g = TempFileGuard(dest.clone());
        let Some(assembler) = PackAssembler::new(None, 4, TransportTimeouts::default()).ok() else { return };
        let packs = three_pack_plan(&base, &content);
        // Timeout-guarded: a channel-lifecycle regression (e.g. dropping `drop(hash_tx)`)
        // would hang the hasher's recv forever, surfacing here as a failure not a hang.
        let expected = reference(&content);
        let fut = assembler.assemble(&dest, &packs, Some(&expected), content.len() as u64);
        let digest = match tokio::time::timeout(Duration::from_secs(30), fut).await {
            Ok(Ok(Some(d))) => Some(d),
            _ => None,
        };
        assert_eq!(digest, Some(expected));
    }

    #[tokio::test]
    async fn assemble_rejects_wrong_whole_file_digest() {
        let content = pattern(3000);
        let pack_a = [&content[0..1000], &content[2000..3000]].concat();
        let pack_b = content[1000..2000].to_vec();
        let mut routes = HashMap::new();
        routes.insert("/packA".to_string(), pack_a);
        routes.insert("/packB".to_string(), pack_b);
        let Some(base) = serve_packs(routes).await.ok() else { return };
        let dest = scratch_path("asm_bad");
        let _g = TempFileGuard(dest.clone());
        let Some(assembler) = PackAssembler::new(None, 4, TransportTimeouts::default()).ok() else { return };
        let packs = three_pack_plan(&base, &content);
        // The bytes assemble correctly, but the declared whole-file digest disagrees:
        // the cross-pack ordering check must reject with Integrity.
        let wrong = "f".repeat(64);
        let fut = assembler.assemble(&dest, &packs, Some(&wrong), content.len() as u64);
        let got = tokio::time::timeout(Duration::from_secs(30), fut).await;
        assert!(matches!(got, Ok(Err(CoreError::Integrity(_)))));
    }
}
