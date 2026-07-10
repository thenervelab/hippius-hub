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
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::error::CoreError;

const CONNECT_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;
const WRITE_BUFFER: usize = 2 * 1024 * 1024;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024;

/// Full-request budget for a single chunk-blob GET.
///
/// `connect_timeout` (below) covers only the TCP/TLS handshake; a slow-loris
/// server that handshakes then dribbles bytes would otherwise hold a connection
/// open forever. 5 minutes per ~64 MiB chunk is generous (a ~220 KB/s floor)
/// yet forecloses an indefinitely-stuck chunk. Mirrors the legacy path's
/// `CHUNK_REQUEST_TIMEOUT`; held in a named const so the regression test can
/// pin the value and clippy's dead-code lint enforces its call site.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

/// One chunk to fetch and where it lands in the assembled file.
///
/// `expected_sha256` is lowercase hex WITHOUT the `sha256:` prefix (the Python
/// boundary strips it); `offset` is the running sum of preceding chunk sizes,
/// so the K tasks write disjoint ranges and can run fully concurrently.
pub struct ChunkPlan {
    pub url: String,
    pub expected_sha256: String,
    pub size: u64,
    pub offset: u64,
}

/// Pulls a chunked artifact's chunks concurrently and assembles them in place.
pub struct ChunkAssembler {
    client: Client,
    auth_token: Option<String>,
    max_concurrent: usize,
}

impl ChunkAssembler {
    /// Build an assembler. `max_concurrent` bounds BOTH the reqwest idle-connection
    /// pool and — via a semaphore in `assemble` — the number of chunk fetches
    /// in flight at once. The latter is the load-bearing bound: with
    /// `.http1_only()` each concurrent GET needs its own TCP connection and
    /// hyper has no per-host in-flight cap, so without the semaphore a
    /// many-thousand-chunk file would open a socket per chunk and exhaust FDs /
    /// ephemeral ports / Harbor's rate limit. We force HTTP/1.1 so those
    /// (bounded) parallel fetches each get their own connection (h2 would
    /// multiplex onto one, capping aggregate throughput).
    pub fn new(auth_token: Option<String>, max_concurrent: usize) -> Result<Self, CoreError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .http1_only()
            .pool_max_idle_per_host(max_concurrent)
            .tcp_keepalive(Duration::from_secs(30))
            .build()?;
        Ok(Self { client, auth_token, max_concurrent: max_concurrent.max(1) })
    }

    /// Fetch every chunk into `dest` (pre-allocated to `total_size`), verifying
    /// each chunk's digest as it streams. When `expected_file_sha256` is
    /// `Some`, also re-reads the assembled file and verifies the whole-file
    /// digest, returning it; otherwise returns `None` (per-chunk verification
    /// alone already proves the bytes — the whole-file pass is the opt-in
    /// belt-and-suspenders that also catches an assembly/offset bug).
    pub async fn assemble(
        &self,
        dest: &Path,
        chunks: &[ChunkPlan],
        expected_file_sha256: Option<&str>,
        total_size: u64,
    ) -> Result<Option<String>, CoreError> {
        let parent = dest.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent).await?;

        // Pre-allocate at the exact size (sparse OK) so each task can seek to
        // its offset and write in place — the OS-safe disjoint-range pattern the
        // legacy downloader documents.
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
        // Static literal template — indicatif only errors on malformed directives.
        #[expect(clippy::expect_used, reason = "infallible static template")]
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("indicatif template is static and infallible")
                .progress_chars("#>-"),
        );
        pb.set_message("📥 Downloading chunks");

        let mut joins: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<(), CoreError>)>> =
            FuturesUnordered::new();
        let mut abort_handles: Vec<AbortHandle> = Vec::with_capacity(chunks.len());
        // Bound in-flight fetches (hence open connections) to `max_concurrent`.
        // Every chunk task is spawned, but each awaits a permit before touching
        // the network — so at most `max_concurrent` sockets are open at once even
        // for a many-thousand-chunk file. Tasks blocked on a permit are still
        // cancelled by the abort-all on first error (they wake at the await).
        let permits = Arc::new(Semaphore::new(self.max_concurrent));

        for (i, plan) in chunks.iter().enumerate() {
            let client = self.client.clone();
            let token = self.auth_token.clone();
            let url = plan.url.clone();
            let expected = plan.expected_sha256.clone();
            let (offset, size) = (plan.offset, plan.size);
            let path = dest.to_path_buf();
            let chunk_pb = pb.clone();
            let permits = Arc::clone(&permits);

            let handle = tokio::spawn(async move {
                // `acquire_owned` fails only if the semaphore is closed, which
                // never happens (we hold the sole `Arc` for the whole call).
                let _permit = match permits.acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let res = fetch_chunk_with_retry(
                    &client,
                    &url,
                    token.as_deref(),
                    &expected,
                    offset,
                    size,
                    &path,
                    &chunk_pb,
                )
                .await;
                (i, res)
            });
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
        }

        // Same cancellation discipline as the legacy path: on the first failure,
        // abort every survivor so a detached `tokio::spawn` task can't keep
        // writing to `dest` after we've bubbled the error up. Aborting an
        // already-finished handle is a documented no-op.
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
        pb.finish_with_message("✅ Chunks complete");

        if let Some(expected_file) = expected_file_sha256 {
            let got = compute_sha256(dest).await?;
            if got != expected_file {
                return Err(CoreError::Integrity(format!(
                    "assembled file: expected sha256 {expected_file}, got {got}"
                )));
            }
            return Ok(Some(got));
        }
        Ok(None)
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

/// Parallel pull + scatter of a chunked-v2 file's pack blobs. Mirrors
/// [`ChunkAssembler`]'s concurrency/abort/verify discipline, but the unit of work
/// is a pack (fetched whole, then sliced into N chunks) rather than one chunk.
pub struct PackAssembler {
    client: Client,
    auth_token: Option<String>,
    max_concurrent: usize,
}

impl PackAssembler {
    /// See [`ChunkAssembler::new`] — same client tuning and semaphore rationale.
    pub fn new(auth_token: Option<String>, max_concurrent: usize) -> Result<Self, CoreError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .http1_only()
            .pool_max_idle_per_host(max_concurrent)
            .tcp_keepalive(Duration::from_secs(30))
            .build()?;
        Ok(Self { client, auth_token, max_concurrent: max_concurrent.max(1) })
    }

    /// Fetch every pack into `dest` (pre-allocated to `total_size`), verifying each
    /// carved chunk's digest, then the whole-file digest. Semantics match
    /// [`ChunkAssembler::assemble`]; `expected_file_sha256` proves chunk *ordering*
    /// across packs (the only thing per-chunk digests can't).
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

        let mut joins: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<(), CoreError>)>> =
            FuturesUnordered::new();
        let mut abort_handles: Vec<AbortHandle> = Vec::with_capacity(packs.len());
        let permits = Arc::new(Semaphore::new(self.max_concurrent));

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

            let handle = tokio::spawn(async move {
                let _permit = match permits.acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => return (i, Err(CoreError::Io(std::io::Error::other(e)))),
                };
                let res = fetch_pack_with_retry(
                    &client, &url, token.as_deref(), pack_size, &targets, &path, &pack_pb,
                )
                .await;
                (i, res)
            });
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
        }

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
            let got = compute_sha256(dest).await?;
            if got != expected_file {
                return Err(CoreError::Integrity(format!(
                    "assembled file: expected sha256 {expected_file}, got {got}"
                )));
            }
            return Ok(Some(got));
        }
        Ok(None)
    }
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

/// Retry wrapper around a single chunk fetch. Transient transport errors back
/// off and retry (bounded); a digest/size mismatch is permanent (a
/// content-addressed blob serves the same bytes again) and returns immediately.
#[expect(
    clippy::too_many_arguments,
    reason = "spawn-captured per-chunk state; bundling into a struct adds a clone per chunk"
)]
async fn fetch_chunk_with_retry(
    client: &Client,
    url: &str,
    token: Option<&str>,
    expected_sha256: &str,
    offset: u64,
    size: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    let mut retries = 0;
    loop {
        // A failed attempt may have written a partial slice at `offset`; the
        // next attempt re-seeks to the same offset and overwrites, so a retry
        // is idempotent (writes are bounded to `size`, below). A retried attempt
        // may re-tick the progress bar for bytes it already counted (a cosmetic
        // overshoot on the bar only — the file bytes are correct).
        match fetch_chunk(
            client,
            url,
            token,
            expected_sha256,
            offset,
            size,
            dest_path,
            pb,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > MAX_RETRIES {
                    return Err(e);
                }
                let wait_ms = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            }
        }
    }
}

/// Fetch one chunk blob (full `200 OK`), stream it to `offset`, and verify its
/// sha256 and byte length against the content-addressed expectation.
///
/// Verification is in-stream: the digest is updated per received buffer
/// (interleaved with the `res.chunk()` awaits, so it never blocks the worker
/// for a whole-chunk hash), and writes are bounded to `size` so a server that
/// over-sends cannot bleed into the next chunk's disjoint range.
#[expect(
    clippy::too_many_arguments,
    reason = "spawn-captured per-chunk state; bundling into a struct adds a clone per chunk"
)]
async fn fetch_chunk(
    client: &Client,
    url: &str,
    token: Option<&str>,
    expected_sha256: &str,
    offset: u64,
    size: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    let mut req = client.get(url).timeout(CHUNK_REQUEST_TIMEOUT);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let mut res = req.send().await?;
    if !res.status().is_success() {
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("chunk GET failed for {url}"),
        ));
    }

    let file = OpenOptions::new().write(true).open(dest_path).await?;
    let mut writer = BufWriter::with_capacity(WRITE_BUFFER, file);
    writer.seek(SeekFrom::Start(offset)).await?;

    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut received: u64 = 0;
    while let Some(buf) = res.chunk().await? {
        received += buf.len() as u64;
        // A blob is exactly `size` bytes; more means the server sent the wrong
        // (larger) object. Refuse before writing past our disjoint range.
        if received > size {
            return Err(CoreError::Integrity(format!(
                "chunk at offset {offset}: server sent more than the declared {size} bytes"
            )));
        }
        hasher.update(&buf);
        writer.write_all(&buf).await?;
        written += buf.len() as u64;
        pb.inc(buf.len() as u64);
    }
    writer.flush().await?;

    if written != size {
        return Err(CoreError::Integrity(format!(
            "chunk at offset {offset}: expected {size} bytes, received {written}"
        )));
    }
    let got = hex::encode(hasher.finalize());
    if got != expected_sha256 {
        return Err(CoreError::Integrity(format!(
            "chunk at offset {offset}: expected sha256 {expected_sha256}, got {got}"
        )));
    }
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

    // Assembler::new must succeed for a realistic concurrency and yield a
    // usable client (constructor is the only fallible setup step).
    #[test]
    fn assembler_new_builds() {
        let a = ChunkAssembler::new(Some("tok".into()), 16);
        assert!(a.is_ok());
    }
}
