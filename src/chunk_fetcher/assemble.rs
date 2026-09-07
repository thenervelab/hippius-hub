//! The pack fan-out: plan validation, the bounded parallel pack fetch with
//! retry, and the `PackAssembler` orchestration that ties fetch, verify, and
//! scatter together.

use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::chunk_fetcher::client::{
    download_client, download_read_idle, global_pack_gate, read_chunk_bounded, TransportTimeouts,
};
use crate::chunk_fetcher::scatter::{verify_and_scatter, verify_file_digest};
use crate::digest::Sha256Digest;
use crate::error::CoreError;
use crate::incremental_hash::{spawn_incremental_hasher, HashSignal};
use crate::transport::CHUNK_REQUEST_TIMEOUT;

const MAX_RETRIES: u32 = 3;

/// Absolute ceiling on a single pack blob's *declared* size. A pack aggregates
/// `FastCDC` chunks toward `HIPPIUS_PACK_SIZE` (~64 MiB default) and may overshoot
/// by at most one chunk (`fastcdc` `MAXIMUM_MAX` = 16 MiB), so no honest pack
/// approaches 1 GiB. The value exists to reject a hostile OCI layer size before
/// any body is read. Kept in lockstep with `hippius_hub.constants.MAX_PACK_BYTES`.
pub(crate) const MAX_PACK_BYTES: u64 = 1024 * 1024 * 1024;

/// Initial `try_reserve` for a pack buffer: honest packs are ~64 MiB, so this
/// avoids ~20 reallocs. Never `declared` itself — 32 concurrent packs each
/// declaring 1 GiB would otherwise commit 32 GiB before a byte arrived.
const PACK_RESERVE_CEILING: u64 = 64 * 1024 * 1024;

fn pack_initial_reserve(declared: u64) -> usize {
    let n = declared.min(PACK_RESERVE_CEILING);
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// Strip userinfo / query / fragment so a presigned redirect cannot leak
/// AWS signature query parameters into Python exceptions or logs.
fn redact_url(url: &str) -> String {
    let (scheme, after_scheme) = match url.split_once("://") {
        Some((s, rest)) => (format!("{s}://"), rest),
        None => (String::new(), url),
    };
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let (authority, remainder) = after_scheme.split_at(authority_end);
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let path_end = remainder.find(['?', '#']).unwrap_or(remainder.len());
    let path = &remainder[..path_end];
    let redacted_tail = if path_end < remainder.len() {
        "?<redacted>"
    } else {
        ""
    };
    format!("{scheme}{host}{path}{redacted_tail}")
}

/// Grow `buf` by `additional` bytes. `Vec::with_capacity` / `extend_from_slice`
/// route a failed allocation to `handle_alloc_error`, which aborts the process
/// (not a panic: `catch_unwind` / pyo3 cannot turn it into an exception).
fn reserve_or_err(buf: &mut Vec<u8>, additional: usize, url: &str) -> Result<(), CoreError> {
    buf.try_reserve(additional).map_err(|e| {
        CoreError::Io(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            format!(
                "pack {}: cannot reserve {additional} more bytes: {e}",
                redact_url(url)
            ),
        ))
    })
}

/// Aborts every held task handle when dropped. Fires on BOTH `assemble`'s
/// early-return error path AND on cancellation (the whole `assemble` future dropped
/// when Ctrl-C interrupts the native call - audit M1). Without it, dropping the
/// `FuturesUnordered`/`Vec<AbortHandle>` would DETACH the spawned pack tasks (a
/// `JoinHandle` drop detaches, not aborts), leaving them writing to `dest` and
/// holding the pack gate after the caller moved on - the exact hazard the download
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
/// where it lands in the assembled file, and its content digest (binary; hex is
/// parsed once at the FFI boundary via [`Sha256Digest::from_hex`]).
pub struct PackChunkTarget {
    pub offset_in_pack: u64,
    pub size: u64,
    pub file_offset: u64,
    pub expected_sha256: Sha256Digest,
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
    // `Arc<str>` (not `String`): the token is captured by every spawned pack
    // task, so the per-pack clone is a pointer bump, not a heap copy (Task C2).
    // Converted once from the constructor's owned `String`.
    auth_token: Option<Arc<str>>,
    max_concurrent: usize,
}

impl PackAssembler {
    /// Clones the shared process-global `download_client` (warm pool across files);
    /// the semaphore in `assemble` - not the client's fixed idle pool - is the real
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
        Ok(Self {
            client,
            auth_token: auth_token.map(Arc::from),
            max_concurrent: max_concurrent.max(1),
        })
    }

    /// Fetch every pack into `dest` (pre-allocated to `total_size`), verifying each
    /// carved chunk's digest, then the whole-file digest. `expected_file_sha256`
    /// proves chunk *ordering* across packs (the only thing per-chunk digests can't).
    ///
    /// `packs` is consumed by value (Task C2): each `PackPlanEntry` moves into its
    /// spawned task, so the fan-out clones no URL strings and rebuilds no per-task
    /// target `Vec`s. The sole caller (`download_packs_native`) builds the plan
    /// specifically for this call and never reuses it.
    // The C2 fan-out restructure brought this method back under clippy's
    // 100-line bound, so the A1-era `#[expect(clippy::too_many_lines)]` is gone
    // (`unfulfilled_lint_expectations` enforced its removal).
    pub async fn assemble(
        &self,
        dest: &Path,
        packs: Vec<PackPlanEntry>,
        expected_file_sha256: Option<&str>,
        total_size: u64,
    ) -> Result<Option<String>, CoreError> {
        validate_pack_plan(&packs, total_size)?;
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
            // durability of the pre-allocation, which is discarded anyway - a crash
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
        pb.set_message("Downloading packs");

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
        // `Arc<Path>` built once so every pack task's capture is a pointer bump,
        // not a fresh `PathBuf` heap copy (Task C2).
        let dest_shared: Arc<Path> = Arc::from(dest);

        for (i, plan) in packs.into_iter().enumerate() {
            let client = self.client.clone();
            let token = self.auth_token.clone();
            // The plan entry MOVES into its task (Task C2): the URL becomes a
            // shared `Arc<str>` and the chunk `Vec` becomes an `Arc<[_]>` once,
            // so retries and the `spawn_blocking` scatter clone pointers instead
            // of rebuilding a per-attempt targets `Vec` + URL `String`.
            let PackPlanEntry { url, size, chunks } = plan;
            let url: Arc<str> = url.into();
            let targets: Arc<[PackChunkTarget]> = chunks.into();
            let path = Arc::clone(&dest_shared);
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
                    &client,
                    &url,
                    token.as_deref(),
                    size,
                    &targets,
                    &path,
                    &pack_pb,
                )
                .await;
                if res.is_ok() {
                    if let Some(tx) = &hash_tx {
                        // Signal the file-offset extents this pack verified+wrote, once,
                        // only AFTER the retry loop succeeded - a retried pack must not
                        // double-count. A closed channel means the hasher task already
                        // exited (error/abort), so a dropped signal merely forgoes the
                        // incremental fast path; the whole-file check then re-reads.
                        let done: HashSignal =
                            targets.iter().map(|t| (t.file_offset, t.size)).collect();
                        let _ = tx.send(done);
                    }
                }
                (i, res)
            });
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
        }
        // Abort every pack task when this scope unwinds - on the early-return error
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
                Err(join_err) => {
                    return Err(CoreError::JoinFailed {
                        index: None,
                        source: join_err,
                    })
                }
                Ok((i, Err(pack_err))) => {
                    return Err(CoreError::ChunkFailed {
                        index: i,
                        source: Box::new(pack_err),
                    })
                }
                Ok((_, Ok(()))) => {}
            }
        }
        pb.finish_with_message("Packs complete");

        if let Some(expected_file) = expected_file_sha256 {
            return Ok(Some(
                verify_file_digest(hasher_task, dest, expected_file).await?,
            ));
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
                redact_url(&pack.url),
                pack.size
            )));
        }
        for c in &pack.chunks {
            let end = c.file_offset.checked_add(c.size).ok_or_else(|| {
                CoreError::Integrity(format!(
                    "chunk at file offset {} size {} overflows u64",
                    c.file_offset, c.size
                ))
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

/// `url`/`targets`/`dest_path` are borrowed `Arc`s (Task C2): each retry attempt
/// re-uses the same shared allocations, and `fetch_pack` clones only the pointers
/// its `spawn_blocking` closure needs to be `'static`.
async fn fetch_pack_with_retry(
    client: &Client,
    url: &Arc<str>,
    token: Option<&str>,
    pack_size: u64,
    targets: &Arc<[PackChunkTarget]>,
    dest_path: &Arc<Path>,
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

/// Stream a pack body into memory under the `pack_size` cap. Returns whatever
/// arrived (the caller checks it against `pack_size`); over-send is rejected here
/// the moment it is detected. Split from `fetch_pack` so a test can observe the
/// buffer's capacity - the property RESEXHAUST-001 is about - not just an error.
async fn read_pack_body(
    res: &mut reqwest::Response,
    pack_size: u64,
    url: &str,
) -> Result<Vec<u8>, CoreError> {
    // Audit L12: cap the stream at `pack_size`. Peak memory is arrived bytes,
    // never the declared size: `with_capacity(declared)` let N concurrent packs
    // commit N GiB before a body byte (RESEXHAUST-001). Initial reserve is
    // min(declared, 64 MiB); further growth is try_reserve of arrived chunks.
    let mut bytes = Vec::new();
    reserve_or_err(&mut bytes, pack_initial_reserve(pack_size), url)?;
    let mut received: u64 = 0;
    // Each body read is bounded by the default-on read-idle window (audit M4): a
    // registry that stops streaming mid-pack is cut as a retryable ReadStall instead
    // of holding the connection until the 5-minute total timeout.
    while let Some(chunk) = read_chunk_bounded(res, download_read_idle()).await? {
        received = received.saturating_add(chunk.len() as u64);
        if received > pack_size {
            // Transport length anomaly, not a wrong-bytes integrity failure - a
            // proxy/CDN that over-sends a self-consistent body can clear on retry,
            // so classify it retryable (matches the Range path's short/over-length
            // handling in chunked_downloader). Bounded by pack_size so a runaway
            // stream is cut here, well under the MAX_PACK_BYTES ceiling.
            return Err(CoreError::BadResponse(format!(
                "pack {}: body exceeds expected {pack_size} bytes (over-send)",
                redact_url(url)
            )));
        }
        reserve_or_err(&mut bytes, chunk.len(), url)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Fetch one pack blob whole, verify each carved chunk's sha256, and scatter each
/// slice to its file offset. Buffering the pack (~64 MiB) is bounded by the
/// semaphore; the length check rejects a server that over-sends before slicing.
async fn fetch_pack(
    client: &Client,
    url: &Arc<str>,
    token: Option<&str>,
    pack_size: u64,
    targets: &Arc<[PackChunkTarget]>,
    dest_path: &Arc<Path>,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    if pack_size > MAX_PACK_BYTES {
        return Err(CoreError::Integrity(format!(
            "pack {}: declares {pack_size} bytes, over the {MAX_PACK_BYTES}-byte ceiling",
            redact_url(url)
        )));
    }
    // `&**url` re-borrows the shared `Arc<str>` as the `&str` that `IntoUrl` wants.
    let mut req = client.get(&**url).timeout(CHUNK_REQUEST_TIMEOUT);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let mut res = req.send().await?;
    if !res.status().is_success() {
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("pack GET failed for {}", redact_url(url)),
        ));
    }
    let bytes = read_pack_body(&mut res, pack_size, url).await?;
    if bytes.len() as u64 != pack_size {
        return Err(CoreError::BadResponse(format!(
            "pack {}: expected {pack_size} bytes, got {}",
            redact_url(url),
            bytes.len()
        )));
    }
    // Verify + scatter on the blocking pool (audit L14). The per-chunk sha256 is
    // CPU-bound and the scatter writes are local disk - neither is async work, so
    // running them inline on the runtime starves the other up-to-32 concurrent pack
    // fetches. `bytes` (the received pack) moves in; the `Arc` clones that make the
    // closure `'static` are pointer bumps (Task C2), not per-attempt heap copies.
    let targets = Arc::clone(targets);
    let dest = Arc::clone(dest_path);
    let url = Arc::clone(url);
    let pb_owned = pb.clone();
    // A join failure (panicked scatter closure / runtime shutdown) is the
    // non-retryable `JoinFailed`: it reproduces on retry, so surfacing it as a
    // retryable `Io` would make `fetch_pack_with_retry` re-download the whole
    // ~64 MiB pack up to MAX_RETRIES times for a certain failure.
    tokio::task::spawn_blocking(move || {
        verify_and_scatter(&url, &bytes, &targets, &dest, &pb_owned)
    })
    .await
    .map_err(|join_err| CoreError::JoinFailed {
        index: None,
        source: join_err,
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_fetcher::test_support::{any_digest, chunk_target};
    use crate::incremental_hash::test_support::{pattern, reference, scratch_path, TempFileGuard};
    use std::collections::HashMap;
    // Production `assemble` no longer names `Duration` directly (the request
    // timeout moved to `crate::transport`); only the test bodies below do.
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                Err(e) => assert!(
                    e.is_cancelled(),
                    "AbortOnDrop must cancel the task, got {e:?}"
                ),
            }
        }
    }

    // An oversized/short/mis-hashed chunk must surface as the permanent
    // Integrity variant, not a retryable transport error - otherwise a
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

    // --- validate_pack_plan ---

    #[test]
    fn validate_pack_plan_accepts_in_bounds_tiling() {
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 1000,
            chunks: vec![
                chunk_target(0, 400, 0, any_digest()),
                chunk_target(400, 600, 400, any_digest()),
            ],
        }];
        assert!(validate_pack_plan(&packs, 1000).is_ok());
    }

    #[test]
    fn validate_pack_plan_rejects_out_of_bounds_chunk() {
        // A chunk at file_offset 1005 in a 1000-byte file would extend the assembled
        // file past total_size - exactly the over-length false-accept the whole-file
        // digest must never miss. It must be rejected before any fetch.
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 1100,
            chunks: vec![
                chunk_target(0, 1000, 0, any_digest()),
                chunk_target(1000, 100, 1005, any_digest()),
            ],
        }];
        assert!(matches!(
            validate_pack_plan(&packs, 1000),
            Err(CoreError::Integrity(_))
        ));
    }

    #[test]
    fn validate_pack_plan_rejects_offset_size_overflow() {
        let packs = vec![PackPlanEntry {
            url: String::new(),
            size: 10,
            chunks: vec![chunk_target(0, u64::MAX, 1, any_digest())],
        }];
        assert!(matches!(
            validate_pack_plan(&packs, u64::MAX),
            Err(CoreError::Integrity(_))
        ));
    }

    #[test]
    fn validate_pack_plan_rejects_pack_size_over_ceiling() {
        // A registry-declared pack size above MAX_PACK_BYTES must be refused BEFORE
        // fetch_pack reserves or streams it - the hostile-manifest DoS the ceiling
        // exists to bound. The chunks are otherwise in-bounds, so only the declared
        // pack.size trips the guard.
        let packs = vec![PackPlanEntry {
            url: "reg/packHuge".to_string(),
            size: MAX_PACK_BYTES + 1,
            chunks: vec![chunk_target(0, 10, 0, any_digest())],
        }];
        assert!(
            matches!(validate_pack_plan(&packs, 10), Err(CoreError::Integrity(ref m)) if m.contains("ceiling"))
        );
        // Exactly at the ceiling is still accepted (boundary, not a hostile value).
        let ok = vec![PackPlanEntry {
            url: String::new(),
            size: MAX_PACK_BYTES,
            chunks: vec![chunk_target(0, 10, 0, any_digest())],
        }];
        assert!(validate_pack_plan(&ok, 10).is_ok());
    }

    #[test]
    fn pack_initial_reserve_is_capped_not_declared() {
        // Restoring `Vec::with_capacity(declared)` (even clamped at 1 GiB) fails
        // this: 32 concurrent 1 GiB declarations would commit 32 GiB before a
        // body byte. The helper is what fetch_pack uses for the first try_reserve.
        assert_eq!(
            super::pack_initial_reserve(MAX_PACK_BYTES),
            64 * 1024 * 1024
        );
        assert_eq!(super::pack_initial_reserve(1024), 1024);
        assert_eq!(super::pack_initial_reserve(0), 0);
    }

    #[test]
    fn python_max_pack_bytes_matches_rust() {
        let py = include_str!("../../hippius_hub/constants.py");
        let Some(line) = py.lines().find(|l| l.starts_with("MAX_PACK_BYTES = ")) else {
            unreachable!("MAX_PACK_BYTES in constants.py")
        };
        let raw = line.trim_start_matches("MAX_PACK_BYTES = ");
        let Some(expr) = raw.split('#').next() else {
            unreachable!("expr")
        };
        let mut acc: u64 = 1;
        for part in expr.split('*') {
            let Ok(n) = part.trim().parse::<u64>() else {
                unreachable!("factor")
            };
            acc = acc.saturating_mul(n);
        }
        assert_eq!(acc, MAX_PACK_BYTES);
    }

    #[test]
    fn redact_url_strips_query_and_userinfo() {
        assert_eq!(
            super::redact_url(
                "https://user:sig@registry.example/v2/x/blobs/sha256:ab?X-Amz-Signature=dead"
            ),
            "https://registry.example/v2/x/blobs/sha256:ab?<redacted>"
        );
        assert_eq!(
            super::redact_url("https://registry.example/v2/x/blobs/sha256:ab"),
            "https://registry.example/v2/x/blobs/sha256:ab"
        );
        assert_eq!(super::redact_url("not a url"), "not a url");
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
                    let path = head
                        .lines()
                        .next()
                        .and_then(|l| l.split(' ').nth(1))
                        .unwrap_or("/");
                    let (status, body): (&str, &[u8]) = match routes.get(path) {
                        Some(b) => ("200 OK", b.as_slice()),
                        None => ("404 Not Found", b"".as_slice()),
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
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
    /// and the middle chunk in pack B - so the plan exercises cross-pack scatter and
    /// out-of-order arrival, not a trivial single-pack copy.
    fn three_pack_plan(base: &str, content: &[u8]) -> Vec<PackPlanEntry> {
        vec![
            PackPlanEntry {
                url: format!("{base}/packA"),
                size: 2000,
                chunks: vec![
                    chunk_target(0, 1000, 0, Sha256Digest::of(&content[0..1000])),
                    chunk_target(1000, 1000, 2000, Sha256Digest::of(&content[2000..3000])),
                ],
            },
            PackPlanEntry {
                url: format!("{base}/packB"),
                size: 1000,
                chunks: vec![chunk_target(
                    0,
                    1000,
                    1000,
                    Sha256Digest::of(&content[1000..2000]),
                )],
            },
        ]
    }

    #[tokio::test]
    async fn pack_body_capacity_tracks_arrived_bytes_not_declared_size() {
        // RESEXHAUST-001: a pack declared at the 1 GiB ceiling that delivers 2000
        // bytes must leave the buffer at the 64 MiB initial reserve, not 1 GiB.
        // Observes the real allocation, so restoring `with_capacity(declared)`
        // (even clamped to MAX_PACK_BYTES) fails here. No skip-guards: a bind or
        // client failure is a test failure, not a silent pass.
        let mut routes = HashMap::new();
        routes.insert("/pack".to_string(), vec![7u8; 2000]);
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let Ok(client) = download_client(TransportTimeouts::default()) else {
            unreachable!("download client")
        };
        let Ok(mut res) = client.get(format!("{base}/pack")).send().await else {
            unreachable!("pack GET")
        };
        let Ok(body) = read_pack_body(&mut res, MAX_PACK_BYTES, "pack").await else {
            unreachable!("short body is returned as-is; fetch_pack does the length check")
        };
        assert_eq!(body.len(), 2000);
        let Ok(ceiling) = usize::try_from(PACK_RESERVE_CEILING) else {
            unreachable!("64 MiB fits usize")
        };
        assert!(
            body.capacity() <= ceiling,
            "buffer capacity {} exceeds the 64 MiB initial reserve",
            body.capacity()
        );
    }

    #[tokio::test]
    async fn fetch_pack_rejects_over_length_body() {
        // Audit L12: a body larger than the declared pack_size must be rejected
        // under a running cap, not buffered whole. Serve 2000 bytes but declare
        // pack_size=1000 - the over-send guard trips before the carve.
        let mut routes = HashMap::new();
        routes.insert("/pack".to_string(), vec![9u8; 2000]);
        let Some(base) = serve_packs(routes).await.ok() else {
            return;
        };
        let Ok(client) = download_client(TransportTimeouts::default()) else {
            return;
        };
        let pb = ProgressBar::hidden();
        let dest = scratch_path("l12_overlen");
        let _g = TempFileGuard(dest.clone());
        // Construction plumbing for the Arc-shared fetch_pack signature (Task C2);
        // the assertions below are unchanged.
        let url: Arc<str> = format!("{base}/pack").into();
        let no_targets: Arc<[PackChunkTarget]> = Vec::new().into();
        let dest_arc: Arc<Path> = dest.as_path().into();
        let res = fetch_pack(client, &url, None, 1000, &no_targets, &dest_arc, &pb).await;
        assert!(
            matches!(res, Err(CoreError::BadResponse(ref m)) if m.contains("over-send")),
            "an over-length pack body must be rejected (bounded), got {res:?}"
        );
        // A transport length anomaly is a plausibly-transient BadResponse, so the
        // retry loop re-attempts it - distinct from a permanent Integrity mismatch.
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
        let Some(base) = serve_packs(routes).await.ok() else {
            return;
        };
        let dest = scratch_path("asm_ok");
        let _g = TempFileGuard(dest.clone());
        let Some(assembler) = PackAssembler::new(None, 4, TransportTimeouts::default()).ok() else {
            return;
        };
        let packs = three_pack_plan(&base, &content);
        // Timeout-guarded: a channel-lifecycle regression (e.g. dropping `drop(hash_tx)`)
        // would hang the hasher's recv forever, surfacing here as a failure not a hang.
        let expected = reference(&content);
        let fut = assembler.assemble(&dest, packs, Some(&expected), content.len() as u64);
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
        let Some(base) = serve_packs(routes).await.ok() else {
            return;
        };
        let dest = scratch_path("asm_bad");
        let _g = TempFileGuard(dest.clone());
        let Some(assembler) = PackAssembler::new(None, 4, TransportTimeouts::default()).ok() else {
            return;
        };
        let packs = three_pack_plan(&base, &content);
        // The bytes assemble correctly, but the declared whole-file digest disagrees:
        // the cross-pack ordering check must reject with Integrity.
        let wrong = "f".repeat(64);
        let fut = assembler.assemble(&dest, packs, Some(&wrong), content.len() as u64);
        let got = tokio::time::timeout(Duration::from_secs(30), fut).await;
        assert!(matches!(got, Ok(Err(CoreError::Integrity(_)))));
    }

    #[test]
    fn redact_url_covers_fragment_port_userinfo_and_clean_inputs() {
        // Fragment alone, query alone, userinfo alone, a port in the authority,
        // and inputs with nothing to strip - each shape is a separate branch of
        // the hand-rolled parser (no `url` crate), so each is pinned.
        assert_eq!(
            super::redact_url("https://h.example:8443/v2/x/blobs/sha256:ab#frag"),
            "https://h.example:8443/v2/x/blobs/sha256:ab?<redacted>"
        );
        assert_eq!(
            super::redact_url("https://h.example/p?a=1#frag"),
            "https://h.example/p?<redacted>"
        );
        assert_eq!(super::redact_url("https://u:p@h.example/p"), "https://h.example/p");
        assert_eq!(super::redact_url("https://u:p@h.example"), "https://h.example");
        assert_eq!(
            super::redact_url("https://h.example?X-Amz-Signature=s"),
            "https://h.example?<redacted>"
        );
        assert_eq!(super::redact_url("http://h.example/"), "http://h.example/");
        assert_eq!(super::redact_url("https://h.example/a@b/c"), "https://h.example/a@b/c");
        assert_eq!(super::redact_url(""), "");
    }

    #[test]
    fn reserve_failure_is_a_retryable_out_of_memory_io_error_with_a_redacted_url() {
        // `try_reserve(usize::MAX)` fails deterministically (capacity overflow)
        // - the same `TryReserveError` path a real allocation failure takes -
        // so this pins the mapping without needing to exhaust memory: an
        // `Io(OutOfMemory)` that the retry loop treats as transient, carrying
        // the pack URL with its presigned query stripped, and no bytes reserved.
        let mut buf: Vec<u8> = Vec::new();
        let res = super::reserve_or_err(
            &mut buf,
            usize::MAX,
            "https://h.example/v2/x/blobs/sha256:ab?X-Amz-Signature=secret",
        );
        let Err(err) = res else {
            unreachable!("reserving usize::MAX bytes cannot succeed")
        };
        assert!(err.is_retryable(), "OutOfMemory must be retryable, got {err:?}");
        match &err {
            CoreError::Io(io) => {
                assert_eq!(io.kind(), std::io::ErrorKind::OutOfMemory);
                let msg = io.to_string();
                assert!(!msg.contains("secret"), "presigned query leaked: {msg}");
                assert!(msg.contains("?<redacted>"), "URL not redacted: {msg}");
                assert!(msg.contains("cannot reserve"), "unexpected message: {msg}");
            }
            other => unreachable!("expected Io(OutOfMemory), got {other:?}"),
        }
        assert_eq!(buf.capacity(), 0, "a failed reserve must not allocate");
    }

    /// `fetch_pack` plumbing shared by the error-shape tests below.
    async fn fetch_pack_plain(
        url: &str,
        pack_size: u64,
        targets: Vec<PackChunkTarget>,
        tag: &str,
    ) -> Result<(), CoreError> {
        let Ok(client) = download_client(TransportTimeouts::default()) else {
            unreachable!("download client")
        };
        let pb = ProgressBar::hidden();
        let dest = scratch_path(tag);
        let _g = TempFileGuard(dest.clone());
        let Ok(()) = std::fs::File::create(&dest).and_then(|f| f.set_len(4000)) else {
            unreachable!("dest create")
        };
        let url: Arc<str> = url.into();
        let targets: Arc<[PackChunkTarget]> = targets.into();
        let dest_arc: Arc<Path> = dest.as_path().into();
        fetch_pack(client, &url, None, pack_size, &targets, &dest_arc, &pb).await
    }

    #[tokio::test]
    async fn fetch_pack_short_body_is_retryable_bad_response_with_redacted_url() {
        // Registry closes the body early (500 of the declared 1000 bytes): a
        // length anomaly is a retryable BadResponse, never Integrity, and the
        // message carries the URL without its presigned query.
        let mut routes = HashMap::new();
        routes.insert("/pack?X-Amz-Signature=secret".to_string(), vec![1u8; 500]);
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let res = fetch_pack_plain(
            &format!("{base}/pack?X-Amz-Signature=secret"),
            1000,
            Vec::new(),
            "short_body",
        )
        .await;
        match &res {
            Err(CoreError::BadResponse(m)) => {
                assert!(m.contains("expected 1000 bytes, got 500"), "{m}");
                assert!(!m.contains("secret"), "presigned query leaked: {m}");
                assert!(m.contains("?<redacted>"), "URL not redacted: {m}");
            }
            other => unreachable!("a short body must be BadResponse, got {other:?}"),
        }
        assert!(res.is_err_and(|e| e.is_retryable()));
    }

    #[tokio::test]
    async fn fetch_pack_over_send_message_is_redacted() {
        let mut routes = HashMap::new();
        routes.insert("/pack?X-Amz-Signature=secret".to_string(), vec![1u8; 1500]);
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let res = fetch_pack_plain(
            &format!("{base}/pack?X-Amz-Signature=secret"),
            1000,
            Vec::new(),
            "over_send_redacted",
        )
        .await;
        match &res {
            Err(CoreError::BadResponse(m)) => {
                assert!(m.contains("over-send"), "{m}");
                assert!(!m.contains("secret"), "presigned query leaked: {m}");
                assert!(m.contains("?<redacted>"), "URL not redacted: {m}");
            }
            other => unreachable!("an over-send must be BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_pack_http_error_status_is_server_error_with_redacted_url() {
        // No route registered: the pack GET 404s. Status errors keep their
        // code (so the retry policy sees 4xx vs 5xx) and redact the URL.
        let Ok(base) = serve_packs(HashMap::new()).await else {
            unreachable!("loopback bind")
        };
        let res = fetch_pack_plain(
            &format!("{base}/missing?X-Amz-Signature=secret"),
            1000,
            Vec::new(),
            "http_404",
        )
        .await;
        match &res {
            Err(CoreError::ServerError(404, m)) => {
                assert!(!m.contains("secret"), "presigned query leaked: {m}");
                assert!(m.contains("?<redacted>"), "URL not redacted: {m}");
            }
            other => unreachable!("a 404 pack GET must be ServerError(404), got {other:?}"),
        }
        assert!(
            res.is_err_and(|e| !e.is_retryable()),
            "a 404 is permanent; the retry loop must not re-GET it"
        );
    }

    #[tokio::test]
    async fn fetch_pack_refuses_a_declared_size_over_the_ceiling_before_any_request() {
        // Port 9 (discard) refuses connections: if the backstop check were
        // removed the GET would fail as a retryable transport error, not the
        // permanent Integrity below - so the test discriminates on the order.
        let res = fetch_pack_plain(
            "http://127.0.0.1:9/pack?X-Amz-Signature=secret",
            MAX_PACK_BYTES + 1,
            Vec::new(),
            "over_ceiling",
        )
        .await;
        match &res {
            Err(CoreError::Integrity(m)) => {
                assert!(m.contains("over the"), "{m}");
                assert!(!m.contains("secret"), "presigned query leaked: {m}");
            }
            other => unreachable!("an over-ceiling declaration must be Integrity, got {other:?}"),
        }
        assert!(res.is_err_and(|e| !e.is_retryable()));
    }

    #[tokio::test]
    async fn fetch_pack_accepts_a_pack_declared_exactly_at_the_ceiling() {
        // The cap is inclusive: exactly MAX_PACK_BYTES passes the backstop and
        // reaches the GET (which then fails on the short body, a BadResponse -
        // proof the request was made rather than refused up front).
        let mut routes = HashMap::new();
        routes.insert("/pack".to_string(), vec![1u8; 10]);
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let res = fetch_pack_plain(&format!("{base}/pack"), MAX_PACK_BYTES, Vec::new(), "at_ceiling").await;
        assert!(
            matches!(res, Err(CoreError::BadResponse(_))),
            "exactly-at-cap must reach the GET, got {res:?}"
        );
    }

    #[tokio::test]
    async fn fetch_pack_of_size_zero_with_no_targets_succeeds_on_an_empty_body() {
        let mut routes = HashMap::new();
        routes.insert("/empty".to_string(), Vec::new());
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let res = fetch_pack_plain(&format!("{base}/empty"), 0, Vec::new(), "size_zero").await;
        assert!(res.is_ok(), "a zero-byte pack with no targets is valid, got {res:?}");
    }

    /// A file where one 1000-byte chunk repeats three times around a 500-byte
    /// tail, packed by the intra-file self-dedup as ONE occurrence: the pack
    /// is `repeat + tail` (1500 bytes) and the plan carries four targets, three
    /// of which share pack range `[0, 1000)` at different file offsets.
    fn shared_range_fixture(base: &str) -> (Vec<u8>, Vec<u8>, Vec<PackPlanEntry>) {
        let repeat = pattern(1000);
        let tail = vec![0x5Au8; 500];
        let content = [
            repeat.as_slice(),
            tail.as_slice(),
            repeat.as_slice(),
            repeat.as_slice(),
        ]
        .concat();
        let pack = [repeat.as_slice(), tail.as_slice()].concat();
        let hx = Sha256Digest::of(&repeat);
        let hy = Sha256Digest::of(&tail);
        let plan = vec![PackPlanEntry {
            url: format!("{base}/shared"),
            size: 1500,
            chunks: vec![
                chunk_target(0, 1000, 0, hx),
                chunk_target(1000, 500, 1000, hy),
                chunk_target(0, 1000, 1500, hx),
                chunk_target(0, 1000, 2500, hx),
            ],
        }];
        (content, pack, plan)
    }

    async fn assemble_shared(pack_served: Option<Vec<u8>>, tag: &str) -> (Result<Option<String>, CoreError>, Vec<u8>, std::path::PathBuf) {
        let mut routes = HashMap::new();
        if let Some(body) = pack_served {
            routes.insert("/shared".to_string(), body);
        }
        let Ok(base) = serve_packs(routes).await else {
            unreachable!("loopback bind")
        };
        let (content, _, plan) = shared_range_fixture(&base);
        let dest = scratch_path(tag);
        let Ok(assembler) = PackAssembler::new(None, 4, TransportTimeouts::default()) else {
            unreachable!("assembler")
        };
        let expected = reference(&content);
        let fut = assembler.assemble(&dest, plan, Some(&expected), content.len() as u64);
        let Ok(res) = tokio::time::timeout(Duration::from_secs(30), fut).await else {
            unreachable!("assemble must settle within the retry budget")
        };
        (res, content, dest)
    }

    #[tokio::test]
    async fn assemble_scatters_a_thrice_shared_pack_range_to_every_file_offset() {
        let (_, pack, _) = shared_range_fixture("http://unused");
        let (res, content, dest) = assemble_shared(Some(pack), "shared_ok").await;
        let _g = TempFileGuard(dest.clone());
        let Ok(Some(digest)) = res else {
            unreachable!("a deduped pack must reconstruct the file, got {res:?}")
        };
        assert_eq!(digest, reference(&content));
        let Ok(got) = std::fs::read(&dest) else {
            unreachable!("read back")
        };
        assert_eq!(got, content, "every occurrence of the repeated chunk must be written");
    }

    #[tokio::test]
    async fn assemble_rejects_a_corrupt_shared_pack_as_permanent_integrity() {
        // One flipped byte inside the shared range: the FIRST target that
        // carves it fails its digest; the error is Integrity (permanent, no
        // retry) wrapped in ChunkFailed by the fan-out.
        let (_, mut pack, _) = shared_range_fixture("http://unused");
        pack[10] ^= 0xFF;
        let (res, _, dest) = assemble_shared(Some(pack), "shared_corrupt").await;
        let _g = TempFileGuard(dest);
        match &res {
            Err(CoreError::ChunkFailed { source, .. }) => {
                assert!(
                    matches!(**source, CoreError::Integrity(_)),
                    "corrupt bytes must be Integrity, got {source:?}"
                );
            }
            other => unreachable!("expected ChunkFailed(Integrity), got {other:?}"),
        }
        assert!(res.is_err_and(|e| !e.is_retryable()));
    }

    #[tokio::test]
    async fn assemble_rejects_a_short_shared_pack_as_bad_response_after_retries() {
        // 1400 of the declared 1500 bytes arrive: retryable BadResponse, walked
        // through the pack retry ladder (the server keeps serving short), then
        // surfaced as ChunkFailed(BadResponse) - not Integrity, not a hang.
        let (_, pack, _) = shared_range_fixture("http://unused");
        let (res, _, dest) = assemble_shared(Some(pack[..1400].to_vec()), "shared_short").await;
        let _g = TempFileGuard(dest);
        match &res {
            Err(CoreError::ChunkFailed { source, .. }) => {
                assert!(
                    matches!(&**source, CoreError::BadResponse(m) if m.contains("expected 1500 bytes, got 1400")),
                    "a short pack must be BadResponse, got {source:?}"
                );
            }
            other => unreachable!("expected ChunkFailed(BadResponse), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assemble_rejects_an_over_sent_shared_pack_as_bad_response() {
        let (_, mut pack, _) = shared_range_fixture("http://unused");
        pack.extend_from_slice(&[0u8; 100]);
        let (res, _, dest) = assemble_shared(Some(pack), "shared_over").await;
        let _g = TempFileGuard(dest);
        match &res {
            Err(CoreError::ChunkFailed { source, .. }) => {
                assert!(
                    matches!(&**source, CoreError::BadResponse(m) if m.contains("over-send")),
                    "an over-sent pack must be BadResponse, got {source:?}"
                );
            }
            other => unreachable!("expected ChunkFailed(BadResponse), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assemble_rejects_a_missing_pack_as_permanent_server_error() {
        // The registry 404s the pack (a GC'd blob): a permanent status, so the
        // fan-out fails on the first attempt without walking the retry ladder,
        // and the status code survives the ChunkFailed wrapping.
        let (res, _, dest) = assemble_shared(None, "shared_missing").await;
        let _g = TempFileGuard(dest);
        match &res {
            Err(CoreError::ChunkFailed { source, .. }) => {
                assert!(
                    matches!(**source, CoreError::ServerError(404, _)),
                    "a missing pack must be ServerError(404), got {source:?}"
                );
            }
            other => unreachable!("expected ChunkFailed(ServerError), got {other:?}"),
        }
        assert!(res.is_err_and(|e| !e.is_retryable()));
    }
}
