//! Whole-file digest resolution for the Range downloader: prefer the digest
//! the background incremental hasher computed during the download, fall back
//! to the authoritative sequential re-read.

use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::path::Path;

use crate::error::CoreError;
use crate::incremental_hash::HasherTask;
use crate::transport::IO_READ_BUFFER;

/// Which route produced the verified whole-file digest. The digest value is
/// identical either way (both hash the same on-disk bytes); the route exists so
/// `download` can record - and tests can observe - whether the overlapped
/// incremental pass actually replaced the full re-read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DigestRoute {
    /// The background incremental hasher covered the file during the download.
    Incremental,
    /// The hasher could not cover the file; a full sequential re-read produced it.
    FullReread,
}

/// Resolve the whole-file digest after every chunk has landed: prefer the digest
/// the background hasher computed (its work overlapped the download), else fall
/// back to the full sequential re-read - the mirror of the pack path's
/// `chunk_fetcher::verify_file_digest`, minus the expected-digest comparison
/// (this path RETURNS the digest; Python compares it). A `JoinError` means the
/// hasher task panicked - surfaced as the non-retryable `JoinFailed` rather than
/// masked by a silent fallback, exactly as the pack path argues. `None` for
/// `hasher_task` (unreachable from `download`, which spawns the hasher whenever
/// it verifies) also re-reads, keeping the function total without a panic.
pub(super) async fn resolve_verified_digest(
    hasher_task: Option<HasherTask>,
    dest_path: &Path,
    content_length: u64,
) -> Result<(String, DigestRoute), CoreError> {
    if let Some(task) = hasher_task {
        match task.await {
            Ok(Some(digest)) => return Ok((digest, DigestRoute::Incremental)),
            Ok(None) => {}
            Err(join_err) => {
                return Err(CoreError::JoinFailed {
                    index: None,
                    source: join_err,
                })
            }
        }
    }

    // Fallback: the pre-C1 verify pass, bar and all. Both routes hash the same
    // on-disk bytes, so this is a slower path to an identical answer.
    let pb_hash = ProgressBar::new(content_length);
    // Same rationale as the download-phase bar: static literal template.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb_hash.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.magenta/red}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .expect("indicatif template is static and infallible")
        .progress_chars("=>-"));
    pb_hash.set_message("Verifying SHA256");

    let hash = compute_sha256(dest_path, &pb_hash).await?;
    pb_hash.finish_with_message("Verified");
    Ok((hash, DigestRoute::FullReread))
}

/// Test-build route probe: bump the incremental counter so the E2E verify test
/// can assert the overlap path ran (see `INCREMENTAL_VERIFIES`).
#[cfg(test)]
pub(super) fn record_verify_route(route: DigestRoute) {
    if route == DigestRoute::Incremental {
        INCREMENTAL_VERIFIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Production builds carry no probe; the route is informational only.
#[cfg(not(test))]
pub(super) fn record_verify_route(_route: DigestRoute) {}

/// Compute the SHA256 of the final file in a single sequential read-pass.
///
/// Audit U1: same justification as `uploader::hash_file_async` - sha2's
/// digest loop is CPU-bound and would block a tokio worker for seconds on a
/// multi-GB verify, starving the parallel download tasks the runtime is
/// trying to drain. `spawn_blocking` parks the work on the dedicated
/// blocking pool instead. The `ProgressBar` is `Send + Sync` (Arc-internal
/// per indicatif docs), so cloning it into the closure for tick updates is
/// safe - `pb.inc` is thread-safe and now runs from the blocking thread.
///
/// The double `?` mirrors `hash_file_async`: outer `?` maps
/// `JoinError -> CoreError::JoinFailed` (non-retryable - a panicked hash
/// closure reproduces on retry), inner `?` propagates `io::Error` from
/// the closure body.
async fn compute_sha256(path: &Path, pb: &ProgressBar) -> Result<String, CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
    let pb = pb.clone(); // indicatif::ProgressBar clones cheaply via internal Arc.
    tokio::task::spawn_blocking(move || -> Result<String, CoreError> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; IO_READ_BUFFER];

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
    .map_err(|join_err| CoreError::JoinFailed {
        index: None,
        source: join_err,
    })?
}

/// Count of downloads whose whole-file digest came from the INCREMENTAL hasher
/// (not the full re-read fallback). Test-only observability: the E2E verify test
/// asserts this advanced across its download, proving the overlap path actually
/// ran rather than silently falling back and still producing the right digest.
#[cfg(test)]
// INVARIANT: the `> before` assertion in the e2e test below is sound only while
// exactly one test drives a verify=true `download()`; a second concurrent such
// test could mask a fallback. Keep new verify-route tests on `resolve_verified_digest`.
static INCREMENTAL_VERIFIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Task C1: overlap the Range path's whole-file verify with the download, mirroring
// the pack path (`chunk_fetcher::PackAssembler::assemble`). Kept in its own test
// module: these tests drive real HTTP 206 sockets plus the hasher lifecycle,
// distinct from the pure chunk-math tests in `plan`.
#[cfg(test)]
mod incremental_verify_tests {
    use super::*;
    use crate::chunked_downloader::test_server::serve_ranges;
    use crate::chunked_downloader::ChunkedDownloader;
    use crate::incremental_hash::spawn_incremental_hasher;
    use crate::incremental_hash::test_support::{pattern, reference, scratch_path, TempFileGuard};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[tokio::test]
    async fn download_verify_overlaps_hash_incrementally() {
        // A multi-chunk verify download must yield the correct whole-file digest
        // FROM THE INCREMENTAL HASHER - not via the fallback full re-read. The
        // digest alone cannot distinguish the two routes (the fallback is also
        // correct), so the route counter is the observation: it advances only when
        // `download` used the incremental digest. 100 bytes at chunk_size 8 = 13
        // chunks (last one short), exercising out-of-order completion + refill.
        let body = pattern(100);
        let Ok((base, server)) = serve_ranges(body.clone()).await else {
            unreachable!("loopback range server binds on an ephemeral port");
        };
        let Ok(dl) = ChunkedDownloader::new(
            format!("{base}/blob"),
            None,
            Some(8),
            Some(body.len() as u64),
            crate::chunk_fetcher::TransportTimeouts::default(),
        ) else {
            unreachable!("downloader builds")
        };
        let dest = scratch_path("dl_verify_e2e");
        let _g = TempFileGuard(dest.clone());

        let before = INCREMENTAL_VERIFIES.load(Ordering::Relaxed);
        let out = tokio::time::timeout(Duration::from_secs(20), dl.download(&dest, true)).await;
        server.abort();

        let Ok(Ok(digest)) = out else {
            unreachable!("13-chunk verify download must complete, got {out:?}")
        };
        assert_eq!(digest, Some(reference(&body)));
        let got = std::fs::read(&dest).ok();
        assert_eq!(got.as_deref(), Some(body.as_slice()));
        assert!(
            INCREMENTAL_VERIFIES.load(Ordering::Relaxed) > before,
            "the whole-file digest must come from the incremental hasher \
             overlapped with the download, not a second sequential read"
        );
    }

    #[tokio::test]
    async fn resolve_prefers_incremental_without_reread() {
        // Some(digest) from the hasher is returned directly, tagged Incremental.
        // `missing` never exists, so a stray fallback re-read would error and
        // fail this assertion - the same observation trick as the pack path's
        // `verify_file_digest_prefers_incremental_without_reread`.
        let missing = scratch_path("dl_verify_fast");
        let digest = reference(b"payload");
        let d = digest.clone();
        let task = tokio::spawn(async move { Some(d) });
        assert_eq!(
            resolve_verified_digest(Some(task), &missing, 7).await.ok(),
            Some((digest, DigestRoute::Incremental))
        );
    }

    #[tokio::test]
    async fn resolve_falls_back_to_reread_when_hasher_bails() {
        // Drive the REAL hasher into its documented bail: signal only [0, 50) of a
        // 100-byte file, then close the channel. The coverage shortfall yields
        // `None`, and the fallback full re-read must still produce the correct
        // digest - proving the fallback is reachable and correct end to end.
        let content = pattern(100);
        let path = scratch_path("dl_verify_fallback");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::write(&path, &content);
        assert!(wrote.is_ok());

        let (tx, task) = spawn_incremental_hasher(&path, content.len() as u64, true);
        if let Some(sender) = &tx {
            let _ = sender.send(vec![(0, 50)]); // the [50, 100) extent never arrives
        }
        drop(tx);

        assert_eq!(
            resolve_verified_digest(task, &path, content.len() as u64)
                .await
                .ok(),
            Some((reference(&content), DigestRoute::FullReread))
        );
    }

    #[tokio::test]
    async fn resolve_none_task_reads_from_disk() {
        // No hasher task at all (the total-function arm): the digest comes from
        // the full re-read, mirroring `verify_file_digest_none_task_reads_from_disk`.
        let content = pattern(2048);
        let path = scratch_path("dl_verify_notask");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::write(&path, &content);
        assert!(wrote.is_ok());
        assert_eq!(
            resolve_verified_digest(None, &path, content.len() as u64)
                .await
                .ok(),
            Some((reference(&content), DigestRoute::FullReread))
        );
    }

    #[tokio::test]
    async fn resolve_surfaces_hasher_join_error_as_join_failed() {
        // A JoinError (hasher task panicked/cancelled) surfaces as the honest,
        // non-retryable JoinFailed - never masked by a silent fallback re-read
        // (mirror of the pack path's classification). Aborting a pending task
        // yields the JoinError without a panic! macro (which the crate denies).
        let missing = scratch_path("dl_verify_join");
        let task: HasherTask =
            tokio::spawn(async { std::future::pending::<Option<String>>().await });
        task.abort();
        let res = resolve_verified_digest(Some(task), &missing, 1).await;
        match res {
            Err(err @ CoreError::JoinFailed { index: None, .. }) => {
                assert!(
                    !err.is_retryable(),
                    "a hasher join failure must classify permanent, got retryable: {err}"
                );
            }
            other => unreachable!("expected JoinFailed {{ index: None }}, got {other:?}"),
        }
    }
}
