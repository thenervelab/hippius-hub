//! Verify-and-scatter of fetched pack bytes plus the whole-file digest
//! resolution (incremental result preferred, sequential re-read fallback).

use indicatif::ProgressBar;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::SeekFrom;

use crate::chunk_fetcher::assemble::PackChunkTarget;
use crate::digest::Sha256Digest;
use crate::error::CoreError;
use crate::incremental_hash::HasherTask;
use crate::transport::IO_READ_BUFFER;

/// Resolve the whole-file digest once every pack has landed: prefer the digest the
/// background hasher computed (its work overlapped the download), else fall back to
/// a full re-read when the incremental pass could not cover the file in order. Both
/// hash the same on-disk bytes, so the fallback is a slower route to an identical
/// answer. A `JoinError` means the hasher task panicked - surfaced as the
/// non-retryable `JoinFailed` rather than masked (the fn is written not to panic,
/// so it is effectively unreachable, but a silent fallback would hide a real
/// defect, and a panic reproduces on retry). Errors on a digest mismatch, which is
/// exactly the cross-pack ordering failure this whole-file check exists to catch.
pub(super) async fn verify_file_digest(
    hasher_task: Option<HasherTask>,
    dest: &Path,
    expected_file: &str,
) -> Result<String, CoreError> {
    let got = match hasher_task {
        Some(task) => match task.await {
            Ok(Some(digest)) => digest,
            Ok(None) => compute_sha256(dest).await?,
            Err(join_err) => {
                return Err(CoreError::JoinFailed {
                    index: None,
                    source: join_err,
                })
            }
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

/// Verify each carved chunk's sha256 against `bytes` and scatter its slice to the
/// file offset. Runs on the blocking pool (audit L14): hashing is CPU-bound and the
/// writes are local disk, so this does no async work and must not sit on the async
/// runtime. A digest mismatch or out-of-range chunk is a PERMANENT `Integrity` error
/// (a content-addressed blob serves the same wrong bytes on retry, and an
/// out-of-bounds range is a bad plan) - distinct from the transport length anomalies
/// in `fetch_pack`, which are the retryable `BadResponse`. A corrupt/mis-placed pack
/// must never be written past its bounds.
pub(super) fn verify_and_scatter(
    url: &str,
    bytes: &[u8],
    targets: &[PackChunkTarget],
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), CoreError> {
    use std::io::{Seek, Write};

    let mut file = std::fs::OpenOptions::new().write(true).open(dest_path)?;
    for PackChunkTarget {
        offset_in_pack,
        size,
        file_offset,
        expected_sha256: expected,
    } in targets
    {
        let start = usize::try_from(*offset_in_pack).map_err(|_| {
            CoreError::Integrity(format!("pack offset {offset_in_pack} exceeds usize"))
        })?;
        let end =
            start
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
        // Binary 32-byte compare on the hot path; hex renders (via Display) only
        // inside the failure message, so the match arm allocates nothing.
        let got = Sha256Digest::of(slice);
        if got != *expected {
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
pub(crate) async fn compute_sha256(path: &Path) -> Result<String, CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
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
        }
        Ok(hex::encode(hasher.finalize()))
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
    use crate::chunk_fetcher::test_support::chunk_target;
    use crate::incremental_hash::test_support::{pattern, reference, scratch_path, TempFileGuard};
    use std::io::Write as _;

    #[test]
    fn verify_and_scatter_writes_chunks_and_rejects_bad_digest() {
        // L14: the verify+scatter loop (moved onto the blocking pool) must place
        // each chunk at its file offset only after its sha256 matches the expected
        // digest, and reject a mismatched chunk as a (retryable) Integrity error.
        use std::io::Read;
        let pb = ProgressBar::hidden();
        let (a, b) = (b"AAAA".as_slice(), b"BB".as_slice()); // 4 + 2 bytes
        let pack: Vec<u8> = [a, b].concat();
        let ha = Sha256Digest::of(a);
        let hb = Sha256Digest::of(b);
        // (offset_in_pack, size, file_offset, expected_digest): scatter A->0, B->4.
        let good = vec![chunk_target(0, 4, 0, ha), chunk_target(4, 2, 4, hb)];

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
        let bad = vec![chunk_target(0, 4, 0, hb)];
        assert!(matches!(
            verify_and_scatter("u", &pack, &bad, &path, &pb),
            Err(CoreError::Integrity(_))
        ));
        let _ = std::fs::remove_file(&path);
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
        assert_eq!(
            verify_file_digest(Some(task), &missing, &expected)
                .await
                .ok(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn verify_file_digest_falls_back_to_reread_when_incremental_none() {
        let content = pattern(2048);
        let path = scratch_path("verify_fallback");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        let task = tokio::spawn(async { None });
        assert_eq!(
            verify_file_digest(Some(task), &path, &reference(&content))
                .await
                .ok(),
            Some(reference(&content))
        );
    }

    #[tokio::test]
    async fn verify_file_digest_none_task_reads_from_disk() {
        let content = pattern(2048);
        let path = scratch_path("verify_notask");
        let _g = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        assert_eq!(
            verify_file_digest(None, &path, &reference(&content))
                .await
                .ok(),
            Some(reference(&content))
        );
    }

    #[tokio::test]
    async fn verify_file_digest_rejects_mismatch() {
        // Incremental yields a digest that disagrees with `expected` -> Integrity, not
        // an accept. Guards the `got != expected_file` comparison against inversion.
        let missing = scratch_path("verify_mismatch");
        let task = tokio::spawn(async { Some("a".repeat(64)) });
        let expected = "b".repeat(64);
        assert!(matches!(
            verify_file_digest(Some(task), &missing, &expected).await,
            Err(CoreError::Integrity(_))
        ));
    }

    #[tokio::test]
    async fn verify_file_digest_surfaces_hasher_join_error_as_join_failed() {
        // A JoinError (task cancelled/panicked) surfaces as the honest
        // `JoinFailed { index: None }` - NOT `Io`, which `is_retryable` would
        // classify transient: a panicked hasher closure reproduces identically on
        // retry, so the error must classify permanent. Aborting a pending task
        // yields the JoinError without a panic! macro (which the crate denies).
        let missing = scratch_path("verify_join");
        let task: HasherTask =
            tokio::spawn(async { std::future::pending::<Option<String>>().await });
        task.abort();
        let res = verify_file_digest(Some(task), &missing, &"c".repeat(64)).await;
        match res {
            Err(err @ CoreError::JoinFailed { index: None, .. }) => {
                assert!(
                    !err.is_retryable(),
                    "a join failure must classify permanent, got retryable: {err}"
                );
            }
            other => unreachable!("expected JoinFailed {{ index: None }}, got {other:?}"),
        }
    }
}
