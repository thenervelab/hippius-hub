//! Incremental whole-file SHA-256, overlapped with a parallel download.
//!
//! Extracted from `chunk_fetcher.rs` (Phase C1) so BOTH download paths - the
//! pack assembler and the legacy Range downloader - can fold the whole-file
//! digest together while chunks are still landing, instead of a second full
//! sequential read after the download completes. The contract is best-effort:
//! `incremental_hash` yields `Some(digest)` only when it covered exactly
//! `[0, total_size)` in order, and `None` otherwise, so a caller always keeps
//! its authoritative full-read fallback and correctness never depends on this
//! fast path.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};

use crate::chunk_fetcher::VERIFY_READ_BUFFER;

/// Extents `(file_offset, size)` one completed pack contributes to the whole-file
/// hasher - the payload of the incremental-hash channel. One byte of the file
/// belongs to exactly one extent, so the extents across all packs tile the file.
pub(crate) type HashSignal = Vec<(u64, u64)>;

/// The join handle for the background incremental hasher; yields the whole-file
/// digest, or `None` when the incremental pass could not cover the file (see
/// `incremental_hash`).
pub(crate) type HasherTask = tokio::task::JoinHandle<Option<String>>;

/// What `spawn_incremental_hasher` hands back: the sender each pack signals
/// completion on and the task handle to await, or `(None, None)` when the caller
/// requested no whole-file digest.
pub(crate) type IncrementalHash = (Option<Sender<HashSignal>>, Option<HasherTask>);

/// Spawn the background incremental hasher (see `incremental_hash`) when the caller
/// asked for whole-file verification, handing back the sender packs signal
/// completion on and the task handle to await. Returns `(None, None)` when no digest
/// was requested, so the fan-out and finalize paths stay uniform either way.
pub(crate) fn spawn_incremental_hasher(
    dest: &Path,
    total_size: u64,
    verify: bool,
) -> IncrementalHash {
    if !verify {
        return (None, None);
    }
    let (tx, rx) = std::sync::mpsc::channel::<HashSignal>();
    let hash_path = dest.to_path_buf();
    let task = tokio::task::spawn_blocking(move || incremental_hash(&rx, &hash_path, total_size));
    (Some(tx), Some(task))
}

/// Whole-file SHA-256 folded together incrementally from packs as they land, used
/// to prove chunk *ordering* across packs (the one property per-chunk digests
/// cannot). Runs on the blocking pool for the same reason as `compute_sha256`: the
/// digest loop is CPU-bound and would starve the runtime's fetch tasks inline.
///
/// Each `recv` carries the `(file_offset, size)` extents one completed pack wrote.
/// `pending` (keyed by start offset) is the reorder buffer for out-of-order packs;
/// it holds only metadata - the bytes are already on disk - so it stays a few bytes
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
/// past `total_size`); any shortfall - an abort closing the channel early, a coverage
/// gap, a read error, or an out-of-bounds extent left in `pending` - yields `None` so
/// the caller re-reads. It therefore hashes the identical bytes `compute_sha256`
/// would and can only ever be a faster route to the same digest, never a different
/// verdict.
pub(crate) fn incremental_hash(
    rx: &Receiver<HashSignal>,
    path: &Path,
    total_size: u64,
) -> Option<String> {
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
                return None; // file shorter than the extents claimed - give up, re-read
            }
            hasher.update(&buf[..n]);
            hashed += n as u64;
        }
    }

    // Channel closed: every pack task has finished. Require BOTH that the hash
    // covered exactly `total_size` AND that `pending` drained. A leftover extent
    // means a pack wrote beyond the contiguous [0, total_size) region - an
    // out-of-bounds placement leaves the on-disk file longer than total_size - so
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

/// Scratch-file helpers shared by this module's tests and `chunk_fetcher`'s
/// (both exercise the hasher contract against real on-disk files). `pub(crate)`
/// under `cfg(test)` so they exist only for the test build.
#[cfg(test)]
pub(crate) mod test_support {
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    /// RAII guard: removes the scratch file on drop so a failing assertion (which
    /// returns early) still cleans up. Ignoring the remove error is intentional -
    /// a leftover temp file is harmless and there is nothing to recover.
    pub(crate) struct TempFileGuard(pub(crate) PathBuf);
    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Distinct scratch path per call (process id + monotonic counter) so parallel
    /// tests never collide.
    pub(crate) fn scratch_path(tag: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hippius_ihash_{tag}_{}_{seq}.bin",
            std::process::id()
        ))
    }

    /// A varied (non-constant) byte pattern so a mis-ordering across a chunk
    /// boundary changes the digest - a constant fill would hide reorder bugs.
    pub(crate) fn pattern(n: usize) -> Vec<u8> {
        let seed = b"HIPPIUS-hub-chunked-v2";
        (0..n).map(|i| seed[i % seed.len()]).collect()
    }

    pub(crate) fn reference(content: &[u8]) -> String {
        hex::encode(Sha256::digest(content))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{pattern, reference, scratch_path, TempFileGuard};
    use super::*;
    use std::io::Write as _;

    // The contract under test: for ANY file content, ANY chunk tiling, and ANY
    // order the completion signals arrive in, `incremental_hash` yields the plain
    // sequential SHA-256 of the file - or `None` (never a wrong digest) when it
    // cannot cover the file. Tests avoid `unwrap`/`expect` (crate-wide `deny`) by
    // returning `io::Result` and using `?`.

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
        // covered: must yield None (-> caller re-reads), not a partial digest.
        let content = pattern(1000);
        let msgs = vec![vec![(0, 400)], vec![(400, 300)]];
        assert_eq!(drive(&content, &msgs).ok(), Some(None));
    }

    #[test]
    fn incremental_empty_file_hashes_empty() {
        // total_size == 0: no extents, channel closes immediately, hashed == 0 ==
        // total_size -> the SHA-256 of the empty input.
        let content: Vec<u8> = Vec::new();
        assert_eq!(drive(&content, &[]).ok(), Some(Some(reference(&content))));
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

    #[tokio::test]
    async fn incremental_agrees_with_compute_sha256() {
        // Pin that the incremental path yields the byte-for-byte same digest as the
        // authoritative re-read it is allowed to replace.
        let content = pattern(5000);
        let path = scratch_path("cmp");
        let _guard = TempFileGuard(path.clone());
        let wrote = std::fs::File::create(&path).and_then(|mut f| f.write_all(&content));
        assert!(wrote.is_ok());
        let authoritative = crate::chunk_fetcher::compute_sha256(&path).await.ok();
        assert!(authoritative.is_some());
        let (tx, rx) = std::sync::mpsc::channel::<Vec<(u64, u64)>>();
        let _ = tx.send(vec![(0, content.len() as u64)]);
        drop(tx);
        assert_eq!(
            incremental_hash(&rx, &path, content.len() as u64),
            authoritative
        );
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
