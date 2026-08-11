use fastcdc::v2020::StreamCDC;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};

use crate::error::CoreError;

/// Per-chunk `(sha256_hex, offset, length)` in file order - the plan a chunked
/// upload works from (offset re-reads the range; digest dedups and addresses).
pub type ChunkList = Vec<(String, u64, u64)>;

/// `FastCDC` average chunk size bounds - pinned to the library's own average
/// range `[AVERAGE_MIN, AVERAGE_MAX]` = `[256 B, 4 MiB]`. The average is the wire
/// contract (see the chunked-artifact plan); min = avg/4 and max = avg*4 are the
/// standard normalized-chunking ratios. Those derivations must ALSO stay under
/// `FastCDC`'s *separate* `MINIMUM_MAX` (1 MiB) and `MAXIMUM_MAX` (16 MiB) ceilings
/// or `StreamCDC::new` panics - and they do so exactly over this interval: at the
/// 4 MiB ceiling min = 1 MiB = `MINIMUM_MAX` and max = 16 MiB = `MAXIMUM_MAX`, the
/// caps themselves. So `[256 B, 4 MiB]` is the largest average range that can
/// never panic. The old 256 MiB cap let averages like the 64 MiB default through
/// to a `StreamCDC::new` panic (min = 16 MiB > `MINIMUM_MAX`); an out-of-range
/// average is now surfaced as a caller error, never clamped and never panicked.
/// (`cdc_bounds_track_fastcdc_limits` asserts these equal the crate constants so a
/// `FastCDC` bump can't silently reopen the panic.)
const CDC_MIN_AVG: u64 = 256; // == fastcdc::v2020::AVERAGE_MIN
const CDC_MAX_AVG: u64 = 4 * 1024 * 1024; // == fastcdc::v2020::AVERAGE_MAX (4 MiB)

/// Cap on per-chunk SHA-256 worker threads in the chunk pipeline. Together
/// with `HASH_QUEUE_DEPTH` this bounds transient CHUNK-BYTES memory: at most
/// `HASH_QUEUE_DEPTH + MAX_HASH_WORKERS` = 8 chunks are in flight off the
/// producer at once, and a chunk is at most 4x the average (16 MiB at the
/// 4 MiB default) -> <= 128 MiB worst case, ~32 MiB typical (avg-sized
/// chunks). Four workers saturate the per-chunk hash side: the producer's
/// own whole-file SHA-256 pass runs at the same per-core rate, so it - not
/// the pool - is the pipeline floor beyond that. Everything downstream of
/// the workers (done channel, collector reorder map, output batches) carries
/// digests and offsets only; see [`CHUNK_BATCH`] for that metadata bound.
const MAX_HASH_WORKERS: usize = 4;

/// Bounded depth of the producer -> hash-worker queue; see
/// [`MAX_HASH_WORKERS`] for the combined memory bound.
const HASH_QUEUE_DEPTH: usize = 4;

/// Chunks per batch on the collector -> consumer output channel. Batches are
/// METADATA only (hex digest `String` + offset + length, ~100 B per triple;
/// the chunk bytes are dropped at hash time), so a batch is ~6 KiB and the
/// bounded output queue holds at most [`BATCH_QUEUE_DEPTH`] of them plus the
/// one being built - KiB-scale regardless of file size. The collector's
/// out-of-order map and the done channel hold digest metadata of the same
/// shape; their worst case is the full chunk list (exactly what B1 buffered
/// anyway), reached only if the consumer stops draining or one worker lags
/// the whole file.
pub(super) const CHUNK_BATCH: usize = 64;

/// Bound of the output batch channel (collector -> consumer); see
/// [`CHUNK_BATCH`] for why this queue is KiB-scale.
pub(super) const BATCH_QUEUE_DEPTH: usize = 2;

/// One chunk's bytes plus the metadata the hash worker echoes back alongside
/// the digest. The echo makes the collector single-input (one [`HashedChunk`]
/// stream to reorder) instead of joining a separate metas channel by index -
/// fewer channels, no join bookkeeping, and the 24 extra bytes ride a message
/// that already moves the chunk `Vec`.
struct HashJob {
    idx: usize,
    offset: u64,
    len: u64,
    data: Vec<u8>,
}

/// A hashed chunk on its way back from the worker pool, still in completion
/// (not file) order.
struct HashedChunk {
    idx: usize,
    offset: u64,
    len: u64,
    digest: [u8; 32],
}

/// Output protocol of the chunk pipeline: zero or more file-ordered `Batch`es
/// followed by exactly one terminal message (`Done` or `Fail`). The
/// whole-file hex rides `Done` so no side channel is needed.
#[derive(Debug)]
pub(super) enum PipelineMsg {
    /// File-ordered `(sha256_hex, offset, length)` triples, at most one batch
    /// size per message, offsets contiguous with the previous batch.
    Batch(Vec<(String, u64, u64)>),
    /// End of stream: every chunk was emitted; carries the whole-file digest.
    Done { whole_hex: String },
    /// Terminal failure (CDC/IO error or a missing digest); nothing follows.
    Fail(CoreError),
}

/// Producer terminal status. Sent on the control channel BEFORE the producer
/// lets its work sender drop - see [`produce_chunks`] for why that ordering
/// makes the collector's post-drain `recv` non-blocking.
enum ProducerEnd {
    /// The reader was drained; `fed` chunks entered the pipeline.
    Finished { fed: usize, whole_hex: String },
    /// CDC/IO failed mid-file; outranks any missing-digest gap.
    Failed(CoreError),
}

/// Chunk a file with `FastCDC` and hash each chunk plus the whole file in one
/// streaming pass (bounded memory - `StreamCDC` never loads the whole file).
///
/// Returns `(whole_file_sha256_hex, [(chunk_sha256_hex, offset, length)])` in
/// file order. The offsets let the caller re-read each chunk's byte range for a
/// parallel upload; the digests drive `HEAD`-dedup and content-addressing.
/// Determinism: for a fixed `avg_size` the boundaries are a pure function of the
/// bytes, so identical files chunk identically and dedup - hence `avg_size` is
/// pinned by the caller, not tuned per upload.
pub fn chunk_and_hash(path: &Path, avg_size: u64) -> Result<(String, ChunkList), CoreError> {
    chunk_and_hash_reader(std::fs::File::open(path)?, avg_size)
}

/// Reader-based core of [`chunk_and_hash`], split out so tests can drive it from
/// an in-memory `Cursor` (no temp file, no I/O `unwrap`). Semantics are
/// identical: `StreamCDC` yields the same boundaries whether the source is a
/// file or a cursor over the same bytes.
///
/// Per-chunk hashing WAS 94% of the measured phase-1 cost when everything ran
/// serially, so the work fans out through [`run_chunk_pipeline`]: a producer
/// thread runs CDC + the whole-file hash (the gear-hash scan is sequential by
/// construction and must never be parallelized - its boundaries are the wire
/// contract), a scoped worker pool hashes chunks, and a collector re-emits
/// them in file order. This wrapper is the thin batch consumer: it drains
/// every batch and concatenates, so its output stays byte-identical to the
/// serial loop, pinned by the `parallel_chunk_hash_matches_serial` proptest
/// against `chunk_and_hash_reader_serial`. Post-B1 the phase is bound by the
/// producer thread itself (measured M1 Pro, 2 GiB): `StreamCDC` scan +
/// per-chunk `Vec` copy ~1.5 s and whole-file SHA + read ~1.2 s, with the
/// per-chunk hashing fully absorbed by the pool.
fn chunk_and_hash_reader<R: std::io::Read + Send>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    run_chunk_pipeline(source, avg_size, CHUNK_BATCH, |out_rx| {
        let mut chunks: ChunkList = Vec::new();
        for msg in out_rx {
            match msg {
                PipelineMsg::Batch(batch) => chunks.extend(batch),
                PipelineMsg::Done { whole_hex } => return Ok((whole_hex, chunks)),
                PipelineMsg::Fail(e) => return Err(e),
            }
        }
        // Unreachable by construction: the collector always terminates the
        // stream with `Done` or `Fail` before dropping its sender.
        Err(CoreError::Integrity(
            "chunk pipeline closed without a terminal message".to_string(),
        ))
    })?
}

/// Run the streaming chunk pipeline over `source`, handing `consume` the
/// output receiver on the calling thread: zero or more file-ordered
/// [`PipelineMsg::Batch`]es, then exactly one terminal `Done`/`Fail` (empty
/// input: zero batches, then `Done`). `batch_size` is [`CHUNK_BATCH`] in
/// production; tests shrink it to force multi-batch emission on small
/// fixtures.
///
/// `consume` must run the stream to its terminal message (or drop the
/// receiver) before returning; to consume from another thread, forward
/// messages to your own channel (see [`ChunkStreamCore::spawn`]). Returning
/// with the receiver alive but undrained deadlocks: the scope below joins the
/// collector, which blocks sending to the full output channel nobody drains.
///
/// Thread shape (all scoped, so `source` may borrow): the PRODUCER feeds CDC
/// output to the bounded work queue while folding the whole-file hash;
/// WORKERS hash chunks and echo their metadata back with the digest; the
/// COLLECTOR reorders completions into file order and batches them onto the
/// bounded output channel. Producer and collector are separate threads on
/// purpose: a collector sharing the producer's thread could not emit until
/// EOF (reintroducing batch-at-EOF) and could deadlock - blocked sending to
/// a full output channel while also being the only thread feeding CDC.
///
/// Shutdown is acyclic in both directions, so no path blocks forever:
/// consumer gone -> collector's send fails -> it drops `done_rx` -> worker
/// sends fail -> they drop `work_rx` -> producer's send fails -> all exit;
/// producer done/failed -> ctrl status queued, `work_tx` dropped -> workers
/// drain the queue and exit -> done channel closes -> collector emits the
/// terminal message and exits.
pub(super) fn run_chunk_pipeline<R, T>(
    source: R,
    avg_size: u64,
    batch_size: usize,
    consume: impl FnOnce(mpsc::Receiver<PipelineMsg>) -> T,
) -> Result<T, CoreError>
where
    R: std::io::Read + Send,
{
    if !(CDC_MIN_AVG..=CDC_MAX_AVG).contains(&avg_size) {
        return Err(CoreError::InvalidArgument(format!(
            "FastCDC average size {avg_size} out of range [{CDC_MIN_AVG}, {CDC_MAX_AVG}]"
        )));
    }
    // The range check above guarantees min/avg/max fit u32; try_from keeps that
    // provable to clippy without an unchecked `as` cast.
    let to_u32 = |v: u64| -> Result<u32, CoreError> {
        u32::try_from(v)
            .map_err(|_| CoreError::InvalidArgument(format!("chunk size {v} exceeds u32")))
    };
    let (min, max) = (to_u32(avg_size / 4)?, to_u32(avg_size * 4)?);
    let avg = to_u32(avg_size)?;

    // StreamCDC allocates and memcpys a Vec per chunk on top of the gear-hash
    // scan; that copy is part of the producer floor (the py-spy "CDC = 6%"
    // figure counted only gear-hash frames and missed it).
    let chunker = StreamCDC::new(source, min, avg, max);

    // Leave one core for the producer (CDC + whole-file hash); beyond
    // MAX_HASH_WORKERS extra workers only add memory, not throughput.
    let workers = std::thread::available_parallelism()
        .map_or(1, |n| n.get().saturating_sub(1))
        .clamp(1, MAX_HASH_WORKERS);

    let out = std::thread::scope(|scope| {
        // Chunk bytes in; hashed metadata out. The work sync_channel's bound
        // is what caps transient chunk-bytes memory (see HASH_QUEUE_DEPTH): a
        // full queue blocks the producer until a worker frees a slot. The
        // done channel is unbounded like B1's, but carries 56-byte metadata
        // only (see CHUNK_BATCH for the metadata-scale analysis).
        let (work_tx, work_rx) = mpsc::sync_channel::<HashJob>(HASH_QUEUE_DEPTH);
        // std-only on purpose: `Receiver` is !Sync, so the Mutex is what makes
        // it MPMC; at 4 workers pulling ms-scale jobs the lock is uncontended,
        // and crossbeam/rayon would add a dependency for nothing.
        let work_rx = Arc::new(Mutex::new(work_rx));
        let (done_tx, done_rx) = mpsc::channel::<HashedChunk>();
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<ProducerEnd>();
        let (out_tx, out_rx) = mpsc::sync_channel::<PipelineMsg>(BATCH_QUEUE_DEPTH);

        for _ in 0..workers {
            let rx = Arc::clone(&work_rx);
            let tx = done_tx.clone();
            scope.spawn(move || hash_worker(&rx, &tx));
        }
        // Workers hold the only remaining done senders; dropping ours lets
        // the collector's drain terminate once they exit.
        drop(done_tx);

        scope.spawn(move || produce_chunks(chunker, &work_tx, &ctrl_tx));
        scope.spawn(move || collect_batches(&done_rx, &ctrl_rx, &out_tx, batch_size));

        consume(out_rx)
    });
    Ok(out)
}

/// Worker body: hash one job at a time off the shared receiver, echoing the
/// job's metadata back with the digest (see [`HashJob`] for why).
fn hash_worker(work_rx: &Mutex<mpsc::Receiver<HashJob>>, done_tx: &mpsc::Sender<HashedChunk>) {
    loop {
        // Hold the lock only for the recv so workers hash unlocked and
        // overlap. Poisoning is unreachable (workers do not panic) but is
        // treated as shutdown, never unwrapped.
        let job = match work_rx.lock() {
            Ok(guard) => guard.recv(),
            Err(_) => break,
        };
        let Ok(job) = job else { break };
        let digest: [u8; 32] = Sha256::digest(&job.data).into();
        let hashed = HashedChunk {
            idx: job.idx,
            offset: job.offset,
            len: job.len,
            digest,
        };
        if done_tx.send(hashed).is_err() {
            break;
        }
    }
}

/// Producer body: CDC + the whole-file hash on one thread. Always leaves
/// exactly one [`ProducerEnd`] on `ctrl_tx` before returning (and therefore
/// before the caller's `work_tx` drops), which is what makes the collector's
/// post-drain ctrl `recv` non-blocking: the done channel only closes after
/// every worker exits, workers only exit after `work_tx` drops, and the
/// status is queued before that.
fn produce_chunks<R: std::io::Read>(
    chunker: StreamCDC<R>,
    work_tx: &mpsc::SyncSender<HashJob>,
    ctrl_tx: &mpsc::Sender<ProducerEnd>,
) {
    let mut whole = Sha256::new();
    let mut fed = 0usize;
    for (idx, result) in chunker.enumerate() {
        match result {
            Ok(cd) => {
                whole.update(&cd.data);
                // Count BEFORE the send: a job the workers never took must
                // still register as fed, so the collector reports the gap as
                // an Integrity error (B1's missing-digest guarantee).
                fed += 1;
                let job = HashJob {
                    idx,
                    offset: cd.offset,
                    len: cd.length as u64,
                    data: cd.data,
                };
                if work_tx.send(job).is_err() {
                    // All workers gone - the fed-count gap surfaces this as
                    // Integrity at the collector.
                    break;
                }
            }
            Err(e) => {
                // A closed ctrl channel means the collector (and consumer)
                // are gone; there is no one left to report to.
                let _ = ctrl_tx.send(ProducerEnd::Failed(CoreError::Io(std::io::Error::other(e))));
                return;
            }
        }
    }
    let _ = ctrl_tx.send(ProducerEnd::Finished {
        fed,
        whole_hex: hex::encode(whole.finalize()),
    });
}

/// Collector body: reorder completion-order digests into file order and emit
/// them in `batch_size` batches. `pending` holds out-of-order completions -
/// digest metadata only (56 B/entry); its worst case is bounded by how far a
/// single lagging chunk can fall behind the rest of the pool, and by the
/// full chunk list in the degenerate case (see [`CHUNK_BATCH`]).
fn collect_batches(
    done_rx: &mpsc::Receiver<HashedChunk>,
    ctrl_rx: &mpsc::Receiver<ProducerEnd>,
    out_tx: &mpsc::SyncSender<PipelineMsg>,
    batch_size: usize,
) {
    // Floor at 1 so a zero batch size degenerates to per-chunk emission
    // instead of an unreachable flush condition.
    let batch_size = batch_size.max(1);
    let mut pending: BTreeMap<usize, HashedChunk> = BTreeMap::new();
    let mut next_emit = 0usize;
    let mut batch: Vec<(String, u64, u64)> = Vec::with_capacity(batch_size);

    for hashed in done_rx {
        pending.insert(hashed.idx, hashed);
        while let Some(chunk) = pending.remove(&next_emit) {
            batch.push((hex::encode(chunk.digest), chunk.offset, chunk.len));
            next_emit += 1;
            if batch.len() >= batch_size {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                if out_tx.send(PipelineMsg::Batch(full)).is_err() {
                    return; // consumer gone - unwind the whole pipeline
                }
            }
        }
    }

    // The done channel closed: every worker has exited, so the producer's
    // terminal status is already queued (it sends before its work sender
    // drops) and this recv cannot block.
    let Ok(end) = ctrl_rx.recv() else {
        // Unreachable by construction; surfaced as a typed error, never a
        // panic, if a future refactor breaks the send-before-drop ordering.
        let _ = out_tx.send(PipelineMsg::Fail(CoreError::Integrity(
            "chunk producer exited without a terminal status".to_string(),
        )));
        return;
    };
    match end {
        ProducerEnd::Failed(e) => {
            // The producer's own error outranks any partial batch or missing
            // digest (B1 parity: produce_err returned before the gap check).
            let _ = out_tx.send(PipelineMsg::Fail(e));
        }
        ProducerEnd::Finished { fed, whole_hex } => {
            if next_emit < fed {
                let _ = out_tx.send(PipelineMsg::Fail(CoreError::Integrity(
                    "chunk hash worker exited early".to_string(),
                )));
            } else {
                if !batch.is_empty() && out_tx.send(PipelineMsg::Batch(batch)).is_err() {
                    return;
                }
                let _ = out_tx.send(PipelineMsg::Done { whole_hex });
            }
        }
    }
}

/// Serial reference implementation of [`chunk_and_hash_reader`] - the exact
/// pre-B1 single-threaded loop, kept test-only as the equivalence oracle for
/// the parallel pipeline (`parallel_chunk_hash_matches_serial`). The output is
/// a WIRE CONTRACT: identical bytes must produce identical boundaries and
/// digests across versions or cross-revision dedup breaks, so any pipeline
/// change must stay byte-identical to this loop. This MUST remain an
/// independent implementation - never refactor it to delegate to
/// `chunk_and_hash_reader`, or the equivalence proptest degenerates into a
/// tautology.
#[cfg(test)]
fn chunk_and_hash_reader_serial<R: std::io::Read>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    if !(CDC_MIN_AVG..=CDC_MAX_AVG).contains(&avg_size) {
        return Err(CoreError::InvalidArgument(format!(
            "FastCDC average size {avg_size} out of range [{CDC_MIN_AVG}, {CDC_MAX_AVG}]"
        )));
    }
    let to_u32 = |v: u64| -> Result<u32, CoreError> {
        u32::try_from(v)
            .map_err(|_| CoreError::InvalidArgument(format!("chunk size {v} exceeds u32")))
    };
    let (min, max) = (to_u32(avg_size / 4)?, to_u32(avg_size * 4)?);
    let avg = to_u32(avg_size)?;

    let chunker = StreamCDC::new(source, min, avg, max);

    let mut whole = Sha256::new();
    let mut chunks: ChunkList = Vec::new();
    for result in chunker {
        let cd = result.map_err(|e| CoreError::Io(std::io::Error::other(e)))?;
        whole.update(&cd.data);
        let chunk_hex = hex::encode(Sha256::digest(&cd.data));
        chunks.push((chunk_hex, cd.offset, cd.length as u64));
    }
    Ok((hex::encode(whole.finalize()), chunks))
}

#[cfg(test)]
mod cdc_tests {
    use super::{chunk_and_hash_reader, CDC_MAX_AVG, CDC_MIN_AVG};
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    const AVG: u64 = 512; // -> min 128, max 2048; small enough for fast tests

    fn chunk(data: &[u8]) -> (String, super::ChunkList) {
        match chunk_and_hash_reader(Cursor::new(data), AVG) {
            Ok(v) => v,
            Err(_) => unreachable!("chunking valid bytes with a valid avg cannot fail"),
        }
    }

    #[test]
    fn out_of_range_avg_is_rejected() {
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), CDC_MIN_AVG - 1).is_err());
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), CDC_MAX_AVG + 1).is_err());
        // The shipped 64 MiB default used to reach StreamCDC::new and PANIC
        // (min = avg/4 = 16 MiB > fastcdc MINIMUM_MAX). It must now be a clean
        // caller error caught before the splitter - the exact value the staging
        // benchmark tripped.
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), 64 * 1024 * 1024).is_err());
    }

    #[test]
    fn cdc_bounds_track_fastcdc_limits() {
        use fastcdc::v2020::{AVERAGE_MAX, AVERAGE_MIN};
        // Our accepted range MUST equal fastcdc's own average bounds: only across
        // [AVERAGE_MIN, AVERAGE_MAX] do the derived min = avg/4 and max = avg*4 stay
        // within fastcdc's MINIMUM_MAX/MAXIMUM_MAX, so StreamCDC::new cannot panic.
        // If a fastcdc bump moves these, fail here rather than ship another panic.
        assert_eq!(CDC_MIN_AVG, u64::from(AVERAGE_MIN));
        assert_eq!(CDC_MAX_AVG, u64::from(AVERAGE_MAX));
    }

    #[test]
    fn chunks_at_the_ceiling_avg_without_panic() {
        // The upper bound is INCLUSIVE and valid: at avg = 4 MiB, fastcdc's derived
        // min = 1 MiB and max = 16 MiB are its exact ceilings - this must chunk, not
        // panic and not be rejected. A buffer past the 16 MiB max forces >1 chunk.
        let data = vec![9u8; 20 * 1024 * 1024];
        match chunk_and_hash_reader(Cursor::new(&data), CDC_MAX_AVG) {
            Ok((_, chunks)) => assert!(chunks.len() >= 2),
            Err(_) => unreachable!("chunking at the ceiling avg must succeed"),
        }
    }

    #[test]
    fn whole_file_digest_matches_reference() {
        let data = vec![7u8; 5000];
        let (whole, _) = chunk(&data);
        assert_eq!(whole, hex::encode(Sha256::digest(&data)));
    }

    /// Reader that yields deterministic bytes until `remaining` is exhausted,
    /// then fails - drives the mid-stream CDC/IO error path with no temp file.
    struct FailAfter {
        remaining: usize,
    }

    impl std::io::Read for FailAfter {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::other("injected mid-stream read failure"));
            }
            let n = buf.len().min(self.remaining);
            for (i, b) in buf.iter_mut().take(n).enumerate() {
                *b = u8::try_from((self.remaining + i) * 31 % 251).unwrap_or(0);
            }
            self.remaining -= n;
            Ok(n)
        }
    }

    #[test]
    fn streamed_batches_are_file_ordered_and_match_serial() {
        use super::{run_chunk_pipeline, PipelineMsg};

        let mut data = vec![0u8; 16_000];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i * 31 % 251).unwrap_or(0);
        }
        let Ok((serial_whole, serial_chunks)) =
            super::chunk_and_hash_reader_serial(Cursor::new(&data), AVG)
        else {
            unreachable!("serial oracle cannot fail on valid input")
        };
        assert!(
            serial_chunks.len() >= 7,
            "fixture must span multiple batches at batch size 3"
        );

        // Worker completion order is nondeterministic; repeat so one lucky
        // in-order arrival cannot mask a collector that skips reordering.
        for _ in 0..16 {
            let outcome = run_chunk_pipeline(Cursor::new(&data), AVG, 3, |out_rx| {
                let mut batches: Vec<Vec<(String, u64, u64)>> = Vec::new();
                for msg in out_rx {
                    match msg {
                        PipelineMsg::Batch(b) => batches.push(b),
                        PipelineMsg::Done { whole_hex } => return Ok((batches, whole_hex)),
                        PipelineMsg::Fail(e) => return Err(e),
                    }
                }
                Err(super::CoreError::Integrity(
                    "stream closed without a terminal message".to_string(),
                ))
            });
            let Ok(Ok((batches, whole))) = outcome else {
                unreachable!("pipeline over valid input cannot fail")
            };

            assert!(
                batches.len() >= 2,
                "batch size 3 must yield several batches"
            );
            for (i, batch) in batches.iter().enumerate() {
                assert!(!batch.is_empty(), "batch {i} is empty");
                assert!(batch.len() <= 3, "batch {i} exceeds the batch size");
                if i + 1 < batches.len() {
                    assert_eq!(batch.len(), 3, "only the final batch may run short");
                }
            }
            // Ordering + completeness in one shot: the triples carry offsets,
            // so any arrival-order emission diverges from the serial oracle.
            let flat: super::ChunkList = batches.into_iter().flatten().collect();
            assert_eq!(
                flat, serial_chunks,
                "batches must concatenate to the serial chunk list"
            );
            assert_eq!(whole, serial_whole);
        }
    }

    #[test]
    fn collector_reorders_out_of_order_completions() {
        use super::{collect_batches, HashedChunk, PipelineMsg, ProducerEnd};
        use std::sync::mpsc;

        // Deterministic ordering pin: thread scheduling rarely delivers
        // out-of-order completions for microsecond-scale test chunks, so the
        // end-to-end test above cannot reliably catch a collector that skips
        // reordering. Drive the collector directly with a hand-scrambled
        // completion order instead.
        let (done_tx, done_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();
        // Roomy output bound: the collector runs on THIS thread below, so its
        // sends must never block (5 chunks at batch size 2 -> 4 messages).
        let (out_tx, out_rx) = mpsc::sync_channel(16);

        for idx in [3usize, 0, 4, 1, 2] {
            let hashed = HashedChunk {
                idx,
                offset: idx as u64 * 10,
                len: 10,
                digest: [u8::try_from(idx).unwrap_or(0); 32],
            };
            let _ = done_tx.send(hashed);
        }
        drop(done_tx);
        let _ = ctrl_tx.send(ProducerEnd::Finished {
            fed: 5,
            whole_hex: "whole".to_string(),
        });
        drop(ctrl_tx);

        collect_batches(&done_rx, &ctrl_rx, &out_tx, 2);
        drop(out_tx);

        let msgs: Vec<_> = out_rx.into_iter().collect();
        // File order restored and batched [2, 2, 1], then Done.
        let mut expected_offset = 0u64;
        let mut batch_sizes = Vec::new();
        for (i, msg) in msgs.iter().enumerate() {
            match msg {
                PipelineMsg::Batch(batch) => {
                    batch_sizes.push(batch.len());
                    for (hex, offset, len) in batch {
                        assert_eq!(*offset, expected_offset, "chunks must emit in file order");
                        assert_eq!(*len, 10);
                        assert_eq!(
                            hex,
                            &hex::encode([u8::try_from(expected_offset / 10).unwrap_or(0); 32])
                        );
                        expected_offset += *len;
                    }
                }
                PipelineMsg::Done { whole_hex } => {
                    assert_eq!(whole_hex, "whole");
                    assert_eq!(i + 1, msgs.len(), "Done must be the final message");
                }
                PipelineMsg::Fail(e) => unreachable!("unexpected failure: {e:?}"),
            }
        }
        assert_eq!(expected_offset, 50, "all five chunks must be emitted");
        assert_eq!(
            batch_sizes,
            [2, 2, 1],
            "batches must flush every batch_size chunks"
        );
    }

    #[test]
    fn vanished_worker_gap_fails_integrity() {
        use super::{collect_batches, HashedChunk, PipelineMsg, ProducerEnd};
        use crate::error::CoreError;
        use std::sync::mpsc;

        // White-box gap check (mirrors the scrambled-order harness above): the
        // producer fed 5 chunks but only 4 digests ever arrive - a worker
        // vanished mid-file. The collector must emit what it can in file order
        // and then FAIL with Integrity, never a silent short `Done`.
        let (done_tx, done_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();
        let (out_tx, out_rx) = mpsc::sync_channel(16);

        for idx in [0usize, 1, 3, 4] {
            // idx 2 never completes
            let hashed = HashedChunk {
                idx,
                offset: idx as u64 * 10,
                len: 10,
                digest: [u8::try_from(idx).unwrap_or(0); 32],
            };
            let _ = done_tx.send(hashed);
        }
        drop(done_tx);
        let _ = ctrl_tx.send(ProducerEnd::Finished {
            fed: 5,
            whole_hex: "whole".to_string(),
        });
        drop(ctrl_tx);

        collect_batches(&done_rx, &ctrl_rx, &out_tx, 2);
        drop(out_tx);

        let msgs: Vec<_> = out_rx.into_iter().collect();
        // Chunks 0 and 1 flush as one full batch; 3 and 4 stay pending behind
        // the hole at 2, so the terminal message must be the Integrity Fail.
        match msgs.as_slice() {
            [PipelineMsg::Batch(batch), PipelineMsg::Fail(CoreError::Integrity(msg))] => {
                assert_eq!(batch.len(), 2, "only the pre-gap chunks may emit");
                assert!(
                    msg.contains("chunk hash worker exited early"),
                    "gap must be named: {msg}"
                );
            }
            other => unreachable!("expected [Batch, Fail(Integrity)], got {other:?}"),
        }
    }

    #[test]
    fn mid_stream_read_error_reaches_the_consumer() {
        use super::{run_chunk_pipeline, PipelineMsg};
        use crate::error::CoreError;

        // Enough good bytes for a few chunks before the reader fails, so the
        // failure lands mid-stream rather than on the first fill.
        let outcome = run_chunk_pipeline(FailAfter { remaining: 5000 }, AVG, 3, |out_rx| {
            for msg in out_rx {
                match msg {
                    PipelineMsg::Batch(batch) => assert!(!batch.is_empty()),
                    PipelineMsg::Done { whole_hex } => {
                        unreachable!("must not report success past a read failure: {whole_hex}")
                    }
                    PipelineMsg::Fail(e) => return Some(e),
                }
            }
            None
        });
        match outcome {
            Ok(Some(CoreError::Io(_))) => {}
            other => unreachable!("expected a mid-stream Io failure, got {other:?}"),
        }

        // The public wrapper surfaces the same failure as a plain Err.
        assert!(matches!(
            chunk_and_hash_reader(FailAfter { remaining: 5000 }, AVG),
            Err(CoreError::Io(_))
        ));
    }

    #[test]
    fn empty_input_emits_zero_batches_then_done() {
        use super::{run_chunk_pipeline, PipelineMsg};

        let outcome = run_chunk_pipeline(Cursor::new(&[][..]), AVG, 3, |out_rx| {
            out_rx.into_iter().collect::<Vec<_>>()
        });
        let Ok(msgs) = outcome else {
            unreachable!("empty input is valid")
        };
        // Exactly one message: `Done` with the sha256 of zero bytes - no empty
        // `Batch` may precede it.
        match msgs.as_slice() {
            [PipelineMsg::Done { whole_hex }] => {
                assert_eq!(whole_hex, &hex::encode(Sha256::digest([])));
            }
            other => unreachable!("expected a lone Done for empty input, got {other:?}"),
        }
    }

    #[test]
    fn boundaries_reshuffle_only_locally_on_a_late_edit() {
        // Determinism + shift-locality (the CDC payoff): inserting a byte near
        // the END must leave the FIRST chunk's digest unchanged - content-defined
        // boundaries re-sync, so unchanged early regions still dedup.
        let mut data = vec![0u8; 8000];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i * 31 % 251).unwrap_or(0); // deterministic, non-degenerate
        }
        let (_, before) = chunk(&data);

        let mut edited = data.clone();
        edited.insert(7000, 0xFF); // late insert shifts only the tail
        let (_, after) = chunk(&edited);

        assert!(
            before.len() > 1 && after.len() > 1,
            "need multiple chunks to test locality"
        );
        assert_eq!(
            before[0].0, after[0].0,
            "first chunk digest must survive a late edit"
        );
    }

    proptest::proptest! {
        // Partition + determinism over arbitrary byte vectors. `Cursor` drives
        // the reader core directly so each case is pure CPU (no temp file).
        #[test]
        fn cdc_partitions_and_is_deterministic(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..20_000usize),
        ) {
            let (whole, chunks) = chunk(&data);

            // Contiguous, gapless offsets summing to the file length.
            let mut expected_offset = 0u64;
            for (_, off, len) in &chunks {
                proptest::prop_assert_eq!(*off, expected_offset);
                expected_offset += *len;
            }
            proptest::prop_assert_eq!(expected_offset, data.len() as u64);

            // Whole-file digest is the reference sha256 of exactly these bytes.
            proptest::prop_assert_eq!(&whole, &hex::encode(Sha256::digest(&data)));

            // Determinism: same bytes -> identical chunk boundaries + digests.
            let (whole2, chunks2) = chunk(&data);
            proptest::prop_assert_eq!(whole, whole2);
            proptest::prop_assert_eq!(chunks, chunks2);
        }

        // B1 equivalence oracle: the parallel worker-pool pipeline must stay
        // BYTE-IDENTICAL to the pre-B1 serial loop - boundaries, digest hex,
        // ordering, and the whole-file hash are a wire contract (identical
        // bytes must chunk identically across versions or dedup breaks).
        // Covers the empty input (0-length vectors) and the full valid small
        // avg range.
        #[test]
        fn parallel_chunk_hash_matches_serial(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..262_144usize),
            avg in CDC_MIN_AVG..8192u64,
        ) {
            let serial = super::chunk_and_hash_reader_serial(Cursor::new(&data), avg);
            let parallel = chunk_and_hash_reader(Cursor::new(&data), avg);
            match (serial, parallel) {
                (Ok(s), Ok(p)) => proptest::prop_assert_eq!(s, p),
                (s, p) => proptest::prop_assert!(
                    false,
                    "both paths must succeed on a valid avg; serial={:?} parallel={:?}",
                    s,
                    p
                ),
            }
        }

        // Size bounds: every chunk is <= max, and every chunk except the last is
        // >= min (FastCDC only lets the final chunk fall below the minimum).
        #[test]
        fn cdc_respects_size_bounds(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..20_000usize),
        ) {
            let (min, max) = (AVG / 4, AVG * 4);
            let (_, chunks) = chunk(&data);
            for (i, (_, _, len)) in chunks.iter().enumerate() {
                proptest::prop_assert!(*len <= max, "chunk {} len {} exceeds max {}", i, len, max);
                if i + 1 < chunks.len() {
                    proptest::prop_assert!(*len >= min, "non-final chunk {} len {} below min {}", i, len, min);
                }
            }
        }
    }
}
