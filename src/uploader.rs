use bytes::Bytes;
use fastcdc::v2020::StreamCDC;
use futures::stream::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::error::CoreError;

/// TCP/TLS handshake budget for `upload_client`, matching
/// `chunk_fetcher::CONNECT_TIMEOUT_SECS` on the download side (audit
/// M-UPLOAD-CONNECT). Kept local rather than re-exported so the two clients stay
/// independently tunable.
const CONNECT_TIMEOUT_SECS: u64 = 30;

/// Read-buffer size for `hash_file_async` (audit L16). Matches
/// `chunk_fetcher`/`chunked_downloader`'s `VERIFY_READ_BUFFER` (8 MiB): the
/// previous 64 KiB buffer did ~128x more `read(2)` syscalls (~82k vs ~640 for a
/// 5 GiB blob) for no benefit - the hash is bandwidth-bound, not latency-bound.
const HASH_READ_BUFFER: usize = 8 * 1024 * 1024;

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
const CHUNK_BATCH: usize = 64;

/// Bound of the output batch channel (collector -> consumer); see
/// [`CHUNK_BATCH`] for why this queue is KiB-scale.
const BATCH_QUEUE_DEPTH: usize = 2;

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
enum PipelineMsg {
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
fn run_chunk_pipeline<R, T>(
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

// Phase 3.8 (audit U4): the local UploadError was folded into the
// crate-wide `CoreError`. The single thiserror-derived enum carries
// `reqwest::Error` and `std::io::Error` via `#[from]`, preserving the
// `?` ergonomics and the `Error::source()` chain through the Python
// boundary in `lib.rs::core_err_to_py`.

/// Compute the SHA256 and total size of a local file.
///
/// Audit U1: the digest loop is CPU-bound and the I/O is unbuffered file
/// reads - neither benefits from running on a tokio worker thread, and the
/// combination starves other futures on the same runtime for seconds on
/// multi-GB blobs. We route the whole pass through `spawn_blocking` so the
/// runtime keeps its worker threads free for actual async work (HTTP, other
/// downloads). `std::fs::File` + `std::io::Read` are the right primitives
/// inside the blocking pool; the async wrappers would only re-block the same
/// thread.
///
/// The double `?` at the end is load-bearing: `spawn_blocking(...).await`
/// produces `Result<Result<T, CoreError>, JoinError>`. The outer
/// `JoinError` (panic in the closure / runtime shutdown) maps to
/// `CoreError::JoinFailed` - non-retryable, because a panicked hash
/// closure reproduces identically on retry - then `?` unwraps the inner
/// Result.
pub async fn hash_file_async(path: &Path) -> Result<(String, u64), CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(String, u64), CoreError> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; HASH_READ_BUFFER];
        let mut total_size = 0u64;

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
            total_size += bytes_read as u64;
        }

        Ok((hex::encode(hasher.finalize()), total_size))
    })
    .await
    .map_err(|join_err| CoreError::JoinFailed {
        index: None,
        source: join_err,
    })?
}

/// Mirror of [`crate::chunked_downloader::MAX_RETRIES`] for the upload
/// path. Audit U3 (Phase 3.11): the downloader retried per-chunk up to
/// 3 times; the uploader did not retry at all, so a single transient
/// 503 lost the whole upload. The two paths now share the same budget
/// and the same [`CoreError::is_retryable`] classifier - see
/// `try_upload_blob_once` for the per-attempt body.
const UPLOAD_MAX_RETRIES: u32 = 3;

/// Upload write-stall watchdog window (audit H1). If reqwest stops pulling body
/// bytes for this long while the body is NOT yet fully sent, the send is aborted
/// with a retryable [`CoreError::Stall`]. Gating on "body not yet fully sent"
/// means a legitimately slow blob-commit RESPONSE (`JuiceFS` backpressure can make
/// a commit take many seconds) never trips it - the watchdog guards only the
/// write phase, which reqwest itself offers no per-operation timeout for.
const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll cadence for the write-stall watchdog. One second keeps the abort latency
/// bounded without measurable overhead against a multi-GB streamed body.
const WRITE_STALL_CHECK: Duration = Duration::from_secs(1);

/// Bound on the wait for the registry's response AFTER the request body is fully
/// sent (audit H1 follow-up). The write-stall watchdog guards only the body-write
/// phase and disarms the instant the body is drained, so without this a Harbor that
/// accepts the pack then hangs on the `JuiceFS` blob commit keeps the TCP connection
/// live (so `tcp_keepalive` never fires) and blocks the pack upload forever, draining
/// the shared pack gate. `send_put_watchdogged` arms this deadline only once `done`
/// flips, so it never caps an honest streamed body - only a hung post-body commit.
/// Generous (2 min) so a legitimately slow commit under `JuiceFS` metadata
/// backpressure (~25 s observed) never false-trips.
const RESPONSE_WAIT_TIMEOUT: Duration = Duration::from_mins(2);

/// Total budget for the zero-body pack upload-init POST (audit H1). Init only
/// allocates an upload session and returns a `Location`; it has no legitimate
/// slow path, so a tight total timeout turns a hung/black-holed registry into a
/// retryable error instead of blocking `try_pack_upload_once` forever - which
/// (via the shared `_pack_upload_gate`) would otherwise wedge the whole folder
/// upload. Unlike the streamed PUT body, a `.timeout()` here can't clip an honest
/// transfer because there is no body to stream.
const INIT_POST_TIMEOUT: Duration = Duration::from_secs(30);

/// Frame size the in-memory pack PUT body is sliced into before streaming (audit
/// H1). The write-stall watchdog re-stamps its progress clock only when reqwest
/// pulls the next body frame, so the frame must drain in well under
/// `WRITE_STALL_TIMEOUT` on any link we mean to support, or a slow-but-progressing
/// peer false-trips `Stall`. At 1 `MiB` the watchdog cut a healthy uplink slower
/// than ~35 KB/s (one frame > 30 s); 256 `KiB` drops that floor to ~8.5 KB/s so a
/// throttled/shared link uploads instead of failing. The slices are cheap `Bytes`
/// views (a refcount bump, not a copy), so more frames cost no extra allocation.
const PUT_FRAME_BYTES: usize = 256 * 1024;

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar - useful for large blobs (multi-GB).
///
/// Audit U3 (Phase 3.11): wraps [`try_upload_blob_once`] in an
/// exponential-backoff retry loop with the same shape as
/// [`crate::chunked_downloader::download_chunk_with_retry`]. Each attempt re-inits
/// a fresh OCI upload session AND re-opens the file inside `try_upload_blob_once`
/// (the previous session is consumed and the previous `FramedRead` stream is spent),
/// so a retry never re-PUTs a dead session (audit L2). Backoff schedule: 200, 400,
/// 800, 1600 ms - four attempts total, ~3 s of backoff before surfacing a transient
/// 5xx as terminal. A 4xx never burns backoff.
pub async fn upload_blob_async(
    uploads_url: &str,
    path: &Path,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let mut retries: u32 = 0;
    loop {
        match try_upload_blob_once(uploads_url, path, digest, auth_token).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                // Same shape as `download_chunk_with_retry`: classify on
                // the error itself (borrow only, so `e` remains
                // returnable), give up on permanent errors immediately,
                // give up on transient errors after the budget is spent.
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                // Full-jitter backoff (audit L-JITTER): a jittered draw within the
                // same 200/400/800/1600 ms cap schedule the deterministic sleep
                // used, so concurrent uploads that hit a registry 429/503 together
                // do not re-collide in lockstep. Shared helper across all four
                // transport retry loops.
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

/// Single upload attempt. Extracted from `upload_blob_async` in audit
/// U3 (Phase 3.11) so the surrounding retry loop has a unit to call
/// repeatedly. Each call opens its own `File` handle, builds its own
/// `FramedRead` stream, and sends one PUT - so the retry loop above
/// gets a fresh body on every attempt (the previous `Body::wrap_stream`
/// is consumed once the request future completes or errors).
/// Process-global HTTP client for blob uploads.
///
/// Mirrors the downloader, which builds its `reqwest::Client` once in
/// `ChunkedDownloader::new` and reuses it across all chunks. Previously
/// `try_upload_blob_once` rebuilt a client on every call - once per blob and
/// once per retry - discarding the keep-alive connection pool and forcing a
/// fresh DNS+TCP+TLS handshake against the registry host the previous blob just
/// finished using (audit N-4 / RUST-3). The `OnceLock` hoists construction out
/// of the per-attempt path so warm connections survive between blobs.
///
/// Construction is fallible (`build()` errors if the TLS backend won't
/// initialize), so this returns `Result` rather than `expect`-ing inside a
/// `get_or_init` closure - the crate denies `panic`/`unwrap` and warns on
/// `expect`. On the rare init race the losing thread's freshly built client is
/// dropped unused (RAII); after first init `get()` returns the shared client
/// immediately. `OnceLock` is valid in statics and never poisoned on panic
/// (doc.rust-lang.org/std/sync/struct.OnceLock.html).
pub(crate) fn upload_client() -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // Force HTTP/1.1 for the same reason as the downloader: avoids h2 single-TCP
    // multiplexing, lets uploads spread across multiple connections if the caller
    // parallelizes.
    //
    // No client-level timeout of any kind on the upload path (audit H-UPLOAD-TIMEOUT).
    // reqwest's `.timeout()` is a wall-clock deadline over the WHOLE request including
    // the streamed body - a 1h cap silently bounded total transfer, and since a reqwest
    // timeout is `is_retryable()`, `upload_blob_async` re-streamed from byte 0 up to the
    // retry budget (~4x the wall). `.read_timeout()` is NOT a safe substitute here: in
    // reqwest 0.12 it is a single non-resetting deadline over the `PendingRequest` phase
    // (request-send + wait-for-response-head) and only becomes a per-read resetting idle
    // timeout for the RESPONSE body - so on an upload it is a fixed wall on the body
    // write, reintroducing the same re-stream failure. Progress is instead bounded per
    // phase in `send_put_watchdogged`: `WRITE_STALL_TIMEOUT` guards the body write (idle,
    // resets on each accepted frame) and `RESPONSE_WAIT_TIMEOUT` bounds the wait for the
    // registry's response after the body is fully sent - neither caps an honest transfer.
    // A dead peer's handshake is caught by `connect_timeout`.
    let built = Client::builder()
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .http1_only()
        .tcp_keepalive(Duration::from_secs(30))
        // Generous fixed idle cap (was 8) so raising HIPPIUS_MAX_INFLIGHT_PACKS /
        // HIPPIUS_UPLOAD_WORKERS above 8 doesn't force connections past the 8th to
        // re-handshake per pack. The real in-flight bound is the _pack_upload_gate,
        // not this idle cap; matches the download client's pool_max_idle.
        .pool_max_idle_per_host(32)
        .build()?;
    Ok(CLIENT.get_or_init(|| built))
}

/// Milliseconds since `base`, saturating a u128->u64 cast that only overflows
/// after ~584 million years of uptime - keeps clippy's truncation lint satisfied
/// without an `unwrap`.
fn elapsed_ms(base: Instant) -> u64 {
    u64::try_from(base.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Stream adapter that flips `done` to `true` the instant the inner stream is
/// exhausted (`poll_next` -> `Ready(None)`).
///
/// This is the "body fully sent" signal for the write-stall watchdog. reqwest
/// polls the body to EOF exactly when it has taken every byte, so EOF is the only
/// reliable end-of-write marker - a pre-stream `metadata().len()` can diverge from
/// the streamed length (the TOCTOU the U2 chunked-encoding design deliberately
/// tolerates: the file may be rewritten between stat and stream). Keying `done`
/// off a byte count against that stat either false-tripped a `Stall` on a
/// fully-sent shorter body or disarmed the watchdog early on a longer one.
///
/// The inner stream is boxed-pinned so the adapter is `Unpin` regardless of the
/// inner stream's pinning, letting `poll_next` project without `unsafe`.
struct DoneOnEof<S> {
    inner: Pin<Box<S>>,
    done: Arc<AtomicBool>,
}

impl<S> DoneOnEof<S> {
    fn new(inner: S, done: Arc<AtomicBool>) -> Self {
        Self {
            inner: Box::pin(inner),
            done,
        }
    }
}

impl<S: Stream> Stream for DoneOnEof<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        // `Self: Unpin` (boxed inner + `Arc`), so `get_mut` is safe.
        let this = self.get_mut();
        let polled = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(None) = &polled {
            this.done.store(true, Ordering::Relaxed);
        }
        polled
    }
}

/// Attach `body_stream` as a chunked-encoded body to `req` and send it, aborting
/// with a retryable [`CoreError::Stall`] on either of the two ways a live-socket peer
/// can wedge a transfer that `connect_timeout`/`tcp_keepalive` cannot see and reqwest
/// offers no per-operation timeout for:
///   - the body WRITE stalls - no frame accepted for `write_stall` (the H1 wedge);
///   - the peer accepts the whole body then never RESPONDS for `response_wait` (a
///     commit hung behind `JuiceFS` backpressure).
///
/// `req` must already carry the method, URL, headers, and auth - only the body is
/// attached here - so the ONE watchdog guards the whole-file/pack `PUT`
/// ([`send_put_watchdogged`]) and the resumable `PATCH` ([`chunked_patch_upload`])
/// alike. `done` (driven off the body stream reaching end-of-input, see
/// [`DoneOnEof`]) switches the guard between the two phases, so the write deadline
/// never caps a slow-but-progressing body and the response deadline is measured only
/// from body-completion - correct even when the streamed length diverges from any
/// earlier stat. `response_wait` is generous relative to `write_stall` because a
/// legitimately slow commit is expected; it exists only to bound an indefinitely
/// hung one.
///
/// Atomics (not a lock) so the body-stamping closure never holds a guard across
/// reqwest's `.await` and the watchdog's reads are wait-free; `Relaxed` is enough
/// because the watchdog only needs to eventually observe the latest stamp, not a
/// happens-before edge (no data is published through the flags).
async fn send_streaming_watchdogged<S>(
    req: reqwest::RequestBuilder,
    body_stream: S,
    write_stall: Duration,
    response_wait: Duration,
) -> Result<reqwest::Response, CoreError>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let base = Instant::now();
    let last_ms = Arc::new(AtomicU64::new(elapsed_ms(base)));
    let done = Arc::new(AtomicBool::new(false));

    // Stamp the progress clock every time reqwest pulls a frame (= the socket
    // accepted the prior bytes); `DoneOnEof` sets `done` when the body is drained.
    let lm = Arc::clone(&last_ms);
    let stamped = body_stream.map(move |frame| {
        if frame.is_ok() {
            lm.store(elapsed_ms(base), Ordering::Relaxed);
        }
        frame
    });
    let body = reqwest::Body::wrap_stream(DoneOnEof::new(stamped, Arc::clone(&done)));

    // Drive the send, aborting on either stall phase (audit H1 + its follow-up).
    // Dropping the send future on a stall (the `return` below) cancels the reqwest
    // request and severs the socket. select! polls the pinned send future and re-arms
    // a 1s timer each round. Two phases, switched on `done` (body fully sent):
    //   - writing (!done): abort if no frame was accepted for `write_stall` (idle,
    //     resets on each accepted frame - never caps a slow-but-progressing body);
    //   - waiting for the response (done): abort if the registry hasn't responded
    //     within `response_wait` of the body finishing (the hung-commit case a live
    //     socket hides from tcp_keepalive). `done_at` is stamped the first tick we
    //     observe `done`, so the deadline is measured from body-completion, not send
    //     start.
    // `req` already carries method/URL/headers/auth (built by the caller); only the
    // body is attached here, so PUT and PATCH share this one watchdog.
    let send_fut = req.body(body).send();
    tokio::pin!(send_fut);
    let stall_ms = u64::try_from(write_stall.as_millis()).unwrap_or(u64::MAX);
    let response_ms = u64::try_from(response_wait.as_millis()).unwrap_or(u64::MAX);
    let mut done_at: Option<u64> = None;
    loop {
        tokio::select! {
            r = &mut send_fut => return Ok(r?),
            () = tokio::time::sleep(WRITE_STALL_CHECK) => {
                let now = elapsed_ms(base);
                if done.load(Ordering::Relaxed) {
                    let since_done = now.saturating_sub(*done_at.get_or_insert(now));
                    if since_done >= response_ms {
                        return Err(CoreError::Stall(Duration::from_millis(since_done)));
                    }
                } else {
                    let idle = now.saturating_sub(last_ms.load(Ordering::Relaxed));
                    if idle >= stall_ms {
                        return Err(CoreError::Stall(Duration::from_millis(idle)));
                    }
                }
            }
        }
    }
}

/// Whole-file/pack `PUT` wrapper over [`send_streaming_watchdogged`]: builds the
/// octet-stream, bearer-authed `PUT` so the two PUT call sites stay one-liners and
/// share the exact two-phase watchdog the resumable `PATCH` uses.
async fn send_put_watchdogged<S>(
    url: &str,
    body_stream: S,
    auth_token: Option<&str>,
    write_stall: Duration,
    response_wait: Duration,
) -> Result<reqwest::Response, CoreError>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let client = upload_client()?;
    let mut req = client
        .put(url)
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }
    send_streaming_watchdogged(req, body_stream, write_stall, response_wait).await
}

/// Stream `reader` to `url` as a chunked-encoded PUT body, ticking a progress
/// bar sized `pb_total`. Shared by the whole-file and byte-range upload paths.
///
/// No explicit Content-Length: reqwest falls back to Transfer-Encoding: chunked
/// for a `wrap_stream` body, so the wire length matches whatever the reader
/// actually yields - the TOCTOU-safe behaviour audit U2 established for the
/// whole-file path. For a range upload the reader is a `Take` bounded to the
/// chunk length, so the body is exactly that range regardless. `pb_total` sizes
/// the progress bar only; it is deliberately NOT used as a "fully sent" signal
/// (see [`DoneOnEof`]).
async fn put_streaming<R>(
    url: &str,
    reader: R,
    pb_total: u64,
    basename: &str,
    auth_token: Option<&str>,
    write_stall: Duration,
) -> Result<(), CoreError>
where
    R: tokio::io::AsyncRead + Send + 'static,
{
    let pb = ProgressBar::new(pb_total);
    // The template string is a compile-time literal; `indicatif` only errors on
    // malformed format directives, which we control at the call site.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Uploading {basename}"));

    // Tick the progress bar on every body frame; the watchdog's write-progress
    // stamping lives in `send_put_watchdogged`. `freeze()` hands reqwest an
    // immutable `Bytes` (a move of the `BytesMut` buffer, not a copy). ProgressBar
    // is Arc-internally -> cloning is cheap.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(reader, BytesCodec::new()).map(move |frame| {
        frame.map(|bytes| {
            pb_stream.inc(bytes.len() as u64);
            bytes.freeze()
        })
    });

    let res =
        match send_put_watchdogged(url, stream, auth_token, write_stall, RESPONSE_WAIT_TIMEOUT)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                let msg = match &e {
                    CoreError::Stall(_) => format!("{basename}: stalled"),
                    _ => format!("{basename}: failed"),
                };
                pb.finish_with_message(msg);
                return Err(e);
            }
        };
    if !res.status().is_success() {
        pb.finish_with_message(format!("{basename}: failed"));
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }
    pb.finish_with_message(format!("{basename}: uploaded"));
    Ok(())
}

fn basename_of(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| "blob".to_string(), |n| n.to_string_lossy().into_owned())
}

/// Bytes per `PATCH` chunk (env `HIPPIUS_UPLOAD_CHUNK_SIZE`, default 16 MiB). A
/// mid-upload disconnect re-sends at most one chunk; the GET-offset resume skips
/// everything the registry already committed. Tests set it small to force many
/// chunks on a tiny payload.
fn upload_chunk_size() -> u64 {
    std::env::var("HIPPIUS_UPLOAD_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(16 * 1024 * 1024)
}

/// Result of the chunked-`PATCH` phase of one session.
enum PatchOutcome {
    /// Every byte was `PATCH`ed; carries the current session URL to finalise.
    Done(String),
    /// The registry rejected `PATCH` (405/501) on the first chunk - the caller
    /// falls back to the monolithic streaming `PUT`.
    Unsupported,
}

/// Parse an OCI upload `Range: 0-<end>` (inclusive) into the count of committed
/// bytes. Harbor returns `0-0` for an EMPTY session (right after POST, nothing
/// committed) and `0-<end>` once bytes land, so `0-0` is treated as 0, not 1 -
/// mis-reading it as 1 on a first-chunk-failure resume would skip byte 0. An
/// absent/garbled header also means nothing committed. Under-counting only
/// re-sends already-present bytes (the server 416s or overwrites); over-counting
/// would silently drop data, so we bias to under-count.
fn committed_bytes(range: Option<&str>) -> u64 {
    match range
        .and_then(|r| r.rsplit('-').next())
        .and_then(|n| n.trim().parse::<u64>().ok())
    {
        Some(0) | None => 0,
        // saturating_add: a hostile `0-18446744073709551615` would otherwise
        // overflow (panic under overflow-checks / wrap to 0 in release). Clamping
        // to u64::MAX only over-reports on a value no real registry sends; the
        // caller's `server_off > offset` guard then treats it as "already past".
        Some(end) => end.saturating_add(1),
    }
}

/// Coerce a resume give-up cause into a RETRYABLE error so `upload_blob_async`
/// opens a FRESH session (offset 0) - the actual recovery for a session that is
/// gone, or a `416` offset-desync the intra-session GET could not resolve. `416`
/// alone is not [`CoreError::is_retryable`], so returning it raw would make the
/// outer loop give up instead of restarting; wrap any non-retryable give-up
/// cause in [`CoreError::SessionRestart`], which is retryable by design and
/// keeps the real cause reachable via `source()`.
fn force_retryable(e: CoreError) -> CoreError {
    if e.is_retryable() {
        e
    } else {
        CoreError::SessionRestart {
            source: Box::new(e),
        }
    }
}

/// Append `?digest=<digest>` to a session URL (raw, unencoded `:` - the registry
/// matches the literal digest; percent-encoding it breaks the match).
fn append_digest(session: &str, digest: &str) -> String {
    let mut url = session.to_owned();
    url.push(if url.contains('?') { '&' } else { '?' });
    url.push_str("digest=");
    url.push_str(digest);
    url
}

/// Resolve a possibly-relative `Location` header against the current session URL
/// (each PATCH/POST response may hand back a new session URL carrying state).
fn resolve_location(current: &str, location: &str) -> Result<String, CoreError> {
    reqwest::Url::parse(current)
        .and_then(|base| base.join(location))
        .map(|u| u.to_string())
        .map_err(|e| CoreError::Integrity(format!("bad upload Location {location:?}: {e}")))
}

/// `POST /blobs/uploads/` to open a fresh session; return the resolved absolute
/// session URL (no `?digest=`) and the registry's optional `OCI-Chunk-Min-Length`
/// (the minimum bytes it will accept per non-final `PATCH`). Split from
/// [`init_upload_session`] so the resumable `PATCH` path can address the raw
/// session while the monolithic path still appends the digest.
async fn post_upload_session(
    uploads_url: &str,
    auth_token: Option<&str>,
) -> Result<(String, Option<u64>), CoreError> {
    let client = upload_client()?;
    let mut init = client
        .post(uploads_url)
        .header(header::CONTENT_LENGTH, "0")
        // Bound the zero-body init POST (audit H1): a hung registry here would
        // otherwise block the upload forever and drain the shared gate.
        .timeout(INIT_POST_TIMEOUT);
    if let Some(token) = auth_token {
        init = init.bearer_auth(token);
    }
    let resp = init.send().await?;
    if !resp.status().is_success() {
        return Err(CoreError::ServerError(
            resp.status().as_u16(),
            "blob upload init failed".to_string(),
        ));
    }
    // OCI: a registry MAY advertise a per-chunk minimum; the client SHOULD honor
    // it (final chunk exempt). Harbor does not send it today, so this is normally
    // None and the default 16 MiB chunk applies unchanged.
    let min_chunk = resp
        .headers()
        .get("OCI-Chunk-Min-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0);
    let location = resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        // A missing Location is an out-of-contract response, not a wrong-bytes
        // integrity failure (an LB mid-rollout can emit it); classify it retryable
        // (BadResponse) so a transient registry hiccup restarts a fresh session
        // instead of failing the upload outright.
        .ok_or_else(|| {
            CoreError::BadResponse("registry omitted Location on upload init".to_string())
        })?;
    Ok((resolve_location(uploads_url, location)?, min_chunk))
}

/// `GET <session>` (OCI "get blob upload status") -> count of bytes the registry
/// has committed, i.e. the resume offset. `Ok(None)` means the session is gone
/// (404) and must be restarted with a fresh `POST`; a transient failure is
/// surfaced as `Err` so the caller backs off.
async fn session_offset(session: &str, auth_token: Option<&str>) -> Result<Option<u64>, CoreError> {
    let client = upload_client()?;
    let mut req = client.get(session).timeout(INIT_POST_TIMEOUT);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        return Ok(None); // session expired / gone
    }
    if !status.is_success() {
        return Err(CoreError::ServerError(
            status.as_u16(),
            "upload status GET failed".to_string(),
        ));
    }
    let range = resp
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());
    Ok(Some(committed_bytes(range)))
}

/// Upload `path` to `session` in [`upload_chunk_size`]-byte chunks via OCI
/// `PATCH`, resuming from the registry-committed offset after any transient
/// failure instead of re-sending the whole blob. Returns the (possibly updated)
/// session URL to close, or [`PatchOutcome::Unsupported`] if `PATCH` is rejected
/// on the first chunk.
async fn chunked_patch_upload(
    session: &str,
    path: &Path,
    size: u64,
    min_chunk: Option<u64>,
    auth_token: Option<&str>,
    pb: &ProgressBar,
) -> Result<PatchOutcome, CoreError> {
    let client = upload_client()?;
    // Honor OCI-Chunk-Min-Length if the registry advertised one: a non-final PATCH
    // below the minimum is rejected, so clamp the configured chunk size UP to it
    // (the final short chunk is spec-exempt). Normally None -> the env/default wins.
    // Cap at `size` so a hostile/broken registry advertising a huge min-length (only
    // filtered `> 0`) - or a huge HIPPIUS_UPLOAD_CHUNK_SIZE - can't make `offset +
    // chunk_size` overflow on a resume; a whole-file chunk is the sensible ceiling.
    let chunk_size = upload_chunk_size()
        .max(min_chunk.unwrap_or(0))
        .min(size.max(1));
    let mut location = session.to_owned();
    let mut offset: u64 = 0;
    // Consecutive resume attempts with no server-side progress before giving up
    // (the outer loop then restarts with a fresh session).
    let mut stall: u32 = 0;

    while offset < size {
        let end = offset.saturating_add(chunk_size).min(size);
        let len = end - offset;
        let body = Bytes::from(read_ranges(path, &[(offset, len)]).await?);
        let end_inclusive = end - 1;

        // Stream the chunk through the SHARED write-stall watchdog (audit H1). The
        // PATCH is now the default data-carrying op; a bare `send().await` would
        // wedge forever against a peer that stops draining mid-body, because
        // `upload_client` has no flat request timeout - the exact H1 hang this work
        // exists to kill. Framing lets the watchdog re-stamp as the socket accepts
        // each frame (see `PUT_FRAME_BYTES`); Harbor accepts the resulting
        // chunked-TE PATCH.
        let frames = frame_bytes(&body, PUT_FRAME_BYTES);
        let body_stream =
            futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let mut req = client
            .patch(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_RANGE, format!("{offset}-{end_inclusive}"));
        if let Some(token) = auth_token {
            req = req.bearer_auth(token);
        }

        // On 202 advance; classify anything else. A transient failure (transport
        // error, a write stall, or 5xx/408/429) drops to the GET-offset resume
        // below; a permanent 4xx fails fast; 405/501 on the first chunk means
        // PATCH is unsupported.
        let transient: CoreError = match send_streaming_watchdogged(
            req,
            body_stream,
            WRITE_STALL_TIMEOUT,
            RESPONSE_WAIT_TIMEOUT,
        )
        .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 202 {
                    if let Some(loc) = resp
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|v| v.to_str().ok())
                    {
                        location = resolve_location(&location, loc)?;
                    }
                    offset = end;
                    pb.set_position(offset);
                    stall = 0;
                    continue;
                }
                let code = status.as_u16();
                if offset == 0 && matches!(code, 405 | 501) {
                    return Ok(PatchOutcome::Unsupported);
                }
                let err = CoreError::ServerError(code, format!("PATCH failed: {status}"));
                // 416 Range Not Satisfiable = out-of-order chunk: the OCI spec
                // says GET the upload status and resync, so route it to the
                // resume path below rather than failing (it is otherwise a 4xx).
                // Any other non-retryable status is a genuine permanent error.
                if code != 416 && !err.is_retryable() {
                    return Err(err); // permanent (e.g. 400/403)
                }
                err
            }
            Err(e) => e, // transport error or write Stall - both retryable
        };

        // Resume from whatever the registry actually committed.
        match session_offset(&location, auth_token).await {
            Ok(Some(server_off)) if server_off > offset => {
                offset = server_off;
                pb.set_position(offset);
                stall = 0;
            }
            // Session gone -> give up on it as a RETRYABLE error so
            // `upload_blob_async` restarts with a fresh session from offset 0
            // (`force_retryable` covers the 416 case, which is not itself
            // retryable but IS recoverable by a fresh session).
            Ok(None) => return Err(force_retryable(transient)),
            // No progress (or a failing status GET): back off and re-send the
            // same chunk, up to the stall budget, then give up (retryable -> the
            // outer loop restarts a fresh session).
            Ok(Some(_)) | Err(_) => {
                stall += 1;
                if stall > UPLOAD_MAX_RETRIES {
                    return Err(force_retryable(transient));
                }
                tokio::time::sleep(crate::retry::backoff_delay(stall)).await;
            }
        }
    }
    Ok(PatchOutcome::Done(location))
}

/// Finalise a chunked upload: `PUT <session>?digest=` with an empty body (all
/// bytes already arrived via `PATCH`). Retries a transient close in place a few
/// times - the bytes are up, so re-closing is cheap and avoids a full
/// fresh-session restart just because the closing PUT blipped.
///
/// No flat `.timeout()` here (unlike the init POST / status GET): this PUT is what
/// triggers the registry's server-side blob COMMIT, and that commit legitimately
/// takes many seconds - minutes on a large blob - under `JuiceFS`/S3 backpressure
/// (observed multi-minute PUT-with-digest finalises). Capping it at the 30s
/// init-POST budget would abort an in-progress commit and fail an upload whose
/// bytes are all durably written. A wedged (half-open) close is instead caught by
/// the client's `tcp_keepalive` and retried; the empty body has no write phase for
/// the stall watchdog to guard.
async fn close_chunked_upload(
    session: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let client = upload_client()?;
    let url = append_digest(session, digest);
    let mut attempt: u32 = 0;
    loop {
        let mut req = client.put(&url).header(header::CONTENT_LENGTH, "0");
        if let Some(token) = auth_token {
            req = req.bearer_auth(token);
        }
        let err: CoreError = match req.send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                CoreError::ServerError(resp.status().as_u16(), "upload close failed".to_string())
            }
            Err(e) => CoreError::from(e),
        };
        attempt += 1;
        if !err.is_retryable() || attempt > UPLOAD_MAX_RETRIES {
            // A 404 here means the session expired between the last PATCH and this
            // close, even though every byte was already committed. Force it retryable
            // so `upload_blob_async` restarts a FRESH session (which re-uploads but
            // succeeds) rather than hard-failing an upload whose bytes are durably up.
            if matches!(&err, CoreError::ServerError(404, _)) {
                return Err(force_retryable(err));
            }
            return Err(err);
        }
        tokio::time::sleep(crate::retry::backoff_delay(attempt)).await;
    }
}

async fn try_upload_blob_once(
    uploads_url: &str,
    path: &Path,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    // Re-init a fresh OCI upload session on every attempt (audit L2): a PATCH/PUT
    // to a session a prior failed attempt already consumed fails, so init lives
    // inside the retried unit (symmetry with `try_pack_upload_once`).
    let (session, min_chunk) = post_upload_session(uploads_url, auth_token).await?;
    let file_size = File::open(path).await?.metadata().await?.len();
    let basename = basename_of(path);

    let pb = ProgressBar::new(file_size);
    // The template string is a compile-time literal; `indicatif` only errors on
    // malformed format directives, which we control at the call site.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Uploading {basename}"));

    // Resumable chunked PATCH; fall back to the monolithic streaming PUT if the
    // registry doesn't support PATCH.
    let result =
        match chunked_patch_upload(&session, path, file_size, min_chunk, auth_token, &pb).await {
            Ok(PatchOutcome::Done(final_session)) => {
                close_chunked_upload(&final_session, digest, auth_token).await
            }
            Ok(PatchOutcome::Unsupported) => {
                let file = File::open(path).await?;
                let put_url = append_digest(&session, digest);
                // put_streaming owns its own progress bar; clear ours so they don't
                // fight over the terminal line.
                pb.finish_and_clear();
                return put_streaming(
                    &put_url,
                    file,
                    file_size,
                    &basename,
                    auth_token,
                    WRITE_STALL_TIMEOUT,
                )
                .await;
            }
            Err(e) => Err(e),
        };

    match &result {
        Ok(()) => pb.finish_with_message(format!("{basename}: uploaded")),
        Err(CoreError::Stall(_)) => pb.finish_with_message(format!("{basename}: stalled")),
        Err(_) => pb.finish_with_message(format!("{basename}: failed")),
    }
    result
}

/// POST-init a fresh OCI blob-upload session and append `?digest=` - the URL a
/// monolithic PUT-with-digest targets. Still used by the pack path
/// ([`try_pack_upload_once`]); the plain path now uses [`post_upload_session`] +
/// chunked `PATCH` directly.
async fn init_upload_session(
    uploads_url: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    // The pack path uploads a whole-buffer monolithic PUT, so the chunk-min hint
    // is irrelevant here - discard it.
    let (session, _min_chunk) = post_upload_session(uploads_url, auth_token).await?;
    Ok(append_digest(&session, digest))
}

/// Read the given file byte-ranges in order into one pack blob and push it via a
/// fresh OCI upload session (POST init + monolithic PUT-with-digest). Returns the
/// pack's sha256 hex - the chunked-v2 caller records it in the pointer blob.
///
/// A pack holds only NEW chunks (chunks the dedup index had no entry for), so its
/// content digest is necessarily new and no HEAD is done (it would always 404).
/// The pack is buffered once (~64 MiB target); at the upload-worker concurrency
/// that is a bounded peak, and it keeps the retry body cheap to re-send.
pub async fn pack_upload_async(
    uploads_url: &str,
    path: &Path,
    ranges: &[(u64, u64)],
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    // Own the pack bytes once as `Bytes`: the hash pass and every retry share a
    // single allocation (a `Bytes` clone is a refcount bump, not a copy), so an
    // in-flight pack costs one pack_size instead of two - `read_ranges`' `Vec`
    // converts in without reallocating. The prior `.body(buf.to_vec())` re-copied
    // the whole pack on each attempt, which the staging peak-RSS benchmark showed
    // roughly doubled resident memory per concurrent upload.
    let body = Bytes::from(read_ranges(path, ranges).await?);
    // Hash the ~64 MiB pack on the blocking pool (audit L14): the digest is
    // CPU-bound and would otherwise stall the runtime's other in-flight pack
    // uploads for the duration. The `Bytes` clone into the closure is a refcount
    // bump, not a copy, so the pack is still buffered exactly once.
    let body_for_hash = body.clone();
    // A join failure here (panicked digest closure / runtime shutdown) is
    // `JoinFailed`, not `Io`: it reproduces on retry, so it must classify
    // permanent rather than burn the pack retry budget below.
    let digest_hex =
        tokio::task::spawn_blocking(move || hex::encode(Sha256::digest(&body_for_hash)))
            .await
            .map_err(|join_err| CoreError::JoinFailed {
                index: None,
                source: join_err,
            })?;
    let digest = format!("sha256:{digest_hex}");
    let mut retries: u32 = 0;
    loop {
        match try_pack_upload_once(uploads_url, &body, &digest, auth_token).await {
            Ok(()) => return Ok(digest_hex),
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                // Full-jitter backoff - see `upload_blob_async` (audit L-JITTER).
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

async fn read_ranges(path: &Path, ranges: &[(u64, u64)]) -> Result<Vec<u8>, CoreError> {
    let mut file = File::open(path).await?;
    let total: u64 = ranges.iter().map(|(_off, len)| *len).sum();
    let cap = usize::try_from(total)
        .map_err(|_| CoreError::InvalidArgument(format!("pack size {total} exceeds usize")))?;
    let mut buf: Vec<u8> = Vec::with_capacity(cap);
    for &(offset, len) in ranges {
        file.seek(SeekFrom::Start(offset)).await?;
        let before = buf.len();
        // read_to_end appends; take() bounds it to exactly `len` bytes.
        (&mut file).take(len).read_to_end(&mut buf).await?;
        let got = (buf.len() - before) as u64;
        if got != len {
            return Err(CoreError::Integrity(format!(
                "short read packing range at offset {offset}: wanted {len}, got {got}"
            )));
        }
    }
    Ok(buf)
}

async fn try_pack_upload_once(
    uploads_url: &str,
    body: &Bytes,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    // Re-init a fresh session per attempt (audit L2/H1) - shared with the plain path.
    let put_url = init_upload_session(uploads_url, digest, auth_token).await?;
    // Route the pack PUT through the same write-stall watchdog as the whole-file
    // path (audit H1). The bare `put.send().await` here previously left the pack
    // PUT - the wedge point behind the shared `_pack_upload_gate` - unprotected
    // against a peer that completes the (now bounded) init POST then stops draining
    // the body mid-write. Framing the in-memory buffer lets the watchdog re-stamp
    // as the socket accepts each frame (see `PUT_FRAME_BYTES`).
    let frames = pack_frames(body);
    let body_stream = futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
    let put_resp = send_put_watchdogged(
        &put_url,
        body_stream,
        auth_token,
        WRITE_STALL_TIMEOUT,
        RESPONSE_WAIT_TIMEOUT,
    )
    .await?;
    if !put_resp.status().is_success() {
        return Err(CoreError::ServerError(
            put_resp.status().as_u16(),
            format!("pack PUT failed: {:?}", put_resp.status()),
        ));
    }
    Ok(())
}

/// Slice an in-memory pack into `PUT_FRAME_BYTES`-sized body frames for the
/// watchdogged PUT. Each frame is a cheap `Bytes` view over the shared buffer (a
/// refcount bump, not a copy); framing keeps the write-stall watchdog re-stamping
/// as the peer drains rather than reading a single large frame as idle.
fn pack_frames(body: &Bytes) -> Vec<Bytes> {
    frame_bytes(body, PUT_FRAME_BYTES)
}

/// Partition `body` into `<=frame`-sized cheap `Bytes` views (refcount slices, no
/// copy). Split out from [`pack_frames`] so the partition invariant (lossless,
/// bounded frame size) is property-testable with a small frame without allocating
/// multi-`MiB` fixtures. `frame` is floored at 1 so a `0` never spins the loop.
fn frame_bytes(body: &Bytes, frame: usize) -> Vec<Bytes> {
    let frame = frame.max(1);
    let mut frames = Vec::with_capacity(body.len().div_ceil(frame).max(1));
    let mut start = 0usize;
    while start < body.len() {
        let end = (start + frame).min(body.len());
        frames.push(body.slice(start..end));
        start = end;
    }
    frames
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
    fn read_ranges_concatenates_in_order() {
        use super::read_ranges;
        use crate::error::CoreError;
        use std::io::Write;

        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            unreachable!("current-thread runtime builds")
        };
        let path = std::env::temp_dir().join(format!("hippius-rr-{}.bin", std::process::id()));
        match std::fs::File::create(&path).and_then(|mut f| f.write_all(b"0123456789")) {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        // Out-of-order, non-contiguous ranges scatter-gather in pack order:
        // [6,4)+[0,3)+[4,2) over "0123456789" -> "6789"+"012"+"45" = "678901245".
        match rt.block_on(read_ranges(&path, &[(6, 4), (0, 3), (4, 2)])) {
            Ok(bytes) => assert_eq!(bytes, b"678901245"),
            Err(_) => unreachable!("read of valid ranges must succeed"),
        }
        // A range past EOF is a short read -> Integrity error, never silent truncation.
        let bad = rt.block_on(read_ranges(&path, &[(8, 5)]));
        assert!(matches!(bad, Err(CoreError::Integrity(_))));
        std::fs::remove_file(&path).unwrap_or(());
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

#[cfg(test)]
mod tests {
    use super::CoreError;
    use bytes::Bytes;

    // --- resumable-upload pure helpers (audit: resumable chunked PATCH) ---

    #[test]
    fn committed_bytes_parses_inclusive_range() {
        // OCI `Range: 0-<end>` is inclusive -> committed = end + 1.
        assert_eq!(super::committed_bytes(Some("0-1023")), 1024);
        assert_eq!(super::committed_bytes(Some("0-9999999")), 10_000_000);
    }

    #[test]
    fn committed_bytes_treats_empty_marker_and_garbage_as_zero() {
        // `0-0` is Harbor's EMPTY-session marker (post-POST, nothing committed),
        // NOT one byte - mis-reading it as 1 would skip byte 0 on a resume.
        assert_eq!(super::committed_bytes(Some("0-0")), 0);
        assert_eq!(super::committed_bytes(None), 0);
        assert_eq!(super::committed_bytes(Some("")), 0);
        assert_eq!(super::committed_bytes(Some("0-notanumber")), 0);
        assert_eq!(super::committed_bytes(Some("bogus")), 0);
    }

    #[test]
    fn append_digest_picks_the_right_query_separator() {
        // Bare session -> `?`; a session that already carries `?_state=` -> `&`.
        assert_eq!(
            super::append_digest("https://reg/v2/x/blobs/uploads/uuid", "sha256:ab"),
            "https://reg/v2/x/blobs/uploads/uuid?digest=sha256:ab"
        );
        assert_eq!(
            super::append_digest("https://reg/v2/x/blobs/uploads/uuid?_state=z", "sha256:ab"),
            "https://reg/v2/x/blobs/uploads/uuid?_state=z&digest=sha256:ab"
        );
    }

    #[test]
    fn resolve_location_handles_absolute_and_relative() {
        // A relative Location resolves against the current session URL; an
        // absolute one replaces it wholesale.
        let base = "https://reg/v2/x/blobs/uploads/uuid?_state=a";
        assert!(
            super::resolve_location(base, "/v2/x/blobs/uploads/uuid?_state=b")
                .is_ok_and(|u| u == "https://reg/v2/x/blobs/uploads/uuid?_state=b")
        );
        assert!(
            super::resolve_location(base, "https://other/v2/x/blobs/uploads/uuid2")
                .is_ok_and(|u| u == "https://other/v2/x/blobs/uploads/uuid2")
        );
        // A malformed Location is a typed Integrity error, not a panic.
        assert!(matches!(
            super::resolve_location("not a url", "also not"),
            Err(CoreError::Integrity(_))
        ));
    }

    /// Source-grep guard. Setting `Content-Length` on a streaming PUT
    /// re-introduces the TOCTOU race fixed in audit U2: between
    /// `metadata().len()` and the actual `FramedRead` consumption the file
    /// can be rewritten, so a fixed length either truncates the body (file
    /// grew) or pads/short-sends (file shrunk). Reqwest's default of
    /// Transfer-Encoding: chunked for a `Body::wrap_stream` body matches
    /// the wire bytes to whatever the stream actually yields. If a future
    /// edit needs a known length, it must hash-and-stat the bytes it is
    /// about to send (e.g. read the file into memory once), not re-stat
    /// the disk file.
    #[test]
    fn upload_does_not_set_content_length_header() {
        // Needle assembled at runtime so this test source does not itself
        // match. The forbidden pattern is the literal `header::` + the
        // reqwest constant name for the Content-Length header.
        let needle = ["header", "CONTENT", "LENGTH"].join("::");
        let src = include_str!("uploader.rs");
        // Count must be exactly the references in *this* test's comments
        // describing what is forbidden - i.e. zero matches of the assembled
        // needle, since we never write it as a contiguous token anywhere.
        assert!(
            !src.contains(&needle),
            "uploader.rs must NOT set the Content-Length header on the streaming PUT \
             - that creates a TOCTOU race vs the file's actual size at stream time"
        );
    }

    #[tokio::test]
    async fn put_streaming_aborts_on_write_stall() {
        // Audit H1: a peer that completes TCP+TLS, reads the request head, then
        // STOPS draining the socket (zero-window) is invisible to `connect_timeout`
        // and `tcp_keepalive`, and reqwest has no per-op write timeout - so without
        // the write-stall watchdog the streamed PUT hangs forever (wedging the
        // folder upload via the shared gate). Serve exactly that stall and assert a
        // retryable `Stall` returns within the window.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Read only the request head + first bytes, then stall: never drain
                // the rest, so the client's send buffer fills and reqwest stops
                // pulling the body. Hold the socket open; send no response.
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let _ = sock.shutdown().await;
            }
        });

        let url = format!("http://{addr}/blob");
        // 8 MiB body far exceeds the OS send buffer, so reqwest keeps pulling then
        // stalls; write_stall = 1s keeps the test fast (trips within ~2 checks).
        let total: u64 = 8 * 1024 * 1024;
        let reader = tokio::io::repeat(0u8).take(total);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::put_streaming(
                &url,
                reader,
                total,
                "stalltest",
                None,
                std::time::Duration::from_secs(1),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a stalled body write must abort via the watchdog as a retryable Stall, got {outcome:?}"
        );
    }

    #[test]
    fn done_on_eof_flips_on_exhaustion() {
        // The core of the H1 watchdog fix: `done` must flip on the stream reaching
        // EOF, NOT on any byte threshold. The old code set `done` from
        // `sent >= pb_total`, which stayed false forever when the streamed length
        // undershot a pre-stat total (a truncated file), false-tripping `Stall`.
        use super::DoneOnEof;
        use futures::StreamExt;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            unreachable!("current-thread runtime builds")
        };
        let done = Arc::new(AtomicBool::new(false));
        let frames = vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"c")),
        ];
        let mut s = DoneOnEof::new(futures::stream::iter(frames), Arc::clone(&done));
        rt.block_on(async {
            assert!(!done.load(Ordering::Relaxed), "done must start false");
            assert!(s.next().await.is_some());
            assert!(
                !done.load(Ordering::Relaxed),
                "done must stay false mid-stream"
            );
            assert!(s.next().await.is_some());
            assert!(
                !done.load(Ordering::Relaxed),
                "done must stay false until EOF"
            );
            assert!(s.next().await.is_none(), "stream must exhaust");
            assert!(done.load(Ordering::Relaxed), "done must flip true on EOF");
        });
    }

    #[tokio::test]
    async fn put_streaming_tolerates_short_body_with_slow_response() {
        // Audit H1 regression: `done` is driven by the body stream reaching EOF, NOT
        // by `pb_total`. The reader yields FEWER bytes than `pb_total` (as if the
        // file were truncated between stat and stream - the U2 TOCTOU), and the
        // server drains the whole body then delays its response past the write-stall
        // window. A byte-count `done` would stay false and false-trip `Stall` on a
        // fully-sent upload; the EOF-driven `done` suppresses it and tolerates the
        // slow (JuiceFS-backpressure-shaped) commit response.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Read until the chunked body terminator (`0\r\n\r\n`), then delay the
                // response past the 1s write-stall window before a 200. The body is
                // all 0x00 bytes, so the ASCII '0'+CRLFCRLF terminator can't collide
                // with body content or a chunk-size line's trailing byte.
                let mut acc: Vec<u8> = Vec::new();
                let mut buf = [0u8; 8192];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            acc.extend_from_slice(&buf[..n]);
                            if acc.windows(5).any(|w| w == b"0\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
                let _ = sock.shutdown().await;
            }
        });

        let url = format!("http://{addr}/blob");
        // Body is 64 KiB but pb_total claims ~10 MiB more - the divergence the fix
        // must tolerate. write_stall = 1s < the server's 2s response delay.
        let actual: u64 = 64 * 1024;
        let pb_total: u64 = actual + 10 * 1024 * 1024;
        let reader = tokio::io::repeat(0u8).take(actual);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::put_streaming(
                &url,
                reader,
                pb_total,
                "shortbody",
                None,
                std::time::Duration::from_secs(1),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Ok(()))),
            "a fully-sent body shorter than pb_total, then a slow response, must NOT trip the watchdog; got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn pack_put_stall_aborts_via_shared_watchdog() {
        // Audit H1: the pack PUT now routes through `send_put_watchdogged` (was a
        // bare `send().await` with no stall protection). Drive that helper with a
        // framed in-memory body against a peer that reads the head then stops
        // draining, and assert the shared watchdog aborts with a retryable `Stall` -
        // the protection the chunked-write pack path previously lacked.
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/v2/blobs/uploads/x?digest=sha256:deadbeef");
        // 8 MiB pack -> many 1 MiB frames; far exceeds the OS send buffer so reqwest
        // stalls mid-write. write_stall = 1s keeps the test fast.
        let body = Bytes::from(vec![0u8; 8 * 1024 * 1024]);
        let frames = super::pack_frames(&body);
        let body_stream =
            futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            // Long response_wait: this test stalls the WRITE phase, so `done` never
            // flips and the response deadline must stay inert - only write_stall fires.
            super::send_put_watchdogged(
                &url,
                body_stream,
                None,
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(30),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a stalled pack PUT must abort via the shared watchdog as a retryable Stall, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn response_wait_aborts_when_peer_hangs_after_body() {
        // Audit H1 follow-up: after the body is fully sent, a peer that accepts every
        // byte then never responds (the hung JuiceFS commit case) must be cut by the
        // response-wait deadline - the gap the write-stall watchdog leaves once `done`
        // flips. Read the WHOLE request (head + the small body) so `done` flips, then
        // hang; a short response_wait keeps the test fast.
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Drain the full request (head + body) so DoneOnEof flips `done`, then
                // never write a response.
                let mut sink = [0u8; 4096];
                while let Ok(n) = sock.read(&mut sink).await {
                    if n == 0 {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/v2/blobs/uploads/x?digest=sha256:deadbeef");
        // A tiny body drains instantly, so `done` flips almost immediately and the
        // response-wait deadline (1s) governs. write_stall is generous so only the
        // response phase can trip.
        let body = Bytes::from(vec![7u8; 64]);
        let frames = super::pack_frames(&body);
        let body_stream =
            futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::send_put_watchdogged(
                &url,
                body_stream,
                None,
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(1),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a peer that hangs after accepting the body must be cut by the response-wait deadline, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn patch_stall_aborts_via_shared_watchdog() {
        // Audit H1 (review P1): the resumable PATCH - now the DEFAULT data-carrying
        // op - routes through `send_streaming_watchdogged`. `upload_client` has no
        // flat request timeout, so without the watchdog a peer that reads the head
        // then stops draining wedges the PATCH forever (the exact H1 hang this work
        // exists to kill). Drive the shared helper with a PATCH request + framed
        // body against that stall and assert a retryable `Stall` within the window.
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/v2/x/blobs/uploads/uuid");
        let Ok(client) = super::upload_client() else {
            return;
        };
        let mut req = client
            .patch(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream");
        req = req.header(reqwest::header::CONTENT_RANGE, "0-8388607");
        // 8 MiB body -> many 1 MiB frames; far exceeds the OS send buffer so reqwest
        // stalls mid-write. write_stall = 1s keeps the test fast.
        let body = Bytes::from(vec![0u8; 8 * 1024 * 1024]);
        let frames = super::frame_bytes(&body, super::PUT_FRAME_BYTES);
        let body_stream =
            futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::send_streaming_watchdogged(
                req,
                body_stream,
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(30),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a stalled PATCH must abort via the shared watchdog as a retryable Stall, got {outcome:?}"
        );
    }

    #[test]
    fn force_retryable_maps_416_to_a_retryable_error() {
        // Review P2: a 416 routed to the resume path is stored as the give-up
        // cause, but 416 is NOT `is_retryable` - returning it raw would make
        // `upload_blob_async` give up instead of restarting a fresh session.
        // `force_retryable` must wrap it in the retryable `SessionRestart`
        // variant with the original 416 preserved as the `source()` cause.
        let mapped =
            super::force_retryable(CoreError::ServerError(416, "PATCH failed: 416".into()));
        assert!(mapped.is_retryable(), "a 416 give-up must become retryable");
        assert!(
            matches!(
                mapped,
                CoreError::SessionRestart { ref source }
                    if matches!(**source, CoreError::ServerError(416, _))
            ),
            "expected SessionRestart wrapping the 416 cause, got {mapped:?}"
        );
        // ... and be reachable through the std `source()` chain, so the Python
        // boundary renders it as a `caused by:` tail.
        let cause = std::error::Error::source(&mapped);
        assert!(
            cause.is_some_and(|c| c.to_string().contains("416")),
            "the 416 must be reachable via source()"
        );
        // An already-retryable cause passes through untouched.
        let passthrough = super::force_retryable(CoreError::ServerError(503, "x".into()));
        assert!(matches!(passthrough, CoreError::ServerError(503, _)));
    }

    proptest::proptest! {
        // `frame_bytes` must be a lossless partition: concatenating the frames
        // reproduces the original bytes exactly, and every frame is within the size
        // bound (so the watchdog re-stamps at least every `frame` bytes). A small
        // frame over small data exercises the multi-frame path without multi-MiB
        // fixtures.
        #[test]
        fn frame_bytes_partitions_losslessly(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096usize),
            frame in 1usize..=64,
        ) {
            let body = Bytes::from(data.clone());
            let frames = super::frame_bytes(&body, frame);
            let mut rejoined: Vec<u8> = Vec::with_capacity(data.len());
            for f in &frames {
                proptest::prop_assert!(f.len() <= frame, "frame {} exceeds bound {}", f.len(), frame);
                proptest::prop_assert!(!f.is_empty(), "no empty frames");
                rejoined.extend_from_slice(f);
            }
            proptest::prop_assert_eq!(rejoined, data);
        }
    }

    // Audit U3 (Phase 3.11): pin the retry classification at the
    // upload-loop entry point. The downloader has the exhaustive 4xx /
    // 5xx / boundary suite in
    // `chunked_downloader::retry_classification_tests`; these two tests
    // pin the property the upload loop depends on without re-litigating
    // the downloader's coverage - the classifier is a method on
    // `CoreError`, so the two paths share one source of truth.

    #[test]
    fn upload_retry_skips_4xx() {
        // Verify that an HTTP 401 returned from the server is NOT retried -
        // a 4xx is permanent, retrying just wastes time.
        let err = CoreError::ServerError(401, "Unauthorized".into());
        assert!(
            !err.is_retryable(),
            "4xx must not be retryable; otherwise upload_blob_async wastes 1.4s before failing"
        );
    }

    #[test]
    fn upload_retry_handles_5xx() {
        let err = CoreError::ServerError(503, "Service Unavailable".into());
        assert!(err.is_retryable());
    }

    // RUST-3 (audit N-4): the upload client is a process-global singleton,
    // built once and reused across blobs/retries rather than rebuilt per
    // attempt. Two calls must hand back the SAME `&'static Client` (pointer
    // equality) - the same invariant `lib.rs::runtime_tests` pins for the
    // shared runtime. `unwrap`/`expect`/`panic!` are denied crate-wide, so we
    // assert via `is_ok` + `if let` instead of unwrapping the Result.
    #[test]
    fn upload_client_returns_same_instance() {
        let a = super::upload_client();
        let b = super::upload_client();
        assert!(a.is_ok() && b.is_ok(), "upload client must build");
        if let (Ok(a), Ok(b)) = (a, b) {
            assert!(
                std::ptr::eq(a, b),
                "upload_client must return one shared instance, not a fresh client per call"
            );
        }
    }
}
