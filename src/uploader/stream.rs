use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use crate::error::CoreError;
use crate::uploader::cdc::{run_chunk_pipeline, PipelineMsg, BATCH_QUEUE_DEPTH, CHUNK_BATCH};

/// One step pulled off a [`ChunkStreamCore`] by its consumer.
#[derive(Debug)]
pub(crate) enum StreamStep {
    /// Next file-ordered batch of `(sha256_hex, offset, length)` triples.
    Batch(Vec<(String, u64, u64)>),
    /// End of stream: every batch was emitted; [`ChunkStreamCore::finish`]
    /// now returns the whole-file hex. Idempotent - repeat calls stay `Eof`.
    Eof,
    /// No message within the wait window; the stream is untouched. This is
    /// the caller's interrupt-poll point (`py.check_signals` in `lib.rs`).
    TimedOut,
}

/// State machine behind [`ChunkStreamCore`]: `Streaming` until the terminal
/// pipeline message arrives, then `Finished` (EOF observed, producer joined,
/// whole-file hex stored) or `Failed` (error/interrupt observed - poisoned so
/// every subsequent call raises a clear error instead of hanging).
enum StreamState {
    /// Pipeline live: the receiver is the consumer end of the forward channel
    /// and `producer` is the plain thread running [`run_chunk_pipeline`].
    Streaming {
        rx: mpsc::Receiver<PipelineMsg>,
        producer: std::thread::JoinHandle<()>,
    },
    /// `Done` observed and the producer joined; `finish` returns the hex.
    Finished { whole_hex: String },
    /// Terminal failure or abort; `message` explains why on every later call.
    Failed { message: String },
}

/// Pull side of a spawned chunk pipeline - the plain-Rust core the
/// `ChunkStream` pyclass (`lib.rs`) thinly wraps, kept Python-free so the
/// state machine is unit-testable without an interpreter.
///
/// [`Self::spawn`] starts a plain `std::thread` (pure CPU + file I/O - no
/// tokio, same reasoning as `chunk_and_hash_native`'s `py.detach`) that runs
/// [`run_chunk_pipeline`] with a consume closure forwarding every
/// [`PipelineMsg`] into a bounded channel owned by this struct. Dropping the
/// receiver (consumer gone, or [`Self::abort`]) fails the forward send, the
/// closure stops draining, and the pipeline's acyclic shutdown unwinds every
/// thread - no extra teardown protocol needed.
pub(crate) struct ChunkStreamCore {
    state: StreamState,
}

impl ChunkStreamCore {
    /// Start the producer thread over `path`. Cannot fail here by design:
    /// the file open (and the `avg_size` validation inside the pipeline)
    /// happen on the producer thread, so their errors surface as the first
    /// [`Self::next_msg`] result instead.
    pub(crate) fn spawn(path: PathBuf, avg_size: u64) -> Self {
        let (tx, rx) = mpsc::sync_channel::<PipelineMsg>(BATCH_QUEUE_DEPTH);
        let producer = std::thread::spawn(move || {
            let source = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    // A failed send means the consumer already dropped the
                    // stream - there is no one left to report to.
                    let _ = tx.send(PipelineMsg::Fail(CoreError::Io(e)));
                    return;
                }
            };
            // Forward every message - terminal included - per
            // run_chunk_pipeline's consume contract. A failed forward means
            // the consumer dropped the stream: stop draining so the pipeline
            // unwinds through its acyclic shutdown.
            let run = run_chunk_pipeline(source, avg_size, CHUNK_BATCH, |out_rx| {
                for msg in out_rx {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });
            if let Err(e) = run {
                // Pre-pipeline validation error (avg_size out of range).
                let _ = tx.send(PipelineMsg::Fail(e));
            }
        });
        Self {
            state: StreamState::Streaming { rx, producer },
        }
    }

    /// Pull the next step, waiting at most `timeout` for a message.
    ///
    /// Terminal transitions happen here: `Done` joins the producer and moves
    /// to `Finished`; `Fail` (or a disconnect without a terminal message, or
    /// a producer panic) poisons the state to `Failed` and returns the error.
    /// A poisoned stream keeps returning a clear `Integrity` error - it can
    /// never hang, because the receiver was dropped at the transition.
    pub(crate) fn next_msg(&mut self, timeout: Duration) -> Result<StreamStep, CoreError> {
        let received = match &self.state {
            StreamState::Finished { .. } => return Ok(StreamStep::Eof),
            StreamState::Failed { message } => {
                return Err(CoreError::Integrity(message.clone()));
            }
            StreamState::Streaming { rx, .. } => rx.recv_timeout(timeout),
        };
        match received {
            Ok(PipelineMsg::Batch(batch)) => Ok(StreamStep::Batch(batch)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(StreamStep::TimedOut),
            Ok(PipelineMsg::Done { whole_hex }) => {
                // The producer already forwarded its terminal message, so the
                // join below is a quick reap, not a wait; a panicked producer
                // (unreachable by construction) outranks the Done.
                self.teardown("chunk stream producer thread panicked")?;
                self.state = StreamState::Finished { whole_hex };
                Ok(StreamStep::Eof)
            }
            Ok(PipelineMsg::Fail(e)) => {
                // Poison with the rendered failure so later calls explain
                // themselves; the pipeline's own error outranks a join panic.
                let _ = self.teardown(&format!("chunk stream failed: {e}"));
                Err(e)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Protocol breach: the producer died without a terminal
                // message. Surfaced as a typed error, never a panic,
                // mirroring collect_batches' ctrl-channel handling.
                self.teardown("chunk stream closed without a terminal message")?;
                Err(CoreError::Integrity(
                    "chunk stream closed without a terminal message".to_string(),
                ))
            }
        }
    }

    /// Whole-file sha256 hex, available once [`Self::next_msg`] returned
    /// [`StreamStep::Eof`] (the producer thread was joined at that
    /// transition, so this is a pure state read). A live stream is refused -
    /// never silently drained: the caller's drain-to-EOF is the explicit
    /// contract - and a failed stream re-raises its poison message.
    pub(crate) fn finish(&self) -> Result<String, CoreError> {
        match &self.state {
            StreamState::Streaming { .. } => Err(CoreError::InvalidArgument(
                "chunk stream not finished; drain next_batch to None first".to_string(),
            )),
            StreamState::Finished { whole_hex } => Ok(whole_hex.clone()),
            StreamState::Failed { message } => Err(CoreError::Integrity(message.clone())),
        }
    }

    /// Abandon a live stream (the Ctrl-C path): poison the state and drop the
    /// receiver so the forward send fails and the pipeline unwinds. Returns
    /// the producer's handle WITHOUT joining - the interrupt should propagate
    /// immediately, and the thread exits on its own once its send fails
    /// (proved by `abort_mid_stream_terminates_the_producer`). `None` if the
    /// stream was already terminal (which is left untouched).
    pub(crate) fn abort(&mut self) -> Option<std::thread::JoinHandle<()>> {
        let StreamState::Streaming { .. } = &self.state else {
            return None;
        };
        let prior = std::mem::replace(
            &mut self.state,
            StreamState::Failed {
                message: "chunk stream interrupted".to_string(),
            },
        );
        let StreamState::Streaming { rx, producer } = prior else {
            // Just matched Streaming above; only reachable if a refactor
            // splits the check from the replace.
            return None;
        };
        drop(rx);
        Some(producer)
    }

    /// Terminal teardown: poison the state to `Failed { message }`, drop the
    /// receiver (unwinding a still-running pipeline through its acyclic
    /// shutdown), and join the producer. The caller overwrites the poison on
    /// the success path (`Done` -> `Finished`). Errors if the producer
    /// panicked - unreachable by construction (pipeline threads do not
    /// panic) but surfaced as a typed error, never a propagated panic.
    fn teardown(&mut self, message: &str) -> Result<(), CoreError> {
        let prior = std::mem::replace(
            &mut self.state,
            StreamState::Failed {
                message: message.to_string(),
            },
        );
        let StreamState::Streaming { rx, producer } = prior else {
            return Ok(());
        };
        drop(rx);
        producer
            .join()
            .map_err(|_| CoreError::Integrity("chunk stream producer thread panicked".to_string()))
    }
}

#[cfg(test)]
mod chunk_stream_tests {
    use super::{ChunkStreamCore, StreamState, StreamStep};
    use crate::error::CoreError;
    use crate::uploader::cdc::{chunk_and_hash, ChunkList, PipelineMsg};
    use std::sync::mpsc;
    use std::time::Duration;

    const AVG: u64 = 512; // matches cdc_tests: min 128, max 2048

    /// Roomy per-recv wait for tests that expect real pipeline progress; the
    /// drain loops below tolerate `TimedOut` so a slow CI box retries rather
    /// than flaking.
    const WAIT: Duration = Duration::from_secs(10);

    fn temp_file_with(tag: &str, data: &[u8]) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("hippius-cs-{tag}-{}.bin", std::process::id()));
        match std::fs::write(&path, data) {
            Ok(()) => path,
            Err(_) => unreachable!("temp file write"),
        }
    }

    /// Drain a live stream to EOF, concatenating every batch.
    fn drain(core: &mut ChunkStreamCore) -> ChunkList {
        let mut streamed = ChunkList::new();
        loop {
            match core.next_msg(WAIT) {
                Ok(StreamStep::Batch(batch)) => streamed.extend(batch),
                Ok(StreamStep::Eof) => return streamed,
                Ok(StreamStep::TimedOut) => {}
                Err(e) => unreachable!("stream over valid input cannot fail: {e:?}"),
            }
        }
    }

    #[test]
    fn stream_over_a_real_file_matches_chunk_and_hash() {
        // Equivalence at the stream level: the spawned pull stream must emit
        // exactly what the buffering wrapper returns for the same file.
        let mut data = vec![0u8; 200_000];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i * 31 % 251).unwrap_or(0);
        }
        let path = temp_file_with("equiv", &data);
        let Ok((expected_whole, expected_chunks)) = chunk_and_hash(&path, AVG) else {
            unreachable!("chunk_and_hash over a valid file cannot fail")
        };

        let mut core = ChunkStreamCore::spawn(path.clone(), AVG);
        let streamed = drain(&mut core);
        assert_eq!(
            streamed, expected_chunks,
            "streamed batches must concatenate to the buffered chunk list"
        );

        // EOF is idempotent, and finish returns the whole-file hex after it.
        match core.next_msg(WAIT) {
            Ok(StreamStep::Eof) => {}
            other => unreachable!("next_msg after EOF must stay EOF, got {other:?}"),
        }
        match core.finish() {
            Ok(hex) => assert_eq!(hex, expected_whole),
            Err(e) => unreachable!("finish after EOF cannot fail: {e:?}"),
        }
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[test]
    fn finish_before_eof_refuses_without_draining() {
        let path = temp_file_with("early-finish", &vec![5u8; 50_000]);
        let mut core = ChunkStreamCore::spawn(path.clone(), AVG);

        // The contract is explicit: finish never silently drains.
        match core.finish() {
            Err(CoreError::InvalidArgument(msg)) => {
                assert!(msg.contains("not finished"), "message must say why: {msg}");
            }
            other => unreachable!("finish before EOF must refuse, got {other:?}"),
        }

        // The refusal must not have consumed the stream: draining still works.
        let streamed = drain(&mut core);
        assert!(!streamed.is_empty());
        assert!(core.finish().is_ok());
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[test]
    fn abort_mid_stream_terminates_the_producer() {
        // 8 MiB of constant bytes -> thousands of chunks, so the pipeline is
        // provably still mid-flight after one batch (cap-2 forward channel).
        let path = temp_file_with("abort", &vec![7u8; 8 * 1024 * 1024]);
        let mut core = ChunkStreamCore::spawn(path.clone(), AVG);
        loop {
            match core.next_msg(WAIT) {
                Ok(StreamStep::Batch(_)) => break,
                Ok(StreamStep::TimedOut) => {}
                other => unreachable!("expected a first batch, got {other:?}"),
            }
        }

        // Dropping the receiver must unwind the whole pipeline: prove the
        // producer thread terminates by joining it under a timeout.
        let Some(producer) = core.abort() else {
            unreachable!("a live stream must yield its producer handle")
        };
        let (joined_tx, joined_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = joined_tx.send(producer.join().is_ok());
        });
        match joined_rx.recv_timeout(WAIT) {
            Ok(true) => {}
            Ok(false) => unreachable!("producer must exit cleanly after abort, not panic"),
            Err(_) => unreachable!("producer did not terminate after abort - pipeline leak"),
        }

        // Aborted == poisoned: subsequent calls raise clearly, never hang.
        match core.next_msg(Duration::from_millis(10)) {
            Err(CoreError::Integrity(msg)) => assert!(msg.contains("interrupted"), "{msg}"),
            other => unreachable!("aborted stream must raise, got {other:?}"),
        }
        assert!(core.finish().is_err());
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[test]
    fn missing_file_surfaces_on_first_next_msg_and_poisons() {
        // spawn cannot fail on I/O - the open happens inside the producer
        // thread - so a bad path must surface as the FIRST message.
        let path = std::env::temp_dir().join(format!(
            "hippius-cs-definitely-missing-{}.bin",
            std::process::id()
        ));
        let mut core = ChunkStreamCore::spawn(path, AVG);
        match core.next_msg(WAIT) {
            Err(CoreError::Io(_)) => {}
            other => unreachable!("expected the open failure, got {other:?}"),
        }
        match core.next_msg(Duration::from_millis(10)) {
            Err(CoreError::Integrity(msg)) => {
                assert!(msg.contains("chunk stream failed"), "{msg}");
            }
            other => unreachable!("failed stream must stay failed, got {other:?}"),
        }
        assert!(core.finish().is_err());
    }

    #[test]
    fn out_of_range_avg_surfaces_on_first_next_msg() {
        let path = temp_file_with("bad-avg", b"some bytes");
        let mut core = ChunkStreamCore::spawn(path.clone(), 1);
        match core.next_msg(WAIT) {
            Err(CoreError::InvalidArgument(_)) => {}
            other => unreachable!("expected the avg validation failure, got {other:?}"),
        }
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[test]
    fn next_msg_times_out_and_preserves_the_stream() {
        // Hand-built Streaming state with a silent channel: a timeout must be
        // a non-event (the Ctrl-C poll point), not a state transition.
        let (tx, rx) = mpsc::sync_channel(2);
        let producer = std::thread::spawn(|| {});
        let mut core = ChunkStreamCore {
            state: StreamState::Streaming { rx, producer },
        };
        match core.next_msg(Duration::from_millis(10)) {
            Ok(StreamStep::TimedOut) => {}
            other => unreachable!("expected a timeout, got {other:?}"),
        }
        let _ = tx.send(PipelineMsg::Batch(vec![("aa".to_string(), 0, 2)]));
        match core.next_msg(WAIT) {
            Ok(StreamStep::Batch(batch)) => assert_eq!(batch.len(), 1),
            other => unreachable!("the stream must survive a timeout, got {other:?}"),
        }
    }

    #[test]
    fn panicked_producer_surfaces_as_a_typed_error() {
        // A producer that dies without a terminal message (channel just
        // disconnects) must surface the panic through join, never hang or
        // pretend EOF.
        let (tx, rx) = mpsc::sync_channel::<PipelineMsg>(2);
        let producer = std::thread::spawn(|| {
            let n = std::hint::black_box(1);
            assert!(n == 2, "deliberate panic: simulated producer crash");
        });
        drop(tx);
        let mut core = ChunkStreamCore {
            state: StreamState::Streaming { rx, producer },
        };
        match core.next_msg(WAIT) {
            Err(CoreError::Integrity(msg)) => assert!(msg.contains("panicked"), "{msg}"),
            other => unreachable!("expected the panic to surface, got {other:?}"),
        }
    }
}
