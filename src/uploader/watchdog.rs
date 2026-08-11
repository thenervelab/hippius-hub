use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use reqwest::header;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::error::CoreError;
use crate::uploader::upload_client;

/// Upload write-stall watchdog window (audit H1). If reqwest stops pulling body
/// bytes for this long while the body is NOT yet fully sent, the send is aborted
/// with a retryable [`CoreError::Stall`]. Gating on "body not yet fully sent"
/// means a legitimately slow blob-commit RESPONSE (`JuiceFS` backpressure can make
/// a commit take many seconds) never trips it - the watchdog guards only the
/// write phase, which reqwest itself offers no per-operation timeout for.
pub(super) const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);

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
pub(super) const RESPONSE_WAIT_TIMEOUT: Duration = Duration::from_mins(2);

/// Frame size the in-memory pack PUT body is sliced into before streaming (audit
/// H1). The write-stall watchdog re-stamps its progress clock only when reqwest
/// pulls the next body frame, so the frame must drain in well under
/// `WRITE_STALL_TIMEOUT` on any link we mean to support, or a slow-but-progressing
/// peer false-trips `Stall`. At 1 `MiB` the watchdog cut a healthy uplink slower
/// than ~35 KB/s (one frame > 30 s); 256 `KiB` drops that floor to ~8.5 KB/s so a
/// throttled/shared link uploads instead of failing. The slices are cheap `Bytes`
/// views (a refcount bump, not a copy), so more frames cost no extra allocation.
pub(super) const PUT_FRAME_BYTES: usize = 256 * 1024;

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
pub(super) async fn send_streaming_watchdogged<S>(
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
pub(super) async fn send_put_watchdogged<S>(
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

/// Slice an in-memory pack into `PUT_FRAME_BYTES`-sized body frames for the
/// watchdogged PUT. Each frame is a cheap `Bytes` view over the shared buffer (a
/// refcount bump, not a copy); framing keeps the write-stall watchdog re-stamping
/// as the peer drains rather than reading a single large frame as idle.
pub(super) fn pack_frames(body: &Bytes) -> Vec<Bytes> {
    frame_bytes(body, PUT_FRAME_BYTES)
}

/// Partition `body` into `<=frame`-sized cheap `Bytes` views (refcount slices, no
/// copy). Split out from [`pack_frames`] so the partition invariant (lossless,
/// bounded frame size) is property-testable with a small frame without allocating
/// multi-`MiB` fixtures. `frame` is floored at 1 so a `0` never spins the loop.
pub(super) fn frame_bytes(body: &Bytes, frame: usize) -> Vec<Bytes> {
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
mod tests {
    use super::CoreError;
    use bytes::Bytes;

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
}
