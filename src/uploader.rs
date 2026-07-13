use bytes::Bytes;
use fastcdc::v2020::StreamCDC;
use futures::stream::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, header};
use sha2::{Digest, Sha256};
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
/// 5 GiB blob) for no benefit — the hash is bandwidth-bound, not latency-bound.
const HASH_READ_BUFFER: usize = 8 * 1024 * 1024;

/// Per-chunk `(sha256_hex, offset, length)` in file order — the plan a chunked
/// upload works from (offset re-reads the range; digest dedups and addresses).
pub type ChunkList = Vec<(String, u64, u64)>;

/// `FastCDC` average chunk size bounds — pinned to the library's own average
/// range `[AVERAGE_MIN, AVERAGE_MAX]` = `[256 B, 4 MiB]`. The average is the wire
/// contract (see the chunked-artifact plan); min = avg/4 and max = avg*4 are the
/// standard normalized-chunking ratios. Those derivations must ALSO stay under
/// `FastCDC`'s *separate* `MINIMUM_MAX` (1 MiB) and `MAXIMUM_MAX` (16 MiB) ceilings
/// or `StreamCDC::new` panics — and they do so exactly over this interval: at the
/// 4 MiB ceiling min = 1 MiB = `MINIMUM_MAX` and max = 16 MiB = `MAXIMUM_MAX`, the
/// caps themselves. So `[256 B, 4 MiB]` is the largest average range that can
/// never panic. The old 256 MiB cap let averages like the 64 MiB default through
/// to a `StreamCDC::new` panic (min = 16 MiB > `MINIMUM_MAX`); an out-of-range
/// average is now surfaced as a caller error, never clamped and never panicked.
/// (`cdc_bounds_track_fastcdc_limits` asserts these equal the crate constants so a
/// `FastCDC` bump can't silently reopen the panic.)
const CDC_MIN_AVG: u64 = 256; // == fastcdc::v2020::AVERAGE_MIN
const CDC_MAX_AVG: u64 = 4 * 1024 * 1024; // == fastcdc::v2020::AVERAGE_MAX (4 MiB)

/// Chunk a file with `FastCDC` and hash each chunk plus the whole file in one
/// streaming pass (bounded memory — `StreamCDC` never loads the whole file).
///
/// Returns `(whole_file_sha256_hex, [(chunk_sha256_hex, offset, length)])` in
/// file order. The offsets let the caller re-read each chunk's byte range for a
/// parallel upload; the digests drive `HEAD`-dedup and content-addressing.
/// Determinism: for a fixed `avg_size` the boundaries are a pure function of the
/// bytes, so identical files chunk identically and dedup — hence `avg_size` is
/// pinned by the caller, not tuned per upload.
pub fn chunk_and_hash(path: &Path, avg_size: u64) -> Result<(String, ChunkList), CoreError> {
    chunk_and_hash_reader(std::fs::File::open(path)?, avg_size)
}

/// Reader-based core of [`chunk_and_hash`], split out so tests can drive it from
/// an in-memory `Cursor` (no temp file, no I/O `unwrap`). Semantics are
/// identical: `StreamCDC` yields the same boundaries whether the source is a
/// file or a cursor over the same bytes.
fn chunk_and_hash_reader<R: std::io::Read>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    if !(CDC_MIN_AVG..=CDC_MAX_AVG).contains(&avg_size) {
        return Err(CoreError::Integrity(format!(
            "FastCDC average size {avg_size} out of range [{CDC_MIN_AVG}, {CDC_MAX_AVG}]"
        )));
    }
    // The range check above guarantees min/avg/max fit u32; try_from keeps that
    // provable to clippy without an unchecked `as` cast.
    let to_u32 = |v: u64| -> Result<u32, CoreError> {
        u32::try_from(v).map_err(|_| CoreError::Integrity(format!("chunk size {v} exceeds u32")))
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
/// reads — neither benefits from running on a tokio worker thread, and the
/// combination starves other futures on the same runtime for seconds on
/// multi-GB blobs. We route the whole pass through `spawn_blocking` so the
/// runtime keeps its worker threads free for actual async work (HTTP, other
/// downloads). `std::fs::File` + `std::io::Read` are the right primitives
/// inside the blocking pool; the async wrappers would only re-block the same
/// thread.
///
/// The double `?` at the end is load-bearing: `spawn_blocking(...).await`
/// produces `Result<Result<T, CoreError>, JoinError>`. We collapse the
/// outer `JoinError` (panic in the closure / runtime shutdown) into our
/// `CoreError::Io` variant via `io::Error::other` so callers see one
/// error surface, then `?` unwraps the inner Result.
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
    .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?
}

/// Mirror of [`crate::chunked_downloader::MAX_RETRIES`] for the upload
/// path. Audit U3 (Phase 3.11): the downloader retried per-chunk up to
/// 3 times; the uploader did not retry at all, so a single transient
/// 503 lost the whole upload. The two paths now share the same budget
/// and the same [`CoreError::is_retryable`] classifier — see
/// `try_upload_blob_once` for the per-attempt body.
const UPLOAD_MAX_RETRIES: u32 = 3;

/// Upload write-stall watchdog window (audit H1). If reqwest stops pulling body
/// bytes for this long while the body is NOT yet fully sent, the send is aborted
/// with a retryable [`CoreError::Stall`]. Gating on "body not yet fully sent"
/// means a legitimately slow blob-commit RESPONSE (`JuiceFS` backpressure can make
/// a commit take many seconds) never trips it — the watchdog guards only the
/// write phase, which reqwest itself offers no per-operation timeout for.
const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll cadence for the write-stall watchdog. One second keeps the abort latency
/// bounded without measurable overhead against a multi-GB streamed body.
const WRITE_STALL_CHECK: Duration = Duration::from_secs(1);

/// Total budget for the zero-body pack upload-init POST (audit H1). Init only
/// allocates an upload session and returns a `Location`; it has no legitimate
/// slow path, so a tight total timeout turns a hung/black-holed registry into a
/// retryable error instead of blocking `try_pack_upload_once` forever — which
/// (via the shared `_pack_upload_gate`) would otherwise wedge the whole folder
/// upload. Unlike the streamed PUT body, a `.timeout()` here can't clip an honest
/// transfer because there is no body to stream.
const INIT_POST_TIMEOUT: Duration = Duration::from_secs(30);

/// Frame size the in-memory pack PUT body is sliced into before streaming (audit
/// H1). The write-stall watchdog re-stamps its progress clock only when reqwest
/// pulls the next body frame, so a single 64 `MiB` frame would read as idle for the
/// whole write and false-trip `Stall` against a slow-but-progressing peer. 1 `MiB`
/// frames re-stamp often enough that only a genuinely stalled socket trips the
/// watchdog, and the slices are cheap `Bytes` views (a refcount bump, not a copy).
const PUT_FRAME_BYTES: usize = 1024 * 1024;

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar — useful for large blobs (multi-GB).
///
/// Audit U3 (Phase 3.11): wraps [`try_upload_blob_once`] in an
/// exponential-backoff retry loop with the same shape as
/// [`crate::chunked_downloader::download_chunk_with_retry`]. Each attempt re-inits
/// a fresh OCI upload session AND re-opens the file inside `try_upload_blob_once`
/// (the previous session is consumed and the previous `FramedRead` stream is spent),
/// so a retry never re-PUTs a dead session (audit L2). Backoff schedule: 200, 400,
/// 800, 1600 ms — four attempts total, ~3 s of backoff before surfacing a transient
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
/// `FramedRead` stream, and sends one PUT — so the retry loop above
/// gets a fresh body on every attempt (the previous `Body::wrap_stream`
/// is consumed once the request future completes or errors).
/// Process-global HTTP client for blob uploads.
///
/// Mirrors the downloader, which builds its `reqwest::Client` once in
/// `ChunkedDownloader::new` and reuses it across all chunks. Previously
/// `try_upload_blob_once` rebuilt a client on every call — once per blob and
/// once per retry — discarding the keep-alive connection pool and forcing a
/// fresh DNS+TCP+TLS handshake against the registry host the previous blob just
/// finished using (audit N-4 / RUST-3). The `OnceLock` hoists construction out
/// of the per-attempt path so warm connections survive between blobs.
///
/// Construction is fallible (`build()` errors if the TLS backend won't
/// initialize), so this returns `Result` rather than `expect`-ing inside a
/// `get_or_init` closure — the crate denies `panic`/`unwrap` and warns on
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
    // No flat whole-request timeout (audit H-UPLOAD-TIMEOUT): reqwest's client
    // `.timeout()` is a wall-clock deadline over the ENTIRE request including the
    // streamed body, so a 1h cap silently bounded total *transfer* time — a
    // legitimately slow large upload (multi-GB model on a slow uplink, the default
    // non-chunked path) would trip it, and because a reqwest timeout is
    // `is_retryable()`, `upload_blob_async` then re-streamed the whole file from
    // byte 0 up to the retry budget (~4× the wall). A dead or stalled peer is
    // instead detected by `connect_timeout` (handshake) + `tcp_keepalive` (an
    // idle/half-open connection) without ever capping an honest transfer — the
    // exact policy `download_client` uses.
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

/// Milliseconds since `base`, saturating a u128→u64 cast that only overflows
/// after ~584 million years of uptime — keeps clippy's truncation lint satisfied
/// without an `unwrap`.
fn elapsed_ms(base: Instant) -> u64 {
    u64::try_from(base.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Stream adapter that flips `done` to `true` the instant the inner stream is
/// exhausted (`poll_next` → `Ready(None)`).
///
/// This is the "body fully sent" signal for the write-stall watchdog. reqwest
/// polls the body to EOF exactly when it has taken every byte, so EOF is the only
/// reliable end-of-write marker — a pre-stream `metadata().len()` can diverge from
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
        Self { inner: Box::pin(inner), done }
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

/// Send a chunked-encoded PUT of `body_stream`, aborting with a retryable
/// [`CoreError::Stall`] if reqwest stops pulling body frames for `write_stall` —
/// i.e. a peer that completed the handshake then stopped draining the socket, the
/// H1 wedge that `connect_timeout`/`tcp_keepalive` cannot see and reqwest offers
/// no per-operation write timeout for.
///
/// Shared by the whole-file ([`put_streaming`]) and pack ([`try_pack_upload_once`])
/// PUT paths so both get the same protection. `done` is driven off the body stream
/// reaching end-of-input (see [`DoneOnEof`]), so the write phase is guarded while a
/// legitimately slow blob-commit RESPONSE (`JuiceFS` backpressure) never trips it,
/// correct even when the streamed length diverges from any earlier stat.
///
/// Atomics (not a lock) so the body-stamping closure never holds a guard across
/// reqwest's `.await` and the watchdog's reads are wait-free; `Relaxed` is enough
/// because the watchdog only needs to eventually observe the latest stamp, not a
/// happens-before edge (no data is published through the flags).
async fn send_put_watchdogged<S>(
    url: &str,
    body_stream: S,
    auth_token: Option<&str>,
    write_stall: Duration,
) -> Result<reqwest::Response, CoreError>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let client = upload_client()?;

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

    let mut req = client
        .put(url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }

    // Drive the send, aborting if the body WRITE stalls (audit H1). Dropping the
    // send future on a stall (the `return` below) cancels the reqwest request and
    // severs the socket the peer stopped draining. select! polls the pinned send
    // future and re-arms a 1s timer each round; the timer branch checks idle time
    // only while `done` is false, so the response-wait phase is never tripped.
    let send_fut = req.send();
    tokio::pin!(send_fut);
    let stall_ms = u64::try_from(write_stall.as_millis()).unwrap_or(u64::MAX);
    loop {
        tokio::select! {
            r = &mut send_fut => return Ok(r?),
            () = tokio::time::sleep(WRITE_STALL_CHECK) => {
                if !done.load(Ordering::Relaxed) {
                    let idle = elapsed_ms(base).saturating_sub(last_ms.load(Ordering::Relaxed));
                    if idle >= stall_ms {
                        return Err(CoreError::Stall(Duration::from_millis(idle)));
                    }
                }
            }
        }
    }
}

/// Stream `reader` to `url` as a chunked-encoded PUT body, ticking a progress
/// bar sized `pb_total`. Shared by the whole-file and byte-range upload paths.
///
/// No explicit Content-Length: reqwest falls back to Transfer-Encoding: chunked
/// for a `wrap_stream` body, so the wire length matches whatever the reader
/// actually yields — the TOCTOU-safe behaviour audit U2 established for the
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
    pb.set_message(format!("📤 {basename}"));

    // Tick the progress bar on every body frame; the watchdog's write-progress
    // stamping lives in `send_put_watchdogged`. `freeze()` hands reqwest an
    // immutable `Bytes` (a move of the `BytesMut` buffer, not a copy). ProgressBar
    // is Arc-internally → cloning is cheap.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(reader, BytesCodec::new()).map(move |frame| {
        frame.map(|bytes| {
            pb_stream.inc(bytes.len() as u64);
            bytes.freeze()
        })
    });

    let res = match send_put_watchdogged(url, stream, auth_token, write_stall).await {
        Ok(res) => res,
        Err(e) => {
            let msg = match &e {
                CoreError::Stall(_) => format!("❌ {basename} stalled"),
                _ => format!("❌ {basename} failed"),
            };
            pb.finish_with_message(msg);
            return Err(e);
        }
    };
    if !res.status().is_success() {
        pb.finish_with_message(format!("❌ {basename} failed"));
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }
    pb.finish_with_message(format!("✅ {basename} uploaded"));
    Ok(())
}

fn basename_of(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| "blob".to_string(), |n| n.to_string_lossy().into_owned())
}

async fn try_upload_blob_once(
    uploads_url: &str,
    path: &Path,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    // Re-init a fresh OCI upload session on every attempt (audit L2): a PUT to a
    // session a prior failed attempt already consumed fails, so init must live inside
    // the retried unit (symmetry with `try_pack_upload_once`), not once on the Python
    // side re-PUTting the same dead session.
    let put_url = init_upload_session(uploads_url, digest, auth_token).await?;
    let file = File::open(path).await?;
    // Size is for the progress bar only — see put_streaming on why it is not a
    // Content-Length.
    let file_size = file.metadata().await?.len();
    put_streaming(&put_url, file, file_size, &basename_of(path), auth_token, WRITE_STALL_TIMEOUT).await
}

/// POST-init a fresh OCI blob-upload session at `uploads_url`, resolve the returned
/// `Location`, and append `?digest={digest}` — the URL a monolithic PUT-with-digest
/// targets. Shared by the plain ([`try_upload_blob_once`]) and pack
/// ([`try_pack_upload_once`]) paths so BOTH re-init per retry attempt (audit L2).
async fn init_upload_session(
    uploads_url: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
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
    let init_resp = init.send().await?;
    if !init_resp.status().is_success() {
        return Err(CoreError::ServerError(
            init_resp.status().as_u16(),
            "blob upload init failed".to_string(),
        ));
    }
    let location = init_resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CoreError::Integrity("registry omitted Location on upload init".to_string()))?;
    // Resolve a possibly-relative Location against the uploads URL, then append the
    // digest as a RAW query pair (":" is legal unencoded in a query; percent-encoding
    // it via query_pairs_mut breaks the registry's digest match).
    let resolved = reqwest::Url::parse(uploads_url)
        .and_then(|base| base.join(location))
        .map_err(|e| CoreError::Integrity(format!("bad upload Location {location:?}: {e}")))?;
    let mut put_url = resolved.to_string();
    put_url.push(if put_url.contains('?') { '&' } else { '?' });
    put_url.push_str("digest=");
    put_url.push_str(digest);
    Ok(put_url)
}

/// Read the given file byte-ranges in order into one pack blob and push it via a
/// fresh OCI upload session (POST init + monolithic PUT-with-digest). Returns the
/// pack's sha256 hex — the chunked-v2 caller records it in the pointer blob.
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
    // in-flight pack costs one pack_size instead of two — `read_ranges`' `Vec`
    // converts in without reallocating. The prior `.body(buf.to_vec())` re-copied
    // the whole pack on each attempt, which the staging peak-RSS benchmark showed
    // roughly doubled resident memory per concurrent upload.
    let body = Bytes::from(read_ranges(path, ranges).await?);
    // Hash the ~64 MiB pack on the blocking pool (audit L14): the digest is
    // CPU-bound and would otherwise stall the runtime's other in-flight pack
    // uploads for the duration. The `Bytes` clone into the closure is a refcount
    // bump, not a copy, so the pack is still buffered exactly once.
    let body_for_hash = body.clone();
    let digest_hex = tokio::task::spawn_blocking(move || hex::encode(Sha256::digest(&body_for_hash)))
        .await
        .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?;
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
                // Full-jitter backoff — see `upload_blob_async` (audit L-JITTER).
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

async fn read_ranges(path: &Path, ranges: &[(u64, u64)]) -> Result<Vec<u8>, CoreError> {
    let mut file = File::open(path).await?;
    let total: u64 = ranges.iter().map(|(_off, len)| *len).sum();
    let cap = usize::try_from(total)
        .map_err(|_| CoreError::Integrity(format!("pack size {total} exceeds usize")))?;
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
    // Re-init a fresh session per attempt (audit L2/H1) — shared with the plain path.
    let put_url = init_upload_session(uploads_url, digest, auth_token).await?;
    // Route the pack PUT through the same write-stall watchdog as the whole-file
    // path (audit H1). The bare `put.send().await` here previously left the pack
    // PUT — the wedge point behind the shared `_pack_upload_gate` — unprotected
    // against a peer that completes the (now bounded) init POST then stops draining
    // the body mid-write. Framing the in-memory buffer lets the watchdog re-stamp
    // as the socket accepts each frame (see `PUT_FRAME_BYTES`).
    let frames = pack_frames(body);
    let body_stream = futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
    let put_resp = send_put_watchdogged(&put_url, body_stream, auth_token, WRITE_STALL_TIMEOUT).await?;
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

/// Partition `body` into `≤frame`-sized cheap `Bytes` views (refcount slices, no
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
    use super::{CDC_MAX_AVG, CDC_MIN_AVG, chunk_and_hash_reader};
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    const AVG: u64 = 512; // → min 128, max 2048; small enough for fast tests

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

        let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
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
        // caller error caught before the splitter — the exact value the staging
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
        // min = 1 MiB and max = 16 MiB are its exact ceilings — this must chunk, not
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

    #[test]
    fn boundaries_reshuffle_only_locally_on_a_late_edit() {
        // Determinism + shift-locality (the CDC payoff): inserting a byte near
        // the END must leave the FIRST chunk's digest unchanged — content-defined
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

            // Determinism: same bytes → identical chunk boundaries + digests.
            let (whole2, chunks2) = chunk(&data);
            proptest::prop_assert_eq!(whole, whole2);
            proptest::prop_assert_eq!(chunks, chunks2);
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
        // describing what is forbidden — i.e. zero matches of the assembled
        // needle, since we never write it as a contiguous token anywhere.
        assert!(
            !src.contains(&needle),
            "uploader.rs must NOT set the Content-Length header on the streaming PUT \
             — that creates a TOCTOU race vs the file's actual size at stream time"
        );
    }

    #[tokio::test]
    async fn put_streaming_aborts_on_write_stall() {
        // Audit H1: a peer that completes TCP+TLS, reads the request head, then
        // STOPS draining the socket (zero-window) is invisible to `connect_timeout`
        // and `tcp_keepalive`, and reqwest has no per-op write timeout — so without
        // the write-stall watchdog the streamed PUT hangs forever (wedging the
        // folder upload via the shared gate). Serve exactly that stall and assert a
        // retryable `Stall` returns within the window.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else { return };
        let Ok(addr) = listener.local_addr() else { return };
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
            super::put_streaming(&url, reader, total, "stalltest", None, std::time::Duration::from_secs(1)),
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
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
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
            assert!(!done.load(Ordering::Relaxed), "done must stay false mid-stream");
            assert!(s.next().await.is_some());
            assert!(!done.load(Ordering::Relaxed), "done must stay false until EOF");
            assert!(s.next().await.is_none(), "stream must exhaust");
            assert!(done.load(Ordering::Relaxed), "done must flip true on EOF");
        });
    }

    #[tokio::test]
    async fn put_streaming_tolerates_short_body_with_slow_response() {
        // Audit H1 regression: `done` is driven by the body stream reaching EOF, NOT
        // by `pb_total`. The reader yields FEWER bytes than `pb_total` (as if the
        // file were truncated between stat and stream — the U2 TOCTOU), and the
        // server drains the whole body then delays its response past the write-stall
        // window. A byte-count `done` would stay false and false-trip `Stall` on a
        // fully-sent upload; the EOF-driven `done` suppresses it and tolerates the
        // slow (JuiceFS-backpressure-shaped) commit response.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else { return };
        let Ok(addr) = listener.local_addr() else { return };
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
                let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
                let _ = sock.shutdown().await;
            }
        });

        let url = format!("http://{addr}/blob");
        // Body is 64 KiB but pb_total claims ~10 MiB more — the divergence the fix
        // must tolerate. write_stall = 1s < the server's 2s response delay.
        let actual: u64 = 64 * 1024;
        let pb_total: u64 = actual + 10 * 1024 * 1024;
        let reader = tokio::io::repeat(0u8).take(actual);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::put_streaming(&url, reader, pb_total, "shortbody", None, std::time::Duration::from_secs(1)),
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
        // draining, and assert the shared watchdog aborts with a retryable `Stall` —
        // the protection the chunked-write pack path previously lacked.
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else { return };
        let Ok(addr) = listener.local_addr() else { return };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/v2/blobs/uploads/x?digest=sha256:deadbeef");
        // 8 MiB pack → many 1 MiB frames; far exceeds the OS send buffer so reqwest
        // stalls mid-write. write_stall = 1s keeps the test fast.
        let body = Bytes::from(vec![0u8; 8 * 1024 * 1024]);
        let frames = super::pack_frames(&body);
        let body_stream = futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::send_put_watchdogged(&url, body_stream, None, std::time::Duration::from_secs(1)),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a stalled pack PUT must abort via the shared watchdog as a retryable Stall, got {outcome:?}"
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

    // Audit U3 (Phase 3.11): pin the retry classification at the
    // upload-loop entry point. The downloader has the exhaustive 4xx /
    // 5xx / boundary suite in
    // `chunked_downloader::retry_classification_tests`; these two tests
    // pin the property the upload loop depends on without re-litigating
    // the downloader's coverage — the classifier is a method on
    // `CoreError`, so the two paths share one source of truth.

    #[test]
    fn upload_retry_skips_4xx() {
        // Verify that an HTTP 401 returned from the server is NOT retried —
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
    // equality) — the same invariant `lib.rs::runtime_tests` pins for the
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
