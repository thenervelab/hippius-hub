//! The shared download transport: the process-global HTTP/1 client, its
//! timeouts, the default-on read-idle stall guard, and the process-global
//! pack gate that bounds cross-file concurrency.

use reqwest::Client;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::error::CoreError;

const CONNECT_TIMEOUT_SECS: u64 = 30;

/// Default per-chunk-read idle timeout for downloads (audit M4). Bounds a peer that
/// completes the handshake then dribbles or stops mid-body: reset on each successful
/// read, so it fires only on genuine no-progress (a 30s gap with zero bytes), never
/// on a slow-but-steady transfer. Default-ON - unlike the opt-in client
/// `read_timeout` - and overridden by `HIPPIUS_READ_TIMEOUT` when set. Scoped per
/// chunk read (an app-level `tokio::time::timeout`), not a global client setting, so
/// it fixes the slow-loris the 5-minute total timeout would otherwise leave open for
/// minutes.
const DOWNLOAD_READ_IDLE: Duration = Duration::from_secs(30);

/// Idle-connection cap for the shared download client. Bounds only *idle*
/// (kept-alive) connections, not in-flight requests - the per-file `Semaphore`
/// (`PackAssembler`) and spawn count (`ChunkedDownloader`) are the real concurrency
/// bounds, so a fixed value is safe regardless of a caller's `max_concurrent`. 32
/// matches the default `max_concurrent`. This does change the pack path's idle-pool
/// sizing (previously `pool_max_idle_per_host(max_concurrent)`) to a fixed cap; a
/// caller running `HIPPIUS_MAX_CONCURRENT` above 32 keeps up to 32 warm idle
/// connections rather than `max_concurrent`, which only affects idle reuse, not the
/// real (semaphore-bounded) concurrency.
const DOWNLOAD_POOL_MAX_IDLE: usize = 32;

/// Process-global HTTP/1 client shared by both download paths (pack assembly here
/// and the legacy Range downloader). Mirrors `uploader::upload_client`: building a
/// `Client` per native call starts with an empty pool and forces a fresh
/// DNS+TCP+TLS handshake to the registry host on every file; the `OnceLock` hoists
/// construction out of the per-file path so warm connections survive across files
/// (the win for many-small-file snapshots). Auth is applied per request, so the
/// shared client carries no per-file credential across origins.
///
/// HTTP/1-only for the same reason the per-call clients were: h2 would multiplex
/// every parallel chunk onto one TCP and cap aggregate throughput at the
/// per-connection ceiling; h1 lets each chunk claim its own connection.
///
/// Construction is fallible (the TLS backend may fail to init), so this returns
/// `Result` rather than `expect`-ing inside a `get_or_init` closure - the crate
/// denies `panic`/`unwrap`. On an init race the loser's freshly built client is
/// dropped unused (RAII); `OnceLock` is valid in statics and never poisoned.
/// Connect + read timeouts for the shared download client. Resolved in Python
/// (`constants.resolve_connect_timeout` / `resolve_read_timeout`) and threaded
/// down so `HIPPIUS_CONNECT_TIMEOUT` / `HIPPIUS_READ_TIMEOUT` reach real
/// transfers, not only `hippius-hub diagnose` (audit L9). `connect` bounds the
/// handshake; `read` is a *stalled-read* bound (reset on each successful read).
///
/// `read` is `Option`: `None` leaves the shared client's *opt-in* `.read_timeout()`
/// off, so the client is byte-for-byte the pre-audit one. The DEFAULT-ON download
/// stall guard (audit M4) lives at the app level instead - [`read_chunk_bounded`]
/// bounds each `res.chunk()` read by [`download_read_idle`] (30s, or
/// `HIPPIUS_READ_TIMEOUT` when set), scoped per chunk rather than as a global client
/// setting. So a slow-loris is cut by default; setting `HIPPIUS_READ_TIMEOUT`
/// additionally arms the client's per-request `.read_timeout()` and lowers the
/// app-level window to the same value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransportTimeouts {
    pub connect: Duration,
    pub read: Option<Duration>,
}

impl Default for TransportTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(CONNECT_TIMEOUT_SECS),
            read: None,
        }
    }
}

impl TransportTimeouts {
    /// Build from optional per-operation seconds; `None` keeps the field default
    /// (`connect` -> 30s, `read` -> no client read timeout). Non-positive values
    /// are unrepresentable - Python's `_resolve_positive_int` rejects them first.
    pub(crate) fn from_secs(connect: Option<u64>, read: Option<u64>) -> Self {
        let d = Self::default();
        Self {
            connect: connect.map_or(d.connect, Duration::from_secs),
            read: read.map(Duration::from_secs),
        }
    }
}

/// Pure client builder, split from `download_client` so a test can assert the
/// read-timeout behavior on a fresh client without racing the process-global
/// singleton (whose first caller fixes its config for the whole process).
fn build_download_client(timeouts: TransportTimeouts) -> Result<Client, CoreError> {
    let mut builder = Client::builder()
        .connect_timeout(timeouts.connect)
        .http1_only()
        .pool_max_idle_per_host(DOWNLOAD_POOL_MAX_IDLE)
        .tcp_keepalive(Duration::from_secs(30));
    if let Some(read) = timeouts.read {
        // Opt-in only (see `TransportTimeouts`): fires on a stalled read (no byte
        // within the window, reset on each successful read), bounding a peer that
        // handshakes then dribbles/stops mid-body - which `connect_timeout` and
        // `tcp_keepalive` cannot see and the per-chunk 5-min total `.timeout()`
        // only catches after 5 minutes (audit M4).
        builder = builder.read_timeout(read);
    }
    Ok(builder.build()?)
}

/// The resolved per-chunk-read idle timeout (audit M4), fixed by the first
/// `download_client` caller (first-caller-wins, like the client itself): every file
/// in a snapshot passes the same env-derived value. `HIPPIUS_READ_TIMEOUT` overrides
/// the `DOWNLOAD_READ_IDLE` default. Read via [`download_read_idle`].
static READ_IDLE: OnceLock<Duration> = OnceLock::new();

/// The default-on download read-idle timeout (audit M4). Falls back to
/// `DOWNLOAD_READ_IDLE` if no download has started yet; in practice the read loops
/// only run after `download_client` has fixed it, so the fallback is belt-and-braces.
pub(crate) fn download_read_idle() -> Duration {
    READ_IDLE.get().copied().unwrap_or(DOWNLOAD_READ_IDLE)
}

/// One response-body read bounded by `idle` (audit M4): a `res.chunk()` yielding no
/// data within `idle` is a retryable [`CoreError::ReadStall`], so a peer that stops
/// mid-body is cut promptly instead of running out the per-chunk 5-minute total
/// timeout. `idle` applies per call (per successful read resets it), so a slow-but-
/// steady transfer is never tripped. Shared by both download read loops. `idle` is a
/// parameter (not read from the global) so tests can drive a short window.
pub(crate) async fn read_chunk_bounded(
    res: &mut reqwest::Response,
    idle: Duration,
) -> Result<Option<bytes::Bytes>, CoreError> {
    match tokio::time::timeout(idle, res.chunk()).await {
        Ok(chunk) => Ok(chunk?),
        Err(_elapsed) => Err(CoreError::ReadStall(idle)),
    }
}

pub(crate) fn download_client(timeouts: TransportTimeouts) -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // First-caller-wins (like `global_pack_gate`): the process-global client is
    // built once with the first download's resolved timeouts. Every file in a
    // snapshot passes the same env-derived values, so the winner is representative;
    // a later differing value is ignored - the documented tradeoff of one shared
    // pool. The loser of an init race drops its freshly built client (RAII).
    let built = build_download_client(timeouts)?;
    let client = CLIENT.get_or_init(|| built);
    // Fix the default-on read-idle window (audit M4) alongside the client, same
    // first-caller-wins discipline; HIPPIUS_READ_TIMEOUT overrides the default.
    let _ = READ_IDLE.get_or_init(|| timeouts.read.unwrap_or(DOWNLOAD_READ_IDLE));
    Ok(client)
}

/// Process-global cap on packs in flight across ALL concurrent downloads (every
/// file in a snapshot), so the nested snapshot-workers x per-file-concurrency
/// parallelism cannot multiply resident 64 MiB pack buffers into an OOM
/// (8 workers x 32 x 64 MiB ~ 16 GB worst case). Sized from the FIRST call's
/// `max_concurrent` (first-caller-wins, like `download_client`): in a uniform
/// snapshot every file passes the same value, so the total in-flight budget equals
/// one file's concurrency - a single large file is never throttled, and N files
/// SHARE that budget rather than each getting the full amount. Mirrors the upload
/// path's `_pack_upload_gate`.
pub(super) fn global_pack_gate(max_concurrent: usize) -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(GATE.get_or_init(|| Arc::new(Semaphore::new(max_concurrent))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn read_chunk_bounded_trips_readstall_on_a_stalled_body() {
        // Audit M4: a peer that sends the head + a few body bytes then stalls (no
        // more data, socket held open) must be cut by the app-level per-read idle
        // window as a retryable ReadStall - not left until the 5-minute total
        // timeout. The client here has NO client read_timeout (default), so the
        // app-level ReadStall is the sole guard, proving it is default-on.
        let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                // Advertise 1000 bytes, send 8, then stall (hold the socket open).
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\nABCDEFGH")
                    .await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let url = format!("http://{addr}/blob");
        let Ok(client) = build_download_client(TransportTimeouts::default()) else {
            unreachable!("client builds")
        };
        let Ok(mut res) = client.get(&url).send().await else {
            unreachable!("GET connects")
        };
        let idle = Duration::from_millis(200);
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match read_chunk_bounded(&mut res, idle).await {
                    Ok(Some(_)) => {}
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        })
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::ReadStall(_)))),
            "a stalled body read must abort as a retryable ReadStall, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn read_timeout_aborts_a_stalled_response_body() {
        // Audit M4: a peer that completes the handshake, sends response headers +
        // a few body bytes, then goes silent (an application-layer stall
        // `connect_timeout`/`tcp_keepalive` cannot see) must be cut by the client's
        // `read_timeout`. Without it the body read hangs until the caller's 5-min
        // total timeout; the download plane's whole point is to fail fast and retry.
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await; // consume the request line/headers
                                                   // Promise 1_000_000 bytes, deliver 8, then stall without closing.
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nabcdefgh")
                    .await;
                let _ = sock.flush().await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let Ok(client) = build_download_client(TransportTimeouts {
            connect: Duration::from_secs(5),
            read: Some(Duration::from_secs(1)),
        }) else {
            server.abort();
            return;
        };
        let url = format!("http://{addr}/blob");
        // `Ok(Err(_))` = the inner future finished with a reqwest error (read_timeout
        // fired - correct). `Err(_)` = the test's own 8s bound elapsed, i.e. the read
        // hung because `read_timeout` was NOT honored (the regression this guards).
        let outcome = tokio::time::timeout(Duration::from_secs(8), async {
            let resp = client.get(&url).send().await?;
            resp.bytes().await
        })
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(_))),
            "a stalled body read must abort via read_timeout, got {outcome:?}"
        );
    }
}
