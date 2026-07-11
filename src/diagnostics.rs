//! Transfer-speed diagnostics probe.
//!
//! Measures, against a single resolved blob URL, the same signals other tools
//! surface (`curl -w`, hf-speedtest, rclone `-P`): a per-phase handshake
//! breakdown (DNS / TCP connect / TLS), a HEAD that reveals any redirect to a
//! download host, time-to-first-byte, and — the headline signal —
//! single-stream vs parallel-stream throughput. If single-stream is slow but
//! parallel recovers, the caller is bandwidth-delay-product limited on a
//! high-RTT path and our 32-way fan-out is the mitigation working as intended.
//!
//! Everything is returned as a serializable struct (the Python layer renders
//! the report); fatal failures (can't resolve, can't connect, HEAD failed)
//! bubble up as `DiagError` since an unreachable host IS the diagnosis. Only
//! the TLS sub-probe is best-effort — a raw-handshake failure is recorded but
//! does not abort the throughput tests, which are what users care about most.

#![expect(
    clippy::cast_possible_truncation,
    reason = "Duration::as_millis() returns u128; for any real network probe the value fits in u64 with vast margin (584M years)"
)]
#![expect(
    clippy::cast_precision_loss,
    reason = "bytes/seconds folded into f64 MB/s for display; the precision loss is below the resolution we'd ever print"
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use reqwest::header::{CONTENT_LENGTH, LOCATION, RANGE};
use reqwest::{Client, Url};
use serde::Serialize;
use tokio::net::TcpStream;

// Server request-id headers worth capturing so a pasted client report can be
// grepped against the registry / object-storage backend logs.
const REQUEST_ID_HEADERS: &[&str] = &[
    "x-amz-request-id",
    "x-amz-id-2",
    "x-request-id",
    "request-id",
    "x-trace-id",
    "docker-content-digest",
];

#[expect(
    dead_code,
    reason = "payloads are surfaced via the Debug formatter in lib.rs (PyRuntimeError formats `{e:?}`), not read directly here"
)]
#[derive(Debug)]
pub enum DiagError {
    Url(String),
    Io(std::io::Error),
    Reqwest(reqwest::Error),
}

impl From<std::io::Error> for DiagError {
    fn from(err: std::io::Error) -> Self {
        DiagError::Io(err)
    }
}

impl From<reqwest::Error> for DiagError {
    fn from(err: reqwest::Error) -> Self {
        DiagError::Reqwest(err)
    }
}

#[derive(Serialize)]
pub struct ChunkTiming {
    index: usize,
    bytes: u64,
    ms: u64,
    mbps: f64,
}

#[derive(Serialize)]
pub struct DiagnosticReport {
    scheme: String,
    host: String,
    port: u16,
    resolved_ip: Option<String>,
    dns_ms: Option<u64>,
    tcp_connect_ms: Option<u64>,
    rtt_ms: Option<u64>,
    tls_handshake_ms: Option<u64>,
    tls_version: Option<String>,
    alpn: Option<String>,
    head_status: Option<u16>,
    redirected: bool,
    location: Option<String>,
    final_host: Option<String>,
    http_version: Option<String>,
    content_length: Option<u64>,
    server_request_ids: BTreeMap<String, String>,
    ttfb_ms: Option<u64>,
    probe_bytes: u64,
    single_stream_mbps: Option<f64>,
    single_stream_ms: Option<u64>,
    parallel_mbps: Option<f64>,
    parallel_ms: Option<u64>,
    max_concurrent: usize,
    parallel_chunks: Vec<ChunkTiming>,
    errors: Vec<String>,
}

/// Megabytes/second (decimal MB, matching how network throughput is usually quoted).
fn mbps(bytes: u64, dur: Duration) -> f64 {
    let secs = dur.as_secs_f64();
    if secs > 0.0 {
        bytes as f64 / 1_000_000.0 / secs
    } else {
        0.0
    }
}

/// Inclusive byte ranges covering `[0, total)` split into at most `n` contiguous
/// chunks. Returns `(start, end_inclusive)` pairs. Mirrors the production
/// downloader's range math so the parallel probe reflects a real download.
fn split_ranges(total: u64, n: usize) -> Vec<(u64, u64)> {
    if total == 0 || n == 0 {
        return Vec::new();
    }
    let chunk = total.div_ceil(n as u64);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    while start < total {
        let stop = std::cmp::min(start + chunk, total);
        ranges.push((start, stop - 1));
        start = stop;
    }
    ranges
}

/// Time a ranged GET, reading the body until EOF or `start..=end` is satisfied.
/// Returns `(bytes_read, total_elapsed, time_to_first_byte)`. The early stop
/// guards against a server that ignores Range and streams the whole blob.
async fn timed_get_range(
    client: &Client,
    url: &str,
    auth_token: Option<&str>,
    start: u64,
    end: u64,
) -> Result<(u64, Duration, Option<Duration>), DiagError> {
    let want = end - start + 1;
    let mut req = client
        .get(url)
        .header(RANGE, format!("bytes={start}-{end}"));
    if let Some(t) = auth_token {
        req = req.bearer_auth(t);
    }

    let t0 = Instant::now();
    let mut resp = req.send().await?;
    if let Err(e) = resp.error_for_status_ref() {
        return Err(DiagError::Reqwest(e));
    }

    let mut total = 0u64;
    let mut ttfb = None;
    while let Some(chunk) = resp.chunk().await? {
        if ttfb.is_none() {
            ttfb = Some(t0.elapsed());
        }
        total += chunk.len() as u64;
        if total >= want {
            break;
        }
    }
    Ok((total, t0.elapsed(), ttfb))
}

/// Collect known request-id headers from a response into `out` (later wins, so
/// the final download host's ids override the registry's when we merge).
fn collect_request_ids(headers: &reqwest::header::HeaderMap, out: &mut BTreeMap<String, String>) {
    for name in REQUEST_ID_HEADERS {
        if let Some(val) = headers.get(*name) {
            if let Ok(s) = val.to_str() {
                out.insert((*name).to_string(), s.to_string());
            }
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "linear DNS → TCP → TLS → HEAD → single-stream → parallel pipeline; splitting hides the phase ordering that makes this readable"
)]
pub async fn probe_blob(
    blob_url: &str,
    auth_token: Option<&str>,
    probe_bytes: u64,
    max_concurrent: Option<usize>,
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> Result<DiagnosticReport, DiagError> {
    let n = max_concurrent.unwrap_or(32);
    let connect_timeout = Duration::from_secs(connect_timeout_secs.unwrap_or(30));
    // Idle/read timeout (audit M-DIAG-TIMEOUT): reqwest's `.timeout()` is a TOTAL
    // deadline that would abort exactly the slow-but-working links this probe
    // exists to measure; `.read_timeout()` instead fires only when a read STALLS
    // (no byte within the window, reset on each successful read), so a stalled or
    // dribbling server ends the probe with a recorded error instead of hanging
    // forever. `.max(1)` floors a truncated sub-second knob so it can never become
    // a 0s (instant-fail) timeout.
    let read_timeout = Duration::from_secs(read_timeout_secs.unwrap_or(30).max(1));
    let mut errors: Vec<String> = Vec::new();
    let mut request_ids: BTreeMap<String, String> = BTreeMap::new();

    let url = Url::parse(blob_url).map_err(|e| DiagError::Url(e.to_string()))?;
    let scheme = url.scheme().to_string();
    let host = url
        .host_str()
        .ok_or_else(|| DiagError::Url("blob URL has no host".to_string()))?
        .to_string();
    let port = url.port_or_known_default().unwrap_or(443);

    // --- DNS ---
    // Take only the first address and let the resolver iterator drop right away,
    // so its borrow of `host` doesn't outlive into the report-construction move.
    let dns_start = Instant::now();
    let first_addr = tokio::net::lookup_host((host.as_str(), port)).await?.next();
    let dns_ms = dns_start.elapsed().as_millis() as u64;
    let resolved_ip = first_addr.map(|a| a.ip().to_string());

    // --- TCP connect (doubles as the RTT estimate) ---
    let addr = first_addr.ok_or_else(|| DiagError::Url("DNS returned no addresses".to_string()))?;
    let tcp_start = Instant::now();
    // Bound the raw connect (audit M-DIAG-TIMEOUT): the reqwest clients'
    // `connect_timeout` does not cover this bare tokio socket, so a black-holed
    // host (SYN sent, never answered) would otherwise hang the probe here forever.
    let tcp = match tokio::time::timeout(connect_timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(DiagError::Io(e)),
        Err(_) => {
            return Err(DiagError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("TCP connect to {addr} timed out after {connect_timeout:?}"),
            )))
        }
    };
    let tcp_connect_ms = tcp_start.elapsed().as_millis() as u64;

    // TLS handshake sub-probe was removed because the only ecosystem option
    // for a raw timed handshake on an already-open socket (tokio-rustls 0.24 +
    // webpki-roots 0.25) carried RUSTSEC-2026-0098/0099/0104. The newer
    // rustls 0.23 line is API-incompatible and adds a CryptoProvider install
    // dance; the timing it would buy us isn't worth the migration here.
    // `tls_handshake_ms / tls_version / alpn` stay in the report shape but
    // are always None — `format_report` already renders that gracefully.
    drop(tcp);
    let tls_handshake_ms: Option<u64> = None;
    let tls_version: Option<String> = None;
    let alpn: Option<String> = None;

    // --- HEAD with redirects disabled, to reveal a redirect to a download host ---
    let probe_client = Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut head_req = probe_client.head(blob_url);
    if let Some(t) = auth_token {
        head_req = head_req.bearer_auth(t);
    }
    let head_resp = head_req.send().await?;
    let head_status = head_resp.status().as_u16();
    let version = head_resp.version();
    let http_version = Some(format!("{version:?}"));
    let redirected = head_resp.status().is_redirection();
    let location = head_resp
        .headers()
        .get(LOCATION)
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string);
    let final_host = location
        .as_deref()
        .and_then(|loc| Url::parse(loc).ok())
        .and_then(|u| u.host_str().map(ToString::to_string));
    let content_length = head_resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    collect_request_ids(head_resp.headers(), &mut request_ids);

    // Clamp the probe to the blob size when known, so we never try to read past EOF.
    let probe = match content_length {
        Some(cl) if cl > 0 => std::cmp::min(probe_bytes, cl),
        _ => probe_bytes,
    };

    // --- Transfer client: follows redirects, HTTP/1.1 only, matching production ---
    let transfer_client = Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .http1_only()
        .pool_max_idle_per_host(n)
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let mut ttfb_ms = None;
    let mut single_stream_mbps = None;
    let mut single_stream_ms = None;
    let mut parallel_mbps = None;
    let mut parallel_ms = None;
    let mut parallel_chunks: Vec<ChunkTiming> = Vec::new();
    // How many bytes we actually probed. Starts at the (possibly content-length
    // clamped) request target and is corrected down to what the single stream
    // really transferred — see below.
    let mut effective_bytes = probe;

    if probe == 0 {
        errors.push("blob is empty; skipping throughput probes".to_string());
    } else {
        // Single stream: one ranged GET, also yielding TTFB on its first byte.
        let (bytes, elapsed, ttfb) =
            timed_get_range(&transfer_client, blob_url, auth_token, 0, probe - 1).await?;
        ttfb_ms = ttfb.map(|d| d.as_millis() as u64);
        single_stream_ms = Some(elapsed.as_millis() as u64);
        single_stream_mbps = Some(mbps(bytes, elapsed));

        // Parallel: split across `n` connections. Use the bytes the single
        // stream actually transferred as ground truth, NOT the unclamped
        // `probe`: when the blob HEAD was a redirect (no Content-Length) and the
        // file is smaller than `probe`, splitting `probe` would issue ranges
        // past EOF → HTTP 416 and fail the whole probe. The single stream
        // already stopped at EOF, so `bytes` is the real downloadable size.
        effective_bytes = bytes;
        let ranges = split_ranges(effective_bytes, n);
        let par_start = Instant::now();
        let results: Vec<Result<(usize, u64, Duration), DiagError>> = stream::iter(
            ranges.into_iter().enumerate(),
        )
        .map(|(i, (start, end))| {
            let client = transfer_client.clone();
            let url = blob_url.to_string();
            let token = auth_token.map(ToString::to_string);
            async move {
                let (bytes, elapsed, _) =
                    timed_get_range(&client, &url, token.as_deref(), start, end).await?;
                Ok((i, bytes, elapsed))
            }
        })
        .buffer_unordered(n)
        .collect()
        .await;
        let par_elapsed = par_start.elapsed();

        let mut total_bytes = 0u64;
        for r in results {
            // Audit M-DIAG-ABORT: a single failed parallel range (429/503/416, or a
            // reset on one stream) must NOT discard the whole report — DNS, TCP,
            // HEAD and the single-stream result are already collected, and the
            // rate-limit verdict reads `parallel_chunks`. Record the failure and
            // compute throughput over the survivors so a partial report still returns
            // on exactly the lossy/rate-limited paths this probe targets.
            match r {
                Ok((index, bytes, elapsed)) => {
                    total_bytes += bytes;
                    parallel_chunks.push(ChunkTiming {
                        index,
                        bytes,
                        ms: elapsed.as_millis() as u64,
                        mbps: mbps(bytes, elapsed),
                    });
                }
                Err(e) => errors.push(format!("parallel chunk failed: {e:?}")),
            }
        }
        parallel_chunks.sort_by_key(|c| c.index);
        parallel_ms = Some(par_elapsed.as_millis() as u64);
        parallel_mbps = Some(mbps(total_bytes, par_elapsed));
    }

    Ok(DiagnosticReport {
        scheme,
        host,
        port,
        resolved_ip,
        dns_ms: Some(dns_ms),
        tcp_connect_ms: Some(tcp_connect_ms),
        rtt_ms: Some(tcp_connect_ms),
        tls_handshake_ms,
        tls_version,
        alpn,
        head_status: Some(head_status),
        redirected,
        location,
        final_host,
        http_version,
        content_length,
        server_request_ids: request_ids,
        ttfb_ms,
        probe_bytes: effective_bytes,
        single_stream_mbps,
        single_stream_ms,
        parallel_mbps,
        parallel_ms,
        max_concurrent: n,
        parallel_chunks,
        errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_ranges_empty_or_zero() {
        assert!(split_ranges(0, 4).is_empty());
        assert!(split_ranges(100, 0).is_empty());
    }

    #[test]
    fn split_ranges_covers_exactly() {
        // 1000 bytes over 4 chunks → 250 each, inclusive ranges, no gaps/overlap.
        let r = split_ranges(1000, 4);
        assert_eq!(r, vec![(0, 249), (250, 499), (500, 749), (750, 999)]);
    }

    #[test]
    fn split_ranges_remainder_truncates_last() {
        // 1001 bytes over 4 → ceil(1001/4)=251-byte chunks; the last range is
        // truncated at the final byte (251*3=753 .. 1000 inclusive).
        let r = split_ranges(1001, 4);
        assert_eq!(r, vec![(0, 250), (251, 501), (502, 752), (753, 1000)]);
        // Ranges are contiguous and cover the whole blob with no overlap.
        let covered: u64 = r.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(covered, 1001);
    }

    #[test]
    fn split_ranges_fewer_bytes_than_chunks() {
        // 3 bytes, 8 chunks → at most 3 single-byte ranges, never empty ones.
        let r = split_ranges(3, 8);
        assert_eq!(r, vec![(0, 0), (1, 1), (2, 2)]);
    }

    #[test]
    #[expect(clippy::float_cmp, reason = "mbps short-circuits to literal 0.0 for zero-duration; testing exact equality is the contract")]
    fn mbps_zero_duration_is_zero() {
        assert_eq!(mbps(1000, Duration::from_secs(0)), 0.0);
    }

    #[test]
    fn mbps_basic() {
        // 1_000_000 bytes in 1s = 1 MB/s.
        assert!((mbps(1_000_000, Duration::from_secs(1)) - 1.0).abs() < 1e-9);
    }
}
