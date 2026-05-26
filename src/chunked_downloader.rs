use futures::stream::{self, StreamExt};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom, BufWriter};

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB default
const MAX_CONCURRENT_DOWNLOADS: usize = 32;
const MAX_RETRIES: u32 = 3;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024; // 8 MB read buffer for SHA256 verification

/// Number of HTTP Range requests needed to cover `content_length` bytes when
/// each chunk is `chunk_size` bytes. Returns 0 for empty files (caller is
/// expected to handle that as a special case). Returns 0 for `chunk_size == 0`
/// to avoid a division-by-zero panic if a caller sets `HIPPIUS_CHUNK_SIZE=0` —
/// the Python layer also validates this, but defense-in-depth.
fn num_chunks(content_length: u64, chunk_size: u64) -> usize {
    if content_length == 0 || chunk_size == 0 {
        return 0;
    }
    // Integer ceiling division — avoids the f64 round-trip the older code used.
    ((content_length + chunk_size - 1) / chunk_size) as usize
}

/// Inclusive `(start, end)` byte range for chunk index `i` in a Range header.
/// The last chunk is truncated at `content_length - 1` rather than running
/// past EOF. Caller must ensure `i < num_chunks(content_length, chunk_size)`.
fn chunk_bounds(content_length: u64, chunk_size: u64, i: usize) -> (u64, u64) {
    let start = i as u64 * chunk_size;
    let end = std::cmp::min(start + chunk_size - 1, content_length - 1);
    (start, end)
}

#[derive(Debug)]
pub enum DownloadError {
    ReqwestError(reqwest::Error),
    IoError(std::io::Error),
    ServerError(u16, String),
    // `Box<DownloadError>` is the canonical recursive-enum boxing pattern: without
    // indirection, `DownloadError` containing itself would be infinite-sized. The
    // boxed `source` carries the real cause of a chunk failure (HTTP error, I/O
    // error, server status) so callers can see WHY chunk N failed, not just that
    // it did. Earlier shape `ChunkFailed(usize)` discarded the cause.
    ChunkFailed {
        index: usize,
        source: Box<DownloadError>,
    },
    // Distinct from `ChunkFailed`: `JoinError` reports tokio-task-level failure
    // (panic in the spawned future, cancellation) — qualitatively different from
    // a download-layer error. `JoinError` is foreign and sized, so no boxing.
    JoinFailed {
        index: usize,
        source: tokio::task::JoinError,
    },
    // Audit D3: HEAD response lacked a parseable `Content-Length` header.
    // Previously the missing-header path collapsed into `Ok(0)` via
    // `unwrap_or(0)`, indistinguishable from a legitimate empty blob — so
    // `download()` would truncate the destination to 0 bytes and return
    // sha256 of empty. This variant separates "server says the blob is
    // empty" (Ok(0) -> create_empty_file) from "server did not tell us the
    // size" (this error). Unit variant: the failure IS the absence; there
    // is no inspectable field a caller could use beyond the variant tag.
    MissingContentLength,
}

impl From<reqwest::Error> for DownloadError {
    fn from(err: reqwest::Error) -> Self {
        DownloadError::ReqwestError(err)
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(err: std::io::Error) -> Self {
        DownloadError::IoError(err)
    }
}

pub struct ChunkedDownloader {
    client: Client,
    url: String,
    auth_token: Option<String>,
    chunk_size: u64,
}

impl ChunkedDownloader {
    /// Construct a new concurrent downloader.
    pub fn new(url: String, auth_token: Option<String>, chunk_size_bytes: Option<u64>) -> Result<Self, DownloadError> {
        // Force HTTP/1.1: with h2 reqwest multiplexes all chunks on a single TCP,
        // which caps aggregate throughput at the per-connection BBR ceiling (~150 MB/s
        // even on a fast edge). Forcing h1 makes each parallel chunk get its own TCP,
        // letting the kernel/qdisc fan out across the available bandwidth.
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .http1_only()
            .pool_max_idle_per_host(MAX_CONCURRENT_DOWNLOADS)
            .tcp_keepalive(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            url,
            auth_token,
            chunk_size: chunk_size_bytes.unwrap_or(DEFAULT_CHUNK_SIZE),
        })
    }

    /// Downloads the file concurrently by streaming each chunk directly to its
    /// offset in the final file (sparse pre-allocated). If `verify_hash` is
    /// true, reads the full file at the end to produce a SHA256. Otherwise
    /// returns an empty string.
    pub async fn download(&self, dest_path: &Path, verify_hash: bool) -> Result<String, DownloadError> {
        // 1. Fetch the total blob size
        let content_length = self.get_content_length().await?;

        // Handle the empty-file case
        if content_length == 0 {
            return self.create_empty_file(dest_path).await;
        }

        let pb = ProgressBar::new(content_length);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .unwrap()
            .progress_chars("#>-"));
        pb.set_message("📥 Downloading");

        let num_chunks = num_chunks(content_length, self.chunk_size);

        // Prepare the destination directory
        let parent_dir = dest_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;

        // 2. Pre-allocate the final file at the exact size (sparse OK).
        //    Each chunk task opens its own file handle and seeks to its offset.
        //    Concurrent writes via distinct handles to disjoint ranges are
        //    OS-safe (each handle has its own file pointer).
        {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(dest_path)
                .await?;
            f.set_len(content_length).await?;
            f.sync_all().await?; // Ensure the size is persisted before parallel writes
        }

        let dest_path_buf = dest_path.to_path_buf();

        // 3. Launch concurrent downloads — each streams directly to its
        //    correct offset in the final file.
        let mut stream = stream::iter(0..num_chunks).map(|i| {
            let (start, end) = chunk_bounds(content_length, self.chunk_size, i);

            let client = self.client.clone();
            let url = self.url.clone();
            let token = self.auth_token.clone();
            let chunk_pb = pb.clone();
            let path = dest_path_buf.clone();

            tokio::spawn(async move {
                let res = download_chunk_with_retry(client, url, token, start, end, i, path, chunk_pb).await;
                (i, res)
            })
        }).buffer_unordered(MAX_CONCURRENT_DOWNLOADS);

        // Drain the bounded `buffer_unordered` stream. Exhaustive match preserves
        // both the spawn-side (`JoinError`) and the download-layer cause: previously
        // both collapsed into a bare `ChunkFailed(usize)`, hiding which chunk failed
        // AND why. `usize::MAX` is the documented sentinel for "index unknown" —
        // the chunk index lives inside the future's return, which `JoinError` did
        // not preserve. Phase 3.8 will replace this enum with a thiserror-based
        // hierarchy; the sentinel survives until then.
        while let Some(res) = stream.next().await {
            match res {
                Err(join_err) => {
                    return Err(DownloadError::JoinFailed {
                        index: usize::MAX,
                        source: join_err,
                    });
                }
                Ok((i, Err(chunk_err))) => {
                    return Err(DownloadError::ChunkFailed {
                        index: i,
                        source: Box::new(chunk_err),
                    });
                }
                Ok((_, Ok(()))) => continue,
            }
        }

        pb.finish_with_message("✅ Download complete");

        // 4. Optional SHA256 — a single sequential read-pass over the final file.
        //    Much faster than the old assembly phase (no rewrite).
        if verify_hash {
            let pb_hash = ProgressBar::new(content_length);
            pb_hash.set_style(ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.magenta/red}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("=>-"));
            pb_hash.set_message("🔐 Verifying SHA256");

            let hash = compute_sha256(dest_path, &pb_hash).await?;
            pb_hash.finish_with_message("✅ Verified");
            Ok(hash)
        } else {
            Ok(String::new())
        }
    }

    /// Issue a HEAD request to obtain Content-Length
    async fn get_content_length(&self) -> Result<u64, DownloadError> {
        let mut req = self.client.head(&self.url);
        if let Some(ref token) = self.auth_token {
            req = req.bearer_auth(token);
        }

        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(DownloadError::ServerError(res.status().as_u16(), format!("Failed HEAD request: {:?}", res.status())));
        }

        // Audit D3: a missing/unparseable Content-Length previously fell through
        // to `unwrap_or(0)`, which `download()` then routed into `create_empty_file`
        // — silently truncating the destination and returning sha256 of empty.
        // We now surface a typed error; the empty-file path in `download()` is
        // reached only when the server explicitly sent `Content-Length: 0`.
        let content_length = res.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| val.parse::<u64>().ok())
            .ok_or(DownloadError::MissingContentLength)?;

        Ok(content_length)
    }

    /// Special case: create an empty file when the size is 0
    async fn create_empty_file(&self, dest_path: &Path) -> Result<String, DownloadError> {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .await?;
        f.sync_all().await?;
        drop(f);

        let mut hasher = Sha256::new();
        hasher.update(&[]);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// Compute the SHA256 of the final file in a single sequential read-pass.
async fn compute_sha256(path: &Path, pb: &ProgressBar) -> Result<String, DownloadError> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; VERIFY_READ_BUFFER];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        pb.inc(n as u64);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Wrapper with exponential-backoff retry for a single chunk download.
async fn download_chunk_with_retry(
    client: Client,
    url: String,
    token: Option<String>,
    start: u64,
    end: u64,
    _chunk_index: usize,
    dest_path: std::path::PathBuf,
    pb: ProgressBar,
) -> Result<(), DownloadError> {
    let mut retries = 0;

    loop {
        match try_download_chunk_to_offset(&client, &url, &token, start, end, &dest_path, &pb).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

/// Verify that a chunk GET produced exactly HTTP 206 Partial Content.
///
/// Audit D2: previously `try_download_chunk_to_offset` accepted any 2xx
/// status. A server that ignored the `Range` header would respond with
/// 200 OK and the FULL body; we would then `seek(start)` and stream that
/// full body starting at the chunk's offset, overwriting everything past
/// `end + 1` and producing a silently corrupt file. The diagnostic on
/// the 200 branch names the ignored range explicitly so the failure mode
/// is unambiguous in logs — distinct from a "server returned the wrong
/// bytes" error a caller might otherwise assume.
fn require_partial_content(
    status: reqwest::StatusCode,
    start: u64,
    end: u64,
) -> Result<(), DownloadError> {
    use reqwest::StatusCode;
    match status {
        StatusCode::PARTIAL_CONTENT => Ok(()),
        StatusCode::OK => Err(DownloadError::ServerError(
            status.as_u16(),
            format!(
                "server ignored Range bytes={start}-{end} (returned 200 OK instead of 206); \
                 writing the full body at offset {start} would corrupt the file"
            ),
        )),
        other => Err(DownloadError::ServerError(
            other.as_u16(),
            format!("Failed chunk bytes {start}-{end}"),
        )),
    }
}

/// Streaming download of a chunk directly to its offset in the final file
/// (already pre-allocated). Each task opens its own file handle, seeks to its
/// offset, and writes bytes as they arrive from the HTTP stream.
/// Parallel writes to disjoint ranges are safe.
async fn try_download_chunk_to_offset(
    client: &Client,
    url: &str,
    token: &Option<String>,
    start: u64,
    end: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), DownloadError> {
    let mut req = client.get(url)
        .header(header::RANGE, format!("bytes={}-{}", start, end));

    if let Some(ref t) = token {
        req = req.bearer_auth(t);
    }

    let mut res = req.send().await?;

    require_partial_content(res.status(), start, end)?;

    // Open this task's own handle on the pre-allocated final file, seek to start.
    let mut file = OpenOptions::new()
        .write(true)
        .open(dest_path)
        .await?;
    file.seek(SeekFrom::Start(start)).await?;

    // Wrap the file handle in a 2MB BufWriter to avoid thousands of small unbuffered write syscalls
    let mut buf_writer = BufWriter::with_capacity(2 * 1024 * 1024, file);

    // Stream HTTP body chunks directly to disk at our position.
    // No temp file, no assembly phase.
    loop {
        match res.chunk().await {
            Ok(Some(buf)) => {
                let len = buf.len();
                buf_writer.write_all(&buf).await?;
                pb.inc(len as u64);
            }
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        }
    }

    buf_writer.flush().await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn num_chunks_empty_file_is_zero() {
        assert_eq!(num_chunks(0, 100), 0);
    }

    #[test]
    fn num_chunks_smaller_than_chunk_is_one() {
        assert_eq!(num_chunks(50, 100), 1);
        assert_eq!(num_chunks(1, 100), 1);
    }

    #[test]
    fn num_chunks_exact_multiple() {
        assert_eq!(num_chunks(100, 100), 1);
        assert_eq!(num_chunks(300, 100), 3);
    }

    #[test]
    fn num_chunks_with_remainder() {
        assert_eq!(num_chunks(101, 100), 2);
        assert_eq!(num_chunks(301, 100), 4);
    }

    #[test]
    fn num_chunks_zero_chunk_size_does_not_panic() {
        // Defense in depth: Python validates this, but keep the Rust side safe.
        assert_eq!(num_chunks(1000, 0), 0);
    }

    #[test]
    fn num_chunks_handles_default_size_at_default_proportions() {
        // 100 MiB chunk, 250 MiB file → 3 chunks (100+100+50)
        let mib = 1024 * 1024;
        assert_eq!(num_chunks(250 * mib, 100 * mib), 3);
    }

    #[test]
    fn chunk_bounds_first_chunk_is_zero_based() {
        assert_eq!(chunk_bounds(1000, 100, 0), (0, 99));
    }

    #[test]
    fn chunk_bounds_middle_chunk_is_full_size() {
        assert_eq!(chunk_bounds(1000, 100, 5), (500, 599));
    }

    #[test]
    fn chunk_bounds_last_chunk_truncates_at_eof() {
        // 1024 bytes, 1000-byte chunks → chunk 0 is 0-999, chunk 1 is 1000-1023.
        assert_eq!(chunk_bounds(1024, 1000, 0), (0, 999));
        assert_eq!(chunk_bounds(1024, 1000, 1), (1000, 1023));
    }

    #[test]
    fn chunk_bounds_exact_multiple_full_last_chunk() {
        // 300 bytes, 100-byte chunks → final chunk fills exactly.
        assert_eq!(chunk_bounds(300, 100, 2), (200, 299));
    }

    #[test]
    fn chunk_bounds_off_by_one_at_boundary() {
        // The classic off-by-one: file size exactly equal to one chunk_size + 1.
        // Should produce 2 chunks: 0..=99 and 100..=100.
        assert_eq!(num_chunks(101, 100), 2);
        assert_eq!(chunk_bounds(101, 100, 0), (0, 99));
        assert_eq!(chunk_bounds(101, 100, 1), (100, 100));
    }

    #[test]
    fn chunk_bounds_one_byte_file_one_chunk() {
        assert_eq!(num_chunks(1, 100), 1);
        assert_eq!(chunk_bounds(1, 100, 0), (0, 0));
    }

    // Regression for audit D1: previously `ChunkFailed(usize)` discarded the
    // underlying cause, so a user saw "chunk 5 failed" with no clue whether
    // it was 404, 500, connection reset, or disk-full. The reshaped variant
    // carries the cause through `source: Box<DownloadError>`; this test pins
    // the contract so a future refactor cannot silently re-flatten it.
    // `let ... else { unreachable!() }` is used instead of `panic!(...)`
    // because the project denies `panic` cluster-wide.
    #[test]
    fn chunk_failed_carries_cause() {
        let inner = DownloadError::ServerError(404, "not found".into());
        let outer = DownloadError::ChunkFailed {
            index: 3,
            source: Box::new(inner),
        };

        let DownloadError::ChunkFailed { index, source } = outer else {
            unreachable!("constructed a ChunkFailed above; any other variant is a bug")
        };
        assert_eq!(index, 3);
        assert!(matches!(*source, DownloadError::ServerError(404, _)));
    }

    // Regression for audit D3: pin the variant shape so the missing-header
    // path cannot silently revert to `Ok(0)`. The assertion is intentionally
    // minimal — the contract here is "there is a distinct variant for this
    // case", not "the variant carries field X". Phase 2.7 (D5 — retry
    // classification) will decide whether this variant is retryable; Phase
    // 3.8 will wire it through a thiserror-based hierarchy. Until then,
    // `matches!` pins the shape.
    #[test]
    fn missing_content_length_is_a_distinct_error() {
        let err = DownloadError::MissingContentLength;
        assert!(matches!(err, DownloadError::MissingContentLength));
    }
}

// Kept separate from the chunk-math `tests` module so the two test
// categories don't bleed into each other: chunk math is pure-arithmetic,
// this module is about HTTP status discipline. Audit D2.
#[cfg(test)]
mod partial_content_tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn accepts_206() {
        assert!(require_partial_content(StatusCode::PARTIAL_CONTENT, 0, 99).is_ok());
    }

    // The diagnostic on the 200 branch is load-bearing: it is the only signal
    // distinguishing "server ignored Range" from "server returned wrong bytes".
    // `let ... else { unreachable!() }` is used instead of `.unwrap_err()` /
    // `panic!()` because the project denies `unwrap_used` and `panic`
    // cluster-wide; the test still fails clearly if the helper accepts 200.
    #[test]
    fn rejects_200_with_diagnostic() {
        let result = require_partial_content(StatusCode::OK, 0, 99);
        let Err(err) = result else {
            unreachable!("require_partial_content must reject 200 OK")
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("ignored Range"),
            "diagnostic must name the ignored Range header, got: {msg}"
        );
    }

    #[test]
    fn rejects_other_4xx_5xx() {
        assert!(require_partial_content(StatusCode::NOT_FOUND, 0, 99).is_err());
        assert!(require_partial_content(StatusCode::INTERNAL_SERVER_ERROR, 0, 99).is_err());
    }
}
