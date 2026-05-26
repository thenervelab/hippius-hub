use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::error::CoreError;

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
        let mut buffer = vec![0u8; 64 * 1024]; // 64 KB chunks
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

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar — useful for large blobs (multi-GB).
///
/// Audit U3 (Phase 3.11): wraps [`try_upload_blob_once`] in an
/// exponential-backoff retry loop with the same shape as
/// [`crate::chunked_downloader::download_chunk_with_retry`]. Each
/// attempt re-opens the file inside `try_upload_blob_once` (the
/// previous `FramedRead` stream has been consumed), so the retry sees a
/// fresh handle. Backoff schedule: 200, 400, 800, 1600 ms — four
/// attempts total, ~3 s of backoff before surfacing a transient 5xx as
/// terminal. A 4xx never burns backoff.
pub async fn upload_blob_async(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), CoreError> {
    let mut retries: u32 = 0;
    loop {
        match try_upload_blob_once(url, path, auth_token).await {
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
                // `2u64.pow(retries) * 100` reproduces the downloader's
                // 200/400/800/1600 ms schedule. `retries` is `u32` to
                // match `UPLOAD_MAX_RETRIES`; `pow` widens to `u64` so
                // the multiplication cannot overflow at this budget.
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
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
async fn try_upload_blob_once(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), CoreError> {
    // Force HTTP/1.1 for the same reason as the downloader: avoids h2 single-TCP
    // multiplexing, lets uploads spread across multiple connections if the caller
    // parallelizes.
    let client = Client::builder()
        .timeout(Duration::from_secs(3600)) // 1h timeout for very large uploads
        .http1_only()
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let file = File::open(path).await?;
    // Snapshot file size for the progress bar UI only. We deliberately
    // do NOT send this as Content-Length because the file may change
    // between this stat() and the actual stream consumption — reqwest
    // uses Transfer-Encoding: chunked when Content-Length is omitted,
    // which sidesteps that TOCTOU race entirely. If the file changes
    // mid-upload the progress bar may briefly read >100% or <100%;
    // that UI quirk is preferable to an HTTP-level length mismatch.
    let file_size = file.metadata().await?.len();

    // Progress bar — the stream wrapper updates it on every chunk emitted to reqwest.
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
    let basename = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "blob".to_string());
    pb.set_message(format!("📤 {}", basename));

    // Wrap the stream so we tick the progress bar on every body chunk emitted
    // to reqwest. ProgressBar is Arc-internally → cloning is cheap.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(file, BytesCodec::new()).map(move |chunk_result| {
        if let Ok(ref bytes) = chunk_result {
            pb_stream.inc(bytes.len() as u64);
        }
        chunk_result
    });
    let body = reqwest::Body::wrap_stream(stream);

    // No explicit Content-Length: reqwest falls back to
    // Transfer-Encoding: chunked for a `Body::wrap_stream` body, so the wire
    // length matches whatever `FramedRead` actually delivers at stream time —
    // not whatever `metadata().len()` reported a few syscalls earlier.
    let mut req = client
        .put(url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);

    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }

    let res = req.send().await?;

    if !res.status().is_success() {
        pb.finish_with_message(format!("❌ {} failed", basename));
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }

    pb.finish_with_message(format!("✅ {} uploaded", basename));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CoreError;

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
}
