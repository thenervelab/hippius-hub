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

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar — useful for large blobs (multi-GB).
pub async fn upload_blob_async(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), CoreError> {
    // Force HTTP/1.1 for the same reason as the downloader: avoids h2 single-TCP
    // multiplexing, lets uploads spread across multiple connections if the caller
    // parallelizes.
    let client = Client::builder()
        .timeout(Duration::from_secs(3600)) // 1h timeout for very large uploads
        .http1_only()
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let file = File::open(path).await?;
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

    let mut req = client
        .put(url)
        .header(header::CONTENT_LENGTH, file_size)
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
