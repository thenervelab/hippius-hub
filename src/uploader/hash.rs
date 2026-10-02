use sha2::{Digest, Sha256};
use std::path::Path;

use crate::error::CoreError;
use crate::transport::IO_READ_BUFFER;

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
        let mut buffer = vec![0u8; IO_READ_BUFFER];
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
