use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::path::PathBuf;
use std::sync::OnceLock;

mod chunked_downloader;
use chunked_downloader::ChunkedDownloader;

/// Process-global multi-threaded tokio runtime.
///
/// Why a single shared runtime: each `tokio::runtime::Runtime::new()` spawns
/// worker threads and registers epoll/kqueue handles, then tears them down on
/// drop. For workloads like `snapshot_download` (which calls `hf_hub_download`
/// once per file), that per-call cost dominates the actual I/O. One runtime
/// reused for the lifetime of the Python process amortises it to zero.
///
/// Why `OnceLock` over `LazyLock`: identical thread-safety guarantees here, but
/// `OnceLock` lets us keep `shared_runtime` as a plain function — easier to
/// move to a fallible builder later (e.g. configurable worker count) without
/// changing call sites.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("hippius-core")
            .build()
            .expect("failed to build the shared tokio runtime — fatal at module init")
    })
}

#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None, verify_hash=true))]
fn download_file_native(
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
    verify_hash: bool,
) -> PyResult<String> {
    let rt = shared_runtime();

    let downloader = ChunkedDownloader::new(url, auth_token, chunk_size)
        .map_err(|e| PyRuntimeError::new_err(format!("Downloader init error: {:?}", e)))?;

    let dest = PathBuf::from(dest_path);

    // Block the Python thread while Rust runs concurrent async I/O.
    let sha256_hash = rt.block_on(async {
        downloader.download(&dest, verify_hash).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))?;

    Ok(sha256_hash)
}

mod uploader;

#[pyfunction]
#[pyo3(signature = (path))]
fn hash_file_native(path: String) -> PyResult<(String, u64)> {
    let rt = shared_runtime();

    let dest = PathBuf::from(path);
    rt.block_on(async {
        uploader::hash_file_async(&dest).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Hash failed: {:?}", e)))
}

#[pyfunction]
#[pyo3(signature = (url, path, auth_token=None))]
fn upload_blob_native(url: String, path: String, auth_token: Option<String>) -> PyResult<()> {
    let rt = shared_runtime();

    let dest = PathBuf::from(path);
    rt.block_on(async {
        uploader::upload_blob_async(&url, &dest, auth_token.as_deref()).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Upload failed: {:?}", e)))
}

/// A Python module implemented in Rust.
#[pymodule]
fn hippius_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(download_file_native, m)?)?;
    m.add_function(wrap_pyfunction!(hash_file_native, m)?)?;
    m.add_function(wrap_pyfunction!(upload_blob_native, m)?)?;
    Ok(())
}

#[cfg(test)]
mod runtime_tests {
    use std::time::Instant;

    // The audit fix replaces N runtime constructions with one shared runtime.
    // This test documents (rather than enforces) the per-call cost so future
    // readers see the motivation. The fix's gain is the absence of the loop
    // body in production code paths, not a tighter bound here.
    #[test]
    fn shared_runtime_avoids_startup_cost() {
        let start = Instant::now();
        for _ in 0..10 {
            let _rt = tokio::runtime::Runtime::new()
                .expect("test runtime construction must succeed");
        }
        let elapsed = start.elapsed();
        eprintln!("10x Runtime::new(): {:?}", elapsed);
    }
}
