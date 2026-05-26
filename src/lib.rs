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
    py: Python<'_>,
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

    // Release the GIL so other Python threads can run during the (long)
    // network/disk I/O. pyo3 acquires the GIL automatically on function
    // entry; allow_threads explicitly releases it for the closure body.
    py.allow_threads(|| {
        rt.block_on(async { downloader.download(&dest, verify_hash).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))
    })
}

mod uploader;

#[pyfunction]
#[pyo3(signature = (path))]
fn hash_file_native(py: Python<'_>, path: String) -> PyResult<(String, u64)> {
    let rt = shared_runtime();
    let dest = PathBuf::from(path);

    // Release the GIL across the blocking hash; see `download_file_native`.
    py.allow_threads(|| {
        rt.block_on(async { uploader::hash_file_async(&dest).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Hash failed: {:?}", e)))
    })
}

#[pyfunction]
#[pyo3(signature = (url, path, auth_token=None))]
fn upload_blob_native(
    py: Python<'_>,
    url: String,
    path: String,
    auth_token: Option<String>,
) -> PyResult<()> {
    let rt = shared_runtime();
    let dest = PathBuf::from(path);

    // Release the GIL across the blocking upload; see `download_file_native`.
    py.allow_threads(|| {
        rt.block_on(async { uploader::upload_blob_async(&url, &dest, auth_token.as_deref()).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Upload failed: {:?}", e)))
    })
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
    #[test]
    fn shared_runtime_returns_same_instance() {
        // The whole point of Task 1.4 is that OnceLock caches a single Runtime
        // for the process lifetime. Pointer equality is the direct expression
        // of that invariant — independent of timing, allocator, or load.
        let a: &'static _ = super::shared_runtime();
        let b: &'static _ = super::shared_runtime();
        assert!(std::ptr::eq(a, b));
    }
}
