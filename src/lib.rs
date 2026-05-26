use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::path::PathBuf;
use std::sync::Once;

mod chunked_downloader;
use chunked_downloader::ChunkedDownloader;
mod diagnostics;

static TRACING_INIT: Once = Once::new();

/// Install a tracing subscriber once, gated on RUST_LOG / HIPPIUS_DEBUG. When
/// neither is set we install nothing, so the `tracing` macros stay runtime
/// no-ops and normal runs are silent. Logs go to stderr to keep stdout (the
/// diagnose report / progress) clean. Called at the start of each entrypoint
/// rather than at import time, because the CLI sets HIPPIUS_DEBUG only just
/// before invoking us — after the module is already imported.
fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let filter = match std::env::var("RUST_LOG") {
            Ok(f) if !f.is_empty() => f,
            _ => match std::env::var("HIPPIUS_DEBUG") {
                Ok(v) if matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes") => {
                    "hippius_core=debug".to_string()
                }
                _ => return,
            },
        };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None, verify_hash=true,
                    max_concurrent=None, connect_timeout_secs=None, read_timeout_secs=None))]
fn download_file_native(
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
    verify_hash: bool,
    max_concurrent: Option<usize>,
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> PyResult<String> {
    init_tracing();

    // Instanciation du runtime Tokio synchrone
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;

    let downloader = ChunkedDownloader::new(
        url, auth_token, chunk_size, max_concurrent, connect_timeout_secs, read_timeout_secs,
    )
        .map_err(|e| PyRuntimeError::new_err(format!("Downloader init error: {:?}", e)))?;

    let dest = PathBuf::from(dest_path);

    // Blocage du thread Python pendant que Rust fait l'I/O asynchrone concurrent
    let sha256_hash = rt.block_on(async {
        downloader.download(&dest, verify_hash).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))?;

    Ok(sha256_hash)
}

#[pyfunction]
#[pyo3(signature = (blob_url, auth_token=None, probe_bytes=33554432, max_concurrent=None, connect_timeout_secs=None))]
fn diagnose_blob_native(
    blob_url: String,
    auth_token: Option<String>,
    probe_bytes: u64,
    max_concurrent: Option<usize>,
    connect_timeout_secs: Option<u64>,
) -> PyResult<String> {
    init_tracing();

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;

    let report = rt.block_on(async {
        diagnostics::probe_blob(&blob_url, auth_token.as_deref(), probe_bytes, max_concurrent, connect_timeout_secs).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Diagnostics failed: {:?}", e)))?;

    serde_json::to_string(&report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to serialize diagnostics: {}", e)))
}

mod uploader;

#[pyfunction]
#[pyo3(signature = (path))]
fn hash_file_native(path: String) -> PyResult<(String, u64)> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
        
    let dest = PathBuf::from(path);
    rt.block_on(async {
        uploader::hash_file_async(&dest).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Hash failed: {:?}", e)))
}

#[pyfunction]
#[pyo3(signature = (url, path, auth_token=None))]
fn upload_blob_native(url: String, path: String, auth_token: Option<String>) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
        
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
    m.add_function(wrap_pyfunction!(diagnose_blob_native, m)?)?;
    Ok(())
}
