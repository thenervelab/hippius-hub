use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::path::PathBuf;

mod chunked_downloader;
use chunked_downloader::ChunkedDownloader;

#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None, verify_hash=true))]
fn download_file_native(
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
    verify_hash: bool,
) -> PyResult<String> {
    // Instanciation du runtime Tokio synchrone
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
        
    let downloader = ChunkedDownloader::new(url, auth_token, chunk_size)
        .map_err(|e| PyRuntimeError::new_err(format!("Downloader init error: {:?}", e)))?;
        
    let dest = PathBuf::from(dest_path);
    
    // Blocage du thread Python pendant que Rust fait l'I/O asynchrone concurrent
    let sha256_hash = rt.block_on(async {
        downloader.download(&dest, verify_hash).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))?;
    
    Ok(sha256_hash)
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
    Ok(())
}
