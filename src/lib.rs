use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::path::PathBuf;

mod chunked_downloader;
use chunked_downloader::ChunkedDownloader;

#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None))]
fn download_file_native(
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
) -> PyResult<String> {
    // Instanciation du runtime Tokio synchrone
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
        
    let downloader = ChunkedDownloader::new(url, auth_token, chunk_size)
        .map_err(|e| PyRuntimeError::new_err(format!("Downloader init error: {:?}", e)))?;
        
    let dest = PathBuf::from(dest_path);
    
    // Blocage du thread Python pendant que Rust fait l'I/O asynchrone concurrent
    let sha256_hash = rt.block_on(async {
        downloader.download(&dest).await
    }).map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))?;
    
    Ok(sha256_hash)
}

/// A Python module implemented in Rust.
#[pymodule]
fn hippius_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(download_file_native, m)?)?;
    Ok(())
}
