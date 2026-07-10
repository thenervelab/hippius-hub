use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::path::PathBuf;

mod error;
pub use error::{CoreError, Result};

mod chunk_fetcher;
mod chunked_downloader;
mod diagnostics;
mod uploader;

use chunk_fetcher::{PackAssembler, PackChunkTarget, PackPlanEntry};
use chunked_downloader::ChunkedDownloader;

/// Render a `CoreError` (plus its full `source()` chain) as a single
/// `PyRuntimeError`. Each link in the chain is appended after a
/// `caused by:` line so the Python `__str__` view shows wrapper +
/// underlying cause (e.g. `chunk 7 failed\ncaused by: server returned
/// 503 (transient)`). Previously the lib.rs callers used
/// `format!("{:?}", e)` on the bare enum, which printed Debug shape
/// without walking `source()` — losing the inner reqwest/io message
/// the audit D8 / U4 findings called out.
fn core_err_to_py(e: &CoreError) -> PyErr {
    let mut msg = e.to_string();
    let mut current: Option<&dyn StdError> = e.source();
    while let Some(src) = current {
        // `caused by:` mirrors `std::error::Report` and the
        // `errors/source_chain_walk` exemplar; the linebreak between
        // links keeps each layer scannable in a Python traceback.
        // `write!` into the owned `String` avoids the intermediate
        // `format!` allocation on every link in the chain.
        // The `Result` is infallible — writing to a `String` cannot
        // fail — so swallowing it is sound; we deliberately do not
        // surface a synthetic error here.
        let _ignored = write!(msg, "\ncaused by: {src}");
        current = src.source();
    }
    PyRuntimeError::new_err(msg)
}

/// Process-global multi-threaded tokio runtime, managed by `pyo3-async-runtimes`.
///
/// `pyo3_async_runtimes::tokio::get_runtime()` returns the library's canonical
/// singleton — an `OnceCell<Pyo3Runtime>` initialized on first call with a
/// multi-thread builder. By routing through it instead of our own `OnceLock`
/// we gain free interop if a future `#[pyfunction]` ever calls
/// `pyo3_async_runtimes::tokio::future_into_py`: that helper spawns onto the
/// same runtime, so sync `block_on` callers and async `future_into_py`
/// callers would share threads instead of fighting over two unrelated pools.
///
/// The library's `get_runtime` signature is `pub fn get_runtime<'a>() -> &'a
/// Runtime`; the `'a` is free elision over the underlying `OnceCell` static, so
/// coercion to `&'static` is sound — the storage outlives the process.
///
/// The previous manual `OnceLock` build with a custom `"hippius-core"` thread
/// name was replaced here in audit STRUCT-1 (Phase 5.1). The thread name was
/// cosmetic; we accepted the library's default in exchange for the interop.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

/// Download a file from `url` to `dest_path` using the shared tokio runtime.
///
/// # Arguments
/// - `url`: HTTPS URL of the blob to download.
/// - `dest_path`: Local filesystem path where the blob is written.
/// - `auth_token`: Optional bearer token; `None` means anonymous.
/// - `chunk_size`: Optional bytes per HTTP Range request; defaults to the
///   value chosen by `ChunkedDownloader::new` (100 MB at time of writing).
/// - `verify_hash`: When `true`, re-reads the file post-download and returns
///   its SHA-256. When `false`, returns `None` and skips the verification pass.
///
/// # Returns
/// `Optional[str]` on the Python side -- the lowercase hex SHA-256 of the
/// file when `verify_hash=True`, otherwise `None`. The `Option<String>`
/// return type makes the "skipped" case unambiguous (audit L6 / Phase 3.12);
/// the previous `""` sentinel collided with any legitimately empty digest.
///
/// # Errors
/// Raises `PyRuntimeError` on any download failure. The exception message
/// includes the full `source()` chain via `\ncaused by:` lines (see
/// `core_err_to_py` in this file).
///
/// # GIL
/// Releases the Python GIL across the blocking I/O via `py.detach`
/// so other Python threads can make progress during the download.
#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None, verify_hash=true))]
fn download_file_native(
    py: Python<'_>,
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
    verify_hash: bool,
) -> PyResult<Option<String>> {
    // Audit L6 (Phase 3.12): `Option<String>` instead of `String` for the
    // hash result. pyo3 0.20's blanket `IntoPy` impl on `Option<T>` maps
    // `None` to Python's `None` and `Some(s)` to `s`, so the Python
    // signature becomes `Optional[str]` automatically. The previous
    // contract returned `""` as an in-band "skipped" sentinel; callers
    // now match on `is None` instead, which is also free of the
    // theoretical collision with `sha256(b"")` = `e3b0c4...` (a
    // non-empty 64-hex string — distinct from `""` but still a
    // sentinel-shaped trap if a future SHA-0-like algorithm ever
    // produced an empty digest).
    let rt = shared_runtime();
    let dest = PathBuf::from(dest_path);

    // Release the GIL so other Python threads can run during the (long)
    // network/disk I/O. pyo3 acquires the GIL automatically on function
    // entry; detach (the post-0.27 name for allow_threads) explicitly
    // releases it for the closure body. Building the downloader — which now
    // clones the shared download client rather than constructing a fresh one —
    // happens inside the closure so client access never holds the GIL, matching
    // download_packs_native (where PackAssembler::new already runs detached).
    py.detach(|| {
        let downloader =
            ChunkedDownloader::new(url, auth_token, chunk_size).map_err(|e| core_err_to_py(&e))?;
        rt.block_on(async { downloader.download(&dest, verify_hash).await })
            .map_err(|e| core_err_to_py(&e))
    })
}

/// Compute the SHA-256 and byte length of a local file.
///
/// # Arguments
/// - `path`: Local filesystem path of the file to hash.
///
/// # Returns
/// `tuple[str, int]` on the Python side -- a 2-tuple of
/// `(lowercase-hex-sha256, byte_length)`. The byte length is taken from
/// the same read pass that produced the digest, so the two values are
/// guaranteed consistent even if another writer concurrently truncates
/// or extends the file mid-call.
///
/// # Errors
/// Raises `PyRuntimeError` if the file cannot be opened or read. The
/// exception message includes the full `source()` chain via
/// `\ncaused by:` lines (see `core_err_to_py` in this file).
///
/// # GIL
/// Releases the Python GIL across the blocking hash via
/// `py.detach` so other Python threads can run while the file is
/// being read.
#[pyfunction]
#[pyo3(signature = (path))]
fn hash_file_native(py: Python<'_>, path: String) -> PyResult<(String, u64)> {
    let rt = shared_runtime();
    let dest = PathBuf::from(path);

    // Release the GIL across the blocking hash; see `download_file_native`.
    py.detach(|| {
        rt.block_on(async { uploader::hash_file_async(&dest).await })
            .map_err(|e| core_err_to_py(&e))
    })
}

/// Split a file into content-defined chunks and hash each chunk + the whole file.
///
/// The chunked-v2 upload primitive: chunk once here, then the Python layer plans
/// packs (`_packing.plan_packs`) and pushes each new pack via `pack_upload_native`.
///
/// # Arguments
/// - `path`: local file to chunk.
/// - `avg_size`: `FastCDC` target average chunk size in bytes (min/max derived
///   as avg/4 .. avg*4). Pinned by the caller — it is part of the layout's wire
///   contract, so identical files chunk identically and dedup.
///
/// # Returns
/// `tuple[str, list[tuple[str, int, int]]]` — the whole-file sha256 hex, and the
/// per-chunk `(sha256_hex, offset, length)` list in file order.
///
/// # Errors
/// `PyRuntimeError` if the file cannot be read or `avg_size` is out of range.
///
/// # GIL
/// Releases the Python GIL across the blocking chunk+hash pass via `py.detach`.
#[pyfunction]
#[pyo3(signature = (path, avg_size))]
fn chunk_and_hash_native(
    py: Python<'_>,
    path: String,
    avg_size: u64,
) -> PyResult<(String, uploader::ChunkList)> {
    let dest = PathBuf::from(path);
    // Sync CPU+I/O pass; no runtime needed. Detach releases the GIL for it.
    py.detach(|| uploader::chunk_and_hash(&dest, avg_size).map_err(|e| core_err_to_py(&e)))
}

/// Upload a local file to `url` as the body of a chunked HTTP PUT.
///
/// # Arguments
/// - `url`: HTTPS URL receiving the blob (typically an OCI registry
///   upload-location URL returned by a prior `POST .../blobs/uploads/`).
/// - `path`: Local filesystem path of the file to upload.
/// - `auth_token`: Optional bearer token; `None` means anonymous.
///
/// # Returns
/// `None` on the Python side. Success is indicated by the absence of an
/// exception; the digest is not returned here -- callers compute it
/// separately via `hash_file_native` when they need it.
///
/// # Errors
/// Raises `PyRuntimeError` on any upload failure (network error, non-2xx
/// response after retries, local file I/O error). The exception message
/// includes the full `source()` chain via `\ncaused by:` lines (see
/// `core_err_to_py` in this file).
///
/// # GIL
/// Releases the Python GIL across the blocking upload via
/// `py.detach` so other Python threads can make progress while
/// the file is being streamed.
#[pyfunction]
#[pyo3(signature = (url, path, auth_token=None))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "pyo3 #[pyfunction] requires owned values to extract from Python args"
)]
fn upload_blob_native(
    py: Python<'_>,
    url: String,
    path: String,
    auth_token: Option<String>,
) -> PyResult<()> {
    let rt = shared_runtime();
    let dest = PathBuf::from(path);

    // Release the GIL across the blocking upload; see `download_file_native`.
    py.detach(|| {
        rt.block_on(async { uploader::upload_blob_async(&url, &dest, auth_token.as_deref()).await })
            .map_err(|e| core_err_to_py(&e))
    })
}

/// Probe the network path to a single blob URL and return a JSON-encoded
/// `DiagnosticReport` (see `src/diagnostics.rs`). Python decodes it on the
/// other side (see `hippius_hub/diagnose.py: report["blob"] = json.loads(raw)`)
/// so the wire contract is intentionally a string — every new field added to
/// `DiagnosticReport` flows through without changing the pyo3 signature.
#[pyfunction]
#[pyo3(signature = (blob_url, auth_token=None, probe_bytes=33_554_432, max_concurrent=None, connect_timeout_secs=None))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "pyo3 #[pyfunction] requires owned values to extract from Python args"
)]
fn diagnose_blob_native(
    py: Python<'_>,
    blob_url: String,
    auth_token: Option<String>,
    probe_bytes: u64,
    max_concurrent: Option<usize>,
    connect_timeout_secs: Option<u64>,
) -> PyResult<String> {
    let rt = shared_runtime();
    py.detach(|| {
        let report = rt
            .block_on(async {
                diagnostics::probe_blob(
                    &blob_url,
                    auth_token.as_deref(),
                    probe_bytes,
                    max_concurrent,
                    connect_timeout_secs,
                )
                .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("{e:?}")))?;
        serde_json::to_string(&report)
            .map_err(|e| PyRuntimeError::new_err(format!("serialize DiagnosticReport: {e}")))
    })
}

/// Pack a file's new-chunk byte ranges into one blob and upload it (chunked-v2).
///
/// # Arguments
/// - `uploads_url`: the repo's `.../blobs/uploads/` endpoint; this call does the
///   POST-init + monolithic PUT itself (a new pack is never dedup-HEADed — its
///   content is new by construction).
/// - `path`: local source file.
/// - `ranges`: `(offset, length)` byte ranges to concatenate, in pack order.
/// - `auth_token`: optional bearer token.
///
/// # Returns
/// The pack blob's lowercase-hex sha256 (no prefix) — recorded in the v2 pointer.
///
/// # GIL
/// Releases the GIL across the blocking read+upload via `py.detach`.
#[pyfunction]
#[pyo3(signature = (uploads_url, path, ranges, auth_token=None))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "pyo3 #[pyfunction] requires owned values to extract from Python args"
)]
fn pack_upload_native(
    py: Python<'_>,
    uploads_url: String,
    path: String,
    ranges: Vec<(u64, u64)>,
    auth_token: Option<String>,
) -> PyResult<String> {
    let rt = shared_runtime();
    let dest = PathBuf::from(path);
    py.detach(|| {
        rt.block_on(async {
            uploader::pack_upload_async(&uploads_url, &dest, &ranges, auth_token.as_deref()).await
        })
        .map_err(|e| core_err_to_py(&e))
    })
}

/// Assemble a chunked-v2 file by pulling its pack blobs in parallel and slicing
/// each chunk to its file offset (the download companion to `pack_upload_native`).
///
/// # Arguments
/// - `pack_urls` / `pack_sizes`: per-pack blob URL and byte length (equal length).
/// - `pack_chunks`: per pack, a list of `(offset_in_pack, size, file_offset,
///   chunk_sha256_hex)` targets to carve and verify.
/// - `dest_path`: destination, pre-allocated to `total_size`.
/// - `total_size`: whole-file byte length (from the pointer's `file.size`).
/// - `file_digest`: optional whole-file sha256 (hex, no prefix) — always passed
///   for chunked files; proves chunk ordering across packs.
///
/// # Returns
/// The verified whole-file sha256 hex when `file_digest` is given, else `None`.
///
/// # GIL
/// Releases the GIL across the blocking transfer via `py.detach`.
#[pyfunction]
#[pyo3(signature = (pack_urls, pack_sizes, pack_chunks, dest_path, total_size, file_digest=None, auth_token=None, max_concurrent=None))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "pyo3 #[pyfunction] requires owned values to extract from Python args"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is a distinct Python-supplied download argument; bundling into a #[pyclass] would add a wrapper type for no call-site clarity"
)]
fn download_packs_native(
    py: Python<'_>,
    pack_urls: Vec<String>,
    pack_sizes: Vec<u64>,
    pack_chunks: Vec<Vec<(u64, u64, u64, String)>>,
    dest_path: String,
    total_size: u64,
    file_digest: Option<String>,
    auth_token: Option<String>,
    max_concurrent: Option<usize>,
) -> PyResult<Option<String>> {
    if pack_urls.len() != pack_sizes.len() || pack_urls.len() != pack_chunks.len() {
        return Err(PyValueError::new_err(format!(
            "pack_urls ({}), pack_sizes ({}), pack_chunks ({}) must have equal length",
            pack_urls.len(),
            pack_sizes.len(),
            pack_chunks.len()
        )));
    }
    let mut packs: Vec<PackPlanEntry> = Vec::with_capacity(pack_urls.len());
    for ((url, size), chunks) in pack_urls.into_iter().zip(pack_sizes).zip(pack_chunks) {
        let targets = chunks
            .into_iter()
            .map(|(offset_in_pack, csize, file_offset, expected_sha256)| PackChunkTarget {
                offset_in_pack,
                size: csize,
                file_offset,
                expected_sha256,
            })
            .collect();
        packs.push(PackPlanEntry { url, size, chunks: targets });
    }

    let rt = shared_runtime();
    let dest = PathBuf::from(dest_path);
    let concurrency = max_concurrent.unwrap_or(32).max(1);

    py.detach(|| {
        let assembler = PackAssembler::new(auth_token, concurrency).map_err(|e| core_err_to_py(&e))?;
        rt.block_on(async {
            assembler
                .assemble(&dest, &packs, file_digest.as_deref(), total_size)
                .await
        })
        .map_err(|e| core_err_to_py(&e))
    })
}

/// A Python module implemented in Rust.
///
/// The `#[pymodule]` signature takes `&Bound<'_, PyModule>` (the pyo3 0.21+
/// Bound API, current in 0.29) instead of the legacy `(Python, &PyModule)`
/// pair. `Bound<'py, T>` is the GIL-bound smart pointer; the
/// `'py` lifetime ties every Python object the closure produces to the
/// GIL acquisition, so the borrow checker enforces what 0.20's GIL Refs
/// proved manually. `wrap_pyfunction!(f, m)` keeps the same call shape
/// because the macro accepts both `Python<'_>` and `&Bound<PyModule>`
/// (see the `WrapPyFunctionArg` impls in `pyo3::impl_::pyfunction`).
#[pymodule]
fn hippius_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(download_file_native, m)?)?;
    m.add_function(wrap_pyfunction!(hash_file_native, m)?)?;
    m.add_function(wrap_pyfunction!(chunk_and_hash_native, m)?)?;
    m.add_function(wrap_pyfunction!(upload_blob_native, m)?)?;
    m.add_function(wrap_pyfunction!(pack_upload_native, m)?)?;
    m.add_function(wrap_pyfunction!(download_packs_native, m)?)?;
    m.add_function(wrap_pyfunction!(diagnose_blob_native, m)?)?;
    Ok(())
}

#[cfg(test)]
mod runtime_tests {
    #[test]
    fn shared_runtime_returns_same_instance() {
        // The singleton invariant survived the STRUCT-1 (Phase 5.1)
        // migration: the underlying storage moved from our local
        // `OnceLock<Runtime>` to `pyo3_async_runtimes`'s
        // `OnceCell<Pyo3Runtime>`, but pointer equality across two calls is
        // still the direct expression of "one runtime per process" — and a
        // regression test that would fire if the library ever broke that
        // contract or we accidentally swapped in a non-singleton wrapper.
        let a: &'static _ = super::shared_runtime();
        let b: &'static _ = super::shared_runtime();
        assert!(std::ptr::eq(a, b));
    }
}
