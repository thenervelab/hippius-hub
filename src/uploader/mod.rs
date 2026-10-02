//! Upload side of the OCI transport: content-defined chunking + hashing, the
//! streaming chunk pipeline, and the blob/pack upload paths.
//!
//! Submodules, by concern:
//! - `cdc`: `FastCDC` constants, `chunk_and_hash`, and the parallel chunk-hash
//!   pipeline (producer / worker pool / collector) with its serial oracle.
//! - `stream`: `ChunkStreamCore`, the pull-based state machine the
//!   `ChunkStream` pyclass in `lib.rs` wraps.
//! - `client`: the process-global upload `reqwest::Client`.
//! - `watchdog`: write-stall / response-wait guards for streamed sends, plus
//!   body framing.
//! - `blob`: OCI upload-session lifecycle - resumable chunked `PATCH`, session
//!   close, and the monolithic streaming `PUT` fallback.
//! - `pack`: multi-range pack upload.
//! - `hash`: whole-file hashing (`hash_file_async`).
//!
//! The re-exports keep every pre-split `crate::uploader::X` path stable.

// Phase 3.8 (audit U4): the local UploadError was folded into the
// crate-wide `CoreError`. The single thiserror-derived enum carries
// `reqwest::Error` and `std::io::Error` via `#[from]`, preserving the
// `?` ergonomics and the `Error::source()` chain through the Python
// boundary in `lib.rs::core_err_to_py`.

mod blob;
mod cdc;
mod client;
mod hash;
mod pack;
mod stream;
mod watchdog;

pub use blob::upload_blob_async;
pub use cdc::{chunk_and_hash, ChunkList};
pub(crate) use client::upload_client;
pub use hash::hash_file_async;
pub use pack::pack_upload_async;
pub(crate) use stream::{ChunkStreamCore, StreamStep};
