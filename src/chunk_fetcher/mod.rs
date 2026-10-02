//! Parallel pull + assemble of a chunked artifact's content-defined chunk blobs.
//!
//! The chunked-artifact layout (docs/plans/2026-07-09-chunked-artifact-layout.md)
//! stores a large file as K independent, content-addressed OCI blobs. Unlike
//! `chunked_downloader.rs` - which parallelises ONE whole-file blob via HTTP
//! `Range` requests (206 slices) and is kept for pre-chunking artifacts - this
//! module fetches each chunk as its own ordinary blob (a full `200 OK`) and
//! writes it to its offset in the pre-allocated destination. Two consequences:
//! no `Range`/206 dependency (the ATS-edge 206 fragility the plan retires), and
//! each chunk verifies against its own digest as it streams, so the assembled
//! file's integrity is proven chunk-by-chunk rather than trusted.
//!
//! Split (Task D2) into `client` (the shared download transport: process-global
//! client, timeouts, read-idle stall guard, global pack gate), `assemble` (the
//! pack plan, fan-out, and retry loop), and `scatter` (per-chunk verify+write
//! and the whole-file digest resolution). The re-exports below keep every
//! pre-split `crate::chunk_fetcher::X` path stable.

mod assemble;
mod client;
mod scatter;

pub use assemble::{PackAssembler, PackChunkTarget, PackPlanEntry};
pub(crate) use client::TransportTimeouts;
pub(crate) use client::{download_client, download_read_idle, read_chunk_bounded};
// The only cross-module caller of `compute_sha256` via this path is the
// `incremental_hash` test module; production callers live inside `scatter`.
#[cfg(test)]
pub(crate) use scatter::compute_sha256;

/// Test fixtures shared by the `assemble` and `scatter` test modules (both build
/// `PackChunkTarget`s). `cfg(test)` so they exist only for the test build.
#[cfg(test)]
pub(crate) mod test_support {
    use crate::chunk_fetcher::PackChunkTarget;
    use crate::digest::Sha256Digest;

    pub(crate) fn chunk_target(
        offset_in_pack: u64,
        size: u64,
        file_offset: u64,
        sha: Sha256Digest,
    ) -> PackChunkTarget {
        PackChunkTarget {
            offset_in_pack,
            size,
            file_offset,
            expected_sha256: sha,
        }
    }

    /// Placeholder digest for plan-validation tests, which never reach the
    /// verify step - only offsets and sizes matter there.
    pub(crate) fn any_digest() -> Sha256Digest {
        Sha256Digest::from([0u8; 32])
    }
}
