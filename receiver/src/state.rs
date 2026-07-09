//! Shared service state and per-upload session.
//!
//! Sessions are held in a `DashMap` keyed by opaque `upload_id`. A session
//! records only what `complete` needs — repo, digest, part count, and which
//! parts have landed — plus its creation time for TTL sweeping. It deliberately
//! does NOT store the client's credential: each request carries its own
//! `Authorization`, and `complete` replays the header from its own request, so
//! no secret sits at rest in the map.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::{DashMap, DashSet};

/// Process-wide configuration, resolved once from the environment at startup.
pub(crate) struct Config {
    pub harbor_base: String,
    pub scratch_dir: PathBuf,
    pub min_part_size: u64,
    pub max_part_size: u64,
    pub session_ttl: Duration,
}

/// One in-flight multipart upload.
pub(crate) struct Session {
    pub repo: String,
    pub digest: String,
    /// Total blob size and the authoritative (clamped) part size — kept so
    /// `put_part` can validate that each arriving part is exactly the length
    /// its position implies (a short/over-long part is rejected, not silently
    /// accepted and left for Harbor's digest to fail unrecoverably).
    pub size: u64,
    pub part_size: u64,
    pub num_parts: u32,
    /// 1-based part numbers whose bytes have fully landed on scratch.
    pub received: DashSet<u32>,
    /// Last time a part landed (or complete was attempted). The TTL sweeper
    /// evicts on *inactivity*, not creation, so a slow multi-hour upload that
    /// is still making progress is never reclaimed out from under itself.
    pub last_activity: Mutex<Instant>,
}

/// Handler state, cheap to `clone` (all shared behind `Arc`).
#[derive(Clone)]
pub(crate) struct AppState {
    pub config: Arc<Config>,
    pub sessions: Arc<DashMap<String, Arc<Session>>>,
    pub http: reqwest::Client,
}
