//! HTTP handlers implementing the opaque-upload-id multipart contract:
//! initiate -> `put_part` (xN, concurrent) -> complete, plus abort.

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::error::ReceiverError;
use crate::harbor::push_blob;
use crate::plan::{clamp_part_size, missing_parts, num_parts, part_len};
use crate::state::{AppState, Session};

#[derive(Deserialize)]
pub(crate) struct InitiateRequest {
    repo: String,
    digest: String,
    size: u64,
    part_size: u64,
}

#[derive(Serialize)]
pub(crate) struct InitiateResponse {
    upload_id: String,
    part_size: u64,
}

/// Upper bound on parts per upload. Caps the `Vec<u32>` that `missing_parts`
/// allocates and the session's bookkeeping: without it, a single unauthenticated
/// `initiate` with a huge `size` drives `num_parts` toward `u32::MAX`, and the
/// first `complete` allocates a multi-GB vector -> an out-of-memory kill. 100k parts cover a
/// ~6 TB blob at the 64 MB default part size with vast margin.
const MAX_PARTS: u32 = 100_000;

/// Allocate an upload session and its scratch directory, returning the opaque
/// `upload_id` and the (possibly clamped) authoritative part size.
pub(crate) async fn initiate(
    State(state): State<AppState>,
    Json(req): Json<InitiateRequest>,
) -> Result<Response, ReceiverError> {
    if req.size == 0 {
        return Err(ReceiverError::BadRequest("size must be > 0".to_string()));
    }
    if !req.digest.starts_with("sha256:") {
        return Err(ReceiverError::BadRequest("digest must be sha256:<hex>".to_string()));
    }
    let part_size = clamp_part_size(req.part_size, state.config.min_part_size, state.config.max_part_size);
    let parts = num_parts(req.size, part_size);
    if parts == 0 || parts > MAX_PARTS {
        return Err(ReceiverError::BadRequest(format!(
            "size/part_size yields {parts} parts; must be 1..={MAX_PARTS}"
        )));
    }

    let upload_id = uuid::Uuid::new_v4().to_string();
    tokio::fs::create_dir_all(scratch_for(&state, &upload_id)).await?;

    let session = Session {
        repo: req.repo,
        digest: req.digest,
        size: req.size,
        part_size,
        num_parts: parts,
        received: dashmap::DashSet::new(),
        last_activity: std::sync::Mutex::new(std::time::Instant::now()),
    };
    state.sessions.insert(upload_id.clone(), Arc::new(session));
    Ok((StatusCode::CREATED, Json(InitiateResponse { upload_id, part_size })).into_response())
}

/// Stream one part's bytes to scratch. Idempotent: the body is written to a
/// temp sibling and atomically renamed into place, so a re-PUT of a part that
/// already landed (client retry, or a receiver restart mid-upload) overwrites
/// cleanly and never leaves a torn part.
pub(crate) async fn put_part(
    State(state): State<AppState>,
    Path((upload_id, part_number)): Path<(String, u32)>,
    body: Body,
) -> Result<Response, ReceiverError> {
    let session = lookup(&state, &upload_id)?;
    if part_number == 0 || part_number > session.num_parts {
        return Err(ReceiverError::InvalidPart(part_number));
    }
    let expected = part_len(session.size, session.part_size, part_number - 1);

    let final_path = scratch_for(&state, &upload_id).join(part_number.to_string());
    // Unique tmp name per request: two concurrent PUTs of the same part number
    // must not interleave writes into one tmp file and rename a torn result.
    let tmp_path = final_path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));

    if let Err(e) = stream_part_to(&tmp_path, body, expected).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(e);
    }
    tokio::fs::rename(&tmp_path, &final_path).await?;
    session.received.insert(part_number);
    touch(&session);
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Stream a part body to `tmp_path`, enforcing that EXACTLY `expected` bytes
/// arrive. The write is capped (a client cannot exhaust scratch by sending more
/// than the part's length), and a short body is rejected so a truncated part is
/// never marked "received" — which would be unrecoverable, since `complete`
/// would not list it missing and only Harbor's digest (a terminal 502 to the
/// client) would catch the corruption.
async fn stream_part_to(tmp_path: &std::path::Path, body: Body, expected: u64) -> Result<(), ReceiverError> {
    let mut file = tokio::fs::File::create(tmp_path).await?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ReceiverError::BadRequest(format!("part body read error: {e}")))?;
        written += chunk.len() as u64;
        if written > expected {
            return Err(ReceiverError::BadRequest(format!("part body exceeds its expected {expected} bytes")));
        }
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if written != expected {
        return Err(ReceiverError::BadRequest(format!("part body is {written} bytes, expected {expected}")));
    }
    Ok(())
}

/// Finalize: if any part is missing, return 409 with the list so the client
/// re-PUTs exactly those; otherwise stream the reassembled blob to Harbor and
/// return its blob Location, then drop the session and its scratch.
pub(crate) async fn complete(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ReceiverError> {
    let session = lookup(&state, &upload_id)?;
    // Reset the activity clock before the (potentially long) Harbor reassembly
    // push, so the inactivity sweeper cannot delete scratch mid-`complete`.
    touch(&session);

    let received: HashSet<u32> = session.received.iter().map(|r| *r).collect();
    let missing = missing_parts(session.num_parts, &received);
    if !missing.is_empty() {
        return Ok((StatusCode::CONFLICT, Json(serde_json::json!({ "missing": missing }))).into_response());
    }

    let part_paths: Vec<_> = (1..=session.num_parts)
        .map(|n| scratch_for(&state, &upload_id).join(n.to_string()))
        .collect();
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let location = push_blob(&state.http, &state.config.harbor_base, &session.repo, &session.digest, auth, part_paths).await?;

    forget(&state, &upload_id).await;
    Ok((StatusCode::CREATED, [(header::LOCATION, location)]).into_response())
}

/// Abort an in-flight upload and reclaim its scratch.
pub(crate) async fn abort(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
) -> Result<Response, ReceiverError> {
    // Guard BEFORE touching the filesystem: unlike put_part/complete, abort does
    // not go through `lookup`, so without this a percent-decoded traversal id
    // would reach `remove_dir_all(scratch_dir.join(upload_id))` and delete an
    // arbitrary directory (axum decodes `..%2f..` -> `../..` into this string).
    if !is_valid_upload_id(&upload_id) {
        return Err(ReceiverError::UnknownUpload);
    }
    forget(&state, &upload_id).await;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// A well-formed `upload_id` is a UUID we minted at `initiate`. Requiring that
/// shape is the path-traversal guard: a UUID has no `/` or `..`, so
/// `scratch_dir.join(upload_id)` can never escape the scratch root even though
/// axum percent-decodes `..%2f..%2fetc` into `../../etc` in the path segment.
fn is_valid_upload_id(upload_id: &str) -> bool {
    uuid::Uuid::parse_str(upload_id).is_ok()
}

fn scratch_for(state: &AppState, upload_id: &str) -> std::path::PathBuf {
    state.config.scratch_dir.join(upload_id)
}

/// Refresh a session's activity clock so the inactivity sweeper does not reclaim
/// an upload that is still progressing. A poisoned lock is skipped (best effort
/// — the worst case is one premature sweep of an already-broken session).
fn touch(session: &Session) {
    if let Ok(mut last) = session.last_activity.lock() {
        *last = std::time::Instant::now();
    }
}

fn lookup(state: &AppState, upload_id: &str) -> Result<Arc<Session>, ReceiverError> {
    // Reject malformed ids up front (defense in depth): a non-UUID is never a
    // live session, and this keeps a traversal id from reaching any path build.
    if !is_valid_upload_id(upload_id) {
        return Err(ReceiverError::UnknownUpload);
    }
    state
        .sessions
        .get(upload_id)
        .map(|s| Arc::clone(&s))
        .ok_or(ReceiverError::UnknownUpload)
}

/// Drop the session and best-effort remove its scratch directory. A failed
/// scratch removal is logged, not surfaced — the session is already gone, so
/// the only consequence is disk the TTL sweeper will reclaim.
async fn forget(state: &AppState, upload_id: &str) {
    state.sessions.remove(upload_id);
    if let Err(e) = tokio::fs::remove_dir_all(scratch_for(state, upload_id)).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(upload_id, error = %e, "failed to remove scratch dir");
    }
}
