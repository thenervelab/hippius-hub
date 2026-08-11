use bytes::Bytes;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::error::CoreError;
use crate::uploader::pack::read_ranges;
use crate::uploader::upload_client;
use crate::uploader::watchdog::{
    frame_bytes, send_put_watchdogged, send_streaming_watchdogged, PUT_FRAME_BYTES,
    RESPONSE_WAIT_TIMEOUT, WRITE_STALL_TIMEOUT,
};

/// Mirror of [`crate::chunked_downloader::MAX_RETRIES`] for the upload
/// path. Audit U3 (Phase 3.11): the downloader retried per-chunk up to
/// 3 times; the uploader did not retry at all, so a single transient
/// 503 lost the whole upload. The two paths now share the same budget
/// and the same [`CoreError::is_retryable`] classifier - see
/// `try_upload_blob_once` for the per-attempt body.
pub(super) const UPLOAD_MAX_RETRIES: u32 = 3;

/// Total budget for the zero-body pack upload-init POST (audit H1). Init only
/// allocates an upload session and returns a `Location`; it has no legitimate
/// slow path, so a tight total timeout turns a hung/black-holed registry into a
/// retryable error instead of blocking `try_pack_upload_once` forever - which
/// (via the shared `_pack_upload_gate`) would otherwise wedge the whole folder
/// upload. Unlike the streamed PUT body, a `.timeout()` here can't clip an honest
/// transfer because there is no body to stream.
const INIT_POST_TIMEOUT: Duration = Duration::from_secs(30);

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar - useful for large blobs (multi-GB).
///
/// Audit U3 (Phase 3.11): wraps [`try_upload_blob_once`] in an
/// exponential-backoff retry loop with the same shape as
/// [`crate::chunked_downloader::download_chunk_with_retry`]. Each attempt re-inits
/// a fresh OCI upload session AND re-opens the file inside `try_upload_blob_once`
/// (the previous session is consumed and the previous `FramedRead` stream is spent),
/// so a retry never re-PUTs a dead session (audit L2). Backoff schedule: 200, 400,
/// 800, 1600 ms - four attempts total, ~3 s of backoff before surfacing a transient
/// 5xx as terminal. A 4xx never burns backoff.
pub async fn upload_blob_async(
    uploads_url: &str,
    path: &Path,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let mut retries: u32 = 0;
    loop {
        match try_upload_blob_once(uploads_url, path, digest, auth_token).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                // Same shape as `download_chunk_with_retry`: classify on
                // the error itself (borrow only, so `e` remains
                // returnable), give up on permanent errors immediately,
                // give up on transient errors after the budget is spent.
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                // Full-jitter backoff (audit L-JITTER): a jittered draw within the
                // same 200/400/800/1600 ms cap schedule the deterministic sleep
                // used, so concurrent uploads that hit a registry 429/503 together
                // do not re-collide in lockstep. Shared helper across all four
                // transport retry loops.
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

/// Stream `reader` to `url` as a chunked-encoded PUT body, ticking a progress
/// bar sized `pb_total`. Shared by the whole-file and byte-range upload paths.
///
/// No explicit Content-Length: reqwest falls back to Transfer-Encoding: chunked
/// for a `wrap_stream` body, so the wire length matches whatever the reader
/// actually yields - the TOCTOU-safe behaviour audit U2 established for the
/// whole-file path. For a range upload the reader is a `Take` bounded to the
/// chunk length, so the body is exactly that range regardless. `pb_total` sizes
/// the progress bar only; it is deliberately NOT used as a "fully sent" signal
/// (see [`DoneOnEof`]).
async fn put_streaming<R>(
    url: &str,
    reader: R,
    pb_total: u64,
    basename: &str,
    auth_token: Option<&str>,
    write_stall: Duration,
) -> Result<(), CoreError>
where
    R: tokio::io::AsyncRead + Send + 'static,
{
    let pb = ProgressBar::new(pb_total);
    // The template string is a compile-time literal; `indicatif` only errors on
    // malformed format directives, which we control at the call site.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Uploading {basename}"));

    // Tick the progress bar on every body frame; the watchdog's write-progress
    // stamping lives in `send_put_watchdogged`. `freeze()` hands reqwest an
    // immutable `Bytes` (a move of the `BytesMut` buffer, not a copy). ProgressBar
    // is Arc-internally -> cloning is cheap.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(reader, BytesCodec::new()).map(move |frame| {
        frame.map(|bytes| {
            pb_stream.inc(bytes.len() as u64);
            bytes.freeze()
        })
    });

    let res =
        match send_put_watchdogged(url, stream, auth_token, write_stall, RESPONSE_WAIT_TIMEOUT)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                let msg = match &e {
                    CoreError::Stall(_) => format!("{basename}: stalled"),
                    _ => format!("{basename}: failed"),
                };
                pb.finish_with_message(msg);
                return Err(e);
            }
        };
    if !res.status().is_success() {
        pb.finish_with_message(format!("{basename}: failed"));
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }
    pb.finish_with_message(format!("{basename}: uploaded"));
    Ok(())
}

fn basename_of(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| "blob".to_string(), |n| n.to_string_lossy().into_owned())
}

/// Bytes per `PATCH` chunk (env `HIPPIUS_UPLOAD_CHUNK_SIZE`, default 16 MiB). A
/// mid-upload disconnect re-sends at most one chunk; the GET-offset resume skips
/// everything the registry already committed. Tests set it small to force many
/// chunks on a tiny payload.
fn upload_chunk_size() -> u64 {
    std::env::var("HIPPIUS_UPLOAD_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(16 * 1024 * 1024)
}

/// Result of the chunked-`PATCH` phase of one session.
enum PatchOutcome {
    /// Every byte was `PATCH`ed; carries the current session URL to finalise.
    Done(String),
    /// The registry rejected `PATCH` (405/501) on the first chunk - the caller
    /// falls back to the monolithic streaming `PUT`.
    Unsupported,
}

/// Parse an OCI upload `Range: 0-<end>` (inclusive) into the count of committed
/// bytes. Harbor returns `0-0` for an EMPTY session (right after POST, nothing
/// committed) and `0-<end>` once bytes land, so `0-0` is treated as 0, not 1 -
/// mis-reading it as 1 on a first-chunk-failure resume would skip byte 0. An
/// absent/garbled header also means nothing committed. Under-counting only
/// re-sends already-present bytes (the server 416s or overwrites); over-counting
/// would silently drop data, so we bias to under-count.
fn committed_bytes(range: Option<&str>) -> u64 {
    match range
        .and_then(|r| r.rsplit('-').next())
        .and_then(|n| n.trim().parse::<u64>().ok())
    {
        Some(0) | None => 0,
        // saturating_add: a hostile `0-18446744073709551615` would otherwise
        // overflow (panic under overflow-checks / wrap to 0 in release). Clamping
        // to u64::MAX only over-reports on a value no real registry sends; the
        // caller's `server_off > offset` guard then treats it as "already past".
        Some(end) => end.saturating_add(1),
    }
}

/// Coerce a resume give-up cause into a RETRYABLE error so `upload_blob_async`
/// opens a FRESH session (offset 0) - the actual recovery for a session that is
/// gone, or a `416` offset-desync the intra-session GET could not resolve. `416`
/// alone is not [`CoreError::is_retryable`], so returning it raw would make the
/// outer loop give up instead of restarting; wrap any non-retryable give-up
/// cause in [`CoreError::SessionRestart`], which is retryable by design and
/// keeps the real cause reachable via `source()`.
fn force_retryable(e: CoreError) -> CoreError {
    if e.is_retryable() {
        e
    } else {
        CoreError::SessionRestart {
            source: Box::new(e),
        }
    }
}

/// Append `?digest=<digest>` to a session URL (raw, unencoded `:` - the registry
/// matches the literal digest; percent-encoding it breaks the match).
fn append_digest(session: &str, digest: &str) -> String {
    let mut url = session.to_owned();
    url.push(if url.contains('?') { '&' } else { '?' });
    url.push_str("digest=");
    url.push_str(digest);
    url
}

/// Resolve a possibly-relative `Location` header against the current session URL
/// (each PATCH/POST response may hand back a new session URL carrying state).
fn resolve_location(current: &str, location: &str) -> Result<String, CoreError> {
    reqwest::Url::parse(current)
        .and_then(|base| base.join(location))
        .map(|u| u.to_string())
        .map_err(|e| CoreError::Integrity(format!("bad upload Location {location:?}: {e}")))
}

/// `POST /blobs/uploads/` to open a fresh session; return the resolved absolute
/// session URL (no `?digest=`) and the registry's optional `OCI-Chunk-Min-Length`
/// (the minimum bytes it will accept per non-final `PATCH`). Split from
/// [`init_upload_session`] so the resumable `PATCH` path can address the raw
/// session while the monolithic path still appends the digest.
async fn post_upload_session(
    uploads_url: &str,
    auth_token: Option<&str>,
) -> Result<(String, Option<u64>), CoreError> {
    let client = upload_client()?;
    let mut init = client
        .post(uploads_url)
        .header(header::CONTENT_LENGTH, "0")
        // Bound the zero-body init POST (audit H1): a hung registry here would
        // otherwise block the upload forever and drain the shared gate.
        .timeout(INIT_POST_TIMEOUT);
    if let Some(token) = auth_token {
        init = init.bearer_auth(token);
    }
    let resp = init.send().await?;
    if !resp.status().is_success() {
        return Err(CoreError::ServerError(
            resp.status().as_u16(),
            "blob upload init failed".to_string(),
        ));
    }
    // OCI: a registry MAY advertise a per-chunk minimum; the client SHOULD honor
    // it (final chunk exempt). Harbor does not send it today, so this is normally
    // None and the default 16 MiB chunk applies unchanged.
    let min_chunk = resp
        .headers()
        .get("OCI-Chunk-Min-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0);
    let location = resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        // A missing Location is an out-of-contract response, not a wrong-bytes
        // integrity failure (an LB mid-rollout can emit it); classify it retryable
        // (BadResponse) so a transient registry hiccup restarts a fresh session
        // instead of failing the upload outright.
        .ok_or_else(|| {
            CoreError::BadResponse("registry omitted Location on upload init".to_string())
        })?;
    Ok((resolve_location(uploads_url, location)?, min_chunk))
}

/// `GET <session>` (OCI "get blob upload status") -> count of bytes the registry
/// has committed, i.e. the resume offset. `Ok(None)` means the session is gone
/// (404) and must be restarted with a fresh `POST`; a transient failure is
/// surfaced as `Err` so the caller backs off.
async fn session_offset(session: &str, auth_token: Option<&str>) -> Result<Option<u64>, CoreError> {
    let client = upload_client()?;
    let mut req = client.get(session).timeout(INIT_POST_TIMEOUT);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        return Ok(None); // session expired / gone
    }
    if !status.is_success() {
        return Err(CoreError::ServerError(
            status.as_u16(),
            "upload status GET failed".to_string(),
        ));
    }
    let range = resp
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());
    Ok(Some(committed_bytes(range)))
}

/// Upload `path` to `session` in [`upload_chunk_size`]-byte chunks via OCI
/// `PATCH`, resuming from the registry-committed offset after any transient
/// failure instead of re-sending the whole blob. Returns the (possibly updated)
/// session URL to close, or [`PatchOutcome::Unsupported`] if `PATCH` is rejected
/// on the first chunk.
async fn chunked_patch_upload(
    session: &str,
    path: &Path,
    size: u64,
    min_chunk: Option<u64>,
    auth_token: Option<&str>,
    pb: &ProgressBar,
) -> Result<PatchOutcome, CoreError> {
    let client = upload_client()?;
    // Honor OCI-Chunk-Min-Length if the registry advertised one: a non-final PATCH
    // below the minimum is rejected, so clamp the configured chunk size UP to it
    // (the final short chunk is spec-exempt). Normally None -> the env/default wins.
    // Cap at `size` so a hostile/broken registry advertising a huge min-length (only
    // filtered `> 0`) - or a huge HIPPIUS_UPLOAD_CHUNK_SIZE - can't make `offset +
    // chunk_size` overflow on a resume; a whole-file chunk is the sensible ceiling.
    let chunk_size = upload_chunk_size()
        .max(min_chunk.unwrap_or(0))
        .min(size.max(1));
    let mut location = session.to_owned();
    let mut offset: u64 = 0;
    // Consecutive resume attempts with no server-side progress before giving up
    // (the outer loop then restarts with a fresh session).
    let mut stall: u32 = 0;

    while offset < size {
        let end = offset.saturating_add(chunk_size).min(size);
        let len = end - offset;
        let body = Bytes::from(read_ranges(path, &[(offset, len)]).await?);
        let end_inclusive = end - 1;

        // Stream the chunk through the SHARED write-stall watchdog (audit H1). The
        // PATCH is now the default data-carrying op; a bare `send().await` would
        // wedge forever against a peer that stops draining mid-body, because
        // `upload_client` has no flat request timeout - the exact H1 hang this work
        // exists to kill. Framing lets the watchdog re-stamp as the socket accepts
        // each frame (see `PUT_FRAME_BYTES`); Harbor accepts the resulting
        // chunked-TE PATCH.
        let frames = frame_bytes(&body, PUT_FRAME_BYTES);
        let body_stream =
            futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
        let mut req = client
            .patch(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_RANGE, format!("{offset}-{end_inclusive}"));
        if let Some(token) = auth_token {
            req = req.bearer_auth(token);
        }

        // On 202 advance; classify anything else. A transient failure (transport
        // error, a write stall, or 5xx/408/429) drops to the GET-offset resume
        // below; a permanent 4xx fails fast; 405/501 on the first chunk means
        // PATCH is unsupported.
        let transient: CoreError = match send_streaming_watchdogged(
            req,
            body_stream,
            WRITE_STALL_TIMEOUT,
            RESPONSE_WAIT_TIMEOUT,
        )
        .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 202 {
                    if let Some(loc) = resp
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|v| v.to_str().ok())
                    {
                        location = resolve_location(&location, loc)?;
                    }
                    offset = end;
                    pb.set_position(offset);
                    stall = 0;
                    continue;
                }
                let code = status.as_u16();
                if offset == 0 && matches!(code, 405 | 501) {
                    return Ok(PatchOutcome::Unsupported);
                }
                let err = CoreError::ServerError(code, format!("PATCH failed: {status}"));
                // 416 Range Not Satisfiable = out-of-order chunk: the OCI spec
                // says GET the upload status and resync, so route it to the
                // resume path below rather than failing (it is otherwise a 4xx).
                // Any other non-retryable status is a genuine permanent error.
                if code != 416 && !err.is_retryable() {
                    return Err(err); // permanent (e.g. 400/403)
                }
                err
            }
            Err(e) => e, // transport error or write Stall - both retryable
        };

        // Resume from whatever the registry actually committed.
        match session_offset(&location, auth_token).await {
            Ok(Some(server_off)) if server_off > offset => {
                offset = server_off;
                pb.set_position(offset);
                stall = 0;
            }
            // Session gone -> give up on it as a RETRYABLE error so
            // `upload_blob_async` restarts with a fresh session from offset 0
            // (`force_retryable` covers the 416 case, which is not itself
            // retryable but IS recoverable by a fresh session).
            Ok(None) => return Err(force_retryable(transient)),
            // No progress (or a failing status GET): back off and re-send the
            // same chunk, up to the stall budget, then give up (retryable -> the
            // outer loop restarts a fresh session).
            Ok(Some(_)) | Err(_) => {
                stall += 1;
                if stall > UPLOAD_MAX_RETRIES {
                    return Err(force_retryable(transient));
                }
                tokio::time::sleep(crate::retry::backoff_delay(stall)).await;
            }
        }
    }
    Ok(PatchOutcome::Done(location))
}

/// Finalise a chunked upload: `PUT <session>?digest=` with an empty body (all
/// bytes already arrived via `PATCH`). Retries a transient close in place a few
/// times - the bytes are up, so re-closing is cheap and avoids a full
/// fresh-session restart just because the closing PUT blipped.
///
/// No flat `.timeout()` here (unlike the init POST / status GET): this PUT is what
/// triggers the registry's server-side blob COMMIT, and that commit legitimately
/// takes many seconds - minutes on a large blob - under `JuiceFS`/S3 backpressure
/// (observed multi-minute PUT-with-digest finalises). Capping it at the 30s
/// init-POST budget would abort an in-progress commit and fail an upload whose
/// bytes are all durably written. A wedged (half-open) close is instead caught by
/// the client's `tcp_keepalive` and retried; the empty body has no write phase for
/// the stall watchdog to guard.
async fn close_chunked_upload(
    session: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let client = upload_client()?;
    let url = append_digest(session, digest);
    let mut attempt: u32 = 0;
    loop {
        let mut req = client.put(&url).header(header::CONTENT_LENGTH, "0");
        if let Some(token) = auth_token {
            req = req.bearer_auth(token);
        }
        let err: CoreError = match req.send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                CoreError::ServerError(resp.status().as_u16(), "upload close failed".to_string())
            }
            Err(e) => CoreError::from(e),
        };
        attempt += 1;
        if !err.is_retryable() || attempt > UPLOAD_MAX_RETRIES {
            // A 404 here means the session expired between the last PATCH and this
            // close, even though every byte was already committed. Force it retryable
            // so `upload_blob_async` restarts a FRESH session (which re-uploads but
            // succeeds) rather than hard-failing an upload whose bytes are durably up.
            if matches!(&err, CoreError::ServerError(404, _)) {
                return Err(force_retryable(err));
            }
            return Err(err);
        }
        tokio::time::sleep(crate::retry::backoff_delay(attempt)).await;
    }
}

async fn try_upload_blob_once(
    uploads_url: &str,
    path: &Path,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    // Re-init a fresh OCI upload session on every attempt (audit L2): a PATCH/PUT
    // to a session a prior failed attempt already consumed fails, so init lives
    // inside the retried unit (symmetry with `try_pack_upload_once`).
    let (session, min_chunk) = post_upload_session(uploads_url, auth_token).await?;
    let file_size = File::open(path).await?.metadata().await?.len();
    let basename = basename_of(path);

    let pb = ProgressBar::new(file_size);
    // The template string is a compile-time literal; `indicatif` only errors on
    // malformed format directives, which we control at the call site.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Uploading {basename}"));

    // Resumable chunked PATCH; fall back to the monolithic streaming PUT if the
    // registry doesn't support PATCH.
    let result =
        match chunked_patch_upload(&session, path, file_size, min_chunk, auth_token, &pb).await {
            Ok(PatchOutcome::Done(final_session)) => {
                close_chunked_upload(&final_session, digest, auth_token).await
            }
            Ok(PatchOutcome::Unsupported) => {
                let file = File::open(path).await?;
                let put_url = append_digest(&session, digest);
                // put_streaming owns its own progress bar; clear ours so they don't
                // fight over the terminal line.
                pb.finish_and_clear();
                return put_streaming(
                    &put_url,
                    file,
                    file_size,
                    &basename,
                    auth_token,
                    WRITE_STALL_TIMEOUT,
                )
                .await;
            }
            Err(e) => Err(e),
        };

    match &result {
        Ok(()) => pb.finish_with_message(format!("{basename}: uploaded")),
        Err(CoreError::Stall(_)) => pb.finish_with_message(format!("{basename}: stalled")),
        Err(_) => pb.finish_with_message(format!("{basename}: failed")),
    }
    result
}

/// POST-init a fresh OCI blob-upload session and append `?digest=` - the URL a
/// monolithic PUT-with-digest targets. Still used by the pack path
/// ([`try_pack_upload_once`]); the plain path now uses [`post_upload_session`] +
/// chunked `PATCH` directly.
pub(super) async fn init_upload_session(
    uploads_url: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    // The pack path uploads a whole-buffer monolithic PUT, so the chunk-min hint
    // is irrelevant here - discard it.
    let (session, _min_chunk) = post_upload_session(uploads_url, auth_token).await?;
    Ok(append_digest(&session, digest))
}

#[cfg(test)]
mod tests {
    use super::CoreError;

    // --- resumable-upload pure helpers (audit: resumable chunked PATCH) ---

    #[test]
    fn committed_bytes_parses_inclusive_range() {
        // OCI `Range: 0-<end>` is inclusive -> committed = end + 1.
        assert_eq!(super::committed_bytes(Some("0-1023")), 1024);
        assert_eq!(super::committed_bytes(Some("0-9999999")), 10_000_000);
    }

    #[test]
    fn committed_bytes_treats_empty_marker_and_garbage_as_zero() {
        // `0-0` is Harbor's EMPTY-session marker (post-POST, nothing committed),
        // NOT one byte - mis-reading it as 1 would skip byte 0 on a resume.
        assert_eq!(super::committed_bytes(Some("0-0")), 0);
        assert_eq!(super::committed_bytes(None), 0);
        assert_eq!(super::committed_bytes(Some("")), 0);
        assert_eq!(super::committed_bytes(Some("0-notanumber")), 0);
        assert_eq!(super::committed_bytes(Some("bogus")), 0);
    }

    #[test]
    fn append_digest_picks_the_right_query_separator() {
        // Bare session -> `?`; a session that already carries `?_state=` -> `&`.
        assert_eq!(
            super::append_digest("https://reg/v2/x/blobs/uploads/uuid", "sha256:ab"),
            "https://reg/v2/x/blobs/uploads/uuid?digest=sha256:ab"
        );
        assert_eq!(
            super::append_digest("https://reg/v2/x/blobs/uploads/uuid?_state=z", "sha256:ab"),
            "https://reg/v2/x/blobs/uploads/uuid?_state=z&digest=sha256:ab"
        );
    }

    #[test]
    fn resolve_location_handles_absolute_and_relative() {
        // A relative Location resolves against the current session URL; an
        // absolute one replaces it wholesale.
        let base = "https://reg/v2/x/blobs/uploads/uuid?_state=a";
        assert!(
            super::resolve_location(base, "/v2/x/blobs/uploads/uuid?_state=b")
                .is_ok_and(|u| u == "https://reg/v2/x/blobs/uploads/uuid?_state=b")
        );
        assert!(
            super::resolve_location(base, "https://other/v2/x/blobs/uploads/uuid2")
                .is_ok_and(|u| u == "https://other/v2/x/blobs/uploads/uuid2")
        );
        // A malformed Location is a typed Integrity error, not a panic.
        assert!(matches!(
            super::resolve_location("not a url", "also not"),
            Err(CoreError::Integrity(_))
        ));
    }

    /// Source-grep guard. Setting `Content-Length` on a streaming PUT
    /// re-introduces the TOCTOU race fixed in audit U2: between
    /// `metadata().len()` and the actual `FramedRead` consumption the file
    /// can be rewritten, so a fixed length either truncates the body (file
    /// grew) or pads/short-sends (file shrunk). Reqwest's default of
    /// Transfer-Encoding: chunked for a `Body::wrap_stream` body matches
    /// the wire bytes to whatever the stream actually yields. If a future
    /// edit needs a known length, it must hash-and-stat the bytes it is
    /// about to send (e.g. read the file into memory once), not re-stat
    /// the disk file.
    #[test]
    fn upload_does_not_set_content_length_header() {
        // Needle assembled at runtime so this test source does not itself
        // match. The forbidden pattern is the literal `header::` + the
        // reqwest constant name for the Content-Length header.
        let needle = ["header", "CONTENT", "LENGTH"].join("::");
        // The uploader is now a module directory; scan every submodule so the
        // guard keeps the coverage the single-file `include_str!` had.
        let sources = [
            include_str!("mod.rs"),
            include_str!("blob.rs"),
            include_str!("cdc.rs"),
            include_str!("client.rs"),
            include_str!("hash.rs"),
            include_str!("pack.rs"),
            include_str!("stream.rs"),
            include_str!("watchdog.rs"),
        ];
        for src in sources {
            assert!(
                !src.contains(&needle),
                "uploader must NOT set the Content-Length header on the streaming PUT \
                 - that creates a TOCTOU race vs the file's actual size at stream time"
            );
        }
    }

    #[tokio::test]
    async fn put_streaming_aborts_on_write_stall() {
        // Audit H1: a peer that completes TCP+TLS, reads the request head, then
        // STOPS draining the socket (zero-window) is invisible to `connect_timeout`
        // and `tcp_keepalive`, and reqwest has no per-op write timeout - so without
        // the write-stall watchdog the streamed PUT hangs forever (wedging the
        // folder upload via the shared gate). Serve exactly that stall and assert a
        // retryable `Stall` returns within the window.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Read only the request head + first bytes, then stall: never drain
                // the rest, so the client's send buffer fills and reqwest stops
                // pulling the body. Hold the socket open; send no response.
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let _ = sock.shutdown().await;
            }
        });

        let url = format!("http://{addr}/blob");
        // 8 MiB body far exceeds the OS send buffer, so reqwest keeps pulling then
        // stalls; write_stall = 1s keeps the test fast (trips within ~2 checks).
        let total: u64 = 8 * 1024 * 1024;
        let reader = tokio::io::repeat(0u8).take(total);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::put_streaming(
                &url,
                reader,
                total,
                "stalltest",
                None,
                std::time::Duration::from_secs(1),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Err(CoreError::Stall(_)))),
            "a stalled body write must abort via the watchdog as a retryable Stall, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn put_streaming_tolerates_short_body_with_slow_response() {
        // Audit H1 regression: `done` is driven by the body stream reaching EOF, NOT
        // by `pb_total`. The reader yields FEWER bytes than `pb_total` (as if the
        // file were truncated between stat and stream - the U2 TOCTOU), and the
        // server drains the whole body then delays its response past the write-stall
        // window. A byte-count `done` would stay false and false-trip `Stall` on a
        // fully-sent upload; the EOF-driven `done` suppresses it and tolerates the
        // slow (JuiceFS-backpressure-shaped) commit response.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Read until the chunked body terminator (`0\r\n\r\n`), then delay the
                // response past the 1s write-stall window before a 200. The body is
                // all 0x00 bytes, so the ASCII '0'+CRLFCRLF terminator can't collide
                // with body content or a chunk-size line's trailing byte.
                let mut acc: Vec<u8> = Vec::new();
                let mut buf = [0u8; 8192];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            acc.extend_from_slice(&buf[..n]);
                            if acc.windows(5).any(|w| w == b"0\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
                let _ = sock.shutdown().await;
            }
        });

        let url = format!("http://{addr}/blob");
        // Body is 64 KiB but pb_total claims ~10 MiB more - the divergence the fix
        // must tolerate. write_stall = 1s < the server's 2s response delay.
        let actual: u64 = 64 * 1024;
        let pb_total: u64 = actual + 10 * 1024 * 1024;
        let reader = tokio::io::repeat(0u8).take(actual);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            super::put_streaming(
                &url,
                reader,
                pb_total,
                "shortbody",
                None,
                std::time::Duration::from_secs(1),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(outcome, Ok(Ok(()))),
            "a fully-sent body shorter than pb_total, then a slow response, must NOT trip the watchdog; got {outcome:?}"
        );
    }

    #[test]
    fn force_retryable_maps_416_to_a_retryable_error() {
        // Review P2: a 416 routed to the resume path is stored as the give-up
        // cause, but 416 is NOT `is_retryable` - returning it raw would make
        // `upload_blob_async` give up instead of restarting a fresh session.
        // `force_retryable` must wrap it in the retryable `SessionRestart`
        // variant with the original 416 preserved as the `source()` cause.
        let mapped =
            super::force_retryable(CoreError::ServerError(416, "PATCH failed: 416".into()));
        assert!(mapped.is_retryable(), "a 416 give-up must become retryable");
        assert!(
            matches!(
                mapped,
                CoreError::SessionRestart { ref source }
                    if matches!(**source, CoreError::ServerError(416, _))
            ),
            "expected SessionRestart wrapping the 416 cause, got {mapped:?}"
        );
        // ... and be reachable through the std `source()` chain, so the Python
        // boundary renders it as a `caused by:` tail.
        let cause = std::error::Error::source(&mapped);
        assert!(
            cause.is_some_and(|c| c.to_string().contains("416")),
            "the 416 must be reachable via source()"
        );
        // An already-retryable cause passes through untouched.
        let passthrough = super::force_retryable(CoreError::ServerError(503, "x".into()));
        assert!(matches!(passthrough, CoreError::ServerError(503, _)));
    }

    // Audit U3 (Phase 3.11): pin the retry classification at the
    // upload-loop entry point. The downloader has the exhaustive 4xx /
    // 5xx / boundary suite in
    // `chunked_downloader::retry_classification_tests`; these two tests
    // pin the property the upload loop depends on without re-litigating
    // the downloader's coverage - the classifier is a method on
    // `CoreError`, so the two paths share one source of truth.

    #[test]
    fn upload_retry_skips_4xx() {
        // Verify that an HTTP 401 returned from the server is NOT retried -
        // a 4xx is permanent, retrying just wastes time.
        let err = CoreError::ServerError(401, "Unauthorized".into());
        assert!(
            !err.is_retryable(),
            "4xx must not be retryable; otherwise upload_blob_async wastes 1.4s before failing"
        );
    }

    #[test]
    fn upload_retry_handles_5xx() {
        let err = CoreError::ServerError(503, "Service Unavailable".into());
        assert!(err.is_retryable());
    }
}
