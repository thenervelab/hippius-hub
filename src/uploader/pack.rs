use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::error::CoreError;
use crate::uploader::blob::{init_upload_session, INIT_POST_TIMEOUT, UPLOAD_MAX_RETRIES};
use crate::uploader::client::upload_client;
use crate::uploader::watchdog::{
    pack_frames, send_put_watchdogged, RESPONSE_WAIT_TIMEOUT, WRITE_STALL_TIMEOUT,
};

/// Bound on pack HEAD (no body). Same order as the init POST: a hung registry
/// must not hold the single-flight slot forever.
const PACK_HEAD_TIMEOUT: Duration = INIT_POST_TIMEOUT;

/// `true` once the leader has landed the pack; a waiter that observes it
/// returns without touching the registry. The digest is part of the key, so
/// there is nothing else to hand over.
type InflightSlot = Arc<AsyncMutex<bool>>;
type SlotKey = (String, String);

fn inflight_slots() -> &'static Mutex<HashMap<SlotKey, InflightSlot>> {
    static SLOTS: OnceLock<Mutex<HashMap<SlotKey, InflightSlot>>> = OnceLock::new();
    SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn slot_for(uploads_url: &str, digest: &str) -> InflightSlot {
    inflight_slots()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry((uploads_url.to_owned(), digest.to_owned()))
        .or_insert_with(|| Arc::new(AsyncMutex::new(false)))
        .clone()
}

fn drop_slot(uploads_url: &str, digest: &str) {
    inflight_slots()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(uploads_url.to_owned(), digest.to_owned()));
}

/// Cache of `(uploads_url, digest)` this process has already landed (a
/// successful pack body upload, or a blob probe that returned 200).
///
/// Unbounded on purpose. `SlotLease` drops *inflight* `Arc<AsyncMutex>` entries
/// so unique packs do not retain a slot for the process lifetime; this set is
/// the opposite — a later call must know the digest landed so it probes the
/// registry instead of repeating the body upload. Two owned `String`s per
/// unique pack (~190 B; ~61 KiB for a 20 GiB unique upload at 64 MiB packs).
/// Fine for the CLI (the process exits). A long-lived library consumer that
/// never exits and uploads unbounded unique packs would want a cap; do not add
/// one here for the CLI path.
fn completed_packs() -> &'static Mutex<HashSet<SlotKey>> {
    static DONE: OnceLock<Mutex<HashSet<SlotKey>>> = OnceLock::new();
    DONE.get_or_init(|| Mutex::new(HashSet::new()))
}

fn was_completed(uploads_url: &str, digest: &str) -> bool {
    completed_packs()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&(uploads_url.to_owned(), digest.to_owned()))
}

fn mark_completed(uploads_url: &str, digest: &str) {
    completed_packs()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((uploads_url.to_owned(), digest.to_owned()));
}

/// Remove the map entry when `put_pack_single_flight` returns, including
/// `?` on a permanent HEAD error. Waiters that already cloned the Arc still
/// see the leader's result.
struct SlotLease {
    uploads_url: String,
    digest: String,
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        drop_slot(&self.uploads_url, &self.digest);
    }
}

/// A retryable HEAD failure means "presence unknown" — PUT. A 502/timeout
/// on HEAD must not fail a 300-pack upload the way a `?` on HEAD would.
fn present_or_unknown(result: Result<bool, CoreError>) -> Result<bool, CoreError> {
    match result {
        Ok(present) => Ok(present),
        Err(e) if e.is_retryable() => Ok(false),
        Err(e) => Err(e),
    }
}

/// `{registry}/v2/{repo}/blobs/uploads/` → `{registry}/v2/{repo}/blobs/{digest}`.
fn blob_head_url(uploads_url: &str, digest: &str) -> Result<String, CoreError> {
    let trimmed = uploads_url.trim_end_matches('/');
    let Some(blobs) = trimmed.strip_suffix("/uploads") else {
        return Err(CoreError::InvalidArgument(format!(
            "uploads URL missing /uploads suffix: {uploads_url}"
        )));
    };
    Ok(format!("{blobs}/{digest}"))
}

async fn blob_already_present(
    uploads_url: &str,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<bool, CoreError> {
    let url = blob_head_url(uploads_url, digest)?;
    let client = upload_client()?;
    let mut req = client.head(&url).timeout(PACK_HEAD_TIMEOUT);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await?;
    match resp.status().as_u16() {
        200 => Ok(true),
        404 => Ok(false),
        status => Err(CoreError::ServerError(
            status,
            format!("pack HEAD {digest}"),
        )),
    }
}

/// Read the given file byte-ranges in order into one pack blob and push it via a
/// fresh OCI upload session (POST init + monolithic PUT-with-digest). Returns the
/// pack's sha256 hex - the chunked-v2 caller records it in the pointer blob.
///
/// Identical pack bytes (intra-file CDC repeats, or two files racing) share one
/// PUT via single-flight. HEAD is skipped for a digest this process has never
/// completed — unique 1 GiB is 16 new packs, all 404s, so an extra RTT before
/// every PUT is wasted. After a successful PUT (or a HEAD 200), later calls
/// HEAD and skip the body. The pack is buffered once (~64 MiB target).
pub async fn pack_upload_async(
    uploads_url: &str,
    path: &Path,
    ranges: &[(u64, u64)],
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    let body = Bytes::from(read_ranges(path, ranges).await?);
    let body_for_hash = body.clone();
    let digest_hex =
        tokio::task::spawn_blocking(move || hex::encode(Sha256::digest(&body_for_hash)))
            .await
            .map_err(|join_err| CoreError::JoinFailed {
                index: None,
                source: join_err,
            })?;
    let digest = format!("sha256:{digest_hex}");
    put_pack_single_flight(uploads_url, &body, &digest, auth_token).await?;
    Ok(digest_hex)
}

async fn put_pack_single_flight(
    uploads_url: &str,
    body: &Bytes,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let slot = slot_for(uploads_url, digest);
    let _lease = SlotLease {
        uploads_url: uploads_url.to_owned(),
        digest: digest.to_owned(),
    };
    let mut guard = slot.lock().await;
    if *guard {
        return Ok(());
    }
    // Unique packs have never been PUT by this process; HEAD would 404.
    // Only probe the registry when a prior success in this process makes
    // a hit plausible (re-upload of the same digest).
    let already = if was_completed(uploads_url, digest) {
        present_or_unknown(blob_already_present(uploads_url, digest, auth_token).await)?
    } else {
        false
    };
    if already {
        *guard = true;
        return Ok(());
    }
    let mut retries: u32 = 0;
    loop {
        match try_pack_upload_once(uploads_url, body, digest, auth_token).await {
            Ok(()) => {
                *guard = true;
                mark_completed(uploads_url, digest);
                return Ok(());
            }
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                tokio::time::sleep(crate::retry::backoff_delay(retries)).await;
            }
        }
    }
}

pub(super) async fn read_ranges(path: &Path, ranges: &[(u64, u64)]) -> Result<Vec<u8>, CoreError> {
    let mut file = File::open(path).await?;
    let total: u64 = ranges.iter().map(|(_off, len)| *len).sum();
    let cap = usize::try_from(total)
        .map_err(|_| CoreError::InvalidArgument(format!("pack size {total} exceeds usize")))?;
    let mut buf: Vec<u8> = Vec::with_capacity(cap);
    for &(offset, len) in ranges {
        file.seek(SeekFrom::Start(offset)).await?;
        let before = buf.len();
        // read_to_end appends; take() bounds it to exactly `len` bytes.
        (&mut file).take(len).read_to_end(&mut buf).await?;
        let got = (buf.len() - before) as u64;
        if got != len {
            return Err(CoreError::Integrity(format!(
                "short read packing range at offset {offset}: wanted {len}, got {got}"
            )));
        }
    }
    Ok(buf)
}

async fn try_pack_upload_once(
    uploads_url: &str,
    body: &Bytes,
    digest: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    // Re-init a fresh session per attempt (audit L2/H1) - shared with the plain path.
    let put_url = init_upload_session(uploads_url, digest, auth_token).await?;
    // Route the pack PUT through the same write-stall watchdog as the whole-file
    // path (audit H1). The bare `put.send().await` here previously left the pack
    // PUT - the wedge point behind the shared `_pack_upload_gate` - unprotected
    // against a peer that completes the (now bounded) init POST then stops draining
    // the body mid-write. Framing the in-memory buffer lets the watchdog re-stamp
    // as the socket accepts each frame (see `PUT_FRAME_BYTES`).
    let frames = pack_frames(body);
    let body_stream = futures::stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
    let put_resp = send_put_watchdogged(
        &put_url,
        body_stream,
        auth_token,
        WRITE_STALL_TIMEOUT,
        RESPONSE_WAIT_TIMEOUT,
    )
    .await?;
    if !put_resp.status().is_success() {
        return Err(CoreError::ServerError(
            put_resp.status().as_u16(),
            format!("pack PUT failed: {:?}", put_resp.status()),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_ranges_concatenates_in_order() {
        use super::read_ranges;
        use crate::error::CoreError;
        use std::io::Write;

        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            unreachable!("current-thread runtime builds")
        };
        let path = std::env::temp_dir().join(format!("hippius-rr-{}.bin", std::process::id()));
        match std::fs::File::create(&path).and_then(|mut f| f.write_all(b"0123456789")) {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        // Out-of-order, non-contiguous ranges scatter-gather in pack order:
        // [6,4)+[0,3)+[4,2) over "0123456789" -> "6789"+"012"+"45" = "678901245".
        match rt.block_on(read_ranges(&path, &[(6, 4), (0, 3), (4, 2)])) {
            Ok(bytes) => assert_eq!(bytes, b"678901245"),
            Err(_) => unreachable!("read of valid ranges must succeed"),
        }
        // A range past EOF is a short read -> Integrity error, never silent truncation.
        let bad = rt.block_on(read_ranges(&path, &[(8, 5)]));
        assert!(matches!(bad, Err(CoreError::Integrity(_))));
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[test]
    fn blob_head_url_strips_uploads_suffix() {
        match super::blob_head_url("https://reg/v2/ns/repo/blobs/uploads/", "sha256:ab") {
            Ok(url) => assert_eq!(url, "https://reg/v2/ns/repo/blobs/sha256:ab"),
            Err(_) => unreachable!("uploads/ suffix"),
        }
        match super::blob_head_url("https://reg/v2/ns/repo/blobs/uploads", "sha256:ab") {
            Ok(url) => assert_eq!(url, "https://reg/v2/ns/repo/blobs/sha256:ab"),
            Err(_) => unreachable!("uploads suffix"),
        }
        assert!(super::blob_head_url("https://reg/v2/ns/repo/blobs/", "sha256:ab").is_err());
    }

    #[tokio::test]
    async fn pack_upload_skips_put_when_head_is_200() {
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;

        let (server, path, uploads, puts, heads) =
            counting_registry_fixture("head", b"pack-bytes", true, false).await;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"pack-bytes")));
        super::mark_completed(&uploads, &digest);
        let Ok(hex) = super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None).await else {
            unreachable!("HEAD 200 must skip PUT")
        };
        assert_eq!(hex, hex::encode(Sha256::digest(b"pack-bytes")));
        assert_eq!(
            heads.load(Ordering::SeqCst),
            1,
            "a completed digest is probed"
        );
        assert_eq!(puts.load(Ordering::SeqCst), 0, "HEAD 200 must skip the PUT");
        server.abort();
    }

    #[test]
    fn retryable_head_error_is_presence_unknown() {
        use crate::error::CoreError;

        match super::present_or_unknown(Err(CoreError::ServerError(502, "pack HEAD".into()))) {
            Ok(false) => {}
            other => unreachable!("502 HEAD must PUT, got {other:?}"),
        }
        match super::present_or_unknown(Err(CoreError::ServerError(403, "pack HEAD".into()))) {
            Err(CoreError::ServerError(403, _)) => {}
            other => unreachable!("403 HEAD must stay permanent, got {other:?}"),
        }
        match super::present_or_unknown(Ok(true)) {
            Ok(true) => {}
            other => unreachable!("HEAD 200 stays present, got {other:?}"),
        }
    }

    /// Serve the HEAD -> POST -> PUT pack handshake, counting HEADs and PUTs.
    ///
    /// HEAD answers 200 when `head_found`, else 404 so a caller that still
    /// probes falls through to PUT. Unique packs must not probe. The first PUT
    /// answers 403 (permanent) when `fail_first_put`, later PUTs 201. Bodies
    /// are drained by idle-timeout rather than Content-Length: the pack PUT is
    /// a framed stream, so it is chunked and has no length header.
    async fn serve_counting_registry(
        listener: tokio::net::TcpListener,
        addr: std::net::SocketAddr,
        puts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        heads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        head_found: bool,
        fail_first_put: bool,
    ) {
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let puts = std::sync::Arc::clone(&puts);
            let heads = std::sync::Arc::clone(&heads);
            tokio::spawn(async move {
                let mut seen = Vec::new();
                let mut buf = [0u8; 4096];
                // First read carries the request line; keep draining until the
                // peer pauses, so a framed body cannot be mistaken for a
                // pipelined request.
                loop {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(120),
                        sock.read(&mut buf),
                    )
                    .await
                    {
                        Ok(Ok(0)) | Err(_) => break,
                        Ok(Ok(n)) => seen.extend_from_slice(&buf[..n]),
                        Ok(Err(_)) => return,
                    }
                }
                let req = String::from_utf8_lossy(&seen);
                let resp = if req.starts_with("HEAD") {
                    heads.fetch_add(1, Ordering::SeqCst);
                    if head_found {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                } else if req.starts_with("POST") {
                    format!(
                        "HTTP/1.1 202 Accepted\r\nLocation: http://{addr}/v2/x/blobs/uploads/s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else if req.starts_with("PUT") {
                    let nth = puts.fetch_add(1, Ordering::SeqCst);
                    if fail_first_put && nth == 0 {
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    } else {
                        "HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                } else {
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    }

    #[tokio::test]
    async fn concurrent_identical_packs_put_once() {
        use std::sync::atomic::Ordering;

        let (server, path, uploads, puts, heads) =
            counting_registry_fixture("two", b"pack-bytes", false, false).await;

        // Two callers, same (uploads_url, digest): the second must wait on the
        // leader's slot and adopt its result, not issue a second PUT.
        let (a, b) = tokio::join!(
            super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None),
            super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None),
        );
        let (Ok(ha), Ok(hb)) = (a, b) else {
            unreachable!("both callers must succeed")
        };
        assert_eq!(ha, hb, "both callers must report the same digest");
        assert_eq!(
            puts.load(Ordering::SeqCst),
            1,
            "single-flight must collapse two identical concurrent packs into one PUT"
        );
        assert_eq!(
            heads.load(Ordering::SeqCst),
            0,
            "a first-seen digest must not HEAD (unique packs always 404)"
        );

        server.abort();
    }

    #[tokio::test]
    async fn unique_pack_skips_head_when_unknown() {
        use std::sync::atomic::Ordering;

        let (server, path, uploads, puts, heads) =
            counting_registry_fixture("uniq", b"unique-pack", false, false).await;
        let Ok(_) = super::pack_upload_async(&uploads, &path.0, &[(0, 11)], None).await else {
            unreachable!("unique pack PUT must succeed")
        };
        assert_eq!(
            heads.load(Ordering::SeqCst),
            0,
            "unknown digest must skip HEAD"
        );
        assert_eq!(
            puts.load(Ordering::SeqCst),
            1,
            "unknown digest must PUT once"
        );

        server.abort();
    }

    /// Bind a loopback counting registry and write `pack` to a scratch file
    /// (removed when the returned guard drops, so a failed assertion still
    /// cleans up).
    async fn counting_registry_fixture(
        tag: &str,
        pack: &[u8],
        head_found: bool,
        fail_first_put: bool,
    ) -> (
        tokio::task::JoinHandle<()>,
        crate::incremental_hash::test_support::TempFileGuard,
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use crate::incremental_hash::test_support::{scratch_path, TempFileGuard};
        use std::io::Write;
        use std::sync::atomic::AtomicUsize;
        use tokio::net::TcpListener;

        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            unreachable!("bind loopback")
        };
        let Ok(addr) = listener.local_addr() else {
            unreachable!("local_addr")
        };
        let puts = std::sync::Arc::new(AtomicUsize::new(0));
        let heads = std::sync::Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(serve_counting_registry(
            listener,
            addr,
            std::sync::Arc::clone(&puts),
            std::sync::Arc::clone(&heads),
            head_found,
            fail_first_put,
        ));
        let path = scratch_path(&format!("sf-{tag}"));
        match std::fs::File::create(&path).and_then(|mut f| f.write_all(pack)) {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        let uploads = format!("http://{addr}/v2/{tag}/blobs/uploads/");
        (server, TempFileGuard(path), uploads, puts, heads)
    }

    #[tokio::test]
    async fn completed_put_makes_the_next_call_head_and_skip_the_body() {
        use std::sync::atomic::Ordering;

        // Re-upload of a digest this process already landed: the second call
        // must HEAD (the gate opened on the first PUT) and, on 200, send no
        // body. Pins `mark_completed` after a successful PUT end to end.
        let (server, path, uploads, puts, heads) =
            counting_registry_fixture("again", b"again-pack", true, false).await;
        let Ok(first) = super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None).await else {
            unreachable!("first PUT must succeed")
        };
        let Ok(second) = super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None).await else {
            unreachable!("HEAD 200 must skip PUT")
        };
        assert_eq!(first, second);
        assert_eq!(
            puts.load(Ordering::SeqCst),
            1,
            "only the first call may PUT"
        );
        assert_eq!(
            heads.load(Ordering::SeqCst),
            1,
            "a completed digest must be probed, not re-sent"
        );

        server.abort();
    }

    #[tokio::test]
    async fn failed_leader_drops_its_slot_and_the_next_call_puts_afresh() {
        use crate::error::CoreError;
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;

        // A permanent PUT failure must leave neither an inflight slot (the
        // lease drops it on the error return) nor a completed mark: the next
        // call starts from nothing and re-PUTs instead of inheriting the
        // failure or skipping to a HEAD.
        let (server, path, uploads, puts, heads) =
            counting_registry_fixture("failed", b"fail-first", false, true).await;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"fail-first")));
        match super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None).await {
            Err(CoreError::ServerError(403, _)) => {}
            other => unreachable!("first PUT must fail permanently, got {other:?}"),
        }
        let leaked = super::inflight_slots()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&(uploads.clone(), digest.clone()));
        assert!(
            !leaked,
            "a failed leader must not leave its slot in the map"
        );
        assert!(
            !super::was_completed(&uploads, &digest),
            "a failed PUT must not mark the digest completed"
        );
        let Ok(_) = super::pack_upload_async(&uploads, &path.0, &[(0, 10)], None).await else {
            unreachable!("the retry after a failed leader must PUT and succeed")
        };
        assert_eq!(puts.load(Ordering::SeqCst), 2, "both calls must PUT");
        assert_eq!(
            heads.load(Ordering::SeqCst),
            0,
            "nothing completed, so no HEAD"
        );

        server.abort();
    }

    #[test]
    fn single_flight_slot_is_per_repo() {
        let repo_a = super::slot_for("https://r/v2/repo-a/blobs/uploads/", "sha256:ab");
        let repo_b = super::slot_for("https://r/v2/repo-b/blobs/uploads/", "sha256:ab");
        assert!(!std::sync::Arc::ptr_eq(&repo_a, &repo_b));
        let repo_a_again = super::slot_for("https://r/v2/repo-a/blobs/uploads/", "sha256:ab");
        assert!(std::sync::Arc::ptr_eq(&repo_a, &repo_a_again));
    }

    /// What a scripted registry answers to one PUT: a status, or hold the
    /// response back for 30 s so the caller can be cancelled mid-flight.
    #[derive(Clone, Copy)]
    enum PutReply {
        Status(u16),
        Stall,
    }

    /// Scripted HEAD -> POST -> PUT registry. `head` is the HEAD status (None =
    /// close the socket without answering, the shape of a dropped connection or
    /// an idle-timeout cut). `put_script` is consumed one entry per PUT; the last
    /// entry repeats. Counts HEADs and PUTs like `serve_counting_registry`.
    #[cfg(test)]
    async fn serve_scripted_registry(
        listener: tokio::net::TcpListener,
        addr: std::net::SocketAddr,
        puts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        heads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        head: Option<u16>,
        put_script: Vec<PutReply>,
    ) {
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let status_line = |code: u16| {
            format!("HTTP/1.1 {code} Scripted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        };
        let put_script = std::sync::Arc::new(put_script);
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let puts = std::sync::Arc::clone(&puts);
            let heads = std::sync::Arc::clone(&heads);
            let put_script = std::sync::Arc::clone(&put_script);
            tokio::spawn(async move {
                let mut seen = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(120),
                        sock.read(&mut buf),
                    )
                    .await
                    {
                        Ok(Ok(0)) | Err(_) => break,
                        Ok(Ok(n)) => seen.extend_from_slice(&buf[..n]),
                        Ok(Err(_)) => return,
                    }
                }
                let req = String::from_utf8_lossy(&seen);
                let resp = if req.starts_with("HEAD") {
                    heads.fetch_add(1, Ordering::SeqCst);
                    if let Some(code) = head {
                        status_line(code)
                    } else {
                        let _ = sock.shutdown().await;
                        return;
                    }
                } else if req.starts_with("POST") {
                    format!(
                        "HTTP/1.1 202 Accepted\r\nLocation: http://{addr}/v2/x/blobs/uploads/s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else if req.starts_with("PUT") {
                    let nth = puts.fetch_add(1, Ordering::SeqCst);
                    let Some(reply) = put_script.get(nth).or(put_script.last()) else {
                        unreachable!("put_script is never empty")
                    };
                    match reply {
                        PutReply::Status(code) => status_line(*code),
                        PutReply::Stall => {
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            status_line(201)
                        }
                    }
                } else {
                    status_line(400)
                };
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    }

    /// Bind a loopback scripted registry and write `pack` to a temp file.
    #[cfg(test)]
    async fn scripted_registry_fixture(
        tag: &str,
        pack: &[u8],
        head: Option<u16>,
        put_script: Vec<PutReply>,
    ) -> (
        tokio::task::JoinHandle<()>,
        std::path::PathBuf,
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::io::Write;
        use std::sync::atomic::AtomicUsize;
        use tokio::net::TcpListener;

        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            unreachable!("bind loopback")
        };
        let Ok(addr) = listener.local_addr() else {
            unreachable!("local_addr")
        };
        let puts = std::sync::Arc::new(AtomicUsize::new(0));
        let heads = std::sync::Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(serve_scripted_registry(
            listener,
            addr,
            std::sync::Arc::clone(&puts),
            std::sync::Arc::clone(&heads),
            head,
            put_script,
        ));
        let path =
            std::env::temp_dir().join(format!("hippius-sr-{tag}-{}.bin", std::process::id()));
        match std::fs::File::create(&path).and_then(|mut f| f.write_all(pack)) {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        let uploads = format!("http://{addr}/v2/{tag}/blobs/uploads/");
        (server, path, uploads, puts, heads)
    }

    #[cfg(test)]
    fn slot_is_inflight(uploads: &str, digest: &str) -> bool {
        super::inflight_slots()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&(uploads.to_owned(), digest.to_owned()))
    }

    #[tokio::test]
    async fn completed_digest_whose_head_404s_is_put_again() {
        use std::sync::atomic::Ordering;

        // The registry garbage-collected the blob between two uploads in one
        // process: the HEAD gate opened, the probe misses, and the pack must be
        // re-sent rather than trusted from the completed cache.
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "gc404",
            b"gc-pack-01",
            Some(404),
            vec![PutReply::Status(201)],
        )
        .await;
        let Ok(first) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("first PUT must succeed")
        };
        let Ok(second) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("HEAD 404 must fall through to a PUT")
        };
        assert_eq!(first, second);
        assert_eq!(
            heads.load(Ordering::SeqCst),
            1,
            "the completed digest is probed once"
        );
        assert_eq!(
            puts.load(Ordering::SeqCst),
            2,
            "a HEAD miss must re-send the pack"
        );

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn completed_digest_whose_head_503s_is_put_not_failed() {
        use std::sync::atomic::Ordering;

        // A 5xx on the probe is "presence unknown": the upload must proceed to
        // the PUT, not fail the whole file on an optimisation's round trip.
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "h503",
            b"h503-pack-",
            Some(503),
            vec![PutReply::Status(201)],
        )
        .await;
        let Ok(_) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("first PUT must succeed")
        };
        let Ok(_) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("a 503 HEAD must not fail the upload")
        };
        assert_eq!(heads.load(Ordering::SeqCst), 1);
        assert_eq!(puts.load(Ordering::SeqCst), 2, "unknown presence must PUT");

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn completed_digest_whose_head_403s_fails_without_a_put() {
        use crate::error::CoreError;
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;

        // 403 is permanent: no PUT would fare better, so the error surfaces and
        // the slot is released for the next caller (the completed mark stays,
        // the first PUT did land).
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "h403",
            b"h403-pack-",
            Some(403),
            vec![PutReply::Status(201)],
        )
        .await;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"h403-pack-")));
        let Ok(_) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("first PUT must succeed")
        };
        match super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await {
            Err(CoreError::ServerError(403, _)) => {}
            other => unreachable!("a 403 HEAD must surface as permanent, got {other:?}"),
        }
        assert_eq!(heads.load(Ordering::SeqCst), 1);
        assert_eq!(
            puts.load(Ordering::SeqCst),
            1,
            "a 403 HEAD must not fall through to PUT"
        );
        assert!(
            !slot_is_inflight(&uploads, &digest),
            "the `?` on HEAD must still drop the single-flight slot"
        );

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn completed_digest_whose_head_connection_drops_is_put() {
        use std::sync::atomic::Ordering;

        // The registry closes the HEAD socket without a status (what a proxy
        // idle cut or a mid-handshake reset looks like): a transport error is
        // retryable, so presence is unknown and the pack is PUT.
        let (server, path, uploads, puts, heads) =
            scripted_registry_fixture("hdrop", b"hdrop-pack", None, vec![PutReply::Status(201)])
                .await;
        let Ok(_) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("first PUT must succeed")
        };
        let Ok(_) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("a dropped HEAD must not fail the upload")
        };
        assert_eq!(heads.load(Ordering::SeqCst), 1);
        assert_eq!(puts.load(Ordering::SeqCst), 2, "unknown presence must PUT");

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn completed_mark_is_per_repo_so_another_repo_skips_the_head() {
        use std::sync::atomic::Ordering;

        // The same bytes landed under repo A. Uploading them to repo B in the
        // same process is a first-seen digest for B: no HEAD (it would 404 on
        // a namespace that never saw the blob), straight to PUT.
        let (server_a, path, uploads_a, puts_a, heads_a) = scripted_registry_fixture(
            "repo-a",
            b"shared-pak",
            Some(200),
            vec![PutReply::Status(201)],
        )
        .await;
        let (server_b, path_b, uploads_b, puts_b, heads_b) = scripted_registry_fixture(
            "repo-b",
            b"shared-pak",
            Some(200),
            vec![PutReply::Status(201)],
        )
        .await;
        let Ok(_) = super::pack_upload_async(&uploads_a, &path, &[(0, 10)], None).await else {
            unreachable!("repo A PUT must succeed")
        };
        let Ok(_) = super::pack_upload_async(&uploads_b, &path, &[(0, 10)], None).await else {
            unreachable!("repo B PUT must succeed")
        };
        assert_eq!(puts_a.load(Ordering::SeqCst), 1);
        assert_eq!(heads_a.load(Ordering::SeqCst), 0);
        assert_eq!(
            puts_b.load(Ordering::SeqCst),
            1,
            "repo B has never seen the digest: PUT"
        );
        assert_eq!(
            heads_b.load(Ordering::SeqCst),
            0,
            "repo B must not inherit repo A's mark"
        );

        server_a.abort();
        server_b.abort();
        std::fs::remove_file(&path).unwrap_or(());
        std::fs::remove_file(&path_b).unwrap_or(());
    }

    #[tokio::test]
    async fn waiter_redoes_the_put_when_the_leader_fails_permanently() {
        use crate::error::CoreError;
        use std::sync::atomic::Ordering;

        // Two concurrent callers, same pack. The leader's PUT is refused with a
        // permanent 403; the waiter must NOT adopt that failure (the slot flag
        // is still `false`) - it takes the lock and PUTs for itself.
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "lead-fail",
            b"lead-fail!",
            Some(200),
            vec![PutReply::Status(403), PutReply::Status(201)],
        )
        .await;
        let (a, b) = tokio::join!(
            super::pack_upload_async(&uploads, &path, &[(0, 10)], None),
            super::pack_upload_async(&uploads, &path, &[(0, 10)], None),
        );
        let outcomes = [a, b];
        let failed = outcomes
            .iter()
            .filter(|r| matches!(r, Err(CoreError::ServerError(403, _))))
            .count();
        let succeeded = outcomes.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            failed, 1,
            "exactly the leader sees the 403, got {outcomes:?}"
        );
        assert_eq!(
            succeeded, 1,
            "the waiter must PUT for itself, got {outcomes:?}"
        );
        assert_eq!(puts.load(Ordering::SeqCst), 2, "leader PUT + waiter PUT");
        assert_eq!(
            heads.load(Ordering::SeqCst),
            0,
            "no HEAD: nothing completed before the waiter ran"
        );

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn cancelled_leader_releases_the_slot_to_the_waiter() {
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        // The leader is dropped (Ctrl-C / task abort) while its PUT is in
        // flight. Its guard and lease go with it, so a waiter blocked on the
        // slot wakes, sees `false`, and PUTs; and no stale slot survives.
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "lead-cancel",
            b"lead-cncl!",
            Some(200),
            vec![PutReply::Stall, PutReply::Status(201)],
        )
        .await;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"lead-cncl!")));
        let (uploads_l, path_l) = (uploads.clone(), path.clone());
        let leader = tokio::spawn(async move {
            super::pack_upload_async(&uploads_l, &path_l, &[(0, 10)], None).await
        });
        let mut waited = 0u32;
        while puts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
            assert!(waited < 1000, "leader never reached its PUT");
        }
        assert!(
            slot_is_inflight(&uploads, &digest),
            "the leader holds the slot"
        );
        let (uploads_w, path_w) = (uploads.clone(), path.clone());
        let waiter = tokio::spawn(async move {
            super::pack_upload_async(&uploads_w, &path_w, &[(0, 10)], None).await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            puts.load(Ordering::SeqCst),
            1,
            "the waiter must be parked on the slot"
        );

        leader.abort();
        match leader.await {
            Err(e) if e.is_cancelled() => {}
            other => unreachable!("leader must be cancelled, got {other:?}"),
        }
        let Ok(Ok(Ok(hex))) = tokio::time::timeout(Duration::from_secs(10), waiter).await else {
            unreachable!("the waiter must wake and PUT after the leader is cancelled")
        };
        assert_eq!(hex, hex::encode(Sha256::digest(b"lead-cncl!")));
        assert_eq!(puts.load(Ordering::SeqCst), 2, "waiter must PUT for itself");
        assert_eq!(heads.load(Ordering::SeqCst), 0);
        assert!(
            !slot_is_inflight(&uploads, &digest),
            "no slot may outlive the cancelled leader and the finished waiter"
        );
        assert!(
            super::was_completed(&uploads, &digest),
            "the waiter's PUT landed"
        );

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn retry_exhaustion_on_put_fails_and_leaves_no_slot_or_mark() {
        use crate::error::CoreError;
        use crate::uploader::blob::UPLOAD_MAX_RETRIES;
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;

        // Every PUT 503s: the retry ladder is walked exactly once
        // (1 + UPLOAD_MAX_RETRIES attempts), the error is the last 503, the
        // slot is released and nothing is marked completed.
        let (server, path, uploads, puts, heads) = scripted_registry_fixture(
            "exhaust",
            b"exhaust-pk",
            Some(200),
            vec![PutReply::Status(503)],
        )
        .await;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"exhaust-pk")));
        match super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await {
            Err(CoreError::ServerError(503, _)) => {}
            other => unreachable!("exhausted retries must surface the 503, got {other:?}"),
        }
        let Ok(expected_puts) = usize::try_from(UPLOAD_MAX_RETRIES + 1) else {
            unreachable!("small constant")
        };
        assert_eq!(
            puts.load(Ordering::SeqCst),
            expected_puts,
            "one attempt per retry budget step"
        );
        assert_eq!(heads.load(Ordering::SeqCst), 0);
        assert!(
            !slot_is_inflight(&uploads, &digest),
            "an exhausted leader must drop its slot"
        );
        assert!(
            !super::was_completed(&uploads, &digest),
            "a failed pack is not completed"
        );

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }

    #[tokio::test]
    async fn init_post_failure_does_not_mark_completed() {
        use crate::error::CoreError;
        use sha2::Digest;
        use sha2::Sha256;
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        // The registry refuses the upload-session POST outright (401): the
        // failure is permanent, no PUT is ever attempted, and a later call in
        // this process must not HEAD as if the pack had landed.
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            unreachable!("bind loopback")
        };
        let Ok(addr) = listener.local_addr() else {
            unreachable!("local_addr")
        };
        let posts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let posts_srv = std::sync::Arc::clone(&posts);
        let server = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let posts = std::sync::Arc::clone(&posts_srv);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    posts.fetch_add(1, Ordering::SeqCst);
                    let _ = sock
                        .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                });
            }
        });
        let path = std::env::temp_dir().join(format!("hippius-post401-{}.bin", std::process::id()));
        match std::fs::File::create(&path)
            .and_then(|mut f| std::io::Write::write_all(&mut f, b"post-401!!"))
        {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        let uploads = format!("http://{addr}/v2/p401/blobs/uploads/");
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"post-401!!")));
        match super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await {
            Err(CoreError::ServerError(401, _)) => {}
            other => unreachable!("a 401 on the init POST is permanent, got {other:?}"),
        }
        assert_eq!(
            posts.load(Ordering::SeqCst),
            1,
            "one POST, no retry, no PUT"
        );
        assert!(!super::was_completed(&uploads, &digest));
        assert!(!slot_is_inflight(&uploads, &digest));

        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }
}
