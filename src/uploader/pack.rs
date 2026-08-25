use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::error::CoreError;
use crate::uploader::blob::{init_upload_session, UPLOAD_MAX_RETRIES};
use crate::uploader::client::upload_client;
use crate::uploader::watchdog::{
    pack_frames, send_put_watchdogged, RESPONSE_WAIT_TIMEOUT, WRITE_STALL_TIMEOUT,
};

/// Bound on pack HEAD (no body). Same order as the init POST: a hung registry
/// must not hold the single-flight slot forever.
const PACK_HEAD_TIMEOUT: Duration = Duration::from_secs(30);

type InflightSlot = Arc<AsyncMutex<Option<String>>>;

fn inflight_slots() -> &'static Mutex<HashMap<String, InflightSlot>> {
    static SLOTS: OnceLock<Mutex<HashMap<String, InflightSlot>>> = OnceLock::new();
    SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn slot_for(digest: &str) -> InflightSlot {
    let mut map = match inflight_slots().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.entry(digest.to_owned())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

fn drop_slot(digest: &str) {
    let mut map = match inflight_slots().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.remove(digest);
}

/// `{registry}/v2/{repo}/blobs/uploads/` → `{registry}/v2/{repo}/blobs/{digest}`.
pub(super) fn blob_head_url(uploads_url: &str, digest: &str) -> Result<String, CoreError> {
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
/// PUT: HEAD the digest, then single-flight the upload. The pack is buffered
/// once (~64 MiB target); at the upload-worker concurrency that is a bounded
/// peak, and it keeps the retry body cheap to re-send.
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
    put_pack_single_flight(uploads_url, &body, &digest, digest_hex, auth_token).await
}

async fn put_pack_single_flight(
    uploads_url: &str,
    body: &Bytes,
    digest: &str,
    digest_hex: String,
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    let slot = slot_for(digest);
    let mut guard = slot.lock().await;
    if let Some(done) = guard.as_ref() {
        return Ok(done.clone());
    }
    if blob_already_present(uploads_url, digest, auth_token).await? {
        *guard = Some(digest_hex.clone());
        drop(guard);
        drop_slot(digest);
        return Ok(digest_hex);
    }
    let mut retries: u32 = 0;
    loop {
        match try_pack_upload_once(uploads_url, body, digest, auth_token).await {
            Ok(()) => {
                *guard = Some(digest_hex.clone());
                drop(guard);
                // Drop the map entry so unique-pack uploads do not leak a
                // digest key per pack for the process lifetime. Waiters that
                // already cloned the slot still see Some; later callers HEAD.
                drop_slot(digest);
                return Ok(digest_hex);
            }
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    drop(guard);
                    drop_slot(digest);
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
        use std::io::Write;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            unreachable!("bind loopback")
        };
        let Ok(addr) = listener.local_addr() else {
            unreachable!("local_addr")
        };
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            }
        });

        let path = std::env::temp_dir().join(format!("hippius-head-{}.bin", std::process::id()));
        match std::fs::File::create(&path).and_then(|mut f| f.write_all(b"pack-bytes")) {
            Ok(()) => {}
            Err(_) => unreachable!("temp file write"),
        }
        let uploads = format!("http://{addr}/v2/x/blobs/uploads/");
        let Ok(hex) = super::pack_upload_async(&uploads, &path, &[(0, 10)], None).await else {
            unreachable!("HEAD 200 must skip PUT")
        };
        assert_eq!(hex, hex::encode(Sha256::digest(b"pack-bytes")));
        server.abort();
        std::fs::remove_file(&path).unwrap_or(());
    }
}
