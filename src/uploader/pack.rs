use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::CoreError;
use crate::uploader::blob::{init_upload_session, UPLOAD_MAX_RETRIES};
use crate::uploader::watchdog::{
    pack_frames, send_put_watchdogged, RESPONSE_WAIT_TIMEOUT, WRITE_STALL_TIMEOUT,
};

/// Read the given file byte-ranges in order into one pack blob and push it via a
/// fresh OCI upload session (POST init + monolithic PUT-with-digest). Returns the
/// pack's sha256 hex - the chunked-v2 caller records it in the pointer blob.
///
/// A pack holds only NEW chunks (chunks the dedup index had no entry for), so its
/// content digest is necessarily new and no HEAD is done (it would always 404).
/// The pack is buffered once (~64 MiB target); at the upload-worker concurrency
/// that is a bounded peak, and it keeps the retry body cheap to re-send.
pub async fn pack_upload_async(
    uploads_url: &str,
    path: &Path,
    ranges: &[(u64, u64)],
    auth_token: Option<&str>,
) -> Result<String, CoreError> {
    // Own the pack bytes once as `Bytes`: the hash pass and every retry share a
    // single allocation (a `Bytes` clone is a refcount bump, not a copy), so an
    // in-flight pack costs one pack_size instead of two - `read_ranges`' `Vec`
    // converts in without reallocating. The prior `.body(buf.to_vec())` re-copied
    // the whole pack on each attempt, which the staging peak-RSS benchmark showed
    // roughly doubled resident memory per concurrent upload.
    let body = Bytes::from(read_ranges(path, ranges).await?);
    // Hash the ~64 MiB pack on the blocking pool (audit L14): the digest is
    // CPU-bound and would otherwise stall the runtime's other in-flight pack
    // uploads for the duration. The `Bytes` clone into the closure is a refcount
    // bump, not a copy, so the pack is still buffered exactly once.
    let body_for_hash = body.clone();
    // A join failure here (panicked digest closure / runtime shutdown) is
    // `JoinFailed`, not `Io`: it reproduces on retry, so it must classify
    // permanent rather than burn the pack retry budget below.
    let digest_hex =
        tokio::task::spawn_blocking(move || hex::encode(Sha256::digest(&body_for_hash)))
            .await
            .map_err(|join_err| CoreError::JoinFailed {
                index: None,
                source: join_err,
            })?;
    let digest = format!("sha256:{digest_hex}");
    let mut retries: u32 = 0;
    loop {
        match try_pack_upload_once(uploads_url, &body, &digest, auth_token).await {
            Ok(()) => return Ok(digest_hex),
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                // Full-jitter backoff - see `upload_blob_async` (audit L-JITTER).
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
}
