//! The LAN leg: hand Harbor one ordinary, native OCI blob upload.
//!
//! This is the whole point of the "serialize the LAN" half of the design — the
//! receiver never writes Harbor's storage layout or reimplements registration.
//! It does the standard two-step OCI monolithic upload (init -> PUT-with-digest)
//! streaming the reassembled parts in order, and Harbor hashes the stream
//! inline and rejects on digest mismatch. The client's `Authorization` is
//! replayed verbatim so Harbor authorizes the push as the client, not as the
//! receiver.

use std::path::PathBuf;

use bytes::Bytes;
use futures::Stream;
use reqwest::header;
use tokio::io::AsyncReadExt;

use crate::error::ReceiverError;

/// Read buffer for streaming each scratch part into the Harbor PUT body.
const STREAM_CHUNK: usize = 1 << 20; // 1 MiB

/// Push the reassembled blob to Harbor and return the registry's blob
/// `Location` (or a synthesized `/v2/{repo}/blobs/{digest}` if Harbor omits it).
pub(crate) async fn push_blob(
    http: &reqwest::Client,
    harbor_base: &str,
    repo: &str,
    digest: &str,
    auth: Option<&str>,
    part_paths: Vec<PathBuf>,
) -> Result<String, ReceiverError> {
    let location = init_upload(http, harbor_base, repo, auth).await?;
    let sep = if location.contains('?') { '&' } else { '?' };
    let put_url = format!("{location}{sep}digest={digest}");

    let body = reqwest::Body::wrap_stream(concat_parts(part_paths));
    let mut put = http
        .put(&put_url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);
    if let Some(auth) = auth {
        put = put.header(header::AUTHORIZATION, auth);
    }
    let res = put.send().await?;
    if !res.status().is_success() {
        // A digest mismatch surfaces here as a 400 from Harbor — the inline
        // hash is the backstop against a misassembled or corrupted part.
        return Err(ReceiverError::Harbor(format!("blob PUT returned {}", res.status())));
    }
    Ok(res
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .map_or_else(|| format!("/v2/{repo}/blobs/{digest}"), ToString::to_string))
}

/// Open the OCI upload session against Harbor and return the absolute upload
/// `Location` URL to PUT the finalized blob to.
async fn init_upload(http: &reqwest::Client, harbor_base: &str, repo: &str, auth: Option<&str>) -> Result<String, ReceiverError> {
    let url = format!("{harbor_base}/v2/{repo}/blobs/uploads/");
    let mut req = http.post(&url).header(header::CONTENT_LENGTH, "0");
    if let Some(auth) = auth {
        req = req.header(header::AUTHORIZATION, auth);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        return Err(ReceiverError::Harbor(format!("upload init returned {}", res.status())));
    }
    let location = res
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ReceiverError::Harbor("registry omitted the Location header".to_string()))?;
    // Harbor returns a relative `Location`; make it absolute against the base.
    if location.starts_with('/') {
        Ok(format!("{harbor_base}{location}"))
    } else {
        Ok(location.to_string())
    }
}

/// Stream the part files back-to-back in the order given, yielding their bytes
/// as one continuous body. Parts live on local `NVMe`, so this sequential read is
/// the cheap LAN tail the staged design accepts.
fn concat_parts(part_paths: Vec<PathBuf>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::try_stream! {
        for path in part_paths {
            let mut file = tokio::fs::File::open(&path).await?;
            let mut buf = vec![0u8; STREAM_CHUNK];
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                yield Bytes::copy_from_slice(&buf[..n]);
            }
        }
    }
}
