//! Concurrent whole-blob download via HTTP `Range` requests (206 slices),
//! kept for pre-chunking artifacts; chunked-v2 files go through
//! `chunk_fetcher` instead.
//!
//! Split (Task D2) into `plan` (the pure chunk-tiling math and the
//! status/`Content-Range` response validation), `download` (the
//! `ChunkedDownloader` orchestration and per-chunk task machinery), and
//! `verify` (whole-file digest resolution: incremental result preferred,
//! sequential re-read fallback). The re-export below keeps the pre-split
//! `crate::chunked_downloader::ChunkedDownloader` path stable.

mod download;
mod plan;
mod verify;

pub use download::ChunkedDownloader;

/// Loopback HTTP/1 206 Range server shared by the `download` (L13 refill) and
/// `verify` (incremental-verify) test modules - previously two duplicated inline
/// copies. `cfg(test)` so it exists only for the test build.
#[cfg(test)]
pub(crate) mod test_server {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Serve `body` over HTTP/1, answering each Range GET with the exact 206 slice
    /// (Content-Range echoed, `connection: close`). The Range header is matched
    /// case-insensitively: hyper writes HTTP/1 header names lowercase (`range:`),
    /// not `Range:`. Returns the base URL plus the accept-loop handle to abort.
    pub(crate) async fn serve_ranges(
        body: Vec<u8>,
    ) -> std::io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    // Read until the end of the request head (hyper may split it
                    // across segments).
                    let mut acc: Vec<u8> = Vec::new();
                    let mut buf = [0u8; 1024];
                    loop {
                        let Ok(n) = sock.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        acc.extend_from_slice(&buf[..n]);
                        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let req = String::from_utf8_lossy(&acc).to_ascii_lowercase();
                    // Echo exactly the requested inclusive byte range as a 206.
                    let Some(rng) = req.lines().find_map(|l| l.strip_prefix("range: bytes="))
                    else {
                        return;
                    };
                    let Some((s, e)) = rng.trim().split_once('-') else {
                        return;
                    };
                    let (Ok(start), Ok(end)) = (s.parse::<usize>(), e.parse::<usize>()) else {
                        return;
                    };
                    let end = end.min(body.len().saturating_sub(1));
                    if start > end {
                        return;
                    }
                    let slice = &body[start..=end];
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        start, end, body.len(), slice.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(slice).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Ok((format!("http://{addr}"), server))
    }
}
