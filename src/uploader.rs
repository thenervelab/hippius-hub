use bytes::Bytes;
use fastcdc::v2020::StreamCDC;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, header};
use sha2::{Digest, Sha256};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::error::CoreError;

/// Per-chunk `(sha256_hex, offset, length)` in file order — the plan a chunked
/// upload works from (offset re-reads the range; digest dedups and addresses).
pub type ChunkList = Vec<(String, u64, u64)>;

/// `FastCDC` average chunk size bounds — pinned to the library's own average
/// range `[AVERAGE_MIN, AVERAGE_MAX]` = `[256 B, 4 MiB]`. The average is the wire
/// contract (see the chunked-artifact plan); min = avg/4 and max = avg*4 are the
/// standard normalized-chunking ratios. Those derivations must ALSO stay under
/// `FastCDC`'s *separate* `MINIMUM_MAX` (1 MiB) and `MAXIMUM_MAX` (16 MiB) ceilings
/// or `StreamCDC::new` panics — and they do so exactly over this interval: at the
/// 4 MiB ceiling min = 1 MiB = `MINIMUM_MAX` and max = 16 MiB = `MAXIMUM_MAX`, the
/// caps themselves. So `[256 B, 4 MiB]` is the largest average range that can
/// never panic. The old 256 MiB cap let averages like the 64 MiB default through
/// to a `StreamCDC::new` panic (min = 16 MiB > `MINIMUM_MAX`); an out-of-range
/// average is now surfaced as a caller error, never clamped and never panicked.
/// (`cdc_bounds_track_fastcdc_limits` asserts these equal the crate constants so a
/// `FastCDC` bump can't silently reopen the panic.)
const CDC_MIN_AVG: u64 = 256; // == fastcdc::v2020::AVERAGE_MIN
const CDC_MAX_AVG: u64 = 4 * 1024 * 1024; // == fastcdc::v2020::AVERAGE_MAX (4 MiB)

/// Chunk a file with `FastCDC` and hash each chunk plus the whole file in one
/// streaming pass (bounded memory — `StreamCDC` never loads the whole file).
///
/// Returns `(whole_file_sha256_hex, [(chunk_sha256_hex, offset, length)])` in
/// file order. The offsets let the caller re-read each chunk's byte range for a
/// parallel upload; the digests drive `HEAD`-dedup and content-addressing.
/// Determinism: for a fixed `avg_size` the boundaries are a pure function of the
/// bytes, so identical files chunk identically and dedup — hence `avg_size` is
/// pinned by the caller, not tuned per upload.
pub fn chunk_and_hash(path: &Path, avg_size: u64) -> Result<(String, ChunkList), CoreError> {
    chunk_and_hash_reader(std::fs::File::open(path)?, avg_size)
}

/// Reader-based core of [`chunk_and_hash`], split out so tests can drive it from
/// an in-memory `Cursor` (no temp file, no I/O `unwrap`). Semantics are
/// identical: `StreamCDC` yields the same boundaries whether the source is a
/// file or a cursor over the same bytes.
fn chunk_and_hash_reader<R: std::io::Read>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    if !(CDC_MIN_AVG..=CDC_MAX_AVG).contains(&avg_size) {
        return Err(CoreError::Integrity(format!(
            "FastCDC average size {avg_size} out of range [{CDC_MIN_AVG}, {CDC_MAX_AVG}]"
        )));
    }
    // The range check above guarantees min/avg/max fit u32; try_from keeps that
    // provable to clippy without an unchecked `as` cast.
    let to_u32 = |v: u64| -> Result<u32, CoreError> {
        u32::try_from(v).map_err(|_| CoreError::Integrity(format!("chunk size {v} exceeds u32")))
    };
    let (min, max) = (to_u32(avg_size / 4)?, to_u32(avg_size * 4)?);
    let avg = to_u32(avg_size)?;

    let chunker = StreamCDC::new(source, min, avg, max);

    let mut whole = Sha256::new();
    let mut chunks: ChunkList = Vec::new();
    for result in chunker {
        let cd = result.map_err(|e| CoreError::Io(std::io::Error::other(e)))?;
        whole.update(&cd.data);
        let chunk_hex = hex::encode(Sha256::digest(&cd.data));
        chunks.push((chunk_hex, cd.offset, cd.length as u64));
    }
    Ok((hex::encode(whole.finalize()), chunks))
}

// Phase 3.8 (audit U4): the local UploadError was folded into the
// crate-wide `CoreError`. The single thiserror-derived enum carries
// `reqwest::Error` and `std::io::Error` via `#[from]`, preserving the
// `?` ergonomics and the `Error::source()` chain through the Python
// boundary in `lib.rs::core_err_to_py`.

/// Compute the SHA256 and total size of a local file.
///
/// Audit U1: the digest loop is CPU-bound and the I/O is unbuffered file
/// reads — neither benefits from running on a tokio worker thread, and the
/// combination starves other futures on the same runtime for seconds on
/// multi-GB blobs. We route the whole pass through `spawn_blocking` so the
/// runtime keeps its worker threads free for actual async work (HTTP, other
/// downloads). `std::fs::File` + `std::io::Read` are the right primitives
/// inside the blocking pool; the async wrappers would only re-block the same
/// thread.
///
/// The double `?` at the end is load-bearing: `spawn_blocking(...).await`
/// produces `Result<Result<T, CoreError>, JoinError>`. We collapse the
/// outer `JoinError` (panic in the closure / runtime shutdown) into our
/// `CoreError::Io` variant via `io::Error::other` so callers see one
/// error surface, then `?` unwraps the inner Result.
pub async fn hash_file_async(path: &Path) -> Result<(String, u64), CoreError> {
    use std::io::Read;

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(String, u64), CoreError> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 64 * 1024]; // 64 KB chunks
        let mut total_size = 0u64;

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
            total_size += bytes_read as u64;
        }

        Ok((hex::encode(hasher.finalize()), total_size))
    })
    .await
    .map_err(|join_err| CoreError::Io(std::io::Error::other(join_err)))?
}

/// Mirror of [`crate::chunked_downloader::MAX_RETRIES`] for the upload
/// path. Audit U3 (Phase 3.11): the downloader retried per-chunk up to
/// 3 times; the uploader did not retry at all, so a single transient
/// 503 lost the whole upload. The two paths now share the same budget
/// and the same [`CoreError::is_retryable`] classifier — see
/// `try_upload_blob_once` for the per-attempt body.
const UPLOAD_MAX_RETRIES: u32 = 3;

/// Stream-upload a file to the OCI URL returned by /blobs/uploads/ (the PUT-with-digest finalises the blob).
/// Shows a per-call progress bar — useful for large blobs (multi-GB).
///
/// Audit U3 (Phase 3.11): wraps [`try_upload_blob_once`] in an
/// exponential-backoff retry loop with the same shape as
/// [`crate::chunked_downloader::download_chunk_with_retry`]. Each
/// attempt re-opens the file inside `try_upload_blob_once` (the
/// previous `FramedRead` stream has been consumed), so the retry sees a
/// fresh handle. Backoff schedule: 200, 400, 800, 1600 ms — four
/// attempts total, ~3 s of backoff before surfacing a transient 5xx as
/// terminal. A 4xx never burns backoff.
pub async fn upload_blob_async(
    url: &str,
    path: &Path,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let mut retries: u32 = 0;
    loop {
        match try_upload_blob_once(url, path, auth_token).await {
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
                // `2u64.pow(retries) * 100` reproduces the downloader's
                // 200/400/800/1600 ms schedule. `retries` is `u32` to
                // match `UPLOAD_MAX_RETRIES`; `pow` widens to `u64` so
                // the multiplication cannot overflow at this budget.
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

/// Single upload attempt. Extracted from `upload_blob_async` in audit
/// U3 (Phase 3.11) so the surrounding retry loop has a unit to call
/// repeatedly. Each call opens its own `File` handle, builds its own
/// `FramedRead` stream, and sends one PUT — so the retry loop above
/// gets a fresh body on every attempt (the previous `Body::wrap_stream`
/// is consumed once the request future completes or errors).
/// Process-global HTTP client for blob uploads.
///
/// Mirrors the downloader, which builds its `reqwest::Client` once in
/// `ChunkedDownloader::new` and reuses it across all chunks. Previously
/// `try_upload_blob_once` rebuilt a client on every call — once per blob and
/// once per retry — discarding the keep-alive connection pool and forcing a
/// fresh DNS+TCP+TLS handshake against the registry host the previous blob just
/// finished using (audit N-4 / RUST-3). The `OnceLock` hoists construction out
/// of the per-attempt path so warm connections survive between blobs.
///
/// Construction is fallible (`build()` errors if the TLS backend won't
/// initialize), so this returns `Result` rather than `expect`-ing inside a
/// `get_or_init` closure — the crate denies `panic`/`unwrap` and warns on
/// `expect`. On the rare init race the losing thread's freshly built client is
/// dropped unused (RAII); after first init `get()` returns the shared client
/// immediately. `OnceLock` is valid in statics and never poisoned on panic
/// (doc.rust-lang.org/std/sync/struct.OnceLock.html).
pub(crate) fn upload_client() -> Result<&'static Client, CoreError> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // Force HTTP/1.1 for the same reason as the downloader: avoids h2 single-TCP
    // multiplexing, lets uploads spread across multiple connections if the caller
    // parallelizes.
    let built = Client::builder()
        .timeout(Duration::from_hours(1)) // 1h timeout for very large uploads
        .http1_only()
        .tcp_keepalive(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .build()?;
    Ok(CLIENT.get_or_init(|| built))
}

/// Stream `reader` to `url` as a chunked-encoded PUT body, ticking a progress
/// bar sized `pb_total`. Shared by the whole-file and byte-range upload paths.
///
/// No explicit Content-Length: reqwest falls back to Transfer-Encoding: chunked
/// for a `wrap_stream` body, so the wire length matches whatever the reader
/// actually yields — the TOCTOU-safe behaviour audit U2 established for the
/// whole-file path. For a range upload the reader is a `Take` bounded to the
/// chunk length, so the body is exactly that range regardless.
async fn put_streaming<R>(
    url: &str,
    reader: R,
    pb_total: u64,
    basename: &str,
    auth_token: Option<&str>,
) -> Result<(), CoreError>
where
    R: tokio::io::AsyncRead + Send + 'static,
{
    let client = upload_client()?;

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
    pb.set_message(format!("📤 {basename}"));

    // Wrap the stream so we tick the progress bar on every body chunk emitted
    // to reqwest. ProgressBar is Arc-internally → cloning is cheap.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(reader, BytesCodec::new()).map(move |chunk_result| {
        if let Ok(ref bytes) = chunk_result {
            pb_stream.inc(bytes.len() as u64);
        }
        chunk_result
    });
    let body = reqwest::Body::wrap_stream(stream);

    let mut req = client
        .put(url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }

    let res = req.send().await?;
    if !res.status().is_success() {
        pb.finish_with_message(format!("❌ {basename} failed"));
        return Err(CoreError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }
    pb.finish_with_message(format!("✅ {basename} uploaded"));
    Ok(())
}

fn basename_of(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| "blob".to_string(), |n| n.to_string_lossy().into_owned())
}

async fn try_upload_blob_once(
    url: &str,
    path: &Path,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let file = File::open(path).await?;
    // Size is for the progress bar only — see put_streaming on why it is not a
    // Content-Length.
    let file_size = file.metadata().await?.len();
    put_streaming(url, file, file_size, &basename_of(path), auth_token).await
}

/// Upload exactly `length` bytes starting at `offset` of `path` as one OCI blob.
///
/// This is the chunked-artifact upload primitive: the file is chunked once (by
/// `chunk_and_hash`) and each chunk's byte range is pushed as its own
/// content-addressed blob, in parallel across chunks. Retries share the
/// downloader/uploader classifier via [`CoreError::is_retryable`]; each attempt
/// re-opens and re-seeks so the body is fresh (the previous `wrap_stream` was
/// consumed).
pub async fn upload_blob_range_async(
    url: &str,
    path: &Path,
    offset: u64,
    length: u64,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let mut retries: u32 = 0;
    loop {
        match try_upload_range_once(url, path, offset, length, auth_token).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                if !e.is_retryable() || retries > UPLOAD_MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

async fn try_upload_range_once(
    url: &str,
    path: &Path,
    offset: u64,
    length: u64,
    auth_token: Option<&str>,
) -> Result<(), CoreError> {
    let mut file = File::open(path).await?;
    file.seek(SeekFrom::Start(offset)).await?;
    // `take(length)` bounds the body to exactly this chunk's bytes, so the
    // chunked-encoded PUT sends the range and nothing past it.
    let reader = file.take(length);
    put_streaming(url, reader, length, &basename_of(path), auth_token).await
}

/// Read the given file byte-ranges in order into one pack blob and push it via a
/// fresh OCI upload session (POST init + monolithic PUT-with-digest). Returns the
/// pack's sha256 hex — the chunked-v2 caller records it in the pointer blob.
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
    // in-flight pack costs one pack_size instead of two — `read_ranges`' `Vec`
    // converts in without reallocating. The prior `.body(buf.to_vec())` re-copied
    // the whole pack on each attempt, which the staging peak-RSS benchmark showed
    // roughly doubled resident memory per concurrent upload.
    let body = Bytes::from(read_ranges(path, ranges).await?);
    let digest_hex = hex::encode(Sha256::digest(&body));
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
                tokio::time::sleep(Duration::from_millis(2u64.pow(retries) * 100)).await;
            }
        }
    }
}

async fn read_ranges(path: &Path, ranges: &[(u64, u64)]) -> Result<Vec<u8>, CoreError> {
    let mut file = File::open(path).await?;
    let total: u64 = ranges.iter().map(|(_off, len)| *len).sum();
    let cap = usize::try_from(total)
        .map_err(|_| CoreError::Integrity(format!("pack size {total} exceeds usize")))?;
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
    let client = upload_client()?;
    let mut init = client.post(uploads_url).header(header::CONTENT_LENGTH, "0");
    if let Some(token) = auth_token {
        init = init.bearer_auth(token);
    }
    let init_resp = init.send().await?;
    if !init_resp.status().is_success() {
        return Err(CoreError::ServerError(
            init_resp.status().as_u16(),
            "pack upload init failed".to_string(),
        ));
    }
    let location = init_resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CoreError::Integrity("registry omitted Location on upload init".to_string()))?;
    // Resolve a possibly-relative Location against the uploads URL, then append the
    // digest as a RAW query pair (":" is legal unencoded in a query; percent-
    // encoding it via query_pairs_mut breaks the registry's digest match).
    let resolved = reqwest::Url::parse(uploads_url)
        .and_then(|base| base.join(location))
        .map_err(|e| CoreError::Integrity(format!("bad upload Location {location:?}: {e}")))?;
    let mut put_url = resolved.to_string();
    put_url.push(if put_url.contains('?') { '&' } else { '?' });
    put_url.push_str("digest=");
    put_url.push_str(digest);
    let mut put = client
        .put(put_url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body.clone());
    if let Some(token) = auth_token {
        put = put.bearer_auth(token);
    }
    let put_resp = put.send().await?;
    if !put_resp.status().is_success() {
        return Err(CoreError::ServerError(
            put_resp.status().as_u16(),
            format!("pack PUT failed: {:?}", put_resp.status()),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod cdc_tests {
    use super::{CDC_MAX_AVG, CDC_MIN_AVG, chunk_and_hash_reader};
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    const AVG: u64 = 512; // → min 128, max 2048; small enough for fast tests

    fn chunk(data: &[u8]) -> (String, super::ChunkList) {
        match chunk_and_hash_reader(Cursor::new(data), AVG) {
            Ok(v) => v,
            Err(_) => unreachable!("chunking valid bytes with a valid avg cannot fail"),
        }
    }

    #[test]
    fn read_ranges_concatenates_in_order() {
        use super::read_ranges;
        use crate::error::CoreError;
        use std::io::Write;

        let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
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
    fn out_of_range_avg_is_rejected() {
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), CDC_MIN_AVG - 1).is_err());
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), CDC_MAX_AVG + 1).is_err());
        // The shipped 64 MiB default used to reach StreamCDC::new and PANIC
        // (min = avg/4 = 16 MiB > fastcdc MINIMUM_MAX). It must now be a clean
        // caller error caught before the splitter — the exact value the staging
        // benchmark tripped.
        assert!(chunk_and_hash_reader(Cursor::new(b"x"), 64 * 1024 * 1024).is_err());
    }

    #[test]
    fn cdc_bounds_track_fastcdc_limits() {
        use fastcdc::v2020::{AVERAGE_MAX, AVERAGE_MIN};
        // Our accepted range MUST equal fastcdc's own average bounds: only across
        // [AVERAGE_MIN, AVERAGE_MAX] do the derived min = avg/4 and max = avg*4 stay
        // within fastcdc's MINIMUM_MAX/MAXIMUM_MAX, so StreamCDC::new cannot panic.
        // If a fastcdc bump moves these, fail here rather than ship another panic.
        assert_eq!(CDC_MIN_AVG, u64::from(AVERAGE_MIN));
        assert_eq!(CDC_MAX_AVG, u64::from(AVERAGE_MAX));
    }

    #[test]
    fn chunks_at_the_ceiling_avg_without_panic() {
        // The upper bound is INCLUSIVE and valid: at avg = 4 MiB, fastcdc's derived
        // min = 1 MiB and max = 16 MiB are its exact ceilings — this must chunk, not
        // panic and not be rejected. A buffer past the 16 MiB max forces >1 chunk.
        let data = vec![9u8; 20 * 1024 * 1024];
        match chunk_and_hash_reader(Cursor::new(&data), CDC_MAX_AVG) {
            Ok((_, chunks)) => assert!(chunks.len() >= 2),
            Err(_) => unreachable!("chunking at the ceiling avg must succeed"),
        }
    }

    #[test]
    fn whole_file_digest_matches_reference() {
        let data = vec![7u8; 5000];
        let (whole, _) = chunk(&data);
        assert_eq!(whole, hex::encode(Sha256::digest(&data)));
    }

    #[test]
    fn boundaries_reshuffle_only_locally_on_a_late_edit() {
        // Determinism + shift-locality (the CDC payoff): inserting a byte near
        // the END must leave the FIRST chunk's digest unchanged — content-defined
        // boundaries re-sync, so unchanged early regions still dedup.
        let mut data = vec![0u8; 8000];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i * 31 % 251).unwrap_or(0); // deterministic, non-degenerate
        }
        let (_, before) = chunk(&data);

        let mut edited = data.clone();
        edited.insert(7000, 0xFF); // late insert shifts only the tail
        let (_, after) = chunk(&edited);

        assert!(
            before.len() > 1 && after.len() > 1,
            "need multiple chunks to test locality"
        );
        assert_eq!(
            before[0].0, after[0].0,
            "first chunk digest must survive a late edit"
        );
    }

    proptest::proptest! {
        // Partition + determinism over arbitrary byte vectors. `Cursor` drives
        // the reader core directly so each case is pure CPU (no temp file).
        #[test]
        fn cdc_partitions_and_is_deterministic(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..20_000usize),
        ) {
            let (whole, chunks) = chunk(&data);

            // Contiguous, gapless offsets summing to the file length.
            let mut expected_offset = 0u64;
            for (_, off, len) in &chunks {
                proptest::prop_assert_eq!(*off, expected_offset);
                expected_offset += *len;
            }
            proptest::prop_assert_eq!(expected_offset, data.len() as u64);

            // Whole-file digest is the reference sha256 of exactly these bytes.
            proptest::prop_assert_eq!(&whole, &hex::encode(Sha256::digest(&data)));

            // Determinism: same bytes → identical chunk boundaries + digests.
            let (whole2, chunks2) = chunk(&data);
            proptest::prop_assert_eq!(whole, whole2);
            proptest::prop_assert_eq!(chunks, chunks2);
        }

        // Size bounds: every chunk is <= max, and every chunk except the last is
        // >= min (FastCDC only lets the final chunk fall below the minimum).
        #[test]
        fn cdc_respects_size_bounds(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..20_000usize),
        ) {
            let (min, max) = (AVG / 4, AVG * 4);
            let (_, chunks) = chunk(&data);
            for (i, (_, _, len)) in chunks.iter().enumerate() {
                proptest::prop_assert!(*len <= max, "chunk {} len {} exceeds max {}", i, len, max);
                if i + 1 < chunks.len() {
                    proptest::prop_assert!(*len >= min, "non-final chunk {} len {} below min {}", i, len, min);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CoreError;

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
        let src = include_str!("uploader.rs");
        // Count must be exactly the references in *this* test's comments
        // describing what is forbidden — i.e. zero matches of the assembled
        // needle, since we never write it as a contiguous token anywhere.
        assert!(
            !src.contains(&needle),
            "uploader.rs must NOT set the Content-Length header on the streaming PUT \
             — that creates a TOCTOU race vs the file's actual size at stream time"
        );
    }

    // Audit U3 (Phase 3.11): pin the retry classification at the
    // upload-loop entry point. The downloader has the exhaustive 4xx /
    // 5xx / boundary suite in
    // `chunked_downloader::retry_classification_tests`; these two tests
    // pin the property the upload loop depends on without re-litigating
    // the downloader's coverage — the classifier is a method on
    // `CoreError`, so the two paths share one source of truth.

    #[test]
    fn upload_retry_skips_4xx() {
        // Verify that an HTTP 401 returned from the server is NOT retried —
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

    // RUST-3 (audit N-4): the upload client is a process-global singleton,
    // built once and reused across blobs/retries rather than rebuilt per
    // attempt. Two calls must hand back the SAME `&'static Client` (pointer
    // equality) — the same invariant `lib.rs::runtime_tests` pins for the
    // shared runtime. `unwrap`/`expect`/`panic!` are denied crate-wide, so we
    // assert via `is_ok` + `if let` instead of unwrapping the Result.
    #[test]
    fn upload_client_returns_same_instance() {
        let a = super::upload_client();
        let b = super::upload_client();
        assert!(a.is_ok() && b.is_ok(), "upload client must build");
        if let (Ok(a), Ok(b)) = (a, b) {
            assert!(
                std::ptr::eq(a, b),
                "upload_client must return one shared instance, not a fresh client per call"
            );
        }
    }
}
