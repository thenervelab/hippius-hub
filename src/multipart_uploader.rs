//! Parallel multipart blob upload to the in-cluster receiver.
//!
//! The single-blob upload ceiling is the one sequential HTTP stream a
//! monolithic OCI PUT forces (`uploader::upload_blob_async`). This module is
//! the client half of the "parallelize the WAN, serialize the LAN" design: it
//! splits one blob into contiguous byte ranges and PUTs them concurrently to a
//! receiver service, which reassembles them and streams a single native blob
//! PUT into Harbor on the fast LAN.
//!
//! Only the *transport* is parallel — the object stays one OCI blob with one
//! sha256 digest. Reassembly correctness does NOT depend on this module's part
//! math agreeing with anything global: the receiver orders parts by their
//! 1-based part number (the `Content-Range` header is advisory), validates that
//! each part's length matches the position it claims, and Harbor hashes the
//! reassembled bytes inline and rejects on digest mismatch. Harbor's inline
//! hash is the final backstop; the part math only has to cover `[0, size)`
//! contiguously with no gaps or overlaps — the property the proptest pins.

use std::io::SeekFrom;
use std::path::Path;
use std::time::Duration;

use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::error::CoreError;
use crate::uploader::upload_client;

/// Max part uploads in flight at once. Mirrors the design's `N = 16`. The
/// futures are polled concurrently by `buffer_unordered` (not `tokio::spawn`ed),
/// so dropping the stream on the first error cancels every in-flight part at
/// its next await point — the eager-spawn detachment hazard the downloader had
/// to guard against (audit D4) cannot arise here because nothing is detached.
const MAX_CONCURRENT_PARTS: usize = 16;

/// Upload retry attempts per part. Mirrors `uploader::UPLOAD_MAX_RETRIES` and
/// the downloader's `MAX_RETRIES`; all three share `CoreError::is_retryable`
/// as the single classifier so a transient 503/429 on one part is retried
/// with the same 200/400/800/1600 ms backoff shape.
const PART_MAX_RETRIES: u32 = 3;

/// Bounded re-drive of `complete` when the receiver reports missing parts. A
/// 409 lists the part numbers it never received (a WAN part that exhausted its
/// own retries); we re-PUT exactly those and re-complete. Bounded so a
/// receiver dropping parts faster than we resend surfaces as a terminal error
/// instead of an unbounded loop.
const COMPLETE_MAX_RETRIES: u32 = 3;

/// Number of parts needed to cover `size` bytes at `part_size` bytes each.
/// Returns 0 for an empty file or `part_size == 0` (defense-in-depth against a
/// degenerate receiver response — the caller fails fast on the latter). The
/// count lives in the 1-based OCI/S3-style part-number space, so it is `u32`.
fn num_parts(size: u64, part_size: u64) -> u32 {
    if size == 0 || part_size == 0 {
        return 0;
    }
    // Same ceiling division as `chunked_downloader::num_chunks`. `unwrap_or`
    // (NOT the denied `unwrap`) saturates the u64->u32 narrowing rather than
    // panicking; unreachable at our >=256 MB routing threshold, kept for
    // total-function safety.
    size.div_ceil(part_size).try_into().unwrap_or(u32::MAX)
}

/// Inclusive `(start, end)` byte range for 0-based part `index`. The last part
/// truncates at `size - 1`. Caller must ensure `index < num_parts(size,
/// part_size)` and `size > 0`.
fn part_bounds(size: u64, part_size: u64, index: u32) -> (u64, u64) {
    let start = u64::from(index) * part_size;
    let end = std::cmp::min(start + part_size - 1, size - 1);
    (start, end)
}

/// Body of the `initiate` POST — declares the blob the receiver is about to
/// broker so it can build the part plan and the Harbor finalize URL. `repo`
/// travels in the body (not the URL) because OCI repo paths contain `/`, which
/// cannot sit mid-path in the receiver's routes; the receiver stores it in the
/// session and the part/complete URLs are opaque `upload_id`-keyed.
#[derive(serde::Serialize)]
struct InitiateRequest<'a> {
    repo: &'a str,
    digest: &'a str,
    size: u64,
    part_size: u64,
}

/// Receiver's `initiate` reply. `part_size` is authoritative — the receiver
/// may clamp the client's request — so the client derives the part count from
/// it via `num_parts` rather than trusting a separate field the receiver could
/// compute inconsistently. `upload_id` scopes the part and complete URLs.
#[derive(serde::Deserialize)]
struct InitiateResponse {
    upload_id: String,
    part_size: u64,
}

/// Body of a 409 from `complete`: the 1-based part numbers the receiver never
/// got. The client re-PUTs exactly these before re-completing.
#[derive(serde::Deserialize)]
struct MissingParts {
    missing: Vec<u32>,
}

/// Immutable per-upload context threaded through the part tasks. Bundled so
/// the part helpers stay well under the positional-argument limit and so each
/// concurrently-polled future borrows one value instead of five.
struct PartPlan<'a> {
    upload_id: &'a str,
    size: u64,
    part_size: u64,
    path: &'a Path,
}

/// Client for the receiver-brokered multipart upload path.
pub(crate) struct MultipartUploader {
    base: String,
    repo: String,
    auth_token: Option<String>,
}

impl MultipartUploader {
    pub(crate) fn new(base: String, repo: String, auth_token: Option<String>) -> Self {
        Self { base, repo, auth_token }
    }

    /// Upload `path` (whose content hashes to `digest`, `size` bytes) via the
    /// receiver: `initiate` -> concurrent part PUTs -> `complete`.
    pub(crate) async fn upload(
        &self,
        digest: &str,
        size: u64,
        path: &Path,
        part_size: u64,
    ) -> Result<(), CoreError> {
        let init = self.initiate(digest, size, part_size).await?;
        if init.part_size == 0 {
            // A zero part_size would make `part_bounds` divide the file into an
            // infinite/degenerate plan; refuse rather than loop.
            return Err(CoreError::ServerError(502, "receiver returned part_size=0".into()));
        }
        let plan = PartPlan { upload_id: &init.upload_id, size, part_size: init.part_size, path };
        // Derive the part count from the authoritative part_size rather than
        // trusting a receiver-supplied field — `part_bounds` and this count
        // must agree for reassembly, so they share one source of truth.
        let total_parts = num_parts(size, init.part_size);
        let pb = new_progress_bar(size, path);
        self.upload_all_parts(&plan, total_parts, &pb).await?;
        self.complete(&plan, &pb).await?;
        pb.finish_with_message("✅ uploaded (multipart)");
        Ok(())
    }

    async fn initiate(&self, digest: &str, size: u64, part_size: u64) -> Result<InitiateResponse, CoreError> {
        let client = upload_client()?;
        let url = format!("{}/v2/blobs/uploads/multipart", self.base);
        let mut req = client
            .post(&url)
            .json(&InitiateRequest { repo: &self.repo, digest, size, part_size });
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token);
        }
        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(CoreError::ServerError(res.status().as_u16(), "multipart initiate failed".into()));
        }
        Ok(res.json().await?)
    }

    /// Fan out every part concurrently. `buffer_unordered` polls up to
    /// `MAX_CONCURRENT_PARTS` at once; the first `Err` propagates via `?`,
    /// which drops the stream and cancels the still-in-flight part futures.
    async fn upload_all_parts(&self, plan: &PartPlan<'_>, num_parts: u32, pb: &ProgressBar) -> Result<(), CoreError> {
        let mut stream = futures::stream::iter(0..num_parts)
            .map(|index| self.upload_part_with_retry(plan, index, pb))
            .buffer_unordered(MAX_CONCURRENT_PARTS);
        while let Some(res) = stream.next().await {
            res?;
        }
        Ok(())
    }

    async fn upload_part_with_retry(&self, plan: &PartPlan<'_>, index: u32, pb: &ProgressBar) -> Result<(), CoreError> {
        let (start, end) = part_bounds(plan.size, plan.part_size, index);
        let mut retries = 0u32;
        loop {
            match self.try_upload_part_once(plan, index, start, end).await {
                Ok(()) => {
                    pb.inc(end - start + 1);
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    // Same classify-then-back-off shape as the downloader: give
                    // up immediately on permanent errors, and after the budget
                    // on transient ones.
                    if !e.is_retryable() || retries > PART_MAX_RETRIES {
                        return Err(e);
                    }
                    let wait_ms = 2u64.pow(retries) * 100;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                }
            }
        }
    }

    async fn try_upload_part_once(&self, plan: &PartPlan<'_>, index: u32, start: u64, end: u64) -> Result<(), CoreError> {
        let client = upload_client()?;
        let len = end - start + 1;
        // Each attempt opens its own handle and streams exactly `len` bytes from
        // `start` via `take` — no Content-Length header, so reqwest uses
        // Transfer-Encoding: chunked and the wire length matches whatever the
        // stream yields (the same TOCTOU-avoidance rationale as `uploader.rs`).
        // The receiver validates the arriving byte count against the length its
        // part number implies, and Harbor's inline digest is the final backstop.
        let mut file = File::open(plan.path).await?;
        file.seek(SeekFrom::Start(start)).await?;
        let body = reqwest::Body::wrap_stream(FramedRead::new(file.take(len), BytesCodec::new()));

        let part_number = index + 1; // OCI/S3 part numbers are 1-based.
        let url = format!(
            "{}/v2/blobs/uploads/multipart/{}/parts/{}",
            self.base, plan.upload_id, part_number
        );
        let mut req = client
            .put(&url)
            .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{}", plan.size))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body);
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token);
        }
        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(CoreError::ServerError(res.status().as_u16(), format!("part {part_number} upload failed")));
        }
        Ok(())
    }

    /// Ask the receiver to finalize. On 409 (parts missing after their own
    /// retries) re-PUT exactly the reported parts and re-complete, bounded by
    /// `COMPLETE_MAX_RETRIES`.
    async fn complete(&self, plan: &PartPlan<'_>, pb: &ProgressBar) -> Result<(), CoreError> {
        let client = upload_client()?;
        let url = format!(
            "{}/v2/blobs/uploads/multipart/{}/complete",
            self.base, plan.upload_id
        );
        let mut attempt = 0u32;
        loop {
            let mut req = client.post(&url);
            if let Some(token) = &self.auth_token {
                req = req.bearer_auth(token);
            }
            let res = req.send().await?;
            let status = res.status();
            if status.is_success() {
                return Ok(());
            }
            if status.as_u16() != 409 {
                return Err(CoreError::ServerError(status.as_u16(), "multipart complete failed".into()));
            }
            attempt += 1;
            if attempt > COMPLETE_MAX_RETRIES {
                return Err(CoreError::ServerError(409, "receiver still missing parts after re-upload".into()));
            }
            let missing: MissingParts = res.json().await?;
            // The wire carries 1-based part numbers. Bound each against the real
            // part count before converting to a 0-based index: an out-of-range
            // value from a buggy/hostile receiver would otherwise drive
            // `part_bounds` past EOF and underflow `end - start + 1`.
            let total = num_parts(plan.size, plan.part_size);
            for part_number in missing.missing {
                if part_number == 0 || part_number > total {
                    return Err(CoreError::ServerError(
                        502,
                        format!("receiver reported out-of-range part {part_number} (of {total})"),
                    ));
                }
                self.upload_part_with_retry(plan, part_number - 1, pb).await?;
            }
        }
    }
}

fn new_progress_bar(size: u64, path: &Path) -> ProgressBar {
    let pb = ProgressBar::new(size);
    // Same static-literal template as the up/down paths; indicatif only errors
    // on malformed directives, which we control at the call site.
    #[expect(clippy::expect_used, reason = "infallible static template")]
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .expect("indicatif template is static and infallible")
            .progress_chars("#>-"),
    );
    let name = path.file_name().map_or_else(|| "blob".to_string(), |n| n.to_string_lossy().into_owned());
    pb.set_message(format!("📤 {name} (multipart)"));
    pb
}

#[cfg(test)]
mod tests {
    use super::{num_parts, part_bounds};

    #[test]
    fn num_parts_boundaries() {
        assert_eq!(num_parts(0, 64), 0, "empty file has no parts");
        assert_eq!(num_parts(1, 64), 1);
        assert_eq!(num_parts(64, 64), 1, "exact multiple is one part");
        assert_eq!(num_parts(65, 64), 2, "one byte over spills to a second part");
        assert_eq!(num_parts(1000, 0), 0, "zero part_size does not divide-by-zero");
    }

    #[test]
    fn part_bounds_last_part_truncates_at_eof() {
        // 1024 bytes, 1000-byte parts -> part 0 is 0..=999, part 1 is 1000..=1023.
        assert_eq!(part_bounds(1024, 1000, 0), (0, 999));
        assert_eq!(part_bounds(1024, 1000, 1), (1000, 1023));
    }

    #[test]
    fn part_bounds_exact_multiple_fills_last() {
        assert_eq!(part_bounds(300, 100, 2), (200, 299));
    }

    // The only invariant reassembly depends on: the parts tile `[0, size)`
    // exactly — contiguous, non-overlapping, spanning the whole file. If this
    // holds, the receiver's in-order concatenation reproduces the original
    // bytes and Harbor's digest matches. The shrinker surfaces boundary sizes
    // the fixtures above didn't enumerate.
    proptest::proptest! {
        #[test]
        fn parts_tile_the_file_exactly(
            size in 1u64..1_000_000_000,
            part_size in 1u64..200_000_000,
        ) {
            let n = num_parts(size, part_size);
            proptest::prop_assert!(n > 0, "non-empty file must have >=1 part");
            let mut expected_start = 0u64;
            let mut total = 0u64;
            for i in 0..n {
                let (s, e) = part_bounds(size, part_size, i);
                proptest::prop_assert_eq!(s, expected_start, "gap or overlap before part {}", i);
                proptest::prop_assert!(s <= e, "part {} has start > end", i);
                total += e - s + 1;
                expected_start = e + 1;
            }
            proptest::prop_assert_eq!(total, size, "parts must sum to the file size");
            let (_, last_end) = part_bounds(size, part_size, n - 1);
            proptest::prop_assert_eq!(last_end, size - 1, "last part must end at EOF");
        }
    }
}

// Orchestration tests kept separate from the pure-math module above: these
// drive the real reqwest initiate/parts/complete flow against a wiremock
// receiver, covering the wire behavior (part fan-out, the 409 re-put loop)
// that the part math alone cannot. `MockServer` speaks plain HTTP/1.1, which
// the shared `upload_client` (http1_only, rustls only kicks in for https) talks
// to directly.
#[cfg(test)]
mod integration_tests {
    use super::MultipartUploader;
    use std::path::PathBuf;

    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Write `size` bytes to a unique temp file and return its path; the caller
    // removes it. A single fixture file does not justify a `tempfile` dev-dep.
    // `unwrap`/`expect` are denied crate-wide, so failures destructure via
    // `let ... else { unreachable! }` like the sibling downloader tests.
    fn write_temp(size: usize, tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hippius_mp_{}_{tag}", std::process::id()));
        let Ok(()) = std::fs::write(&path, vec![0u8; size]) else {
            unreachable!("temp fixture write must succeed in the test environment")
        };
        path
    }

    fn initiate_ok(part_size: u64) -> Mock {
        // Opaque path — repo travels in the request body, not the URL.
        Mock::given(method("POST"))
            .and(path("/v2/blobs/uploads/multipart"))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_body_json(serde_json::json!({"upload_id": "u1", "part_size": part_size})),
            )
    }

    fn part_put_ok() -> Mock {
        Mock::given(method("PUT"))
            .and(path_regex(r"/parts/\d+$"))
            .respond_with(ResponseTemplate::new(204))
    }

    fn count_puts_to(requests: &[wiremock::Request], suffix: &str) -> usize {
        requests
            .iter()
            .filter(|r| r.method.as_str() == "PUT" && r.url.path().ends_with(suffix))
            .count()
    }

    // 10 bytes at part_size 4 -> parts (0..=3),(4..=7),(8..=9): three PUTs.
    #[tokio::test]
    async fn happy_path_initiate_parts_complete() {
        let server = MockServer::start().await;
        let repo = "proj/model";
        initiate_ok(4).mount(&server).await;
        part_put_ok().mount(&server).await;
        Mock::given(method("POST"))
            .and(path_regex(r"/complete$"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let file = write_temp(10, "happy");
        let uploader = MultipartUploader::new(server.uri(), repo.into(), None);
        let res = uploader.upload("sha256:abc", 10, &file, 4).await;
        let _ = std::fs::remove_file(&file);

        assert!(res.is_ok(), "happy path must succeed: {res:?}");
        let Some(requests) = server.received_requests().await else {
            unreachable!("wiremock records requests by default")
        };
        let puts = requests.iter().filter(|r| r.method.as_str() == "PUT").count();
        assert_eq!(puts, 3, "one PUT per part for 10 bytes at part_size 4");
    }

    // First `complete` reports part 2 missing; the client must re-PUT exactly
    // part 2 and re-complete. Success proves the 409 loop ran; the request
    // count proves it targeted the right part.
    #[tokio::test]
    async fn complete_409_reputs_missing_part() {
        let server = MockServer::start().await;
        let repo = "proj/model";
        initiate_ok(4).mount(&server).await;
        part_put_ok().mount(&server).await;
        Mock::given(method("POST"))
            .and(path_regex(r"/complete$"))
            .respond_with(
                ResponseTemplate::new(409).set_body_json(serde_json::json!({"missing": [2]})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"/complete$"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let file = write_temp(10, "reput");
        let uploader = MultipartUploader::new(server.uri(), repo.into(), None);
        let res = uploader.upload("sha256:abc", 10, &file, 4).await;
        let _ = std::fs::remove_file(&file);

        assert!(res.is_ok(), "409-then-complete must succeed: {res:?}");
        let Some(requests) = server.received_requests().await else {
            unreachable!("wiremock records requests by default")
        };
        assert!(
            count_puts_to(&requests, "/parts/2") >= 2,
            "part 2 must be re-PUT after the 409"
        );
    }

    // A permanent 4xx on a part is not retried and fails the whole upload.
    #[tokio::test]
    async fn permanent_part_error_fails_fast() {
        let server = MockServer::start().await;
        let repo = "proj/model";
        initiate_ok(4).mount(&server).await;
        Mock::given(method("PUT"))
            .and(path_regex(r"/parts/\d+$"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let file = write_temp(10, "perm");
        let uploader = MultipartUploader::new(server.uri(), repo.into(), None);
        let res = uploader.upload("sha256:abc", 10, &file, 4).await;
        let _ = std::fs::remove_file(&file);

        let Err(err) = res else {
            unreachable!("a 401 on a part must fail the upload")
        };
        assert!(!err.is_retryable(), "surfaced error should be the terminal 401");
    }
}
