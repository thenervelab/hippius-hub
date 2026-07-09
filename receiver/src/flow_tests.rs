//! In-process flow tests for the receiver: drive the axum router with
//! `oneshot` (no network bind) through initiate -> parts -> complete, with a
//! wiremock Harbor standing in for the LAN leg and a tempdir for scratch. This
//! is the receiver-side analog of the client's orchestration tests — it covers
//! the wire behavior (session lifecycle, the 409-missing path, the Harbor push)
//! that the pure `plan` unit tests cannot.
//!
//! `unwrap`/`expect` are denied crate-wide, so fallible test setup destructures
//! with `let ... else { unreachable! }`, matching the main crate's test style.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use dashmap::DashMap;
use tower::ServiceExt;
use wiremock::matchers::{header, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::state::{AppState, Config};

fn build_request(method: &str, uri: &str, headers: &[(&str, &str)], body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    match builder.body(body) {
        Ok(req) => req,
        Err(e) => unreachable!("static test request must build: {e}"),
    }
}

/// Drive the router once. `Router`'s `Service::Error` is `Infallible`, so the
/// `Err` arm is uninhabited — `match e {}` discharges it without a panic and
/// without an irrefutable `let ... else`.
async fn call(app: &axum::Router, req: Request<Body>) -> axum::response::Response {
    match app.clone().oneshot(req).await {
        Ok(resp) => resp,
        Err(e) => match e {},
    }
}

async fn read_json(resp: axum::response::Response) -> serde_json::Value {
    let Ok(bytes) = axum::body::to_bytes(resp.into_body(), usize::MAX).await else {
        unreachable!("response body must be readable")
    };
    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
}

fn test_state(harbor_base: String, scratch: std::path::PathBuf) -> AppState {
    state_with(harbor_base, scratch, 1024)
}

fn state_with(harbor_base: String, scratch: std::path::PathBuf, max_sessions: usize) -> AppState {
    AppState {
        config: Arc::new(Config {
            harbor_base,
            scratch_dir: scratch,
            min_part_size: 1, // let a tiny fixture split into real parts
            max_part_size: 1024 * 1024,
            session_ttl: Duration::from_hours(1),
            max_sessions,
        }),
        sessions: Arc::new(DashMap::new()),
        http: reqwest::Client::builder()
            .http1_only()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new()),
    }
}

async fn initiate(app: &axum::Router, size: u64, part_size: u64) -> String {
    let body = serde_json::json!({
        "repo": "proj/model", "digest": "sha256:abc", "size": size, "part_size": part_size
    })
    .to_string();
    let req = build_request(
        "POST",
        "/v2/blobs/uploads/multipart",
        &[("content-type", "application/json")],
        Body::from(body),
    );
    let resp = call(app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED, "initiate must 201");
    read_json(resp).await["upload_id"].as_str().unwrap_or("").to_string()
}

async fn put_part(app: &axum::Router, upload_id: &str, part_number: u32, bytes: Vec<u8>) -> StatusCode {
    let uri = format!("/v2/blobs/uploads/multipart/{upload_id}/parts/{part_number}");
    call(app, build_request("PUT", &uri, &[], Body::from(bytes))).await.status()
}

// A 10-byte blob at part_size 4 -> parts of 4, 4, 2 bytes. All three land, then
// complete streams the reassembly to the mock Harbor and returns 201.
#[tokio::test]
async fn full_flow_pushes_reassembled_blob_to_harbor() {
    let harbor = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"/blobs/uploads/$"))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("location", format!("{}/v2/proj/model/blobs/uploads/xyz", harbor.uri())),
        )
        .mount(&harbor)
        .await;
    // The finalize PUT must carry an explicit Content-Length equal to the blob
    // size (10), NOT chunked framing: the header matcher only matches when the
    // receiver set it, so a regression to `Transfer-Encoding: chunked` makes this
    // mock miss -> push_blob sees a non-2xx -> complete 502s and the assert below
    // fails. This pins F-3 (fixed-length monolithic LAN push).
    Mock::given(method("PUT"))
        .and(path_regex(r"/blobs/uploads/xyz"))
        .and(header("content-length", "10"))
        .respond_with(ResponseTemplate::new(201).insert_header("location", "/v2/proj/model/blobs/sha256:abc"))
        .mount(&harbor)
        .await;

    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));

    let upload_id = initiate(&app, 10, 4).await;
    assert!(!upload_id.is_empty(), "initiate must return an upload_id");
    for (n, len) in [(1u32, 4usize), (2, 4), (3, 2)] {
        assert_eq!(put_part(&app, &upload_id, n, vec![0u8; len]).await, StatusCode::NO_CONTENT, "part {n}");
    }

    let uri = format!("/v2/blobs/uploads/multipart/{upload_id}/complete");
    let resp = call(&app, build_request("POST", &uri, &[("authorization", "Bearer t")], Body::empty())).await;
    assert_eq!(resp.status(), StatusCode::CREATED, "complete must push to Harbor and 201");
}

// Complete before all parts land: the receiver must 409 with the exact missing
// part numbers and NOT contact Harbor (no Harbor mocks are mounted, so any call
// would surface as a 502).
#[tokio::test]
async fn complete_before_all_parts_reports_missing() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));

    let upload_id = initiate(&app, 10, 4).await; // 3 parts expected
    assert_eq!(put_part(&app, &upload_id, 1, vec![0u8; 4]).await, StatusCode::NO_CONTENT);

    let uri = format!("/v2/blobs/uploads/multipart/{upload_id}/complete");
    let resp = call(&app, build_request("POST", &uri, &[], Body::empty())).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT, "missing parts must 409");
    assert_eq!(read_json(resp).await["missing"], serde_json::json!([2, 3]));
}

// A percent-decoded path-traversal upload_id must be rejected before any
// filesystem path is built — never acted on as a successful abort. axum decodes
// `..%2f..%2fetc` into `../../etc` in the :upload_id segment; the UUID guard
// turns that into a client error instead of a `remove_dir_all` on a traversed
// path (the finding from the PR review).
#[tokio::test]
async fn abort_rejects_traversal_upload_id() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));
    let resp = call(
        &app,
        build_request("DELETE", "/v2/blobs/uploads/multipart/..%2f..%2fetc", &[], Body::empty()),
    )
    .await;
    assert!(
        resp.status().is_client_error(),
        "traversal id must be a client-error rejection, got {}",
        resp.status()
    );
    assert_ne!(resp.status(), StatusCode::NO_CONTENT, "must not be treated as a successful abort");
}

// A part whose body length doesn't match its position is rejected (400) and NOT
// marked received, so `complete` reports it missing rather than shipping a torn
// blob whose only detector would be Harbor's digest (a terminal failure).
#[tokio::test]
async fn put_part_wrong_length_rejected_and_not_received() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));
    let upload_id = initiate(&app, 10, 4).await; // parts of 4, 4, 2

    // Part 1 must be 4 bytes; send 3.
    assert_eq!(put_part(&app, &upload_id, 1, vec![0u8; 3]).await, StatusCode::BAD_REQUEST);

    let uri = format!("/v2/blobs/uploads/multipart/{upload_id}/complete");
    let resp = call(&app, build_request("POST", &uri, &[], Body::empty())).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(read_json(resp).await["missing"], serde_json::json!([1, 2, 3]));
}

// An oversized part body is refused — the scratch-exhaustion cap.
#[tokio::test]
async fn put_part_oversized_body_rejected() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));
    let upload_id = initiate(&app, 10, 4).await;
    assert_eq!(put_part(&app, &upload_id, 1, vec![0u8; 9999]).await, StatusCode::BAD_REQUEST);
}

// A declared size that explodes into too many parts is refused at initiate,
// before any session or the missing-parts Vec is allocated (OOM guard).
#[tokio::test]
async fn initiate_rejects_absurd_part_count() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));
    // min_part_size is 1 in tests, so 10M bytes at part_size 1 -> 10M parts.
    let body = serde_json::json!({
        "repo": "proj/model", "digest": "sha256:abc", "size": 10_000_000, "part_size": 1
    })
    .to_string();
    let resp = call(
        &app,
        build_request("POST", "/v2/blobs/uploads/multipart", &[("content-type", "application/json")], Body::from(body)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "too many parts must be refused at initiate");
}

// The unauthenticated `initiate` path is admission-controlled: once the session
// table is full, a further initiate is refused with 503 (retryable) rather than
// allocating unbounded scratch. Exercised with a max_sessions of 1 so the second
// call trips the cap. This is the receiver-side half of the F-1 DoS mitigation.
#[tokio::test]
async fn initiate_refuses_when_session_table_is_full() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(state_with(harbor.uri(), tmp.path().to_path_buf(), 1));

    let first = initiate(&app, 10, 4).await;
    assert!(!first.is_empty(), "first initiate under the cap must succeed");

    let body = serde_json::json!({
        "repo": "proj/model", "digest": "sha256:abc", "size": 10, "part_size": 4
    })
    .to_string();
    let resp = call(
        &app,
        build_request("POST", "/v2/blobs/uploads/multipart", &[("content-type", "application/json")], Body::from(body)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "initiate over the cap must 503");
}

// Unknown upload_id on a part PUT is a 404, not a silent accept.
#[tokio::test]
async fn put_part_unknown_upload_is_404() {
    let harbor = MockServer::start().await;
    let Ok(tmp) = tempfile::tempdir() else {
        unreachable!("tempdir must be creatable")
    };
    let app = crate::app_router(test_state(harbor.uri(), tmp.path().to_path_buf()));
    assert_eq!(put_part(&app, "does-not-exist", 1, vec![0u8; 4]).await, StatusCode::NOT_FOUND);
}
