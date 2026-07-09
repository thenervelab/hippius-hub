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
use wiremock::matchers::{method, path_regex};
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
    AppState {
        config: Arc::new(Config {
            harbor_base,
            scratch_dir: scratch,
            min_part_size: 1, // let a tiny fixture split into real parts
            max_part_size: 1024 * 1024,
            session_ttl: Duration::from_hours(1),
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
    Mock::given(method("PUT"))
        .and(path_regex(r"/blobs/uploads/xyz"))
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
