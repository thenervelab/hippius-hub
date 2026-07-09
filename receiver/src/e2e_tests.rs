//! End-to-end over real sockets: the receiver's axum server bound to an
//! ephemeral port, driven by a real reqwest client through the full
//! initiate -> concurrent part PUTs -> complete flow, with a wiremock Harbor as
//! the LAN target. Unlike `flow_tests` (in-process `oneshot`), this exercises
//! `axum::serve`, real TCP accept, and concurrent parts over separate
//! connections — the closest thing to the deployed path without a cluster.
//!
//! `unwrap`/`expect` are denied crate-wide; fallible setup uses `let ... else`.
//! The receiver's reqwest has no `json` feature, so JSON is (de)serialized by
//! hand via `serde_json` rather than reqwest's `.json()` helpers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::state::{AppState, Config};

fn test_state(harbor_base: String, scratch: PathBuf) -> AppState {
    AppState {
        config: Arc::new(Config {
            harbor_base,
            scratch_dir: scratch,
            min_part_size: 1,
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

/// Bind an ephemeral port, spawn `axum::serve`, return the base URL. The
/// listener is bound before serving, so the kernel accepts the client's connect
/// into the backlog even before `serve` starts — no readiness race.
async fn spawn_receiver(state: AppState) -> String {
    let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
        unreachable!("binding an ephemeral loopback port must succeed")
    };
    let Ok(addr) = listener.local_addr() else {
        unreachable!("a bound listener has a local address")
    };
    let app = crate::app_router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    match reqwest::Client::builder().http1_only().build() {
        Ok(c) => c,
        Err(_) => reqwest::Client::new(),
    }
}

#[tokio::test]
async fn e2e_real_socket_upload_reaches_harbor() {
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
    let base = spawn_receiver(test_state(harbor.uri(), tmp.path().to_path_buf())).await;
    let http = client();

    // initiate (JSON by hand — no reqwest json feature in the receiver crate)
    let init_body = serde_json::json!({
        "repo": "proj/model", "digest": "sha256:abc", "size": 10, "part_size": 4
    })
    .to_string();
    let Ok(resp) = http
        .post(format!("{base}/v2/blobs/uploads/multipart"))
        .header("content-type", "application/json")
        .body(init_body)
        .send()
        .await
    else {
        unreachable!("initiate request must send")
    };
    assert_eq!(resp.status().as_u16(), 201, "initiate must 201");
    let Ok(bytes) = resp.bytes().await else {
        unreachable!("initiate body must read")
    };
    let init: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    let upload_id = init["upload_id"].as_str().unwrap_or("").to_string();
    assert!(!upload_id.is_empty(), "initiate must return an upload_id");

    // parts 1,2,3 concurrently over separate connections
    let mut handles = Vec::new();
    for (part_number, len) in [(1u32, 4usize), (2, 4), (3, 2)] {
        let http = http.clone();
        let base = base.clone();
        let upload_id = upload_id.clone();
        handles.push(tokio::spawn(async move {
            http.put(format!("{base}/v2/blobs/uploads/multipart/{upload_id}/parts/{part_number}"))
                .body(vec![0u8; len])
                .send()
                .await
                .map(|r| r.status().as_u16())
        }));
    }
    for handle in handles {
        let Ok(Ok(status)) = handle.await else {
            unreachable!("each part PUT task must complete")
        };
        assert_eq!(status, 204, "part PUT must 204");
    }

    // complete -> reassembles and pushes to the mock Harbor
    let Ok(resp) = http
        .post(format!("{base}/v2/blobs/uploads/multipart/{upload_id}/complete"))
        .header("authorization", "Bearer t")
        .send()
        .await
    else {
        unreachable!("complete request must send")
    };
    assert_eq!(resp.status().as_u16(), 201, "complete must push to Harbor and 201");

    // Harbor saw exactly the init + the monolithic blob PUT.
    let Some(requests) = harbor.received_requests().await else {
        unreachable!("wiremock records requests by default")
    };
    let put_to_harbor = requests
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path().contains("/blobs/uploads/xyz"))
        .count();
    assert_eq!(put_to_harbor, 1, "exactly one monolithic blob PUT to Harbor");
}
