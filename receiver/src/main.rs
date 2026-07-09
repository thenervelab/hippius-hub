//! hub-blob-receiver — the in-cluster staged reassembler for parallel blob
//! uploads. It terminates N concurrent WAN part streams from the hippius-hub
//! client, stages them on local `NVMe`, and on completion streams one native OCI
//! blob PUT into Harbor on the fast LAN. See `docs/plans/` in hippius-hub for
//! the design ("parallelize the WAN, serialize the LAN").

mod error;
mod handlers;
mod harbor;
mod plan;
mod state;

#[cfg(test)]
mod flow_tests;

use std::sync::Arc;
use std::time::Duration;

use axum::routing::{delete, get, post, put};
use axum::Router;
use dashmap::DashMap;

use crate::state::{AppState, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = load_config();
    tokio::fs::create_dir_all(&config.scratch_dir).await?;

    let state = AppState {
        config: Arc::new(config),
        sessions: Arc::new(DashMap::new()),
        // Force HTTP/1.1 for the LAN leg into Harbor, consistent with the
        // client's up/down paths (avoids h2 single-connection multiplexing).
        http: reqwest::Client::builder().http1_only().build()?,
    };

    spawn_sweeper(state.clone());

    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "hub-blob-receiver listening");
    axum::serve(listener, app_router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn load_config() -> Config {
    Config {
        harbor_base: env_or("HARBOR_BASE", "https://registry.hippius.com"),
        scratch_dir: env_or("SCRATCH_DIR", "/scratch").into(),
        min_part_size: 1024 * 1024,       // 1 MiB
        max_part_size: 512 * 1024 * 1024, // 512 MiB — bounds any single part
        session_ttl: Duration::from_hours(1),
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v2/blobs/uploads/multipart", post(handlers::initiate))
        .route("/v2/blobs/uploads/multipart/:upload_id/parts/:part_number", put(handlers::put_part))
        .route("/v2/blobs/uploads/multipart/:upload_id/complete", post(handlers::complete))
        .route("/v2/blobs/uploads/multipart/:upload_id", delete(handlers::abort))
        .with_state(state)
}

/// Periodically reclaim sessions (and their scratch) that outlived the TTL —
/// the backstop for a client that opened an upload and never finished, so
/// abandoned `NVMe` does not accumulate.
fn spawn_sweeper(state: AppState) {
    tokio::spawn(async move {
        let interval = state.config.session_ttl / 4;
        loop {
            tokio::time::sleep(interval).await;
            sweep_expired(&state).await;
        }
    });
}

async fn sweep_expired(state: &AppState) {
    let ttl = state.config.session_ttl;
    let expired: Vec<String> = state
        .sessions
        .iter()
        .filter(|entry| entry.value().created.elapsed() > ttl)
        .map(|entry| entry.key().clone())
        .collect();
    for id in expired {
        state.sessions.remove(&id);
        let _ = tokio::fs::remove_dir_all(state.config.scratch_dir.join(&id)).await;
        tracing::info!(upload_id = id, "swept expired upload session");
    }
}

/// Resolve when either Ctrl-C or SIGTERM (k8s pod termination) arrives, so
/// `axum::serve` stops accepting new connections but lets in-flight `complete`
/// requests drain.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::warn!(error = %e, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received; draining");
}
