//! Receiver error type and its HTTP mapping.
//!
//! Client-caused faults (unknown upload, bad part number, malformed request)
//! map to 4xx; failures talking to Harbor or the local disk map to 502 because
//! from the client's perspective the receiver is an upstream that failed. The
//! body is a small JSON `{"error": "..."}` so the client can log a reason.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiverError {
    #[error("unknown upload_id")]
    UnknownUpload,

    #[error("invalid part number {0}")]
    InvalidPart(u32),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("local I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("harbor error: {0}")]
    Harbor(String),

    #[error("upstream transport error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

impl IntoResponse for ReceiverError {
    fn into_response(self) -> Response {
        let status = match &self {
            ReceiverError::UnknownUpload => StatusCode::NOT_FOUND,
            ReceiverError::InvalidPart(_) | ReceiverError::BadRequest(_) => StatusCode::BAD_REQUEST,
            // The receiver is an upstream from the client's view; disk/Harbor
            // failures are gateway failures, not the client's fault.
            ReceiverError::Io(_) | ReceiverError::Harbor(_) | ReceiverError::Reqwest(_) => StatusCode::BAD_GATEWAY,
        };
        (status, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}
