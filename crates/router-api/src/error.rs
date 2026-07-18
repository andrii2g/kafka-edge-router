//! Consistent protocol error mapping.

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;
use serde::Serialize;

/// Public API failure mapped to both HTTP and gRPC status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Missing or invalid credentials.
    #[error("authentication required")]
    Unauthorized,
    /// Authenticated caller requested a different tenant.
    #[error("tenant is not authorized")]
    Forbidden,
    /// Request contract or command is invalid.
    #[error("{0}")]
    BadRequest(String),
    /// A bounded command or publish rate has been exceeded.
    #[error("rate limit exceeded")]
    RateLimited,
    /// Kafka publishing is disabled.
    #[error("publishing is not configured")]
    PublisherUnavailable,
    /// Kafka did not acknowledge before the configured deadline.
    #[error("publish acknowledgement timed out")]
    PublisherTimeout,
    /// The bounded Kafka producer queue is saturated.
    #[error("publisher queue is full")]
    PublisherQueueFull,
    /// Backend operation failed.
    #[error("{0}")]
    Backend(String),
}

impl ApiError {
    /// Converts this API failure to a tonic status.
    pub fn into_status(self) -> tonic::Status {
        let message = self.to_string();
        match self {
            Self::Unauthorized => tonic::Status::unauthenticated(message),
            Self::Forbidden => tonic::Status::permission_denied(message),
            Self::BadRequest(_) => tonic::Status::invalid_argument(message),
            Self::RateLimited | Self::PublisherQueueFull => {
                tonic::Status::resource_exhausted(message)
            }
            Self::PublisherUnavailable => tonic::Status::failed_precondition(message),
            Self::PublisherTimeout => tonic::Status::deadline_exceeded(message),
            Self::Backend(_) => tonic::Status::internal(message),
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::PublisherUnavailable | Self::PublisherQueueFull => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::PublisherTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::Backend(_) => StatusCode::BAD_GATEWAY,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::BadRequest(_) => "bad_request",
            Self::RateLimited => "rate_limited",
            Self::PublisherUnavailable => "publisher_unavailable",
            Self::PublisherTimeout => "publisher_timeout",
            Self::PublisherQueueFull => "publisher_queue_full",
            Self::Backend(_) => "backend_error",
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorBody {
            code: self.code(),
            message: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}
