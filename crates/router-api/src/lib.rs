//! Public HTTP, WebSocket, SSE, and gRPC servers.

mod auth;
mod error;
mod grpc;
mod http;
mod publish;
mod state;

pub use auth::{AuthConfig, AuthMode, Authenticator, Principal};
pub use error::ApiError;
pub use grpc::serve_grpc;
pub use http::{http_router, serve_http};
pub use state::{ApiConfig, ApiState, HealthState};
