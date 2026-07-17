//! Shared API state and health gates.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use router_core::{MessagePublisher, Router};
use serde::Deserialize;

use crate::{AuthConfig, Authenticator};

fn default_http_body_limit() -> usize {
    1_048_576
}

fn default_stream_queue_capacity() -> usize {
    256
}

fn default_max_stream_queue_capacity() -> usize {
    4_096
}

fn default_sse_keep_alive_secs() -> u64 {
    15
}

/// API limits shared by protocol adapters.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// Maximum HTTP request body size.
    pub http_body_limit_bytes: usize,
    /// Default queue capacity for WS, SSE, and gRPC streams.
    pub stream_queue_capacity: usize,
    /// Maximum client-requested queue capacity for a live stream.
    pub max_stream_queue_capacity: usize,
    /// SSE keep-alive interval.
    pub sse_keep_alive_secs: u64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            http_body_limit_bytes: default_http_body_limit(),
            stream_queue_capacity: default_stream_queue_capacity(),
            max_stream_queue_capacity: default_max_stream_queue_capacity(),
            sse_keep_alive_secs: default_sse_keep_alive_secs(),
        }
    }
}

/// Mutable liveness/readiness gates exposed through HTTP and gRPC status APIs.
#[derive(Debug, Default)]
pub struct HealthState {
    live: AtomicBool,
    ready: AtomicBool,
}

impl HealthState {
    /// Sets process liveness.
    pub fn set_live(&self, value: bool) {
        self.live.store(value, Ordering::Release);
    }

    /// Sets readiness to receive public traffic.
    pub fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::Release);
    }

    /// Returns current liveness.
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    /// Returns current readiness.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Cloneable dependency container shared by handlers.
#[derive(Clone)]
pub struct ApiState {
    /// Routing engine.
    pub router: Arc<Router>,
    /// Authentication implementation.
    pub authenticator: Authenticator,
    /// Optional Kafka publisher.
    pub publisher: Option<Arc<dyn MessagePublisher>>,
    /// Health gates.
    pub health: Arc<HealthState>,
    /// API limits.
    pub config: ApiConfig,
}

impl ApiState {
    /// Constructs API state from daemon dependencies.
    pub fn new(
        router: Arc<Router>,
        auth: AuthConfig,
        publisher: Option<Arc<dyn MessagePublisher>>,
        health: Arc<HealthState>,
        config: ApiConfig,
    ) -> Self {
        Self {
            router,
            authenticator: Authenticator::new(auth),
            publisher,
            health,
            config,
        }
    }
}
