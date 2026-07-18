//! Shared API state and health gates.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use router_core::{ConnectionId, MessagePublisher, Router};
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

fn default_ws_max_message_bytes() -> usize {
    65_536
}

fn default_ws_max_frame_bytes() -> usize {
    65_536
}

fn default_ws_max_commands_per_second() -> u32 {
    32
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
    /// Maximum assembled inbound WebSocket message size.
    pub ws_max_message_bytes: usize,
    /// Maximum inbound WebSocket frame size.
    pub ws_max_frame_bytes: usize,
    /// Maximum application commands accepted per WebSocket connection per second.
    pub ws_max_commands_per_second: u32,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            http_body_limit_bytes: default_http_body_limit(),
            stream_queue_capacity: default_stream_queue_capacity(),
            max_stream_queue_capacity: default_max_stream_queue_capacity(),
            sse_keep_alive_secs: default_sse_keep_alive_secs(),
            ws_max_message_bytes: default_ws_max_message_bytes(),
            ws_max_frame_bytes: default_ws_max_frame_bytes(),
            ws_max_commands_per_second: default_ws_max_commands_per_second(),
        }
    }
}

pub(crate) fn resolve_stream_queue_capacity(
    config: &ApiConfig,
    requested: Option<usize>,
) -> Result<usize, usize> {
    let capacity = requested.unwrap_or(config.stream_queue_capacity);
    if capacity == 0 || capacity > config.max_stream_queue_capacity {
        return Err(config.max_stream_queue_capacity);
    }
    Ok(capacity)
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

pub(crate) struct ConnectionGuard {
    router: Arc<Router>,
    connection_id: ConnectionId,
}

impl ConnectionGuard {
    pub(crate) fn new(router: Arc<Router>, connection_id: ConnectionId) -> Self {
        Self {
            router,
            connection_id,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.router.unregister_connection(self.connection_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use router_core::{DeliveryProtocol, RouteFilter, Router, RouterConfig, SubscriptionId};

    use super::{resolve_stream_queue_capacity, ApiConfig, ConnectionGuard};

    #[test]
    fn live_stream_queue_capacity_boundaries() {
        let config = ApiConfig {
            stream_queue_capacity: 4,
            max_stream_queue_capacity: 8,
            ..ApiConfig::default()
        };

        assert_eq!(resolve_stream_queue_capacity(&config, None), Ok(4));
        assert_eq!(resolve_stream_queue_capacity(&config, Some(8)), Ok(8));
        assert_eq!(resolve_stream_queue_capacity(&config, Some(0)), Err(8));
        assert_eq!(resolve_stream_queue_capacity(&config, Some(9)), Err(8));
    }

    #[test]
    fn repeated_connection_guard_drops_are_idempotent() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        router
            .subscribe(
                registration.connection_id,
                SubscriptionId::new("subscription-a").expect("subscription id"),
                RouteFilter {
                    tenant_id: Arc::from("tenant-a"),
                    kind: None,
                    message_type: None,
                    channel: None,
                    actor_id: None,
                    audience_type: None,
                    audience_id: None,
                },
            )
            .expect("subscribe");

        let first = ConnectionGuard::new(Arc::clone(&router), registration.connection_id);
        let second = ConnectionGuard::new(Arc::clone(&router), registration.connection_id);
        drop(first);
        drop(second);

        assert_eq!(router.status().active_connections, 0);
        assert_eq!(router.status().subscriptions, 0);
    }
}
