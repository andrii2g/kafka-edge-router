//! Shared API state and health gates.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use router_core::{ConnectionId, MessagePublisher, Router};
use serde::Deserialize;
use tokio::sync::watch;

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

fn default_grpc_max_decoding_message_bytes() -> usize {
    4 * 1_024 * 1_024
}

fn default_grpc_max_encoding_message_bytes() -> usize {
    4 * 1_024 * 1_024
}

fn default_grpc_concurrency_limit() -> usize {
    256
}

fn default_grpc_keep_alive_interval_secs() -> u64 {
    30
}

fn default_grpc_keep_alive_timeout_secs() -> u64 {
    10
}

fn default_grpc_health_enabled() -> bool {
    true
}

fn default_publish_max_payload_bytes() -> usize {
    1_048_576
}

fn default_max_rate_limit_principals() -> usize {
    10_000
}

fn default_global_command_rate() -> u32 {
    10_000
}

fn default_principal_command_rate() -> u32 {
    64
}

fn default_global_publish_rate() -> u32 {
    1_000
}

fn default_principal_publish_rate() -> u32 {
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
    /// Maximum decoded size of one inbound gRPC message.
    pub grpc_max_decoding_message_bytes: usize,
    /// Maximum encoded size of one outbound gRPC message.
    pub grpc_max_encoding_message_bytes: usize,
    /// Maximum concurrent gRPC requests per HTTP/2 connection.
    pub grpc_concurrency_limit: usize,
    /// HTTP/2 keep-alive ping interval for gRPC connections.
    pub grpc_keep_alive_interval_secs: u64,
    /// Deadline for a gRPC HTTP/2 keep-alive acknowledgement.
    pub grpc_keep_alive_timeout_secs: u64,
    /// Expose the standard gRPC health service.
    pub grpc_health_enabled: bool,
    /// Expose server reflection. Keep disabled in production.
    pub grpc_reflection_enabled: bool,
    /// Maximum raw payload accepted by HTTP or gRPC publishing.
    pub publish_max_payload_bytes: usize,
    /// Maximum principal counters retained by the fixed-window limiter.
    pub max_rate_limit_principals: usize,
    /// Maximum protocol commands accepted process-wide per second.
    pub global_commands_per_second: u32,
    /// Maximum protocol commands accepted per principal per second.
    pub principal_commands_per_second: u32,
    /// Maximum publish commands accepted process-wide per second.
    pub global_publishes_per_second: u32,
    /// Maximum publish commands accepted per principal per second.
    pub principal_publishes_per_second: u32,
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
            grpc_max_decoding_message_bytes: default_grpc_max_decoding_message_bytes(),
            grpc_max_encoding_message_bytes: default_grpc_max_encoding_message_bytes(),
            grpc_concurrency_limit: default_grpc_concurrency_limit(),
            grpc_keep_alive_interval_secs: default_grpc_keep_alive_interval_secs(),
            grpc_keep_alive_timeout_secs: default_grpc_keep_alive_timeout_secs(),
            grpc_health_enabled: default_grpc_health_enabled(),
            grpc_reflection_enabled: false,
            publish_max_payload_bytes: default_publish_max_payload_bytes(),
            max_rate_limit_principals: default_max_rate_limit_principals(),
            global_commands_per_second: default_global_command_rate(),
            principal_commands_per_second: default_principal_command_rate(),
            global_publishes_per_second: default_global_publish_rate(),
            principal_publishes_per_second: default_principal_publish_rate(),
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
#[derive(Debug)]
pub struct HealthState {
    live: AtomicBool,
    ready: AtomicBool,
    ready_changes: watch::Sender<bool>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            live: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            ready_changes: watch::channel(false).0,
        }
    }
}

impl HealthState {
    /// Sets process liveness.
    pub fn set_live(&self, value: bool) {
        self.live.store(value, Ordering::Release);
    }

    /// Sets readiness to receive public traffic.
    pub fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::Release);
        self.ready_changes.send_replace(value);
    }

    /// Returns current liveness.
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    /// Returns current readiness.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn readiness(&self) -> watch::Receiver<bool> {
        self.ready_changes.subscribe()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RateKind {
    Command,
    Publish,
}

struct PrincipalRate {
    window_started: Instant,
    commands: u32,
    publishes: u32,
}

struct RateState {
    window_started: Instant,
    commands: u32,
    publishes: u32,
    principals: HashMap<Arc<str>, PrincipalRate>,
}

struct AbuseLimiter {
    config: ApiConfig,
    state: Mutex<RateState>,
}

impl AbuseLimiter {
    fn new(config: ApiConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RateState {
                window_started: Instant::now(),
                commands: 0,
                publishes: 0,
                principals: HashMap::new(),
            }),
        }
    }

    fn allow(&self, tenant_id: &Arc<str>, kind: RateKind) -> bool {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.window_started.elapsed() >= Duration::from_secs(1) {
            state.window_started = now;
            state.commands = 0;
            state.publishes = 0;
            state.principals.clear();
        }
        let (global, global_limit, principal_limit) = match kind {
            RateKind::Command => (
                state.commands,
                self.config.global_commands_per_second,
                self.config.principal_commands_per_second,
            ),
            RateKind::Publish => (
                state.publishes,
                self.config.global_publishes_per_second,
                self.config.principal_publishes_per_second,
            ),
        };
        if global >= global_limit {
            return false;
        }
        if !state.principals.contains_key(tenant_id)
            && state.principals.len() >= self.config.max_rate_limit_principals
        {
            return false;
        }
        let principal = state
            .principals
            .entry(Arc::clone(tenant_id))
            .or_insert(PrincipalRate {
                window_started: now,
                commands: 0,
                publishes: 0,
            });
        if principal.window_started.elapsed() >= Duration::from_secs(1) {
            principal.window_started = now;
            principal.commands = 0;
            principal.publishes = 0;
        }
        let accepted = match kind {
            RateKind::Command => &mut principal.commands,
            RateKind::Publish => &mut principal.publishes,
        };
        if *accepted >= principal_limit {
            return false;
        }
        *accepted += 1;
        match kind {
            RateKind::Command => state.commands += 1,
            RateKind::Publish => state.publishes += 1,
        }
        true
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
    abuse_limiter: Arc<AbuseLimiter>,
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
        Self::with_authenticator(router, Authenticator::new(auth), publisher, health, config)
    }

    /// Constructs API state with a preloaded reloadable authenticator.
    pub fn with_authenticator(
        router: Arc<Router>,
        authenticator: Authenticator,
        publisher: Option<Arc<dyn MessagePublisher>>,
        health: Arc<HealthState>,
        config: ApiConfig,
    ) -> Self {
        let abuse_limiter = Arc::new(AbuseLimiter::new(config.clone()));
        Self {
            router,
            authenticator,
            publisher,
            health,
            config,
            abuse_limiter,
        }
    }

    pub(crate) fn allow_rate(&self, tenant_id: &Arc<str>, kind: RateKind) -> bool {
        let allowed = self.abuse_limiter.allow(tenant_id, kind);
        if !allowed {
            self.router.metrics().record_security_limit_rejection();
        }
        allowed
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

    use super::{
        resolve_stream_queue_capacity, AbuseLimiter, ApiConfig, ConnectionGuard, RateKind,
    };

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
    fn fixed_window_limits_are_global_per_principal_and_bounded() {
        let limiter = AbuseLimiter::new(ApiConfig {
            max_rate_limit_principals: 1,
            global_commands_per_second: 2,
            principal_commands_per_second: 1,
            global_publishes_per_second: 1,
            principal_publishes_per_second: 1,
            ..ApiConfig::default()
        });
        let tenant_a = Arc::<str>::from("tenant-a");
        let tenant_b = Arc::<str>::from("tenant-b");

        assert!(limiter.allow(&tenant_a, RateKind::Command));
        assert!(!limiter.allow(&tenant_a, RateKind::Command));
        assert!(!limiter.allow(&tenant_b, RateKind::Command));
        assert!(limiter.allow(&tenant_a, RateKind::Publish));
        assert!(!limiter.allow(&tenant_a, RateKind::Publish));
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
