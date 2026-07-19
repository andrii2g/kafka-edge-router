//! Layered daemon configuration and semantic validation.

use std::{collections::HashSet, net::SocketAddr, path::Path};

use anyhow::{bail, Context};
use router_api::{ApiConfig, AuthConfig, AuthMode};
use router_core::RouterConfig;
use router_kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use router_webhook::{WebhookConfig, WebhookDeliveryMode};
use serde::Deserialize;

fn default_http_addr() -> String {
    "0.0.0.0:8080".to_owned()
}

fn default_grpc_addr() -> String {
    "0.0.0.0:9090".to_owned()
}

fn default_shutdown_grace_secs() -> u64 {
    10
}

fn default_log_filter() -> String {
    "routerd=info,router_core=info,router_kafka=info,router_api=info,router_webhook=info".to_owned()
}

/// Complete deserialized process configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Listener and shutdown behavior.
    pub server: ServerConfig,
    /// Protocol limits.
    pub api: ApiConfig,
    /// Core queue and subscription policy.
    pub router: RouterConfig,
    /// Authentication settings.
    pub auth: AuthConfig,
    /// Kafka consumer and optional producer.
    pub kafka: KafkaConfig,
    /// Static webhook workers.
    pub webhooks: WebhookConfig,
    /// Log formatting.
    pub logging: LoggingConfig,
    /// Tracing export and optional dependency-aware readiness.
    pub observability: ObservabilityConfig,
}

impl AppConfig {
    /// Loads TOML and overlays `ROUTER__SECTION__FIELD` environment variables.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let value = config::Config::builder()
            .add_source(config::File::from(path).required(true))
            .add_source(
                config::Environment::with_prefix("ROUTER")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .with_context(|| format!("failed to load configuration from {}", path.display()))?;
        let mut configuration: Self = value
            .try_deserialize()
            .context("failed to deserialize configuration")?;
        configuration.apply_runtime_identity(|name| std::env::var(name).ok())?;
        configuration.validate()?;
        Ok(configuration)
    }

    fn apply_runtime_identity<F>(&mut self, mut lookup: F) -> anyhow::Result<()>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let Some(variable) = self.kafka.group_id_suffix_env.as_deref() else {
            return Ok(());
        };
        if !valid_environment_name(variable) {
            bail!("kafka.group_id_suffix_env must be a valid environment variable name");
        }
        let suffix = lookup(variable).with_context(|| {
            format!("environment variable {variable} is required for the Kafka group id")
        })?;
        if suffix.is_empty()
            || suffix.len() > 128
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            bail!("Kafka group-id suffix from {variable} must be 1..=128 ASCII letters, digits, '.', '_', or '-'");
        }
        let group_id = format!("{}.{}", self.kafka.consumer.group_id, suffix);
        if group_id.len() > 255 {
            bail!("resolved Kafka consumer group id must not exceed 255 bytes");
        }
        self.kafka.consumer.group_id = group_id;
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.validate_listener_and_limits()?;
        self.validate_kafka_and_auth()?;
        self.validate_observability()?;
        self.validate_webhooks()
    }

    fn validate_listener_and_limits(&self) -> anyhow::Result<()> {
        let http_addr = self
            .server
            .http_addr
            .parse::<SocketAddr>()
            .context("server.http_addr is not a socket address")?;
        let grpc_addr = self
            .server
            .grpc_addr
            .parse::<SocketAddr>()
            .context("server.grpc_addr is not a socket address")?;
        if self.server.security_mode == SecurityMode::ProtectedProxy
            && (!http_addr.ip().is_loopback() || !grpc_addr.ip().is_loopback())
        {
            bail!("protected_proxy mode requires loopback-only HTTP and gRPC listeners");
        }
        if self.server.security_mode == SecurityMode::ProtectedProxy
            && self.auth.mode == AuthMode::Disabled
        {
            bail!("protected_proxy mode requires authentication");
        }
        if matches!(
            self.auth.mode,
            AuthMode::TrustedHeader | AuthMode::ProxyMtls
        ) && self.server.security_mode != SecurityMode::ProtectedProxy
        {
            bail!("proxy identity modes require protected_proxy security mode");
        }
        if self.api.http_body_limit_bytes == 0
            || self.api.sse_keep_alive_secs == 0
            || self.api.ws_max_message_bytes == 0
            || self.api.ws_max_frame_bytes == 0
            || self.api.ws_max_commands_per_second == 0
            || self.api.grpc_max_decoding_message_bytes == 0
            || self.api.grpc_max_encoding_message_bytes == 0
            || self.api.grpc_concurrency_limit == 0
            || self.api.grpc_keep_alive_interval_secs == 0
            || self.api.grpc_keep_alive_timeout_secs == 0
            || self.api.publish_max_payload_bytes == 0
            || self.api.max_rate_limit_principals == 0
            || self.api.global_commands_per_second == 0
            || self.api.principal_commands_per_second == 0
            || self.api.global_publishes_per_second == 0
            || self.api.principal_publishes_per_second == 0
        {
            bail!("HTTP, SSE, WebSocket, and gRPC limits must be positive");
        }
        if self.router.default_queue_capacity == 0
            || self.router.max_queue_capacity == 0
            || self.api.stream_queue_capacity == 0
            || self.api.max_stream_queue_capacity == 0
            || self.router.max_connections == 0
            || self.router.max_connections_per_tenant == 0
            || self.router.max_subscriptions == 0
            || self.router.max_subscriptions_per_tenant == 0
            || self.router.max_subscriptions_per_connection == 0
            || self.router.slow_consumer_strikes == 0
        {
            bail!(
                "queue capacities, subscription limits, and slow-consumer strikes must be positive"
            );
        }
        if self.api.ws_max_frame_bytes > self.api.ws_max_message_bytes {
            bail!("api.ws_max_frame_bytes must not exceed api.ws_max_message_bytes");
        }
        if self.router.default_queue_capacity > self.router.max_queue_capacity {
            bail!("router.default_queue_capacity must not exceed router.max_queue_capacity");
        }
        if self.api.stream_queue_capacity > self.api.max_stream_queue_capacity {
            bail!("api.stream_queue_capacity must not exceed api.max_stream_queue_capacity");
        }
        if self.api.max_stream_queue_capacity > self.router.max_queue_capacity {
            bail!("api.max_stream_queue_capacity must not exceed router.max_queue_capacity");
        }
        Ok(())
    }

    fn validate_kafka_and_auth(&self) -> anyhow::Result<()> {
        if self.kafka.consumer.topics.is_empty() {
            bail!("kafka.consumer.topics must not be empty");
        }
        if self.kafka.consumer.max_payload_bytes == 0 {
            bail!("kafka.consumer.max_payload_bytes must be positive");
        }
        if self.kafka.consumer.brokers.trim().is_empty()
            || self.kafka.consumer.client_id.trim().is_empty()
        {
            bail!("Kafka consumer brokers and client_id must not be empty");
        }
        if !valid_kafka_group_id(&self.kafka.consumer.group_id) {
            bail!(
                "kafka.consumer.group_id must be 1..=255 ASCII letters, digits, '.', '_', or '-'"
            );
        }
        if self
            .kafka
            .consumer
            .topics
            .iter()
            .any(|topic| topic.trim().is_empty())
        {
            bail!("kafka.consumer.topics must not contain empty names");
        }
        if !matches!(
            self.kafka.consumer.auto_offset_reset.as_str(),
            "earliest" | "latest"
        ) {
            bail!("kafka.consumer.auto_offset_reset must be earliest or latest");
        }
        if self.kafka.producer.enabled
            && (self.kafka.producer.brokers.trim().is_empty()
                || self.kafka.producer.client_id.trim().is_empty()
                || self.kafka.producer.topic.trim().is_empty()
                || self.kafka.producer.delivery_timeout_ms == 0)
        {
            bail!("enabled Kafka producer fields and delivery timeout must not be empty or zero");
        }
        if self.auth.mode == AuthMode::StaticBearer && self.auth.bearer_tokens.is_empty() {
            bail!("auth.bearer_tokens must not be empty in static_bearer mode");
        }
        if self.auth.mode == AuthMode::Jwt {
            let jwt = &self.auth.jwt;
            if jwt.jwks_path.as_os_str().is_empty()
                || jwt.issuer.trim().is_empty()
                || jwt.audience.trim().is_empty()
                || jwt.algorithms.is_empty()
                || jwt.refresh_interval_secs == 0
                || jwt.max_jwks_bytes == 0
                || jwt.max_jwks_keys == 0
                || jwt.tenant_claim.trim().is_empty()
                || jwt.scope_claim.trim().is_empty()
                || jwt.subscribe_scope.trim().is_empty()
                || jwt.publish_scope.trim().is_empty()
            {
                bail!("JWT mode requires JWKS path, issuer, audience, algorithms, claims, scopes, and positive refresh bounds");
            }
            if jwt.algorithms.iter().any(|algorithm| {
                !matches!(
                    algorithm.as_str(),
                    "RS256" | "RS384" | "RS512" | "ES256" | "ES384" | "EdDSA"
                )
            }) {
                bail!("JWT algorithms must be asymmetric and explicitly supported");
            }
        }
        if self.auth.mode == AuthMode::ProxyMtls
            && (self.auth.proxy_identity_header.trim().is_empty()
                || self.auth.proxy_identities.is_empty()
                || self.auth.proxy_identities.values().any(|identity| {
                    identity.tenant_id.trim().is_empty()
                        || (!identity.subscribe && !identity.publish)
                }))
        {
            bail!(
                "proxy_mtls mode requires a header and bounded identities with tenant permissions"
            );
        }
        if self
            .auth
            .publish_tenants
            .iter()
            .any(|tenant| tenant.trim().is_empty())
        {
            bail!("auth.publish_tenants must not contain empty tenant ids");
        }
        if self.auth.mode == AuthMode::StaticBearer
            && self.auth.publish_tenants.iter().any(|tenant| {
                !self
                    .auth
                    .bearer_tokens
                    .values()
                    .any(|mapped| mapped == tenant)
            })
        {
            bail!("every auth.publish_tenants entry must have a static bearer mapping");
        }
        Ok(())
    }

    fn validate_observability(&self) -> anyhow::Result<()> {
        let telemetry = &self.observability.opentelemetry;
        if telemetry.enabled
            && (!matches!(
                telemetry.endpoint.split_once("://"),
                Some(("http" | "https", _))
            ) || telemetry.service_name.trim().is_empty()
                || telemetry.timeout_ms == 0
                || telemetry.shutdown_timeout_ms == 0
                || !(0.0..=1.0).contains(&telemetry.sampling_ratio))
        {
            bail!("enabled OpenTelemetry settings require an HTTP(S) endpoint, service name, positive timeouts, and sampling_ratio within 0..=1");
        }
        let readiness = &self.observability.kafka_readiness;
        if readiness.enabled
            && (readiness.check_interval_ms == 0
                || readiness.stale_after_secs == 0
                || readiness.failure_threshold == 0
                || readiness.success_threshold == 0)
        {
            bail!("enabled Kafka readiness intervals and hysteresis thresholds must be positive");
        }
        Ok(())
    }
    fn validate_webhooks(&self) -> anyhow::Result<()> {
        if self.webhooks.enabled && self.webhooks.mode == WebhookDeliveryMode::Durable {
            let durable = &self.webhooks.durable;
            if durable.brokers.trim().is_empty()
                || durable.client_id.trim().is_empty()
                || durable.group_id.trim().is_empty()
                || durable.delivery_topic.trim().is_empty()
                || durable.retry_topic.trim().is_empty()
                || durable.dead_letter_topic.trim().is_empty()
                || durable.delivery_timeout_ms == 0
                || durable.max_record_bytes == 0
                || durable.max_recovery_records == 0
            {
                bail!("durable webhook Kafka fields and bounds must be non-empty and positive");
            }
            let topics = [
                durable.delivery_topic.as_str(),
                durable.retry_topic.as_str(),
                durable.dead_letter_topic.as_str(),
            ];
            if topics[0] == topics[1] || topics[0] == topics[2] || topics[1] == topics[2] {
                bail!("durable webhook delivery, retry, and dead-letter topics must be distinct");
            }
            if self.webhooks.destinations.is_empty() {
                bail!("durable webhook mode requires at least one destination");
            }
        }
        let mut webhook_ids = HashSet::new();
        for destination in &self.webhooks.destinations {
            if destination.id.trim().is_empty() {
                bail!("webhook destination id must not be empty");
            }
            if !webhook_ids.insert(destination.id.as_str()) {
                bail!("duplicate webhook destination id: {}", destination.id);
            }
            if destination.queue_capacity == 0
                || destination.queue_capacity > self.router.max_queue_capacity
                || destination.timeout_ms == 0
                || destination.max_attempts == 0
                || destination.initial_backoff_ms == 0
                || destination.max_backoff_ms == 0
            {
                bail!(
                    "webhook {} queue must be within 1..={}, and timeout, attempt, and backoff values must be positive",
                    destination.id,
                    self.router.max_queue_capacity
                );
            }
            if destination.initial_backoff_ms > destination.max_backoff_ms {
                bail!(
                    "webhook {} initial_backoff_ms must not exceed max_backoff_ms",
                    destination.id
                );
            }
            destination
                .filter
                .validate()
                .with_context(|| format!("invalid webhook {} filter", destination.id))?;
        }
        Ok(())
    }
}

/// Listener addresses and graceful shutdown deadline.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// HTTP/WS/SSE address.
    pub http_addr: String,
    /// Public gRPC address.
    pub grpc_addr: String,
    /// Deadline before remaining tasks are aborted.
    pub shutdown_grace_secs: u64,
    /// Development plaintext or mandatory loopback protected-proxy mode.
    pub security_mode: SecurityMode,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: default_http_addr(),
            grpc_addr: default_grpc_addr(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
            security_mode: SecurityMode::Development,
        }
    }
}

/// Public transport protection policy.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMode {
    /// Plaintext listeners permitted for isolated development.
    #[default]
    Development,
    /// TLS/mTLS terminates at a local proxy; daemon listeners must be loopback-only.
    ProtectedProxy,
}
/// Kafka adapters.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Optional environment variable appended to the consumer group id at startup.
    pub group_id_suffix_env: Option<String>,
    /// Mandatory consumer.
    pub consumer: KafkaConsumerConfig,
    /// Optional producer selected with `enabled`.
    pub producer: KafkaProducerConfig,
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(byte) if byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_kafka_group_id(group_id: &str) -> bool {
    !group_id.is_empty()
        && group_id.len() <= 255
        && group_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Structured logging configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Default tracing filter, overridden by `RUST_LOG`.
    pub filter: String,
    /// Emit one JSON object per log event.
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: default_log_filter(),
            json: false,
        }
    }
}

/// Production observability settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    /// Optional OTLP trace export.
    pub opentelemetry: OpenTelemetryConfig,
    /// Optional Kafka dependency for readiness.
    pub kafka_readiness: KafkaReadinessConfig,
}

/// OTLP/HTTP trace export settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct OpenTelemetryConfig {
    /// Enable OTLP trace export.
    pub enabled: bool,
    /// OTLP HTTP endpoint.
    pub endpoint: String,
    /// Bounded service name attached to exported spans.
    pub service_name: String,
    /// Export request timeout.
    pub timeout_ms: u64,
    /// Provider flush deadline during shutdown.
    pub shutdown_timeout_ms: u64,
    /// Parent-based root trace sampling ratio in 0..=1.
    pub sampling_ratio: f64,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://127.0.0.1:4318".to_owned(),
            service_name: "kafka-edge-router".to_owned(),
            timeout_ms: 5_000,
            shutdown_timeout_ms: 5_000,
            sampling_ratio: 0.1,
        }
    }
}

/// Kafka readiness dependency and transition thresholds.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct KafkaReadinessConfig {
    /// Make readiness depend on recent Kafka health.
    pub enabled: bool,
    /// Health polling interval.
    pub check_interval_ms: u64,
    /// Maximum age of a healthy Kafka observation.
    pub stale_after_secs: u64,
    /// Consecutive unhealthy checks required to become unready.
    pub failure_threshold: u32,
    /// Consecutive healthy checks required to become ready.
    pub success_threshold: u32,
}

impl Default for KafkaReadinessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_ms: 1_000,
            stale_after_secs: 30,
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use router_core::RouteFilter;
    use router_webhook::{WebhookDeliveryMode, WebhookDestinationConfig};

    use super::AppConfig;

    fn webhook(queue_capacity: usize) -> WebhookDestinationConfig {
        WebhookDestinationConfig {
            id: "destination-a".to_owned(),
            url: "https://example.com/events".to_owned(),
            filter: RouteFilter {
                tenant_id: Arc::from("tenant-a"),
                kind: None,
                message_type: None,
                channel: None,
                actor_id: None,
                recipient_type: None,
                recipient_identity: None,
            },
            queue_capacity,
            timeout_ms: 1_000,
            max_attempts: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            signing_secret: None,
            headers: BTreeMap::new(),
            allowed_hosts: Vec::new(),
            allowed_ports: vec![443],
            allow_private_ips: false,
            allow_http: false,
        }
    }

    #[test]
    fn queue_cap_hierarchy_accepts_exact_limits() {
        let mut config = AppConfig::default();
        config.router.default_queue_capacity = 8;
        config.router.max_queue_capacity = 8;
        config.api.stream_queue_capacity = 8;
        config.api.max_stream_queue_capacity = 8;
        assert!(config.validate_listener_and_limits().is_ok());
    }

    #[test]
    fn queue_cap_hierarchy_rejects_zero_and_live_over_limit() {
        let mut zero = AppConfig::default();
        zero.api.stream_queue_capacity = 0;
        assert!(zero.validate_listener_and_limits().is_err());

        let mut over = AppConfig::default();
        over.router.max_queue_capacity = 8;
        over.api.stream_queue_capacity = 8;
        over.api.max_stream_queue_capacity = 9;
        assert!(over.validate_listener_and_limits().is_err());
    }

    #[test]
    fn websocket_limits_must_be_positive_and_frame_bounded() {
        let mut valid = AppConfig::default();
        valid.api.ws_max_message_bytes = 128;
        valid.api.ws_max_frame_bytes = 64;
        valid.api.ws_max_commands_per_second = 1;
        assert!(valid.validate_listener_and_limits().is_ok());

        let mut zero = valid.clone();
        zero.api.ws_max_commands_per_second = 0;
        assert!(zero.validate_listener_and_limits().is_err());

        let mut zero_publish = valid.clone();
        zero_publish.api.publish_max_payload_bytes = 0;
        assert!(zero_publish.validate_listener_and_limits().is_err());

        let mut oversized_frame = valid;
        oversized_frame.api.ws_max_frame_bytes = 129;
        assert!(oversized_frame.validate_listener_and_limits().is_err());
    }
    #[test]
    fn grpc_transport_limits_must_be_positive() {
        let valid = AppConfig::default();
        assert!(valid.validate_listener_and_limits().is_ok());

        for clear_limit in [
            |config: &mut AppConfig| config.api.grpc_max_decoding_message_bytes = 0,
            |config: &mut AppConfig| config.api.grpc_max_encoding_message_bytes = 0,
            |config: &mut AppConfig| config.api.grpc_concurrency_limit = 0,
            |config: &mut AppConfig| config.api.grpc_keep_alive_interval_secs = 0,
            |config: &mut AppConfig| config.api.grpc_keep_alive_timeout_secs = 0,
        ] {
            let mut invalid = valid.clone();
            clear_limit(&mut invalid);
            assert!(invalid.validate_listener_and_limits().is_err());
        }
    }

    #[test]
    fn static_webhook_queue_capacity_respects_core_cap() {
        for (capacity, expected_valid) in [(0, false), (8, true), (9, false)] {
            let mut config = AppConfig::default();
            config.router.max_queue_capacity = 8;
            config.webhooks.destinations = vec![webhook(capacity)];
            assert_eq!(
                config.validate_webhooks().is_ok(),
                expected_valid,
                "webhook queue capacity {capacity}"
            );
        }
    }
    #[test]
    fn durable_webhook_configuration_requires_distinct_bounded_topics() {
        let mut valid = AppConfig::default();
        valid.webhooks.enabled = true;
        valid.webhooks.mode = WebhookDeliveryMode::Durable;
        valid.webhooks.destinations = vec![webhook(8)];
        assert!(valid.validate_webhooks().is_ok());

        let mut duplicate = valid.clone();
        duplicate.webhooks.durable.retry_topic = duplicate.webhooks.durable.delivery_topic.clone();
        assert!(duplicate.validate_webhooks().is_err());

        let mut unbounded = valid.clone();
        unbounded.webhooks.durable.max_recovery_records = 0;
        assert!(unbounded.validate_webhooks().is_err());

        let mut empty = valid;
        empty.webhooks.destinations.clear();
        assert!(empty.validate_webhooks().is_err());
    }
    #[test]
    fn observability_bounds_are_validated_when_enabled() {
        let mut config = AppConfig::default();
        config.observability.opentelemetry.enabled = true;
        assert!(config.validate_observability().is_ok());

        config.observability.opentelemetry.sampling_ratio = 1.1;
        assert!(config.validate_observability().is_err());

        let mut readiness = AppConfig::default();
        readiness.observability.kafka_readiness.enabled = true;
        readiness.observability.kafka_readiness.failure_threshold = 0;
        assert!(readiness.validate_observability().is_err());
    }
    #[test]
    fn proxy_identity_modes_require_protected_transport() {
        let mut config = AppConfig::default();
        config.auth.mode = router_api::AuthMode::TrustedHeader;
        let error = config.validate().expect_err("unprotected proxy identity");
        assert!(error
            .to_string()
            .contains("proxy identity modes require protected_proxy"));
    }
    #[test]
    fn protected_proxy_mode_requires_loopback_and_authentication() {
        let mut config = AppConfig::default();
        config.server.security_mode = super::SecurityMode::ProtectedProxy;
        assert!(config.validate_listener_and_limits().is_err());

        config.auth.mode = router_api::AuthMode::TrustedHeader;
        assert!(config.validate_listener_and_limits().is_err());

        config.server.http_addr = "127.0.0.1:8080".to_owned();
        config.server.grpc_addr = "127.0.0.1:9090".to_owned();
        assert!(config.validate_listener_and_limits().is_ok());
    }

    #[test]
    fn kafka_group_id_is_bounded_and_portable() {
        let mut config = AppConfig::default();
        config.kafka.consumer.group_id = "router/group".to_owned();
        assert!(config.validate_kafka_and_auth().is_err());

        config.kafka.consumer.group_id = "r".repeat(256);
        assert!(config.validate_kafka_and_auth().is_err());

        config.kafka.consumer.group_id = "router.prod_1".to_owned();
        assert!(config.validate_kafka_and_auth().is_ok());
    }

    #[test]
    fn runtime_group_suffix_is_required_bounded_and_appended_once() {
        let mut valid = AppConfig::default();
        valid.kafka.consumer.group_id = "router".to_owned();
        valid.kafka.group_id_suffix_env = Some("POD_UID".to_owned());
        valid
            .apply_runtime_identity(|name| (name == "POD_UID").then(|| "pod-123".to_owned()))
            .expect("valid pod identity");
        assert_eq!(valid.kafka.consumer.group_id, "router.pod-123");

        let mut missing = AppConfig::default();
        missing.kafka.group_id_suffix_env = Some("POD_UID".to_owned());
        assert!(missing.apply_runtime_identity(|_| None).is_err());

        let mut invalid_name = AppConfig::default();
        invalid_name.kafka.group_id_suffix_env = Some("POD-UID".to_owned());
        assert!(invalid_name
            .apply_runtime_identity(|_| Some("pod".to_owned()))
            .is_err());

        let mut invalid_suffix = AppConfig::default();
        invalid_suffix.kafka.group_id_suffix_env = Some("POD_UID".to_owned());
        assert!(invalid_suffix
            .apply_runtime_identity(|_| Some("pod/123".to_owned()))
            .is_err());
    }
}
