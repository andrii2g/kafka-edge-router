//! Layered daemon configuration and semantic validation.

use std::{collections::HashSet, net::SocketAddr, path::Path};

use anyhow::{bail, Context};
use router_api::{ApiConfig, AuthConfig, AuthMode};
use router_core::RouterConfig;
use router_kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use router_webhook::WebhookConfig;
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
        let configuration: Self = value
            .try_deserialize()
            .context("failed to deserialize configuration")?;
        configuration.validate()?;
        Ok(configuration)
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.validate_listener_and_limits()?;
        self.validate_kafka_and_auth()?;
        self.validate_webhooks()
    }

    fn validate_listener_and_limits(&self) -> anyhow::Result<()> {
        self.server
            .http_addr
            .parse::<SocketAddr>()
            .context("server.http_addr is not a socket address")?;
        self.server
            .grpc_addr
            .parse::<SocketAddr>()
            .context("server.grpc_addr is not a socket address")?;
        if self.api.http_body_limit_bytes == 0
            || self.api.sse_keep_alive_secs == 0
            || self.api.ws_max_message_bytes == 0
            || self.api.ws_max_frame_bytes == 0
            || self.api.ws_max_commands_per_second == 0
        {
            bail!("HTTP, SSE, and WebSocket limits must be positive");
        }
        if self.router.default_queue_capacity == 0
            || self.router.max_queue_capacity == 0
            || self.api.stream_queue_capacity == 0
            || self.api.max_stream_queue_capacity == 0
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
            || self.kafka.consumer.group_id.trim().is_empty()
            || self.kafka.consumer.client_id.trim().is_empty()
        {
            bail!("Kafka consumer brokers, group_id, and client_id must not be empty");
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
        Ok(())
    }

    fn validate_webhooks(&self) -> anyhow::Result<()> {
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: default_http_addr(),
            grpc_addr: default_grpc_addr(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
        }
    }
}

/// Kafka adapters.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Mandatory consumer.
    pub consumer: KafkaConsumerConfig,
    /// Optional producer selected with `enabled`.
    pub producer: KafkaProducerConfig,
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use router_core::RouteFilter;
    use router_webhook::WebhookDestinationConfig;

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
                audience_type: None,
                audience_id: None,
            },
            queue_capacity,
            timeout_ms: 1_000,
            max_attempts: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            signing_secret: None,
            headers: BTreeMap::new(),
            allowed_hosts: Vec::new(),
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

        let mut oversized_frame = valid;
        oversized_frame.api.ws_max_frame_bytes = 129;
        assert!(oversized_frame.validate_listener_and_limits().is_err());
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
}
