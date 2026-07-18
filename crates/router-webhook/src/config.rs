//! Webhook configuration contracts.

use std::collections::BTreeMap;

use router_core::RouteFilter;
use serde::Deserialize;

fn default_durable_brokers() -> String {
    "localhost:9092".to_owned()
}

fn default_durable_client_id() -> String {
    "kafka-edge-router-webhook".to_owned()
}

fn default_durable_group_id() -> String {
    "kafka-edge-router-webhook".to_owned()
}

fn default_delivery_topic() -> String {
    "router.webhook.delivery".to_owned()
}

fn default_retry_topic() -> String {
    "router.webhook.retry".to_owned()
}

fn default_dead_letter_topic() -> String {
    "router.webhook.dead-letter".to_owned()
}

fn default_delivery_timeout_ms() -> u64 {
    10_000
}

fn default_max_record_bytes() -> usize {
    2_097_152
}

fn default_max_recovery_records() -> usize {
    10_000
}

fn default_queue_capacity() -> usize {
    256
}

fn default_timeout_ms() -> u64 {
    5_000
}

fn default_max_attempts() -> u32 {
    5
}

fn default_initial_backoff_ms() -> u64 {
    250
}

fn default_max_backoff_ms() -> u64 {
    30_000
}

/// Outbound webhook module configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct WebhookConfig {
    /// Enables static destinations.
    pub enabled: bool,
    /// Explicit reliability boundary. Durable mode persists before source commit.
    pub mode: WebhookDeliveryMode,
    /// Kafka-backed delivery settings used only in durable mode.
    pub durable: DurableWebhookConfig,
    /// Independently ordered destinations.
    pub destinations: Vec<WebhookDestinationConfig>,
}

/// Webhook reliability mode. Modes are never mixed within one process.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookDeliveryMode {
    /// Bounded destination queues and in-memory sleeps; restart loses pending work.
    #[default]
    Volatile,
    /// Kafka-backed commands, retry state, and dead letters.
    Durable,
}

/// Kafka topics and client identity for durable webhook delivery.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DurableWebhookConfig {
    /// Comma-separated Kafka broker endpoints.
    pub brokers: String,
    /// Producer/consumer client id prefix.
    pub client_id: String,
    /// Consumer group prefix shared by all router replicas.
    pub group_id: String,
    /// Initial delivery command topic.
    pub delivery_topic: String,
    /// Persisted retry schedule topic.
    pub retry_topic: String,
    /// Terminal dead-letter topic.
    pub dead_letter_topic: String,
    /// Kafka acknowledgement deadline.
    pub delivery_timeout_ms: u64,
    /// Maximum serialized durable command size.
    pub max_record_bytes: usize,
    /// Maximum retry records materialized during one recovery pass.
    pub max_recovery_records: usize,
    /// Additional librdkafka properties. Semantic invariants are applied last.
    pub properties: BTreeMap<String, String>,
}

impl Default for DurableWebhookConfig {
    fn default() -> Self {
        Self {
            brokers: default_durable_brokers(),
            client_id: default_durable_client_id(),
            group_id: default_durable_group_id(),
            delivery_topic: default_delivery_topic(),
            retry_topic: default_retry_topic(),
            dead_letter_topic: default_dead_letter_topic(),
            delivery_timeout_ms: default_delivery_timeout_ms(),
            max_record_bytes: default_max_record_bytes(),
            max_recovery_records: default_max_recovery_records(),
            properties: BTreeMap::new(),
        }
    }
}

/// One statically configured outbound destination.
#[derive(Clone, Debug, Deserialize)]
pub struct WebhookDestinationConfig {
    /// Stable operator-defined destination id.
    pub id: String,
    /// HTTPS endpoint, or HTTP only when explicitly enabled.
    pub url: String,
    /// Route filter registered in the core matcher.
    pub filter: RouteFilter,
    /// Bounded destination queue capacity.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// Connect and total request timeout.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Total attempts including the first request.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Initial exponential retry delay.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum exponential retry delay.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Optional HMAC-SHA256 signing secret.
    #[serde(default)]
    pub signing_secret: Option<String>,
    /// Additional non-reserved request headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Explicit hostname allowlist. Empty means only the configured hostname.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// Allows literal private/local IP destinations. Disabled by default.
    #[serde(default)]
    pub allow_private_ips: bool,
    /// Allows plain HTTP. Disabled by default.
    #[serde(default)]
    pub allow_http: bool,
}
