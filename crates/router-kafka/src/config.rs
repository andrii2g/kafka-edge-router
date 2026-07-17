//! Kafka-specific configuration structures.

use std::collections::BTreeMap;

use serde::Deserialize;

fn default_brokers() -> String {
    "localhost:9092".to_owned()
}

fn default_group_id() -> String {
    "rust-kafka-edge-router-local".to_owned()
}

fn default_consumer_client_id() -> String {
    "rust-kafka-edge-router-consumer".to_owned()
}

fn default_producer_client_id() -> String {
    "rust-kafka-edge-router-producer".to_owned()
}

fn default_topic() -> String {
    "router.input".to_owned()
}

fn default_topics() -> Vec<String> {
    vec![default_topic()]
}

fn default_auto_offset_reset() -> String {
    "earliest".to_owned()
}

fn default_max_payload_bytes() -> usize {
    1_048_576
}

fn default_delivery_timeout_ms() -> u64 {
    10_000
}

/// Consumer properties and validation policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct KafkaConsumerConfig {
    /// Comma-separated broker endpoints.
    pub brokers: String,
    /// Consumer-group id. Use one unique group per router node in the MVP topology.
    pub group_id: String,
    /// Kafka client id.
    pub client_id: String,
    /// Input topics.
    pub topics: Vec<String>,
    /// `earliest` or `latest` when no committed offset exists.
    pub auto_offset_reset: String,
    /// Maximum accepted payload size before copying from librdkafka.
    pub max_payload_bytes: usize,
    /// Whether poison records are committed after being logged.
    pub commit_invalid_messages: bool,
    /// Additional librdkafka properties.
    pub properties: BTreeMap<String, String>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: default_brokers(),
            group_id: default_group_id(),
            client_id: default_consumer_client_id(),
            topics: default_topics(),
            auto_offset_reset: default_auto_offset_reset(),
            max_payload_bytes: default_max_payload_bytes(),
            commit_invalid_messages: true,
            properties: BTreeMap::new(),
        }
    }
}

/// Idempotent Kafka producer properties.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct KafkaProducerConfig {
    /// Enables HTTP/gRPC publishing.
    pub enabled: bool,
    /// Comma-separated broker endpoints.
    pub brokers: String,
    /// Kafka client id.
    pub client_id: String,
    /// Output topic consumed by this or another router deployment.
    pub topic: String,
    /// Broker acknowledgement deadline.
    pub delivery_timeout_ms: u64,
    /// Additional librdkafka properties.
    pub properties: BTreeMap<String, String>,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            brokers: default_brokers(),
            client_id: default_producer_client_id(),
            topic: default_topic(),
            delivery_timeout_ms: default_delivery_timeout_ms(),
            properties: BTreeMap::new(),
        }
    }
}
