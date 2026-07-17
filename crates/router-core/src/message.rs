//! Immutable Kafka message representation shared across delivery queues.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{ids::validate_identifier, CoreError};

/// Kafka source coordinates retained for tracing and diagnostics.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaPosition {
    /// Source topic.
    pub topic: Arc<str>,
    /// Source partition.
    pub partition: i32,
    /// Source offset.
    pub offset: i64,
}

/// Routing metadata decoded from Kafka headers.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RoutingMetadata {
    /// Stable idempotency key.
    pub message_id: Arc<str>,
    /// Tenant boundary. It is mandatory on every message.
    pub tenant_id: Arc<str>,
    /// Domain category such as `content` or `event`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<Arc<str>>,
    /// Domain subtype such as `broadcast`.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub message_type: Option<Arc<str>>,
    /// Logical channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Arc<str>>,
    /// Actor identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<Arc<str>>,
    /// Audience category, paired with `audience_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience_type: Option<Arc<str>>,
    /// Audience identifier, paired with `audience_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience_id: Option<Arc<str>>,
    /// MIME type of the payload.
    pub content_type: Arc<str>,
    /// Producer timestamp in Unix milliseconds when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<i64>,
    /// Kafka source coordinates when the event came from Kafka.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<KafkaPosition>,
}

impl RoutingMetadata {
    /// Validates routing invariants and identifier limits.
    pub fn validate(&self) -> Result<(), CoreError> {
        validate_identifier("message_id", &self.message_id, 256)?;
        validate_identifier("tenant_id", &self.tenant_id, 256)?;
        validate_identifier("content_type", &self.content_type, 256)?;

        for (field, value) in [
            ("kind", self.kind.as_deref()),
            ("type", self.message_type.as_deref()),
            ("channel", self.channel.as_deref()),
            ("actor_id", self.actor_id.as_deref()),
            ("audience_type", self.audience_type.as_deref()),
            ("audience_id", self.audience_id.as_deref()),
        ] {
            if let Some(value) = value {
                validate_identifier(field, value, 256)?;
            }
        }

        if self.audience_type.is_some() != self.audience_id.is_some() {
            return Err(CoreError::IncompleteAudience);
        }
        Ok(())
    }
}

/// Immutable message with a cheaply cloned payload and cached wire representation.
#[derive(Debug)]
pub struct RoutedMessage {
    /// Routing metadata.
    pub metadata: RoutingMetadata,
    /// Original Kafka payload.
    pub payload: Bytes,
    pub(crate) cached_payload_json: OnceLock<serde_json::Value>,
}

impl RoutedMessage {
    /// Constructs and validates a message.
    pub fn new(metadata: RoutingMetadata, payload: Bytes) -> Result<Self, CoreError> {
        metadata.validate()?;
        Ok(Self {
            metadata,
            payload,
            cached_payload_json: OnceLock::new(),
        })
    }
}
