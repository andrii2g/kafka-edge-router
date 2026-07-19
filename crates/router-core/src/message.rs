//! Immutable Kafka message representation shared across delivery queues.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use opentelemetry::{global, propagation::Extractor};
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
    /// Open recipient category, paired with `recipient_identity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient_type: Option<Arc<str>>,
    /// Recipient identity, paired with `recipient_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient_identity: Option<Arc<str>>,
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
            ("recipient_type", self.recipient_type.as_deref()),
            ("recipient_identity", self.recipient_identity.as_deref()),
        ] {
            if let Some(value) = value {
                validate_identifier(field, value, 256)?;
            }
        }

        if self.recipient_type.is_some() != self.recipient_identity.is_some() {
            return Err(CoreError::IncompleteRecipient);
        }
        Ok(())
    }
}

/// Bounded W3C trace headers propagated with a routed message.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TraceContext {
    traceparent: Arc<str>,
    tracestate: Option<Arc<str>>,
}

impl TraceContext {
    /// Constructs bounded trace headers. The OpenTelemetry propagator performs semantic validation.
    pub fn new(traceparent: &str, tracestate: Option<&str>) -> Result<Self, CoreError> {
        validate_identifier("traceparent", traceparent, 256)?;
        if let Some(tracestate) = tracestate {
            validate_identifier("tracestate", tracestate, 512)?;
        }
        Ok(Self {
            traceparent: Arc::from(traceparent),
            tracestate: tracestate.map(Arc::from),
        })
    }

    /// Restores this remote parent onto a tracing span.
    pub fn set_span_parent(&self, span: &tracing::Span) {
        let parent = global::get_text_map_propagator(|propagator| propagator.extract(self));
        let _ = span.set_parent(parent);
    }
}

impl Extractor for TraceContext {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => Some(&self.traceparent),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        if self.tracestate.is_some() {
            vec!["traceparent", "tracestate"]
        } else {
            vec!["traceparent"]
        }
    }
}

/// Immutable message with a cheaply cloned payload and cached wire representation.
#[derive(Debug)]
pub struct RoutedMessage {
    /// Routing metadata.
    pub metadata: RoutingMetadata,
    /// Original Kafka payload.
    pub payload: Bytes,
    trace_context: Option<TraceContext>,
    pub(crate) cached_payload_json: OnceLock<serde_json::Value>,
}

impl RoutedMessage {
    /// Constructs and validates a message.
    pub fn new(metadata: RoutingMetadata, payload: Bytes) -> Result<Self, CoreError> {
        Self::new_with_trace_context(metadata, payload, None)
    }

    /// Constructs a message with optional bounded W3C trace propagation headers.
    pub fn new_with_trace_context(
        metadata: RoutingMetadata,
        payload: Bytes,
        trace_context: Option<TraceContext>,
    ) -> Result<Self, CoreError> {
        metadata.validate()?;
        Ok(Self {
            metadata,
            payload,
            trace_context,
            cached_payload_json: OnceLock::new(),
        })
    }

    /// Restores the extracted remote parent onto a tracing span when available.
    pub fn set_span_parent(&self, span: &tracing::Span) {
        if let Some(trace_context) = &self.trace_context {
            trace_context.set_span_parent(span);
        }
    }

    /// Returns the bounded W3C trace context when present.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
    }

    /// Returns true when this message carries a W3C traceparent header.
    pub fn has_trace_context(&self) -> bool {
        self.trace_context.is_some()
    }
}
