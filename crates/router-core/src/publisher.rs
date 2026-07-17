//! Transport-neutral publish API used by HTTP and gRPC adapters.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Command accepted by a configured message publisher.
#[derive(Clone, Debug)]
pub struct PublishCommand {
    /// Optional caller-supplied idempotency key.
    pub message_id: Option<Arc<str>>,
    /// Authorized tenant.
    pub tenant_id: Arc<str>,
    /// Optional routing dimensions.
    pub kind: Option<Arc<str>>,
    /// Optional routing dimensions.
    pub message_type: Option<Arc<str>>,
    /// Optional routing dimensions.
    pub channel: Option<Arc<str>>,
    /// Optional routing dimensions.
    pub actor_id: Option<Arc<str>>,
    /// Optional routing dimensions.
    pub audience_type: Option<Arc<str>>,
    /// Optional routing dimensions.
    pub audience_id: Option<Arc<str>>,
    /// Payload MIME type.
    pub content_type: Arc<str>,
    /// Raw payload bytes.
    pub payload: Bytes,
}

/// Kafka acknowledgement returned to a caller.
#[derive(Clone, Debug)]
pub struct PublishReceipt {
    /// Effective message id.
    pub message_id: String,
    /// Kafka topic.
    pub topic: String,
    /// Kafka partition.
    pub partition: i32,
    /// Kafka offset.
    pub offset: i64,
}

/// Publish backend failure safe to map to a protocol error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishErrorKind {
    /// Caller-supplied metadata violates the message contract.
    InvalidInput,
    /// Kafka or another configured backend failed.
    Backend,
}

/// Publish failure with a stable classification for protocol adapters.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct PublishError {
    kind: PublishErrorKind,
    message: String,
}

impl PublishError {
    /// Creates an invalid-input failure.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            kind: PublishErrorKind::InvalidInput,
            message: message.into(),
        }
    }

    /// Creates a backend failure. Adapters should log this message and return a generic
    /// public error because backend diagnostics can contain deployment details.
    pub fn backend(message: impl Into<String>) -> Self {
        Self {
            kind: PublishErrorKind::Backend,
            message: message.into(),
        }
    }

    /// Returns the stable failure classification.
    pub const fn kind(&self) -> PublishErrorKind {
        self.kind
    }
}

/// Asynchronous publisher abstraction implemented by Kafka.
#[async_trait]
pub trait MessagePublisher: Send + Sync {
    /// Publishes one command and waits for broker acknowledgement.
    async fn publish(&self, command: PublishCommand) -> Result<PublishReceipt, PublishError>;
}
