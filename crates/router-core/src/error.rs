//! Core validation and state-transition failures.

use thiserror::Error;

/// Errors returned by the transport-independent router.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A string identifier was empty, oversized, or contained control characters.
    #[error("invalid {field}: {reason}")]
    InvalidIdentifier {
        /// Logical field name.
        field: &'static str,
        /// Human-readable validation reason.
        reason: String,
    },
    /// The requested connection does not exist.
    #[error("connection does not exist")]
    ConnectionNotFound,
    /// A subscription id already exists on the connection.
    #[error("subscription already exists")]
    SubscriptionExists,
    /// The requested subscription does not exist on the connection.
    #[error("subscription does not exist")]
    SubscriptionNotFound,
    /// The subscription tenant differs from the connection tenant.
    #[error("subscription tenant differs from connection tenant")]
    TenantMismatch,
    /// The configured subscription limit has been reached.
    #[error("connection subscription limit reached")]
    SubscriptionLimitReached,
    /// A process-wide or per-tenant connection limit has been reached.
    #[error("connection limit reached")]
    ConnectionLimitReached,
    /// A process-wide or per-tenant subscription limit has been reached.
    #[error("subscription limit reached")]
    GlobalSubscriptionLimitReached,
    /// A requested delivery queue is empty or exceeds the configured hard cap.
    #[error("queue capacity {requested} must be between 1 and {maximum}")]
    InvalidQueueCapacity {
        /// Requested queue slots.
        requested: usize,
        /// Process-wide maximum queue slots per connection.
        maximum: usize,
    },
    /// Both audience fields must be present together.
    #[error("audience_type and audience_id must either both be set or both be absent")]
    IncompleteAudience,
    /// A mandatory routing field is absent.
    #[error("missing mandatory routing field: {0}")]
    MissingField(&'static str),
}
