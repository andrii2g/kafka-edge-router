//! Strongly typed connection and subscription identifiers.

use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CoreError;

/// Opaque identifier for one protocol connection or webhook worker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ConnectionId(Uuid);

impl ConnectionId {
    /// Generates a random version-4 identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ConnectionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Client-defined identifier for one filter on a connection.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SubscriptionId(Arc<str>);

impl SubscriptionId {
    /// Validates and constructs a subscription id.
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, CoreError> {
        let value = value.into();
        validate_identifier("subscription_id", &value, 128)?;
        Ok(Self(value))
    }

    /// Returns the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

pub(crate) fn validate_identifier(
    field: &'static str,
    value: &str,
    max_len: usize,
) -> Result<(), CoreError> {
    if value.trim().is_empty() {
        return Err(CoreError::InvalidIdentifier {
            field,
            reason: "must not be empty".to_owned(),
        });
    }
    if value.len() > max_len {
        return Err(CoreError::InvalidIdentifier {
            field,
            reason: format!("must not exceed {max_len} bytes"),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(CoreError::InvalidIdentifier {
            field,
            reason: "must not contain control characters".to_owned(),
        });
    }
    Ok(())
}
