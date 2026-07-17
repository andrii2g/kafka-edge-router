//! Exact-and-wildcard route filters and candidate-key generation.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{ids::validate_identifier, CoreError, RoutingMetadata};

/// A tenant-scoped subscription. `None` on an optional dimension means wildcard.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct RouteFilter {
    /// Mandatory tenant boundary.
    pub tenant_id: Arc<str>,
    /// Optional exact kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<Arc<str>>,
    /// Optional exact type.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub message_type: Option<Arc<str>>,
    /// Optional exact channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Arc<str>>,
    /// Optional exact actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<Arc<str>>,
    /// Optional exact audience category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience_type: Option<Arc<str>>,
    /// Optional exact audience id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience_id: Option<Arc<str>>,
}

impl RouteFilter {
    /// Validates the filter without inspecting payload data.
    pub fn validate(&self) -> Result<(), CoreError> {
        validate_identifier("tenant_id", &self.tenant_id, 256)?;
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

    /// Reference matcher used by tests and diagnostics.
    pub fn matches(&self, metadata: &RoutingMetadata) -> bool {
        self.tenant_id == metadata.tenant_id
            && optional_matches(&self.kind, &metadata.kind)
            && optional_matches(&self.message_type, &metadata.message_type)
            && optional_matches(&self.channel, &metadata.channel)
            && optional_matches(&self.actor_id, &metadata.actor_id)
            && optional_matches(&self.audience_type, &metadata.audience_type)
            && optional_matches(&self.audience_id, &metadata.audience_id)
    }
}

fn optional_matches(expected: &Option<Arc<str>>, actual: &Option<Arc<str>>) -> bool {
    expected.as_ref().is_none_or(|expected| actual.as_ref() == Some(expected))
}

/// Hash key stored in the compiled subscription index.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteKey {
    tenant_id: Arc<str>,
    kind: Option<Arc<str>>,
    message_type: Option<Arc<str>>,
    channel: Option<Arc<str>>,
    actor_id: Option<Arc<str>>,
    audience_type: Option<Arc<str>>,
    audience_id: Option<Arc<str>>,
}

impl From<&RouteFilter> for RouteKey {
    fn from(filter: &RouteFilter) -> Self {
        Self {
            tenant_id: Arc::clone(&filter.tenant_id),
            kind: filter.kind.clone(),
            message_type: filter.message_type.clone(),
            channel: filter.channel.clone(),
            actor_id: filter.actor_id.clone(),
            audience_type: filter.audience_type.clone(),
            audience_id: filter.audience_id.clone(),
        }
    }
}

impl RouteKey {
    /// Expands a message into exact/wildcard keys. With six populated optional
    /// dimensions, this creates exactly 64 direct hash lookups.
    pub fn candidates(metadata: &RoutingMetadata) -> SmallVec<[Self; 64]> {
        let mut keys = SmallVec::new();
        keys.push(Self {
            tenant_id: Arc::clone(&metadata.tenant_id),
            kind: metadata.kind.clone(),
            message_type: metadata.message_type.clone(),
            channel: metadata.channel.clone(),
            actor_id: metadata.actor_id.clone(),
            audience_type: metadata.audience_type.clone(),
            audience_id: metadata.audience_id.clone(),
        });

        macro_rules! branch_wildcard {
            ($field:ident, $populated:expr) => {
                if $populated {
                    let original_len = keys.len();
                    for index in 0..original_len {
                        let mut wildcard = keys[index].clone();
                        wildcard.$field = None;
                        keys.push(wildcard);
                    }
                }
            };
        }

        branch_wildcard!(kind, metadata.kind.is_some());
        branch_wildcard!(message_type, metadata.message_type.is_some());
        branch_wildcard!(channel, metadata.channel.is_some());
        branch_wildcard!(actor_id, metadata.actor_id.is_some());
        branch_wildcard!(audience_type, metadata.audience_type.is_some());
        branch_wildcard!(audience_id, metadata.audience_id.is_some());
        keys
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{RouteFilter, RouteKey};
    use crate::RoutingMetadata;

    fn metadata() -> RoutingMetadata {
        RoutingMetadata {
            message_id: Arc::from("m-1"),
            tenant_id: Arc::from("tenant-a"),
            kind: Some(Arc::from("content")),
            message_type: Some(Arc::from("broadcast")),
            channel: Some(Arc::from("news")),
            actor_id: Some(Arc::from("u-1")),
            audience_type: Some(Arc::from("team")),
            audience_id: Some(Arc::from("team-7")),
            content_type: Arc::from("application/json"),
            timestamp_ms: None,
            source: None,
        }
    }

    #[test]
    fn creates_all_exact_and_wildcard_candidates() {
        let candidates = RouteKey::candidates(&metadata());
        assert_eq!(candidates.len(), 64);
    }

    #[test]
    fn wildcard_filter_matches_reference_matcher() {
        let filter = RouteFilter {
            tenant_id: Arc::from("tenant-a"),
            kind: Some(Arc::from("content")),
            message_type: None,
            channel: Some(Arc::from("news")),
            actor_id: None,
            audience_type: None,
            audience_id: None,
        };
        assert!(filter.matches(&metadata()));
    }

    #[test]
    fn tenant_boundary_never_wildcards() {
        let filter = RouteFilter {
            tenant_id: Arc::from("tenant-b"),
            kind: None,
            message_type: None,
            channel: None,
            actor_id: None,
            audience_type: None,
            audience_id: None,
        };
        assert!(!filter.matches(&metadata()));
    }
}
