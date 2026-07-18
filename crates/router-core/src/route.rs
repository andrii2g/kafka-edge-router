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
            && optional_matches(self.kind.as_deref(), metadata.kind.as_deref())
            && optional_matches(
                self.message_type.as_deref(),
                metadata.message_type.as_deref(),
            )
            && optional_matches(self.channel.as_deref(), metadata.channel.as_deref())
            && optional_matches(self.actor_id.as_deref(), metadata.actor_id.as_deref())
            && optional_matches(
                self.audience_type.as_deref(),
                metadata.audience_type.as_deref(),
            )
            && optional_matches(self.audience_id.as_deref(), metadata.audience_id.as_deref())
    }
}

fn optional_matches(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected.is_none_or(|expected| actual == Some(expected))
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
        let mut keys = SmallVec::<[Self; 64]>::new();
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
    use std::{collections::HashSet, sync::Arc};

    use super::{RouteFilter, RouteKey};
    use crate::RoutingMetadata;

    const OPTIONAL_DIMENSIONS: u32 = 6;

    fn optional(mask: u32, bit: u32, value: &'static str) -> Option<Arc<str>> {
        (mask & (1 << bit) != 0).then(|| Arc::from(value))
    }

    fn metadata_for_mask(mask: u32, tenant: &str) -> RoutingMetadata {
        RoutingMetadata {
            message_id: Arc::from("m-1"),
            tenant_id: Arc::from(tenant),
            kind: optional(mask, 0, "content"),
            message_type: optional(mask, 1, "broadcast"),
            channel: optional(mask, 2, "news"),
            actor_id: optional(mask, 3, "u-1"),
            audience_type: optional(mask, 4, "team"),
            audience_id: optional(mask, 5, "team-7"),
            content_type: Arc::from("application/json"),
            timestamp_ms: None,
            source: None,
        }
    }

    fn filter_for_mask(mask: u32, tenant: &str) -> RouteFilter {
        RouteFilter {
            tenant_id: Arc::from(tenant),
            kind: optional(mask, 0, "content"),
            message_type: optional(mask, 1, "broadcast"),
            channel: optional(mask, 2, "news"),
            actor_id: optional(mask, 3, "u-1"),
            audience_type: optional(mask, 4, "team"),
            audience_id: optional(mask, 5, "team-7"),
        }
    }

    #[derive(Clone, Copy)]
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }
    }

    #[test]
    fn indexed_candidates_match_reference_for_every_dimension_combination() {
        for message_mask in 0..(1 << OPTIONAL_DIMENSIONS) {
            let metadata = metadata_for_mask(message_mask, "tenant-a");
            let candidates = RouteKey::candidates(&metadata);
            for filter_mask in 0..(1 << OPTIONAL_DIMENSIONS) {
                let filter = filter_for_mask(filter_mask, "tenant-a");
                let indexed = candidates.contains(&RouteKey::from(&filter));
                assert_eq!(
                    indexed,
                    filter.matches(&metadata),
                    "message mask {message_mask:06b}, filter mask {filter_mask:06b}"
                );
            }
        }
    }

    #[test]
    fn candidate_count_and_uniqueness_hold_for_every_message_shape() {
        for mask in 0..(1 << OPTIONAL_DIMENSIONS) {
            let metadata = metadata_for_mask(mask, "tenant-a");
            let candidates = RouteKey::candidates(&metadata);
            let unique: HashSet<_> = candidates.iter().collect();
            assert_eq!(candidates.len(), 1 << mask.count_ones());
            assert_eq!(unique.len(), candidates.len(), "mask {mask:06b}");
            assert!(
                candidates
                    .iter()
                    .all(|candidate| candidate.tenant_id == metadata.tenant_id),
                "tenant must never branch for mask {mask:06b}"
            );
        }
    }

    #[test]
    fn randomized_index_equivalence_and_tenant_isolation() {
        let mut random = Lcg(0x5eed_cafe_f00d_beef);
        for case in 0..10_000 {
            let message_mask = (random.next() & 0x3f) as u32;
            let filter_mask = (random.next() & 0x3f) as u32;
            let same_tenant = random.next() & 1 == 0;
            let metadata = metadata_for_mask(message_mask, "tenant-a");
            let mut filter = filter_for_mask(
                filter_mask,
                if same_tenant { "tenant-a" } else { "tenant-b" },
            );

            if random.next() % 4 == 0 {
                match random.next() % u64::from(OPTIONAL_DIMENSIONS) {
                    0 if filter.kind.is_some() => filter.kind = Some(Arc::from("other")),
                    1 if filter.message_type.is_some() => {
                        filter.message_type = Some(Arc::from("other"));
                    }
                    2 if filter.channel.is_some() => filter.channel = Some(Arc::from("other")),
                    3 if filter.actor_id.is_some() => filter.actor_id = Some(Arc::from("other")),
                    4 if filter.audience_type.is_some() => {
                        filter.audience_type = Some(Arc::from("other"));
                    }
                    5 if filter.audience_id.is_some() => {
                        filter.audience_id = Some(Arc::from("other"));
                    }
                    _ => {}
                }
            }

            let candidates = RouteKey::candidates(&metadata);
            let indexed = candidates.contains(&RouteKey::from(&filter));
            assert_eq!(indexed, filter.matches(&metadata), "random case {case}");
            if !same_tenant {
                assert!(!indexed, "cross-tenant candidate in random case {case}");
            }
            let unique: HashSet<_> = candidates.iter().collect();
            assert_eq!(unique.len(), candidates.len(), "random case {case}");
        }
    }
}
