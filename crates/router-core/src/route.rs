//! Exact-and-wildcard route filters and candidate-key generation.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{ids::validate_identifier, CoreError, RoutingMetadata};

/// A tenant-scoped subscription. `None` on an optional dimension means wildcard.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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
    /// Optional exact recipient category, paired with `recipient_identity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient_type: Option<Arc<str>>,
    /// Optional exact recipient identity, paired with `recipient_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient_identity: Option<Arc<str>>,
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
            && recipient_matches(
                self.recipient_type.as_deref(),
                self.recipient_identity.as_deref(),
                metadata.recipient_type.as_deref(),
                metadata.recipient_identity.as_deref(),
            )
    }
}

fn optional_matches(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected.is_none_or(|expected| actual == Some(expected))
}

fn recipient_matches(
    expected_type: Option<&str>,
    expected_identity: Option<&str>,
    actual_type: Option<&str>,
    actual_identity: Option<&str>,
) -> bool {
    match (expected_type, expected_identity) {
        (None, None) => true,
        (Some(expected_type), Some(expected_identity)) => {
            actual_type == Some(expected_type) && actual_identity == Some(expected_identity)
        }
        _ => false,
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RecipientKey {
    recipient_type: Arc<str>,
    recipient_identity: Arc<str>,
}

fn recipient_key(
    recipient_type: Option<&Arc<str>>,
    recipient_identity: Option<&Arc<str>>,
) -> Option<RecipientKey> {
    match (recipient_type, recipient_identity) {
        (Some(recipient_type), Some(recipient_identity)) => Some(RecipientKey {
            recipient_type: Arc::clone(recipient_type),
            recipient_identity: Arc::clone(recipient_identity),
        }),
        _ => None,
    }
}

/// Hash key stored in the compiled subscription index.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteKey {
    tenant_id: Arc<str>,
    kind: Option<Arc<str>>,
    message_type: Option<Arc<str>>,
    channel: Option<Arc<str>>,
    actor_id: Option<Arc<str>>,
    recipient: Option<RecipientKey>,
}

impl From<&RouteFilter> for RouteKey {
    fn from(filter: &RouteFilter) -> Self {
        Self {
            tenant_id: Arc::clone(&filter.tenant_id),
            kind: filter.kind.clone(),
            message_type: filter.message_type.clone(),
            channel: filter.channel.clone(),
            actor_id: filter.actor_id.clone(),
            recipient: recipient_key(
                filter.recipient_type.as_ref(),
                filter.recipient_identity.as_ref(),
            ),
        }
    }
}

impl RouteKey {
    /// Expands a message into exact/wildcard keys. With five populated optional
    /// dimensions, this creates exactly 32 direct hash lookups.
    pub fn candidates(metadata: &RoutingMetadata) -> SmallVec<[Self; 32]> {
        let mut keys = SmallVec::<[Self; 32]>::new();
        keys.push(Self {
            tenant_id: Arc::clone(&metadata.tenant_id),
            kind: metadata.kind.clone(),
            message_type: metadata.message_type.clone(),
            channel: metadata.channel.clone(),
            actor_id: metadata.actor_id.clone(),
            recipient: recipient_key(
                metadata.recipient_type.as_ref(),
                metadata.recipient_identity.as_ref(),
            ),
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
        branch_wildcard!(recipient, metadata.recipient_type.is_some());
        keys
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use super::{RouteFilter, RouteKey};
    use crate::RoutingMetadata;

    const OPTIONAL_DIMENSIONS: u32 = 5;

    fn optional(mask: u32, bit: u32, value: &'static str) -> Option<Arc<str>> {
        (mask & (1 << bit) != 0).then(|| Arc::from(value))
    }

    fn recipient(mask: u32, bit: u32) -> (Option<Arc<str>>, Option<Arc<str>>) {
        if mask & (1 << bit) != 0 {
            (Some(Arc::from("team")), Some(Arc::from("team-7")))
        } else {
            (None, None)
        }
    }

    fn metadata_for_mask(mask: u32, tenant: &str) -> RoutingMetadata {
        let (recipient_type, recipient_identity) = recipient(mask, 4);
        RoutingMetadata {
            message_id: Arc::from("m-1"),
            tenant_id: Arc::from(tenant),
            kind: optional(mask, 0, "content"),
            message_type: optional(mask, 1, "broadcast"),
            channel: optional(mask, 2, "news"),
            actor_id: optional(mask, 3, "u-1"),
            recipient_type,
            recipient_identity,
            content_type: Arc::from("application/json"),
            timestamp_ms: None,
            source: None,
        }
    }

    fn filter_for_mask(mask: u32, tenant: &str) -> RouteFilter {
        let (recipient_type, recipient_identity) = recipient(mask, 4);
        RouteFilter {
            tenant_id: Arc::from(tenant),
            kind: optional(mask, 0, "content"),
            message_type: optional(mask, 1, "broadcast"),
            channel: optional(mask, 2, "news"),
            actor_id: optional(mask, 3, "u-1"),
            recipient_type,
            recipient_identity,
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
                    "message mask {message_mask:05b}, filter mask {filter_mask:05b}"
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
            assert_eq!(unique.len(), candidates.len(), "mask {mask:05b}");
            assert!(
                candidates
                    .iter()
                    .all(|candidate| candidate.tenant_id == metadata.tenant_id),
                "tenant must never branch for mask {mask:05b}"
            );
        }
    }

    #[test]
    fn recipient_types_are_open_exact_values() {
        let mut metadata = metadata_for_mask(1 << 4, "tenant-a");
        metadata.recipient_type = Some(Arc::from("superteam"));
        metadata.recipient_identity = Some(Arc::from("bca321"));

        let mut filter = filter_for_mask(1 << 4, "tenant-a");
        filter.recipient_type = Some(Arc::from("superteam"));
        filter.recipient_identity = Some(Arc::from("bca321"));
        assert!(filter.matches(&metadata));
        assert!(RouteKey::candidates(&metadata).contains(&RouteKey::from(&filter)));

        filter.recipient_type = Some(Arc::from("team"));
        assert!(!filter.matches(&metadata));
        assert!(!RouteKey::candidates(&metadata).contains(&RouteKey::from(&filter)));
    }

    #[test]
    fn randomized_index_equivalence_and_tenant_isolation() {
        let mut random = Lcg(0x5eed_cafe_f00d_beef);
        for case in 0..10_000 {
            let message_mask = (random.next() & 0x1f) as u32;
            let filter_mask = (random.next() & 0x1f) as u32;
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
                    4 if filter.recipient_type.is_some() => {
                        if random.next() & 1 == 0 {
                            filter.recipient_type = Some(Arc::from("other"));
                        } else {
                            filter.recipient_identity = Some(Arc::from("other"));
                        }
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
