# ADR 0006: Model recipient routing as an extensible atomic pair

- Status: accepted
- Date: 2026-07-19

## Context

Routing needs to target an optional recipient described by a category and an identity.
Valid categories include `audience`, `team`, `superteam`, and domain-specific values not
known by the router.

Treating category and identity as independent matcher dimensions would generate route
candidates for invalid half-pairs. A generic runtime map of filterable headers would be
more flexible, but it would make memory cardinality and the exponential `2^n` candidate
bound dependent on untrusted input.

## Decision

- Name the public fields `recipient_type` and `recipient_identity`.
- Encode them as Kafka headers `x-recipient-type` and `x-recipient-identity`.
- Require both values together and compile them into one atomic `RecipientKey` dimension.
- Keep recipient types as bounded, case-sensitive strings rather than an enum.
- Assign stable protobuf field numbers and never reuse or renumber them after publication.
- Keep routing dimensions schema-defined. A new dimension requires an explicit code and
  contract change, a fixed upper bound, property tests, protocol tests, and benchmarks.
- Continue to prohibit tenant wildcards, runtime-defined routing keys, and payload
  expression matching.

## Consequences

- `audience`, `team`, `superteam`, and future recipient categories work without router
  changes.
- Fully populated messages require at most 32 candidate lookups.
- Kafka, JSON, query, configuration, protobuf, and generated-source names use one
  consistent recipient vocabulary.
- Adding an independent routing dimension doubles worst-case candidate lookups and cannot
  be introduced as an unreviewed configuration option.

## Revisit trigger

Revisit the fixed schema only when a concrete use case cannot be represented by the
existing dimensions or open recipient vocabulary. Any proposal must preserve tenant
isolation, bounded memory, deterministic matching, and a benchmarked candidate limit.