# ADR 0001: Use one Kafka consumer group per router node

- Status: accepted
- Date: 2026-07-17

## Context

Clients connect to arbitrary router nodes. In a shared Kafka consumer group, only the
node owning a partition sees its records, so it would need a distributed subscription
registry and peer forwarding to reach clients on other nodes.

## Decision

The first topology gives every router node a unique consumer group. Every node consumes
the complete input stream and dispatches only to its local connections.

## Consequences

Positive:

- no distributed subscription state;
- no peer protocol or cluster membership;
- simple node failure and load balancing;
- local route lookups only.

Negative:

- Kafka reads, payload copies, and matching multiply by node count;
- operators must guarantee unique group ids; and
- adding nodes increases broker egress.

## Revisit trigger

Revisit only after production measurements show duplicated work materially limits broker
or router capacity. A replacement requires a separate ADR covering aggregate route
advertisement, peer backpressure, generation fencing, and failure recovery.
