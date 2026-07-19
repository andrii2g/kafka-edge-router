# ADR 0002: Route from Kafka headers, not payload expressions

- Status: accepted
- Date: 2026-07-17

## Context

Routing based on arbitrary JSON paths or expressions would require payload parsing,
format-specific logic, expression limits, and a more complex authorization surface.
Large or adversarial payloads would directly affect matcher latency.

## Decision

The producer places the routing plane in bounded UTF-8 Kafka headers. The router treats
the payload as opaque bytes until a protocol adapter encodes it for a destination.
Subscriptions support exact values and wildcards over five optional logical dimensions.

## Consequences

- matching cost is predictable and payload-format independent;
- producers must maintain the header contract;
- content-based routing needs an upstream normalization service; and
- header evolution must be versioned carefully.
