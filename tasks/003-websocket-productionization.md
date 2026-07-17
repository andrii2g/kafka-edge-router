# Task 003: Productionize WebSocket delivery

## Goal

Add explicit WebSocket resource limits, command-rate control, protocol compatibility tests,
and clear close behavior for slow or unauthorized clients.

## Scope

- WebSocket handler and command types in `router-api`
- API configuration
- adapter/integration tests
- protocol/security docs

## Required work

1. Cap requested queue capacity with a server-configured maximum.
2. Configure maximum frame/message size and reject oversized commands.
3. Add a token-bucket or equivalent bounded command-rate limiter per connection.
4. Add maximum subscription count error frames without closing a healthy connection.
5. Use stable application error codes and document them.
6. Send an intentional close code/reason when core evicts a slow consumer.
7. Test subscribe, duplicate subscribe, unsubscribe, ping, malformed JSON, binary input,
   tenant mismatch, queue saturation, cancellation, and reconnect.
8. Verify no task or core registration remains after disconnect.
9. Add optional compression only after measuring CPU and memory behavior; otherwise
   explicitly keep it disabled.

## Acceptance criteria

- untrusted query values cannot allocate arbitrarily large queues;
- protocol errors are deterministic and documented;
- tenant-isolation negative tests pass;
- connection cleanup is proven after every exit path; and
- a slow WebSocket cannot delay Kafka ingestion.

## Required commands

```bash
cargo test --locked -p router-api websocket
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
python scripts/validate-repo.py
```

## Commit title

```text
feat(ws): enforce bounded production protocol limits
```
