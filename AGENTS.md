# Codex operating instructions

This file is authoritative for automated contributors working in this repository.

## Agent autonomy

When assigned an implementation task:

- Inspect the repository immediately.
- Do not ask permission to read files, edit workspace files, restore dependencies, compile, lint, or run tests.
- Implement, validate, and report the result in the same task.
- Resolve ordinary ambiguity using established project conventions.
- Prefer a reasonable working implementation over asking optional questions.
- Ask only when blocked by missing credentials, an irreversible/destructive operation, deployment or publication, remote mutations, material cost, or genuinely unknowable business requirements.
- Never ask "Should I proceed?", "Should I build?", or "Should I run tests?" when those actions are necessary to complete the requested task.

## WSL command execution

The Codex agent runs in Windows native mode.

The repository-local WSL distribution variable is:

```text
WSL_DISTRIBUTION=Ubuntu-24.04
```

Users may change this value to the name of their installed WSL distribution. Read the configured value from this section and substitute it for `<WSL_DISTRIBUTION>` in the commands below.

When Linux tooling is required, execute it through WSL using:

```powershell
wsl --distribution <WSL_DISTRIBUTION> -- bash -lc "<command>"
```

Use the following path translation:

- Windows repository path: `C:\github\<repository>`
- WSL repository path: `/mnt/c/github/<repository>`

Do not ask for confirmation before executing ordinary, non-destructive WSL commands
needed to inspect, build, lint, or test the project.

Prefer one complete WSL command rather than many small calls.

Examples:

```powershell
wsl --distribution <WSL_DISTRIBUTION> -- bash -lc `
  "cd /mnt/c/github/project && cargo fmt --check && cargo test"

wsl --distribution <WSL_DISTRIBUTION> -- bash -lc `
  "cd /mnt/c/github/project && dotnet build && dotnet test"
```

## Mission

Implement and harden a low-latency Kafka edge router without compromising tenant
isolation, bounded memory, deterministic matching, or explicit delivery semantics.

## Read order

Before editing:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/DELIVERY_SEMANTICS.md`
4. the selected file in `tasks/`
5. the affected crate's public types and tests

Do not start an unscoped refactor. Select one task and satisfy its acceptance criteria.

## Non-negotiable invariants

- Never introduce an unbounded channel, queue, collection fed by untrusted traffic, or
  retry loop without a hard cap.
- Never await socket, HTTP, disk, DNS, or Kafka producer I/O while holding a DashMap
  guard, mutex guard, or route-index borrow.
- Never parse payloads in the route matcher. Kafka headers are the routing plane.
- Never make tenant id wildcardable.
- Never trust a tenant supplied in a filter after authentication; rewrite it to the
  authenticated principal and reject mismatches.
- Never call outbound webhooks from the Kafka consumer loop.
- Never claim exactly-once or guaranteed end-to-end delivery without a persisted
  acknowledgement protocol and tests proving the state transitions.
- Never remove message ids from any protocol envelope.
- Never log bearer tokens, signing secrets, Kafka passwords, full payloads, or arbitrary
  authorization headers.
- Never enable HTTP redirects for webhooks.
- Never add `unsafe` without a dedicated ADR, benchmarks demonstrating need, and a
  safety review. Workspace lints currently deny it.

## Architecture boundaries

- `router-core` must remain transport-independent. It may depend on Tokio channels but
  not Axum, Tonic, Reqwest, or rdkafka.
- `router-kafka` translates between Kafka and core types. It must not know WebSocket,
  SSE, gRPC, or webhook details.
- `router-api` owns public protocols and authentication adapters. Business routing stays
  in core.
- `router-webhook` consumes core deliveries through the same bounded connection API as
  live transports.
- `routerd` only composes configuration, dependencies, listeners, signals, and tasks.
- `router-proto` owns the source `.proto`; generated code must never be edited manually.

## Performance rules

- Prefer `Arc<RoutedMessage>` and `Bytes` clones to payload copies.
- Encode only for destinations that need an encoding.
- One long-lived task per connection is acceptable; one task per message is not.
- Keep route matching O(candidate keys + matches), not O(all subscriptions).
- Bound request bodies, subscription counts, queue sizes, retries, and timeouts.
- Add a benchmark before replacing clear code with a lower-level optimization.
- Record the baseline, hardware, message size, fan-out, and command for every benchmark.

## Error and shutdown rules

- Invalid client input maps to HTTP 400 or gRPC `INVALID_ARGUMENT`.
- Authentication and authorization failures must be distinguishable.
- Backend details may be logged but public errors must not expose secrets.
- On `SIGTERM`, readiness becomes false before consumers and listeners drain.
- Any registration path must unregister on every error and cancellation path; prefer an
  RAII guard.
- A component exiting before shutdown is a daemon-level failure unless documented as
  optional and kept alive intentionally.

## Testing requirements

Every behavior change needs the narrowest useful test:

- pure matcher and validation rules: unit tests in `router-core`;
- Kafka header and key behavior: unit tests in `router-kafka`;
- protocol contracts: adapter tests or integration tests;
- lifecycle and race fixes: concurrency tests using barriers, not sleeps where possible;
- security fixes: positive and negative cases;
- performance changes: Criterion or a reproducible load scenario.

Required before a task is complete:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
python scripts/validate-repo.py
```

Run `cargo test --doc --workspace` when public examples or rustdoc change.

## Change protocol

- Keep public API changes intentional and document migration impact.
- Update the protobuf field numbers only by adding fields; never reuse or renumber a
  published field.
- Update `docs/IMPLEMENTATION_STATUS.md` when a task changes capability status.
- Update `CHANGELOG.md` under `Unreleased` for externally observable changes.
- Add or update an ADR for a delivery-semantic, topology, security-boundary, or storage
  decision.
- Use a conventional commit title from the task file.

## Definition of done

A task is done only when:

- all acceptance criteria are demonstrably met;
- required tests pass;
- failure paths and cancellation paths are covered;
- configuration and operational docs are accurate;
- no new unbounded resource exists;
- no placeholder `TODO` remains in the changed behavior unless the task explicitly
  creates a separately tracked follow-up; and
- the implementation-status matrix and changelog are current.
