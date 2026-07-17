## Summary

## Selected task

- [ ] I linked exactly one primary task from `tasks/` or explained why this is an emergency fix.

## Semantics and risk

- Delivery-semantic change:
- Tenant/security-boundary change:
- Queue/memory-bound change:
- Protobuf compatibility impact:

## Validation

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `python scripts/validate-repo.py`
- [ ] Relevant integration/load tests

## Documentation

- [ ] `CHANGELOG.md`
- [ ] `docs/IMPLEMENTATION_STATUS.md`
- [ ] ADR added/updated when required
