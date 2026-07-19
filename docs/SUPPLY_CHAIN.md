# Software supply-chain policy

This policy separates four questions that scanners often blur together:

1. **Identity:** is this the exact source or artifact that was reviewed?
2. **Provenance:** who built or published it, and through which workflow?
3. **Known risk:** do current advisory databases report a vulnerability?
4. **Code trust:** has the source been audited or explicitly accepted as a baseline?

No single tool answers all four.

## Enforced controls

| Input | Identity and policy control |
| --- | --- |
| Rust crates | `Cargo.lock` checksums, `--locked`, crates.io-only source policy, RustSec, license policy, and cargo-vet |
| GitHub Actions | full 40-character commit SHA; the trailing version comment is update metadata only |
| build and runtime images | readable tag plus immutable manifest digest |
| release image | immutable digest, Trivy HIGH/CRITICAL gate, keyless Cosign signature, and GitHub build attestation |
| Kubernetes rollout | signature and attestation verification before the exact digest is applied |

Dependabot opens weekly Cargo, Actions, and Docker update PRs. GitHub dependency review
rejects PRs that introduce dependencies with known vulnerabilities of moderate severity
or higher. Repository validation prevents mutable Action and container references from
being reintroduced.

## Rust trust baseline

`cargo vet` imports published audits from Google, Mozilla, Bytecode Alliance, and ISRG.
Imported audits are third-party claims and remain explicit trust decisions; they are not
endorsements by this project.

The initial `supply-chain/config.toml` exemptions record dependencies accepted when this
policy was adopted on 2026-07-19. An exemption means **accepted baseline**, not **source
audited**. Cargo-vet removes exemptions when an imported or local audit covers the same
version. Every new crate or version must be covered by an audit, a trusted imported audit,
or a deliberately reviewed exemption in the PR.

Reviewers should inspect maintainer identity, repository and crates.io ownership,
publication history, build scripts, native code, unsafe code, enabled features, network or
filesystem access, transitive impact, and the diff from the previously accepted version.
Security-sensitive crates deserve a local cargo-vet audit instead of a routine exemption.

## Commands

Install the exact policy tool versions used by CI:

```bash
cargo install cargo-deny --version 0.20.2 --locked
cargo install cargo-vet --version 0.10.2 --locked
```

Run:

```bash
cargo deny check
cargo vet --locked
cargo audit
python scripts/validate-repo.py
```

Use `cargo vet suggest` to find published audits before adding an exemption. Keep
`supply-chain/imports.lock` committed so CI evaluates the same imported audit data.

## Exceptions

There are no vulnerability exceptions at policy adoption. Future exceptions must be
narrowly scoped to an advisory ID or image finding and include the affected component,
risk analysis, compensating control, owner, approval date, and expiry or removal condition.
A broad scanner disable, mutable version reference, or undocumented ignore is prohibited.

`CODEOWNERS` identifies security-sensitive files. Because the repository currently has one
maintainer, branch protection does not require self-approval; automated security checks are
the merge gate. Require code-owner approval after a second qualified maintainer is added.
