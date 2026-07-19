# Codex contribution prompt

Use this prompt from the repository root after defining the issue or change request:

> Read `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md`, and
> `docs/DELIVERY_SEMANTICS.md`, followed by the relevant issue, ADR, runbook, public types,
> and tests. Restate the requested behavior and acceptance criteria before editing. Keep
> the change within that scope, preserve every non-negotiable invariant, and follow
> established project patterns. Run all required validation commands and fix failures
> rather than suppressing checks. Update maintained documentation, the changelog, and an
> ADR when the change affects their contracts. Finish with a concise summary of changed
> files, commands run, results, remaining risks, and the proposed conventional commit
> title. Do not begin unrelated follow-up work.

One Codex session should normally produce one reviewable change. For release qualification,
use the checklist in `docs/RELEASE.md` and retain evidence as directed there.
