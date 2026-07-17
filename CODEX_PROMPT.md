# Initial Codex prompt

Use this prompt from the repository root:

> Read `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md`, and
> `docs/DELIVERY_SEMANTICS.md`. Then execute `tasks/000-bootstrap-and-compile.md` exactly.
> Work only within that task's scope. Inspect current code before editing. Preserve all
> non-negotiable invariants. Run every required validation command, fix failures rather
> than suppressing lints, update the implementation-status matrix and changelog, and
> finish with a concise summary containing changed files, commands run, test results,
> remaining risks, and the proposed conventional commit title. Do not begin task 001.

For later tasks, replace the task path and explicitly prohibit starting the next task.
One Codex session should normally produce one reviewable commit.
