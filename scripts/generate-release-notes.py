#!/usr/bin/env python3
"""Combine curated release notes with a bounded commit changelog."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

MAX_COMMITS = 500


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", help="revision used for pre-tag validation")
    args = parser.parse_args()
    curated = Path("docs/releases") / f"{args.tag}.md"
    if not curated.is_file():
        raise SystemExit(f"missing {curated}")
    target = args.revision or args.tag
    tags = [tag for tag in git("tag", "--sort=-version:refname").splitlines() if tag != args.tag]
    previous = next(
        (tag for tag in tags if subprocess.run(
            ["git", "merge-base", "--is-ancestor", tag, target], check=False
        ).returncode == 0),
        None,
    )
    revision = f"{previous}..{target}" if previous else target
    commits = git("log", revision, f"--max-count={MAX_COMMITS + 1}", "--pretty=- %s (`%h`)").splitlines()
    if len(commits) > MAX_COMMITS:
        raise SystemExit(f"release changelog exceeds {MAX_COMMITS} commits")
    body = curated.read_text(encoding="utf-8").rstrip()
    body += "\n\n## Commit changelog\n\n"
    body += "\n".join(commits) if commits else "No commits since the previous tag."
    body += "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body, encoding="utf-8")


if __name__ == "__main__":
    main()