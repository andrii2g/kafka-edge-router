#!/usr/bin/env python3
"""Validate that a release tag exactly matches the locked workspace version."""

from __future__ import annotations

import argparse
import re
import subprocess
import tomllib
from pathlib import Path

TAG_PATTERN = re.compile(r"^v(?P<version>0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-rc\.[1-9]\d*)?$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    match = TAG_PATTERN.fullmatch(args.tag)
    if match is None:
        raise SystemExit("tag must be vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-rc.N")
    manifest = tomllib.loads(Path("Cargo.toml").read_text(encoding="utf-8"))
    version = manifest["workspace"]["package"]["version"]
    tagged_base = args.tag.removeprefix("v").split("-", 1)[0]
    if tagged_base != version:
        raise SystemExit(f"tag base {tagged_base} does not match workspace version {version}")
    notes = Path("docs/releases") / f"{args.tag}.md"
    if not notes.is_file():
        raise SystemExit(f"missing curated release notes: {notes}")
    subprocess.run(["cargo", "metadata", "--locked", "--no-deps"], check=True)
    tag_commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{args.tag}^{{commit}}"], text=True
    ).strip()
    head_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if tag_commit != head_commit:
        raise SystemExit(f"tag {args.tag} does not resolve to checked-out HEAD")


if __name__ == "__main__":
    main()