#!/usr/bin/env python3
"""Validate repository structure and text artifacts without invoking Cargo."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import tomllib
from pathlib import Path

import yaml
from tree_sitter import Language, Parser
import tree_sitter_rust

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []
EXCLUDED_PARTS = {".git", ".vs", "target"}


def repository_paths(pattern: str):
    """Yield repository paths while excluding generated and VCS directories."""
    for path in ROOT.rglob(pattern):
        if EXCLUDED_PARTS.isdisjoint(path.relative_to(ROOT).parts):
            yield path

EXPECTED = [
    ".editorconfig",
    ".gitattributes",
    "Cargo.toml",
    "rust-toolchain.toml",
    "README.md",
    "AGENTS.md",
    "docs/contributing/CODEX_PROMPT.md",
    "compose.yaml",
    "Dockerfile",
    "config/router.toml",
    "crates/router-core/src/router.rs",
    "crates/router-kafka/src/ingestor.rs",
    "crates/router-api/src/http.rs",
    "crates/router-api/src/grpc.rs",
    "crates/router-webhook/src/manager.rs",
    "crates/router-proto/proto/router/v1/router.proto",
    "crates/routerd/src/main.rs",
    "deploy/kubernetes/base/deployment.yaml",
    "deploy/kubernetes/kustomization.yaml",
    "deploy/kubernetes/overlays/rc/kustomization.yaml",
]

TEXT_SUFFIXES = {
    ".md", ".rs", ".toml", ".yaml", ".yml", ".json", ".sh", ".proto", ".html",
    ".txt", ".service",
}


def error(message: str) -> None:
    ERRORS.append(message)


def check_expected() -> None:
    for relative in EXPECTED:
        if not (ROOT / relative).is_file():
            error(f"missing expected file: {relative}")


def check_text_files() -> None:
    for path in sorted(repository_paths("*")):
        if not path.is_file() or ".git" in path.parts:
            continue
        if path.stat().st_size == 0:
            error(f"empty file: {path.relative_to(ROOT)}")
        if path.suffix not in TEXT_SUFFIXES and path.name not in {
            "Dockerfile", "Makefile", ".gitignore", ".dockerignore", "CODEOWNERS"
        }:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            error(f"non-UTF-8 text file {path.relative_to(ROOT)}: {exc}")
            continue
        if "\r\n" in text:
            error(f"CRLF line endings: {path.relative_to(ROOT)}")
        for number, line in enumerate(text.splitlines(), start=1):
            if line.rstrip() != line:
                error(f"trailing whitespace: {path.relative_to(ROOT)}:{number}")
            if "\t" in line and path.suffix != ".md" and path.name != "Makefile":
                error(f"tab character: {path.relative_to(ROOT)}:{number}")


def check_toml() -> None:
    for path in sorted(repository_paths("*.toml")):
        try:
            with path.open("rb") as stream:
                tomllib.load(stream)
        except Exception as exc:  # noqa: BLE001 - validation tool reports parser detail
            error(f"invalid TOML {path.relative_to(ROOT)}: {exc}")


def check_yaml() -> None:
    for suffix in ("*.yaml", "*.yml"):
        for path in sorted(repository_paths(suffix)):
            try:
                with path.open(encoding="utf-8") as stream:
                    list(yaml.safe_load_all(stream))
            except Exception as exc:  # noqa: BLE001
                error(f"invalid YAML {path.relative_to(ROOT)}: {exc}")


def check_json() -> None:
    for path in sorted(repository_paths("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            error(f"invalid JSON {path.relative_to(ROOT)}: {exc}")


def check_shell() -> None:
    for path in sorted((ROOT / "scripts").glob("*.sh")):
        relative = path.relative_to(ROOT).as_posix()
        result = subprocess.run(
            ["bash", "-n", relative],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            error(f"shell syntax {path.relative_to(ROOT)}: {result.stderr.strip()}")
        if os.name == "nt":
            tracked = subprocess.run(
                ["git", "ls-files", "--stage", "--", relative],
                text=True,
                capture_output=True,
                check=True,
            ).stdout.split()
            if not tracked:
                continue
            executable = tracked[0] == "100755"
        else:
            executable = bool(path.stat().st_mode & stat.S_IXUSR)
        if not executable:
            error(f"script is not executable: {path.relative_to(ROOT)}")


def check_rust_syntax() -> None:
    parser = Parser(Language(tree_sitter_rust.language()))
    for path in sorted(repository_paths("*.rs")):
        tree = parser.parse(path.read_bytes())
        if tree.root_node.has_error:
            error(f"Rust syntax tree contains an error: {path.relative_to(ROOT)}")


def check_markdown_links() -> None:
    pattern = re.compile(r"!?(?:\[[^\]]*\])\(([^)]+)\)")
    for path in sorted(repository_paths("*.md")):
        text = path.read_text(encoding="utf-8")
        for raw in pattern.findall(text):
            target = raw.strip().split()[0].strip("<>")
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = target.split("#", 1)[0]
            if not target:
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT.resolve())
            except ValueError:
                error(f"markdown link escapes repository: {path.relative_to(ROOT)} -> {raw}")
                continue
            if not resolved.exists():
                error(f"broken markdown link: {path.relative_to(ROOT)} -> {raw}")


def check_workspace_members() -> None:
    with (ROOT / "Cargo.toml").open("rb") as stream:
        root_manifest = tomllib.load(stream)
    for member in root_manifest.get("workspace", {}).get("members", []):
        manifest = ROOT / member / "Cargo.toml"
        if not manifest.is_file():
            error(f"workspace member missing Cargo.toml: {member}")


def check_proto() -> None:
    proto = ROOT / "crates/router-proto/proto/router/v1/router.proto"
    text = proto.read_text(encoding="utf-8")
    if 'package router.v1;' not in text:
        error("protobuf package must remain router.v1")
    if "service KafkaRouter" not in text:
        error("protobuf KafkaRouter service is missing")
    field_numbers: dict[tuple[str, int], int] = {}
    current_message = ""
    for line in text.splitlines():
        message = re.match(r"\s*message\s+(\w+)", line)
        if message:
            current_message = message.group(1)
        field = re.search(r"=\s*(\d+)\s*;", line)
        if field and current_message:
            number = int(field.group(1))
            key = (current_message, number)
            field_numbers[key] = field_numbers.get(key, 0) + 1
    duplicates = [key for key, count in field_numbers.items() if count > 1]
    if duplicates:
        error(f"duplicate protobuf field numbers: {duplicates}")


def main() -> int:
    os.chdir(ROOT)
    check_expected()
    check_text_files()
    check_toml()
    check_yaml()
    check_json()
    check_shell()
    check_rust_syntax()
    check_markdown_links()
    check_workspace_members()
    check_proto()

    if ERRORS:
        print(f"repository validation failed with {len(ERRORS)} error(s):", file=sys.stderr)
        for item in ERRORS:
            print(f"- {item}", file=sys.stderr)
        return 1

    counts = {
        "files": sum(1 for path in repository_paths("*") if path.is_file()),
        "rust": sum(1 for _ in repository_paths("*.rs")),
        "toml": sum(1 for _ in repository_paths("*.toml")),
        "yaml": sum(1 for _ in repository_paths("*.yaml")) + sum(1 for _ in repository_paths("*.yml")),
        "markdown": sum(1 for _ in repository_paths("*.md")),
    }
    print("repository validation passed")
    print(" ".join(f"{name}={value}" for name, value in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
