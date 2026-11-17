#!/usr/bin/env python3
"""Guard: verification scripts must not call CLI subcommands that no longer exist.

The reconstruction removed `titan storage` maintenance subcommands and the `research` command
group. Rust tests and the shell guards could not see that the Python verification scripts still
invoked them, so a regression surfaced only when the scripts were run by hand. This check closes
that gap: it reads the subcommand inventory from the CLI source and then scans the Python/shell
verification scripts for `titan <group> <subcommand>` invocations that are not in that inventory.

It is deliberately a *name* check, not a behavioural test: it catches deleted-command references,
which is the failure mode that actually occurred. Usage:

    python3 scripts/check_script_cli_commands.py [--json]

Exit 0 when every referenced subcommand exists, 1 when one is missing, 2 on a usage error.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI_SOURCE = ROOT / "crates" / "titan-cli" / "src" / "storage_cli.rs"
MAIN_SOURCE = ROOT / "crates" / "titan-cli" / "src" / "main.rs"
SCRIPT_DIRS = (ROOT / "scripts",)
# Subcommands whose names are common English words would produce false positives if matched
# anywhere; they are only checked inside an explicit `storage`/`research` invocation.
TRACKED_GROUPS = ("storage", "research")

# `Subcommand::Variant {` inside a `#[derive(Subcommand)]` enum, kebab-cased at match time.
VARIANT_RE = re.compile(r"^\s{4}([A-Z][A-Za-z0-9]*)\s*(?:\{|,|$)", re.MULTILINE)
# `words = ["research", "backtest", ...]` and `"storage ..."` style invocations.
STRING_LIST_RE = re.compile(r'\[\s*"(storage|research)"\s*,\s*"([a-z0-9-]+)"')
FLAG_LIST_RE = re.compile(r'"storage",\s*"([a-z0-9-]+)"')
SHELL_RE = re.compile(r'\btitan\s+(storage|research)\s+([a-z0-9-]+)')


def kebab(variant: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "-", variant).lower()


def enum_variants(text: str, enum_name: str) -> set[str]:
    """Kebab-cased variants of the named `Subcommand` enum."""
    start = text.find(f"enum {enum_name} {{")
    if start < 0:
        return set()
    depth, index = 0, start
    while index < len(text):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                break
        index += 1
    body = text[start:index]
    return {kebab(match) for match in VARIANT_RE.findall(body)}


def cli_inventory() -> dict[str, set[str]]:
    storage = CLI_SOURCE.read_text()
    groups: dict[str, set[str]] = {
        "storage": enum_variants(storage, "StorageCommand") | {"inspect"},
    }
    if MAIN_SOURCE.exists():
        # The research group was removed entirely; keep the key so references are reported.
        groups["research"] = set()
    return groups


def referenced_commands() -> list[tuple[Path, int, str, str]]:
    found: list[tuple[Path, int, str, str]] = []
    for directory in SCRIPT_DIRS:
        for path in sorted(directory.glob("*")):
            if path.suffix not in {".py", ".sh"} or path.name.startswith("check_"):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                for match in STRING_LIST_RE.finditer(line):
                    found.append((path, number, match.group(1), match.group(2)))
                for match in FLAG_LIST_RE.finditer(line):
                    found.append((path, number, "storage", match.group(1)))
                for match in SHELL_RE.finditer(line):
                    found.append((path, number, match.group(1), match.group(2)))
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    inventory = cli_inventory()
    missing = []
    for path, number, group, subcommand in referenced_commands():
        if subcommand not in inventory.get(group, set()):
            missing.append(
                {
                    "file": str(path.relative_to(ROOT)),
                    "line": number,
                    "command": f"titan {group} {subcommand}",
                }
            )

    if args.json:
        print(json.dumps({"passed": not missing, "missing": missing}, indent=2))
    elif missing:
        for item in missing:
            print(f"{item['file']}:{item['line']}: {item['command']} does not exist", file=sys.stderr)
        print("verification scripts reference removed CLI subcommands", file=sys.stderr)
    else:
        print("checked verification scripts: every referenced CLI subcommand exists")
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
