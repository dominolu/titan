#!/usr/bin/env python3
"""Guard: documentation links must resolve.

A reconstruction that deletes or renames documents leaves `[text](path.md)` links pointing at
nothing, and nothing in the build notices. This check fails on any link inside `docs/` (and the
repository-root documents) whose target file does not exist.

Existing historical links are pinned in `scripts/doc_link_allowlist.txt` so the check can be
enabled without rewriting records of past work. The allowlist is a *debt ledger*, not a waiver: a
new broken link fails unless it is added deliberately, and an entry that stops being broken is also
reported so the list cannot silently rot.

    python3 scripts/check_doc_links.py [--json]

Exit 0 when every non-allowlisted link resolves, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALLOWLIST = ROOT / "scripts" / "doc_link_allowlist.txt"
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)#\s]+\.md)(?:#[^)]*)?\)")
SCAN_ROOTS = (ROOT / "docs", ROOT / "README.md", ROOT / "AGENTS.md")


def markdown_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if root.is_dir():
            files.extend(sorted(root.rglob("*.md")))
        elif root.is_file():
            files.append(root)
    return files


def allowlisted() -> set[str]:
    if not ALLOWLIST.exists():
        return set()
    entries = set()
    for line in ALLOWLIST.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            entries.add(line)
    return entries


def broken_links() -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    for path in markdown_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in LINK_RE.finditer(text):
            target = match.group(1)
            if "://" in target:
                continue
            resolved = (path.parent / target).resolve()
            if resolved.exists():
                continue
            found.append(
                {
                    "file": str(path.relative_to(ROOT)),
                    "link": target,
                    "line": text[: match.start()].count("\n") + 1,
                }
            )
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    allowed = allowlisted()
    broken = broken_links()
    unresolved = [item for item in broken if item["link"] not in allowed]
    now_resolving = sorted(allowed - {item["link"] for item in broken})

    if args.json:
        print(
            json.dumps(
                {
                    "passed": not unresolved,
                    "unresolved": unresolved,
                    "stale_allowlist_entries": now_resolving,
                },
                indent=2,
            )
        )
        return 0 if not unresolved else 1

    for item in sorted(unresolved, key=lambda item: (item["file"], item["link"])):
        print(f"{item['file']}:{item['line']}: broken link -> {item['link']}", file=sys.stderr)
    if now_resolving:
        print(
            "these allowlisted links now resolve and must be removed from "
            f"{ALLOWLIST.relative_to(ROOT)}: {', '.join(now_resolving)}",
            file=sys.stderr,
        )
    if unresolved:
        print(
            "documentation links must resolve; add a deliberate exception to "
            f"{ALLOWLIST.relative_to(ROOT)} with a reason",
            file=sys.stderr,
        )
        return 1
    print(f"checked {len(markdown_files())} documents: every link resolves ({len(allowed)} historical exceptions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
