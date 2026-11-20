#!/usr/bin/env python3
"""Read repository content without echoing matched private values."""
from __future__ import annotations

import argparse
import ipaddress
import json
import re
import subprocess
import sys
from pathlib import Path
from zipfile import BadZipFile, ZipFile
from io import BytesIO
from xml.etree import ElementTree


PATTERNS = {
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "access-token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[A-Z0-9]{16}|sk-[A-Za-z0-9_-]{20,}|xox[baprs]-[A-Za-z0-9-]+)\b"),
    "personal-directory": re.compile(r"/(?:Users|home)/[^/\s\"'<>`,:;)\\]+"),
    "ssh-fingerprint": re.compile(r"SHA256:[A-Za-z0-9+/]{43}(?:=)?\b"),
}
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
WALLET = re.compile(r"(?<![A-Za-z0-9])0x[0-9a-fA-F]{40}(?![A-Za-z0-9])")
CREDENTIAL = re.compile(r'''(?i)["']?\b(api[_-]?key|api[_-]?secret|private[_-]?key|client[_-]?secret|password|access[_-]?token)["']?\s*[:=]\s*["']([A-Za-z0-9_+/=-]{24,})["']''')
DOCUMENTATION_NETWORKS = tuple(ipaddress.ip_network(x) for x in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"))
# Local-link and private ranges carry no remote host identity. `169.254.0.0/16` is required by
# `build_env.sh`'s proxy bypass (some venues resolve to link-local addresses on offline hosts), and
# a bypass list is a routing rule, not an address of a real server.
LOCAL_NETWORKS = tuple(ipaddress.ip_network(x) for x in ("169.254.0.0/16",))
PUBLIC_ADDRESSES = {
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506",
    "0x4a62316623ad457f02cdc5d997ded67a383ec569",
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "0x912ce59144191c1204e64559fe8253a0e49e6548",
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "0xc31e54faf074f5c3191d1f3a92849a4b7424e6e2",
    "0xf164fc0ec4e93095b804a4795bbe1e041497b92a",
}


def inspect_text(path: str, text: str) -> list[dict]:
    findings = []
    for line_no, line in enumerate(text.splitlines(), 1):
        kinds = {kind for kind, rx in PATTERNS.items() if rx.search(line)}
        if not path.endswith(".svg"):
            for match in IPV4.finditer(line):
                try:
                    address = ipaddress.ip_address(match.group())
                except ValueError:
                    continue
                if not (address.is_loopback or address.is_unspecified
                        or any(address in net for net in DOCUMENTATION_NETWORKS)
                        or any(address in net for net in LOCAL_NETWORKS)):
                    kinds.add("infrastructure-address")
        for match in WALLET.finditer(line):
            address = match.group().lower()
            if address not in PUBLIC_ADDRESSES and len(set(address[2:])) > 2:
                kinds.add("unreviewed-wallet")
        for match in CREDENTIAL.finditer(line):
            value = match.group(2)
            if not (re.fullmatch(r"[A-Z][A-Z0-9_]+", value) or len(set(value)) <= 4):
                kinds.add("credential-literal")
        findings.extend({"file": path, "line": line_no, "kind": kind} for kind in sorted(kinds))
    return findings


def inspect_content(path: str, data: bytes) -> list[dict]:
    if path.startswith(("docs/verification/", "docs/validation/")) and not path.endswith(".md"):
        return [{"file": path, "line": 1, "kind": "private-evidence-export"}]
    if path.endswith(".docx"):
        findings = []
        try:
            with ZipFile(BytesIO(data)) as archive:
                for name in archive.namelist():
                    if name.endswith(".xml"):
                        xml = ElementTree.fromstring(archive.read(name))
                        findings.extend(inspect_text(path + "!" + name, "\n".join(xml.itertext())))
        except (BadZipFile, ElementTree.ParseError):
            findings.append({"file": path, "line": 1, "kind": "unreadable-document"})
        return findings
    if b"\0" in data[:8192]:
        return []
    return inspect_text(path, data.decode("utf-8", "replace"))


def is_noreply(email: str) -> bool:
    """GitHub's own addresses: the account's noreply identity, or GitHub as the committer."""
    return email.endswith("@users.noreply.github.com") or email == "noreply@github.com"


def git(*args: str, cwd: Path) -> bytes:
    return subprocess.check_output(["git", *args], cwd=cwd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged", action="store_true", help="Review only index blobs, including ignored files explicitly staged")
    parser.add_argument("--history", action="store_true", help="Also review every reachable historical blob and author email")
    parser.add_argument("--revision", action="append", default=[], help="Review an explicit outgoing revision instead of private tooling refs")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    root = Path(git("rev-parse", "--show-toplevel", cwd=args.root).decode().strip())
    names = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z", cwd=root) if args.staged else git("ls-files", "-z", cwd=root)
    findings = []
    count = 0
    for name in filter(None, names.decode().split("\0")):
        if args.staged:
            data = git("show", ":" + name, cwd=root)
        else:
            path = root / name
            if not path.is_file() or path.is_symlink():
                continue
            data = path.read_bytes()
        findings.extend(inspect_content(name, data))
        count += 1
    if args.history:
        # A `--revision` is one range and may name several revision arguments, as a push does for
        # a ref the remote does not have yet; splitting keeps each word one revision to git.
        revisions = [word for value in args.revision for word in value.split()] or ["HEAD", "--branches", "--tags", "--remotes"]
        objects = {}
        for line in git("rev-list", "--objects", *revisions, cwd=root).decode().splitlines():
            if " " in line:
                sha, name = line.split(" ", 1)
                objects.setdefault(sha, name)
            elif re.fullmatch(r"[0-9a-f]{40}", line):
                objects.setdefault(line, "<git-object>")
        raw = subprocess.check_output(["git", "cat-file", "--batch"], cwd=root, input=("\n".join(objects) + "\n").encode())
        offset = 0
        for sha, name in objects.items():
            end = raw.index(b"\n", offset)
            header = raw[offset:end].decode().split()
            size = int(header[2])
            data = raw[end + 1:end + 1 + size]
            offset = end + size + 2
            if header[1] == "blob":
                findings.extend(inspect_content(name, data))
        for email in set(record for record in git("log", *revisions, "--format=%ae%n%ce", cwd=root).decode().splitlines() if record):
            if email and not is_noreply(email):
                findings.append({"file": "<git-author-metadata>", "line": 1, "kind": "personal-author-email"})
    # The commits this run reviewed. Their own identity is reported separately below: a commit
    # under review that was already published (the outgoing end of a no-op push, an already merged
    # branch) is published history like any other, and failing the push again over it changes
    # nothing that is not already visible.
    reviewed_commits = (
        sorted({commit for line in git("rev-list", *revisions, cwd=root).decode().split() if (commit := line)})
        if args.revision
        else []
    )
    reported = {(x["file"], x["line"], x["kind"]) for x in findings}
    unique = [dict(zip(("file", "line", "kind"), item)) for item in sorted(reported, key=lambda item: (item[0], item[1], item[2]))]
    published = [
        line.split("\t")[0]
        for line in (
            str(git("log", *reviewed_commits, "--no-walk", "--format=%H%x09%ae%x09%ce", cwd=root).decode()).splitlines()
            if reviewed_commits
            else []
        )
        if line
        and (not is_noreply(line.split("\t")[1]) or not is_noreply(line.split("\t")[2]))
    ]
    summary = {"passed": not unique, "files_reviewed": count, "findings": unique}
    if published:
        # Only the commits reviewed above are named, and only their headers: saying which commits
        # carry another author does not disclose the address itself.
        summary["reviewed_commits_with_published_identity"] = sorted(published)
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if unique else 0


if __name__ == "__main__":
    sys.exit(main())
