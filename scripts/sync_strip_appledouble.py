#!/usr/bin/env python3
"""Drop macOS resource-fork members (``.?_name``) from a tar archive before it leaves the machine.

A file copied onto an extended-attribute volume — every file that has been through rsync from a
Mac, and every file checked out by some editors — gets a sibling ``._name`` member carrying the
resource fork. The members are Apple metadata with no build meaning, but they are real files once
extracted, so on a Linux build server they show up in ``git status`` forever and drag any
``find``-based tool over the tree.

Done here rather than with an exclude pattern because the names are computed from the sibling,
not stored anywhere a ``--exclude`` could match them reliably across the tar implementations the
two machines have.

Usage: sync_strip_appledouble.py <archive.tar.gz>

Rewrites the archive in place. Members are copied through unchanged except the dropped ones; the
archive is only replaced when the rewrite succeeded, so a failure leaves the original in place.
"""

from __future__ import annotations

import os
import sys
import tarfile
import tempfile


def is_apple_double(name: str) -> bool:
    base = os.path.basename(name.rstrip("/"))
    return base.startswith("._") or base == ".DS_Store"


def strip(path: str) -> int:
    dropped = 0
    directory = os.path.dirname(os.path.abspath(path)) or "."
    handle, staged = tempfile.mkstemp(suffix=".tar.gz", dir=directory)
    os.close(handle)
    try:
        with tarfile.open(path, "r:gz") as source, tarfile.open(staged, "w:gz") as target:
            for member in source:
                if is_apple_double(member.name):
                    dropped += 1
                    continue
                data = source.extractfile(member) if member.isreg() else None
                target.addfile(member, data)
        os.replace(staged, path)
    except BaseException:
        os.unlink(staged)
        raise
    return dropped


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <archive.tar.gz>", file=sys.stderr)
        return 2
    dropped = strip(argv[1])
    if dropped:
        print(f"sync: stripped {dropped} macOS metadata member(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
