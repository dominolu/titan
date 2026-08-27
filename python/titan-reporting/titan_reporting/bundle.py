"""Strict reader for immutable Rust-authored ResultBundle v1 directories."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class BundleError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResultBundle:
    root: Path
    manifest: dict[str, Any]
    result: dict[str, Any]


def load_bundle(path: str | Path) -> ResultBundle:
    root = Path(path)
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise BundleError("ResultBundle is incomplete: manifest.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise BundleError(f"unsupported ResultBundle schema {manifest.get('schema_version')}")
    decoded: dict[str, Any] = {}
    for descriptor in manifest.get("files", ()):
        relative = descriptor.get("path", "")
        target = (root / relative).resolve()
        if root.resolve() not in target.parents:
            raise BundleError(f"Bundle file escapes root: {relative}")
        content = target.read_bytes()
        if len(content) != int(descriptor["bytes"]):
            raise BundleError(f"Bundle size mismatch: {relative}")
        if hashlib.sha256(content).hexdigest() != descriptor["sha256"]:
            raise BundleError(f"Bundle SHA-256 mismatch: {relative}")
        decoded[relative] = json.loads(content)
    if "result.json" not in decoded:
        raise BundleError("ResultBundle has no result.json")
    return ResultBundle(root=root, manifest=manifest, result=decoded["result.json"])
