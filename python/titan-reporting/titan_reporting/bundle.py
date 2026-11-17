"""Strict reader for immutable Rust-authored ResultBundle v1 directories."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class BundleError(RuntimeError):
    pass


def _host_abi_fingerprint() -> str:
    """The strategy ABI fingerprint this reporting environment expects, or "" when unknown.

    The reporting package is deliberately runtime-agnostic (it renders committed bundles), so the
    expected value comes from the deployment. When it is not configured the schema and integrity
    checks still apply; the Rust CLI always enforces the fingerprint comparison.
    """
    import os

    value = os.environ.get("TITAN_ABI_FINGERPRINT", "").strip().lower()
    if value.startswith("0x"):
        value = value[2:]
    return value if len(value) == 64 else ""


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
    recorded = manifest.get("abi_fingerprint")
    expected = _host_abi_fingerprint()
    if recorded and expected and recorded.strip().lower() != expected:
        raise BundleError(f"bundle_abi_mismatch: recorded {recorded}, this host runs {expected}")
    decoded: dict[str, Any] = {}
    seen: set[str] = set()
    for descriptor in manifest.get("files", ()):
        relative = descriptor.get("path", "")
        if relative in seen:
            raise BundleError(f"Duplicate bundle file: {relative}")
        seen.add(relative)
        target = (root / relative).resolve()
        if root.resolve() not in target.parents:
            raise BundleError(f"Bundle file escapes root: {relative}")
        if target.stat().st_size != int(descriptor["bytes"]):
            raise BundleError(f"Bundle size mismatch: {relative}")
        digest = hashlib.sha256()
        with target.open("rb") as stream:
            for block in iter(lambda: stream.read(65536), b""):
                digest.update(block)
        if digest.hexdigest() != descriptor["sha256"]:
            raise BundleError(f"Bundle SHA-256 mismatch: {relative}")
        if target.suffix == ".json":
            decoded[relative] = json.loads(target.read_text(encoding="utf-8"))
    if "result.json" not in decoded:
        raise BundleError("ResultBundle has no result.json")
    return ResultBundle(root=root, manifest=manifest, result=decoded["result.json"])
