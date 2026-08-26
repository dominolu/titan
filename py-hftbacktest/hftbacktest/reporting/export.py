from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import polars as pl

from .models import MetricValue, ReportBundle, ReportConfig, ValidationResult


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(_json_value(value), ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export_report_bundle(
    bundle: ReportBundle,
    metrics: Mapping[str, MetricValue],
    validation: ValidationResult,
    output: str | Path,
    *,
    format: str,
    config: ReportConfig,
    derived_tables: Mapping[str, pl.DataFrame] | None = None,
) -> Path:
    if format not in {"parquet", "csv"}:
        raise ValueError("format must be 'parquet' or 'csv'")
    destination = Path(output).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    backup: Path | None = None
    try:
        files: dict[str, dict[str, Any]] = {}
        suffix = ".parquet" if format == "parquet" else ".csv"
        tables = bundle.tables()
        source_names = frozenset(tables)
        if derived_tables:
            overlap = tables.keys() & derived_tables.keys()
            if overlap:
                raise ValueError(f"derived table names conflict with bundle tables: {sorted(overlap)}")
            tables.update({name: frame.clone() for name, frame in derived_tables.items()})
        for name, frame in tables.items():
            path = temporary / f"{name}{suffix}"
            if format == "parquet":
                frame.write_parquet(path)
            else:
                frame.write_csv(path)
            files[name] = {
                "path": path.name,
                "rows": len(frame),
                "schema_version": bundle.schema_version,
                "kind": "source" if name in source_names else "canonical_derived",
                "sha256": _checksum(path),
            }
        metrics_path = temporary / "metrics.json"
        _write_json(metrics_path, {key: item.to_dict() for key, item in metrics.items()})
        issues_path = temporary / "validation.json"
        _write_json(
            issues_path,
            {
                "status": validation.status.value,
                "issues": [item.to_dict() for item in validation.issues],
            },
        )
        manifest = {
            "schema_version": bundle.schema_version,
            "format": format,
            "metadata": bundle.metadata.to_dict(config.redact_fields),
            "tables": files,
            "metrics": {"path": metrics_path.name, "sha256": _checksum(metrics_path)},
            "validation": {"path": issues_path.name, "sha256": _checksum(issues_path)},
        }
        _write_json(temporary / "manifest.json", manifest)

        if destination.exists():
            backup = destination.with_name(
                f".{destination.name}.backup.{uuid.uuid4().hex}"
            )
            os.replace(destination, backup)
        os.replace(temporary, destination)
        if backup is not None:
            if backup.is_dir():
                shutil.rmtree(backup)
            else:
                backup.unlink()
        return destination
    except Exception:
        if backup is not None and backup.exists():
            if destination.exists():
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
            os.replace(backup, destination)
        raise
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
