"""Dependency-free native HTML renderer; it never recomputes Runtime facts."""

from __future__ import annotations

import html
import json
from pathlib import Path

from .bundle import ResultBundle


def render_html(bundle: ResultBundle, output: str | Path) -> Path:
    output = Path(output)
    payload = html.escape(json.dumps(bundle.result, indent=2, ensure_ascii=False))
    title = html.escape(f"Titan run {bundle.manifest['run_id']}")
    document = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>body{{font:15px system-ui;margin:2rem;max-width:1000px}}pre{{background:#111;color:#eee;padding:1rem;overflow:auto}}</style>
</head><body><h1>{title}</h1><p>Verified ResultBundle schema v1.</p><pre>{payload}</pre></body></html>"""
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(document, encoding="utf-8")
    temporary.replace(output)
    return output


def render_quantstats(bundle: ResultBundle, output: str | Path) -> Path:
    """Optional QuantStats appendix; failure never mutates the verified native Bundle."""
    returns = bundle.result.get("returns")
    if not isinstance(returns, list):
        raise RuntimeError("ResultBundle has no canonical returns table")
    output = Path(output)
    if not returns:
        title = html.escape(f"Titan {bundle.manifest['run_id']} — QuantStats")
        document = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>body{{font:15px system-ui;margin:2rem;max-width:800px}}</style>
</head><body><h1>{title}</h1><p>No canonical return observations were recorded by the Rust Runtime.</p>
<p>No performance statistics were inferred by the renderer.</p></body></html>"""
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(document, encoding="utf-8")
        temporary.replace(output)
        return output
    try:
        import pandas as pd
        import quantstats as qs
    except ImportError as error:
        raise RuntimeError("QuantStats renderer requires pandas and quantstats") from error
    series = pd.Series(
        [float(item["return"]) for item in returns],
        index=pd.to_datetime([int(item["timestamp_ns"]) for item in returns], unit="ns", utc=True),
    )
    temporary = output.with_suffix(output.suffix + ".tmp")
    qs.reports.html(series, output=str(temporary), title=f"Titan {bundle.manifest['run_id']}")
    temporary.replace(output)
    return output
