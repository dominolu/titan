#!/usr/bin/env python3
"""Static preflight for a cross_exchange_mm runtime configuration.

This script only reads a TOML configuration (and optionally credential-file
metadata). It never connects to an exchange, never starts a strategy and never
prints credential values.

Instrument units, derived base deltas and hedge ratios are **not** recomputed here: the script
asks the platform validator (`titan instruments verify --config ... --json`) and reports its
stable reason codes. A second implementation in Python previously disagreed with the platform
(the review found a "false PASS" path), so the local arithmetic was removed on purpose.

Exit codes: 0 = no FAIL (WARN allowed), 2 = at least one FAIL, 3 = usage/parse error.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover - Python < 3.11
    sys.exit("python 3.11+ (tomllib) is required")

FAIL, WARN, PASS = "FAIL", "WARN", "PASS"
ZERO_DIGEST = [0] * 32
MANAGED_STOP_LIMIT_NS = 10_000_000_000


class Report:
    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def add(self, level: str, check: str, detail: str) -> None:
        self.items.append({"level": level, "check": check, "detail": detail})

    def failed(self) -> bool:
        return any(item["level"] == FAIL for item in self.items)


def platform_instrument_report(config: Path, titan: str | None) -> dict | None:
    """Runs the platform unit validator and returns its JSON report.

    Returns ``None`` when no binary is configured or the invocation fails; the caller decides
    whether that is a FAIL or a WARN. The validator is the single authority for unit arithmetic.
    """
    if not titan:
        return None
    try:
        completed = subprocess.run(
            [titan, "instruments", "verify", "--config", str(config), "--json"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or "valid" not in payload:
        return None
    return payload


def instrument_map(entries: list) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for entry in entries:
        if isinstance(entry, dict) and isinstance(entry.get("asset_id"), int):
            result[entry["asset_id"]] = entry
    return result


def preflight(config: Path, args: argparse.Namespace, validator=None) -> Report:
    report = Report()
    try:
        raw = tomllib.loads(config.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        report.add(FAIL, "config", f"cannot read TOML: {exc}")
        return report

    strategies = raw.get("strategies") or []
    if len(strategies) != 1:
        report.add(FAIL, "strategy-count", f"expected exactly one strategy instance, found {len(strategies)}")
        return report
    strategy = strategies[0]

    markets = strategy.get("markets") or []
    accounts = strategy.get("accounts") or []
    if len(markets) != 2:
        report.add(FAIL, "markets", f"cross-exchange quoting needs exactly 2 markets, found {len(markets)}")
    else:
        report.add(PASS, "markets", f"two markets bound: local {markets[0].get('local_market_no')}, {markets[1].get('local_market_no')}")
    if len(accounts) != 2:
        report.add(FAIL, "accounts", f"cross-exchange quoting needs exactly 2 accounts, found {len(accounts)}")
    else:
        keys = [item.get("account_key") for item in accounts]
        if len(set(keys)) != 2:
            report.add(FAIL, "accounts", "maker and hedge must use distinct account_key values")
        else:
            report.add(PASS, "accounts", "two distinct accounts bound")

    if markets:
        asset_nos = [item.get("local_asset_no") for item in markets]
        if len(set(asset_nos)) != len(asset_nos):
            report.add(FAIL, "markets", "market local_asset_no values must differ")
        for item in markets:
            if item.get("data_mode") != "tick":
                report.add(WARN, "markets", f"market {item.get('local_market_no')} data_mode is not tick")
    if accounts:
        local_nos = [item.get("local_account_no") for item in accounts]
        if len(set(local_nos)) != len(local_nos):
            report.add(FAIL, "accounts", "account local_account_no values must differ")

    validate = validator or (lambda path: platform_instrument_report(path, getattr(args, "titan", None)))
    instrument_report = validate(config)
    if instrument_report is None:
        level = WARN if args.allow_shadow else FAIL
        report.add(level, "platform-units",
                   "platform instrument validator was not run (set --titan or TITAN_BIN); "
                   "unit arithmetic is not re-implemented here")
    elif instrument_report.get("valid") is True:
        count = len(instrument_report.get("instruments") or [])
        report.add(PASS, "platform-units", f"platform validator accepted {count} instrument binding(s)")
    else:
        for error in instrument_report.get("errors") or []:
            report.add(FAIL, "platform-units",
                       f"{error.get('code')} [{error.get('subject')}] {error.get('detail')}")
        if not instrument_report.get("errors"):
            report.add(FAIL, "platform-units", "platform validator reported failure without a reason code")

    runtime = strategy.get("runtime") or {}
    timer = runtime.get("timer_interval")
    if not isinstance(timer, dict) or (timer.get("secs", 0) == 0 and timer.get("nanos", 0) <= 0):
        report.add(FAIL, "timer", "timer_interval is required: the strategy declares REQUIRES_PERIODIC_TIMER")
    else:
        nanos = int(timer.get("secs", 0)) * 1_000_000_000 + int(timer.get("nanos", 0))
        report.add(PASS, "timer", f"periodic timer configured at {nanos} ns")
        if nanos > 1_000_000_000:
            report.add(WARN, "timer", "timer slower than 1 s weakens expiry/deadline response")
    stop = runtime.get("stop_timeout")
    if isinstance(stop, dict):
        stop_ns = int(stop.get("secs", 0)) * 1_000_000_000 + int(stop.get("nanos", 0))
        if stop_ns <= 0:
            report.add(FAIL, "stop-budget", "stop_timeout must be positive")
        elif stop_ns > MANAGED_STOP_LIMIT_NS:
            report.add(FAIL, "stop-budget", "managed live path rejects stop_timeout above 10 s")
        elif stop_ns <= 10_000_000_000:
            report.add(WARN, "stop-budget",
                       "stop_timeout does not exceed the strategy's default 10 s order/hedge deadline; "
                       "cancel + hedge + confirmation may not fit")
        else:
            report.add(PASS, "stop-budget", f"stop_timeout {stop_ns} ns")
    else:
        report.add(FAIL, "stop-budget", "explicit stop_timeout is required")

    limits = runtime.get("execution_limits")
    if limits is None:
        level = WARN if args.allow_shadow else FAIL
        report.add(level, "execution-limits",
                   "no platform execution envelope. Controlled live run needs the multi-binding "
                   "execution limits (platform stage B); a shadow run must be explicitly allowed")
    else:
        report.add(WARN, "execution-limits",
                   "legacy execution_limits supports one market only; it cannot cover two legs")

    package = strategy.get("package") or {}
    digest = package.get("expected_digest")
    if digest == ZERO_DIGEST:
        report.add(WARN, "artifact", "artifact digest is the placeholder; compile and freeze the bundle first")
    elif not isinstance(digest, list) or len(digest) != 32:
        report.add(FAIL, "artifact", "expected_digest must be 32 bytes")

    sources = {item.get("source_key"): item for item in raw.get("market_sources") or [] if isinstance(item, dict)}
    account_entries = {item.get("account_key"): item for item in raw.get("accounts") or [] if isinstance(item, dict)}
    assets: dict[int, dict] = {}
    for source in sources.values():
        assets.update(instrument_map(source.get("instruments") or []))
    for account in account_entries.values():
        # Only used to map local asset numbers to asset ids; the units themselves are validated by
        # the platform, never recomputed here.
        assets.update(instrument_map(account.get("instruments") or []))

    maker_market_no = args.maker_market
    hedge_market_no = args.hedge_market
    by_local = {item.get("local_market_no"): item for item in markets if isinstance(item, dict)}
    if maker_market_no not in by_local or hedge_market_no not in by_local:
        report.add(FAIL, "pairing", f"maker/hedge local_market_no {maker_market_no}/{hedge_market_no} not bound")
    else:
        maker_asset = by_local[maker_market_no].get("asset_id")
        hedge_asset = by_local[hedge_market_no].get("asset_id")
        if maker_asset is None or hedge_asset is None or maker_asset == hedge_asset:
            report.add(FAIL, "pairing", "maker and hedge must bind distinct asset_id values")
        else:
            report.add(PASS, "pairing",
                       f"maker asset {maker_asset} / hedge asset {hedge_asset}; "
                       "unit arithmetic is proven by the platform validator")
        maker_account = next((a for a in accounts if a.get("local_account_no") == 0), None)
        hedge_account = next((a for a in accounts if a.get("local_account_no") == 1), None)
        if maker_account and hedge_account:
            maker_currencies = {item.get("native_currency") for item in
                                account_entries.get(maker_account.get("account_key"), {}).get("currencies", [])}
            hedge_currencies = {item.get("native_currency") for item in
                                account_entries.get(hedge_account.get("account_key"), {}).get("currencies", [])}
            shared = maker_currencies & hedge_currencies
            if shared:
                report.add(PASS, "collateral", f"shared quote/collateral currency: {sorted(shared)}")
            else:
                report.add(FAIL, "collateral",
                           "no shared quote/collateral currency; cross-currency pairs need a verified FX source")
        else:
            report.add(FAIL, "collateral", "maker/hedge local_account_no 0 and 1 must both be bound")

    for item in markets:
        source = sources.get(item.get("source_key"))
        if source is None:
            report.add(FAIL, "enabled", f"market source {item.get('source_key')} not declared")
        elif not source.get("enabled"):
            report.add(WARN, "enabled", f"market source {item.get('source_key')} is disabled")
    for entry in accounts:
        account = account_entries.get(entry.get("account_key"))
        if account is None:
            report.add(FAIL, "enabled", f"account {entry.get('account_key')} not declared")
        elif not account.get("enabled"):
            report.add(WARN, "enabled", f"account {entry.get('account_key')} is disabled")
    if not strategy.get("enabled"):
        report.add(WARN, "enabled", "strategy instance is disabled")

    if args.check_secrets:
        service = raw.get("account_service") or {}
        root = service.get("secret_root")
        if not isinstance(root, str) or not root:
            report.add(FAIL, "secrets", "account_service.secret_root is required")
        else:
            base = (config.parent / root).resolve()
            for account in account_entries.values():
                reference = account.get("credential_ref")
                if not isinstance(reference, str) or not reference.startswith("secret://file/"):
                    report.add(FAIL, "secrets", f"account {account.get('account_key')} has no secret://file ref")
                    continue
                name = reference.removeprefix("secret://file/")
                path = (base / name).resolve()
                if base not in path.parents:
                    report.add(FAIL, "secrets", f"{name} escapes secret_root")
                    continue
                if not path.is_file():
                    report.add(FAIL, "secrets", f"credential file {name} is missing")
                    continue
                mode = stat.S_IMODE(path.stat().st_mode)
                if mode & 0o077:
                    report.add(FAIL, "secrets", f"credential file {name} must be 0600 (mode {oct(mode)})")
                elif path.stat().st_size > 64 * 1024:
                    report.add(FAIL, "secrets", f"credential file {name} exceeds 64 KiB")
                else:
                    report.add(PASS, "secrets", f"credential file {name} present with restrictive mode")

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--maker-market", type=int, default=1,
                        help="local_market_no of the traded/maker market (default 1)")
    parser.add_argument("--hedge-market", type=int, default=0,
                        help="local_market_no of the reference/hedge market (default 0)")
    parser.add_argument("--check-secrets", action="store_true",
                        help="also verify credential file presence and permissions")
    parser.add_argument("--allow-shadow", action="store_true",
                        help="treat the missing execution envelope as WARN instead of FAIL")
    parser.add_argument("--titan", default=os.environ.get("TITAN_BIN"),
                        help="path to the titan binary for the platform unit validator "
                             "(default: $TITAN_BIN)")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if not args.config.is_file():
        print(f"config not found: {args.config}", file=sys.stderr)
        return 3
    report = preflight(args.config, args)
    if args.json:
        print(json.dumps({"ok": not report.failed(), "items": report.items}, indent=2))
    else:
        for item in report.items:
            print(f"[{item['level']}] {item['check']}: {item['detail']}")
        print("RESULT:", "FAIL" if report.failed() else "PASS")
    return 2 if report.failed() else 0


if __name__ == "__main__":
    sys.exit(main())
