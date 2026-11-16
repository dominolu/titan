#!/usr/bin/env python3
"""Guard: strategies must not declare exchange facts as parameters.

Unit/price parameters are legitimate business inputs (grid bounds, risk caps, tick-based
distances). The exchange facts below must come from the V14 object table instead:
  * contract size / multiplier, tick size, lot size, min notional, fee rates
The check is name-based and conservative; it exits non-zero listing offending parameters.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

FORBIDDEN = {
    "contract_multiplier",
    "tick_size",
    "lot_size",
    "quantity_lot",
    "price_tick",
    "min_quantity",
    "min_notional_quote",
    "maker_fee_bps",
    "taker_fee_bps",
    "hedge_fee_bps",
    "reference_tick_ratio",
    "quote_units_per_tick_lot",
    "hedge_lots_per_maker_lot",
    "hedge_ratio_numerator",
    "hedge_ratio_denominator",
    "linear_quote_currency",
}

PARAM = re.compile(r'(?:integer|positive|FloatParam|IntParam|EnumParam)\(\s*"([a-z0-9_]+)"')


def parameters(source: str) -> list[str]:
    # Scan only up to the subscription block so object/pair construction is excluded.
    match = re.search(r"parameters=\((.*?)\),\s*\n?\s*subscriptions", source, re.S)
    return PARAM.findall(match.group(1)) if match else PARAM.findall(source)


def main(argv: list[str]) -> int:
    targets = [Path(arg) for arg in argv[1:]] or sorted(Path("strategies").glob("*.py"))
    # Explicit targets are scanned in full: they are usually synthetic probes, not real strategies.
    failures = []
    for path in targets:
        if path.name.startswith("_"):
            continue
        for name in parameters(path.read_text(encoding="utf-8")):
            if name in FORBIDDEN:
                failures.append(f"{path}: {name}")
    if failures:
        print("strategy declares exchange facts as parameters:", file=sys.stderr)
        for item in failures:
            print(f"  {item}", file=sys.stderr)
        return 1
    print(f"checked {len(targets)} strategies: no exchange facts as parameters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
