"""Report already-computed Rust historical replay results; no strategy or matching logic."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cases = []
    histories = []
    lines = ["| 行情 | 方向 | 模型 | 净收益 USDT | 净收益率 | 最大回撤 USDT / % | 最大持仓名义 USDT | 含在途保守上界 USDT | 完成 |",
             "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    regimes = (("sideways", "2023-09-02"), ("up", "2023-10-23"), ("down", "2023-08-17"))
    for regime, day in regimes:
        histories.append(dict(regime=regime, date=day, **json.loads((args.data / (day + ".json")).read_text())))
        for direction in ("neutral", "long_bias", "short_bias"):
            for model in ("base", "stress"):
                name = f"{regime}-{direction}-{model}"
                result = json.loads((args.input / name / "result.json").read_text())
                result.update(name=name, regime=regime, date=day, direction_name=direction, model_name=model)
                cases.append(result)
                lines.append(f"| {regime} | {direction} | {model} | {result['net_pnl_usdt']:.2f} | {result['net_return_pct']:.3f}% | "
                             f"{result['max_drawdown_usdt']:.2f} / {result['max_drawdown_pct']:.3f}% | {result['max_position_notional_usdt']:.2f} | "
                             f"{result['max_pending_bound_notional_usdt']:.2f} | {'是' if result['success'] else '失败'} |")
    summary = {"schema_version": 1, "cases": cases, "history": histories,
               "success_count": sum(case["success"] for case in cases), "case_count": len(cases),
               "metric_source": "Rust replay measures every original accepted BBO and each execution/funding event; minute curve samples are display only"}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False))
    (args.output / "table.txt").write_text("\n".join(lines) + "\n")
    # Charts are reports only; metrics above remain those from the full-resolution Rust engine.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    figure, axes = plt.subplots(3, 1, figsize=(12, 12), constrained_layout=True)
    colors = {"neutral": "#2563eb", "long_bias": "#16a34a", "short_bias": "#dc2626"}
    for axis, (regime, day) in zip(axes, regimes):
        for direction in colors:
            for model in ("base", "stress"):
                path = args.input / f"{regime}-{direction}-{model}" / "equity.csv"
                rows = list(csv.DictReader(path.open()))
                start = int(rows[0]["time_ns"]) // 86_400_000_000_000 * 86_400_000_000_000
                axis.plot([(int(row["time_ns"]) - start) / 3_600_000_000_000 for row in rows],
                          [float(row["equity_usdt"]) - 10000 for row in rows], color=colors[direction],
                          linestyle="-" if model == "base" else "--", linewidth=1.4, label=f"{direction} / {model}")
        axis.axhline(0, color="#64748b", linewidth=0.7)
        axis.set(title=f"BTCUSDT perpetual | {day} | {regime}", xlabel="UTC hour", ylabel="Net equity change (USDT)", xlim=(0, 24))
        axis.grid(alpha=0.2)
        axis.legend(loc="best", ncol=3, fontsize=8)
    figure.suptitle("Native Numba grid: original BBO + aggregate trades\n10,000 USDT per independent run; curves sampled per minute", fontsize=13)
    figure.savefig(args.output / "equity.png", dpi=170)
    plt.close(figure)
    print(f"{summary['success_count']}/{summary['case_count']} replays completed")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
