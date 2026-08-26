"""Run the canonical dual-MA Bar strategy and generate a self-contained report."""

import argparse
import json
import time
from pathlib import Path

import numpy as np
import polars as pl

from dual_ma_bar_backtest import load_bars
from hftbacktest.eventbot import run_event_bot
from hftbacktest.reporting import BacktestReport, ReportConfig
from hftbacktest.strategies import create_dual_ma_strategy


DEFAULT_BAR_FEE_MODEL = {
    "type": "rate",
    "maker_rate": 0.001,
    "taker_rate": 0.001,
    "assessment": "every_fill",
}


def _portfolio_tables(bars, reports, initial_capital):
    fills = sorted(
        (report for report in reports if report["kind"] == "fill"),
        key=lambda report: (report["delivery_ts"], report["sequence"]),
    )
    fill_timestamps = np.asarray(
        [report["delivery_ts"] for report in fills], dtype=np.int64
    )
    signed_quantities = np.asarray(
        [
            report["exec_qty"] * (1.0 if report["side"] == "buy" else -1.0)
            for report in fills
        ],
        dtype=np.float64,
    )
    fill_prices = np.asarray(
        [report["exec_price"] for report in fills], dtype=np.float64
    )
    signed_fees = np.asarray(
        [
            0.0
            if report["account_delta"] is None
            else report["account_delta"]["fee"]
            for report in fills
        ],
        dtype=np.float64,
    )

    fill_quantity = np.abs(signed_quantities)
    fill_value = fill_quantity * fill_prices
    cumulative_position = np.concatenate(([0.0], np.cumsum(signed_quantities)))
    cumulative_cash_gross = np.concatenate(
        ([initial_capital], initial_capital + np.cumsum(-signed_quantities * fill_prices))
    )
    cumulative_fee = np.concatenate(([0.0], np.cumsum(np.maximum(signed_fees, 0.0))))
    cumulative_rebate = np.concatenate(
        ([0.0], np.cumsum(np.maximum(-signed_fees, 0.0)))
    )
    cumulative_volume = np.concatenate(([0.0], np.cumsum(fill_quantity)))
    cumulative_value = np.concatenate(([0.0], np.cumsum(fill_value)))

    timestamps = bars["bar"]["close_ts"].astype(np.int64, copy=False)
    close = bars["bar"]["close"].astype(np.float64, copy=False)
    fill_indices = np.searchsorted(fill_timestamps, timestamps, side="right")
    position = cumulative_position[fill_indices]
    cash_gross = cumulative_cash_gross[fill_indices]
    fee = cumulative_fee[fill_indices]
    rebate = cumulative_rebate[fill_indices]
    cash = cash_gross - fee + rebate
    notional = position * close
    gross_exposure = np.abs(notional)
    equity_gross = cash_gross + notional
    equity_net = equity_gross - fee + rebate
    leverage = np.divide(
        gross_exposure,
        equity_net,
        out=np.zeros_like(gross_exposure),
        where=equity_net != 0.0,
    )

    portfolio = pl.DataFrame(
        {
            "timestamp": timestamps,
            "timestamp_kind": ["local_delivery"] * len(timestamps),
            "equity_gross": equity_gross,
            "equity_net": equity_net,
            "cash": cash,
            "realized_pnl": [None] * len(timestamps),
            "unrealized_pnl": [None] * len(timestamps),
            "fee": fee,
            "rebate": rebate,
            "funding": np.zeros(len(timestamps)),
            "external_flow": np.zeros(len(timestamps)),
            "gross_exposure": gross_exposure,
            "net_exposure": notional,
            "margin": [None] * len(timestamps),
            "leverage": leverage,
            "num_trades": fill_indices,
            "trading_volume": cumulative_volume[fill_indices],
            "trading_value": cumulative_value[fill_indices],
            "reporting_currency": ["USD"] * len(timestamps),
        }
    )
    accounts = pl.DataFrame(
        {
            "timestamp": timestamps,
            "view_kind": ["local_delivered"] * len(timestamps),
            "venue_id": ["XNAS"] * len(timestamps),
            "currency_id": ["USD"] * len(timestamps),
            "balance": cash,
            "fee": fee,
            "rebate": rebate,
            "funding": np.zeros(len(timestamps)),
            "realized_pnl": [None] * len(timestamps),
            "unrealized_pnl": [None] * len(timestamps),
            "margin": [None] * len(timestamps),
        }
    )
    positions = pl.DataFrame(
        {
            "timestamp": timestamps,
            "view_kind": ["local_delivered"] * len(timestamps),
            "venue_id": ["XNAS"] * len(timestamps),
            "instrument_id": ["AAPL"] * len(timestamps),
            "currency_id": ["USD"] * len(timestamps),
            "quantity": position,
            "mark_price": close,
            "notional": notional,
            "realized_pnl": [None] * len(timestamps),
            "unrealized_pnl": [None] * len(timestamps),
            "margin": [None] * len(timestamps),
        }
    )
    return portfolio, accounts, positions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/AAPL_1m_all_sources.parquet")
    parser.add_argument("--source", default="auto")
    parser.add_argument("--short-period", type=int, default=20)
    parser.add_argument("--long-period", type=int, default=50)
    parser.add_argument("--quantity", type=float, default=100.0)
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0)
    parser.add_argument("--timeframe-ns", type=int, default=60_000_000_000)
    parser.add_argument(
        "--bar-matching",
        choices=("next_open", "signal_close", "touch", "conservative_ohlc"),
        default="next_open",
    )
    parser.add_argument(
        "--output", default="backtest_reports/titan_dual_ma_aapl_report/report.html"
    )
    parser.add_argument(
        "--renderer", choices=("native", "quantstats"), default="native"
    )
    args = parser.parse_args()

    bars, data_audit = load_bars(
        args.data,
        args.source,
        args.timeframe_ns,
        return_audit=True,
        wall_clock_timezone="auto",
    )
    strategy = create_dual_ma_strategy(
        closes=bars["bar"]["close"],
        short_period=args.short_period,
        long_period=args.long_period,
        timeframe_ns=args.timeframe_ns,
        quantity=args.quantity,
    )
    strategy.state_i64[0] = 1
    started = time.perf_counter_ns()
    result = run_event_bot(
        data_mode="bar",
        bars=bars,
        history_capacity=args.long_period + 1,
        on_bar=strategy.on_bar,
        on_filled=strategy.on_filled,
        on_stop=strategy.on_stop,
        state=strategy.state,
        state_i64=strategy.state_i64,
        bar_matching=args.bar_matching,
        return_result=True,
    )
    elapsed_ns = time.perf_counter_ns() - started
    portfolio, accounts, positions = _portfolio_tables(
        bars, result.execution_reports, args.initial_capital
    )
    config = ReportConfig(
        reporting_currency="USD",
        initial_capital=args.initial_capital,
        calendar="weekday",
        timezone="America/New_York",
        periods_per_year=252,
        trading_weekdays=(0, 1, 2, 3, 4),
        venue_id="XNAS",
        instrument_id="AAPL",
    )
    report_result = {
        "execution_reports": result.execution_reports,
        "order_count": result.order_count,
        "fill_count": result.fill_count,
        "reject_count": result.reject_count,
        "cancel_count": result.cancel_count,
        "expire_count": result.expire_count,
        "metadata": {
            "strategy_id": "dual_ma",
            "strategy_parameters": {
                "short_period": args.short_period,
                "long_period": args.long_period,
                "quantity": args.quantity,
            },
            "model_identities": {"fee": DEFAULT_BAR_FEE_MODEL},
        },
    }
    report = BacktestReport.from_result(
        report_result,
        config,
        portfolio_snapshots=portfolio,
        account_snapshots=accounts,
        position_snapshots=positions,
        currency_map={0: "USD"},
    )
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    artifact = report.generate(output, renderer=args.renderer)
    bundle_path = report.export(output.parent / "bundle", format="parquet")
    metrics = {name: metric.to_dict() for name, metric in artifact.metrics.items()}
    summary = {
        "bars": len(bars),
        "source": args.source,
        "data_audit": data_audit,
        "short_period": args.short_period,
        "long_period": args.long_period,
        "quantity": args.quantity,
        "bar_matching": args.bar_matching,
        "fee_model": DEFAULT_BAR_FEE_MODEL,
        "golden_crosses": int(result.state[2]),
        "death_crosses": int(result.state[3]),
        "order_count": result.order_count,
        "fill_count": result.fill_count,
        "final_position": float(result.state[6]),
        "elapsed_ms": elapsed_ns / 1_000_000,
        "report_status": artifact.status.value,
        "report_provider": artifact.provider,
        "report": str(artifact.path),
        "bundle": str(bundle_path),
        "metrics": metrics,
        "issues": [issue.to_dict() for issue in artifact.issues],
    }
    summary_path = output.parent / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("DUAL_MA_REPORT=" + json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
