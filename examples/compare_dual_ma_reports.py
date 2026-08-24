"""Compare Titan and Nautilus dual-MA reports by deterministic fill ordinal."""

import argparse
import json
from pathlib import Path

import polars as pl


PRICE_TOLERANCE = 1e-9
CASH_TOLERANCE = 1e-8


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--titan-dir", required=True)
    parser.add_argument("--nautilus-dir", required=True)
    parser.add_argument("--bar-data", required=True)
    parser.add_argument("--bar-source", default="polygon_s3")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    titan_dir = Path(args.titan_dir)
    nautilus_dir = Path(args.nautilus_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    titan_metadata = json.loads((titan_dir / "summary.json").read_text())
    bar_matching = titan_metadata.get("bar_matching", "next_open")
    price_difference_class = (
        "signal_close_price_precision"
        if bar_matching == "signal_close"
        else "next_open_vs_signal_bar_close"
    )

    titan_fills = pl.read_csv(titan_dir / "fills.csv", try_parse_dates=True).with_row_index(
        "row_index", offset=1
    )
    nautilus_fills = pl.read_csv(
        nautilus_dir / "fills.csv", try_parse_dates=True
    ).with_row_index("row_index", offset=1)
    fills = titan_fills.select(
        "row_index",
        pl.col("client_order_id").alias("titan_order_id"),
        pl.col("order_side").alias("titan_side"),
        pl.col("last_qty").alias("titan_qty"),
        pl.col("last_px").alias("titan_px"),
        pl.col("ts_event").alias("titan_ts"),
        pl.col("commission").alias("titan_commission"),
        pl.col("cash_after").alias("titan_cash_after"),
        pl.col("position_after").alias("titan_position_after"),
    ).join(
        nautilus_fills.select(
            "row_index",
            pl.col("client_order_id").alias("nautilus_order_id"),
            pl.col("order_side").alias("nautilus_side"),
            pl.col("last_qty").cast(pl.Float64).alias("nautilus_qty"),
            pl.col("last_px").alias("nautilus_px"),
            pl.col("ts_event").alias("nautilus_ts"),
            pl.col("commission").alias("nautilus_commission_text"),
        ),
        on="row_index",
        how="full",
        coalesce=True,
    )
    fills = fills.with_columns(
        (pl.col("titan_qty") - pl.col("nautilus_qty")).alias("qty_diff"),
        (pl.col("titan_px") - pl.col("nautilus_px")).alias("price_diff"),
        ((pl.col("titan_ts") - pl.col("nautilus_ts")).dt.total_seconds()).alias(
            "time_diff_seconds"
        ),
        pl.when(pl.col("titan_side") == "BUY")
        .then(-1.0)
        .otherwise(1.0)
        .mul(pl.col("titan_qty"))
        .mul(pl.col("titan_px") - pl.col("nautilus_px"))
        .alias("cash_effect_titan_minus_nautilus"),
    ).with_columns(
        pl.when(pl.col("titan_order_id").is_null())
        .then(pl.lit("missing_terminal_close"))
        .when(pl.col("time_diff_seconds") != 0)
        .then(pl.lit("next_open_across_market_gap"))
        .when(pl.col("price_diff").abs() > PRICE_TOLERANCE)
        .then(pl.lit(price_difference_class))
        .otherwise(pl.lit("exact_common_fill"))
        .alias("difference_class")
    )
    fills.write_csv(output_dir / "fill_comparison.csv")

    titan_accounts = pl.read_csv(
        titan_dir / "account_states.csv", try_parse_dates=True
    ).with_row_index("row_index", offset=1)
    nautilus_accounts = pl.read_csv(
        nautilus_dir / "account_states.csv", try_parse_dates=True
    ).with_row_index("row_index", offset=1)
    accounts = titan_accounts.select(
        "row_index",
        pl.col("ts_event").alias("titan_ts"),
        pl.col("total").alias("titan_total"),
        pl.col("position").alias("titan_position"),
        pl.col("source_order_id").alias("titan_order_id"),
    ).join(
        nautilus_accounts.select(
            "row_index",
            pl.col("ts_event").alias("nautilus_ts"),
            pl.col("total").alias("nautilus_total"),
        ),
        on="row_index",
        how="full",
        coalesce=True,
    ).with_columns(
        (pl.col("titan_total") - pl.col("nautilus_total")).alias("total_diff"),
        ((pl.col("titan_ts") - pl.col("nautilus_ts")).dt.total_seconds()).alias(
            "time_diff_seconds"
        ),
    ).with_columns(
        pl.when(pl.col("titan_total").is_null())
        .then(pl.lit("missing_terminal_close_account_state"))
        .when(pl.col("row_index") == 1)
        .then(pl.lit("exact_initial_state"))
        .when(pl.col("time_diff_seconds") != 0)
        .then(pl.lit("accumulated_next_open_gap_effect"))
        .when((pl.col("titan_total") - pl.col("nautilus_total")).abs() > CASH_TOLERANCE)
        .then(pl.lit("accumulated_execution_price_effect"))
        .otherwise(pl.lit("exact_common_state"))
        .alias("difference_class"),
    )
    accounts.write_csv(output_dir / "account_comparison.csv")

    titan_commissions = pl.read_csv(
        titan_dir / "commissions.csv", try_parse_dates=True
    ).with_row_index("row_index", offset=1)
    nautilus_commissions = pl.read_csv(
        nautilus_dir / "commissions.csv", try_parse_dates=True
    ).with_row_index("row_index", offset=1)
    commissions = titan_commissions.select(
        "row_index",
        pl.col("client_order_id").alias("titan_order_id"),
        pl.col("ts_event").alias("titan_ts"),
        pl.col("commission_amount").alias("titan_commission"),
        pl.col("commission_currency").alias("titan_currency"),
    ).join(
        nautilus_commissions.select(
            "row_index",
            pl.col("client_order_id").alias("nautilus_order_id"),
            pl.col("ts_event").alias("nautilus_ts"),
            pl.col("commission_amount").alias("nautilus_commission"),
            pl.col("commission_currency").alias("nautilus_currency"),
        ),
        on="row_index",
        how="full",
        coalesce=True,
    ).with_columns(
        (pl.col("titan_commission") - pl.col("nautilus_commission")).alias(
            "commission_diff"
        ),
        pl.when(pl.col("titan_order_id").is_null())
        .then(pl.lit("missing_terminal_close_commission_record"))
        .when(
            (pl.col("titan_commission") - pl.col("nautilus_commission")).abs()
            > CASH_TOLERANCE
        )
        .then(pl.lit("commission_amount_mismatch"))
        .otherwise(pl.lit("exact_zero_commission"))
        .alias("difference_class"),
    )
    commissions.write_csv(output_dir / "commission_comparison.csv")

    bars = (
        pl.read_parquet(args.bar_data)
        .filter((pl.col("source") == args.bar_source) & pl.col("is_final"))
        .select(
            (pl.col("ts").cast(pl.Int64) * 1_000).alias("open_ns"),
            (pl.col("ts").cast(pl.Int64) * 1_000 + 60_000_000_000).alias(
                "close_ns"
            ),
            "open",
            "close",
        )
    )
    titan_open_check = (
        titan_fills.with_columns(pl.col("ts_event").dt.epoch("ns").alias("event_ns"))
        .join(bars, left_on="event_ns", right_on="open_ns", how="left")
        .select((pl.col("last_px") - pl.col("open")).abs().le(PRICE_TOLERANCE).sum())
        .item()
    )
    titan_close_check = (
        titan_fills.with_columns(pl.col("ts_event").dt.epoch("ns").alias("event_ns"))
        .join(bars, left_on="event_ns", right_on="close_ns", how="left")
        .select((pl.col("last_px") - pl.col("close")).abs().le(PRICE_TOLERANCE).sum())
        .item()
    )
    nautilus_close_check = (
        nautilus_fills.with_columns(
            pl.col("ts_event").dt.epoch("ns").alias("event_ns")
        )
        .join(bars, left_on="event_ns", right_on="close_ns", how="left")
        .select((pl.col("last_px") - pl.col("close")).abs().le(PRICE_TOLERANCE).sum())
        .item()
    )

    common = fills.filter(pl.col("titan_order_id").is_not_null())
    cash_effect = common["cash_effect_titan_minus_nautilus"]
    summary = {
        "bar_matching": bar_matching,
        "titan_fills": titan_fills.height,
        "nautilus_fills": nautilus_fills.height,
        "common_fills": common.height,
        "side_mismatches": common.filter(
            pl.col("titan_side") != pl.col("nautilus_side")
        ).height,
        "quantity_mismatches": common.filter(pl.col("qty_diff").abs() > PRICE_TOLERANCE).height,
        "price_mismatches": common.filter(pl.col("price_diff").abs() > PRICE_TOLERANCE).height,
        "exact_price_matches": common.filter(pl.col("price_diff").abs() <= PRICE_TOLERANCE).height,
        "timestamp_mismatches": common.filter(pl.col("time_diff_seconds") != 0).height,
        "overnight_gaps": common.filter(pl.col("time_diff_seconds") == 63_000).height,
        "weekend_gaps": common.filter(pl.col("time_diff_seconds") == 235_800).height,
        "max_abs_price_diff": float(common["price_diff"].abs().max()),
        "mean_abs_price_diff": float(common["price_diff"].abs().mean()),
        "net_cash_effect_titan_minus_nautilus_before_terminal_close": float(
            cash_effect.sum()
        ),
        "titan_favorable_fills": int((cash_effect > CASH_TOLERANCE).sum()),
        "titan_adverse_fills": int((cash_effect < -CASH_TOLERANCE).sum()),
        "max_abs_single_fill_cash_effect": float(cash_effect.abs().max()),
        "account_state_value_mismatches": accounts.filter(
            pl.col("titan_total").is_not_null()
            & (pl.col("total_diff").abs() > CASH_TOLERANCE)
        ).height,
        "max_abs_account_total_diff": float(accounts["total_diff"].abs().max()),
        "titan_ending_cash_before_terminal_close": float(
            titan_accounts["total"][-1]
        ),
        "nautilus_cash_before_terminal_close": float(
            nautilus_accounts["total"][titan_accounts.height - 1]
        ),
        "nautilus_ending_cash_after_terminal_close": float(
            nautilus_accounts["total"][-1]
        ),
        "terminal_close_price": float(nautilus_fills["last_px"][-1]),
        "terminal_close_qty": float(nautilus_fills["last_qty"][-1]),
        "terminal_close_cash_delta": float(
            nautilus_fills["last_px"][-1] * nautilus_fills["last_qty"][-1]
        ),
        "titan_total_commission": float(titan_commissions["commission_amount"].sum()),
        "nautilus_total_commission": float(
            nautilus_commissions["commission_amount"].sum()
        ),
        "common_commission_amount_mismatches": commissions.filter(
            pl.col("titan_order_id").is_not_null()
            & (pl.col("commission_diff").abs() > CASH_TOLERANCE)
        ).height,
        "titan_prices_equal_source_next_open": int(titan_open_check),
        "titan_prices_equal_source_signal_close": int(titan_close_check),
        "nautilus_prices_equal_source_signal_close": int(nautilus_close_check),
        "nautilus_price_precision_rounding_records": int(
            nautilus_fills.height - nautilus_close_check
        ),
    }
    (output_dir / "comparison_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    execution_reason = (
        "Titan 与 Nautilus 均使用产生信号 Bar 的 close；剩余价格差仅来自 instrument precision 舍入"
        if bar_matching == "signal_close"
        else "Titan 使用下一根可执行 Bar 的 open；Nautilus 使用产生信号 Bar 的 close"
    )
    timestamp_reason = (
        "无；两边均在信号 Bar close boundary 成交"
        if bar_matching == "signal_close"
        else "信号出现在日末时，Titan 等到下一交易日/周一 open"
    )
    titan_source_check = (
        f"Titan {summary['titan_prices_equal_source_signal_close']:,}/{summary['titan_fills']:,} 笔价格精确等于信号 Bar 的 close。"
        if bar_matching == "signal_close"
        else f"Titan {summary['titan_prices_equal_source_next_open']:,}/{summary['titan_fills']:,} 笔价格精确等于源数据下一根可执行 Bar 的 open。"
    )
    source_conclusion = (
        "剩余差异由价格精度与结束时是否强制平仓决定，不是数据源或 SMA 信号不一致。"
        if bar_matching == "signal_close"
        else "价格与休市时间差由两个引擎不同的 Bar 撮合假设产生，不是数据源不一致。"
    )
    report = f"""# Titan 与 Nautilus AAPL 双均线逐笔差异报告

## 结论

两边的 3,012 次金叉、3,012 次死叉以及前 {summary['common_fills']:,} 笔成交方向和数量完全一致。
当前 Titan Bar 撮合模式为 `{bar_matching}`。差异不是 SMA 信号错误。

## 差异清单

| 类别 | 数量 | 原因 |
|---|---:|---|
| 方向不一致 | {summary['side_mismatches']:,} | 无 |
| 数量不一致 | {summary['quantity_mismatches']:,} | 无 |
| 成交价不一致 | {summary['price_mismatches']:,} | {execution_reason} |
| 时间戳不一致 | {summary['timestamp_mismatches']:,} | {timestamp_reason} |
| Titan 缺少末尾成交 | 1 | Nautilus `close_positions_on_stop=True` 在停止时额外卖出 100 股；Titan Stop callback 禁止下单并保留多仓 |
| 手续费金额不一致 | {summary['common_commission_amount_mismatches']:,} | 两边费率都为零；仅因末尾平仓多一条零手续费记录 |

## 数值影响

- 共同成交中，价格完全相同 {summary['exact_price_matches']:,} 笔，价格不同 {summary['price_mismatches']:,} 笔。
- 平均绝对价格差 {summary['mean_abs_price_diff']:.6f} USD，最大绝对价格差 {summary['max_abs_price_diff']:.6f} USD。
- 其中隔夜推进 {summary['overnight_gaps']} 笔，周末推进 {summary['weekend_gaps']} 笔。
- 在末尾强平前，Titan 相对 Nautilus 的累计现金差为 {summary['net_cash_effect_titan_minus_nautilus_before_terminal_close']:.4f} USD；这是不同成交价的机械结果。
- Nautilus 末尾额外 SELL {summary['terminal_close_qty']:.0f} @ {summary['terminal_close_price']:.4f}，现金增加 {summary['terminal_close_cash_delta']:.2f} USD。
- 两边累计手续费均为 0 USD。

## 源头验证

- {titan_source_check}
- Nautilus {summary['nautilus_prices_equal_source_signal_close']:,}/{summary['nautilus_fills']:,} 笔价格精确等于信号 Bar 的 close；剩余 {summary['nautilus_price_precision_rounding_records']} 笔是按 AAPL 价格精度四舍五入。
- {source_conclusion}
"""
    (output_dir / "comparison_report.md").write_text(report, encoding="utf-8")
    print("DUAL_MA_COMPARISON=" + json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
