//! Runs the Rust market-making strategy on the backtest engine.
//!
//! ```console
//! # synthetic demo data (no files needed)
//! cargo run -p titan-examples --bin backtest
//!
//! # real data in the normalized npz format (key `data`)
//! cargo run -p titan-examples --bin backtest -- --data /path/to/btc.npz
//!
//! # native 1-minute Bar replay from Parquet
//! cargo run -p titan-examples --bin backtest --release -- \
//!   --data-kind bar --data data/AAPL_1m_all_sources.parquet --bar-source polygon_s3
//! ```

use std::{ffi::c_void, fs::File, path::Path, time::Instant};

use anyhow::{Context, Result};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use clap::{Parser, ValueEnum};
use hftbacktest::{
    backtest::{
        Backtest, DataSource,
        ExchangeKind::NoPartialFillExchange,
        L2AssetBuilder,
        assettype::LinearAsset,
        data::{Data, Field, NpyDTyped, POD, read_npy_file, read_npz_file},
        models::{
            CommonFees, ConstantLatency, PowerProbQueueFunc3, ProbQueueModel, TradingValueFeeModel,
        },
    },
    market_data::{BAR_COMPLETE, BAR_NATIVE, Bar},
    prelude::{
        BUY_EVENT, Bot, DEPTH_SNAPSHOT_EVENT, EXCH_EVENT, Event, HashMapMarketDepth, LOCAL_EVENT,
        SELL_EVENT, StrategyCtx, StrategySpec, TRADE_EVENT, run_strategy,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use titan_examples::market_making::MarketMaking;
use titan_runtime::{
    CallbackRegistry, MaterializedBarSource, StrategyEventKind, StrategyRuntimeContext,
    TimedBarItem, run_event_runtime,
};

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum DataKind {
    #[default]
    Tick,
    Bar,
}

/// Flat NPY storage row. Its field order mirrors `TimedBarItem` without a nested dtype,
/// which lets the existing aligned NPY reader validate and copy it efficiently.
#[repr(C)]
#[derive(Clone)]
struct TimedBarRow {
    asset_no: u64,
    timeframe_ns: i64,
    open_ts: i64,
    close_ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    quote_volume: f64,
    buy_volume: f64,
    trade_count: u64,
    flags: u64,
}

unsafe impl POD for TimedBarRow {}

impl NpyDTyped for TimedBarRow {
    fn descr() -> Vec<Field> {
        let endian = if cfg!(target_endian = "little") {
            "<"
        } else {
            ">"
        };
        [
            ("asset_no", "u8"),
            ("timeframe_ns", "i8"),
            ("open_ts", "i8"),
            ("close_ts", "i8"),
            ("open", "f8"),
            ("high", "f8"),
            ("low", "f8"),
            ("close", "f8"),
            ("volume", "f8"),
            ("quote_volume", "f8"),
            ("buy_volume", "f8"),
            ("trade_count", "u8"),
            ("flags", "u8"),
        ]
        .into_iter()
        .map(|(name, ty)| Field {
            name: name.to_string(),
            ty: format!("{endian}{ty}"),
        })
        .collect()
    }
}

impl From<&TimedBarRow> for TimedBarItem {
    fn from(row: &TimedBarRow) -> Self {
        Self {
            asset_no: row.asset_no,
            timeframe_ns: row.timeframe_ns,
            bar: Bar {
                open_ts: row.open_ts,
                close_ts: row.close_ts,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                quote_volume: row.quote_volume,
                buy_volume: row.buy_volume,
                trade_count: row.trade_count,
                flags: row.flags,
            },
        }
    }
}

#[derive(Parser, Debug)]
struct Args {
    /// Input kind. Tick expects normalized NPZ; Bar expects the canonical Parquet columns.
    #[arg(long, value_enum, default_value_t)]
    data_kind: DataKind,

    /// Input path. Omit only in Tick mode to run on synthetic demo data.
    #[arg(long)]
    data: Option<String>,

    /// Select one source from an all-sources Bar file (for example `polygon_s3`).
    #[arg(long)]
    bar_source: Option<String>,

    /// Fixed Bar duration in nanoseconds.
    #[arg(long, default_value_t = 60_000_000_000)]
    bar_timeframe_ns: i64,

    /// Number of preceding closed bars retained by the native runtime.
    #[arg(long, default_value_t = 1024)]
    history_capacity: usize,

    /// Number of complete Bar runtime invocations (loads the converted file once).
    #[arg(long, default_value_t = 1)]
    runs: usize,

    /// Include rows whose `is_final` column is false.
    #[arg(long, default_value_t = false)]
    include_unfinalized_bars: bool,

    #[arg(long, default_value_t = 0.0)]
    a: f64,
    #[arg(long, default_value_t = 1.0)]
    b: f64,
    #[arg(long, default_value_t = 0.05)]
    c: f64,
    #[arg(long, default_value_t = 1.0)]
    hs: f64,
    #[arg(long, default_value_t = 1000.0)]
    max_notional_position: f64,
    #[arg(long, default_value_t = 100.0)]
    notional_qty: f64,
}

fn event(ev: u64, ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev,
        exch_ts: ts,
        local_ts: ts,
        px,
        qty,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    }
}

/// Generates ~60s of tick data: a two-sided book plus a random-walk trade stream.
fn demo_data() -> Data<Event> {
    let t0 = 1_000_000_000i64;
    let mut rows = Vec::new();
    let mut mid = 100.0f64;
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut rand = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed >> 32) as f64 / u32::MAX as f64
    };

    for i in 1..=5 {
        rows.push(event(
            DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT,
            t0,
            mid - i as f64 * 0.1,
            1.0 + i as f64 * 0.5,
        ));
        rows.push(event(
            DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT,
            t0,
            mid + i as f64 * 0.1,
            1.0 + i as f64 * 0.5,
        ));
    }

    let mut ts = t0;
    while ts < t0 + 60_000_000_000 {
        ts += 25_000_000; // 25 ms
        // 大部分步长 1 tick，偶尔 2 tick，让做市单有被吃掉的机会。
        mid += (rand() - 0.5) * 0.3;
        mid = mid.clamp(99.0, 101.0);
        mid = (mid * 10.0).round() / 10.0; // 对齐 0.1 tick

        let side_buy = rand() > 0.5;
        // 成交打印在被动方最优价：买单吃 ask，卖单吃 bid——与真实盘口成交一致，
        // 这样挂在最优价的做市单会被吃掉。
        let trade_px = if side_buy { mid + 0.1 } else { mid - 0.1 };
        rows.push(event(
            TRADE_EVENT | if side_buy { BUY_EVENT } else { SELL_EVENT } | EXCH_EVENT | LOCAL_EVENT,
            ts,
            trade_px,
            0.1 + rand() * 0.9,
        ));
        // 刷新双边最优价。
        rows.push(event(
            DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT,
            ts,
            mid - 0.1,
            1.0,
        ));
        rows.push(event(
            DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT,
            ts,
            mid + 0.1,
            1.0,
        ));
    }

    Data::from_data(&rows)
}

#[derive(Default)]
struct BarReplayStats {
    batches: u64,
    bars: u64,
    first_open_ts: Option<i64>,
    last_close_ts: Option<i64>,
    volume: f64,
}

unsafe extern "C" fn on_native_bar(ctx: *mut StrategyRuntimeContext) -> i32 {
    // Safety: run_bar_replay keeps both objects alive for the synchronous runtime call.
    let ctx = unsafe { &mut *ctx };
    let stats = unsafe { &mut *(ctx.user_data as *mut BarReplayStats) };
    let bars = if ctx.bars_ptr.is_null() {
        &[]
    } else {
        // Safety: the runtime guarantees this view until the callback returns.
        unsafe { std::slice::from_raw_parts(ctx.bars_ptr, ctx.num_bars) }
    };
    stats.batches += 1;
    stats.bars += bars.len() as u64;
    for item in bars {
        stats.first_open_ts.get_or_insert(item.bar.open_ts);
        stats.last_close_ts = Some(item.bar.close_ts);
        stats.volume += item.bar.volume;
    }
    0
}

fn required_column<'a, T: Array + 'static>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .with_context(|| format!("missing required Parquet column `{name}`"))?
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("Parquet column `{name}` has an incompatible type"))
}

fn read_bar_parquet(
    path: &Path,
    selected_source: Option<&str>,
    timeframe_ns: i64,
    include_unfinalized: bool,
) -> Result<Vec<TimedBarItem>> {
    anyhow::ensure!(timeframe_ns > 0, "--bar-timeframe-ns must be positive");
    let file = File::open(path)
        .with_context(|| format!("failed to open Bar Parquet file {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("failed to read Parquet metadata")?
        .with_batch_size(65_536)
        .build()
        .context("failed to construct Parquet batch reader")?;
    let mut records = Vec::new();

    for batch in reader {
        let batch = batch.context("failed to decode a Parquet record batch")?;
        let timestamps = required_column::<TimestampMicrosecondArray>(&batch, "ts")?;
        let opens = required_column::<Float64Array>(&batch, "open")?;
        let highs = required_column::<Float64Array>(&batch, "high")?;
        let lows = required_column::<Float64Array>(&batch, "low")?;
        let closes = required_column::<Float64Array>(&batch, "close")?;
        let volumes = required_column::<Float64Array>(&batch, "volume")?;
        let vwaps = required_column::<Float64Array>(&batch, "vwap")?;
        let transaction_counts = required_column::<Int32Array>(&batch, "transaction_count")?;
        let sources = required_column::<StringArray>(&batch, "source")?;
        let final_flags = required_column::<BooleanArray>(&batch, "is_final")?;

        for row in 0..batch.num_rows() {
            if timestamps.is_null(row)
                || opens.is_null(row)
                || highs.is_null(row)
                || lows.is_null(row)
                || closes.is_null(row)
                || volumes.is_null(row)
                || sources.is_null(row)
            {
                anyhow::bail!("Bar Parquet contains a null required value at row {row}");
            }
            if selected_source.is_some_and(|source| sources.value(row) != source) {
                continue;
            }
            let is_final = !final_flags.is_null(row) && final_flags.value(row);
            if !include_unfinalized && !is_final {
                continue;
            }
            let open_ts = timestamps
                .value(row)
                .checked_mul(1_000)
                .context("Bar timestamp overflows nanoseconds")?;
            let close_ts = open_ts
                .checked_add(timeframe_ns)
                .context("Bar close timestamp overflow")?;
            let volume = volumes.value(row);
            let quote_volume = if !vwaps.is_null(row) && vwaps.value(row).is_finite() {
                vwaps.value(row) * volume
            } else {
                0.0
            };
            let trade_count = if transaction_counts.is_null(row) {
                0
            } else {
                u64::try_from(transaction_counts.value(row)).unwrap_or(0)
            };
            records.push(TimedBarItem {
                asset_no: 0,
                timeframe_ns,
                bar: Bar {
                    open_ts,
                    close_ts,
                    open: opens.value(row),
                    high: highs.value(row),
                    low: lows.value(row),
                    close: closes.value(row),
                    volume,
                    quote_volume,
                    buy_volume: 0.0,
                    trade_count,
                    flags: BAR_COMPLETE | BAR_NATIVE,
                },
            });
        }
    }

    anyhow::ensure!(
        !records.is_empty(),
        "no finalized Bar rows matched{}",
        selected_source
            .map(|source| format!(" source `{source}`"))
            .unwrap_or_default()
    );
    records
        .sort_unstable_by_key(|record| (record.bar.close_ts, record.timeframe_ns, record.asset_no));
    if records.windows(2).any(|pair| {
        pair[0].bar.close_ts == pair[1].bar.close_ts
            && pair[0].timeframe_ns == pair[1].timeframe_ns
            && pair[0].asset_no == pair[1].asset_no
    }) {
        anyhow::bail!(
            "Bar file contains overlapping sources; pass --bar-source (AAPL example: polygon_s3)"
        );
    }
    Ok(records)
}

fn read_bar_npy(path: &Path) -> Result<Vec<TimedBarItem>> {
    let filepath = path
        .to_str()
        .context("converted Bar NPY path is not UTF-8")?;
    let rows = read_npy_file::<TimedBarRow>(filepath)
        .with_context(|| format!("failed to read converted Bar NPY {}", path.display()))?;
    anyhow::ensure!(!rows.is_empty(), "converted Bar NPY is empty");
    Ok((0..rows.len()).map(|index| (&rows[index]).into()).collect())
}

fn run_bar_replay(args: &Args) -> Result<()> {
    let path = args
        .data
        .as_deref()
        .context("Bar mode requires --data <file.parquet|file.npy>")?;
    anyhow::ensure!(args.runs > 0, "--runs must be positive");
    let input = Path::new(path);
    let load_started = Instant::now();
    let records = match input.extension().and_then(|value| value.to_str()) {
        Some("parquet") => read_bar_parquet(
            input,
            args.bar_source.as_deref(),
            args.bar_timeframe_ns,
            args.include_unfinalized_bars,
        )?,
        Some("npy") => read_bar_npy(input)?,
        _ => anyhow::bail!("Bar mode accepts canonical .parquet or converted flat .npy input"),
    };
    let load_seconds = load_started.elapsed().as_secs_f64();
    let mut callbacks = CallbackRegistry::default();
    callbacks.set(StrategyEventKind::Bar, on_native_bar);
    let mut durations = Vec::with_capacity(args.runs);
    let mut final_stats = BarReplayStats::default();
    for _ in 0..args.runs {
        let started = Instant::now();
        let mut source = MaterializedBarSource::new(&records, args.history_capacity)
            .context("invalid materialized Bar input")?;
        let mut stats = BarReplayStats::default();
        let mut ctx = StrategyRuntimeContext {
            user_data: (&mut stats as *mut BarReplayStats).cast::<c_void>(),
            ..StrategyRuntimeContext::default()
        };
        source.configure_context(&mut ctx);
        run_event_runtime(&mut source, &callbacks, &mut ctx).context("Bar replay failed")?;
        durations.push(started.elapsed().as_secs_f64());
        final_stats = stats;
    }
    eprintln!(
        "bar replay: rows={} batches={} timeframe_ns={} first_open_ts={} last_close_ts={} volume={:.4} load_seconds={:.6}",
        final_stats.bars,
        final_stats.batches,
        records.first().map_or(0, |record| record.timeframe_ns),
        final_stats.first_open_ts.unwrap_or_default(),
        final_stats.last_close_ts.unwrap_or_default(),
        final_stats.volume,
        load_seconds,
    );
    durations.sort_unstable_by(f64::total_cmp);
    let total: f64 = durations.iter().sum();
    let mean = total / args.runs as f64;
    let median = if args.runs % 2 == 0 {
        (durations[args.runs / 2 - 1] + durations[args.runs / 2]) / 2.0
    } else {
        durations[args.runs / 2]
    };
    let p95 = durations[(args.runs * 95).div_ceil(100).saturating_sub(1)];
    eprintln!(
        "benchmark: runs={} total_seconds={:.6} mean_ms={:.3} median_ms={:.3} p95_ms={:.3} min_ms={:.3} max_ms={:.3} throughput_bars_per_second={:.0}",
        args.runs,
        total,
        mean * 1_000.0,
        median * 1_000.0,
        p95 * 1_000.0,
        durations[0] * 1_000.0,
        durations[args.runs - 1] * 1_000.0,
        records.len() as f64 * args.runs as f64 / total,
    );
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    if matches!(args.data_kind, DataKind::Bar) {
        return run_bar_replay(&args);
    }

    let data = match &args.data {
        Some(path) => read_npz_file::<Event>(path, "data")
            .with_context(|| format!("failed to load backtest data from {path}"))?,
        None => {
            eprintln!("No --data given; running on synthetic demo data.");
            demo_data()
        }
    };

    let asset = L2AssetBuilder::default()
        .data(vec![DataSource::Data(data)])
        .latency_model(ConstantLatency::new(0, 0))
        .asset_type(LinearAsset::new(1.0))
        .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
        .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
        .exchange(NoPartialFillExchange)
        .depth(|| HashMapMarketDepth::new(0.1, 0.001))
        .last_trades_capacity(1024)
        .build()
        .context("failed to build backtest asset")?;

    let mut backtester = Backtest::builder()
        .add_asset(asset)
        .build()
        .context("failed to build backtester")?;

    let spec = StrategySpec {
        markets: vec![vec![0]],
        symbol_ids: vec![0],
    };
    let mut ctx = StrategyCtx::new(&spec, 1)?;
    let mut strategy = MarketMaking::new(
        args.a,
        args.b,
        args.c,
        args.hs,
        args.max_notional_position,
        args.notional_qty,
    );

    // 10 ms 全局帧，1 s bar——与 README Python 示例一致。
    run_strategy(
        &mut backtester,
        &mut strategy,
        &mut ctx,
        10_000_000,
        1_000_000_000,
    )?;

    let state = backtester.state_values(0);
    eprintln!(
        "frames={} bars={} orders_placed={} orders_canceled={}",
        strategy.frames, strategy.bars, strategy.orders_placed, strategy.orders_canceled,
    );
    eprintln!(
        "final: position={} balance={:.6} fee={:.6} trades={} volume={:.4}",
        state.position, state.balance, state.fee, state.num_trades, state.trading_volume
    );
    Ok(())
}
