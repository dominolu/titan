//! Runs the Rust market-making strategy on the backtest engine.
//!
//! ```console
//! # synthetic demo data (no files needed)
//! cargo run -p titan-examples --bin backtest
//!
//! # real data in the normalized npz format (key `data`)
//! cargo run -p titan-examples --bin backtest -- --data /path/to/btc.npz
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use hftbacktest::{
    backtest::{
        Backtest,
        DataSource,
        ExchangeKind::NoPartialFillExchange,
        L2AssetBuilder,
        assettype::LinearAsset,
        data::{Data, read_npz_file},
        models::{
            CommonFees,
            ConstantLatency,
            PowerProbQueueFunc3,
            ProbQueueModel,
            TradingValueFeeModel,
        },
    },
    prelude::{
        Bot,
        Event,
        HashMapMarketDepth,
        StrategyCtx,
        StrategySpec,
        BUY_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EXCH_EVENT,
        LOCAL_EVENT,
        SELL_EVENT,
        TRADE_EVENT,
        run_strategy,
    },
};
use titan_examples::market_making::MarketMaking;

#[derive(Parser, Debug)]
struct Args {
    /// Normalized backtest data (npz, key `data`). Omit to run on synthetic demo data.
    #[arg(long)]
    data: Option<String>,

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

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

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
    run_strategy(&mut backtester, &mut strategy, &mut ctx, 10_000_000, 1_000_000_000)?;

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
