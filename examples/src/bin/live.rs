//! Runs the Rust market-making strategy on a live connector via iceoryx shared-memory IPC.
//!
//! Start a connector first (matching `--connector-name`), then:
//!
//! ```console
//! cargo run -p connector -- --name my-okx --connector okx --config okx_demo.toml
//! cargo run -p titan-examples --bin live -- \
//!     --connector-name my-okx --symbol BTC-USDT-SWAP \
//!     --tick-size 0.1 --lot-size 0.001 --run-seconds 30
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use hftbacktest::{
    live::{Instrument, LiveBotBuilder, ipc::iceoryx::IceoryxUnifiedChannel},
    prelude::{Bot, HashMapMarketDepth, StrategyCtx, StrategySpec, run_strategy, run_strategy_for},
};
use titan_examples::market_making::MarketMaking;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "market-making-live",
    about = "Rust market-making strategy on a live connector"
)]
struct Args {
    /// Connector name; must match the `--name` of the running connector process.
    #[arg(long)]
    connector_name: String,
    #[arg(long, default_value = "BTC-USDT-SWAP")]
    symbol: String,
    #[arg(long, default_value_t = 0.1)]
    tick_size: f64,
    #[arg(long, default_value_t = 0.001)]
    lot_size: f64,
    /// Global frame interval in milliseconds.
    #[arg(long, default_value_t = 1)]
    frame_ms: u64,
    /// Bar interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    bar_ms: u64,
    /// Run duration in seconds; 0 = run until interrupted.
    #[arg(long, default_value_t = 0)]
    run_seconds: u64,

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

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut hbt = LiveBotBuilder::new()
        .register(Instrument::new(
            &args.connector_name,
            &args.symbol,
            args.tick_size,
            args.lot_size,
            HashMapMarketDepth::new(args.tick_size, args.lot_size),
            1024,
        ))
        .order_recv_hook(|_old, new| {
            tracing::info!(
                order_id = new.order_id,
                side = ?new.side,
                req = ?new.req,
                status = ?new.status,
                price_tick = new.price_tick,
                qty = new.qty,
                "order update"
            );
            Ok(())
        })
        .build::<IceoryxUnifiedChannel>()
        .context("failed to build LiveBot — is the connector process running?")?;
    info!(
        connector_name = %args.connector_name,
        symbol = %args.symbol,
        "connected to connector; waiting for market data"
    );

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

    let frame_ns = args.frame_ms as i64 * 1_000_000;
    let bar_ns = args.bar_ms as i64 * 1_000_000;
    let run = if args.run_seconds > 0 {
        run_strategy_for(
            &mut hbt,
            &mut strategy,
            &mut ctx,
            frame_ns,
            bar_ns,
            args.run_seconds as i64 * 1_000_000_000,
        )
    } else {
        run_strategy(&mut hbt, &mut strategy, &mut ctx, frame_ns, bar_ns)
    };
    if let Err(e) = run {
        error!(?e, "strategy loop exited with an error");
    }

    // 清理残留挂单（已成交/已拒绝的不可撤，直接清理出本地订单表）。
    hbt.clear_inactive_orders(Some(0));
    let open: Vec<u64> = hbt
        .orders(0)
        .iter()
        .filter(|(_, order)| order.cancellable())
        .map(|(&order_id, _)| order_id)
        .collect();
    for order_id in open {
        match hbt.cancel(0, order_id, true) {
            Ok(_) => info!(order_id, "canceled on shutdown"),
            Err(e) => warn!(?e, order_id, "cancel failed on shutdown"),
        }
    }

    info!(
        frames = strategy.frames,
        orders_placed = strategy.orders_placed,
        orders_canceled = strategy.orders_canceled,
        position = hbt.position(0),
        "live run finished"
    );
    Ok(())
}
