//! Release benchmark for the P0-B Tick migration hot path.
//!
//! Compares the monomorphized legacy matcher with a no-op observer against the same matcher with
//! the shared exchange-time OutcomeBus enabled. Run with `--release`.

use std::{hint::black_box, time::Instant};

use anyhow::Result;
use clap::Parser;
use hftbacktest::{
    backtest::{
        Asset, Backtest, DataSource,
        ExchangeKind::NoPartialFillExchange as NoPartialKind,
        L2AssetBuilder,
        assettype::LinearAsset,
        data::Data,
        models::{CommonFees, ConstantLatency, RiskAdverseQueueModel, TradingValueFeeModel},
        order::order_bus,
        proc::{Local, LocalProcessor, NoPartialFillExchange, Processor},
        state::State,
    },
    depth::HashMapMarketDepth,
    prelude::{
        Bot, EXCH_ASK_DEPTH_EVENT, EXCH_BID_DEPTH_EVENT, Event, LOCAL_ASK_DEPTH_EVENT,
        LOCAL_BID_DEPTH_EVENT, OrdType, TimeInForce,
    },
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 1_000_000)]
    events: usize,
    #[arg(long, default_value_t = 10)]
    runs: usize,
    #[arg(long, default_value_t = 2)]
    warmup: usize,
}

fn data(events: usize) -> Data<Event> {
    let mut rows = Vec::with_capacity(events + 2);
    rows.push(Event {
        ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
        exch_ts: 1,
        local_ts: 1,
        px: 101.0,
        qty: 100.0,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    });
    for index in 0..events {
        let ts = index as i64 + 2;
        rows.push(Event {
            ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
            exch_ts: ts,
            local_ts: ts,
            px: 99.0 - (index & 1) as f64,
            qty: 10.0 + (index & 7) as f64,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        });
    }
    Data::from_data(&rows)
}

fn build_noop(data: Data<Event>) -> Result<Backtest<HashMapMarketDepth>> {
    let latency = ConstantLatency::new(0, 0);
    let (order_e2l, order_l2e) = order_bus(latency);
    let asset_type = LinearAsset::new(1.0);
    let fee = TradingValueFeeModel::new(CommonFees::new(0.0, 0.0));
    let local: Box<dyn LocalProcessor<HashMapMarketDepth>> = Box::new(Local::new(
        HashMapMarketDepth::new(1.0, 1.0),
        State::new(asset_type.clone(), fee.clone()),
        0,
        order_l2e,
    ));
    let exchange: Box<dyn Processor> = Box::new(NoPartialFillExchange::new(
        HashMapMarketDepth::new(1.0, 1.0),
        State::new(asset_type, fee),
        RiskAdverseQueueModel::new(),
        order_e2l,
    ));
    Backtest::builder()
        .add_asset(Asset {
            local,
            exch: exchange,
            reader: hftbacktest::backtest::data::Reader::builder()
                .data(vec![DataSource::Data(data)])
                .build()?,
            outcome_bus: None,
            shared_execution: None,
        })
        .build()
        .map_err(Into::into)
}

fn build_observed(data: Data<Event>) -> Result<Backtest<HashMapMarketDepth>> {
    Backtest::builder()
        .add_asset(
            L2AssetBuilder::default()
                .data(vec![DataSource::Data(data)])
                .latency_model(ConstantLatency::new(0, 0))
                .asset_type(LinearAsset::new(1.0))
                .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                .queue_model(RiskAdverseQueueModel::new())
                .exchange(NoPartialKind)
                .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                .build()?,
        )
        .build()
        .map_err(Into::into)
}

fn one_run(mut backtest: Backtest<HashMapMarketDepth>) -> Result<u64> {
    backtest.elapse_bt(1)?;
    backtest.submit_buy_order(0, 1, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
    backtest.goto_end()?;
    Ok(black_box(backtest.current_timestamp() as u64))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let source = data(args.events);
    for _ in 0..args.warmup {
        one_run(build_noop(source.clone())?)?;
        one_run(build_observed(source.clone())?)?;
    }
    let mut noop_seconds = 0.0;
    let mut observed_seconds = 0.0;
    let mut noop_checksum = 0;
    let mut observed_checksum = 0;
    // Alternate A/B order so CPU frequency and thermal drift cannot systematically favor one
    // implementation.
    for run in 0..args.runs {
        if run & 1 == 0 {
            let start = Instant::now();
            noop_checksum ^= one_run(build_noop(source.clone())?)?;
            noop_seconds += start.elapsed().as_secs_f64();
            let start = Instant::now();
            observed_checksum ^= one_run(build_observed(source.clone())?)?;
            observed_seconds += start.elapsed().as_secs_f64();
        } else {
            let start = Instant::now();
            observed_checksum ^= one_run(build_observed(source.clone())?)?;
            observed_seconds += start.elapsed().as_secs_f64();
            let start = Instant::now();
            noop_checksum ^= one_run(build_noop(source.clone())?)?;
            noop_seconds += start.elapsed().as_secs_f64();
        }
    }
    let regression_pct = (observed_seconds / noop_seconds - 1.0) * 100.0;
    println!(
        "events={} runs={} noop_seconds={noop_seconds:.6} observed_seconds={observed_seconds:.6} regression_pct={regression_pct:.3} noop_eps={:.0} observed_eps={:.0} checksum={}",
        args.events,
        args.runs,
        args.events as f64 * args.runs as f64 / noop_seconds,
        args.events as f64 * args.runs as f64 / observed_seconds,
        noop_checksum ^ observed_checksum,
    );
    Ok(())
}
