use std::path::PathBuf;

use titan_strategy_runtime::{
    CallbackCommandStagingV13, NativeArtifactLoaderV13, StagedCommandV13,
    StrategyRuntimeContextV13, TitanFillView, TitanMarketView, V13EventKind, V13TrustPolicy,
};

fn main() {
    let artifact_path = PathBuf::from(std::env::args_os().nth(1).expect("artifact path"));
    let cache = PathBuf::from(std::env::args_os().nth(2).expect("cache path"));
    let artifact = NativeArtifactLoaderV13::new(cache, V13TrustPolicy::default())
        .load(&artifact_path)
        .expect("load pair-arb artifact");
    let mut instance = artifact.instantiate(91, 1).expect("instantiate pair-arb");
    let markets = [
        TitanMarketView {
            asset_no: 0,
            best_bid_ticks: 100,
            best_ask_ticks: 101,
            ..Default::default()
        },
        TitanMarketView {
            asset_no: 1,
            best_bid_ticks: 202,
            best_ask_ticks: 203,
            ..Default::default()
        },
    ];
    let mut context = StrategyRuntimeContextV13 {
        now_ns: 1_000_000_000,
        markets_ptr: markets.as_ptr(),
        markets_len: markets.len() as u64,
        ..Default::default()
    };
    instance
        .invoke(V13EventKind::Start, &mut context)
        .expect("start callback");
    instance.start().expect("open instance command gate");

    let mut staging = CallbackCommandStagingV13::new(8, 100, 500).expect("command staging");
    staging.begin_callback(instance.command_gate_open(), &[]);
    staging.bind_context(&mut context);
    instance
        .invoke(V13EventKind::Tick, &mut context)
        .expect("tick callback");
    let initiator_id = match staging.finish_callback(0).first() {
        Some(StagedCommandV13::Submit { order_id, request }) => {
            assert_eq!(request.account_no, 0);
            assert_eq!(request.asset_no, 0);
            assert_eq!(request.qty_lots, 2);
            assert_eq!(request.time_in_force, 4);
            *order_id
        }
        command => panic!("expected initiator submit, got {command:?}"),
    };

    let fills = [TitanFillView {
        order_id: initiator_id,
        asset_no: 0,
        account_no: 0,
        fill_price_ticks: 100,
        fill_qty_lots: 2,
        cumulative_filled_lots: 2,
        receive_ts_ns: 1_100_000_000,
        account_sequence: 1,
        side: 1,
        final_fill: 1,
        ..Default::default()
    }];
    context.now_ns = 1_100_000_000;
    context.fills_ptr = fills.as_ptr();
    context.fills_len = fills.len() as u64;
    staging.begin_callback(instance.command_gate_open(), &[]);
    staging.bind_context(&mut context);
    instance
        .invoke(V13EventKind::Fill, &mut context)
        .expect("fill callback");
    match staging.finish_callback(0).first() {
        Some(StagedCommandV13::Submit { request, .. }) => {
            assert_eq!(request.account_no, 1);
            assert_eq!(request.asset_no, 1);
            assert_eq!(request.side, 2);
            assert_eq!(request.qty_lots, 2);
            assert_eq!(request.time_in_force, 2);
        }
        command => panic!("expected hedge submit, got {command:?}"),
    }
    println!("V13 pair-arb state-machine smoke passed");
}
