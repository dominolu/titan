use std::path::PathBuf;

use titan_strategy_runtime::{
    CallbackCommandStagingV13, NativeStrategyLoaderV13, StagedCommandV13,
    StrategyRuntimeContextV13, TitanAccountView, TitanFillView, TitanMarketView, TitanPositionView,
    TitanTickView, V13EventKind, V13TrustPolicy,
};

fn main() {
    let strategy_path = PathBuf::from(std::env::args_os().nth(1).expect("compiled_strategy path"));
    let cache = PathBuf::from(std::env::args_os().nth(2).expect("cache path"));
    let compiled_strategy = NativeStrategyLoaderV13::new(cache, V13TrustPolicy::default())
        .load(&strategy_path)
        .expect("load pair-arb compiled_strategy");
    let mut instance = compiled_strategy
        .instantiate(91, 1)
        .expect("instantiate pair-arb");
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
    let accounts = [
        TitanAccountView {
            account_no: 0,
            account_epoch: 1,
            account_sequence: 1,
            state: 4,
            ..Default::default()
        },
        TitanAccountView {
            account_no: 1,
            account_epoch: 1,
            account_sequence: 1,
            state: 4,
            ..Default::default()
        },
    ];
    let positions = [
        TitanPositionView {
            account_no: 0,
            asset_no: 0,
            ..Default::default()
        },
        TitanPositionView {
            account_no: 1,
            asset_no: 1,
            ..Default::default()
        },
    ];
    let mut context = StrategyRuntimeContextV13 {
        now_ns: 1_000_000_000,
        markets_ptr: markets.as_ptr(),
        markets_len: markets.len() as u64,
        accounts_ptr: accounts.as_ptr(),
        accounts_len: accounts.len() as u64,
        positions_ptr: positions.as_ptr(),
        positions_len: positions.len() as u64,
        ..Default::default()
    };
    instance
        .invoke(V13EventKind::Start, &mut context)
        .expect("start callback");
    instance.start().expect("open instance command gate");

    let ticks = [
        TitanTickView {
            asset_no: 0,
            receive_ts_ns: 1_000_000_000,
            price_ticks: 101,
            qty_lots: 1,
            ..Default::default()
        },
        TitanTickView {
            asset_no: 1,
            receive_ts_ns: 1_000_000_000,
            price_ticks: 202,
            qty_lots: 1,
            ..Default::default()
        },
    ];
    context.ticks_ptr = ticks.as_ptr();
    context.ticks_len = ticks.len() as u64;
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
    // Native recovery callback is command-free and accepts a host-established new session.
    let mut original = compiled_strategy.instantiate(92, 1).unwrap();
    original.invoke(V13EventKind::Start, &mut context).unwrap();
    let snapshot = original.freeze_state(1).unwrap();
    let mut recovered = compiled_strategy.instantiate(92, 2).unwrap();
    recovered.restore_state(&snapshot).unwrap();
    let new_accounts = accounts.map(|mut account| {
        account.account_epoch = 2;
        account.account_sequence = 9;
        account
    });
    context.accounts_ptr = new_accounts.as_ptr();
    context.active_orders_ptr = std::ptr::null();
    context.active_orders_len = 0;
    let mut recovery_staging = CallbackCommandStagingV13::new(8, 92, 2).unwrap();
    recovery_staging.begin_callback(false, &[]);
    recovery_staging.bind_context(&mut context);
    recovered
        .invoke(V13EventKind::Recover, &mut context)
        .unwrap();
    assert!(recovery_staging.finish_callback(0).is_empty());
    assert!(!recovered.command_gate_open());
    recovered.complete_restore([0; 32], 2).unwrap();
    recovered.invoke(V13EventKind::Start, &mut context).unwrap();
    println!("V13 pair-arb state-machine smoke passed");
}
