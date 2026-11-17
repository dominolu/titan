use std::{ffi::c_void, path::PathBuf};

use titan_strategy_runtime::{
    EventKind, NativeStrategyLoader, StrategyRuntimeContext, TitanMarketView,
    TitanSubmitOrderRequest,
};

unsafe extern "C" fn submit(
    _: *mut c_void,
    request: *const TitanSubmitOrderRequest,
    order_id: *mut u64,
) -> i32 {
    let request = unsafe { &*request };
    assert_eq!(request.account_no, 0);
    assert_eq!(request.asset_no, 0);
    assert_eq!(request.qty_lots, 2);
    assert_eq!(request.price_ticks, 100);
    unsafe { *order_id = 77 };
    0
}

fn main() {
    let manifest = PathBuf::from(std::env::args_os().nth(1).expect("manifest path"));
    let cache = PathBuf::from(std::env::args_os().nth(2).expect("cache path"));
    let loader = NativeStrategyLoader::new(cache);
    let compiled_strategy = loader.load(&manifest).expect("load compiled strategy");
    let mut instance = compiled_strategy
        .instantiate(42, 1)
        .expect("instantiate compiled_strategy");
    instance.start().expect("start compiled strategy instance");
    let markets = [TitanMarketView {
        asset_no: 0,
        best_bid_ticks: 2,
        ..TitanMarketView::default()
    }];
    let mut context = StrategyRuntimeContext {
        submit_order: Some(submit),
        markets_ptr: markets.as_ptr(),
        markets_len: markets.len() as u64,
        ..StrategyRuntimeContext::default()
    };
    instance
        .invoke(EventKind::Tick, &mut context)
        .expect("invoke on_tick");
    let words: Vec<u64> = instance
        .state
        .as_bytes()
        .chunks_exact(8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
        .collect();
    assert_eq!(&words[..5], &[5, 77, 0, 0, 3]);
    instance.update_checkpoint_boundary(7, [9; 32]);
    let snapshot = instance.freeze_state(1).expect("freeze checkpoint");
    let mut successor = compiled_strategy
        .instantiate(42, 2)
        .expect("instantiate successor generation");
    successor
        .restore_state(&snapshot)
        .expect("restore persisted generation");
    successor
        .complete_restore([9; 32], 2)
        .expect("reconcile successor generation");
    assert_eq!(successor.state.as_bytes(), instance.state.as_bytes());
    assert!(!successor.command_gate_open());
    println!("compiled strategy native compiled_strategy smoke passed: {words:?}");
}
