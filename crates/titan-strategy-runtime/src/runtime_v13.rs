use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, OnceLock, atomic::{AtomicU64, Ordering}},
    time::{Instant, SystemTime},
};

use titan_account_service::{
    AssetId, BalanceChangedV1, DirectCancelOrderRequest, DirectNewOrderRequest, FillV2,
    OrderChangedV1, PositionChangedV1, StreamInvalidatedV1, StreamStateChangedV1,
};
use titan_core_types::{CoreError, EventHandler, EventView};
use titan_event_engine::{EngineError, LaneProgress, PrimaryAsyncLaneHandle, SubscriberState};

use crate::*;

const PUBLIC_CAPACITY: usize = 1_024;

pub struct NativeV13RuntimeFactory;

impl StrategyRuntimeFactory for NativeV13RuntimeFactory {
    fn strategy_type(&self) -> &str { "native-v13" }

    fn create(
        &self,
        definition: &StrategyDefinition,
        artifact: StrategyArtifact,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError> {
        let artifact = artifact.native;
        let routing_keys = context.markets.iter().map(|item| u64::from(item.asset_id))
            .chain(context.accounts.iter().map(|item| u64::from(item.account.account_id.0)))
            .collect();
        let config = StrategyInstanceConfigV13 {
            strategy_instance_id: u64::from(definition.strategy_id.0),
            markets: context.markets.iter().map(|item| (item.local_asset_no, item.asset_id)).collect(),
            accounts: context.accounts.iter().map(|item| (item.local_account_no, item.account.account_id.0)).collect(),
            routing_keys,
            max_commands_per_callback: definition.runtime.timer_capacity.max(16),
            max_handler_duration: definition.runtime.max_handler_duration,
        };
        let generation = context.strategy.generation;
        let instance = artifact.instantiate_with_config(&config, generation)
            .map_err(|_| v13_runtime_error("v13_instantiate_failed"))?;
        let mut markets = vec![TitanMarketView::default(); context.markets.iter()
            .map(|item| item.local_asset_no as usize + 1).max().unwrap_or(0)];
        for binding in context.markets.iter() {
            let market = &mut markets[binding.local_asset_no as usize];
            market.asset_no = binding.local_asset_no;
            market.tick_size = 1;
            market.lot_size = 1;
        }
        let accounts = context.accounts.iter().map(|binding| TitanAccountView {
            account_no: binding.local_account_no,
            state: 4,
            ..TitanAccountView::default()
        }).collect();
        Ok(Arc::new(NativeV13Runtime {
            core: Arc::new(NativeV13Core {
                context,
                inner: Mutex::new(NativeV13Inner {
                    lifecycle: StrategyLifecycle::Defined,
                    instance,
                    staging: CallbackCommandStagingV13::new(
                        config.max_commands_per_callback,
                        config.strategy_instance_id,
                        generation,
                    ).map_err(|_| v13_runtime_error("v13_staging_failed"))?,
                    markets,
                    positions: Vec::with_capacity(PUBLIC_CAPACITY),
                    balances: Vec::with_capacity(PUBLIC_CAPACITY),
                    accounts,
                    active_orders: Vec::with_capacity(PUBLIC_CAPACITY),
                    ticks: Vec::with_capacity(PUBLIC_CAPACITY),
                    depth: Vec::with_capacity(PUBLIC_CAPACITY),
                    callback_count: 0,
                    command_count: 0,
                    last_error: None,
                    stop_called: false,
                    flight_records: VecDeque::with_capacity(128),
                }),
                lane: OnceLock::new(),
                next_operation: AtomicU64::new(1),
                operations: Mutex::new(BTreeMap::new()),
            }),
        }))
    }
}

pub struct NativeV13Runtime { core: Arc<NativeV13Core> }

struct NativeV13Core {
    context: StrategyRuntimeBuildContext,
    inner: Mutex<NativeV13Inner>,
    lane: OnceLock<PrimaryAsyncLaneHandle>,
    next_operation: AtomicU64,
    operations: Mutex<BTreeMap<StrategyOperationId, StrategyOperationSnapshot>>,
}

struct NativeV13Inner {
    lifecycle: StrategyLifecycle,
    instance: StrategyInstanceV13,
    staging: CallbackCommandStagingV13,
    markets: Vec<TitanMarketView>,
    positions: Vec<TitanPositionView>,
    balances: Vec<TitanBalanceView>,
    accounts: Vec<TitanAccountView>,
    active_orders: Vec<TitanActiveOrderView>,
    ticks: Vec<TitanTickView>,
    depth: Vec<TitanDepthView>,
    callback_count: u64,
    command_count: u64,
    last_error: Option<Arc<str>>,
    stop_called: bool,
    flight_records: VecDeque<StrategyFlightRecord>,
}

impl NativeV13Runtime {
    fn schedule(&self, action: impl FnOnce(&Arc<NativeV13Core>) -> LocalResult<()> + Send + 'static)
        -> LocalResult<StrategyOperationId>
    {
        let lane = self.core.lane.get().ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let id = StrategyOperationId(self.core.next_operation.fetch_add(1, Ordering::AcqRel));
        self.core.operations.lock().unwrap_or_else(|p| p.into_inner()).insert(id, StrategyOperationSnapshot {
            id, strategy: Some(self.core.context.strategy), state: StrategyOperationState::Pending,
            detail: Arc::from("pending"),
        });
        let core = self.core.clone();
        lane.submit_safe_point(move || {
            let result = action(&core);
            let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
            let snapshot = operations.get_mut(&id).expect("operation exists");
            match result {
                Ok(()) => { snapshot.state = StrategyOperationState::Succeeded; snapshot.detail = Arc::from("succeeded"); Ok(()) }
                Err(error) => { snapshot.state = StrategyOperationState::Failed; snapshot.detail = error.reason_code; Err(EngineError::SafePointPanicked) }
            }
        }).map_err(|_| v13_runtime_error("control_queue_full"))?;
        Ok(id)
    }

    fn invoke(inner: &mut NativeV13Inner, core: &NativeV13Core, kind: V13EventKind,
              schema_version: u32, configure: impl FnOnce(&mut StrategyRuntimeContextV13))
        -> LocalResult<()>
    {
        let mut context = StrategyRuntimeContextV13 {
            event_schema_version: schema_version,
            now_ns: core.context.clock.now_ns(),
            markets_ptr: inner.markets.as_ptr(), markets_len: inner.markets.len() as u64,
            positions_ptr: inner.positions.as_ptr(), positions_len: inner.positions.len() as u64,
            balances_ptr: inner.balances.as_ptr(), balances_len: inner.balances.len() as u64,
            accounts_ptr: inner.accounts.as_ptr(), accounts_len: inner.accounts.len() as u64,
            active_orders_ptr: inner.active_orders.as_ptr(), active_orders_len: inner.active_orders.len() as u64,
            ..StrategyRuntimeContextV13::default()
        };
        configure(&mut context);
        let gate = core.context.activation.is_open() && inner.instance.command_gate_open();
        inner.staging.begin_callback(gate, &inner.active_orders);
        inner.staging.bind_context(&mut context);
        if inner.instance.invoke(kind, &mut context).is_err() {
            inner.staging.finish_callback(-1);
            core.context.activation.close();
            inner.lifecycle = StrategyLifecycle::Failed;
            inner.last_error = Some(Arc::from("v13_callback_failed"));
            return Err(v13_runtime_error("v13_callback_failed"));
        }
        let commands = inner.staging.finish_callback(0).to_vec();
        commit_commands(core, inner, &commands)?;
        inner.staging.clear_committed();
        inner.callback_count = inner.callback_count.saturating_add(1);
        Ok(())
    }
}

impl EventHandler for NativeV13Runtime {
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError> {
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(inner.lifecycle, StrategyLifecycle::Running | StrategyLifecycle::Stopping) { return Ok(()); }
        if self.core.lane.get().is_some_and(|lane| lane.health().state != SubscriberState::Normal) {
            return Ok(());
        }
        let result = dispatch_event(&self.core, &mut inner, event);
        if result.is_err() {
            self.core.context.activation.close();
            inner.lifecycle = StrategyLifecycle::Failed;
            inner.last_error = Some(Arc::from("v13_event_dispatch_failed"));
            return Err(v13_core_error());
        }
        Ok(())
    }
}

fn dispatch_event(core: &NativeV13Core, inner: &mut NativeV13Inner, event: EventView<'_>) -> LocalResult<()> {
    match (event.event_type, event.schema_version) {
        (titan_market_service::BBO_EVENT | titan_market_service::TRADE_BATCH_EVENT, 1) => {
            decode_market_batch(core, inner, event, false)?;
            let ptr = inner.ticks.as_ptr(); let len = inner.ticks.len() as u64;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Tick, 1, |ctx| { ctx.ticks_ptr = ptr; ctx.ticks_len = len; })
        }
        (titan_market_service::DEPTH_BATCH_EVENT, 1) => {
            decode_market_batch(core, inner, event, true)?;
            let ptr = inner.depth.as_ptr(); let len = inner.depth.len() as u64;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Depth, 1, |ctx| { ctx.depth_ptr = ptr; ctx.depth_len = len; })
        }
        (titan_account_service::FILL_EVENT, titan_account_service::FILL_EVENT_SCHEMA_VERSION) => {
            let fill = FillV2::decode(event.payload).map_err(|_| v13_runtime_error("fill_decode"))?;
            let (account_no, asset_no) = account_binding(core, fill.header.account_id, fill.asset_id)?;
            let order_id = v13_strategy_order_id(core.context.strategy, fill.client_order_id);
            if order_id == 0 { return Ok(()); }
            let view = TitanFillView { order_id,
                asset_no, account_no, fill_price_ticks: fill.price_ticks,
                fill_qty_lots: fill.last_fill_quantity_lots,
                cumulative_filled_lots: fill.cumulative_filled_quantity_lots,
                exchange_ts_ns: fill.header.exchange_ts, receive_ts_ns: fill.header.receive_ts,
                account_sequence: fill.header.account_version, side: fill.side,
                liquidity: fill.liquidity, final_fill: u8::from(fill.header.flags & titan_account_service::event_flags::FINAL != 0),
                reserved: [0; 5] };
            update_active_fill(inner, &view);
            NativeV13Runtime::invoke(inner, core, V13EventKind::Fill, 2, |ctx| { ctx.fills_ptr = &view; ctx.fills_len = 1; })
        }
        (titan_account_service::ORDER_CHANGED_EVENT, 1) => {
            let value = OrderChangedV1::decode(event.payload).map_err(|_| v13_runtime_error("order_decode"))?;
            let (account_no, asset_no) = account_binding(core, value.header.account_id, value.asset_id)?;
            let order_id = v13_strategy_order_id(core.context.strategy, value.client_order_id);
            if order_id == 0 { return Ok(()); }
            let view = TitanOrderEventView { order_id,
                asset_no, account_no, price_ticks: value.price_ticks, qty_lots: value.quantity_lots,
                cumulative_filled_lots: value.filled_quantity_lots, event_ts_ns: value.header.receive_ts,
                account_sequence: value.header.account_version, status: value.status, reason: 0, reserved: [0; 6] };
            update_active_order(inner, &view, value.side, value.order_type, value.time_in_force)?;
            if matches!(value.status, 6 | 7 | 8) {
                let cancel = TitanCancelEventView { order_id, asset_no, account_no,
                    event_ts_ns: value.header.receive_ts,
                    account_sequence: value.header.account_version,
                    request_result: 0, final_status: value.status, reserved: [0; 6] };
                NativeV13Runtime::invoke(inner, core, V13EventKind::Cancel, 1, |ctx| {
                    ctx.cancel_events_ptr = &cancel; ctx.cancel_events_len = 1;
                })?;
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Order, 1, |ctx| { ctx.order_events_ptr = &view; ctx.order_events_len = 1; })
        }
        (titan_account_service::POSITION_CHANGED_EVENT, 1) => {
            let value = PositionChangedV1::decode(event.payload).map_err(|_| v13_runtime_error("position_decode"))?;
            let (account_no, asset_no) = account_binding(core, value.header.account_id, value.asset_id)?;
            let view = TitanPositionEventView { asset_no, account_no, qty_lots: value.quantity_lots,
                average_price_ticks: value.entry_price_ticks, realized_pnl_ticks: value.realized_pnl_units,
                event_ts_ns: value.header.receive_ts, account_sequence: value.header.account_version };
            upsert_position(inner, view)?;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Position, 1, |ctx| { ctx.position_events_ptr = &view; ctx.position_events_len = 1; })
        }
        (titan_account_service::BALANCE_CHANGED_EVENT, 1) => {
            let value = BalanceChangedV1::decode(event.payload).map_err(|_| v13_runtime_error("balance_decode"))?;
            let account_no = local_account(core, value.header.account_id)?;
            let view = TitanBalanceEventView { account_no, currency_no: value.currency_id,
                total_units: value.wallet_units, available_units: value.available_units,
                event_ts_ns: value.header.receive_ts, account_sequence: value.header.account_version };
            upsert_balance(inner, view)?;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Balance, 1, |ctx| { ctx.balance_events_ptr = &view; ctx.balance_events_len = 1; })
        }
        (titan_account_service::STREAM_STATE_CHANGED_EVENT, 1) | (titan_account_service::STREAM_INVALIDATED_EVENT, 1) => {
            let (header, state, reason) = if event.event_type == titan_account_service::STREAM_STATE_CHANGED_EVENT {
                let value = StreamStateChangedV1::decode(event.payload).map_err(|_| v13_runtime_error("account_state_decode"))?;
                (value.0.header, value.0.state, value.0.reason_code)
            } else {
                let value = StreamInvalidatedV1::decode(event.payload).map_err(|_| v13_runtime_error("account_state_decode"))?;
                (value.0.header, value.0.state, value.0.reason_code)
            };
            let account_no = local_account(core, header.account_id)?;
            let view = TitanAccountStateEventView { account_no, account_epoch: header.account_epoch,
                event_ts_ns: header.receive_ts, account_sequence: header.account_version,
                state, reason: reason as u8, ..TitanAccountStateEventView::default() };
            upsert_account(inner, view);
            let result = NativeV13Runtime::invoke(inner, core, V13EventKind::AccountState, 1, |ctx| { ctx.account_state_events_ptr = &view; ctx.account_state_events_len = 1; });
            if state != 4 { core.context.activation.close(); }
            result
        }
        _ => Err(v13_runtime_error("unsupported_v13_event")),
    }
}

fn decode_market_batch(core: &NativeV13Core, inner: &mut NativeV13Inner, event: EventView<'_>, depth: bool) -> LocalResult<()> {
    if event.payload.len() < titan_market_service::MarketBatchHeaderV1::ENCODED_LEN { return Err(v13_runtime_error("market_header")); }
    let asset_id = u32::from_le_bytes(event.payload[0..4].try_into().unwrap());
    let count = usize::from(u16::from_le_bytes(event.payload[8..10].try_into().unwrap()));
    if count > PUBLIC_CAPACITY { return Err(v13_runtime_error("market_capacity")); }
    let asset_no = core.context.markets.iter().find(|item| item.asset_id == asset_id && item.source.market_stream_id().is_some_and(|id| id.0 == event.metadata.source_id))
        .map(|item| item.local_asset_no).ok_or_else(|| v13_runtime_error("market_not_bound"))?;
    let expected = titan_market_service::MarketBatchHeaderV1::ENCODED_LEN + count * titan_market_service::DepthItemV1::ENCODED_LEN;
    if event.payload.len() != expected { return Err(v13_runtime_error("market_length")); }
    let exchange_ts = i64::from_le_bytes(event.payload[36..44].try_into().unwrap());
    let receive_ts = i64::from_le_bytes(event.payload[44..52].try_into().unwrap());
    let sequence = u64::from_le_bytes(event.payload[28..36].try_into().unwrap());
    inner.ticks.clear(); inner.depth.clear();
    for index in 0..count {
        let offset = 52 + index * 24;
        let price = i64::from_le_bytes(event.payload[offset..offset + 8].try_into().unwrap());
        let qty = i64::from_le_bytes(event.payload[offset + 8..offset + 16].try_into().unwrap());
        let side = event.payload[offset + 16]; let action = event.payload[offset + 17];
        inner.ticks.push(TitanTickView { asset_no, kind: if depth { 2 } else { 1 }, side,
            exchange_ts_ns: exchange_ts, receive_ts_ns: receive_ts, price_ticks: price,
            qty_lots: qty, source_sequence: sequence, ..TitanTickView::default() });
        if depth { inner.depth.push(TitanDepthView { asset_no, level: index as u32,
            exchange_ts_ns: exchange_ts, receive_ts_ns: receive_ts, price_ticks: price,
            qty_lots: qty, source_sequence: sequence, side, action, is_snapshot: 0, reserved: [0; 5] }); }
        let market = &mut inner.markets[asset_no as usize];
        market.source_sequence = sequence;
        if side == 1 { market.best_bid_ticks = price; market.best_bid_qty_lots = qty; }
        else if side == 2 { market.best_ask_ticks = price; market.best_ask_qty_lots = qty; }
    }
    Ok(())
}

fn commit_commands(core: &NativeV13Core, inner: &mut NativeV13Inner, commands: &[StagedCommandV13]) -> LocalResult<()> {
    for command in commands {
        match *command {
            StagedCommandV13::Submit { order_id, request } => {
                let binding = execution_binding(core, request.account_no, request.asset_no)?;
                let side = match request.side { 1 => 1, 2 => -1, _ => return Err(v13_runtime_error("invalid_side")) };
                let order_type = match request.order_type { 1 => 0, 2 => 1, _ => return Err(v13_runtime_error("unsupported_order_type")) };
                let time_in_force = match request.time_in_force { 1 => 0, 2 => 1, 3 => 2, 4 => 3, _ => return Err(v13_runtime_error("unsupported_tif")) };
                binding.handle.submit(DirectNewOrderRequest { asset_id: AssetId(*binding.assets.get(&u64::from(request.asset_no)).unwrap()),
                    side, order_type, time_in_force, price_ticks: request.price_ticks,
                    quantity_lots: request.qty_lots, client_order_id: client_order_id(core.context.strategy, order_id) })
                    .map_err(|_| v13_runtime_error("submit_dispatch_failed"))?;
                if inner.active_orders.len() >= PUBLIC_CAPACITY { return Err(v13_runtime_error("active_order_capacity")); }
                inner.active_orders.push(TitanActiveOrderView { order_id, asset_no: request.asset_no,
                    account_no: request.account_no, price_ticks: request.price_ticks, qty_lots: request.qty_lots,
                    created_ts_ns: core.context.clock.now_ns(), updated_ts_ns: core.context.clock.now_ns(),
                    side: request.side, order_type: request.order_type, time_in_force: request.time_in_force,
                    status: 1, reduce_only: request.reduce_only, ..TitanActiveOrderView::default() });
            }
            StagedCommandV13::Cancel { request, .. } => {
                let binding = execution_binding(core, request.account_no, request.asset_no)?;
                binding.handle.cancel(DirectCancelOrderRequest { asset_id: AssetId(*binding.assets.get(&u64::from(request.asset_no)).unwrap()),
                    client_order_id: Some(client_order_id(core.context.strategy, request.order_id)), venue_order_id: None })
                    .map_err(|_| v13_runtime_error("cancel_dispatch_failed"))?;
                if let Some(order) = inner.active_orders.iter_mut().find(|item| item.order_id == request.order_id) { order.status = 5; }
            }
        }
        inner.command_count = inner.command_count.saturating_add(1);
    }
    Ok(())
}

fn execution_binding(core: &NativeV13Core, account_no: u32, asset_no: u32) -> LocalResult<&StrategyExecutionBinding> {
    let binding = core.context.execution.iter().find(|item| item.local_account_no == account_no)
        .ok_or_else(|| v13_runtime_error("account_not_bound"))?;
    if !binding.assets.contains_key(&u64::from(asset_no)) { return Err(v13_runtime_error("asset_not_bound")); }
    Ok(binding)
}

fn local_account(core: &NativeV13Core, account_id: u32) -> LocalResult<u32> {
    core.context.accounts.iter().find(|item| item.account.account_id.0 == account_id)
        .map(|item| item.local_account_no).ok_or_else(|| v13_runtime_error("account_not_bound"))
}
fn account_binding(core: &NativeV13Core, account_id: u32, asset_id: u32) -> LocalResult<(u32, u32)> {
    let account = core.context.accounts.iter().find(|item| item.account.account_id.0 == account_id)
        .ok_or_else(|| v13_runtime_error("account_not_bound"))?;
    let asset = account.tradable_assets.iter().find(|item| item.asset_id == asset_id)
        .ok_or_else(|| v13_runtime_error("asset_not_bound"))?;
    Ok((account.local_account_no, asset.local_asset_no))
}

fn client_order_prefix(strategy: StrategyHandle) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new(); digest.update(b"titan.strategy.direct-order.v13");
    digest.update(strategy.strategy_id.0.to_le_bytes()); digest.update(strategy.generation.to_le_bytes());
    digest.finalize()[..8].try_into().unwrap()
}
fn client_order_id(strategy: StrategyHandle, order_id: u64) -> titan_account_service::Id128 {
    let mut value = [0; 16]; value[..8].copy_from_slice(&client_order_prefix(strategy));
    value[8..].copy_from_slice(&order_id.to_le_bytes()); titan_account_service::Id128(value)
}
pub(crate) fn v13_strategy_order_id(strategy: StrategyHandle, value: titan_account_service::Id128) -> u64 {
    if value.0[..8] == client_order_prefix(strategy) { u64::from_le_bytes(value.0[8..].try_into().unwrap()) } else { 0 }
}

fn update_active_fill(inner: &mut NativeV13Inner, fill: &TitanFillView) {
    if let Some(order) = inner.active_orders.iter_mut().find(|item| item.order_id == fill.order_id) {
        order.cumulative_filled_lots = fill.cumulative_filled_lots; order.updated_ts_ns = fill.receive_ts_ns;
        order.account_sequence = fill.account_sequence;
        if fill.final_fill != 0 || order.cumulative_filled_lots >= order.qty_lots { order.status = 4; }
    }
}
fn update_active_order(inner: &mut NativeV13Inner, value: &TitanOrderEventView, side: u8, order_type: u8, tif: u8) -> LocalResult<()> {
    if let Some(order) = inner.active_orders.iter_mut().find(|item| item.order_id == value.order_id) {
        order.cumulative_filled_lots = value.cumulative_filled_lots; order.updated_ts_ns = value.event_ts_ns;
        order.account_sequence = value.account_sequence; order.status = value.status;
    } else if value.order_id != 0 && value.status < 4 {
        if inner.active_orders.len() >= PUBLIC_CAPACITY { return Err(v13_runtime_error("active_order_capacity")); }
        inner.active_orders.push(TitanActiveOrderView { order_id: value.order_id, asset_no: value.asset_no,
            account_no: value.account_no, price_ticks: value.price_ticks, qty_lots: value.qty_lots,
            cumulative_filled_lots: value.cumulative_filled_lots, created_ts_ns: value.event_ts_ns,
            updated_ts_ns: value.event_ts_ns, account_sequence: value.account_sequence, side,
            order_type, time_in_force: tif, status: value.status, ..TitanActiveOrderView::default() });
    }
    if matches!(value.status, 4 | 6 | 7 | 8) { inner.active_orders.retain(|item| item.order_id != value.order_id); }
    Ok(())
}
fn upsert_position(inner: &mut NativeV13Inner, event: TitanPositionEventView) -> LocalResult<()> {
    let value = TitanPositionView { asset_no: event.asset_no, account_no: event.account_no,
        qty_lots: event.qty_lots, average_price_ticks: event.average_price_ticks,
        realized_pnl_ticks: event.realized_pnl_ticks, account_sequence: event.account_sequence };
    if let Some(current) = inner.positions.iter_mut().find(|item| item.account_no == value.account_no && item.asset_no == value.asset_no) { *current = value; }
    else {
        if inner.positions.len() >= PUBLIC_CAPACITY { return Err(v13_runtime_error("position_capacity")); }
        inner.positions.push(value);
    }
    Ok(())
}
fn upsert_balance(inner: &mut NativeV13Inner, event: TitanBalanceEventView) -> LocalResult<()> {
    let value = TitanBalanceView { account_no: event.account_no, currency_no: event.currency_no,
        total_units: event.total_units, available_units: event.available_units, account_sequence: event.account_sequence };
    if let Some(current) = inner.balances.iter_mut().find(|item| item.account_no == value.account_no && item.currency_no == value.currency_no) { *current = value; }
    else {
        if inner.balances.len() >= PUBLIC_CAPACITY { return Err(v13_runtime_error("balance_capacity")); }
        inner.balances.push(value);
    }
    Ok(())
}
fn upsert_account(inner: &mut NativeV13Inner, event: TitanAccountStateEventView) {
    if let Some(value) = inner.accounts.iter_mut().find(|item| item.account_no == event.account_no) {
        value.account_epoch = event.account_epoch; value.account_sequence = event.account_sequence;
        value.state = event.state; value.reason = event.reason;
    }
}

impl StrategyRuntime for NativeV13Runtime {
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()> { self.core.lane.set(lane).map_err(|_| v13_runtime_error("lane_already_attached")) }
    fn seed_public_state(&self, seed: StrategyPublicStateSeedV13) -> LocalResult<()> {
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.lifecycle != StrategyLifecycle::Defined { return Err(v13_runtime_error("seed_after_prepare")); }
        if seed.positions.len() > PUBLIC_CAPACITY || seed.balances.len() > PUBLIC_CAPACITY
            || seed.accounts.len() > PUBLIC_CAPACITY || seed.active_orders.len() > PUBLIC_CAPACITY {
            return Err(v13_runtime_error("public_state_capacity"));
        }
        inner.positions = seed.positions.to_vec();
        inner.balances = seed.balances.to_vec();
        inner.accounts = seed.accounts.to_vec();
        inner.active_orders = seed.active_orders.to_vec();
        Ok(())
    }
    fn fire_timer(&self, timer_id: u64) -> LocalResult<()> {
        let lane = self.core.lane.get().ok_or_else(|| v13_runtime_error("lane_not_attached"))?; let core = self.core.clone();
        lane.submit_safe_point(move || { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Running { return Ok(()); }
            let timer = TitanTimerView { timer_id, scheduled_ts_ns: core.context.clock.now_ns(), fired_ts_ns: core.context.clock.now_ns() };
            NativeV13Runtime::invoke(&mut inner, &core, V13EventKind::Timer, 1, |ctx| { ctx.timer_ptr = &timer; ctx.timer_len = 1; }).map_err(|_| EngineError::SafePointPanicked) })
            .map(|_| ()).map_err(|_| v13_runtime_error("timer_queue_full"))
    }
    fn prepare(&self) -> LocalResult<StrategyOperationId> { self.schedule(|core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); if inner.lifecycle != StrategyLifecycle::Defined { return Err(v13_runtime_error("invalid_prepare_state")); } inner.lifecycle = StrategyLifecycle::Ready; Ok(()) }) }
    fn start(&self) -> LocalResult<StrategyOperationId> { self.schedule(|core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); if inner.lifecycle != StrategyLifecycle::Ready { return Err(v13_runtime_error("invalid_start_state")); }
        NativeV13Runtime::invoke(&mut inner, core, V13EventKind::Start, 1, |_| {})?; inner.instance.start().map_err(|_| v13_runtime_error("v13_start_failed"))?; core.context.activation.open(); inner.lifecycle = StrategyLifecycle::Running; Ok(()) }) }
    fn pause(&self, _reason: PauseReason) -> LocalResult<StrategyOperationId> { self.core.context.activation.close(); self.schedule(|core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); if inner.lifecycle != StrategyLifecycle::Running { return Err(v13_runtime_error("invalid_pause_state")); } inner.lifecycle = StrategyLifecycle::Paused; Ok(()) }) }
    fn resume(&self) -> LocalResult<StrategyOperationId> { self.schedule(|core| { if core.lane.get().is_some_and(|lane| lane.health().state != SubscriberState::Normal) { return Err(v13_runtime_error("subscriber_not_normal")); } let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); if inner.lifecycle != StrategyLifecycle::Paused { return Err(v13_runtime_error("invalid_resume_state")); } core.context.activation.open(); inner.lifecycle = StrategyLifecycle::Running; Ok(()) }) }
    fn invalidate(&self, reason: Arc<str>) -> LocalResult<StrategyOperationId> { self.core.context.activation.close(); self.schedule(move |core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); inner.lifecycle = StrategyLifecycle::Invalidated; inner.last_error = Some(reason); Ok(()) }) }
    fn stop(&self, _deadline: Instant) -> LocalResult<StrategyOperationId> { self.schedule(|core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); inner.lifecycle = StrategyLifecycle::Stopping; if !inner.stop_called { NativeV13Runtime::invoke(&mut inner, core, V13EventKind::Stop, 1, |_| {})?; inner.stop_called = true; } core.context.activation.close(); inner.instance.stop(); inner.lifecycle = StrategyLifecycle::Stopped; Ok(()) }) }
    fn freeze_state(&self, request: StrategyStateSnapshotRequest) -> LocalResult<StrategyOperationId> { self.schedule(move |core| { let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner()); let identity = public_state_identity_v13(&inner.accounts, &inner.active_orders, &inner.positions, &inner.balances); inner.instance.update_checkpoint_boundary(core.lane.get().map_or(0, |lane| lane.progress().committed_sequence), identity); let snapshot = inner.instance.freeze_state(request.checkpoint_id).map_err(|_| v13_runtime_error("snapshot_failed"))?; core.context.state_snapshot_sink.submit(StrategyPrivateStateSnapshot { checkpoint_id: snapshot.checkpoint_id, strategy: core.context.strategy, generation: snapshot.generation, event_committed_sequence: snapshot.event_committed_sequence, artifact_digest: snapshot.artifact_digest, binding_digest: snapshot.binding_digest, abi_version: snapshot.abi_version, state_schema_version: snapshot.state_schema_version, state_schema_hash: snapshot.state_schema_hash, state_alignment: snapshot.state_alignment, state_bytes: snapshot.state_bytes, public_state_identity: snapshot.public_state_identity, checksum: snapshot.checksum }) }) }
    fn state(&self) -> StrategyRuntimeStateSnapshot { let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner()); StrategyRuntimeStateSnapshot { handle: self.core.context.strategy, lifecycle: inner.lifecycle, command_gate_open: inner.instance.command_gate_open(), activation_gate_open: self.core.context.activation.is_open() } }
    fn health(&self) -> StrategyRuntimeHealthSnapshot { let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner()); StrategyRuntimeHealthSnapshot { lifecycle: inner.lifecycle, healthy: !matches!(inner.lifecycle, StrategyLifecycle::Failed | StrategyLifecycle::Invalidated), degraded_reason: inner.last_error.clone(), heartbeat_at: SystemTime::now() } }
    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot { let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner()); StrategyRuntimeDiagnosticSnapshot { summary: Arc::from("native V13 strategy runtime"), callback_count: inner.callback_count, command_count: inner.command_count, last_error_code: inner.last_error.clone(), lane_progress: self.core.lane.get().map_or(LaneProgress::default(), |lane| lane.progress()), flight_records: inner.flight_records.iter().cloned().collect::<Vec<_>>().into() } }
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot { self.core.operations.lock().unwrap_or_else(|p| p.into_inner()).get(&id).cloned().unwrap_or(StrategyOperationSnapshot { id, strategy: Some(self.core.context.strategy), state: StrategyOperationState::Failed, detail: Arc::from("unknown_operation") }) }
}

fn v13_runtime_error(code: &'static str) -> StrategyError { StrategyError::new(StrategyErrorKind::InvalidState, "runtime_v13", code, "V13 runtime operation failed") }
fn v13_core_error() -> CoreError { CoreError::new(titan_core_types::ErrorKind::ComponentFailed, titan_core_types::ComponentIdentity::new("titan.strategy", "runtime-v13"), titan_core_types::ComponentState::Running, "v13_callback", "V13 strategy callback failed") }
