use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Instant, SystemTime},
};

use titan_account_service::{
    AssetId, BalanceChangedV1, DirectCancelOrderRequest, DirectNewOrderRequest,
    ExecutionRequestKind, FillV2, ObservedExecutionOutcome, ObservedExecutionResult,
    OrderChangedV1, PositionChangedV1, StreamInvalidatedV1, StreamStateChangedV1,
};
use titan_core_types::{CoreError, EventHandler, EventView};
use titan_event_engine::{EngineError, LaneProgress, PrimaryAsyncLaneHandle, SubscriberState};

use crate::depth_book::{DEPTH_BATCH_CAPACITY, HostDepthBook};
use crate::*;

const PUBLIC_CAPACITY: usize = 1_024;

pub struct NativeV13RuntimeFactory;

impl StrategyRuntimeFactory for NativeV13RuntimeFactory {
    fn runtime_type(&self) -> &str {
        "native-v13"
    }

    fn create(
        &self,
        definition: &StrategyDefinition,
        compiled_strategy: LoadedStrategy,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError> {
        let compiled_strategy = compiled_strategy.native;
        let routing_keys = context
            .markets
            .iter()
            .map(|item| u64::from(item.asset_id))
            .chain(
                context
                    .accounts
                    .iter()
                    .map(|item| u64::from(item.account.account_id.0)),
            )
            .collect();
        let config = StrategyInstanceConfigV13 {
            strategy_instance_id: u64::from(definition.runtime_strategy_no.0),
            markets: context
                .markets
                .iter()
                .map(|item| (item.local_asset_no, item.asset_id))
                .collect(),
            accounts: context
                .accounts
                .iter()
                .map(|item| (item.local_account_no, item.account.account_id.0))
                .collect(),
            routing_keys,
            max_commands_per_callback: definition.runtime.max_commands_per_callback,
            max_handler_duration: definition.runtime.max_handler_duration,
        };
        let generation = context.strategy.generation;
        let mut instance = compiled_strategy
            .instantiate_with_config(&config, generation)
            .map_err(|_| v13_runtime_error("v13_instantiate_failed"))?;
        instance.bind_resolved_routes(&context.markets, &context.accounts);
        let mut markets = vec![
            TitanMarketView::default();
            context
                .markets
                .iter()
                .map(|item| item.local_asset_no as usize + 1)
                .max()
                .unwrap_or(0)
        ];
        for binding in context.markets.iter() {
            let market = &mut markets[binding.local_asset_no as usize];
            market.asset_no = binding.local_asset_no;
            market.tick_size = 1;
            market.lot_size = 1;
        }
        let accounts = context
            .accounts
            .iter()
            .map(|binding| TitanAccountView {
                account_no: binding.local_account_no,
                state: 0,
                ..TitanAccountView::default()
            })
            .collect();
        Ok(Arc::new(NativeV13Runtime {
            core: Arc::new(NativeV13Core {
                execution_limits: definition.runtime.execution_limits.clone(),
                host_depth: compiled_strategy
                    .manifest
                    .capabilities
                    .contains(StrategyCapabilitiesV13::HOST_DEPTH_100),
                pause_safe: compiled_strategy
                    .manifest
                    .capabilities
                    .contains(StrategyCapabilitiesV13::PAUSE_SAFE),
                business_observation: compiled_strategy
                    .manifest
                    .business_observation
                    .as_ref()
                    .map(|plan| {
                        BusinessObservationPublisher::new(
                            plan.clone(),
                            context.strategy,
                            compiled_strategy.manifest.state,
                        )
                    })
                    .transpose()
                    .map_err(|_| v13_runtime_error("business_observation_identity_mismatch"))?,
                state_identity: compiled_strategy.manifest.state,
                context,
                inner: Mutex::new(NativeV13Inner {
                    lifecycle: StrategyLifecycle::Defined,
                    instance,
                    staging: CallbackCommandStagingV13::new(
                        config.max_commands_per_callback,
                        config.strategy_instance_id,
                        generation,
                    )
                    .map_err(|_| v13_runtime_error("v13_staging_failed"))?,
                    markets,
                    last_bbo_ns: 0,
                    positions: Vec::with_capacity(PUBLIC_CAPACITY),
                    balances: Vec::with_capacity(PUBLIC_CAPACITY),
                    accounts,
                    active_orders: Vec::with_capacity(PUBLIC_CAPACITY),
                    recovery_orders: Vec::new(),
                    recovery_executions: Vec::new(),
                    recovered_identity: None,
                    orders_list: BTreeMap::new(),
                    ticks: Vec::with_capacity(PUBLIC_CAPACITY),
                    depth: Vec::with_capacity(DEPTH_BATCH_CAPACITY),
                    depth_books: BTreeMap::new(),
                    depth_info: DepthBatchInfoV1::default(),
                    order_versions: BTreeMap::new(),
                    pending_fills: BTreeMap::new(),
                    settled_fills: BTreeMap::new(),
                    awaiting_fills: BTreeMap::new(),
                    inventory: InventoryProof::default(),
                    checkpoint_dirty: false,
                    last_checkpoint_token: None,
                    last_checkpoint_ns: 0,
                    checkpoint_interval_ns: definition
                        .runtime
                        .checkpoint_interval
                        .as_nanos()
                        .min(i64::MAX as u128) as i64,
                    position_versions: BTreeMap::new(),
                    balance_versions: BTreeMap::new(),
                    account_versions: BTreeMap::new(),
                    callback_count: 0,
                    command_count: 0,
                    last_error: None,
                    stop_called: false,
                    stop_ready: false,
                    recovery_order_results: compiled_strategy
                        .manifest
                        .capabilities
                        .contains(StrategyCapabilitiesV13::RECOVERY_ORDER_RESULTS),
                    stop_barrier: compiled_strategy
                        .manifest
                        .capabilities
                        .contains(StrategyCapabilitiesV13::STOP_BARRIER),
                    stop_operation: None,
                    stop_deadline: None,
                    flight_records: VecDeque::with_capacity(128),
                }),
                lane: OnceLock::new(),
                stop_request: Mutex::new(None),
                next_operation: AtomicU64::new(1),
                async_failure: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                operations: Mutex::new(titan_core_types::OperationHistory::new(|value| {
                    value.state == StrategyOperationState::Pending
                })),
            }),
        }))
    }
}

pub struct NativeV13Runtime {
    core: Arc<NativeV13Core>,
}

struct NativeV13Core {
    host_depth: bool,
    pause_safe: bool,
    execution_limits: Option<ExecutionLimits>,
    business_observation: Option<BusinessObservationPublisher>,
    state_identity: RecoveryStateIdentityV13,
    context: StrategyRuntimeBuildContext,
    inner: Mutex<NativeV13Inner>,
    lane: OnceLock<PrimaryAsyncLaneHandle>,
    stop_request: Mutex<Option<StrategyOperationId>>,
    next_operation: AtomicU64,
    async_failure: AtomicBool,
    // REST completions can arrive after the lane has shut down. A successfully
    // stopped run no longer needs them to close any execution boundary.
    stopped: AtomicBool,
    operations:
        Mutex<titan_core_types::OperationHistory<StrategyOperationId, StrategyOperationSnapshot>>,
}

struct NativeV13Inner {
    last_bbo_ns: i64,
    lifecycle: StrategyLifecycle,
    instance: StrategyInstanceV13,
    staging: CallbackCommandStagingV13,
    markets: Vec<TitanMarketView>,
    positions: Vec<TitanPositionView>,
    balances: Vec<TitanBalanceView>,
    accounts: Vec<TitanAccountView>,
    active_orders: Vec<TitanActiveOrderView>,
    recovery_orders: Vec<TitanActiveOrderView>,
    recovery_executions: Vec<RecoveryExecutionHistory>,
    recovered_identity: Option<[u8; 32]>,
    orders_list: BTreeMap<u64, StrategyOrderRecordV13>,
    ticks: Vec<TitanTickView>,
    depth: Vec<TitanDepthView>,
    depth_books: BTreeMap<u32, HostDepthBook>,
    depth_info: DepthBatchInfoV1,
    order_versions: BTreeMap<(u32, u64), u64>,
    pending_fills: BTreeMap<(u32, u64), TitanFillView>,
    settled_fills: BTreeMap<(u32, u64), i64>,
    awaiting_fills: BTreeMap<(u32, u64), i64>,
    inventory: InventoryProof,
    checkpoint_dirty: bool,
    last_checkpoint_token: Option<RecoverySubmitToken>,
    last_checkpoint_ns: i64,
    checkpoint_interval_ns: i64,
    position_versions: BTreeMap<(u32, u32, u8), u64>,
    balance_versions: BTreeMap<(u32, u32), u64>,
    account_versions: BTreeMap<u32, u64>,
    callback_count: u64,
    command_count: u64,
    last_error: Option<Arc<str>>,
    stop_called: bool,
    stop_ready: bool,
    recovery_order_results: bool,
    stop_barrier: bool,
    stop_operation: Option<StrategyOperationId>,
    stop_deadline: Option<Instant>,
    flight_records: VecDeque<StrategyFlightRecord>,
}

pub(crate) fn orders_list_checksum(orders: &[StrategyOrderRecordV13]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.orders-list.v13");
    digest.update((orders.len() as u64).to_le_bytes());
    for order in orders {
        digest.update(order.order_id.to_le_bytes());
        digest.update(order.asset_no.to_le_bytes());
        digest.update(order.account_no.to_le_bytes());
        digest.update(order.price_ticks.to_le_bytes());
        digest.update(order.qty_lots.to_le_bytes());
        digest.update(order.cumulative_filled_lots.to_le_bytes());
        digest.update(order.last_fill_price_ticks.to_le_bytes());
        digest.update(order.submit_ts_ns.to_le_bytes());
        digest.update(order.last_fill_ts_ns.to_le_bytes());
        digest.update(order.last_event_ts_ns.to_le_bytes());
        digest.update(order.account_sequence.to_le_bytes());
        digest.update(order.fill_count.to_le_bytes());
        digest.update([
            order.side,
            order.order_type,
            order.time_in_force,
            order.status,
            order.reduce_only,
            order.error_code,
            order.position_side,
        ]);
        digest.update(order.reserved);
    }
    digest.finalize().into()
}

fn merge_seeded_order_record(
    orders: &mut BTreeMap<u64, StrategyOrderRecordV13>,
    value: TitanActiveOrderView,
) -> LocalResult<()> {
    let record = orders.entry(value.order_id).or_default();
    if record.order_id != 0
        && (record.account_no != value.account_no || record.asset_no != value.asset_no)
    {
        return Err(v13_runtime_error("orders_list_identity_mismatch"));
    }
    if record.order_id != 0 && record.account_sequence > value.account_sequence {
        return Ok(());
    }
    record.order_id = value.order_id;
    record.asset_no = value.asset_no;
    record.account_no = value.account_no;
    record.price_ticks = value.price_ticks;
    record.qty_lots = value.qty_lots;
    record.cumulative_filled_lots = value.cumulative_filled_lots;
    if record.submit_ts_ns == 0 {
        record.submit_ts_ns = value.created_ts_ns;
    }
    record.last_event_ts_ns = value.updated_ts_ns;
    record.account_sequence = value.account_sequence;
    record.side = value.side;
    record.order_type = value.order_type;
    record.time_in_force = value.time_in_force;
    record.status = value.status;
    record.reduce_only = value.reduce_only;
    record.position_side = value.position_side;
    Ok(())
}

impl NativeV13Runtime {
    fn schedule(
        &self,
        action: impl FnOnce(&Arc<NativeV13Core>) -> LocalResult<()> + Send + 'static,
    ) -> LocalResult<StrategyOperationId> {
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let id = StrategyOperationId(self.core.next_operation.fetch_add(1, Ordering::AcqRel));
        self.core
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                StrategyOperationSnapshot {
                    id,
                    strategy: Some(self.core.context.strategy),
                    state: StrategyOperationState::Pending,
                    detail: Arc::from("pending"),
                },
            );
        let core = self.core.clone();
        let admission = lane.submit_safe_point(move || {
            let result = action(&core);
            let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
            let snapshot = operations.get_mut(&id).expect("operation exists");
            match result {
                Ok(()) => {
                    snapshot.state = StrategyOperationState::Succeeded;
                    snapshot.detail = Arc::from("succeeded");
                    Ok(())
                }
                Err(error) => {
                    snapshot.state = StrategyOperationState::Failed;
                    snapshot.detail = error.reason_code;
                    Ok(())
                }
            }
        });
        if admission.is_err() {
            let mut operations = self
                .core
                .operations
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let snapshot = operations.get_mut(&id).expect("operation exists");
            snapshot.state = StrategyOperationState::Failed;
            snapshot.detail = Arc::from("control_queue_full");
            return Err(v13_runtime_error("control_queue_full"));
        }
        Ok(id)
    }

    fn invoke(
        inner: &mut NativeV13Inner,
        core: &NativeV13Core,
        kind: V13EventKind,
        schema_version: u32,
        configure: impl FnOnce(&mut StrategyRuntimeContextV13),
    ) -> LocalResult<()> {
        let mut context = crate::v13::callback_context_v13(
            core.context.clock.now_ns(),
            schema_version,
            &inner.markets,
            &inner.positions,
            &inner.balances,
            &inner.accounts,
            &inner.active_orders,
        );
        if core.pause_safe && inner.lifecycle == StrategyLifecycle::Paused {
            context.flags |= V13_PAUSED;
        }
        configure(&mut context);
        if kind == V13EventKind::Recover && inner.recovery_order_results {
            context.event_payload_ptr = inner.recovery_orders.as_ptr().cast();
            context.event_payload_len =
                std::mem::size_of_val(inner.recovery_orders.as_slice()) as u64;
        }
        let commands = match crate::v13::invoke_staged_v13(
            &mut inner.instance,
            &mut inner.staging,
            &mut context,
            kind,
            core.context.activation.is_open(),
            &inner.active_orders,
        ) {
            Ok(commands) => commands,
            Err(_) => {
                core.context.activation.close();
                fail_runtime(core, inner, Arc::from("v13_callback_failed"));
                return Err(v13_runtime_error("v13_callback_failed"));
            }
        };
        if kind == V13EventKind::Recover && inner.staging.attempted_command() {
            fail_runtime(
                core,
                inner,
                Arc::from("recovery_callback_attempted_command"),
            );
            return Err(v13_runtime_error("recovery_callback_attempted_command"));
        }
        if core.host_depth {
            let ready = !core.context.markets.is_empty()
                && core.context.markets.iter().all(|b| {
                    inner
                        .depth_books
                        .get(&b.local_asset_no)
                        .is_some_and(|book| book.ready)
                        && core.context.market_service.as_ref().is_none_or(|service| {
                            service.health(b.source).is_ok_and(|h| {
                                h.state == titan_market_service::ConnectorHealth::Running
                            })
                        })
                });
            if let Err(reason) = crate::depth_execution::validate_depth_commands(
                ready,
                &commands,
                &inner.positions,
                &inner.active_orders,
            ) {
                fail_runtime(core, inner, Arc::from(reason));
                return Err(v13_runtime_error(reason));
            }
        }
        let committed = commit_commands(core, inner, &commands).and_then(|_| {
            if core.async_failure.load(Ordering::Acquire) {
                Err(v13_runtime_error("execution_result_queue_full"))
            } else {
                Ok(())
            }
        });
        if let Err(error) = committed {
            fail_runtime(core, inner, error.reason_code.clone());
            return Err(error);
        }
        if inner.lifecycle == StrategyLifecycle::Stopping && context.flags & V13_STOP_READY != 0 {
            inner.stop_ready = true;
        }
        inner.staging.clear_committed();
        inner.callback_count = inner.callback_count.saturating_add(1);
        inner.checkpoint_dirty = true;
        prune_terminal_history(inner);
        Ok(())
    }
}

impl EventHandler for NativeV13Runtime {
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError> {
        let timing = tracing::enabled!(tracing::Level::DEBUG).then(Instant::now);
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if self.core.async_failure.load(Ordering::Acquire) {
            fail_runtime(
                &self.core,
                &mut inner,
                Arc::from("execution_result_queue_full"),
            );
            return Err(v13_core_error());
        }
        if matches!(
            inner.lifecycle,
            StrategyLifecycle::Stopped | StrategyLifecycle::Failed | StrategyLifecycle::Invalidated
        ) {
            return Ok(());
        }
        if self
            .core
            .lane
            .get()
            .is_some_and(|lane| lane.health().state != SubscriberState::Normal)
        {
            return Ok(());
        }
        if self
            .core
            .context
            .state_snapshot_sink
            .check_health()
            .is_err()
        {
            fail_runtime(
                &self.core,
                &mut inner,
                Arc::from("checkpoint_writer_failed"),
            );
            return Err(v13_core_error());
        }
        let invoke_callbacks = matches!(
            inner.lifecycle,
            StrategyLifecycle::Running | StrategyLifecycle::Paused | StrategyLifecycle::Stopping
        );
        let result = dispatch_event(&self.core, &mut inner, event, invoke_callbacks)
            .and_then(|_| finish_stop_if_drained(&self.core, &mut inner));
        prune_terminal_history(&mut inner);
        if let Some(started) = timing {
            tracing::debug!(
                event_type = event.event_type,
                elapsed_us = started.elapsed().as_micros() as u64,
                "strategy event dispatch duration"
            );
        }
        if let Err(error) = result {
            fail_runtime(&self.core, &mut inner, error.reason_code);
            return Err(v13_core_error());
        }
        publish_business_observation(&self.core, &inner, false);
        Ok(())
    }
}

fn dispatch_event(
    core: &NativeV13Core,
    inner: &mut NativeV13Inner,
    event: EventView<'_>,
    invoke_callbacks: bool,
) -> LocalResult<()> {
    if titan_account_service::ACCOUNT_EVENT_TYPES.contains(&event.event_type) {
        let header = titan_account_service::decode_account_event_header(event.payload)
            .map_err(|_| v13_runtime_error("account_header_decode"))?;
        let binding = core
            .context
            .accounts
            .iter()
            .find(|binding| binding.account.account_id.0 == header.account_id)
            .ok_or_else(|| v13_runtime_error("account_not_bound"))?;
        if header.account_generation < binding.account.generation {
            return Ok(());
        }
        if header.account_generation != binding.account.generation {
            return Err(v13_runtime_error("account_generation_requires_recovery"));
        }
        let account_no = local_account(core, header.account_id)?;
        if let Some(current) = inner
            .accounts
            .iter()
            .find(|value| value.account_no == account_no)
            && current.account_epoch != 0
            && current.account_epoch != header.account_epoch
        {
            if invoke_callbacks {
                return Err(v13_runtime_error("account_epoch_requires_recovery"));
            }
            return Ok(());
        }
    }
    match (event.event_type, event.schema_version) {
        (titan_market_service::BBO_EVENT | titan_market_service::TRADE_BATCH_EVENT, 1) => {
            decode_market_batch(core, inner, event, false)?;
            if !invoke_callbacks {
                return Ok(());
            }
            let ptr = inner.ticks.as_ptr();
            let len = inner.ticks.len() as u64;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Tick, 1, |ctx| {
                ctx.ticks_ptr = ptr;
                ctx.ticks_len = len;
            })
        }
        (titan_market_service::DEPTH_BATCH_EVENT, 1) => {
            decode_market_batch(core, inner, event, true)?;
            if !invoke_callbacks {
                return Ok(());
            }
            if core.host_depth && inner.depth_info.version == 0 {
                return Ok(());
            }
            let ptr = inner.depth.as_ptr();
            let len = inner.depth.len() as u64;
            let info = inner.depth_info;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Depth, 1, |ctx| {
                ctx.depth_ptr = ptr;
                ctx.depth_len = len;
                if core.host_depth {
                    ctx.event_payload_ptr = (&info as *const DepthBatchInfoV1).cast();
                    ctx.event_payload_len = std::mem::size_of::<DepthBatchInfoV1>() as u64;
                }
            })
        }
        (titan_market_service::STREAM_INVALIDATED_EVENT, 1) if core.host_depth => {
            if event.payload.len() != 12 {
                return Err(v13_runtime_error("depth_invalidation_length"));
            }
            let asset_id = u32::from_le_bytes(event.payload[..4].try_into().unwrap());
            let epoch = u64::from_le_bytes(event.payload[4..12].try_into().unwrap());
            let Some(asset_no) = core
                .context
                .markets
                .iter()
                .find(|b| b.asset_id == asset_id && b.source_stream.0 == event.metadata.source_id)
                .map(|b| b.local_asset_no)
            else {
                return Ok(());
            };
            let book = inner.depth_books.entry(asset_no).or_default();
            // A publication/connection failure may not know the venue epoch.
            // Its ordered watermark still invalidates the current book.
            let epoch = if epoch == 0 { book.epoch() } else { epoch };
            if !book.invalidate_at(epoch, event.metadata.source_sequence) {
                return Ok(());
            }
            inner.depth.clear();
            let info = DepthBatchInfoV1 {
                version: 1,
                asset_no,
                stream_epoch: epoch,
                reset: 1,
                publication_sequence: event.metadata.source_sequence,
                ..Default::default()
            };
            inner.depth_info = info;
            if !invoke_callbacks {
                return Ok(());
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Depth, 1, |ctx| {
                ctx.event_payload_ptr = (&info as *const DepthBatchInfoV1).cast();
                ctx.event_payload_len = std::mem::size_of::<DepthBatchInfoV1>() as u64;
            })
        }
        (titan_account_service::FILL_EVENT, titan_account_service::FILL_EVENT_SCHEMA_VERSION) => {
            let fill =
                FillV2::decode(event.payload).map_err(|_| v13_runtime_error("fill_decode"))?;
            let (account_no, asset_no) =
                account_binding(core, fill.header.account_id, fill.asset_id)?;
            let order_id = v13_strategy_order_id(core.context.strategy, fill.client_order_id);
            if order_id == 0 {
                return Ok(());
            }
            let view = TitanFillView {
                order_id,
                asset_no,
                account_no,
                fill_price_ticks: fill.price_ticks,
                fill_qty_lots: fill.last_fill_quantity_lots,
                cumulative_filled_lots: fill.cumulative_filled_quantity_lots,
                exchange_ts_ns: fill.header.exchange_ts,
                receive_ts_ns: fill.header.receive_ts,
                account_sequence: fill.header.account_version,
                side: fill.side,
                liquidity: fill.liquidity,
                final_fill: u8::from(
                    fill.header.flags & titan_account_service::event_flags::FINAL != 0,
                ),
                reserved: [0; 5],
            };
            if !update_active_fill(inner, &view)? {
                // The account journal retains native replay provenance. Strategy inventory,
                // callbacks and consumed-fill counters advance only for a new cumulative fill.
                return Ok(());
            }
            if !invoke_callbacks {
                return Ok(());
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Fill, 2, |ctx| {
                ctx.fills_ptr = &view;
                ctx.fills_len = 1;
            })
        }
        (titan_account_service::ORDER_CHANGED_EVENT, 1) => {
            let value = OrderChangedV1::decode(event.payload)
                .map_err(|_| v13_runtime_error("order_decode"))?;
            let (account_no, asset_no) =
                account_binding(core, value.header.account_id, value.asset_id)?;
            let order_id = v13_strategy_order_id(core.context.strategy, value.client_order_id);
            if order_id == 0 {
                return Ok(());
            }
            let status = account_order_status_to_v13(value.status)?;
            let order_type = account_order_type_to_v13(value.order_type)?;
            let time_in_force = account_tif_to_v13(value.time_in_force)?;
            let reduce_only =
                u8::from(value.header.flags & titan_account_service::event_flags::REDUCE_ONLY != 0);
            let view = TitanOrderEventView {
                order_id,
                asset_no,
                account_no,
                price_ticks: value.price_ticks,
                qty_lots: value.quantity_lots,
                cumulative_filled_lots: value.filled_quantity_lots,
                event_ts_ns: value.header.receive_ts,
                account_sequence: value.header.account_version,
                status,
                reason: 0,
                position_side: {
                    let reported =
                        titan_account_service::event_flags::position_side(value.header.flags);
                    if reported == 0 {
                        inner
                            .orders_list
                            .get(&order_id)
                            .map_or(0, |v| v.position_side)
                    } else {
                        reported
                    }
                },
                reserved: [0; 5],
            };
            update_active_order(
                inner,
                &view,
                value.side,
                order_type,
                time_in_force,
                reduce_only,
            )?;
            if !invoke_callbacks {
                return Ok(());
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Order, 1, |ctx| {
                ctx.order_events_ptr = &view;
                ctx.order_events_len = 1;
            })?;
            if matches!(status, 6..=8) {
                let cancel = TitanCancelEventView {
                    order_id,
                    asset_no,
                    account_no,
                    event_ts_ns: value.header.receive_ts,
                    account_sequence: value.header.account_version,
                    request_result: 0,
                    final_status: status,
                    reserved: [0; 6],
                };
                NativeV13Runtime::invoke(inner, core, V13EventKind::Cancel, 1, |ctx| {
                    ctx.cancel_events_ptr = &cancel;
                    ctx.cancel_events_len = 1;
                })?;
            }
            Ok(())
        }
        (titan_account_service::POSITION_CHANGED_EVENT, 1) => {
            let value = PositionChangedV1::decode(event.payload)
                .map_err(|_| v13_runtime_error("position_decode"))?;
            let (account_no, asset_no) =
                account_binding(core, value.header.account_id, value.asset_id)?;
            let view = TitanPositionEventView {
                asset_no,
                account_no,
                position_side: value.position_side,
                reserved: [0; 7],
                qty_lots: value.quantity_lots,
                average_price_ticks: value.entry_price_ticks,
                realized_pnl_ticks: value.realized_pnl_units,
                event_ts_ns: value.header.receive_ts,
                account_sequence: value.header.account_version,
            };
            upsert_position(inner, view)?;
            if !invoke_callbacks {
                return Ok(());
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Position, 1, |ctx| {
                ctx.position_events_ptr = &view;
                ctx.position_events_len = 1;
            })
        }
        (titan_account_service::BALANCE_CHANGED_EVENT, 1) => {
            let value = BalanceChangedV1::decode(event.payload)
                .map_err(|_| v13_runtime_error("balance_decode"))?;
            let account_no = local_account(core, value.header.account_id)?;
            let view = TitanBalanceEventView {
                account_no,
                currency_no: value.currency_id,
                total_units: value.wallet_units,
                available_units: value.available_units,
                event_ts_ns: value.header.receive_ts,
                account_sequence: value.header.account_version,
            };
            upsert_balance(inner, view)?;
            if !invoke_callbacks {
                return Ok(());
            }
            NativeV13Runtime::invoke(inner, core, V13EventKind::Balance, 1, |ctx| {
                ctx.balance_events_ptr = &view;
                ctx.balance_events_len = 1;
            })
        }
        (titan_account_service::STREAM_STATE_CHANGED_EVENT, 1)
        | (titan_account_service::STREAM_INVALIDATED_EVENT, 1) => {
            let (header, state, reason) =
                if event.event_type == titan_account_service::STREAM_STATE_CHANGED_EVENT {
                    let value = StreamStateChangedV1::decode(event.payload)
                        .map_err(|_| v13_runtime_error("account_state_decode"))?;
                    (value.0.header, value.0.state, value.0.reason_code)
                } else {
                    let value = StreamInvalidatedV1::decode(event.payload)
                        .map_err(|_| v13_runtime_error("account_state_decode"))?;
                    (value.0.header, value.0.state, value.0.reason_code)
                };
            let account_no = local_account(core, header.account_id)?;
            let view = TitanAccountStateEventView {
                account_no,
                account_epoch: header.account_epoch,
                event_ts_ns: header.receive_ts,
                account_sequence: header.account_version,
                state,
                reason: reason as u8,
                ..TitanAccountStateEventView::default()
            };
            upsert_account(inner, view);
            if state != 4 {
                core.context.activation.close();
                if inner.lifecycle == StrategyLifecycle::Running {
                    inner.lifecycle = StrategyLifecycle::Paused;
                }
            }
            if !invoke_callbacks {
                return Ok(());
            }

            NativeV13Runtime::invoke(inner, core, V13EventKind::AccountState, 1, |ctx| {
                ctx.account_state_events_ptr = &view;
                ctx.account_state_events_len = 1;
            })
        }
        _ => Err(v13_runtime_error("unsupported_v13_event")),
    }
}

fn decode_market_batch(
    core: &NativeV13Core,
    inner: &mut NativeV13Inner,
    event: EventView<'_>,
    depth: bool,
) -> LocalResult<()> {
    if event.payload.len() < titan_market_service::MarketBatchHeaderV1::ENCODED_LEN {
        return Err(v13_runtime_error("market_header"));
    }
    let asset_id = u32::from_le_bytes(event.payload[0..4].try_into().unwrap());
    let count = usize::from(u16::from_le_bytes(event.payload[8..10].try_into().unwrap()));
    if count
        > if depth {
            DEPTH_BATCH_CAPACITY
        } else {
            PUBLIC_CAPACITY
        }
    {
        return Err(v13_runtime_error("market_capacity"));
    }
    let asset_no = core
        .context
        .markets
        .iter()
        .find(|item| item.asset_id == asset_id && item.source_stream.0 == event.metadata.source_id)
        .map(|item| item.local_asset_no)
        .ok_or_else(|| v13_runtime_error("market_not_bound"))?;
    let expected = titan_market_service::MarketBatchHeaderV1::ENCODED_LEN
        + count * titan_market_service::DepthItemV1::ENCODED_LEN;
    if event.payload.len() != expected {
        return Err(v13_runtime_error("market_length"));
    }
    let exchange_ts = i64::from_le_bytes(event.payload[36..44].try_into().unwrap());
    let receive_ts = i64::from_le_bytes(event.payload[44..52].try_into().unwrap());
    let sequence = u64::from_le_bytes(event.payload[28..36].try_into().unwrap());
    let snapshot = if depth {
        let kind = u16::from_le_bytes(event.payload[4..6].try_into().unwrap());
        let flags = u16::from_le_bytes(event.payload[6..8].try_into().unwrap());
        if !matches!((kind, flags), (1, 1) | (2, 0)) {
            return Err(v13_runtime_error("depth_header_flags"));
        }
        kind == 1
    } else {
        false
    };
    let is_bbo = event.event_type == titan_market_service::BBO_EVENT;
    if is_bbo {
        inner.last_bbo_ns = receive_ts;
    }
    inner.ticks.clear();
    inner.depth.clear();
    for index in 0..count {
        let offset = 52 + index * 24;
        let price = i64::from_le_bytes(event.payload[offset..offset + 8].try_into().unwrap());
        let qty = i64::from_le_bytes(event.payload[offset + 8..offset + 16].try_into().unwrap());
        let side = event.payload[offset + 16];
        let action = event.payload[offset + 17];
        inner.ticks.push(TitanTickView {
            asset_no,
            kind: if depth { 2 } else { 1 },
            side,
            exchange_ts_ns: exchange_ts,
            receive_ts_ns: receive_ts,
            price_ticks: price,
            qty_lots: qty,
            source_sequence: sequence,
            ..TitanTickView::default()
        });
        if depth {
            inner.depth.push(TitanDepthView {
                asset_no,
                level: index as u32,
                exchange_ts_ns: exchange_ts,
                receive_ts_ns: receive_ts,
                price_ticks: price,
                qty_lots: qty,
                source_sequence: sequence,
                side,
                action,
                is_snapshot: u8::from(snapshot),
                reserved: [0; 5],
            });
        }
        let market = &mut inner.markets[asset_no as usize];
        market.source_sequence = sequence;
        if is_bbo && side == 1 {
            market.best_bid_ticks = price;
            market.best_bid_qty_lots = qty;
        } else if is_bbo && side == 2 {
            market.best_ask_ticks = price;
            market.best_ask_qty_lots = qty;
        }
    }
    if depth && core.host_depth {
        let mut info = DepthBatchInfoV1 {
            version: 1,
            asset_no,
            stream_epoch: u64::from_le_bytes(event.payload[12..20].try_into().unwrap()),
            first_update_sequence: u64::from_le_bytes(event.payload[20..28].try_into().unwrap()),
            last_update_sequence: sequence,
            publication_sequence: event.metadata.source_sequence,
            ..Default::default()
        };
        let book = inner.depth_books.entry(asset_no).or_default();
        let mut output = Vec::with_capacity(200);
        if !book
            .apply(&mut info, snapshot, &inner.depth, &mut output)
            .map_err(v13_runtime_error)?
        {
            inner.depth_info = DepthBatchInfoV1::default();
            inner.depth.clear();
            return Ok(());
        }
        if snapshot && !book.ready && info.stream_epoch != 0 {
            return Err(v13_runtime_error("depth_snapshot_insufficient_coverage"));
        }
        if !book.ready && !book.snapshot_pending && !snapshot {
            let service = core
                .context
                .market_service
                .as_ref()
                .ok_or_else(|| v13_runtime_error("depth_snapshot_service_missing"))?;
            let binding = core
                .context
                .markets
                .iter()
                .find(|b| b.local_asset_no == asset_no)
                .unwrap();
            service
                .request_snapshot(binding.source, titan_market_service::AssetId(asset_id))
                .map_err(|_| v13_runtime_error("depth_snapshot_request_failed"))?;
            book.snapshot_pending = true;
        }
        for item in &mut output {
            item.exchange_ts_ns = exchange_ts;
            item.receive_ts_ns = receive_ts;
        }
        inner.depth = output;
        inner.depth_info = info;
    }
    Ok(())
}

fn validate_execution_envelope(
    limits: &ExecutionLimits,
    market: TitanMarketView,
    positions: &[TitanPositionView],
    orders: &[TitanActiveOrderView],
    last_bbo_ns: i64,
    commands: &[StagedCommandV13],
    now: i64,
) -> LocalResult<()> {
    let cap = limits
        .max_notional_tick_lots
        .parse::<i128>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| v13_runtime_error("invalid_execution_limit"))?;
    if limits.max_market_age_ns <= 0 || !(1..=500).contains(&limits.max_price_deviation_bps) {
        return Err(v13_runtime_error("invalid_execution_limit"));
    }
    // Cancellation-only callbacks do not require a fresh market.
    if !commands
        .iter()
        .any(|c| matches!(c, StagedCommandV13::Submit { .. }))
    {
        return Ok(());
    }
    let gross = positions
        .iter()
        .try_fold(0i128, |sum, p| {
            sum.checked_add(i128::from(p.qty_lots).abs())
        })
        .ok_or_else(|| v13_runtime_error("execution_limit_overflow"))?;
    let mut notional = titan_account_service::exposure_tick_lots(
        market.best_ask_ticks,
        gross,
        orders.iter().map(|o| {
            (
                o.qty_lots.saturating_sub(o.cumulative_filled_lots).max(0),
                o.price_ticks,
            )
        }),
    )
    .map_err(|_| v13_runtime_error("execution_limit_overflow"))?
    .1;
    for command in commands {
        let StagedCommandV13::Submit { request, .. } = command else {
            continue;
        };
        if request.asset_no != 0
            || request.order_type != 1
            || request.price_ticks <= 0
            || request.qty_lots <= 0
            || market.best_bid_ticks <= 0
            || market.best_ask_ticks < market.best_bid_ticks
            || now
                .checked_sub(last_bbo_ns)
                .is_none_or(|age| age < 0 || age > limits.max_market_age_ns)
        {
            return Err(v13_runtime_error("execution_price_unproven"));
        }
        let reference = if request.side == 1 {
            market.best_ask_ticks
        } else {
            market.best_bid_ticks
        };
        let deviation = (i128::from(request.price_ticks) - i128::from(reference)).abs() * 10_000;
        if deviation > i128::from(reference) * i128::from(limits.max_price_deviation_bps) {
            return Err(v13_runtime_error("execution_price_limit"));
        }
        // Venue reduce-only prevents new inventory; protective exits retain the price envelope.
        if request.reduce_only == 0 {
            notional = notional
                .checked_add(
                    i128::from(request.qty_lots)
                        .checked_mul(i128::from(market.best_ask_ticks.max(request.price_ticks)))
                        .ok_or_else(|| v13_runtime_error("execution_limit_overflow"))?,
                )
                .ok_or_else(|| v13_runtime_error("execution_limit_overflow"))?;
            if notional > cap {
                return Err(v13_runtime_error("execution_exposure_limit"));
            }
        }
    }
    Ok(())
}

fn commit_commands(
    core: &NativeV13Core,
    inner: &mut NativeV13Inner,
    commands: &[StagedCommandV13],
) -> LocalResult<()> {
    if let Some(limits) = &core.execution_limits {
        if inner.markets.len() != 1 {
            return Err(v13_runtime_error("execution_binding_unproven"));
        }
        inner.inventory.validate_opening(
            &inner.positions,
            !inner.pending_fills.is_empty() || !inner.awaiting_fills.is_empty(),
            commands,
        )?;
        validate_execution_envelope(
            limits,
            inner.markets[0],
            &inner.positions,
            &inner.active_orders,
            inner.last_bbo_ns,
            commands,
            core.context.clock.now_ns(),
        )?;
    }
    let submits = commands
        .iter()
        .filter(|command| matches!(command, StagedCommandV13::Submit { .. }))
        .count();
    if inner.active_orders.len().saturating_add(submits) > PUBLIC_CAPACITY {
        return Err(v13_runtime_error("active_order_capacity"));
    }
    // Validate the entire callback before any external side effect.
    for command in commands {
        match *command {
            StagedCommandV13::Submit { request, .. } => {
                execution_binding(core, request.account_no, request.asset_no)?;
                if !matches!(request.side, 1 | 2)
                    || !matches!(request.order_type, 1 | 2)
                    || request.qty_lots <= 0
                    || request.position_side > 2
                {
                    return Err(v13_runtime_error("invalid_order"));
                }
                v13_tif_to_account(request.time_in_force)?;
            }
            StagedCommandV13::Cancel { request, .. } => {
                execution_binding(core, request.account_no, request.asset_no)?;
            }
        }
    }
    // Register the complete batch before invoking any external executor.
    for command in commands {
        match *command {
            StagedCommandV13::Submit { order_id, request } => {
                let order = TitanActiveOrderView {
                    order_id,
                    asset_no: request.asset_no,
                    account_no: request.account_no,
                    price_ticks: request.price_ticks,
                    qty_lots: request.qty_lots,
                    created_ts_ns: core.context.clock.now_ns(),
                    updated_ts_ns: core.context.clock.now_ns(),
                    side: request.side,
                    order_type: request.order_type,
                    time_in_force: request.time_in_force,
                    status: 1,
                    reduce_only: request.reduce_only,
                    position_side: request.position_side,
                    ..Default::default()
                };
                merge_seeded_order_record(&mut inner.orders_list, order)?;
                inner.active_orders.push(order);
            }
            StagedCommandV13::Cancel { request, .. } => {
                if let Some(order) = inner
                    .active_orders
                    .iter_mut()
                    .find(|item| item.order_id == request.order_id)
                {
                    order.status = 5;
                    order.updated_ts_ns = core.context.clock.now_ns();
                }
                if let Some(record) = inner.orders_list.get_mut(&request.order_id) {
                    record.status = 5;
                    record.last_event_ts_ns = core.context.clock.now_ns();
                }
            }
        }
    }
    if !commands.is_empty() && core.context.state_snapshot_sink.enabled() {
        let timing = tracing::enabled!(tracing::Level::DEBUG).then(Instant::now);
        checkpoint_state(core, inner, core.context.clock.now_ns().max(1) as u64)?;
        flush_checkpoint(
            core,
            inner,
            Instant::now() + std::time::Duration::from_secs(5),
        )
        .map_err(|_| v13_runtime_error("command_intent_flush_failed"))?;
        if let Some(started) = timing {
            tracing::debug!(
                elapsed_us = started.elapsed().as_micros() as u64,
                "command intent durability duration"
            );
        }
    }
    // Durability may take time: the same price envelope must still hold at dispatch.
    if let Some(limits) = &core.execution_limits {
        validate_execution_envelope(
            limits,
            inner.markets[0],
            &inner.positions,
            &[],
            inner.last_bbo_ns,
            commands,
            core.context.clock.now_ns(),
        )?;
    }
    for command in commands {
        match *command {
            StagedCommandV13::Submit { order_id, request } => {
                let binding = execution_binding(core, request.account_no, request.asset_no)?;
                let _ = binding.handle.submit(DirectNewOrderRequest {
                    position_side: request.position_side,
                    asset_id: AssetId(*binding.assets.get(&u64::from(request.asset_no)).unwrap()),
                    side: if request.side == 1 { 1 } else { -1 },
                    order_type: u8::from(request.order_type == 2),
                    time_in_force: v13_tif_to_account(request.time_in_force)?,
                    price_ticks: request.price_ticks,
                    quantity_lots: request.qty_lots,
                    reduce_only: request.reduce_only != 0,
                    client_order_id: client_order_id(core.context.strategy, order_id),
                });
            }
            StagedCommandV13::Cancel { request, .. } => {
                let binding = execution_binding(core, request.account_no, request.asset_no)?;
                let _ = binding.handle.cancel(DirectCancelOrderRequest {
                    asset_id: AssetId(*binding.assets.get(&u64::from(request.asset_no)).unwrap()),
                    client_order_id: Some(client_order_id(core.context.strategy, request.order_id)),
                    venue_order_id: None,
                });
            }
        }
        inner.command_count = inner.command_count.saturating_add(1);
    }
    Ok(())
}

fn execution_binding(
    core: &NativeV13Core,
    account_no: u32,
    asset_no: u32,
) -> LocalResult<&StrategyExecutionBinding> {
    let binding = core
        .context
        .execution
        .iter()
        .find(|item| item.local_account_no == account_no)
        .ok_or_else(|| v13_runtime_error("account_not_bound"))?;
    if !binding.assets.contains_key(&u64::from(asset_no)) {
        return Err(v13_runtime_error("asset_not_bound"));
    }
    Ok(binding)
}

fn local_account(core: &NativeV13Core, account_id: u32) -> LocalResult<u32> {
    core.context
        .accounts
        .iter()
        .find(|item| item.account.account_id.0 == account_id)
        .map(|item| item.local_account_no)
        .ok_or_else(|| v13_runtime_error("account_not_bound"))
}
fn account_binding(
    core: &NativeV13Core,
    account_id: u32,
    asset_id: u32,
) -> LocalResult<(u32, u32)> {
    let account = core
        .context
        .accounts
        .iter()
        .find(|item| item.account.account_id.0 == account_id)
        .ok_or_else(|| v13_runtime_error("account_not_bound"))?;
    let asset = account
        .tradable_assets
        .iter()
        .find(|item| item.asset_id == asset_id)
        .ok_or_else(|| v13_runtime_error("asset_not_bound"))?;
    Ok((account.local_account_no, asset.local_asset_no))
}

fn client_order_prefix(strategy: StrategyHandle) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.direct-order.v13");
    // Ownership survives process restarts and strategy replacement. Uniqueness between
    // generations is carried by the generation-prefixed V13 order id.
    digest.update(strategy.runtime_strategy_no.0.to_le_bytes());
    digest.finalize()[..8].try_into().unwrap()
}
pub(crate) fn client_order_id(
    strategy: StrategyHandle,
    order_id: u64,
) -> titan_account_service::Id128 {
    let mut value = [0; 16];
    value[..8].copy_from_slice(&client_order_prefix(strategy));
    value[8..].copy_from_slice(&order_id.to_le_bytes());
    titan_account_service::Id128(value)
}
pub(crate) fn v13_strategy_order_id(
    strategy: StrategyHandle,
    value: titan_account_service::Id128,
) -> u64 {
    if value.0[..8] == client_order_prefix(strategy) {
        u64::from_le_bytes(value.0[8..].try_into().unwrap())
    } else {
        0
    }
}

/// Expected inventory is anchored in the coherent seed, then advanced only by independent fills.
/// Comparing quantities handles both fill-before-position and position-before-fill delivery.
#[derive(Default)]
struct InventoryProof {
    expected: BTreeMap<(u32, u32, u8), i128>,
}
impl InventoryProof {
    fn validate_opening(
        &self,
        positions: &[TitanPositionView],
        unsettled: bool,
        commands: &[StagedCommandV13],
    ) -> LocalResult<()> {
        if (unsettled || !self.confirmed(positions))
            && commands
                .iter()
                .any(|c| matches!(c,StagedCommandV13::Submit{request,..} if request.reduce_only==0))
        {
            return Err(v13_runtime_error("execution_inventory_unproven"));
        }
        Ok(())
    }
    fn seed(&mut self, positions: &[TitanPositionView]) {
        self.expected = positions
            .iter()
            .map(|p| {
                (
                    (p.account_no, p.asset_no, p.position_side),
                    i128::from(p.qty_lots),
                )
            })
            .collect();
    }
    fn fill(&mut self, fill: &TitanFillView, previous: i64, position_side: u8) -> LocalResult<()> {
        if fill.cumulative_filled_lots < 0 || !matches!(fill.side, 1 | 2) {
            return Err(v13_runtime_error("invalid_inventory_fill"));
        }
        let delta = i128::from(fill.cumulative_filled_lots.saturating_sub(previous).max(0));
        let key = (fill.account_no, fill.asset_no, position_side);
        if !self.expected.contains_key(&key) && self.expected.len() >= PUBLIC_CAPACITY {
            return Err(v13_runtime_error("inventory_proof_capacity"));
        }
        let expected = self.expected.entry(key).or_default();
        *expected = expected
            .checked_add(if fill.side == 1 { delta } else { -delta })
            .ok_or_else(|| v13_runtime_error("inventory_proof_overflow"))?;
        Ok(())
    }
    fn confirmed(&self, positions: &[TitanPositionView]) -> bool {
        self.expected.iter().all(|(key, expected)| {
            positions
                .iter()
                .find(|p| (p.account_no, p.asset_no, p.position_side) == *key)
                .map_or(0, |p| i128::from(p.qty_lots))
                == *expected
        }) && positions.iter().all(|p| {
            self.expected
                .get(&(p.account_no, p.asset_no, p.position_side))
                .copied()
                .unwrap_or(0)
                == i128::from(p.qty_lots)
        })
    }
}

fn update_active_fill(inner: &mut NativeV13Inner, fill: &TitanFillView) -> LocalResult<bool> {
    let key = (fill.account_no, fill.order_id);
    let previous = inner.settled_fills.get(&key).copied().unwrap_or(0);
    let position_side = inner
        .orders_list
        .get(&fill.order_id)
        .map_or(0, |o| o.position_side);
    inner.inventory.fill(fill, previous, position_side)?;
    if fill.cumulative_filled_lots <= previous {
        return Ok(false);
    }
    inner
        .settled_fills
        .insert(key, previous.max(fill.cumulative_filled_lots));
    if inner
        .awaiting_fills
        .get(&key)
        .is_some_and(|target| *target <= fill.cumulative_filled_lots)
    {
        inner.awaiting_fills.remove(&key);
    }
    inner
        .order_versions
        .insert((fill.account_no, fill.order_id), fill.account_sequence);
    if inner.lifecycle == StrategyLifecycle::Defined {
        inner
            .pending_fills
            .insert((fill.account_no, fill.order_id), *fill);
    }
    if let Some(order) = inner
        .active_orders
        .iter_mut()
        .find(|item| item.order_id == fill.order_id)
    {
        order.cumulative_filled_lots = fill.cumulative_filled_lots;
        order.updated_ts_ns = fill.receive_ts_ns;
        order.account_sequence = fill.account_sequence;
        if fill.final_fill != 0 || order.cumulative_filled_lots >= order.qty_lots {
            order.status = 4;
        }
    }
    if let Some(order) = inner.orders_list.get_mut(&fill.order_id) {
        order.cumulative_filled_lots = fill.cumulative_filled_lots;
        order.last_fill_price_ticks = fill.fill_price_ticks;
        order.last_fill_ts_ns = fill.receive_ts_ns;
        order.last_event_ts_ns = fill.receive_ts_ns;
        order.account_sequence = fill.account_sequence;
        order.fill_count = order.fill_count.saturating_add(1);
        if fill.final_fill != 0 || order.cumulative_filled_lots >= order.qty_lots {
            order.status = 4;
        } else {
            order.status = 3;
        }
    }
    Ok(true)
}

fn recovered_fill_boundaries(
    records: &BTreeMap<u64, StrategyOrderRecordV13>,
) -> LocalResult<BTreeMap<(u32, u64), i64>> {
    records
        .iter()
        .map(|(id, order)| {
            if *id == 0
                || *id != order.order_id
                || order.qty_lots <= 0
                || order.cumulative_filled_lots < 0
                || order.cumulative_filled_lots > order.qty_lots
            {
                return Err(v13_runtime_error("recovery_fill_boundary_invalid"));
            }
            Ok(((order.account_no, *id), order.cumulative_filled_lots))
        })
        .collect()
}
fn update_active_order(
    inner: &mut NativeV13Inner,
    value: &TitanOrderEventView,
    side: u8,
    order_type: u8,
    tif: u8,
    reduce_only: u8,
) -> LocalResult<()> {
    let position_side = if value.position_side == 0 {
        inner
            .orders_list
            .get(&value.order_id)
            .map_or(0, |record| record.position_side)
    } else {
        value.position_side
    };
    if position_side > 2
        || inner
            .orders_list
            .get(&value.order_id)
            .is_some_and(|record| record.position_side != position_side)
    {
        return Err(v13_runtime_error("order_position_side_changed"));
    }
    let key = (value.account_no, value.order_id);
    if value.cumulative_filled_lots > inner.settled_fills.get(&key).copied().unwrap_or(0) {
        inner
            .awaiting_fills
            .insert(key, value.cumulative_filled_lots);
    }
    inner
        .order_versions
        .insert((value.account_no, value.order_id), value.account_sequence);
    if inner
        .pending_fills
        .get(&(value.account_no, value.order_id))
        .is_some_and(|fill| fill.account_sequence <= value.account_sequence)
    {
        inner
            .pending_fills
            .remove(&(value.account_no, value.order_id));
    }
    if let Some(order) = inner
        .active_orders
        .iter_mut()
        .find(|item| item.order_id == value.order_id)
    {
        order.cumulative_filled_lots = value.cumulative_filled_lots;
        order.updated_ts_ns = value.event_ts_ns;
        order.account_sequence = value.account_sequence;
        order.status = value.status;
        order.order_type = order_type;
        order.time_in_force = tif;
        order.reduce_only |= reduce_only;
        order.position_side = position_side;
    } else if value.order_id != 0 && value.status < 4 {
        if inner.active_orders.len() >= PUBLIC_CAPACITY {
            return Err(v13_runtime_error("active_order_capacity"));
        }
        inner.active_orders.push(TitanActiveOrderView {
            order_id: value.order_id,
            asset_no: value.asset_no,
            account_no: value.account_no,
            price_ticks: value.price_ticks,
            qty_lots: value.qty_lots,
            cumulative_filled_lots: value.cumulative_filled_lots,
            created_ts_ns: value.event_ts_ns,
            updated_ts_ns: value.event_ts_ns,
            account_sequence: value.account_sequence,
            side,
            order_type,
            time_in_force: tif,
            status: value.status,
            reduce_only,
            position_side,
            ..TitanActiveOrderView::default()
        });
    }
    let record = inner
        .orders_list
        .entry(value.order_id)
        .or_insert(StrategyOrderRecordV13 {
            order_id: value.order_id,
            asset_no: value.asset_no,
            account_no: value.account_no,
            price_ticks: value.price_ticks,
            qty_lots: value.qty_lots,
            submit_ts_ns: value.event_ts_ns,
            side,
            order_type,
            time_in_force: tif,
            reduce_only,
            ..StrategyOrderRecordV13::default()
        });
    record.position_side = position_side;
    record.price_ticks = value.price_ticks;
    record.qty_lots = value.qty_lots;
    record.cumulative_filled_lots = value.cumulative_filled_lots;
    record.last_event_ts_ns = value.event_ts_ns;
    record.account_sequence = value.account_sequence;
    record.status = value.status;
    record.error_code = value.reason;
    record.reduce_only |= reduce_only;
    if matches!(value.status, 4 | 6 | 7 | 8) {
        inner
            .active_orders
            .retain(|item| item.order_id != value.order_id);
    }
    Ok(())
}

pub(crate) fn account_order_type_to_v13(value: u8) -> LocalResult<u8> {
    match value {
        0 => Ok(1),
        1 => Ok(2),
        _ => Err(v13_runtime_error("unsupported_account_order_type")),
    }
}

pub(crate) fn account_tif_to_v13(value: u8) -> LocalResult<u8> {
    match value {
        0 => Ok(1),
        1 => Ok(4),
        2 => Ok(3),
        3 => Ok(2),
        _ => Err(v13_runtime_error("unsupported_account_tif")),
    }
}

fn v13_tif_to_account(value: u8) -> LocalResult<u8> {
    match value {
        1 => Ok(0),
        2 => Ok(3),
        3 => Ok(2),
        4 => Ok(1),
        _ => Err(v13_runtime_error("unsupported_tif")),
    }
}

pub(crate) fn account_order_status_to_v13(value: u8) -> LocalResult<u8> {
    match value {
        0 => Ok(1),
        1 => Ok(2),
        2 => Ok(8),
        3 => Ok(4),
        4 => Ok(6),
        5 => Ok(3),
        6 => Ok(7),
        7 => Ok(2),
        255 => Ok(255),
        _ => Err(v13_runtime_error("unsupported_account_order_status")),
    }
}
fn upsert_position(inner: &mut NativeV13Inner, event: TitanPositionEventView) -> LocalResult<()> {
    if event.position_side > 2
        || (event.position_side == 1 && event.qty_lots < 0)
        || (event.position_side == 2 && event.qty_lots > 0)
        || inner.positions.iter().any(|v| {
            v.account_no == event.account_no
                && v.asset_no == event.asset_no
                && (v.position_side == 0) != (event.position_side == 0)
        })
    {
        return Err(v13_runtime_error("position_mode_changed_requires_recovery"));
    }
    inner.position_versions.insert(
        (event.account_no, event.asset_no, event.position_side),
        event.account_sequence,
    );
    let value = TitanPositionView {
        asset_no: event.asset_no,
        account_no: event.account_no,
        position_side: event.position_side,
        reserved: [0; 7],
        qty_lots: event.qty_lots,
        average_price_ticks: event.average_price_ticks,
        realized_pnl_ticks: event.realized_pnl_ticks,
        account_sequence: event.account_sequence,
    };
    if let Some(current) = inner.positions.iter_mut().find(|item| {
        item.account_no == value.account_no
            && item.asset_no == value.asset_no
            && item.position_side == value.position_side
    }) {
        *current = value;
    } else {
        if inner.positions.len() >= PUBLIC_CAPACITY {
            return Err(v13_runtime_error("position_capacity"));
        }
        inner.positions.push(value);
    }
    Ok(())
}
fn upsert_balance(inner: &mut NativeV13Inner, event: TitanBalanceEventView) -> LocalResult<()> {
    inner.balance_versions.insert(
        (event.account_no, event.currency_no),
        event.account_sequence,
    );
    let value = TitanBalanceView {
        account_no: event.account_no,
        currency_no: event.currency_no,
        total_units: event.total_units,
        available_units: event.available_units,
        account_sequence: event.account_sequence,
    };
    if let Some(current) = inner
        .balances
        .iter_mut()
        .find(|item| item.account_no == value.account_no && item.currency_no == value.currency_no)
    {
        *current = value;
    } else {
        if inner.balances.len() >= PUBLIC_CAPACITY {
            return Err(v13_runtime_error("balance_capacity"));
        }
        inner.balances.push(value);
    }
    Ok(())
}
fn upsert_account(inner: &mut NativeV13Inner, event: TitanAccountStateEventView) {
    inner
        .account_versions
        .insert(event.account_no, event.account_sequence);
    if let Some(value) = inner
        .accounts
        .iter_mut()
        .find(|item| item.account_no == event.account_no)
    {
        value.account_epoch = event.account_epoch;
        value.account_sequence = event.account_sequence;
        value.state = event.state;
        value.reason = event.reason;
    }
}

fn dispatch_execution_result(
    core: &NativeV13Core,
    inner: &mut NativeV13Inner,
    result: ObservedExecutionResult,
) -> LocalResult<()> {
    if !matches!(
        inner.lifecycle,
        StrategyLifecycle::Running | StrategyLifecycle::Paused | StrategyLifecycle::Stopping
    ) {
        return Ok(());
    }
    let client_order_id = result
        .client_order_id
        .ok_or_else(|| v13_runtime_error("execution_result_missing_client_order_id"))?;
    let order_id = v13_strategy_order_id(core.context.strategy, client_order_id);
    if order_id == 0 {
        return Ok(());
    }
    let (account_no, asset_no) =
        account_binding(core, result.account.account_id.0, result.asset_id.0)?;
    let Some(existing) = inner
        .active_orders
        .iter()
        .find(|order| order.order_id == order_id)
        .copied()
    else {
        // A final private-stream fact may win the race with the REST completion.
        return Ok(());
    };
    let now = core.context.clock.now_ns();
    match result.request_kind {
        ExecutionRequestKind::Submit => {
            // Canonical private-stream facts are authoritative. A late REST completion must not
            // overwrite an order that has already advanced beyond local PendingSubmit.
            if existing.status != 1 {
                return Ok(());
            }
            let status = match result.outcome {
                ObservedExecutionOutcome::Accepted(info) => {
                    let status = account_order_status_to_v13(info.status)?;
                    // REST acceptance does not carry cumulative fills. Only canonical account
                    // facts may retire an accepted order and close the stopping boundary.
                    if matches!(status, 4 | 6 | 8) {
                        2
                    } else {
                        status
                    }
                }
                ObservedExecutionOutcome::Rejected(_) => 7,
                ObservedExecutionOutcome::Unknown(_) => 255,
            };
            let view = TitanOrderEventView {
                order_id,
                asset_no,
                account_no,
                price_ticks: existing.price_ticks,
                qty_lots: existing.qty_lots,
                cumulative_filled_lots: existing.cumulative_filled_lots,
                event_ts_ns: now,
                account_sequence: existing.account_sequence,
                status,
                reason: if status == 7 { 1 } else { 2 },
                position_side: existing.position_side,
                reserved: [0; 5],
            };
            update_active_order(
                inner,
                &view,
                existing.side,
                existing.order_type,
                existing.time_in_force,
                existing.reduce_only,
            )?;
            NativeV13Runtime::invoke(inner, core, V13EventKind::Order, 1, |ctx| {
                ctx.order_events_ptr = &view;
                ctx.order_events_len = 1;
            })
        }
        ExecutionRequestKind::Cancel => {
            let request_result = match result.outcome {
                ObservedExecutionOutcome::Accepted(_) => 0,
                ObservedExecutionOutcome::Rejected(_) => 1,
                ObservedExecutionOutcome::Unknown(_) => 2,
            };
            let restored_status = if existing.cumulative_filled_lots > 0 {
                3
            } else {
                2
            };
            if let Some(order) = inner
                .active_orders
                .iter_mut()
                .find(|order| order.order_id == order_id)
            {
                order.status = match request_result {
                    0 => 5,
                    1 => restored_status,
                    _ => 255,
                };
                order.updated_ts_ns = now;
            }
            if let Some(order) = inner.orders_list.get_mut(&order_id) {
                order.status = match request_result {
                    0 => 5,
                    1 => restored_status,
                    _ => 255,
                };
                order.last_event_ts_ns = now;
                order.error_code = request_result;
            }
            let view = TitanCancelEventView {
                order_id,
                asset_no,
                account_no,
                event_ts_ns: now,
                account_sequence: existing.account_sequence,
                request_result,
                final_status: 0,
                reserved: [0; 6],
            };
            NativeV13Runtime::invoke(inner, core, V13EventKind::Cancel, 1, |ctx| {
                ctx.cancel_events_ptr = &view;
                ctx.cancel_events_len = 1;
            })
        }
    }
}

fn checkpoint_state(
    core: &NativeV13Core,
    inner: &mut NativeV13Inner,
    checkpoint_id: u64,
) -> LocalResult<()> {
    if !core.context.state_snapshot_sink.enabled() {
        return Ok(());
    }
    prune_terminal_history(inner);
    let checkpoint_id = checkpoint_id.max(1);
    let identity = public_state_identity_v13(
        &inner.accounts,
        &inner.active_orders,
        &inner.positions,
        &inner.balances,
    );
    inner.instance.update_checkpoint_boundary(
        core.lane
            .get()
            .map_or(0, |lane| lane.progress().committed_sequence),
        identity,
    );
    let snapshot = inner
        .instance
        .freeze_state(checkpoint_id)
        .map_err(|_| v13_runtime_error("snapshot_failed"))?;
    let orders_list: Arc<[StrategyOrderRecordV13]> = inner
        .orders_list
        .values()
        .copied()
        .collect::<Vec<_>>()
        .into();
    let orders_list_checksum = orders_list_checksum(&orders_list);
    let recovery = RecoveryBaseline::capture(
        recovery_seed(
            &inner.accounts,
            &inner.positions,
            &inner.balances,
            &inner.active_orders,
            &[],
        ),
        snapshot.checksum,
        orders_list_checksum,
    )
    .with_pending_execution(
        !inner.awaiting_fills.is_empty()
            || !inner.pending_fills.is_empty()
            || (core.execution_limits.is_some() && !inner.inventory.confirmed(&inner.positions)),
        snapshot.checksum,
        orders_list_checksum,
    );
    let result = core
        .context
        .state_snapshot_sink
        .submit_token(StrategyRecoveryPoint {
            checkpoint_id: snapshot.checkpoint_id,
            strategy: core.context.strategy,
            strategy_instance_id: snapshot.strategy_instance_id,
            strategy_type: snapshot.strategy_type,
            source_version: snapshot.source_version,
            generation: snapshot.generation,
            event_committed_sequence: snapshot.event_committed_sequence,
            compiled_strategy_digest: snapshot.compiled_strategy_digest,
            binding_digest: snapshot.binding_digest,
            abi_version: snapshot.abi_version,
            state_schema_version: snapshot.state_schema_version,
            state_schema_hash: snapshot.state_schema_hash,
            state_alignment: snapshot.state_alignment,
            state_bytes: snapshot.state_bytes,
            orders_list,
            orders_list_checksum,
            public_state_identity: snapshot.public_state_identity,
            checksum: snapshot.checksum,
            recovery: Some(recovery),
        });
    if result.is_err() {
        fail_runtime(core, inner, Arc::from("checkpoint_submit_failed"));
    }
    if let Ok(token) = &result {
        inner.last_checkpoint_token = Some(token.clone());
        inner.checkpoint_dirty = false;
        inner.last_checkpoint_ns = core.context.clock.now_ns();
    }
    result.map(|_| ())
}

fn flush_checkpoint(
    core: &NativeV13Core,
    inner: &NativeV13Inner,
    deadline: Instant,
) -> LocalResult<()> {
    match &inner.last_checkpoint_token {
        Some(token) => core
            .context
            .state_snapshot_sink
            .flush_through(token, deadline),
        None => core.context.state_snapshot_sink.flush(deadline),
    }
}

fn prune_terminal_history(inner: &mut NativeV13Inner) {
    const HISTORY_LIMIT: usize = 4096;
    if inner.orders_list.len() <= HISTORY_LIMIT + PUBLIC_CAPACITY {
        return;
    }
    let mut terminal: Vec<_> = inner
        .orders_list
        .values()
        .filter(|order| {
            matches!(order.status, 4 | 6 | 7 | 8)
                && !inner
                    .awaiting_fills
                    .contains_key(&(order.account_no, order.order_id))
                && !inner
                    .active_orders
                    .iter()
                    .any(|active| active.order_id == order.order_id)
        })
        .map(|order| (order.last_event_ts_ns, order.account_no, order.order_id))
        .collect();
    terminal.sort_unstable();
    let remove = terminal.len().saturating_sub(HISTORY_LIMIT);
    for (_, account, id) in terminal.into_iter().take(remove) {
        inner.orders_list.remove(&id);
        inner.order_versions.remove(&(account, id));
        inner.settled_fills.remove(&(account, id));
        inner.pending_fills.remove(&(account, id));
    }
}

fn publish_business_observation(core: &NativeV13Core, inner: &NativeV13Inner, force: bool) {
    if !matches!(
        inner.lifecycle,
        StrategyLifecycle::Running
            | StrategyLifecycle::Paused
            | StrategyLifecycle::Stopping
            | StrategyLifecycle::Stopped
    ) {
        return;
    }
    if let Some(publisher) = &core.business_observation {
        if force {
            publisher.request();
        }
        publisher.publish(
            inner.instance.state.as_bytes(),
            inner.callback_count,
            || core.context.clock.now_ns(),
        );
    }
}

fn fail_runtime(core: &NativeV13Core, inner: &mut NativeV13Inner, reason: Arc<str>) {
    if let Some(publisher) = &core.business_observation {
        publisher.invalidate();
    }
    tracing::warn!(
        runtime_strategy_no = core.context.strategy.runtime_strategy_no.0,
        generation = core.context.strategy.generation,
        reason_code = reason.as_ref(),
        "strategy runtime failed"
    );
    core.context.activation.invalidate();
    inner.instance.stop();
    inner.lifecycle = StrategyLifecycle::Failed;
    inner.last_error.get_or_insert_with(|| reason.clone());
    fail_pending_stop(core, inner, reason);
}

fn fail_pending_stop(core: &NativeV13Core, inner: &mut NativeV13Inner, reason: Arc<str>) {
    if let Some(id) = inner.stop_operation.take()
        && let Some(operation) = core
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
    {
        operation.state = StrategyOperationState::Failed;
        operation.detail = reason;
    }
    inner.stop_deadline = None;
}

fn finish_stop_if_drained(core: &NativeV13Core, inner: &mut NativeV13Inner) -> LocalResult<()> {
    if inner
        .stop_deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        fail_runtime(core, inner, Arc::from("stop_drain_deadline_exceeded"));
        return Err(v13_runtime_error("stop_drain_deadline_exceeded"));
    }
    if inner.lifecycle != StrategyLifecycle::Stopping
        || !inner.active_orders.is_empty()
        || !inner.awaiting_fills.is_empty()
        || (core.execution_limits.is_some() && !inner.inventory.confirmed(&inner.positions))
        || (inner.stop_barrier && !inner.stop_ready)
    {
        return Ok(());
    }
    let checkpoint = checkpoint_state(core, inner, core.context.clock.now_ns().max(1) as u64)
        .and_then(|_| {
            flush_checkpoint(
                core,
                inner,
                inner.stop_deadline.unwrap_or_else(Instant::now),
            )
        });
    if let Err(error) = checkpoint {
        fail_runtime(core, inner, error.reason_code.clone());
        return Err(error);
    }
    core.context.activation.close();
    inner.instance.stop();
    inner.lifecycle = StrategyLifecycle::Stopped;
    core.stopped.store(true, Ordering::Release);
    publish_business_observation(core, inner, true);
    if let Some(operation_id) = inner.stop_operation.take()
        && let Some(operation) = core
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&operation_id)
    {
        operation.state = StrategyOperationState::Succeeded;
        operation.detail = Arc::from("drained_and_stopped");
    }
    inner.stop_deadline = None;
    Ok(())
}

impl StrategyRuntime for NativeV13Runtime {
    fn execution_risk(&self) -> LocalResult<Vec<titan_account_service::ExecutionRiskEvidence>> {
        use titan_account_service::{ExecutionRiskEvidence, ExecutionRiskOrder};
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut result = Vec::new();
        for binding in self.core.context.accounts.iter() {
            let epoch = inner
                .accounts
                .iter()
                .find(|a| a.account_no == binding.local_account_no)
                .map_or(0, |a| a.account_epoch);
            for asset in binding.tradable_assets.iter() {
                let positions = inner
                    .positions
                    .iter()
                    .filter(|p| {
                        p.account_no == binding.local_account_no
                            && p.asset_no == asset.local_asset_no
                    })
                    .map(|p| (p.position_side, p.qty_lots))
                    .collect();
                let mut expected = inner
                    .inventory
                    .expected
                    .iter()
                    .filter(|((account, no, _), _)| {
                        *account == binding.local_account_no && *no == asset.local_asset_no
                    })
                    .map(|((_, _, side), q)| (*side, *q))
                    .collect::<BTreeMap<_, _>>();
                for ((account, order), target) in &inner.awaiting_fills {
                    if *account == binding.local_account_no
                        && let Some(o) = inner.orders_list.get(order)
                        && o.asset_no == asset.local_asset_no
                    {
                        let delta = i128::from(
                            target
                                .saturating_sub(
                                    inner
                                        .settled_fills
                                        .get(&(*account, *order))
                                        .copied()
                                        .unwrap_or(0),
                                )
                                .max(0),
                        );
                        let value = expected.entry(o.position_side).or_default();
                        *value = value
                            .checked_add(if o.side == 1 { delta } else { -delta })
                            .ok_or_else(|| v13_runtime_error("inventory_proof_overflow"))?;
                    }
                }
                let orders = inner
                    .active_orders
                    .iter()
                    .filter(|o| {
                        o.account_no == binding.local_account_no
                            && o.asset_no == asset.local_asset_no
                    })
                    .map(|o| ExecutionRiskOrder {
                        client_order_id: client_order_id(self.core.context.strategy, o.order_id),
                        price_ticks: o.price_ticks,
                        quantity_lots: o.qty_lots,
                        filled_lots: o.cumulative_filled_lots,
                    })
                    .collect();
                result.push(ExecutionRiskEvidence {
                    intent_revision: inner.command_count,
                    account_id: binding.account.account_id.0,
                    account_generation: binding.account.generation,
                    account_epoch: epoch,
                    asset_id: asset.asset_id,
                    positions,
                    expected_positions: expected
                        .into_iter()
                        .map(|(s, q)| (s, q.to_string()))
                        .collect(),
                    orders,
                    inventory_confirmed: inner.inventory.confirmed(&inner.positions)
                        && inner.awaiting_fills.is_empty()
                        && inner.pending_fills.is_empty(),
                });
            }
        }
        Ok(result)
    }
    fn pause_supported(&self) -> bool {
        self.core.pause_safe
    }
    fn release_safe(&self) -> bool {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.lifecycle == StrategyLifecycle::Stopped
            && !inner.instance.command_gate_open()
            && !self.core.context.activation.is_open()
            && inner.active_orders.is_empty()
            && inner.pending_fills.is_empty()
            && inner.awaiting_fills.is_empty()
            && (self.core.execution_limits.is_none() || inner.inventory.confirmed(&inner.positions))
    }
    fn recovery_status(&self) -> LocalResult<serde_json::Value> {
        let identity = &self.core.state_identity;
        Ok(
            serde_json::json!({"available":true,"state_schema_version":identity.version,
            "state_schema_hash":identity.hash.iter().map(|v|format!("{v:02x}")).collect::<String>()}),
        )
    }
    fn business_observation(&self) -> LocalResult<BusinessObservationSnapshot> {
        let Some(publisher) = &self.core.business_observation else {
            return Ok(BusinessObservationSnapshot::unavailable(
                "business observation not declared",
            ));
        };
        if self.core.async_failure.load(Ordering::Acquire)
            || self
                .core
                .lane
                .get()
                .is_some_and(|lane| lane.health().state == SubscriberState::Failed)
        {
            publisher.invalidate();
        }
        publisher
            .snapshot()
            .map_err(|_| v13_runtime_error("business_observation_projection_failed"))
    }
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()> {
        self.core
            .lane
            .set(lane)
            .map_err(|_| v13_runtime_error("lane_already_attached"))
    }
    fn seed_public_state(&self, seed: StrategyPublicStateSeedV13) -> LocalResult<()> {
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.lifecycle != StrategyLifecycle::Defined {
            return Err(v13_runtime_error("seed_after_prepare"));
        }
        if seed.positions.len() > PUBLIC_CAPACITY
            || seed.balances.len() > PUBLIC_CAPACITY
            || seed.accounts.len() > PUBLIC_CAPACITY
            || seed.active_orders.len() > PUBLIC_CAPACITY
        {
            return Err(v13_runtime_error("public_state_capacity"));
        }
        let mut position_keys = std::collections::BTreeSet::new();
        for value in seed.positions.iter() {
            if value.position_side > 2
                || (value.position_side == 1 && value.qty_lots < 0)
                || (value.position_side == 2 && value.qty_lots > 0)
                || !position_keys.insert((value.account_no, value.asset_no, value.position_side))
                || seed.positions.iter().any(|v| {
                    v.account_no == value.account_no
                        && v.asset_no == value.asset_no
                        && (v.position_side == 0) != (value.position_side == 0)
                })
            {
                return Err(v13_runtime_error("invalid_position_seed"));
            }
        }
        for value in seed.positions.iter().copied() {
            let key = (value.account_no, value.asset_no, value.position_side);
            if inner
                .position_versions
                .get(&key)
                .is_some_and(|version| *version > value.account_sequence)
            {
                continue;
            }
            inner.position_versions.insert(key, value.account_sequence);
            if let Some(current) = inner
                .positions
                .iter_mut()
                .find(|item| (item.account_no, item.asset_no, item.position_side) == key)
            {
                *current = value;
            } else {
                if inner.positions.len() >= PUBLIC_CAPACITY {
                    return Err(v13_runtime_error("position_capacity"));
                }
                inner.positions.push(value);
            }
        }
        for value in seed.balances.iter().copied() {
            let key = (value.account_no, value.currency_no);
            if inner
                .balance_versions
                .get(&key)
                .is_some_and(|version| *version > value.account_sequence)
            {
                continue;
            }
            inner.balance_versions.insert(key, value.account_sequence);
            if let Some(current) = inner
                .balances
                .iter_mut()
                .find(|item| (item.account_no, item.currency_no) == key)
            {
                *current = value;
            } else {
                if inner.balances.len() >= PUBLIC_CAPACITY {
                    return Err(v13_runtime_error("balance_capacity"));
                }
                inner.balances.push(value);
            }
        }
        for value in seed.accounts.iter().copied() {
            if inner
                .account_versions
                .get(&value.account_no)
                .is_some_and(|version| *version > value.account_sequence)
            {
                continue;
            }
            inner
                .account_versions
                .insert(value.account_no, value.account_sequence);
            if let Some(current) = inner
                .accounts
                .iter_mut()
                .find(|item| item.account_no == value.account_no)
            {
                *current = value;
            } else {
                if inner.accounts.len() >= PUBLIC_CAPACITY {
                    return Err(v13_runtime_error("account_capacity"));
                }
                inner.accounts.push(value);
            }
        }
        for (account_no, boundary) in seed.order_boundaries.iter().copied() {
            let versions = inner.order_versions.clone();
            let seeded_orders = &seed.active_orders;
            inner.active_orders.retain(|order| {
                order.account_no != account_no
                    || versions
                        .get(&(account_no, order.order_id))
                        .copied()
                        .unwrap_or(0)
                        > boundary
                    || seeded_orders.iter().any(|seeded| {
                        seeded.account_no == account_no && seeded.order_id == order.order_id
                    })
            });
        }
        for mut value in seed.active_orders.iter().copied() {
            let key = (value.account_no, value.order_id);
            if inner
                .order_versions
                .get(&key)
                .is_some_and(|version| *version > value.account_sequence)
            {
                let Some(fill) = inner.pending_fills.get(&key).copied() else {
                    continue;
                };
                if fill.final_fill != 0 || fill.cumulative_filled_lots >= value.qty_lots {
                    continue;
                }
                value.cumulative_filled_lots = fill.cumulative_filled_lots;
                value.updated_ts_ns = fill.receive_ts_ns;
                value.account_sequence = fill.account_sequence;
                value.status = 3;
            }
            inner.order_versions.insert(key, value.account_sequence);
            if let Some(current) = inner
                .active_orders
                .iter_mut()
                .find(|item| (item.account_no, item.order_id) == key)
            {
                *current = value;
            } else {
                if inner.active_orders.len() >= PUBLIC_CAPACITY {
                    return Err(v13_runtime_error("active_order_capacity"));
                }
                inner.active_orders.push(value);
            }
            merge_seeded_order_record(&mut inner.orders_list, value)?;
        }
        inner.pending_fills.clear();
        let settled: Vec<_> = inner
            .active_orders
            .iter()
            .map(|order| {
                (
                    (order.account_no, order.order_id),
                    order.cumulative_filled_lots,
                )
            })
            .collect();
        inner.settled_fills.extend(settled);
        inner.awaiting_fills.clear();
        let positions = inner.positions.clone();
        inner.inventory.seed(&positions);
        inner.recovery_orders = seed.recovery_orders.to_vec();
        Ok(())
    }
    fn seed_recovery_executions(
        &self,
        evidence: Arc<[RecoveryExecutionHistory]>,
    ) -> LocalResult<()> {
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.lifecycle != StrategyLifecycle::Defined || evidence.len() > 128 {
            return Err(v13_runtime_error("recovery_history_invalid_boundary"));
        }
        inner.recovery_executions = evidence.to_vec();
        Ok(())
    }
    fn restore_state(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()> {
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        let result = (|| {
            if inner.lifecycle != StrategyLifecycle::Defined {
                return Err(v13_runtime_error("restore_after_prepare"));
            }
            let expected_orders_checksum = orders_list_checksum(&snapshot.orders_list);
            let legacy_empty_orders =
                snapshot.orders_list.is_empty() && snapshot.orders_list_checksum == [0; 32];
            if !legacy_empty_orders && snapshot.orders_list_checksum != expected_orders_checksum {
                return Err(v13_runtime_error("orders_list_checksum_mismatch"));
            }
            let mut restored_orders = BTreeMap::new();
            for order in snapshot.orders_list.iter().copied() {
                if order.order_id == 0 || restored_orders.insert(order.order_id, order).is_some() {
                    return Err(v13_runtime_error("orders_list_invalid_identity"));
                }
            }
            let recovery = snapshot.recovery.clone();
            let consumed = recovered_fill_boundaries(&restored_orders)?;
            let mut replay = Vec::new();
            if let Some(baseline) = &recovery {
                baseline
                    .validate(
                        snapshot.checksum,
                        snapshot.orders_list_checksum,
                        snapshot.public_state_identity,
                    )
                    .map_err(v13_runtime_error)?;
                if !inner.pending_fills.is_empty()
                    || !inner.awaiting_fills.is_empty()
                    || (self.core.execution_limits.is_some()
                        && !inner.inventory.confirmed(&inner.positions))
                {
                    return Err(v13_runtime_error("recovery_pending_fill_requires_history"));
                }
                let current = recovery_seed(
                    &inner.accounts,
                    &inner.positions,
                    &inner.balances,
                    &inner.active_orders,
                    &inner.recovery_orders,
                );
                // Callback opt-in preserves strict behavior for existing compiled_strategies.
                if inner.instance.has_callback(V13EventKind::Recover) {
                    replay = recovery_execution_plan(
                        baseline,
                        &current,
                        &mut restored_orders,
                        &inner.recovery_executions,
                    )
                    .map_err(v13_runtime_error)?;
                }
            }
            let restored = snapshot.native_snapshot();
            inner
                .instance
                .restore_state(&restored)
                .map_err(|_| v13_runtime_error("snapshot_restore_failed"))?;
            inner.orders_list = restored_orders;
            if !replay.is_empty() {
                let baseline = recovery
                    .as_ref()
                    .ok_or_else(|| v13_runtime_error("recovery_history_baseline_missing"))?;
                inner.settled_fills = consumed;
                inner.inventory.seed(&baseline.public.positions);
                for fill in &replay {
                    if !update_active_fill(&mut inner, fill)? {
                        return Err(v13_runtime_error("recovery_history_replay_not_new"));
                    }
                    let mut context = crate::v13::callback_context_v13(
                        self.core.context.clock.now_ns(),
                        2,
                        &inner.markets,
                        &inner.positions,
                        &inner.balances,
                        &inner.accounts,
                        &inner.active_orders,
                    );
                    if self.core.pause_safe {
                        context.flags |= V13_PAUSED;
                    }
                    context.fills_ptr = fill;
                    context.fills_len = 1;
                    let NativeV13Inner {
                        instance, staging, ..
                    } = &mut *inner;
                    instance
                        .replay_fill(staging, &mut context)
                        .map_err(|_| v13_runtime_error("recovery_history_callback_rejected"))?;
                    inner.callback_count = inner.callback_count.saturating_add(1);
                }
                inner.pending_fills.clear();
                inner.awaiting_fills.clear();
                if !inner.inventory.confirmed(&inner.positions) {
                    return Err(v13_runtime_error("recovery_history_inventory_unproven"));
                }
            }
            let identity = public_state_identity_v13(
                &inner.accounts,
                &inner.active_orders,
                &inner.positions,
                &inner.balances,
            );
            let reconciled =
                recovery.is_some() && inner.instance.has_callback(V13EventKind::Recover);
            if reconciled {
                NativeV13Runtime::invoke(&mut inner, &self.core, V13EventKind::Recover, 1, |_| {})?;
                inner
                    .instance
                    .complete_reconciled_restore(identity, self.core.context.strategy.generation)
                    .map_err(|_| v13_runtime_error("snapshot_reconcile_failed"))?;
            } else {
                inner
                    .instance
                    .complete_restore(identity, self.core.context.strategy.generation)
                    .map_err(|_| v13_runtime_error("snapshot_reconcile_failed"))?;
            }
            inner.recovered_identity = Some(identity);
            if reconciled {
                // Only the validated, no-pending-execution checkpoint and unchanged venue
                // reconciliation prove these quantities were already consumed. Terminal orders
                // are absent from the new connector's open-order seed, but their replays must
                // not advance the inventory seeded from the recovered account position again.
                for (key, consumed) in recovered_fill_boundaries(&inner.orders_list)? {
                    let previous = inner.settled_fills.entry(key).or_default();
                    *previous = (*previous).max(consumed);
                }
            }
            checkpoint_state(
                &self.core,
                &mut inner,
                self.core.context.clock.now_ns().max(1) as u64,
            )?;
            flush_checkpoint(
                &self.core,
                &inner,
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .map_err(|_| v13_runtime_error("recovery_checkpoint_flush_failed"))
        })();
        if let Err(error) = &result {
            fail_runtime(&self.core, &mut inner, error.reason_code.clone());
        }
        result
    }
    fn fire_timer(&self, timer_id: u64) -> LocalResult<()> {
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let core = self.core.clone();
        lane.submit_safe_point(move || {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if !matches!(
                inner.lifecycle,
                StrategyLifecycle::Running
                    | StrategyLifecycle::Paused
                    | StrategyLifecycle::Stopping
            ) {
                return Ok(());
            }
            if core.host_depth
                && core
                    .lane
                    .get()
                    .is_some_and(|lane| lane.health().state != SubscriberState::Normal)
            {
                fail_runtime(&core, &mut inner, Arc::from("depth_delivery_unhealthy"));
                return Err(EngineError::SafePointPanicked);
            }
            if core.host_depth
                && let Some(service) = &core.context.market_service
            {
                for binding in core.context.markets.iter() {
                    if !service
                        .health(binding.source)
                        .is_ok_and(|h| h.state == titan_market_service::ConnectorHealth::Running)
                    {
                        let book = inner.depth_books.entry(binding.local_asset_no).or_default();
                        if book.ready {
                            let epoch = book.epoch();
                            book.invalidate(epoch);
                            let info = DepthBatchInfoV1 {
                                version: 1,
                                asset_no: binding.local_asset_no,
                                stream_epoch: epoch,
                                reset: 1,
                                ..Default::default()
                            };
                            if NativeV13Runtime::invoke(
                                &mut inner,
                                &core,
                                V13EventKind::Depth,
                                1,
                                |ctx| {
                                    ctx.event_payload_ptr =
                                        (&info as *const DepthBatchInfoV1).cast();
                                    ctx.event_payload_len =
                                        std::mem::size_of::<DepthBatchInfoV1>() as u64;
                                },
                            )
                            .is_err()
                            {
                                return Err(EngineError::SafePointPanicked);
                            }
                        }
                    }
                }
            }
            let timer = TitanTimerView {
                timer_id,
                scheduled_ts_ns: core.context.clock.now_ns(),
                fired_ts_ns: core.context.clock.now_ns(),
            };
            let result =
                NativeV13Runtime::invoke(&mut inner, &core, V13EventKind::Timer, 1, |ctx| {
                    ctx.timer_ptr = &timer;
                    ctx.timer_len = 1;
                })
                .and_then(|_| {
                    if inner.checkpoint_dirty
                        && timer.fired_ts_ns.saturating_sub(inner.last_checkpoint_ns)
                            >= inner.checkpoint_interval_ns
                    {
                        checkpoint_state(&core, &mut inner, timer.fired_ts_ns as u64)
                    } else {
                        Ok(())
                    }
                })
                .and_then(|_| finish_stop_if_drained(&core, &mut inner));
            if let Err(error) = &result {
                fail_runtime(&core, &mut inner, error.reason_code.clone());
            } else {
                publish_business_observation(&core, &inner, false);
            }
            result.map_err(|_| EngineError::SafePointPanicked)
        })
        .map(|_| ())
        .map_err(|_| {
            let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
            fail_runtime(&self.core, &mut inner, Arc::from("timer_queue_full"));
            v13_runtime_error("timer_queue_full")
        })
    }
    fn advance_lifecycle(&self) -> LocalResult<()> {
        if self
            .core
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lifecycle
            != StrategyLifecycle::Stopping
        {
            return Ok(());
        }
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let core = self.core.clone();
        lane.submit_safe_point(move || {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            let _ = finish_stop_if_drained(&core, &mut inner);
            Ok(())
        })
        .map(|_| ())
        .map_err(|_| v13_runtime_error("control_queue_full"))
    }
    fn execution_result(&self, result: ObservedExecutionResult) -> LocalResult<()> {
        if self.core.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let core = self.core.clone();
        let queued = lane.submit_safe_point(move || {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if dispatch_execution_result(&core, &mut inner, result)
                .and_then(|_| finish_stop_if_drained(&core, &mut inner))
                .is_err()
            {
                fail_runtime(
                    &core,
                    &mut inner,
                    Arc::from("execution_result_dispatch_failed"),
                );
                return Err(EngineError::SafePointPanicked);
            }
            publish_business_observation(&core, &inner, false);
            Ok(())
        });
        if queued.is_err() {
            self.core.context.activation.invalidate();
            self.core.async_failure.store(true, Ordering::Release);
            if let Some(publisher) = &self.core.business_observation {
                publisher.invalidate();
            }
            // Local rejection observers may run inside commit_commands while it owns inner.
            if let Ok(mut inner) = self.core.inner.try_lock() {
                fail_runtime(
                    &self.core,
                    &mut inner,
                    Arc::from("execution_result_queue_full"),
                );
            }
            return Err(v13_runtime_error("execution_result_queue_full"));
        }
        Ok(())
    }
    fn prepare(&self) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Defined {
                return Err(v13_runtime_error("invalid_prepare_state"));
            }
            inner.lifecycle = StrategyLifecycle::Ready;
            Ok(())
        })
    }
    fn start(&self) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Ready {
                return Err(v13_runtime_error("invalid_start_state"));
            }
            if inner
                .accounts
                .iter()
                .any(|account| account.state != 4 || account.account_epoch == 0)
            {
                return Err(v13_runtime_error("account_not_ready"));
            }
            if let Some(identity) = inner.recovered_identity
                && public_state_identity_v13(
                    &inner.accounts,
                    &inner.active_orders,
                    &inner.positions,
                    &inner.balances,
                ) != identity
            {
                return Err(v13_runtime_error("recovery_state_changed_before_start"));
            }
            if inner.active_orders.iter().any(|order| {
                order.qty_lots <= 0
                    || order.cumulative_filled_lots < 0
                    || order.cumulative_filled_lots > order.qty_lots
                    || !matches!(order.status, 1 | 2 | 3 | 5)
            }) {
                return Err(v13_runtime_error("active_order_state_invalid"));
            }
            NativeV13Runtime::invoke(&mut inner, core, V13EventKind::Start, 1, |_| {})?;
            if inner.instance.start().is_err() {
                fail_runtime(core, &mut inner, Arc::from("v13_start_failed"));
                return Err(v13_runtime_error("v13_start_failed"));
            }
            if !core.context.activation.open() {
                fail_runtime(core, &mut inner, Arc::from("activation_invalidated"));
                return Err(v13_runtime_error("activation_invalidated"));
            }
            inner.lifecycle = StrategyLifecycle::Running;
            publish_business_observation(core, &inner, true);
            Ok(())
        })
    }
    fn pause(&self, _reason: PauseReason) -> LocalResult<StrategyOperationId> {
        if !self.core.pause_safe {
            return Err(v13_runtime_error("pause_capability_unavailable"));
        }
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle == StrategyLifecycle::Paused {
                return Ok(());
            }
            if inner.lifecycle != StrategyLifecycle::Running {
                return Err(v13_runtime_error("invalid_pause_state"));
            }
            core.context.activation.close();
            inner.lifecycle = StrategyLifecycle::Paused;
            Ok(())
        })
    }
    fn resume(&self) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            if core
                .lane
                .get()
                .is_some_and(|lane| lane.health().state != SubscriberState::Normal)
            {
                return Err(v13_runtime_error("subscriber_not_normal"));
            }
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Paused {
                return Err(v13_runtime_error("invalid_resume_state"));
            }
            if inner
                .accounts
                .iter()
                .any(|account| account.state != 4 || account.account_epoch == 0)
            {
                return Err(v13_runtime_error("account_not_ready"));
            }
            if !core.context.activation.open() {
                fail_runtime(core, &mut inner, Arc::from("activation_invalidated"));
                return Err(v13_runtime_error("activation_invalidated"));
            }
            inner.lifecycle = StrategyLifecycle::Running;
            Ok(())
        })
    }
    fn invalidate(&self, reason: Arc<str>) -> LocalResult<StrategyOperationId> {
        self.core.context.activation.invalidate();
        // Invalidation must not wait behind the callback lock or depend on control
        // queue admission, just as closing the activation gate does not.
        if let Some(publisher) = &self.core.business_observation {
            publisher.invalidate();
        }
        self.schedule(move |core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            // Closing again is idempotent and preserves the terminal gate state
            // established before control queue admission.
            core.context.activation.close();
            if inner.lifecycle != StrategyLifecycle::Failed {
                inner.lifecycle = StrategyLifecycle::Invalidated;
            }
            inner.last_error.get_or_insert_with(|| reason.clone());
            fail_pending_stop(core, &mut inner, reason);
            Ok(())
        })
    }
    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId> {
        if matches!(
            self.state().lifecycle,
            StrategyLifecycle::Failed | StrategyLifecycle::Invalidated
        ) {
            return Err(v13_runtime_error("invalid_stop_state"));
        }
        let mut stop_request = self
            .core
            .stop_request
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(id) = *stop_request {
            return Ok(id);
        }
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| v13_runtime_error("lane_not_attached"))?;
        let id = StrategyOperationId(self.core.next_operation.fetch_add(1, Ordering::AcqRel));
        self.core
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                StrategyOperationSnapshot {
                    id,
                    strategy: Some(self.core.context.strategy),
                    state: StrategyOperationState::Pending,
                    detail: Arc::from("draining"),
                },
            );
        let core = self.core.clone();
        let admission = lane.submit_safe_point(move || {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            let result = (|| {
                if inner.stop_operation.is_some() {
                    return Err(v13_runtime_error("stop_already_pending"));
                }
                if matches!(
                    inner.lifecycle,
                    StrategyLifecycle::Defined
                        | StrategyLifecycle::Ready
                        | StrategyLifecycle::Stopped
                ) {
                    core.context.activation.close();
                    inner.instance.stop();
                    inner.lifecycle = StrategyLifecycle::Stopped;
                    core.stopped.store(true, Ordering::Release);
                    return Ok(());
                }
                if !matches!(
                    inner.lifecycle,
                    StrategyLifecycle::Running | StrategyLifecycle::Paused
                ) {
                    return Err(v13_runtime_error("invalid_stop_state"));
                }
                inner.lifecycle = StrategyLifecycle::Stopping;
                inner.stop_ready = false;
                inner.stop_operation = Some(id);
                inner.stop_deadline = Some(deadline);
                if !core.context.activation.open() {
                    fail_runtime(&core, &mut inner, Arc::from("activation_invalidated"));
                    return Err(v13_runtime_error("activation_invalidated"));
                }
                if !inner.stop_called {
                    NativeV13Runtime::invoke(&mut inner, &core, V13EventKind::Stop, 1, |_| {})?;
                    inner.stop_called = true;
                }
                checkpoint_state(&core, &mut inner, core.context.clock.now_ns().max(1) as u64)?;
                finish_stop_if_drained(&core, &mut inner)
            })();
            if let Err(error) = result {
                core.context.activation.close();
                inner.lifecycle = StrategyLifecycle::Failed;
                inner.last_error = Some(error.reason_code.clone());
                if let Some(publisher) = &core.business_observation {
                    publisher.invalidate();
                }
                let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
                let operation = operations.get_mut(&id).expect("operation exists");
                operation.state = StrategyOperationState::Failed;
                operation.detail = error.reason_code;
                return Err(EngineError::SafePointPanicked);
            }
            publish_business_observation(&core, &inner, true);
            if inner.lifecycle == StrategyLifecycle::Stopped {
                let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
                let operation = operations.get_mut(&id).expect("operation exists");
                operation.state = StrategyOperationState::Succeeded;
                operation.detail = Arc::from("stopped");
            }
            Ok(())
        });
        if admission.is_err() {
            let mut operations = self
                .core
                .operations
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let operation = operations.get_mut(&id).expect("stop operation exists");
            operation.state = StrategyOperationState::Failed;
            operation.detail = Arc::from("control_queue_full");
            return Err(v13_runtime_error("control_queue_full"));
        }
        *stop_request = Some(id);
        Ok(id)
    }
    fn freeze_state(
        &self,
        request: StrategyStateSnapshotRequest,
    ) -> LocalResult<StrategyOperationId> {
        self.schedule(move |core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            let result = checkpoint_state(core, &mut inner, request.checkpoint_id).and_then(|_| {
                flush_checkpoint(
                    core,
                    &inner,
                    Instant::now() + std::time::Duration::from_secs(5),
                )
            });
            if let Err(error) = &result {
                fail_runtime(core, &mut inner, error.reason_code.clone());
            }
            result
        })
    }
    fn state(&self) -> StrategyRuntimeStateSnapshot {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        StrategyRuntimeStateSnapshot {
            handle: self.core.context.strategy,
            lifecycle: inner.lifecycle,
            command_gate_open: inner.instance.command_gate_open(),
            activation_gate_open: self.core.context.activation.is_open(),
        }
    }
    fn health(&self) -> StrategyRuntimeHealthSnapshot {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        StrategyRuntimeHealthSnapshot {
            lifecycle: inner.lifecycle,
            healthy: !matches!(
                inner.lifecycle,
                StrategyLifecycle::Failed | StrategyLifecycle::Invalidated
            ),
            degraded_reason: inner.last_error.clone(),
            heartbeat_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        StrategyRuntimeDiagnosticSnapshot {
            summary: Arc::from("native V13 strategy runtime"),
            callback_count: inner.callback_count,
            command_count: inner.command_count,
            last_error_code: inner.last_error.clone(),
            lane_progress: self
                .core
                .lane
                .get()
                .map_or(LaneProgress::default(), |lane| lane.progress()),
            flight_records: inner
                .flight_records
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .into(),
        }
    }
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot {
        self.core
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned()
            .unwrap_or(StrategyOperationSnapshot {
                id,
                strategy: Some(self.core.context.strategy),
                state: StrategyOperationState::Failed,
                detail: Arc::from("unknown_operation"),
            })
    }
}

fn v13_runtime_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::InvalidState,
        "runtime_v13",
        code,
        "V13 runtime operation failed",
    )
}
fn v13_core_error() -> CoreError {
    CoreError::new(
        titan_core_types::ErrorKind::ComponentFailed,
        titan_core_types::ComponentIdentity::new("titan.strategy", "runtime-v13"),
        titan_core_types::ComponentState::Running,
        "v13_callback",
        "V13 strategy callback failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovered_terminal_fill_replay_keeps_inventory_and_consumed_boundary() {
        let record = StrategyOrderRecordV13 {
            order_id: 7,
            account_no: 0,
            qty_lots: 1,
            cumulative_filled_lots: 1,
            fill_count: 1,
            side: 1,
            status: 4,
            ..Default::default()
        };
        let boundaries = recovered_fill_boundaries(&BTreeMap::from([(7, record)])).unwrap();
        let held = TitanPositionView {
            qty_lots: 1,
            ..Default::default()
        };
        let mut inventory = InventoryProof::default();
        inventory.seed(&[held]);
        inventory
            .fill(
                &TitanFillView {
                    order_id: 7,
                    side: 1,
                    fill_qty_lots: 1,
                    cumulative_filled_lots: 1,
                    ..Default::default()
                },
                boundaries[&(0, 7)],
                0,
            )
            .unwrap();
        assert!(inventory.confirmed(&[held]));
        inventory
            .fill(
                &TitanFillView {
                    order_id: 8,
                    side: 2,
                    fill_qty_lots: 1,
                    cumulative_filled_lots: 1,
                    ..Default::default()
                },
                0,
                0,
            )
            .unwrap();
        assert!(inventory.confirmed(&[TitanPositionView::default()]));
        for cumulative in [-1, 2] {
            assert!(
                recovered_fill_boundaries(&BTreeMap::from([(
                    7,
                    StrategyOrderRecordV13 {
                        cumulative_filled_lots: cumulative,
                        ..record
                    },
                )]))
                .is_err()
            );
        }
    }

    #[test]
    fn inventory_confirmation_handles_fill_position_orderings_and_hedge_legs() {
        let flat = TitanPositionView::default();
        let filled = TitanPositionView {
            qty_lots: 10,
            ..flat
        };
        let fill = TitanFillView {
            side: 1,
            fill_qty_lots: 10,
            cumulative_filled_lots: 10,
            ..Default::default()
        };
        for position_first in [false, true] {
            let mut proof = InventoryProof::default();
            proof.seed(&[flat]);
            if position_first {
                assert!(!proof.confirmed(&[filled]));
            }
            proof.fill(&fill, 0, 0).unwrap();
            assert!(!proof.confirmed(&[flat]));
            assert!(proof.confirmed(&[filled]));
            proof.fill(&fill, 10, 0).unwrap();
            assert!(proof.confirmed(&[filled]));
            proof
                .fill(
                    &TitanFillView {
                        side: 2,
                        cumulative_filled_lots: 3,
                        ..fill
                    },
                    0,
                    0,
                )
                .unwrap();
            assert!(!proof.confirmed(&[filled]));
            assert!(proof.confirmed(&[TitanPositionView {
                qty_lots: 7,
                ..flat
            }]));
        }
        let mut proof = InventoryProof::default();
        proof.seed(&[]);
        proof.fill(&fill, 0, 1).unwrap();
        proof
            .fill(&TitanFillView { side: 2, ..fill }, 0, 2)
            .unwrap();
        assert!(!proof.confirmed(&[flat]));
        assert!(proof.confirmed(&[
            TitanPositionView {
                qty_lots: 10,
                position_side: 1,
                ..flat
            },
            TitanPositionView {
                qty_lots: -10,
                position_side: 2,
                ..flat
            }
        ]));
    }

    #[test]
    fn filled_risk_stays_reserved_until_positions_confirm_and_protective_exits_remain_allowed() {
        let flat = TitanPositionView::default();
        let position = TitanPositionView {
            qty_lots: 10,
            ..flat
        };
        let opening = StagedCommandV13::Submit {
            order_id: 2,
            request: TitanSubmitOrderRequest {
                qty_lots: 1,
                price_ticks: 1000,
                side: 1,
                order_type: 1,
                ..Default::default()
            },
        };
        let protective = StagedCommandV13::Submit {
            order_id: 3,
            request: TitanSubmitOrderRequest {
                qty_lots: 10,
                price_ticks: 1000,
                side: 2,
                order_type: 1,
                reduce_only: 1,
                ..Default::default()
            },
        };
        let mut inventory = InventoryProof::default();
        inventory.seed(&[flat]);
        // A newer unrelated fact or the removal of a terminal order cannot confirm inventory.
        assert!(
            inventory
                .validate_opening(&[position], false, &[opening])
                .is_err()
        );
        inventory
            .fill(
                &TitanFillView {
                    side: 1,
                    cumulative_filled_lots: 10,
                    fill_qty_lots: 10,
                    ..Default::default()
                },
                0,
                0,
            )
            .unwrap();
        assert!(
            inventory
                .validate_opening(&[flat], false, &[opening])
                .is_err()
        );
        assert!(
            inventory
                .validate_opening(&[flat], false, &[protective])
                .is_ok()
        );
        assert!(
            inventory
                .validate_opening(&[position], true, &[opening])
                .is_err()
        );
        assert!(
            inventory
                .validate_opening(&[position], false, &[opening])
                .is_ok()
        );
        let limits = ExecutionLimits {
            max_notional_tick_lots: "10000".into(),
            max_price_deviation_bps: 50,
            max_market_age_ns: 100,
        };
        assert!(
            validate_execution_envelope(
                &limits,
                TitanMarketView {
                    best_bid_ticks: 999,
                    best_ask_ticks: 1000,
                    ..Default::default()
                },
                &[position],
                &[],
                100,
                &[opening],
                150
            )
            .is_err()
        );
    }

    #[test]
    fn execution_envelope_bounds_batch_price_staleness_hedges_and_reductions() {
        let limits = ExecutionLimits {
            max_notional_tick_lots: "10000".into(),
            max_price_deviation_bps: 50,
            max_market_age_ns: 100,
        };
        let market = TitanMarketView {
            best_bid_ticks: 999,
            best_ask_ticks: 1000,
            ..Default::default()
        };
        let submit = |qty, price, reduce| StagedCommandV13::Submit {
            order_id: 1,
            request: TitanSubmitOrderRequest {
                qty_lots: qty,
                price_ticks: price,
                side: 1,
                order_type: 1,
                reduce_only: reduce,
                ..Default::default()
            },
        };
        let check = |positions: &[TitanPositionView],
                     orders: &[TitanActiveOrderView],
                     commands: &[StagedCommandV13],
                     now| {
            validate_execution_envelope(&limits, market, positions, orders, 100, commands, now)
        };
        assert!(check(&[], &[], &[submit(10, 1000, 0)], 150).is_ok());
        assert!(check(&[], &[], &[submit(6, 1000, 0), submit(5, 1000, 0)], 150).is_err());
        assert!(check(&[], &[], &[submit(1, 1006, 0)], 150).is_err());
        assert!(check(&[], &[], &[submit(1, 1000, 0)], 201).is_err());
        assert!(check(&[], &[], &[submit(1, 1000, 0)], 99).is_err());
        let hedge = [
            TitanPositionView {
                qty_lots: 6,
                position_side: 1,
                ..Default::default()
            },
            TitanPositionView {
                qty_lots: -6,
                position_side: 2,
                ..Default::default()
            },
        ];
        assert!(check(&hedge, &[], &[submit(1, 1000, 0)], 150).is_err());
        assert!(check(&hedge, &[], &[submit(6, 1000, 1)], 150).is_ok());
        let pending = [TitanActiveOrderView {
            qty_lots: 8,
            cumulative_filled_lots: 3,
            ..Default::default()
        }];
        assert!(check(&[], &pending, &[submit(6, 1000, 0)], 150).is_err());
        assert!(check(&[], &pending, &[submit(5, 1000, 0)], 150).is_ok());
        let higher_limit = [TitanActiveOrderView {
            qty_lots: 5,
            price_ticks: 1005,
            ..Default::default()
        }];
        assert!(check(&[], &higher_limit, &[submit(5, 1000, 0)], 150).is_err());
        let cancel = StagedCommandV13::Cancel {
            command_id: 1,
            request: TitanCancelOrderRequest::default(),
        };
        assert!(check(&[], &[], &[cancel], 1000).is_ok());
    }

    #[test]
    fn account_enums_are_explicitly_translated_to_v13() {
        assert_eq!(account_order_type_to_v13(0).unwrap(), 1);
        assert_eq!(account_order_type_to_v13(1).unwrap(), 2);
        assert_eq!(account_tif_to_v13(0).unwrap(), 1);
        assert_eq!(account_tif_to_v13(1).unwrap(), 4);
        assert_eq!(account_tif_to_v13(2).unwrap(), 3);
        assert_eq!(account_tif_to_v13(3).unwrap(), 2);
        assert_eq!(v13_tif_to_account(1).unwrap(), 0);
        assert_eq!(v13_tif_to_account(2).unwrap(), 3);
        assert_eq!(v13_tif_to_account(3).unwrap(), 2);
        assert_eq!(v13_tif_to_account(4).unwrap(), 1);
        assert_eq!(account_order_status_to_v13(1).unwrap(), 2);
        assert_eq!(account_order_status_to_v13(2).unwrap(), 8);
        assert_eq!(account_order_status_to_v13(3).unwrap(), 4);
        assert_eq!(account_order_status_to_v13(4).unwrap(), 6);
        assert_eq!(account_order_status_to_v13(5).unwrap(), 3);
        assert_eq!(account_order_status_to_v13(6).unwrap(), 7);
    }

    #[test]
    fn client_order_ownership_is_stable_across_generations() {
        let first = StrategyHandle {
            runtime_strategy_no: RuntimeStrategyNo(42),
            generation: 1,
        };
        let second = StrategyHandle {
            runtime_strategy_no: RuntimeStrategyNo(42),
            generation: 2,
        };
        let encoded = client_order_id(first, 7);
        assert_eq!(v13_strategy_order_id(second, encoded), 7);
    }

    #[test]
    fn orders_list_checksum_detects_tampering() {
        let mut orders = [StrategyOrderRecordV13 {
            order_id: 7,
            account_no: 1,
            asset_no: 2,
            qty_lots: 3,
            cumulative_filled_lots: 1,
            ..StrategyOrderRecordV13::default()
        }];
        let checksum = orders_list_checksum(&orders);
        orders[0].cumulative_filled_lots = 2;
        assert_ne!(orders_list_checksum(&orders), checksum);
    }

    #[test]
    fn newer_public_seed_advances_existing_order_history() {
        let mut orders = BTreeMap::from([(
            7,
            StrategyOrderRecordV13 {
                order_id: 7,
                account_no: 1,
                asset_no: 2,
                cumulative_filled_lots: 1,
                account_sequence: 10,
                status: 3,
                ..StrategyOrderRecordV13::default()
            },
        )]);
        merge_seeded_order_record(
            &mut orders,
            TitanActiveOrderView {
                order_id: 7,
                account_no: 1,
                asset_no: 2,
                qty_lots: 5,
                cumulative_filled_lots: 2,
                account_sequence: 11,
                status: 3,
                ..TitanActiveOrderView::default()
            },
        )
        .unwrap();
        assert_eq!(orders[&7].cumulative_filled_lots, 2);
        assert_eq!(orders[&7].account_sequence, 11);
    }
}
