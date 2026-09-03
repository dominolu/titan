use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Instant, SystemTime},
};

use titan_account_plugin::{FillV2, OrderChangedV1};
use titan_event_engine::{EngineError, LaneProgress, PrimaryAsyncLaneHandle, SubscriberState};
use titan_plugin_engine::{EventHandler, EventView, PluginError, ResourceScopeHandle};
use titan_runtime::{CallbackRegistry, StrategyEventKind, StrategyRuntimeContext};
use titan_runtime_abi::{BarItem, Event, FillEvent, OrderEvent, TickItem};

use crate::*;

#[derive(Default)]
pub struct StrategyActivationGate(AtomicBool);

impl StrategyActivationGate {
    pub fn open(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct StrategyCommandGate {
    owner: StrategyHandle,
    open: AtomicBool,
}

impl StrategyCommandGate {
    pub fn new(owner: StrategyHandle) -> Self {
        Self {
            owner,
            open: AtomicBool::new(false),
        }
    }
    pub fn owner(&self) -> StrategyHandle {
        self.owner
    }
    pub fn open(&self) {
        self.open.store(true, Ordering::Release);
    }
    pub fn close(&self) {
        self.open.store(false, Ordering::Release);
    }
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }
}

pub trait StrategyClock: Send + Sync {
    fn now_ns(&self) -> i64;
}

pub trait StrategyMetrics: Send + Sync {
    fn callback_duration(&self, _kind: StrategyEventKind, _duration_ns: u64) {}
}

pub trait StrategyStateSnapshotSink: Send + Sync {
    fn submit(&self, snapshot: StrategyPrivateStateSnapshot) -> LocalResult<()>;
}

#[derive(Clone, Debug)]
pub struct StrategyPrivateStateSnapshot {
    pub checkpoint_id: u64,
    pub strategy: StrategyHandle,
    pub state_f64: Arc<[f64]>,
    pub state_i64: Arc<[i64]>,
}

pub struct StrategyRuntimeBuildContext {
    pub strategy: StrategyHandle,
    pub artifact_id: StrategyArtifactId,
    pub markets: Arc<[ResolvedMarketBinding]>,
    pub accounts: Arc<[ResolvedAccountBinding]>,
    pub event_adapter: Arc<dyn StrategyEventAdapter>,
    pub command_gateway: Arc<dyn StrategyCommandGateway>,
    pub state_snapshot_sink: Arc<dyn StrategyStateSnapshotSink>,
    pub clock: Arc<dyn StrategyClock>,
    pub metrics: Arc<dyn StrategyMetrics>,
    pub resources: ResourceScopeHandle,
    pub activation: Arc<StrategyActivationGate>,
    pub command_gate: Arc<StrategyCommandGate>,
}

pub trait StrategyRuntimeFactory: Send + Sync {
    fn strategy_type(&self) -> &str;
    fn create(
        &self,
        definition: &StrategyDefinition,
        artifact: StrategyArtifact,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError>;
}

pub trait StrategyRuntime: EventHandler + Send + Sync {
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()>;
    fn prepare(&self) -> LocalResult<StrategyOperationId>;
    fn start(&self) -> LocalResult<StrategyOperationId>;
    fn pause(&self, reason: PauseReason) -> LocalResult<StrategyOperationId>;
    fn resume(&self) -> LocalResult<StrategyOperationId>;
    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId>;
    fn freeze_state(
        &self,
        request: StrategyStateSnapshotRequest,
    ) -> LocalResult<StrategyOperationId>;
    fn state(&self) -> StrategyRuntimeStateSnapshot;
    fn health(&self) -> StrategyRuntimeHealthSnapshot;
    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot;
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot;
}

#[derive(Default)]
pub struct StrategyRuntimeFactoryRegistry {
    factories: std::sync::RwLock<BTreeMap<Arc<str>, Arc<dyn StrategyRuntimeFactory>>>,
}

impl StrategyRuntimeFactoryRegistry {
    pub fn register(&self, factory: Arc<dyn StrategyRuntimeFactory>) -> LocalResult<()> {
        let key: Arc<str> = Arc::from(factory.strategy_type());
        let mut factories = self.factories.write().unwrap_or_else(|p| p.into_inner());
        if factories.insert(key, factory).is_some() {
            return Err(StrategyError::new(
                StrategyErrorKind::AlreadyExists,
                "register_runtime_factory",
                "strategy_type_conflict",
                "strategy runtime type is already registered",
            ));
        }
        Ok(())
    }

    pub fn get(&self, strategy_type: &str) -> LocalResult<Arc<dyn StrategyRuntimeFactory>> {
        self.factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(strategy_type)
            .cloned()
            .ok_or_else(|| {
                StrategyError::new(
                    StrategyErrorKind::LoadFailed,
                    "create_runtime",
                    "runtime_factory_not_registered",
                    "strategy runtime factory is not registered",
                )
            })
    }
}

pub trait StrategyEventAdapter: Send + Sync {
    fn invoke(
        &self,
        event: EventView<'_>,
        callbacks: &CallbackRegistry,
        context: &mut StrategyRuntimeContext,
    ) -> LocalResult<StrategyEventKind>;
}

/// Mechanical canonical-event to ABI adapter. It neither deduplicates nor infers fills.
pub struct CanonicalStrategyEventAdapter {
    local_assets: BTreeMap<u32, u64>,
    market_scratch: Mutex<Vec<TickItem>>,
}

impl Default for CanonicalStrategyEventAdapter {
    fn default() -> Self {
        Self {
            local_assets: BTreeMap::new(),
            market_scratch: Mutex::new(Vec::with_capacity(1_024)),
        }
    }
}

impl CanonicalStrategyEventAdapter {
    pub fn new(markets: &[ResolvedMarketBinding]) -> Self {
        Self {
            local_assets: markets
                .iter()
                .map(|binding| (binding.asset_id, u64::from(binding.local_asset_no)))
                .collect(),
            market_scratch: Mutex::new(Vec::with_capacity(1_024)),
        }
    }
}

impl StrategyEventAdapter for CanonicalStrategyEventAdapter {
    fn invoke(
        &self,
        event: EventView<'_>,
        callbacks: &CallbackRegistry,
        context: &mut StrategyRuntimeContext,
    ) -> LocalResult<StrategyEventKind> {
        let kind = match (event.event_type, event.schema_version) {
            (
                titan_market_plugin::DEPTH_BATCH_EVENT
                | titan_market_plugin::TRADE_BATCH_EVENT
                | titan_market_plugin::BBO_EVENT,
                1,
            ) => {
                if event.payload.len() < titan_market_plugin::MarketBatchHeaderV1::ENCODED_LEN {
                    return Err(adapter_error("market_batch_header"));
                }
                let asset_id = u32::from_le_bytes(event.payload[0..4].try_into().unwrap());
                let item_count =
                    usize::from(u16::from_le_bytes(event.payload[8..10].try_into().unwrap()));
                let local_asset = *self
                    .local_assets
                    .get(&asset_id)
                    .ok_or_else(|| adapter_error("market_asset_not_bound"))?;
                let expected = titan_market_plugin::MarketBatchHeaderV1::ENCODED_LEN
                    .checked_add(
                        item_count
                            .checked_mul(titan_market_plugin::DepthItemV1::ENCODED_LEN)
                            .ok_or_else(|| adapter_error("market_batch_overflow"))?,
                    )
                    .ok_or_else(|| adapter_error("market_batch_overflow"))?;
                if event.payload.len() != expected {
                    return Err(adapter_error("market_batch_length"));
                }
                let exchange_ts = i64::from_le_bytes(event.payload[36..44].try_into().unwrap());
                let receive_ts = i64::from_le_bytes(event.payload[44..52].try_into().unwrap());
                let event_code = if event.event_type == titan_market_plugin::TRADE_BATCH_EVENT {
                    3
                } else if event.event_type == titan_market_plugin::BBO_EVENT {
                    4
                } else {
                    2
                };
                let mut scratch = self
                    .market_scratch
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if item_count > scratch.capacity() {
                    return Err(adapter_error("market_batch_capacity"));
                }
                scratch.clear();
                for index in 0..item_count {
                    let offset = titan_market_plugin::MarketBatchHeaderV1::ENCODED_LEN
                        + index * titan_market_plugin::DepthItemV1::ENCODED_LEN;
                    let price =
                        i64::from_le_bytes(event.payload[offset..offset + 8].try_into().unwrap())
                            as f64;
                    let quantity = i64::from_le_bytes(
                        event.payload[offset + 8..offset + 16].try_into().unwrap(),
                    ) as f64;
                    let side = u64::from(event.payload[offset + 16]);
                    scratch.push(TickItem {
                        asset_no: local_asset,
                        event: Event {
                            ev: event_code | (side << 8),
                            exch_ts: exchange_ts,
                            local_ts: receive_ts,
                            px: price,
                            qty: quantity,
                            order_id: 0,
                            ival: 0,
                            fval: 0.0,
                        },
                    });
                }
                context.ticks_ptr = scratch.as_ptr();
                context.num_ticks = scratch.len();
                callbacks
                    .invoke(StrategyEventKind::Tick, context)
                    .map_err(|_| adapter_error("tick_callback"))?;
                StrategyEventKind::Tick
            }
            (
                titan_market_plugin::BAR_BATCH_EVENT,
                titan_market_plugin::MARKET_EVENT_SCHEMA_VERSION,
            ) => {
                let batch = titan_market_plugin::BarBatchV1::decode(event.payload)
                    .map_err(|_| adapter_error("bar_batch_v1"))?;
                let bars = batch
                    .items
                    .into_iter()
                    .map(|item| {
                        self.local_assets
                            .get(&item.asset_id)
                            .copied()
                            .map(|asset_no| BarItem {
                                asset_no,
                                bar: item.bar,
                            })
                            .ok_or_else(|| adapter_error("bar_asset_not_bound"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                context.bars_ptr = bars.as_ptr();
                context.num_bars = bars.len();
                context.bar_timeframe_ns = batch.timeframe_ns;
                context.bar_close_ts = batch.close_ts;
                callbacks
                    .invoke(StrategyEventKind::Bar, context)
                    .map_err(|_| adapter_error("bar_callback"))?;
                StrategyEventKind::Bar
            }
            (titan_account_plugin::FILL_EVENT, titan_account_plugin::FILL_EVENT_SCHEMA_VERSION) => {
                let fill = FillV2::decode(event.payload).map_err(|_| adapter_error("fill_v2"))?;
                let asset_no = self
                    .local_assets
                    .get(&fill.asset_id)
                    .copied()
                    .ok_or_else(|| adapter_error("fill_asset_not_bound"))?;
                let view = FillEvent {
                    asset_no,
                    order_id: u64::from_le_bytes(fill.client_order_id.0[..8].try_into().unwrap()),
                    venue_order_id: u64::from_le_bytes(
                        fill.venue_order_id.0[..8].try_into().unwrap(),
                    ),
                    exch_ts: fill.header.exchange_ts,
                    local_ts: fill.header.receive_ts,
                    sequence: fill.header.account_version,
                    price: fill.price_ticks as f64,
                    last_fill_qty: fill.last_fill_quantity_lots as f64,
                    cumulative_filled_qty: fill.cumulative_filled_quantity_lots as f64,
                    venue_no: 0,
                    instrument_id: fill.asset_id,
                    reason: 0,
                    side: fill.side as i8,
                    maker: u8::from(fill.liquidity == 1),
                    _reserved: [0; 2],
                };
                context.fills_ptr = &view;
                context.num_fills = 1;
                callbacks
                    .invoke(StrategyEventKind::Filled, context)
                    .map_err(|_| adapter_error("filled_callback"))?;
                StrategyEventKind::Filled
            }
            (titan_account_plugin::ORDER_CHANGED_EVENT, 1) => {
                let order =
                    OrderChangedV1::decode(event.payload).map_err(|_| adapter_error("order_v1"))?;
                let asset_no = self
                    .local_assets
                    .get(&order.asset_id)
                    .copied()
                    .ok_or_else(|| adapter_error("order_asset_not_bound"))?;
                let view = OrderEvent {
                    asset_no,
                    order_id: u64::from_le_bytes(order.client_order_id.0[..8].try_into().unwrap()),
                    venue_order_id: u64::from_le_bytes(
                        order.venue_order_id.0[..8].try_into().unwrap(),
                    ),
                    exch_ts: order.header.exchange_ts,
                    local_ts: order.header.receive_ts,
                    sequence: order.header.account_version,
                    price: order.price_ticks as f64,
                    qty: order.quantity_lots as f64,
                    exec_price: order.average_price_ticks as f64,
                    exec_qty: order.filled_quantity_lots as f64,
                    venue_no: 0,
                    instrument_id: order.asset_id,
                    reason: 0,
                    side: order.side as i8,
                    status: order.status,
                    request: 0,
                    maker: 0,
                    _reserved: [0; 4],
                };
                context.orders_ptr = &view;
                context.num_orders = 1;
                callbacks
                    .invoke(StrategyEventKind::Order, context)
                    .map_err(|_| adapter_error("order_callback"))?;
                StrategyEventKind::Order
            }
            _ => {
                context.payload_ptr = event.payload.as_ptr().cast();
                context.payload_len = event.payload.len();
                let kind = if event.event_type.contains("Bar") {
                    StrategyEventKind::Bar
                } else if event.event_type.contains("Timer") {
                    StrategyEventKind::Timer
                } else if event.event_type.contains("Funding") {
                    StrategyEventKind::Funding
                } else if event.event_type.contains("Position") {
                    StrategyEventKind::Position
                } else {
                    StrategyEventKind::Tick
                };
                callbacks
                    .invoke(kind, context)
                    .map_err(|_| adapter_error("callback"))?;
                kind
            }
        };
        context.clear_views();
        Ok(kind)
    }
}

fn adapter_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::CallbackFailed,
        "event_adapter",
        code,
        "canonical event could not be dispatched to the strategy callback",
    )
}

struct NativeRuntimeInner {
    lifecycle: StrategyLifecycle,
    artifact: StrategyArtifact,
    commands: Vec<titan_runtime_abi::OrderCommand>,
    callback_count: u64,
    command_count: u64,
    budget_violations: u64,
    consecutive_budget_violations: u32,
    last_error: Option<Arc<str>>,
    stop_called: bool,
    flight_records: VecDeque<StrategyFlightRecord>,
    next_flight_sequence: u64,
}

struct NativeRuntimeCore {
    definition: StrategyDefinition,
    context: StrategyRuntimeBuildContext,
    inner: Mutex<NativeRuntimeInner>,
    lane: OnceLock<PrimaryAsyncLaneHandle>,
    next_operation: AtomicU64,
    operations: Mutex<BTreeMap<StrategyOperationId, StrategyOperationSnapshot>>,
}

pub struct NativeStrategyRuntime {
    core: Arc<NativeRuntimeCore>,
}

pub struct NativeStrategyRuntimeFactory {
    strategy_type: Arc<str>,
}

impl NativeStrategyRuntimeFactory {
    pub fn new(strategy_type: impl Into<Arc<str>>) -> Self {
        Self {
            strategy_type: strategy_type.into(),
        }
    }
}

impl StrategyRuntimeFactory for NativeStrategyRuntimeFactory {
    fn strategy_type(&self) -> &str {
        &self.strategy_type
    }

    fn create(
        &self,
        definition: &StrategyDefinition,
        mut artifact: StrategyArtifact,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError> {
        artifact
            .state
            .f64_values
            .resize(definition.runtime.state_f64_capacity, 0.0);
        artifact
            .state
            .i64_values
            .resize(definition.runtime.state_i64_capacity, 0);
        Ok(Arc::new(NativeStrategyRuntime {
            core: Arc::new(NativeRuntimeCore {
                definition: definition.clone(),
                context,
                inner: Mutex::new(NativeRuntimeInner {
                    lifecycle: StrategyLifecycle::Defined,
                    artifact,
                    commands: vec![
                        titan_runtime_abi::OrderCommand::default();
                        definition.runtime.command_capacity
                    ],
                    callback_count: 0,
                    command_count: 0,
                    budget_violations: 0,
                    consecutive_budget_violations: 0,
                    last_error: None,
                    stop_called: false,
                    flight_records: VecDeque::with_capacity(128),
                    next_flight_sequence: 1,
                }),
                lane: OnceLock::new(),
                next_operation: AtomicU64::new(1),
                operations: Mutex::new(BTreeMap::new()),
            }),
        }))
    }
}

impl NativeStrategyRuntime {
    fn schedule(
        &self,
        action: impl FnOnce(&Arc<NativeRuntimeCore>) -> LocalResult<()> + Send + 'static,
    ) -> LocalResult<StrategyOperationId> {
        let lane = self
            .core
            .lane
            .get()
            .ok_or_else(|| runtime_error("lane_not_attached"))?;
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
        lane.submit_safe_point(move || {
            let result = action(&core);
            let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
            let snapshot = operations.get_mut(&id).expect("operation was inserted");
            if snapshot.state != StrategyOperationState::Pending {
                return result.map_err(|_| EngineError::SafePointPanicked);
            }
            match result {
                Ok(()) => {
                    snapshot.state = StrategyOperationState::Succeeded;
                    snapshot.detail = Arc::from("succeeded");
                    Ok(())
                }
                Err(error) => {
                    snapshot.state = StrategyOperationState::Failed;
                    snapshot.detail = error.reason_code.clone();
                    Err(EngineError::SafePointPanicked)
                }
            }
        })
        .map_err(|_| runtime_error("control_queue_full"))?;
        Ok(id)
    }

    fn schedule_before(
        &self,
        deadline: Instant,
        action: impl FnOnce(&Arc<NativeRuntimeCore>) -> LocalResult<()> + Send + 'static,
    ) -> LocalResult<StrategyOperationId> {
        let id = self.schedule(action)?;
        let core = self.core.clone();
        std::thread::Builder::new()
            .name(format!("strategy-operation-deadline-{}", id.0))
            .spawn(move || {
                std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                let mut operations = core.operations.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(snapshot) = operations.get_mut(&id)
                    && snapshot.state == StrategyOperationState::Pending
                {
                    snapshot.state = StrategyOperationState::Failed;
                    snapshot.detail = Arc::from("operation_deadline");
                }
            })
            .map_err(|_| runtime_error("deadline_watchdog_start_failed"))?;
        Ok(id)
    }
}

impl EventHandler for NativeStrategyRuntime {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        let event_trace = event.trace;
        let mut inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(
            inner.lifecycle,
            StrategyLifecycle::Failed | StrategyLifecycle::Stopped
        ) {
            return Ok(());
        }
        if !self.core.context.activation.is_open() {
            // Account/risk facts may still update framework state while paused. The current
            // adapter is stateless, so it deliberately suppresses user callbacks here.
            return Ok(());
        }
        let started = Instant::now();
        let NativeRuntimeInner {
            artifact,
            commands,
            callback_count,
            command_count,
            budget_violations,
            consecutive_budget_violations,
            last_error,
            lifecycle,
            stop_called,
            flight_records,
            next_flight_sequence,
        } = &mut *inner;
        commands.fill(titan_runtime_abi::OrderCommand::default());
        let mut context = StrategyRuntimeContext {
            generation: self.core.context.strategy.generation,
            now: self.core.context.clock.now_ns(),
            state_f64_ptr: artifact.state.f64_values.as_mut_ptr(),
            state_f64_len: artifact.state.f64_values.len(),
            state_i64_ptr: artifact.state.i64_values.as_mut_ptr(),
            state_i64_len: artifact.state.i64_values.len(),
            commands_ptr: commands.as_mut_ptr(),
            command_capacity: commands.len(),
            ..StrategyRuntimeContext::default()
        };
        let result =
            self.core
                .context
                .event_adapter
                .invoke(event, &artifact.callbacks, &mut context);
        context.clear_views();
        *callback_count += 1;
        let elapsed = started.elapsed();
        self.core.context.metrics.callback_duration(
            event_kind_from_u32(context.event_kind),
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        if elapsed > self.core.definition.runtime.callback_budget.soft_budget {
            *budget_violations += 1;
            *consecutive_budget_violations = consecutive_budget_violations.saturating_add(1);
        } else {
            *consecutive_budget_violations = 0;
        }
        push_flight_record(
            flight_records,
            next_flight_sequence,
            "callback",
            Arc::from(event.event_type),
        );
        if elapsed > self.core.definition.runtime.callback_budget.stall_threshold
            || *consecutive_budget_violations
                >= self
                    .core
                    .definition
                    .runtime
                    .callback_budget
                    .max_consecutive_violations
        {
            self.core.context.command_gate.close();
            self.core.context.activation.close();
            *last_error = Some(Arc::from("callback_stall"));
            push_flight_record(
                flight_records,
                next_flight_sequence,
                "fault",
                Arc::from("callback_stall"),
            );
            *lifecycle = StrategyLifecycle::Invalidated;
        }
        if let Err(error) = result {
            self.core.context.command_gate.close();
            self.core.context.activation.close();
            context.last_error = -1;
            let _ = artifact
                .callbacks
                .invoke(StrategyEventKind::Error, &mut context);
            let _ = artifact
                .callbacks
                .invoke(StrategyEventKind::Stop, &mut context);
            *stop_called = true;
            *last_error = Some(error.reason_code);
            push_flight_record(
                flight_records,
                next_flight_sequence,
                "fault",
                Arc::from("callback_failed"),
            );
            *lifecycle = StrategyLifecycle::Failed;
            return Err(plugin_callback_error());
        }
        if *lifecycle == StrategyLifecycle::Invalidated {
            return Ok(());
        }
        let count = context.num_commands;
        if count > commands.len() {
            self.core.context.command_gate.close();
            self.core.context.activation.close();
            context.last_error = -1;
            let _ = artifact
                .callbacks
                .invoke(StrategyEventKind::Error, &mut context);
            if !*stop_called {
                let _ = artifact
                    .callbacks
                    .invoke(StrategyEventKind::Stop, &mut context);
                *stop_called = true;
            }
            *last_error = Some(Arc::from("command_buffer_overflow"));
            push_flight_record(
                flight_records,
                next_flight_sequence,
                "fault",
                Arc::from("command_buffer_overflow"),
            );
            *lifecycle = StrategyLifecycle::Failed;
            return Err(plugin_callback_error());
        }
        for command in &commands[..count] {
            if let Err(error) = self.core.context.command_gateway.execute(
                self.core.context.strategy,
                *command,
                event_trace,
            ) {
                self.core.context.command_gate.close();
                self.core.context.activation.close();
                context.last_error = -1;
                let _ = artifact
                    .callbacks
                    .invoke(StrategyEventKind::Error, &mut context);
                if !*stop_called {
                    let _ = artifact
                        .callbacks
                        .invoke(StrategyEventKind::Stop, &mut context);
                    *stop_called = true;
                }
                *last_error = Some(error.reason_code);
                push_flight_record(
                    flight_records,
                    next_flight_sequence,
                    "fault",
                    Arc::from("command_rejected"),
                );
                *lifecycle = StrategyLifecycle::Failed;
                return Err(plugin_callback_error());
            }
            *command_count += 1;
        }
        Ok(())
    }
}

impl StrategyRuntime for NativeStrategyRuntime {
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()> {
        self.core
            .lane
            .set(lane)
            .map_err(|_| runtime_error("lane_already_attached"))
    }

    fn prepare(&self) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Defined {
                return Err(runtime_error("invalid_prepare_state"));
            }
            inner.lifecycle = StrategyLifecycle::Ready;
            Ok(())
        })
    }

    fn start(&self) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Ready {
                return Err(runtime_error("invalid_start_state"));
            }
            let mut context = callback_context(core, &mut inner);
            if inner
                .artifact
                .callbacks
                .invoke(StrategyEventKind::Start, &mut context)
                .is_err()
            {
                context.last_error = -1;
                let _ = inner
                    .artifact
                    .callbacks
                    .invoke(StrategyEventKind::Error, &mut context);
                let _ = inner
                    .artifact
                    .callbacks
                    .invoke(StrategyEventKind::Stop, &mut context);
                inner.stop_called = true;
                inner.lifecycle = StrategyLifecycle::Failed;
                return Err(runtime_error("on_start_failed"));
            }
            context.clear_views();
            core.context.activation.open();
            core.context.command_gate.open();
            inner.lifecycle = StrategyLifecycle::Running;
            Ok(())
        })
    }

    fn pause(&self, _reason: PauseReason) -> LocalResult<StrategyOperationId> {
        self.schedule(|core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Running {
                return Err(runtime_error("invalid_pause_state"));
            }
            core.context.command_gate.close();
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
                return Err(runtime_error("subscriber_not_normal"));
            }
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner.lifecycle != StrategyLifecycle::Paused {
                return Err(runtime_error("invalid_resume_state"));
            }
            core.context.activation.open();
            core.context.command_gate.open();
            inner.lifecycle = StrategyLifecycle::Running;
            Ok(())
        })
    }

    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId> {
        self.core.context.command_gate.close();
        self.core.context.activation.close();
        self.schedule_before(deadline, |core| {
            let mut inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            inner.lifecycle = StrategyLifecycle::Stopping;
            if !inner.stop_called {
                let mut context = callback_context(core, &mut inner);
                let _ = inner
                    .artifact
                    .callbacks
                    .invoke(StrategyEventKind::Stop, &mut context);
                context.clear_views();
                inner.stop_called = true;
            }
            inner.lifecycle = StrategyLifecycle::Stopped;
            Ok(())
        })
    }

    fn freeze_state(
        &self,
        request: StrategyStateSnapshotRequest,
    ) -> LocalResult<StrategyOperationId> {
        self.schedule(move |core| {
            let inner = core.inner.lock().unwrap_or_else(|p| p.into_inner());
            core.context
                .state_snapshot_sink
                .submit(StrategyPrivateStateSnapshot {
                    checkpoint_id: request.checkpoint_id,
                    strategy: core.context.strategy,
                    state_f64: inner.artifact.state.f64_values.clone().into(),
                    state_i64: inner.artifact.state.i64_values.clone().into(),
                })
        })
    }

    fn state(&self) -> StrategyRuntimeStateSnapshot {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        StrategyRuntimeStateSnapshot {
            handle: self.core.context.strategy,
            lifecycle: inner.lifecycle,
            command_gate_open: self.core.context.command_gate.is_open(),
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
            callback_budget_violations: inner.budget_violations,
            heartbeat_at: SystemTime::now(),
        }
    }

    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot {
        let inner = self.core.inner.lock().unwrap_or_else(|p| p.into_inner());
        StrategyRuntimeDiagnosticSnapshot {
            summary: Arc::from("native strategy runtime"),
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

fn push_flight_record(
    records: &mut VecDeque<StrategyFlightRecord>,
    next_sequence: &mut u64,
    category: &'static str,
    detail: Arc<str>,
) {
    if records.len() == 128 {
        records.pop_front();
    }
    records.push_back(StrategyFlightRecord {
        sequence: *next_sequence,
        observed_at: SystemTime::now(),
        category: Arc::from(category),
        detail,
    });
    *next_sequence = next_sequence.wrapping_add(1);
}

fn callback_context(
    core: &NativeRuntimeCore,
    inner: &mut NativeRuntimeInner,
) -> StrategyRuntimeContext {
    StrategyRuntimeContext {
        generation: core.context.strategy.generation,
        now: core.context.clock.now_ns(),
        state_f64_ptr: inner.artifact.state.f64_values.as_mut_ptr(),
        state_f64_len: inner.artifact.state.f64_values.len(),
        state_i64_ptr: inner.artifact.state.i64_values.as_mut_ptr(),
        state_i64_len: inner.artifact.state.i64_values.len(),
        commands_ptr: inner.commands.as_mut_ptr(),
        command_capacity: inner.commands.len(),
        ..StrategyRuntimeContext::default()
    }
}

fn runtime_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::InvalidState,
        "runtime",
        code,
        "invalid runtime operation",
    )
}

fn plugin_callback_error() -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        titan_plugin_engine::PluginIdentity::new("titan.strategy", "runtime"),
        titan_plugin_engine::LifecycleState::Running,
        "strategy_callback",
        "strategy callback failed",
    )
}

fn event_kind_from_u32(value: u32) -> StrategyEventKind {
    match value {
        0 => StrategyEventKind::Start,
        1 => StrategyEventKind::Order,
        2 => StrategyEventKind::Filled,
        3 => StrategyEventKind::Position,
        4 => StrategyEventKind::Funding,
        5 => StrategyEventKind::Bar,
        6 => StrategyEventKind::Tick,
        7 => StrategyEventKind::Timer,
        9 => StrategyEventKind::Stop,
        _ => StrategyEventKind::Error,
    }
}
