//! Venue factories and the shared control-plane runtime used by MarketPlugin V1.
//!
//! Each adapter owns a dedicated Tokio runtime. Stopping the adapter drops that runtime after the
//! connector's shutdown hook, so all network/retry tasks are bounded by the supplied
//! deadline instead of escaping the MarketPlugin resource scope.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use titan_market_plugin::{
    AssetId, ConnectorDiagnosticSnapshot, ConnectorError, ConnectorHealth, ConnectorHealthSnapshot,
    ConnectorOperationSnapshot, InstrumentSnapshot, MarketConnector, MarketConnectorContext,
    MarketConnectorFactory, MarketDataKind, MarketPluginFactory, MarketSourceDefinition,
    MarketSubscribeRequest, MarketSubscription, OperationId, OperationState,
};
use titan_plugin_engine::ClosureResource;
use tokio::sync::{mpsc, oneshot};

use crate::{
    connector::{
        Connector, ConnectorBuilder, DirectPublication, PublishEvent, direct_publish_sender,
    },
    market_event::MarketEventBridge,
};

const COMMAND_QUEUE_CAPACITY: usize = 256;
const OPERATION_HISTORY_LIMIT: usize = 1_024;

enum RuntimeCommand {
    Subscribe(String, Vec<MarketDataKind>),
    Unsubscribe(String, Vec<MarketDataKind>, OperationId),
    Snapshot(String, OperationId),
}

#[derive(Default)]
struct SubscriptionState {
    refs: HashMap<AssetId, HashMap<MarketDataKind, usize>>,
    leases: HashMap<u64, (AssetId, Vec<MarketDataKind>)>,
}

#[derive(Default)]
struct OperationStore {
    values: HashMap<OperationId, (OperationState, Arc<str>)>,
    terminal_order: VecDeque<OperationId>,
}

impl OperationStore {
    fn set(&mut self, id: OperationId, state: OperationState, detail: impl Into<Arc<str>>) {
        let terminal = state != OperationState::Pending;
        let was_terminal = self
            .values
            .get(&id)
            .is_some_and(|(value, _)| *value != OperationState::Pending);
        self.values.insert(id, (state, detail.into()));
        if terminal && !was_terminal {
            self.terminal_order.push_back(id);
        }
        while self.terminal_order.len() > OPERATION_HISTORY_LIMIT {
            if let Some(expired) = self.terminal_order.pop_front() {
                self.values.remove(&expired);
            }
        }
    }

    fn fail_pending(&mut self, detail: &'static str) {
        let pending: Vec<_> = self
            .values
            .iter()
            .filter_map(|(id, (state, _))| (*state == OperationState::Pending).then_some(*id))
            .collect();
        for id in pending {
            self.set(id, OperationState::Failed, detail);
        }
    }
}

struct RunningRuntime {
    stop: Option<oneshot::Sender<Instant>>,
    command: mpsc::Sender<RuntimeCommand>,
    thread: Option<JoinHandle<()>>,
}

struct MarketConnectorRuntime {
    context: MarketConnectorContext,
    symbols: Arc<HashMap<String, AssetId>>,
    connector: Mutex<Option<Box<dyn Connector>>>,
    runtime: Mutex<Option<RunningRuntime>>,
    running: AtomicBool,
    next_id: AtomicU64,
    subscriptions: Mutex<SubscriptionState>,
    active_kinds: Arc<Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>>,
    health: Arc<Mutex<(ConnectorHealth, Arc<str>)>>,
    operations: Arc<Mutex<OperationStore>>,
}

impl MarketConnectorRuntime {
    fn new(connector: Box<dyn Connector>, context: MarketConnectorContext) -> Arc<Self> {
        let symbols: HashMap<_, _> = context
            .instruments
            .iter()
            .map(|binding| (binding.native_symbol.to_string(), binding.asset_id))
            .collect();
        Arc::new(Self {
            context,
            symbols: Arc::new(symbols),
            connector: Mutex::new(Some(connector)),
            runtime: Mutex::new(None),
            running: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            subscriptions: Mutex::new(SubscriptionState::default()),
            active_kinds: Arc::new(Mutex::new(HashMap::new())),
            health: Arc::new(Mutex::new((ConnectorHealth::Created, Arc::from("created")))),
            operations: Arc::new(Mutex::new(OperationStore::default())),
        })
    }

    fn update_health(&self, state: ConnectorHealth, message: impl Into<Arc<str>>) {
        *self.health.lock().unwrap_or_else(|p| p.into_inner()) = (state, message.into());
    }
}

impl MarketConnector for MarketConnectorRuntime {
    fn start(&self) -> Result<(), ConnectorError> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(ConnectorError::new("connector already running"));
        }
        let mut connector = self
            .connector
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or_else(|| ConnectorError::new("connector cannot be restarted after stop"))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| ConnectorError::new(error.to_string()))?;
        let (command_tx, mut command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (stop_tx, stop_rx) = oneshot::channel::<Instant>();
        let context = self.context.clone();
        let symbols = self.symbols.clone();
        let health = self.health.clone();
        let operations = self.operations.clone();
        let active_kinds = self.active_kinds.clone();
        let event_bridge =
            MarketEventBridge::new(context.clone(), symbols.clone(), active_kinds.clone());
        let overflowed_symbols = Arc::new(Mutex::new(HashSet::new()));
        let publish_overflow = overflowed_symbols.clone();
        let publish_health = health.clone();
        let event_tx = direct_publish_sender(move |publication| {
            let (result, symbol, connector_error, market_activity) = match publication {
                DirectPublication::Event(value) => (
                    event_bridge.publish(value),
                    value.lossy_market_symbol(),
                    matches!(
                        value,
                        PublishEvent::LiveEvent(hftbacktest::types::LiveEvent::Error(_))
                    ),
                    matches!(
                        value,
                        PublishEvent::FeedBatch { .. }
                            | PublishEvent::StreamInvalidated { .. }
                            | PublishEvent::LiveEvent(hftbacktest::types::LiveEvent::Feed { .. })
                            | PublishEvent::LiveEvent(
                                hftbacktest::types::LiveEvent::Funding { .. }
                            )
                    ),
                ),
                DirectPublication::NativeMarket(batch) => {
                    let symbol = batch.symbol();
                    (
                        event_bridge.publish_native(batch),
                        Some(symbol),
                        false,
                        true,
                    )
                }
            };
            if let Err(error) = result {
                if let Some(symbol) = symbol {
                    publish_overflow
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .insert(symbol.to_string());
                }
                *publish_health.lock().unwrap_or_else(|p| p.into_inner()) =
                    (ConnectorHealth::Degraded, Arc::from(error.to_string()));
            } else if market_activity && !connector_error {
                *publish_health.lock().unwrap_or_else(|p| p.into_inner()) = (
                    ConnectorHealth::Running,
                    Arc::from("connector is publishing data"),
                );
            }
        });
        let initial_symbols: Vec<_> = self
            .symbols
            .iter()
            .filter_map(|(symbol, asset)| {
                active_kinds
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(asset)
                    .map(|kinds| (symbol.clone(), kinds.iter().copied().collect::<Vec<_>>()))
            })
            .collect();
        self.update_health(
            ConnectorHealth::Starting,
            "runtime started; network readiness is asynchronous",
        );
        let thread = std::thread::Builder::new().name(format!("market-source-{}", context.source.source_id.0)).spawn(move || {
            runtime.block_on(async move {
                for (symbol, kinds) in initial_symbols {
                    connector.subscribe_market_data(symbol, kinds);
                }
                connector.run_market_data(event_tx);
                let mut stop_rx = std::pin::pin!(stop_rx);
                let mut recovery_check = tokio::time::interval(Duration::from_millis(1));
                recovery_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        deadline = &mut stop_rx => {
                            if let Ok(deadline) = deadline {
                                let remaining = deadline.saturating_duration_since(Instant::now());
                                let _ = tokio::time::timeout(remaining, connector.shutdown()).await;
                            }
                            break;
                        }
                        _ = recovery_check.tick() => {
                            let symbols = {
                                let mut overflowed = overflowed_symbols.lock().unwrap_or_else(|p| p.into_inner());
                                std::mem::take(&mut *overflowed)
                            };
                            if !symbols.is_empty() {
                                let symbols = symbols.into_iter().collect();
                                connector.recover_market_data(symbols);
                                *health.lock().unwrap_or_else(|p| p.into_inner()) = (
                                    ConnectorHealth::Degraded,
                                    Arc::from("direct market publication failed; streams invalidated and snapshots requested"),
                                );
                            }
                        }
                        command = command_rx.recv() => {
                            match command {
                                Some(RuntimeCommand::Subscribe(symbol, kinds)) => connector.subscribe_market_data(symbol, kinds),
                                Some(RuntimeCommand::Unsubscribe(symbol, kinds, operation_id)) => {
                                    connector.unsubscribe_market_data(symbol, kinds);
                                    operations.lock().unwrap_or_else(|p| p.into_inner()).set(
                                        operation_id, OperationState::Succeeded,
                                        "unsubscribe delivered to connector",
                                    );
                                }
                                Some(RuntimeCommand::Snapshot(symbol, operation_id)) => {
                                    connector.request_snapshot(symbol);
                                    operations.lock().unwrap_or_else(|p| p.into_inner()).set(
                                        operation_id, OperationState::Succeeded,
                                        "snapshot request delivered to connector",
                                    );
                                }
                                None => break,
                            }
                        }
                    }
                }
            });
        }).map_err(|error| ConnectorError::new(error.to_string()))?;
        *self.runtime.lock().unwrap_or_else(|p| p.into_inner()) = Some(RunningRuntime {
            stop: Some(stop_tx),
            command: command_tx,
            thread: Some(thread),
        });
        Ok(())
    }

    fn stop(&self, deadline: Instant) -> Result<(), ConnectorError> {
        let Some(mut runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        else {
            self.running.store(false, Ordering::Release);
            return Ok(());
        };
        if let Some(stop) = runtime.stop.take() {
            let _ = stop.send(deadline);
        }
        if let Some(thread) = runtime.thread.take() {
            while !thread.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            if !thread.is_finished() {
                runtime.thread = Some(thread);
                *self.runtime.lock().unwrap_or_else(|p| p.into_inner()) = Some(runtime);
                self.update_health(ConnectorHealth::Failed, "stop deadline exceeded");
                return Err(ConnectorError::new("stop deadline exceeded"));
            }
            thread
                .join()
                .map_err(|_| ConnectorError::new("connector runtime panicked"))?;
        }
        self.running.store(false, Ordering::Release);
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .fail_pending("connector stopped before operation completed");
        self.update_health(ConnectorHealth::Stopped, "stopped");
        Ok(())
    }

    fn subscribe(
        &self,
        request: MarketSubscribeRequest,
    ) -> Result<MarketSubscription, ConnectorError> {
        if !self
            .context
            .instruments
            .iter()
            .any(|binding| binding.asset_id == request.asset_id)
        {
            return Err(ConnectorError::new("unknown asset"));
        }
        if request.kinds.is_empty() {
            return Err(ConnectorError::new(
                "at least one market data kind is required",
            ));
        }
        let symbol = self
            .symbols
            .iter()
            .find_map(|(symbol, asset)| (*asset == request.asset_id).then(|| symbol.clone()))
            .ok_or_else(|| ConnectorError::new("unknown asset"))?;
        let mut kinds: Vec<_> = request.kinds.iter().copied().collect();
        kinds.sort_by_key(|kind| *kind as u8);
        kinds.dedup();
        let subscription_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        let additions: Vec<_> = kinds
            .iter()
            .copied()
            .filter(|kind| {
                subscriptions
                    .refs
                    .get(&request.asset_id)
                    .and_then(|refs| refs.get(kind))
                    .copied()
                    .unwrap_or(0)
                    == 0
            })
            .collect();
        if !additions.is_empty()
            && let Some(runtime) = self
                .runtime
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
        {
            runtime
                .command
                .try_send(RuntimeCommand::Subscribe(symbol, additions.clone()))
                .map_err(|error| ConnectorError::new(error.to_string()))?;
        }
        let refs = subscriptions.refs.entry(request.asset_id).or_default();
        for kind in &kinds {
            *refs.entry(*kind).or_insert(0) += 1;
        }
        subscriptions
            .leases
            .insert(subscription_id, (request.asset_id, kinds));
        drop(subscriptions);
        if !additions.is_empty() {
            self.active_kinds
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entry(request.asset_id)
                .or_default()
                .extend(additions);
        }
        Ok(MarketSubscription {
            id: subscription_id,
        })
    }
    fn unsubscribe(&self, subscription: MarketSubscription) -> Result<OperationId, ConnectorError> {
        let operation_id = OperationId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        let (asset_id, kinds) = subscriptions
            .leases
            .get(&subscription.id)
            .cloned()
            .ok_or_else(|| ConnectorError::new("unknown subscription"))?;
        let removals: Vec<_> = kinds
            .iter()
            .copied()
            .filter(|kind| {
                subscriptions
                    .refs
                    .get(&asset_id)
                    .and_then(|refs| refs.get(kind))
                    .copied()
                    == Some(1)
            })
            .collect();
        let symbol = self
            .symbols
            .iter()
            .find_map(|(symbol, asset)| (*asset == asset_id).then(|| symbol.clone()))
            .ok_or_else(|| ConnectorError::new("unknown asset"))?;
        let runtime_guard = self.runtime.lock().unwrap_or_else(|p| p.into_inner());
        if !removals.is_empty()
            && let Some(runtime) = runtime_guard.as_ref()
        {
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(
                    operation_id,
                    OperationState::Pending,
                    "queued for connector runtime",
                );
            if let Err(error) = runtime.command.try_send(RuntimeCommand::Unsubscribe(
                symbol,
                removals.clone(),
                operation_id,
            )) {
                self.operations
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .set(operation_id, OperationState::Failed, error.to_string());
                return Err(ConnectorError::new(error.to_string()));
            }
        }
        subscriptions.leases.remove(&subscription.id);
        if let Some(refs) = subscriptions.refs.get_mut(&asset_id) {
            for kind in &kinds {
                if let Some(count) = refs.get_mut(kind) {
                    *count -= 1;
                    if *count == 0 {
                        refs.remove(kind);
                    }
                }
            }
            if refs.is_empty() {
                subscriptions.refs.remove(&asset_id);
            }
        }
        drop(subscriptions);
        drop(runtime_guard);
        if !removals.is_empty() {
            let mut active = self.active_kinds.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(active_for_asset) = active.get_mut(&asset_id) {
                for kind in &removals {
                    active_for_asset.remove(kind);
                }
                if active_for_asset.is_empty() {
                    active.remove(&asset_id);
                }
            }
        }
        if removals.is_empty() {
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(
                    operation_id,
                    OperationState::Succeeded,
                    "shared subscription reference released",
                );
        } else if self
            .runtime
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_none()
        {
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(
                    operation_id,
                    OperationState::Succeeded,
                    "subscription released before connector start",
                );
        }
        Ok(operation_id)
    }
    fn request_snapshot(&self, asset_id: AssetId) -> Result<OperationId, ConnectorError> {
        if !self
            .context
            .instruments
            .iter()
            .any(|binding| binding.asset_id == asset_id)
        {
            return Err(ConnectorError::new("unknown asset"));
        }
        if !self
            .active_kinds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&asset_id)
        {
            return Err(ConnectorError::new("asset is not subscribed"));
        }
        let symbol = self
            .symbols
            .iter()
            .find_map(|(symbol, asset)| (*asset == asset_id).then(|| symbol.clone()))
            .ok_or_else(|| ConnectorError::new("unknown asset"))?;
        let runtime = self.runtime.lock().unwrap_or_else(|p| p.into_inner());
        let runtime = runtime
            .as_ref()
            .ok_or_else(|| ConnectorError::new("connector is not running"))?;
        let operation_id = OperationId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set(
                operation_id,
                OperationState::Pending,
                "queued for connector runtime",
            );
        if let Err(error) = runtime
            .command
            .try_send(RuntimeCommand::Snapshot(symbol, operation_id))
        {
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(operation_id, OperationState::Failed, error.to_string());
            return Err(ConnectorError::new(error.to_string()));
        }
        Ok(operation_id)
    }
    fn instruments(&self) -> Arc<[InstrumentSnapshot]> {
        let available = matches!(
            self.health.lock().unwrap_or_else(|p| p.into_inner()).0,
            ConnectorHealth::Running | ConnectorHealth::Degraded
        );
        self.context
            .instruments
            .iter()
            .map(|binding| InstrumentSnapshot {
                native_symbol: binding.native_symbol.clone(),
                asset_id: binding.asset_id,
                available,
            })
            .collect::<Vec<_>>()
            .into()
    }
    fn health(&self) -> ConnectorHealthSnapshot {
        let value = self
            .health
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        ConnectorHealthSnapshot {
            state: value.0,
            message: value.1,
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot {
        ConnectorDiagnosticSnapshot {
            summary: Arc::from(
                "connector-owned stream metadata with shared Market ABI publication",
            ),
        }
    }
    fn operation(&self, id: OperationId) -> ConnectorOperationSnapshot {
        let value = self
            .operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values
            .get(&id)
            .cloned()
            .unwrap_or((OperationState::Failed, Arc::from("unknown operation")));
        ConnectorOperationSnapshot {
            id,
            state: value.0,
            detail: value.1,
        }
    }
}

fn register_resource(connector: &Arc<MarketConnectorRuntime>) -> Result<(), ConnectorError> {
    let weak: Weak<MarketConnectorRuntime> = Arc::downgrade(connector);
    connector
        .context
        .resources
        .register(
            "market-connector-runtime",
            ClosureResource(Some(move || {
                if let Some(connector) = weak.upgrade() {
                    connector
                        .stop(Instant::now() + Duration::from_secs(5))
                        .map_err(|error| {
                            titan_plugin_engine::PluginError::new(
                                titan_plugin_engine::ErrorKind::ResourceReleaseFailed,
                                titan_plugin_engine::PluginIdentity::new(
                                    "titan.market",
                                    "connector",
                                ),
                                titan_plugin_engine::LifecycleState::Stopping,
                                "stop_market_connector_runtime",
                                error.to_string(),
                            )
                        })?;
                }
                Ok(())
            })),
        )
        .map_err(|error| ConnectorError::new(error.to_string()))
}

fn create_runtime<C>(
    definition: &MarketSourceDefinition,
    context: MarketConnectorContext,
) -> Result<Arc<dyn MarketConnector>, ConnectorError>
where
    C: Connector + ConnectorBuilder + 'static,
    C::Error: std::fmt::Debug,
{
    let config = std::str::from_utf8(&definition.connector_config)
        .map_err(|_| ConnectorError::new("connector_config must be UTF-8 TOML"))?;
    let venue = C::build_from(config)
        .map_err(|error| ConnectorError::new(format!("invalid connector config: {error:?}")))?;
    let connector = MarketConnectorRuntime::new(Box::new(venue), context);
    register_resource(&connector)?;
    Ok(connector)
}

#[cfg(feature = "binancefutures")]
pub struct BinanceFuturesMarketFactory;
#[cfg(feature = "binancefutures")]
impl MarketConnectorFactory for BinanceFuturesMarketFactory {
    fn connector_type(&self) -> &str {
        "binance-futures"
    }
    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        create_runtime::<crate::binancefutures::BinanceFutures>(definition, context)
    }
}

#[cfg(feature = "okx")]
pub struct OkxMarketFactory;
#[cfg(feature = "okx")]
impl MarketConnectorFactory for OkxMarketFactory {
    fn connector_type(&self) -> &str {
        "okx"
    }
    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        create_runtime::<crate::okx::Okx>(definition, context)
    }
}

#[cfg(feature = "hyperliquid")]
pub struct HyperliquidMarketFactory;
#[cfg(feature = "hyperliquid")]
impl MarketConnectorFactory for HyperliquidMarketFactory {
    fn connector_type(&self) -> &str {
        "hyperliquid"
    }
    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        create_runtime::<crate::hyperliquid::Hyperliquid>(definition, context)
    }
}

/// Builds a MarketPlugin factory with every venue connector enabled for this connector build.
pub fn builtin_market_plugin_factory() -> MarketPluginFactory {
    let factory = MarketPluginFactory::new();
    #[cfg(feature = "binancefutures")]
    let factory = factory.with_factory(Arc::new(BinanceFuturesMarketFactory));
    #[cfg(feature = "okx")]
    let factory = factory.with_factory(Arc::new(OkxMarketFactory));
    #[cfg(feature = "hyperliquid")]
    let factory = factory.with_factory(Arc::new(HyperliquidMarketFactory));
    factory
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_history_is_bounded_and_pending_operations_are_terminalized() {
        let mut store = OperationStore::default();
        let pending = OperationId(1);
        store.set(pending, OperationState::Pending, "pending");
        for value in 2..=(OPERATION_HISTORY_LIMIT as u64 + 2) {
            store.set(OperationId(value), OperationState::Succeeded, "done");
        }
        assert!(store.values.len() <= OPERATION_HISTORY_LIMIT + 1);
        store.fail_pending("stopped");
        assert_eq!(
            store.values.get(&pending).unwrap().0,
            OperationState::Failed
        );
    }

    /// Public-market live smoke test for the complete path:
    /// Binance Futures testnet -> connector -> MarketEventBridge -> MarketPlugin publisher ->
    /// EventEngine subscriber. No authenticated API or trading method is used.
    #[cfg(feature = "binancefutures")]
    #[test]
    #[ignore]
    fn binance_futures_live_market_plugin_pipeline() {
        use std::{
            collections::{BTreeSet, HashMap},
            sync::atomic::{AtomicBool, Ordering},
        };

        use semver::Version;
        use titan_event_engine::{
            AsyncFastLaneConfig, EventClass, EventEngine, EventEngineConfig, PoolKind,
            SubscriberRuntimeMode,
        };
        use titan_market_plugin::{
            BBO_EVENT, DEPTH_BATCH_EVENT, FUNDING_RATE_EVENT, MARKET_EVENT_TYPES,
            MarketAdminRequest, MarketAdminResponse, MarketDataKind, MarketInstrumentBinding,
            MarketRequest, MarketResponse, TRADE_BATCH_EVENT,
        };
        use titan_plugin_engine::{
            ApiVersion, DispatchOutcome, EventControl, EventHandler, EventQos, EventView,
            ExecutionModel, ExecutionSpec, PluginEngine, PluginError, PluginIdentity, PluginSpec,
            ServiceKey, ServiceScope, StopReason, SubscriptionLimits, SubscriptionSpec,
            TraceContext,
        };

        #[derive(Clone, Copy, Debug)]
        struct ForwardedMarketEvent {
            event_type: &'static str,
            observed_ns: u64,
            latency_ns: Option<u64>,
            depth_kind: u16,
            depth_epoch: u64,
            last_depth_sequence: u64,
        }

        struct Forward(Arc<std::sync::Mutex<Vec<ForwardedMarketEvent>>>);
        impl EventHandler for Forward {
            fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
                let event_type = match event.event_type {
                    DEPTH_BATCH_EVENT => DEPTH_BATCH_EVENT,
                    TRADE_BATCH_EVENT => TRADE_BATCH_EVENT,
                    BBO_EVENT => BBO_EVENT,
                    FUNDING_RATE_EVENT => FUNDING_RATE_EVENT,
                    value => panic!("unexpected market event: {value}"),
                };
                let observed_ns = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64;
                let latency_ns = (event.payload.len()
                    >= titan_market_plugin::MarketBatchHeaderV1::ENCODED_LEN)
                    .then(|| i64::from_le_bytes(event.payload[44..52].try_into().unwrap()))
                    .filter(|receive_ts| *receive_ts > 0)
                    .and_then(|receive_ts| {
                        let elapsed = i128::from(observed_ns) - i128::from(receive_ts);
                        (elapsed >= 0).then_some(elapsed as u64)
                    });
                assert_eq!(u32_at(event.payload, 0), 1);
                let (depth_kind, depth_epoch, _first_depth_sequence, last_depth_sequence) =
                    if event_type == DEPTH_BATCH_EVENT {
                        assert!(
                            event.payload.len()
                                >= titan_market_plugin::MarketBatchHeaderV1::ENCODED_LEN
                        );
                        assert!(u16_at(event.payload, 8) > 0);
                        let depth_kind = u16_at(event.payload, 4);
                        let depth_epoch = u64_at(event.payload, 12);
                        let first = u64_at(event.payload, 20);
                        let last = u64_at(event.payload, 28);
                        assert!(depth_epoch > 0);
                        assert!(first <= last);
                        (depth_kind, depth_epoch, first, last)
                    } else {
                        if event_type == FUNDING_RATE_EVENT {
                            assert_eq!(event.payload.len(), 20);
                            let rate = f64::from_le_bytes(event.payload[4..12].try_into().unwrap());
                            assert!(rate.is_finite());
                        }
                        (0, 0, 0, 0)
                    };
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(ForwardedMarketEvent {
                        event_type,
                        observed_ns,
                        latency_ns,
                        depth_kind,
                        depth_epoch,
                        last_depth_sequence,
                    });
                Ok(())
            }
        }

        struct Discard;
        impl EventHandler for Discard {
            fn handle(&self, _event: EventView<'_>) -> Result<(), PluginError> {
                Ok(())
            }
        }

        fn service_key(name: &str) -> ServiceKey {
            ServiceKey {
                id: titan_plugin_engine::ServiceId::new("titan.market", name),
                version: Version::new(1, 0, 0),
                scope: ServiceScope::Global,
            }
        }

        fn call<R: 'static>(
            engine: &PluginEngine,
            service: &str,
            request: impl Send + Sync + 'static,
        ) -> R {
            *engine
                .services()
                .bind(&service_key(service))
                .unwrap()
                .call(Box::new(request), TraceContext::default())
                .unwrap()
                .downcast::<R>()
                .unwrap()
        }

        fn u16_at(payload: &[u8], offset: usize) -> u16 {
            u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap())
        }

        fn u32_at(payload: &[u8], offset: usize) -> u32 {
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
        }

        fn u64_at(payload: &[u8], offset: usize) -> u64 {
            u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
        }

        fn percentile(sorted: &[u64], quantile: f64) -> u64 {
            let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
            sorted[index]
        }

        let mut event_config = EventEngineConfig::default();
        event_config.ingress.max_sources = 32;
        // Binance's 1,000-level snapshot needs 52 + 2,000 * 24 bytes in the worst case.
        event_config.arena.market_batch.block_bytes = 64 * 1_024;
        event_config.subscribers.default_capacity = 4_096;
        event_config.subscribers.critical_reserve = 8;
        event_config.subscribers.idle_sleep_us = 1_000;
        // Market publication actively unparks the EventEngine. Avoid burning the sibling SMT
        // thread on empty polling, which otherwise steals decode/ABI-encode cycles from Depth.
        event_config.runtime.spin_iterations = 0;
        event_config.runtime.sleep_us = 1_000;
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        // The normal subscriber is an audit/mirror path. Keep it parked so the asynchronous
        // FastLane worker owns the latency-sensitive CPU instead of competing with another
        // busy-spin consumer.
        event_config.subscribers.runtime_mode = SubscriberRuntimeMode::Park;
        let event_engine = EventEngine::new(event_config).unwrap();
        let event_handle = event_engine.handle();
        for event_type in MARKET_EVENT_TYPES {
            event_handle
                .register_event(event_type, 1, EventClass::Market, PoolKind::MarketBatch)
                .unwrap();
        }
        event_engine.start().unwrap();

        // Keep the measured handler free of per-event allocation and cross-thread wakeups. The old
        // probe cloned every ABI payload and allocated an unbounded mpsc node per event, so its own
        // work created FastLane backlog and was incorrectly included in the next event's latency.
        let forwarded_events = Arc::new(std::sync::Mutex::new(Vec::with_capacity(100_000)));
        let fast_lane_config = AsyncFastLaneConfig {
            priority_event_types: vec![Arc::from(DEPTH_BATCH_EVENT), Arc::from(TRADE_BATCH_EVENT)],
            runtime_mode: if parallelism >= 2 {
                SubscriberRuntimeMode::Dedicated
            } else {
                SubscriberRuntimeMode::SpinSleep
            },
            cpu_affinity: (parallelism >= 2).then_some(1),
            ..AsyncFastLaneConfig::default()
        };
        let fast_lane = event_handle
            .register_async_fast_lane(
                &[
                    (DEPTH_BATCH_EVENT, 1),
                    (TRADE_BATCH_EVENT, 1),
                    (BBO_EVENT, 1),
                    (FUNDING_RATE_EVENT, 1),
                ],
                vec![1],
                fast_lane_config,
                Arc::new(Forward(forwarded_events.clone())),
            )
            .unwrap();

        let route = event_handle
            .begin_route_update(event_handle.current_route_version())
            .unwrap();
        for event_type in [
            DEPTH_BATCH_EVENT,
            TRADE_BATCH_EVENT,
            BBO_EVENT,
            FUNDING_RATE_EVENT,
        ] {
            event_handle
                .stage_subscription_in_mailbox(
                    route,
                    &PluginIdentity::new("live-test", "market-consumer"),
                    "market-data",
                    &SubscriptionSpec {
                        event_type: Arc::from(event_type),
                        schema_version: 1,
                        qos: if matches!(event_type, BBO_EVENT | FUNDING_RATE_EVENT) {
                            EventQos::Latest
                        } else {
                            EventQos::ReliableOrdered
                        },
                        capacity: 4_096,
                        routing_keys: Arc::from([1]),
                    },
                )
                .unwrap();
        }
        let (_, subscriptions) = event_handle.commit_at_safe_point(route).unwrap();
        let receivers: Vec<_> = subscriptions
            .iter()
            .fold(HashMap::new(), |mut receivers, subscription| {
                receivers
                    .entry(subscription.mailbox_id)
                    .or_insert_with(|| subscription.receiver.clone());
                receivers
            })
            .into_values()
            .collect();
        let consumers_running = Arc::new(AtomicBool::new(true));
        let consumer_running = consumers_running.clone();
        let consumers = vec![std::thread::spawn(move || {
            let handler = Discard;
            while consumer_running.load(Ordering::Acquire) {
                let mut open = false;
                for receiver in &receivers {
                    match receiver
                        .dispatch_next(&handler, Duration::from_millis(1))
                        .unwrap()
                    {
                        DispatchOutcome::Delivered | DispatchOutcome::Idle => open = true,
                        DispatchOutcome::Closed => {}
                    }
                }
                if !open {
                    break;
                }
            }
        })];

        let mut plugin_engine =
            PluginEngine::new(Arc::new(event_handle.clone()), ApiVersion::new(1, 0)).unwrap();
        plugin_engine
            .register(
                Arc::new(builtin_market_plugin_factory()),
                Version::new(1, 0, 0),
                "connector-live-test",
            )
            .unwrap();
        plugin_engine
            .apply(&[PluginSpec {
                instance_id: Arc::from("market"),
                plugin_type: Arc::from(titan_market_plugin::MARKET_PLUGIN_TYPE),
                config: Arc::new(titan_plugin_engine::ConfigSnapshot::new(
                    1,
                    serde_json::json!({"market_plugin":{"max_sources":2,"max_instruments":4}}),
                )),
                enabled: true,
                execution: ExecutionSpec {
                    model: ExecutionModel::Passive,
                    cpu_affinity: None,
                    callback_budget: None,
                },
                subscription_limits: SubscriptionLimits {
                    max_capacity: 4_096,
                    allowed_qos: BTreeSet::from([
                        EventQos::ReliableOrdered,
                        EventQos::BestEffort,
                        EventQos::Latest,
                    ]),
                },
                service_scopes: vec![
                    (
                        titan_plugin_engine::ServiceId::new("titan.market", "admin"),
                        ServiceScope::Global,
                    ),
                    (
                        titan_plugin_engine::ServiceId::new("titan.market", "market"),
                        ServiceScope::Global,
                    ),
                ],
                required_service_scopes: vec![],
            }])
            .unwrap();

        let stream_url = std::env::var("TITAN_BINANCE_FUTURES_WS_URL")
            .unwrap_or_else(|_| "wss://fstream.binancefuture.com/ws".to_string());
        let api_url = std::env::var("TITAN_BINANCE_FUTURES_API_URL")
            .unwrap_or_else(|_| "https://testnet.binancefuture.com".to_string());
        let connector_config = format!(
            "stream_url = {stream_url:?}\napi_url = {api_url:?}\norder_prefix = \"market-plugin-live-test\"\napi_key = \"\"\nsecret = \"\"\nsafety_timeout_ms = 0\n"
        );
        let definition = MarketSourceDefinition {
            source_key: Arc::from("binance-futures-live"),
            connector_type: Arc::from("binance-futures"),
            connector_config: Arc::from(connector_config.into_bytes()),
            instruments: Arc::from([MarketInstrumentBinding {
                native_symbol: Arc::from("btcusdt"),
                asset_id: AssetId(1),
                price_tick: 0.1,
                quantity_lot: 0.0001,
            }]),
            enabled: true,
            definition_version: 1,
        };
        let created: titan_market_plugin::LocalResult<MarketAdminResponse> = call(
            &plugin_engine,
            "admin",
            MarketAdminRequest::Create(definition),
        );
        let source = match created.unwrap() {
            MarketAdminResponse::Handle(value) => value,
            response => panic!("unexpected create response: {response:?}"),
        };
        let subscribed: titan_market_plugin::LocalResult<MarketResponse> = call(
            &plugin_engine,
            "market",
            MarketRequest::Subscribe(
                source,
                MarketSubscribeRequest {
                    asset_id: AssetId(1),
                    kinds: Arc::from([
                        MarketDataKind::Depth,
                        MarketDataKind::Trades,
                        MarketDataKind::Bbo,
                        MarketDataKind::FundingRate,
                    ]),
                },
            ),
        );
        assert!(matches!(
            subscribed.unwrap(),
            MarketResponse::Subscription(_)
        ));
        let started: titan_market_plugin::LocalResult<MarketAdminResponse> =
            call(&plugin_engine, "admin", MarketAdminRequest::Start(source));
        assert!(matches!(
            started.unwrap(),
            MarketAdminResponse::OperationId(_)
        ));

        std::thread::sleep(Duration::from_secs(20));

        let health: titan_market_plugin::LocalResult<MarketResponse> =
            call(&plugin_engine, "market", MarketRequest::Health(source));
        let health = match health.unwrap() {
            MarketResponse::Health(value) => value,
            response => panic!("unexpected health response: {response:?}"),
        };
        let engine_metrics = event_engine.metrics().snapshot();
        let stopped: titan_market_plugin::LocalResult<MarketAdminResponse> = call(
            &plugin_engine,
            "admin",
            MarketAdminRequest::Stop(source, Instant::now() + Duration::from_secs(5)),
        );
        assert!(matches!(
            stopped.unwrap(),
            MarketAdminResponse::OperationId(_)
        ));
        plugin_engine.shutdown(StopReason::Shutdown).unwrap();
        event_handle.unregister_fast_lane(fast_lane);
        event_engine.stop().unwrap();
        consumers_running.store(false, Ordering::Release);
        for consumer in consumers {
            consumer.join().unwrap();
        }

        let events = std::mem::take(
            &mut *forwarded_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let mut snapshots = 0usize;
        let mut deltas = 0usize;
        let mut deltas_before_snapshot = 0usize;
        let mut trades = 0usize;
        let mut bbo = 0usize;
        let mut funding = 0usize;
        let mut depth_epoch = None;
        let mut last_depth_sequence = None;
        let mut snapshot_received_ns = None;
        let mut latencies: HashMap<String, Vec<u64>> = HashMap::new();
        for event in events {
            if let Some(latency_ns) = event.latency_ns {
                let latency_key = if event.event_type == DEPTH_BATCH_EVENT {
                    match event.depth_kind {
                        1 => format!("{}.Snapshot", event.event_type),
                        2 if snapshot_received_ns.is_some_and(|received: u64| {
                            event.observed_ns.saturating_sub(received) >= 1_000_000_000
                        }) =>
                        {
                            format!("{}.DeltaSteady", event.event_type)
                        }
                        2 => format!("{}.DeltaWarmup", event.event_type),
                        _ => event.event_type.to_string(),
                    }
                } else {
                    event.event_type.to_string()
                };
                latencies.entry(latency_key).or_default().push(latency_ns);
            }
            match event.event_type {
                DEPTH_BATCH_EVENT => {
                    if event.depth_kind == 1 {
                        snapshots += 1;
                        snapshot_received_ns = Some(event.observed_ns);
                        depth_epoch = Some(event.depth_epoch);
                        last_depth_sequence = Some(event.last_depth_sequence);
                    } else {
                        assert_eq!(event.depth_kind, 2);
                        if depth_epoch.is_none() {
                            deltas_before_snapshot += 1;
                            continue;
                        }
                        assert_eq!(depth_epoch, Some(event.depth_epoch));
                        if let Some(previous) = last_depth_sequence {
                            assert!(
                                event.last_depth_sequence >= previous,
                                "depth sequence regressed"
                            );
                        }
                        last_depth_sequence = Some(event.last_depth_sequence);
                        deltas += 1;
                    }
                }
                TRADE_BATCH_EVENT => trades += 1,
                BBO_EVENT => bbo += 1,
                FUNDING_RATE_EVENT => funding += 1,
                _ => unreachable!(),
            }
        }

        println!(
            "pipeline snapshot={snapshots} delta={deltas} delta_before_snapshot={deltas_before_snapshot} trade={trades} bbo={bbo} funding={funding} epoch={depth_epoch:?} last_sequence={last_depth_sequence:?} health={:?}: {}",
            health.state, health.message
        );
        println!(
            "event_engine dispatch={:?} subscriber={:?} drain={:?} drops={} resync={} rejected={} fast_lane_enqueued={} fast_lane_drops={} fast_lane_depth_max={} fast_lane_enqueue={:?} fast_lane_handler={:?}",
            engine_metrics.dispatch_latency,
            engine_metrics.subscriber_latency,
            engine_metrics.drain_latency,
            engine_metrics.drop_total,
            engine_metrics.resync_total,
            engine_metrics.publish_rejected_total,
            engine_metrics.fast_lane_enqueue_total,
            engine_metrics.fast_lane_drop_total,
            engine_metrics.fast_lane_depth_max,
            engine_metrics.fast_lane_enqueue_latency,
            engine_metrics.fast_lane_latency,
        );
        for (event_type, values) in &mut latencies {
            values.sort_unstable();
            println!(
                "latency event={event_type} samples={} p50_us={:.3} p90_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3}",
                values.len(),
                percentile(values, 0.50) as f64 / 1_000.0,
                percentile(values, 0.90) as f64 / 1_000.0,
                percentile(values, 0.95) as f64 / 1_000.0,
                percentile(values, 0.99) as f64 / 1_000.0,
                values.last().copied().unwrap() as f64 / 1_000.0,
            );
        }
        for event_type in [
            format!("{DEPTH_BATCH_EVENT}.DeltaSteady"),
            TRADE_BATCH_EVENT.to_string(),
            BBO_EVENT.to_string(),
        ] {
            let values = latencies
                .get(&event_type)
                .unwrap_or_else(|| panic!("no latency samples for {event_type}"));
            assert!(
                percentile(values, 0.50) < 30_000,
                "{event_type} p50 latency did not meet the 30us target"
            );
        }
        assert!(matches!(health.state, ConnectorHealth::Running));
        assert!(snapshots > 0, "no ABI depth snapshot reached EventEngine");
        assert!(deltas > 0, "no ABI depth delta reached EventEngine");
        assert_eq!(
            deltas_before_snapshot, 0,
            "ABI depth delta reached EventEngine before its snapshot"
        );
        assert!(trades > 0, "no ABI trades reached EventEngine");
        assert!(bbo > 0, "no ABI BBO reached EventEngine");
        // Funding is intentionally not an acceptance requirement for this short latency window;
        // Binance may not emit a mark-price update during every 20-second sample.
    }
}
