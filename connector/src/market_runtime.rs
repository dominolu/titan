//! Static venue factories and the shared runtime used by the market service.
//!
//! Each adapter owns a dedicated Tokio runtime. Stopping the adapter drops that runtime after the
//! connector's shutdown hook, so all network/retry tasks are bounded by the supplied
//! deadline instead of escaping the market service resource scope.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use titan_core_types::ClosureResource;
use titan_market_service::{
    AssetId, ConnectorDiagnosticSnapshot, ConnectorError, ConnectorHealth, ConnectorHealthSnapshot,
    ConnectorOperationSnapshot, InstrumentSnapshot, MarketConnector, MarketConnectorContext,
    MarketConnectorFactory, MarketDataKind, MarketSourceDefinition, MarketSubscribeRequest,
    MarketSubscription, OperationId, OperationState,
};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::{
    connector::{
        Connector, ConnectorBuilder, DirectPublication, PublishEvent, direct_publish_sender,
    },
    market_event::MarketEventBridge,
};

const COMMAND_QUEUE_CAPACITY: usize = 256;
const OPERATION_HISTORY_LIMIT: usize = 1_024;

enum RuntimeCommand {
    Subscribe(String, Vec<MarketDataKind>, bool),
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
    shutdown_result: Arc<Mutex<Option<Result<(), Arc<str>>>>>,
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
        crate::ensure_rustls_crypto_provider();
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(ConnectorError::new("connector already running"));
        }
        let mut connector = match self
            .connector
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            Some(connector) => connector,
            None => {
                self.running.store(false, Ordering::Release);
                self.update_health(
                    ConnectorHealth::Failed,
                    "connector cannot be restarted after resources were released",
                );
                return Err(ConnectorError::new(
                    "connector cannot be restarted after stop",
                ));
            }
        };
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                *self.connector.lock().unwrap_or_else(|p| p.into_inner()) = Some(connector);
                self.running.store(false, Ordering::Release);
                self.update_health(ConnectorHealth::Failed, error.to_string());
                return Err(ConnectorError::new(error.to_string()));
            }
        };
        let (command_tx, mut command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (stop_tx, stop_rx) = oneshot::channel::<Instant>();
        let shutdown_result = Arc::new(Mutex::new(None));
        let thread_shutdown_result = shutdown_result.clone();
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
                    matches!(value, PublishEvent::ConnectorError(_)),
                    matches!(
                        value,
                        PublishEvent::FeedBatch { .. }
                            | PublishEvent::StreamInvalidated { .. }
                            | PublishEvent::Funding { .. }
                            | PublishEvent::MarkPrice { .. }
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
                DirectPublication::Account(_) => (
                    Err(ConnectorError::new(
                        "account publication reached a market-only connector runtime",
                    )),
                    None,
                    true,
                    false,
                ),
            };
            if let Err(error) = result {
                warn!(
                    symbol = symbol.unwrap_or(""),
                    error = %error,
                    "market publication failed; scheduling stream recovery",
                );
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
                            let result = match deadline {
                                Ok(deadline) => {
                                    let remaining = deadline.saturating_duration_since(Instant::now());
                                    match tokio::time::timeout(remaining, connector.shutdown()).await {
                                        Ok(Ok(())) => Ok(()),
                                        Ok(Err(error)) => Err(Arc::from(error)),
                                        Err(_) => Err(Arc::from("connector shutdown deadline exceeded")),
                                    }
                                }
                                Err(_) => Err(Arc::from("connector stop signal was dropped")),
                            };
                            *thread_shutdown_result.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
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
                                let mut health =
                                    health.lock().unwrap_or_else(|p| p.into_inner());
                                let publication_error = health.1.clone();
                                *health = (
                                    ConnectorHealth::Degraded,
                                    Arc::from(format!(
                                        "direct market publication failed; streams invalidated and snapshots requested; last error: {publication_error}"
                                    )),
                                );
                            }
                        }
                        command = command_rx.recv() => {
                            match command {
                                Some(RuntimeCommand::Subscribe(symbol, kinds, snapshot_after)) => {
                                    connector.subscribe_market_data(symbol.clone(), kinds);
                                    if snapshot_after {
                                        connector.request_snapshot(symbol);
                                    }
                                },
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
        }).map_err(|error| {
            self.running.store(false, Ordering::Release);
            self.update_health(ConnectorHealth::Failed, error.to_string());
            ConnectorError::new(error.to_string())
        })?;
        *self.runtime.lock().unwrap_or_else(|p| p.into_inner()) = Some(RunningRuntime {
            stop: Some(stop_tx),
            command: command_tx,
            thread: Some(thread),
            shutdown_result,
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
        let shutdown_result = runtime
            .shutdown_result
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .unwrap_or_else(|| {
                Err(Arc::from(
                    "connector runtime exited without shutdown result",
                ))
            });
        if let Err(error) = shutdown_result {
            self.running.store(false, Ordering::Release);
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .fail_pending("connector shutdown failed");
            self.update_health(ConnectorHealth::Failed, error.clone());
            return Err(ConnectorError::new(error));
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
        let needs_shared_snapshot = additions.len() < kinds.len();
        if let Some(runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            if !additions.is_empty() {
                runtime
                    .command
                    .try_send(RuntimeCommand::Subscribe(
                        symbol.clone(),
                        additions.clone(),
                        needs_shared_snapshot,
                    ))
                    .map_err(|error| ConnectorError::new(error.to_string()))?;
            }
            if needs_shared_snapshot && additions.is_empty() {
                // The EventEngine route is created before this call. A new consumer sharing an
                // existing venue subscription still needs a replacement boundary of its own;
                // request it from the concrete connector instead of replaying a service-side
                // cache or manufacturing stream coordinates here.
                let operation_id = OperationId(self.next_id.fetch_add(1, Ordering::Relaxed));
                self.operations
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .set(
                        operation_id,
                        OperationState::Pending,
                        "shared subscription snapshot queued",
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
            }
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
                            titan_core_types::CoreError::new(
                                titan_core_types::ErrorKind::ResourceReleaseFailed,
                                titan_core_types::ComponentIdentity::new(
                                    "titan.market",
                                    "connector",
                                ),
                                titan_core_types::ComponentState::Stopping,
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
    crate::ensure_rustls_crypto_provider();
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
        crate::ensure_rustls_crypto_provider();
        let config = std::str::from_utf8(&definition.connector_config)
            .map_err(|_| ConnectorError::new("connector_config must be UTF-8 TOML"))?;
        let venue = crate::hyperliquid::Hyperliquid::build_market_from(config)
            .map_err(|error| ConnectorError::new(format!("invalid connector config: {error:?}")))?;
        let connector = MarketConnectorRuntime::new(Box::new(venue), context);
        register_resource(&connector)?;
        Ok(connector)
    }
}

/// Static venue catalog used by TradingRuntime. The returned factories are ordinary in-process
/// implementations; no manifest, shared library or JSON ABI participates in construction.
pub fn venue_market_factories() -> Vec<Arc<dyn MarketConnectorFactory>> {
    let mut values: Vec<Arc<dyn MarketConnectorFactory>> = Vec::new();
    #[cfg(feature = "binancefutures")]
    values.push(Arc::new(BinanceFuturesMarketFactory));
    #[cfg(feature = "okx")]
    values.push(Arc::new(OkxMarketFactory));
    #[cfg(feature = "hyperliquid")]
    values.push(Arc::new(HyperliquidMarketFactory));
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmptyOrders;
    impl crate::connector::GetOrders for EmptyOrders {
        fn orders(&self, _: Option<String>) -> Vec<hftbacktest::types::Order> {
            Vec::new()
        }
    }

    struct RuntimeProbeConnector {
        calls: Arc<Mutex<Vec<String>>>,
        shutdown_error: Option<String>,
        shutdown_delay: Duration,
        publish_on_run: bool,
        snapshot_delay: Duration,
    }

    #[async_trait::async_trait]
    impl Connector for RuntimeProbeConnector {
        fn register(&mut self, _: String) {}
        fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("subscribe:{symbol}:{kinds:?}"));
        }
        fn unsubscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("unsubscribe:{symbol}:{kinds:?}"));
        }
        fn request_snapshot(&mut self, symbol: String) {
            if !self.snapshot_delay.is_zero() {
                std::thread::sleep(self.snapshot_delay);
            }
            self.calls
                .lock()
                .unwrap()
                .push(format!("snapshot:{symbol}"));
        }
        fn order_manager(&self) -> Arc<Mutex<dyn crate::connector::GetOrders + Send + 'static>> {
            Arc::new(Mutex::new(EmptyOrders))
        }
        fn run(&mut self, tx: crate::connector::PublishSender) {
            if self.publish_on_run {
                assert!(
                    tx.try_send_native_market(crate::connector::NativeMarketBatch::Depth {
                        symbol: "BTC",
                        bids: crate::connector::NativeDepthLevels::Borrowed(&[("100", "1")]),
                        asks: crate::connector::NativeDepthLevels::Borrowed(&[("101", "1")]),
                        exchange_ts: 1,
                        receive_ts: 2,
                        stream: crate::connector::MarketStreamMetadata {
                            epoch: 1,
                            first_update_sequence: 1,
                            last_update_sequence: 1,
                            snapshot: false,
                        },
                    })
                );
            }
        }
        async fn shutdown(&self) -> Result<(), String> {
            self.calls.lock().unwrap().push("shutdown".to_owned());
            // Deliberately model a non-cooperative third-party shutdown call. The adapter's
            // outer thread deadline still has to return promptly and retain the JoinHandle for
            // a later reap even when Tokio cannot pre-empt the future.
            if !self.shutdown_delay.is_zero() {
                std::thread::sleep(self.shutdown_delay);
            }
            self.shutdown_error.clone().map_or(Ok(()), Err)
        }
    }

    struct NoopMarketSink;
    impl titan_market_service::MarketEventSink for NoopMarketSink {
        fn publish_market(
            &self,
            _: &str,
            _: &[u8],
            _: AssetId,
            _: i64,
            _: i64,
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            Ok(())
        }
        fn publish_control(
            &self,
            _: &str,
            _: &[u8],
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            Ok(())
        }
    }

    struct RejectMarketSink;
    impl titan_market_service::MarketEventSink for RejectMarketSink {
        fn publish_market(
            &self,
            _: &str,
            _: &[u8],
            _: AssetId,
            _: i64,
            _: i64,
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            Err(titan_core_types::CoreError::new(
                titan_core_types::ErrorKind::ComponentFailed,
                titan_core_types::ComponentIdentity::new("test", "sink"),
                titan_core_types::ComponentState::Running,
                "publish_market",
                "injected queue full",
            ))
        }
        fn publish_control(
            &self,
            _: &str,
            _: &[u8],
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            Ok(())
        }
    }

    fn runtime_context(scope: &titan_core_types::ResourceScope) -> MarketConnectorContext {
        runtime_context_with_sink(scope, Arc::new(NoopMarketSink))
    }

    fn runtime_context_with_sink(
        scope: &titan_core_types::ResourceScope,
        sink: Arc<dyn titan_market_service::MarketEventSink>,
    ) -> MarketConnectorContext {
        MarketConnectorContext {
            source: titan_market_service::MarketSourceHandle {
                source_id: titan_market_service::MarketSourceId(1),
                generation: 1,
            },
            instruments: Arc::from([titan_market_service::MarketInstrumentBinding {
                native_symbol: Arc::from("BTC"),
                asset_id: AssetId(7),
                price_tick: "0.1".parse().unwrap(),
                quantity_lot: "0.001".parse().unwrap(),
            }]),
            market_source_stream: titan_market_service::SourceStreamId(1),
            control_source_stream: titan_market_service::SourceStreamId(2),
            event_publisher: titan_market_service::MarketEventPublisher::from_sink(sink),
            resources: scope.handle(),
        }
    }

    fn assert_venue_factory_local_contract(factory: &dyn MarketConnectorFactory, config: &str) {
        let mut scope = titan_core_types::ResourceScope::new(
            titan_core_types::ComponentIdentity::new("test", factory.connector_type()),
        );
        let definition = MarketSourceDefinition {
            source_key: Arc::from(format!("{}-source", factory.connector_type())),
            connector_type: Arc::from(factory.connector_type()),
            connector_config: Arc::from(config.as_bytes()),
            instruments: Arc::from([titan_market_service::MarketInstrumentBinding {
                native_symbol: Arc::from("BTC-USDT"),
                asset_id: AssetId(7),
                price_tick: "0.1".parse().unwrap(),
                quantity_lot: "0.001".parse().unwrap(),
            }]),
            enabled: true,
            definition_version: 1,
        };
        let connector = factory
            .create(&definition, runtime_context(&scope))
            .expect("public market connector config must be accepted");

        assert_eq!(connector.health().state, ConnectorHealth::Created);
        assert_eq!(connector.instruments().len(), 1);
        assert!(!connector.instruments()[0].available);
        assert!(
            connector
                .subscribe(MarketSubscribeRequest {
                    asset_id: AssetId(7),
                    kinds: Arc::from([]),
                })
                .is_err()
        );
        let subscription = connector
            .subscribe(MarketSubscribeRequest {
                asset_id: AssetId(7),
                kinds: Arc::from([MarketDataKind::Depth, MarketDataKind::Depth]),
            })
            .unwrap();
        assert!(connector.request_snapshot(AssetId(7)).is_err());
        let release = connector.unsubscribe(subscription).unwrap();
        assert_eq!(
            connector.operation(release).state,
            OperationState::Succeeded
        );
        assert!(connector.unsubscribe(subscription).is_err());
        assert!(connector.request_snapshot(AssetId(999)).is_err());
        connector
            .stop(Instant::now() + Duration::from_millis(10))
            .unwrap();
        drop(connector);
        scope.close().unwrap();
    }

    #[cfg(feature = "binancefutures")]
    #[test]
    fn binance_futures_factory_obeys_the_unified_local_connector_contract() {
        assert_venue_factory_local_contract(
            &BinanceFuturesMarketFactory,
            r#"
stream_url = "wss://fstream.binance.com"
api_url = "https://fapi.binance.com"
"#,
        );
    }

    #[cfg(feature = "okx")]
    #[test]
    fn okx_factory_obeys_the_unified_local_connector_contract() {
        assert_venue_factory_local_contract(
            &OkxMarketFactory,
            r#"
rest_url = "https://www.okx.com"
public_ws_url = "wss://ws.okx.com:8443/ws/v5/public"
private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"
api_key = ""
secret = ""
passphrase = ""
"#,
        );
    }

    #[cfg(feature = "hyperliquid")]
    #[test]
    fn hyperliquid_factory_obeys_the_unified_local_connector_contract() {
        assert_venue_factory_local_contract(
            &HyperliquidMarketFactory,
            r#"
info_url = "https://api.hyperliquid.xyz/info"
exchange_url = "https://api.hyperliquid.xyz/exchange"
ws_url = "wss://api.hyperliquid.xyz/ws"
"#,
        );
    }

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

    #[test]
    fn shared_subscription_snapshot_and_shutdown_follow_the_unified_runtime_contract() {
        let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "test",
            "market-runtime",
        ));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: calls.clone(),
                shutdown_error: None,
                shutdown_delay: Duration::ZERO,
                publish_on_run: false,
                snapshot_delay: Duration::ZERO,
            }),
            runtime_context(&scope),
        );
        runtime.start().unwrap();
        let request = MarketSubscribeRequest {
            asset_id: AssetId(7),
            kinds: Arc::from([MarketDataKind::Depth]),
        };
        let first = runtime.subscribe(request.clone()).unwrap();
        let second = runtime.subscribe(request).unwrap();
        let snapshot = runtime.request_snapshot(AssetId(7)).unwrap();
        let first_release = runtime.unsubscribe(first).unwrap();
        let second_release = runtime.unsubscribe(second).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline
            && (runtime.operation(snapshot).state == OperationState::Pending
                || runtime.operation(second_release).state == OperationState::Pending)
        {
            std::thread::yield_now();
        }
        assert_eq!(runtime.operation(snapshot).state, OperationState::Succeeded);
        assert_eq!(
            runtime.operation(first_release).state,
            OperationState::Succeeded
        );
        assert_eq!(
            runtime.operation(second_release).state,
            OperationState::Succeeded
        );
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.starts_with("subscribe:"))
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.starts_with("unsubscribe:"))
                .count(),
            1
        );
        assert_eq!(
            calls.iter().filter(|call| *call == "snapshot:BTC").count(),
            2,
            "one snapshot is for the shared consumer and one is explicitly requested"
        );
        assert_eq!(calls.iter().filter(|call| *call == "shutdown").count(), 1);
    }

    #[test]
    fn direct_publication_queue_full_degrades_and_requests_venue_recovery() {
        let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "test",
            "market-recovery",
        ));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: calls.clone(),
                shutdown_error: None,
                shutdown_delay: Duration::ZERO,
                publish_on_run: true,
                snapshot_delay: Duration::ZERO,
            }),
            runtime_context_with_sink(&scope, Arc::new(RejectMarketSink)),
        );
        runtime
            .subscribe(MarketSubscribeRequest {
                asset_id: AssetId(7),
                kinds: Arc::from([MarketDataKind::Depth]),
            })
            .unwrap();
        runtime.start().unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline
            && !calls
                .lock()
                .unwrap()
                .iter()
                .any(|call| call == "snapshot:BTC")
        {
            std::thread::yield_now();
        }
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .any(|call| call == "snapshot:BTC")
        );
        assert_eq!(runtime.health().state, ConnectorHealth::Degraded);
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn shutdown_failure_is_not_misreported_as_stopped() {
        let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "test",
            "market-shutdown",
        ));
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: Arc::new(Mutex::new(Vec::new())),
                shutdown_error: Some("injected shutdown failure".to_owned()),
                shutdown_delay: Duration::ZERO,
                publish_on_run: false,
                snapshot_delay: Duration::ZERO,
            }),
            runtime_context(&scope),
        );
        runtime.start().unwrap();
        let error = runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap_err();
        assert!(error.to_string().contains("injected shutdown failure"));
        assert_eq!(runtime.health().state, ConnectorHealth::Failed);
    }

    #[test]
    fn rejected_restart_does_not_leave_the_running_admission_latched() {
        let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "test",
            "market-restart",
        ));
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: Arc::new(Mutex::new(Vec::new())),
                shutdown_error: None,
                shutdown_delay: Duration::ZERO,
                publish_on_run: false,
                snapshot_delay: Duration::ZERO,
            }),
            runtime_context(&scope),
        );
        runtime.start().unwrap();
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();

        for _ in 0..2 {
            let error = runtime.start().unwrap_err();
            assert!(error.to_string().contains("cannot be restarted"));
            assert!(!runtime.running.load(Ordering::Acquire));
        }
        assert_eq!(runtime.health().state, ConnectorHealth::Failed);
    }

    #[test]
    fn shutdown_deadline_is_bounded_and_a_later_stop_reaps_the_runtime() {
        let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "test",
            "market-shutdown-deadline",
        ));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: calls.clone(),
                shutdown_error: None,
                shutdown_delay: Duration::from_millis(100),
                publish_on_run: false,
                snapshot_delay: Duration::ZERO,
            }),
            runtime_context(&scope),
        );
        runtime.start().unwrap();

        let started = Instant::now();
        let error = runtime
            .stop(Instant::now() + Duration::from_millis(5))
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(80));
        assert!(error.to_string().contains("deadline"));
        assert_eq!(runtime.health().state, ConnectorHealth::Failed);

        // The first call deliberately retains the still-running JoinHandle. A later bounded
        // cleanup must reap it instead of detaching the connector thread or calling shutdown a
        // second time.
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(calls.lock().unwrap().as_slice(), &["shutdown"]);
        assert!(runtime.runtime.lock().unwrap().is_none());
    }

    #[test]
    fn command_pressure_is_bounded_and_scope_release_drops_the_runtime() {
        let mut scope = titan_core_types::ResourceScope::new(
            titan_core_types::ComponentIdentity::new("test", "market-pressure"),
        );
        let runtime = MarketConnectorRuntime::new(
            Box::new(RuntimeProbeConnector {
                calls: Arc::new(Mutex::new(Vec::new())),
                shutdown_error: None,
                shutdown_delay: Duration::ZERO,
                publish_on_run: false,
                snapshot_delay: Duration::from_millis(10),
            }),
            runtime_context(&scope),
        );
        runtime
            .subscribe(MarketSubscribeRequest {
                asset_id: AssetId(7),
                kinds: Arc::from([MarketDataKind::Depth]),
            })
            .unwrap();
        runtime.start().unwrap();

        let mut accepted = Vec::new();
        let mut rejected = false;
        for _ in 0..(COMMAND_QUEUE_CAPACITY * 4) {
            match runtime.request_snapshot(AssetId(7)) {
                Ok(operation) => accepted.push(operation),
                Err(_) => {
                    rejected = true;
                    break;
                }
            }
        }
        assert!(rejected, "bounded command queue must expose pressure");
        assert!(accepted.len() <= COMMAND_QUEUE_CAPACITY + 1);
        assert!(
            runtime
                .operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values
                .len()
                <= OPERATION_HISTORY_LIMIT + 1
        );

        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert!(
            accepted.iter().all(|operation| {
                runtime.operation(*operation).state != OperationState::Pending
            })
        );
        let weak = Arc::downgrade(&runtime);
        drop(runtime);
        scope.close().unwrap();
        assert!(weak.upgrade().is_none());
    }
}
