use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use semver::Version;
use titan_event_engine::{EventClass, EventEngine, EventEngineConfig, PoolKind};
use titan_plugin_engine::{
    ApiVersion, DispatchOutcome, EventControl, EventHandler, EventQos, EventView, ExecutionModel,
    ExecutionSpec, PluginEngine, PluginError, PluginIdentity, PluginSpec, ServiceKey, ServiceScope,
    StopReason, SubscriptionLimits, SubscriptionSpec, TraceContext,
};
use titan_runtime_abi::{BAR_COMPLETE, Bar};

use crate::*;

struct FakeConnector {
    context: MarketConnectorContext,
    running: AtomicBool,
    next_id: AtomicU64,
    fail_start: bool,
    stop_calls: Option<Arc<AtomicU64>>,
}

impl MarketConnector for FakeConnector {
    fn start(&self) -> Result<(), ConnectorError> {
        if self.fail_start {
            return Err(ConnectorError::new("injected start failure"));
        }
        self.running.store(true, Ordering::Release);
        let header = MarketBatchHeaderV1 {
            asset_id: self.context.instruments[0].asset_id.0,
            stream_epoch: 1,
            first_update_sequence: 1,
            last_update_sequence: 1,
            ..MarketBatchHeaderV1::default()
        };
        let payload = encode_depth_batch(
            header,
            &[DepthItemV1 {
                price_ticks: 100,
                quantity_lots: 2,
                side: 1,
                action: 1,
                ..DepthItemV1::default()
            }],
        )
        .unwrap();
        self.context
            .event_publisher
            .publish_market(
                DEPTH_BATCH_EVENT,
                &payload,
                self.context.instruments[0].asset_id,
                10,
                11,
                TraceContext::default(),
            )
            .map_err(|error| ConnectorError::new(error.to_string()))
    }
    fn stop(&self, _: Instant) -> Result<(), ConnectorError> {
        if let Some(calls) = &self.stop_calls {
            calls.fetch_add(1, Ordering::AcqRel);
        }
        self.running.store(false, Ordering::Release);
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
        Ok(MarketSubscription {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
        })
    }
    fn unsubscribe(&self, _: MarketSubscription) -> Result<OperationId, ConnectorError> {
        Ok(OperationId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }
    fn request_snapshot(&self, _: AssetId) -> Result<OperationId, ConnectorError> {
        Ok(OperationId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }
    fn instruments(&self) -> Arc<[InstrumentSnapshot]> {
        self.context
            .instruments
            .iter()
            .map(|binding| InstrumentSnapshot {
                native_symbol: binding.native_symbol.clone(),
                asset_id: binding.asset_id,
                available: true,
            })
            .collect::<Vec<_>>()
            .into()
    }
    fn health(&self) -> ConnectorHealthSnapshot {
        ConnectorHealthSnapshot {
            state: if self.running.load(Ordering::Acquire) {
                ConnectorHealth::Running
            } else {
                ConnectorHealth::Stopped
            },
            message: Arc::from("fake"),
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot {
        ConnectorDiagnosticSnapshot {
            summary: Arc::from("fake"),
        }
    }
    fn operation(&self, id: OperationId) -> ConnectorOperationSnapshot {
        ConnectorOperationSnapshot {
            id,
            state: OperationState::Succeeded,
            detail: Arc::from("fake"),
        }
    }
}

struct FakeFactory;
impl MarketConnectorFactory for FakeFactory {
    fn connector_type(&self) -> &str {
        "fake"
    }
    fn create(
        &self,
        _: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        Ok(Arc::new(FakeConnector {
            context,
            running: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            fail_start: false,
            stop_calls: None,
        }))
    }
}

struct StartFailFactory {
    stop_calls: Arc<AtomicU64>,
}

impl MarketConnectorFactory for StartFailFactory {
    fn connector_type(&self) -> &str {
        "start-fail"
    }

    fn create(
        &self,
        _: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        Ok(Arc::new(FakeConnector {
            context,
            running: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            fail_start: true,
            stop_calls: Some(self.stop_calls.clone()),
        }))
    }
}

struct RejectFactory;
impl MarketConnectorFactory for RejectFactory {
    fn connector_type(&self) -> &str {
        "reject"
    }
    fn create(
        &self,
        _: &MarketSourceDefinition,
        _: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        Err(ConnectorError::new("injected create failure"))
    }
}

fn definition(key: &str, asset: u32) -> MarketSourceDefinition {
    MarketSourceDefinition {
        source_key: Arc::from(key),
        connector_type: Arc::from("fake"),
        connector_config: Arc::from([]),
        instruments: Arc::from([MarketInstrumentBinding {
            native_symbol: Arc::from("BTCUSDT"),
            asset_id: AssetId(asset),
            price_tick: 0.1,
            quantity_lot: 0.001,
        }]),
        enabled: true,
        definition_version: 1,
    }
}

fn plugin_spec() -> PluginSpec {
    PluginSpec {
        instance_id: Arc::from("market"),
        plugin_type: Arc::from(MARKET_PLUGIN_TYPE),
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
            max_capacity: 16,
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
    }
}

fn admin_key() -> ServiceKey {
    ServiceKey {
        id: titan_plugin_engine::ServiceId::new("titan.market", "admin"),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}
fn market_key() -> ServiceKey {
    ServiceKey {
        id: titan_plugin_engine::ServiceId::new("titan.market", "market"),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}

fn admin_call(
    engine: &PluginEngine,
    request: MarketAdminRequest,
) -> LocalResult<MarketAdminResponse> {
    *engine
        .services()
        .bind(&admin_key())
        .unwrap()
        .call(Box::new(request), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<MarketAdminResponse>>()
        .unwrap()
}
fn market_call(engine: &PluginEngine, request: MarketRequest) -> LocalResult<MarketResponse> {
    *engine
        .services()
        .bind(&market_key())
        .unwrap()
        .call(Box::new(request), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<MarketResponse>>()
        .unwrap()
}

struct RecordingHandler(std::sync::mpsc::Sender<Vec<u8>>);
impl EventHandler for RecordingHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.0.send(event.payload.to_vec()).unwrap();
        Ok(())
    }
}

#[test]
fn abi_has_stable_little_endian_encoding() {
    let header = MarketBatchHeaderV1 {
        asset_id: 7,
        ..Default::default()
    };
    let item = DepthItemV1 {
        price_ticks: -2,
        quantity_lots: 3,
        side: 1,
        action: 2,
        ..Default::default()
    };
    let payload = encode_depth_batch(header, &[item]).unwrap();
    assert_eq!(
        payload.len(),
        MarketBatchHeaderV1::ENCODED_LEN + DepthItemV1::ENCODED_LEN
    );
    assert_eq!(&payload[..4], &7_u32.to_le_bytes());
    assert_eq!(&payload[8..10], &1_u16.to_le_bytes());
    assert_eq!(&payload[52..60], &(-2_i64).to_le_bytes());

    let mut direct = vec![0_u8; payload.len()];
    MarketBatchHeaderV1 {
        item_count: 1,
        ..header
    }
    .encode_into_slice(&mut direct)
    .unwrap();
    item.encode_into_slice(&mut direct[MarketBatchHeaderV1::ENCODED_LEN..])
        .unwrap();
    assert_eq!(direct, payload);
}

#[test]
fn plugin_registry_services_generation_and_direct_event_path_work() {
    let mut config = EventEngineConfig::default();
    config.ingress.max_sources = 32;
    config.subscribers.default_capacity = 16;
    config.subscribers.critical_reserve = 2;
    let event_engine = EventEngine::new(config).unwrap();
    let event_handle = event_engine.handle();
    for event_type in MARKET_EVENT_TYPES {
        event_handle
            .register_event(event_type, 1, EventClass::Market, PoolKind::MarketBatch)
            .unwrap();
    }
    event_engine.start().unwrap();

    let route = event_handle
        .begin_route_update(event_handle.current_route_version())
        .unwrap();
    event_handle
        .stage_subscription(
            route,
            &PluginIdentity::new("test", "consumer"),
            &SubscriptionSpec {
                event_type: Arc::from(DEPTH_BATCH_EVENT),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                capacity: 8,
                routing_keys: Arc::from([1001]),
            },
        )
        .unwrap();
    let (_, mut subscriptions) = event_handle.commit_at_safe_point(route).unwrap();
    let receiver = subscriptions.pop().unwrap().receiver;
    let (tx, rx) = std::sync::mpsc::channel();
    let consumer = std::thread::spawn(move || {
        let handler = RecordingHandler(tx);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if receiver
                .dispatch_next(&handler, Duration::from_millis(10))
                .unwrap()
                == DispatchOutcome::Delivered
            {
                return;
            }
        }
        panic!("event was not delivered");
    });

    let mut plugin_engine =
        PluginEngine::new(Arc::new(event_handle.clone()), ApiVersion::new(1, 0)).unwrap();
    plugin_engine
        .register(
            Arc::new(
                MarketPluginFactory::new()
                    .with_factory(Arc::new(FakeFactory))
                    .with_factory(Arc::new(RejectFactory)),
            ),
            Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugin_engine.apply(&[plugin_spec()]).unwrap();
    assert!(
        matches!(admin_call(&plugin_engine, MarketAdminRequest::List).unwrap(), MarketAdminResponse::Sources(values) if values.is_empty())
    );

    let handle = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Create(definition("primary", 1001)),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(value) => value,
        _ => panic!(),
    };
    assert!(
        matches!(market_call(&plugin_engine, MarketRequest::Resolve(Arc::from("primary"))).unwrap(), MarketResponse::Handle(value) if value == handle)
    );
    admin_call(&plugin_engine, MarketAdminRequest::Start(handle)).unwrap();
    let payload = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(&payload[..4], &1001_u32.to_le_bytes());
    consumer.join().unwrap();

    admin_call(
        &plugin_engine,
        MarketAdminRequest::Stop(handle, Instant::now() + Duration::from_secs(1)),
    )
    .unwrap();
    admin_call(&plugin_engine, MarketAdminRequest::Remove(handle)).unwrap();
    assert_eq!(
        market_call(&plugin_engine, MarketRequest::Resolve(Arc::from("primary")))
            .unwrap_err()
            .kind,
        MarketErrorKind::SourceNotFound
    );
    let recreated = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Create(definition("primary", 1001)),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(value) => value,
        _ => panic!(),
    };
    assert!(recreated.generation > handle.generation);
    assert_eq!(
        market_call(&plugin_engine, MarketRequest::Health(handle))
            .unwrap_err()
            .kind,
        MarketErrorKind::StaleHandle
    );
    let mut replacement = definition("primary", 1001);
    replacement.definition_version = 2;
    let replaced = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Replace(recreated, replacement),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(value) => value,
        _ => panic!(),
    };
    assert!(replaced.generation > recreated.generation);
    assert_eq!(
        market_call(&plugin_engine, MarketRequest::Health(recreated))
            .unwrap_err()
            .kind,
        MarketErrorKind::StaleHandle
    );

    plugin_engine.shutdown(StopReason::Shutdown).unwrap();
    event_engine.stop().unwrap();
}

#[test]
fn duplicate_assets_capacity_and_failed_factory_are_rolled_back() {
    let event_engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    let event_handle = event_engine.handle();
    event_engine.start().unwrap();
    let mut plugin_engine =
        PluginEngine::new(Arc::new(event_handle), ApiVersion::new(1, 0)).unwrap();
    plugin_engine
        .register(
            Arc::new(
                MarketPluginFactory::new()
                    .with_factory(Arc::new(FakeFactory))
                    .with_factory(Arc::new(RejectFactory)),
            ),
            Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugin_engine.apply(&[plugin_spec()]).unwrap();
    let mut missing = definition("missing", 4);
    missing.connector_type = Arc::from("missing");
    assert_eq!(
        admin_call(&plugin_engine, MarketAdminRequest::Create(missing))
            .unwrap_err()
            .kind,
        MarketErrorKind::FactoryNotFound
    );
    let mut rejected = definition("rejected", 4);
    rejected.connector_type = Arc::from("reject");
    assert_eq!(
        admin_call(&plugin_engine, MarketAdminRequest::Create(rejected))
            .unwrap_err()
            .kind,
        MarketErrorKind::ConnectorRejected
    );
    assert!(
        matches!(admin_call(&plugin_engine, MarketAdminRequest::List).unwrap(), MarketAdminResponse::Sources(values) if values.is_empty())
    );
    let mut invalid_units = definition("invalid-units", 9);
    Arc::make_mut(&mut invalid_units.instruments)[0].price_tick = 0.0;
    assert_eq!(
        admin_call(&plugin_engine, MarketAdminRequest::Create(invalid_units))
            .unwrap_err()
            .kind,
        MarketErrorKind::InvalidDefinition
    );
    let one = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Create(definition("one", 1)),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(handle) => handle,
        _ => panic!(),
    };
    assert_eq!(
        admin_call(
            &plugin_engine,
            MarketAdminRequest::Create(definition("two", 1))
        )
        .unwrap_err()
        .kind,
        MarketErrorKind::AlreadyExists
    );
    let _two = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Create(definition("two", 2)),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(handle) => handle,
        _ => panic!(),
    };
    assert_eq!(
        admin_call(
            &plugin_engine,
            MarketAdminRequest::Create(definition("three", 3))
        )
        .unwrap_err()
        .kind,
        MarketErrorKind::CapacityExceeded
    );
    admin_call(&plugin_engine, MarketAdminRequest::Remove(one)).unwrap();
    let three = match admin_call(
        &plugin_engine,
        MarketAdminRequest::Create(definition("three", 3)),
    )
    .unwrap()
    {
        MarketAdminResponse::Handle(handle) => handle,
        _ => panic!(),
    };
    assert_eq!(three.source_id, one.source_id);
    assert!(three.generation > one.generation);
    assert_eq!(
        market_call(&plugin_engine, MarketRequest::Health(one))
            .unwrap_err()
            .kind,
        MarketErrorKind::StaleHandle
    );
    plugin_engine.shutdown(StopReason::Shutdown).unwrap();
    event_engine.stop().unwrap();
}

#[test]
fn duplicate_factory_registration_is_rejected() {
    let core = MarketPluginCore::new(MarketPluginConfig::default());
    core.register_factory(Arc::new(FakeFactory)).unwrap();
    assert_eq!(
        core.register_factory(Arc::new(FakeFactory))
            .unwrap_err()
            .kind,
        MarketErrorKind::AlreadyExists
    );
}

#[test]
fn connector_start_failure_is_retained_for_diagnostics_and_core_shutdown_releases_it() {
    let events = EventEngine::new(EventEngineConfig::default()).unwrap();
    events.start().unwrap();
    let stop_calls = Arc::new(AtomicU64::new(0));
    let mut plugins = PluginEngine::new(Arc::new(events.handle()), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(
                MarketPluginFactory::new().with_factory(Arc::new(StartFailFactory {
                    stop_calls: stop_calls.clone(),
                })),
            ),
            Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugins.apply(&[plugin_spec()]).unwrap();

    let mut candidate = definition("failed", 71);
    candidate.connector_type = Arc::from("start-fail");
    let handle = match admin_call(&plugins, MarketAdminRequest::Create(candidate)).unwrap() {
        MarketAdminResponse::Handle(handle) => handle,
        _ => panic!("unexpected create response"),
    };
    assert_eq!(
        admin_call(&plugins, MarketAdminRequest::Start(handle))
            .unwrap_err()
            .kind,
        MarketErrorKind::ConnectorRejected
    );
    assert!(matches!(
        admin_call(&plugins, MarketAdminRequest::List).unwrap(),
        MarketAdminResponse::Sources(values)
            if values.len() == 1 && values[0].lifecycle == ConnectorLifecycle::Failed
    ));

    plugins.shutdown(StopReason::Failure).unwrap();
    events.stop().unwrap();
    assert_eq!(stop_calls.load(Ordering::Acquire), 1);
    assert_eq!(events.arena().outstanding_blocks(), 0);
}

#[test]
fn closed_bar_batch_v1_round_trips_and_rejects_partial_or_mismatched_bars() {
    let batch = BarBatchV1 {
        timeframe_ns: 60,
        close_ts: 120,
        items: vec![BarRecordV1 {
            asset_id: 7,
            bar: Bar {
                open_ts: 60,
                close_ts: 120,
                open: 1.0,
                high: 3.0,
                low: 0.5,
                close: 2.0,
                volume: 4.0,
                quote_volume: 8.0,
                buy_volume: 2.5,
                trade_count: 9,
                flags: BAR_COMPLETE,
            },
        }],
    };
    let encoded = batch.encode().unwrap();
    assert_eq!(encoded.len(), BarBatchV1::HEADER_LEN + BarBatchV1::ITEM_LEN);
    assert_eq!(BarBatchV1::decode(&encoded).unwrap(), batch);

    let mut partial = batch.clone();
    partial.items[0].bar.flags = titan_runtime_abi::BAR_PARTIAL;
    assert!(partial.encode().is_err());
    let mut mismatched = batch;
    mismatched.items[0].bar.close_ts += 1;
    assert!(mismatched.encode().is_err());
}
