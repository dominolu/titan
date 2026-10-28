//! Receive-to-native-strategy benchmark using the real MarketService, EventEngine PRIMARY async
//! lane and NativeV13Runtime. Public data only; the strategy has no account or execution binding.
#[path = "market_local_latency_probe/compiled_strategy.rs"]
mod compiled_strategy;
use anyhow::{Result, ensure};
use chrono::Utc;
use market::{MarketAdminService, MarketService};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use strategy::{StrategyRecoveryStore, StrategyRuntimeFactory};
use titan_core_types::{
    ActivationGate, ApiVersion, ComponentIdentity, EventPublisher, EventQos, ResourceScope,
};
use titan_event_engine::*;
use titan_market_service as market;
use titan_strategy_runtime as strategy;

// Allocation audit is enabled only during warmup; steady-state latency sampling has counting off.
struct ProbeAllocator;
static COUNT_ALLOC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static ALLOCATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[global_allocator]
static ALLOCATOR: ProbeAllocator = ProbeAllocator;
unsafe impl std::alloc::GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        if COUNT_ALLOC.load(std::sync::atomic::Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        if COUNT_ALLOC.load(std::sync::atomic::Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        unsafe { std::alloc::System.realloc(p, layout, size) }
    }
}
struct Clock;
impl strategy::StrategyClock for Clock {
    fn now_ns(&self) -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap()
    }
}
struct Metrics;
impl strategy::StrategyMetrics for Metrics {}
#[derive(Default)]
struct Sink(Mutex<Option<strategy::StrategyRecoveryPoint>>);
impl StrategyRecoveryStore for Sink {
    fn submit(&self, value: strategy::StrategyRecoveryPoint) -> strategy::LocalResult<()> {
        *self.0.lock().unwrap() = Some(value);
        Ok(())
    }
}
fn wait(runtime: &dyn strategy::StrategyRuntime, id: strategy::StrategyOperationId) -> Result<()> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let op = runtime.operation(id);
        match op.state {
            strategy::StrategyOperationState::Succeeded => return Ok(()),
            strategy::StrategyOperationState::Failed => {
                anyhow::bail!("strategy operation: {}", op.detail)
            }
            _ => {
                ensure!(Instant::now() < end, "operation timeout");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
fn stats(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let n = values.len();
    if n == 0 {
        return Value::Null;
    }
    let p = |q: f64| values[((n as f64 * q).ceil() as usize).saturating_sub(1)];
    json!({"n":n,"min":values[0],"mean":values.iter().sum::<f64>()/n as f64,"p50":p(0.5),"p95":p(0.95),"p99":p(0.99),"max":values[n-1],"negative":values.iter().filter(|v|**v<0.0).count()})
}
fn run(compiled_strategy: &Path, venue: &str, seconds: u64, output: &Path) -> Result<()> {
    fs::create_dir_all(output)?;
    let (connector_type, symbol, tick, lot, mut config) = match venue {
        "gate-legacy" | "gate-borrowed" => (
            "gate-crossex",
            "GATE_FUTURE_BTC_USDT",
            "0.1",
            "0.0001",
            format!("borrowed_depth = {}\n", venue == "gate-borrowed"),
        ),
        "crossex-binance" => (
            "gate-crossex",
            "BINANCE_FUTURE_BTC_USDT",
            "0.1",
            "0.001",
            String::new(),
        ),
        "binance" => anyhow::bail!(
            "native Binance's 2000-level initial snapshot exceeds this probe's bounded arena/view configuration; configure broker-specific capacities before enabling this mode; use crossex-binance for five-level gateway measurements"
        ),
        _ => anyhow::bail!("unsupported probe venue"),
    };
    if let Ok(url) = std::env::var("TITAN_L2_PROBE_LOOPBACK") {
        ensure!(
            venue.starts_with("gate-") && url.starts_with("ws://127.0.0.1:"),
            "loopback override is restricted to a local mock"
        );
        config.push_str(&format!("public_ws_url = {url:?}\n[market_quantity_multipliers]\nGATE_FUTURE_BTC_USDT = 0.0001\n"));
    }
    let mut cfg = EventEngineConfig::default();
    cfg.arena.market_batch.slots = 128;
    cfg.arena.market_batch.block_bytes = 32768;
    cfg.arena.market_batch.low_watermark = 8;
    cfg.runtime.spin_iterations = 0;
    let engine = EventEngine::new(cfg)?;
    let events = engine.handle();
    for event in market::MARKET_EVENT_TYPES {
        events.register_event(event, 1, EventClass::Market, PoolKind::MarketBatch)?;
    }
    engine.start()?;
    let gate = Arc::new(ActivationGate::new());
    gate.activate();
    let identity = ComponentIdentity::new("probe", "public-market");
    let allowed = market::MARKET_EVENT_TYPES
        .into_iter()
        .map(|e| (Arc::from(e), BTreeSet::from([1])))
        .collect::<BTreeMap<_, _>>();
    let publisher = EventPublisher::new(identity.clone(), allowed, gate, Arc::new(events.clone()));
    let markets = market::MarketServiceCore::new(market::MarketCoreConfig::default());
    for factory in connector::market_runtime::venue_market_factories() {
        markets.register_factory(factory)?;
    }
    markets.activate(identity, publisher);
    let source = markets.create(market::MarketSourceDefinition {
        source_key: Arc::from("probe-source"),
        connector_type: Arc::from(connector_type),
        connector_config: Arc::from(config.as_bytes()),
        instruments: Arc::from([market::MarketInstrumentBinding {
            native_symbol: Arc::from(symbol),
            asset_id: market::AssetId(1),
            price_tick: tick.parse().map_err(anyhow::Error::msg)?,
            quantity_lot: lot.parse().map_err(anyhow::Error::msg)?,
        }]),
        enabled: true,
        definition_version: 1,
    })?;
    let loader = strategy::NativeStrategyLoaderV13::new(
        output.join("cache"),
        strategy::V13TrustPolicy::default(),
    );
    let native = Arc::new(loader.load(compiled_strategy)?);
    let id = strategy::CompiledStrategyDigest {
        digest: native.manifest.compiled_strategy_digest,
    };
    let subscription = strategy::StrategySubscriptionSpec {
        event_type: Arc::from(market::DEPTH_BATCH_EVENT),
        schema_version: 1,
        routing_keys: Arc::from([1]),
        qos: EventQos::ReliableOrdered,
    };
    let package = strategy::StrategyFileRef {
        loader_type: Arc::from("native-v13"),
        uri: Arc::from(compiled_strategy.to_string_lossy().as_ref()),
        expected_digest: id.digest,
        signature_ref: None,
    };
    let handle = strategy::StrategyHandle {
        runtime_strategy_no: strategy::RuntimeStrategyNo(777),
        generation: 1,
    };
    let runtime_config = strategy::StrategyRuntimeSpec {
        checkpoint_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let definition = strategy::StrategyDefinition {
        strategy_key: Arc::from("no-order-latency"),
        runtime_strategy_no: handle.runtime_strategy_no,
        strategy_file: package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: strategy::RiskScopeRef(Arc::from("no-orders")),
        runtime: runtime_config,
        recovery: strategy::StrategyRecoveryPolicy::Fresh,
        enabled: true,
        run_config_revision: 1,
    };
    let sink = Arc::new(Sink::default());
    let scope = ResourceScope::new(ComponentIdentity::new("probe", "strategy"));
    let native_strategy = strategy::LoadedStrategy {
        id,
        manifest: strategy::CompiledStrategyMetadata {
            runtime_type: Arc::from("native-v13"),
            source_version: "1.0.0".parse()?,
            runtime_abi: ApiVersion::new(1, 0),
            parameter_schema: Arc::new(json!({})),
            parameter_schema_version: 1,
            recovery_state_schema_version: 1,
            state_byte_len: native.manifest.state.byte_len,
            callbacks: strategy::StrategyCallbackMask(8),
            capabilities: strategy::StrategyCapabilities::READ_DEPTH,
            subscriptions: Arc::from([subscription]),
            compiled_strategy_digest: id.digest,
        },
        native,
    };
    let runtime = strategy::NativeV13RuntimeFactory.create(
        &definition,
        native_strategy,
        strategy::StrategyRuntimeBuildContext {
            market_service: None,
            strategy: handle,
            compiled_strategy_digest: id,
            markets: Arc::from([strategy::ResolvedMarketBinding {
                source_stream: markets.source_stream(source)?,
                local_market_no: 0,
                local_asset_no: 0,
                source,
                asset_id: 1,
                data_mode: strategy::StrategyDataMode::Tick,
            }]),
            accounts: Arc::from([]),
            execution: Arc::from([]),
            state_snapshot_sink: sink.clone(),
            clock: Arc::new(Clock),
            metrics: Arc::new(Metrics),
            resources: scope.handle(),
            activation: Arc::new(strategy::StrategyActivationGate::default()),
        },
    )?;
    let lane = events.register_primary_async_lane(
        &[PrimarySubscriptionSpec {
            event_type: Arc::from(market::DEPTH_BATCH_EVENT),
            schema_version: 1,
            qos: EventQos::ReliableOrdered,
            routing_keys: Arc::from([1]),
        }],
        PrimaryAsyncLaneConfig {
            capacity: 256,
            critical_reserve: 16,
            reliable_pending_capacity: 64,
            snapshot_staging_capacity: 256,
            control_capacity: 16,
            runtime_mode: SubscriberRuntimeMode::SpinSleep,
            spin_iterations: 256,
            idle_sleep: Duration::from_micros(10),
            max_handler_duration: Duration::from_millis(5),
            ..Default::default()
        },
        runtime.clone(),
    )?;
    runtime.attach_lane(lane.clone())?;
    wait(runtime.as_ref(), runtime.prepare()?)?;
    wait(runtime.as_ref(), runtime.start()?)?;
    markets.subscribe(
        source,
        market::MarketSubscribeRequest {
            asset_id: market::AssetId(1),
            kinds: Arc::from([market::MarketDataKind::Depth]),
        },
    )?;
    markets.start(source)?;
    let wall_start = Utc::now().timestamp_nanos_opt().unwrap();
    let end = Instant::now() + Duration::from_secs(seconds + 10);
    std::thread::sleep(Duration::from_secs(5));
    let audit_callbacks_start = lane.progress().committed_sequence;
    let audit_start = ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed);
    COUNT_ALLOC.store(true, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(Duration::from_secs(5));
    COUNT_ALLOC.store(false, std::sync::atomic::Ordering::Relaxed);
    let audit_allocations = ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed) - audit_start;
    let audit_callbacks = lane.progress().committed_sequence - audit_callbacks_start;
    while Instant::now() < end {
        std::thread::sleep(
            Duration::from_secs(30).min(end.saturating_duration_since(Instant::now())),
        );
        eprintln!(
            "{}",
            json!({"venue":venue,"lane_progress":format!("{:?}",lane.progress()),"strategy_state":format!("{:?}",runtime.state().lifecycle)})
        );
    }
    markets.stop(source, Instant::now() + Duration::from_secs(10))?;
    let barrier = lane.submit_safe_point(|| Ok(()))?;
    barrier.wait(Instant::now() + Duration::from_secs(10))?;
    wait(
        runtime.as_ref(),
        runtime.freeze_state(strategy::StrategyStateSnapshotRequest { checkpoint_id: 1 })?,
    )?;
    wait(
        runtime.as_ref(),
        runtime.stop(Instant::now() + Duration::from_secs(10))?,
    )?;
    let snapshot = sink
        .0
        .lock()
        .unwrap()
        .clone()
        .context("missing state snapshot")?;
    let state = &snapshot.state_bytes;
    let word = |offset: usize| u64::from_le_bytes(state[offset..offset + 8].try_into().unwrap());
    let n = word(0) as usize;
    ensure!(n <= compiled_strategy::CAPACITY, "probe state overflow");
    let mut csv = fs::File::create(output.join("samples.csv"))?;
    writeln!(csv, "receive_ns,strategy_entry_ns,depth_levels,latency_ns")?;
    let mut latencies = Vec::new();
    for i in 0..n {
        let o = 8 + i * 24;
        let receive = word(o) as i64;
        let entry = word(o + 8) as i64;
        let count = word(o + 16);
        if receive < wall_start + 10_000_000_000
            || receive >= wall_start + (seconds as i64 + 10) * 1_000_000_000
        {
            continue;
        }
        let latency = entry - receive;
        writeln!(csv, "{receive},{entry},{count},{latency}")?;
        latencies.push(latency as f64 / 1000.0);
    }
    let metrics = engine.metrics().snapshot();
    let result = json!({"venue":venue,"symbol":symbol,"seconds":seconds,"warmup_seconds":10,"account_used":false,"allocation_audit":{"warmup_seconds":5,"rust_alloc_or_realloc_calls":audit_allocations,"callbacks":audit_callbacks,"scope":"whole probe process; counting disabled during latency window"},"path":"WS frame before JSON -> connector -> MarketEventBridge -> EventArena -> PRIMARY async lane -> NativeV13Runtime -> native on_depth entry","clock":"same-host CLOCK_REALTIME; exchange timestamps excluded","latency_us":stats(latencies),"callbacks_total":n,"drop_total":metrics.drop_total,"resync_total":metrics.resync_total,"lane_progress":format!("{:?}",lane.progress()),"strategy_state":format!("{:?}",runtime.state().lifecycle)});
    fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    println!("{result}");
    ensure!(
        result["latency_us"]["n"].as_u64().unwrap_or(0) > 0,
        "no measured callbacks"
    );
    ensure!(
        metrics.drop_total == 0 && metrics.resync_total == 0,
        "pipeline dropped or resynced"
    );
    events.unregister_primary_async_lane_before(
        lane.token(),
        Instant::now() + Duration::from_secs(10),
    )?;
    engine.stop()?;
    Ok(())
}
use anyhow::Context;
fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 3 && args[1] == "--build-compiled_strategy" {
        return compiled_strategy::build(Path::new(&args[2]));
    }
    ensure!(
        args.len() == 5,
        "usage: market_local_latency_probe ARTIFACT VENUE SECONDS OUTPUT_DIR; build the compiled_strategy on the build server with --build-compiled_strategy DIR"
    );
    let seconds: u64 = args[3].parse()?;
    ensure!((3..=120).contains(&seconds), "duration must be 3..120");
    run(
        &PathBuf::from(&args[1]),
        &args[2],
        seconds,
        &PathBuf::from(&args[4]),
    )
}
