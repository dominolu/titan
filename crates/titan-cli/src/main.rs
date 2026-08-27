use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
    time::Instant,
};

mod registry;
use registry::{Registry, now_ns, process_start_time};

use clap::{Parser, Subcommand};
use hftbacktest::{
    backtest::{
        Backtest, DataSource,
        ExchangeKind::NoPartialFillExchange,
        L2AssetBuilder,
        assettype::LinearAsset,
        data::Data,
        execution::{ExecutionReport, FundingReport},
        models::{
            CommonFees, ConstantLatency, PowerProbQueueFunc3, ProbQueueModel, TradingValueFeeModel,
        },
        result::{AccountSnapshot, execution_report_counts},
    },
    depth::HashMapMarketDepth,
    live::{Instrument, LiveBotBuilder, ipc::iceoryx::IceoryxUnifiedChannel},
    prelude::Bot,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use titan_python_host::{EmbeddedPythonCompiler, StrategyCompiler, StrategySpec};
use titan_runtime::{
    CallbackRegistry, HybridFrameSource, MaterializedBarSource, RuntimeEvent, RuntimeEventSource,
    RuntimeRunStats, StrategyRuntimeContext, TickFrameSource, run_event_runtime_counted,
    runtime_abi_descriptor,
};
use titan_runtime_abi::{BAR_COMPLETE, BAR_NATIVE, Bar, Event, TimedBarItem};

const RUN_SPEC_VERSION: u32 = 1;

#[derive(Parser)]
#[command(
    name = "titan",
    version,
    about = "Titan controller and Rust strategy worker"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a RunSpec and execute it in an isolated worker process.
    Run {
        #[arg(value_name = "RUN_SPEC.json")]
        spec: PathBuf,
        #[arg(long)]
        detach: bool,
    },
    /// Internal worker entrypoint. It is not a stable user-facing interface.
    #[command(hide = true)]
    RunWorker {
        #[arg(long, value_name = "RUN_SPEC.json")]
        spec: PathBuf,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        owner_token: String,
        #[arg(long)]
        registry: PathBuf,
    },
    /// Validate a RunSpec without starting Python or a worker.
    Validate {
        #[arg(value_name = "RUN_SPEC.json")]
        spec: PathBuf,
    },
    /// List recorded runs.
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Show one run record.
    Show { run_id: String },
    /// Print the worker log for one run.
    Logs { run_id: String },
    /// Request termination of a detached worker.
    Stop { run_id: String },
    /// Render a completed ResultBundle without loading Python or Runtime.
    Report {
        run_id: String,
        /// Spawn the isolated Python renderer and write a native HTML report.
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, default_value = "native")]
        renderer: String,
    },
    /// Inspect or compile static strategy manifests.
    Strategy {
        #[command(subcommand)]
        command: StrategyCommands,
    },
}

#[derive(Subcommand)]
enum StrategyCommands {
    /// List manifests without importing Python.
    Ls,
    /// Show one static manifest.
    Show { name: String },
    /// Validate one static manifest without importing Python.
    Validate { name: String },
    /// Eagerly import and Numba-compile one strategy.
    Compile {
        name: String,
        #[arg(long, default_value = "{}")]
        parameters: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunSpec {
    schema_version: u32,
    strategy: StrategyRunSpec,
    backend: BackendSpec,
    #[serde(default = "default_history_capacity")]
    history_capacity: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrategyRunSpec {
    entrypoint: String,
    #[serde(default)]
    parameters: serde_json::Value,
    #[serde(default)]
    python_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrategyManifest {
    schema_version: u32,
    strategy_id: String,
    strategy_version: String,
    entrypoint: String,
    capabilities: Vec<String>,
    #[serde(default)]
    python_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendSpec {
    Bar {
        data: PathBuf,
    },
    Tick {
        data: PathBuf,
        #[serde(default = "default_tick_size")]
        tick_size: f64,
        #[serde(default = "default_lot_size")]
        lot_size: f64,
        #[serde(default = "default_frame_interval")]
        frame_interval_ns: i64,
        #[serde(default = "default_max_tick_batch")]
        max_tick_batch: usize,
    },
    Hybrid {
        tick_data: PathBuf,
        bar_data: PathBuf,
        #[serde(default = "default_tick_size")]
        tick_size: f64,
        #[serde(default = "default_lot_size")]
        lot_size: f64,
        #[serde(default = "default_frame_interval")]
        frame_interval_ns: i64,
        #[serde(default = "default_max_tick_batch")]
        max_tick_batch: usize,
    },
    Live {
        instruments: Vec<LiveInstrumentSpec>,
        #[serde(default = "default_frame_interval")]
        frame_interval_ns: i64,
        #[serde(default = "default_max_tick_batch")]
        max_tick_batch: usize,
        #[serde(default)]
        bot_id: Option<u64>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LiveInstrumentSpec {
    connector: String,
    symbol: String,
    tick_size: f64,
    lot_size: f64,
    #[serde(default = "default_last_trades_capacity")]
    last_trades_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarInput {
    asset_no: u64,
    timeframe_ns: i64,
    open_ts: i64,
    close_ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    #[serde(default)]
    volume: f64,
    #[serde(default)]
    quote_volume: f64,
    #[serde(default)]
    buy_volume: f64,
    #[serde(default)]
    trade_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TickInput {
    ev: u64,
    exch_ts: i64,
    local_ts: i64,
    px: f64,
    qty: f64,
    #[serde(default)]
    order_id: u64,
    #[serde(default)]
    ival: i64,
    #[serde(default)]
    fval: f64,
}

#[derive(Debug, Serialize)]
struct WorkerResult {
    schema_version: u32,
    strategy_id: String,
    strategy_version: String,
    abi_fingerprint: String,
    market_event_count: u64,
    callback_count: Vec<u64>,
    start_exchange_ts: i64,
    end_exchange_ts: i64,
    wall_time_ns: u64,
    order_count: u64,
    fill_count: u64,
    reject_count: u64,
    cancel_count: u64,
    expire_count: u64,
    execution_reports: Vec<ExecutionReportResult>,
    funding_reports: Vec<FundingReportResult>,
    exchange_final: Vec<AccountSnapshotResult>,
    local_delivered_final: Vec<AccountSnapshotResult>,
    returns: Vec<ReturnObservation>,
    state_f64: Vec<f64>,
    state_i64: Vec<i64>,
}

#[derive(Debug, Serialize)]
struct ExecutionReportResult {
    kind: String,
    reason: String,
    venue_no: u32,
    instrument_id: u32,
    asset_no: u32,
    order_id: u64,
    venue_order_id: u64,
    exchange_ts: i64,
    delivery_ts: i64,
    sequence: u64,
    status: String,
    side: String,
    order_price: f64,
    order_qty: f64,
    exec_price: f64,
    exec_qty: f64,
    maker: bool,
    account_delta: Option<AccountDeltaResult>,
}

#[derive(Debug, Serialize)]
struct AccountDeltaResult {
    instrument_id: u32,
    position_delta: f64,
    trade_qty: f64,
    trade_value: f64,
    currency: u32,
    cash_delta: f64,
    fee: f64,
    funding: f64,
    execution_price: f64,
    realized_pnl: f64,
}

#[derive(Debug, Serialize)]
struct FundingReportResult {
    event_id: u64,
    venue_no: u32,
    instrument_id: u32,
    currency: u32,
    exchange_ts: i64,
    delivery_ts: i64,
    sequence: u64,
    position_qty: f64,
    rate: f64,
    mark_price: f64,
    amount: f64,
}

#[derive(Debug, Serialize)]
struct AccountSnapshotResult {
    venue_no: u32,
    asset_no: u32,
    currency: u32,
    position: f64,
    balance: f64,
    fee: f64,
    funding: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    margin: f64,
}

#[derive(Debug, Serialize)]
struct ReturnObservation {
    timestamp_ns: i64,
    r#return: f64,
}

struct RuntimeExecutionOutcome {
    stats: RuntimeRunStats,
    execution_reports: Vec<ExecutionReport>,
    funding_reports: Vec<FundingReport>,
    exchange_final: Vec<AccountSnapshot>,
    local_delivered_final: Vec<AccountSnapshot>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BundleFile {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct BundleManifest {
    schema_version: u32,
    run_id: String,
    strategy_id: String,
    strategy_version: String,
    abi_fingerprint: String,
    committed_at_ns: i64,
    files: Vec<BundleFile>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid JSON in {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("unsupported RunSpec schema_version {0}; expected {RUN_SPEC_VERSION}")]
    Schema(u32),
    #[error("strategy entrypoint must use module:function syntax")]
    Entrypoint,
    #[error("history_capacity must be positive")]
    HistoryCapacity,
    #[error("worker spawn failed: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("worker exited with status {0}")]
    WorkerExit(std::process::ExitStatus),
    #[error("strategy compilation failed: {0}")]
    Compile(#[from] titan_python_host::PythonHostError),
    #[error("invalid callback descriptor: {0}")]
    Callback(#[from] titan_runtime::RuntimeError),
    #[error("invalid Bar input: {0}")]
    Bar(#[from] titan_runtime::MaterializedBarError),
    #[error("Tick/Hybrid engine configuration failed: {0}")]
    Engine(String),
    #[error("result serialization failed: {0}")]
    ResultJson(#[source] serde_json::Error),
    #[error("run registry failed: {0}")]
    Registry(#[from] rusqlite::Error),
    #[error("run {0} was not found")]
    RunNotFound(String),
    #[error("run {0} is not running")]
    NotRunning(String),
    #[error("cannot signal worker {pid}: {source}")]
    Signal { pid: u32, source: std::io::Error },
}

impl CliError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Schema(_)
            | Self::Entrypoint
            | Self::HistoryCapacity
            | Self::Json { .. }
            | Self::Bar(_)
            | Self::Engine(_) => 10,
            Self::Compile(_) => 20,
            Self::Callback(_) => 30,
            Self::WorkerExit(_) => 31,
            Self::Registry(_) => 40,
            Self::RunNotFound(_) | Self::NotRunning(_) => 41,
            Self::Signal { .. } => 42,
            Self::Read { .. } | Self::Spawn(_) | Self::ResultJson(_) => 50,
        }
    }
}

struct Heartbeat {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct StopAwareSource<S> {
    inner: S,
    stop: Arc<AtomicBool>,
}

impl<S: RuntimeEventSource> RuntimeEventSource for StopAwareSource<S> {
    type Error = S::Error;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        if self.stop.load(Ordering::Relaxed) {
            Ok(None)
        } else {
            self.inner.next_event()
        }
    }

    fn after_callback(
        &mut self,
        kind: u32,
        context: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        self.inner.after_callback(kind, context)
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.finish()
    }

    fn classify_error(
        &self,
        error: &Self::Error,
    ) -> (
        hftbacktest::backtest::result::EngineComponent,
        hftbacktest::backtest::result::EngineErrorCode,
    ) {
        self.inner.classify_error(error)
    }
}

impl Heartbeat {
    fn start(path: PathBuf, run_id: String, token: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                if let Ok(registry) = Registry::open(&path) {
                    let _ = registry.heartbeat(&run_id, &token);
                }
                thread::sleep(Duration::from_millis(500));
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn default_history_capacity() -> usize {
    1024
}

fn default_tick_size() -> f64 {
    0.01
}
fn default_lot_size() -> f64 {
    0.001
}
fn default_frame_interval() -> i64 {
    10_000_000
}
fn default_max_tick_batch() -> usize {
    65_536
}
fn default_last_trades_capacity() -> usize {
    1024
}

fn registry_path() -> PathBuf {
    std::env::var_os("TITAN_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".titan"))
        .join("runs.sqlite3")
}

fn strategies_path() -> PathBuf {
    std::env::var_os("TITAN_STRATEGIES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("strategies"))
}

fn strategy_manifest_path(name: &str) -> PathBuf {
    strategies_path().join(name).join("strategy.json")
}

fn load_strategy_manifest(name: &str) -> Result<StrategyManifest, CliError> {
    let path = strategy_manifest_path(name);
    let bytes = fs::read(&path).map_err(|source| CliError::Read {
        path: path.clone(),
        source,
    })?;
    let manifest = serde_json::from_slice::<StrategyManifest>(&bytes)
        .map_err(|source| CliError::Json { path, source })?;
    validate_strategy_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_strategy_manifest(manifest: &StrategyManifest) -> Result<(), CliError> {
    if manifest.schema_version != 1 {
        return Err(CliError::Schema(manifest.schema_version));
    }
    if manifest.strategy_id.is_empty()
        || manifest.strategy_version.is_empty()
        || manifest.entrypoint.split_once(':').is_none()
    {
        return Err(CliError::Entrypoint);
    }
    const ALLOWED: &[&str] = &["bar", "tick", "hybrid", "live", "timer", "funding"];
    if manifest
        .capabilities
        .iter()
        .any(|item| !ALLOWED.contains(&item.as_str()))
    {
        return Err(CliError::Engine("unknown strategy capability".into()));
    }
    Ok(())
}

fn strategy_command(command: StrategyCommands) -> Result<(), CliError> {
    match command {
        StrategyCommands::Ls => {
            let root = strategies_path();
            let entries = fs::read_dir(&root).map_err(|source| CliError::Read {
                path: root.clone(),
                source,
            })?;
            let mut manifests = Vec::new();
            for entry in entries.flatten() {
                let path = entry.path().join("strategy.json");
                if path.is_file()
                    && let Ok(bytes) = fs::read(&path)
                    && let Ok(manifest) = serde_json::from_slice::<StrategyManifest>(&bytes)
                {
                    manifests.push(manifest);
                }
            }
            manifests.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
            for manifest in manifests {
                println!(
                    "{}\t{}\t{}",
                    manifest.strategy_id,
                    manifest.strategy_version,
                    manifest.capabilities.join(",")
                );
            }
            Ok(())
        }
        StrategyCommands::Show { name } => {
            let manifest = load_strategy_manifest(&name)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest).map_err(CliError::ResultJson)?
            );
            Ok(())
        }
        StrategyCommands::Validate { name } => {
            load_strategy_manifest(&name)?;
            println!("valid");
            Ok(())
        }
        StrategyCommands::Compile { name, parameters } => {
            let manifest = load_strategy_manifest(&name)?;
            let parameters =
                serde_json::from_str(&parameters).map_err(|source| CliError::Json {
                    path: PathBuf::from("--parameters"),
                    source,
                })?;
            let mut compiler = EmbeddedPythonCompiler::default()
                .with_python_path("python/titan-strategy-sdk")
                .with_python_path(strategies_path());
            for path in manifest.python_paths {
                compiler = compiler.with_python_path(path);
            }
            let loaded = compiler.compile(
                &StrategySpec {
                    entrypoint: manifest.entrypoint,
                    parameters,
                },
                &runtime_abi_descriptor(),
            )?;
            println!(
                "{}\t{}\t{}",
                loaded.metadata.strategy_id,
                loaded.metadata.strategy_version,
                loaded.metadata.capabilities.join(",")
            );
            Ok(())
        }
    }
}

fn new_run_identity() -> (String, String) {
    let now = now_ns();
    let pid = std::process::id();
    (
        format!("run-{now:x}-{pid:x}"),
        format!("owner-{pid:x}-{now:x}"),
    )
}

fn load_spec(path: &Path) -> Result<RunSpec, CliError> {
    let bytes = fs::read(path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    let spec = serde_json::from_slice::<RunSpec>(&bytes).map_err(|source| CliError::Json {
        path: path.into(),
        source,
    })?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &RunSpec) -> Result<(), CliError> {
    if spec.schema_version != RUN_SPEC_VERSION {
        return Err(CliError::Schema(spec.schema_version));
    }
    let (module, function) = spec
        .strategy
        .entrypoint
        .split_once(':')
        .ok_or(CliError::Entrypoint)?;
    if module.is_empty() || function.is_empty() {
        return Err(CliError::Entrypoint);
    }
    if spec.history_capacity == 0 {
        return Err(CliError::HistoryCapacity);
    }
    match &spec.backend {
        BackendSpec::Bar { .. } => {}
        BackendSpec::Tick {
            tick_size,
            lot_size,
            frame_interval_ns,
            max_tick_batch,
            ..
        }
        | BackendSpec::Hybrid {
            tick_size,
            lot_size,
            frame_interval_ns,
            max_tick_batch,
            ..
        } => {
            if *tick_size <= 0.0
                || *lot_size <= 0.0
                || *frame_interval_ns <= 0
                || *max_tick_batch == 0
            {
                return Err(CliError::Engine("invalid Tick backend limits".into()));
            }
        }
        BackendSpec::Live {
            instruments,
            frame_interval_ns,
            max_tick_batch,
            ..
        } => {
            if instruments.is_empty() || *frame_interval_ns <= 0 || *max_tick_batch == 0 {
                return Err(CliError::Engine("invalid Live backend limits".into()));
            }
            if instruments.iter().any(|instrument| {
                instrument.connector.is_empty()
                    || instrument.symbol.is_empty()
                    || instrument.tick_size <= 0.0
                    || instrument.lot_size <= 0.0
            }) {
                return Err(CliError::Engine("invalid Live instrument".into()));
            }
        }
    }
    Ok(())
}

fn controller(spec_path: &Path, detach: bool) -> Result<(), CliError> {
    load_spec(spec_path)?; // static validation: this process never touches titan-python-host APIs.
    let spec_path = fs::canonicalize(spec_path).map_err(|source| CliError::Read {
        path: spec_path.into(),
        source,
    })?;
    let registry_path = registry_path();
    let registry = Registry::open(&registry_path)?;
    let (run_id, owner_token) = new_run_identity();
    let run_dir = registry_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("runs")
        .join(&run_id);
    fs::create_dir_all(&run_dir).map_err(|source| CliError::Read {
        path: run_dir.clone(),
        source,
    })?;
    let result_path = run_dir.join("result.json");
    let log_path = run_dir.join("worker.log");
    registry.create(&run_id, &owner_token, &spec_path, &result_path, &log_path)?;
    let executable = std::env::current_exe().map_err(CliError::Spawn)?;
    let mut command = Command::new(executable);
    command
        .arg("run-worker")
        .arg("--spec")
        .arg(&spec_path)
        .arg("--run-id")
        .arg(&run_id)
        .arg("--owner-token")
        .arg(&owner_token)
        .arg("--registry")
        .arg(&registry_path);
    if detach {
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|source| CliError::Read {
                path: log_path.clone(),
                source,
            })?;
        let stderr = stdout.try_clone().map_err(CliError::Spawn)?;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        let child = command.spawn().map_err(CliError::Spawn)?;
        let _ = registry.spawned(&run_id, &owner_token, child.id())?;
        println!("{run_id}");
        Ok(())
    } else {
        let mut child = command.spawn().map_err(CliError::Spawn)?;
        let _ = registry.spawned(&run_id, &owner_token, child.id())?;
        let terminate = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&terminate)).map_err(
            |source| CliError::Signal {
                pid: std::process::id(),
                source,
            },
        )?;
        signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&terminate)).map_err(
            |source| CliError::Signal {
                pid: std::process::id(),
                source,
            },
        )?;
        let mut forwarded = false;
        let status = loop {
            if let Some(status) = child.try_wait().map_err(CliError::Spawn)? {
                break status;
            }
            if terminate.load(Ordering::Relaxed) && !forwarded {
                // Safety: this is the still-owned child handle and it has not been reaped.
                if unsafe { libc::kill(child.id() as i32, libc::SIGTERM) } != 0 {
                    return Err(CliError::Signal {
                        pid: child.id(),
                        source: std::io::Error::last_os_error(),
                    });
                }
                forwarded = true;
            }
            thread::sleep(Duration::from_millis(20));
        };
        if status.success() {
            Ok(())
        } else {
            Err(CliError::WorkerExit(status))
        }
    }
}

fn read_bars(path: &Path) -> Result<Vec<TimedBarItem>, CliError> {
    let bytes = fs::read(path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    let rows =
        serde_json::from_slice::<Vec<BarInput>>(&bytes).map_err(|source| CliError::Json {
            path: path.into(),
            source,
        })?;
    Ok(rows
        .into_iter()
        .map(|row| TimedBarItem {
            asset_no: row.asset_no,
            timeframe_ns: row.timeframe_ns,
            bar: Bar {
                open_ts: row.open_ts,
                close_ts: row.close_ts,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                quote_volume: row.quote_volume,
                buy_volume: row.buy_volume,
                trade_count: row.trade_count,
                flags: BAR_COMPLETE | BAR_NATIVE,
            },
        })
        .collect())
}

fn read_ticks(path: &Path) -> Result<Vec<Event>, CliError> {
    let bytes = fs::read(path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    let rows =
        serde_json::from_slice::<Vec<TickInput>>(&bytes).map_err(|source| CliError::Json {
            path: path.into(),
            source,
        })?;
    Ok(rows
        .into_iter()
        .map(|row| Event {
            ev: row.ev,
            exch_ts: row.exch_ts,
            local_ts: row.local_ts,
            px: row.px,
            qty: row.qty,
            order_id: row.order_id,
            ival: row.ival,
            fval: row.fval,
        })
        .collect())
}

fn build_tick_backtest(
    events: &[Event],
    tick_size: f64,
    lot_size: f64,
) -> Result<Backtest<HashMapMarketDepth>, CliError> {
    if events.is_empty()
        || !tick_size.is_finite()
        || tick_size <= 0.0
        || !lot_size.is_finite()
        || lot_size <= 0.0
    {
        return Err(CliError::Engine(
            "Tick data must be non-empty and tick/lot sizes positive".into(),
        ));
    }
    let asset = L2AssetBuilder::default()
        .data(vec![DataSource::Data(Data::from_data(events))])
        .latency_model(ConstantLatency::new(0, 0))
        .asset_type(LinearAsset::new(1.0))
        .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
        .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
        .exchange(NoPartialFillExchange)
        .depth(move || HashMapMarketDepth::new(tick_size, lot_size))
        .last_trades_capacity(1024)
        .build()
        .map_err(|error| CliError::Engine(error.to_string()))?;
    Backtest::builder()
        .add_asset(asset)
        .build()
        .map_err(|error| CliError::Engine(error.to_string()))
}

fn execution_outcome(
    stats: RuntimeRunStats,
    reports: &[ExecutionReport],
    funding: &[FundingReport],
    snapshots: (Vec<AccountSnapshot>, Vec<AccountSnapshot>),
) -> RuntimeExecutionOutcome {
    RuntimeExecutionOutcome {
        stats,
        execution_reports: reports.to_vec(),
        funding_reports: funding.to_vec(),
        exchange_final: snapshots.0,
        local_delivered_final: snapshots.1,
    }
}

fn execute_backend(
    backend: BackendSpec,
    history_capacity: usize,
    callbacks: &CallbackRegistry,
    context: &mut StrategyRuntimeContext,
    stop: Arc<AtomicBool>,
) -> Result<RuntimeExecutionOutcome, CliError> {
    match backend {
        BackendSpec::Bar { data } => {
            let records = read_bars(&data)?;
            let mut source = MaterializedBarSource::new(&records, history_capacity)?;
            source.configure_context(context);
            let mut source = StopAwareSource {
                inner: source,
                stop,
            };
            let stats = run_event_runtime_counted(&mut source, callbacks, context)
                .map_err(CliError::Callback)?;
            Ok(execution_outcome(
                stats,
                source.inner.execution_reports(),
                source.inner.funding_reports(),
                source.inner.account_snapshots(),
            ))
        }
        BackendSpec::Tick {
            data,
            tick_size,
            lot_size,
            frame_interval_ns,
            max_tick_batch,
        } => {
            let events = read_ticks(&data)?;
            let mut backtest = build_tick_backtest(&events, tick_size, lot_size)?;
            let mut source = TickFrameSource::new(&mut backtest, frame_interval_ns, max_tick_batch);
            source.configure_context(context);
            let mut source = StopAwareSource {
                inner: source,
                stop,
            };
            let stats = run_event_runtime_counted(&mut source, callbacks, context)
                .map_err(CliError::Callback)?;
            Ok(execution_outcome(
                stats,
                source.inner.execution_reports(),
                source.inner.funding_reports(),
                source.inner.account_snapshots(),
            ))
        }
        BackendSpec::Hybrid {
            tick_data,
            bar_data,
            tick_size,
            lot_size,
            frame_interval_ns,
            max_tick_batch,
        } => {
            let events = read_ticks(&tick_data)?;
            let bars = read_bars(&bar_data)?;
            let mut backtest = build_tick_backtest(&events, tick_size, lot_size)?;
            let mut source = HybridFrameSource::new(
                &mut backtest,
                &bars,
                history_capacity,
                frame_interval_ns,
                max_tick_batch,
            )
            .map_err(|error| CliError::Engine(error.to_string()))?;
            source.configure_context(context);
            let mut source = StopAwareSource {
                inner: source,
                stop,
            };
            let stats = run_event_runtime_counted(&mut source, callbacks, context)
                .map_err(CliError::Callback)?;
            Ok(execution_outcome(
                stats,
                source.inner.execution_reports(),
                source.inner.funding_reports(),
                source.inner.account_snapshots(),
            ))
        }
        BackendSpec::Live {
            instruments,
            frame_interval_ns,
            max_tick_batch,
            bot_id,
        } => {
            if instruments.is_empty() {
                return Err(CliError::Engine(
                    "Live requires at least one instrument".into(),
                ));
            }
            let mut builder = LiveBotBuilder::new();
            for instrument in instruments {
                if instrument.connector.is_empty()
                    || instrument.symbol.is_empty()
                    || !instrument.tick_size.is_finite()
                    || instrument.tick_size <= 0.0
                    || !instrument.lot_size.is_finite()
                    || instrument.lot_size <= 0.0
                {
                    return Err(CliError::Engine("invalid Live instrument".into()));
                }
                builder = builder.register(Instrument::new(
                    &instrument.connector,
                    &instrument.symbol,
                    instrument.tick_size,
                    instrument.lot_size,
                    HashMapMarketDepth::new(instrument.tick_size, instrument.lot_size),
                    instrument.last_trades_capacity,
                ));
            }
            if let Some(bot_id) = bot_id {
                builder = builder.id(bot_id);
            }
            let mut live = builder
                .build::<IceoryxUnifiedChannel>()
                .map_err(|error| CliError::Engine(error.to_string()))?;
            let result = {
                let mut source = TickFrameSource::new(&mut live, frame_interval_ns, max_tick_batch);
                source.configure_context(context);
                let mut source = StopAwareSource {
                    inner: source,
                    stop,
                };
                let stats = run_event_runtime_counted(&mut source, callbacks, context)
                    .map_err(CliError::Callback)?;
                Ok::<_, CliError>(execution_outcome(
                    stats,
                    source.inner.execution_reports(),
                    source.inner.funding_reports(),
                    source.inner.account_snapshots(),
                ))
            };
            let outcome = result?;
            live.close()
                .map_err(|error| CliError::Engine(error.to_string()))?;
            Ok(outcome)
        }
    }
}

fn account_delta_result(
    delta: hftbacktest::backtest::execution::AccountDelta,
) -> AccountDeltaResult {
    AccountDeltaResult {
        instrument_id: delta.instrument_id.0,
        position_delta: delta.position_delta,
        trade_qty: delta.trade_qty,
        trade_value: delta.trade_value,
        currency: delta.currency.0,
        cash_delta: delta.cash_delta,
        fee: delta.fee,
        funding: delta.funding,
        execution_price: delta.execution_price,
        realized_pnl: delta.realized_pnl,
    }
}

fn execution_report_result(report: ExecutionReport) -> ExecutionReportResult {
    ExecutionReportResult {
        kind: format!("{:?}", report.kind).to_ascii_lowercase(),
        reason: format!("{:?}", report.reason).to_ascii_lowercase(),
        venue_no: report.venue_id.0,
        instrument_id: report.instrument_id.0,
        asset_no: report.asset_no,
        order_id: report.order_id,
        venue_order_id: report.venue_order_id,
        exchange_ts: report.exchange_ts,
        delivery_ts: report.delivery_ts,
        sequence: report.sequence,
        status: format!("{:?}", report.status).to_ascii_lowercase(),
        side: format!("{:?}", report.side).to_ascii_lowercase(),
        order_price: report.order_price,
        order_qty: report.order_qty,
        exec_price: report.exec_price,
        exec_qty: report.exec_qty,
        maker: report.maker,
        account_delta: report.account_delta.map(account_delta_result),
    }
}

fn funding_report_result(report: FundingReport) -> FundingReportResult {
    FundingReportResult {
        event_id: report.event.event_id,
        venue_no: report.event.venue_id.0,
        instrument_id: report.event.instrument_id.0,
        currency: report.event.currency.0,
        exchange_ts: report.event.settlement_ts,
        delivery_ts: report.delivery_ts,
        sequence: report.sequence,
        position_qty: report.position_qty,
        rate: report.event.rate,
        mark_price: report.event.mark_price,
        amount: report.amount,
    }
}

fn account_snapshot_result(snapshot: AccountSnapshot) -> AccountSnapshotResult {
    AccountSnapshotResult {
        venue_no: snapshot.venue_no,
        asset_no: snapshot.asset_no,
        currency: snapshot.currency.0,
        position: snapshot.position,
        balance: snapshot.balance,
        fee: snapshot.fee,
        funding: snapshot.funding,
        realized_pnl: snapshot.realized_pnl,
        unrealized_pnl: snapshot.unrealized_pnl,
        margin: snapshot.margin,
    }
}

fn worker(
    spec_path: &Path,
    registry_path: &Path,
    run_id: &str,
    token: &str,
) -> Result<(), CliError> {
    let registry = Registry::open(registry_path)?;
    if !registry.running(run_id, token, std::process::id())? {
        return Err(CliError::Engine("worker owner token/state mismatch".into()));
    }
    let _heartbeat = Heartbeat::start(registry_path.into(), run_id.into(), token.into());
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stop)).map_err(
        |source| CliError::Signal {
            pid: std::process::id(),
            source,
        },
    )?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stop)).map_err(
        |source| CliError::Signal {
            pid: std::process::id(),
            source,
        },
    )?;
    let spec = load_spec(spec_path)?;
    let mut compiler = EmbeddedPythonCompiler::default();
    for path in &spec.strategy.python_paths {
        compiler = compiler.with_python_path(path);
    }
    let abi = runtime_abi_descriptor();
    let loaded = compiler.compile(
        &StrategySpec {
            entrypoint: spec.strategy.entrypoint,
            parameters: spec.strategy.parameters,
        },
        &abi,
    )?;
    // Safety: the Python compiler validated the signature and `loaded` keeps every cfunc alive
    // until after the synchronous Runtime call and final state copy below.
    let callbacks = unsafe { CallbackRegistry::from_addresses(&loaded.callback_addresses) }?;
    let mut context = StrategyRuntimeContext {
        state_f64_ptr: loaded.state_f64_ptr,
        state_f64_len: loaded.state_f64_len,
        state_i64_ptr: loaded.state_i64_ptr,
        state_i64_len: loaded.state_i64_len,
        ..StrategyRuntimeContext::default()
    };
    let started = Instant::now();
    let outcome = execute_backend(
        spec.backend,
        spec.history_capacity,
        &callbacks,
        &mut context,
        Arc::clone(&stop),
    )?;
    let counts = execution_report_counts(&outcome.execution_reports);
    let result = WorkerResult {
        schema_version: 1,
        strategy_id: loaded.metadata.strategy_id.clone(),
        strategy_version: loaded.metadata.strategy_version.clone(),
        abi_fingerprint: abi.fingerprint,
        market_event_count: outcome.stats.market_event_count,
        callback_count: outcome.stats.callback_count.to_vec(),
        start_exchange_ts: outcome.stats.start_exchange_ts,
        end_exchange_ts: outcome.stats.end_exchange_ts,
        wall_time_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        order_count: counts.order_count,
        fill_count: counts.fill_count,
        reject_count: counts.reject_count,
        cancel_count: counts.cancel_count,
        expire_count: counts.expire_count,
        execution_reports: outcome
            .execution_reports
            .into_iter()
            .map(execution_report_result)
            .collect(),
        funding_reports: outcome
            .funding_reports
            .into_iter()
            .map(funding_report_result)
            .collect(),
        exchange_final: outcome
            .exchange_final
            .into_iter()
            .map(account_snapshot_result)
            .collect(),
        local_delivered_final: outcome
            .local_delivered_final
            .into_iter()
            .map(account_snapshot_result)
            .collect(),
        // Period returns are analytics derived from canonical snapshots. An empty table is
        // explicit when this backend did not record an equity series; renderers never infer it.
        returns: Vec::new(),
        // Safety: these validated buffers remain owned by `loaded` here.
        state_f64: unsafe {
            std::slice::from_raw_parts(loaded.state_f64_ptr, loaded.state_f64_len)
        }
        .to_vec(),
        state_i64: unsafe {
            std::slice::from_raw_parts(loaded.state_i64_ptr, loaded.state_i64_len)
        }
        .to_vec(),
    };
    let result_json = serde_json::to_string_pretty(&result).map_err(CliError::ResultJson)?;
    let record = registry
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    commit_bundle(&record.result_path, run_id, &result, &result_json)?;
    println!("{result_json}");
    let final_state = if stop.load(Ordering::Relaxed) {
        "STOPPED"
    } else {
        "COMPLETED"
    };
    registry.finish(run_id, token, final_state, 0, None)?;
    drop(callbacks);
    drop(loaded); // cfunc/state keepalive is dropped only after Runtime and result extraction.
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|source| CliError::Read {
            path: temporary.clone(),
            source,
        })?;
    file.write_all(bytes).map_err(|source| CliError::Read {
        path: temporary.clone(),
        source,
    })?;
    file.sync_all().map_err(|source| CliError::Read {
        path: temporary.clone(),
        source,
    })?;
    fs::rename(&temporary, path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    if let Some(parent) = path.parent() {
        FileSync::sync_directory(parent)?;
    }
    Ok(())
}

struct FileSync;

impl FileSync {
    fn sync_directory(path: &Path) -> Result<(), CliError> {
        let directory = fs::File::open(path).map_err(|source| CliError::Read {
            path: path.into(),
            source,
        })?;
        directory.sync_all().map_err(|source| CliError::Read {
            path: path.into(),
            source,
        })
    }
}

fn commit_bundle(
    result_path: &Path,
    run_id: &str,
    result: &WorkerResult,
    result_json: &str,
) -> Result<(), CliError> {
    atomic_write(result_path, result_json.as_bytes())?;
    let digest = Sha256::digest(result_json.as_bytes());
    let manifest = BundleManifest {
        schema_version: 1,
        run_id: run_id.into(),
        strategy_id: result.strategy_id.clone(),
        strategy_version: result.strategy_version.clone(),
        abi_fingerprint: result.abi_fingerprint.clone(),
        committed_at_ns: now_ns(),
        files: vec![BundleFile {
            path: "result.json".into(),
            bytes: result_json.len() as u64,
            sha256: format!("{digest:x}"),
        }],
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(CliError::ResultJson)?;
    atomic_write(&result_path.with_file_name("manifest.json"), &manifest_json)
}

fn list_runs(json: bool) -> Result<(), CliError> {
    let registry = Registry::open(&registry_path())?;
    registry.reconcile()?;
    let runs = registry.list()?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&runs).map_err(CliError::ResultJson)?
        );
    } else {
        for run in runs {
            println!(
                "{}\t{}\t{}",
                run.id,
                run.state,
                run.pid.map_or("-".into(), |p| p.to_string())
            );
        }
    }
    Ok(())
}

fn show_run(run_id: &str) -> Result<(), CliError> {
    let run = Registry::open(&registry_path())?
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&run).map_err(CliError::ResultJson)?
    );
    Ok(())
}

fn logs(run_id: &str) -> Result<(), CliError> {
    let run = Registry::open(&registry_path())?
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    let content = fs::read_to_string(&run.log_path).map_err(|source| CliError::Read {
        path: run.log_path,
        source,
    })?;
    print!("{content}");
    Ok(())
}

fn stop_run(run_id: &str) -> Result<(), CliError> {
    let registry = Registry::open(&registry_path())?;
    let process = registry
        .request_stop(run_id)?
        .ok_or_else(|| CliError::NotRunning(run_id.into()))?;
    let pid = process.pid;
    if pid == 0 || pid > i32::MAX as u32 {
        return Err(CliError::Signal {
            pid,
            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "PID exceeds pid_t"),
        });
    }
    if process_start_time(pid) != Some(process.start_time) {
        registry.reconcile()?;
        return Err(CliError::NotRunning(run_id.into()));
    }
    // Safety: PID and OS process start time both match the active worker identity.
    if unsafe { libc::kill(pid as i32, libc::SIGTERM) } != 0 {
        return Err(CliError::Signal {
            pid,
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(())
}

fn report(run_id: &str, output: Option<&Path>, renderer: &str) -> Result<(), CliError> {
    let run = Registry::open(&registry_path())?
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    let manifest_path = run.result_path.with_file_name("manifest.json");
    let manifest_bytes = fs::read(&manifest_path).map_err(|source| CliError::Read {
        path: manifest_path.clone(),
        source,
    })?;
    let manifest: BundleManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|source| CliError::Json {
            path: manifest_path.clone(),
            source,
        })?;
    if manifest.schema_version != 1 {
        return Err(CliError::Engine(format!(
            "unsupported ResultBundle schema {}",
            manifest.schema_version
        )));
    }
    let bundle = run
        .result_path
        .parent()
        .ok_or_else(|| CliError::Engine("ResultBundle has no parent directory".into()))?;
    let canonical_bundle = fs::canonicalize(bundle).map_err(|source| CliError::Read {
        path: bundle.into(),
        source,
    })?;
    for descriptor in &manifest.files {
        let target = bundle.join(&descriptor.path);
        let canonical_target = fs::canonicalize(&target).map_err(|source| CliError::Read {
            path: target.clone(),
            source,
        })?;
        if !canonical_target.starts_with(&canonical_bundle) {
            return Err(CliError::Engine("ResultBundle file escapes root".into()));
        }
        let content = fs::read(&canonical_target).map_err(|source| CliError::Read {
            path: canonical_target.clone(),
            source,
        })?;
        if content.len() as u64 != descriptor.bytes
            || format!("{:x}", Sha256::digest(&content)) != descriptor.sha256
        {
            return Err(CliError::Engine(format!(
                "ResultBundle integrity mismatch: {}",
                descriptor.path
            )));
        }
    }
    let result = fs::read_to_string(&run.result_path).map_err(|source| CliError::Read {
        path: run.result_path.clone(),
        source,
    })?;
    if let Some(output) = output {
        let python = std::env::var_os("TITAN_REPORT_PYTHON").unwrap_or_else(|| "python3".into());
        let reporting_path = std::env::var_os("TITAN_REPORTING_PATH")
            .unwrap_or_else(|| "python/titan-reporting".into());
        let status = Command::new(python)
            .env("PYTHONPATH", reporting_path)
            .args(["-m", "titan_reporting"])
            .arg(bundle)
            .arg("--output")
            .arg(output)
            .arg("--renderer")
            .arg(renderer)
            .status()
            .map_err(CliError::Spawn)?;
        if !status.success() {
            return Err(CliError::WorkerExit(status));
        }
        println!("{}", output.display());
        return Ok(());
    }
    println!("{result}");
    Ok(())
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Commands::Run { spec, detach } => controller(&spec, detach),
        Commands::RunWorker {
            spec,
            run_id,
            owner_token,
            registry,
        } => {
            let result = worker(&spec, &registry, &run_id, &owner_token);
            if let Err(error) = &result
                && let Ok(registry) = Registry::open(&registry)
            {
                let _ = registry.finish(
                    &run_id,
                    &owner_token,
                    "FAILED",
                    i32::from(error.exit_code()),
                    Some(&error.to_string()),
                );
            }
            result
        }
        Commands::Validate { spec } => load_spec(&spec).map(|_| println!("valid")),
        Commands::Ls { json } => list_runs(json),
        Commands::Show { run_id } => show_run(&run_id),
        Commands::Logs { run_id } => logs(&run_id),
        Commands::Stop { run_id } => stop_run(&run_id),
        Commands::Report {
            run_id,
            output,
            renderer,
        } => report(&run_id, output.as_deref(), &renderer),
        Commands::Strategy { command } => strategy_command(command),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("titan: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}
