#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::process::CommandExt};
use std::{
    collections::BTreeMap,
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
use registry::{Registry, StopAction, now_ns, process_start_time};

use clap::{Parser, Subcommand, ValueEnum};
use hftbacktest::{
    backtest::{
        Backtest, DataSource, ExchangeKind, L2AssetBuilder,
        assettype::{AssetType, InverseAsset, LinearAsset},
        data::Data,
        execution::{ExecutionReport, FundingReport},
        models::{
            CommonFees, ConstantLatency, PowerProbQueueFunc3, ProbQueueModel, QueueModel,
            RiskAdverseQueueModel, TradingValueFeeModel,
        },
        result::{AccountSnapshot, execution_report_counts},
    },
    depth::HashMapMarketDepth,
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
    /// Run the configured EventEngine/PluginEngine graph until SIGINT.
    CoreRun {
        #[arg(short = 'c', long = "config", value_name = "RUNTIME.toml")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Resolve a strategy and config, then execute it in an isolated worker process.
    Run {
        #[arg(value_name = "STRATEGY")]
        strategy: String,
        #[arg(short = 'e', long = "env", value_enum)]
        env: Environment,
        #[arg(short = 'm', long = "mode", value_enum)]
        mode: EventMode,
        #[arg(short = 'c', long = "config", value_name = "CONFIG.toml")]
        config: PathBuf,
        #[arg(long)]
        detach: bool,
        #[arg(long)]
        json: bool,
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
    /// Validate a strategy and run config without starting Python or a worker.
    Validate {
        #[arg(value_name = "STRATEGY")]
        strategy: String,
        #[arg(short = 'e', long = "env", value_enum)]
        env: Environment,
        #[arg(short = 'm', long = "mode", value_enum)]
        mode: EventMode,
        #[arg(short = 'c', long = "config", value_name = "CONFIG.toml")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// List recorded runs.
    Ls {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        active: bool,
        #[arg(long, value_enum)]
        env: Option<Environment>,
        #[arg(long, value_enum)]
        mode: Option<EventMode>,
        #[arg(long)]
        strategy: Option<String>,
        #[arg(long)]
        status: Option<RunStatus>,
    },
    /// Show one run record.
    Show {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Print the worker log for one run.
    Logs {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Request termination of a detached worker.
    Stop {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Render a completed ResultBundle without loading Python or Runtime.
    Report {
        run_id: String,
        /// Spawn the isolated Python renderer and write a native HTML report.
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, default_value = "native")]
        renderer: String,
        #[arg(long)]
        json: bool,
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
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Show one static manifest.
    Show {
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Validate one static manifest without importing Python.
    Validate {
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Eagerly import and Numba-compile one strategy.
    Compile {
        name: String,
        #[arg(long, default_value = "{}")]
        parameters: String,
        #[arg(long)]
        json: bool,
    },
}

impl Commands {
    fn json_requested(&self) -> bool {
        match self {
            Self::CoreRun { json, .. }
            | Self::Run { json, .. }
            | Self::Validate { json, .. }
            | Self::Ls { json, .. }
            | Self::Show { json, .. }
            | Self::Logs { json, .. }
            | Self::Stop { json, .. }
            | Self::Report { json, .. } => *json,
            Self::Strategy { command } => match command {
                StrategyCommands::Ls { json }
                | StrategyCommands::Show { json, .. }
                | StrategyCommands::Validate { json, .. }
                | StrategyCommands::Compile { json, .. } => *json,
            },
            Self::RunWorker { .. } => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "snake_case")]
enum Environment {
    Backtest,
    Live,
}

impl Environment {
    fn as_str(self) -> &'static str {
        match self {
            Self::Backtest => "backtest",
            Self::Live => "live",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "snake_case")]
enum EventMode {
    Bar,
    Tick,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "snake_case")]
enum RunStatus {
    Starting,
    Loading,
    Compiling,
    Ready,
    Running,
    StopRequested,
    Completed,
    Stopped,
    Failed,
    Stale,
    Cancelled,
}

impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Loading => "LOADING",
            Self::Compiling => "COMPILING",
            Self::Ready => "READY",
            Self::Running => "RUNNING",
            Self::StopRequested => "STOP_REQUESTED",
            Self::Completed => "COMPLETED",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
            Self::Stale => "STALE",
            Self::Cancelled => "CANCELLED",
        }
    }
}

impl EventMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bar => "bar",
            Self::Tick => "tick",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunSpec {
    schema_version: u32,
    environment: Environment,
    event_mode: EventMode,
    config_path: PathBuf,
    config_sha256: String,
    strategy: StrategyRunSpec,
    backend: BackendSpec,
    #[serde(default = "default_history_capacity")]
    history_capacity: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrategyRunSpec {
    strategy_id: String,
    strategy_version: String,
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
    events: Vec<EventMode>,
    environments: Vec<Environment>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    parameters: BTreeMap<String, ParameterSpec>,
    #[serde(default)]
    python_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ParameterSpec {
    #[serde(rename = "type")]
    kind: ParameterKind,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    default: Option<serde_json::Value>,
    #[serde(default)]
    minimum: Option<f64>,
    #[serde(default)]
    maximum: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ParameterKind {
    Integer,
    Number,
    Boolean,
    String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunConfig {
    schema_version: u32,
    #[serde(default = "default_history_capacity")]
    history_capacity: usize,
    #[serde(default)]
    strategy: RunConfigStrategy,
    #[serde(default)]
    backtest: Option<BacktestConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunConfigStrategy {
    #[serde(default)]
    parameters: BTreeMap<String, toml::Value>,
    #[serde(default)]
    python_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BacktestConfig {
    #[serde(default)]
    data: Option<PathBuf>,
    #[serde(default)]
    tick_data: Option<PathBuf>,
    #[serde(default)]
    bar_data: Option<PathBuf>,
    #[serde(default = "default_tick_size")]
    tick_size: f64,
    #[serde(default = "default_lot_size")]
    lot_size: f64,
    #[serde(default = "default_frame_interval")]
    frame_interval_ns: i64,
    #[serde(default = "default_max_tick_batch")]
    max_tick_batch: usize,
    #[serde(default)]
    execution: BacktestExecutionSpec,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct BacktestExecutionSpec {
    #[serde(default)]
    entry_latency_ns: i64,
    #[serde(default)]
    response_latency_ns: i64,
    #[serde(default)]
    maker_fee: f64,
    #[serde(default)]
    taker_fee: f64,
    #[serde(default = "default_queue_power")]
    queue_power: f64,
    #[serde(default)]
    queue: QueueModelSpec,
    #[serde(default)]
    exchange: ExchangeModel,
    #[serde(default)]
    asset: AssetModel,
    #[serde(default = "default_contract_size")]
    contract_size: f64,
    #[serde(default)]
    latency_offset_ns: i64,
    #[serde(default = "default_last_trades_capacity")]
    last_trades_capacity: usize,
}

impl Default for BacktestExecutionSpec {
    fn default() -> Self {
        Self {
            entry_latency_ns: 0,
            response_latency_ns: 0,
            maker_fee: 0.0,
            taker_fee: 0.0,
            queue_power: default_queue_power(),
            queue: QueueModelSpec::default(),
            exchange: ExchangeModel::default(),
            asset: AssetModel::default(),
            contract_size: default_contract_size(),
            latency_offset_ns: 0,
            last_trades_capacity: default_last_trades_capacity(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExchangeModel {
    #[default]
    NoPartialFill,
    PartialFill,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum QueueModelSpec {
    RiskAverse,
    #[default]
    PowerProbability,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AssetModel {
    #[default]
    Linear,
    Inverse,
}

#[derive(Debug, Serialize)]
struct StrategyCatalogEntry {
    strategy_id: String,
    strategy_version: Option<String>,
    events: Vec<EventMode>,
    environments: Vec<Environment>,
    status: String,
    source: PathBuf,
    error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendSpec {
    Backtest {
        source: BacktestSourceSpec,
        #[serde(default)]
        execution: BacktestExecutionSpec,
    },
    CoreLive {
        strategy_key: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BacktestSourceSpec {
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
    run_id: String,
    strategy_id: String,
    strategy_version: String,
    environment: Environment,
    event_mode: EventMode,
    config_sha256: String,
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
    #[error("invalid TOML in {path}: {source}")]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("unsupported RunSpec schema_version {0}; expected {RUN_SPEC_VERSION}")]
    Schema(u32),
    #[error("strategy entrypoint must use module:function syntax")]
    Entrypoint,
    #[error("history_capacity must be positive")]
    HistoryCapacity,
    #[error("worker spawn failed: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("worker failed: {0}")]
    WorkerFailed(String),
    #[error("report generation failed: {0}")]
    ReportFailed(String),
    #[error("strategy compilation failed: {0}")]
    Compile(#[from] titan_python_host::PythonHostError),
    #[error("invalid callback descriptor: {0}")]
    Callback(#[from] titan_runtime::RuntimeError),
    #[error("invalid Bar input: {0}")]
    Bar(#[from] titan_runtime::MaterializedBarError),
    #[error("invalid configuration: {0}")]
    Engine(String),
    #[error("result serialization failed: {0}")]
    ResultJson(#[source] serde_json::Error),
    #[error("run registry failed: {0}")]
    Registry(#[from] rusqlite::Error),
    #[error("run {0} was not found")]
    RunNotFound(String),
    #[error("strategy {0} was not found")]
    StrategyNotFound(String),
    #[error("run {0} is not running")]
    NotRunning(String),
    #[error("cannot signal worker {pid}: {source}")]
    Signal { pid: u32, source: std::io::Error },
}

impl CliError {
    fn code(&self) -> &'static str {
        match self {
            Self::Schema(_) => "INVALID_SCHEMA",
            Self::Entrypoint => "INVALID_ENTRYPOINT",
            Self::HistoryCapacity => "INVALID_HISTORY_CAPACITY",
            Self::Json { .. } => "INVALID_JSON",
            Self::Toml { .. } => "INVALID_TOML",
            Self::Bar(_) | Self::Engine(_) => "INVALID_CONFIGURATION",
            Self::Compile(_) => "STRATEGY_COMPILE_FAILED",
            Self::Callback(_) => "RUNTIME_CALLBACK_FAILED",
            Self::WorkerFailed(_) => "WORKER_FAILED",
            Self::ReportFailed(_) => "REPORT_FAILED",
            Self::Registry(_) => "REGISTRY_FAILED",
            Self::RunNotFound(_) => "RUN_NOT_FOUND",
            Self::StrategyNotFound(_) => "STRATEGY_NOT_FOUND",
            Self::NotRunning(_) => "RUN_NOT_ACTIVE",
            Self::Signal { .. } => "SIGNAL_FAILED",
            Self::Read { .. } => "READ_FAILED",
            Self::Spawn(_) => "SPAWN_FAILED",
            Self::ResultJson(_) => "SERIALIZATION_FAILED",
        }
    }

    fn exit_code(&self) -> u8 {
        match self {
            Self::Schema(_)
            | Self::Entrypoint
            | Self::HistoryCapacity
            | Self::Json { .. }
            | Self::Toml { .. }
            | Self::Bar(_)
            | Self::Engine(_) => 10,
            Self::Compile(_) => 20,
            Self::Callback(_) => 30,
            Self::WorkerFailed(_) => 31,
            Self::ReportFailed(_) => 32,
            Self::Registry(_) => 40,
            Self::RunNotFound(_) | Self::StrategyNotFound(_) | Self::NotRunning(_) => 41,
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

#[cfg(unix)]
struct ProcessOutputGuard {
    stdout: libc::c_int,
    stderr: libc::c_int,
}

#[cfg(unix)]
impl Drop for ProcessOutputGuard {
    fn drop(&mut self) {
        let _ = std::io::stdout().flush();
        let _ = std::io::stderr().flush();
        // Safety: both descriptors were returned by dup and remain owned by this guard.
        unsafe {
            libc::dup2(self.stdout, libc::STDOUT_FILENO);
            libc::dup2(self.stderr, libc::STDERR_FILENO);
            libc::close(self.stdout);
            libc::close(self.stderr);
        }
    }
}

#[cfg(unix)]
fn redirect_process_output(path: &Path) -> Result<ProcessOutputGuard, CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Read {
            path: parent.into(),
            source,
        })?;
    }
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| CliError::Read {
            path: path.into(),
            source,
        })?;
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    // Safety: dup/dup2 operate on valid process descriptors and errors are checked immediately.
    unsafe {
        let stdout = libc::dup(libc::STDOUT_FILENO);
        if stdout < 0 {
            return Err(CliError::Spawn(std::io::Error::last_os_error()));
        }
        let stderr = libc::dup(libc::STDERR_FILENO);
        if stderr < 0 {
            libc::close(stdout);
            return Err(CliError::Spawn(std::io::Error::last_os_error()));
        }
        if libc::dup2(output.as_raw_fd(), libc::STDOUT_FILENO) < 0
            || libc::dup2(output.as_raw_fd(), libc::STDERR_FILENO) < 0
        {
            let error = std::io::Error::last_os_error();
            libc::dup2(stdout, libc::STDOUT_FILENO);
            libc::dup2(stderr, libc::STDERR_FILENO);
            libc::close(stdout);
            libc::close(stderr);
            return Err(CliError::Spawn(error));
        }
        Ok(ProcessOutputGuard { stdout, stderr })
    }
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
fn default_queue_power() -> f64 {
    3.0
}
fn default_contract_size() -> f64 {
    1.0
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
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(CliError::Engine("invalid strategy id".into()));
    }
    let path = strategy_manifest_path(name);
    if !path.try_exists().map_err(|source| CliError::Read {
        path: path.clone(),
        source,
    })? {
        return Err(CliError::StrategyNotFound(name.into()));
    }
    let bytes = fs::read(&path).map_err(|source| CliError::Read {
        path: path.clone(),
        source,
    })?;
    let manifest = serde_json::from_slice::<StrategyManifest>(&bytes)
        .map_err(|source| CliError::Json { path, source })?;
    validate_strategy_manifest(&manifest)?;
    if manifest.strategy_id != name {
        return Err(CliError::Engine(format!(
            "strategy directory {name} contains manifest for {}",
            manifest.strategy_id
        )));
    }
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
    if manifest.events.is_empty() || manifest.environments.is_empty() {
        return Err(CliError::Engine(
            "strategy events and environments must not be empty".into(),
        ));
    }
    const ALLOWED: &[&str] = &["timer", "funding"];
    if manifest
        .capabilities
        .iter()
        .any(|item| !ALLOWED.contains(&item.as_str()))
    {
        return Err(CliError::Engine("unknown strategy capability".into()));
    }
    for (name, spec) in &manifest.parameters {
        if name.is_empty() {
            return Err(CliError::Engine("parameter name must not be empty".into()));
        }
        if let (Some(minimum), Some(maximum)) = (spec.minimum, spec.maximum)
            && minimum > maximum
        {
            return Err(CliError::Engine(format!(
                "parameter {name} minimum exceeds maximum"
            )));
        }
        if let Some(default) = &spec.default {
            validate_parameter_value(name, default, spec)?;
        }
    }
    Ok(())
}

fn validate_parameter_value(
    name: &str,
    value: &serde_json::Value,
    spec: &ParameterSpec,
) -> Result<(), CliError> {
    let valid_type = match spec.kind {
        ParameterKind::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        ParameterKind::Number => value.as_f64().is_some(),
        ParameterKind::Boolean => value.is_boolean(),
        ParameterKind::String => value.is_string(),
    };
    if !valid_type {
        return Err(CliError::Engine(format!(
            "strategy parameter {name} has the wrong type"
        )));
    }
    if let Some(number) = value.as_f64() {
        if spec.minimum.is_some_and(|minimum| number < minimum) {
            return Err(CliError::Engine(format!(
                "strategy parameter {name} is below its minimum"
            )));
        }
        if spec.maximum.is_some_and(|maximum| number > maximum) {
            return Err(CliError::Engine(format!(
                "strategy parameter {name} exceeds its maximum"
            )));
        }
    }
    Ok(())
}

fn resolve_parameters(
    manifest: &StrategyManifest,
    configured: BTreeMap<String, toml::Value>,
) -> Result<serde_json::Value, CliError> {
    let configured = configured
        .into_iter()
        .map(|(name, value)| {
            serde_json::to_value(value)
                .map(|value| (name, value))
                .map_err(CliError::ResultJson)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    resolve_json_parameters(manifest, configured)
}

fn resolve_json_parameters(
    manifest: &StrategyManifest,
    mut configured: BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value, CliError> {
    if let Some(unknown) = configured
        .keys()
        .find(|name| !manifest.parameters.contains_key(*name))
    {
        return Err(CliError::Engine(format!(
            "unknown strategy parameter {unknown}"
        )));
    }
    let mut resolved = serde_json::Map::new();
    for (name, spec) in &manifest.parameters {
        let value = configured.remove(name).or_else(|| spec.default.clone());
        match value {
            Some(value) => {
                validate_parameter_value(name, &value, spec)?;
                resolved.insert(name.clone(), value);
            }
            None if spec.required => {
                return Err(CliError::Engine(format!(
                    "missing required strategy parameter {name}"
                )));
            }
            None => {}
        }
    }
    Ok(serde_json::Value::Object(resolved))
}

fn strategy_command(command: StrategyCommands) -> Result<(), CliError> {
    match command {
        StrategyCommands::Ls { json } => {
            let root = strategies_path();
            let entries = fs::read_dir(&root).map_err(|source| CliError::Read {
                path: root.clone(),
                source,
            })?;
            let mut catalog = Vec::new();
            for entry in entries.flatten() {
                let source = entry.path().join("strategy.json");
                if !source.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                let item = match load_strategy_manifest(&name) {
                    Ok(manifest) => StrategyCatalogEntry {
                        strategy_id: manifest.strategy_id,
                        strategy_version: Some(manifest.strategy_version),
                        events: manifest.events,
                        environments: manifest.environments,
                        status: "VALID".into(),
                        source,
                        error: None,
                    },
                    Err(error) => StrategyCatalogEntry {
                        strategy_id: name,
                        strategy_version: None,
                        events: Vec::new(),
                        environments: Vec::new(),
                        status: "INVALID".into(),
                        source,
                        error: Some(error.to_string()),
                    },
                };
                catalog.push(item);
            }
            catalog.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "strategies": catalog
                    }))
                    .map_err(CliError::ResultJson)?
                );
            } else {
                println!("STRATEGY\tVERSION\tEVENTS\tENVIRONMENTS\tSTATUS\tSOURCE");
                for item in catalog {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        item.strategy_id,
                        item.strategy_version.as_deref().unwrap_or("-"),
                        item.events
                            .iter()
                            .map(|value| value.as_str())
                            .collect::<Vec<_>>()
                            .join(","),
                        item.environments
                            .iter()
                            .map(|value| value.as_str())
                            .collect::<Vec<_>>()
                            .join(","),
                        item.status,
                        item.source.display()
                    );
                }
            }
            Ok(())
        }
        StrategyCommands::Show { name, json } => {
            let manifest = load_strategy_manifest(&name)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "strategy": manifest
                    }))
                    .map_err(CliError::ResultJson)?
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&manifest).map_err(CliError::ResultJson)?
                );
            }
            Ok(())
        }
        StrategyCommands::Validate { name, json } => {
            let manifest = load_strategy_manifest(&name)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "valid": true,
                        "strategy_id": manifest.strategy_id,
                        "strategy_version": manifest.strategy_version
                    })
                );
            } else {
                println!("valid");
            }
            Ok(())
        }
        StrategyCommands::Compile {
            name,
            parameters,
            json,
        } => {
            let manifest = load_strategy_manifest(&name)?;
            let parameters_value: serde_json::Value =
                serde_json::from_str(&parameters).map_err(|source| CliError::Json {
                    path: PathBuf::from("--parameters"),
                    source,
                })?;
            let configured: BTreeMap<String, serde_json::Value> = parameters_value
                .as_object()
                .ok_or_else(|| CliError::Engine("--parameters must be a JSON object".into()))?
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            let parameters = resolve_json_parameters(&manifest, configured)?;
            let strategy_root =
                fs::canonicalize(strategies_path()).map_err(|source| CliError::Read {
                    path: strategies_path(),
                    source,
                })?;
            let mut compiler = EmbeddedPythonCompiler::default().with_python_path(strategy_root);
            let sdk_path = std::env::var_os("TITAN_STRATEGY_SDK")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("python/titan-strategy-sdk"));
            if sdk_path.exists() {
                compiler =
                    compiler.with_python_path(fs::canonicalize(&sdk_path).map_err(|source| {
                        CliError::Read {
                            path: sdk_path,
                            source,
                        }
                    })?);
            }
            let manifest_path = strategy_manifest_path(&name);
            let manifest_root = manifest_path.parent().unwrap_or(Path::new("."));
            for path in &manifest.python_paths {
                compiler = compiler.with_python_path(resolve_path(manifest_root, path)?);
            }
            let strategy_spec = StrategySpec {
                entrypoint: manifest.entrypoint.clone(),
                parameters,
            };
            #[cfg(unix)]
            let loaded = {
                let compile_log = registry_path().with_file_name("strategy-compile.log");
                let _output = redirect_process_output(&compile_log)?;
                compiler.compile(&strategy_spec, &runtime_abi_descriptor())?
            };
            #[cfg(not(unix))]
            let loaded = compiler.compile(&strategy_spec, &runtime_abi_descriptor())?;
            if loaded.metadata.strategy_id != manifest.strategy_id
                || loaded.metadata.strategy_version != manifest.strategy_version
            {
                return Err(CliError::Engine(
                    "compiled strategy identity does not match manifest".into(),
                ));
            }
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "strategy_id": loaded.metadata.strategy_id,
                        "strategy_version": loaded.metadata.strategy_version,
                        "capabilities": loaded.metadata.capabilities
                    })
                );
            } else {
                println!(
                    "{}\t{}\t{}",
                    loaded.metadata.strategy_id,
                    loaded.metadata.strategy_version,
                    loaded.metadata.capabilities.join(",")
                );
            }
            Ok(())
        }
    }
}

fn resolve_path(base: &Path, path: &Path) -> Result<PathBuf, CliError> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    fs::canonicalize(&candidate).map_err(|source| CliError::Read {
        path: candidate,
        source,
    })
}

fn resolve_run_spec(
    strategy_name: &str,
    environment: Environment,
    event_mode: EventMode,
    config_path: &Path,
) -> Result<RunSpec, CliError> {
    if environment == Environment::Live {
        return resolve_core_live_run_spec(strategy_name, event_mode, config_path);
    }
    let manifest = load_strategy_manifest(strategy_name)?;
    if !manifest.environments.contains(&environment) {
        return Err(CliError::Engine(format!(
            "strategy {strategy_name} does not support environment {}",
            environment.as_str()
        )));
    }
    if !manifest.events.contains(&event_mode) {
        return Err(CliError::Engine(format!(
            "strategy {strategy_name} does not support event mode {}",
            event_mode.as_str()
        )));
    }

    let config_path = fs::canonicalize(config_path).map_err(|source| CliError::Read {
        path: config_path.into(),
        source,
    })?;
    let config_bytes = fs::read(&config_path).map_err(|source| CliError::Read {
        path: config_path.clone(),
        source,
    })?;
    let config_text = std::str::from_utf8(&config_bytes)
        .map_err(|error| CliError::Engine(format!("run config is not UTF-8: {error}")))?;
    let config = toml::from_str::<RunConfig>(config_text).map_err(|source| CliError::Toml {
        path: config_path.clone(),
        source,
    })?;
    if config.schema_version != RUN_SPEC_VERSION {
        return Err(CliError::Schema(config.schema_version));
    }
    if config.history_capacity == 0 {
        return Err(CliError::HistoryCapacity);
    }
    let base = config_path.parent().unwrap_or(Path::new("."));
    let parameters = resolve_parameters(&manifest, config.strategy.parameters)?;
    let strategy_root = fs::canonicalize(strategies_path()).map_err(|source| CliError::Read {
        path: strategies_path(),
        source,
    })?;
    let manifest_path = strategy_manifest_path(strategy_name);
    let manifest_root = manifest_path.parent().unwrap_or(Path::new("."));
    let mut python_paths = vec![strategy_root];
    let sdk_path = std::env::var_os("TITAN_STRATEGY_SDK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python/titan-strategy-sdk"));
    if sdk_path.exists() {
        python_paths.push(
            fs::canonicalize(&sdk_path).map_err(|source| CliError::Read {
                path: sdk_path,
                source,
            })?,
        );
    }
    for path in manifest.python_paths {
        python_paths.push(resolve_path(manifest_root, &path)?);
    }
    for path in config.strategy.python_paths {
        python_paths.push(resolve_path(base, &path)?);
    }
    python_paths.sort();
    python_paths.dedup();

    let backend = match environment {
        Environment::Backtest => {
            let backend = config.backtest.ok_or_else(|| {
                CliError::Engine("backtest environment requires a [backtest] section".into())
            })?;
            let execution = backend.execution;
            match event_mode {
                EventMode::Bar | EventMode::Tick
                    if backend.tick_data.is_some() || backend.bar_data.is_some() =>
                {
                    return Err(CliError::Engine(
                        "bar/tick mode accepts backtest.data only".into(),
                    ));
                }
                EventMode::Hybrid if backend.data.is_some() => {
                    return Err(CliError::Engine(
                        "hybrid mode accepts backtest.tick_data and backtest.bar_data only".into(),
                    ));
                }
                _ => {}
            }
            let source = match event_mode {
                EventMode::Bar => BacktestSourceSpec::Bar {
                    data: resolve_path(
                        base,
                        backend.data.as_deref().ok_or_else(|| {
                            CliError::Engine("bar mode requires backtest.data".into())
                        })?,
                    )?,
                },
                EventMode::Tick => BacktestSourceSpec::Tick {
                    data: resolve_path(
                        base,
                        backend.data.as_deref().ok_or_else(|| {
                            CliError::Engine("tick mode requires backtest.data".into())
                        })?,
                    )?,
                    tick_size: backend.tick_size,
                    lot_size: backend.lot_size,
                    frame_interval_ns: backend.frame_interval_ns,
                    max_tick_batch: backend.max_tick_batch,
                },
                EventMode::Hybrid => BacktestSourceSpec::Hybrid {
                    tick_data: resolve_path(
                        base,
                        backend.tick_data.as_deref().ok_or_else(|| {
                            CliError::Engine("hybrid mode requires backtest.tick_data".into())
                        })?,
                    )?,
                    bar_data: resolve_path(
                        base,
                        backend.bar_data.as_deref().ok_or_else(|| {
                            CliError::Engine("hybrid mode requires backtest.bar_data".into())
                        })?,
                    )?,
                    tick_size: backend.tick_size,
                    lot_size: backend.lot_size,
                    frame_interval_ns: backend.frame_interval_ns,
                    max_tick_batch: backend.max_tick_batch,
                },
            };
            BackendSpec::Backtest { source, execution }
        }
        Environment::Live => unreachable!("live returned before legacy run-spec resolution"),
    };

    let spec = RunSpec {
        schema_version: RUN_SPEC_VERSION,
        environment,
        event_mode,
        config_path,
        config_sha256: format!("{:x}", Sha256::digest(&config_bytes)),
        strategy: StrategyRunSpec {
            strategy_id: manifest.strategy_id,
            strategy_version: manifest.strategy_version,
            entrypoint: manifest.entrypoint,
            parameters,
            python_paths,
        },
        backend,
        history_capacity: config.history_capacity,
    };
    validate_spec(&spec)?;
    Ok(spec)
}

fn resolve_core_live_run_spec(
    strategy_name: &str,
    event_mode: EventMode,
    config_path: &Path,
) -> Result<RunSpec, CliError> {
    let config_path = fs::canonicalize(config_path).map_err(|source| CliError::Read {
        path: config_path.into(),
        source,
    })?;
    let config_bytes = fs::read(&config_path).map_err(|source| CliError::Read {
        path: config_path.clone(),
        source,
    })?;
    let adapted = load_core_configuration(&config_path, Some((strategy_name, event_mode)))?;
    let definition = adapted
        .strategies
        .first()
        .ok_or_else(|| CliError::Engine(format!("strategy {strategy_name} is not enabled")))?;
    let parameters =
        serde_json::from_slice(&definition.parameters).map_err(|source| CliError::Json {
            path: config_path.clone(),
            source,
        })?;
    let spec = RunSpec {
        schema_version: RUN_SPEC_VERSION,
        environment: Environment::Live,
        event_mode,
        config_path,
        config_sha256: format!("{:x}", Sha256::digest(&config_bytes)),
        strategy: StrategyRunSpec {
            strategy_id: definition.strategy_key.to_string(),
            strategy_version: definition.definition_version.to_string(),
            entrypoint: definition.entrypoint.to_string(),
            parameters,
            python_paths: Vec::new(),
        },
        backend: BackendSpec::CoreLive {
            strategy_key: definition.strategy_key.to_string(),
        },
        history_capacity: default_history_capacity(),
    };
    validate_spec(&spec)?;
    Ok(spec)
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
    if spec.strategy.strategy_id.is_empty() || spec.strategy.strategy_version.is_empty() {
        return Err(CliError::Engine(
            "resolved strategy identity must not be empty".into(),
        ));
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
        BackendSpec::Backtest { source, execution } => {
            if spec.environment != Environment::Backtest {
                return Err(CliError::Engine(
                    "backtest backend requires backtest environment".into(),
                ));
            }
            let expected_mode = match source {
                BacktestSourceSpec::Bar { .. } => EventMode::Bar,
                BacktestSourceSpec::Tick {
                    tick_size,
                    lot_size,
                    frame_interval_ns,
                    max_tick_batch,
                    ..
                }
                | BacktestSourceSpec::Hybrid {
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
                    match source {
                        BacktestSourceSpec::Tick { .. } => EventMode::Tick,
                        BacktestSourceSpec::Hybrid { .. } => EventMode::Hybrid,
                        _ => unreachable!(),
                    }
                }
            };
            if spec.event_mode != expected_mode {
                return Err(CliError::Engine(
                    "backtest source does not match event mode".into(),
                ));
            }
            if execution.entry_latency_ns < 0
                || execution.response_latency_ns < 0
                || !execution.maker_fee.is_finite()
                || !execution.taker_fee.is_finite()
                || (execution.queue == QueueModelSpec::PowerProbability
                    && (!execution.queue_power.is_finite() || execution.queue_power <= 0.0))
                || !execution.contract_size.is_finite()
                || execution.contract_size <= 0.0
            {
                return Err(CliError::Engine("invalid backtest execution model".into()));
            }
            if matches!(source, BacktestSourceSpec::Bar { .. })
                && *execution != BacktestExecutionSpec::default()
            {
                return Err(CliError::Engine(
                    "backtest.execution currently applies to tick and hybrid modes only".into(),
                ));
            }
        }
        BackendSpec::CoreLive { strategy_key } => {
            if spec.environment != Environment::Live {
                return Err(CliError::Engine(
                    "Core live backend requires live environment".into(),
                ));
            }
            if strategy_key.is_empty() || strategy_key != &spec.strategy.strategy_id {
                return Err(CliError::Engine(
                    "Core live strategy identity is inconsistent".into(),
                ));
            }
        }
    }
    Ok(())
}

fn controller(
    strategy_name: &str,
    environment: Environment,
    event_mode: EventMode,
    config_path: &Path,
    detach: bool,
    json: bool,
) -> Result<(), CliError> {
    // Resolution and validation are intentionally Python-free in the controller.
    let spec = resolve_run_spec(strategy_name, environment, event_mode, config_path)?;
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
    let spec_path = run_dir.join("run.json");
    let spec_json = serde_json::to_vec_pretty(&spec).map_err(CliError::ResultJson)?;
    atomic_write(&spec_path, &spec_json)?;
    let result_path = run_dir.join("result.json");
    let log_path = run_dir.join("worker.log");
    registry.create(
        &run_id,
        &owner_token,
        &spec.strategy.strategy_id,
        &spec.strategy.strategy_version,
        spec.environment.as_str(),
        spec.event_mode.as_str(),
        &spec_path,
        &spec.config_path,
        &spec.config_sha256,
        &result_path,
        &log_path,
    )?;
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
        #[cfg(unix)]
        // Safety: pre_exec only invokes the async-signal-safe setsid syscall in the child.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let child = command.spawn().map_err(CliError::Spawn)?;
        let _ = registry.spawned(&run_id, &owner_token, child.id())?;
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "schema_version": 1,
                    "run_id": run_id,
                    "state": "STARTING",
                    "strategy_id": strategy_name,
                    "environment": environment.as_str(),
                    "event_mode": event_mode.as_str()
                })
            );
        } else {
            println!("{run_id}");
        }
        Ok(())
    } else {
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
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
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
            let result = fs::read_to_string(&result_path).map_err(|source| CliError::Read {
                path: result_path,
                source,
            })?;
            println!("{result}");
            Ok(())
        } else {
            let detail = registry
                .get(&run_id)?
                .and_then(|run| run.error)
                .unwrap_or_else(|| format!("worker exited with {status}"));
            Err(CliError::WorkerFailed(detail))
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
    execution: BacktestExecutionSpec,
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
    match execution.asset {
        AssetModel::Linear => build_tick_backtest_with_asset(
            events,
            tick_size,
            lot_size,
            execution,
            LinearAsset::new(execution.contract_size),
        ),
        AssetModel::Inverse => build_tick_backtest_with_asset(
            events,
            tick_size,
            lot_size,
            execution,
            InverseAsset::new(execution.contract_size),
        ),
    }
}

fn build_tick_backtest_with_asset<AT>(
    events: &[Event],
    tick_size: f64,
    lot_size: f64,
    execution: BacktestExecutionSpec,
    asset_type: AT,
) -> Result<Backtest<HashMapMarketDepth>, CliError>
where
    AT: AssetType + Clone + 'static,
{
    match execution.queue {
        QueueModelSpec::RiskAverse => build_tick_backtest_with_models(
            events,
            tick_size,
            lot_size,
            execution,
            asset_type,
            RiskAdverseQueueModel::<HashMapMarketDepth>::new(),
        ),
        QueueModelSpec::PowerProbability => build_tick_backtest_with_models(
            events,
            tick_size,
            lot_size,
            execution,
            asset_type,
            ProbQueueModel::new(PowerProbQueueFunc3::new(execution.queue_power)),
        ),
    }
}

fn build_tick_backtest_with_models<AT, QM>(
    events: &[Event],
    tick_size: f64,
    lot_size: f64,
    execution: BacktestExecutionSpec,
    asset_type: AT,
    queue_model: QM,
) -> Result<Backtest<HashMapMarketDepth>, CliError>
where
    AT: AssetType + Clone + 'static,
    QM: QueueModel<HashMapMarketDepth> + 'static,
{
    let exchange = match execution.exchange {
        ExchangeModel::NoPartialFill => ExchangeKind::NoPartialFillExchange,
        ExchangeModel::PartialFill => ExchangeKind::PartialFillExchange,
    };
    let asset = L2AssetBuilder::default()
        .data(vec![DataSource::Data(Data::from_data(events))])
        .latency_offset(execution.latency_offset_ns)
        .latency_model(ConstantLatency::new(
            execution.entry_latency_ns,
            execution.response_latency_ns,
        ))
        .asset_type(asset_type)
        .fee_model(TradingValueFeeModel::new(CommonFees::new(
            execution.maker_fee,
            execution.taker_fee,
        )))
        .queue_model(queue_model)
        .exchange(exchange)
        .depth(move || HashMapMarketDepth::new(tick_size, lot_size))
        .last_trades_capacity(execution.last_trades_capacity)
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
        BackendSpec::Backtest { source, execution } => match source {
            BacktestSourceSpec::Bar { data } => {
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
            BacktestSourceSpec::Tick {
                data,
                tick_size,
                lot_size,
                frame_interval_ns,
                max_tick_batch,
            } => {
                let events = read_ticks(&data)?;
                let mut backtest = build_tick_backtest(&events, tick_size, lot_size, execution)?;
                let mut source =
                    TickFrameSource::new(&mut backtest, frame_interval_ns, max_tick_batch);
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
            BacktestSourceSpec::Hybrid {
                tick_data,
                bar_data,
                tick_size,
                lot_size,
                frame_interval_ns,
                max_tick_batch,
            } => {
                let events = read_ticks(&tick_data)?;
                let bars = read_bars(&bar_data)?;
                let mut backtest = build_tick_backtest(&events, tick_size, lot_size, execution)?;
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
        },
        BackendSpec::CoreLive { .. } => Err(CliError::Engine(
            "Core live backends are executed by the Core Runtime worker".into(),
        )),
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
    registry.transition(run_id, token, "COMPILING")?;
    if let BackendSpec::CoreLive { strategy_key } = &spec.backend {
        return core_live_worker(&registry, run_id, token, &spec, strategy_key, stop);
    }
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
    if loaded.metadata.strategy_id != spec.strategy.strategy_id
        || loaded.metadata.strategy_version != spec.strategy.strategy_version
    {
        return Err(CliError::Engine(format!(
            "compiled strategy identity {}@{} does not match manifest {}@{}",
            loaded.metadata.strategy_id,
            loaded.metadata.strategy_version,
            spec.strategy.strategy_id,
            spec.strategy.strategy_version
        )));
    }
    let required_callbacks: &[&str] = match spec.event_mode {
        EventMode::Bar => &["bar"],
        EventMode::Tick => &["tick"],
        EventMode::Hybrid => &["bar", "tick"],
    };
    if let Some(missing) = required_callbacks.iter().find(|required| {
        !loaded
            .metadata
            .capabilities
            .iter()
            .any(|actual| actual == **required)
    }) {
        return Err(CliError::Engine(format!(
            "compiled strategy is missing required {missing} callback"
        )));
    }
    registry.transition(run_id, token, "READY")?;
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
    registry.transition(run_id, token, "RUNNING")?;
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
        run_id: run_id.into(),
        strategy_id: loaded.metadata.strategy_id.clone(),
        strategy_version: loaded.metadata.strategy_version.clone(),
        environment: spec.environment,
        event_mode: spec.event_mode,
        config_sha256: spec.config_sha256,
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
    registry.update_metrics(
        run_id,
        token,
        result.market_event_count,
        result.order_count,
        result.fill_count,
    )?;
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

fn core_live_worker(
    registry: &Registry,
    run_id: &str,
    token: &str,
    spec: &RunSpec,
    strategy_key: &str,
    stop: Arc<AtomicBool>,
) -> Result<(), CliError> {
    let adapted =
        load_core_configuration(&spec.config_path, Some((strategy_key, spec.event_mode)))?;
    let mut runtime = titan_cli::ConfiguredCoreRuntime::start(adapted)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    registry.transition(run_id, token, "READY")?;
    registry.transition(run_id, token, "RUNNING")?;
    let started = Instant::now();
    while !stop.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(50));
    }
    runtime
        .shutdown(titan_plugin_engine::StopReason::Shutdown)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    let result = serde_json::json!({
        "schema_version": 1,
        "run_id": run_id,
        "strategy_id": strategy_key,
        "strategy_version": spec.strategy.strategy_version,
        "environment": spec.environment,
        "event_mode": spec.event_mode,
        "config_sha256": spec.config_sha256,
        "wall_time_ns": started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        "status": "STOPPED"
    });
    let result_json = serde_json::to_string_pretty(&result).map_err(CliError::ResultJson)?;
    let record = registry
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    atomic_write(&record.result_path, result_json.as_bytes())?;
    registry.update_metrics(run_id, token, 0, 0, 0)?;
    registry.finish(run_id, token, "STOPPED", 0, None)?;
    println!("{result_json}");
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

fn list_runs(
    json: bool,
    active: bool,
    environment: Option<Environment>,
    event_mode: Option<EventMode>,
    strategy: Option<&str>,
    status: Option<RunStatus>,
) -> Result<(), CliError> {
    let registry = Registry::open(&registry_path())?;
    registry.reconcile()?;
    let mut runs = registry.list()?;
    const ACTIVE: &[&str] = &[
        "STARTING",
        "LOADING",
        "COMPILING",
        "READY",
        "RUNNING",
        "STOP_REQUESTED",
    ];
    runs.retain(|run| {
        (!active || ACTIVE.contains(&run.state.as_str()))
            && environment.is_none_or(|value| run.environment == value.as_str())
            && event_mode.is_none_or(|value| run.event_mode == value.as_str())
            && strategy.is_none_or(|value| run.strategy_id == value)
            && status.is_none_or(|value| run.state == value.as_str())
    });
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "runs": runs
            }))
            .map_err(CliError::ResultJson)?
        );
    } else {
        println!("ID\tSTRATEGY\tENV\tMODE\tSTATUS\tPID\tREPORT");
        for run in runs {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                run.id,
                run.strategy_id,
                run.environment,
                run.event_mode,
                run.state,
                run.pid.map_or("-".into(), |p| p.to_string()),
                run.report_state
            );
        }
    }
    Ok(())
}

fn show_run(run_id: &str, json: bool) -> Result<(), CliError> {
    let registry = Registry::open(&registry_path())?;
    registry.reconcile()?;
    let run = registry
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "run": run
            }))
            .map_err(CliError::ResultJson)?
        );
    } else {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            run.id,
            run.strategy_id,
            run.environment,
            run.event_mode,
            run.state,
            run.health,
            run.pid.map_or("-".into(), |pid| pid.to_string())
        );
    }
    Ok(())
}

fn logs(run_id: &str, json: bool) -> Result<(), CliError> {
    let run = Registry::open(&registry_path())?
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    let content = fs::read_to_string(&run.log_path).map_err(|source| CliError::Read {
        path: run.log_path.clone(),
        source,
    })?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "schema_version": 1,
                "run_id": run_id,
                "log_path": run.log_path,
                "content": content
            })
        );
    } else {
        print!("{content}");
    }
    Ok(())
}

fn stop_run(run_id: &str, json: bool) -> Result<(), CliError> {
    let registry = Registry::open(&registry_path())?;
    let action = registry
        .request_stop(run_id)?
        .ok_or_else(|| CliError::NotRunning(run_id.into()))?;
    if action == StopAction::Cancelled {
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "schema_version": 1,
                    "run_id": run_id,
                    "state": "CANCELLED",
                    "pid": null
                })
            );
        } else {
            println!("CANCELLED");
        }
        return Ok(());
    }
    let StopAction::Signal(process) = action else {
        unreachable!()
    };
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
    if json {
        println!(
            "{}",
            serde_json::json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "STOP_REQUESTED",
                "pid": pid
            })
        );
    } else {
        println!("STOP_REQUESTED");
    }
    Ok(())
}

fn normalized_absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(CliError::Spawn)?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return Err(CliError::Engine(
                        "report output escapes filesystem root".into(),
                    ));
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

fn canonical_destination(path: &Path) -> Result<PathBuf, CliError> {
    let normalized = normalized_absolute_path(path)?;
    if normalized.exists() {
        return fs::canonicalize(&normalized).map_err(|source| CliError::Read {
            path: normalized,
            source,
        });
    }
    let mut ancestor = normalized.clone();
    let mut missing = Vec::new();
    while !ancestor.exists() {
        let name = ancestor
            .file_name()
            .ok_or_else(|| CliError::Engine("report output has no existing ancestor".into()))?;
        missing.push(name.to_os_string());
        if !ancestor.pop() {
            return Err(CliError::Engine(
                "report output has no existing ancestor".into(),
            ));
        }
    }
    let mut destination = fs::canonicalize(&ancestor).map_err(|source| CliError::Read {
        path: ancestor,
        source,
    })?;
    for component in missing.iter().rev() {
        destination.push(component);
    }
    Ok(destination)
}

fn report(run_id: &str, output: Option<&Path>, renderer: &str, json: bool) -> Result<(), CliError> {
    if !matches!(renderer, "native" | "quantstats") {
        return Err(CliError::Engine(format!(
            "unsupported report renderer {renderer}"
        )));
    }
    let registry = Registry::open(&registry_path())?;
    let run = registry
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
        let output = normalized_absolute_path(output)?;
        let destination = canonical_destination(&output)?;
        if destination.starts_with(&canonical_bundle) {
            return Err(CliError::Engine(
                "report output must be outside the immutable ResultBundle".into(),
            ));
        }
        let (_, report_token) = new_run_identity();
        if !registry.report_started(run_id, &report_token, &output)? {
            return Err(CliError::Engine(
                "report is already generating or run is not completed/stopped".into(),
            ));
        }
        let python = std::env::var_os("TITAN_REPORT_PYTHON").unwrap_or_else(|| "python3".into());
        let reporting_path = std::env::var_os("TITAN_REPORTING_PATH")
            .unwrap_or_else(|| "python/titan-reporting".into());
        let rendered = match Command::new(python)
            .env("PYTHONPATH", reporting_path)
            .args(["-m", "titan_reporting"])
            .arg(bundle)
            .arg("--output")
            .arg(&output)
            .arg("--renderer")
            .arg(renderer)
            .output()
        {
            Ok(output) => output,
            Err(error) => {
                let _ = registry.report_finished(run_id, &report_token, false);
                return Err(CliError::Spawn(error));
            }
        };
        if !rendered.status.success() {
            registry.report_finished(run_id, &report_token, false)?;
            return Err(CliError::ReportFailed(
                String::from_utf8_lossy(&rendered.stderr).trim().to_owned(),
            ));
        }
        if !registry.report_finished(run_id, &report_token, true)? {
            return Err(CliError::Engine("report ownership was lost".into()));
        }
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "schema_version": 1,
                    "run_id": run_id,
                    "renderer": renderer,
                    "report_path": output
                })
            );
        } else {
            println!("{}", output.display());
        }
        return Ok(());
    }
    println!("{result}");
    Ok(())
}

fn run_configured_core(config: &Path, json: bool) -> Result<(), CliError> {
    run_selected_core(config, None, json)
}

fn run_selected_core(
    config: &Path,
    selected: Option<(&str, EventMode)>,
    json: bool,
) -> Result<(), CliError> {
    let adapted = load_core_configuration(config, selected)?;
    let mut runtime = titan_cli::ConfiguredCoreRuntime::start(adapted)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    let interrupt = Arc::new(AtomicBool::new(false));
    let terminate = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, interrupt.clone())
        .map_err(CliError::Spawn)?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, terminate.clone())
        .map_err(CliError::Spawn)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "schema_version": 1,
                "status": "RUNNING"
            })
        );
        std::io::stdout().flush().map_err(CliError::Spawn)?;
    } else {
        println!("Titan Core Runtime is running; press Ctrl-C to stop");
    }
    while !interrupt.load(Ordering::Acquire) && !terminate.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(50));
    }
    let reason = if interrupt.load(Ordering::Acquire) {
        "SIGINT"
    } else {
        "SIGTERM"
    };
    runtime
        .shutdown(titan_plugin_engine::StopReason::Shutdown)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "schema_version": 1,
                "status": "STOPPED",
                "reason": reason
            })
        );
    }
    Ok(())
}

fn load_core_configuration(
    config: &Path,
    selected: Option<(&str, EventMode)>,
) -> Result<titan_cli::AdaptedConfiguration, CliError> {
    let mut adapted = titan_cli::ConfigurationAdapter::load_toml(config)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    if let Some((strategy_key, mode)) = selected {
        let selected = adapted
            .strategies
            .iter()
            .find(|definition| definition.strategy_key.as_ref() == strategy_key)
            .ok_or_else(|| {
                CliError::Engine(format!(
                    "strategy {strategy_key} is not defined in the Core Runtime config"
                ))
            })?;
        if !selected.enabled {
            return Err(CliError::Engine(format!(
                "strategy {strategy_key} is disabled"
            )));
        }
        let mode_matches = selected.markets.iter().all(|binding| {
            matches!(
                (mode, binding.data_mode),
                (
                    EventMode::Tick,
                    titan_strategy_plugin::StrategyDataMode::Tick
                ) | (
                    EventMode::Bar,
                    titan_strategy_plugin::StrategyDataMode::Bar { .. }
                ) | (
                    EventMode::Hybrid,
                    titan_strategy_plugin::StrategyDataMode::Hybrid { .. }
                )
            )
        });
        if !mode_matches {
            return Err(CliError::Engine(format!(
                "strategy {strategy_key} bindings do not match {} mode",
                mode.as_str()
            )));
        }
        adapted
            .strategies
            .retain(|definition| definition.strategy_key.as_ref() == strategy_key);
    }
    Ok(adapted)
}

fn main() -> ExitCode {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    let json_argument = arguments.iter().any(|argument| argument == "--json");
    let cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            if json_argument && exit_code != 0 {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "error": {
                            "code": "CLI_USAGE",
                            "message": error.to_string()
                        }
                    })
                );
            } else {
                let _ = error.print();
            }
            return ExitCode::from(exit_code.clamp(0, i32::from(u8::MAX)) as u8);
        }
    };
    let json_requested = cli.command.json_requested();
    let result = match cli.command {
        Commands::CoreRun { config, json } => run_configured_core(&config, json),
        Commands::Run {
            strategy,
            env,
            mode,
            config,
            detach,
            json,
        } => controller(&strategy, env, mode, &config, detach, json),
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
        Commands::Validate {
            strategy,
            env,
            mode,
            config,
            json,
        } => {
            if env == Environment::Live {
                load_core_configuration(&config, Some((&strategy, mode))).map(|_| {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "schema_version": 1,
                                "valid": true,
                                "strategy_id": strategy,
                                "environment": env.as_str(),
                                "event_mode": mode.as_str()
                            })
                        );
                    } else {
                        println!("valid");
                    }
                })
            } else {
                resolve_run_spec(&strategy, env, mode, &config).map(|spec| {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "schema_version": 1,
                                "valid": true,
                                "strategy_id": strategy,
                                "environment": spec.environment.as_str(),
                                "event_mode": spec.event_mode.as_str(),
                                "config_sha256": spec.config_sha256
                            })
                        );
                    } else {
                        println!("valid");
                    }
                })
            }
        }
        Commands::Ls {
            json,
            active,
            env,
            mode,
            strategy,
            status,
        } => list_runs(json, active, env, mode, strategy.as_deref(), status),
        Commands::Show { run_id, json } => show_run(&run_id, json),
        Commands::Logs { run_id, json } => logs(&run_id, json),
        Commands::Stop { run_id, json } => stop_run(&run_id, json),
        Commands::Report {
            run_id,
            output,
            renderer,
            json,
        } => report(&run_id, output.as_deref(), &renderer, json),
        Commands::Strategy { command } => strategy_command(command),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if json_requested {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "error": {
                            "code": error.code(),
                            "message": error.to_string()
                        }
                    })
                );
            } else {
                eprintln!("titan: {error}");
            }
            ExitCode::from(error.exit_code())
        }
    }
}
