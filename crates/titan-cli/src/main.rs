#[cfg(unix)]
use std::os::unix::process::CommandExt;
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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    /// Run the configured static TradingRuntime until SIGINT.
    CoreRun {
        #[arg(short = 'c', long = "config", value_name = "RUNTIME.toml")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Run a V13 artifact backtest or a configured Core Live strategy in an isolated worker.
    Run {
        /// `.titan` path for backtest; configured strategy_key for live.
        #[arg(value_name = "ARTIFACT_OR_STRATEGY_KEY")]
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
    /// Validate a V13 artifact/backtest trace or Core Live deployment without starting Python.
    Validate {
        #[arg(value_name = "ARTIFACT_OR_STRATEGY_KEY")]
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
    /// List compiled ABI V13 artifacts without importing strategy Python.
    Ls {
        #[arg(long = "trusted-key", value_name = "KEY_ID=HEX")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        require_signature: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show one compiled ABI V13 artifact manifest.
    Show {
        artifact: PathBuf,
        #[arg(long = "trusted-key", value_name = "KEY_ID=HEX")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        require_signature: bool,
        #[arg(long)]
        json: bool,
    },
    /// Validate one compiled ABI V13 artifact, including native digest and signature policy.
    Validate {
        artifact: PathBuf,
        #[arg(long = "trusted-key", value_name = "KEY_ID=HEX")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        require_signature: bool,
        #[arg(long)]
        json: bool,
    },
    /// Compile an ABI V13 strategy to a native artifact.
    Compile {
        #[arg(long, value_name = "strategy.py")]
        strategy: PathBuf,
        #[arg(long, default_value = "{}")]
        parameters: String,
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "x86-64")]
        cpu_baseline: String,
        #[arg(long, default_value = "pair", value_parser = ["pair", "bundle"])]
        artifact_format: String,
        #[arg(long, value_name = "ARTIFACT")]
        output: Option<PathBuf>,
        #[arg(long, value_name = "PRIVATE_KEY")]
        signing_key: Option<PathBuf>,
        #[arg(long, requires = "signing_key")]
        key_id: Option<String>,
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
                StrategyCommands::Ls { json, .. }
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
    artifact: PathBuf,
    artifact_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunConfig {
    schema_version: u32,
    #[serde(default = "default_history_capacity")]
    history_capacity: usize,
    #[serde(default)]
    backtest: Option<BacktestConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BacktestConfig {
    data: PathBuf,
    #[serde(default = "default_command_capacity")]
    command_capacity: usize,
    #[serde(default)]
    allow_unsigned_artifact: bool,
    #[serde(default)]
    trusted_ed25519_keys: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V13Trace {
    schema_version: u32,
    initial: V13TraceState,
    events: Vec<V13TraceEvent>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct V13TraceState {
    now_ns: i64,
    #[serde(default)]
    markets: Vec<titan_strategy_runtime::TitanMarketView>,
    #[serde(default)]
    positions: Vec<titan_strategy_runtime::TitanPositionView>,
    #[serde(default)]
    balances: Vec<titan_strategy_runtime::TitanBalanceView>,
    #[serde(default)]
    accounts: Vec<titan_strategy_runtime::TitanAccountView>,
    #[serde(default)]
    active_orders: Vec<titan_strategy_runtime::TitanActiveOrderView>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum V13TraceEventKind {
    Tick,
    Depth,
    Fill,
    Order,
    Cancel,
    Position,
    Balance,
    AccountState,
    Timer,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V13TraceEvent {
    kind: V13TraceEventKind,
    now_ns: i64,
    #[serde(default)]
    state: Option<V13TraceState>,
    #[serde(default)]
    ticks: Vec<titan_strategy_runtime::TitanTickView>,
    #[serde(default)]
    depth: Vec<titan_strategy_runtime::TitanDepthView>,
    #[serde(default)]
    fills: Vec<titan_strategy_runtime::TitanFillView>,
    #[serde(default)]
    orders: Vec<titan_strategy_runtime::TitanOrderEventView>,
    #[serde(default)]
    cancels: Vec<titan_strategy_runtime::TitanCancelEventView>,
    #[serde(default)]
    positions: Vec<titan_strategy_runtime::TitanPositionEventView>,
    #[serde(default)]
    balances: Vec<titan_strategy_runtime::TitanBalanceEventView>,
    #[serde(default)]
    account_states: Vec<titan_strategy_runtime::TitanAccountStateEventView>,
    #[serde(default)]
    timer: Option<titan_strategy_runtime::TitanTimerView>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendSpec {
    Backtest {
        data: PathBuf,
        command_capacity: usize,
        allow_unsigned_artifact: bool,
        trusted_ed25519_keys: BTreeMap<String, String>,
    },
    CoreLive {
        strategy_key: String,
    },
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
    #[error("history_capacity must be positive")]
    HistoryCapacity,
    #[error("worker spawn failed: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("worker failed: {0}")]
    WorkerFailed(String),
    #[error("report generation failed: {0}")]
    ReportFailed(String),
    #[error("ABI V13 strategy compilation failed: {0}")]
    StaticCompile(String),
    #[error("invalid configuration: {0}")]
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
    fn code(&self) -> &'static str {
        match self {
            Self::Schema(_) => "INVALID_SCHEMA",
            Self::HistoryCapacity => "INVALID_HISTORY_CAPACITY",
            Self::Json { .. } => "INVALID_JSON",
            Self::Toml { .. } => "INVALID_TOML",
            Self::Engine(_) => "INVALID_CONFIGURATION",
            Self::StaticCompile(_) => "STRATEGY_COMPILE_FAILED",
            Self::WorkerFailed(_) => "WORKER_FAILED",
            Self::ReportFailed(_) => "REPORT_FAILED",
            Self::Registry(_) => "REGISTRY_FAILED",
            Self::RunNotFound(_) => "RUN_NOT_FOUND",
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
            | Self::HistoryCapacity
            | Self::Json { .. }
            | Self::Toml { .. }
            | Self::Engine(_) => 10,
            Self::StaticCompile(_) => 20,
            Self::WorkerFailed(_) => 31,
            Self::ReportFailed(_) => 32,
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

fn default_command_capacity() -> usize {
    1_024
}

fn registry_path() -> PathBuf {
    std::env::var_os("TITAN_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".titan"))
        .join("runs.sqlite3")
}

fn strategy_command(command: StrategyCommands) -> Result<(), CliError> {
    match command {
        StrategyCommands::Ls {
            trusted_keys,
            require_signature,
            json,
        } => {
            let trust = cli_trust_policy(require_signature, &trusted_keys)?;
            let root = PathBuf::from("deploy");
            let mut catalog = Vec::new();
            if root.is_dir() {
                for deployment in fs::read_dir(&root)
                    .map_err(|source| CliError::Read {
                        path: root.clone(),
                        source,
                    })?
                    .flatten()
                {
                    let artifacts = deployment.path().join("artifacts");
                    if !artifacts.is_dir() {
                        continue;
                    }
                    for entry in fs::read_dir(&artifacts)
                        .map_err(|source| CliError::Read {
                            path: artifacts.clone(),
                            source,
                        })?
                        .flatten()
                    {
                        let source = entry.path();
                        if source.extension().and_then(|value| value.to_str()) != Some("titan") {
                            continue;
                        }
                        catalog.push(v13_manifest_json(&source, trust.clone())?);
                    }
                }
            }
            catalog
                .sort_by_key(|value| value["strategy_id"].as_str().unwrap_or_default().to_owned());
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
                println!("STRATEGY\tVERSION\tABI\tSIGNED\tARTIFACT");
                for item in catalog {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        item["strategy_id"].as_str().unwrap_or("-"),
                        item["strategy_version"].as_str().unwrap_or("-"),
                        item["abi_version"].as_u64().unwrap_or_default(),
                        item["signed"].as_bool().unwrap_or(false),
                        item["artifact"].as_str().unwrap_or("-"),
                    );
                }
            }
            Ok(())
        }
        StrategyCommands::Show {
            artifact,
            trusted_keys,
            require_signature,
            json,
        } => {
            let trust = cli_trust_policy(require_signature, &trusted_keys)?;
            let manifest = v13_manifest_json(&artifact, trust)?;
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
        StrategyCommands::Validate {
            artifact,
            trusted_keys,
            require_signature,
            json,
        } => {
            let trust = cli_trust_policy(require_signature, &trusted_keys)?;
            let manifest = v13_manifest_json(&artifact, trust)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "valid": true,
                        "strategy_id": manifest["strategy_id"],
                        "strategy_version": manifest["strategy_version"],
                        "artifact_digest": manifest["artifact_digest"]
                    })
                );
            } else {
                println!("valid");
            }
            Ok(())
        }
        StrategyCommands::Compile {
            strategy,
            parameters,
            target,
            cpu_baseline,
            artifact_format,
            output,
            signing_key,
            key_id,
            json,
        } => compile_v13_strategy(
            &strategy,
            &parameters,
            target.as_deref(),
            &cpu_baseline,
            &artifact_format,
            output.as_deref(),
            signing_key.as_deref(),
            key_id.as_deref(),
            json,
        ),
    }
}

fn v13_manifest_json(
    path: &Path,
    trust: titan_strategy_runtime::V13TrustPolicy,
) -> Result<serde_json::Value, CliError> {
    let path = fs::canonicalize(path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    let manifest = inspect_v13_artifact_with_policy(&path, trust)?;
    Ok(serde_json::json!({
        "strategy_id": manifest.strategy_id,
        "strategy_version": manifest.strategy_version,
        "abi_version": 13,
        "artifact_digest": hex_digest(&manifest.artifact_digest),
        "native_digest": hex_digest(&manifest.native_digest),
        "target_triple": manifest.target_triple,
        "cpu_baseline": manifest.cpu_baseline,
        "signed": manifest.signature.is_some(),
        "key_id": manifest.signature.as_ref().map(|value| value.key_id.as_ref()),
        "artifact": path,
    }))
}

fn inspect_v13_artifact_with_policy(
    path: &Path,
    trust: titan_strategy_runtime::V13TrustPolicy,
) -> Result<titan_strategy_runtime::ArtifactManifestV13, CliError> {
    use titan_strategy_runtime::NativeArtifactLoaderV13;
    let loader =
        NativeArtifactLoaderV13::new(std::env::temp_dir().join("titan-cli-v13-inspect"), trust);
    loader
        .inspect(path)
        .map_err(|error| CliError::Engine(error.to_string()))
}

fn cli_trust_policy(
    require_signature: bool,
    trusted_keys: &[String],
) -> Result<titan_strategy_runtime::V13TrustPolicy, CliError> {
    let mut ed25519_keys = BTreeMap::new();
    for item in trusted_keys {
        let (key_id, encoded) = item
            .split_once('=')
            .ok_or_else(|| CliError::Engine("--trusted-key must use KEY_ID=HEX format".into()))?;
        if key_id.is_empty() {
            return Err(CliError::Engine(
                "--trusted-key key id cannot be empty".into(),
            ));
        }
        let key = decode_ed25519_key(encoded).map_err(|message| {
            CliError::Engine(format!("invalid trusted Ed25519 key {key_id}: {message}"))
        })?;
        if ed25519_keys.insert(Arc::from(key_id), key).is_some() {
            return Err(CliError::Engine(format!(
                "duplicate trusted Ed25519 key id {key_id}"
            )));
        }
    }
    if require_signature && ed25519_keys.is_empty() {
        return Err(CliError::Engine(
            "--require-signature requires at least one --trusted-key KEY_ID=HEX".into(),
        ));
    }
    Ok(titan_strategy_runtime::V13TrustPolicy {
        require_signature,
        ed25519_keys,
    })
}

fn backtest_trust_policy(
    allow_unsigned_artifact: bool,
    trusted_ed25519_keys: &BTreeMap<String, String>,
) -> Result<titan_strategy_runtime::V13TrustPolicy, CliError> {
    let ed25519_keys = trusted_ed25519_keys
        .iter()
        .map(|(key_id, encoded)| {
            decode_ed25519_key(encoded)
                .map(|key| (Arc::from(key_id.as_str()), key))
                .map_err(|message| {
                    CliError::Engine(format!(
                        "invalid backtest trusted Ed25519 key {key_id}: {message}"
                    ))
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if !allow_unsigned_artifact && ed25519_keys.is_empty() {
        return Err(CliError::Engine(
            "backtest requires trusted_ed25519_keys unless allow_unsigned_artifact is explicitly enabled"
                .into(),
        ));
    }
    Ok(titan_strategy_runtime::V13TrustPolicy {
        require_signature: !allow_unsigned_artifact,
        ed25519_keys,
    })
}

fn decode_ed25519_key(value: &str) -> Result<[u8; 32], &'static str> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err("public key must contain 64 hexadecimal characters");
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "public key contains non-hexadecimal characters")?;
    }
    Ok(output)
}

fn hex_digest(value: &[u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn compile_v13_strategy(
    strategy: &Path,
    parameters: &str,
    target: Option<&str>,
    cpu_baseline: &str,
    artifact_format: &str,
    output: Option<&Path>,
    signing_key: Option<&Path>,
    key_id: Option<&str>,
    json: bool,
) -> Result<(), CliError> {
    let output = output.ok_or_else(|| {
        CliError::Engine("ABI V13 compile requires --output <artifact-path>".into())
    })?;
    if parameters == "{}" {
        return Err(CliError::Engine(
            "ABI V13 compile requires --parameters <parameters.json>".into(),
        ));
    }
    if !strategy.is_file() {
        return Err(CliError::Engine(format!(
            "ABI V13 strategy source does not exist: {}",
            strategy.display()
        )));
    }
    let parameters_path = Path::new(parameters);
    if !parameters_path.is_file() {
        return Err(CliError::Engine(format!(
            "ABI V13 parameters file does not exist: {}",
            parameters_path.display()
        )));
    }

    let sdk_path = std::env::var_os("TITAN_STRATEGY_SDK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python/titan-strategy-sdk"));
    if !sdk_path.is_dir() {
        return Err(CliError::Engine(format!(
            "Titan Strategy SDK directory does not exist: {}",
            sdk_path.display()
        )));
    }
    let mut python_paths = vec![
        fs::canonicalize(&sdk_path).map_err(|source| CliError::Read {
            path: sdk_path.clone(),
            source,
        })?,
    ];
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        python_paths.extend(std::env::split_paths(&existing));
    }
    let python_path = std::env::join_paths(python_paths)
        .map_err(|error| CliError::Engine(format!("cannot construct PYTHONPATH: {error}")))?;
    let python = std::env::var_os("TITAN_STRATEGY_PYTHON")
        .unwrap_or_else(|| std::ffi::OsString::from("python3"));
    let mut command = Command::new(python);
    command
        .env("PYTHONPATH", python_path)
        .arg("-m")
        .arg("titan_strategy.cli")
        .arg("compile")
        .arg("--strategy")
        .arg(strategy)
        .arg("--parameters")
        .arg(parameters_path)
        .arg("--cpu-baseline")
        .arg(cpu_baseline)
        .arg("--artifact-format")
        .arg(artifact_format)
        .arg("--output")
        .arg(output);
    if let Some(target) = target {
        command.arg("--target").arg(target);
    }
    if let Some(signing_key) = signing_key {
        command.arg("--signing-key").arg(signing_key);
        command.arg("--key-id").arg(
            key_id.ok_or_else(|| {
                CliError::Engine("--key-id is required with --signing-key".into())
            })?,
        );
    }
    let result = command.output().map_err(CliError::Spawn)?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr).trim().to_owned();
        return Err(CliError::StaticCompile(if stderr.is_empty() {
            format!("compiler exited with {}", result.status)
        } else {
            stderr
        }));
    }
    let stdout = String::from_utf8(result.stdout).map_err(|error| {
        CliError::StaticCompile(format!("compiler output is not UTF-8: {error}"))
    })?;
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).map_err(|error| {
        CliError::StaticCompile(format!("compiler returned invalid JSON: {error}"))
    })?;
    if json {
        println!("{value}");
    } else {
        println!(
            "{}\t{}\t{}",
            value["strategy_id"].as_str().unwrap_or("-"),
            value["strategy_version"].as_str().unwrap_or("-"),
            value["artifact_digest"].as_str().unwrap_or("-")
        );
        if let Some(paths) = value["paths"].as_array() {
            for path in paths.iter().filter_map(serde_json::Value::as_str) {
                println!("{path}");
            }
        }
    }
    Ok(())
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
    strategy_target: &str,
    environment: Environment,
    event_mode: EventMode,
    config_path: &Path,
) -> Result<RunSpec, CliError> {
    if environment == Environment::Live {
        return resolve_core_live_run_spec(strategy_target, event_mode, config_path);
    }
    if event_mode != EventMode::Tick {
        return Err(CliError::Engine(format!(
            "ABI V13 backtest currently supports tick event traces only; {} is not published",
            event_mode.as_str(),
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
    let backend = config.backtest.ok_or_else(|| {
        CliError::Engine("backtest environment requires a [backtest] section".into())
    })?;
    if backend.command_capacity == 0 {
        return Err(CliError::Engine(
            "backtest.command_capacity must be positive".into(),
        ));
    }
    let artifact = fs::canonicalize(strategy_target).map_err(|source| CliError::Read {
        path: PathBuf::from(strategy_target),
        source,
    })?;
    let trust = backtest_trust_policy(
        backend.allow_unsigned_artifact,
        &backend.trusted_ed25519_keys,
    )?;
    let manifest = inspect_v13_artifact_with_policy(&artifact, trust)?;
    let data = resolve_path(base, &backend.data)?;

    let spec = RunSpec {
        schema_version: RUN_SPEC_VERSION,
        environment,
        event_mode,
        config_path,
        config_sha256: format!("{:x}", Sha256::digest(&config_bytes)),
        strategy: StrategyRunSpec {
            strategy_id: manifest.strategy_id.to_string(),
            strategy_version: manifest.strategy_version.to_string(),
            artifact,
            artifact_digest: hex_digest(&manifest.artifact_digest),
        },
        backend: BackendSpec::Backtest {
            data,
            command_capacity: backend.command_capacity,
            allow_unsigned_artifact: backend.allow_unsigned_artifact,
            trusted_ed25519_keys: backend.trusted_ed25519_keys,
        },
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
    if !matches!(event_mode, EventMode::Tick) {
        return Err(CliError::Engine(
            "live Core Runtime currently supports tick mode only; Bar/Hybrid live profiles are disabled until a production BarBatch publisher is implemented".into(),
        ));
    }
    let config_path = fs::canonicalize(config_path).map_err(|source| CliError::Read {
        path: config_path.into(),
        source,
    })?;
    let config_bytes = fs::read(&config_path).map_err(|source| CliError::Read {
        path: config_path.clone(),
        source,
    })?;
    let adapted = load_core_configuration(&config_path, Some((strategy_name, event_mode)))?;
    let trust = adapted.v13_trust_policy();
    let definition = adapted
        .strategies
        .first()
        .ok_or_else(|| CliError::Engine(format!("strategy {strategy_name} is not enabled")))?;
    let artifact = definition
        .package
        .uri
        .strip_prefix("file://")
        .ok_or_else(|| CliError::Engine("V13 artifact URI must use file://".into()))?;
    let artifact = fs::canonicalize(artifact).map_err(|source| CliError::Read {
        path: PathBuf::from(artifact),
        source,
    })?;
    let manifest = inspect_v13_artifact_with_policy(&artifact, trust)?;
    let spec = RunSpec {
        schema_version: RUN_SPEC_VERSION,
        environment: Environment::Live,
        event_mode,
        config_path,
        config_sha256: format!("{:x}", Sha256::digest(&config_bytes)),
        strategy: StrategyRunSpec {
            strategy_id: definition.strategy_key.to_string(),
            strategy_version: manifest.strategy_version.to_string(),
            artifact,
            artifact_digest: hex_digest(&manifest.artifact_digest),
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
    if spec.strategy.artifact_digest.len() != 64
        || !spec
            .strategy
            .artifact_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(CliError::Engine(
            "resolved V13 artifact digest is invalid".into(),
        ));
    }
    if spec.history_capacity == 0 {
        return Err(CliError::HistoryCapacity);
    }
    match &spec.backend {
        BackendSpec::Backtest {
            data,
            command_capacity,
            allow_unsigned_artifact,
            trusted_ed25519_keys,
        } => {
            if spec.environment != Environment::Backtest {
                return Err(CliError::Engine(
                    "backtest backend requires backtest environment".into(),
                ));
            }
            if spec.event_mode != EventMode::Tick {
                return Err(CliError::Engine(
                    "V13 trace backtest requires tick mode".into(),
                ));
            }
            if *command_capacity == 0 || !data.is_file() {
                return Err(CliError::Engine("invalid V13 trace backtest input".into()));
            }
            backtest_trust_policy(*allow_unsigned_artifact, trusted_ed25519_keys)?;
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
    v13_backtest_worker(&registry, run_id, token, &spec, stop)
}

fn v13_backtest_worker(
    registry: &Registry,
    run_id: &str,
    token: &str,
    spec: &RunSpec,
    stop: Arc<AtomicBool>,
) -> Result<(), CliError> {
    use titan_strategy_runtime::{NativeArtifactLoaderV13, OfflineV13Adapter, StagedCommandV13};
    let BackendSpec::Backtest {
        data,
        command_capacity,
        allow_unsigned_artifact,
        trusted_ed25519_keys,
    } = &spec.backend
    else {
        return Err(CliError::Engine(
            "backtest worker received a live spec".into(),
        ));
    };
    let trace_bytes = fs::read(data).map_err(|source| CliError::Read {
        path: data.clone(),
        source,
    })?;
    let trace =
        serde_json::from_slice::<V13Trace>(&trace_bytes).map_err(|source| CliError::Json {
            path: data.clone(),
            source,
        })?;
    if trace.schema_version != 1 {
        return Err(CliError::Engine(format!(
            "unsupported V13 trace schema_version {}",
            trace.schema_version
        )));
    }
    let trust = backtest_trust_policy(*allow_unsigned_artifact, trusted_ed25519_keys)?;
    let loader = NativeArtifactLoaderV13::new(
        registry_path()
            .parent()
            .unwrap_or(Path::new("."))
            .join("artifact-cache"),
        trust,
    );
    let artifact = loader
        .load(&spec.strategy.artifact)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    if hex_digest(&artifact.manifest.artifact_digest) != spec.strategy.artifact_digest {
        return Err(CliError::Engine(
            "V13 artifact digest changed after run resolution".into(),
        ));
    }
    let initial = trace.initial;
    let mut adapter = OfflineV13Adapter::new(
        Arc::new(artifact),
        1,
        1,
        *command_capacity,
        initial.markets,
        initial.positions,
        initial.balances,
        initial.accounts,
        initial.active_orders,
    )
    .map_err(|error| CliError::Engine(error.to_string()))?;
    adapter
        .start(initial.now_ns)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    registry.transition(run_id, token, "READY")?;
    registry.transition(run_id, token, "RUNNING")?;
    let started = Instant::now();
    let mut event_count = 0_u64;
    let mut submit_count = 0_u64;
    let mut cancel_count = 0_u64;
    let mut last_now_ns = initial.now_ns;
    for event in trace.events {
        if stop.load(Ordering::Acquire) {
            break;
        }
        if let Some(update) = event.state {
            adapter.update_public_state(
                update.markets,
                update.positions,
                update.balances,
                update.accounts,
                update.active_orders,
            );
        }
        last_now_ns = last_now_ns.max(event.now_ns);
        let commands = match event.kind {
            V13TraceEventKind::Tick => adapter.on_ticks(event.now_ns, &event.ticks),
            V13TraceEventKind::Depth => adapter.on_depth(event.now_ns, &event.depth),
            V13TraceEventKind::Fill => adapter.on_fills(event.now_ns, &event.fills),
            V13TraceEventKind::Order => adapter.on_orders(event.now_ns, &event.orders),
            V13TraceEventKind::Cancel => adapter.on_cancels(event.now_ns, &event.cancels),
            V13TraceEventKind::Position => adapter.on_positions(event.now_ns, &event.positions),
            V13TraceEventKind::Balance => adapter.on_balances(event.now_ns, &event.balances),
            V13TraceEventKind::AccountState => {
                adapter.on_account_states(event.now_ns, &event.account_states)
            }
            V13TraceEventKind::Timer => adapter.on_timer(
                event.now_ns,
                &event
                    .timer
                    .unwrap_or(titan_strategy_runtime::TitanTimerView {
                        fired_ts_ns: event.now_ns,
                        ..Default::default()
                    }),
            ),
        }
        .map_err(|error| CliError::Engine(error.to_string()))?;
        for command in commands {
            match command {
                StagedCommandV13::Submit { .. } => submit_count += 1,
                StagedCommandV13::Cancel { .. } => cancel_count += 1,
            }
        }
        event_count += 1;
    }
    let stop_commands = adapter
        .stop(last_now_ns)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    for command in stop_commands {
        match command {
            StagedCommandV13::Submit { .. } => submit_count += 1,
            StagedCommandV13::Cancel { .. } => cancel_count += 1,
        }
    }
    let state_sha256 = format!("{:x}", Sha256::digest(adapter.state_bytes()));
    let status = if stop.load(Ordering::Acquire) {
        "STOPPED"
    } else {
        "COMPLETED"
    };
    let result = serde_json::json!({
        "schema_version": 1,
        "run_id": run_id,
        "strategy_id": spec.strategy.strategy_id,
        "strategy_version": spec.strategy.strategy_version,
        "artifact_digest": spec.strategy.artifact_digest,
        "environment": spec.environment,
        "event_mode": spec.event_mode,
        "config_sha256": spec.config_sha256,
        "trace_sha256": format!("{:x}", Sha256::digest(&trace_bytes)),
        "state_sha256": state_sha256,
        "event_count": event_count,
        "submit_count": submit_count,
        "cancel_count": cancel_count,
        "wall_time_ns": started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        "status": status,
    });
    let result_json = serde_json::to_string_pretty(&result).map_err(CliError::ResultJson)?;
    let record = registry
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    atomic_write(&record.result_path, result_json.as_bytes())?;
    let exit_code = 0;
    let final_state = if status == "COMPLETED" {
        "COMPLETED"
    } else {
        "STOPPED"
    };
    if !registry.finish(run_id, token, final_state, exit_code, None)? {
        return Err(CliError::Engine(
            "run ownership changed before backtest completion".into(),
        ));
    }
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
    let mut runtime = titan_cli::TradingRuntime::start(adapted)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    registry.transition(run_id, token, "READY")?;
    registry.transition(run_id, token, "RUNNING")?;
    let started = Instant::now();
    while !stop.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(50));
    }
    runtime
        .shutdown()
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
    let mut runtime = titan_cli::TradingRuntime::start(adapted)
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
        .shutdown()
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
                    titan_strategy_runtime::StrategyDataMode::Tick
                ) | (
                    EventMode::Bar,
                    titan_strategy_runtime::StrategyDataMode::Bar { .. }
                ) | (
                    EventMode::Hybrid,
                    titan_strategy_runtime::StrategyDataMode::Hybrid { .. }
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
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

#[cfg(test)]
mod cli_v13_tests {
    use super::*;

    #[test]
    fn parses_documented_v13_compile_command() {
        let cli = Cli::try_parse_from([
            "titan",
            "strategy",
            "compile",
            "--strategy",
            "strategy.py",
            "--parameters",
            "parameters.json",
            "--target",
            "x86_64-unknown-linux-gnu",
            "--cpu-baseline",
            "x86-64-v2",
            "--artifact-format",
            "bundle",
            "--output",
            "pair.titan",
        ])
        .expect("documented V13 compile command must parse");
        let Commands::Strategy {
            command:
                StrategyCommands::Compile {
                    strategy,
                    artifact_format,
                    output,
                    ..
                },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(strategy, PathBuf::from("strategy.py"));
        assert_eq!(artifact_format, "bundle");
        assert_eq!(output, Some(PathBuf::from("pair.titan")));
    }

    #[test]
    fn strategy_validation_trust_keys_are_explicit_and_strict() {
        assert!(cli_trust_policy(true, &[]).is_err());
        assert!(cli_trust_policy(false, &["missing-separator".into()]).is_err());

        let policy = cli_trust_policy(true, &[format!("production-2026-09={}", "01".repeat(32))])
            .expect("valid key must build a signature-required trust policy");
        assert!(policy.require_signature);
        assert_eq!(policy.ed25519_keys["production-2026-09"], [1_u8; 32]);
    }
}
