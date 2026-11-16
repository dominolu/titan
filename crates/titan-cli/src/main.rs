#[cfg(unix)]
use std::os::unix::process::CommandExt;
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

mod compiler;
mod recovery_cli;
mod storage_cli;
mod strategy_workspace;
use registry::{Registry, StopAction, now_ns, process_start_time};
use titan_cli::registry;

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const RUN_SPEC_VERSION: u32 = 1;

#[derive(Parser)]
#[command(
    name = "titan",
    version,
    about = "Titan background service and compatible runtime tools; use titan_cli for daily management"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ServeRole {
    Manager,
    Node,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the background management service or an execution node. Use titan_cli for daily operations.
    #[command(
        after_help = "Examples:\n  titan serve --config /private/server.toml\n  titan serve node --config /private/node.toml\n\nThe manager does not start strategies automatically."
    )]
    Serve {
        #[arg(value_enum, default_value = "manager")]
        role: ServeRole,
        #[arg(long, value_name = "SERVER.toml")]
        config: PathBuf,
    },
    /// Query local facts and maintain immutable result publications without executing strategies.
    Storage {
        #[command(subcommand)]
        command: storage_cli::StorageCommand,
    },
    /// Inspect, migrate, export and back up platform recovery storage without trading.
    Recovery {
        #[arg(long)]
        database: PathBuf,
        #[command(subcommand)]
        command: recovery_cli::RecoveryCommand,
    },
    /// Validate configured instrument units without starting a service or touching an exchange.
    Instruments {
        #[command(subcommand)]
        command: InstrumentsCommands,
    },
    /// Serve or access the management application and authenticated execution node.
    Console {
        #[command(flatten)]
        options: titan_cli::management::ClientOptions,
        #[command(subcommand)]
        command: titan_cli::management::ConsoleCommand,
    },
    /// Run the configured static TradingRuntime until SIGINT.
    CoreRun {
        #[arg(short = 'c', long = "config", value_name = "RUNTIME.toml")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Run a V13 compiled_strategy backtest or a configured Core Live strategy in an isolated worker.
    Run {
        /// `.titan` path for backtest; configured strategy_key for live.
        #[arg(value_name = "STRATEGY_FILE_OR_KEY")]
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
    /// Validate a V13 compiled_strategy/backtest trace or Core Live deployment without starting Python.
    Validate {
        #[arg(value_name = "STRATEGY_FILE_OR_KEY")]
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
    /// Describe one source using its default parameters and optional overrides.
    Describe(strategy_workspace::SourceArgs),
    /// Discover flat strategy source files without importing Python.
    Discover {
        directory: Option<PathBuf>,
        #[arg(long)]
        workspace: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Execute the source's inline TESTS against real native callbacks.
    Test(strategy_workspace::CompileArgs),
    /// Build an immutable, verified release, with an optional runtime definition.
    Build {
        #[command(flatten)]
        args: strategy_workspace::CompileArgs,
        #[arg(long)]
        runtime: Option<PathBuf>,
        #[arg(long, requires = "runtime")]
        strategy_key: Option<String>,
    },
    /// Verify an immutable release without Python or compilation.
    VerifyRelease {
        release: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Publish a verified release into an immutable filesystem destination; never starts it.
    Publish {
        release: PathBuf,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// List compiled compiled strategies without importing strategy Python.
    Ls {
        #[arg(long)]
        directory: Option<PathBuf>,
        #[arg(long)]
        workspace: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Show one compiled compiled strategy manifest.
    Show {
        compiled_strategy: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Validate one compiled compiled strategy, including native digest and target checks.
    Validate {
        compiled_strategy: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Compile one source file; defaults to a bundle with inline parameters and tests.
    Compile(strategy_workspace::CompileArgs),
}

#[derive(Subcommand)]
enum InstrumentsCommands {
    /// Verify tick/lot/multiplier/quote units and derived hedge ratios across all definitions.
    Verify {
        #[arg(short = 'c', long = "config", value_name = "RUNTIME.toml")]
        config: PathBuf,
        /// Authoritative metadata snapshot to compare the configuration against.
        #[arg(long, value_name = "SNAPSHOT.json")]
        metadata_snapshot: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Recompute or refresh the digest of an offline metadata snapshot.
    SnapshotDigest {
        #[arg(value_name = "SNAPSHOT.json")]
        snapshot: PathBuf,
        /// Rewrite the snapshot with its recomputed digest.
        #[arg(long)]
        update: bool,
        #[arg(long)]
        json: bool,
    },
}

impl Commands {
    fn json_requested(&self) -> bool {
        match self {
            Self::Recovery { .. } | Self::Storage { .. } => true,
            Self::Instruments { command } => match command {
                InstrumentsCommands::Verify { json, .. }
                | InstrumentsCommands::SnapshotDigest { json, .. } => *json,
            },
            Self::CoreRun { json, .. }
            | Self::Run { json, .. }
            | Self::Validate { json, .. }
            | Self::Ls { json, .. }
            | Self::Show { json, .. }
            | Self::Logs { json, .. }
            | Self::Stop { json, .. }
            | Self::Report { json, .. } => *json,
            Self::Strategy { command } => match command {
                StrategyCommands::Describe(args) => args.json,
                StrategyCommands::Compile(args)
                | StrategyCommands::Test(args)
                | StrategyCommands::Build { args, .. } => args.source.json,
                StrategyCommands::Discover { json, .. }
                | StrategyCommands::VerifyRelease { json, .. }
                | StrategyCommands::Publish { json, .. }
                | StrategyCommands::Ls { json, .. }
                | StrategyCommands::Show { json, .. }
                | StrategyCommands::Validate { json, .. } => *json,
            },
            Self::RunWorker { .. } | Self::Console { .. } | Self::Serve { .. } => false,
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
    #[serde(alias = "strategy_id")]
    strategy_key: String,
    #[serde(alias = "strategy_version")]
    source_version: String,
    #[serde(alias = "artifact")]
    compiled_strategy: PathBuf,
    #[serde(alias = "artifact_digest")]
    compiled_strategy_digest: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendSpec {
    Backtest {
        data: PathBuf,
        command_capacity: usize,
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
    #[error("Strategy compilation failed: {0}")]
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
            Self::Schema(_) | Self::HistoryCapacity | Self::Json { .. } | Self::Engine(_) => 10,
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

fn registry_path() -> PathBuf {
    std::env::var_os("TITAN_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".titan"))
        .join("runs.sqlite3")
}

fn strategy_command(command: StrategyCommands) -> Result<(), CliError> {
    match command {
        StrategyCommands::Describe(args) => {
            let value = strategy_workspace::describe(&args)?;
            print_strategy_value(value, args.json)
        }
        StrategyCommands::Discover {
            directory,
            workspace,
            json,
        } => print_strategy_value(
            strategy_workspace::discover(directory.as_deref(), workspace.as_deref())?,
            json,
        ),
        StrategyCommands::Test(args) => {
            print_strategy_value(strategy_workspace::test(&args)?, args.source.json)
        }
        StrategyCommands::Build {
            args,
            runtime,
            strategy_key,
        } => print_strategy_value(
            strategy_workspace::build(&args, runtime.as_deref(), strategy_key.as_deref())?,
            args.source.json,
        ),
        StrategyCommands::VerifyRelease { release, json } => {
            print_strategy_value(strategy_workspace::verify_release(&release)?, json)
        }
        StrategyCommands::Publish {
            release,
            directory,
            json,
        } => print_strategy_value(strategy_workspace::publish(&release, &directory)?, json),
        StrategyCommands::Ls {
            directory,
            workspace,
            json,
        } => {
            let mut catalog = Vec::new();
            for source in
                strategy_workspace::compiled_paths(directory.as_deref(), workspace.as_deref())?
            {
                catalog.push(v13_manifest_json(&source)?);
            }
            catalog.sort_by_key(|value| {
                value["strategy_type"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            });
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
                println!("STRATEGY\tVERSION\tABI\tARTIFACT");
                for item in catalog {
                    println!(
                        "{}\t{}\t{}\t{}",
                        item["strategy_type"].as_str().unwrap_or("-"),
                        item["source_version"].as_str().unwrap_or("-"),
                        item["abi_version"].as_u64().unwrap_or_default(),
                        item["compiled_strategy"].as_str().unwrap_or("-"),
                    );
                }
            }
            Ok(())
        }
        StrategyCommands::Show {
            compiled_strategy,
            json,
        } => {
            let manifest = v13_manifest_json(&compiled_strategy)?;
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
            compiled_strategy,
            json,
        } => {
            let manifest = v13_manifest_json(&compiled_strategy)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "valid": true,
                        "strategy_type": manifest["strategy_type"],
                        "source_version": manifest["source_version"],
                        "compiled_strategy_digest": manifest["compiled_strategy_digest"]
                    })
                );
            } else {
                println!("valid");
            }
            Ok(())
        }
        StrategyCommands::Compile(args) => {
            print_strategy_value(strategy_workspace::compile(&args)?, args.source.json)
        }
    }
}

fn v13_manifest_json(path: &Path) -> Result<serde_json::Value, CliError> {
    let path = fs::canonicalize(path).map_err(|source| CliError::Read {
        path: path.into(),
        source,
    })?;
    let manifest = inspect_compiled_strategy(&path)?;
    Ok(serde_json::json!({
        "strategy_type": manifest.strategy_type,
        "source_version": manifest.source_version,
        "abi_version": manifest.abi_version,
        "compiled_strategy_digest": hex_digest(&manifest.compiled_strategy_digest),
        "parameters_digest": hex_digest(&manifest.parameters_digest),
        "source_digest": manifest.source_digest.as_ref().map(hex_digest),
        "native_library_sha256": hex_digest(&manifest.native_library_sha256),
        "target_triple": manifest.target_triple,
        "cpu_baseline": manifest.cpu_baseline,
        "callback_mask": manifest.callback_mask,
        "state_len": manifest.state.byte_len,
        "state_alignment": manifest.state.alignment,
        "recovery_state_schema_version": manifest.state.version,
        "recovery_state_schema_hash": hex_digest(&manifest.state.hash),
        "business_observation_schema": manifest.business_observation.as_ref().map(|v| serde_json::json!({"schema":v.schema,"schema_hash":v.schema_hash})),
        "compiled_strategy": path,
    }))
}

fn inspect_compiled_strategy(
    path: &Path,
) -> Result<titan_strategy_runtime::ArtifactMetadata, CliError> {
    use titan_strategy_runtime::NativeStrategyLoader;
    let loader = NativeStrategyLoader::new(std::env::temp_dir().join("titan-cli-v13-inspect"));
    loader
        .inspect(path)
        .map_err(|error| CliError::Engine(error.to_string()))
}

fn hex_digest(value: &[u8; 32]) -> String {
    titan_strategy_runtime::hex_identity(value)
}

struct CompileRequest<'a> {
    strategy: &'a Path,
    parameters: &'a str,
    target: Option<&'a str>,
    cpu_baseline: &'a str,
    strategy_format: &'a str,
    output: Option<&'a Path>,
    workspace: Option<&'a strategy_workspace::Workspace>,
    update_lock: bool,
    require_tests: bool,
}

fn print_native_test_summary(value: &serde_json::Value) {
    if value["mode"] == "not_declared" {
        println!("native tests: not declared");
    } else if let (Some(passed), Some(declared)) =
        (value["passed"].as_u64(), value["declared"].as_u64())
    {
        println!("native tests: {passed}/{declared} passed");
    }
}

fn print_strategy_value(mut value: serde_json::Value, json: bool) -> Result<(), CliError> {
    if json {
        println!("{value}");
    } else if let Some(paths) = value["paths"].as_array() {
        println!(
            "{}\t{}\t{}",
            value["strategy_type"].as_str().unwrap_or("-"),
            value["source_version"].as_str().unwrap_or("-"),
            value["compiled_strategy_digest"].as_str().unwrap_or("-")
        );
        for path in paths.iter().filter_map(serde_json::Value::as_str) {
            println!("{path}");
        }
        print_native_test_summary(&value["tests"]);
    } else if let Some(sources) = value["sources"].as_array() {
        println!("NAME\tSOURCE");
        for source in sources {
            println!(
                "{}\t{}",
                source["name"].as_str().unwrap_or("-"),
                source["source"].as_str().unwrap_or("-")
            );
        }
    } else if value["release"].is_string() || value["published"].is_string() {
        println!("release: {}", value["release_id"].as_str().unwrap_or("-"));
        println!(
            "{}",
            value["release"]
                .as_str()
                .or(value["published"].as_str())
                .unwrap()
        );
        print_native_test_summary(&value["tests"]);
    } else if value["tests"]["passed"].is_number() {
        println!(
            "{} {}",
            value["strategy_type"].as_str().unwrap_or("-"),
            value["source_version"].as_str().unwrap_or("-")
        );
        print_native_test_summary(&value["tests"]);
    } else {
        if let Some(metadata) = value["metadata"].as_object_mut() {
            metadata.remove("initial_state");
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(CliError::ResultJson)?
        );
    }
    Ok(())
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
    // The trace replay backend was removed with its fixtures. Reject here, before resolving the
    // config, inspecting the artifact or spawning a worker.
    let _ = event_mode;
    Err(CliError::Engine(
        "titan run -e backtest was removed; run the historical backtest instead: \
         cargo run -p hftbacktest --example grid_history"
            .into(),
    ))
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
    let adapted = load_core_configuration_bytes(
        &config_path,
        &config_bytes,
        Some((strategy_name, event_mode)),
    )?;
    let definition = adapted
        .strategies
        .first()
        .ok_or_else(|| CliError::Engine(format!("strategy {strategy_name} is not enabled")))?;
    let compiled_strategy = definition
        .strategy_file
        .uri
        .strip_prefix("file://")
        .ok_or_else(|| CliError::Engine("V13 compiled_strategy URI must use file://".into()))?;
    let compiled_strategy =
        fs::canonicalize(compiled_strategy).map_err(|source| CliError::Read {
            path: PathBuf::from(compiled_strategy),
            source,
        })?;
    let manifest = inspect_compiled_strategy(&compiled_strategy)?;
    let spec = RunSpec {
        schema_version: RUN_SPEC_VERSION,
        environment: Environment::Live,
        event_mode,
        config_path,
        config_sha256: format!("{:x}", Sha256::digest(&config_bytes)),
        strategy: StrategyRunSpec {
            strategy_key: definition.strategy_key.to_string(),
            source_version: manifest.source_version.to_string(),
            compiled_strategy,
            compiled_strategy_digest: hex_digest(&manifest.compiled_strategy_digest),
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
    if spec.strategy.strategy_key.is_empty() || spec.strategy.source_version.is_empty() {
        return Err(CliError::Engine(
            "resolved strategy identity must not be empty".into(),
        ));
    }
    if spec.strategy.compiled_strategy_digest.len() != 64
        || !spec
            .strategy
            .compiled_strategy_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(CliError::Engine(
            "resolved V13 compiled_strategy digest is invalid".into(),
        ));
    }
    if spec.history_capacity == 0 {
        return Err(CliError::HistoryCapacity);
    }
    match &spec.backend {
        BackendSpec::Backtest {
            data,
            command_capacity,
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
        }
        BackendSpec::CoreLive { strategy_key } => {
            if spec.environment != Environment::Live {
                return Err(CliError::Engine(
                    "Core live backend requires live environment".into(),
                ));
            }
            if strategy_key.is_empty() || strategy_key != &spec.strategy.strategy_key {
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
    registry.create(crate::registry::NewRun {
        id: &run_id,
        owner_token: &owner_token,
        strategy_id: &spec.strategy.strategy_key,
        strategy_version: &spec.strategy.source_version,
        environment: spec.environment.as_str(),
        event_mode: spec.event_mode.as_str(),
        spec_path: &spec_path,
        config_path: &spec.config_path,
        config_sha256: &spec.config_sha256,
        result_path: &result_path,
        log_path: &log_path,
    })?;
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

/// The deterministic trace replay backend was removed.
///
/// It replayed a frozen, hand-authored event trace through the strategy and compared the emitted
/// order intents. It produced no P&L and no matching, so it could not answer anything the real
/// historical backtest (`hftbacktest/examples/grid_history.rs`) does not, while still needing a
/// fixture per scenario and an ABI-schema version for the trace format. The adapter it used
/// (`OfflineAdapter`) stays: the real historical backtest runs strategies through it.
fn v13_backtest_worker(
    _registry: &Registry,
    _run_id: &str,
    _token: &str,
    _spec: &RunSpec,
    _stop: Arc<AtomicBool>,
) -> Result<(), CliError> {
    Err(CliError::Engine(
        "trace replay was removed; run the historical backtest (hftbacktest grid_history) instead"
            .into(),
    ))
}

fn core_live_worker(
    registry: &Registry,
    run_id: &str,
    token: &str,
    spec: &RunSpec,
    strategy_key: &str,
    stop: Arc<AtomicBool>,
) -> Result<(), CliError> {
    let bytes = fs::read(&spec.config_path).map_err(|source| CliError::Read {
        path: spec.config_path.clone(),
        source,
    })?;
    if format!("{:x}", Sha256::digest(&bytes)) != spec.config_sha256 {
        return Err(CliError::Engine(
            "configuration changed after run resolution".into(),
        ));
    }
    let mut adapted = load_core_configuration_bytes(
        &spec.config_path,
        &bytes,
        Some((strategy_key, spec.event_mode)),
    )?;
    let manifest = inspect_compiled_strategy(&spec.strategy.compiled_strategy)?;
    if hex_digest(&manifest.compiled_strategy_digest) != spec.strategy.compiled_strategy_digest
        || manifest.source_version.as_ref() != spec.strategy.source_version
    {
        return Err(CliError::Engine(
            "strategy compiled_strategy changed after run resolution".into(),
        ));
    }
    let definition = adapted
        .strategies
        .first_mut()
        .ok_or_else(|| CliError::Engine("selected strategy missing".into()))?;
    if definition.strategy_file.expected_digest != manifest.compiled_strategy_digest {
        return Err(CliError::Engine(
            "configured compiled_strategy identity differs from validated plan".into(),
        ));
    }
    definition.strategy_file.uri = Arc::from(format!(
        "file://{}",
        spec.strategy.compiled_strategy.display()
    ));
    let mut runtime = titan_cli::TradingRuntime::start(adapted)
        .map_err(|error| CliError::Engine(error.to_string()))?;
    registry.transition(run_id, token, "READY")?;
    registry.transition(run_id, token, "RUNNING")?;
    let started = Instant::now();
    monitor_and_shutdown(&mut runtime, || stop.load(Ordering::Acquire))?;
    let result = serde_json::json!({
        "schema_version": 1,
        "run_id": run_id,
        "strategy_id": strategy_key,
        "strategy_version": spec.strategy.source_version,
        "environment": spec.environment,
        "event_mode": spec.event_mode,
        "config_sha256": spec.config_sha256,
        "compiled_strategy_digest": spec.strategy.compiled_strategy_digest,
        "returns": [],
        "wall_time_ns": started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        "status": "STOPPED"
    });
    let result_json = serde_json::to_string_pretty(&result).map_err(CliError::ResultJson)?;
    let record = registry
        .get(run_id)?
        .ok_or_else(|| CliError::RunNotFound(run_id.into()))?;
    write_result_bundle(&record.result_path, run_id, spec, result_json.as_bytes())?;
    registry.update_metrics(run_id, token, 0, 0, 0)?;
    registry.finish(run_id, token, "STOPPED", 0, None)?;
    println!("{result_json}");
    Ok(())
}

fn write_result_bundle(
    path: &Path,
    run_id: &str,
    spec: &RunSpec,
    bytes: &[u8],
) -> Result<(), CliError> {
    let manifest_path = path.with_file_name("manifest.json");
    if manifest_path.exists() {
        return Err(CliError::Engine("ResultBundle is already committed".into()));
    }
    atomic_write(path, bytes)?;
    let mut files = vec![BundleFile {
        path: "result.json".into(),
        bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
    }];
    for name in ["history.db", "facts.parquet"] {
        let file_path = path.with_file_name(name);
        if file_path.exists() {
            use std::io::Read;
            let mut file = fs::File::open(&file_path).map_err(|source| CliError::Read {
                path: file_path.clone(),
                source,
            })?;
            let mut hash = Sha256::new();
            let mut buffer = [0; 65536];
            loop {
                let count = file.read(&mut buffer).map_err(|source| CliError::Read {
                    path: file_path.clone(),
                    source,
                })?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            files.push(BundleFile {
                path: name.into(),
                bytes: file
                    .metadata()
                    .map_err(|source| CliError::Read {
                        path: file_path,
                        source,
                    })?
                    .len(),
                sha256: format!("{:x}", hash.finalize()),
            });
        }
    }
    let manifest = BundleManifest {
        schema_version: 1,
        run_id: run_id.into(),
        strategy_id: spec.strategy.strategy_key.clone(),
        strategy_version: spec.strategy.source_version.clone(),
        abi_fingerprint: hex_digest(&titan_strategy_runtime::ABI_FINGERPRINT),
        committed_at_ns: now_ns(),
        files,
    };
    titan_persistence::commit_result_bundle(
        path.parent().unwrap_or(Path::new(".")),
        &registry_path()
            .parent()
            .unwrap_or(Path::new("."))
            .join("storage"),
        &serde_json::to_vec_pretty(&manifest).map_err(CliError::ResultJson)?,
        Some(&manifest.abi_fingerprint),
    )
    .map_err(|e| CliError::Engine(e.to_string()))?;
    Ok(())
}

fn monitor_and_shutdown(
    runtime: &mut titan_cli::TradingRuntime,
    should_stop: impl Fn() -> bool,
) -> Result<(), CliError> {
    let mut failure = None;
    while !should_stop() {
        if let Err(error) = runtime.check_health() {
            failure = Some(error.to_string());
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let shutdown = runtime
        .shutdown()
        .map_err(|error| CliError::Engine(error.to_string()));
    if let Some(error) = failure {
        shutdown?;
        return Err(CliError::Engine(error));
    }
    shutdown
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

/// Refuses a ResultBundle that was produced by a different strategy ABI.
///
/// The manifest has always carried `abi_fingerprint`; until now nothing compared it with the host,
/// so a bundle from an older ABI could be replayed against a newer runtime while its numbers were
/// derived from a different object schema. The comparison is the only reader of that field.
fn verify_bundle_abi_fingerprint(recorded: &str) -> Result<(), CliError> {
    let host = hex_digest(&titan_strategy_runtime::ABI_FINGERPRINT);
    if recorded != host {
        return Err(CliError::Engine(format!(
            "bundle_abi_mismatch: ResultBundle was recorded against another strategy ABI (fingerprint {recorded}); this host runs ABI {} (fingerprint {host}); re-run the strategy, result bundles cannot be migrated",
            titan_strategy_runtime::STRATEGY_ABI
        )));
    }
    Ok(())
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
    let storage = registry_path()
        .parent()
        .ok_or_else(|| CliError::Engine("registry parent missing".into()))?
        .join("storage");
    if !manifest_path.is_file() || !run.result_path.is_file() {
        titan_persistence::rehydrate_result_bundle(
            &storage,
            run_id,
            run.result_path
                .parent()
                .ok_or_else(|| CliError::Engine("bundle parent missing".into()))?,
        )
        .map_err(|e| CliError::Engine(e.to_string()))?;
    }
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
    if manifest.run_id != run_id || !manifest.files.iter().any(|file| file.path == "result.json") {
        return Err(CliError::Engine(
            "ResultBundle identity or result descriptor missing".into(),
        ));
    }
    verify_bundle_abi_fingerprint(&manifest.abi_fingerprint)?;
    let bundle = run
        .result_path
        .parent()
        .ok_or_else(|| CliError::Engine("ResultBundle has no parent directory".into()))?;
    if manifest
        .files
        .iter()
        .any(|f| !bundle.join(&f.path).is_file())
    {
        titan_persistence::rehydrate_result_bundle(&storage, run_id, bundle)
            .map_err(|e| CliError::Engine(e.to_string()))?;
    }
    let canonical_bundle = fs::canonicalize(bundle).map_err(|source| CliError::Read {
        path: bundle.into(),
        source,
    })?;
    let mut paths = std::collections::BTreeSet::new();
    let mut verified_result = None;
    for descriptor in &manifest.files {
        if !paths.insert(&descriptor.path) {
            return Err(CliError::Engine(
                "duplicate ResultBundle file descriptor".into(),
            ));
        }
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
        if descriptor.path == "result.json" {
            verified_result = Some(
                String::from_utf8(content).map_err(|error| CliError::Engine(error.to_string()))?,
            );
        }
    }
    let result =
        verified_result.ok_or_else(|| CliError::Engine("ResultBundle result is missing".into()))?;
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

fn load_metadata_snapshot(path: &Path) -> Result<titan_market_service::MetadataSnapshot, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid metadata snapshot {}: {error}", path.display()))
}

fn instruments_command(command: InstrumentsCommands) -> ExitCode {
    match command {
        InstrumentsCommands::Verify {
            config,
            metadata_snapshot,
            json,
        } => {
            let metadata = match metadata_snapshot
                .as_deref()
                .map(load_metadata_snapshot)
                .transpose()
            {
                Ok(metadata) => metadata,
                Err(error) => {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "schema_version": 1,
                                "valid": false,
                                "errors": [{"code": "configuration_invalid", "subject": config.display().to_string(), "detail": error}]
                            })
                        );
                    } else {
                        eprintln!("invalid metadata snapshot: {error}");
                    }
                    return ExitCode::FAILURE;
                }
            };
            let report = match titan_cli::verify_instrument_units_toml(&config, metadata.as_ref()) {
                Ok(report) => report,
                Err(error) => {
                    if json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "schema_version": 1,
                                "valid": false,
                                "errors": [{"code": "configuration_invalid", "subject": config.display().to_string(), "detail": error.to_string()}]
                            })
                        );
                    } else {
                        eprintln!("invalid configuration: {error}");
                    }
                    return ExitCode::FAILURE;
                }
            };
            let metadata_digest = report.metadata_digest.map(|digest| {
                digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            });
            if json {
                let instruments = report
                    .instruments
                    .iter()
                    .map(|units| {
                        serde_json::json!({
                            "source_key": units.source_key,
                            "native_symbol": units.native_symbol,
                            "asset_id": units.asset_id,
                            "price_tick": units.price_tick.to_string(),
                            "quantity_lot": units.quantity_lot.to_string(),
                            "contract_multiplier": units.contract_multiplier.to_string(),
                            "base_delta_per_lot": units.base_delta_per_lot.to_string(),
                            "min_qty_lots": units.min_qty_lots(),
                            "min_notional_quote": units.min_notional_quote.map(|unit| unit.to_string()),
                            "quote_currency": units.quote_currency,
                        })
                    })
                    .collect::<Vec<_>>();
                let errors = report
                    .errors
                    .iter()
                    .map(|error| {
                        serde_json::json!({
                            "code": error.code(),
                            "subject": error.subject(),
                            "detail": error.detail(),
                        })
                    })
                    .collect::<Vec<_>>();
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "valid": report.is_ok(),
                        "metadata_verified": metadata.is_some(),
                        "metadata_digest": metadata_digest,
                        "instruments": instruments,
                        "errors": errors,
                    })
                );
            } else {
                for units in report.instruments.iter() {
                    println!(
                        "{}:{} asset {} tick {} lot {} multiplier {} base_delta {} min_lots {:?} quote {:?}",
                        units.source_key,
                        units.native_symbol,
                        units.asset_id,
                        units.price_tick,
                        units.quantity_lot,
                        units.contract_multiplier,
                        units.base_delta_per_lot,
                        units.min_qty_lots(),
                        units.quote_currency,
                    );
                }
                for error in report.errors.iter() {
                    eprintln!("{} [{}] {}", error.code(), error.subject(), error.detail());
                }
                if report.instruments.is_empty() && report.is_ok() {
                    println!(
                        "no enabled instrument bindings were validated; enable the market sources/accounts to check units"
                    );
                }
                match metadata_digest.as_deref() {
                    Some(digest) => println!("metadata snapshot digest {digest}"),
                    None => println!(
                        "metadata not verified: pass --metadata-snapshot to prove units against venue metadata"
                    ),
                }
                println!(
                    "instrument unit validation {}: {} instrument(s), {} error(s)",
                    if report.is_ok() { "passed" } else { "failed" },
                    report.instruments.len(),
                    report.errors.len()
                );
            }
            if report.is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        InstrumentsCommands::SnapshotDigest {
            snapshot,
            update,
            json,
        } => {
            let mut value = match load_metadata_snapshot(&snapshot) {
                Ok(value) => value,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::FAILURE;
                }
            };
            let stored = value.digest;
            let computed = value.compute_digest();
            value.digest = computed;
            if update {
                match serde_json::to_vec_pretty(&value)
                    .map_err(|error| error.to_string())
                    .and_then(|bytes| {
                        fs::write(&snapshot, bytes).map_err(|error| error.to_string())
                    }) {
                    Ok(()) => {}
                    Err(error) => {
                        eprintln!("cannot rewrite {}: {error}", snapshot.display());
                        return ExitCode::FAILURE;
                    }
                }
            }
            let hex = |bytes: &[u8; 32]| {
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            let matches = stored == computed;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "schema_version": 1,
                        "snapshot": snapshot.display().to_string(),
                        "stored_digest": hex(&stored),
                        "computed_digest": hex(&computed),
                        "matches": matches,
                        "updated": update,
                    })
                );
            } else {
                println!("stored   {}", hex(&stored));
                println!("computed {}", hex(&computed));
                if !matches && !update {
                    eprintln!("digest mismatch; re-run with --update to refresh the snapshot");
                }
            }
            if matches || update {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
    }
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
    monitor_and_shutdown(&mut runtime, || {
        interrupt.load(Ordering::Acquire) || terminate.load(Ordering::Acquire)
    })?;
    let reason = if interrupt.load(Ordering::Acquire) {
        "SIGINT"
    } else {
        "SIGTERM"
    };
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
    let bytes = fs::read(config).map_err(|source| CliError::Read {
        path: config.into(),
        source,
    })?;
    load_core_configuration_bytes(config, &bytes, selected)
}

fn load_core_configuration_bytes(
    config: &Path,
    bytes: &[u8],
    selected: Option<(&str, EventMode)>,
) -> Result<titan_cli::AdaptedConfiguration, CliError> {
    let text = std::str::from_utf8(bytes).map_err(|error| CliError::Engine(error.to_string()))?;
    let mut adapted = titan_cli::ConfigurationAdapter::load_contents(text, config)
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

#[derive(Clone)]
struct DiagnosticWriter {
    used: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    limit: usize,
}
impl std::io::Write for DiagnosticWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        use std::sync::atomic::Ordering;
        let before = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_add(bytes.len()).min(self.limit))
            })
            .unwrap_or(0);
        let count = self.limit.saturating_sub(before).min(bytes.len());
        if count > 0 {
            std::io::Write::write_all(&mut std::io::stderr(), &bytes[..count])?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut std::io::stderr())
    }
}
fn main() -> ExitCode {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    let limit = if arguments
        .iter()
        .any(|a| a == "observe-worker" || a == "managed-worker")
    {
        4 * 1024 * 1024
    } else {
        usize::MAX
    };
    let diagnostics = DiagnosticWriter {
        used: Default::default(),
        limit,
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(move || diagnostics.clone())
        .with_ansi(false)
        .try_init();
    let json_argument = arguments
        .iter()
        .any(|argument| argument == "--json" || argument == "--ndjson");
    let console_argument = arguments
        .get(1)
        .is_some_and(|argument| argument == "console");
    let cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            if json_argument && exit_code != 0 && console_argument {
                titan_cli::management::render_client_error(
                    &anyhow::anyhow!(titan_cli::management::ClientUsageError::new(
                        "INVALID_INPUT",
                        "invalid command syntax; use --help to inspect supported arguments"
                    )),
                    true,
                );
            } else if json_argument && exit_code != 0 {
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
        Commands::Serve { role, config } => {
            let command = match role {
                ServeRole::Manager => titan_cli::management::ConsoleCommand::Manager { config },
                ServeRole::Node => titan_cli::management::ConsoleCommand::Node {
                    config: Some(config),
                    command: None,
                },
            };
            return match titan_cli::management::run(command, Default::default()) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    titan_cli::management::render_client_error(&error, false);
                    ExitCode::from(
                        titan_cli::management::client_exit_code(&error).clamp(0, 255) as u8,
                    )
                }
            };
        }
        Commands::Storage { command } => storage_cli::run(command)
            .map_err(|error| CliError::Engine(format!("storage maintenance: {error:#}"))),
        Commands::Recovery { database, command } => recovery_cli::run(database, command)
            .map_err(|error| CliError::Engine(format!("recovery maintenance: {error:#}"))),
        Commands::Console { command, options } => {
            let json = options.json || options.ndjson;
            return match titan_cli::management::run(command, options) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    titan_cli::management::render_client_error(&error, json);
                    ExitCode::from(
                        titan_cli::management::client_exit_code(&error).clamp(0, 255) as u8,
                    )
                }
            };
        }
        Commands::Instruments { command } => return instruments_command(command),
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
mod cli_tests {
    use super::*;

    #[test]
    fn parses_documented_compile_command() {
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
            "--strategy-format",
            "bundle",
            "--output",
            "pair.titan",
        ])
        .expect("documented V13 compile command must parse");
        let Commands::Strategy {
            command:
                StrategyCommands::Compile(strategy_workspace::CompileArgs {
                    source,
                    strategy_format,
                    output,
                    ..
                }),
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(source.strategy, Some(PathBuf::from("strategy.py")));
        assert_eq!(strategy_format.as_deref(), Some("bundle"));
        assert_eq!(output, Some(PathBuf::from("pair.titan")));
    }
}
