//! Source discovery, workspace resolution and immutable strategy releases. Never starts a strategy.
use super::{CliError, CompileV13Request, compiler, hex_digest, v13_manifest_json};
use clap::Args;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};
use titan_strategy_runtime::{V13TrustPolicy, compiler_host_target_v13, compiler_json_v13};

fn error(message: impl std::fmt::Display) -> CliError {
    CliError::StaticCompile(message.to_string())
}

#[derive(Args, Debug)]
pub(super) struct SourceArgs {
    #[arg(value_name = "SOURCE.py", conflicts_with = "strategy")]
    pub source: Option<PathBuf>,
    #[arg(long, required_unless_present = "source")]
    pub strategy: Option<PathBuf>,
    #[arg(long, value_name = "PARAMETERS.json")]
    pub parameters: Option<PathBuf>,
    /// Override a typed parameter. Values are JSON scalars or unquoted enum text.
    #[arg(long = "set", value_name = "NAME=VALUE")]
    pub overrides: Vec<String>,
    #[arg(long, value_name = "titan.toml")]
    pub workspace: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub(super) struct CompileArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    #[arg(long)]
    pub target: Option<String>,
    #[arg(long)]
    pub cpu_baseline: Option<String>,
    #[arg(long, alias = "artifact-format", value_parser = ["pair", "bundle"])]
    pub strategy_format: Option<String>,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub signing_key: Option<PathBuf>,
    #[arg(long, requires = "signing_key")]
    pub key_id: Option<String>,
    /// Intentionally accept and record a changed workspace toolchain.
    #[arg(long)]
    pub update_lock: bool,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    schema_version: u32,
    strategy: StrategyConfig,
    toolchain: ToolchainConfig,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StrategyConfig {
    source_dir: PathBuf,
    build_dir: PathBuf,
    cpu_baseline: String,
}
impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            source_dir: "strategies".into(),
            build_dir: "build/strategies".into(),
            cpu_baseline: "x86-64".into(),
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ToolchainConfig {
    sdk: Option<PathBuf>,
    python: Option<PathBuf>,
    python_version: Option<String>,
    rust_version: Option<String>,
}

pub(super) struct Workspace {
    pub root: PathBuf,
    config: Config,
    configured: bool,
}
impl Workspace {
    fn load(source: Option<&Path>, explicit: Option<&Path>) -> Result<Self, CliError> {
        let cwd = std::env::current_dir().map_err(error)?;
        let path = if let Some(path) = explicit {
            Some(
                fs::canonicalize(if path.is_dir() {
                    path.join("titan.toml")
                } else {
                    path.to_owned()
                })
                .map_err(error)?,
            )
        } else {
            let source_dir = source.and_then(Path::parent).unwrap_or(&cwd);
            source_dir
                .ancestors()
                .chain(cwd.ancestors())
                .map(|p| p.join("titan.toml"))
                .find(|p| p.is_file())
        };
        if let Some(path) = path {
            let config: Config =
                toml::from_str(&fs::read_to_string(&path).map_err(error)?).map_err(error)?;
            if config.schema_version != 1 {
                return Err(error("unsupported titan.toml schema_version"));
            }
            Ok(Self {
                root: path.parent().unwrap().to_owned(),
                config,
                configured: true,
            })
        } else {
            Ok(Self {
                root: cwd,
                config: Config::default(),
                configured: false,
            })
        }
    }
    pub fn sdk(&self) -> Result<PathBuf, CliError> {
        let path = std::env::var_os("TITAN_STRATEGY_SDK")
            .map(PathBuf::from)
            .or_else(|| {
                self.config
                    .toolchain
                    .sdk
                    .as_ref()
                    .map(|p| self.root.join(p))
            })
            .unwrap_or_else(|| self.root.join("python/titan-strategy-sdk"));
        fs::canonicalize(path).map_err(|e| {
            error(format!(
                "cannot resolve SDK; set TITAN_STRATEGY_SDK or toolchain.sdk: {e}"
            ))
        })
    }
    pub fn python(&self) -> PathBuf {
        std::env::var_os("TITAN_STRATEGY_PYTHON")
            .map(PathBuf::from)
            .or_else(|| {
                self.config.toolchain.python.as_ref().map(|p| {
                    if p.components().count() == 1 {
                        p.clone()
                    } else {
                        self.root.join(p)
                    }
                })
            })
            .unwrap_or_else(|| PathBuf::from("python3"))
    }
    pub fn check_lock(
        &self,
        metadata: &Value,
        sdk: &Path,
        update: bool,
    ) -> Result<Value, CliError> {
        let build = &metadata["build"];
        if let Some(expected) = &self.config.toolchain.python_version
            && build["python"].as_str() != Some(expected)
        {
            return Err(error(
                "Python version differs from workspace toolchain.python_version",
            ));
        }
        if let Some(expected) = &self.config.toolchain.rust_version
            && !env!("TITAN_RUSTC_VERSION").starts_with(&format!("rustc {expected} "))
        {
            return Err(error("Titan binary was built by a different Rust version"));
        }
        let value = json!({"schema_version":1,"abi_fingerprint":metadata["abi_fingerprint"],
            "bridge":build,"rustc":env!("TITAN_RUSTC_VERSION"),
            "titan_binary_sha256":digest(&fs::read(std::env::current_exe().map_err(error)?).map_err(error)?),
            "sdk_digest":sdk_digest(sdk)?});
        let path = self.root.join("titan.lock");
        if self.configured && path.exists() && !update {
            let old: Value = serde_json::from_slice(&fs::read(path).map_err(error)?)
                .map_err(|e| error(format!("invalid workspace titan.lock: {e}")))?;
            if old != value {
                return Err(error(
                    "workspace toolchain changed; review it and pass --update-lock",
                ));
            }
        }
        Ok(value)
    }
    /// Reject identities available without importing Python before starting any SDK code.
    pub fn preflight_lock(&self, sdk_digest: &str, update: bool) -> Result<(), CliError> {
        if let Some(expected) = &self.config.toolchain.rust_version
            && !env!("TITAN_RUSTC_VERSION").starts_with(&format!("rustc {expected} "))
        {
            return Err(error("Titan binary was built by a different Rust version"));
        }
        let path = self.root.join("titan.lock");
        if !self.configured || !path.exists() || update {
            return Ok(());
        }
        let old: Value = serde_json::from_slice(&fs::read(path).map_err(error)?)
            .map_err(|e| error(format!("invalid workspace titan.lock: {e}")))?;
        let identity = json!({"schema_version":1,
            "abi_fingerprint":titan_strategy_runtime::V13_ABI_FINGERPRINT,
            "rustc":env!("TITAN_RUSTC_VERSION"),
            "titan_binary_sha256":digest(&fs::read(std::env::current_exe().map_err(error)?).map_err(error)?),
            "sdk_digest":sdk_digest});
        if identity
            .as_object()
            .unwrap()
            .iter()
            .any(|(key, value)| old.get(key) != Some(value))
        {
            return Err(error(
                "workspace toolchain changed before SDK execution; review it and pass --update-lock",
            ));
        }
        Ok(())
    }
    pub fn save_lock(&self, value: &Value, update: bool) -> Result<(), CliError> {
        use std::io::Write;
        if !self.configured {
            return Ok(());
        }
        let path = self.root.join("titan.lock");
        if update {
            return atomic_json(&path, value);
        }
        let mut candidate = tempfile::NamedTempFile::new_in(&self.root).map_err(error)?;
        candidate
            .write_all(&serde_json::to_vec_pretty(value).map_err(error)?)
            .map_err(error)?;
        candidate.as_file().sync_all().map_err(error)?;
        match candidate.persist_noclobber(&path) {
            Ok(_) => Ok(()),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                let current: Value =
                    serde_json::from_slice(&fs::read(path).map_err(error)?).map_err(error)?;
                if &current == value {
                    Ok(())
                } else {
                    Err(error(
                        "workspace toolchain changed before publication; review and pass --update-lock",
                    ))
                }
            }
            Err(e) => Err(error(e)),
        }
    }
}

struct Prepared {
    source: PathBuf,
    parameters: String,
    workspace: Workspace,
}
fn prepare(args: &SourceArgs) -> Result<Prepared, CliError> {
    let source = args
        .source
        .as_ref()
        .or(args.strategy.as_ref())
        .ok_or_else(|| error("a strategy source is required"))?;
    let source = fs::canonicalize(source).map_err(error)?;
    if source.extension().and_then(|s| s.to_str()) != Some("py") {
        return Err(error("strategy source must be a .py file"));
    }
    let parameters = merge_parameters(args.parameters.as_deref(), &args.overrides)?;
    Ok(Prepared {
        workspace: Workspace::load(Some(&source), args.workspace.as_deref())?,
        source,
        parameters: parameters.to_string(),
    })
}

fn merge_parameters(path: Option<&Path>, overrides: &[String]) -> Result<Value, CliError> {
    let mut value: Value = match path {
        Some(p) => serde_json::from_slice(&fs::read(p).map_err(error)?).map_err(error)?,
        None => json!({}),
    };
    let object = value
        .as_object_mut()
        .ok_or_else(|| error("parameters must be a JSON object"))?;
    let mut seen = std::collections::BTreeSet::new();
    for item in overrides {
        let (name, text) = item
            .split_once('=')
            .ok_or_else(|| error("--set requires NAME=VALUE"))?;
        if name.is_empty()
            || name.starts_with('_')
            || !name
                .chars()
                .enumerate()
                .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
            || !seen.insert(name)
        {
            return Err(error("--set parameter names must be valid and unique"));
        }
        if text.is_empty() {
            return Err(error(
                "--set value must not be empty; use JSON quotes for an empty string",
            ));
        }
        let parsed = match serde_json::from_str::<Value>(text) {
            Ok(v) => v,
            Err(_) if text.starts_with(['{', '[', '"']) => {
                return Err(error("invalid JSON in --set"));
            }
            Err(_) => Value::String(text.to_owned()),
        };
        if parsed.is_null() || parsed.is_array() || parsed.is_object() {
            return Err(error("--set requires a scalar parameter value"));
        }
        object.insert(name.to_owned(), parsed);
    }
    Ok(value)
}

fn request<'a>(
    prepared: &'a Prepared,
    args: &'a CompileArgs,
    output: &'a Path,
    require_tests: bool,
) -> CompileV13Request<'a> {
    CompileV13Request {
        strategy: &prepared.source,
        parameters: &prepared.parameters,
        target: args.target.as_deref(),
        cpu_baseline: args
            .cpu_baseline
            .as_deref()
            .unwrap_or(&prepared.workspace.config.strategy.cpu_baseline),
        strategy_format: args.strategy_format.as_deref().unwrap_or("bundle"),
        output: Some(output),
        signing_key: args.signing_key.as_deref(),
        key_id: args.key_id.as_deref(),
        workspace: Some(&prepared.workspace),
        update_lock: args.update_lock,
        require_tests,
    }
}
pub(super) fn compile(args: &CompileArgs) -> Result<Value, CliError> {
    let prepared = prepare(&args.source)?;
    let name = source_name(&prepared.source)?;
    let output = args.output.clone().unwrap_or_else(|| {
        prepared
            .workspace
            .root
            .join(&prepared.workspace.config.strategy.build_dir)
            .join(format!("{name}.titan"))
    });
    compiler::compile(&request(&prepared, args, &output, false))
}
pub(super) fn describe(args: &SourceArgs) -> Result<Value, CliError> {
    let prepared = prepare(args)?;
    compiler::describe(
        &prepared.source,
        &prepared.parameters,
        Some(&prepared.workspace),
    )
}
pub(super) fn test(args: &CompileArgs) -> Result<Value, CliError> {
    if args.output.is_some() {
        return Err(error(
            "strategy test does not publish an output; use compile or build",
        ));
    }
    let prepared = prepare(&args.source)?;
    let temp = tempfile::tempdir().map_err(error)?;
    let output = temp.path().join("test.titan");
    let result = compiler::compile(&request(&prepared, args, &output, true))?;
    Ok(
        json!({"strategy_type":result["strategy_type"],"source_version":result["source_version"],"tests":result["tests"],"valid":true}),
    )
}

fn source_name(path: &Path) -> Result<String, CliError> {
    let name = if path.file_stem().and_then(|s| s.to_str()) == Some("strategy") {
        path.parent().and_then(Path::file_name)
    } else {
        path.file_stem()
    };
    name.and_then(|s| s.to_str())
        .map(str::to_owned)
        .ok_or_else(|| error("invalid source filename"))
}
pub(super) fn discover(directory: Option<&Path>, config: Option<&Path>) -> Result<Value, CliError> {
    let workspace = Workspace::load(directory, config)?;
    let directory = directory
        .map(Path::to_owned)
        .unwrap_or_else(|| workspace.root.join(&workspace.config.strategy.source_dir));
    let mut sources = Vec::new();
    for entry in fs::read_dir(&directory).map_err(error)? {
        let path = entry.map_err(error)?.path();
        if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("py")
            && !path.file_name().unwrap().to_string_lossy().starts_with('_')
        {
            sources.push(
                json!({"name":source_name(&path)?,"source":fs::canonicalize(path).map_err(error)?}),
            );
        }
    }
    sources.sort_by_key(|v| v["name"].as_str().unwrap().to_owned());
    Ok(json!({"schema_version":1,"sources":sources}))
}

pub(super) fn compiled_paths(
    directory: Option<&Path>,
    config: Option<&Path>,
) -> Result<Vec<PathBuf>, CliError> {
    fn visit(dir: &Path, output: &mut Vec<PathBuf>, depth: usize) -> Result<(), CliError> {
        if !dir.exists() {
            return Ok(());
        }
        if depth > 8 || output.len() > 4096 {
            return Err(error("compiled strategy catalog capacity exceeded"));
        }
        for entry in fs::read_dir(dir).map_err(error)? {
            let entry = entry.map_err(error)?;
            let ty = entry.file_type().map_err(error)?;
            if entry.file_name().to_string_lossy().starts_with('.') || ty.is_symlink() {
                continue;
            }
            if ty.is_dir() {
                visit(&entry.path(), output, depth + 1)?;
            } else if ty.is_file() && entry.path().extension().is_some_and(|s| s == "titan") {
                output.push(entry.path());
            }
        }
        Ok(())
    }
    let workspace = Workspace::load(None, config)?;
    let mut output = Vec::new();
    if let Some(directory) = directory {
        visit(directory, &mut output, 0)?;
    } else {
        visit(
            &workspace.root.join(&workspace.config.strategy.build_dir),
            &mut output,
            0,
        )?;
        visit(&workspace.root.join("deploy"), &mut output, 0)?;
    }
    output.sort();
    output.dedup();
    Ok(output)
}

fn digest(bytes: &[u8]) -> String {
    hex_digest(&Sha256::digest(bytes).into())
}
pub(super) fn sdk_digest(sdk: &Path) -> Result<String, CliError> {
    let mut files = BTreeMap::new();
    collect_files(
        &sdk.join("titan_strategy"),
        &sdk.join("titan_strategy"),
        &mut files,
        true,
    )?;
    Ok(digest(&compiler_json_v13(
        &serde_json::to_value(files).map_err(error)?,
    )))
}
fn atomic_json(path: &Path, value: &Value) -> Result<(), CliError> {
    use std::io::Write;
    let mut temp =
        tempfile::NamedTempFile::new_in(path.parent().unwrap_or(Path::new("."))).map_err(error)?;
    temp.write_all(&serde_json::to_vec_pretty(value).map_err(error)?)
        .map_err(error)?;
    temp.as_file().sync_all().map_err(error)?;
    temp.persist(path).map_err(error)?;
    Ok(())
}
fn collect_files(
    root: &Path,
    dir: &Path,
    output: &mut BTreeMap<String, String>,
    python_only: bool,
) -> Result<(), CliError> {
    for entry in fs::read_dir(dir).map_err(error)? {
        let entry = entry.map_err(error)?;
        let path = entry.path();
        let ty = entry.file_type().map_err(error)?;
        if ty.is_symlink() {
            return Err(error(
                "symlinks are not accepted in release or SDK source files",
            ));
        }
        if ty.is_dir() {
            if entry.file_name() != "__pycache__" {
                collect_files(root, &path, output, python_only)?;
            }
        } else if ty.is_file()
            && (!python_only || path.extension().and_then(|s| s.to_str()) == Some("py"))
        {
            let relative = path
                .strip_prefix(root)
                .map_err(error)?
                .to_str()
                .ok_or_else(|| error("non UTF-8 path"))?
                .replace('\\', "/");
            output.insert(relative, digest(&fs::read(path).map_err(error)?));
        }
    }
    Ok(())
}

pub(super) fn build(
    args: &CompileArgs,
    runtime: Option<&Path>,
    strategy_key: Option<&str>,
) -> Result<Value, CliError> {
    if args
        .strategy_format
        .as_deref()
        .is_some_and(|s| s != "bundle")
    {
        return Err(error("release builds require bundle format"));
    }
    let mut prepared = prepare(&args.source)?;
    let destination = args.output.clone().unwrap_or_else(|| {
        prepared
            .workspace
            .root
            .join(&prepared.workspace.config.strategy.build_dir)
            .join("releases")
    });
    fs::create_dir_all(&destination).map_err(error)?;
    let stage = tempfile::Builder::new()
        .prefix(".titan-release-")
        .tempdir_in(&destination)
        .map_err(error)?;
    let source_snapshot = stage.path().join("source.py");
    fs::write(&source_snapshot, fs::read(&prepared.source).map_err(error)?).map_err(error)?;
    prepared.source = source_snapshot;
    let artifact = stage.path().join("artifacts/strategy.titan");
    let result = compiler::compile(&request(&prepared, args, &artifact, false))?;
    atomic_json(
        &stage.path().join("parameters.json"),
        &result["normalized_parameters"],
    )?;
    atomic_json(
        &stage.path().join("verification.json"),
        &json!({"native_tests":result["tests"],"compiled_strategy_valid":true}),
    )?;
    let binary = std::env::current_exe().map_err(error)?;
    fs::copy(&binary, stage.path().join("titan")).map_err(error)?;
    let runtime_key = if let Some(runtime) = runtime {
        Some(prepare_runtime(
            runtime,
            stage.path(),
            &result,
            strategy_key,
        )?)
    } else {
        None
    };
    let mut files = BTreeMap::new();
    collect_files(stage.path(), stage.path(), &mut files, false)?;
    let mut release = json!({"schema_version":1,"strategy_type":result["strategy_type"],"source_version":result["source_version"],
        "compiled_strategy_digest":result["compiled_strategy_digest"],"source_digest":result["source_digest"],
        "parameters_digest":result["parameters_digest"],"toolchain":result["toolchain"],
        "target":args.target.as_deref().unwrap_or(compiler_host_target_v13()),
        "cpu_baseline":args.cpu_baseline.as_deref().unwrap_or(&prepared.workspace.config.strategy.cpu_baseline),
        "runtime_strategy_key":runtime_key,"files":files});
    let id = digest(&compiler_json_v13(&release));
    release["release_id"] = json!(id);
    atomic_json(&stage.path().join("release.json"), &release)?;
    let name = format!("{}-{}", result["strategy_type"].as_str().unwrap(), &id);
    let output = destination.join(name);
    // A repeat build cannot replace an existing release, even if it has the same name.
    if output.exists() {
        let mut existing = BTreeMap::new();
        let mut candidate = BTreeMap::new();
        collect_files(&output, &output, &mut existing, false)?;
        collect_files(stage.path(), stage.path(), &mut candidate, false)?;
        if existing != candidate {
            return Err(error("existing release differs"));
        }
    } else {
        fs::rename(stage.path(), &output).map_err(error)?;
    }
    Ok(
        json!({"release_id":id,"release":fs::canonicalize(output).map_err(error)?,"strategy_type":result["strategy_type"],"tests":result["tests"],"valid":true}),
    )
}

fn prepare_runtime(
    path: &Path,
    stage: &Path,
    result: &Value,
    key: Option<&str>,
) -> Result<String, CliError> {
    let mut runtime: toml::Value =
        toml::from_str(&fs::read_to_string(path).map_err(error)?).map_err(error)?;
    let strategies = runtime
        .get_mut("strategies")
        .and_then(toml::Value::as_array_mut)
        .ok_or_else(|| error("runtime requires [[strategies]]"))?;
    if strategies.len() != 1 {
        return Err(error(
            "release runtime must contain exactly one strategy instance",
        ));
    }
    let strategy = &mut strategies[0];
    let actual = strategy
        .get("strategy_key")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| error("runtime strategy_key missing"))?
        .to_owned();
    if key.is_some_and(|key| key != actual) {
        return Err(error("selected runtime strategy_key differs"));
    }
    let hex = result["compiled_strategy_digest"].as_str().unwrap();
    let bytes: Vec<toml::Value> = (0..64)
        .step_by(2)
        .map(|i| i64::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .map(toml::Value::Integer)
        .collect();
    let strategy = strategy
        .as_table_mut()
        .ok_or_else(|| error("invalid strategy table"))?;
    strategy.remove("strategy_file");
    strategy.insert("package".into(), toml::Value::try_from(json!({"loader_type":"native-v13","uri":"file://artifacts/strategy.titan","expected_digest":bytes})).map_err(error)?);
    let service = runtime
        .get_mut("strategy_service")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| error("runtime strategy_service missing"))?;
    service.remove("allowed_strategy_roots");
    service.insert(
        "allowed_artifact_roots".into(),
        toml::Value::Array(vec![toml::Value::String("artifacts".into())]),
    );
    // Relative deployment paths must be provisioned on the target node, never copied from the build server.
    fs::write(
        stage.join("runtime.toml"),
        toml::to_string_pretty(&runtime).map_err(error)?,
    )
    .map_err(error)?;
    validate_release_runtime(&stage.join("runtime.toml"))?;
    Ok(actual)
}

/// Validate configuration and native trust without provisioning target-node directories.
/// The adapted configuration is discarded: the emitted file retains every deployment path.
fn validate_release_runtime(path: &Path) -> Result<(), CliError> {
    let mut config: titan_cli::ApplicationConfig =
        toml::from_str(&fs::read_to_string(path).map_err(error)?).map_err(error)?;
    if config.strategies.len() != 1 || !config.strategies[0].enabled {
        return Err(error("release runtime requires one enabled strategy"));
    }
    let scratch = tempfile::tempdir().map_err(error)?;
    let relocate = |original: &Path, name: &str| -> Result<PathBuf, CliError> {
        if original.as_os_str().is_empty() {
            return Err(error("deployment directory must not be empty"));
        }
        Ok(scratch.path().join(name))
    };
    if let Some(root) = &mut config.account_service.secret_root {
        *root = relocate(root, "secrets")?;
        fs::create_dir(root).map_err(error)?;
    }
    if let Some(root) = &mut config.strategy_service.checkpoint_root {
        *root = relocate(root, "checkpoints")?;
    }
    if let Some(storage) = &mut config.strategy_service.recovery_storage {
        storage.validate().map_err(error)?;
        // Keep the database filename so the ordinary storage-path validation still applies.
        let name = storage
            .path
            .file_name()
            .ok_or_else(|| error("recovery database filename missing"))?;
        storage.path = scratch.path().join("recovery").join(name);
    }
    let adapted =
        titan_cli::ConfigurationAdapter::adapt(config, path.parent().unwrap_or(Path::new(".")))
            .map_err(error)?;
    let definition = &adapted.strategies[0];
    let artifact = definition
        .strategy_file
        .uri
        .strip_prefix("file://")
        .ok_or_else(|| error("release runtime requires a file:// native artifact"))?;
    let manifest = super::inspect_compiled_strategy_with_policy(
        Path::new(artifact),
        adapted.v13_trust_policy(),
    )?;
    if manifest.compiled_strategy_digest != definition.strategy_file.expected_digest {
        return Err(error("release runtime artifact digest differs"));
    }
    Ok(())
}

pub(super) fn verify_release(path: &Path, trust: V13TrustPolicy) -> Result<Value, CliError> {
    let release_path = path.join("release.json");
    let mut release: Value =
        serde_json::from_slice(&fs::read(&release_path).map_err(error)?).map_err(error)?;
    let id = release
        .as_object_mut()
        .and_then(|o| o.remove("release_id"))
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or_else(|| error("release_id missing"))?;
    if release["schema_version"] != 1 || digest(&compiler_json_v13(&release)) != id {
        return Err(error("release manifest identity mismatch"));
    }
    let expected: BTreeMap<String, String> =
        serde_json::from_value(release["files"].clone()).map_err(error)?;
    for name in [
        "source.py",
        "parameters.json",
        "artifacts/strategy.titan",
        "verification.json",
        "titan",
    ] {
        if !expected.contains_key(name) {
            return Err(error("release is missing a required publication file"));
        }
    }
    if release["toolchain"]["titan_binary_sha256"].as_str()
        != expected.get("titan").map(String::as_str)
    {
        return Err(error(
            "release Titan binary differs from its build toolchain",
        ));
    }
    for name in expected.keys() {
        if Path::new(name)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(error("unsafe release path"));
        }
    }
    let mut actual = BTreeMap::new();
    collect_files(path, path, &mut actual, false)?;
    actual.remove("release.json");
    if expected != actual {
        return Err(error("release files or checksums differ"));
    }
    let verification: Value =
        serde_json::from_slice(&fs::read(path.join("verification.json")).map_err(error)?)
            .map_err(error)?;
    let tests = &verification["native_tests"];
    if verification["compiled_strategy_valid"] != true
        || tests["declared"].as_u64().is_none()
        || tests["declared"] != tests["passed"]
        || tests["cases"].as_array().is_none_or(|cases| {
            cases.len() as u64 != tests["passed"].as_u64().unwrap_or(u64::MAX)
                || cases.iter().any(|case| case["passed"] != true)
        })
    {
        return Err(error("release native verification evidence is incomplete"));
    }
    let manifest = v13_manifest_json(&path.join("artifacts/strategy.titan"), trust)?;
    if manifest["compiled_strategy_digest"] != release["compiled_strategy_digest"]
        || manifest["strategy_type"] != release["strategy_type"]
        || manifest["source_version"] != release["source_version"]
    {
        return Err(error("release artifact identity differs"));
    }
    let parameters: Value =
        serde_json::from_slice(&fs::read(path.join("parameters.json")).map_err(error)?)
            .map_err(error)?;
    if digest(&fs::read(path.join("source.py")).map_err(error)?) != release["source_digest"]
        || manifest["source_digest"] != release["source_digest"]
        || digest(&compiler_json_v13(&parameters)) != release["parameters_digest"]
        || manifest["parameters_digest"] != release["parameters_digest"]
        || manifest["target_triple"] != release["target"]
        || manifest["cpu_baseline"] != release["cpu_baseline"]
    {
        return Err(error(
            "release source, parameters or target identity differs",
        ));
    }
    if let Some(key) = release["runtime_strategy_key"].as_str() {
        let runtime: toml::Value =
            toml::from_str(&fs::read_to_string(path.join("runtime.toml")).map_err(error)?)
                .map_err(error)?;
        let strategies = runtime
            .get("strategies")
            .and_then(toml::Value::as_array)
            .ok_or_else(|| error("release runtime strategies missing"))?;
        if strategies.len() != 1
            || strategies[0]
                .get("strategy_key")
                .and_then(toml::Value::as_str)
                != Some(key)
            || strategies[0]
                .get("package")
                .and_then(|p| p.get("uri"))
                .and_then(toml::Value::as_str)
                != Some("file://artifacts/strategy.titan")
        {
            return Err(error("release runtime binding differs"));
        }
        let bytes = strategies[0]
            .get("package")
            .and_then(|p| p.get("expected_digest"))
            .and_then(toml::Value::as_array)
            .ok_or_else(|| error("release runtime digest missing"))?;
        let bytes: Vec<u8> = bytes
            .iter()
            .map(|v| {
                v.as_integer()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| error("invalid release runtime digest"))
            })
            .collect::<Result<_, _>>()?;
        if bytes.len() != 32
            || bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
                != release["compiled_strategy_digest"]
        {
            return Err(error("release runtime digest differs"));
        }
        validate_release_runtime(&path.join("runtime.toml"))?;
    }
    Ok(
        json!({"valid":true,"release_id":id,"strategy_type":release["strategy_type"],"files":expected.len()}),
    )
}
pub(super) fn publish(
    source: &Path,
    directory: &Path,
    trust: V13TrustPolicy,
) -> Result<Value, CliError> {
    let verified = verify_release(source, trust.clone())?;
    fs::create_dir_all(directory).map_err(error)?;
    let output = directory.join(verified["release_id"].as_str().unwrap());
    if output.exists() {
        let existing = verify_release(&output, trust)?;
        if existing["release_id"] != verified["release_id"] {
            return Err(error("publish destination contains another release"));
        }
    } else {
        let stage = tempfile::Builder::new()
            .prefix(".titan-publish-")
            .tempdir_in(directory)
            .map_err(error)?;
        let mut files = BTreeMap::new();
        collect_files(source, source, &mut files, false)?;
        for name in files.keys() {
            let target = stage.path().join(name);
            fs::create_dir_all(target.parent().unwrap()).map_err(error)?;
            fs::copy(source.join(name), target).map_err(error)?;
        }
        verify_release(stage.path(), trust)?;
        fs::rename(stage.path(), &output).map_err(error)?;
    }
    Ok(
        json!({"valid":true,"release_id":verified["release_id"],"published":fs::canonicalize(output).map_err(error)?}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overrides_preserve_scalar_types_and_reject_duplicates_and_structures() {
        let value = merge_parameters(
            None,
            &[
                "lots=2".into(),
                "enabled=false".into(),
                "mode=neutral".into(),
            ],
        )
        .unwrap();
        assert_eq!(value, json!({"lots":2,"enabled":false,"mode":"neutral"}));
        for values in [
            vec!["x=1", "x=2"],
            vec!["x={}"],
            vec!["x=[1]"],
            vec!["x="],
            vec!["x=null"],
            vec!["=2"],
        ] {
            assert!(
                merge_parameters(
                    None,
                    &values.into_iter().map(str::to_owned).collect::<Vec<_>>()
                )
                .is_err()
            );
        }
    }
    #[test]
    fn discovery_does_not_import_sources() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("mm.py"),
            "raise RuntimeError('must not import')",
        )
        .unwrap();
        fs::write(temp.path().join("__init__.py"), "").unwrap();
        let result = discover(Some(temp.path()), None).unwrap();
        assert_eq!(result["sources"].as_array().unwrap().len(), 1);
        assert_eq!(result["sources"][0]["name"], "mm");
    }

    #[test]
    fn publishing_a_lock_never_overwrites_a_concurrent_toolchain_without_update() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = Workspace {
            root: temp.path().to_owned(),
            config: Config::default(),
            configured: true,
        };
        workspace.save_lock(&json!({"toolchain":1}), false).unwrap();
        assert!(workspace.save_lock(&json!({"toolchain":2}), false).is_err());
        let current: Value =
            serde_json::from_slice(&fs::read(temp.path().join("titan.lock")).unwrap()).unwrap();
        assert_eq!(current, json!({"toolchain":1}));
        workspace.save_lock(&json!({"toolchain":2}), true).unwrap();
    }
}
