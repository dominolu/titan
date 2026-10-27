use super::http::Error;
use super::{checked_id, config::NodeConfig, contract::*, digest, fault, hex, inside, nonce};
use crate::registry::{NewRun, Registry, StopAction};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use titan_domain_types::json_numbers_as_strings as precise;

pub(crate) fn atomic_json(path: &FsPath, value: &impl serde::Serialize) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", nonce()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    std::fs::rename(tmp, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
fn authenticate(config: &NodeConfig, headers: &HeaderMap) -> Result<()> {
    let expected = std::env::var(&config.token_env).context("missing node credential reference")?;
    ensure!(expected.len() >= 32, "node credential is too short");
    let actual = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if digest(actual.as_bytes()) != digest(expected.as_bytes()) {
        return Err(fault(
            401,
            "UNAUTHENTICATED",
            "node authentication required",
        ));
    }
    Ok(())
}
fn directory(config: &NodeConfig, id: &str) -> Result<PathBuf> {
    checked_id(id)?;
    Ok(config.work_root.join("operations").join(id))
}
fn occupies_task_slot(record: &crate::registry::RunRecord) -> bool {
    record.strategy_id == "management-task"
        && matches!(
            record.state.as_str(),
            "STARTING" | "LOADING" | "COMPILING" | "READY" | "RUNNING" | "STOP_REQUESTED"
        )
        && record
            .pid
            .zip(record.process_start_time)
            .is_none_or(|(pid, start)| {
                // Preserve the uncertain admission without treating a dead process as occupied CPU.
                crate::registry::process_start_time(pid) == Some(start)
            })
}
pub(crate) fn registry(config: &NodeConfig) -> Result<Registry> {
    Ok(Registry::open(&config.work_root.join("runs.sqlite3"))?)
}
pub(crate) fn read_receipt(config: &NodeConfig, id: &str) -> Result<Receipt> {
    let dir = directory(config, id)?;
    let data = match std::fs::read(dir.join("receipt.json")) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if dir.exists() || registry(config)?.get(id)?.is_some() {
                return Err(fault(
                    503,
                    "RECEIPT_UNCERTAIN",
                    "admission evidence exists but its receipt is unavailable",
                ));
            }
            return Err(fault(404, "NOT_FOUND", "operation has never been admitted"));
        }
        Err(error) => return Err(error.into()),
    };
    let mut receipt: Receipt = serde_json::from_slice(&data)?;
    if !receipt.state.terminal() {
        let record = registry(config)?.get(id)?;
        if record
            .as_ref()
            .is_some_and(|r| r.state == "CANCELLED" && r.pid.is_none())
        {
            receipt.state = OperationState::Cancelled;
            receipt.error = Some("cancelled before worker admission".into());
            if std::fs::read(dir.join("request.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<NodeRequest>(&b).ok())
                .is_some_and(|r| matches!(r.action, Action::Start { .. }))
            {
                receipt.result = Some(json!({"start_admission_absent":true}));
            }
            return Ok(receipt);
        }
        if let Some(record) = record
            && record.pid.is_some()
            && record
                .pid
                .zip(record.process_start_time)
                .is_none_or(|(pid, start)| crate::registry::process_start_time(pid) != Some(start))
        {
            receipt.state = OperationState::Unknown;
            receipt.error =
                Some("worker identity is no longer provable; no automatic restart".into());
        }
    }
    Ok(receipt)
}
pub async fn serve_node(config: NodeConfig, config_path: PathBuf) -> Result<()> {
    ensure!(
        config
            .listen
            .parse::<std::net::SocketAddr>()?
            .ip()
            .is_loopback(),
        "execution node must bind loopback; use an authenticated TLS/SSH tunnel for remote access"
    );
    std::fs::create_dir_all(config.work_root.join("operations"))?;
    let config_path = config_path.canonicalize()?;
    let listen = config.listen.clone();
    // Bind before spawning observers. One node owns each local Registry/work directory.
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    use std::os::fd::AsRawFd;
    let owner = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(config.work_root.join("node.lock"))?;
    ensure!(
        unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "node work root is already owned"
    );
    std::fs::write(
        config.work_root.join("node-config.ref"),
        config_path.to_str().context("invalid node config path")?,
    )?;
    let initial_catalog = catalog_items(&config)?;
    let live = super::live::LiveNode::open(&config, &config_path)?;
    let router = Router::new()
        .route("/v1/catalog", get(catalog))
        .route("/v1/operations", post(accept))
        .route("/v1/operations/{id}", get(receipt))
        .route("/v1/operations/{id}/cancel", post(cancel))
        .route("/v1/files/{id}/{*path}", get(file))
        .route("/v1/runs/{id}/history", get(history))
        .route("/v1/runs/{id}/log", get(runtime_log))
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(axum::middleware::from_fn(super::http::json_errors))
        .with_state(Arc::new((
            config,
            config_path,
            initial_catalog,
            std::sync::Mutex::new(()),
            live,
        )));
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
type NodeState = Arc<(
    NodeConfig,
    PathBuf,
    Vec<Resource>,
    std::sync::Mutex<()>,
    super::live::LiveNode,
)>;
#[derive(serde::Deserialize)]
struct HistoryQuery {
    kind: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}
async fn runtime_log(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> std::result::Result<Json<Value>, Error> {
    authenticate(&state.0, &headers)?;
    if super::control::is_managed(&state.0, &id) {
        return Ok(Json(super::control::read_log(
            &state.0,
            &id,
            query.offset.unwrap_or(0),
        )?));
    }
    Ok(Json(state.4.read_log(&id, query.offset.unwrap_or(0))?))
}
async fn history(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> std::result::Result<Json<Value>, Error> {
    authenticate(&state.0, &headers)?;
    let kind = query.kind.filter(|k| !k.is_empty());
    if !kind.as_deref().is_none_or(|k| {
        matches!(
            k,
            "fill"
                | "ledger"
                | "order"
                | "position"
                | "balance"
                | "stream"
                | "equity_sample"
                | "funding"
                | "cashflow"
                | "risk_sample"
        )
    }) {
        return Err(fault(
            400,
            "INVALID_HISTORY_KIND",
            "unsupported account history kind",
        )
        .into());
    }
    if super::control::is_managed(&state.0, &id) {
        return Ok(Json(
            super::control::history(
                &state.0,
                &id,
                kind,
                query.offset.unwrap_or(0),
                query.limit.unwrap_or(50).clamp(1, 200),
            )
            .await?,
        ));
    }
    Ok(Json(
        state
            .4
            .history(
                &id,
                kind,
                query.offset.unwrap_or(0),
                query.limit.unwrap_or(50).clamp(1, 200),
            )
            .await?,
    ))
}
async fn catalog(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<Resource>>, Error> {
    let config = &state.0;
    authenticate(config, &headers)?;
    let mut out = state.2.clone();
    for item in &mut out {
        if item.kind == "dataset" {
            let id = item.body["dataset_id"]
                .as_str()
                .context("missing dataset identity")?;
            let pointer = config.work_root.join("datasets").join(format!("{id}.json"));
            if pointer.is_file() {
                let updated: Resource =
                    serde_json::from_slice(&std::fs::read(pointer).map_err(anyhow::Error::from)?)
                        .map_err(anyhow::Error::from)?;
                if updated.body["input_sha256"] == item.body["input_sha256"] {
                    *item = updated;
                }
            }
        }
    }
    out.extend(state.4.catalog().await?);
    out.extend(super::control::catalog(config).await?);
    let published = config.work_root.join("published");
    if published.is_dir() {
        let entries = std::fs::read_dir(published)
            .map_err(anyhow::Error::from)?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)?;
        super::require(
            entries.len() <= 1024,
            "published resource capacity exceeded",
        )?;
        for entry in entries {
            if entry.path().extension().is_none_or(|e| e != "json") {
                continue;
            }
            let bytes = std::fs::read(entry.path()).map_err(anyhow::Error::from)?;
            super::require(bytes.len() <= 1_048_576, "published resource exceeds limit")?;
            let resource: Resource = serde_json::from_slice(&bytes).map_err(anyhow::Error::from)?;
            super::require(
                resource.node_id == config.id
                    && matches!(resource.kind.as_str(), "strategy_version" | "deployment"),
                "invalid published resource",
            )?;
            let operation = resource.body["operation_id"]
                .as_str()
                .context("missing publication operation")?;
            if read_receipt(config, operation)?.state == OperationState::Succeeded {
                out.push(resource);
            }
        }
    }
    Ok(Json(out))
}
fn catalog_items(config: &NodeConfig) -> Result<Vec<Resource>> {
    let mut out = Vec::new();
    for (id, template) in &config.parameter_presets {
        let (strategy_id, strategy_name) = template.strategy_identity(&config.id)?;
        let source = std::fs::read(&template.source)?;
        let parameters: Value = serde_json::from_slice(&std::fs::read(&template.parameters)?)?;
        out.push(resource(config,"parameter_preset",id,false,json!({"strategy_id":strategy_id,"strategy_name":strategy_name,"parameter_preset_id":id,"parameters":parameters,"source_digest":digest(&source),"environment":"backtest"})));
    }
    for (id, dataset) in &config.datasets {
        let input_sha256 = json!({"trades":digest_file(&dataset.trades)?,"quotes":digest_file(&dataset.quotes)?,"funding":digest_file(&dataset.funding)?});
        out.push(resource(config,"dataset",id,false,json!({"dataset_id":id,"regime":dataset.regime,"prepared":false,"prepared_sha256":null,"prepared_operation_id":null,"input_sha256":input_sha256,"environment":"backtest"})));
    }
    for (id, binding) in &config.bindings {
        out.push(resource(config,"binding",id,false,json!({"binding_id":id,"strategy_key":binding.strategy_key,"control_enabled":config.live_control,"account":binding.account,"account_identity":binding.account_identity,"instrument":binding.instrument,"max_exposure_quote":binding.max_exposure_quote,"max_price_deviation_bps":binding.max_price_deviation_bps,"max_duration_seconds":binding.max_duration_seconds,"environment":"live"})));
    }
    Ok(out)
}
pub(crate) fn resource(
    config: &NodeConfig,
    kind: &str,
    id: &str,
    frozen: bool,
    body: Value,
) -> Resource {
    let mut resource = super::legacy::resource(Resource {
        id: format!("{}-{}-{id}", config.id, super::legacy::id_tag(kind)),
        kind: kind.into(),
        node_id: config.id.clone(),
        revision: String::new(),
        frozen,
        digest: String::new(),
        body,
    });
    resource.digest = digest(&titan_strategy_runtime::compiler_json_v13(&resource.body));
    resource
}
async fn accept(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(input): Json<NodeRequest>,
) -> std::result::Result<Json<Receipt>, Error> {
    let config = &state.0;
    authenticate(config, &headers)?;
    super::require(
        matches!(input.api_version, 1 | API_VERSION),
        "unsupported node protocol",
    )?;
    if input.action.environment() == "live" && !config.live_control {
        return Err(fault(
            403,
            "CAPABILITY_DISABLED",
            "live control capability is disabled on this node",
        )
        .into());
    }
    super::require(
        config
            .environments
            .iter()
            .any(|v| v == input.action.environment()),
        "node capability is not enabled",
    )?;
    let _admission = state
        .3
        .lock()
        .map_err(|_| anyhow::anyhow!("node admission lock unavailable"))?;
    let dir = directory(config, &input.operation_id)?;
    if dir.exists() {
        let old: NodeRequest = serde_json::from_slice(
            &std::fs::read(dir.join("request.json")).map_err(anyhow::Error::from)?,
        )
        .map_err(anyhow::Error::from)?;
        super::require(
            old.input_digest == input.input_digest
                && serde_json::to_vec(&old).map_err(anyhow::Error::from)?
                    == serde_json::to_vec(&input).map_err(anyhow::Error::from)?,
            "operation identity conflicts with existing receipt",
        )?;
        return Ok(Json(read_receipt(config, &input.operation_id)?));
    }
    let active = registry(config)?
        .list()
        .map_err(anyhow::Error::from)?
        .iter()
        .filter(|r| occupies_task_slot(r))
        .cloned()
        .collect::<Vec<_>>();
    let stopping = matches!(input.action, Action::Stop { .. });
    let active_count = active
        .iter()
        .filter(|record| {
            let stop = std::fs::read(&record.spec_path)
                .ok()
                .and_then(|b| serde_json::from_slice::<NodeRequest>(&b).ok())
                .is_some_and(|request| matches!(request.action, Action::Stop { .. }));
            stop == stopping
        })
        .count();
    let control = matches!(
        input.action,
        Action::Pause { .. } | Action::Resume { .. } | Action::Stop { .. }
    );
    if active_count >= if control { 8 } else { 1 } {
        return Err(fault(429, "NODE_BUSY", "node task capacity is occupied").into());
    }
    // Directory creation is the durable unique admission boundary, before process creation.
    std::fs::create_dir(&dir).map_err(anyhow::Error::from)?;
    atomic_json(&dir.join("request.json"), &input)?;
    let receipt = Receipt {
        operation_id: input.operation_id.clone(),
        input_digest: input.input_digest.clone(),
        state: OperationState::Unknown,
        result: None,
        error: Some("admitted; worker start pending".into()),
    };
    atomic_json(&dir.join("receipt.json"), &receipt)?;
    let token = nonce();
    let reg = registry(config)?;
    let result = if matches!(input.action, Action::Backtest { .. }) {
        dir.join("history/result.json")
    } else {
        dir.join("task-result.json")
    };
    reg.create(NewRun {
        id: &input.operation_id,
        owner_token: &token,
        strategy_id: "management-task",
        strategy_version: "1",
        environment: input.action.environment(),
        event_mode: "tick",
        spec_path: &dir.join("request.json"),
        config_path: &state.1,
        config_sha256: &input.input_digest,
        result_path: &result,
        log_path: &dir.join("worker.log"),
    })
    .map_err(anyhow::Error::from)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("worker.log"))
        .map_err(anyhow::Error::from)?;
    let mut command = std::process::Command::new(&config.titan_program);
    command
        .args(["console", "node-worker", "--config"])
        .arg(&state.1)
        .args(["--operation", &input.operation_id, "--owner-token", &token])
        .stdin(Stdio::null())
        .stdout(log.try_clone().map_err(anyhow::Error::from)?)
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command.spawn().map_err(anyhow::Error::from)?;
    reg.spawned(&input.operation_id, &token, child.id())
        .map_err(anyhow::Error::from)?;
    // Reap without owning the execution lifetime; a node restart does not kill this worker.
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
    });
    Ok(Json(read_receipt(config, &input.operation_id)?))
}
async fn receipt(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<Receipt>, Error> {
    authenticate(&state.0, &headers)?;
    let old = read_receipt(&state.0, &id)?;
    Ok(Json(
        super::control::reconcile_receipt(&state.0, old).await?,
    ))
}
async fn cancel(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<Receipt>, Error> {
    let config = &state.0;
    authenticate(config, &headers)?;
    let old = read_receipt(config, &id)?;
    if old.state.terminal() {
        return Ok(Json(old));
    }
    if let Some(StopAction::Signal(process)) = registry(config)?
        .request_stop(&id)
        .map_err(anyhow::Error::from)?
        && crate::registry::process_start_time(process.pid) == Some(process.start_time)
    {
        // The still-matching process handles SIGTERM and cleans its own subprocess groups.
        unsafe {
            libc::kill(process.pid as i32, libc::SIGTERM);
        }
    }
    Ok(Json(read_receipt(config, &id)?))
}
async fn file(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Path((id, path)): Path<(String, String)>,
) -> std::result::Result<Response, Error> {
    authenticate(&state.0, &headers)?;
    let file = inside(&directory(&state.0, &id)?, &path)?;
    // Internal request/owner/configuration material is never downloadable.
    let name = file.file_name().and_then(|v| v.to_str()).unwrap_or("");
    super::require(
        matches!(
            name,
            "result.json"
                | "manifest.json"
                | "chart.json"
                | "equity.csv"
                | "fills.csv"
                | "journal.txt"
                | "journal.jsonl"
                | "report.html"
                | "worker.log"
                | "strategy.titan"
                | "layout.json"
        ),
        "file is not a published result",
    )?;
    let kind = match file.extension().and_then(|v| v.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "json" => "application/json",
        "csv" => "text/csv; charset=utf-8",
        _ => "application/octet-stream",
    };
    let file = tokio::fs::File::open(file)
        .await
        .map_err(anyhow::Error::from)?;
    Ok((
        [
            (header::CONTENT_TYPE, kind),
            (header::CACHE_CONTROL, "no-store"),
        ],
        axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    )
        .into_response())
}

pub(crate) async fn worker(config: NodeConfig, id: String, token: String) -> Result<()> {
    let dir = directory(&config, &id)?;
    let request: NodeRequest = serde_json::from_slice(&std::fs::read(dir.join("request.json"))?)?;
    let reg = registry(&config)?;
    ensure!(
        reg.running(&id, &token, std::process::id())?,
        "worker owner mismatch"
    );
    let stop = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, stop.clone())?;
    }
    let heartbeat_stop = stop.clone();
    let hb = config.clone();
    let hb_id = id.clone();
    let hb_token = token.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            if heartbeat_stop.load(Ordering::Acquire) {
                break;
            }
            let _ = registry(&hb).and_then(|r| Ok(r.heartbeat(&hb_id, &hb_token)?));
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    atomic_json(
        &dir.join("receipt.json"),
        &Receipt {
            operation_id: id.clone(),
            input_digest: request.input_digest.clone(),
            state: OperationState::Running,
            result: None,
            error: None,
        },
    )?;
    let result = execute(&config, &request, &dir, &stop).await;
    let absent = result.is_err()
        && matches!(request.action, Action::Start { .. })
        && !config
            .work_root
            .join("managed")
            .join(&id)
            .join("managed.json")
            .exists()
        && registry(&config)?.get(&format!("worker-{id}"))?.is_none();
    let (state, error, result) = match result {
        Ok(result) => {
            let success = result.get("business_success") != Some(&Value::Bool(false));
            (
                if result["outcome_unknown"] == true {
                    OperationState::Unknown
                } else if success {
                    OperationState::Succeeded
                } else if stop.load(Ordering::Acquire) {
                    OperationState::Cancelled
                } else {
                    OperationState::Failed
                },
                if success {
                    None
                } else {
                    Some(
                        "engine did not confirm successful completion; inspect preserved result"
                            .into(),
                    )
                },
                Some(result),
            )
        }
        Err(error) => (
            if stop.load(Ordering::Acquire) {
                if request.action.environment() == "live" && !absent {
                    OperationState::Unknown
                } else {
                    OperationState::Cancelled
                }
            } else {
                OperationState::Failed
            },
            Some(format!("{error:#}")),
            if absent {
                Some(json!({"start_admission_absent":true}))
            } else {
                None
            },
        ),
    };
    let final_receipt = Receipt {
        operation_id: id.clone(),
        input_digest: request.input_digest,
        state: state.clone(),
        result,
        error: error.clone(),
    };
    atomic_json(&dir.join("receipt.json"), &final_receipt)?;
    if final_receipt.state == OperationState::Succeeded
        && let Some(items) = final_receipt
            .result
            .as_ref()
            .and_then(|r| r["resources"].as_array())
    {
        for item in items {
            let resource: Resource = serde_json::from_value(item.clone())?;
            if matches!(resource.kind.as_str(), "strategy_version" | "deployment") {
                checked_id(&resource.id)?;
                std::fs::create_dir_all(config.work_root.join("published"))?;
                atomic_json(
                    &config
                        .work_root
                        .join("published")
                        .join(format!("{}.json", resource.id)),
                    &resource,
                )?;
            }
        }
    }
    reg.finish(
        &id,
        &token,
        if state == OperationState::Succeeded {
            "COMPLETED"
        } else if state == OperationState::Cancelled {
            "CANCELLED"
        } else {
            "FAILED"
        },
        if state == OperationState::Succeeded {
            0
        } else {
            1
        },
        error.as_deref(),
    )?;
    heartbeat.abort();
    Ok(())
}
fn reference<'a>(request: &'a NodeRequest, kind: &str) -> Result<&'a Resource> {
    request
        .resources
        .iter()
        .find(|r| r.kind == kind)
        .context(format!("missing {kind} reference"))
}
async fn execute(
    config: &NodeConfig,
    request: &NodeRequest,
    dir: &FsPath,
    stop: &AtomicBool,
) -> Result<Value> {
    let id = &request.operation_id;
    match &request.action {
        Action::Describe {
            parameter_preset_id,
            parameters,
        } => {
            let template = config
                .parameter_presets
                .get(parameter_preset_id)
                .context("unknown template")?;
            let source = std::fs::read(&template.source)?;
            ensure!(
                reference(request, "parameter_preset")?.body["source_digest"].as_str()
                    == Some(digest(&source).as_str()),
                "template source changed"
            );
            std::fs::write(dir.join("strategy.py"), source)?;
            atomic_json(&dir.join("parameters.json"), parameters)?;
            let mut cmd = tokio::process::Command::new(&config.titan_program);
            cmd.args(["strategy", "describe", "--strategy"])
                .arg(dir.join("strategy.py"))
                .arg("--parameters")
                .arg(dir.join("parameters.json"))
                .arg("--json");
            run_command(config, cmd, dir, stop).await
        }
        Action::Compile { .. } => {
            let version = reference(request, "parameter_version")?;
            ensure!(version.frozen, "parameter version is not frozen");
            let parameter_preset_id = version.body["parameter_preset_id"]
                .as_str()
                .context("missing template")?;
            let template = config
                .parameter_presets
                .get(parameter_preset_id)
                .context("unknown template")?;
            let (strategy_id, strategy_name) = template.strategy_identity(&config.id)?;
            ensure!(
                version.body["strategy_id"]
                    .as_str()
                    .is_none_or(|id| id == strategy_id),
                "strategy template identity changed"
            );
            let source = std::fs::read(&template.source)?;
            ensure!(
                version.body["source_digest"].as_str() == Some(digest(&source).as_str()),
                "source changed since parameter freeze"
            );
            std::fs::write(dir.join("strategy.py"), source)?;
            atomic_json(
                &dir.join("parameters.json"),
                &version.body["normalized_parameters"],
            )?;
            let mut cmd = tokio::process::Command::new(&config.titan_program);
            cmd.args(["strategy", "compile", "--strategy"])
                .arg(dir.join("strategy.py"))
                .arg("--parameters")
                .arg(dir.join("parameters.json"))
                .args(["--strategy-format", "bundle", "--output"])
                .arg(dir.join("strategy.titan"))
                .arg("--json");
            if let (Some(key), Some(key_id)) = (&config.signing_key, &config.signing_key_id) {
                cmd.arg("--signing-key")
                    .arg(key)
                    .arg("--key-id")
                    .arg(key_id);
            }
            let result = run_command(config, cmd, dir, stop).await?;
            let actual = result.get("result").unwrap_or(&result);
            let metadata = &actual["metadata"];
            let layout = history_layout(metadata)?;
            atomic_json(&dir.join("layout.json"), &layout)?;
            let compiled_strategy = resource(
                config,
                "strategy_version",
                id,
                true,
                json!({"strategy_id":strategy_id,"strategy_name":strategy_name,"environment":"backtest","operation_id":id,"compiled_strategy_digest":actual["compiled_strategy_digest"],"strategy_file_sha256":digest(&std::fs::read(dir.join("strategy.titan"))?),"parameter_version_id":version.id,"parameter_preset_id":parameter_preset_id,"metadata":metadata,"normalized_parameters":actual["normalized_parameters"]}),
            );
            Ok(json!({"compiler":actual,"resources":[compiled_strategy]}))
        }
        Action::Prepare { dataset_id } => {
            let dataset = config.datasets.get(dataset_id).context("unknown dataset")?;
            let original = reference(request, "dataset")?;
            let source_hashes = json!({"trades":digest_file(&dataset.trades)?,"quotes":digest_file(&dataset.quotes)?,"funding":digest_file(&dataset.funding)?});
            ensure!(
                source_hashes == original.body["input_sha256"],
                "raw inputs changed after dataset registration"
            );
            let output = dir.join("prepared.bin");
            let mut cmd = tokio::process::Command::new(
                config
                    .history_program
                    .as_ref()
                    .context("history runner unavailable")?,
            );
            cmd.args(["prepare", "--trades"])
                .arg(&dataset.trades)
                .arg("--quotes")
                .arg(&dataset.quotes)
                .arg("--funding")
                .arg(&dataset.funding)
                .arg("--output")
                .arg(&output);
            let summary = run_command(config, cmd, dir, stop).await?;
            let final_hashes = json!({"trades":digest_file(&dataset.trades)?,"quotes":digest_file(&dataset.quotes)?,"funding":digest_file(&dataset.funding)?});
            ensure!(
                source_hashes == final_hashes,
                "raw inputs changed during preparation"
            );
            let prepared_sha256 = digest_file(&output)?;
            std::fs::File::open(&output)?.sync_all()?;
            let mut body = original.body.clone();
            body["prepared"] = true.into();
            body["prepared_sha256"] = prepared_sha256.clone().into();
            body["prepared_operation_id"] = id.clone().into();
            body["quality"] = precise(summary.clone());
            let updated = resource(config, "dataset", dataset_id, false, body);
            std::fs::create_dir_all(config.work_root.join("datasets"))?;
            // Only publish the new immutable cache after successful completion; cancellation protects the old cache.
            atomic_json(
                &config
                    .work_root
                    .join("datasets")
                    .join(format!("{dataset_id}.json")),
                &updated,
            )?;
            Ok(json!({"data":summary,"prepared_sha256":prepared_sha256,"resources":[updated]}))
        }
        Action::Backtest {
            dataset_id, stress, ..
        } => {
            config.datasets.get(dataset_id).context("unknown dataset")?;
            let compiled_strategy = reference(request, "strategy_version")?;
            ensure!(
                compiled_strategy.node_id == config.id,
                "backtest compiled_strategy is on another node"
            );
            let source = directory(
                config,
                compiled_strategy.body["operation_id"]
                    .as_str()
                    .context("missing compiled_strategy operation")?,
            )?;
            ensure!(
                digest_file(&source.join("strategy.titan"))?
                    == compiled_strategy.body["strategy_file_sha256"]
                        .as_str()
                        .unwrap_or(""),
                "compiled_strategy content changed"
            );
            let dataset_ref = reference(request, "dataset")?;
            let preparation = dataset_ref.body["prepared_operation_id"]
                .as_str()
                .context("dataset has no verified preparation operation")?;
            let preparation_receipt = read_receipt(config, preparation)?;
            ensure!(
                preparation_receipt.state == OperationState::Succeeded,
                "dataset preparation did not succeed"
            );
            let published = preparation_receipt
                .result
                .as_ref()
                .and_then(|v| v["resources"].as_array())
                .and_then(|items| items.iter().find(|item| item["id"] == dataset_ref.id))
                .context("preparation did not publish this dataset")?;
            ensure!(
                published["body"] == dataset_ref.body,
                "dataset provenance differs from its preparation receipt"
            );
            let prepared = directory(config, preparation)?.join("prepared.bin");
            let prepared_sha256 = digest_file(&prepared)?;
            ensure!(
                dataset_ref.body["prepared_sha256"].as_str() == Some(prepared_sha256.as_str()),
                "prepared dataset changed or has not been prepared"
            );
            let mut cmd = tokio::process::Command::new(
                config
                    .history_program
                    .as_ref()
                    .context("history runner unavailable")?,
            );
            cmd.args(["run", "--strategy-file"])
                .arg(source.join("strategy.titan"))
                .arg("--layout")
                .arg(source.join("layout.json"))
                .arg("--data")
                .arg(&prepared)
                .arg("--output")
                .arg(dir.join("history"))
                .arg("--run-id")
                .arg(id);
            if *stress {
                cmd.arg("--stress");
            }
            let execution = run_command(config, cmd, dir, stop).await;
            let metrics = match &execution {
                Ok(value) => value.clone(),
                Err(_) if dir.join("history/result.json").is_file() => {
                    serde_json::from_slice(&std::fs::read(dir.join("history/result.json"))?)?
                }
                Err(_) => return execution,
            };
            let success = execution.is_ok()
                && metrics["success"] == true
                && digest_file(&prepared)? == prepared_sha256;
            let record = resource(
                config,
                "backtest",
                id,
                true,
                json!({"strategy_id":compiled_strategy.body["strategy_id"],"strategy_name":compiled_strategy.body["strategy_name"],"operation_id":id,"strategy_version_id":compiled_strategy.id,"dataset_id":dataset_id,"data_sha256":prepared_sha256,"input_sha256":dataset_ref.body["input_sha256"],"quality":dataset_ref.body.get("quality"),"stress":stress,"business_success":success,"metrics":precise(metrics),"environment":"backtest"}),
            );
            Ok(json!({"business_success":success,"resources":[record]}))
        }
        Action::Report { .. } => {
            let backtest = reference(request, "backtest")?;
            let source = backtest.body["operation_id"]
                .as_str()
                .context("missing backtest operation")?;
            let mut cmd = tokio::process::Command::new(&config.titan_program);
            cmd.args(["report", source, "--output"])
                .arg(dir.join("report.html"))
                .arg("--json")
                .env("TITAN_HOME", &config.work_root);
            let result = run_command(config, cmd, dir, stop).await?;
            Ok(
                json!({"report":result,"resources":[resource(config,"report",id,true,json!({"strategy_id":backtest.body["strategy_id"],"strategy_name":backtest.body["strategy_name"],"operation_id":id,"backtest_id":backtest.id,"environment":"backtest"}))]}),
            )
        }
        _ => super::control::execute(config, request, stop).await,
    }
}
pub(crate) fn digest_file(path: &FsPath) -> Result<String> {
    use sha2::Digest;
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hash = sha2::Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    Ok(hex(&hash.finalize()))
}
async fn run_command(
    config: &NodeConfig,
    mut command: tokio::process::Command,
    dir: &FsPath,
    stop: &AtomicBool,
) -> Result<Value> {
    ensure!(
        !stop.load(Ordering::Acquire),
        "task cancelled before subprocess admission"
    );
    let stdout = dir.join("command-output.json");
    let stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("worker.log"))?;
    let stderr_remaining = (4_u64 * 1024 * 1024).saturating_sub(stderr.metadata()?.len()) as usize;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    let mut child = command.spawn()?;
    let pid = child.id().context("missing child identity")?;
    let overflow = Arc::new(AtomicBool::new(false));
    let mut output = tokio::spawn(bounded_output(
        child.stdout.take().context("missing stdout")?,
        std::fs::File::create(&stdout)?,
        4 * 1024 * 1024,
        overflow.clone(),
    ));
    let mut errors = tokio::spawn(bounded_output(
        child.stderr.take().context("missing stderr")?,
        stderr,
        stderr_remaining,
        overflow.clone(),
    ));
    let deadline = Instant::now() + Duration::from_secs(config.task_seconds);
    let mut terminated = None;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if (stop.load(Ordering::Acquire)
            || Instant::now() >= deadline
            || overflow.load(Ordering::Acquire))
            && terminated.is_none()
        {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGTERM);
            }
            terminated = Some(Instant::now());
        }
        if terminated.is_some_and(|start| start.elapsed() > Duration::from_secs(5)) {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = child.kill().await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    for drain in [&mut output, &mut errors] {
        match tokio::time::timeout(Duration::from_secs(1), &mut *drain).await {
            Ok(result) => result??,
            Err(_) => {
                drain.abort();
                anyhow::bail!("subprocess retained its output pipes after exit");
            }
        }
    }
    ensure!(
        !overflow.load(Ordering::Acquire),
        "task stdout/stderr limit exceeded; output is truncated"
    );
    ensure!(
        terminated.is_none(),
        "task cancelled or exceeded its execution deadline"
    );
    ensure!(
        status.success(),
        "task process failed ({status}); see worker.log"
    );
    ensure!(
        std::fs::metadata(&stdout)?.len() <= 4 * 1024 * 1024,
        "task output limit exceeded"
    );
    let content = std::fs::read(&stdout)?;
    if content.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&content)?)
}
pub(crate) async fn bounded_output(
    mut input: impl tokio::io::AsyncRead + Unpin,
    mut file: std::fs::File,
    mut remaining: usize,
    overflow: Arc<AtomicBool>,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buffer = [0_u8; 8192];
    loop {
        let size = input.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        let keep = size.min(remaining);
        file.write_all(&buffer[..keep])?;
        remaining -= keep;
        if keep < size {
            overflow.store(true, Ordering::Release);
        }
    }
    file.sync_all()?;
    Ok(())
}
fn history_layout(metadata: &Value) -> Result<Value> {
    fn visit(
        fields: &[Value],
        prefix: &str,
        base: u64,
        out: &mut serde_json::Map<String, Value>,
    ) -> Result<()> {
        for field in fields {
            let name = format!(
                "{prefix}{}",
                field["name"].as_str().context("missing state field name")?
            );
            let offset = base + field["offset"].as_u64().context("missing field offset")?;
            if let Some(children) = field["fields"].as_array() {
                visit(children, &format!("{name}."), offset, out)?;
            } else if field["shape"].as_array().is_none_or(Vec::is_empty) {
                let kind = field["type"]
                    .as_str()
                    .context("missing scalar type")?
                    .chars()
                    .next()
                    .context("empty scalar type")?
                    .to_string();
                out.insert(
                    name,
                    json!({"offset":offset,"size":field["itemsize"],"kind":kind}),
                );
            }
        }
        Ok(())
    }
    let mut fields = serde_json::Map::new();
    visit(
        metadata["recovery_state_schema"]["fields"]
            .as_array()
            .context("missing state schema")?,
        "",
        0,
        &mut fields,
    )?;
    let hash: Vec<u8> = serde_json::from_value(metadata["recovery_state_schema_hash"].clone())?;
    Ok(
        json!({"state_len":metadata["state_len"],"schema_version":metadata["recovery_state_schema_version"],"schema_hash":hex(&hash),"fields":fields}),
    )
}
