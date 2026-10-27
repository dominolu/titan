//! Worker-owned local query/control boundary. Management never calls venue APIs through this layer.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use titan_account_service::{AccountFactJournal, AccountService, AccountServiceCore};
use titan_strategy_runtime::{
    BusinessObservationSnapshot, StrategyAdminService, StrategyService, StrategyServiceCore,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const HOST_API_VERSION: u32 = 2;
const MAX_REQUEST: usize = 65_536;
const MAX_RESPONSE: usize = 8 * 1024 * 1024;
/// A native runtime owns blocking shutdown/join operations; keep its entire lifetime off async lanes.
pub(crate) fn run_runtime_owner<T: Send>(work: impl FnOnce() -> Result<T> + Send) -> Result<T> {
    std::thread::scope(|scope| {
        scope.spawn(work).join().map_err(|_| {
            anyhow::anyhow!("runtime owner panicked; worker outcome requires reconciliation")
        })?
    })
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostClaim {
    pub run_id: String,
    #[serde(alias = "generation")]
    pub worker_session_id: String,
    pub account: String,
    pub instrument: String,
    pub binding_id: String,
    pub token: String,
    pub socket: PathBuf,
    pub readonly: bool,
}
// Stored receipts retain their original bytes. Normalize only their identity
// for comparison, rejecting duplicate/ambiguous spellings through serde.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlReceiptIdentity {
    id: String,
    digest: String,
    #[serde(alias = "generation", alias = "run_generation")]
    expected_run_generation: String,
    action: String,
}
pub(crate) fn control_receipt_identity(input: &Value) -> Result<Value> {
    let identity: ControlReceiptIdentity = serde_json::from_value(input.clone())?;
    Ok(serde_json::to_value(identity)?)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostQuery {
    Control {
        operation_id: String,
        input_digest: String,
        #[serde(alias = "strategy_generation")]
        expected_run_generation: String,
        action: String,
    },
    ControlReceipt {
        operation_id: String,
        input_digest: String,
        #[serde(alias = "strategy_generation")]
        expected_run_generation: String,
        action: String,
    },
    Snapshot,
    History {
        kind: Option<String>,
        offset: usize,
        limit: usize,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    api_version: u32,
    run_id: String,
    #[serde(alias = "generation")]
    worker_session_id: String,
    token: String,
    query: HostQuery,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    api_version: u32,
    run_id: String,
    #[serde(alias = "generation")]
    worker_session_id: String,
    pid: String,
    start_time: String,
    payload: Option<Value>,
    error: Option<String>,
}

pub fn host_socket(root: &Path, run: &str, generation: &str) -> PathBuf {
    let hash = Sha256::digest(format!("{}:{run}:{generation}", root.display()).as_bytes());
    let short: String = hash[..16].iter().map(|v| format!("{v:02x}")).collect();
    PathBuf::from(format!("/tmp/titan-host-{}-{short}/ipc.sock", unsafe {
        libc::geteuid()
    }))
}
pub async fn query_host(
    claim: &HostClaim,
    query: HostQuery,
    pid: u32,
    start: u64,
) -> Result<Value> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::UnixStream::connect(&claim.socket),
    )
    .await??;
    let request = serde_json::to_vec(&Request {
        api_version: HOST_API_VERSION,
        run_id: claim.run_id.clone(),
        worker_session_id: claim.worker_session_id.clone(),
        token: claim.token.clone(),
        query,
    })?;
    ensure!(
        request.len() <= MAX_REQUEST,
        "worker request limit exceeded"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        stream.write_u32(request.len() as u32).await?;
        stream.write_all(&request).await?;
        let size = stream.read_u32().await? as usize;
        ensure!(size <= MAX_RESPONSE, "worker reply limit exceeded");
        let mut bytes = vec![0; size];
        stream.read_exact(&mut bytes).await?;
        let reply: Reply = serde_json::from_slice(&bytes)?;
        ensure!(
            reply.api_version == HOST_API_VERSION
                && reply.run_id == claim.run_id
                && reply.worker_session_id == claim.worker_session_id
                && reply.pid == pid.to_string()
                && reply.start_time == start.to_string(),
            "worker reply identity changed"
        );
        if let Some(error) = reply.error {
            anyhow::bail!("worker rejected query: {error}");
        }
        reply.payload.context("worker returned no payload")
    })
    .await?
}

struct State {
    claim: HostClaim,
    pid: u32,
    start: u64,
    stop: AtomicBool,
    account: Arc<AccountServiceCore>,
    strategy: Option<Arc<StrategyServiceCore>>,
    journal: Arc<AccountFactJournal>,
    snapshot: RwLock<Value>,
    control: Mutex<()>,
}
pub struct WorkerHost {
    state: Arc<State>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl WorkerHost {
    pub fn start(
        claim: HostClaim,
        account: Arc<AccountServiceCore>,
        strategy: Option<Arc<StrategyServiceCore>>,
        journal: Arc<AccountFactJournal>,
    ) -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        ensure!(claim.token.len() >= 32, "worker credential is too short");
        let parent = claim
            .socket
            .parent()
            .context("worker socket has no parent")?;
        match std::fs::DirBuilder::new().mode(0o700).create(parent) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let metadata = std::fs::symlink_metadata(parent)?;
        ensure!(
            metadata.is_dir()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o777 == 0o700,
            "worker socket directory ownership is invalid"
        );
        let pid = std::process::id();
        let start = crate::registry::process_start_time(pid)
            .context("worker process identity unavailable")?;
        let state = Arc::new(State {
            claim,
            pid,
            start,
            stop: AtomicBool::new(false),
            account,
            strategy,
            journal,
            snapshot: RwLock::new(json!({"available":false,"reason":"worker is starting"})),
            control: Mutex::new(()),
        });
        let shared = state.clone();
        let (ready, wait) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("worker-host".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let listener = match tokio::net::UnixListener::bind(&shared.claim.socket) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready.send(Err(error.to_string()));
                            return;
                        }
                    };
                    let _ = std::fs::set_permissions(
                        &shared.claim.socket,
                        std::fs::Permissions::from_mode(0o600),
                    );
                    shared.sample();
                    let _ = ready.send(Ok(()));
                    let ledger = tokio::spawn(State::poll_ledger(shared.clone()));
                    let sampling = shared.clone();
                    let sampler = tokio::task::spawn_blocking(move || {
                        while !sampling.stop.load(Ordering::Acquire) {
                            sampling.sample();
                            std::thread::sleep(Duration::from_millis(500));
                        }
                    });
                    let slots = Arc::new(tokio::sync::Semaphore::new(4));
                    while !shared.stop.load(Ordering::Acquire) {
                        let accepted =
                            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                                .await;
                        let Ok(Ok((stream, _))) = accepted else {
                            continue;
                        };
                        let Ok(permit) = slots.clone().try_acquire_owned() else {
                            continue;
                        };
                        let connection = shared.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let _ = tokio::time::timeout(
                                Duration::from_secs(2),
                                connection.serve(stream),
                            )
                            .await;
                        });
                    }
                    sampler.abort();
                    let _ = sampler.await;
                    ledger.abort();
                    let _ = ledger.await;
                    let _ = std::fs::remove_file(&shared.claim.socket);
                });
            })?;
        match wait.recv_timeout(Duration::from_secs(15)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                state.stop.store(true, Ordering::Release);
                anyhow::bail!("worker IPC start failed: {error}");
            }
            Err(_) => {
                state.stop.store(true, Ordering::Release);
                anyhow::bail!("worker IPC start timed out");
            }
        }
        Ok(Self {
            state,
            thread: Some(thread),
        })
    }
    pub fn snapshot(&self) -> Value {
        self.state
            .snapshot
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub fn finish(&mut self) -> Value {
        self.state.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let mut snapshot = self.snapshot();
        snapshot["process_state"] = "stopped".into();
        snapshot["stopped_at_ns"] = now().into();
        snapshot
    }
}
impl Drop for WorkerHost {
    fn drop(&mut self) {
        self.finish();
    }
}
impl State {
    fn control(
        &self,
        id: &str,
        digest: &str,
        generation: &str,
        action: &str,
        admit: bool,
    ) -> Result<Value> {
        ensure!(
            !self.claim.readonly,
            "readonly worker cannot control strategies"
        );
        crate::management::checked_id(id)?;
        ensure!(
            matches!(action, "pause" | "resume" | "stop"),
            "unsupported worker control"
        );
        let _guard = self
            .control
            .lock()
            .map_err(|_| anyhow::anyhow!("control lock unavailable"))?;
        let service = self
            .strategy
            .as_ref()
            .context("strategy service unavailable")?;
        let directory = self
            .claim
            .socket
            .parent()
            .context("missing socket directory")?
            .join("controls");
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{id}.json"));
        let identity =
            json!({"id":id,"digest":digest,"expected_run_generation":generation,"action":action});
        let mut receipt: Value = if path.exists() {
            let old: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
            ensure!(
                control_receipt_identity(&old["input"])? == identity,
                "control idempotency conflict"
            );
            old
        } else {
            ensure!(
                admit,
                "control receipt absent; engine action was not queried"
            );
            ensure!(
                std::fs::read_dir(&directory)?.count() < if action == "stop" { 1025 } else { 1024 },
                "worker control receipt capacity exceeded"
            );
            let instances = service.list();
            ensure!(instances.len() == 1, "control requires one bound strategy");
            let handle = instances[0].handle;
            ensure!(
                handle.generation.to_string() == generation,
                "strategy generation changed"
            );
            let mut pending =
                json!({"input":identity,"state":"unknown","detail":"admitted before engine call"});
            atomic_receipt(&path, &pending)?;
            use titan_strategy_runtime::PauseReason;
            let result = match action {
                "pause" => service.pause(handle, PauseReason::User),
                "resume" => service.resume(handle),
                "stop" => service.stop(handle, std::time::Instant::now() + Duration::from_secs(10)),
                _ => unreachable!(),
            };
            match result {
                Ok(operation) => {
                    pending["runtime_operation_id"] = operation.0.to_string().into();
                    pending["state"] = "running".into();
                }
                Err(error) => {
                    pending["state"] = "failed".into();
                    pending["detail"] = error.to_string().into();
                }
            }
            atomic_receipt(&path, &pending)?;
            pending
        };
        if matches!(receipt["state"].as_str(), Some("succeeded" | "failed")) {
            return Ok(receipt);
        }
        let before = receipt.clone();
        if let Some(id) = receipt["runtime_operation_id"].as_str() {
            let operation =
                service.operation(titan_strategy_runtime::StrategyOperationId(id.parse()?));
            receipt["state"] = match operation.state {
                titan_strategy_runtime::StrategyOperationState::Pending => "running",
                titan_strategy_runtime::StrategyOperationState::Succeeded => "succeeded",
                titan_strategy_runtime::StrategyOperationState::Failed => "failed",
            }
            .into();
            receipt["detail"] = operation.detail.as_ref().into();
            if receipt != before {
                atomic_receipt(&path, &receipt)?;
            }
        }
        Ok(receipt)
    }
    async fn poll_ledger(state: Arc<Self>) {
        while !state.stop.load(Ordering::Acquire) {
            let to_ms = now().parse::<i64>().unwrap_or(0) / 1_000_000 - 2000;
            let from_ms = state
                .journal
                .ledger_cursor(&state.claim.account)
                .ok()
                .flatten()
                .map(|end| end.saturating_sub(10_000))
                .unwrap_or(to_ms.saturating_sub(3_600_000))
                .max(0);
            let mut records = Vec::new();
            let mut complete = false;
            let mut reason = None;
            for page in 1..=8 {
                let result: Result<Vec<titan_account_service::AccountLedgerRecord>> = async {
                    let handle = state.account.resolve(&state.claim.account)?;
                    let query = state
                        .account
                        .ledger_page(handle, from_ms, to_ms, page, 1000)?;
                    Ok(tokio::time::timeout(Duration::from_secs(2), query).await??)
                }
                .await;
                match result {
                    Ok(items) => {
                        let end = items.len() < 1000;
                        records.extend(items);
                        if end {
                            complete = true;
                            break;
                        }
                    }
                    Err(error) => {
                        reason = Some(error.to_string());
                        break;
                    }
                }
            }
            if !complete && reason.is_none() {
                reason = Some("ledger scan capacity exceeded; coverage is incomplete".into());
            }
            let _ = state.journal.ledger_batch(
                &state.claim.account,
                from_ms,
                to_ms,
                records,
                complete,
                reason,
            );
            for _ in 0..300 {
                if state.stop.load(Ordering::Acquire) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    fn sample(&self) {
        let value=self.build_snapshot().unwrap_or_else(|error|json!({"available":false,"reason":error.to_string(),"captured_at_ns":now(),"account":self.claim.account,"instrument":self.claim.instrument}));
        *self.snapshot.write().unwrap_or_else(|p| p.into_inner()) = value;
    }
    fn execution_risk(&self) -> Result<Vec<titan_account_service::ExecutionRiskEvidence>> {
        let mut evidence = Vec::new();
        if let Some(service) = &self.strategy {
            for strategy in service.list().iter() {
                evidence.extend(service.execution_risk(strategy.handle)?);
            }
        }
        Ok(evidence)
    }
    fn build_snapshot(&self) -> Result<Value> {
        let account = self.account.resolve(&self.claim.account)?;
        let health = self.account.health(account)?;
        let diagnostics = self.account.diagnostics(account)?;
        let mut execution = self.execution_risk()?;
        let mut account_flat = false;
        let image = match self.account.recovery_snapshot(account) {
            Ok(image) => {
                // A sample spans two independently owned projections. A changed intent or
                // inventory view cannot prove that the combined exposure is complete.
                if execution != self.execution_risk()? {
                    for evidence in &mut execution {
                        evidence.inventory_confirmed = false;
                    }
                }
                account_flat = image.positions.items.iter().all(|p| p.quantity_lots == 0)
                    && image
                        .orders
                        .items
                        .iter()
                        .all(|o| matches!(o.status, 2 | 3 | 4 | 6));
                json!({"available":true,"image":self.journal.observe_account_with_execution(&self.claim.account,&image,&execution).map_err(|e|anyhow::anyhow!(e.to_string()))?})
            }
            Err(error) => {
                let old = self.snapshot.read().unwrap_or_else(|p| p.into_inner());
                json!({"available":false,"reason":error.to_string(),"last_valid_image":old.get("account_snapshot").and_then(|v|v.get("image")).or_else(||old.get("account_snapshot").and_then(|v|v.get("last_valid_image")))})
            }
        };
        let mut strategies = Vec::new();
        let mut release_safe = false;
        if let Some(service) = &self.strategy {
            release_safe = !service.list().is_empty()
                && service
                    .list()
                    .iter()
                    .all(|s| service.release_safe(s.handle).unwrap_or(false));
            for instance in service.list().iter() {
                let state = service.state(instance.handle)?;
                let diagnostic = service.diagnostics(instance.handle)?;
                strategies.push(json!({"strategy_key":instance.strategy_key,"runtime_strategy_no":instance.handle.runtime_strategy_no.0.to_string(),"run_generation":instance.handle.generation.to_string(),"lifecycle":format!("{:?}",state.lifecycle),"pause_supported":service.pause_supported(instance.handle).unwrap_or(false),"command_gate_open":state.command_gate_open,"activation_gate_open":state.activation_gate_open,"compiled_strategy_digest":hex(&instance.compiled_strategy_digest.digest),"callback_count":diagnostic.callback_count.to_string(),"command_count":diagnostic.command_count.to_string(),"last_error_code":diagnostic.last_error_code,"recovery":service.recovery_status(instance.handle).unwrap_or_else(|error|json!({"available":false,"reason":error.to_string()})),"business_observation":service.business_observation(instance.handle).unwrap_or_else(|error|BusinessObservationSnapshot::unavailable(error.to_string()))}));
            }
        }
        let financial=self.journal.summary(&self.claim.account).unwrap_or_else(|error|json!({"available":false,"reason":error.to_string(),"net_pnl":null,"max_drawdown":null}));
        // Observation versions are bounded u32 contract fields. Preserve the
        // canonical public schema bytes used by its hash; its metric values and
        // all 64-bit source identifiers already use exact strings.
        let observations: Vec<_> = strategies
            .iter_mut()
            .map(|item| {
                item.as_object_mut()
                    .and_then(|object| object.remove("business_observation"))
            })
            .collect();
        let mut snapshot = titan_domain_types::json_numbers_as_strings(
            json!({"available":true,"source":"trading_runtime","run_id":self.claim.run_id,"worker_session_id":self.claim.worker_session_id,"account":self.claim.account,"instrument":self.claim.instrument,"mode":if self.claim.readonly{"observe"}else{"trade"},"readonly":self.claim.readonly,"process_state":"running","pid":self.pid.to_string(),"start_time":self.start.to_string(),"captured_at_ns":now(),"account_health":health,"account_diagnostics":diagnostics,"account_snapshot":image,"financial":financial,"strategies":strategies}),
        );
        if let Some(strategies) = snapshot["strategies"].as_array_mut() {
            for (strategy, observation) in strategies.iter_mut().zip(observations) {
                strategy["business_observation"] = observation.unwrap_or(Value::Null);
            }
        }
        snapshot["release_proof"] = json!({"safe":release_safe && account_flat,"source":"trading_runtime","account_epoch":snapshot["account_snapshot"]["image"]["epoch"],"captured_at_ns":snapshot["captured_at_ns"]});
        Ok(snapshot)
    }
    async fn serve(self: Arc<Self>, mut stream: tokio::net::UnixStream) -> Result<()> {
        let size = stream.read_u32().await? as usize;
        ensure!(size <= MAX_REQUEST, "IPC input too large");
        let mut bytes = vec![0; size];
        stream.read_exact(&mut bytes).await?;
        let input: Request = serde_json::from_slice(&bytes)?;
        let authorized = Sha256::digest(input.token.as_bytes())
            == Sha256::digest(self.claim.token.as_bytes())
            && matches!(input.api_version, 1 | HOST_API_VERSION)
            && input.run_id == self.claim.run_id
            && input.worker_session_id == self.claim.worker_session_id;
        let result: Result<Value> = if !authorized {
            Err(anyhow::anyhow!("worker identity or credential mismatch"))
        } else {
            match input.query {
                HostQuery::Control {
                    operation_id,
                    input_digest,
                    expected_run_generation,
                    action,
                } => {
                    let state = self.clone();
                    tokio::task::spawn_blocking(move || {
                        state.control(
                            &operation_id,
                            &input_digest,
                            &expected_run_generation,
                            &action,
                            true,
                        )
                    })
                    .await?
                }
                HostQuery::ControlReceipt {
                    operation_id,
                    input_digest,
                    expected_run_generation,
                    action,
                } => {
                    let state = self.clone();
                    tokio::task::spawn_blocking(move || {
                        state.control(
                            &operation_id,
                            &input_digest,
                            &expected_run_generation,
                            &action,
                            false,
                        )
                    })
                    .await?
                }
                HostQuery::Snapshot => Ok(self
                    .snapshot
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()),
                HostQuery::History {
                    kind,
                    offset,
                    limit,
                } => {
                    let journal = self.journal.clone();
                    let account = self.claim.account.clone();
                    tokio::task::spawn_blocking(move || {
                        journal
                            .history(&account, kind.as_deref(), offset, limit)
                            .map(|items| json!({"items":items,"recording":journal.health()}))
                            .map_err(|e| anyhow::anyhow!(e.to_string()))
                    })
                    .await?
                }
            }
        };
        let (payload, error) = match result {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let reply = serde_json::to_vec(&Reply {
            api_version: input.api_version,
            run_id: self.claim.run_id.clone(),
            worker_session_id: self.claim.worker_session_id.clone(),
            pid: self.pid.to_string(),
            start_time: self.start.to_string(),
            payload,
            error,
        })?;
        ensure!(reply.len() <= MAX_RESPONSE, "IPC output too large");
        stream.write_u32(reply.len() as u32).await?;
        stream.write_all(&reply).await?;
        Ok(())
    }
}
fn now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
fn atomic_receipt(path: &Path, value: &Value) -> Result<()> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(path.parent().context("missing receipt directory")?)?.sync_all()?;
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn control_receipt_generation_aliases_preserve_identity_and_reject_duplicates() {
        let canonical = serde_json::json!({"id":"op","digest":"hash","expected_run_generation":"2","action":"pause"});
        for name in ["generation", "run_generation", "expected_run_generation"] {
            let mut historical = canonical.clone();
            historical
                .as_object_mut()
                .unwrap()
                .remove("expected_run_generation");
            historical[name] = "2".into();
            assert_eq!(
                super::control_receipt_identity(&historical).unwrap(),
                canonical
            );
        }
        let mut ambiguous = canonical;
        ambiguous["generation"] = "1".into();
        assert!(super::control_receipt_identity(&ambiguous).is_err());
    }

    use super::*;
    #[tokio::test]
    async fn async_entry_keeps_native_runtime_creation_shutdown_and_drop_on_one_sync_owner() {
        let tmp = tempfile::tempdir().unwrap();
        run_runtime_owner(|| {
            assert!(tokio::runtime::Handle::try_current().is_err());
            let raw = toml::from_str("schema_version=1")?;
            let adapted = crate::ConfigurationAdapter::adapt(raw, tmp.path())?;
            let mut runtime = crate::TradingRuntime::start_observed(
                adapted,
                tmp.path().join("facts.db"),
                "owner-regression".into(),
            )?;
            runtime.check_health()?;
            runtime.shutdown()?;
            drop(runtime);
            Ok(())
        })
        .unwrap();
    }
    #[tokio::test]
    async fn worker_query_rejects_wrong_token_generation_pid_and_unbounded_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let claim = HostClaim {
            run_id: "run".into(),
            worker_session_id: "one".into(),
            account: "missing".into(),
            instrument: "BTC".into(),
            binding_id: "binding".into(),
            token: "a".repeat(64),
            socket: host_socket(tmp.path(), "run", "one"),
            readonly: true,
        };
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "test".into(), &[]).unwrap();
        let account = AccountServiceCore::new(Default::default());
        let mut host = WorkerHost::start(claim.clone(), account, None, journal.clone()).unwrap();
        let pid = std::process::id();
        let start = crate::registry::process_start_time(pid).unwrap();
        let snapshot = query_host(&claim, HostQuery::Snapshot, pid, start)
            .await
            .unwrap();
        assert_eq!(snapshot["available"], false);
        assert!(snapshot.get("positions").is_none());
        let mut bad = claim.clone();
        bad.token = "b".repeat(64);
        assert!(
            query_host(&bad, HostQuery::Snapshot, pid, start)
                .await
                .is_err()
        );
        bad = claim.clone();
        bad.worker_session_id = "stale".into();
        assert!(
            query_host(&bad, HostQuery::Snapshot, pid, start)
                .await
                .is_err()
        );
        assert!(
            query_host(&claim, HostQuery::Snapshot, pid, start + 1)
                .await
                .is_err()
        );
        assert!(serde_json::from_value::<HostQuery>(json!({"method":"submit_order"})).is_err());
        let mut malicious = tokio::net::UnixStream::connect(&claim.socket)
            .await
            .unwrap();
        malicious.write_u32((MAX_REQUEST + 1) as u32).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), malicious.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            query_host(&claim, HostQuery::Snapshot, pid, start)
                .await
                .is_ok()
        );
        host.finish();
        assert!(!claim.socket.exists());
        journal
            .close(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
}
