//! Fixed research orchestration over the existing management protocol.
//! Journals record intent before writes. An uncertain stage is only reconciled,
//! never retried under a new operation identity or recreated as a new draft.
use super::{
    client::{Client, ClientError, read_json},
    contract::{Action, ResourceExpectation, SaveVersion},
    frontend_config::{self, FileLock},
    frontend_targets::{self as targets, invalid, text},
    managed_operation::{WriteOptions, submit_expected},
};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
};

const MAX_JOURNAL: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Args)]
pub struct ExecutionOptions {
    /// Return after acceptance. A research pipeline stops at its first unfinished stage.
    #[arg(long)]
    pub detach: bool,
    /// Bounded observation time per operation; interruption never cancels the remote work.
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..=86400))]
    pub timeout: u64,
    /// Reconcile this private journal ID without resubmitting an uncertain stage.
    #[arg(long)]
    pub journal: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchCommand {
    /// Describe, save a draft, freeze it against that describe result, and compile.
    Build {
        target: Option<String>,
        /// Override the preset's parameters with a JSON object.
        #[arg(long, alias = "params")]
        parameters: Option<std::path::PathBuf>,
        /// Select a fixed preset ID when the strategy has several presets.
        #[arg(long)]
        preset: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Prepare a unique dataset if needed, then backtest a fixed strategy version.
    Backtest {
        target: Option<String>,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, alias = "data")]
        dataset: Option<String>,
        #[arg(long)]
        stress: bool,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Produce a report from one explicit or uniquely attributable backtest.
    Report {
        target: Option<String>,
        #[arg(long)]
        backtest: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
}
impl ResearchCommand {
    pub(super) fn has_target(&self) -> bool {
        match self {
            Self::Build {
                target, execution, ..
            }
            | Self::Backtest {
                target, execution, ..
            }
            | Self::Report {
                target, execution, ..
            } => target.is_some() || execution.journal.is_some(),
        }
    }
}

struct Journal {
    id: String,
    file: File,
    events: Vec<Value>,
    _lock: FileLock,
    _index_lock: Option<FileLock>,
}
fn pipeline_identity(kind: &str, input: &Value) -> Value {
    match kind {
        "build" => {
            json!({"preset_id":input["preset"]["id"],"node":input["preset"]["node_id"],"source_digest":input["preset"]["body"]["source_digest"],"parameters":input["parameters"]})
        }
        "backtest" => {
            json!({"version_id":input["version"]["id"],"version_digest":input["version"]["digest"],"dataset_id":input["dataset"]["id"],"input_sha256":input["dataset"]["body"]["input_sha256"],"stress":input["stress"]})
        }
        "report" => {
            json!({"backtest_id":input["backtest"]["id"],"digest":input["backtest"]["digest"]})
        }
        _ => input.clone(),
    }
}
fn journal_error(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_CONFIG", message).into()
}
fn connection(client: &Client) -> String {
    super::digest(&serde_json::to_vec(&json!({
        "url":client.options().url,"username":client.options().username,"profile":client.options().profile,
    })).expect("fixed connection identity serializes"))
}
impl Journal {
    async fn create(client: &Client, kind: &str, input: Value) -> Result<Self> {
        let root = frontend_config::journal_root(client.options())?;
        let signature = pipeline_identity(kind, &input);
        let hash = super::digest(&serde_json::to_vec(
            &json!({"kind":kind,"connection":connection(client),"input":signature}),
        )?);
        let index_lock = FileLock::try_acquire(&root.join(format!("index-{hash}.lock")))?
            .ok_or_else(|| {
                journal_error(
                    "the same fixed pipeline is already active; reconcile its original journal",
                )
            })?;
        let index_path = root.join(format!("index-{hash}.json"));
        if index_path.exists() {
            let mut index_file = frontend_config::private_file(&index_path, false)?;
            let mut bytes = Vec::new();
            (&mut index_file).take(1025).read_to_end(&mut bytes)?;
            if bytes.len() > 1024 {
                return Err(journal_error(
                    "private pipeline index exceeds its size limit",
                ));
            }
            let index: Value = serde_json::from_slice(&bytes)
                .map_err(|_| journal_error("invalid private pipeline index"))?;
            let mut existing = Self::resume(client, text(&index, "journal_id")?, kind)?;
            if pipeline_identity(kind, &existing.input()) != signature {
                return Err(journal_error(
                    "private pipeline index does not match its fixed input",
                ));
            }
            if !existing.finished() {
                existing.reconcile_for_new_command(client, kind).await?;
            }
            if !existing.finished() {
                existing._index_lock = Some(index_lock);
                return Ok(existing);
            }
        }
        let id = super::nonce();
        let lock = FileLock::try_acquire(&root.join(format!("{id}.lock")))?
            .ok_or_else(|| journal_error("pipeline journal is busy"))?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join(format!("{id}.jsonl")))
            .map_err(|_| journal_error("cannot create private pipeline journal"))?;
        let mut journal = Self {
            id,
            file,
            events: Vec::new(),
            _lock: lock,
            _index_lock: Some(index_lock),
        };
        journal.record(json!({"event":"pipeline","schema_version":1,"kind":kind,"connection":connection(client),"input":input}))?;
        let mut temporary = tempfile::NamedTempFile::new_in(&root)
            .map_err(|_| journal_error("cannot stage private pipeline index"))?;
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
        serde_json::to_writer(&mut temporary, &json!({"journal_id":journal.id}))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|_| journal_error("cannot persist private pipeline index"))?;
        temporary
            .persist(&index_path)
            .map_err(|_| journal_error("cannot publish private pipeline index"))?;
        File::open(&root)
            .and_then(|file| file.sync_all())
            .map_err(|_| journal_error("cannot persist private journal directory"))?;
        journal.announce()?;
        Ok(journal)
    }
    fn resume(client: &Client, id: &str, kind: &str) -> Result<Self> {
        if id.len() != 64 || !id.bytes().all(|v| v.is_ascii_hexdigit()) {
            return Err(invalid(
                "journal must be its exact 64-character ID, not a path",
            ));
        }
        let root = frontend_config::journal_root(client.options())?;
        let path = root.join(format!("{id}.jsonl"));
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|_| journal_error("private journal does not exist"))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MAX_JOURNAL
        {
            return Err(journal_error("private journal has an invalid type or size"));
        }
        let lock = FileLock::try_acquire(&root.join(format!("{id}.lock")))?
            .ok_or_else(|| journal_error("pipeline journal is busy"))?;
        let mut file = frontend_config::private_file(&path, true)?;
        let mut contents = String::new();
        (&mut file)
            .take(MAX_JOURNAL + 1)
            .read_to_string(&mut contents)
            .map_err(|_| journal_error("cannot read private journal"))?;
        if contents.len() as u64 > MAX_JOURNAL || !contents.ends_with('\n') {
            return Err(journal_error(
                "private journal is incomplete or exceeds its size limit",
            ));
        }
        let events = contents
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .map_err(|_| journal_error("private journal contains an invalid record"))
            })
            .collect::<Result<Vec<_>>>()?;
        let head = events
            .first()
            .ok_or_else(|| journal_error("private journal is empty"))?;
        if head["event"] != "pipeline"
            || head["schema_version"] != 1
            || head["kind"] != kind
            || head["connection"] != connection(client)
        {
            return Err(invalid(
                "journal does not belong to this command and management connection",
            ));
        }
        let journal = Self {
            id: id.to_owned(),
            file,
            events,
            _lock: lock,
            _index_lock: None,
        };
        journal.announce()?;
        Ok(journal)
    }
    fn announce(&self) -> Result<()> {
        let mut stderr = std::io::stderr().lock();
        writeln!(stderr, "journal_id={}", self.id)
            .and_then(|_| stderr.flush())
            .map_err(|_| journal_error("cannot retain private journal identity"))?;
        Ok(())
    }
    fn record(&mut self, event: Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.push(b'\n');
        if self
            .file
            .metadata()?
            .len()
            .saturating_add(bytes.len() as u64)
            > MAX_JOURNAL
        {
            return Err(journal_error("private journal exceeds its size limit"));
        }
        self.file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_all())
            .map_err(|_| journal_error("cannot persist private journal stage"))?;
        self.events.push(event);
        Ok(())
    }
    async fn reconcile_for_new_command(&mut self, client: &Client, kind: &str) -> Result<()> {
        let Some(intent) = self
            .events
            .iter()
            .rev()
            .find(|event| event["event"] == "operation_intent")
            .cloned()
        else {
            return Ok(());
        };
        let stage = text(&intent, "stage")?;
        let node = text(&intent, "node_id")?;
        let action: Action = serde_json::from_value(intent["action"].clone())
            .map_err(|_| journal_error("journal has an invalid fixed operation action"))?;
        let receipt = if let Some(previous) = self.event("operation_receipt", stage) {
            let previous = &previous["receipt"];
            operation_identity(previous, node, &action)?;
            let id = text(previous, "id")?;
            let current = client.get(&format!("/api/v2/operations/{id}"), &[]).await?;
            if current["id"].as_str() != Some(id) {
                return Err(ClientError::new(
                    "PROTOCOL_ERROR",
                    "recorded operation identity changed during reconciliation",
                )
                .into());
            }
            current
        } else {
            client
                .get(
                    "/api/v2/operations",
                    &[("idempotency_key", text(&intent, "key")?)],
                )
                .await?
        };
        if receipt.is_null() {
            let _ = self.confirm_submission_rejected(stage, text(&intent, "key")?, &receipt)?;
        } else {
            operation_identity(&receipt, node, &action)?;
            self.retain_reconciled_receipt(kind, stage, &receipt)?;
        }
        Ok(())
    }
    fn confirm_submission_rejected(
        &mut self,
        stage: &str,
        key: &str,
        lookup: &Value,
    ) -> Result<Option<ClientError>> {
        if !lookup.is_null() || self.event("operation_receipt", stage).is_some() {
            return Ok(None);
        }
        let Some(intent) = self.event("operation_intent", stage) else {
            return Ok(None);
        };
        let Some(rejected) = self.event("submission_rejected", stage) else {
            return Ok(None);
        };
        if intent["key"].as_str() != Some(key) || rejected["key"].as_str() != Some(key) {
            return Ok(None);
        }
        let Some(status) = rejected["status"]
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
        else {
            return Ok(None);
        };
        let Some(code) = rejected["code"].as_str() else {
            return Ok(None);
        };
        let mut error = ClientError::new(
            code,
            "operation submission was explicitly rejected; its original key has no accepted operation",
        );
        error.status = Some(status);
        error.request_id = rejected["request_id"].as_str().map(str::to_owned);
        if !definite_submission_rejection(&error) {
            return Ok(None);
        }
        if self.event("finished", stage).is_none() {
            self.record(json!({"event":"finished","stage":stage,"outcome":"rejected","key":key,"code":code,"status":status,"operation_lookup":null}))?;
        }
        Ok(Some(error))
    }
    fn retain_reconciled_receipt(
        &mut self,
        kind: &str,
        stage: &str,
        receipt: &Value,
    ) -> Result<()> {
        self.retain_receipt(stage, receipt)?;
        if kind == "control"
            && receipt["state"] == "succeeded"
            && self.event("finished", stage).is_none()
        {
            self.record(
                json!({"event":"finished","stage":stage,"outcome":"succeeded","receipt":receipt}),
            )?;
        }
        Ok(())
    }
    fn retain_receipt(&mut self, stage: &str, receipt: &Value) -> Result<()> {
        self.record(json!({"event":"operation_receipt","stage":stage,"receipt":receipt}))?;
        if matches!(receipt["state"].as_str(), Some("failed" | "cancelled"))
            && self.event("finished", stage).is_none()
        {
            self.record(json!({"event":"finished","stage":stage,"outcome":receipt["state"],"receipt":receipt}))?;
        }
        Ok(())
    }
    fn finished(&self) -> bool {
        self.events.iter().any(|event| event["event"] == "finished")
    }
    fn input(&self) -> Value {
        self.events[0]["input"].clone()
    }
    fn event(&self, event: &str, stage: &str) -> Option<Value> {
        self.events
            .iter()
            .rev()
            .find(|v| v["event"] == event && v["stage"] == stage)
            .cloned()
    }
}

fn definite_submission_rejection(error: &ClientError) -> bool {
    matches!(error.status, Some(400 | 403 | 409 | 412 | 422)) && error.code != "PROTOCOL_ERROR"
}

fn operation_identity(value: &Value, node: &str, action: &Action) -> Result<()> {
    if value["node_id"].as_str() != Some(node) || value["input"] != serde_json::to_value(action)? {
        return Err(ClientError::new(
            "PROTOCOL_ERROR",
            "operation receipt does not match the fixed stage input",
        )
        .into());
    }
    super::managed_strategy::checked_id(text(value, "id")?)
}

async fn observe(
    client: &Client,
    journal: &mut Journal,
    stage: &str,
    node: &str,
    action: &Action,
    initial: Value,
    timeout: u64,
) -> Result<Value> {
    let id = text(&initial, "id")?.to_owned();
    let mut latest = initial;
    let observe = async {
        loop {
            match latest["state"].as_str() {
                Some("succeeded") => return Ok(latest.clone()),
                Some("failed") => {
                    return Err(ClientError::new(
                        "OPERATION_FAILED",
                        "operation failed; inspect its retained receipt",
                    )
                    .into());
                }
                Some("cancelled") => {
                    return Err(
                        ClientError::new("OPERATION_CANCELLED", "operation was cancelled").into(),
                    );
                }
                Some("pending" | "running" | "unknown") => {}
                _ => {
                    return Err(ClientError::new(
                        "PROTOCOL_ERROR",
                        "operation has no recognized state",
                    )
                    .into());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let value = client.get(&format!("/api/v2/operations/{id}"), &[]).await?;
            operation_identity(&value, node, action)?;
            if value["id"] != id {
                return Err(ClientError::new(
                    "PROTOCOL_ERROR",
                    "operation identity changed during observation",
                )
                .into());
            }
            latest = value;
        }
    };
    let result = tokio::select! {
        result = tokio::time::timeout(std::time::Duration::from_secs(timeout), observe) => result.ok(),
        _ = tokio::signal::ctrl_c() => Some(Err(ClientError::new("INTERRUPTED", "observation stopped; no remote cancellation was sent").into())),
    };
    let result = result.unwrap_or_else(|| {
        if latest["state"] == "unknown" {
            Err(ClientError::new(
                "OPERATION_UNKNOWN",
                "operation remains unknown; retain its journal and reconcile without resubmission",
            )
            .into())
        } else {
            Err(ClientError::new(
                "WAIT_TIMEOUT",
                "observation deadline elapsed; the operation continues under its original identity",
            )
            .into())
        }
    });
    journal.retain_receipt(stage, &latest)?;
    if result.is_err() {
        client.print_kind("operation", &latest)?;
    }
    result
}

async fn operation(
    client: &Client,
    journal: &mut Journal,
    stage: &str,
    node: &str,
    action: Action,
    execution: &ExecutionOptions,
) -> Result<Value> {
    operation_expected(
        client,
        journal,
        stage,
        node,
        action,
        execution,
        std::collections::BTreeMap::new(),
    )
    .await
}

async fn operation_expected(
    client: &Client,
    journal: &mut Journal,
    stage: &str,
    node: &str,
    action: Action,
    execution: &ExecutionOptions,
    expected_resources: std::collections::BTreeMap<String, ResourceExpectation>,
) -> Result<Value> {
    if client
        .options()
        .node
        .as_deref()
        .is_some_and(|selected| selected != node)
        || client
            .environment()
            .is_some_and(|selected| selected != action.environment())
    {
        return Err(invalid(
            "recorded stage does not match the selected node or environment",
        ));
    }
    let receipt = if let Some(intent) = journal.event("operation_intent", stage) {
        if intent["node_id"].as_str() != Some(node)
            || intent["action"] != serde_json::to_value(&action)?
        {
            return Err(invalid(
                "journal stage input changed; no write was submitted",
            ));
        }
        // An intent proves that a write may have left this process. Even a null
        // by-key lookup is not permission to retry it.
        let receipt = if let Some(previous) = journal.event("operation_receipt", stage) {
            let id = text(&previous["receipt"], "id")?;
            let value = client.get(&format!("/api/v2/operations/{id}"), &[]).await?;
            if value["id"].as_str() != Some(id) {
                return Err(ClientError::new(
                    "PROTOCOL_ERROR",
                    "recorded operation response identity changed",
                )
                .into());
            }
            value
        } else {
            client
                .get(
                    "/api/v2/operations",
                    &[("idempotency_key", text(&intent, "key")?)],
                )
                .await?
        };
        if receipt.is_null() {
            if let Some(error) =
                journal.confirm_submission_rejected(stage, text(&intent, "key")?, &receipt)?
            {
                return Err(error.into());
            }
            return Err(ClientError::new("OPERATION_UNKNOWN", "stage acceptance is unproven; retain the journal and reconcile without resubmission").into());
        }
        receipt
    } else {
        let key = super::nonce();
        journal.record(json!({"event":"operation_intent","stage":stage,"key":key,"node_id":node,"action":action,"expected_resources":expected_resources}))?;
        match submit_expected(
            client,
            node,
            action.clone(),
            WriteOptions {
                idempotency_key: Some(key.clone()),
                wait: false,
                timeout_seconds: execution.timeout,
            },
            expected_resources,
        )
        .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Some(rejected) = error.downcast_ref::<ClientError>()
                    && definite_submission_rejection(rejected)
                {
                    journal.record(json!({"event":"submission_rejected","stage":stage,"key":key,"code":rejected.code,"status":rejected.status,"request_id":rejected.request_id}))?;
                    if let Ok(lookup) = client
                        .get("/api/v2/operations", &[("idempotency_key", key.as_str())])
                        .await
                    {
                        let _ = journal.confirm_submission_rejected(stage, &key, &lookup)?;
                    }
                }
                return Err(error);
            }
        }
    };
    operation_identity(&receipt, node, &action)?;
    journal.retain_receipt(stage, &receipt)?;
    // Even detach must preserve explicit failed/unknown outcomes. Observation of
    // non-success receipts goes through the one shared operation state policy.
    let state = receipt["state"].as_str();
    if execution.detach && matches!(state, Some("pending" | "running")) {
        return Ok(receipt);
    }
    observe(
        client,
        journal,
        stage,
        node,
        &action,
        receipt,
        execution.timeout,
    )
    .await
}

fn ready(value: &Value) -> bool {
    value["state"] == "succeeded"
}
fn output(
    client: &Client,
    journal: &mut Journal,
    stage: &str,
    operation: Value,
    resource: Option<Value>,
) -> Result<()> {
    if ready(&operation) {
        journal.record(json!({"event":"finished","stage":stage,"outcome":"succeeded"}))?;
    }
    client.print_kind("research", &json!({"journal_id":journal.id,"stage":stage,"complete":ready(&operation),"operation":operation,"resource":resource}))
}

pub(super) async fn control(
    client: &Client,
    node: &str,
    action: Action,
    execution: &ExecutionOptions,
) -> Result<()> {
    let input = json!({"node_id":node,"action":action});
    let mut journal = if let Some(id) = &execution.journal {
        Journal::resume(client, id, "control")?
    } else {
        Journal::create(client, "control", input.clone()).await?
    };
    if journal.input() != input {
        return Err(invalid(
            "control journal fixes another target or generation",
        ));
    }
    let value = operation(client, &mut journal, "control", node, action, execution).await?;
    if ready(&value) {
        journal.record(json!({"event":"finished","stage":"control","outcome":"succeeded"}))?;
    }
    client.print_kind("operation", &value)
}

pub(super) async fn resume_control(
    client: &Client,
    verb: &str,
    target: Option<&str>,
    execution: &ExecutionOptions,
) -> Result<()> {
    let id = execution
        .journal
        .as_deref()
        .ok_or_else(|| invalid("control reconciliation requires its journal ID"))?;
    let mut journal = Journal::resume(client, id, "control")?;
    let input = journal.input();
    let action: Action = serde_json::from_value(input["action"].clone())
        .map_err(|_| journal_error("control journal has an invalid fixed action"))?;
    let (expected_verb, reference, kind) = match &action {
        Action::Start { deployment_id } => ("start", deployment_id.as_str(), "deployment"),
        Action::Restore { run_id, .. } => ("start", run_id.as_str(), "run"),
        Action::Pause { run_id, .. } => ("pause", run_id.as_str(), "run"),
        Action::Resume { run_id, .. } => ("resume", run_id.as_str(), "run"),
        Action::Stop { run_id, .. } => ("stop", run_id.as_str(), "run"),
        _ => return Err(invalid("journal is not a strategy control operation")),
    };
    if verb != expected_verb {
        return Err(invalid("journal fixes another control action"));
    }
    if let Some(target) = target {
        let current = targets::resolve(client, target, &[kind]).await?;
        if current["id"].as_str() != Some(reference) {
            return Err(invalid(
                "target does not identify the journal's fixed control resource",
            ));
        }
    }
    let value = operation(
        client,
        &mut journal,
        "control",
        text(&input, "node_id")?,
        action,
        execution,
    )
    .await?;
    if ready(&value) {
        journal.record(json!({"event":"finished","stage":"control","outcome":"succeeded"}))?;
    }
    client.print_kind("operation", &value)
}

fn validate_resource(
    value: &Value,
    node: &str,
    preset: &Value,
    parameters: &Value,
    frozen: bool,
) -> Result<()> {
    if value["kind"] != "parameter_version"
        || value["node_id"].as_str() != Some(node)
        || value["body"]["parameter_preset_id"] != preset["body"]["parameter_preset_id"]
        || value["body"]["parameters"] != *parameters
        || value["body"]["source_digest"] != preset["body"]["source_digest"]
        || value["body"]["strategy_id"] != preset["body"]["strategy_id"]
        || value["frozen"].as_bool() != Some(frozen)
    {
        return Err(ClientError::new(
            "PROTOCOL_ERROR",
            "saved parameter stage identity or frozen inputs do not match",
        )
        .into());
    }
    super::managed_strategy::checked_id(text(value, "id")?)?;
    text(value, "revision")?;
    Ok(())
}

async fn saved_stage(
    client: &Client,
    journal: &mut Journal,
    stage: &str,
    path: &str,
    input: &SaveVersion,
    preset: &Value,
    describe: &Value,
) -> Result<Value> {
    let frozen = input.freeze_operation_id.is_some();
    let expected = serde_json::to_value(input)?;
    let value = if let Some(saved) = journal.event("saved", stage) {
        let original = &saved["resource"];
        let value = targets::resource(client, text(original, "id")?).await?;
        if value["id"] != original["id"] {
            return Err(invalid("saved parameter identity changed"));
        }
        let current_frozen = value["frozen"]
            .as_bool()
            .ok_or_else(|| invalid("saved parameter has no frozen state"))?;
        validate_resource(
            &value,
            &input.node_id,
            preset,
            &input.parameters,
            current_frozen,
        )?;
        // A later freeze legitimately changes the current revision. Keep the
        // durably recorded draft identity/revision for the original PATCH intent.
        if frozen { value } else { original.clone() }
    } else if let Some(intent) = journal.event("save_intent", stage) {
        if intent["path"] != path || intent["input"] != expected {
            return Err(invalid("saved parameter stage input changed"));
        }
        // A draft creation has no idempotency key or client-assigned identity.
        // Its missing response cannot be attributed by scanning equal inputs.
        if !frozen {
            return Err(ClientError::new(
                "OPERATION_UNKNOWN",
                "parameter draft acceptance is unproven; reconcile its identity before continuing",
            )
            .into());
        }
        let id = path
            .rsplit('/')
            .next()
            .ok_or_else(|| invalid("invalid saved stage path"))?;
        targets::resource(client, id).await?
    } else {
        journal
            .record(json!({"event":"save_intent","stage":stage,"path":path,"input":expected}))?;
        if frozen {
            client.patch(path, expected).await?
        } else {
            client.post(path, expected).await?
        }
    };
    validate_resource(&value, &input.node_id, preset, &input.parameters, frozen)?;
    if frozen
        && (value["body"]["normalized_parameters"] != describe["result"]["normalized_parameters"]
            || value["body"]["metadata"] != describe["result"]["metadata"])
    {
        return Err(ClientError::new(
            "PROTOCOL_ERROR",
            "frozen parameter result differs from its fixed describe evidence",
        )
        .into());
    }
    journal.record(json!({"event":"saved","stage":stage,"resource":value}))?;
    Ok(value)
}

async fn result_resource(client: &Client, receipt: &Value, kind: &str) -> Result<Value> {
    let items = receipt["result"]["resources"].as_array().ok_or_else(|| {
        ClientError::new(
            "PROTOCOL_ERROR",
            "successful operation has no resource identities",
        )
    })?;
    let value = targets::unique(
        items
            .iter()
            .filter(|value| value["kind"].as_str() == Some(kind))
            .cloned()
            .collect(),
    )?;
    let value = targets::resource(client, text(&value, "id")?).await?;
    if value["kind"].as_str() != Some(kind) || value["body"]["operation_id"] != receipt["id"] {
        return Err(ClientError::new(
            "PROTOCOL_ERROR",
            "result resource does not belong to the completed operation",
        )
        .into());
    }
    Ok(value)
}

fn validate_dataset(fixed: &Value, current: &Value, prepared: Option<&Value>) -> Result<()> {
    if current["id"] != fixed["id"]
        || current["node_id"] != fixed["node_id"]
        || current["kind"] != "dataset"
        || current["body"]["dataset_id"] != fixed["body"]["dataset_id"]
        || !fixed["body"]["input_sha256"].is_object()
        || current["body"]["input_sha256"] != fixed["body"]["input_sha256"]
    {
        return Err(invalid(
            "dataset source identity changed since the research input was fixed",
        ));
    }
    let (hash, operation_id) = if let Some(prepared) = prepared {
        let resource = prepared["result"]["resources"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["id"] == fixed["id"] && item["kind"] == "dataset")
            })
            .ok_or_else(|| invalid("prepare receipt has no matching dataset resource"))?;
        if resource["body"]["input_sha256"] != fixed["body"]["input_sha256"] {
            return Err(invalid(
                "prepared data does not match the fixed source input",
            ));
        }
        (&prepared["result"]["prepared_sha256"], &prepared["id"])
    } else {
        (
            &fixed["body"]["prepared_sha256"],
            &fixed["body"]["prepared_operation_id"],
        )
    };
    if current["body"]["prepared"] != true
        || hash.as_str().is_none()
        || operation_id.as_str().is_none()
        || current["body"]["prepared_sha256"] != *hash
        || current["body"]["prepared_operation_id"] != *operation_id
    {
        return Err(invalid(
            "dataset preparation changed; the journal cannot adopt another preparation",
        ));
    }
    Ok(())
}

fn verify_target(input: &Value, target: Option<&str>) -> Result<()> {
    if target.is_some_and(|target| input["target"].as_str() != Some(target)) {
        return Err(invalid(
            "journal fixes another target; omit the target to reconcile the recorded pipeline",
        ));
    }
    Ok(())
}

async fn owned_choice(
    client: &Client,
    target: &str,
    explicit: Option<&str>,
    kind: &str,
) -> Result<Value> {
    let value = targets::resolve(client, explicit.unwrap_or(target), &[kind]).await?;
    if explicit.is_some() {
        let owner = targets::strategy(client, target).await?;
        if value["body"]["strategy_id"] != owner["id"] {
            return Err(invalid(
                "selected resource does not belong to the requested strategy",
            ));
        }
    }
    Ok(value)
}

pub(super) async fn run(client: &Client, command: ResearchCommand) -> Result<()> {
    match command {
        ResearchCommand::Build {
            target,
            parameters,
            preset,
            execution,
        } => {
            let mut journal = if let Some(id) = &execution.journal {
                Journal::resume(client, id, "build")?
            } else {
                let target = target
                    .as_deref()
                    .ok_or_else(|| invalid("build requires a strategy or preset target"))?;
                let preset =
                    owned_choice(client, target, preset.as_deref(), "parameter_preset").await?;
                let parameters = match parameters.as_deref() {
                    Some(path) => read_json(path)?,
                    None => preset["body"]["parameters"].clone(),
                };
                if !parameters.is_object() {
                    return Err(invalid("build parameters must be a JSON object"));
                }
                Journal::create(
                    client,
                    "build",
                    json!({"target":target,"preset":preset,"parameters":parameters}),
                )
                .await?
            };
            let input = journal.input();
            verify_target(&input, target.as_deref())?;
            let fixed = &input["preset"];
            if let Some(preset) = preset.as_deref()
                && fixed["id"].as_str() != Some(preset)
            {
                return Err(invalid("preset differs from the fixed journal input"));
            }
            if let Some(path) = parameters.as_deref()
                && read_json(path)? != input["parameters"]
            {
                return Err(invalid(
                    "parameter file differs from the fixed journal input",
                ));
            }
            let node = text(fixed, "node_id")?;
            let preset_id = text(&fixed["body"], "parameter_preset_id")?;
            let described = operation(
                client,
                &mut journal,
                "describe",
                node,
                Action::Describe {
                    parameter_preset_id: preset_id.into(),
                    parameters: input["parameters"].clone(),
                },
                &execution,
            )
            .await?;
            if !ready(&described) {
                return output(client, &mut journal, "describe", described, None);
            }
            if described["result"]["source_digest"] != fixed["body"]["source_digest"] {
                return Err(invalid(
                    "preset source changed; validation no longer matches the fixed build input",
                ));
            }
            let draft_input = SaveVersion {
                node_id: node.into(),
                parameter_preset_id: preset_id.into(),
                parameters: input["parameters"].clone(),
                expected_revision: None,
                freeze_operation_id: None,
            };
            let draft = saved_stage(
                client,
                &mut journal,
                "draft",
                "/api/v2/parameter-versions",
                &draft_input,
                fixed,
                &described,
            )
            .await?;
            let id = text(&draft, "id")?.to_owned();
            let freeze = SaveVersion {
                expected_revision: Some(text(&draft, "revision")?.into()),
                freeze_operation_id: Some(text(&described, "id")?.into()),
                ..draft_input
            };
            let frozen = saved_stage(
                client,
                &mut journal,
                "freeze",
                &format!("/api/v2/parameter-versions/{id}"),
                &freeze,
                fixed,
                &described,
            )
            .await?;
            let receipt = operation(
                client,
                &mut journal,
                "compile",
                node,
                Action::Compile {
                    parameter_version_id: text(&frozen, "id")?.into(),
                },
                &execution,
            )
            .await?;
            let resource = if ready(&receipt) {
                Some(result_resource(client, &receipt, "strategy_version").await?)
            } else {
                None
            };
            output(client, &mut journal, "compile", receipt, resource)
        }
        ResearchCommand::Backtest {
            target,
            version,
            dataset,
            stress,
            execution,
        } => {
            let mut journal = if let Some(id) = &execution.journal {
                Journal::resume(client, id, "backtest")?
            } else {
                let target = target
                    .as_deref()
                    .ok_or_else(|| invalid("backtest requires a strategy or fixed version"))?;
                let version =
                    owned_choice(client, target, version.as_deref(), "strategy_version").await?;
                let node = text(&version, "node_id")?;
                let all = targets::resources(client, "dataset", None).await?;
                let selected = targets::unique(
                    all.into_iter()
                        .filter(|value| {
                            value["node_id"].as_str() == Some(node)
                                && dataset.as_deref().is_none_or(|id| {
                                    value["id"].as_str() == Some(id)
                                        || value["body"]["dataset_id"].as_str() == Some(id)
                                })
                        })
                        .collect(),
                )?;
                Journal::create(
                    client,
                    "backtest",
                    json!({"target":target,"version":version,"dataset":selected,"stress":stress}),
                )
                .await?
            };
            let input = journal.input();
            verify_target(&input, target.as_deref())?;
            let version_id = text(&input["version"], "id")?;
            let dataset_id = text(&input["dataset"]["body"], "dataset_id")?;
            if version.as_deref().is_some_and(|id| id != version_id)
                || dataset.as_deref().is_some_and(|id| {
                    id != dataset_id && input["dataset"]["id"].as_str() != Some(id)
                })
                || (stress && input["stress"] != true)
            {
                return Err(invalid(
                    "backtest options differ from the fixed journal input",
                ));
            }
            let node = text(&input["version"], "node_id")?;
            let mut preparation = None;
            if input["dataset"]["body"]["prepared"] != true
                || journal.event("operation_intent", "prepare").is_some()
            {
                let prepared = operation(
                    client,
                    &mut journal,
                    "prepare",
                    node,
                    Action::Prepare {
                        dataset_id: dataset_id.into(),
                    },
                    &execution,
                )
                .await?;
                if !ready(&prepared) {
                    return output(client, &mut journal, "prepare", prepared, None);
                }
                preparation = Some(prepared);
            }
            let mut expected = std::collections::BTreeMap::new();
            if journal.event("operation_intent", "backtest").is_none() {
                let current = targets::resource(client, text(&input["dataset"], "id")?).await?;
                validate_dataset(&input["dataset"], &current, preparation.as_ref())?;
                expected.insert(
                    text(&current, "id")?.to_owned(),
                    ResourceExpectation {
                        revision: text(&current, "revision")?.to_owned(),
                        digest: text(&current, "digest")?.to_owned(),
                    },
                );
            }
            let receipt = operation_expected(
                client,
                &mut journal,
                "backtest",
                node,
                Action::Backtest {
                    strategy_version_id: version_id.into(),
                    dataset_id: dataset_id.into(),
                    stress: input["stress"] == true,
                },
                &execution,
                expected,
            )
            .await?;
            let resource = if ready(&receipt) {
                Some(result_resource(client, &receipt, "backtest").await?)
            } else {
                None
            };
            output(client, &mut journal, "backtest", receipt, resource)
        }
        ResearchCommand::Report {
            target,
            backtest,
            execution,
        } => {
            let mut journal = if let Some(id) = &execution.journal {
                Journal::resume(client, id, "report")?
            } else {
                let target = target
                    .as_deref()
                    .ok_or_else(|| invalid("report requires a strategy or fixed backtest"))?;
                let backtest =
                    owned_choice(client, target, backtest.as_deref(), "backtest").await?;
                Journal::create(
                    client,
                    "report",
                    json!({"target":target,"backtest":backtest}),
                )
                .await?
            };
            let input = journal.input();
            verify_target(&input, target.as_deref())?;
            let id = text(&input["backtest"], "id")?;
            if backtest.as_deref().is_some_and(|selected| selected != id) {
                return Err(invalid("backtest differs from the fixed journal input"));
            }
            let receipt = operation(
                client,
                &mut journal,
                "report",
                text(&input["backtest"], "node_id")?,
                Action::Report {
                    backtest_id: id.into(),
                },
                &execution,
            )
            .await?;
            let resource = if ready(&receipt) {
                Some(result_resource(client, &receipt, "report").await?)
            } else {
                None
            };
            output(client, &mut journal, "report", receipt, resource)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Journal, validate_dataset};
    use serde_json::json;

    fn journal_fixture() -> (tempfile::TempDir, Journal) {
        let root = tempfile::tempdir().unwrap();
        let file =
            super::frontend_config::private_file(&root.path().join("journal.jsonl"), true).unwrap();
        let lock = super::FileLock::try_acquire(&root.path().join("journal.lock"))
            .unwrap()
            .unwrap();
        let journal = Journal {
            id: "synthetic-journal".into(),
            file,
            events: Vec::new(),
            _lock: lock,
            _index_lock: None,
        };
        (root, journal)
    }

    #[test]
    fn proved_failed_or_cancelled_receipt_releases_index_without_another_intent() {
        for state in ["failed", "cancelled"] {
            let (root, mut journal) = journal_fixture();
            assert!(!journal.finished());
            let receipt = json!({"id":"original-operation","state":state});
            journal
                .retain_reconciled_receipt("control", "control", &receipt)
                .unwrap();
            assert!(journal.finished());
            let events = std::fs::read_to_string(root.path().join("journal.jsonl")).unwrap();
            let events = events
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0]["event"], "operation_receipt");
            assert_eq!(events[1]["event"], "finished");
            assert_eq!(events[1]["outcome"], state);
            assert_eq!(events[1]["receipt"], receipt);
            journal.retain_receipt("control", &receipt).unwrap();
            assert_eq!(
                journal
                    .events
                    .iter()
                    .filter(|event| event["event"] == "finished")
                    .count(),
                1
            );
            assert!(
                journal
                    .events
                    .iter()
                    .all(|event| event["event"] != "operation_intent")
            );
        }
    }

    #[test]
    fn unproved_operation_states_keep_the_original_index() {
        let (_root, mut journal) = journal_fixture();
        for state in ["pending", "running", "unknown"] {
            journal
                .retain_reconciled_receipt(
                    "control",
                    "control",
                    &json!({"id":"original-operation","state":state}),
                )
                .unwrap();
            assert!(!journal.finished());
        }
    }

    #[test]
    fn late_control_success_releases_index_but_research_intermediate_success_continues() {
        for (kind, stage) in [("build", "describe"), ("backtest", "prepare")] {
            let (_root, mut journal) = journal_fixture();
            journal
                .retain_reconciled_receipt(
                    kind,
                    stage,
                    &json!({"id":"original-operation","state":"succeeded"}),
                )
                .unwrap();
            assert!(!journal.finished());
        }
        let (_root, mut journal) = journal_fixture();
        journal
            .retain_reconciled_receipt(
                "control",
                "control",
                &json!({"id":"original-operation","state":"succeeded"}),
            )
            .unwrap();
        assert!(journal.finished());
    }

    #[test]
    fn explicit_admission_rejection_plus_empty_original_key_lookup_releases_index() {
        for status in [400, 403, 409, 412, 422] {
            let (_root, mut journal) = journal_fixture();
            journal
                .record(json!({"event":"operation_intent","stage":"control","key":"original-key"}))
                .unwrap();
            journal.record(json!({"event":"submission_rejected","stage":"control","key":"original-key","code":"HTTP_ERROR","status":status})).unwrap();
            let error = journal
                .confirm_submission_rejected("control", "original-key", &serde_json::Value::Null)
                .unwrap()
                .unwrap();
            assert_eq!(error.status, Some(status));
            assert!(journal.finished());
            assert_eq!(
                journal.event("finished", "control").unwrap()["outcome"],
                "rejected"
            );
            assert_eq!(
                journal
                    .events
                    .iter()
                    .filter(|event| event["event"] == "operation_intent")
                    .count(),
                1
            );
        }
    }

    #[test]
    fn empty_lookup_without_definite_matching_rejection_never_releases_index() {
        let (_root, mut journal) = journal_fixture();
        journal
            .record(json!({"event":"operation_intent","stage":"control","key":"original-key"}))
            .unwrap();
        assert!(
            journal
                .confirm_submission_rejected("control", "original-key", &serde_json::Value::Null)
                .unwrap()
                .is_none()
        );
        for (status, code, key) in [
            (500, "HTTP_ERROR", "original-key"),
            (400, "PROTOCOL_ERROR", "original-key"),
            (409, "HTTP_ERROR", "another-key"),
        ] {
            journal.record(json!({"event":"submission_rejected","stage":"control","key":key,"code":code,"status":status})).unwrap();
            assert!(
                journal
                    .confirm_submission_rejected(
                        "control",
                        "original-key",
                        &serde_json::Value::Null
                    )
                    .unwrap()
                    .is_none()
            );
            assert!(!journal.finished());
        }
    }

    #[test]
    fn rejection_cannot_override_existing_or_previously_accepted_operation() {
        let (_root, mut journal) = journal_fixture();
        journal
            .record(json!({"event":"operation_intent","stage":"control","key":"original-key"}))
            .unwrap();
        journal.record(json!({"event":"submission_rejected","stage":"control","key":"original-key","code":"CONFLICT","status":409})).unwrap();
        let receipt = json!({"id":"accepted-operation","state":"pending"});
        assert!(
            journal
                .confirm_submission_rejected("control", "original-key", &receipt)
                .unwrap()
                .is_none()
        );
        assert!(!journal.finished());
        journal.retain_receipt("control", &receipt).unwrap();
        assert!(
            journal
                .confirm_submission_rejected("control", "original-key", &serde_json::Value::Null)
                .unwrap()
                .is_none()
        );
        assert!(!journal.finished());
    }

    #[test]
    fn backtest_cannot_adopt_another_prepare_even_with_identical_source_bytes() {
        let fixed = json!({"id":"build-dataset-market","node_id":"build","kind":"dataset","body":{"dataset_id":"market","input_sha256":{"quotes":"source-a"},"prepared":false}});
        let mut current = fixed.clone();
        current["body"]["prepared"] = json!(true);
        current["body"]["prepared_sha256"] = json!("prepared-a");
        current["body"]["prepared_operation_id"] = json!("prepare-a");
        let receipt = json!({"id":"prepare-a","result":{"prepared_sha256":"prepared-a","resources":[current.clone()]}});
        assert!(validate_dataset(&fixed, &current, Some(&receipt)).is_ok());
        current["body"]["prepared_operation_id"] = json!("prepare-b");
        assert!(validate_dataset(&fixed, &current, Some(&receipt)).is_err());
        current["body"]["prepared_operation_id"] = json!("prepare-a");
        current["body"]["prepared_sha256"] = json!("prepared-b");
        assert!(validate_dataset(&fixed, &current, Some(&receipt)).is_err());
    }

    #[test]
    fn prepared_header_and_prepare_receipt_both_remain_bound_to_original_sources() {
        let fixed = json!({"id":"build-dataset-market","node_id":"build","kind":"dataset","body":{"dataset_id":"market","input_sha256":{"quotes":"source-a"},"prepared":true,"prepared_sha256":"prepared-a","prepared_operation_id":"prepare-a"}});
        assert!(validate_dataset(&fixed, &fixed, None).is_ok());
        let mut changed = fixed.clone();
        changed["body"]["input_sha256"]["quotes"] = json!("source-b");
        assert!(validate_dataset(&fixed, &changed, None).is_err());
        let receipt = json!({"id":"prepare-a","result":{"prepared_sha256":"prepared-a","resources":[changed]}});
        assert!(validate_dataset(&fixed, &fixed, Some(&receipt)).is_err());
    }
}
