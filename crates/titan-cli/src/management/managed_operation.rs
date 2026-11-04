//! Management operations share the same admission, idempotency and observation path.
use super::{
    client::{Client, ClientError, read_json},
    contract::{Action, SubmitOperation},
};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use std::{io::Write, path::PathBuf, time::Duration};

#[derive(Debug, Clone, Args)]
pub struct WriteOptions {
    /// Reuse this key only with exactly the same fixed request.
    #[arg(long)]
    pub idempotency_key: Option<String>,
    /// Observe the existing operation until it completes; never resubmit it.
    #[arg(long)]
    pub wait: bool,
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(0..=86400))]
    pub timeout_seconds: u64,
}
impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            idempotency_key: None,
            wait: false,
            timeout_seconds: 120,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum OperationCommand {
    #[command(alias = "ls")]
    List {
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1000000))]
        offset: u32,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        from_ns: Option<String>,
        #[arg(long)]
        to_ns: Option<String>,
    },
    #[command(alias = "show")]
    Get { id: String },
    /// Find an accepted request after a disconnect, without submitting another write.
    ByKey { key: String },
    /// Submit an immutable request document through the public operation API.
    Submit {
        #[arg(long)]
        request: PathBuf,
        #[command(flatten)]
        write: WriteOptions,
    },
    Wait {
        id: String,
        #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(0..=86400))]
        timeout_seconds: u64,
    },
    /// Request cancellation; receipt alone does not prove business completion.
    Cancel {
        id: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(0..=86400))]
        timeout_seconds: u64,
    },
}
fn input(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_INPUT", message).into()
}
fn checked_id(id: &str) -> Result<()> {
    super::checked_id(id).map_err(|_| input("invalid operation or node identity"))
}
fn checked_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(input(
            "idempotency key must contain 1 to 128 ASCII letters, digits, underscores or hyphens",
        ));
    }
    Ok(())
}
fn checked_timeout(seconds: u64) -> Result<()> {
    if seconds > 86400 {
        return Err(input("observation timeout must not exceed 86400 seconds"));
    }
    Ok(())
}
pub async fn execute(client: &Client, command: OperationCommand) -> Result<()> {
    let value = match command {
        OperationCommand::List {
            limit,
            offset,
            idempotency_key,
            run_id,
            from_ns,
            to_ns,
        } => {
            let limit = limit.to_string();
            let offset = offset.to_string();
            let mut query = vec![("limit", limit.as_str()), ("offset", offset.as_str())];
            if let Some(key) = idempotency_key.as_deref() {
                checked_key(key)?;
                query.push(("idempotency_key", key));
            }
            if let Some(run) = run_id.as_deref() {
                checked_id(run)?;
                query.push(("run_id", run));
            }
            for (name, value) in [("from_ns", from_ns.as_deref()), ("to_ns", to_ns.as_deref())] {
                if let Some(value) = value {
                    value
                        .parse::<u64>()
                        .map_err(|_| input("time filters must be unsigned nanoseconds"))?;
                    query.push((name, value));
                }
            }
            client.get("/api/v2/operations", &query).await?
        }
        OperationCommand::Get { id } => {
            checked_id(&id)?;
            client.get(&format!("/api/v2/operations/{id}"), &[]).await?
        }
        OperationCommand::ByKey { key } => {
            checked_key(&key)?;
            client
                .get("/api/v2/operations", &[("idempotency_key", &key)])
                .await?
        }
        OperationCommand::Submit { request, mut write } => {
            let request: SubmitOperation = serde_json::from_value(read_json(&request)?)
                .map_err(|_| input("request must contain node_id, idempotency_key and a supported fixed action"))?;
            if write
                .idempotency_key
                .as_deref()
                .is_some_and(|key| key != request.idempotency_key)
            {
                return Err(input("request and command line idempotency keys differ"));
            }
            write.idempotency_key = Some(request.idempotency_key);
            submit(client, &request.node_id, request.action, write).await?
        }
        OperationCommand::Wait {
            id,
            timeout_seconds,
        } => {
            checked_id(&id)?;
            wait_operation(client, &id, timeout_seconds, None).await?
        }
        OperationCommand::Cancel {
            id,
            wait,
            timeout_seconds,
        } => {
            checked_id(&id)?;
            checked_timeout(timeout_seconds)?;
            let value = client
                .post(&format!("/api/v2/operations/{id}/cancel"), json!({}))
                .await?;
            if wait {
                wait_operation(client, &id, timeout_seconds, Some(value)).await?
            } else {
                value
            }
        }
    };
    client.print_kind("operation", &value)
}

/// Return the public receipt; callers decide when to print the successful response.
/// Persist the recovery key to stderr before any request can leave this process.
pub async fn submit(
    client: &Client,
    node: &str,
    action: Action,
    options: WriteOptions,
) -> Result<Value> {
    submit_expected(
        client,
        node,
        action,
        options,
        std::collections::BTreeMap::new(),
    )
    .await
}

/// Bind caller-observed resource identities to the exact snapshot admitted by the server.
pub async fn submit_expected(
    client: &Client,
    node: &str,
    action: Action,
    options: WriteOptions,
    expected_resources: std::collections::BTreeMap<String, super::contract::ResourceExpectation>,
) -> Result<Value> {
    checked_id(node)?;
    checked_timeout(options.timeout_seconds)?;
    if client
        .options()
        .node
        .as_deref()
        .is_some_and(|selected| selected != node)
    {
        return Err(input("request node does not match the selected node"));
    }
    if client
        .environment()
        .is_some_and(|environment| environment != action.environment())
    {
        return Err(input(
            "selected environment does not match the fixed action",
        ));
    }
    let key = options.idempotency_key.unwrap_or_else(super::nonce);
    checked_key(&key)?;
    let mut request = json!({"idempotency_key":key,"node_id":node,"action":action});
    if !expected_resources.is_empty() {
        request["expected_resources"] = serde_json::to_value(expected_resources)?;
    }
    {
        let mut stderr = std::io::stderr().lock();
        writeln!(stderr, "idempotency_key={key}")
            .and_then(|_| stderr.flush())
            .map_err(|_| {
                ClientError::new(
                    "OUTPUT_ERROR",
                    "cannot retain idempotency key before submission",
                )
            })?;
    }
    preview_live_target(client, node, &action).await?;
    let value = client.post("/api/v2/operations", request).await?;
    if !options.wait {
        return Ok(value);
    }
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::new("PROTOCOL_ERROR", "operation receipt has no identity"))?
        .to_owned();
    checked_id(&id)?;
    wait_operation(client, &id, options.timeout_seconds, Some(value)).await
}
// Scalar projection prevents nested resource bodies or private configuration from
// crossing the diagnostic boundary, even if a malformed record changes a field type.
fn preview_scalar(value: &Value) -> Value {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => value.clone(),
        _ => Value::Null,
    }
}

async fn preview_resource(client: &Client, id: &str) -> Result<Value> {
    checked_id(id)?;
    client.get(&format!("/api/v2/resources/{id}"), &[]).await
}

async fn preview_live_target(client: &Client, node: &str, action: &Action) -> Result<()> {
    let (kind, target, version, requested_generation, recovery_point) = match action {
        Action::Deploy {
            strategy_version_id,
            binding_id,
        } => {
            checked_id(binding_id)?;
            let binding = format!("{node}-binding-{binding_id}");
            let target = preview_resource(client, &binding).await?;
            let version = preview_resource(client, strategy_version_id).await?;
            ("deploy", target, Some(version), None, None)
        }
        Action::Start { deployment_id } => (
            "start",
            preview_resource(client, deployment_id).await?,
            None,
            None,
            None,
        ),
        Action::Pause { run_id, generation } => (
            "pause",
            preview_resource(client, run_id).await?,
            None,
            Some(generation),
            None,
        ),
        Action::Resume { run_id, generation } => (
            "resume",
            preview_resource(client, run_id).await?,
            None,
            Some(generation),
            None,
        ),
        Action::Stop { run_id, generation } => (
            "stop",
            preview_resource(client, run_id).await?,
            None,
            Some(generation),
            None,
        ),
        Action::Restore {
            run_id,
            generation,
            checkpoint_id,
        } => (
            "restore",
            preview_resource(client, run_id).await?,
            None,
            Some(generation),
            Some(checkpoint_id),
        ),
        _ => return Ok(()),
    };
    let body = &target["body"];
    let mut data = serde_json::Map::new();
    data.insert("action".into(), json!(kind));
    data.insert("node_id".into(), json!(node));
    data.insert("resource_id".into(), preview_scalar(&target["id"]));
    data.insert(
        "resource_node_id".into(),
        preview_scalar(&target["node_id"]),
    );
    data.insert(
        "resource_revision".into(),
        preview_scalar(&target["revision"]),
    );
    data.insert(
        "run_id".into(),
        if matches!(
            action,
            Action::Pause { .. }
                | Action::Resume { .. }
                | Action::Stop { .. }
                | Action::Restore { .. }
        ) {
            preview_scalar(&target["id"])
        } else {
            Value::Null
        },
    );
    for field in [
        "strategy_id",
        "strategy_version_id",
        "deployment_id",
        "binding_id",
        "account",
        "instrument",
        "environment",
        "run_generation",
        "recovery_point_sha256",
        "max_exposure_quote",
        "max_price_deviation_bps",
        "max_duration_seconds",
        "expires_at_ns",
        "stop_action",
    ] {
        data.insert(field.into(), preview_scalar(&body[field]));
    }
    if matches!(action, Action::Start { .. }) {
        data.insert("deployment_id".into(), preview_scalar(&target["id"]));
    }
    // Deploy's strategy identity and stop policy come from its fixed version;
    // Start and run controls use only the returned deployment/run record.
    if let Some(version) = version {
        data.insert(
            "strategy_id".into(),
            preview_scalar(&version["body"]["strategy_id"]),
        );
        data.insert("strategy_version_id".into(), preview_scalar(&version["id"]));
        data.insert(
            "stop_action".into(),
            preview_scalar(&version["body"]["normalized_parameters"]["stop_action"]),
        );
    }
    data.insert(
        "expected_run_generation".into(),
        json!(requested_generation),
    );
    data.insert(
        "requested_recovery_point_sha256".into(),
        json!(recovery_point),
    );
    let preview = json!({"schema_version":1,"api_version":super::contract::API_VERSION,
        "kind":"control_target","data":data});
    let mut stderr = std::io::stderr().lock();
    writeln!(stderr, "{preview}")
        .and_then(|_| stderr.flush())
        .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write control target preview"))?;
    Ok(())
}

fn completed(value: &Value) -> Result<bool> {
    match value.get("state").and_then(Value::as_str) {
        Some("succeeded") => Ok(true),
        Some("pending" | "running") => Ok(false),
        Some("failed") => Err(ClientError::new(
            "OPERATION_FAILED",
            "operation failed; inspect the operation receipt",
        )
        .into()),
        Some("cancelled") => {
            Err(ClientError::new("OPERATION_CANCELLED", "operation was cancelled").into())
        }
        Some("unknown") => Err(ClientError::new(
            "OPERATION_UNKNOWN",
            "operation outcome is unknown; retain its identity and reconcile without resubmission",
        )
        .into()),
        _ => Err(ClientError::new(
            "PROTOCOL_ERROR",
            "operation response has no recognized state",
        )
        .into()),
    }
}
pub async fn wait_operation(
    client: &Client,
    id: &str,
    timeout_seconds: u64,
    initial: Option<Value>,
) -> Result<Value> {
    checked_id(id)?;
    checked_timeout(timeout_seconds)?;
    let mut latest = initial;
    let observe = async {
        loop {
            if let Some(value) = &latest {
                match completed(value) {
                    Ok(true) => return Ok(value.clone()),
                    Ok(false) => {}
                    Err(error) => return Err(error),
                }
            }
            let value = client.get(&format!("/api/v2/operations/{id}"), &[]).await?;
            if value.get("id").and_then(Value::as_str) != Some(id) {
                return Err(ClientError::new(
                    "PROTOCOL_ERROR",
                    "operation response identity changed",
                )
                .into());
            }
            latest = Some(value);
            if let Ok(false) = completed(latest.as_ref().expect("latest observation assigned")) {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    let result = tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(timeout_seconds), observe) => {
            result.unwrap_or_else(|_| Err(ClientError::new("WAIT_TIMEOUT", "observation deadline elapsed; the operation continues and can be observed again").into()))
        }
        _ = tokio::signal::ctrl_c() => {
            Err(ClientError::new("INTERRUPTED", "observation stopped; no cancellation was sent to the operation").into())
        }
    };
    if result.is_err()
        && let Some(value) = &latest
    {
        client.print_kind("operation", value)?;
    }
    result
}
