//! Managed deployment and process ownership. Lifecycle and recovery decisions stay in E.
use super::{checked_id, config::NodeConfig, contract::*, digest, hex, node, nonce};
use crate::{
    registry::NewRun,
    runtime_host::{HostClaim, HostQuery, WorkerHost, host_socket, query_host},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use titan_account_service::AccountService;
use titan_strategy_runtime::{ExecutionLimits, StrategyAdminService, StrategyRecoveryPolicy};

#[derive(Clone, Serialize, Deserialize)]
struct Deployment {
    resource: Resource,
    runtime_sha256: String,
    binding_sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct ManagedSpec {
    claim: HostClaim,
    deployment_id: String,
    deployment_sha256: String,
    checkpoint: Option<String>,
    previous_generation: Option<String>,
    predecessor_run_id: Option<String>,
    reservation_operation_id: String,
    budget: RunBudget,
}
#[derive(Clone, Serialize, Deserialize)]
struct RunBudget {
    boot_id: String,
    deadline_boottime_ns: String,
    expires_at_ns: String,
}
#[cfg(target_os = "linux")]
fn boot_clock() -> Result<(String, u128)> {
    let mut clock = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    ensure!(
        unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut clock) } == 0 && clock.tv_sec >= 0,
        "node monotonic clock unavailable"
    );
    Ok((
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .to_owned(),
        clock.tv_sec as u128 * 1_000_000_000 + clock.tv_nsec as u128,
    ))
}
#[cfg(not(target_os = "linux"))]
fn boot_clock() -> Result<(String, u128)> {
    bail!("managed execution requires Linux boot identity")
}
impl RunBudget {
    fn new(seconds: u64) -> Result<Self> {
        let (boot_id, now) = boot_clock()?;
        let delta = u128::from(seconds) * 1_000_000_000;
        Ok(Self {
            boot_id,
            deadline_boottime_ns: (now + delta).to_string(),
            expires_at_ns: (super::now_ns().parse::<u128>()? + delta).to_string(),
        })
    }
    fn remaining(&self) -> Result<u128> {
        let (boot_id, now) = boot_clock()?;
        ensure!(
            boot_id == self.boot_id,
            "node boot changed; recovery budget continuity is unproven"
        );
        self.deadline_boottime_ns
            .parse::<u128>()?
            .checked_sub(now)
            .context("managed runtime budget exhausted")
    }
}
fn root(config: &NodeConfig, id: &str) -> Result<PathBuf> {
    checked_id(id)?;
    Ok(config.work_root.join("managed").join(id))
}
fn deploy_root(config: &NodeConfig, id: &str) -> Result<PathBuf> {
    checked_id(id)?;
    Ok(config.work_root.join("deployments").join(id))
}
fn local_id<'a>(config: &NodeConfig, id: &'a str, kind: &str) -> Result<&'a str> {
    let prefix = format!("{}-{kind}-", config.id);
    let id = id
        .strip_prefix(&prefix)
        .context("resource belongs to another node or kind")?;
    checked_id(id)?;
    Ok(id)
}
fn reference<'a>(request: &'a NodeRequest, kind: &str) -> Result<&'a Resource> {
    request
        .resources
        .iter()
        .find(|r| r.kind == kind)
        .context(format!("missing {kind} reference"))
}
fn binding_hash(config: &NodeConfig, id: &str) -> Result<String> {
    let binding = config.bindings.get(id).context("unknown binding")?;
    Ok(digest(&serde_json::to_vec(
        &json!({"binding":id,"account":binding.account,"account_identity":binding.account_identity,"instrument":binding.instrument,"strategy":binding.strategy_key,"runtime":node::digest_file(&binding.runtime_config)?,"exposure":binding.max_exposure_quote,"price_bps":binding.max_price_deviation_bps,"duration":binding.max_duration_seconds}),
    )?))
}
fn load_deployment(config: &NodeConfig, id: &str) -> Result<Deployment> {
    let id = local_id(config, id, "deployment")?;
    Ok(serde_json::from_slice(&std::fs::read(
        deploy_root(config, id)?.join("deployment.json"),
    )?)?)
}
fn runtime_config(
    config: &NodeConfig,
    deployment: &Deployment,
    checkpoint: Option<&str>,
) -> Result<crate::ApplicationConfig> {
    let binding_id = deployment.resource.body["binding_id"]
        .as_str()
        .context("missing binding")?;
    let binding = config
        .bindings
        .get(binding_id)
        .context("binding disappeared")?;
    checked_id(
        binding
            .account_identity
            .as_deref()
            .context("canonical account identity required")?,
    )?;
    ensure!(
        binding_hash(config, binding_id)? == deployment.binding_sha256,
        "binding configuration drifted"
    );
    let bytes = std::fs::read(&binding.runtime_config)?;
    ensure!(
        digest(&bytes) == deployment.runtime_sha256,
        "runtime configuration drifted"
    );
    let mut raw: crate::ApplicationConfig = toml::from_str(std::str::from_utf8(&bytes)?)?;
    ensure!(
        raw.managed_by_console,
        "runtime configuration must declare managed_by_console=true"
    );
    raw.managed_by_console = false;
    ensure!(
        !raw.strategy_service.allow_unsigned_artifacts
            && !raw.strategy_service.trusted_ed25519_keys.is_empty(),
        "managed deployment requires signature trust"
    );
    raw.strategies
        .retain(|s| s.strategy_key.as_ref() == binding.strategy_key);
    ensure!(
        raw.strategies.len() == 1,
        "managed binding requires exactly one declared strategy"
    );
    raw.accounts
        .retain(|a| a.account_key.as_ref() == binding.account);
    ensure!(
        raw.accounts.len() == 1 && raw.accounts[0].enabled,
        "managed account missing or disabled"
    );
    let account = &mut raw.accounts[0];
    ensure!(
        account.instruments.len() == 1
            && account.instruments[0].native_symbol.as_ref() == binding.instrument,
        "managed execution requires one exact instrument"
    );
    // All facts are observed; shutdown must never cancel another owner's orders.
    account.ownership = titan_account_service::OrderOwnershipPolicy::ObserveAll;
    account.shutdown_order_policy = titan_account_service::ShutdownOrderPolicy::LeaveOpen;
    let instrument = &account.instruments[0];
    let units = [
        instrument.price_tick,
        instrument.quantity_lot,
        instrument.contract_multiplier,
    ];
    let quote: titan_domain_types::DecimalUnit = binding
        .max_exposure_quote
        .parse()
        .map_err(anyhow::Error::msg)?;
    let numerator = units
        .iter()
        .try_fold(1_i128, |n, u| n.checked_mul(i128::from(u.coefficient())))
        .context("unit overflow")?;
    let scale: u32 = units.iter().map(|u| u32::from(u.scale())).sum();
    ensure!(numerator > 0, "instrument units must be positive");
    let top = i128::from(quote.coefficient())
        .checked_mul(10_i128.checked_pow(scale).context("unit scale overflow")?)
        .context("quote overflow")?;
    let bottom = numerator
        .checked_mul(10_i128.pow(u32::from(quote.scale())))
        .context("unit overflow")?;
    let cap = top / bottom;
    ensure!(cap > 0, "exposure envelope is below one tick lot");
    let strategy = &mut raw.strategies[0];
    ensure!(
        strategy.enabled
            && strategy.accounts.len() == 1
            && strategy.accounts[0].account_key.as_ref() == binding.account
            && strategy.accounts[0].local_account_no == 0
            && strategy.accounts[0].tradable_assets.len() == 1
            && strategy.accounts[0].tradable_assets[0].local_asset_no == 0
            && strategy.accounts[0].tradable_assets[0].asset_id == instrument.asset_id.0,
        "strategy account binding mismatch"
    );
    ensure!(
        strategy.markets.len() == 1
            && strategy.markets[0].local_asset_no == 0
            && strategy.markets[0].asset_id == instrument.asset_id.0,
        "strategy market binding mismatch"
    );
    let source = strategy.markets[0].source_key.clone();
    raw.market_sources.retain(|s| s.source_key == source);
    ensure!(
        raw.market_sources.len() == 1,
        "managed market source missing"
    );
    let artifact = deploy_root(
        config,
        local_id(config, &deployment.resource.id, "deployment")?,
    )?
    .join("strategy.titan");
    ensure!(
        node::digest_file(&artifact)?
            == deployment.resource.body["blob_sha256"]
                .as_str()
                .unwrap_or(""),
        "deployed blob changed"
    );
    strategy.package.uri = Arc::from(format!("file://{}", artifact.display()));
    let bytes = deployment.resource.body["artifact_digest"]
        .as_str()
        .context("missing artifact digest")?;
    strategy.package.expected_digest = decode_hash(bytes)?;
    strategy.runtime.execution_limits = Some(ExecutionLimits {
        max_notional_tick_lots: cap.to_string(),
        max_price_deviation_bps: binding.max_price_deviation_bps,
        max_market_age_ns: 2_000_000_000,
    });
    strategy.runtime.recovery_checkpoint_sha256 = checkpoint.map(str::to_owned);
    strategy.recovery = if checkpoint.is_some() {
        StrategyRecoveryPolicy::RequireCheckpoint
    } else {
        StrategyRecoveryPolicy::Fresh
    };
    ensure!(
        strategy.runtime.stop_timeout <= Duration::from_secs(10),
        "managed stop timeout exceeds 10 seconds"
    );
    raw.strategy_service.allowed_artifact_roots = vec![config.work_root.join("deployments")];
    raw.strategy_service.checkpoint_root = Some(
        config
            .work_root
            .join("checkpoints")
            .join(
                binding
                    .account_identity
                    .as_deref()
                    .context("canonical account identity required")?,
            )
            .join(strategy.strategy_id.0.to_string()),
    );
    Ok(raw)
}
fn decode_hash(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "invalid digest");
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}

pub(super) async fn execute(
    config: &NodeConfig,
    request: &NodeRequest,
    stop: &AtomicBool,
) -> Result<Value> {
    ensure!(config.live_control, "live capability disabled");
    match &request.action {
        Action::Deploy {
            artifact_id,
            binding_id,
        } => {
            let artifact = reference(request, "artifact")?;
            let binding = reference(request, "binding")?;
            ensure!(
                &artifact.id == artifact_id
                    && binding.node_id == config.id
                    && binding.body["binding_id"] == *binding_id,
                "deployment references mismatch"
            );
            let configured = config.bindings.get(binding_id).context("unknown binding")?;
            checked_id(
                configured
                    .account_identity
                    .as_deref()
                    .context("canonical account identity required")?,
            )?;
            ensure!(
                (20..=86400).contains(&configured.max_duration_seconds)
                    && (1..=500).contains(&configured.max_price_deviation_bps),
                "invalid managed safety envelope"
            );
            let final_root = deploy_root(config, &request.operation_id)?;
            ensure!(
                !final_root.exists(),
                "deployment already exists without a receipt"
            );
            let staged = config
                .work_root
                .join("deployments")
                .join(format!(".{}", request.operation_id));
            std::fs::create_dir_all(&staged)?;
            let path = staged.join("strategy.titan");
            if artifact.node_id == config.id {
                let operation = artifact.body["operation_id"]
                    .as_str()
                    .context("artifact operation missing")?;
                checked_id(operation)?;
                std::fs::copy(
                    super::inside(
                        &config.work_root.join("operations").join(operation),
                        "strategy.titan",
                    )?,
                    &path,
                )?;
            } else {
                let source = config
                    .artifact_sources
                    .get(&artifact.node_id)
                    .context("artifact source is not approved on this node")?;
                let endpoint = reqwest::Url::parse(&source.endpoint)?;
                ensure!(
                    endpoint.scheme() == "https"
                        || matches!(endpoint.host_str(), Some("127.0.0.1" | "localhost")),
                    "artifact source requires TLS or authenticated tunnel"
                );
                let operation = artifact.body["operation_id"]
                    .as_str()
                    .context("artifact operation missing")?;
                checked_id(operation)?;
                let response = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()?
                    .get(format!(
                        "{}/v1/files/{operation}/strategy.titan",
                        source.endpoint.trim_end_matches('/')
                    ))
                    .bearer_auth(source.token()?)
                    .send()
                    .await?
                    .error_for_status()?;
                use futures_util::StreamExt;
                use tokio::io::AsyncWriteExt;
                let mut stream = response.bytes_stream();
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .await?;
                let mut length = 0;
                while let Some(bytes) = stream.next().await {
                    let bytes = bytes?;
                    length += bytes.len();
                    ensure!(
                        length <= 64 * 1024 * 1024 && !stop.load(Ordering::Acquire),
                        "deployment transfer cancelled or too large"
                    );
                    file.write_all(&bytes).await?;
                }
                file.sync_all().await?;
            }
            ensure!(
                node::digest_file(&path)? == artifact.body["blob_sha256"].as_str().unwrap_or(""),
                "artifact transfer digest mismatch"
            );
            let runtime_sha256 = node::digest_file(&configured.runtime_config)?;
            let mut body = binding.body.clone();
            body["artifact_id"] = artifact.id.clone().into();
            body["artifact_digest"] = artifact.body["artifact_digest"].clone();
            body["blob_sha256"] = artifact.body["blob_sha256"].clone();
            body["operation_id"] = request.operation_id.clone().into();
            body["account_identity"] = configured.account_identity.clone().into();
            let raw: crate::ApplicationConfig =
                toml::from_str(&std::fs::read_to_string(&configured.runtime_config)?)?;
            let definition = raw
                .strategies
                .iter()
                .find(|s| s.strategy_key.as_ref() == configured.strategy_key)
                .context("strategy binding missing")?;
            body["strategy_id"] = definition.strategy_id.0.to_string().into();
            let parameters = &artifact.body["normalized_parameters"];
            body["stop_action"] = parameters["stop_action"].clone();
            let deployment = Deployment {
                resource: node::resource(config, "deployment", &request.operation_id, true, body),
                runtime_sha256,
                binding_sha256: binding_hash(config, binding_id)?,
            };
            // Validation needs final URI, so inspect staged bytes with the target's trust policy first.
            let trust = trust_policy(&raw)?;
            let loader = titan_strategy_runtime::NativeArtifactLoaderV13::new(
                config.work_root.join("native-cache"),
                trust,
            );
            let loaded = loader.load(&path)?;
            ensure!(
                hex(&loaded.manifest.artifact_digest)
                    == artifact.body["artifact_digest"].as_str().unwrap_or(""),
                "artifact identity mismatch"
            );
            ensure!(
                matches!(loaded.manifest.cpu_baseline.as_ref(), "x86-64" | "generic"),
                "managed deployment requires portable CPU baseline"
            );
            ensure!(
                !stop.load(Ordering::Acquire),
                "deployment cancelled before publication"
            );
            node::atomic_json(&staged.join("deployment.json"), &deployment)?;
            std::fs::File::open(&path)?.sync_all()?;
            std::fs::rename(&staged, &final_root)?;
            std::fs::File::open(final_root.parent().context("deployment parent missing")?)?
                .sync_all()?;
            // Reject incompatible binding before exposing an executable deployment.
            let raw = runtime_config(config, &deployment, None)?;
            crate::ConfigurationAdapter::adapt(
                raw,
                configured
                    .runtime_config
                    .parent()
                    .context("runtime config parent missing")?,
            )?;
            Ok(json!({"resources":[deployment.resource]}))
        }
        Action::Start { deployment_id } => {
            let deployment = load_deployment(config, deployment_id)?;
            ensure!(
                reference(request, "deployment")?.body == deployment.resource.body,
                "deployment reference drifted"
            );
            launch(config, request, &deployment, None, stop).await
        }
        Action::Recover {
            generation,
            checkpoint_id,
            ..
        } => {
            let old = reference(request, "run")?;
            let worker = old.body["worker_id"]
                .as_str()
                .context("run has no worker")?;
            let previous: ManagedSpec = serde_json::from_slice(&std::fs::read(
                root(config, worker)?.join("managed.json"),
            )?)?;
            let record = node::registry(config)?
                .get(&format!("worker-{worker}"))?
                .context("old worker missing")?;
            ensure!(
                record
                    .pid
                    .zip(record.process_start_time)
                    .is_none_or(
                        |(pid, start)| crate::registry::process_start_time(pid) != Some(start)
                    ),
                "old worker still alive; stop before recovery"
            );
            ensure!(
                old.body["generation"] == *generation,
                "old generation changed"
            );
            let deployment = load_deployment(config, &previous.deployment_id)?;
            let raw = runtime_config(config, &deployment, Some(checkpoint_id))?;
            let checkpoint = checkpoint_path(&raw)?;
            ensure!(
                node::digest_file(&checkpoint)? == *checkpoint_id,
                "checkpoint reference changed"
            );
            let snapshot: titan_strategy_runtime::StrategyPrivateStateSnapshot =
                serde_json::from_slice(&std::fs::read(&checkpoint)?)?;
            ensure!(
                snapshot.generation.to_string() == *generation
                    && snapshot.strategy.strategy_id == raw.strategies[0].strategy_id,
                "checkpoint generation or strategy mismatch"
            );
            launch(
                config,
                request,
                &deployment,
                Some(RecoveryLaunch {
                    checkpoint: checkpoint_id.clone(),
                    generation: generation.clone(),
                    previous,
                }),
                stop,
            )
            .await
        }
        Action::Pause { generation, .. }
        | Action::Resume { generation, .. }
        | Action::Stop { generation, .. } => {
            let run = reference(request, "run")?;
            let worker = run.body["worker_id"]
                .as_str()
                .context("missing managed worker")?;
            let spec: ManagedSpec = serde_json::from_slice(&std::fs::read(
                root(config, worker)?.join("managed.json"),
            )?)?;
            let record = node::registry(config)?
                .get(&format!("worker-{worker}"))?
                .context("managed worker not found")?;
            let (pid, start) = record
                .pid
                .zip(record.process_start_time)
                .context("worker identity missing")?;
            ensure!(
                crate::registry::process_start_time(pid) == Some(start),
                "worker identity unavailable"
            );
            let action = match request.action {
                Action::Pause { .. } => "pause",
                Action::Resume { .. } => "resume",
                _ => "stop",
            };
            let deadline = Instant::now() + Duration::from_secs(25);
            loop {
                let receipt = query_host(
                    &spec.claim,
                    HostQuery::Control {
                        operation_id: request.operation_id.clone(),
                        input_digest: request.input_digest.clone(),
                        strategy_generation: generation.clone(),
                        action: action.into(),
                    },
                    pid,
                    start,
                )
                .await;
                match receipt {
                    Ok(receipt) if receipt["state"] == "succeeded" => {
                        return Ok(
                            json!({"runtime":receipt,"resources":catalog_one(config,&spec).await?}),
                        );
                    }
                    Ok(receipt) if receipt["state"] == "failed" => {
                        return Ok(json!({"business_success":false,"runtime":receipt}));
                    }
                    Ok(receipt) if receipt["state"] == "unknown" => {
                        return Ok(json!({"outcome_unknown":true,"runtime":receipt}));
                    }
                    Err(error) if Instant::now() >= deadline => {
                        return Ok(json!({"outcome_unknown":true,"detail":error.to_string()}));
                    }
                    _ => {}
                }
                if Instant::now() >= deadline {
                    return Ok(
                        json!({"outcome_unknown":true,"detail":"engine control remains pending"}),
                    );
                }
                // A control request cannot be undone by killing its adapter. Query its same receipt.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        _ => bail!("unsupported managed operation"),
    }
}
/// Reconcile an admitted control from E's existing receipt; never admit an engine action on GET.
pub(super) async fn reconcile_receipt(config: &NodeConfig, old: Receipt) -> Result<Receipt> {
    if old.state != OperationState::Unknown {
        return Ok(old);
    }
    let request: NodeRequest = serde_json::from_slice(&std::fs::read(
        config
            .work_root
            .join("operations")
            .join(&old.operation_id)
            .join("request.json"),
    )?)?;
    ensure!(
        request.operation_id == old.operation_id && request.input_digest == old.input_digest,
        "receipt identity changed"
    );
    if matches!(
        request.action,
        Action::Start { .. } | Action::Recover { .. }
    ) {
        let path = root(config, &old.operation_id)?.join("managed.json");
        let Some(spec) = std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice::<ManagedSpec>(&b).ok())
        else {
            return Ok(old);
        };
        ensure!(
            spec.claim.run_id == old.operation_id,
            "managed admission identity changed"
        );
        let resources = catalog_one(config, &spec).await?;
        if resources.iter().any(|r| {
            r.kind == "run" && r.body["available"] == true && r.body["lifecycle"] == "Running"
        }) {
            return Ok(Receipt {
                state: OperationState::Succeeded,
                result: Some(json!({"resources":resources})),
                error: None,
                ..old
            });
        }
        let record = node::registry(config)?
            .get(&format!("worker-{}", old.operation_id))?
            .context("managed worker missing")?;
        if record.finished_at_ns.is_some() {
            let absent = std::fs::read(root(config, &old.operation_id)?.join("final.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .is_some_and(|v| {
                    v["run_id"] == old.operation_id && v["start_admission_absent"] == true
                });
            return Ok(Receipt {
                state: OperationState::Failed,
                result: Some(
                    json!({"business_success":false,"start_admission_absent":absent,"resources":resources}),
                ),
                error: Some("runtime did not confirm running; preserved engine evidence".into()),
                ..old
            });
        }
        return Ok(old);
    }
    let (run_id, generation, action) = match &request.action {
        Action::Pause { run_id, generation } => (run_id, generation, "pause"),
        Action::Resume { run_id, generation } => (run_id, generation, "resume"),
        Action::Stop { run_id, generation } => (run_id, generation, "stop"),
        _ => return Ok(old),
    };
    let run = reference(&request, "run")?;
    ensure!(&run.id == run_id, "run reference changed");
    let worker = run.body["worker_id"]
        .as_str()
        .context("worker identity missing")?;
    let spec: ManagedSpec =
        serde_json::from_slice(&std::fs::read(root(config, worker)?.join("managed.json"))?)?;
    let record = node::registry(config)?
        .get(&format!("worker-{worker}"))?
        .context("worker missing")?;
    let receipt = if let Some((pid, start)) = record
        .pid
        .zip(record.process_start_time)
        .filter(|(pid, start)| crate::registry::process_start_time(*pid) == Some(*start))
    {
        match query_host(
            &spec.claim,
            HostQuery::ControlReceipt {
                operation_id: old.operation_id.clone(),
                input_digest: old.input_digest.clone(),
                strategy_generation: generation.clone(),
                action: action.into(),
            },
            pid,
            start,
        )
        .await
        {
            Ok(v) => v,
            Err(_) => return Ok(old),
        }
    } else {
        let path = spec
            .claim
            .socket
            .parent()
            .context("socket parent missing")?
            .join("controls")
            .join(format!("{}.json", old.operation_id));
        match std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        {
            Some(v) => v,
            None => return Ok(old),
        }
    };
    ensure!(
        receipt["input"]
            == json!({"id":old.operation_id,"digest":old.input_digest,"generation":generation,"action":action}),
        "engine control receipt identity changed"
    );
    let state = match receipt["state"].as_str() {
        Some("succeeded") => OperationState::Succeeded,
        Some("failed") => OperationState::Failed,
        _ => return Ok(old),
    };
    Ok(Receipt {
        result: Some(json!({"runtime":receipt,"resources":catalog_one(config,&spec).await?})),
        error: if state == OperationState::Failed {
            Some("engine control failed; inspect its receipt".into())
        } else {
            None
        },
        state,
        ..old
    })
}
fn trust_policy(raw: &crate::ApplicationConfig) -> Result<titan_strategy_runtime::V13TrustPolicy> {
    ensure!(
        !raw.strategy_service.allow_unsigned_artifacts
            && !raw.strategy_service.trusted_ed25519_keys.is_empty(),
        "signature trust required"
    );
    let keys = raw
        .strategy_service
        .trusted_ed25519_keys
        .iter()
        .map(|(k, v)| Ok((Arc::from(k.as_str()), decode_hash(v)?)))
        .collect::<Result<_>>()?;
    Ok(titan_strategy_runtime::V13TrustPolicy {
        require_signature: true,
        ed25519_keys: keys,
    })
}
fn checkpoint_path(raw: &crate::ApplicationConfig) -> Result<PathBuf> {
    Ok(raw
        .strategy_service
        .checkpoint_root
        .as_ref()
        .context("checkpoint root missing")?
        .join(format!("strategy-{}.json", raw.strategies[0].strategy_id.0)))
}
struct RecoveryLaunch {
    checkpoint: String,
    generation: String,
    previous: ManagedSpec,
}
async fn launch(
    config: &NodeConfig,
    request: &NodeRequest,
    deployment: &Deployment,
    recovery: Option<RecoveryLaunch>,
    stop: &AtomicBool,
) -> Result<Value> {
    let checkpoint = recovery.as_ref().map(|r| r.checkpoint.clone());
    let previous_generation = recovery.as_ref().map(|r| r.generation.clone());
    let reservation = recovery.as_ref().map_or_else(
        || request.operation_id.clone(),
        |r| r.previous.reservation_operation_id.clone(),
    );
    let prior_budget = recovery.as_ref().map(|r| r.previous.budget.clone());
    let raw = runtime_config(config, deployment, checkpoint.as_deref())?;
    let active = node::registry(config)?
        .list()?
        .iter()
        .filter(|r| {
            r.strategy_id == "management-runtime"
                && r.pid.zip(r.process_start_time).is_some_and(|(pid, start)| {
                    crate::registry::process_start_time(pid) == Some(start)
                })
        })
        .count();
    ensure!(active < 8, "managed worker capacity is occupied");
    let binding = config
        .bindings
        .get(
            deployment.resource.body["binding_id"]
                .as_str()
                .context("missing binding")?,
        )
        .context("binding disappeared")?;
    ensure!(
        !stop.load(Ordering::Acquire),
        "start cancelled before admission"
    );
    let id = &request.operation_id;
    let dir = root(config, id)?;
    std::fs::create_dir_all(&dir)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    let generation = nonce();
    let spec = ManagedSpec {
        claim: HostClaim {
            run_id: id.clone(),
            generation: generation.clone(),
            account: binding.account.clone(),
            instrument: binding.instrument.clone(),
            binding_id: deployment.resource.body["binding_id"]
                .as_str()
                .unwrap()
                .into(),
            token: nonce(),
            socket: host_socket(&config.work_root, id, &generation),
            readonly: false,
        },
        deployment_id: deployment.resource.id.clone(),
        deployment_sha256: digest(&serde_json::to_vec(deployment)?),
        checkpoint,
        previous_generation,
        predecessor_run_id: request.action.run().map(|(id, _)| id.to_owned()),
        reservation_operation_id: reservation,
        budget: match prior_budget {
            Some(budget) => budget,
            None => RunBudget::new(binding.max_duration_seconds)?,
        },
    };
    let namespace = config
        .work_root
        .join("checkpoints")
        .join(
            binding
                .account_identity
                .as_deref()
                .context("canonical account required")?,
        )
        .join(raw.strategies[0].strategy_id.0.to_string());
    std::fs::create_dir_all(&namespace)?;
    node::atomic_json(&dir.join("managed.json"), &spec)?;
    let token = nonce();
    let worker_id = format!("worker-{id}");
    node::registry(config)?.create(NewRun {
        id: &worker_id,
        owner_token: &token,
        strategy_id: "management-runtime",
        strategy_version: "1",
        environment: "live",
        event_mode: "tick",
        spec_path: &dir.join("managed.json"),
        config_path: &config.work_root.join("node-config.ref"),
        config_sha256: &spec.deployment_sha256,
        result_path: &dir.join("final.json"),
        log_path: &dir.join("worker.log"),
    })?;
    // Config path is saved by serve_node, never supplied by the browser.
    let config_path = std::fs::read_to_string(config.work_root.join("node-config.ref"))?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("worker.log"))?;
    let mut command = std::process::Command::new(&config.titan_program);
    command
        .args([
            "console",
            "managed-worker",
            "--config",
            config_path.trim(),
            "--run",
            id,
            "--owner-token",
            &token,
        ])
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    use std::os::unix::process::CommandExt;
    command.process_group(0);
    let mut child = command.spawn()?;
    node::registry(config)?.spawned(&worker_id, &token, child.id())?;
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if stop.load(Ordering::Acquire) {
            let record = node::registry(config)?
                .get(&worker_id)?
                .context("worker registry missing")?;
            if let Some((pid, start)) = record.pid.zip(record.process_start_time)
                && crate::registry::process_start_time(pid) == Some(start)
            {
                unsafe {
                    libc::kill(pid as i32, libc::SIGTERM);
                }
            }
        }
        let resources = catalog_one(config, &spec).await?;
        if resources
            .iter()
            .any(|r| r.kind == "run" && r.body["lifecycle"] == "Running")
            && !stop.load(Ordering::Acquire)
        {
            return Ok(json!({"resources":resources}));
        }
        let record = node::registry(config)?
            .get(&worker_id)?
            .context("worker registry missing")?;
        if record.finished_at_ns.is_some() {
            let absent = std::fs::read(dir.join("final.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .is_some_and(|v| v["start_admission_absent"] == true && v["run_id"] == *id);
            return Ok(
                json!({"business_success":false,"start_admission_absent":absent,"outcome_unknown":stop.load(Ordering::Acquire) && record.state!="STOPPED" && !absent,"detail":record.error,"resources":resources}),
            );
        }
        if Instant::now() >= deadline {
            return Ok(
                json!({"outcome_unknown":true,"detail":"worker startup not confirmed","resources":resources}),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
pub(super) async fn catalog(config: &NodeConfig) -> Result<Vec<Resource>> {
    let dir = config.work_root.join("managed");
    if !dir.exists() {
        return Ok(vec![]);
    };
    let mut specs = vec![];
    let entries = std::fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    ensure!(entries.len() <= 1024, "managed catalog capacity exceeded");
    for entry in entries {
        let path = entry.path().join("managed.json");
        if path.is_file() {
            let spec: ManagedSpec = serde_json::from_slice(&std::fs::read(path)?)?;
            specs.push(spec);
        }
    }
    use futures_util::{StreamExt, stream};
    let mut tasks = stream::iter(
        specs
            .clone()
            .into_iter()
            .map(|spec| async move { catalog_one(config, &spec).await }),
    )
    .buffer_unordered(8);
    let mut out: Vec<Resource> = vec![];
    while let Some(resources) = tasks.next().await {
        out.extend(resources?);
    }
    let mut latest = std::collections::BTreeMap::<String, Resource>::new();
    for run in &out {
        let binding = run.body["binding_id"]
            .as_str()
            .context("run binding missing")?
            .to_owned();
        let at = run.body["source_started_at_ns"]
            .as_str()
            .context("run source time missing")?
            .parse::<i64>()?;
        if latest.get(&binding).is_none_or(|old| {
            old.body["source_started_at_ns"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .is_none_or(|old_at| at > old_at)
        }) {
            latest.insert(binding, run.clone());
        }
    }
    for (binding, run) in latest {
        let spec = specs
            .iter()
            .find(|s| run.body["worker_id"] == s.claim.run_id)
            .context("account source missing")?;
        out.extend(super::live::financial_alerts(
            config,
            &spec.claim,
            run.body["generation"].as_str().unwrap_or("unconfirmed"),
            &run.body["observation"],
        ));
        out.push(node::resource(config, "account", &binding, false, run.body));
    }
    Ok(out)
}
async fn catalog_one(config: &NodeConfig, spec: &ManagedSpec) -> Result<Vec<Resource>> {
    let record = node::registry(config)?
        .get(&format!("worker-{}", spec.claim.run_id))?
        .context("managed registry missing")?;
    let process = record
        .pid
        .zip(record.process_start_time)
        .filter(|(pid, start)| crate::registry::process_start_time(*pid) == Some(*start));
    let snapshot = if let Some((pid, start)) = process {
        query_host(&spec.claim, HostQuery::Snapshot, pid, start)
            .await
            .ok()
    } else {
        None
    };
    let available = snapshot.as_ref().is_some_and(|s| {
        s["available"] == true
            && s["captured_at_ns"]
                .as_str()
                .and_then(|s| s.parse::<u128>().ok())
                .zip(super::now_ns().parse::<u128>().ok())
                .is_some_and(|(at, now)| now.checked_sub(at).is_some_and(|d| d <= 5_000_000_000))
    });
    let snapshot = match snapshot {
        Some(s) => Some(s),
        None => std::fs::read(root(config, &spec.claim.run_id)?.join("final.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok()),
    };
    let observation = snapshot.unwrap_or(Value::Null);
    let strategy = &observation["strategies"][0];
    let identity: Value =
        std::fs::read(root(config, &spec.claim.run_id)?.join("runtime-identity.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
    let generation = strategy["generation"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| identity["generation"].as_str().map(str::to_owned))
        .unwrap_or_else(|| "unconfirmed".into());
    let lifecycle = strategy["lifecycle"].as_str().unwrap_or("Unknown");
    let mut actions = vec![];
    if available && config.live_control {
        match lifecycle {
            "Running" => {
                if strategy["pause_supported"] == true {
                    actions.push("pause");
                }
                actions.push("stop");
            }
            "Paused" => actions.extend(["resume", "stop"]),
            "Stopping" => actions.push("stop"),
            _ => {}
        }
    }
    let deployment = load_deployment(config, &spec.deployment_id)?;
    let checkpoint = config
        .work_root
        .join("checkpoints")
        .join(
            deployment.resource.body["account_identity"]
                .as_str()
                .context("canonical account missing")?,
        )
        .join(
            deployment.resource.body["strategy_id"]
                .as_str()
                .context("strategy identity missing")?,
        )
        .join(format!(
            "strategy-{}.json",
            deployment.resource.body["strategy_id"].as_str().unwrap()
        ));
    let checkpoint_generation = std::fs::read(&checkpoint)
        .ok()
        .and_then(|b| {
            serde_json::from_slice::<titan_strategy_runtime::StrategyPrivateStateSnapshot>(&b).ok()
        })
        .map(|s| s.generation.to_string());
    let checkpoint_id =
        if process.is_none() && checkpoint_generation.as_deref() == Some(generation.as_str()) {
            Some(node::digest_file(&checkpoint)?)
        } else {
            None
        };
    if process.is_none()
        && checkpoint_id.is_some()
        && config.live_control
        && spec
            .budget
            .remaining()
            .is_ok_and(|remaining| remaining > 15_000_000_000)
    {
        actions.push("recover");
    }
    let safe = record.state == "STOPPED" && observation["release_proof"]["safe"] == true;
    let body = json!({"account":spec.claim.account,"account_identity":deployment.resource.body["account_identity"],"strategy_id":deployment.resource.body["strategy_id"],"instrument":spec.claim.instrument,"binding_id":spec.claim.binding_id,"source_started_at_ns":record.started_at_ns.to_string(),"worker_id":spec.claim.run_id,"generation":generation,"deployment_id":spec.deployment_id,"reservation_operation_id":spec.reservation_operation_id,"predecessor_run_id":spec.predecessor_run_id,"admission_operation_id":spec.claim.run_id,"environment":"live","mode":"trade","process_state":if process.is_some() || record.finished_at_ns.is_some(){record.state.as_str()}else{"UNKNOWN"},"lifecycle":lifecycle,"available":available,"observation_stale":!available,"observation":observation,"allowed_actions":actions,"checkpoint_id":checkpoint_id,"stop_action":deployment.resource.body["stop_action"],"release_proof":{"safe":safe,"source":"trading_runtime","worker_id":spec.claim.run_id},"expires_at_ns":spec.budget.expires_at_ns});
    Ok(vec![node::resource(
        config,
        "run",
        &spec.claim.run_id,
        false,
        body,
    )])
}

pub(super) fn managed_worker(config: NodeConfig, id: String, token: String) -> Result<()> {
    let dir = root(&config, &id)?;
    let spec: ManagedSpec = serde_json::from_slice(&std::fs::read(dir.join("managed.json"))?)?;
    ensure!(
        !spec.claim.readonly
            && spec.claim.run_id == id
            && spec.claim.socket == host_socket(&config.work_root, &id, &spec.claim.generation),
        "invalid managed owner claim"
    );
    let worker_id = format!("worker-{id}");
    let reg = node::registry(&config)?;
    ensure!(
        reg.running(&worker_id, &token, std::process::id())?,
        "managed owner mismatch"
    );
    let stopped = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, stopped.clone())?;
    }
    let activation_attempted = AtomicBool::new(false);
    let result = (|| {
        let deployment = load_deployment(&config, &spec.deployment_id)?;
        ensure!(
            digest(&serde_json::to_vec(&deployment)?) == spec.deployment_sha256,
            "deployment changed after admission"
        );
        let raw = runtime_config(&config, &deployment, spec.checkpoint.as_deref())?;
        let checkpoint = checkpoint_path(&raw)?;
        // A single host owns the checkpoint namespace, including its generation watermark.
        use std::os::fd::AsRawFd;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(
                checkpoint
                    .parent()
                    .context("checkpoint parent missing")?
                    .join("owner.lock"),
            )?;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "strategy namespace is already owned"
        );
        let remaining = spec.budget.remaining()?;
        ensure!(
            remaining > 15_000_000_000,
            "controlled runtime budget exhausted"
        );
        let binding = &config.bindings[&spec.claim.binding_id];
        let adapted = crate::ConfigurationAdapter::adapt(
            raw,
            binding
                .runtime_config
                .parent()
                .context("runtime config parent missing")?,
        )?;
        let runtime = crate::TradingRuntime::start_managed(
            adapted,
            config.work_root.join("account-facts.sqlite3"),
            format!("{}:{}", id, spec.claim.generation),
        )?;
        let mut runtime = ManagedRuntime {
            runtime,
            closed: false,
        };
        let service = runtime
            .strategy_service()
            .context("strategy service missing")?
            .clone();
        if spec.checkpoint.is_none() {
            let account = runtime.account_service().resolve(&binding.account)?;
            let image = runtime.account_service().recovery_snapshot(account)?;
            ensure!(
                image.positions.items.iter().all(|p| p.quantity_lots == 0)
                    && image
                        .orders
                        .items
                        .iter()
                        .all(|o| matches!(o.status, 2 | 3 | 4 | 6)),
                "fresh start requires a proven flat account/instrument without unknown orders"
            );
        }
        let startup = Instant::now() + Duration::from_secs(15);
        while runtime.check_dependency_health().is_err() && Instant::now() < startup {
            std::thread::sleep(Duration::from_millis(100));
        }
        runtime.check_dependency_health()?;
        ensure!(
            !stopped.load(Ordering::Acquire) && spec.budget.remaining()? > 12_000_000_000,
            "start cancelled or runtime budget exhausted"
        );
        let handle = service.list()[0].handle;
        node::atomic_json(
            &dir.join("runtime-identity.json"),
            &json!({"strategy_id":handle.strategy_id.0.to_string(),"generation":handle.generation.to_string()}),
        )?;
        let prepare = service.prepare(handle)?;
        crate::core_runtime::wait_strategy_operation(
            service.as_ref(),
            prepare,
            Duration::from_secs(10),
        )?;
        activation_attempted.store(true, Ordering::Release);
        let start = service.start(handle)?;
        crate::core_runtime::wait_strategy_operation(
            service.as_ref(),
            start,
            Duration::from_secs(10),
        )?;
        let mut host = WorkerHost::start(
            spec.claim.clone(),
            runtime.account_service().clone(),
            Some(service.clone()),
            runtime.observation().context("journal missing")?.clone(),
        )?;
        reg.transition(&worker_id, &token, "READY")?;
        reg.transition(&worker_id, &token, "RUNNING")?;
        let mut heartbeat_at = Instant::now();
        let mut stopping = false;
        let mut stop_deadline = None;
        let mut failure = None;
        loop {
            if heartbeat_at.elapsed() >= Duration::from_millis(500) {
                reg.heartbeat(&worker_id, &token)?;
                heartbeat_at = Instant::now();
            }
            let instances = service.list();
            ensure!(instances.len() == 1, "managed instance disappeared");
            let strategy = &instances[0];
            if matches!(
                strategy.lifecycle,
                titan_strategy_runtime::StrategyLifecycle::Stopped
            ) {
                break;
            }
            if matches!(
                strategy.lifecycle,
                titan_strategy_runtime::StrategyLifecycle::Failed
            ) {
                failure = Some("strategy failed; recovery evidence retained".to_owned());
                break;
            }
            if !stopping
                && (stopped.load(Ordering::Acquire)
                    || spec.budget.remaining().is_err()
                    || spec
                        .budget
                        .remaining()
                        .is_ok_and(|remaining| remaining <= 12_000_000_000)
                    || runtime.check_dependency_health().is_err())
            {
                stopping = true;
                let deadline = Instant::now() + Duration::from_secs(10);
                stop_deadline = Some(deadline);
                service.stop(strategy.handle, deadline)?;
                reg.transition(&worker_id, &token, "STOP_REQUESTED")?;
            }
            if stop_deadline.is_some_and(|d| Instant::now() >= d) {
                failure = Some("managed stop deadline exceeded".into());
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        // Capture E's terminal strategy/account facts before tearing down the account connection.
        std::thread::sleep(Duration::from_millis(600));
        let snapshot = host.finish();
        node::atomic_json(&dir.join("final.json"), &snapshot)?;
        let shutdown = runtime.close();
        shutdown?;
        if let Some(error) = failure {
            bail!("{error}");
        }
        Ok::<_, anyhow::Error>(())
    })();
    if result.is_err() && !activation_attempted.load(Ordering::Acquire) {
        node::atomic_json(
            &dir.join("final.json"),
            &json!({"available":false,"source":"runtime_owner","run_id":id,"start_admission_absent":true,"reason":"strategy activation was never attempted"}),
        )?;
    }
    reg.finish(
        &worker_id,
        &token,
        if result.is_ok() { "STOPPED" } else { "FAILED" },
        if result.is_ok() { 0 } else { 1 },
        result.as_ref().err().map(ToString::to_string).as_deref(),
    )?;
    result
}

struct ManagedRuntime {
    runtime: crate::TradingRuntime,
    closed: bool,
}
impl std::ops::Deref for ManagedRuntime {
    type Target = crate::TradingRuntime;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}
impl ManagedRuntime {
    fn close(&mut self) -> Result<()> {
        let result = self.runtime.shutdown();
        self.closed = true;
        Ok(result?)
    }
}
impl Drop for ManagedRuntime {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.runtime.shutdown();
        }
    }
}

pub(super) fn is_managed(config: &NodeConfig, id: &str) -> bool {
    root(config, id).is_ok_and(|dir| dir.join("managed.json").is_file())
}
pub(super) fn read_log(config: &NodeConfig, id: &str, offset: usize) -> Result<Value> {
    use std::io::{Read, Seek, SeekFrom};
    let dir = root(config, id)?;
    let mut file = std::fs::File::open(super::inside(&dir, "worker.log")?)?;
    let size = file.metadata()?.len();
    let offset = (offset as u64).min(size);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![];
    file.take(65_536).read_to_end(&mut bytes)?;
    Ok(
        json!({"worker_id":id,"offset":offset.to_string(),"next_offset":(offset+bytes.len() as u64).to_string(),"size":size.to_string(),"more":offset+ (bytes.len() as u64)<size,"text":String::from_utf8_lossy(&bytes)}),
    )
}
pub(super) async fn history(
    config: &NodeConfig,
    id: &str,
    kind: Option<String>,
    offset: usize,
    limit: usize,
) -> Result<Value> {
    let spec: ManagedSpec =
        serde_json::from_slice(&std::fs::read(root(config, id)?.join("managed.json"))?)?;
    let record = node::registry(config)?
        .get(&format!("worker-{id}"))?
        .context("managed worker missing")?;
    if let Some((pid, start)) = record
        .pid
        .zip(record.process_start_time)
        .filter(|(pid, start)| crate::registry::process_start_time(*pid) == Some(*start))
    {
        return query_host(
            &spec.claim,
            HostQuery::History {
                kind,
                offset,
                limit,
            },
            pid,
            start,
        )
        .await;
    }
    let items = titan_account_service::AccountFactJournal::read_history(
        &config.work_root.join("account-facts.sqlite3"),
        &spec.claim.account,
        kind.as_deref(),
        offset,
        limit,
    )
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(json!({"items":items,"source":{"worker_id":id,"scope":"account","persisted":true}}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_keeps_monotonic_deadline_and_rejects_changed_boot_or_exhausted_budget() {
        let budget = RunBudget::new(120).unwrap();
        let restored = budget.clone();
        assert_eq!(budget.deadline_boottime_ns, restored.deadline_boottime_ns);
        assert!(restored.remaining().unwrap() <= 120_000_000_000);
        let mut wrong = restored.clone();
        wrong.boot_id = "another-boot".into();
        assert!(wrong.remaining().is_err());
        wrong = restored;
        wrong.deadline_boottime_ns = "0".into();
        assert!(wrong.remaining().is_err());
    }
}
