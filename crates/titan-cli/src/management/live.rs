//! N forwards worker observations. Account connections and financial facts remain inside E.
use super::{checked_id, config::NodeConfig, contract::Resource, digest, node, nonce};
use crate::{
    registry::NewRun,
    runtime_host::{HostClaim, HostQuery, WorkerHost, host_socket, query_host},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
struct ObserverSpec {
    claim: HostClaim,
    runtime_sha256: String,
    binding_fingerprint: String,
}
pub(crate) struct LiveNode {
    config: NodeConfig,
    specs: Vec<ObserverSpec>,
    last: Mutex<BTreeMap<String, Value>>,
}
fn directory(config: &NodeConfig, id: &str) -> Result<PathBuf> {
    checked_id(id)?;
    Ok(config.work_root.join("observations").join(id))
}
fn fingerprint(config: &NodeConfig, binding_id: &str) -> Result<(String, String)> {
    let b = config
        .bindings
        .get(binding_id)
        .context("unknown observation binding")?;
    let hash = digest(&std::fs::read(&b.runtime_config)?);
    Ok((
        digest(&serde_json::to_vec(
            &json!({"binding_id":binding_id,"account":b.account,"instrument":b.instrument,"runtime_config_sha256":hash,"max_duration_seconds":b.max_duration_seconds,"mode":"observe"}),
        )?),
        hash,
    ))
}
impl LiveNode {
    pub fn read_log(&self, id: &str, offset: usize) -> Result<Value> {
        use std::io::{Read, Seek, SeekFrom};
        let root = directory(&self.config, id)?;
        let record = node::registry(&self.config)?
            .get(id)?
            .context("unknown observation worker")?;
        ensure!(
            record.strategy_id == "management-observer",
            "unsupported observation log source"
        );
        let path = super::inside(&root, "worker.log")?;
        let mut file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        let offset = (offset as u64).min(size);
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::new();
        file.take(65_536).read_to_end(&mut bytes)?;
        Ok(
            json!({"worker_id":id,"offset":offset.to_string(),"next_offset":(offset+bytes.len() as u64).to_string(),"size":size.to_string(),"more":offset+(bytes.len() as u64)<size,"text":String::from_utf8_lossy(&bytes)}),
        )
    }
    pub fn open(config: &NodeConfig, config_path: &Path) -> Result<Self> {
        ensure!(
            config.observe_bindings.len() <= 8,
            "observation binding capacity exceeded"
        );
        ensure!(
            config.observe_bindings.is_empty() || config.environments.iter().any(|e| e == "live"),
            "live observation capability is not enabled"
        );
        std::fs::create_dir_all(config.work_root.join("observations"))?;
        let mut specs = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for binding_id in &config.observe_bindings {
            checked_id(binding_id)?;
            ensure!(seen.insert(binding_id), "duplicate observation binding");
            let binding = config
                .bindings
                .get(binding_id)
                .context("unknown observation binding")?;
            ensure!(
                (1..=3600).contains(&binding.max_duration_seconds),
                "invalid observation duration"
            );
            let (plan, runtime_sha256) = fingerprint(config, binding_id)?;
            let mut existing = None;
            for record in node::registry(config)?.list()? {
                if record.strategy_id != "management-observer" || record.finished_at_ns.is_some() {
                    continue;
                }
                let path = directory(config, &record.id)?.join("observe.json");
                if !path.is_file() {
                    continue;
                }
                let spec: ObserverSpec = serde_json::from_slice(&std::fs::read(path)?)?;
                if spec.claim.binding_id == *binding_id {
                    ensure!(
                        spec.binding_fingerprint == plan,
                        "active observer configuration changed; explicit owner shutdown is required"
                    );
                    ensure!(
                        record
                            .pid
                            .zip(record.process_start_time)
                            .is_some_and(|(pid, start)| crate::registry::process_start_time(pid)
                                == Some(start)),
                        "observer identity cannot be proven; automatic adoption refused"
                    );
                    existing = Some(spec);
                    break;
                }
            }
            if let Some(spec) = existing {
                specs.push(spec);
                continue;
            }
            let id = nonce();
            let generation = nonce();
            let dir = directory(config, &id)?;
            std::fs::create_dir(&dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
            }
            let spec = ObserverSpec {
                claim: HostClaim {
                    run_id: id.clone(),
                    worker_session_id: generation.clone(),
                    account: binding.account.clone(),
                    instrument: binding.instrument.clone(),
                    binding_id: binding_id.clone(),
                    token: nonce(),
                    socket: host_socket(&config.work_root, &id, &generation),
                    readonly: true,
                },
                runtime_sha256,
                binding_fingerprint: plan.clone(),
            };
            node::atomic_json(&dir.join("observe.json"), &spec)?;
            let token = nonce();
            node::registry(config)?.create(NewRun {
                id: &id,
                owner_token: &token,
                strategy_id: "management-observer",
                strategy_version: "1",
                environment: "live",
                event_mode: "observe",
                spec_path: &dir.join("observe.json"),
                config_path,
                config_sha256: &plan,
                result_path: &dir.join("final.json"),
                log_path: &dir.join("worker.log"),
            })?;
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("worker.log"))?;
            let mut command = std::process::Command::new(&config.titan_program);
            command
                .args(["console", "observe-worker", "--config"])
                .arg(config_path)
                .args(["--run", &id, "--owner-token", &token])
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.process_group(0);
            }
            let child = command.spawn()?;
            node::registry(config)?.spawned(&id, &token, child.id())?;
            std::thread::spawn(move || {
                let mut child = child;
                let _ = child.wait();
            });
            specs.push(spec);
        }
        Ok(Self {
            config: config.clone(),
            specs,
            last: Mutex::new(BTreeMap::new()),
        })
    }
    pub async fn catalog(&self) -> Result<Vec<Resource>> {
        use futures_util::{StreamExt, stream};
        let mut tasks = stream::iter(
            self.specs
                .clone()
                .into_iter()
                .map(|spec| async move { self.snapshot(&spec).await }),
        )
        .buffer_unordered(8);
        let mut resources = Vec::new();
        while let Some(result) = tasks.next().await {
            resources.extend(result?);
        }
        Ok(resources)
    }
    async fn snapshot(&self, spec: &ObserverSpec) -> Result<Vec<Resource>> {
        let record = node::registry(&self.config)?
            .get(&spec.claim.run_id)?
            .context("observation registry record missing")?;
        let running = record
            .pid
            .zip(record.process_start_time)
            .filter(|(pid, start)| crate::registry::process_start_time(*pid) == Some(*start));
        let observed = if let Some((pid, start)) = running {
            query_host(&spec.claim, HostQuery::Snapshot, pid, start).await
        } else {
            Err(anyhow::anyhow!("worker process identity is unavailable"))
        };
        let (snapshot, available, reason) = match observed {
            Ok(snapshot) => {
                let fresh = snapshot["captured_at_ns"]
                    .as_str()
                    .and_then(|s| s.parse::<u128>().ok())
                    .zip(super::now_ns().parse::<u128>().ok())
                    .is_some_and(|(captured, now)| {
                        now.checked_sub(captured)
                            .is_some_and(|age| age <= 5_000_000_000)
                    });
                let available = snapshot["available"] == true && fresh;
                let reason = if available {
                    None
                } else {
                    Some(
                        snapshot["reason"]
                            .as_str()
                            .unwrap_or(
                                "worker snapshot is unavailable, expired or has an invalid clock",
                            )
                            .to_owned(),
                    )
                };
                self.last
                    .lock()
                    .map_err(|_| anyhow::anyhow!("observation cache unavailable"))?
                    .insert(spec.claim.run_id.clone(), snapshot.clone());
                (Some(snapshot), available, reason)
            }
            Err(error) => {
                let old = self
                    .last
                    .lock()
                    .map_err(|_| anyhow::anyhow!("observation cache unavailable"))?
                    .get(&spec.claim.run_id)
                    .cloned();
                let final_path = directory(&self.config, &spec.claim.run_id)?.join("final.json");
                let persisted = if record.finished_at_ns.is_some() && final_path.is_file() {
                    Some(serde_json::from_slice(&std::fs::read(final_path)?)?)
                } else {
                    None
                };
                (
                    persisted.or(old),
                    false,
                    Some(record.error.clone().unwrap_or_else(|| error.to_string())),
                )
            }
        };
        let process_state =
            if record.finished_at_ns.is_none() && record.pid.is_some() && running.is_none() {
                "UNKNOWN"
            } else {
                record.state.as_str()
            };
        let body = json!({"account":spec.claim.account,"instrument":spec.claim.instrument,"binding_id":spec.claim.binding_id,"worker_id":spec.claim.run_id,"worker_session_id":spec.claim.worker_session_id,"environment":"live","mode":"observe","process_state":process_state,"available":available,"observation_stale":!available,"reason":reason,"observation":snapshot,"allowed_actions":[]});
        let mut resources = vec![
            node::resource(&self.config, "run", &spec.claim.run_id, false, body.clone()),
            node::resource(&self.config, "account", &spec.claim.binding_id, false, body),
        ];
        if !available {
            resources.push(node::resource(&self.config,"alert",&format!("observer-{}",spec.claim.binding_id),false,json!({"account":spec.claim.account,"environment":"live","source":"node_worker_identity","code":"OBSERVATION_UNAVAILABLE","worker_id":spec.claim.run_id,"worker_session_id":spec.claim.worker_session_id,"condition_identity":format!("{}:unavailable",spec.claim.worker_session_id),"message":reason,"active":true})));
        } else {
            resources.push(node::resource(&self.config,"alert",&format!("observer-{}",spec.claim.binding_id),false,json!({"account":spec.claim.account,"environment":"live","source":"node_worker_identity","code":"OBSERVATION_UNAVAILABLE","condition_identity":format!("{}:unavailable",spec.claim.worker_session_id),"active":false})));
        }
        if let Some(snapshot) = snapshot.as_ref() {
            resources.extend(financial_alerts(
                &self.config,
                &spec.claim,
                &spec.claim.worker_session_id,
                snapshot,
            ));
        }
        Ok(resources)
    }
    pub async fn history(
        &self,
        id: &str,
        kind: Option<String>,
        offset: usize,
        limit: usize,
    ) -> Result<Value> {
        checked_id(id)?;
        let stored: ObserverSpec = serde_json::from_slice(&std::fs::read(
            directory(&self.config, id)?.join("observe.json"),
        )?)?;
        let spec = &stored;
        ensure!(
            spec.claim.run_id == id,
            "observation source identity mismatch"
        );
        let record = node::registry(&self.config)?
            .get(id)?
            .context("observation registry record missing")?;
        ensure!(
            record.strategy_id == "management-observer",
            "unsupported worker source"
        );
        let process = record
            .pid
            .zip(record.process_start_time)
            .filter(|(pid, start)| crate::registry::process_start_time(*pid) == Some(*start));
        let Some((pid, start)) = process else {
            let items = titan_account_service::AccountFactJournal::read_history(
                &self.config.work_root.join("account-facts.sqlite3"),
                &spec.claim.account,
                kind.as_deref(),
                offset,
                limit,
            )
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            return Ok(
                json!({"items":items,"recording":{"active":false,"history_complete":false},"source":{"worker_id":id,"worker_session_id":spec.claim.worker_session_id,"scope":"account","persisted":true}}),
            );
        };
        let mut result = query_host(
            &spec.claim,
            HostQuery::History {
                kind,
                offset,
                limit,
            },
            pid,
            start,
        )
        .await?;
        result["source"] = json!({"worker_id":id,"worker_session_id":spec.claim.worker_session_id,"pid":pid.to_string(),"start_time":start.to_string(),"scope":"account","persisted":false});
        Ok(result)
    }
}
pub(crate) fn observe_worker(config: NodeConfig, id: String, token: String) -> Result<()> {
    let dir = directory(&config, &id)?;
    let spec: ObserverSpec = serde_json::from_slice(&std::fs::read(dir.join("observe.json"))?)?;
    ensure!(
        spec.claim.readonly
            && spec.claim.run_id == id
            && spec.claim.socket
                == host_socket(&config.work_root, &id, &spec.claim.worker_session_id),
        "invalid observation claim"
    );
    let reg = node::registry(&config)?;
    ensure!(
        reg.running(&id, &token, std::process::id())?,
        "observer owner changed"
    );
    let stopped = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, stopped.clone())?;
    }
    let h = config.clone();
    let hi = id.clone();
    let ht = token.clone();
    let hs = stopped.clone();
    let heartbeat = std::thread::spawn(move || {
        while !hs.load(Ordering::Acquire) {
            let _ = node::registry(&h).and_then(|r| Ok(r.heartbeat(&hi, &ht)?));
            std::thread::sleep(Duration::from_millis(500));
        }
    });
    let result = (|| {
        let binding = config
            .bindings
            .get(&spec.claim.binding_id)
            .context("observation binding disappeared")?;
        let deadline = Instant::now() + Duration::from_secs(binding.max_duration_seconds);
        let (plan, hash) = fingerprint(&config, &spec.claim.binding_id)?;
        ensure!(
            plan == spec.binding_fingerprint && hash == spec.runtime_sha256,
            "observation configuration drifted after admission"
        );
        let contents = std::fs::read_to_string(&binding.runtime_config)?;
        ensure!(
            digest(contents.as_bytes()) == spec.runtime_sha256,
            "runtime configuration changed"
        );
        let mut raw: crate::ApplicationConfig = toml::from_str(&contents)?;
        raw.managed_by_console = false;
        raw.strategies.clear();
        raw.market_sources.retain(|source| {
            source
                .instruments
                .iter()
                .any(|i| i.native_symbol.as_ref() == binding.instrument)
        });
        raw.accounts
            .retain(|a| a.account_key.as_ref() == binding.account);
        ensure!(
            raw.accounts.len() == 1 && raw.accounts[0].enabled,
            "selected observation account is unavailable or disabled"
        );
        ensure!(
            raw.accounts[0]
                .instruments
                .iter()
                .any(|i| i.native_symbol.as_ref() == binding.instrument),
            "observation instrument is not bound to the account"
        );
        raw.accounts[0].shutdown_order_policy =
            titan_account_service::ShutdownOrderPolicy::LeaveOpen;
        raw.accounts[0].ownership = titan_account_service::OrderOwnershipPolicy::ObserveAll;
        let adapted = crate::ConfigurationAdapter::adapt(
            raw,
            binding
                .runtime_config
                .parent()
                .context("runtime configuration has no parent")?,
        )?;
        let mut runtime = crate::TradingRuntime::start_observed(
            adapted,
            config.work_root.join("account-facts.sqlite3"),
            format!("{}:{}", id, spec.claim.worker_session_id),
        )?;
        let mut host = WorkerHost::start(
            spec.claim.clone(),
            runtime.account_service().clone(),
            None,
            runtime
                .observation()
                .context("recorder not initialized")?
                .clone(),
        )?;
        reg.transition(&id, &token, "READY")?;
        reg.transition(&id, &token, "RUNNING")?;
        let mut failure = None;
        while !stopped.load(Ordering::Acquire) && Instant::now() < deadline {
            if let Err(error) = runtime.check_health() {
                failure = Some(error.to_string());
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        reg.transition(&id, &token, "STOP_REQUESTED")?;
        let snapshot = host.finish();
        runtime.shutdown()?;
        node::atomic_json(&dir.join("final.json"), &snapshot)?;
        if let Some(error) = failure {
            anyhow::bail!("observation dependency failed: {error}");
        }
        Ok::<_, anyhow::Error>(())
    })();
    stopped.store(true, Ordering::Release);
    let _ = heartbeat.join();
    reg.finish(
        &id,
        &token,
        if result.is_ok() { "STOPPED" } else { "FAILED" },
        if result.is_ok() { 0 } else { 1 },
        result.as_ref().err().map(ToString::to_string).as_deref(),
    )?;
    result
}

pub(super) fn financial_alerts(
    config: &NodeConfig,
    claim: &HostClaim,
    generation: &str,
    snapshot: &Value,
) -> Vec<Resource> {
    let mut resources = Vec::new();
    if let Some(diagnostics) = snapshot["financial"]["diagnostics"].as_array() {
        for diagnostic in diagnostics {
            let Some(code) = diagnostic["code"].as_str() else {
                continue;
            };
            let key = digest(code.as_bytes());
            resources.push(node::resource(config,"alert",&format!("{}-{}",claim.binding_id,&key[..16]),false,json!({"account":claim.account,"environment":"live","source":"account_fact_journal","code":code,"condition_identity":format!("{generation}:{code}"),"active":diagnostic["active"].as_bool().unwrap_or(true),"message":diagnostic["detail"],"worker_id":claim.run_id,"run_generation":generation})));
        }
    }
    resources
}
