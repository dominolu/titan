use super::{
    config::ManagerConfig,
    contract::*,
    digest, fault, nonce, now_ns,
    store::{Store, account_allowed, allowed},
};
use anyhow::{Context, Result, ensure};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) struct App {
    pub config: ManagerConfig,
    pub store: Store,
    pub client: reqwest::Client,
    attempts: Mutex<HashMap<String, (u64, u32)>>,
}
impl App {
    pub fn open(config: ManagerConfig) -> Result<Arc<Self>> {
        ensure!(config.nodes.len() <= 32, "node capacity exceeded");
        for id in config.nodes.keys() {
            super::checked_id(id)?;
        }
        ensure!(
            config.session_seconds > 0 && config.session_seconds <= 86400,
            "invalid session duration"
        );
        ensure!(
            !config.users.is_empty(),
            "at least one configured user is required"
        );
        ensure!(
            config.public_origin.starts_with("https://")
                || (!config.secure_cookie && config.public_origin.starts_with("http://127.0.0.1:")),
            "HTTP is only allowed for loopback development"
        );
        for user in &config.users {
            PasswordHash::new(&user.password_hash)
                .map_err(|_| anyhow::anyhow!("invalid configured password hash"))?;
        }
        for node in config.nodes.values() {
            let url = reqwest::Url::parse(&node.endpoint)?;
            ensure!(
                url.scheme() == "https"
                    || matches!(url.host_str(), Some("127.0.0.1" | "localhost")),
                "remote node requires TLS or an authenticated local SSH tunnel"
            );
            ensure!(
                node.token()?.len() >= 32,
                "node credential must contain at least 32 characters"
            );
        }
        Ok(Arc::new(Self {
            store: Store::open(&config.database)?,
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            attempts: Mutex::new(HashMap::new()),
        }))
    }
    pub fn login(&self, username: &str, password: &str) -> Result<(Session, String)> {
        let now = now_ns().parse::<u64>()? / 1_000_000_000;
        let user = self.config.users.iter().find(|u| u.username == username);
        let key = user
            .map(|u| u.username.as_str())
            .unwrap_or("unknown")
            .to_owned();
        {
            let mut attempts = self
                .attempts
                .lock()
                .map_err(|_| anyhow::anyhow!("login limiter unavailable"))?;
            let entry = attempts.entry(key.clone()).or_insert((now, 0));
            if now.saturating_sub(entry.0) >= 60 {
                *entry = (now, 0);
            }
            if entry.1 >= 10 {
                return Err(fault(429, "RATE_LIMITED", "too many login attempts"));
            }
            entry.1 += 1;
        }
        let valid = user.is_some_and(|u| {
            PasswordHash::new(&u.password_hash)
                .ok()
                .is_some_and(|hash| {
                    Argon2::default()
                        .verify_password(password.as_bytes(), &hash)
                        .is_ok()
                })
        });
        if !valid {
            return Err(fault(401, "INVALID_CREDENTIALS", "invalid credentials"));
        }
        let user = user.expect("validated user");
        let token = nonce();
        let csrf = super::digest(format!("csrf:{token}").as_bytes());
        let expires =
            (now_ns().parse::<u64>()? + self.config.session_seconds * 1_000_000_000).to_string();
        self.store
            .session_create(username, &token, &csrf, &expires)?;
        self.store.audit(username, "login", "session", "")?;
        Ok((
            Session {
                identity: user.identity(),
                csrf_token: csrf,
                expires_at_ns: expires,
                api_version: API_VERSION,
                build: super::build_id().into(),
            },
            token,
        ))
    }
    pub fn authenticate(&self, token: &str) -> Result<(Identity, String, String)> {
        let (username, csrf_hash, expires) = self
            .store
            .session(token)?
            .ok_or_else(|| fault(401, "SESSION_EXPIRED", "session expired"))?;
        let user = self
            .config
            .users
            .iter()
            .find(|u| u.username == username)
            .ok_or_else(|| fault(401, "SESSION_EXPIRED", "identity no longer configured"))?;
        Ok((user.identity(), csrf_hash, expires))
    }
    pub fn authorize(
        &self,
        identity: &Identity,
        node: &str,
        env: &str,
        account: Option<&str>,
        write: bool,
    ) -> Result<()> {
        if !allowed(identity, node, env)
            || (write && identity.role != Role::Operator)
            || !account_allowed(identity, account)
        {
            self.store.audit(&identity.username, "denied", node, env)?;
            return Err(fault(
                403,
                "FORBIDDEN",
                "resource or action is not authorized",
            ));
        }
        Ok(())
    }
    pub fn resource(&self, identity: &Identity, id: &str, write: bool) -> Result<Resource> {
        let resource = self.store.resource(id)?;
        let env = resource
            .body
            .get("environment")
            .and_then(Value::as_str)
            .unwrap_or("backtest");
        self.authorize(
            identity,
            &resource.node_id,
            env,
            resource.body.get("account").and_then(Value::as_str),
            write,
        )?;
        Ok(resource)
    }
    pub fn operation(&self, identity: &Identity, id: &str, write: bool) -> Result<Operation> {
        let op = self.store.operation(id)?;
        self.authorize(
            identity,
            &op.node_id,
            op.input.environment(),
            op.account.as_deref(),
            write,
        )?;
        for resource in self.store.request(id)?.resources {
            if let Some(account) = resource.body.get("account").and_then(Value::as_str) {
                self.authorize(
                    identity,
                    &op.node_id,
                    op.input.environment(),
                    Some(account),
                    write,
                )?;
            }
        }
        Ok(op)
    }
    pub fn file_node(&self, identity: &Identity, id: &str, path: &str) -> Result<String> {
        match self.operation(identity, id, false) {
            Ok(op) => Ok(op.node_id),
            Err(error)
                if path == "strategy.titan"
                    && error
                        .downcast_ref::<super::Fault>()
                        .is_some_and(|f| f.status == 404) =>
            {
                let artifact = self.store.artifact_for_operation(id)?;
                Ok(self.resource(identity, &artifact.id, false)?.node_id)
            }
            Err(error) => Err(error),
        }
    }
    pub fn submit(&self, identity: &Identity, input: SubmitOperation) -> Result<Operation> {
        ensure!(
            !input.idempotency_key.is_empty() && input.idempotency_key.len() <= 128,
            "invalid idempotency key"
        );
        self.authorize(
            identity,
            &input.node_id,
            input.action.environment(),
            None,
            true,
        )?;
        let payload = digest(&titan_strategy_runtime::compiler_json_v13(
            &serde_json::to_value(&input)?,
        ));
        if let Some((old, hash)) = self
            .store
            .by_key(&identity.username, &input.idempotency_key)?
        {
            if hash != payload {
                return Err(fault(
                    409,
                    "IDEMPOTENCY_CONFLICT",
                    "same key has different input",
                ));
            }
            self.operation(identity, &old.id, true)?;
            return Ok(old);
        }
        let node = self
            .config
            .nodes
            .get(&input.node_id)
            .context("unknown node")?;
        if input.action.environment() == "live" && !node.live_control {
            return Err(fault(
                403,
                "CAPABILITY_DISABLED",
                "live control capability is disabled for this node",
            ));
        }
        ensure!(
            node.environments
                .iter()
                .any(|e| e == input.action.environment()),
            "node role does not allow this action"
        );
        let ids: Vec<String> = match &input.action {
            Action::Describe { template_id, .. } => {
                vec![format!("{}-template-{template_id}", input.node_id)]
            }
            Action::Build { version_id } => vec![version_id.clone()],
            Action::Prepare { dataset_id } => {
                vec![format!("{}-dataset-{dataset_id}", input.node_id)]
            }
            Action::Backtest {
                artifact_id,
                dataset_id,
                ..
            } => vec![
                artifact_id.clone(),
                format!("{}-dataset-{dataset_id}", input.node_id),
            ],
            Action::Report { backtest_id } => vec![backtest_id.clone()],
            Action::Deploy {
                artifact_id,
                binding_id,
            } => vec![
                artifact_id.clone(),
                format!("{}-binding-{binding_id}", input.node_id),
            ],
            Action::Start { deployment_id } => vec![deployment_id.clone()],
            action => {
                let (run, _) = action.run().context("missing run identity")?;
                vec![run.into()]
            }
        };
        let mut resources = Vec::new();
        for id in ids {
            let resource = self.resource(identity, &id, false)?;
            if !matches!(input.action, Action::Deploy { .. }) {
                ensure!(
                    resource.node_id == input.node_id,
                    "resource belongs to another node"
                );
            }
            if let Some(account) = resource.body.get("account").and_then(Value::as_str) {
                self.authorize(
                    identity,
                    &input.node_id,
                    input.action.environment(),
                    Some(account),
                    true,
                )?;
            }
            resources.push(resource);
        }
        if matches!(input.action, Action::Build { .. }) {
            ensure!(
                resources[0].kind == "version" && resources[0].frozen,
                "build requires a frozen parameter version"
            );
        }
        if let Some((_, generation)) = input.action.run() {
            ensure!(
                resources[0].body["generation"].as_str() == Some(generation),
                "run generation changed"
            );
        }
        let occupancy = if matches!(input.action, Action::Start { .. } | Action::Recover { .. }) {
            let body = &resources[0].body;
            Some(serde_json::to_string(&json!([
                body["account_identity"]
                    .as_str()
                    .context("missing canonical account identity")?,
                body["instrument"].as_str().context("missing instrument")?
            ]))?)
        } else {
            None
        };
        self.store.accept(
            &identity.username,
            &input,
            resources,
            &payload,
            occupancy.as_deref(),
        )
    }
    pub fn save_version(
        &self,
        identity: &Identity,
        id: Option<&str>,
        input: SaveVersion,
    ) -> Result<Resource> {
        self.authorize(identity, &input.node_id, "backtest", None, true)?;
        ensure!(input.parameters.is_object(), "parameters must be an object");
        let template = self.resource(
            identity,
            &format!("{}-template-{}", input.node_id, input.template_id),
            false,
        )?;
        let body = json!({"research_id":template.body["research_id"],"research_name":template.body["research_name"],"template_id":input.template_id,"parameters":input.parameters,"source_digest":template.body["source_digest"],"environment":"backtest"});
        let mut frozen = false;
        let mut final_body = body.clone();
        if let Some(op_id) = &input.freeze_operation_id {
            let op = self.operation(identity, op_id, false)?;
            ensure!(
                op.state == OperationState::Succeeded,
                "validation operation has not succeeded"
            );
            let Action::Describe {
                template_id,
                parameters,
            } = &op.input
            else {
                anyhow::bail!("freeze requires a describe/validate result");
            };
            ensure!(
                template_id == &input.template_id
                    && parameters == &input.parameters
                    && op.node_id == input.node_id,
                "validation does not match these inputs"
            );
            let result = op.result.context("missing validation result")?;
            ensure!(
                result["source_digest"] == template.body["source_digest"],
                "source changed since validation"
            );
            final_body["normalized_parameters"] = result["normalized_parameters"].clone();
            final_body["metadata"] = result["metadata"].clone();
            frozen = true;
        }
        if let Some(id) = id {
            let old = self.resource(identity, id, true)?;
            ensure!(
                old.kind == "version" && old.node_id == input.node_id,
                "incorrect resource type"
            );
            ensure!(
                input.expected_revision.is_some(),
                "update requires expected revision"
            );
        }
        let resource = Resource {
            id: id.map(str::to_owned).unwrap_or_else(nonce),
            kind: "version".into(),
            node_id: input.node_id,
            revision: String::new(),
            frozen,
            digest: String::new(),
            body: final_body,
        };
        let resource = self.store.save_resource_audited(
            resource,
            input.expected_revision.as_deref(),
            &identity.username,
            if frozen { "freeze" } else { "save_version" },
        )?;
        Ok(resource)
    }
    pub fn spawn(self: &Arc<Self>) {
        for (id, node) in &self.config.nodes {
            for lane in 0..4 {
                let app = self.clone();
                let id = id.clone();
                let node = node.clone();
                tokio::spawn(async move {
                    loop {
                        let result = match lane {
                            0 => app.tick_catalog(&id, &node).await,
                            1 => app.tick_operations(&id, &node, true, Some(true)).await,
                            2 => app.tick_operations(&id, &node, true, Some(false)).await,
                            _ => app.tick_operations(&id, &node, false, None).await,
                        };
                        if let Err(error) = result {
                            tracing::warn!(node=%id,error=%error,"management reconciliation deferred");
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }
        }
    }
    async fn tick_catalog(&self, id: &str, node: &super::config::RemoteNode) -> Result<()> {
        let catalog: Result<()> = async {
            let response = self
                .client
                .get(format!(
                    "{}/v1/catalog",
                    node.endpoint.trim_end_matches('/')
                ))
                .bearer_auth(node.token()?)
                .send()
                .await?;
            let items: Vec<Resource> = node_json(response).await?;
            for mut item in items {
                ensure!(item.node_id == id, "node catalog identity mismatch");
                if item.kind == "alert"
                    && let Some(body) = item.body.as_object_mut()
                {
                    body.remove("acknowledgement");
                }
                self.store.save_resource(item, None)?;
            }
            Ok(())
        }
        .await;
        for environment in &node.environments {
            let key = format!("node-status-{id}-{environment}");
            let previous = self.store.resource(&key).ok();
            let last_seen = if catalog.is_ok() {
                Some(now_ns())
            } else {
                previous
                    .as_ref()
                    .and_then(|r| r.body["last_seen_at_ns"].as_str().map(str::to_owned))
            };
            self.store.save_resource(Resource{id:key,kind:"node".into(),node_id:id.into(),revision:String::new(),frozen:false,digest:String::new(),body:json!({"environment":environment,"online":catalog.is_ok(),"last_seen_at_ns":last_seen,"last_attempt_ns":now_ns(),"source":"management_transport","reason":catalog.as_ref().err().map(ToString::to_string)})},None)?;
        }
        catalog
    }
    async fn tick_operations(
        &self,
        id: &str,
        node: &super::config::RemoteNode,
        stops: bool,
        pending: Option<bool>,
    ) -> Result<()> {
        let active: Vec<_> = self
            .store
            .active()?
            .into_iter()
            .filter(|op| op.node_id == id && matches!(op.input, Action::Stop { .. }) == stops)
            .filter(|op| pending.is_none_or(|value| value == (op.state == OperationState::Pending)))
            .collect();
        let mut dispatched: std::collections::HashSet<String> = active
            .iter()
            // Unknown is a result classification, not proof that a process still owns a slot.
            // N remains the admission authority and can conclusively reject a busy slot.
            .filter(|op| op.state == OperationState::Running)
            .map(|op| op.node_id.clone())
            .collect();
        for op in active {
            if op.state == OperationState::Pending
                && dispatched.contains(&op.node_id)
                && op.input.environment() != "live"
            {
                continue;
            }
            let endpoint = node.endpoint.trim_end_matches('/');
            let token = node.token()?;
            let response = self
                .client
                .get(format!("{endpoint}/v1/operations/{}", op.id))
                .bearer_auth(&token)
                .send()
                .await;
            let result: Result<Option<Receipt>> = async {
                let response = response?;
                if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    let error: ApiError = node_json_allow_error(response).await?;
                    ensure!(
                        error.code == "RECEIPT_UNCERTAIN",
                        "node response unavailable"
                    );
                    return Ok(Some(Receipt {
                        operation_id: op.id.clone(),
                        input_digest: op.input_digest.clone(),
                        state: OperationState::Unknown,
                        result: op.result.clone(),
                        error: Some(error.message),
                    }));
                }
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    let absent: ApiError = node_json_allow_error(response).await?;
                    ensure!(
                        absent.code == "NOT_FOUND",
                        "node did not prove operation absence"
                    );
                    if op.state == OperationState::Pending {
                        if !dispatched.insert(op.node_id.clone())
                            && op.input.environment() != "live"
                        {
                            return Ok(None);
                        }
                        // The authenticated node proved absence; claim dispatch atomically against cancellation.
                        let Some(request) = self.store.begin_dispatch_after_absence(&op.id)? else {
                            return Ok(None);
                        };
                        let response = self
                            .client
                            .post(format!("{endpoint}/v1/operations"))
                            .bearer_auth(&token)
                            .json(&request)
                            .send()
                            .await?;
                        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                            let rejection: ApiError = node_json_allow_error(response).await?;
                            if rejection.code == "NODE_BUSY" {
                                // This authenticated rejection is issued before the node admission boundary.
                                self.store.admission_rejected(&op.id, &op.input_digest)?;
                                return Ok(None);
                            }
                            anyhow::bail!("node rejected admission without conclusive receipt");
                        }
                        return Ok(Some(node_json(response).await?));
                    }
                    return Ok(Some(Receipt {
                        operation_id: op.id.clone(),
                        input_digest: op.input_digest.clone(),
                        state: OperationState::Unknown,
                        result: op.result.clone(),
                        error: Some(
                            "node has no conclusive receipt; automatic redispatch disabled".into(),
                        ),
                    }));
                }
                let mut receipt: Receipt = node_json(response).await?;
                if op.cancel_requested && !receipt.state.terminal() {
                    receipt = node_json(
                        self.client
                            .post(format!("{endpoint}/v1/operations/{}/cancel", op.id))
                            .bearer_auth(&token)
                            .send()
                            .await?,
                    )
                    .await?;
                }
                Ok(Some(receipt))
            }
            .await;
            match result {
                Ok(Some(receipt)) => {
                    ensure!(
                        receipt.operation_id == op.id && receipt.input_digest == op.input_digest,
                        "receipt identity mismatch"
                    );
                    if let Some(items) = receipt
                        .result
                        .as_ref()
                        .and_then(|v| v.get("resources"))
                        .and_then(Value::as_array)
                    {
                        for item in items {
                            let item: Resource = serde_json::from_value(item.clone())?;
                            ensure!(item.node_id == op.node_id, "result resource node mismatch");
                            self.store.save_resource(item, None)?;
                        }
                    }
                    if receipt.state == OperationState::Unknown {
                        self.store
                            .update_operation_if_current(&receipt, &op.revision)?;
                    } else {
                        self.store.update_operation(&receipt)?;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    if op.state != OperationState::Pending {
                        self.store.update_operation_if_current(
                            &Receipt {
                                operation_id: op.id,
                                input_digest: op.input_digest,
                                state: OperationState::Unknown,
                                result: op.result,
                                error: Some(format!("node response unavailable: {error}")),
                            },
                            &op.revision,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}
pub(crate) async fn node_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T> {
    node_json_allow_error(response.error_for_status()?).await
}

async fn node_json_allow_error<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T> {
    use futures_util::StreamExt;
    const LIMIT: usize = 16 * 1024 * 1024;
    ensure!(
        response
            .content_length()
            .is_none_or(|len| len <= LIMIT as u64),
        "node response exceeds the protocol limit"
    );
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            bytes.len() + chunk.len() <= LIMIT,
            "node response exceeds the protocol limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
