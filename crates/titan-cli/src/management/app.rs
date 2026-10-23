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
        let occupancy = if matches!(input.action, Action::Start { .. }) {
            let body = &resources[0].body;
            Some(format!(
                "{}:{}",
                body["account"].as_str().context("missing account")?,
                body["instrument"].as_str().context("missing instrument")?
            ))
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
        let body = json!({"template_id":input.template_id,"parameters":input.parameters,"source_digest":template.body["source_digest"],"environment":"backtest"});
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
        let resource = self
            .store
            .save_resource(resource, input.expected_revision.as_deref())?;
        self.store.audit(
            &identity.username,
            if frozen { "freeze" } else { "save_version" },
            &resource.id,
            &resource.digest,
        )?;
        Ok(resource)
    }
    pub fn spawn(self: &Arc<Self>) {
        let app = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(error) = app.tick().await {
                    tracing::warn!(error=%error,"management reconciliation deferred");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    async fn tick(&self) -> Result<()> {
        for (id, node) in &self.config.nodes {
            let result = self
                .client
                .get(format!(
                    "{}/v1/catalog",
                    node.endpoint.trim_end_matches('/')
                ))
                .bearer_auth(node.token()?)
                .send()
                .await;
            if let Ok(response) = result
                && response.status().is_success()
            {
                let items: Vec<Resource> = response.json().await?;
                for item in items {
                    ensure!(item.node_id == *id, "node catalog identity mismatch");
                    self.store.save_resource(item, None)?;
                }
            }
        }
        let active = self.store.active()?;
        let mut dispatched: std::collections::HashSet<String> = active
            .iter()
            .filter(|op| op.state != OperationState::Pending)
            .map(|op| op.node_id.clone())
            .collect();
        for op in active {
            if op.state == OperationState::Pending && op.cancel_requested {
                self.store.update_operation(&Receipt {
                    operation_id: op.id,
                    input_digest: op.input_digest,
                    state: OperationState::Cancelled,
                    result: None,
                    error: None,
                })?;
                continue;
            }
            let Some(node) = self.config.nodes.get(&op.node_id) else {
                continue;
            };
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
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    if op.state == OperationState::Pending {
                        if op.cancel_requested {
                            return Ok(Some(Receipt {
                                operation_id: op.id.clone(),
                                input_digest: op.input_digest.clone(),
                                state: OperationState::Cancelled,
                                result: None,
                                error: None,
                            }));
                        }
                        if !dispatched.insert(op.node_id.clone()) {
                            return Ok(None);
                        }
                        // Persist uncertainty before sending. A crash or lost reply never causes blind redispatch.
                        self.store.update_operation(&Receipt {
                            operation_id: op.id.clone(),
                            input_digest: op.input_digest.clone(),
                            state: OperationState::Unknown,
                            result: None,
                            error: Some("dispatch acknowledgement pending".into()),
                        })?;
                        let request = self.store.request(&op.id)?;
                        let response = self
                            .client
                            .post(format!("{endpoint}/v1/operations"))
                            .bearer_auth(&token)
                            .json(&request)
                            .send()
                            .await?;
                        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                            let rejection: ApiError = response.json().await?;
                            if rejection.code == "NODE_BUSY" {
                                // This authenticated rejection is issued before the node admission boundary.
                                return Ok(Some(Receipt {
                                    operation_id: op.id.clone(),
                                    input_digest: op.input_digest.clone(),
                                    state: OperationState::Pending,
                                    result: None,
                                    error: Some("node capacity occupied; not admitted".into()),
                                }));
                            }
                            anyhow::bail!("node rejected admission without conclusive receipt");
                        }
                        return Ok(Some(response.error_for_status()?.json().await?));
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
                let mut receipt: Receipt = response.error_for_status()?.json().await?;
                if op.cancel_requested && !receipt.state.terminal() {
                    receipt = self
                        .client
                        .post(format!("{endpoint}/v1/operations/{}/cancel", op.id))
                        .bearer_auth(&token)
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
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
                    self.store.update_operation(&receipt)?;
                }
                Ok(None) => {}
                Err(error) => {
                    if op.state != OperationState::Pending {
                        self.store.update_operation(&Receipt {
                            operation_id: op.id,
                            input_digest: op.input_digest,
                            state: OperationState::Unknown,
                            result: op.result,
                            error: Some(format!("node response unavailable: {error}")),
                        })?;
                    }
                }
            }
        }
        Ok(())
    }
}
