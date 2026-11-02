//! Account directory and inert strategy-source registration. Neither activates a connector/run.
use super::{
    app::{App, node_json},
    config::{NodeConfig, StrategyParameterPreset},
    contract::*,
    fault,
    http::{ApiResult, identity},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Arc};

fn validate_profile(input: &AccountProfileInput) -> Result<()> {
    ensure!(
        !input.name.trim().is_empty() && input.name.len() <= 128,
        "account name is required (maximum 128 bytes)"
    );
    ensure!(
        matches!(
            input.exchange.as_str(),
            "binance" | "okx" | "hyperliquid" | "gate" | "backpack" | "lighter"
        ),
        "unsupported exchange"
    );
    ensure!(
        matches!(input.environment.as_str(), "backtest" | "live"),
        "invalid account environment"
    );
    ensure!(input.notes.len() <= 512, "account notes exceed limit");
    let reference = &input.credential_ref;
    ensure!(
        reference.is_empty()
            || reference
                .strip_prefix("secret://env/")
                .is_some_and(|name| !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
        "use a secret://env/NAME reference, never a credential value"
    );
    Ok(())
}

pub(super) async fn accounts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Resource>>> {
    let actor = identity(&app, &headers, false)?;
    Ok(Json(app.store.list_resources(
        &actor,
        Some("account_profile"),
        0,
        200,
    )?))
}

pub(super) async fn create_account(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<AccountProfileInput>,
) -> ApiResult<Json<Resource>> {
    Ok(save_account(app, headers, None, input)?)
}

pub(super) async fn update_account(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<AccountProfileInput>,
) -> ApiResult<Json<Resource>> {
    Ok(save_account(app, headers, Some(id), input)?)
}

fn save_account(
    app: Arc<App>,
    headers: HeaderMap,
    id: Option<String>,
    input: AccountProfileInput,
) -> Result<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    app.authorize(&actor, &input.node_id, &input.environment, None, true)?;
    validate_profile(&input)?;
    ensure!(
        app.config.nodes.contains_key(&input.node_id),
        "node is not configured"
    );
    let created = if let Some(id) = &id {
        super::checked_id(id)?;
        let old = app.store.resource(id)?;
        ensure!(old.kind == "account_profile", "account profile not found");
        app.authorize(
            &actor,
            &old.node_id,
            old.body["environment"].as_str().unwrap_or("live"),
            None,
            true,
        )?;
        ensure!(
            input.expected_revision.is_some(),
            "expected_revision is required when editing"
        );
        old.body["created_at_ns"].clone()
    } else {
        ensure!(
            input.expected_revision.is_none(),
            "new profile cannot have an expected revision"
        );
        json!(super::now_ns())
    };
    let resource = Resource {
        id: id.unwrap_or_else(|| format!("account-profile-{}", super::nonce())),
        kind: "account_profile".into(),
        node_id: input.node_id,
        revision: String::new(),
        digest: String::new(),
        frozen: false,
        body: json!({"name":input.name.trim(),"exchange":input.exchange,"environment":input.environment,"credential_ref":input.credential_ref,"notes":input.notes,"enabled":input.enabled,"connection_state":"not_connected","created_at_ns":created,"updated_at_ns":super::now_ns()}),
    };
    Ok(Json(app.store.save_resource_audited(
        resource,
        input.expected_revision.as_deref(),
        &actor.username,
        "save_account_profile",
    )?))
}

pub(super) fn validate_source(input: &StrategySourceInput) -> Result<()> {
    super::checked_id(&input.strategy_id)?;
    ensure!(
        !matches!(
            input.strategy_id.as_str(),
            "live" | "backtest" | "unassigned"
        ),
        "reserved strategy ID"
    );
    ensure!(
        !input.name.trim().is_empty() && input.name.len() <= 128,
        "strategy name is required (maximum 128 bytes)"
    );
    ensure!(
        input.filename.ends_with(".py")
            && input.filename.len() <= 128
            && input
                .filename
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
            && !input.filename.starts_with('.'),
        "upload one Python .py file with a plain filename"
    );
    ensure!(
        !input.source.trim().is_empty()
            && input.source.len() <= 262_144
            && !input.source.contains('\0'),
        "source must be nonempty UTF-8 Python text, at most 256 KiB"
    );
    Ok(())
}

pub(super) async fn upload_strategy(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<StrategySourceInput>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    app.authorize(&actor, &input.node_id, "backtest", None, true)?;
    validate_source(&input)?;
    let node = app
        .config
        .nodes
        .get(&input.node_id)
        .context("node is not configured")?;
    super::require(
        node.environments.iter().any(|e| e == "backtest"),
        "source uploads require a build node",
    )?;
    let response = app
        .client
        .post(format!(
            "{}/v1/strategy-sources",
            node.endpoint.trim_end_matches('/')
        ))
        .bearer_auth(node.token()?)
        .json(&input)
        .send()
        .await
        .map_err(anyhow::Error::from)?;
    if response.status() == reqwest::StatusCode::FORBIDDEN {
        return Err(fault(
            403,
            "UPLOAD_DISABLED",
            "strategy uploads are disabled on this node",
        )
        .into());
    }
    let resource: Resource = node_json(response).await?;
    super::require(
        resource.node_id == input.node_id
            && resource.kind == "parameter_preset"
            && resource.body["strategy_id"] == input.strategy_id
            && resource.body["source_digest"] == super::digest(input.source.as_bytes()),
        "uploaded source identity mismatch",
    )?;
    Ok(Json(app.store.save_resource_audited(
        resource,
        None,
        &actor.username,
        "upload_strategy_source",
    )?))
}

#[derive(Serialize, Deserialize)]
struct UploadedSource {
    strategy_id: String,
    name: String,
    filename: String,
    source_digest: String,
}
fn source_root(config: &NodeConfig) -> PathBuf {
    config.work_root.join("strategy-sources")
}

pub(super) fn uploaded_presets(
    config: &NodeConfig,
) -> Result<BTreeMap<String, StrategyParameterPreset>> {
    let root = source_root(config);
    let mut presets = BTreeMap::new();
    if !root.exists() {
        return Ok(presets);
    }
    for entry in std::fs::read_dir(&root)? {
        let path = entry?.path();
        if path.extension().is_none_or(|v| v != "json") {
            continue;
        }
        ensure!(presets.len() < 128, "uploaded source capacity exceeded");
        let bytes = std::fs::read(&path)?;
        ensure!(bytes.len() < 4096, "invalid source registration");
        let record: UploadedSource = serde_json::from_slice(&bytes)?;
        ensure!(
            record.source_digest.len() == 64
                && record.source_digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid source digest"
        );
        let source = root.join(format!("{}.py", record.source_digest));
        ensure!(
            super::digest(&std::fs::read(&source)?) == record.source_digest,
            "uploaded source changed"
        );
        let key = path
            .file_stem()
            .and_then(|v| v.to_str())
            .context("invalid source registration ID")?
            .to_owned();
        super::checked_id(&key)?;
        ensure!(
            !config.parameter_presets.contains_key(&key),
            "source registration conflicts with configured preset"
        );
        presets.insert(
            key,
            StrategyParameterPreset {
                strategy_id: Some(record.strategy_id),
                legacy_research_id: None,
                strategy_name: Some(record.name),
                source,
                parameters: None,
                layout: None,
            },
        );
    }
    Ok(presets)
}

pub(super) fn register_source(
    config: &NodeConfig,
    input: &StrategySourceInput,
) -> Result<Resource> {
    if !config.allow_strategy_uploads
        || !config.environments.iter().any(|e| e == "backtest")
        || config.id != input.node_id
    {
        return Err(fault(
            403,
            "UPLOAD_DISABLED",
            "strategy uploads are disabled on this node",
        ));
    }
    validate_source(input)?;
    let presets = uploaded_presets(config)?;
    let source_digest = super::digest(input.source.as_bytes());
    let key = format!(
        "upload-{}",
        super::digest(
            format!(
                "{}\0{}\0{}\0{}",
                input.strategy_id, input.name, input.filename, source_digest
            )
            .as_bytes()
        )
    );
    ensure!(
        presets.contains_key(&key) || presets.len() < 128,
        "uploaded source capacity exceeded"
    );
    let root = source_root(config);
    std::fs::create_dir_all(&root)?;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let path = root.join(format!("{source_digest}.py"));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
    {
        Ok(mut file) => {
            file.write_all(input.source.as_bytes())?;
            file.sync_all()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => ensure!(
            std::fs::read(&path)? == input.source.as_bytes(),
            "source digest collision"
        ),
        Err(e) => return Err(e.into()),
    }
    let record = UploadedSource {
        strategy_id: input.strategy_id.clone(),
        name: input.name.trim().into(),
        filename: input.filename.clone(),
        source_digest: source_digest.clone(),
    };
    super::node::atomic_json(&root.join(format!("{key}.json")), &record)?;
    std::fs::set_permissions(
        root.join(format!("{key}.json")),
        std::fs::Permissions::from_mode(0o600),
    )?;
    Ok(super::node::resource(
        config,
        "parameter_preset",
        &key,
        false,
        json!({"strategy_id":input.strategy_id,"strategy_name":input.name.trim(),"parameter_preset_id":key,"parameters":{},"source_digest":source_digest,"source_filename":input.filename,"source_origin":"upload","environment":"backtest"}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profiles_accept_references_and_reject_credential_values() {
        let mut input: AccountProfileInput = serde_json::from_value(json!({"node_id":"build","name":"模拟账户","exchange":"okx","environment":"backtest","credential_ref":"secret://env/TEST_ACCOUNT","notes":"示例","enabled":true})).unwrap();
        validate_profile(&input).unwrap();
        input.credential_ref = "plaintext-value".into();
        assert!(validate_profile(&input).is_err());
        input.credential_ref = "secret://env/KEY?value=private".into();
        assert!(validate_profile(&input).is_err());
    }
    #[test]
    fn upload_is_inert_immutable_idempotent_and_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let config: NodeConfig = serde_json::from_value(json!({"id":"build","listen":"127.0.0.1:8041","token_env":"UNUSED","work_root":dir.path(),"titan_program":"/unused","environments":["backtest"],"allow_strategy_uploads":true})).unwrap();
        let mut input = StrategySourceInput {
            node_id: "build".into(),
            strategy_id: "sample".into(),
            name: "示例策略".into(),
            filename: "sample.py".into(),
            source: "raise RuntimeError('must never execute on upload')\n".into(),
        };
        let first = register_source(&config, &input).unwrap();
        assert_eq!(first.id, register_source(&config, &input).unwrap().id);
        let presets = uploaded_presets(&config).unwrap();
        assert_eq!(presets.len(), 1);
        let old_path = presets.values().next().unwrap().source.clone();
        input.source.push_str("# next version\n");
        assert_ne!(first.id, register_source(&config, &input).unwrap().id);
        assert_eq!(uploaded_presets(&config).unwrap().len(), 2);
        std::fs::write(old_path, "changed").unwrap();
        assert!(uploaded_presets(&config).is_err());
    }
    #[test]
    fn upload_rejects_path_traversal_oversize_and_disabled_nodes() {
        let input = StrategySourceInput {
            node_id: "build".into(),
            strategy_id: "sample".into(),
            name: "Sample".into(),
            filename: "../sample.py".into(),
            source: "# source".into(),
        };
        assert!(validate_source(&input).is_err());
        let mut input = StrategySourceInput {
            filename: "sample.py".into(),
            ..input
        };
        input.source = "x".repeat(262_145);
        assert!(validate_source(&input).is_err());
        input.source = "# source".into();
        let config: NodeConfig = serde_json::from_value(json!({"id":"build","listen":"127.0.0.1:8041","token_env":"UNUSED","work_root":"/unused","titan_program":"/unused","environments":["backtest"]})).unwrap();
        assert!(register_source(&config, &input).is_err());
    }
    #[test]
    fn account_directory_enforces_role_scope_and_optimistic_revision() {
        use super::super::config::{ManagerConfig, RemoteNode, User};
        use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
        use axum::http::header;
        let dir = tempfile::tempdir().unwrap();
        let salt = SaltString::encode_b64(b"catalog-fixture-salt").unwrap();
        let hash = Argon2::default()
            .hash_password(b"fixture", &salt)
            .unwrap()
            .to_string();
        let users = [("operator", Role::Operator), ("reader", Role::Reader)]
            .into_iter()
            .map(|(name, role)| User {
                username: name.into(),
                password_hash: hash.clone(),
                role,
                nodes: vec!["build".into()],
                accounts: vec![],
                environments: vec!["backtest".into()],
            })
            .collect();
        let mut app = App::open(ManagerConfig {
            listen: "127.0.0.1:8040".into(),
            public_origin: "http://127.0.0.1:8040".into(),
            database: dir.path().join("manager.db"),
            web_root: dir.path().into(),
            secure_cookie: false,
            session_seconds: 3600,
            nodes: BTreeMap::new(),
            users,
        })
        .unwrap();
        Arc::get_mut(&mut app).unwrap().config.nodes.insert(
            "build".into(),
            RemoteNode {
                endpoint: "http://127.0.0.1:8041".into(),
                token_env: "UNUSED".into(),
                environments: vec!["backtest".into()],
                live_control: false,
            },
        );
        let expires = (super::super::now_ns().parse::<u64>().unwrap() + 60_000_000_000).to_string();
        for user in ["operator", "reader"] {
            app.store
                .session_create(user, user, "fixture-csrf", &expires)
                .unwrap();
        }
        let headers = |user: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::COOKIE,
                format!("titan_session={user}").parse().unwrap(),
            );
            headers.insert(header::ORIGIN, "http://127.0.0.1:8040".parse().unwrap());
            headers.insert("x-csrf-token", "fixture-csrf".parse().unwrap());
            headers
        };
        let mut input: AccountProfileInput = serde_json::from_value(json!({"node_id":"build","name":"模拟账户","exchange":"okx","environment":"backtest","credential_ref":"","notes":"示例","enabled":true})).unwrap();
        assert!(save_account(app.clone(), headers("reader"), None, input.clone()).is_err());
        let mut missing_csrf = headers("operator");
        missing_csrf.remove("x-csrf-token");
        assert!(save_account(app.clone(), missing_csrf, None, input.clone()).is_err());
        input.environment = "live".into();
        assert!(save_account(app.clone(), headers("operator"), None, input.clone()).is_err());
        input.environment = "backtest".into();
        let first = save_account(app.clone(), headers("operator"), None, input.clone())
            .unwrap()
            .0;
        input.expected_revision = Some(first.revision.clone());
        input.enabled = false;
        let next = save_account(
            app.clone(),
            headers("operator"),
            Some(first.id.clone()),
            input.clone(),
        )
        .unwrap()
        .0;
        assert_ne!(next.revision, first.revision);
        assert!(
            save_account(
                app.clone(),
                headers("operator"),
                Some(first.id.clone()),
                input
            )
            .is_err()
        );
        let reopened = super::super::store::Store::open(&dir.path().join("manager.db")).unwrap();
        assert_eq!(reopened.resource(&first.id).unwrap().body["enabled"], false);
    }
}
