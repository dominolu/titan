//! Resolve readable targets to fixed, authorized manager identities.
use super::{
    client::{Client, ClientError},
    frontend_config,
    managed_strategy::add_scope,
};
use anyhow::Result;
use serde_json::Value;

const PAGE: usize = 200;
const MAX_RECORDS: usize = 20_000;

pub(super) fn invalid(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_INPUT", message).into()
}

pub(super) fn not_found(message: &str) -> anyhow::Error {
    let mut error = ClientError::new("NOT_FOUND", message);
    error.status = Some(404);
    error.into()
}

pub(super) fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            ClientError::new("PROTOCOL_ERROR", "manager record has no required identity").into()
        })
}

fn alias(client: &Client, target: &str) -> Result<Option<String>> {
    let profile = frontend_config::profile_value(client.options())?;
    Ok(profile
        .get("aliases")
        .and_then(|v| v.get(target))
        .and_then(toml::Value::as_str)
        .map(str::to_owned))
}

fn scope(client: &Client, resource: &Value) -> Result<()> {
    if client
        .options()
        .node
        .as_deref()
        .is_some_and(|v| resource["node_id"].as_str() != Some(v))
        || client
            .environment()
            .is_some_and(|v| resource["body"]["environment"].as_str() != Some(v))
    {
        return Err(invalid(
            "target does not match the selected node or environment",
        ));
    }
    Ok(())
}

async fn maybe_get(client: &Client, path: &str) -> Result<Option<Value>> {
    let mut query = Vec::new();
    add_scope(client, &mut query);
    match client.get(path, &query).await {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if error
                .downcast_ref::<ClientError>()
                .is_some_and(|e| e.status == Some(404)) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn resource(client: &Client, id: &str) -> Result<Value> {
    super::managed_strategy::checked_id(id)?;
    let value = client.get(&format!("/api/v2/resources/{id}"), &[]).await?;
    scope(client, &value)?;
    Ok(value)
}

async fn exact_resource(client: &Client, target: &str) -> Result<Option<Value>> {
    if super::checked_id(target).is_err() {
        return Ok(None);
    }
    let value = maybe_get(client, &format!("/api/v2/resources/{target}")).await?;
    if let Some(value) = &value {
        scope(client, value)?;
    }
    Ok(value)
}

// Every page is authorized and filtered by the server before pagination. If the
// bound is exhausted, resolution fails rather than selecting from a partial set.
async fn pages(client: &Client, path: &str, filters: &[(&str, &str)]) -> Result<Vec<Value>> {
    let mut items = Vec::new();
    let mut offset = 0_usize;
    loop {
        let offset_text = offset.to_string();
        let mut query = filters.to_vec();
        query.push(("offset", &offset_text));
        query.push(("limit", "200"));
        add_scope(client, &mut query);
        let page = client.get(path, &query).await?;
        let page = page.as_array().ok_or_else(|| {
            ClientError::new(
                "PROTOCOL_ERROR",
                "manager returned an invalid directory page",
            )
        })?;
        if page.len() > PAGE {
            return Err(ClientError::new(
                "PROTOCOL_ERROR",
                "manager exceeded the directory page limit",
            )
            .into());
        }
        items.extend(page.iter().cloned());
        if page.len() < PAGE {
            return Ok(items);
        }
        offset += PAGE;
        if offset >= MAX_RECORDS {
            return Err(invalid(
                "target directory exceeds the resolution budget; narrow the node or environment scope",
            ));
        }
    }
}

pub(super) async fn resources(
    client: &Client,
    kind: &str,
    strategy: Option<&str>,
) -> Result<Vec<Value>> {
    let mut filters = vec![("kind", kind)];
    if let Some(strategy) = strategy {
        filters.push(("strategy_id", strategy));
    }
    pages(client, "/api/v2/resources", &filters).await
}

pub(super) async fn strategies(client: &Client) -> Result<Vec<Value>> {
    pages(client, "/api/v2/strategies", &[]).await
}

pub(super) fn unique(mut items: Vec<Value>) -> Result<Value> {
    match items.len() {
        0 => Err(not_found(
            "no matching authorized target; list targets or provide an exact ID",
        )),
        1 => Ok(items.remove(0)),
        _ => Err(ClientError::new(
            "AMBIGUOUS_TARGET",
            "target is ambiguous; provide an exact resource ID or a fixed profile alias",
        )
        .into()),
    }
}

pub(super) async fn strategy(client: &Client, target: &str) -> Result<Value> {
    let mapped = alias(client, target)?;
    let name = mapped.as_deref().unwrap_or(target);
    if super::checked_id(name).is_ok()
        && let Some(value) = maybe_get(client, &format!("/api/v2/strategies/{name}")).await?
    {
        return Ok(value);
    }
    if mapped.is_some() {
        return Err(not_found(
            "strategy alias does not reference an authorized strategy ID",
        ));
    }
    unique(
        strategies(client)
            .await?
            .into_iter()
            .filter(|v| v["name"].as_str() == Some(name))
            .collect(),
    )
}

/// Exact resources and aliases win over display names. A strategy name may only
/// select a resource when its complete, authorized directory has one candidate.
pub(super) async fn resolve(client: &Client, target: &str, kinds: &[&str]) -> Result<Value> {
    let mapped = alias(client, target)?;
    let name = mapped.as_deref().unwrap_or(target);
    if let Some(value) = exact_resource(client, name).await? {
        if !value["kind"]
            .as_str()
            .is_some_and(|kind| kinds.contains(&kind))
        {
            return Err(invalid(
                "exact target has an unsupported resource kind for this command",
            ));
        }
        return Ok(value);
    }
    let owner = strategy(client, target).await?;
    let owner = text(&owner, "id")?;
    let mut matches = Vec::new();
    for kind in kinds {
        matches.extend(resources(client, kind, Some(owner)).await?);
    }
    unique(matches)
}

pub(super) async fn start(client: &Client, target: &str) -> Result<Value> {
    let mapped = alias(client, target)?;
    let name = mapped.as_deref().unwrap_or(target);
    if let Some(value) = exact_resource(client, name).await? {
        if !matches!(value["kind"].as_str(), Some("deployment" | "run")) {
            return Err(invalid(
                "start requires a bound deployment or a fixed recoverable run",
            ));
        }
        return Ok(value);
    }
    let owner = strategy(client, target).await?;
    unique(resources(client, "deployment", Some(text(&owner, "id")?)).await?)
}

pub(super) async fn account(client: &Client, target: &str) -> Result<Value> {
    let mapped = alias(client, target)?;
    let name = mapped.as_deref().unwrap_or(target);
    if let Some(value) = exact_resource(client, name).await? {
        if !matches!(
            value["kind"].as_str(),
            Some("account" | "account_profile" | "binding" | "run")
        ) {
            return Err(invalid(
                "target is not an account directory or runtime resource",
            ));
        }
        return Ok(value);
    }
    if mapped.is_some() {
        return Err(invalid(
            "account alias does not reference an authorized resource ID",
        ));
    }
    let mut matches = Vec::new();
    for kind in ["account", "account_profile", "binding"] {
        matches.extend(
            resources(client, kind, None)
                .await?
                .into_iter()
                .filter(|value| {
                    value["body"]["name"].as_str() == Some(name)
                        || value["body"]["account"].as_str() == Some(name)
                }),
        );
    }
    unique(matches)
}
