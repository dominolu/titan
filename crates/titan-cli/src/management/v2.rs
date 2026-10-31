//! Strategy-scoped public API. All writes use the existing admission and receipt path.
use super::{
    app::App,
    contract::*,
    fault,
    http::{ApiResult, identity},
};
use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Default, Deserialize)]
pub(super) struct Page {
    offset: Option<usize>,
    limit: Option<usize>,
}
fn scoped(
    app: &App,
    actor: &Identity,
    strategy: &str,
    kind: &str,
    id: &str,
) -> anyhow::Result<Resource> {
    super::checked_id(strategy)?;
    super::checked_id(id)?;
    app.store
        .list_resources_scoped(actor, Some(kind), 0, 1, Some(strategy), Some(id))?
        .into_iter()
        .next()
        .ok_or_else(|| fault(404, "NOT_FOUND", "strategy record not found"))
}

// Project before JSON serialization so private state cannot consume the response
// body limit. HTTP/SSE and resource handlers share the same field rules.
fn public_resource(mut resource: Resource) -> Resource {
    public_response(&mut resource.body);
    resource
}

// HTTP/SSE expose only public metadata. Internal stored records and signed
// strategy files retain recovery layouts and projection bindings unchanged.
pub(super) fn public_response(value: &mut Value) {
    // Persisted SSE payloads predate Resource's current deserialize adapter.
    // Reuse that adapter without changing their IDs, revisions or digests.
    if value.get("node_id").is_some()
        && value.get("body").is_some()
        && let Ok(resource) = serde_json::from_value::<Resource>(value.clone())
    {
        *value = serde_json::to_value(resource).expect("resource serialization");
    }
    match value {
        Value::Array(items) => items.iter_mut().for_each(public_response),
        Value::Object(object) => {
            if object.contains_key("strategy_key")
                && object.contains_key("lifecycle")
                && object.remove("business").is_some()
            {
                for (old, new) in [
                    ("strategy_id", "runtime_strategy_no"),
                    ("generation", "run_generation"),
                    ("artifact_digest", "compiled_strategy_digest"),
                ] {
                    if let Some(source) = object.remove(old) {
                        object.entry(new.to_owned()).or_insert(source);
                    }
                }
                object.insert("business_observation".into(), json!({"available":false,"complete":false,"reason":"historical runtime did not declare a business observation protocol"}));
            }
            if let Some(metadata) = object.get_mut("metadata").and_then(Value::as_object_mut) {
                for name in [
                    "recovery_state_schema",
                    "state_schema",
                    "state_description",
                    "initial_state",
                ] {
                    metadata.remove(name);
                }
                if let Some(observation) = metadata
                    .get_mut("business_observation")
                    .and_then(Value::as_object_mut)
                {
                    observation.remove("projection");
                }
            }
            object.values_mut().for_each(public_response);
        }
        _ => {}
    }
}
pub(super) async fn detail(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(strategy): Path<String>,
) -> ApiResult<Json<StrategySummary>> {
    let actor = identity(&app, &headers, false)?;
    let value =
        tokio::task::spawn_blocking(move || app.store.list_strategies(&actor, 0, usize::MAX))
            .await
            .context("strategy query failed")??
            .into_iter()
            .find(|v| v.id == strategy)
            .ok_or_else(|| fault(404, "NOT_FOUND", "strategy not found"))?;
    Ok(Json(value))
}
async fn list(
    app: Arc<App>,
    headers: HeaderMap,
    strategy: String,
    kind: &'static str,
    page: Page,
) -> ApiResult<Json<Vec<Resource>>> {
    let actor = identity(&app, &headers, false)?;
    super::checked_id(&strategy)?;
    let values = tokio::task::spawn_blocking(move || {
        app.store.list_resources_scoped(
            &actor,
            Some(kind),
            page.offset.unwrap_or(0).min(1_000_000),
            page.limit.unwrap_or(20).clamp(1, 200),
            Some(&strategy),
            None,
        )
    })
    .await
    .context("strategy history query failed")??;
    Ok(Json(values.into_iter().map(public_resource).collect()))
}
async fn versions(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(strategy): Path<String>,
    Query(page): Query<Page>,
) -> ApiResult<Json<Vec<Resource>>> {
    list(app, headers, strategy, "strategy_version", page).await
}
async fn parameters(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(strategy): Path<String>,
    Query(page): Query<Page>,
) -> ApiResult<Json<Vec<Resource>>> {
    list(app, headers, strategy, "parameter_version", page).await
}
async fn runs(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(strategy): Path<String>,
    Query(page): Query<Page>,
) -> ApiResult<Json<Vec<Resource>>> {
    list(app, headers, strategy, "run", page).await
}
async fn version(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, false)?;
    let value = scoped(&app, &actor, &strategy, "strategy_version", &id)?;
    Ok(Json(public_resource(value)))
}
async fn observation_schema(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let actor = identity(&app, &headers, false)?;
    let value = scoped(&app, &actor, &strategy, "strategy_version", &id)?;
    let schema = &value.body["metadata"]["business_observation"]["schema"];
    if schema.is_null() {
        return Ok(Json(
            json!({"available":false,"reason":"business observation not declared"}),
        ));
    }
    Ok(Json(
        json!({"available":true,"schema":schema,"schema_hash":super::digest(&titan_strategy_runtime::compiler_json_v13(schema))}),
    ))
}
fn snapshot_instance(body: &Value) -> Option<&Value> {
    let generation = body
        .get("run_generation")
        .or_else(|| body.get("generation"))?;
    body["observation"]["strategies"]
        .as_array()?
        .iter()
        .find(|item| {
            item["runtime_strategy_no"] == body["runtime_strategy_no"]
                && item
                    .get("run_generation")
                    .or_else(|| item.get("generation"))
                    == Some(generation)
        })
}
async fn run_snapshot(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
) -> ApiResult<Json<StrategyRunSnapshot>> {
    let actor = identity(&app, &headers, false)?;
    let run = scoped(&app, &actor, &strategy, "run", &id)?;
    let body = &run.body;
    let observed = &body["observation"];
    let instance = snapshot_instance(body);
    let unavailable =
        json!({"available":false,"complete":false,"reason":"runtime observation unavailable"});
    let business = instance
        .and_then(|i| i.get("business_observation"))
        .cloned()
        .unwrap_or_else(|| unavailable.clone());
    let mut recovery = instance
        .and_then(|i| i.get("recovery"))
        .cloned()
        .unwrap_or_else(|| unavailable.clone());
    recovery["recovery_point_sha256"] = body
        .get("recovery_point_sha256")
        .or_else(|| body.get("checkpoint_id"))
        .cloned()
        .unwrap_or(Value::Null);
    recovery["recovery_reference"] = body
        .get("recovery_reference")
        .cloned()
        .unwrap_or(Value::Null);
    let runtime = instance
        .map(|item| {
            let mut item = item.clone();
            if let Some(object) = item.as_object_mut() {
                object.remove("business_observation");
                object.remove("recovery");
            }
            item
        })
        .unwrap_or_else(|| unavailable.clone());
    Ok(Json(StrategyRunSnapshot {
        strategy_id: strategy,
        strategy_version_id: body
            .get("strategy_version_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        run_id: id,
        run_generation: body
            .get("run_generation")
            .or_else(|| body.get("generation"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        revision: run.revision,
        sampled_at_ns: observed
            .get("captured_at_ns")
            .and_then(Value::as_str)
            .map(str::to_owned),
        available: body["available"] == true,
        runtime,
        account: observed
            .get("account_snapshot")
            .cloned()
            .unwrap_or_else(|| unavailable.clone()),
        financial: observed
            .get("financial")
            .cloned()
            .unwrap_or_else(|| unavailable.clone()),
        risk: observed["account_snapshot"]["image"]
            .get("risk")
            .cloned()
            .unwrap_or(unavailable),
        business_observation: business,
        recovery,
    }))
}
async fn run_history(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
    Query(query): Query<super::http::ListQuery>,
) -> ApiResult<Json<Value>> {
    let actor = identity(&app, &headers, false)?;
    scoped(&app, &actor, &strategy, "run", &id)?;
    super::http::runtime_query(app, headers, id, query, "history").await
}
async fn run_facts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
    Query(query): Query<super::http::ListQuery>,
) -> ApiResult<Json<Value>> {
    let actor = identity(&app, &headers, false)?;
    scoped(&app, &actor, &strategy, "run", &id)?;
    super::http::runtime_query(app, headers, id, query, "facts").await
}
async fn backtest_facts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((strategy, id)): Path<(String, String)>,
    Query(query): Query<super::http::ListQuery>,
) -> ApiResult<Json<Value>> {
    let actor = identity(&app, &headers, false)?;
    scoped(&app, &actor, &strategy, "backtest", &id)?;
    super::http::runtime_query(app, headers, id, query, "facts").await
}

async fn submit(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(mut input): Json<StrategyOperationRequest>,
) -> ApiResult<Response> {
    let actor = identity(&app, &headers, true)?;
    let requested = input
        .action
        .as_object_mut()
        .and_then(|v| v.remove("strategy_id"));
    let action: Action =
        serde_json::from_value(input.action).context("invalid strategy operation")?;
    if let Some(requested) = requested {
        let requested = requested.as_str().context("strategy_id must be text")?;
        let reference = match &action {
            Action::Compile {
                parameter_version_id,
            } => ("parameter_version", parameter_version_id.clone()),
            Action::Backtest {
                strategy_version_id,
                ..
            }
            | Action::Deploy {
                strategy_version_id,
                ..
            } => ("strategy_version", strategy_version_id.clone()),
            Action::Pause { run_id, .. }
            | Action::Resume { run_id, .. }
            | Action::Stop { run_id, .. }
            | Action::Restore { run_id, .. } => ("run", run_id.clone()),
            Action::Start { deployment_id } => ("deployment", deployment_id.clone()),
            Action::Report { backtest_id } => ("backtest", backtest_id.clone()),
            Action::Describe {
                parameter_preset_id,
                ..
            } => (
                "parameter_preset",
                format!(
                    "{}-{}-{parameter_preset_id}",
                    input.node_id,
                    super::legacy::id_tag("parameter_preset")
                ),
            ),
            Action::Prepare { .. } => {
                return Err(fault(
                    400,
                    "INVALID_SCOPE",
                    "shared dataset preparation has no strategy owner",
                )
                .into());
            }
        };
        scoped(&app, &actor, requested, reference.0, &reference.1)?;
    }

    let operation = app.submit(
        &actor,
        SubmitOperation {
            idempotency_key: input.idempotency_key,
            node_id: input.node_id,
            action,
        },
    )?;
    Ok((StatusCode::ACCEPTED, Json(operation)).into_response())
}
pub(super) fn routes() -> Router<Arc<App>> {
    use super::http;
    Router::new()
        .route(
            "/api/v2/session",
            get(http::session).post(http::login).delete(http::logout),
        )
        .route("/api/v2/snapshot", get(http::snapshot))
        .route("/api/v2/strategies", get(http::strategy))
        .route("/api/v2/strategies/{strategy_id}", get(detail))
        .route(
            "/api/v2/strategies/{strategy_id}/parameter-versions",
            get(parameters),
        )
        .route("/api/v2/strategies/{strategy_id}/versions", get(versions))
        .route(
            "/api/v2/strategies/{strategy_id}/versions/{id}",
            get(version),
        )
        .route(
            "/api/v2/strategies/{strategy_id}/versions/{id}/business-observation-schema",
            get(observation_schema),
        )
        .route("/api/v2/strategies/{strategy_id}/runs", get(runs))
        .route(
            "/api/v2/strategies/{strategy_id}/runs/{id}/snapshot",
            get(run_snapshot),
        )
        .route(
            "/api/v2/strategies/{strategy_id}/runs/{id}/history",
            get(run_history),
        )
        .route(
            "/api/v2/strategies/{strategy_id}/runs/{id}/facts",
            get(run_facts),
        )
        .route(
            "/api/v2/strategies/{strategy_id}/backtests/{id}/facts",
            get(backtest_facts),
        )
        .route("/api/v2/resources/{id}/facts", get(http::resource_facts))
        .route("/api/v2/operations", get(http::operations).post(submit))
        .route("/api/v2/operations/{id}", get(http::operation))
        .route("/api/v2/operations/{id}/cancel", post(http::cancel))
        .route("/api/v2/parameter-versions", post(http::create_version))
        .route(
            "/api/v2/parameter-versions/{id}",
            get(http::resource).patch(http::update_version),
        )
        .route("/api/v2/resources", get(http::resources))
        .route("/api/v2/resources/{id}", get(http::resource))
        .route(
            "/api/v2/resources/{id}/history",
            get(http::resource_history),
        )
        .route("/api/v2/resources/{id}/log", get(http::resource_log))
        .route(
            "/api/v2/resources/{id}/acknowledge",
            post(http::acknowledge),
        )
        .route("/api/v2/events", get(http::events))
        .route("/api/v2/files/{id}/{*path}", get(http::file))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn run_snapshot_selects_both_strategy_number_and_run_generation() {
        let mut body = json!({"runtime_strategy_no":"77","run_generation":"2","observation":{"strategies":[
            {"runtime_strategy_no":"77","run_generation":"1","marker":"old"},
            {"runtime_strategy_no":"78","run_generation":"2","marker":"other"},
            {"runtime_strategy_no":"77","run_generation":"2","marker":"current"}
        ]}});
        assert_eq!(snapshot_instance(&body).unwrap()["marker"], "current");
        body["run_generation"] = json!("3");
        assert!(snapshot_instance(&body).is_none());
        body.as_object_mut().unwrap().remove("run_generation");
        assert!(snapshot_instance(&body).is_none());
        body["generation"] = json!("2");
        assert_eq!(snapshot_instance(&body).unwrap()["marker"], "current");
    }
    #[test]
    fn historical_streams_use_canonical_names_without_exporting_private_state() {
        let mut value = json!({"payload":{"id":"old-artifact-id","kind":"artifact","node_id":"n","revision":"2","frozen":true,"digest":"original","body":{"research_id":"grid","artifact_digest":"signed","observation":{"strategies":[{"strategy_key":"grid","lifecycle":"Running","strategy_id":"77","generation":"1","business":{"data":{"grid":{"internal":1}}}}]}}}});
        public_response(&mut value);
        let resource = &value["payload"];
        assert_eq!(resource["kind"], "strategy_version");
        assert_eq!(resource["body"]["strategy_id"], "grid");
        assert_eq!(resource["id"], "old-artifact-id");
        assert_eq!(resource["digest"], "original");
        let instance = &resource["body"]["observation"]["strategies"][0];
        assert!(instance.get("business").is_none());
        assert_eq!(instance["runtime_strategy_no"], "77");
        assert_eq!(instance["business_observation"]["available"], false);
    }
    #[test]
    fn public_stream_and_snapshot_metadata_exclude_private_recovery_bindings() {
        let mut value = json!({"resource":{"id":"unchanged","digest":"signed","body":{"metadata":{"recovery_state_schema":{"fields":[]},"initial_state":[0],"business_observation":{"schema":{"schema_id":"titan.grid"},"projection":[{"source":"grid.anchor"}]}}}}});
        public_response(&mut value);
        let resource = &value["resource"];
        assert_eq!(resource["id"], "unchanged");
        assert_eq!(resource["digest"], "signed");
        let metadata = &resource["body"]["metadata"];
        assert!(metadata.get("recovery_state_schema").is_none());
        assert!(metadata.get("initial_state").is_none());
        assert!(metadata["business_observation"].get("projection").is_none());
        assert_eq!(
            metadata["business_observation"]["schema"]["schema_id"],
            "titan.grid"
        );
    }
}
