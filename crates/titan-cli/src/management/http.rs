use super::{Fault, app::App, config::ManagerConfig, contract::*, digest, fault, nonce, now_ns};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event as SseEvent, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc, time::Duration};

pub(crate) struct Error(anyhow::Error);
impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, code, message) = if let Some(error) = self.0.downcast_ref::<Fault>() {
            (
                StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_REQUEST),
                error.code,
                error.message.clone(),
            )
        } else {
            tracing::warn!(error=%self.0,"management request rejected");
            (
                StatusCode::BAD_REQUEST,
                "INVALID_REQUEST",
                self.0.to_string(),
            )
        };
        (
            status,
            Json(ApiError {
                code: code.into(),
                message,
                request_id: nonce(),
            }),
        )
            .into_response()
    }
}
pub(super) type ApiResult<T> = std::result::Result<T, Error>;
pub(crate) async fn json_errors(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let api = request.uri().path().starts_with("/api/") || request.uri().path().starts_with("/v1/");
    let response = next.run(request).await;
    if api
        && (response.status().is_client_error() || response.status().is_server_error())
        && !response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"application/json"))
    {
        return (
            response.status(),
            Json(ApiError {
                code: "INVALID_REQUEST".into(),
                message: "request did not match the API contract".into(),
                request_id: nonce(),
            }),
        )
            .into_response();
    }
    response
}
async fn management_response(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    let legacy = path.starts_with("/api/v1/") && !path.starts_with("/api/v1/files/");
    let public = path.starts_with("/api/v2/") && !path.starts_with("/api/v2/files/");
    let response = next.run(request).await;
    if !(legacy || public)
        || !response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"application/json"))
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let result = async {
        let bytes = axum::body::to_bytes(body, 8 * 1024 * 1024).await?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if legacy {
            super::legacy::response(&mut value);
        } else {
            super::v2::public_response(&mut value);
        }
        anyhow::Ok(serde_json::to_vec(&value)?)
    }
    .await;
    match result {
        Ok(bytes) => {
            parts.headers.remove(header::CONTENT_LENGTH);
            Response::from_parts(parts, axum::body::Body::from(bytes))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                code: "RESPONSE_UNAVAILABLE".into(),
                message: "use the strategy API v2".into(),
                request_id: nonce(),
            }),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub(super) struct Login {
    username: String,
    password: String,
}
#[derive(Default, Deserialize)]
pub(super) struct ListQuery {
    cursor: Option<String>,
    newest_first: Option<bool>,
    limit: Option<usize>,
    idempotency_key: Option<String>,
    offset: Option<usize>,
    kind: Option<String>,
    from_ns: Option<String>,
    to_ns: Option<String>,
    run_id: Option<String>,
    #[serde(alias = "run_generation", alias = "worker_session_id")]
    generation: Option<String>,
    #[serde(alias = "research_id")]
    strategy_id: Option<String>,
    resource_id: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct StreamQuery {
    cursor: Option<String>,
}

pub fn manager_router(config: ManagerConfig) -> Result<Router> {
    let app = App::open(config)?;
    app.spawn();
    Ok(Router::new()
        .route("/api/v1/session", get(session).post(login).delete(logout))
        .route("/api/v1/snapshot", get(snapshot))
        .route("/api/v1/operations", get(operations).post(submit))
        .route("/api/v1/operations/{id}", get(operation))
        .route("/api/v1/operations/{id}/cancel", post(cancel))
        .route("/api/v1/parameter-versions", post(create_version))
        .route(
            "/api/v1/parameter-versions/{id}",
            get(resource).patch(update_version),
        )
        .route("/api/v1/resources/{id}", get(resource))
        .route("/api/v1/resources/{id}/history", get(resource_history))
        .route("/api/v1/resources/{id}/log", get(resource_log))
        .route("/api/v1/resources/{id}/acknowledge", post(acknowledge))
        .route("/api/v1/resources", get(resources))
        .route("/api/v1/strategies", get(resources))
        .route("/api/v1/backtests", get(resources))
        .route("/api/v1/runs", get(resources))
        .route("/api/v1/accounts/{id}", get(resource))
        .route("/api/v1/files/{id}/{*path}", get(file))
        .route("/api/v1/events", get(events))
        .merge(super::v2::routes())
        .route("/api/v1/research", get(strategy))
        .route("/api/v1/artifacts", get(resources))
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(axum::middleware::from_fn(json_errors))
        .layer(axum::middleware::from_fn(management_response))
        .fallback(static_file)
        .with_state(app))
}
pub async fn serve_manager(config: ManagerConfig) -> Result<()> {
    let listen = config.listen.clone();
    let router = manager_router(config)?;
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(%listen,"management service listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
fn token(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(';')
                .map(str::trim)
                .find_map(|part| part.strip_prefix("titan_session="))
        })
        .ok_or_else(|| fault(401, "UNAUTHENTICATED", "authentication required"))
}
pub(super) fn identity(app: &App, headers: &HeaderMap, write: bool) -> Result<Identity> {
    let (identity, csrf, _) = app.authenticate(token(headers)?)?;
    if write {
        let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
        let submitted = headers
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if origin != Some(app.config.public_origin.as_str()) || digest(submitted.as_bytes()) != csrf
        {
            return Err(fault(
                403,
                "CSRF_REJECTED",
                "request origin or CSRF token is invalid",
            ));
        }
    }
    Ok(identity)
}
pub(super) async fn login(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Login>,
) -> ApiResult<Response> {
    if headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())
        != Some(app.config.public_origin.as_str())
    {
        return Err(fault(403, "CSRF_REJECTED", "invalid login origin").into());
    }
    let verify = app.clone();
    let (session, token) =
        tokio::task::spawn_blocking(move || verify.login(&input.username, &input.password))
            .await
            .map_err(anyhow::Error::from)??;
    let cookie = format!(
        "titan_session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        app.config.session_seconds,
        if app.config.secure_cookie {
            "; Secure"
        } else {
            ""
        }
    );
    Ok((
        [
            (header::SET_COOKIE, cookie),
            (header::CACHE_CONTROL, "no-store".into()),
        ],
        Json(session),
    )
        .into_response())
}
pub(super) async fn session(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let raw = token(&headers)?;
    let (identity, _, expires) = app.authenticate(raw)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(Session {
            identity,
            csrf_token: digest(format!("csrf:{raw}").as_bytes()),
            expires_at_ns: expires,
            api_version: API_VERSION,
            build: super::build_id().into(),
        }),
    )
        .into_response())
}
pub(super) async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Response> {
    let actor = identity(&app, &headers, true)?;
    app.store.session_remove(token(&headers)?)?;
    app.store.audit(&actor.username, "logout", "session", "")?;
    Ok((
        [(
            header::SET_COOKIE,
            "titan_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
        )],
        StatusCode::NO_CONTENT,
    )
        .into_response())
}
pub(super) async fn snapshot(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Snapshot>> {
    let actor = identity(&app, &headers, false)?;
    Ok(Json(app.store.snapshot(
        &actor,
        query.limit.unwrap_or(100).clamp(1, 200),
    )?))
}
pub(super) async fn operations(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let actor = identity(&app, &headers, false)?;
    if let Some(key) = query.idempotency_key {
        let op = app.store.by_key(&actor.username, &key)?.map(|(op, _)| op);
        if let Some(op) = &op {
            app.operation(&actor, &op.id, false)?;
        }
        Ok(Json(serde_json::to_value(op).map_err(anyhow::Error::from)?))
    } else {
        let from = query
            .from_ns
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(anyhow::Error::from)?;
        let to = query
            .to_ns
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(anyhow::Error::from)?;
        Ok(Json(
            serde_json::to_value(app.store.list_operations(
                &actor,
                query.offset.unwrap_or(0).min(1_000_000),
                query.limit.unwrap_or(100).clamp(1, 200),
                from,
                to,
                query.run_id.as_deref(),
            )?)
            .map_err(anyhow::Error::from)?,
        ))
    }
}
pub(super) async fn strategy(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<StrategySummary>>> {
    let actor = identity(&app, &headers, false)?;
    let result = tokio::task::spawn_blocking(move || {
        app.store.list_strategies(
            &actor,
            query.offset.unwrap_or(0).min(1_000_000),
            query.limit.unwrap_or(20).clamp(1, 200),
        )
    })
    .await
    .context("strategy query worker failed")??;
    Ok(Json(result))
}
pub(super) async fn resources(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    uri: axum::extract::OriginalUri,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<Resource>>> {
    let actor = identity(&app, &headers, false)?;
    let kind = match uri.0.path() {
        "/api/v1/strategies" => Some("parameter_preset"),
        "/api/v1/compiled_strategies" | "/api/v1/artifacts" => Some("strategy_version"),
        "/api/v1/backtests" => Some("backtest"),
        "/api/v1/runs" => Some("run"),
        _ => query.kind.as_deref(),
    };
    if let Some(id) = &query.strategy_id {
        super::checked_id(id)?;
    }
    let offset = query.offset.unwrap_or(0).min(1_000_000);
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let kind = kind.map(|value| super::legacy::kind(value).to_owned());
    let result = tokio::task::spawn_blocking(move || {
        if query.strategy_id.is_none() && query.resource_id.is_none() {
            return app
                .store
                .list_resources(&actor, kind.as_deref(), offset, limit);
        }
        app.store.list_resources_scoped(
            &actor,
            kind.as_deref(),
            offset,
            limit,
            query.strategy_id.as_deref(),
            query.resource_id.as_deref(),
        )
    })
    .await
    .context("resource query worker failed")??;
    Ok(Json(result))
}
pub(super) async fn resource(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, false)?;
    Ok(Json(app.resource(&actor, &id, false)?))
}
#[derive(Deserialize)]
pub(super) struct Acknowledge {
    expected_revision: String,
    condition_identity: String,
}
pub(super) async fn acknowledge(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<Acknowledge>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    let mut resource = app.resource(&actor, &id, true)?;
    if resource.kind != "alert"
        || resource.body["condition_identity"].as_str() != Some(input.condition_identity.as_str())
    {
        return Err(fault(409, "ALERT_CHANGED", "alert condition changed").into());
    }
    resource.body["acknowledgement"] = serde_json::json!({"actor":actor.username,"at_ns":now_ns(),"condition_identity":input.condition_identity});
    let saved = app.store.save_resource_audited(
        resource,
        Some(&input.expected_revision),
        &actor.username,
        "acknowledge_alert",
    )?;
    Ok(Json(saved))
}
pub(super) async fn resource_history(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    runtime_query(app, headers, id, query, "history").await
}
pub(super) async fn resource_facts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    runtime_query(app, headers, id, query, "facts").await
}
pub(super) async fn resource_log(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    runtime_query(app, headers, id, query, "log").await
}
pub(super) async fn runtime_query(
    app: Arc<App>,
    headers: HeaderMap,
    id: String,
    query: ListQuery,
    endpoint: &str,
) -> ApiResult<Json<serde_json::Value>> {
    let actor = identity(&app, &headers, false)?;
    let resource = app.resource(&actor, &id, false)?;
    if !(matches!(resource.kind.as_str(), "run" | "account")
        || endpoint == "facts" && resource.kind == "backtest")
    {
        return Err(fault(400, "INVALID_RESOURCE", "resource has no runtime history").into());
    }
    if let Some(generation) = query.generation
        && resource.body["run_generation"]
            .as_str()
            .or_else(|| resource.body["worker_session_id"].as_str())
            != Some(generation.as_str())
    {
        return Err(fault(409, "STALE_GENERATION", "runtime source generation changed").into());
    }
    let worker = resource.body["worker_id"]
        .as_str()
        .or_else(|| {
            if endpoint == "facts" && resource.kind == "backtest" {
                resource.body["operation_id"].as_str()
            } else {
                None
            }
        })
        .context("resource has no worker identity")?;
    super::checked_id(worker)?;
    let node = app
        .config
        .nodes
        .get(&resource.node_id)
        .context("unknown runtime node")?;
    let response = app
        .client
        .get(format!(
            "{}/v1/runs/{worker}/{endpoint}",
            node.endpoint.trim_end_matches('/')
        ))
        .bearer_auth(node.token()?)
        .query(&[
            ("cursor", query.cursor.unwrap_or_default()),
            (
                "newest_first",
                query.newest_first.unwrap_or(false).to_string(),
            ),
            ("kind", query.kind.unwrap_or_default()),
            (
                "offset",
                query.offset.unwrap_or(0).min(8_388_608).to_string(),
            ),
            ("limit", query.limit.unwrap_or(50).clamp(1, 200).to_string()),
        ])
        .send()
        .await
        .map_err(anyhow::Error::from)?;
    if endpoint == "facts" && response.status() == reqwest::StatusCode::CONFLICT {
        return Err(fault(
            409,
            "STALE_HISTORY_CURSOR",
            "历史查询范围已变化，请重新查询",
        )
        .into());
    }
    let response = response.error_for_status().map_err(anyhow::Error::from)?;
    Ok(Json(super::app::node_json(response).await?))
}
pub(super) async fn operation(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Operation>> {
    let actor = identity(&app, &headers, false)?;
    Ok(Json(app.operation(&actor, &id, false)?))
}
async fn submit(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<SubmitOperation>,
) -> ApiResult<Response> {
    let actor = identity(&app, &headers, true)?;
    Ok((StatusCode::ACCEPTED, Json(app.submit(&actor, input)?)).into_response())
}
pub(super) async fn cancel(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Operation>> {
    let actor = identity(&app, &headers, true)?;
    app.operation(&actor, &id, true)?;
    app.store.audit(&actor.username, "cancel", &id, "")?;
    Ok(Json(app.store.cancel(&id)?))
}
pub(super) async fn create_version(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<SaveVersion>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    Ok(Json(app.save_version(&actor, None, input)?))
}
pub(super) async fn update_version(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<SaveVersion>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    Ok(Json(app.save_version(&actor, Some(&id), input)?))
}

pub(super) async fn events(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
) -> ApiResult<Sse<impl futures_util::Stream<Item = std::result::Result<SseEvent, Infallible>>>> {
    let _ = identity(&app, &headers, false)?;
    let session_token = token(&headers)?.to_owned();
    let mut cursor = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or(query.cursor)
        .unwrap_or_default();
    let legacy = uri.path().starts_with("/api/v1/");
    let stream = async_stream::stream! {
        loop {
            let actor=match app.authenticate(&session_token) {Ok((actor,_,_))=>actor,Err(_)=>{yield Ok(SseEvent::default().event("auth_expired").data("{}"));break;}};
            match app.store.events(&actor,&cursor) {
                Ok((items,next))=>{
                    for item in items {
                        let mut value=serde_json::to_value(&item).unwrap_or_default();
                        if legacy {super::legacy::response(&mut value);} else {super::v2::public_response(&mut value);}
                        let data=serde_json::to_string(&value).unwrap_or_default();
                        yield Ok(SseEvent::default().event("update").id(&item.cursor).data(data));
                    }
                    cursor=next;
                    yield Ok(SseEvent::default().event("watermark").id(&cursor).data(now_ns()));
                }
                Err(_)=>{yield Ok(SseEvent::default().event("resync_required").data("{}"));break;}
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10))))
}
pub(super) async fn file(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((id, path)): Path<(String, String)>,
) -> ApiResult<Response> {
    let actor = identity(&app, &headers, false)?;
    let node_id = app.file_node(&actor, &id, &path)?;
    let node = app.config.nodes.get(&node_id).context("unknown node")?;
    let mut url = reqwest::Url::parse(&format!(
        "{}/v1/files/",
        node.endpoint.trim_end_matches('/')
    ))
    .map_err(anyhow::Error::from)?;
    {
        let mut parts = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid node URL"))?;
        parts.pop_if_empty();
        parts.push(&id);
        for part in path.split('/') {
            if part == ".." || part.is_empty() {
                return Err(fault(400, "INVALID_PATH", "invalid file reference").into());
            }
            parts.push(part);
        }
    }
    let response = app
        .client
        .get(url)
        .bearer_auth(node.token()?)
        .send()
        .await
        .map_err(anyhow::Error::from)?
        .error_for_status()
        .map_err(anyhow::Error::from)?;
    let kind = response.headers().get(header::CONTENT_TYPE).cloned();
    let mut output = Response::new(axum::body::Body::from_stream(response.bytes_stream()));
    if let Some(kind) = kind {
        output.headers_mut().insert(header::CONTENT_TYPE, kind);
    }
    output.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static(
            "sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'",
        ),
    );
    output.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    Ok(output)
}
async fn static_file(State(app): State<Arc<App>>, request: axum::extract::Request) -> Response {
    let path = request.uri().path().trim_start_matches('/');
    if path == "api" || path.starts_with("api/") {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                code: "NOT_FOUND".into(),
                message: "API route not found".into(),
                request_id: nonce(),
            }),
        )
            .into_response();
    }
    if request.method() != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let relative = if path.is_empty() { "index.html" } else { path };
    let file = super::inside(&app.config.web_root, relative).or_else(|error| {
        if path.starts_with("assets/") || path.contains('.') {
            Err(error)
        } else {
            super::inside(&app.config.web_root, "index.html")
        }
    });
    let Ok(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = match file.extension().and_then(|v| v.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "json" => "application/json",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    };
    match tokio::fs::File::open(file).await {
        Ok(file) => (
            [
                (header::CONTENT_TYPE, content_type),
                (
                    header::CACHE_CONTROL,
                    if path.starts_with("assets/") || path.starts_with("static/") {
                        "public, max-age=31536000, immutable"
                    } else {
                        "no-cache"
                    },
                ),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    #[tokio::test]
    async fn version_page_projects_private_state_before_response_limit() {
        use super::super::{config::User, store::Store};
        use argon2::{Argon2, PasswordHasher, password_hash::SaltString};

        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("manager.db");
        let store = Store::open(&database).unwrap();
        store
            .session_create("reader", "test-token", "csrf", "9223372036854775807")
            .unwrap();
        let mut digests = std::collections::BTreeMap::new();
        let mut unprojected_bytes = 0;
        for i in 0..65 {
            let resource = store.save_resource(Resource {
                id: format!("version-{i}"), kind: "strategy_version".into(), node_id: "build".into(),
                revision: String::new(), frozen: true, digest: String::new(),
                body: json!({"strategy_id":"sample","environment":"backtest","metadata":{
                    "initial_state":vec![0_u8;65536],"recovery_state_schema":{"fields":[]},
                    "state_schema":{},"state_description":{},
                    "business_observation":{"schema":{"schema_id":"public"},"projection":[{"source":"private"}]}
                }}),
            }, None).unwrap();
            unprojected_bytes += serde_json::to_vec(&resource).unwrap().len();
            digests.insert(resource.id, resource.digest);
        }
        assert!(unprojected_bytes > 8 * 1024 * 1024);
        let hash = Argon2::default()
            .hash_password(
                b"test-password",
                &SaltString::generate(&mut rand::rngs::OsRng),
            )
            .unwrap()
            .to_string();
        let app = manager_router(ManagerConfig {
            listen: "127.0.0.1:0".into(),
            public_origin: "http://127.0.0.1:3200".into(),
            database,
            web_root: temp.path().into(),
            secure_cookie: false,
            session_seconds: 3600,
            nodes: Default::default(),
            users: vec![User {
                username: "reader".into(),
                password_hash: hash,
                role: Role::Reader,
                nodes: vec!["build".into()],
                accounts: vec![],
                environments: vec!["backtest".into()],
            }],
        })
        .unwrap();
        for path in [
            "/api/v2/strategies/sample/versions?limit=65",
            "/api/v2/strategies/sample/versions/version-0",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::COOKIE, "titan_session=test-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
                .await
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let items = if let Some(items) = value.as_array() {
                assert_eq!(items.len(), 65);
                items.clone()
            } else {
                vec![value]
            };
            for item in items {
                assert_eq!(
                    item["digest"].as_str(),
                    Some(digests[item["id"].as_str().unwrap()].as_str())
                );
                let metadata = &item["body"]["metadata"];
                for field in [
                    "initial_state",
                    "recovery_state_schema",
                    "state_schema",
                    "state_description",
                ] {
                    assert!(metadata.get(field).is_none(), "{path}: {field}");
                }
                assert!(metadata["business_observation"].get("projection").is_none());
                assert_eq!(
                    metadata["business_observation"]["schema"]["schema_id"],
                    "public"
                );
            }
        }
        let stored = store.resource("version-0").unwrap();
        assert_eq!(
            stored.body["metadata"]["initial_state"]
                .as_array()
                .unwrap()
                .len(),
            65536
        );
        assert_eq!(stored.digest, digests["version-0"]);
    }

    #[tokio::test]
    async fn public_json_boundary_filters_detail_and_list_but_preserves_download_bytes() {
        let resource = json!({"id":"original", "digest":"signed", "body":{"metadata":{
            "recovery_state_schema":{}, "state_schema":{}, "state_description":{},
            "initial_state":[1], "business_observation":{
                "schema":{"schema_id":"public"}, "projection":[{"source":"private"}]
            }
        }}});
        let download_bytes = serde_json::to_vec_pretty(&resource).unwrap();
        let app =
            Router::new()
                .route(
                    "/api/v2/detail",
                    get({
                        let resource = resource.clone();
                        move || async move { Json(resource) }
                    }),
                )
                .route(
                    "/api/v2/list",
                    get({
                        let resource = resource.clone();
                        move || async move { Json(vec![resource]) }
                    }),
                )
                .route(
                    "/api/v2/files/original/manifest.json",
                    get({
                        let download_bytes = download_bytes.clone();
                        move || async move {
                            ([(header::CONTENT_TYPE, "application/json")], download_bytes)
                        }
                    }),
                )
                .layer(axum::middleware::from_fn(management_response));
        for path in [
            "/api/v2/detail",
            "/api/v2/list",
            "/api/v2/files/original/manifest.json",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            if path.starts_with("/api/v2/files/") {
                assert_eq!(bytes.as_ref(), download_bytes.as_slice());
                continue;
            }
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let item = if value.is_array() { &value[0] } else { &value };
            assert_eq!(item["id"], "original");
            assert_eq!(item["digest"], "signed");
            let metadata = &item["body"]["metadata"];
            for field in [
                "recovery_state_schema",
                "state_schema",
                "state_description",
                "initial_state",
            ] {
                assert!(metadata.get(field).is_none(), "{path}: {field}");
            }
            assert!(metadata["business_observation"].get("projection").is_none());
            assert_eq!(
                metadata["business_observation"]["schema"]["schema_id"],
                "public"
            );
        }
    }
}
