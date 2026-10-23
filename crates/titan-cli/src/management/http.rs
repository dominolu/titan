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
type ApiResult<T> = std::result::Result<T, Error>;
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
#[derive(Deserialize)]
struct Login {
    username: String,
    password: String,
}
#[derive(Default, Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    idempotency_key: Option<String>,
    offset: Option<usize>,
    kind: Option<String>,
    from_ns: Option<String>,
    to_ns: Option<String>,
    run_id: Option<String>,
}
#[derive(Deserialize)]
struct StreamQuery {
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
        .route("/api/v1/resources", get(resources))
        .route("/api/v1/strategies", get(resources))
        .route("/api/v1/artifacts", get(resources))
        .route("/api/v1/backtests", get(resources))
        .route("/api/v1/runs", get(resources))
        .route("/api/v1/accounts/{id}", get(resource))
        .route("/api/v1/files/{id}/{*path}", get(file))
        .route("/api/v1/events", get(events))
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(axum::middleware::from_fn(json_errors))
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
fn identity(app: &App, headers: &HeaderMap, write: bool) -> Result<Identity> {
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
async fn login(
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
async fn session(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Response> {
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
async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Response> {
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
async fn snapshot(
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
async fn operations(
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
async fn resources(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    uri: axum::extract::OriginalUri,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<Resource>>> {
    let actor = identity(&app, &headers, false)?;
    let kind = match uri.0.path() {
        "/api/v1/strategies" => Some("template"),
        "/api/v1/artifacts" => Some("artifact"),
        "/api/v1/backtests" => Some("backtest"),
        "/api/v1/runs" => Some("run"),
        _ => query.kind.as_deref(),
    };
    Ok(Json(app.store.list_resources(
        &actor,
        kind,
        query.offset.unwrap_or(0).min(1_000_000),
        query.limit.unwrap_or(100).clamp(1, 200),
    )?))
}
async fn resource(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, false)?;
    Ok(Json(app.resource(&actor, &id, false)?))
}
async fn operation(
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
async fn cancel(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Operation>> {
    let actor = identity(&app, &headers, true)?;
    app.operation(&actor, &id, true)?;
    app.store.audit(&actor.username, "cancel", &id, "")?;
    Ok(Json(app.store.cancel(&id)?))
}
async fn create_version(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<SaveVersion>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    Ok(Json(app.save_version(&actor, None, input)?))
}
async fn update_version(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<SaveVersion>,
) -> ApiResult<Json<Resource>> {
    let actor = identity(&app, &headers, true)?;
    Ok(Json(app.save_version(&actor, Some(&id), input)?))
}

async fn events(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> ApiResult<Sse<impl futures_util::Stream<Item = std::result::Result<SseEvent, Infallible>>>> {
    let _ = identity(&app, &headers, false)?;
    let session_token = token(&headers)?.to_owned();
    let mut cursor = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or(query.cursor)
        .unwrap_or_default();
    let stream = async_stream::stream! {
        loop {
            let actor=match app.authenticate(&session_token) {Ok((actor,_,_))=>actor,Err(_)=>{yield Ok(SseEvent::default().event("auth_expired").data("{}"));break;}};
            match app.store.events(&actor,&cursor) {
                Ok((items,next))=>{
                    for item in items {
                        let data=serde_json::to_string(&item).unwrap_or_default();
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
async fn file(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((id, path)): Path<(String, String)>,
) -> ApiResult<Response> {
    let actor = identity(&app, &headers, false)?;
    let op = app.operation(&actor, &id, false)?;
    let node = app.config.nodes.get(&op.node_id).context("unknown node")?;
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
