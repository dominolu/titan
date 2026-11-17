//! Shared authenticated management API client; transport diagnostics never expose URLs.
use super::contract::API_VERSION;
use anyhow::Result;
use clap::Args;
use reqwest::{Method, Url};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_BODY: usize = 8 * 1024 * 1024;
const MAX_INPUT: u64 = 1024 * 1024;

#[derive(Clone, Default, Args)]
pub struct ClientOptions {
    /// Select a named connection; overrides TITAN_PROFILE and the saved default.
    #[arg(long, global = true)]
    pub profile: Option<String>,
    /// Client TOML file; defaults to TITAN_CLIENT_CONFIG, TITAN_CONFIG or the user config directory.
    #[arg(id = "client_config", long = "client-config", global = true)]
    pub config: Option<PathBuf>,
    /// Override the management base URL; embedded credentials are rejected.
    #[arg(long, global = true)]
    pub url: Option<String>,
    /// Override the management login user for this invocation.
    #[arg(long, global = true)]
    pub username: Option<String>,
    /// Environment variable containing the login secret; never pass the secret itself.
    #[arg(long, global = true)]
    pub password_env: Option<String>,
    /// Restrict queries and actions to one authorized execution node.
    #[arg(long, global = true)]
    pub node: Option<String>,
    /// Filter the authorized environment, for example live or backtest.
    #[arg(long, global = true)]
    pub environment: Option<String>,
    /// Require an existing service; still verify configured local service identity.
    #[arg(long, global = true)]
    pub no_auto_start: bool,
    /// Emit a versioned JSON envelope; diagnostics go to stderr.
    #[arg(long, global = true, conflicts_with = "ndjson")]
    pub json: bool,
    /// Emit one versioned JSON envelope per line for streams.
    #[arg(long, global = true, conflicts_with = "json")]
    pub ndjson: bool,
}
impl std::fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientOptions")
            .field("json", &self.json)
            .field("ndjson", &self.ndjson)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ClientError {
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
    pub status: Option<u16>,
}
impl ClientError {
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            request_id: None,
            status: None,
        }
    }
    pub fn exit_code(&self) -> i32 {
        match self.code.as_str() {
            "INTERRUPTED" => 130,
            "WAIT_TIMEOUT" => 8,
            "OPERATION_FAILED" | "OPERATION_CANCELLED" | "OPERATION_UNKNOWN" => 7,
            "INVALID_INPUT" | "INVALID_CONFIG" | "AMBIGUOUS_TARGET" => 2,
            "STALE_GENERATION"
            | "STALE_HISTORY_CURSOR"
            | "RESYNC_REQUIRED"
            | "VERSION_CONFLICT" => 5,
            _ => match self.status {
                Some(401 | 403) => 3,
                Some(404) => 4,
                Some(409 | 412) => 5,
                Some(400 | 422) => 2,
                _ => 6,
            },
        }
    }
}
pub fn exit_code(error: &anyhow::Error) -> i32 {
    error
        .downcast_ref::<ClientError>()
        .map_or(1, ClientError::exit_code)
}
pub fn render_error(error: &anyhow::Error, json_output: bool) {
    let fallback = ClientError::new(
        "CLI_ERROR",
        "command failed; inspect the command inputs and service availability",
    );
    let error = error.downcast_ref::<ClientError>().unwrap_or(&fallback);
    if json_output {
        let envelope = json!({"schema_version":1,"api_version":API_VERSION,"kind":"error",
            "data":{"code":error.code,"message":error.message,"request_id":error.request_id}});
        let _ = writeln!(std::io::stderr().lock(), "{envelope}");
    } else {
        let _ = writeln!(std::io::stderr().lock(), "{error}");
    }
}
fn invalid(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_CONFIG", message).into()
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path).map_err(|_| invalid("cannot open input file"))?;
    let mut bytes = Vec::new();
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read input file"))?;
    if bytes.len() as u64 > maximum {
        return Err(invalid("input file exceeds the size limit"));
    }
    Ok(bytes)
}
pub fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&read_bounded(path, MAX_INPUT)?)
        .map_err(|_| ClientError::new("INVALID_INPUT", "input file is not valid JSON").into())
}

pub struct Client {
    http: reqwest::Client,
    base: Url,
    origin: String,
    csrf: String,
    service_identity: Option<String>,
    options: ClientOptions,
}
impl Client {
    pub async fn connect(options: &ClientOptions) -> Result<Self> {
        let options = super::frontend_config::resolved_options(options)?;
        let raw_url = options
            .url
            .as_deref()
            .ok_or_else(|| invalid("provide --url or a configured client profile"))?;
        let mut base = Url::parse(raw_url).map_err(|_| invalid("invalid management URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || !matches!(base.path(), "" | "/")
        {
            return Err(invalid(
                "management URL must be an HTTP(S) origin without credentials, path, query or fragment",
            ));
        }
        base.set_path("/");
        let origin = base.origin().ascii_serialization();
        let username = options
            .username
            .as_deref()
            .ok_or_else(|| invalid("provide --username or a configured client profile"))?;
        let password_ref = options
            .password_env
            .as_deref()
            .ok_or_else(|| invalid("provide --password-env or a configured client profile"))?;
        let password = std::env::var(password_ref)
            .map_err(|_| invalid("password environment reference is missing"))?;
        if password.is_empty() {
            return Err(invalid("password environment reference is empty"));
        }
        let http = reqwest::Client::builder()
            .cookie_store(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| invalid("cannot initialize management transport"))?;
        let response = http
            .post(
                base.join("api/v2/session")
                    .map_err(|_| invalid("invalid session route"))?,
            )
            .header("Origin", &origin)
            .json(&json!({"username":username,"password":password}))
            .send()
            .await
            .map_err(transport_error)?;
        let service_identity = response
            .headers()
            .get("x-titan-manager-identity")
            .and_then(|value| value.to_str().ok())
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .map(str::to_owned);
        let session = response_json(response).await?;
        if session.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
            return Err(
                ClientError::new("PROTOCOL_ERROR", "unsupported management API version").into(),
            );
        }
        let csrf = session
            .get("csrf_token")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                ClientError::new("PROTOCOL_ERROR", "session response has no CSRF token")
            })?
            .to_owned();
        Ok(Self {
            http,
            base,
            origin,
            csrf,
            service_identity,
            options,
        })
    }
    pub fn service_identity(&self) -> Option<&str> {
        self.service_identity.as_deref()
    }
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }
    pub fn is_json(&self) -> bool {
        self.options.json || self.options.ndjson
    }
    pub fn node(&self) -> Result<&str> {
        self.options
            .node
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("provide --node or a configured node"))
    }
    pub fn environment(&self) -> Option<&str> {
        self.options.environment.as_deref()
    }
    pub fn print(&self, data: &Value) -> Result<()> {
        self.print_kind("result", data)
    }
    pub fn print_kind(&self, kind: &str, data: &Value) -> Result<()> {
        let envelope =
            json!({"schema_version":1,"api_version":API_VERSION,"kind":kind,"data":data});
        let mut stdout = std::io::stdout().lock();
        if !self.is_json() {
            super::human::write(&mut stdout, kind, data)
                .and_then(|_| stdout.flush())
                .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write command output"))?;
            return Ok(());
        }
        serde_json::to_writer(&mut stdout, &envelope)
            .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write command output"))?;
        writeln!(stdout)
            .and_then(|_| stdout.flush())
            .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write command output"))?;
        Ok(())
    }
    pub async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value> {
        self.request(Method::GET, path, query, None).await
    }
    pub async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, &[], Some(body)).await
    }
    pub async fn patch(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::PATCH, path, &[], Some(body)).await
    }
    pub async fn delete(&self, path: &str) -> Result<Value> {
        self.request(Method::DELETE, path, &[], None).await
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
    ) -> Result<Value> {
        if !(path.starts_with("/api/v2/") || path.starts_with("/api/v1/"))
            || path.contains(['?', '#', '\\'])
        {
            return Err(ClientError::new("INVALID_INPUT", "invalid management API path").into());
        }
        let url = self
            .base
            .join(path)
            .map_err(|_| ClientError::new("INVALID_INPUT", "invalid management API path"))?;
        if url.origin() != self.base.origin() {
            return Err(
                ClientError::new("INVALID_INPUT", "management API path changes origin").into(),
            );
        }
        let write = method != Method::GET;
        let mut request = self
            .http
            .request(method, url)
            .query(query)
            .header("Accept", "application/json");
        if write {
            request = request
                .header("Origin", &self.origin)
                .header("X-CSRF-Token", &self.csrf);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        response_json(request.send().await.map_err(transport_error)?).await
    }
}
fn transport_error(error: reqwest::Error) -> ClientError {
    if error.is_timeout() {
        ClientError::new(
            "TRANSPORT_TIMEOUT",
            "management request timed out; a submitted write may have been accepted",
        )
    } else {
        ClientError::new(
            "TRANSPORT_ERROR",
            "management transport failed; a submitted write may have been accepted",
        )
    }
}
async fn response_json(mut response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    if response
        .content_length()
        .is_some_and(|size| size > MAX_BODY as u64)
    {
        return Err(ClientError::new(
            "RESPONSE_TOO_LARGE",
            "management response exceeds the size limit",
        )
        .into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > MAX_BODY.saturating_sub(bytes.len()) {
            return Err(ClientError::new(
                "RESPONSE_TOO_LARGE",
                "management response exceeds the size limit",
            )
            .into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
        let mut error = ClientError::new("PROTOCOL_ERROR", "management response is not valid JSON");
        error.status = Some(status.as_u16());
        error
    })?;
    if !status.is_success() {
        // Only known protocol codes may cross the CLI boundary. Error bodies can
        // contain reflected input, URLs or credentials even when valid JSON.
        let code = value
            .get("code")
            .and_then(Value::as_str)
            .filter(|code| {
                matches!(
                    *code,
                    "UNAUTHENTICATED"
                        | "FORBIDDEN"
                        | "CSRF_REJECTED"
                        | "INVALID_CREDENTIALS"
                        | "SESSION_EXPIRED"
                        | "RATE_LIMITED"
                        | "INVALID_REQUEST"
                        | "INVALID_INPUT"
                        | "INVALID_SCOPE"
                        | "INVALID_HISTORY_QUERY"
                        | "INVALID_HISTORY_KIND"
                        | "INVALID_RESOURCE"
                        | "INVALID_PATH"
                        | "NOT_FOUND"
                        | "HISTORY_UNAVAILABLE"
                        | "CONFLICT"
                        | "REVISION_CONFLICT"
                        | "VERSION_CONFLICT"
                        | "IDEMPOTENCY_CONFLICT"
                        | "RUN_BUSY"
                        | "BINDING_OCCUPIED"
                        | "NODE_BUSY"
                        | "RECEIPT_UNCERTAIN"
                        | "CAPABILITY_DISABLED"
                        | "STALE_GENERATION"
                        | "STALE_HISTORY_CURSOR"
                        | "RESYNC_REQUIRED"
                        | "ALERT_CHANGED"
                        | "UPLOAD_DISABLED"
                        | "CAPACITY_EXCEEDED"
                        | "RESPONSE_UNAVAILABLE"
                )
            })
            .unwrap_or("HTTP_ERROR");
        let request_id = value
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(str::to_owned);
        let message = match status.as_u16() {
            401 => "authentication required or session expired",
            403 => "management request is not authorized",
            404 => "management record was not found",
            409 | 412 => {
                "management request conflicts with the current revision or idempotency key"
            }
            400 | 422 => "management request was rejected; check the fixed inputs",
            429 => {
                "management request rate or capacity limit reached; wait before retrying authentication or observation"
            }
            _ => "management service rejected the request",
        };
        return Err(ClientError {
            code: code.into(),
            message: message.into(),
            request_id,
            status: Some(status.as_u16()),
        }
        .into());
    }
    Ok(value)
}
