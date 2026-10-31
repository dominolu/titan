//! Lossless, operation-specific access to official venue APIs beyond the portable BrokerApi.
//! Native monetary values keep their documented JSON types; no f64/unit conversion is applied.
use crate::{
    api::{ApiError, SubmitGuard, check_submit_guard},
    venue::{error, invalid},
};
use reqwest::{Method, Url};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
pub struct NativeRequest {
    pub path: Map<String, Value>,
    pub query: Map<String, Value>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}
impl NativeRequest {
    pub fn query(value: Value) -> Result<Self, ApiError> {
        Ok(Self {
            query: value
                .as_object()
                .cloned()
                .ok_or_else(|| invalid("native", "query must be an object"))?,
            ..Self::default()
        })
    }
    pub fn body(value: Value) -> Self {
        Self {
            body: Some(value),
            ..Self::default()
        }
    }
    pub fn with_path(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.path.insert(name.into(), value.into());
        self
    }
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }
}

/// The response remains streamable (not limited to JSON, important for CSV/export endpoints).
pub struct NativeResponse {
    pub operation: &'static str,
    venue: &'static str,
    write: bool,
    response: reqwest::Response,
}
impl NativeResponse {
    pub fn status(&self) -> u16 {
        self.response.status().as_u16()
    }
    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        self.response.headers()
    }
    pub fn into_response(self) -> reqwest::Response {
        self.response
    }
    pub async fn json(self) -> Result<Value, ApiError> {
        self.response.json().await.map_err(|_| {
            if self.write {
                ApiError::transport(
                    self.venue,
                    "write response undecodable; reconcile before retry",
                )
            } else {
                error(self.venue, "native response is not JSON")
            }
        })
    }
    pub async fn bytes(self) -> Result<Vec<u8>, ApiError> {
        self.response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|_| ApiError::transport(self.venue, "native response body interrupted"))
    }
}

pub struct Operation {
    pub id: &'static str,
    pub method: &'static str,
    pub path: &'static str,
    pub instruction: Option<&'static str>,
    pub source: &'static str,
    pub default_server: &'static str,
    pub family: &'static str,
    pub document: &'static Value,
    pub definition: &'static Value,
}
impl Operation {
    pub fn is_write(&self) -> bool {
        !matches!(self.method, "GET" | "HEAD")
    }
    pub fn parameters(&self) -> impl Iterator<Item = &Value> {
        self.definition["parameters"]
            .as_array()
            .into_iter()
            .flatten()
    }
}

pub(crate) struct Prepared {
    pub url: Url,
    pub query: Vec<(String, String)>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub encoding: Option<String>,
}
fn scalar(v: &Value, venue: &'static str) -> Result<String, ApiError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(invalid(venue, "expected native scalar")),
    }
}
fn segment(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
fn resolve<'a>(document: &'a Value, schema: &'a Value) -> Result<&'a Value, ()> {
    match schema["$ref"].as_str() {
        Some(reference) => document
            .pointer(reference.strip_prefix('#').ok_or(())?)
            .ok_or(()),
        None => Ok(schema),
    }
}
// Deliberately a structural validator, not a claim to implement every JSON Schema vocabulary.
// Required fields, native JSON types, alternatives, enums, numeric bounds and sizes are checked.
fn validate(document: &Value, schema: &Value, value: &Value, depth: usize) -> Result<(), ()> {
    if depth > 64 {
        return Err(());
    }
    let schema = resolve(document, schema)?;
    if value.is_null() && schema["nullable"] == true {
        return Ok(());
    }
    for (key, exact) in [("oneOf", true), ("anyOf", false)] {
        if let Some(options) = schema[key].as_array() {
            let matches = options
                .iter()
                .filter(|s| validate(document, s, value, depth + 1).is_ok())
                .count();
            if matches == 0 || exact && matches != 1 {
                return Err(());
            }
        }
    }
    if let Some(parts) = schema["allOf"].as_array() {
        for part in parts {
            validate(document, part, value, depth + 1)?;
        }
    }
    if let Some(options) = schema["enum"].as_array() {
        // Some official Lighter schemas declare integer enums as JSON strings. Validate against
        // their numeric meaning while retaining the declared integer type on the wire.
        let numeric_enum = schema["type"] == "integer"
            && options.iter().any(|item| {
                item.as_str()
                    .and_then(|s| serde_json::from_str::<Value>(s).ok())
                    .as_ref()
                    == Some(value)
            });
        if !options.contains(value) && !numeric_enum {
            return Err(());
        }
    }
    match schema["type"].as_str() {
        Some("object") => {
            let object = value.as_object().ok_or(())?;
            if let Some(required) = schema["required"].as_array() {
                for key in required {
                    if !object.contains_key(key.as_str().ok_or(())?) {
                        return Err(());
                    }
                }
            }
            for (key, v) in object {
                if let Some(child) = schema["properties"].get(key) {
                    validate(document, child, v, depth + 1)?;
                } else if schema["additionalProperties"] == false {
                    return Err(());
                } else if schema["additionalProperties"].is_object() {
                    validate(document, &schema["additionalProperties"], v, depth + 1)?;
                }
            }
        }
        Some("array") => {
            let a = value.as_array().ok_or(())?;
            for (key, lower) in [("minItems", true), ("maxItems", false)] {
                if let Some(n) = schema[key].as_u64()
                    && (lower && (a.len() as u64) < n || !lower && (a.len() as u64) > n)
                {
                    return Err(());
                }
            }
            for v in a {
                validate(document, &schema["items"], v, depth + 1)?;
            }
        }
        Some("string") => {
            let s = value.as_str().ok_or(())?;
            let len = s.chars().count() as u64;
            for (key, lower) in [("minLength", true), ("maxLength", false)] {
                if let Some(n) = schema[key].as_u64()
                    && (lower && len < n || !lower && len > n)
                {
                    return Err(());
                }
            }
        }
        Some("boolean") if !value.is_boolean() => return Err(()),
        Some("integer") | Some("number") => {
            if !value.is_number()
                || schema["type"] == "integer" && !(value.is_i64() || value.is_u64())
            {
                return Err(());
            }
            let n = value.as_f64().ok_or(())?;
            for (key, lower) in [("minimum", true), ("maximum", false)] {
                if let Some(bound) = schema[key].as_f64()
                    && (lower && n < bound || !lower && n > bound)
                {
                    return Err(());
                }
            }
            match schema["format"].as_str() {
                Some("int64") if value.as_i64().is_none() => return Err(()),
                Some("uint64") if value.as_u64().is_none() => return Err(()),
                Some("int32") if value.as_i64().and_then(|n| i32::try_from(n).ok()).is_none() => {
                    return Err(());
                }
                Some("uint32") if value.as_u64().and_then(|n| u32::try_from(n).ok()).is_none() => {
                    return Err(());
                }
                _ => {}
            }
        }
        Some("null") if !value.is_null() => return Err(()),
        _ => {}
    }
    Ok(())
}

pub(crate) fn prepare(
    venue: &'static str,
    server: &str,
    operation: &Operation,
    request: NativeRequest,
) -> Result<Prepared, ApiError> {
    let mut route = operation.path.to_string();
    let mut query = Vec::new();
    let mut headers = BTreeMap::new();
    let mut path = request.path;
    let mut supplied_query = request.query;
    let mut supplied_headers = request.headers;
    for raw in operation.parameters() {
        let parameter = resolve(operation.document, raw)
            .map_err(|_| error(venue, "unresolved official parameter"))?;
        let name = parameter["name"]
            .as_str()
            .ok_or_else(|| error(venue, "unnamed official parameter"))?;
        let location = parameter["in"].as_str().unwrap_or("");
        // Credentials are injected after preparation and cannot be replaced by a request.
        if location == "header"
            && [
                "x-api-key",
                "x-signature",
                "x-timestamp",
                "x-window",
                "authorization",
            ]
            .contains(&name.to_ascii_lowercase().as_str())
        {
            continue;
        }
        let value = match location {
            "path" => path.remove(name),
            "query" => supplied_query.remove(name),
            "header" => {
                let key = supplied_headers
                    .keys()
                    .find(|k| k.eq_ignore_ascii_case(name))
                    .cloned();
                key.and_then(|k| supplied_headers.remove(&k)).map(|s| {
                    if parameter["schema"]["type"] == "integer" {
                        s.parse::<i64>()
                            .map(Value::from)
                            .unwrap_or(Value::String(s))
                    } else {
                        Value::String(s)
                    }
                })
            }
            _ => return Err(error(venue, "unsupported official parameter location")),
        };
        let Some(value) = value else {
            if parameter["required"] == true {
                return Err(invalid(
                    venue,
                    format!("missing native {location} parameter {name}"),
                ));
            }
            continue;
        };
        validate(operation.document, &parameter["schema"], &value, 0)
            .map_err(|_| invalid(venue, format!("invalid native parameter {name}")))?;
        if location == "path" {
            let value = scalar(&value, venue)?;
            if value == "." || value == ".." {
                return Err(invalid(
                    venue,
                    "native path parameter cannot be a dot segment",
                ));
            }
            route = route.replace(&format!("{{{name}}}"), &segment(&value));
        } else if location == "header" {
            headers.insert(name.to_string(), scalar(&value, venue)?);
        } else if let Some(values) = value.as_array() {
            if parameter["explode"] == false {
                query.push((
                    name.into(),
                    values
                        .iter()
                        .map(|v| scalar(v, venue))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(","),
                ));
            } else {
                for v in values {
                    query.push((name.into(), scalar(v, venue)?));
                }
            }
        } else {
            query.push((name.into(), scalar(&value, venue)?));
        }
    }
    if !path.is_empty()
        || !supplied_query.is_empty()
        || !supplied_headers.is_empty()
        || route.contains(['{', '}'])
    {
        return Err(invalid(
            venue,
            "unknown native parameter or credential header override",
        ));
    }
    let request_body = resolve(operation.document, &operation.definition["requestBody"])
        .map_err(|_| error(venue, "unresolved official request body"))?;
    let content = request_body["content"].as_object();
    let encoding = content.and_then(|c| c.keys().next()).cloned();
    if let Some(body) = &request.body {
        let schema = encoding
            .as_ref()
            .and_then(|e| request_body["content"][e].get("schema"))
            .ok_or_else(|| invalid(venue, "endpoint has no request body"))?;
        validate(operation.document, schema, body, 0)
            .map_err(|_| invalid(venue, "invalid native request body"))?;
    } else if request_body["required"] == true {
        return Err(invalid(venue, "native request body required"));
    }
    let url = Url::parse(&format!("{}{route}", server.trim_end_matches('/')))
        .map_err(|_| invalid(venue, "invalid native route"))?;
    Ok(Prepared {
        url,
        query,
        headers,
        body: request.body,
        encoding,
    })
}
pub(crate) fn build(
    http: &reqwest::Client,
    operation: &Operation,
    prepared: &Prepared,
    venue: &'static str,
) -> Result<reqwest::RequestBuilder, ApiError> {
    let method = Method::from_bytes(operation.method.as_bytes())
        .map_err(|_| error(venue, "invalid official method"))?;
    let mut request = http
        .request(method, prepared.url.clone())
        .query(&prepared.query);
    for (k, v) in &prepared.headers {
        request = request.header(k, v);
    }
    if let Some(body) = &prepared.body {
        if prepared
            .encoding
            .as_deref()
            .is_some_and(|e| e.starts_with("application/x-www-form-urlencoded"))
        {
            let fields = body
                .as_object()
                .ok_or_else(|| invalid(venue, "form body must be object"))?
                .iter()
                .map(|(k, v)| Ok((k.clone(), scalar(v, venue)?)))
                .collect::<Result<Vec<_>, ApiError>>()?;
            request = request.form(&fields);
        } else {
            request = request.json(body);
        }
    }
    Ok(request)
}
pub(crate) async fn send(
    venue: &'static str,
    operation: &Operation,
    request: reqwest::RequestBuilder,
    guard: Option<SubmitGuard>,
) -> Result<NativeResponse, ApiError> {
    check_submit_guard(&guard)?;
    let response = request
        .send()
        .await
        .map_err(|e| ApiError::transport(venue, e.without_url()))?;
    if !response.status().is_success() {
        return Err(ApiError::http(
            venue,
            response.status().as_u16(),
            "native request rejected",
        ));
    }
    Ok(NativeResponse {
        operation: operation.id,
        venue,
        write: operation.is_write(),
        response,
    })
}
