use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method, Url};
use serde_json::Value;
use sha2::{Digest, Sha512};

use crate::api::{ApiError, SubmitGuard, check_submit_guard};

pub(super) const EXCHANGE: &str = "gate-crossex";

pub(super) fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(EXCHANGE, "INVALID_ARGUMENT", message)
}
pub(super) fn protocol(message: impl Into<String>) -> ApiError {
    ApiError::new(EXCHANGE, "INVALID_RESPONSE", message)
}
pub(super) fn unsupported(capability: &str) -> ApiError {
    ApiError::new(
        EXCHANGE,
        "UNSUPPORTED",
        format!("CrossEx adapter does not support {capability}"),
    )
}
pub(super) fn text(value: &Value, key: &str) -> Result<String, ApiError> {
    match &value[key] {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(protocol(format!("missing or invalid {key}"))),
    }
}
pub(super) fn number(value: &Value, key: &str) -> Result<f64, ApiError> {
    let n = text(value, key)?
        .parse::<f64>()
        .map_err(|_| protocol(format!("invalid {key}")))?;
    if !n.is_finite() {
        return Err(protocol(format!("non-finite {key}")));
    }
    Ok(n)
}
pub(super) fn integer(value: &Value, key: &str) -> Result<i64, ApiError> {
    text(value, key)?
        .parse()
        .map_err(|_| protocol(format!("invalid {key}")))
}
pub(super) fn optional_number(value: &Value, key: &str) -> Result<f64, ApiError> {
    if value.get(key).is_none_or(Value::is_null) {
        Ok(0.0)
    } else {
        number(value, key)
    }
}
pub(super) fn rows(value: &Value) -> Result<&[Value], ApiError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| protocol("expected array"))
}
pub(super) fn symbol_parts(symbol: &str) -> Result<(&str, &str, &str), ApiError> {
    let parts: Vec<_> = symbol.split('_').collect();
    if parts.len() != 4
        || !matches!(parts[1], "FUTURE" | "SPOT" | "MARGIN")
        || parts.iter().any(|p| p.is_empty())
        || !symbol
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(unsupported(
            "symbols other than EXCHANGE_{FUTURE,SPOT,MARGIN}_BASE_QUOTE; use native extensions for securities",
        ));
    }
    Ok((parts[0], parts[2], parts[3]))
}
pub(super) fn identifier(id: &str) -> Result<&str, ApiError> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid("invalid order identifier"));
    }
    Ok(id)
}
pub(super) fn signature(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: i64,
) -> String {
    let payload = format!(
        "{method}\n{path}\n{query}\n{}\n{timestamp}",
        hex::encode(Sha512::digest(body.as_bytes()))
    );
    sign(secret, &payload)
}
pub(super) fn sign(secret: &str, payload: &str) -> String {
    let mut mac =
        Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Gate CrossEx APIv4 REST facade. Quantities are always base units, including OKX futures.
/// Clones share reqwest's connection pool. Credentials are deliberately not Debug-printable.
#[derive(Clone)]
pub struct GateCrossExClient {
    pub(super) http: Client,
    pub(super) base_url: Url,
    pub(super) key: String,
    pub(super) secret: String,
    pub(super) public_ws_url: String,
    pub(super) private_ws_url: String,
    pub(super) exchange_type: Option<String>,
    pub(super) bound_symbols: Arc<Mutex<std::collections::HashSet<String>>>,
    pub(super) market_quantity_multipliers: BTreeMap<String, f64>,
    observed_orders: Arc<Mutex<HashMap<String, crate::api::OrderInfo>>>,
}

impl GateCrossExClient {
    pub(super) fn record_order(&self, order: &crate::api::OrderInfo) {
        let mut cache = self
            .observed_orders
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let key = if order.client_order_id.is_empty() {
            order.order_id.clone()
        } else {
            order.client_order_id.clone()
        };
        if cache.get(&key).is_some_and(|old| {
            old.update_time > order.update_time || old.executed_qty > order.executed_qty
        }) {
            return;
        }
        if cache.len() >= 8192
            && !cache.contains_key(&key)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, v)| v.update_time)
                .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(key, order.clone());
    }
    pub(super) fn cached_order(&self, id: &str) -> Option<crate::api::OrderInfo> {
        self.observed_orders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }
    pub fn new(base_url: &str, api_key: &str, secret: &str) -> Result<Self, ApiError> {
        crate::ensure_rustls_crypto_provider();
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ApiError::transport(EXCHANGE, e))?;
        Self::with_client(base_url, api_key, secret, http)
    }
    pub fn with_client(
        base_url: &str,
        api_key: &str,
        secret: &str,
        http: Client,
    ) -> Result<Self, ApiError> {
        let base_url =
            Url::parse(base_url.trim_end_matches('/')).map_err(|_| invalid("invalid rest_url"))?;
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
        {
            return Err(invalid(
                "rest_url must be an HTTP(S) API base without credentials, query or fragment",
            ));
        }
        Ok(Self {
            http,
            base_url,
            key: api_key.to_owned(),
            secret: secret.to_owned(),
            public_ws_url: "wss://api.gateio.ws/ws/crossex/public".into(),
            private_ws_url: "wss://api.gateio.ws/ws/crossex".into(),
            exchange_type: None,
            bound_symbols: Default::default(),
            market_quantity_multipliers: Default::default(),
            observed_orders: Default::default(),
        })
    }
    pub(super) async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
        auth: bool,
        guard: Option<SubmitGuard>,
    ) -> Result<Value, ApiError> {
        let mut url = self.base_url.clone();
        url.set_path(&format!(
            "{}{path}",
            self.base_url.path().trim_end_matches('/')
        ));
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())));
        }
        let body = body
            .map(|v| serde_json::to_string(&v))
            .transpose()
            .map_err(|_| invalid("invalid request body"))?
            .unwrap_or_default();
        let write = method != Method::GET;
        let mut request = self
            .http
            .request(method.clone(), url.clone())
            .header("Accept", "application/json");
        if auth {
            if self.key.is_empty() || self.secret.is_empty() {
                return Err(invalid("CrossEx credentials are missing"));
            }
            let timestamp = Utc::now().timestamp();
            // The signature uses the unescaped query, in the same order as the URL.
            let query_string = query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            request = request
                .header("KEY", &self.key)
                .header("Timestamp", timestamp.to_string())
                .header(
                    "SIGN",
                    signature(
                        &self.secret,
                        method.as_str(),
                        url.path(),
                        &query_string,
                        &body,
                        timestamp,
                    ),
                );
        }
        if !body.is_empty() {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        let request = request
            .build()
            .map_err(|_| invalid("cannot build CrossEx request"))?;
        check_submit_guard(&guard)?;
        // Never retry mutations: a timeout may already have created an exchange order.
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| ApiError::transport(EXCHANGE, e.without_url()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| ApiError::transport(EXCHANGE, e.without_url()))?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            if !status.is_success() {
                ApiError::http(EXCHANGE, status.as_u16(), "non-JSON error response")
            } else if write {
                ApiError::transport(
                    EXCHANGE,
                    "invalid response after mutation; reconcile by client order id",
                )
            } else {
                protocol("invalid JSON response")
            }
        })?;
        if !status.is_success() || value.get("label").is_some() {
            let mut error = ApiError::new(
                EXCHANGE,
                value["label"].as_str().unwrap_or(status.as_str()),
                value["message"]
                    .as_str()
                    .unwrap_or("CrossEx request failed"),
            );
            error.status = Some(status.as_u16());
            return Err(error);
        }
        Ok(value)
    }
    pub(super) async fn get(
        &self,
        path: &str,
        query: &[(&str, String)],
        auth: bool,
    ) -> Result<Value, ApiError> {
        self.request(Method::GET, path, query, None, auth, None)
            .await
    }
    pub(super) async fn account_payload(&self) -> Result<Value, ApiError> {
        let query = self
            .exchange_type
            .as_ref()
            .map(|v| vec![("exchange_type", v.clone())])
            .unwrap_or_default();
        let value = self.get("/crossex/accounts", &query, true).await?;
        if value["account_mode"] != "CROSS_EXCHANGE"
            && !(value["account_mode"] == "ISOLATED_EXCHANGE" && self.exchange_type.is_some())
        {
            return Err(invalid(
                "ISOLATED_EXCHANGE requires exchange_type in connector configuration",
            ));
        }
        Ok(value)
    }
    pub(super) async fn paged(
        &self,
        path: &str,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<Vec<Value>, ApiError> {
        symbol_parts(symbol)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        // A bounded scan fails closed instead of quietly omitting live orders.
        for page in 1..=100_u32 {
            let size = limit.map_or(1000, |n| n.min(1000));
            let value = self
                .get(
                    path,
                    &[
                        ("symbol", symbol.to_owned()),
                        ("page", page.to_string()),
                        ("limit", size.to_string()),
                    ],
                    true,
                )
                .await?;
            let values = rows(&value)?;
            for value in values {
                if value["symbol"] == symbol {
                    result.push(value.clone());
                }
            }
            if let Some(limit) = limit
                && result.len() >= limit as usize
            {
                result.truncate(limit as usize);
                return Ok(result);
            }
            if values.len() < size as usize {
                return Ok(result);
            }
        }
        Err(protocol(
            "pagination limit exceeded; snapshot is incomplete",
        ))
    }
}

#[cfg(test)]
pub(super) fn aggregate_account(value: &Value) -> Result<crate::api::AccountInfo, ApiError> {
    aggregate_account_valued(value, &BTreeMap::new())
}
pub(super) fn aggregate_account_valued(
    value: &Value,
    rates: &BTreeMap<String, f64>,
) -> Result<crate::api::AccountInfo, ApiError> {
    use crate::api::{AccountInfo, Balance};
    if !matches!(
        value["account_mode"].as_str(),
        Some("CROSS_EXCHANGE" | "ISOLATED_EXCHANGE")
    ) {
        return Err(unsupported("ISOLATED_EXCHANGE account mode"));
    }
    let mut balances: BTreeMap<String, Balance> = BTreeMap::new();
    for asset in rows(&value["assets"])? {
        let coin = text(asset, "coin")?;
        let balance = balances.entry(coin.clone()).or_insert(Balance {
            asset: coin,
            wallet_balance: 0.0,
            available_balance: 0.0,
            unrealized_pnl: 0.0,
            margin_balance: 0.0,
        });
        balance.wallet_balance += number(asset, "balance")?;
        balance.available_balance += number(asset, "available_balance")?;
        balance.unrealized_pnl += number(asset, "upnl")?;
        balance.margin_balance += number(asset, "equity")?;
    }
    let balances: Vec<_> = balances.into_values().collect();
    // margin_balance can include collateral discounts. It is not wallet balance plus PnL.
    let mut total_unrealized_pnl = 0.0;
    let mut total_wallet_balance = 0.0;
    for balance in &balances {
        let rate = if balance.asset == "USDT" {
            1.0
        } else if balance.wallet_balance == 0.0 && balance.unrealized_pnl == 0.0 {
            0.0
        } else {
            *rates
                .get(&balance.asset)
                .ok_or_else(|| unsupported("missing collateral/USDT valuation"))?
        };
        if !rate.is_finite() || rate < 0.0 {
            return Err(protocol("invalid collateral valuation"));
        }
        total_wallet_balance += balance.wallet_balance * rate;
        total_unrealized_pnl += balance.unrealized_pnl * rate;
    }
    if !total_wallet_balance.is_finite()
        || !total_unrealized_pnl.is_finite()
        || balances.iter().any(|b| {
            ![
                b.wallet_balance,
                b.available_balance,
                b.unrealized_pnl,
                b.margin_balance,
            ]
            .iter()
            .all(|n| n.is_finite())
        })
    {
        return Err(protocol("account aggregate overflow"));
    }
    let margin = number(value, "margin_balance")?;
    Ok(AccountInfo {
        total_wallet_balance,
        total_margin_balance: margin,
        total_unrealized_pnl,
        available_balance: number(value, "available_margin")?,
        balances,
        timestamp: integer(value, "update_time")?,
    })
}
