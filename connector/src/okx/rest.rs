use base64::{Engine as _, engine::general_purpose};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::time::Duration;

use crate::okx::{
    OkxError,
    msg::rest::{
        Books, BooksResponse, CancelResponse, CancelResult, Instrument, InstrumentsResponse,
        OrderResponse, OrderResult, Position, PositionsResponse,
    },
};

#[derive(Clone)]
pub struct OkxClient {
    client: reqwest::Client,
    url: String,
    api_key: String,
    secret: String,
    passphrase: String,
    simulated: bool,
    td_mode: String,
}

#[allow(dead_code)]
impl OkxClient {
    pub fn new(url: &str, api_key: &str, secret: &str, passphrase: &str) -> Self {
        Self::with_options(
            url,
            api_key,
            secret,
            passphrase,
            false,
            default_http_client(),
            "cross".to_string(),
        )
    }

    pub fn with_simulated(
        url: &str,
        api_key: &str,
        secret: &str,
        passphrase: &str,
        simulated: bool,
    ) -> Self {
        Self::with_options(
            url,
            api_key,
            secret,
            passphrase,
            simulated,
            default_http_client(),
            "cross".to_string(),
        )
    }

    pub(crate) fn with_options(
        url: &str,
        api_key: &str,
        secret: &str,
        passphrase: &str,
        simulated: bool,
        client: reqwest::Client,
        td_mode: String,
    ) -> Self {
        Self {
            client,
            url: url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
            passphrase: passphrase.to_string(),
            simulated,
            td_mode,
        }
    }

    /// 设置默认交易模式（cross / isolated），统一下单接口使用。
    pub fn set_td_mode(&mut self, td_mode: &str) {
        self.td_mode = td_mode.to_string();
    }

    pub fn td_mode(&self) -> &str {
        &self.td_mode
    }

    pub(crate) fn timestamp() -> String {
        // OKX requires ISO 8601 with milliseconds precision, e.g. 2026-08-19T03:00:00.123Z
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    pub(crate) fn sign(&self, timestamp: &str, method: &str, path: &str, body: &str) -> String {
        let s = format!("{timestamp}{method}{path}{body}");
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(s.as_bytes());
        let digest = mac.finalize().into_bytes();
        general_purpose::STANDARD.encode(digest)
    }

    pub(crate) async fn get_noauth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
    ) -> Result<T, OkxError> {
        let mut request = self
            .client
            .get(format!("{}{}", self.url, path))
            .header("Accept", "application/json");
        if let Some((name, value)) = simulated_header(self.simulated) {
            request = request.header(name, value);
        }
        let resp = request.send().await?.json().await?;
        Ok(resp)
    }

    pub(crate) async fn get<T: for<'a> Deserialize<'a>>(&self, path: &str) -> Result<T, OkxError> {
        let timestamp = Self::timestamp();
        let signature = self.sign(&timestamp, "GET", path, "");
        let mut request = self
            .client
            .get(format!("{}{}", self.url, path))
            .header("Accept", "application/json")
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase);
        if let Some((name, value)) = simulated_header(self.simulated) {
            request = request.header(name, value);
        }
        let resp = request.send().await?.json().await?;
        Ok(resp)
    }

    pub(crate) async fn post<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, OkxError> {
        let timestamp = Self::timestamp();
        let signature = self.sign(&timestamp, "POST", path, &body);
        let mut request = self
            .client
            .post(format!("{}{}", self.url, path))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .body(body);
        if let Some((name, value)) = simulated_header(self.simulated) {
            request = request.header(name, value);
        }
        let resp = request.send().await?.json().await?;
        Ok(resp)
    }

    pub async fn submit_order(
        &self,
        inst_id: &str,
        td_mode: &str,
        pos_side: Option<&str>,
        cl_ord_id: &str,
        side: &str,
        ord_type: &str,
        px: Option<&str>,
        sz: &str,
    ) -> Result<OrderResult, OkxError> {
        let body = build_submit_body(
            inst_id, td_mode, pos_side, cl_ord_id, side, ord_type, px, sz,
        );

        let resp: OrderResponse = self.post("/api/v5/trade/order", body).await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty order response"))
    }

    pub async fn cancel_order(&self, inst_id: &str, cl_ord_id: &str) -> Result<(), OkxError> {
        let body = format!("{{\"instId\":\"{inst_id}\",\"clOrdId\":\"{cl_ord_id}\"}}");
        let resp: CancelResponse = self.post("/api/v5/trade/cancel-order", body).await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        // OKX reports per-order business failures (e.g. 51401, the order no longer exists) in
        // `data[].sCode` while the top-level `code` stays "0".
        check_cancel_result(resp.data)
    }

    pub async fn cancel_all_orders(
        &self,
        inst_id: &str,
        td_mode: &str,
        pos_side: Option<&str>,
    ) -> Result<(), OkxError> {
        let pos_side = pos_side
            .map(|side| format!(",\"posSide\":\"{side}\""))
            .unwrap_or_default();
        let body = format!("{{\"instId\":\"{inst_id}\",\"tdMode\":\"{td_mode}\"{pos_side}}}");
        let resp: CancelResponse = self.post("/api/v5/trade/cancel-all-orders", body).await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_books(&self, inst_id: &str, sz: u32) -> Result<Books, OkxError> {
        let resp: BooksResponse = self
            .get_noauth(&format!("/api/v5/market/books?instId={inst_id}&sz={sz}"))
            .await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty books response"))
    }

    pub async fn get_positions(&self, inst_id: &str) -> Result<Vec<Position>, OkxError> {
        let resp: PositionsResponse = self
            .get(&format!("/api/v5/account/positions?instId={inst_id}"))
            .await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        Ok(resp.data)
    }

    pub async fn get_instruments(&self, inst_id: &str) -> Result<Instrument, OkxError> {
        let resp: InstrumentsResponse = self
            .get_noauth(&format!(
                "/api/v5/public/instruments?instType=SWAP&instId={inst_id}"
            ))
            .await?;
        if resp.code != "0" {
            return Err(OkxError::OrderError {
                code: resp.code,
                msg: resp.msg,
            });
        }
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty instruments response"))
    }
}

fn default_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("static OKX HTTP client configuration is valid")
}

/// The header that switches OKX to demo trading. Demo trading uses the same REST host with this
/// header on every request.
pub(crate) fn simulated_header(simulated: bool) -> Option<(&'static str, &'static str)> {
    simulated.then_some(("x-simulated-trading", "1"))
}

/// Builds the JSON body of `POST /api/v5/trade/order`. Market orders omit `px`.
pub(crate) fn build_submit_body(
    inst_id: &str,
    td_mode: &str,
    pos_side: Option<&str>,
    cl_ord_id: &str,
    side: &str,
    ord_type: &str,
    px: Option<&str>,
    sz: &str,
) -> String {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        serde_json::Value::String(inst_id.to_string()),
    );
    body.insert(
        "tdMode".to_string(),
        serde_json::Value::String(td_mode.to_string()),
    );
    if let Some(pos_side) = pos_side {
        body.insert(
            "posSide".to_string(),
            serde_json::Value::String(pos_side.to_string()),
        );
    }
    body.insert(
        "clOrdId".to_string(),
        serde_json::Value::String(cl_ord_id.to_string()),
    );
    body.insert(
        "side".to_string(),
        serde_json::Value::String(side.to_string()),
    );
    body.insert(
        "ordType".to_string(),
        serde_json::Value::String(ord_type.to_string()),
    );
    body.insert("sz".to_string(), serde_json::Value::String(sz.to_string()));
    if let Some(px) = px {
        body.insert("px".to_string(), serde_json::Value::String(px.to_string()));
    }
    serde_json::Value::Object(body).to_string()
}

/// OKX reports per-order business failures in `data[].sCode` (e.g. 51401: order no longer exists)
/// while the top-level `code` stays "0". Maps the per-order status to a result.
pub(crate) fn check_cancel_result(data: Vec<CancelResult>) -> Result<(), OkxError> {
    match data.into_iter().next() {
        Some(result) if result.s_code == "0" => Ok(()),
        Some(result) => Err(OkxError::OrderError {
            code: result.s_code,
            msg: result.s_msg,
        }),
        None => Err(OkxError::InvalidArg("empty cancel response")),
    }
}
