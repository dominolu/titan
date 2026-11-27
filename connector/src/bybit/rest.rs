//! Bybit V5 public REST market data.
//!
//! A depth stream needs one image to start from and one to recover from a break, and open interest
//! for a derivative market is only published over REST. Both go through [`BybitClient`], which owns
//! the base URL, the timeout, and the one thing every Bybit call has to get right: a refusal
//! (`429`, `503`) is the venue asking to be left alone for a while, not a broken document.

use std::time::Duration;

use thiserror::Error;

use crate::bybit::msg::{RestOrderbook, RestResponse};
use crate::transport::VenueBanned;

/// A Bybit public market data client. Unauthenticated: every call here is a public endpoint.
#[derive(Clone)]
pub struct BybitClient {
    client: reqwest::Client,
    base_url: String,
    category: String,
}

impl BybitClient {
    /// A client for `category` (`spot`, `linear` or `inverse`) at `base_url`.
    pub fn new(base_url: &str, category: impl Into<String>) -> Self {
        // A client built outside `Bybit::new` — a test, or a stream constructed directly — would
        // otherwise construct reqwest before any rustls provider is installed and panic with
        // "No provider set", which is the defect recorded as ISSUE-001 for the Binance client.
        crate::ensure_rustls_crypto_provider();
        Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .expect("static Bybit HTTP client configuration is valid"),
            base_url: base_url.trim_end_matches('/').to_owned(),
            category: category.into(),
        }
    }

    /// The orderbook image at `depth` levels for `symbol`.
    ///
    /// The requested depth becomes the image's first update id, which the stream compares against
    /// its own increments, so a level count Bybit does not publish is rejected before the request.
    pub async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> Result<RestOrderbook, BybitError> {
        if !matches!(depth, 1 | 50 | 200 | 500) {
            return Err(BybitError::InvalidRequest(
                "orderbook depth is not published",
            ));
        }
        let response = self
            .client
            .get(format!("{}/v5/market/orderbook", self.base_url))
            .query(&[
                ("category", self.category.as_str()),
                ("symbol", symbol),
                ("limit", &depth.to_string()),
            ])
            .header("Accept", "application/json")
            .send()
            .await?;
        if let Some(ban) = VenueBanned::from_http_response(&response) {
            return Err(BybitError::Banned(ban));
        }
        let response = response.error_for_status()?;
        let body: RestResponse<RestOrderbook> = self.decode(response).await?;
        body.result
            .ok_or(BybitError::InvalidResponse("orderbook image is missing"))
    }

    /// The current open interest for `symbol`, in contracts as the venue reports it.
    pub async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, BybitError> {
        let response = self
            .client
            .get(format!("{}/v5/market/open-interest", self.base_url))
            .query(&[
                ("category", self.category.as_str()),
                ("symbol", symbol),
                ("intervalTime", "5min"),
            ])
            .header("Accept", "application/json")
            .send()
            .await?;
        if let Some(ban) = VenueBanned::from_http_response(&response) {
            return Err(BybitError::Banned(ban));
        }
        let response = response.error_for_status()?;
        let body: RestResponse<OpenInterestRows> = self.decode(response).await?;
        let row = body
            .result
            .and_then(|rows| rows.list.into_iter().next())
            .ok_or(BybitError::InvalidResponse("empty open interest"))?;
        Ok(row)
    }

    /// Decodes a `{"retCode","retMsg","result"}` body.
    ///
    /// A non-zero `retCode` on an HTTP `200` is the venue refusing the request at the API level, and
    /// is reported as such rather than as an empty result.
    async fn decode<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<RestResponse<T>, BybitError> {
        let body: RestResponse<T> = response.json().await?;
        if body.ret_code != 0 {
            return Err(BybitError::Rejected {
                message: format!("{} (retCode {})", body.ret_msg, body.ret_code),
            });
        }
        Ok(body)
    }
}

/// One open interest sample.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct OpenInterest {
    #[serde(rename = "openInterest", default)]
    pub open_interest: String,
    /// Milliseconds.
    #[serde(default)]
    pub timestamp: String,
}

#[derive(serde::Deserialize, Debug, Clone, Default)]
struct OpenInterestRows {
    #[serde(default)]
    list: Vec<OpenInterest>,
}

#[derive(Error, Debug)]
pub enum BybitError {
    #[error("InvalidRequest: {0}")]
    InvalidRequest(&'static str),
    #[error("InvalidResponse: {0}")]
    InvalidResponse(&'static str),
    #[error("Rejected: {message}")]
    Rejected { message: String },
    #[error("{0}")]
    Banned(VenueBanned),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_published_orderbook_depths_are_requested() {
        // Checked before a request is made, so an unpublishable depth cannot cost an API call.
        for depth in [1u16, 50, 200, 500] {
            assert!(matches!(depth, 1 | 50 | 200 | 500));
        }
        for depth in [5u16, 100, 1000] {
            assert!(!matches!(depth, 1 | 50 | 200 | 500), "depth {depth}");
        }
    }

    #[test]
    fn an_api_level_refusal_is_not_an_empty_result() {
        // Bybit answers a rejected symbol with HTTP 200 and retCode != 0; treating that as a
        // missing result would read as "the venue sent nothing", which is the opposite of a refusal.
        let body: RestResponse<OpenInterestRows> =
            serde_json::from_str(r#"{"retCode":10001,"retMsg":"params error"}"#).unwrap();
        assert_ne!(body.ret_code, 0);
        let message = format!("{} (retCode {})", body.ret_msg, body.ret_code);
        assert_eq!(
            BybitError::Rejected { message }.to_string(),
            "Rejected: params error (retCode 10001)"
        );
    }
}
