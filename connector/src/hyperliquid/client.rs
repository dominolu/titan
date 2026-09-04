use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::hyperliquid::{
    HyperliquidError,
    msg::{ClearinghouseState, ExchangeRequest, ExchangeResponse, Meta, OpenOrder},
    signing::{L1Signature, sign_l1_action},
};
use crate::utils::next_nonce;

#[derive(Clone)]
pub struct HyperliquidClient {
    client: reqwest::Client,
    info_url: String,
    exchange_url: String,
    /// 可选签名器（API wallet 私钥），用于统一 BrokerApi 的交易/账户管理接口。
    private_key: Option<[u8; 32]>,
    account_address: Option<String>,
    is_mainnet: bool,
    nonce_counter: Arc<Mutex<u64>>,
}

#[allow(dead_code)]
impl HyperliquidClient {
    pub fn new(info_url: &str, exchange_url: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .expect("static Hyperliquid HTTP client configuration is valid"),
            info_url: info_url.to_string(),
            exchange_url: exchange_url.to_string(),
            private_key: None,
            account_address: None,
            is_mainnet: false,
            nonce_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// 附加签名器后，可通过统一 [`BrokerApi`](`crate::api::BrokerApi`) 执行交易/账户管理操作。
    pub fn with_signer(
        mut self,
        private_key: [u8; 32],
        account_address: String,
        is_mainnet: bool,
    ) -> Self {
        self.private_key = Some(private_key);
        self.account_address = Some(account_address);
        self.is_mainnet = is_mainnet;
        self
    }

    pub fn has_signer(&self) -> bool {
        self.private_key.is_some()
    }

    pub fn account_address(&self) -> Option<&str> {
        self.account_address.as_deref()
    }

    /// 对 action 签名并提交到 /exchange。
    pub async fn sign_and_post<A: Serialize>(
        &self,
        action: &A,
    ) -> Result<ExchangeResponse, HyperliquidError> {
        let Some(private_key) = self.private_key else {
            return Err(HyperliquidError::InvalidArg(
                "signer not configured; call with_signer first",
            ));
        };
        let nonce = next_nonce(&self.nonce_counter);
        let signature = sign_l1_action(action, &private_key, nonce, None, self.is_mainnet)?;
        self.post_exchange(action, nonce, &signature).await
    }

    pub async fn post_info(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, HyperliquidError> {
        let resp = self
            .client
            .post(&self.info_url)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn get_meta(&self) -> Result<Meta, HyperliquidError> {
        let resp = self.post_info(serde_json::json!({"type": "meta"})).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn get_clearinghouse_state(
        &self,
        user: &str,
    ) -> Result<ClearinghouseState, HyperliquidError> {
        let resp = self
            .post_info(serde_json::json!({"type": "clearinghouseState", "user": user}))
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn get_open_orders(&self, user: &str) -> Result<Vec<OpenOrder>, HyperliquidError> {
        let resp = self
            .post_info(serde_json::json!({"type": "openOrders", "user": user}))
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    /// Returns the current funding rate per asset from `metaAndAssetCtxs`.
    pub async fn get_funding_rates(&self) -> Result<HashMap<String, f64>, HyperliquidError> {
        let resp = self
            .post_info(serde_json::json!({"type": "metaAndAssetCtxs"}))
            .await?;
        let arr = resp.as_array().ok_or(HyperliquidError::InvalidArg(
            "metaAndAssetCtxs response is not an array",
        ))?;
        let universe = arr
            .first()
            .and_then(|meta| meta.get("universe").and_then(|u| u.as_array()))
            .ok_or(HyperliquidError::InvalidArg(
                "missing universe in metaAndAssetCtxs",
            ))?;
        let ctxs =
            arr.get(1)
                .and_then(|ctxs| ctxs.as_array())
                .ok_or(HyperliquidError::InvalidArg(
                    "missing asset contexts in metaAndAssetCtxs",
                ))?;

        let mut rates = HashMap::new();
        for (asset, ctx) in universe.iter().zip(ctxs.iter()) {
            let name = asset
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            let funding = ctx
                .get("funding")
                .and_then(|f| f.as_str())
                .and_then(|f| f.parse::<f64>().ok())
                .unwrap_or(0.0);
            if !name.is_empty() {
                rates.insert(name, funding);
            }
        }
        Ok(rates)
    }

    pub async fn post_exchange<A: Serialize>(
        &self,
        action: &A,
        nonce: u64,
        signature: &L1Signature,
    ) -> Result<ExchangeResponse, HyperliquidError> {
        let req = ExchangeRequest {
            action,
            nonce,
            signature: signature.clone(),
        };
        let resp = self
            .client
            .post(&self.exchange_url)
            .header("Accept", "application/json")
            .json(&req)
            .send()
            .await?
            .text()
            .await?;
        match serde_json::from_str::<ExchangeResponse>(&resp) {
            Ok(parsed) => Ok(parsed),
            Err(error) => {
                tracing::error!(
                    %resp,
                    "Failed to parse the Hyperliquid exchange response: {error}"
                );
                Err(HyperliquidError::Serde(error))
            }
        }
    }
}
