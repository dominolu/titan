//! Solana JSON-RPC 封装：读路径（HTTP）+ WS 行情订阅共用。

use std::time::Duration;

use serde_json::{Value, json};

use crate::solana::SolanaError;

#[derive(Clone)]
pub struct SolanaRpc {
    client: reqwest::Client,
    rpc_url: String,
    ws_url: String,
}

impl SolanaRpc {
    pub fn new(rpc_url: &str, ws_url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("static Solana HTTP client configuration is valid");
        Self {
            client,
            rpc_url: rpc_url.to_string(),
            ws_url: ws_url.to_string(),
        }
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, SolanaError> {
        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
            .map_err(SolanaError::Http)?;
        let mut body: Value = resp.json().await.map_err(SolanaError::Http)?;
        if let Some(err) = body.get("error") {
            return Err(SolanaError::Rpc(
                err["message"].as_str().unwrap_or("unknown").to_string(),
            ));
        }
        Ok(body["result"].take())
    }

    pub async fn slot(&self) -> Result<u64, SolanaError> {
        Ok(self
            .call("getSlot", json!([{"commitment": "confirmed"}]))
            .await?
            .as_u64()
            .unwrap_or(0))
    }

    pub async fn balance(&self, pubkey: &str) -> Result<u64, SolanaError> {
        Ok(self.call("getBalance", json!([pubkey])).await?["value"]
            .as_u64()
            .unwrap_or(0))
    }

    pub async fn latest_blockhash(&self) -> Result<String, SolanaError> {
        // finalized：公共 RPC 多节点负载均衡下，confirmed 哈希会撞 "Blockhash not found"；
        // finalized 哈希在所有后端均可用且仍在 ~90s 有效窗口内（实盘探针验证）。
        let v = self
            .call("getLatestBlockhash", json!([{"commitment": "finalized"}]))
            .await?;
        Ok(v["value"]["blockhash"]
            .as_str()
            .ok_or(SolanaError::Decode("blockhash"))?
            .to_string())
    }

    /// 读取 SPL token 账户的 amount（u64）。账户不存在返回 0。
    pub async fn token_account_amount(&self, token_account: &str) -> Result<u64, SolanaError> {
        let v = self
            .call(
                "getAccountInfo",
                json!([token_account, {"encoding": "base64"}]),
            )
            .await?;
        let Some(data_b64) = v["value"]["data"][0].as_str() else {
            return Ok(0);
        };
        use base64::Engine;
        let raw = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .map_err(|_| SolanaError::Decode("token account"))?;
        if raw.len() < crate::solana::types::SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET + 8 {
            return Ok(0);
        }
        let off = crate::solana::types::SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET;
        Ok(u64::from_le_bytes(raw[off..off + 8].try_into().unwrap()))
    }

    pub async fn send_transaction(&self, tx_base58: &str) -> Result<String, SolanaError> {
        self.send_transaction_with(tx_base58, false).await
    }

    /// `skip_preflight = true` 时跳过本地模拟直接转发给领导者，
    /// 用于穿透公共 RPC 预检的误报（真实失败仍会在链上状态中体现）。
    pub async fn send_transaction_with(
        &self,
        tx_base58: &str,
        skip_preflight: bool,
    ) -> Result<String, SolanaError> {
        let v = self
            .call(
                "sendTransaction",
                json!([tx_base58, {"encoding": "base58", "skipPreflight": skip_preflight, "maxRetries": 1}]),
            )
            .await?;
        Ok(v.as_str().unwrap_or_default().to_string())
    }

    /// 交易状态：None = 未上链；Some(Ok) = 成功；Some(Err) = 失败。
    pub async fn signature_status(
        &self,
        signature: &str,
    ) -> Result<Option<Result<(), String>>, SolanaError> {
        let v = self
            .call(
                "getSignatureStatuses",
                json!([[signature], {"searchTransactionHistory": false}]),
            )
            .await?;
        let status = &v["value"][0];
        if status.is_null() {
            return Ok(None);
        }
        if status["err"].is_null() {
            Ok(Some(Ok(())))
        } else {
            Ok(Some(Err(
                serde_json::to_string(&status["err"]).unwrap_or_default()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_builds() {
        let _ = SolanaRpc::new("https://127.0.0.1:1", "ws://127.0.0.1:1");
    }
}
