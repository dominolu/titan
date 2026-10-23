//! Lossless Gate CrossEx extensions. Decimal strings, exchange-specific fields and asynchronous
//! acceptance responses remain intact. These methods never retry writes or run automatically.
use super::{
    GateCrossExClient,
    rest::{identifier, invalid, text},
};
use crate::api::ApiError;
use reqwest::Method;
use serde_json::{Value, json};

/// Ordered query parameters; signatures use this order, the transport performs URL escaping.
pub type Query = Vec<(String, String)>;

impl GateCrossExClient {
    pub async fn crossex_request(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<Value>,
        auth: bool,
    ) -> Result<Value, ApiError> {
        if !path.starts_with("/crossex/") || path.contains("..") || path.contains(['?', '#']) {
            return Err(invalid("native request must use a CrossEx API path"));
        }
        let query: Vec<_> = query.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        self.request(method, path, &query, body, auth, None).await
    }
    pub async fn list_rule_symbols(&self) -> Result<Value, ApiError> {
        self.get("/crossex/rule/symbols", &[], false).await
    }
    pub async fn list_rule_risk_limits(&self, symbols: &[String]) -> Result<Value, ApiError> {
        if symbols.is_empty() {
            return Err(invalid("risk limits require at least one symbol"));
        }
        for symbol in symbols {
            super::rest::symbol_parts(symbol)?;
        }
        self.get(
            "/crossex/rule/risk_limits",
            &[("symbols", symbols.join(","))],
            false,
        )
        .await
    }
    pub async fn get_account_raw(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/accounts", query, None, true)
            .await
    }
    pub async fn update_account(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(Method::PUT, "/crossex/accounts", &[], Some(body), true)
            .await
    }
    pub async fn list_positions(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/positions", query, None, true)
            .await
    }
    pub async fn list_margin_positions(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/margin_positions", query, None, true)
            .await
    }
    pub async fn list_security_positions(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/security_positions",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn get_position_leverages(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/positions/leverage",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn set_position_leverage(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/positions/leverage",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn get_margin_leverages(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/margin_positions/leverage",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn set_margin_leverage(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/margin_positions/leverage",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn get_fee(&self) -> Result<Value, ApiError> {
        self.get("/crossex/fee", &[], true).await
    }
    pub async fn list_transfer_coins(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/transfers/coin", query, None, true)
            .await
    }
    pub async fn list_transfers(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/transfers", query, None, true)
            .await
    }
    pub async fn create_transfer(&self, body: Value) -> Result<Value, ApiError> {
        let coin = body
            .get("coin")
            .or_else(|| body.get("asset"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("transfer coin is required"))?;
        let amount = text(&body, "amount")?;
        if !amount
            .parse::<f64>()
            .is_ok_and(|v| v.is_finite() && v > 0.0)
        {
            return Err(invalid("transfer amount must be positive"));
        }
        let mut normalized = json!({"coin":coin,"amount":amount,
            "from":transfer_account(&body,"from")?,"to":transfer_account(&body,"to")?});
        if let Some(id) = body.get("text") {
            normalized["text"] = id.clone();
        }
        self.crossex_request(
            Method::POST,
            "/crossex/transfers",
            &[],
            Some(normalized),
            true,
        )
        .await
    }
    pub async fn create_order(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(Method::POST, "/crossex/orders", &[], Some(body), true)
            .await
    }
    pub async fn query_order(&self, id: &str) -> Result<Value, ApiError> {
        self.get(&format!("/crossex/orders/{}", identifier(id)?), &[], true)
            .await
    }
    pub async fn cancel_order_raw(&self, id: &str) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::DELETE,
            &format!("/crossex/orders/{}", identifier(id)?),
            &[],
            None,
            true,
        )
        .await
    }
    pub async fn update_order(&self, id: &str, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::PUT,
            &format!("/crossex/orders/{}", identifier(id)?),
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn batch_cancel_orders(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/batch_cancel_orders",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn list_open_orders(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/open_orders", query, None, true)
            .await
    }
    pub async fn list_history_orders(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/history_orders", query, None, true)
            .await
    }
    pub async fn list_history_trades(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/history_trades", query, None, true)
            .await
    }
    pub async fn list_account_book(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/account_book", query, None, true)
            .await
    }
    pub async fn list_history_positions(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/history_positions", query, None, true)
            .await
    }
    pub async fn list_history_margin_positions(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/history_margin_positions",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn list_history_margin_interests(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/history_margin_interests",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn get_interest_rates(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/interest_rate", query, None, true)
            .await
    }
    pub async fn get_coin_discount_rates(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/coin_discount_rate",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn get_adl_ranks(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(Method::GET, "/crossex/adl_rank", query, None, true)
            .await
    }
    pub async fn close_position(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(Method::POST, "/crossex/position", &[], Some(body), true)
            .await
    }
    pub async fn get_margin_mode(&self, query: &Query) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::GET,
            "/crossex/positions/margin_mode",
            query,
            None,
            true,
        )
        .await
    }
    pub async fn set_margin_mode(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/positions/margin_mode",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn adjust_isolated_margin(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/positions/margin",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn create_convert_quote(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/convert/quote",
            &[],
            Some(body),
            true,
        )
        .await
    }
    pub async fn create_convert_order(&self, body: Value) -> Result<Value, ApiError> {
        self.crossex_request(
            Method::POST,
            "/crossex/convert/orders",
            &[],
            Some(body),
            true,
        )
        .await
    }
}
fn transfer_account(body: &Value, direction: &str) -> Result<String, ApiError> {
    let exchange = body
        .get(format!("{direction}_exchange"))
        .or_else(|| body.get(direction))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("transfer source and destination are required"))?
        .to_ascii_uppercase();
    let business = body
        .get(format!("{direction}_business_type"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let account = if exchange == "SPOT" || business.eq_ignore_ascii_case("SPOT") {
        "SPOT".to_owned()
    } else if [
        "BINANCE",
        "OKX",
        "GATE",
        "BYBIT",
        "KRAKEN",
        "HYPERLIQUID",
        "DERIBIT",
        "LIGHTER",
    ]
    .contains(&exchange.as_str())
    {
        format!("CROSSEX_{exchange}")
    } else {
        exchange
    };
    if account != "SPOT"
        && account != "CROSSEX"
        && ![
            "CROSSEX_BINANCE",
            "CROSSEX_OKX",
            "CROSSEX_GATE",
            "CROSSEX_BYBIT",
            "CROSSEX_KRAKEN",
            "CROSSEX_HYPERLIQUID",
            "CROSSEX_DERIBIT",
            "CROSSEX_LIGHTER",
        ]
        .contains(&account.as_str())
    {
        return Err(invalid("invalid transfer account"));
    }
    Ok(account)
}

impl GateCrossExClient {
    /// Best-effort batch with a result for every input, preserving partial successes and unknown
    /// outcomes. CrossEx does not offer atomic batch submission; writes are never retried.
    pub async fn submit_order_results(
        &self,
        requests: &[crate::api::UnifiedOrderRequest],
    ) -> Vec<Result<crate::api::OrderInfo, ApiError>> {
        use crate::api::BrokerApi;
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            results.push(self.submit_order(request).await);
        }
        results
    }
    pub async fn cancel_order_results(
        &self,
        requests: &[crate::api::CancelOrderRequest],
    ) -> Vec<Result<crate::api::OrderInfo, ApiError>> {
        use crate::api::BrokerApi;
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            results.push(self.cancel_order(request).await);
        }
        results
    }
}
