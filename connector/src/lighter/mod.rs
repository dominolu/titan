//! Lighter Core native connector. Uses the official pinned signer via C ABI.
mod native;
pub mod signer;
pub use native::{
    AuthorizationProvider, LighterNativeClient, LighterNativeWebSocket, LighterOperation,
    WEBSOCKET_CHANNELS,
};
mod stream;
use crate::{api::*, venue::*};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use signer::{Signed, SignerAccount};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
const NAME: &str = "lighter";
pub type Lighter = VenueConnector<LighterClient>;

#[cfg(test)]
mod market_width_tests {
    use super::*;
    use crate::venue::test_support::http;
    #[tokio::test]
    async fn market_ids_keep_all_native_48_bits_without_i32_truncation() {
        let wide = 1i64 << 40;
        let metadata = json!({"code":200,"order_book_details":[{"market_id":wide,"symbol":"ETH","market_type":"perp","status":"active","supported_price_decimals":1,"supported_size_decimals":3,"price_decimals":1,"size_decimals":3,"min_base_amount":"0.001"}]});
        let (url, mut rx, task) = http(vec![(200, metadata)]).await;
        let client = LighterClient::new(Config {
            rest_url: url,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(client.market("ETH").await.unwrap().index, wide);
        rx.recv().await.unwrap();
        task.await.unwrap();
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rest_url: String,
    pub ws_url: String,
    pub chain_id: i32,
    pub account_index: i64,
    pub api_key_index: u8,
    pub api_private_key: String,
    pub signer_path: Option<String>,
    pub signer_sha256: Option<String>,
    pub client_id_journal: Option<String>,
    pub reconnect_ms: u64,
    pub confirmation_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rest_url: "https://mainnet.zklighter.elliot.ai".into(),
            ws_url: "wss://mainnet.zklighter.elliot.ai/stream".into(),
            chain_id: 304,
            account_index: 0,
            api_key_index: 2,
            api_private_key: String::new(),
            signer_path: None,
            signer_sha256: None,
            client_id_journal: None,
            reconnect_ms: 1000,
            confirmation_ms: 5000,
        }
    }
}
#[derive(Clone)]
pub struct LighterClient {
    config: Config,
    http: reqwest::Client,
    signer: Option<Arc<SignerAccount>>,
    ids: Arc<Mutex<ClientIds>>,
}
#[derive(Clone)]
struct Market {
    info: InstrumentInfo,
    index: i64,
    price_unit: f64,
    quantity_unit: f64,
}
impl LighterClient {
    pub fn new(config: Config) -> Result<Self, ApiError> {
        endpoint(&config.rest_url, false, NAME)?;
        endpoint(&config.ws_url, true, NAME)?;
        if !matches!(config.chain_id, 300 | 304)
            || config.account_index < 0
            || config.account_index > (1_i64 << 48) - 2
            || config.api_key_index == 255
            || config.reconnect_ms == 0
            || !(100..=60_000).contains(&config.confirmation_ms)
        {
            return Err(invalid(
                NAME,
                "invalid Core network, account, API key or timing",
            ));
        }
        for (raw, ws) in [(&config.rest_url, false), (&config.ws_url, true)] {
            let url = reqwest::Url::parse(raw).map_err(|_| invalid(NAME, "invalid endpoint"))?;
            let host = url.host_str().unwrap_or("");
            if host.contains("lighter.xyz") || host.ends_with("elliot.ai") {
                let expected = if config.chain_id == 304 {
                    "mainnet.zklighter.elliot.ai"
                } else {
                    "testnet.zklighter.elliot.ai"
                };
                if host != expected || ws && url.path() != "/stream" {
                    return Err(invalid(NAME, "endpoint and Core chain ID mismatch"));
                }
            }
        }
        if !config.api_private_key.is_empty() && config.client_id_journal.is_none() {
            return Err(invalid(
                NAME,
                "private connector requires client_id_journal",
            ));
        }
        let ids = ClientIds::open(
            config.client_id_journal.as_deref(),
            &format!(
                "lighter {} {} {} {}",
                config.rest_url, config.chain_id, config.account_index, config.api_key_index
            ),
            (1u64 << 48) - 1,
            NAME,
        )?;
        let signer = if config.api_private_key.is_empty() {
            None
        } else {
            Some(SignerAccount::load(&config)?)
        };
        Ok(Self {
            http: http_client(NAME)?,
            config,
            signer,
            ids,
        })
    }
    fn signer(&self) -> Result<&Arc<SignerAccount>, ApiError> {
        self.signer
            .as_ref()
            .ok_or_else(|| invalid(NAME, "private signer required"))
    }
    async fn get(&self, path: &str, params: Value, private: bool) -> Result<Value, ApiError> {
        let mut r = self
            .http
            .get(format!(
                "{}/api/v1/{path}",
                self.config.rest_url.trim_end_matches('/')
            ))
            .query(&params);
        if private {
            r = r.header("Authorization", self.signer()?.auth()?);
        }
        let v = response(r.send().await, NAME).await?;
        check(&v)?;
        Ok(v)
    }
    async fn markets(&self) -> Result<Vec<Market>, ApiError> {
        let v = self
            .get("orderBookDetails", json!({"filter":"perp"}), false)
            .await?;
        rows(&v["order_book_details"], NAME)?
            .iter()
            .map(|r| {
                let pp = integer(r, "supported_price_decimals", NAME)?;
                let qp = integer(r, "supported_size_decimals", NAME)?;
                if !(0..=9).contains(&pp) || !(0..=18).contains(&qp) {
                    return Err(error(NAME, "unsupported decimal scale"));
                }
                let native_price = integer(r, "price_decimals", NAME)?;
                let native_size = integer(r, "size_decimals", NAME)?;
                if !(pp..=18).contains(&native_price) || !(qp..=18).contains(&native_size) {
                    return Err(error(
                        NAME,
                        "native decimal scale is inconsistent with order steps",
                    ));
                }
                let index = integer(r, "market_id", NAME)?;
                if !(0..=(1i64 << 48) - 1).contains(&index) || index == 255 {
                    return Err(error(NAME, "market ID outside native protocol range"));
                }
                Ok(Market {
                    index,
                    price_unit: 10f64.powi(-(native_price as i32)),
                    quantity_unit: 10f64.powi(-(native_size as i32)),
                    info: InstrumentInfo {
                        symbol: text(r, "symbol", NAME)?,
                        base_asset: text(r, "symbol", NAME)?,
                        quote_asset: "USDC".into(),
                        margin_asset: "USDC".into(),
                        tick_size: 10f64.powi(-(pp as i32)),
                        lot_size: 10f64.powi(-(qp as i32)),
                        min_qty: number(r, "min_base_amount", NAME)?,
                        contract_size: 1.0,
                        price_precision: pp as u32,
                        qty_precision: qp as u32,
                        tradable: r["status"] == "active" && r["market_type"] == "perp",
                    },
                })
            })
            .collect()
    }
    async fn market(&self, symbol: &str) -> Result<Market, ApiError> {
        self.markets()
            .await?
            .into_iter()
            .find(|m| m.info.symbol == symbol && m.info.tradable)
            .ok_or_else(|| invalid(NAME, "unknown or inactive perpetual"))
    }
    async fn account_raw(&self) -> Result<Value, ApiError> {
        self.signer()?;
        let v = self
            .get(
                "account",
                json!({"by":"index","value":self.config.account_index.to_string()}),
                false,
            )
            .await?;
        let a = rows(&v["accounts"], NAME)?;
        if a.len() != 1 || integer(&a[0], "account_index", NAME)? != self.config.account_index {
            return Err(error(NAME, "account identity mismatch"));
        }
        Ok(a[0].clone())
    }
    fn order(&self, v: &Value, m: &Market) -> Result<OrderInfo, ApiError> {
        if integer(v, "owner_account_index", NAME)? != self.config.account_index
            || integer(v, "market_index", NAME)? != m.index
        {
            return Err(error(NAME, "order identity mismatch"));
        }
        let qty = number(v, "initial_base_amount", NAME)?;
        let done = number(v, "filled_base_amount", NAME)?;
        let remaining = number(v, "remaining_base_amount", NAME)?;
        if qty < 0.0 || done < 0.0 || remaining < 0.0 || done > qty + 1e-10 {
            return Err(error(NAME, "invalid order quantities"));
        }
        let status = match v["status"].as_str() {
            Some("open") => {
                if done > 0.0 {
                    ApiOrderStatus::PartiallyFilled
                } else {
                    ApiOrderStatus::New
                }
            }
            Some("filled") => ApiOrderStatus::Filled,
            Some("pending" | "in-progress") => ApiOrderStatus::Unknown,
            Some("canceled-expired") => ApiOrderStatus::Expired,
            Some(s) if s == "canceled" || s.starts_with("canceled-") => ApiOrderStatus::Canceled,
            _ => return Err(error(NAME, "unknown order status")),
        };
        let native = integer(v, "client_order_index", NAME)?;
        if native < 0 {
            return Err(error(NAME, "negative client ID"));
        }
        Ok(OrderInfo {
            symbol: m.info.symbol.clone(),
            order_id: text(v, "order_index", NAME)?,
            client_order_id: self
                .ids
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .original(native as u64),
            side: if v["is_ask"]
                .as_bool()
                .ok_or_else(|| error(NAME, "missing is_ask"))?
            {
                ApiSide::Sell
            } else {
                ApiSide::Buy
            },
            order_type: match v["type"].as_str() {
                Some("limit") => ApiOrderType::Limit,
                Some("market") => ApiOrderType::Market,
                _ => ApiOrderType::Unknown,
            },
            status,
            price: number(v, "price", NAME)?,
            qty,
            executed_qty: done,
            avg_price: if done > 0.0 {
                number(v, "filled_quote_amount", NAME)? / done
            } else {
                0.0
            },
            leaves_qty: remaining,
            time_in_force: match v["time_in_force"].as_str() {
                Some("good-till-time") => ApiTimeInForce::GTC,
                Some("immediate-or-cancel") => ApiTimeInForce::IOC,
                Some("post-only") => ApiTimeInForce::GTX,
                _ => ApiTimeInForce::Unknown,
            },
            reduce_only: v["reduce_only"]
                .as_bool()
                .ok_or_else(|| error(NAME, "missing reduce_only"))?,
            position_side: ApiPositionSide::Net,
            create_time: timestamp(v, "created_at", 1000, NAME)?,
            update_time: if v["transaction_time"].is_null() {
                timestamp(v, "timestamp", 1000, NAME)?
            } else {
                integer(v, "transaction_time", NAME)? / 1000
            },
            stop_price: None,
        })
    }
    async fn orders_page(
        &self,
        m: &Market,
        active: bool,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Value, ApiError> {
        let mut p = json!({"account_index":self.config.account_index,"market_id":m.index});
        if !active {
            p["limit"] = json!(limit.clamp(1, 100));
            if let Some(c) = cursor {
                p["cursor"] = json!(c);
            }
        }
        self.get(
            if active {
                "accountActiveOrders"
            } else {
                "accountInactiveOrders"
            },
            p,
            true,
        )
        .await
    }
    async fn mutate<F>(&self, guard: Option<SubmitGuard>, sign: F) -> Result<(), ApiError>
    where
        F: FnOnce(&SignerAccount, i64) -> Result<Signed, ApiError> + Send,
    {
        let signer = self.signer()?;
        let mut state = signer.nonce.lock().await;
        if state.uncertain {
            return Err(invalid(
                NAME,
                "nonce outcome is uncertain; reconcile before restarting account",
            ));
        }
        let nonce=match state.next{Some(n)=>n,None=>integer(&self.get("nextNonce",json!({"account_index":self.config.account_index,"api_key_index":self.config.api_key_index}),false).await?,"nonce",NAME)?};
        if nonce < 0 || nonce == i64::MAX {
            return Err(error(NAME, "invalid nonce"));
        }
        check_submit_guard(&guard)?;
        let signed = sign(signer, nonce)?;
        check_submit_guard(&guard)?;
        // Set before the first transport poll: cancellation/drop must also freeze nonce reuse.
        state.uncertain = true;
        let result = response(
            self.http
                .post(format!("{}/api/v1/sendTx", self.config.rest_url))
                .form(&[
                    ("tx_type", signed.kind.to_string()),
                    ("tx_info", signed.info),
                ])
                .send()
                .await,
            NAME,
        )
        .await
        .and_then(|v| {
            check(&v)?;
            Ok(())
        });
        // No write is retried. Even a rejection can race an earlier accepted transaction: freeze this
        // signer until the account has reconciled, rather than guessing nonce consumption.
        match result {
            Ok(()) => {
                state.next = Some(nonce + 1);
                state.uncertain = false;
                Ok(())
            }
            Err(e) => {
                state.uncertain = true;
                Err(if e.outcome_unknown() {
                    e
                } else {
                    ApiError::transport(
                        NAME,
                        "transaction not confirmed; reconcile nonce and order",
                    )
                })
            }
        }
    }
    async fn confirm(
        &self,
        symbol: &str,
        id: Option<&str>,
        client: Option<&str>,
        expected: impl Fn(&OrderInfo) -> bool,
    ) -> Result<OrderInfo, ApiError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.config.confirmation_ms);
        loop {
            match tokio::time::timeout_at(deadline, self.get_order(symbol, id, client)).await {
                Ok(Ok(o)) if expected(&o) => return Ok(o),
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ApiError::transport(
                    NAME,
                    "transaction accepted but order state unconfirmed; reconcile original identity",
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn submit(
        &self,
        r: &UnifiedOrderRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        if r.order_type != ApiOrderType::Limit
            || r.stop_price.is_some()
            || r.position_side.is_some_and(|s| s != ApiPositionSide::Net)
        {
            return Err(unsupported(
                NAME,
                "unpriced market/trigger/hedge order; use price-protected IOC limit",
            ));
        }
        if !r.qty.is_finite() || r.qty <= 0.0 {
            return Err(invalid(NAME, "positive quantity required"));
        }
        let m = self.market(&r.symbol).await?;
        if r.qty < m.info.min_qty {
            return Err(invalid(NAME, "quantity below minimum"));
        }
        let price = r
            .price
            .filter(|p| p.is_finite() && *p > 0.0)
            .ok_or_else(|| invalid(NAME, "limit price required"))?;
        decimal(price, m.info.tick_size, NAME)?;
        decimal(r.qty, m.info.lot_size, NAME)?;
        let p = scaled(price, m.price_unit, u32::MAX as u64)? as u32;
        let q = scaled(r.qty, m.quantity_unit, (1_u64 << 48) - 1)? as i64;
        let tif = match r.time_in_force {
            ApiTimeInForce::GTC => 1,
            ApiTimeInForce::IOC => 0,
            ApiTimeInForce::GTX => 2,
            _ => return Err(unsupported(NAME, "time in force")),
        };
        let ask = match r.side {
            ApiSide::Sell => true,
            ApiSide::Buy => false,
            _ => return Err(invalid(NAME, "invalid side")),
        };
        let original = r
            .client_order_id
            .as_deref()
            .ok_or_else(|| invalid(NAME, "client order ID required"))?;
        check_submit_guard(&guard)?;
        let native = self
            .ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .allocate_order(original, Some(r.reduce_only))? as i64;
        self.mutate(guard, |s, n| {
            s.order(
                signer::OrderParameters {
                    market: m.index,
                    id: native,
                    amount: q,
                    price: p,
                    ask,
                    tif,
                    reduce: r.reduce_only,
                },
                n,
            )
        })
        .await?;
        self.confirm(&r.symbol, None, Some(original), |o| {
            o.status != ApiOrderStatus::Unknown
        })
        .await
    }
    fn fill(&self, v: &Value, m: &Market) -> Result<Fill, ApiError> {
        if integer(v, "market_id", NAME)? != m.index
            || number(v, "size", NAME)? <= 0.0
            || number(v, "price", NAME)? <= 0.0
            || number(v, "usd_amount", NAME)? < 0.0
        {
            return Err(error(NAME, "invalid native trade market or amounts"));
        }
        let ask = integer(v, "ask_account_id", NAME)? == self.config.account_index;
        let bid = integer(v, "bid_account_id", NAME)? == self.config.account_index;
        if ask == bid {
            return Err(error(NAME, "trade account ownership ambiguous"));
        }
        let maker = v["is_maker_ask"]
            .as_bool()
            .ok_or_else(|| error(NAME, "missing maker side"))?
            == ask;
        let client = integer(
            v,
            if ask {
                "ask_client_id"
            } else {
                "bid_client_id"
            },
            NAME,
        )?;
        if client < 0 {
            return Err(error(NAME, "negative trade client ID"));
        }
        // Official FeeTick is 1e6: Trade fee fields are signed rates in ppm. Use the
        // authoritative quote amount, including any native integrator rate, not size*price.
        // The prover keeps the product at collateral precision without USDC-6 rounding.
        let rate = optional_number(v, if maker { "maker_fee" } else { "taker_fee" }, NAME)?
            .unwrap_or(0.0)
            + optional_number(
                v,
                if maker {
                    "integrator_maker_fee"
                } else {
                    "integrator_taker_fee"
                },
                NAME,
            )?
            .unwrap_or(0.0);
        Ok(Fill {
            symbol: m.info.symbol.clone(),
            trade_id: text(v, "trade_id", NAME)?,
            order_id: text(v, if ask { "ask_id" } else { "bid_id" }, NAME)?,
            client_order_id: self
                .ids
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .original(client as u64),
            price: number(v, "price", NAME)?,
            qty: number(v, "size", NAME)?,
            side: if ask { ApiSide::Sell } else { ApiSide::Buy },
            fee: number(v, "usd_amount", NAME)? * rate / 1e6,
            fee_asset: "USDC".into(),
            realized_pnl: optional_number(
                v,
                if ask {
                    "ask_account_pnl"
                } else {
                    "bid_account_pnl"
                },
                NAME,
            )?
            .unwrap_or(0.0),
            maker,
            timestamp: integer(v, "transaction_time", NAME)? / 1000,
        })
    }
}
fn check(v: &Value) -> Result<(), ApiError> {
    if v["code"] != 200 {
        return Err(error(NAME, format!("exchange code {}", v["code"])));
    }
    Ok(())
}
// market_stats rates are percentages and funding_timestamp is the previous hourly settlement.
fn next_funding(v: &Value) -> Result<i64, ApiError> {
    integer(v, "funding_timestamp", NAME)?
        .checked_add(3_600_000)
        .ok_or_else(|| error(NAME, "funding timestamp overflow"))
}
fn scaled(value: f64, step: f64, max: u64) -> Result<u64, ApiError> {
    let n = crate::units::integer_units(value, step, true).map_err(|e| invalid(NAME, e))?;
    if n <= 0 || n as u64 > max {
        return Err(invalid(NAME, "scaled order value out of range"));
    }
    Ok(n as u64)
}
#[async_trait::async_trait]
impl BrokerApi for LighterClient {
    fn supports_order_execution_evidence(&self) -> bool {
        true
    }
    async fn ping(&self) -> Result<(), ApiError> {
        self.get("orderBooks", json!({}), false).await.map(|_| ())
    }
    async fn get_server_time(&self) -> Result<i64, ApiError> {
        Err(unsupported(NAME, "dedicated server time"))
    }
    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        Ok(self.markets().await?.into_iter().map(|m| m.info).collect())
    }
    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        let frame =
            stream::snapshot(self, "market_stats", self.market(symbol).await?.index).await?;
        let v = &frame["market_stats"];
        Ok(Ticker {
            symbol: symbol.into(),
            last_price: number(v, "last_trade_price", NAME)?,
            mark_price: Some(number(v, "mark_price", NAME)?),
            index_price: Some(number(v, "index_price", NAME)?),
            funding_rate: Some(number(v, "current_funding_rate", NAME)? / 100.0),
            next_funding_time: Some(next_funding(v)?),
            open_24h: 0.0,
            high_24h: number(v, "daily_price_high", NAME)?,
            low_24h: number(v, "daily_price_low", NAME)?,
            volume_24h: number(v, "daily_base_token_volume", NAME)?,
            quote_volume_24h: number(v, "daily_quote_token_volume", NAME)?,
            timestamp: integer(&frame, "timestamp", NAME)?,
        })
    }
    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let value = self
            .get("orderBookDetails", json!({"filter":"perp"}), false)
            .await?;
        rows(&value["order_book_details"], NAME)?
            .iter()
            .map(|v| {
                Ok(Ticker {
                    symbol: text(v, "symbol", NAME)?,
                    last_price: number(v, "last_trade_price", NAME)?,
                    mark_price: optional_number(v, "mark_price", NAME)?,
                    index_price: optional_number(v, "index_price", NAME)?,
                    funding_rate: None,
                    next_funding_time: None,
                    open_24h: 0.0,
                    high_24h: number(v, "daily_price_high", NAME)?,
                    low_24h: number(v, "daily_price_low", NAME)?,
                    volume_24h: number(v, "daily_base_token_volume", NAME)?,
                    quote_volume_24h: number(v, "daily_quote_token_volume", NAME)?,
                    timestamp: 0,
                })
            })
            .collect()
    }
    async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<OrderBook, ApiError> {
        if limit == 0 {
            return Err(invalid(NAME, "positive depth limit required"));
        }
        let v = stream::snapshot(self, "order_book", self.market(symbol).await?.index).await?;
        let parse = |k| -> Result<Vec<PriceLevel>, ApiError> {
            rows(&v["order_book"][k], NAME)?
                .iter()
                .take(limit as usize)
                .map(|p| {
                    Ok(PriceLevel {
                        price: number(p, "price", NAME)?,
                        qty: number(p, "size", NAME)?,
                    })
                })
                .collect()
        };
        Ok(OrderBook {
            symbol: symbol.into(),
            bids: parse("bids")?,
            asks: parse("asks")?,
            timestamp: integer(&v["order_book"], "last_updated_at", NAME)? / 1000,
        })
    }
    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let m = self.market(symbol).await?;
        let v=self.get("trades",json!({"market_id":m.index,"sort_by":"timestamp","sort_dir":"desc","limit":limit.clamp(1,100)}),false).await?;
        rows(&v["trades"], NAME)?
            .iter()
            .map(|r| {
                Ok(Trade {
                    symbol: symbol.into(),
                    id: text(r, "trade_id", NAME)?,
                    price: number(r, "price", NAME)?,
                    qty: number(r, "size", NAME)?,
                    side: if r["is_maker_ask"] == true {
                        ApiSide::Buy
                    } else {
                        ApiSide::Sell
                    },
                    timestamp: integer(r, "timestamp", NAME)?,
                })
            })
            .collect()
    }
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, ApiError> {
        let seconds = match interval {
            "1m" => 60,
            "5m" => 300,
            "15m" => 900,
            "30m" => 1800,
            "1h" => 3600,
            "4h" => 14400,
            "12h" => 43200,
            "1d" => 86400,
            _ => return Err(unsupported(NAME, "candle resolution")),
        };
        if limit == 0 || limit > 500 {
            return Err(invalid(NAME, "candle limit must be 1..500"));
        }
        let market = self.market(symbol).await?;
        let end = Utc::now().timestamp();
        let value=self.get("candles",json!({"market_id":market.index,"resolution":interval,"start_timestamp":end-i64::from(limit)*seconds,"end_timestamp":end,"count_back":limit,"set_timestamp_to_end":false}),false).await?;
        if value["r"] != interval {
            return Err(error(NAME, "candle resolution mismatch"));
        }
        let mut bars: Vec<_> = rows(&value["c"], NAME)?
            .iter()
            .map(|r| {
                let open_time = integer(r, "t", NAME)?;
                Ok(Kline {
                    symbol: symbol.into(),
                    interval: interval.into(),
                    open_time,
                    close_time: open_time
                        .checked_add(seconds * 1000 - 1)
                        .ok_or_else(|| error(NAME, "candle time overflow"))?,
                    open: number(r, "o", NAME)?,
                    high: number(r, "h", NAME)?,
                    low: number(r, "l", NAME)?,
                    close: number(r, "c", NAME)?,
                    volume: number(r, "v", NAME)?,
                    quote_volume: number(r, "V", NAME)?,
                })
            })
            .collect::<Result<_, ApiError>>()?;
        bars.sort_by_key(|b| b.open_time);
        if bars.len() > limit as usize {
            bars.drain(..bars.len() - limit as usize);
        }
        Ok(bars)
    }
    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError> {
        let t = self.get_ticker(symbol).await?;
        Ok(FundingRate {
            symbol: symbol.into(),
            funding_rate: t
                .funding_rate
                .ok_or_else(|| error(NAME, "missing funding"))?,
            next_funding_time: t.next_funding_time.unwrap_or(0),
            timestamp: t.timestamp,
        })
    }
    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        if limit == 0 || limit > 750 {
            return Err(invalid(NAME, "funding history limit must be 1..750"));
        }
        let market = self.market(symbol).await?;
        let end = Utc::now().timestamp();
        let value=self.get("fundings",json!({"market_id":market.index,"resolution":"1h","start_timestamp":end-i64::from(limit)*3600,"end_timestamp":end,"count_back":limit}),false).await?;
        if value["resolution"] != "1h" {
            return Err(error(NAME, "funding resolution mismatch"));
        }
        let mut history: Vec<_> = rows(&value["fundings"], NAME)?
            .iter()
            .map(|r| {
                let rate = number(r, "rate", NAME)?;
                if rate < 0.0 {
                    return Err(error(NAME, "negative unsigned funding history rate"));
                }
                let direction = match r["direction"].as_str() {
                    Some("long") => 1.0,
                    Some("short") => -1.0,
                    _ => return Err(error(NAME, "unknown funding direction")),
                };
                let timestamp = timestamp(r, "timestamp", 1000, NAME)?;
                Ok(FundingRate {
                    symbol: symbol.into(),
                    funding_rate: rate * direction / 100.0,
                    timestamp,
                    next_funding_time: timestamp,
                })
            })
            .collect::<Result<_, ApiError>>()?;
        history.sort_by_key(|r| r.timestamp);
        if history.len() > limit as usize {
            history.drain(..history.len() - limit as usize);
        }
        Ok(history)
    }
    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let v = self
            .get(
                "orderBookDetails",
                json!({"market_id":self.market(symbol).await?.index}),
                false,
            )
            .await?;
        let r = rows(&v["order_book_details"], NAME)?
            .first()
            .ok_or_else(|| error(NAME, "market missing"))?;
        Ok(OpenInterest {
            symbol: symbol.into(),
            open_interest: number(r, "open_interest", NAME)?,
            timestamp: 0,
        })
    }
    async fn submit_order(&self, r: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        self.submit(r, None).await
    }
    async fn submit_order_fenced(
        &self,
        r: &UnifiedOrderRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        self.submit(r, guard).await
    }
    async fn submit_orders(&self, r: &[UnifiedOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        if r.len() != 1 {
            return Err(unsupported(
                NAME,
                "non-atomic batch submit; submit individually",
            ));
        }
        Ok(vec![self.submit_order(&r[0]).await?])
    }
    async fn cancel_order(&self, r: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        let m = self.market(&r.symbol).await?;
        let o = self
            .get_order(
                &r.symbol,
                r.order_id.as_deref(),
                r.client_order_id.as_deref(),
            )
            .await?;
        if matches!(
            o.status,
            ApiOrderStatus::Filled
                | ApiOrderStatus::Canceled
                | ApiOrderStatus::Expired
                | ApiOrderStatus::Rejected
        ) {
            return Ok(o);
        }
        let index = o
            .order_id
            .parse()
            .map_err(|_| error(NAME, "invalid native order index"))?;
        self.mutate(None, |s, n| s.cancel(m.index, index, n))
            .await?;
        self.confirm(&r.symbol, Some(&o.order_id), None, |o| {
            matches!(
                o.status,
                ApiOrderStatus::Filled
                    | ApiOrderStatus::Canceled
                    | ApiOrderStatus::Expired
                    | ApiOrderStatus::Rejected
            )
        })
        .await
    }
    async fn cancel_orders(&self, r: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        let mut result = Vec::new();
        let mut failure = None;
        for r in r {
            match self.cancel_order(r).await {
                Ok(o) => result.push(o),
                Err(e) => failure = Some(e),
            }
        }
        if let Some(e) = failure {
            Err(e)
        } else {
            Ok(result)
        }
    }
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        let open = self.get_open_orders(symbol).await?;
        let req: Vec<_> = open
            .into_iter()
            .map(|o| CancelOrderRequest {
                symbol: symbol.into(),
                order_id: Some(o.order_id),
                client_order_id: None,
            })
            .collect();
        let result = self.cancel_orders(&req).await;
        if !self.get_open_orders(symbol).await?.is_empty() {
            return Err(ApiError::transport(
                NAME,
                "cancel-all leaves outstanding orders",
            ));
        }
        result.map(|_| ())
    }
    async fn cancel_all_after(&self, _: u64) -> Result<(), ApiError> {
        Err(unsupported(NAME, "account-wide scheduled cancellation"))
    }
    async fn amend_order(&self, r: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        if r.new_stop_price.is_some() {
            return Err(unsupported(NAME, "trigger amendment"));
        }
        let m = self.market(&r.symbol).await?;
        let old = self
            .get_order(
                &r.symbol,
                r.order_id.as_deref(),
                r.client_order_id.as_deref(),
            )
            .await?;
        if old.order_type != ApiOrderType::Limit
            || !matches!(
                old.status,
                ApiOrderStatus::New | ApiOrderStatus::PartiallyFilled
            )
        {
            return Err(invalid(NAME, "only active limit orders can be amended"));
        }
        let p = r.new_price.unwrap_or(old.price);
        let q = r.new_qty.unwrap_or(old.qty);
        if q < old.executed_qty || q < m.info.min_qty {
            return Err(invalid(NAME, "invalid amended quantity"));
        }
        decimal(p, m.info.tick_size, NAME)?;
        decimal(q, m.info.lot_size, NAME)?;
        let ps = scaled(p, m.price_unit, u32::MAX as u64)? as u32;
        let qs = scaled(q, m.quantity_unit, (1_u64 << 48) - 1)? as i64;
        let index = old
            .order_id
            .parse()
            .map_err(|_| error(NAME, "invalid order index"))?;
        self.mutate(None, |s, n| s.modify(m.index, index, qs, ps, 0, n))
            .await?;
        self.confirm(&r.symbol, Some(&old.order_id), None, |o| {
            (o.price - p).abs() < m.info.tick_size / 2.0
                && (o.qty - q).abs() < m.info.lot_size / 2.0
        })
        .await
    }
    async fn get_order(
        &self,
        symbol: &str,
        id: Option<&str>,
        client: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        if id.is_none() && client.is_none() {
            return Err(invalid(NAME, "order identity required"));
        }
        let m = self.market(symbol).await?;
        let native = client
            .map(|c| self.ids.lock().unwrap_or_else(|p| p.into_inner()).native(c))
            .transpose()?;
        let matches = |r: &Value| -> Result<bool, ApiError> {
            Ok(
                id.is_none_or(|id| r["order_index"].to_string().trim_matches('"') == id)
                    && native.is_none_or(|n| r["client_order_index"].as_u64() == Some(n)),
            )
        };
        let v = self.orders_page(&m, true, None, 100).await?;
        for r in rows(&v["orders"], NAME)? {
            if matches(r)? {
                return self.order(r, &m);
            }
        }
        let mut cursor = None;
        for _ in 0..100 {
            let v = self.orders_page(&m, false, cursor.as_deref(), 100).await?;
            for r in rows(&v["orders"], NAME)? {
                if matches(r)? {
                    return self.order(r, &m);
                }
            }
            let next = v["next_cursor"].as_str().filter(|s| !s.is_empty());
            match next {
                None => {
                    return Err(ApiError::new(
                        NAME,
                        "ORDER_NOT_FOUND",
                        "order identity absent from bounded history",
                    ));
                }
                Some(n) if cursor.as_deref() != Some(n) => cursor = Some(n.to_owned()),
                _ => return Err(error(NAME, "history cursor did not advance")),
            }
        }
        Err(error(NAME, "order history search capacity exceeded"))
    }
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let m = self.market(symbol).await?;
        let v = self.orders_page(&m, true, None, 100).await?;
        if v["next_cursor"].as_str().is_some_and(|s| !s.is_empty()) {
            return Err(error(NAME, "incomplete active orders"));
        }
        rows(&v["orders"], NAME)?
            .iter()
            .map(|r| self.order(r, &m))
            .collect()
    }
    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let m = self.market(symbol).await?;
        let v = self.orders_page(&m, false, None, limit).await?;
        rows(&v["orders"], NAME)?
            .iter()
            .map(|r| self.order(r, &m))
            .collect()
    }
    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        if limit == 0 || limit > 4096 {
            return Err(invalid(NAME, "fill limit must be 1..4096"));
        }
        let market = self.market(symbol).await?;
        let mut result = Vec::new();
        let mut cursor = None;
        while result.len() < limit as usize {
            let count = (limit as usize - result.len()).min(100);
            let mut params = json!({"market_id":market.index,"account_index":self.config.account_index,"sort_by":"timestamp","sort_dir":"desc","limit":count});
            if let Some(cursor) = &cursor {
                params["cursor"] = json!(cursor);
            }
            let value = self.get("trades", params, true).await?;
            let trades = rows(&value["trades"], NAME)?;
            if trades.len() > count {
                return Err(error(NAME, "fill page exceeds requested limit"));
            }
            for trade in trades {
                result.push(self.fill(trade, &market)?);
            }
            let next = value["next_cursor"].as_str().filter(|s| !s.is_empty());
            if trades.is_empty() || next.is_none() {
                break;
            }
            let next = next.unwrap();
            if cursor.as_deref() == Some(next) {
                return Err(error(NAME, "fill cursor did not advance"));
            }
            cursor = Some(next.to_owned());
        }
        Ok(result)
    }
    async fn get_account_position_mode(&self) -> Result<ApiPositionMode, ApiError> {
        Ok(ApiPositionMode::Net)
    }
    fn position_currency(&self, _: &str) -> Option<String> {
        Some("USDC".into())
    }
    async fn validate_account_configuration(&self) -> Result<(), ApiError> {
        let a = self.account_raw().await?;
        if integer(&a, "account_trading_mode", NAME)? != 0 {
            return Err(unsupported(
                NAME,
                "unified trading accounts; simple USDC mode required",
            ));
        }
        for p in rows(&a["positions"], NAME)? {
            if integer(p, "margin_mode", NAME)? != 0 && number(p, "position", NAME)? != 0.0 {
                return Err(unsupported(NAME, "isolated margin positions"));
            }
        }
        for a in rows(&a["assets"], NAME)? {
            if a["symbol"] != "USDC" && number(a, "balance", NAME)? != 0.0 {
                return Err(unsupported(NAME, "non-USDC assets"));
            }
        }
        Ok(())
    }
    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let a = self.account_raw().await?;
        let wallet = number(&a, "collateral", NAME)?;
        let pnl = rows(&a["positions"], NAME)?.iter().try_fold(0.0, |n, p| {
            Ok::<_, ApiError>(n + number(p, "unrealized_pnl", NAME)?)
        })?;
        let available = number(&a, "available_balance", NAME)?;
        Ok(AccountInfo {
            total_wallet_balance: wallet,
            total_margin_balance: wallet + pnl,
            total_unrealized_pnl: pnl,
            available_balance: available,
            balances: vec![Balance {
                asset: "USDC".into(),
                wallet_balance: wallet,
                available_balance: available,
                unrealized_pnl: pnl,
                margin_balance: wallet + pnl,
            }],
            timestamp: integer(&a, "transaction_time", NAME)? / 1000,
        })
    }
    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let a = self.account_raw().await?;
        rows(&a["positions"], NAME)?
            .iter()
            .filter(|p| symbol.is_none_or(|s| p["symbol"] == s))
            .map(|p| position(p, integer(&a, "transaction_time", NAME)? / 1000))
            .collect()
    }
    async fn set_leverage(
        &self,
        _: &str,
        _: f64,
        _: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        Err(unsupported(NAME, "account mode or leverage mutation"))
    }
    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        let a = self.account_raw().await?;
        let p = rows(&a["positions"], NAME)?
            .iter()
            .find(|p| p["symbol"] == symbol)
            .ok_or_else(|| error(NAME, "no position leverage fact"))?;
        let fraction = number(p, "initial_margin_fraction", NAME)?;
        if fraction <= 0.0 {
            return Err(error(NAME, "invalid initial margin fraction"));
        }
        Ok(LeverageInfo {
            symbol: symbol.into(),
            leverage: 10000.0 / fraction,
            margin_type: ApiMarginType::Cross,
            position_side: ApiPositionSide::Net,
        })
    }
    async fn get_fee_rates(&self, _: &str) -> Result<FeeRate, ApiError> {
        Err(unsupported(
            NAME,
            "account-specific fees; Standard/Premium rates differ",
        ))
    }
    async fn get_income_history(&self, _: &str, _: u32) -> Result<Vec<IncomeRecord>, ApiError> {
        Err(unsupported(NAME, "income history"))
    }
}
fn position(p: &Value, ts: i64) -> Result<PositionInfo, ApiError> {
    let size = number(p, "position", NAME)?;
    let sign = integer(p, "sign", NAME)?;
    if !matches!(sign, -1..=1) || size < 0.0 || size != 0.0 && sign == 0 {
        return Err(error(NAME, "invalid position sign"));
    }
    let qty = size * sign as f64;
    Ok(PositionInfo {
        symbol: text(p, "symbol", NAME)?,
        position_side: ApiPositionSide::Net,
        qty,
        entry_price: number(p, "avg_entry_price", NAME)?,
        mark_price: if size > 0.0 {
            number(p, "position_value", NAME)?.abs() / size
        } else {
            0.0
        },
        liquidation_price: optional_number(p, "liquidation_price", NAME)?.unwrap_or(0.0),
        leverage: if number(p, "initial_margin_fraction", NAME)? > 0.0 {
            10000.0 / number(p, "initial_margin_fraction", NAME)?
        } else {
            0.0
        },
        margin_type: if p["margin_mode"] == 0 {
            ApiMarginType::Cross
        } else {
            ApiMarginType::Isolated
        },
        unrealized_pnl: number(p, "unrealized_pnl", NAME)?,
        realized_pnl: number(p, "realized_pnl", NAME)?,
        notional: number(p, "position_value", NAME)?,
        update_time: ts,
    })
}
#[async_trait::async_trait]
impl Venue for LighterClient {
    const NAME: &'static str = NAME;
    fn build(config: &str) -> Result<Self, ApiError> {
        Self::new(toml::from_str(config).map_err(|_| invalid(NAME, "invalid config"))?)
    }
    fn reconnect_ms(&self) -> u64 {
        self.config.reconnect_ms
    }
    async fn session(
        &self,
        desired: Desired,
        orders: Arc<Mutex<Orders>>,
        tx: crate::connector::PublishSender,
        private: bool,
        epoch: u64,
    ) -> Result<(), ApiError> {
        stream::session(self, desired, orders, tx, private, epoch).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::test_support::*;
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn market() -> Value {
        json!({"code":200,"order_book_details":[{"market_id":0,"symbol":"ETH","market_type":"perp","status":"active","supported_price_decimals":1,"supported_size_decimals":3,"size_decimals":3,"price_decimals":1,"min_base_amount":"0.001"}],"spot_order_book_details":[]})
    }
    fn order(account: i64) -> Value {
        json!({"owner_account_index":account,"market_index":0,"order_index":999,"client_order_index":1,"initial_base_amount":"0.020","filled_base_amount":"0","filled_quote_amount":"0","remaining_base_amount":"0.020","is_ask":false,"type":"limit","status":"open","price":"100.1","time_in_force":"post-only","reduce_only":true,"created_at":1700000000,"timestamp":1700000001,"transaction_time":1700000001123456i64})
    }
    #[test]
    fn exact_integer_scaling_and_network_isolation() {
        assert_eq!(scaled(100.1, 0.1, u32::MAX as u64).unwrap(), 1001);
        assert!(scaled(100.15, 0.1, u32::MAX as u64).is_err());
        assert!(scaled(1e20, 0.1, u32::MAX as u64).is_err());
        assert!(
            LighterClient::new(Config {
                chain_id: 300,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            LighterClient::new(Config {
                rest_url: "https://api.rh.lighter.xyz".into(),
                ..Default::default()
            })
            .is_err()
        );
    }
    #[test]
    fn book_nonce_is_continuous_but_offsets_need_not_be() {
        let mut s = stream::DepthSequence::default();
        assert!(s.accept(false, 100, 101).is_err());
        s.accept(true, 0, 100).unwrap();
        s.accept(false, 100, 103).unwrap();
        assert!(s.accept(false, 102, 104).is_err());
        assert!(s.accept(false, 103, 102).is_err());
        s.accept(true, 0, 900).unwrap();
        s.accept(false, 900, 901).unwrap();
    }
    #[test]
    fn native_fill_uses_quote_times_ppm_with_signed_rebates_and_native_pnl() {
        let c = LighterClient::new(Config {
            account_index: 42,
            ..Default::default()
        })
        .unwrap();
        let m = Market {
            index: 0,
            price_unit: 0.1,
            quantity_unit: 0.001,
            info: InstrumentInfo {
                symbol: "ETH".into(),
                base_asset: "ETH".into(),
                quote_asset: "USDC".into(),
                margin_asset: "USDC".into(),
                tick_size: 0.1,
                lot_size: 0.001,
                min_qty: 0.001,
                contract_size: 1.0,
                price_precision: 1,
                qty_precision: 3,
                tradable: true,
            },
        };
        let mut v = json!({"market_id":0,"trade_id":16164557907i64,"ask_id":999,"bid_id":1000,"ask_client_id":1,"bid_client_id":2,"ask_account_id":42,"bid_account_id":43,"is_maker_ask":true,"size":"0.1336","price":"2181.83","usd_amount":"291.492488","maker_fee":28,"taker_fee":196,"ask_account_pnl":"1.234567","transaction_time":1773854156686065i64});
        let fill = c.fill(&v, &m).unwrap();
        assert!((fill.fee - 0.008161789664).abs() < 1e-15);
        assert_eq!(fill.realized_pnl, 1.234567);
        assert_eq!(fill.timestamp, 1773854156686);
        assert_eq!(fill.order_id, "999");
        v["maker_fee"] = json!(-28);
        assert!(c.fill(&v, &m).unwrap().fee < 0.0);
        v["bid_account_id"] = json!(42);
        assert!(c.fill(&v, &m).is_err());
    }
    #[tokio::test]
    async fn native_abi_serializes_nonce_freezes_unknown_and_checks_final_guard() {
        // Compiled only by tests on the designated build server. No account keys or network outside
        // loopback are used. The fixture validates every C argument and is explicitly not a signer.
        let lib = path("signer").replace(".jsonl", ".so");
        let source = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/lighter_signer.c"
        );
        assert!(
            std::process::Command::new("cc")
                .args(["-shared", "-fPIC", source, "-o", &lib])
                .status()
                .unwrap()
                .success()
        );
        let sha = hex::encode(Sha256::digest(std::fs::read(&lib).unwrap()));
        let responses = vec![
            (200, market()),
            (200, json!({"code":200,"nonce":5})),
            (200, json!({"code":200})),
            (200, market()),
            (
                200,
                json!({"code":200,"orders":[order(42)],"next_cursor":""}),
            ),
            (503, json!({"code":500})),
            (0, json!({})), // last transport intentionally never receives a response
        ];
        let (url, mut rx, task) = http(responses).await;
        let path = path("lighter-abi");
        let c = LighterClient::new(Config {
            rest_url: url,
            ws_url: "ws://127.0.0.1:1".into(),
            account_index: 42,
            api_private_key: "fixture-key".into(),
            signer_path: Some(lib.clone()),
            signer_sha256: Some(sha),
            client_id_journal: Some(path.clone()),
            ..Default::default()
        })
        .unwrap();
        let request = UnifiedOrderRequest {
            symbol: "ETH".into(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(100.1),
            qty: 0.02,
            time_in_force: ApiTimeInForce::GTX,
            reduce_only: true,
            position_side: Some(ApiPositionSide::Net),
            client_order_id: Some("original-128-bit-identity".into()),
            stop_price: None,
        };
        let result = c.submit_order(&request).await.unwrap();
        assert_eq!(result.client_order_id, "original-128-bit-identity");
        assert_eq!(result.update_time, 1700000001123);
        let mut sent = Vec::new();
        for _ in 0..5 {
            sent.push(rx.recv().await.unwrap());
        }
        assert!(sent[2].body.contains("tx_type=14"));
        assert!(sent[2].body.contains("Nonce%22%3A5"));
        assert_eq!(c.signer().unwrap().nonce.lock().await.next, Some(6));
        let calls = Arc::new(AtomicUsize::new(0));
        let out = calls.clone();
        let guard: SubmitGuard = Arc::new(move || {
            if out.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(ApiError::new(
                    "test",
                    "STALE",
                    "epoch changed during signing",
                ))
            }
        });
        let e = c
            .mutate(Some(guard), |s, n| s.cancel(0, 999, n))
            .await
            .unwrap_err();
        assert_eq!(e.code, "STALE");
        assert!(!c.signer().unwrap().nonce.lock().await.uncertain);
        let e = c
            .mutate(None, |s, n| s.cancel(0, 999, n))
            .await
            .unwrap_err();
        assert!(e.outcome_unknown());
        rx.recv().await.unwrap();
        let e = c
            .mutate(None, |s, n| s.cancel(0, 999, n))
            .await
            .unwrap_err();
        assert_eq!(e.code, "INVALID");
        let second_path = crate::venue::test_support::path("lighter-cancelled-transport");
        let second = LighterClient::new(Config {
            account_index: 43,
            client_id_journal: Some(second_path.clone()),
            ..c.config.clone()
        })
        .unwrap();
        second.signer().unwrap().nonce.lock().await.next = Some(9);
        let writer = second.clone();
        let sending =
            tokio::spawn(async move { writer.mutate(None, |s, n| s.cancel(0, 999, n)).await });
        let request = rx.recv().await.unwrap();
        assert!(request.head.starts_with("POST /api/v1/sendTx "));
        sending.abort();
        assert!(sending.await.unwrap_err().is_cancelled());
        assert!(second.signer().unwrap().nonce.lock().await.uncertain);
        assert!(
            second
                .mutate(None, |s, n| s.cancel(0, 999, n))
                .await
                .is_err()
        );
        task.abort();
        let _ = task.await;
        drop(second);
        drop(c);
        std::fs::remove_file(second_path).unwrap();
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(lib).unwrap();
    }
}
#[cfg(test)]
mod native_units_tests {
    use super::*;
    use crate::venue::test_support::*;
    #[tokio::test]
    async fn signing_uses_native_scale_independently_of_accepted_order_steps() {
        let(url,mut rx,task)=http(vec![(200,json!({"code":200,"order_book_details":[{"market_id":0,"symbol":"ETH","market_type":"perp","status":"active","supported_price_decimals":1,"supported_size_decimals":3,"size_decimals":5,"price_decimals":3,"min_base_amount":"0.001"}]}))]).await;
        let c = LighterClient::new(Config {
            rest_url: url,
            ..Default::default()
        })
        .unwrap();
        let m = c.market("ETH").await.unwrap();
        assert_eq!(m.info.tick_size, 0.1);
        assert_eq!(m.info.lot_size, 0.001);
        assert_eq!(
            scaled(100.1, m.price_unit, u32::MAX as u64).unwrap(),
            100100
        );
        assert_eq!(scaled(0.02, m.quantity_unit, (1 << 48) - 1).unwrap(), 2000);
        rx.recv().await.unwrap();
        task.await.unwrap();
    }
    #[test]
    fn funding_timestamp_is_milliseconds_and_points_to_the_previous_hour() {
        assert_eq!(
            next_funding(&json!({"funding_timestamp":1786370400000i64})).unwrap(),
            1786374000000
        );
    }
}
#[cfg(test)]
mod historical_tests {
    use super::*;
    use crate::venue::test_support::*;
    fn market() -> Value {
        json!({"code":200,"order_book_details":[{"market_id":0,"symbol":"ETH","market_type":"perp","status":"active","supported_price_decimals":2,"supported_size_decimals":4,"size_decimals":4,"price_decimals":2,"min_base_amount":"0.0020"}]})
    }
    #[tokio::test]
    async fn candles_query_seconds_decode_milliseconds_and_respect_limit() {
        let bars = json!({"code":200,"r":"1m","c":[{"t":1793376780000i64,"o":2230.36,"h":2232.13,"l":2229.96,"c":2231.22,"v":208.1684,"V":464449.223857},{"t":1793376840000i64,"o":2231.22,"h":2232.17,"l":2230.15,"c":2232.09,"v":147.7166,"V":329587.965404},{"t":1793376900000i64,"o":2232.09,"h":2233.18,"l":2230.96,"c":2231.42,"v":106.1461,"V":236936.457572}]});
        let (url, mut rx, task) = http(vec![(200, market()), (200, bars)]).await;
        let c = LighterClient::new(Config {
            rest_url: url,
            ..Default::default()
        })
        .unwrap();
        let bars = c.get_klines("ETH", "1m", 2).await.unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open_time, 1793376840000);
        assert_eq!(bars[1].close_time, 1793376959999);
        rx.recv().await.unwrap();
        let request = rx.recv().await.unwrap();
        assert!(request.head.contains("count_back=2"));
        let path = request
            .head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = reqwest::Url::parse(&format!("http://fixture{path}")).unwrap();
        let end = url
            .query_pairs()
            .find(|(k, _)| k == "end_timestamp")
            .unwrap()
            .1
            .parse::<i64>()
            .unwrap();
        assert!(end < 10_000_000_000);
        task.await.unwrap();
    }
    #[tokio::test]
    async fn funding_history_keeps_payer_sign_and_converts_seconds_to_milliseconds() {
        let(url,mut rx,task)=http(vec![(200,market()),(200,json!({"code":200,"resolution":"1h","fundings":[{"timestamp":1793372400i64,"rate":"0.0002","value":"0.004","direction":"long"},{"timestamp":1793376000i64,"rate":"0.0001","value":"0.00222742","direction":"short"}]}))]).await;
        let c = LighterClient::new(Config {
            rest_url: url,
            ..Default::default()
        })
        .unwrap();
        let f = c.get_funding_rate_history("ETH", 2).await.unwrap();
        assert_eq!(f[0].timestamp, 1793372400000);
        assert!((f[0].funding_rate - 0.000002).abs() < 1e-15);
        assert!((f[1].funding_rate + 0.000001).abs() < 1e-15);
        for _ in 0..2 {
            rx.recv().await.unwrap();
        }
        task.await.unwrap();
    }
}
