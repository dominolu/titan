//! Backpack REST/WS. Portable BrokerApi handles USDC perpetuals; the native facade covers all APIs.
mod native;
mod stream;
use crate::{api::*, venue::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
pub use native::{BackpackNativeWebSocket, BackpackOperation, WEBSOCKET_STREAMS};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
const NAME: &str = "backpack";
pub type Backpack = VenueConnector<BackpackClient>;
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rest_url: String,
    pub ws_url: String,
    pub api_key: String,
    pub secret: String,
    pub client_id_journal: Option<String>,
    pub reconnect_ms: u64,
    pub window_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rest_url: "https://api.backpack.exchange".into(),
            ws_url: "wss://ws.backpack.exchange".into(),
            api_key: String::new(),
            secret: String::new(),
            client_id_journal: None,
            reconnect_ms: 1000,
            window_ms: 5000,
        }
    }
}
#[derive(Clone)]
pub struct BackpackClient {
    config: Config,
    http: reqwest::Client,
    key: Option<Arc<SigningKey>>,
    ids: Arc<Mutex<ClientIds>>,
}
impl BackpackClient {
    pub fn new(config: Config) -> Result<Self, ApiError> {
        endpoint(&config.rest_url, false, NAME)?;
        endpoint(&config.ws_url, true, NAME)?;
        if config.reconnect_ms == 0 || config.window_ms == 0 || config.window_ms > 60_000 {
            return Err(invalid(NAME, "invalid reconnect/window"));
        }
        let key = if config.api_key.is_empty() && config.secret.is_empty() {
            None
        } else {
            let seed: [u8; 32] = STANDARD
                .decode(&config.secret)
                .map_err(|_| invalid(NAME, "secret must be a base64 Ed25519 seed"))?
                .try_into()
                .map_err(|_| invalid(NAME, "secret must decode to 32 bytes"))?;
            let key = SigningKey::from_bytes(&seed);
            if STANDARD.encode(key.verifying_key().as_bytes()) != config.api_key {
                return Err(invalid(NAME, "API verifying key does not match secret"));
            }
            Some(Arc::new(key))
        };
        if key.is_some() && config.client_id_journal.is_none() {
            return Err(invalid(
                NAME,
                "private connector requires client_id_journal",
            ));
        }
        let ids = ClientIds::open(
            config.client_id_journal.as_deref(),
            &format!("backpack {} {}", config.rest_url, config.api_key),
            u32::MAX as u64,
            NAME,
        )?;
        Ok(Self {
            http: http_client(NAME)?,
            config,
            key,
            ids,
        })
    }
    fn signature(&self, instruction: &str, params: &Value, ts: i64) -> Result<String, ApiError> {
        let key = self
            .key
            .as_ref()
            .ok_or_else(|| invalid(NAME, "credentials required"))?;
        let mut s = format!("instruction={instruction}");
        let object = params
            .as_object()
            .ok_or_else(|| invalid(NAME, "request must be an object"))?;
        let sorted: BTreeMap<_, _> = object.iter().collect();
        for (k, v) in sorted {
            let value = match v {
                Value::String(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                _ => return Err(invalid(NAME, "nested signing parameter")),
            };
            if value.contains(['&', '=']) {
                return Err(invalid(NAME, "invalid signing parameter"));
            }
            s.push_str(&format!("&{k}={value}"));
        }
        s.push_str(&format!("&timestamp={ts}&window={}", self.config.window_ms));
        Ok(STANDARD.encode(key.sign(s.as_bytes()).to_bytes()))
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        params: Value,
        instruction: Option<&str>,
        guard: Option<SubmitGuard>,
    ) -> Result<Value, ApiError> {
        let mut r = self.http.request(
            method.clone(),
            format!("{}{path}", self.config.rest_url.trim_end_matches('/')),
        );
        if let Some(instruction) = instruction {
            let ts = Utc::now().timestamp_millis();
            r = r
                .header("X-API-Key", &self.config.api_key)
                .header("X-Timestamp", ts.to_string())
                .header("X-Window", self.config.window_ms.to_string())
                .header("X-Signature", self.signature(instruction, &params, ts)?);
        }
        if method == Method::GET {
            r = r.query(&params);
        } else {
            r = r.json(&params);
        }
        check_submit_guard(&guard)?;
        response(r.send().await, NAME).await
    }
    async fn get(
        &self,
        path: &str,
        params: Value,
        instruction: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.request(Method::GET, path, params, instruction, None)
            .await
    }
    async fn instrument(&self, symbol: &str) -> Result<InstrumentInfo, ApiError> {
        self.get_instruments()
            .await?
            .into_iter()
            .find(|v| v.symbol == symbol && v.tradable)
            .ok_or_else(|| invalid(NAME, "unknown or closed USDC perpetual"))
    }
    fn original(&self, v: &Value, key: &str) -> Result<String, ApiError> {
        if v[key].is_null() {
            return Ok(String::new());
        }
        let n = text(v, key, NAME)?
            .parse()
            .map_err(|_| error(NAME, "invalid client ID"))?;
        Ok(self
            .ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .original(n))
    }
    fn order(&self, v: &Value) -> Result<OrderInfo, ApiError> {
        let qty = number(v, "quantity", NAME)?;
        let done = number(v, "executedQuantity", NAME)?;
        if qty < 0.0 || done < 0.0 || done > qty + 1e-10 {
            return Err(error(NAME, "invalid order quantities"));
        }
        let status = match v["status"].as_str() {
            Some("New") => ApiOrderStatus::New,
            Some("PartiallyFilled") => ApiOrderStatus::PartiallyFilled,
            Some("Filled") => ApiOrderStatus::Filled,
            Some("Cancelled") => ApiOrderStatus::Canceled,
            Some("Expired") => ApiOrderStatus::Expired,
            Some("Rejected") => ApiOrderStatus::Rejected,
            Some("TriggerPending") => ApiOrderStatus::Untriggered,
            _ => return Err(error(NAME, "unknown order status")),
        };
        Ok(OrderInfo {
            symbol: text(v, "symbol", NAME)?,
            order_id: text(v, "id", NAME)?,
            client_order_id: self.original(v, "clientId")?,
            side: side(&v["side"])?,
            order_type: ApiOrderType::from_venue_str(&text(v, "orderType", NAME)?),
            status,
            price: optional_number(v, "price", NAME)?.unwrap_or(0.0),
            qty,
            executed_qty: done,
            avg_price: if done > 0.0 {
                number(v, "executedQuoteQuantity", NAME)? / done
            } else {
                0.0
            },
            leaves_qty: (qty - done).max(0.0),
            time_in_force: if v["postOnly"] == true {
                ApiTimeInForce::GTX
            } else {
                ApiTimeInForce::from_venue_str(v["timeInForce"].as_str().unwrap_or("GTC"))
            },
            reduce_only: v["reduceOnly"]
                .as_bool()
                .or_else(|| {
                    text(v, "clientId", NAME).ok()?.parse().ok().and_then(|n| {
                        self.ids
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .reduce_only(n)
                    })
                })
                .unwrap_or(false),
            position_side: ApiPositionSide::Net,
            create_time: if v["createdAt"].as_str().is_some_and(|s| s.contains('T')) {
                iso(&text(v, "createdAt", NAME)?)?
            } else {
                integer(v, "createdAt", NAME)? / 1000
            },
            update_time: 0,
            stop_price: optional_number(v, "triggerPrice", NAME)?,
        })
    }
    fn selector(
        &self,
        symbol: &str,
        id: Option<&str>,
        client: Option<&str>,
    ) -> Result<Value, ApiError> {
        let mut v = json!({"symbol":symbol});
        match (id, client) {
            (Some(id), _) => v["orderId"] = json!(id),
            (None, Some(id)) => {
                v["clientId"] = json!(
                    self.ids
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .native(id)?
                )
            }
            _ => return Err(invalid(NAME, "order identity required")),
        };
        Ok(v)
    }
    async fn submit(
        &self,
        r: &UnifiedOrderRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        if r.position_side.is_some_and(|s| s != ApiPositionSide::Net) || r.stop_price.is_some() {
            return Err(unsupported(NAME, "hedge or trigger orders"));
        }
        if !r.qty.is_finite() || r.qty <= 0.0 {
            return Err(invalid(NAME, "positive quantity required"));
        }
        let m = self.instrument(&r.symbol).await?;
        if r.qty < m.min_qty {
            return Err(invalid(NAME, "quantity below minimum"));
        }
        let mut v = json!({"symbol":r.symbol,"side":match r.side{ApiSide::Buy=>"Bid",ApiSide::Sell=>"Ask",_=>return Err(invalid(NAME,"invalid side"))},"quantity":decimal(r.qty,m.lot_size,NAME)?,"reduceOnly":r.reduce_only,"selfTradePrevention":"RejectTaker"});
        match r.order_type {
            ApiOrderType::Limit => {
                v["orderType"] = json!("Limit");
                let p = r
                    .price
                    .filter(|p| p.is_finite() && *p > 0.0)
                    .ok_or_else(|| invalid(NAME, "limit price required"))?;
                v["price"] = json!(decimal(p, m.tick_size, NAME)?);
            }
            ApiOrderType::Market => {
                v["orderType"] = json!("Market");
                if r.price.is_some() || r.time_in_force != ApiTimeInForce::IOC {
                    return Err(invalid(NAME, "market orders require IOC and no price"));
                }
            }
            _ => return Err(unsupported(NAME, "order type")),
        }
        let tif = match r.time_in_force {
            ApiTimeInForce::GTX => {
                v["postOnly"] = json!(true);
                "GTC"
            }
            ApiTimeInForce::GTC => "GTC",
            ApiTimeInForce::IOC => "IOC",
            ApiTimeInForce::FOK => "FOK",
            _ => return Err(invalid(NAME, "unknown time in force")),
        };
        if r.order_type == ApiOrderType::Market && v["postOnly"] == true {
            return Err(invalid(NAME, "market cannot be post-only"));
        }
        v["timeInForce"] = json!(tif);
        check_submit_guard(&guard)?;
        let original = r
            .client_order_id
            .as_deref()
            .ok_or_else(|| invalid(NAME, "client order ID required"))?;
        v["clientId"] = json!(
            self.ids
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .allocate_order(original, Some(r.reduce_only))?
        );
        let raw = self
            .request(
                Method::POST,
                "/api/v1/order",
                v,
                Some("orderExecute"),
                guard,
            )
            .await?;
        self.order(&raw)
            .and_then(|order| {
                if order.symbol != r.symbol
                    || order.client_order_id != original
                    || order.side != r.side
                    || (order.qty - r.qty).abs() > m.lot_size / 2.0
                    || order.reduce_only != r.reduce_only
                {
                    return Err(error(NAME, "accepted order identity mismatch"));
                }
                Ok(order)
            })
            .map_err(|_| {
                ApiError::transport(
                    NAME,
                    "accepted order response cannot be decoded; reconcile client identity",
                )
            })
    }
}
fn side(v: &Value) -> Result<ApiSide, ApiError> {
    match v.as_str() {
        Some("Bid") => Ok(ApiSide::Buy),
        Some("Ask") => Ok(ApiSide::Sell),
        _ => Err(error(NAME, "invalid side")),
    }
}
fn iso(s: &str) -> Result<i64, ApiError> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.timestamp_millis())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .map(|t| t.and_utc().timestamp_millis())
        })
        .map_err(|_| error(NAME, "invalid ISO timestamp"))
}
fn public_trade_time(value: &Value) -> Result<i64, ApiError> {
    // The official Rust SDK exposes integers while older REST examples use strings. Accept
    // unambiguous modern Unix milliseconds/microseconds; reject other numeric magnitudes.
    let integer = value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok());
    if let Some(n) = integer {
        return match n {
            1_000_000_000_000..=99_999_999_999_999 => Ok(n),
            1_000_000_000_000_000..=99_999_999_999_999_999 => Ok(n / 1000),
            _ => Err(error(NAME, "unsupported public trade timestamp unit")),
        };
    }
    iso(value
        .as_str()
        .ok_or_else(|| error(NAME, "invalid public trade timestamp"))?)
}

fn ticker(v: &Value) -> Result<Ticker, ApiError> {
    Ok(Ticker {
        symbol: text(v, "symbol", NAME)?,
        last_price: number(v, "lastPrice", NAME)?,
        mark_price: None,
        index_price: None,
        funding_rate: None,
        next_funding_time: None,
        open_24h: number(v, "firstPrice", NAME)?,
        high_24h: number(v, "high", NAME)?,
        low_24h: number(v, "low", NAME)?,
        volume_24h: number(v, "volume", NAME)?,
        quote_volume_24h: number(v, "quoteVolume", NAME)?,
        timestamp: 0,
    })
}
#[async_trait::async_trait]
impl BrokerApi for BackpackClient {
    fn supplies_fill_realized_pnl(&self) -> bool {
        false
    }
    fn supports_order_execution_evidence(&self) -> bool {
        true
    }
    async fn ping(&self) -> Result<(), ApiError> {
        self.get("/api/v1/status", json!({}), None)
            .await
            .map(|_| ())
    }
    async fn get_server_time(&self) -> Result<i64, ApiError> {
        let r = self
            .http
            .get(format!("{}/api/v1/time", self.config.rest_url))
            .send()
            .await
            .map_err(|e| ApiError::transport(NAME, e.without_url()))?;
        if !r.status().is_success() {
            return Err(ApiError::http(NAME, r.status().as_u16(), "time failed"));
        }
        r.text()
            .await
            .map_err(|_| error(NAME, "invalid time"))?
            .trim()
            .parse()
            .map_err(|_| error(NAME, "invalid time"))
    }
    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        let v = self.get("/api/v1/markets", json!({}), None).await?;
        rows(&v, NAME)?
            .iter()
            .filter(|r| r["marketType"] == "PERP" && r["quoteSymbol"] == "USDC")
            .map(|r| {
                let tick = number(&r["filters"]["price"], "tickSize", NAME)?;
                let lot = number(&r["filters"]["quantity"], "stepSize", NAME)?;
                if tick <= 0.0 || lot <= 0.0 {
                    return Err(error(NAME, "invalid instrument steps"));
                }
                Ok(InstrumentInfo {
                    symbol: text(r, "symbol", NAME)?,
                    base_asset: text(r, "baseSymbol", NAME)?,
                    quote_asset: "USDC".into(),
                    margin_asset: "USDC".into(),
                    tick_size: tick,
                    lot_size: lot,
                    min_qty: number(&r["filters"]["quantity"], "minQuantity", NAME)?,
                    contract_size: 1.0,
                    price_precision: precision(tick),
                    qty_precision: precision(lot),
                    tradable: r["orderBookState"] == "Open",
                })
            })
            .collect()
    }
    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        ticker(
            &self
                .get("/api/v1/ticker", json!({"symbol":symbol}), None)
                .await?,
        )
    }
    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let v = self.get("/api/v1/tickers", json!({}), None).await?;
        rows(&v, NAME)?.iter().map(ticker).collect()
    }
    async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<OrderBook, ApiError> {
        if limit == 0 {
            return Err(invalid(NAME, "positive depth limit required"));
        }
        let v = self
            .get("/api/v1/depth", json!({"symbol":symbol}), None)
            .await?;
        let parse = |k| -> Result<Vec<PriceLevel>, ApiError> {
            levels(&v[k], NAME)?
                .into_iter()
                .take(limit as usize)
                .map(|(p, q)| {
                    Ok(PriceLevel {
                        price: p.parse().map_err(|_| error(NAME, "invalid price"))?,
                        qty: q.parse().map_err(|_| error(NAME, "invalid quantity"))?,
                    })
                })
                .collect()
        };
        Ok(OrderBook {
            symbol: symbol.into(),
            bids: parse("bids")?,
            asks: parse("asks")?,
            timestamp: integer(&v, "timestamp", NAME)? / 1000,
        })
    }
    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let v = self
            .get(
                "/api/v1/trades",
                json!({"symbol":symbol,"limit":limit.clamp(1,1000)}),
                None,
            )
            .await?;
        rows(&v, NAME)?
            .iter()
            .map(|v| {
                Ok(Trade {
                    symbol: symbol.into(),
                    id: text(v, "id", NAME)?,
                    price: number(v, "price", NAME)?,
                    qty: number(v, "quantity", NAME)?,
                    side: if v["isBuyerMaker"] == true {
                        ApiSide::Sell
                    } else {
                        ApiSide::Buy
                    },
                    timestamp: public_trade_time(&v["timestamp"])?,
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
            "1s" => 1,
            "1m" => 60,
            "3m" => 180,
            "5m" => 300,
            "15m" => 900,
            "30m" => 1800,
            "1h" => 3600,
            "2h" => 7200,
            "4h" => 14400,
            "6h" => 21600,
            "8h" => 28800,
            "12h" => 43200,
            "1d" => 86400,
            "3d" => 259200,
            "1w" => 604800,
            _ => return Err(unsupported(NAME, "candle interval")),
        };
        if limit == 0 || limit > 1000 {
            return Err(invalid(NAME, "candle limit must be 1..1000"));
        }
        let end = Utc::now().timestamp();
        let v=self.get("/api/v1/klines",json!({"symbol":symbol,"interval":interval,"startTime":end-i64::from(limit)*seconds,"endTime":end}),None).await?;
        let mut bars: Vec<_> = rows(&v, NAME)?
            .iter()
            .map(|r| {
                Ok(Kline {
                    symbol: symbol.into(),
                    interval: interval.into(),
                    open_time: iso(&text(r, "start", NAME)?)?,
                    close_time: iso(&text(r, "end", NAME)?)?,
                    open: number(r, "open", NAME)?,
                    high: number(r, "high", NAME)?,
                    low: number(r, "low", NAME)?,
                    close: number(r, "close", NAME)?,
                    volume: number(r, "volume", NAME)?,
                    quote_volume: number(r, "quoteVolume", NAME)?,
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
        let v = self
            .get("/api/v1/markPrices", json!({"symbol":symbol}), None)
            .await?;
        let r = rows(&v, NAME)?
            .iter()
            .find(|r| r["symbol"] == symbol)
            .ok_or_else(|| error(NAME, "funding not found"))?;
        Ok(FundingRate {
            symbol: symbol.into(),
            funding_rate: number(r, "fundingRate", NAME)?,
            next_funding_time: integer(r, "nextFundingTimestamp", NAME)?,
            timestamp: 0,
        })
    }
    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        let v = self
            .get(
                "/api/v1/fundingRates",
                json!({"symbol":symbol,"limit":limit.clamp(1,10000)}),
                None,
            )
            .await?;
        rows(&v, NAME)?
            .iter()
            .map(|r| {
                let ts = iso(&text(r, "intervalEndTimestamp", NAME)?)?;
                Ok(FundingRate {
                    symbol: symbol.into(),
                    funding_rate: number(r, "fundingRate", NAME)?,
                    next_funding_time: ts,
                    timestamp: ts,
                })
            })
            .collect()
    }
    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let v = self
            .get("/api/v1/openInterest", json!({"symbol":symbol}), None)
            .await?;
        let r = rows(&v, NAME)?
            .iter()
            .find(|r| r["symbol"] == symbol)
            .ok_or_else(|| error(NAME, "OI not found"))?;
        Ok(OpenInterest {
            symbol: symbol.into(),
            open_interest: number(r, "openInterest", NAME)?,
            timestamp: integer(r, "timestamp", NAME)?,
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
        let v = self.selector(
            &r.symbol,
            r.order_id.as_deref(),
            r.client_order_id.as_deref(),
        )?;
        let raw = self
            .request(
                Method::DELETE,
                "/api/v1/order",
                v,
                Some("orderCancel"),
                None,
            )
            .await?;
        self.order(&raw).map_err(|_| {
            ApiError::transport(NAME, "cancel response unknown; reconcile original identity")
        })
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
        self.request(
            Method::DELETE,
            "/api/v1/orders",
            json!({"symbol":symbol}),
            Some("orderCancelAll"),
            None,
        )
        .await?;
        if !self.get_open_orders(symbol).await?.is_empty() {
            return Err(ApiError::transport(
                NAME,
                "cancel-all has outstanding orders",
            ));
        }
        Ok(())
    }
    async fn cancel_all_after(&self, _: u64) -> Result<(), ApiError> {
        Err(unsupported(NAME, "dead-man switch"))
    }
    async fn amend_order(&self, _: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        Err(unsupported(
            NAME,
            "atomic amend; native API has no modify endpoint",
        ))
    }
    async fn get_order(
        &self,
        symbol: &str,
        id: Option<&str>,
        client: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        let v = self.selector(symbol, id, client)?;
        match self.get("/api/v1/order", v, Some("orderQuery")).await {
            Ok(v) => {
                let order = self.order(&v)?;
                if order.symbol != symbol
                    || id.is_some_and(|id| order.order_id != id)
                    || client.is_some_and(|id| order.client_order_id != id)
                {
                    return Err(error(NAME, "queried order identity mismatch"));
                }
                Ok(order)
            }
            Err(e) if e.status == Some(404) => {
                // The native order query only returns open orders. Terminal absence is not
                // cancellation evidence: search actual history using the original identity.
                for page in 0..100 {
                    let v = self
                        .get(
                            "/wapi/v1/history/orders",
                            json!({"symbol":symbol,"limit":1000,"offset":page*1000}),
                            Some("orderHistoryQueryAll"),
                        )
                        .await?;
                    let values = rows(&v, NAME)?;
                    for value in values {
                        let order = self.order(value)?;
                        if order.symbol == symbol
                            && id.is_none_or(|id| order.order_id == id)
                            && client.is_none_or(|id| order.client_order_id == id)
                        {
                            return Ok(order);
                        }
                    }
                    if values.len() < 1000 {
                        return Err(ApiError::new(
                            NAME,
                            "ORDER_NOT_FOUND",
                            "no authoritative order in history",
                        ));
                    }
                }
                Err(error(NAME, "order history search capacity exceeded"))
            }
            Err(e) => Err(e),
        }
    }
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let v = self
            .get(
                "/api/v1/orders",
                json!({"symbol":symbol}),
                Some("orderQueryAll"),
            )
            .await?;
        rows(&v, NAME)?.iter().map(|r| self.order(r)).collect()
    }
    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let v = self
            .get(
                "/wapi/v1/history/orders",
                json!({"symbol":symbol,"limit":limit.clamp(1,1000)}),
                Some("orderHistoryQueryAll"),
            )
            .await?;
        rows(&v, NAME)?.iter().map(|r| self.order(r)).collect()
    }
    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        let v = self
            .get(
                "/wapi/v1/history/fills",
                json!({"symbol":symbol,"limit":limit.clamp(1,1000)}),
                Some("fillHistoryQueryAll"),
            )
            .await?;
        rows(&v, NAME)?
            .iter()
            .map(|r| {
                Ok(Fill {
                    symbol: text(r, "symbol", NAME)?,
                    trade_id: text(r, "tradeId", NAME)?,
                    order_id: text(r, "orderId", NAME)?,
                    client_order_id: self.original(r, "clientId")?,
                    price: number(r, "price", NAME)?,
                    qty: number(r, "quantity", NAME)?,
                    side: side(&r["side"])?,
                    fee: number(r, "fee", NAME)?,
                    fee_asset: text(r, "feeSymbol", NAME)?,
                    realized_pnl: 0.0,
                    maker: r["isMaker"]
                        .as_bool()
                        .ok_or_else(|| error(NAME, "missing maker"))?,
                    timestamp: iso(&text(r, "timestamp", NAME)?)?,
                })
            })
            .collect()
    }
    async fn get_account_position_mode(&self) -> Result<ApiPositionMode, ApiError> {
        Ok(ApiPositionMode::Net)
    }
    fn position_currency(&self, _: &str) -> Option<String> {
        Some("USDC".into())
    }
    async fn validate_account_configuration(&self) -> Result<(), ApiError> {
        let a = self
            .get("/api/v1/account", json!({}), Some("accountQuery"))
            .await?;
        if a["liquidating"] == true {
            return Err(invalid(NAME, "account is liquidating"));
        }
        Ok(())
    }
    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let v = self
            .get("/api/v1/capital", json!({}), Some("balanceQuery"))
            .await?;
        let mut balances = Vec::new();
        for (asset, r) in v
            .as_object()
            .ok_or_else(|| error(NAME, "invalid balances"))?
        {
            let available = number(r, "available", NAME)?;
            let wallet = available + number(r, "locked", NAME)? + number(r, "staked", NAME)?;
            if asset != "USDC" && wallet != 0.0 {
                return Err(unsupported(NAME, "non-USDC collateral valuation"));
            }
            balances.push(Balance {
                asset: asset.clone(),
                wallet_balance: wallet,
                available_balance: available,
                unrealized_pnl: 0.0,
                margin_balance: wallet,
            });
        }
        let c = self
            .get(
                "/api/v1/capital/collateral",
                json!({}),
                Some("collateralQuery"),
            )
            .await?;
        let wallet = balances
            .iter()
            .find(|b| b.asset == "USDC")
            .map_or(0.0, |b| b.wallet_balance);
        let margin = number(&c, "netEquity", NAME)?;
        let pnl = number(&c, "pnlUnrealized", NAME)?;
        let available = number(&c, "netEquityAvailable", NAME)?;
        if let Some(balance) = balances.iter_mut().find(|b| b.asset == "USDC") {
            balance.available_balance = available;
            balance.margin_balance = margin;
            balance.unrealized_pnl = pnl;
        } else {
            balances.push(Balance {
                asset: "USDC".into(),
                wallet_balance: wallet,
                available_balance: available,
                margin_balance: margin,
                unrealized_pnl: pnl,
            });
        }
        Ok(AccountInfo {
            total_wallet_balance: wallet,
            total_margin_balance: margin,
            total_unrealized_pnl: pnl,
            available_balance: available,
            balances,
            timestamp: 0,
        })
    }
    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let v = self
            .get("/api/v1/position", json!({}), Some("positionQuery"))
            .await?;
        rows(&v, NAME)?
            .iter()
            .filter(|r| symbol.is_none_or(|s| r["symbol"] == s))
            .map(|r| {
                let qty = number(r, "netQuantity", NAME)?;
                Ok(PositionInfo {
                    symbol: text(r, "symbol", NAME)?,
                    position_side: ApiPositionSide::Net,
                    qty,
                    entry_price: number(r, "entryPrice", NAME)?,
                    mark_price: number(r, "markPrice", NAME)?,
                    liquidation_price: optional_number(r, "estLiquidationPrice", NAME)?
                        .unwrap_or(0.0),
                    leverage: 0.0,
                    margin_type: ApiMarginType::Cross,
                    unrealized_pnl: number(r, "pnlUnrealized", NAME)?,
                    realized_pnl: number(r, "pnlRealized", NAME)?,
                    notional: qty * number(r, "markPrice", NAME)?,
                    update_time: 0,
                })
            })
            .collect()
    }
    async fn set_leverage(
        &self,
        _: &str,
        _: f64,
        _: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        Err(unsupported(NAME, "account leverage mutation"))
    }
    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        let v = self
            .get("/api/v1/account", json!({}), Some("accountQuery"))
            .await?;
        Ok(LeverageInfo {
            symbol: symbol.into(),
            leverage: number(&v, "leverageLimit", NAME)?,
            margin_type: ApiMarginType::Cross,
            position_side: ApiPositionSide::Net,
        })
    }
    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        let v = self
            .get("/api/v1/account", json!({}), Some("accountQuery"))
            .await?;
        Ok(FeeRate {
            symbol: symbol.into(),
            maker_fee: number(&v, "futuresMakerFee", NAME)?,
            taker_fee: number(&v, "futuresTakerFee", NAME)?,
            timestamp: 0,
        })
    }
    async fn get_income_history(&self, _: &str, _: u32) -> Result<Vec<IncomeRecord>, ApiError> {
        Err(unsupported(
            NAME,
            "income history; use native funding/settlement endpoints",
        ))
    }
}
#[async_trait::async_trait]
impl Venue for BackpackClient {
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
    use crate::connector::{AccountPublication, DirectPublication, direct_publish_sender};
    use crate::venue::test_support::*;
    use std::collections::HashMap;
    fn config(url: &str, path: &str) -> Config {
        let key = SigningKey::from_bytes(&[7; 32]);
        Config {
            rest_url: url.into(),
            api_key: STANDARD.encode(key.verifying_key().as_bytes()),
            secret: STANDARD.encode([7; 32]),
            client_id_journal: Some(path.into()),
            ..Default::default()
        }
    }
    fn market() -> Value {
        json!([{"symbol":"BTC_USDC_PERP","baseSymbol":"BTC","quoteSymbol":"USDC","marketType":"PERP","filters":{"price":{"tickSize":"0.1"},"quantity":{"stepSize":"0.001","minQuantity":"0.001"}},"orderBookState":"Open"}])
    }
    fn request() -> UnifiedOrderRequest {
        UnifiedOrderRequest {
            symbol: "BTC_USDC_PERP".into(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(100.1),
            qty: 0.002,
            time_in_force: ApiTimeInForce::GTX,
            reduce_only: true,
            position_side: Some(ApiPositionSide::Net),
            client_order_id: Some("00112233445566778899aabbccddeeff".into()),
            stop_price: None,
        }
    }
    fn order() -> Value {
        json!({"symbol":"BTC_USDC_PERP","id":"98765432109876543210","clientId":1,"quantity":"0.002","executedQuantity":"0","executedQuoteQuantity":"0","side":"Bid","orderType":"Limit","status":"New","price":"100.1","timeInForce":"GTC","postOnly":true,"reduceOnly":true,"createdAt":1700000000000000i64})
    }
    #[tokio::test]
    async fn native_submit_has_official_signature_and_durable_original_identity() {
        let (url, mut rx, task) = http(vec![(200, market()), (200, order())]).await;
        let path = path("bpx-submit");
        let client = BackpackClient::new(config(&url, &path)).unwrap();
        let result = client.submit_order(&request()).await.unwrap();
        assert_eq!(result.client_order_id, request().client_order_id.unwrap());
        assert_eq!(result.order_id, "98765432109876543210");
        rx.recv().await.unwrap();
        let sent = rx.recv().await.unwrap();
        task.await.unwrap();
        let body: Value = serde_json::from_str(&sent.body).unwrap();
        assert_eq!(body["clientId"], 1);
        assert_eq!(body["quantity"], "0.002");
        assert_eq!(body["postOnly"], true);
        assert_eq!(body["reduceOnly"], true);
        let header = |name: &str| {
            sent.head
                .lines()
                .find_map(|s| {
                    s.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                        .map(|(_, v)| v.trim().to_owned())
                })
                .unwrap()
        };
        let ts = header("x-timestamp");
        let expected = format!(
            "instruction=orderExecute&clientId=1&orderType=Limit&postOnly=true&price=100.1&quantity=0.002&reduceOnly=true&selfTradePrevention=RejectTaker&side=Bid&symbol=BTC_USDC_PERP&timeInForce=GTC&timestamp={ts}&window=5000"
        );
        let signature =
            ed25519_dalek::Signature::from_slice(&STANDARD.decode(header("x-signature")).unwrap())
                .unwrap();
        SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .verify_strict(expected.as_bytes(), &signature)
            .unwrap();
        drop(client);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn expired_admission_is_checked_after_metadata_and_before_id_allocation() {
        let (url, mut rx, task) = http(vec![(200, market())]).await;
        let path = path("bpx-guard");
        let client = BackpackClient::new(config(&url, &path)).unwrap();
        let guard: SubmitGuard = Arc::new(|| Err(ApiError::new("test", "STALE", "epoch changed")));
        let e = client
            .submit_order_fenced(&request(), Some(guard))
            .await
            .unwrap_err();
        assert_eq!(e.code, "STALE");
        rx.recv().await.unwrap();
        task.await.unwrap();
        assert!(
            client
                .ids
                .lock()
                .unwrap()
                .native(request().client_order_id.as_deref().unwrap())
                .is_err()
        );
        drop(client);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn restore_queries_native_id_without_truncating_original() {
        let path = path("bpx-restore");
        let (url, mut rx, task) = http(vec![(200, order())]).await;
        let client = BackpackClient::new(config(&url, &path)).unwrap();
        let original = request().client_order_id.unwrap();
        client.ids.lock().unwrap().allocate(&original).unwrap();
        drop(client);
        let client = BackpackClient::new(config(&url, &path)).unwrap();
        let result = client
            .get_order("BTC_USDC_PERP", None, Some(&original))
            .await
            .unwrap();
        assert_eq!(result.client_order_id, original);
        let sent = rx.recv().await.unwrap();
        assert!(sent.head.contains("clientId=1"));
        assert!(!sent.head.contains(&original));
        task.await.unwrap();
        drop(client);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn snapshot_bridge_skips_old_updates_and_rejects_gaps() {
        let mut s = stream::DepthSequence {
            last: 100,
            bridged: false,
        };
        assert!(!s.accept(98, 100).unwrap());
        assert!(s.accept(99, 102).unwrap());
        assert!(s.accept(103, 104).unwrap());
        assert!(s.accept(106, 107).is_err());
        assert!(s.accept(106, 105).is_err());
    }
    #[test]
    fn private_fill_uses_native_trade_price_fee_and_cumulative_order_separately() {
        let client = BackpackClient::new(Config::default()).unwrap();
        let mut d = Desired::default();
        d.accounts.insert("BTC_USDC_PERP".into());
        let orders = Mutex::new(Orders::default());
        let tick = HashMap::from([("BTC_USDC_PERP".into(), 0.1)]);
        let output = Arc::new(Mutex::new(Vec::new()));
        let out = output.clone();
        let tx = direct_publish_sender(move |p| {
            if let DirectPublication::Account(f) = p {
                out.lock().unwrap().push(f.clone());
            }
        });
        let v = json!({"e":"orderFill","s":"BTC_USDC_PERP","i":"987","c":1,"S":"Bid","o":"LIMIT","f":"GTC","q":"0.004","p":"100.1","r":true,"X":"PartiallyFilled","t":567,"l":"0.001","z":"0.002","Z":"0.2001","L":"100.0","m":true,"n":"-0.000001","N":"USDC","T":1700000000123456i64});
        stream::order(&client, &v, &d, &tick, &orders, &tx).unwrap();
        let output = output.lock().unwrap();
        assert_eq!(output.len(), 2);
        if let AccountPublication::OrderWithNativeFills {
            order,
            average_price,
            reduce_only,
            ..
        } = &output[0]
        {
            assert_eq!(order.exec_qty, 0.0);
            assert!((*average_price - 100.05).abs() < 1e-10);
            assert!(*reduce_only);
        } else {
            panic!()
        };
        if let AccountPublication::NativeFillWithoutRealizedPnl { fill, .. } = &output[1] {
            assert_eq!(fill.trade_id, "567");
            assert_eq!(fill.qty, 0.001);
            assert_eq!(fill.price, 100.0);
            assert_eq!(fill.fee, -0.000001);
            assert_eq!(fill.timestamp, 1700000000123);
        } else {
            panic!()
        }
    }
}
#[cfg(test)]
mod history_tests {
    use super::*;
    use crate::venue::test_support::*;
    #[tokio::test]
    async fn a_missing_open_order_requires_terminal_history_and_keeps_reduce_only() {
        let history = json!([{"id":"321","clientId":1,"symbol":"BTC_USDC_PERP","quantity":"0.002","executedQuantity":"0","executedQuoteQuantity":"0","price":"100.1","side":"Bid","orderType":"Limit","status":"Cancelled","createdAt":"2026-10-30T00:00:00Z","timeInForce":"GTC","postOnly":true}]);
        let (url, mut rx, task) = http(vec![
            (404, json!({"code":"ORDER_NOT_FOUND"})),
            (200, history),
        ])
        .await;
        let path = path("terminal-bpx");
        let key = SigningKey::from_bytes(&[7; 32]);
        let c = BackpackClient::new(Config {
            rest_url: url,
            api_key: STANDARD.encode(key.verifying_key().as_bytes()),
            secret: STANDARD.encode([7; 32]),
            client_id_journal: Some(path.clone()),
            ..Default::default()
        })
        .unwrap();
        c.ids
            .lock()
            .unwrap()
            .allocate_order("original", Some(true))
            .unwrap();
        let o = c
            .get_order("BTC_USDC_PERP", None, Some("original"))
            .await
            .unwrap();
        assert_eq!(o.status, ApiOrderStatus::Canceled);
        assert!(o.reduce_only);
        assert_eq!(o.create_time, 1793318400000);
        rx.recv().await.unwrap();
        let sent = rx.recv().await.unwrap();
        assert!(sent.head.contains("/wapi/v1/history/orders?"));
        assert!(sent.head.contains("offset=0"));
        task.await.unwrap();
        drop(c);
        std::fs::remove_file(path).unwrap();
    }
}
#[cfg(test)]
mod collateral_tests {
    use super::*;
    use crate::venue::test_support::*;
    #[tokio::test]
    async fn per_currency_projection_uses_collateral_equity_and_margin_availability() {
        let (url, mut rx, task) = http(vec![
            (
                200,
                json!({"USDC":{"available":"90","locked":"10","staked":"0"}}),
            ),
            (
                200,
                json!({"netEquity":"105","netEquityAvailable":"65","pnlUnrealized":"5"}),
            ),
        ])
        .await;
        let path = path("collateral");
        let key = SigningKey::from_bytes(&[7; 32]);
        let c = BackpackClient::new(Config {
            rest_url: url,
            api_key: STANDARD.encode(key.verifying_key().as_bytes()),
            secret: STANDARD.encode([7; 32]),
            client_id_journal: Some(path.clone()),
            ..Default::default()
        })
        .unwrap();
        let a = c.get_account().await.unwrap();
        assert_eq!(a.balances[0].wallet_balance, 100.0);
        assert_eq!(a.balances[0].available_balance, 65.0);
        assert_eq!(a.balances[0].margin_balance, 105.0);
        assert_eq!(a.balances[0].unrealized_pnl, 5.0);
        for _ in 0..2 {
            rx.recv().await.unwrap();
        }
        task.await.unwrap();
        drop(c);
        std::fs::remove_file(path).unwrap();
    }
}
#[cfg(test)]
mod public_schema_probe {
    use super::*;
    #[tokio::test]
    #[ignore = "public REST schema inspection on the build server only; no credentials or account calls"]
    async fn official_public_timestamp_encodings() {
        let client = BackpackClient::new(Config::default()).unwrap();
        for (path, params) in [
            (
                "/api/v1/trades",
                json!({"symbol":"BTC_USDC_PERP","limit":1}),
            ),
            ("/api/v1/markPrices", json!({"symbol":"BTC_USDC_PERP"})),
        ] {
            let mut result = client.get(path, params.clone(), None).await;
            for _ in 0..2 {
                if !result.as_ref().is_err_and(|e| e.code == "TRANSPORT") {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                result = client.get(path, params.clone(), None).await;
            }
            let v = result.unwrap();
            let row = &rows(&v, NAME).unwrap()[0];
            println!(
                "{}",
                json!({"path":path,"timestamp":row["timestamp"],"nextFundingTimestamp":row["nextFundingTimestamp"]})
            );
        }
    }
}
#[cfg(test)]
mod public_time_tests {
    use super::*;
    #[test]
    fn public_tape_accepts_explicit_iso_and_disjoint_numeric_encodings() {
        assert_eq!(
            public_trade_time(&json!(1793376000123i64)).unwrap(),
            1793376000123
        );
        assert_eq!(
            public_trade_time(&json!(1793376000123456i64)).unwrap(),
            1793376000123
        );
        assert_eq!(
            public_trade_time(&json!("2026-10-30T16:00:00Z")).unwrap(),
            1793376000000
        );
        assert!(public_trade_time(&json!(1793376000)).is_err());
        assert!(public_trade_time(&json!(123456789012345i64)).is_err());
    }
}
