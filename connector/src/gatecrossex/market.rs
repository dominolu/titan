//! Query-style snapshots obtained from CrossEx's public websocket (there are no public REST
//! market endpoints). Historical samples are deliberately never synthesized from current data.
use super::{CrossExMessage, GateCrossExClient, GateCrossExWebSocket, rest::*};
use crate::api::*;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

impl GateCrossExClient {
    /// Configure native websocket endpoints; primarily useful for test gateways and proxies.
    pub fn with_websocket_urls(mut self, public: &str, private: &str) -> Result<Self, ApiError> {
        for endpoint in [public, private] {
            let url =
                reqwest::Url::parse(endpoint).map_err(|_| invalid("invalid websocket endpoint"))?;
            if !matches!(url.scheme(), "ws" | "wss")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(invalid("invalid websocket endpoint"));
            }
        }
        self.public_ws_url = public.into();
        self.private_ws_url = private.into();
        Ok(self)
    }
    /// Scope isolated accounts to an underlying exchange. No account mode is changed.
    pub fn with_exchange_type(mut self, exchange: &str) -> Result<Self, ApiError> {
        identifier(exchange)?;
        if exchange != exchange.to_ascii_uppercase() {
            return Err(invalid("exchange_type must be uppercase"));
        }
        self.exchange_type = Some(exchange.into());
        Ok(self)
    }
    pub(super) async fn public_snapshots(
        &self,
        channel: &str,
        symbols: &[String],
    ) -> Result<Vec<CrossExMessage>, ApiError> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        for symbol in symbols {
            symbol_parts(symbol)?;
        }
        let mut socket = GateCrossExWebSocket::connect(self, false).await?;
        let mut needed: HashSet<_> = symbols.iter().cloned().collect();
        for chunk in symbols.chunks(100) {
            socket.subscribe(channel, chunk).await?;
        }
        let mut found = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            while !needed.is_empty() {
                let frame = socket.next().await?;
                if frame.channel == channel && frame.event == "update" {
                    let symbol = text(&frame.result, "s")?;
                    if needed.remove(&symbol) {
                        found.insert(symbol, frame);
                    }
                }
            }
            Ok::<_, ApiError>(())
        })
        .await
        .map_err(|_| protocol("public snapshot timed out; incomplete result discarded"))??;
        Ok(found.into_values().collect())
    }
    pub(super) async fn public_snapshot(
        &self,
        channel: &str,
        symbol: &str,
    ) -> Result<CrossExMessage, ApiError> {
        self.public_snapshots(channel, &[symbol.to_owned()])
            .await?
            .pop()
            .ok_or_else(|| protocol("missing snapshot"))
    }
    pub(super) async fn ticker_snapshot(&self, symbol: &str) -> Result<Ticker, ApiError> {
        ticker(&self.public_snapshot("ticker", symbol).await?)
    }
    /// Collateral totals use current venue ticker prices in USDT. Native balances
    /// retain their own currency units; discounts remain the server's margin valuation.
    pub(super) async fn account_valued(&self, value: &Value) -> Result<AccountInfo, ApiError> {
        let mut coins = HashSet::new();
        for asset in rows(&value["assets"])? {
            let coin = text(asset, "coin")?;
            if coin != "USDT" && (number(asset, "balance")? != 0.0 || number(asset, "upnl")? != 0.0)
            {
                coins.insert(coin);
            }
        }
        let mut rates = BTreeMap::new();
        if !coins.is_empty() {
            let rules = self.list_rule_symbols().await?;
            for coin in coins {
                let symbol = rows(&rules)?
                    .iter()
                    .filter(|v| v["state"] == "live" && v["business_type"] == "SPOT")
                    .filter_map(|v| v["symbol"].as_str())
                    .find(|s| {
                        symbol_parts(s)
                            .is_ok_and(|(_, base, quote)| base == coin && quote == "USDT")
                    })
                    .ok_or_else(|| {
                        unsupported(&format!("no live {coin}/USDT collateral valuation pair"))
                    })?;
                let tick = self.ticker_snapshot(symbol).await?;
                if tick.last_price <= 0.0
                    || chrono::Utc::now()
                        .timestamp_millis()
                        .abs_diff(tick.timestamp)
                        > 60_000
                {
                    return Err(protocol("stale or invalid collateral price"));
                }
                rates.insert(coin, tick.last_price);
            }
        }
        aggregate_account_valued(value, &rates)
    }
}
pub(super) fn ticker(frame: &CrossExMessage) -> Result<Ticker, ApiError> {
    let v = &frame.result;
    Ok(Ticker {
        symbol: text(v, "s")?,
        last_price: number(v, "lp")?,
        mark_price: None,
        index_price: None,
        funding_rate: None,
        next_funding_time: None,
        open_24h: number(v, "o")?,
        high_24h: number(v, "h")?,
        low_24h: number(v, "l")?,
        volume_24h: number(v, "v")?,
        quote_volume_24h: optional_number(v, "q")?,
        timestamp: integer(v, "ts")?,
    })
}
pub(super) fn book(frame: &CrossExMessage, limit: u32) -> Result<OrderBook, ApiError> {
    let parse = |v: &Value| -> Result<Vec<PriceLevel>, ApiError> {
        rows(v)?
            .iter()
            .take(limit as usize)
            .map(|v| {
                let a = rows(v)?;
                if a.len() != 2 {
                    return Err(protocol("invalid book level"));
                }
                let row = serde_json::json!({"p":a[0],"q":a[1]});
                let price = number(&row, "p")?;
                let qty = number(&row, "q")?;
                if price <= 0.0 || qty < 0.0 {
                    return Err(protocol("invalid book price or size"));
                }
                Ok(PriceLevel { price, qty })
            })
            .collect()
    };
    let v = &frame.result;
    Ok(OrderBook {
        symbol: text(v, "s")?,
        bids: parse(&v["b"])?,
        asks: parse(&v["a"])?,
        timestamp: integer(v, "ts")?,
    })
}
