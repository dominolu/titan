//! Gate CrossEx: unified futures execution through Gate, retaining native venue-qualified symbols.
//! See docs/gate_crossex_broker.md for supported capabilities and account constraints.
mod brokerapi;
mod depth;
mod market;
mod market_units;
mod native;
mod rest;
mod stream;
#[cfg(test)]
mod tests;
mod websocket;

pub use native::Query;
pub use rest::GateCrossExClient;
pub use websocket::{CrossExMessage, GateCrossExWebSocket};

use crate::{
    api::{ApiError, BrokerApi},
    connector::{Connector, ConnectorBuilder, GetOrders, PublishSender, ShutdownFuture},
};
use hftbacktest::types::Order;
use rest::{invalid, symbol_parts};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use titan_market_service::MarketDataKind;
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rest_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: String,
    pub secret: String,
    /// CrossEx snapshots are limited depth; default five levels works across Binance, OKX and Gate.
    pub depth_levels: u32,
    /// Borrow common L2 frames and scale directly into integer ABI lots; false is a compatibility rollback.
    pub borrowed_depth: bool,
    pub reconnect_ms: u64,
    /// Required for an ISOLATED_EXCHANGE account.
    pub exchange_type: Option<String>,
    /// Verified public-feed contract-to-base overrides; never applied to order quantities.
    pub market_quantity_multipliers: std::collections::BTreeMap<String, f64>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rest_url: "https://api.gateio.ws/api/v4".into(),
            public_ws_url: "wss://api.gateio.ws/ws/crossex/public".into(),
            private_ws_url: "wss://api.gateio.ws/ws/crossex".into(),
            api_key: String::new(),
            secret: String::new(),
            depth_levels: 5,
            borrowed_depth: true,
            reconnect_ms: 1000,
            exchange_type: None,
            market_quantity_multipliers: Default::default(),
        }
    }
}
#[derive(Default)]
struct OrderManager {
    orders: HashMap<String, (String, Order)>,
    notionals: HashMap<String, Option<f64>>,
}
impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|(s, o)| o.active() && symbol.as_ref().is_none_or(|v| s == v))
            .map(|(_, o)| o.clone())
            .collect()
    }
}
#[derive(Default)]
struct Desired {
    accounts: HashSet<String>,
    market: HashMap<String, HashSet<MarketDataKind>>,
}
struct Task {
    handle: JoinHandle<()>,
}

pub struct GateCrossEx {
    config: Config,
    client: GateCrossExClient,
    desired: Arc<Mutex<Desired>>,
    orders: Arc<Mutex<OrderManager>>,
    revision: watch::Sender<u64>,
    tasks: Vec<Task>,
}
impl GateCrossEx {
    fn changed(&self) {
        self.revision.send_modify(|r| *r = r.wrapping_add(1));
    }
    fn start(&mut self, tx: PublishSender, private: bool) {
        let config = self.config.clone();
        let client = self.client.clone();
        let desired = self.desired.clone();
        let orders = self.orders.clone();
        let revision = self.revision.subscribe();
        let handle = tokio::spawn(stream::run(
            config, client, desired, orders, revision, tx, private,
        ));
        self.tasks.push(Task { handle });
    }
}
impl ConnectorBuilder for GateCrossEx {
    type Error = ApiError;
    fn build_from(config: &str) -> Result<Self, Self::Error> {
        crate::ensure_rustls_crypto_provider();
        let config: Config =
            toml::from_str(config).map_err(|_| invalid("invalid CrossEx config"))?;
        if !matches!(config.depth_levels, 1 | 5 | 10 | 20 | 30 | 50 | 100 | 400)
            || config.reconnect_ms == 0
        {
            return Err(invalid("invalid depth_levels or reconnect_ms"));
        }
        for endpoint in [&config.public_ws_url, &config.private_ws_url] {
            let url =
                reqwest::Url::parse(endpoint).map_err(|_| invalid("invalid WebSocket endpoint"))?;
            if !matches!(url.scheme(), "ws" | "wss")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(invalid("invalid WebSocket endpoint"));
            }
        }
        let mut client = GateCrossExClient::new(&config.rest_url, &config.api_key, &config.secret)?;
        if let Some(exchange) = &config.exchange_type {
            rest::identifier(exchange)?;
            if exchange != &exchange.to_ascii_uppercase() {
                return Err(invalid("exchange_type must be uppercase"));
            }
        }
        client.exchange_type = config.exchange_type.clone();
        client = client.with_websocket_urls(&config.public_ws_url, &config.private_ws_url)?;
        client = client.with_market_quantity_multipliers(&config.market_quantity_multipliers)?;
        let (revision, _) = watch::channel(0);
        Ok(Self {
            config,
            client,
            desired: Default::default(),
            orders: Default::default(),
            revision,
            tasks: Vec::new(),
        })
    }
}
#[async_trait::async_trait]
impl Connector for GateCrossEx {
    fn register_account(&mut self, symbol: String) {
        self.client
            .bound_symbols
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(symbol.clone());
        // Invalid bindings are rejected by bootstrap/get_open_orders before READY.
        self.desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .insert(symbol);
        self.changed();
    }
    fn register(&mut self, symbol: String) {
        self.register_account(symbol.clone());
        self.subscribe_market_data(
            symbol,
            vec![
                MarketDataKind::Depth,
                MarketDataKind::Bbo,
                MarketDataKind::Trades,
                MarketDataKind::MarkPrice,
                MarketDataKind::FundingRate,
            ],
        );
    }
    fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        self.desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .market
            .entry(symbol)
            .or_default()
            .extend(kinds);
        self.changed();
    }
    fn unregister(&mut self, symbol: String) {
        {
            let mut desired = self.desired.lock().unwrap_or_else(|p| p.into_inner());
            desired.accounts.remove(&symbol);
            self.client
                .bound_symbols
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&symbol);
            desired.market.remove(&symbol);
        }
        self.changed();
    }
    fn unsubscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        {
            let mut desired = self.desired.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(active) = desired.market.get_mut(&symbol) {
                for kind in kinds {
                    active.remove(&kind);
                }
                if active.is_empty() {
                    desired.market.remove(&symbol);
                }
            }
        }
        self.changed();
    }
    fn request_snapshot(&mut self, _: String) {
        self.changed();
    }
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.orders.clone()
    }
    fn run(&mut self, tx: PublishSender) {
        self.run_market_data(tx.clone());
        if !self.config.api_key.is_empty() {
            self.run_account(tx);
        }
    }
    fn run_market_data(&mut self, tx: PublishSender) {
        self.start(tx, false);
    }
    fn run_account(&mut self, tx: PublishSender) {
        self.start(tx, true);
    }
    fn broker_api(&self) -> Option<Arc<dyn BrokerApi>> {
        Some(Arc::new(self.client.clone()))
    }
    fn track_managed_order(&self, symbol: &str, id: &str, order: &Order) {
        let mut manager = self.orders.lock().unwrap_or_else(|p| p.into_inner());
        let filled = order.qty - order.leaves_qty;
        if manager.orders.get(id).is_some_and(|(_, old)| {
            old.exch_timestamp > order.exch_timestamp || old.qty - old.leaves_qty > filled
        }) {
            return;
        }
        let notional = if filled == 0.0 {
            Some(0.0)
        } else {
            self.client
                .cached_order(id)
                .filter(|v| {
                    v.update_time.saturating_mul(1_000_000) == order.exch_timestamp
                        && (v.executed_qty - filled).abs() < 1e-10
                })
                .map(|v| v.avg_price * v.executed_qty)
        };
        manager.notionals.insert(id.to_owned(), notional);
        manager
            .orders
            .insert(id.to_owned(), (symbol.to_owned(), order.clone()));
    }
    fn reject_pending_order(&self, id: &str) {
        if let Some((_, order)) = self
            .orders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .orders
            .get_mut(id)
        {
            crate::utils::reject_pending_order(order);
        }
    }
    fn shutdown(&self) -> ShutdownFuture {
        let handles: Vec<_> = self.tasks.iter().map(|t| t.handle.abort_handle()).collect();
        let client = self.client.clone();
        let symbols: Vec<_> = self
            .desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .iter()
            .cloned()
            .collect();
        Box::pin(async move {
            // AccountRuntime owns its configured shutdown cancellation policy. This legacy hook
            // mirrors other connectors and only touches registered symbols.
            for handle in handles {
                handle.abort();
            }
            if client.key.is_empty() {
                return Ok(());
            }
            let mut failures = Vec::new();
            for symbol in symbols {
                symbol_parts(&symbol).map_err(|e| e.to_string())?;
                if let Err(e) = client.cancel_all_orders(&symbol).await {
                    failures.push(e.to_string());
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            }
        })
    }
}
impl Drop for GateCrossEx {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.handle.abort();
        }
    }
}
