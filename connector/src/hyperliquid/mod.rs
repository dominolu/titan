mod brokerapi;
pub mod client;
#[allow(dead_code)]
mod msg;
mod ordermanager;
mod signing;
mod ws;

#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

#[allow(unused_imports)]
use hftbacktest::{
    prelude::get_precision,
    types::{ErrorKind, LiveError, OrdType, Order, Side, TimeInForce, Value},
};
use serde::Deserialize;
use thiserror::Error;
use titan_market_plugin::MarketDataKind;
use tokio::sync::{broadcast, broadcast::Sender};
use tracing::{error, warn};

#[allow(unused_imports)]
use crate::{
    api::BrokerApi,
    connector::{
        AccountPublication, Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent,
    },
    hyperliquid::{
        client::HyperliquidClient,
        msg::{CancelStatus, ExchangeResponse, Meta, OrderStatus, OrderTypeWire, OrderWire, Tif},
        ordermanager::{OrderManager, SharedOrderManager},
        signing::derive_address,
    },
    utils::{ExponentialBackoff, Retry},
};

#[derive(Error, Debug)]
pub enum HyperliquidError {
    #[error("AssetNotFound: {0}")]
    AssetNotFound(String),
    #[error("OrderError: {0}")]
    OrderError(String),
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("K256: {0}")]
    K256(#[from] k256::ecdsa::Error),
    #[error("Hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl HyperliquidError {
    pub fn to_value(&self) -> Value {
        match self {
            HyperliquidError::OrderError(msg) => Value::Map({
                let mut map = HashMap::new();
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            _ => Value::String(self.to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    info_url: String,
    exchange_url: String,
    ws_url: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    account_address: String,
    #[serde(default)]
    is_mainnet: bool,
    /// Exchange-side scheduled-cancel timeout. Zero disables the heartbeat.
    #[serde(default = "default_safety_timeout_ms")]
    safety_timeout_ms: u64,
}

fn default_safety_timeout_ms() -> u64 {
    30_000
}

fn validate_safety_timeout_ms(timeout_ms: u64) -> Result<(), HyperliquidError> {
    if timeout_ms != 0 && timeout_ms < 5_000 {
        return Err(HyperliquidError::InvalidArg(
            "safety_timeout_ms must be 0 (disabled) or >= 5000",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct AssetInfo {
    pub index: u32,
    pub sz_decimals: u32,
}

pub type SharedAssets = Arc<Mutex<HashMap<String, AssetInfo>>>;
pub type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;
pub type SharedMarketSubscriptions = Arc<Mutex<HashMap<String, HashSet<MarketDataKind>>>>;

fn all_market_kinds() -> Vec<MarketDataKind> {
    vec![MarketDataKind::Depth, MarketDataKind::Trades]
}

pub struct Hyperliquid {
    config: Config,
    private_key: [u8; 32],
    account_address: String,
    symbols: SharedSymbolSet,
    assets: SharedAssets,
    order_manager: SharedOrderManager,
    client: HyperliquidClient,
    market_tx: Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
    #[cfg(test)]
    reconnect_fault: Arc<AtomicU8>,
}

async fn ensure_assets(
    client: &HyperliquidClient,
    assets: &SharedAssets,
) -> Result<(), HyperliquidError> {
    if !assets.lock().unwrap().is_empty() {
        return Ok(());
    }
    let meta = client.get_meta().await?;
    *assets.lock().unwrap() = build_assets_map(&meta);
    Ok(())
}

/// Maps each asset name to its exchange-assigned index. The index is the position in the
/// `universe` array (the testnet universe order differs from mainnet, e.g. BTC is index 3 there).
fn build_assets_map(meta: &Meta) -> HashMap<String, AssetInfo> {
    let mut map = HashMap::new();
    for (index, asset) in meta.universe.iter().enumerate() {
        map.insert(
            asset.name.clone(),
            AssetInfo {
                index: index as u32,
                sz_decimals: asset.sz_decimals,
            },
        );
    }
    map
}

impl Hyperliquid {
    fn start_safety_heartbeat(&self) {
        let timeout_ms = self.config.safety_timeout_ms;
        if timeout_ms == 0 {
            return;
        }
        let client = self.client.clone();
        tokio::spawn(async move {
            let refresh_ms = (timeout_ms / 3).max(1_000);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(refresh_ms));
            loop {
                interval.tick().await;
                if let Err(error) = BrokerApi::cancel_all_after(&client, timeout_ms).await {
                    error!(?error, "failed to refresh scheduled-cancel safety net");
                }
            }
        });
    }

    fn connect_ws(&self, ev_tx: crate::connector::PublishSender, private_channels: bool) {
        let ws_url = self.config.ws_url.clone();
        let order_manager = self.order_manager.clone();
        let assets = self.assets.clone();
        let symbols = self.symbols.clone();
        let account_address = self.account_address.clone();
        let client = self.client.clone();
        let market_tx = self.market_tx.clone();
        let market_subscriptions = self.market_subscriptions.clone();
        #[cfg(test)]
        let reconnect_fault = self.reconnect_fault.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: HyperliquidError| {
                    error!(?error, "An error occurred in the WebSocket connection.");
                    publish_stream_error(
                        &ev_tx,
                        private_channels,
                        LiveError::with(ErrorKind::ConnectionInterrupted, error.to_value()),
                    );
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = ws::HyperliquidWs::new(
                        ev_tx.clone(),
                        order_manager.clone(),
                        assets.clone(),
                        symbols.clone(),
                        account_address.clone(),
                        client.clone(),
                        market_tx.subscribe(),
                        market_subscriptions.clone(),
                        private_channels,
                        #[cfg(test)]
                        reconnect_fault.clone(),
                    );
                    if let Err(error) = stream.connect(&ws_url).await {
                        error!(?error, "A connection error occurred.");
                        publish_stream_error(
                            &ev_tx,
                            private_channels,
                            LiveError::with(ErrorKind::ConnectionInterrupted, error.to_value()),
                        );
                    } else {
                        publish_stream_error(
                            &ev_tx,
                            private_channels,
                            LiveError::new(ErrorKind::ConnectionInterrupted),
                        );
                    }
                    Err::<(), HyperliquidError>(HyperliquidError::ConnectionInterrupted)
                })
                .await;
        });
    }

    fn connect_assets_loader(&self) {
        let client = self.client.clone();
        let assets = self.assets.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: HyperliquidError| {
                    error!(
                        ?error,
                        "An error occurred while loading the asset universe."
                    );
                    Ok(())
                })
                .retry(|| async {
                    ensure_assets(&client, &assets).await?;
                    Ok::<(), HyperliquidError>(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for Hyperliquid {
    type Error = HyperliquidError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        crate::ensure_rustls_crypto_provider();
        let config: Config = toml::from_str(config)?;
        validate_safety_timeout_ms(config.safety_timeout_ms)?;
        let private_key_hex = config.private_key.trim_start_matches("0x");
        let private_key_bytes = hex::decode(private_key_hex)?;
        if private_key_bytes.len() != 32 {
            return Err(HyperliquidError::InvalidArg("private_key must be 32 bytes"));
        }
        let mut private_key = [0u8; 32];
        private_key.copy_from_slice(&private_key_bytes);

        let account_address = if config.account_address.is_empty() {
            derive_address(&private_key)?
        } else {
            let derived = derive_address(&private_key)?;
            if !config.account_address.eq_ignore_ascii_case(&derived) {
                warn!(
                    "account_address differs from the address derived from private_key; \
                    assuming an API wallet (agent) setup. Make sure the agent is approved \
                    by the account."
                );
            }
            config.account_address.clone()
        };

        let (market_tx, _) = broadcast::channel(500);
        let order_manager = Arc::new(Mutex::new(OrderManager::new()));
        let client = HyperliquidClient::new(&config.info_url, &config.exchange_url).with_signer(
            private_key,
            account_address.clone(),
            config.is_mainnet,
        );
        Ok(Hyperliquid {
            config,
            private_key,
            account_address,
            symbols: Default::default(),
            assets: Default::default(),
            order_manager,
            client,
            market_tx,
            market_subscriptions: Default::default(),
            #[cfg(test)]
            reconnect_fault: Arc::new(AtomicU8::new(0)),
        })
    }
}

impl Hyperliquid {
    /// Builds the public market-data connector without constructing or retaining a signer.
    /// Account construction continues to use `ConnectorBuilder` and requires a valid private key.
    pub(crate) fn build_market_from(config: &str) -> Result<Self, HyperliquidError> {
        crate::ensure_rustls_crypto_provider();
        let mut config: Config = toml::from_str(config)?;
        validate_safety_timeout_ms(config.safety_timeout_ms)?;
        config.private_key.clear();
        config.account_address.clear();
        let client = HyperliquidClient::new(&config.info_url, &config.exchange_url);
        let (market_tx, _) = broadcast::channel(500);
        Ok(Self {
            config,
            private_key: [0; 32],
            account_address: String::new(),
            symbols: Default::default(),
            assets: Default::default(),
            order_manager: Arc::new(Mutex::new(OrderManager::new())),
            client,
            market_tx,
            market_subscriptions: Default::default(),
            #[cfg(test)]
            reconnect_fault: Arc::new(AtomicU8::new(0)),
        })
    }

    #[cfg(test)]
    fn arm_private_reconnect_fault(&self) {
        self.reconnect_fault.store(1, Ordering::Release);
    }
}

fn publish_stream_error(
    sender: &crate::connector::PublishSender,
    private_channels: bool,
    error: LiveError,
) {
    if private_channels {
        sender
            .send_account(AccountPublication::Error(error))
            .expect("account publication receiver must remain live while connector runs");
    } else {
        sender
            .send(PublishEvent::ConnectorError(error))
            .expect("market publication receiver must remain live while connector runs");
    }
}

#[async_trait::async_trait]
impl Connector for Hyperliquid {
    fn register_account(&mut self, symbol: String) {
        if self.symbols.lock().unwrap().insert(symbol.clone()) {
            let _ = self
                .market_tx
                .send(MarketDataCommand::InitializeTrading { symbol });
        }
    }
    fn register(&mut self, symbol: String) {
        if symbol.to_uppercase() != symbol {
            error!("Hyperliquid coin must be uppercase, e.g. BTC.");
        }
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            let _ = self.market_tx.send(MarketDataCommand::InitializeTrading {
                symbol: symbol.clone(),
            });
        }
        drop(symbols);
        self.subscribe_market_data(symbol, all_market_kinds());
    }

    fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        self.market_subscriptions
            .lock()
            .unwrap()
            .entry(symbol.clone())
            .or_default()
            .extend(kinds.iter().copied());
        let _ = self
            .market_tx
            .send(MarketDataCommand::Subscribe { symbol, kinds });
    }

    fn unregister(&mut self, symbol: String) {
        self.symbols.lock().unwrap().remove(&symbol);
        let kinds = self
            .market_subscriptions
            .lock()
            .unwrap()
            .get(&symbol)
            .map(|v| v.iter().copied().collect())
            .unwrap_or_default();
        self.unsubscribe_market_data(symbol, kinds);
    }

    fn unsubscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        let mut subscriptions = self.market_subscriptions.lock().unwrap();
        if let Some(active) = subscriptions.get_mut(&symbol) {
            for kind in &kinds {
                active.remove(kind);
            }
            if active.is_empty() {
                subscriptions.remove(&symbol);
            }
        }
        drop(subscriptions);
        let _ = self
            .market_tx
            .send(MarketDataCommand::Unsubscribe { symbol, kinds });
    }

    fn request_snapshot(&mut self, symbol: String) {
        let _ = self.market_tx.send(MarketDataCommand::Snapshot { symbol });
    }

    fn recover_market_data(&mut self, symbols: Vec<String>) {
        for symbol in symbols {
            let _ = self.market_tx.send(MarketDataCommand::Snapshot { symbol });
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_assets_loader();
        self.connect_ws(ev_tx, true);
        self.start_safety_heartbeat();
    }

    fn run_market_data(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_assets_loader();
        self.connect_ws(ev_tx, false);
    }

    fn run_account(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_assets_loader();
        self.connect_ws(ev_tx, true);
        self.start_safety_heartbeat();
    }

    fn track_managed_order(&self, symbol: &str, client_order_id: &str, order: &Order) {
        self.order_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .track_managed_order(symbol, client_order_id, order.clone());
    }

    fn broker_api(&self) -> Option<Arc<dyn crate::api::BrokerApi>> {
        Some(Arc::new(self.client.clone()))
    }

    async fn shutdown(&self) -> Result<(), String> {
        // Public market-data connectors are intentionally built without a signer and have no
        // account orders or scheduled-cancel heartbeat to clear.
        if self.private_key == [0; 32] {
            return Ok(());
        }
        if let Err(error) = BrokerApi::cancel_all_after(&self.client, 0).await {
            return Err(format!("failed to clear scheduled cancellation: {error}"));
        }
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        let mut errors = Vec::new();
        for symbol in symbols {
            if let Err(error) = BrokerApi::cancel_all_orders(&self.client, &symbol).await {
                errors.push(format!("{symbol}: {error}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

#[cfg(test)]
fn build_order_wire(
    asset_info: &AssetInfo,
    order: Order,
    cloid: String,
) -> Result<OrderWire, HyperliquidError> {
    let b = match order.side {
        Side::Buy => true,
        Side::Sell => false,
        Side::None | Side::Unsupported => {
            return Err(HyperliquidError::InvalidArg("side"));
        }
    };
    let tif = match order.time_in_force {
        TimeInForce::GTC => "Gtc",
        TimeInForce::GTX => "Alo",
        TimeInForce::IOC => "Ioc",
        TimeInForce::FOK | TimeInForce::Unsupported => {
            return Err(HyperliquidError::InvalidArg("time_in_force"));
        }
    };
    if order.order_type != OrdType::Limit {
        return Err(HyperliquidError::InvalidArg("order_type"));
    }
    Ok(OrderWire {
        a: asset_info.index,
        b,
        p: trim_wire_decimals(format!(
            "{:.prec$}",
            order.price_tick as f64 * order.tick_size,
            prec = get_precision(order.tick_size)
        )),
        s: trim_wire_decimals(format!(
            "{:.prec$}",
            order.qty,
            prec = asset_info.sz_decimals as usize
        )),
        r: false,
        t: OrderTypeWire {
            limit: Tif {
                tif: tif.to_string(),
            },
        },
        c: Some(cloid),
    })
}

#[cfg(test)]
mod reconnect_tests {
    use super::*;

    #[tokio::test]
    async fn public_stream_reconnects_and_replays_desired_subscription() {
        let (url, mut subscriptions, server) =
            crate::connector::reconnecting_websocket_server(2).await;
        let config = format!(
            "info_url = \"http://127.0.0.1:9/info\"\nexchange_url = \"http://127.0.0.1:9/exchange\"\nws_url = {url:?}\nsafety_timeout_ms = 0\n"
        );
        let mut connector = Hyperliquid::build_market_from(&config).unwrap();
        connector.subscribe_market_data(
            "BTC".to_owned(),
            vec![MarketDataKind::Depth, MarketDataKind::Trades],
        );
        let (events, _event_receiver) = crate::connector::test_publish_channel();
        connector.run_market_data(events);

        for _ in 0..2 {
            let mut frames = Vec::new();
            for _ in 0..2 {
                frames.push(
                    tokio::time::timeout(std::time::Duration::from_secs(3), subscriptions.recv())
                        .await
                        .expect("connector did not reconnect before deadline")
                        .expect("websocket fixture ended before reconnect"),
                );
            }
            // Hyperliquid emits one command per channel. Both commands must be rebuilt from the
            // shared desired state on each newly accepted socket.
            assert!(frames.iter().all(|frame| frame.contains("subscribe")));
            assert!(frames.iter().all(|frame| frame.contains("BTC")));
            assert!(frames.iter().any(|frame| frame.contains("l2Book")));
            assert!(frames.iter().any(|frame| frame.contains("trades")));
        }
        server.await.unwrap();
    }
}

/// Hyperliquid rejects prices/sizes with trailing zeros (e.g. "0.00100"). Strips trailing zeros
/// and the decimal point from a fixed-decimal string, matching the official SDK's float_to_wire.
#[cfg(test)]
fn trim_wire_decimals(s: String) -> String {
    if !s.contains('.') {
        return s;
    }
    let t = s.trim_end_matches('0').trim_end_matches('.');
    if t.is_empty() {
        "0".to_string()
    } else {
        t.to_string()
    }
}

#[cfg(test)]
fn parse_order_statuses(resp: &ExchangeResponse) -> Result<Option<OrderStatus>, HyperliquidError> {
    if resp.status != "ok" {
        return Err(HyperliquidError::OrderError(exchange_error_message(resp)));
    }
    let data = resp
        .response
        .as_ref()
        .and_then(|r| r.data.as_ref())
        .ok_or(HyperliquidError::OrderError("empty response".to_string()))?;
    let statuses = data
        .get("statuses")
        .and_then(|s| s.as_array())
        .ok_or(HyperliquidError::OrderError("missing statuses".to_string()))?;
    if statuses.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(statuses[0].clone())?))
}

#[cfg(test)]
fn parse_cancel_statuses(
    resp: &ExchangeResponse,
) -> Result<Option<CancelStatus>, HyperliquidError> {
    if resp.status != "ok" {
        return Err(HyperliquidError::OrderError(exchange_error_message(resp)));
    }
    let data = resp
        .response
        .as_ref()
        .and_then(|r| r.data.as_ref())
        .ok_or(HyperliquidError::OrderError("empty response".to_string()))?;
    let statuses = data
        .get("statuses")
        .and_then(|s| s.as_array())
        .ok_or(HyperliquidError::OrderError("missing statuses".to_string()))?;
    if statuses.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(statuses[0].clone())?))
}

#[cfg(test)]
fn exchange_error_message(resp: &ExchangeResponse) -> String {
    match resp
        .response
        .as_ref()
        .and_then(|r| r.data.as_ref())
        .and_then(|d| d.as_str())
    {
        Some(msg) => msg.to_string(),
        None => resp.status.clone(),
    }
}

#[cfg(test)]
mod wire_format_tests {
    use super::*;

    #[test]
    fn test_trim_wire_decimals() {
        assert_eq!(trim_wire_decimals("61005.0".to_string()), "61005");
        assert_eq!(trim_wire_decimals("99000.0".to_string()), "99000");
        assert_eq!(trim_wire_decimals("0.00100".to_string()), "0.001");
        assert_eq!(trim_wire_decimals("0.0".to_string()), "0");
        assert_eq!(trim_wire_decimals("123.5".to_string()), "123.5");
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::hyperliquid::msg::{
        AssetMeta, CancelStatus, ExchangeResponse, ExchangeResponseData, Filled, Meta, OrderStatus,
        Resting,
    };
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    fn asset(index: u32, sz_decimals: u32) -> AssetInfo {
        AssetInfo { index, sz_decimals }
    }

    fn wire_order(
        price_tick: i64,
        tick_size: f64,
        qty: f64,
        side: Side,
        tif: TimeInForce,
    ) -> Order {
        Order::new(1, price_tick, tick_size, qty, side, OrdType::Limit, tif)
    }

    // ------------------------------------------------------------------
    // build_assets_map
    // ------------------------------------------------------------------

    fn meta_with(names: &[&str]) -> Meta {
        Meta {
            universe: names
                .iter()
                .map(|name| AssetMeta {
                    name: name.to_string(),
                    sz_decimals: 2,
                    max_leverage: 10,
                })
                .collect(),
        }
    }

    #[test]
    fn test_build_assets_map_mainnet_order() {
        let map = build_assets_map(&meta_with(&["BTC", "ETH", "SOL"]));
        assert_eq!(map["BTC"].index, 0);
        assert_eq!(map["ETH"].index, 1);
        assert_eq!(map["SOL"].index, 2);
    }

    #[test]
    fn test_build_assets_map_testnet_order() {
        // Testnet universe order differs from mainnet: BTC is at index 3, SOL at 0.
        let map = build_assets_map(&meta_with(&["SOL", "APT", "ATOM", "BTC", "ETH"]));
        assert_eq!(map["SOL"].index, 0);
        assert_eq!(map["BTC"].index, 3);
        assert_eq!(map["ETH"].index, 4);
    }

    // ------------------------------------------------------------------
    // build_order_wire
    // ------------------------------------------------------------------

    #[test]
    fn test_build_order_wire_price_precision() {
        let wire = build_order_wire(
            &asset(0, 5),
            wire_order(610_052, 0.1, 1.0, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.p, "61005.2");

        let wire = build_order_wire(
            &asset(0, 5),
            wire_order(63_000, 1.0, 1.0, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.p, "63000");

        let wire = build_order_wire(
            &asset(0, 5),
            wire_order(1_234_567, 0.01, 1.0, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.p, "12345.67");
    }

    #[test]
    fn test_build_order_wire_size_precision() {
        let wire = build_order_wire(
            &asset(0, 5),
            wire_order(63_000, 1.0, 0.001, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.s, "0.001");

        let wire = build_order_wire(
            &asset(0, 2),
            wire_order(63_000, 1.0, 1.5, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.s, "1.5");

        let wire = build_order_wire(
            &asset(0, 0),
            wire_order(63_000, 1.0, 1.0, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert_eq!(wire.s, "1");
    }

    #[test]
    fn test_build_order_wire_tif_mapping() {
        for (tif, expected) in [
            (TimeInForce::GTC, "Gtc"),
            (TimeInForce::GTX, "Alo"),
            (TimeInForce::IOC, "Ioc"),
        ] {
            let wire = build_order_wire(
                &asset(0, 5),
                wire_order(63_000, 1.0, 0.001, Side::Buy, tif),
                "0xab".repeat(16),
            )
            .unwrap();
            assert_eq!(wire.t.limit.tif, expected);
        }
    }

    #[test]
    fn test_build_order_wire_rejects_fok_and_unsupported_tif() {
        for tif in [TimeInForce::FOK, TimeInForce::Unsupported] {
            let result = build_order_wire(
                &asset(0, 5),
                wire_order(63_000, 1.0, 0.001, Side::Buy, tif),
                "0xab".repeat(16),
            );
            assert!(matches!(result, Err(HyperliquidError::InvalidArg(_))));
        }
    }

    #[test]
    fn test_build_order_wire_side_and_asset() {
        let buy = build_order_wire(
            &asset(3, 5),
            wire_order(63_000, 1.0, 0.001, Side::Buy, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert!(buy.b);
        assert_eq!(buy.a, 3);

        let sell = build_order_wire(
            &asset(3, 5),
            wire_order(63_000, 1.0, 0.001, Side::Sell, TimeInForce::GTC),
            "0xab".repeat(16),
        )
        .unwrap();
        assert!(!sell.b);
    }

    #[test]
    fn test_build_order_wire_rejects_invalid_side_and_type() {
        let invalid_side = build_order_wire(
            &asset(0, 5),
            wire_order(63_000, 1.0, 0.001, Side::None, TimeInForce::GTC),
            "0xab".repeat(16),
        );
        assert!(matches!(invalid_side, Err(HyperliquidError::InvalidArg(_))));

        let mut market = wire_order(63_000, 1.0, 0.001, Side::Buy, TimeInForce::GTC);
        market.order_type = OrdType::Market;
        let invalid_type = build_order_wire(&asset(0, 5), market, "0xab".repeat(16));
        assert!(matches!(invalid_type, Err(HyperliquidError::InvalidArg(_))));
    }

    #[test]
    fn test_build_order_wire_injects_cloid_and_reduce_only() {
        let cloid = "0xab".repeat(16);
        let wire = build_order_wire(
            &asset(0, 5),
            wire_order(63_000, 1.0, 0.001, Side::Buy, TimeInForce::GTC),
            cloid.clone(),
        )
        .unwrap();
        assert_eq!(wire.c.as_deref(), Some(cloid.as_str()));
        assert!(!wire.r);
    }

    // ------------------------------------------------------------------
    // parse_order_statuses / parse_cancel_statuses / exchange_error_message
    // ------------------------------------------------------------------

    fn exchange_response(status: &str, data: Option<serde_json::Value>) -> ExchangeResponse {
        ExchangeResponse {
            status: status.to_string(),
            response: data.map(|data| ExchangeResponseData {
                type_: String::new(),
                data: Some(data),
            }),
        }
    }

    #[test]
    fn test_parse_order_statuses_resting_and_filled() {
        let resting = exchange_response(
            "ok",
            Some(serde_json::json!({"statuses": [{"resting": {"oid": 42}}]})),
        );
        assert!(matches!(
            parse_order_statuses(&resting).unwrap(),
            Some(OrderStatus::Resting {
                resting: Resting { oid: 42 }
            })
        ));

        let filled = exchange_response(
            "ok",
            Some(
                serde_json::json!({"statuses": [{"filled": {"totalSz": "0.001", "avgPx": "64200", "oid": 43}}]}),
            ),
        );
        assert!(matches!(
            parse_order_statuses(&filled).unwrap(),
            Some(OrderStatus::Filled {
                filled: Filled { oid: 43, .. }
            })
        ));
    }

    #[test]
    fn test_parse_order_statuses_error_and_empty() {
        let error = exchange_response(
            "ok",
            Some(serde_json::json!({"statuses": [{"error": "invalid price"}]})),
        );
        assert!(matches!(
            parse_order_statuses(&error).unwrap(),
            Some(OrderStatus::Error { error }) if error == "invalid price"
        ));

        let empty = exchange_response("ok", Some(serde_json::json!({"statuses": []})));
        assert!(parse_order_statuses(&empty).unwrap().is_none());
    }

    #[test]
    fn test_parse_order_statuses_err_response() {
        let err = exchange_response("err", Some(serde_json::json!("insufficient balance")));
        assert!(matches!(
            parse_order_statuses(&err),
            Err(HyperliquidError::OrderError(msg)) if msg == "insufficient balance"
        ));
    }

    #[test]
    fn test_parse_cancel_statuses() {
        let success = exchange_response("ok", Some(serde_json::json!({"statuses": ["success"]})));
        assert!(matches!(
            parse_cancel_statuses(&success).unwrap(),
            Some(CancelStatus::Success(_))
        ));

        let error = exchange_response(
            "ok",
            Some(serde_json::json!({"statuses": [{"error": "not found"}]})),
        );
        assert!(matches!(
            parse_cancel_statuses(&error).unwrap(),
            Some(CancelStatus::Error { error }) if error == "not found"
        ));

        let empty = exchange_response("ok", Some(serde_json::json!({"statuses": []})));
        assert!(parse_cancel_statuses(&empty).unwrap().is_none());
    }

    #[test]
    fn test_exchange_error_message() {
        let with_msg = exchange_response("err", Some(serde_json::json!("agent not authorized")));
        assert_eq!(exchange_error_message(&with_msg), "agent not authorized");

        let without_msg = exchange_response("err", None);
        assert_eq!(exchange_error_message(&without_msg), "err");
    }

    // ------------------------------------------------------------------
    // build_from / Config
    // ------------------------------------------------------------------

    fn config_str(private_key: &str, account_address: &str) -> String {
        format!(
            r#"info_url = "https://api.hyperliquid-testnet.xyz/info"
exchange_url = "https://api.hyperliquid-testnet.xyz/exchange"
ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
private_key = "{private_key}"
account_address = "{account_address}"
is_mainnet = false
"#
        )
    }

    #[test]
    fn test_build_from_private_key_with_and_without_0x() {
        let key_hex = hex::encode([7u8; 32]);
        let with_prefix =
            Hyperliquid::build_from(&config_str(&format!("0x{key_hex}"), "")).unwrap();
        let without_prefix = Hyperliquid::build_from(&config_str(&key_hex, "")).unwrap();
        assert_eq!(with_prefix.private_key, [7u8; 32]);
        assert_eq!(without_prefix.private_key, [7u8; 32]);
    }

    #[test]
    fn test_build_from_rejects_invalid_private_key() {
        assert!(Hyperliquid::build_from(&config_str("not-hex", "")).is_err());
        assert!(Hyperliquid::build_from(&config_str(&"ab".repeat(20), "")).is_err());
    }

    #[test]
    fn public_market_builder_does_not_require_or_retain_a_private_key() {
        let public = r#"info_url = "http://localhost/info"
exchange_url = "http://localhost/exchange"
ws_url = "ws://localhost/ws"
"#;
        let connector = Hyperliquid::build_market_from(public).unwrap();
        assert_eq!(connector.private_key, [0; 32]);
        assert!(connector.account_address.is_empty());
        assert!(Hyperliquid::build_from(public).is_err());
    }

    #[test]
    fn test_build_from_derives_address_when_empty() {
        let key_hex = hex::encode([7u8; 32]);
        let connector = Hyperliquid::build_from(&config_str(&key_hex, "")).unwrap();
        assert_eq!(
            connector.account_address,
            "0x4a62316623ad457f02cdc5d997ded67a383ec569"
        );
        assert!(!connector.config.is_mainnet);
    }

    #[test]
    fn test_build_from_accepts_agent_mode() {
        let key_hex = hex::encode([7u8; 32]);
        let agent_address = "0x0a7ffbb0e836b4859f01ece24c361dce5df11957";
        let connector = Hyperliquid::build_from(&config_str(&key_hex, agent_address)).unwrap();
        assert_eq!(connector.account_address, agent_address.to_string());
    }
}

/// 实盘 WS 探针：公共流（l2Book/trades）+ 私有流（orderUpdates/userEvents + 下单触发）。
///
/// 运行方式同 `hyperliquid::brokerapi::tests::live_private_api_smoke`
/// （HL_PRIVATE_KEY / HL_ACCOUNT_ADDRESS 环境变量，--ignored --nocapture）。
#[cfg(test)]
mod live_ws_tests {
    use super::*;
    use crate::api::{
        AmendOrderRequest, ApiOrderType, ApiSide, ApiTimeInForce, CancelOrderRequest,
        UnifiedOrderRequest,
    };
    use crate::connector::{
        AccountPublication, DirectPublication, PublishEvent, direct_publish_sender,
    };
    use hftbacktest::types::{DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, TRADE_EVENT};
    use std::time::Duration;
    use tokio::sync::mpsc::unbounded_channel;

    #[allow(dead_code)]
    #[derive(Debug)]
    enum PrivateFact {
        Ready,
        Error(String),
        Order {
            client_order_id: Option<String>,
            venue_order_id: Option<String>,
            order: hftbacktest::types::Order,
        },
        Position(String, f64),
    }

    async fn wait_private_order(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<PrivateFact>,
        expected_cloid: &str,
        venue_order_id: &str,
        expected_status: hftbacktest::types::Status,
    ) -> hftbacktest::types::Order {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let fact = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("timeout waiting for private order fact")
                .expect("publisher channel closed");
            println!("  fact: {fact:?}");
            if let PrivateFact::Order {
                client_order_id,
                venue_order_id: Some(venue),
                order,
            } = fact
                && venue == venue_order_id
                && order.status == expected_status
            {
                assert_eq!(client_order_id.as_deref(), Some(expected_cloid));
                return order;
            }
        }
    }

    fn assert_rest_ws_fields(
        rest: &crate::api::OrderInfo,
        ws: &hftbacktest::types::Order,
        expected_status: hftbacktest::types::Status,
    ) {
        let ws_price = ws.price_tick as f64 * ws.tick_size;
        assert!((rest.price - ws_price).abs() <= ws.tick_size / 2.0);
        assert!((rest.qty - ws.qty).abs() < 1e-12);
        assert!((rest.executed_qty - ws.exec_qty).abs() < 1e-12);
        assert!((rest.leaves_qty - ws.leaves_qty).abs() < 1e-12);
        assert_eq!(ws.status, expected_status);
        assert_eq!(format!("{:?}", rest.status), format!("{expected_status:?}"));
        if rest.update_time > 0 && ws.exch_timestamp > 0 {
            assert_eq!(rest.update_time * 1_000_000, ws.exch_timestamp);
        }
    }

    async fn spot_usdc(client: &HyperliquidClient, account: &str) -> f64 {
        client
            .post_info(serde_json::json!({"type": "spotClearinghouseState", "user": account}))
            .await
            .expect("spotClearinghouseState")["balances"]
            .as_array()
            .and_then(|balances| balances.iter().find(|balance| balance["coin"] == "USDC"))
            .and_then(|balance| balance["total"].as_str())
            .and_then(|total| total.parse().ok())
            .unwrap_or(0.0)
    }

    const MAINNET_CFG: &str = concat!(
        "info_url = \"https://api.hyperliquid.xyz/info\"\n",
        "exchange_url = \"https://api.hyperliquid.xyz/exchange\"\n",
        "ws_url = \"wss://api.hyperliquid.xyz/ws\"\n",
        "safety_timeout_ms = 0\n",
        "is_mainnet = true\n"
    );

    fn env_creds() -> (String, String) {
        (
            std::env::var("HL_PRIVATE_KEY").expect("HL_PRIVATE_KEY is required"),
            std::env::var("HL_ACCOUNT_ADDRESS").expect("HL_ACCOUNT_ADDRESS is required"),
        )
    }

    /// 公共流：订阅 BTC Depth+Trades，20s 内应各收到至少一批 FeedBatch。
    #[tokio::test]
    #[ignore]
    async fn live_ws_public_streams_probe() {
        // workspace 同时启用 aws-lc-rs 与 ring，测试进程需手动选择 provider
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut connector = Hyperliquid::build_market_from(MAINNET_CFG).unwrap();
        connector.subscribe_market_data(
            "BTC".to_owned(),
            vec![MarketDataKind::Depth, MarketDataKind::Trades],
        );
        let (events, mut receiver) = crate::connector::test_publish_channel();
        connector.run_market_data(events);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let (mut saw_depth, mut saw_trade) = (false, false);
        while !(saw_depth && saw_trade) {
            let ev = tokio::time::timeout_at(deadline, receiver.recv())
                .await
                .expect("timeout waiting for public streams")
                .expect("publish channel closed");
            match ev {
                PublishEvent::FeedBatch { events, .. } => {
                    for e in &events {
                        if e.is(TRADE_EVENT) {
                            saw_trade = true;
                        }
                        if e.is(DEPTH_EVENT) || e.is(DEPTH_SNAPSHOT_EVENT) {
                            saw_depth = true;
                        }
                    }
                }
                PublishEvent::ConnectorError(e) => panic!("public stream error: {e:?}"),
                _ => {}
            }
        }
        println!("public streams OK: depth={saw_depth} trades={saw_trade}");
    }

    /// 私有流：连接 orderUpdates/userEvents，经 REST 下深价单/改单/撤单，
    /// 每一步都应通过私有流推回 AccountPublication::Order 事实。
    #[tokio::test]
    #[ignore]
    async fn live_ws_private_stream_probe() {
        // 同上：aws-lc-rs / ring 双 feature 下需显式安装 provider
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (key, account) = env_creds();
        let config =
            format!("{MAINNET_CFG}private_key = \"{key}\"\naccount_address = \"{account}\"\n");
        let mut connector = Hyperliquid::build_from(&config).unwrap();
        connector.register_account("ETH".to_owned());
        connector.arm_private_reconnect_fault();

        // 自定义 publisher：Event 与 Account 两条路都记录
        let (tx, mut rx) = unbounded_channel::<PrivateFact>();
        let publisher = direct_publish_sender(move |publication| match publication {
            DirectPublication::Event(e) => match e {
                PublishEvent::PrivateStreamReady => {
                    let _ = tx.send(PrivateFact::Ready);
                }
                PublishEvent::ConnectorError(err) => {
                    let _ = tx.send(PrivateFact::Error(format!("ConnectorError: {err:?}")));
                }
                _ => {}
            },
            DirectPublication::Account(a) => match a {
                AccountPublication::Order {
                    client_order_id,
                    venue_order_id,
                    order,
                    ..
                } => {
                    let _ = tx.send(PrivateFact::Order {
                        client_order_id: client_order_id.clone(),
                        venue_order_id: venue_order_id.clone(),
                        order: order.clone(),
                    });
                }
                AccountPublication::Position { symbol, qty, .. } => {
                    let _ = tx.send(PrivateFact::Position(symbol.clone(), *qty));
                }
                AccountPublication::Error(e) => {
                    let _ = tx.send(PrivateFact::Error(format!("AccountError: {e:?}")));
                }
            },
            DirectPublication::NativeMarket(_) => {}
        });
        connector.run_account(publisher);

        let api = connector.broker_api().expect("broker api available");

        // 首次 READY 后注入一次真实 socket 断开；连接器必须重连并重放两个私有订阅。
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut ready_count = 0;
        while ready_count < 2 {
            let m = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("timeout waiting for private stream reconnect")
                .expect("publisher channel closed");
            if matches!(m, PrivateFact::Ready) {
                ready_count += 1;
                println!("private stream READY #{ready_count}");
                continue;
            }
            println!("  pre-ready: {m:?}");
        }
        println!("private stream reconnect OK");

        // 深价 GTC 下单（mid 的 50%，不成交）
        let ticker = api.get_ticker("ETH").await.unwrap();
        let mid = ticker.mark_price.unwrap_or(ticker.last_price);
        let deep_px = (mid * 0.5 * 10.0).round() / 10.0;
        let req = UnifiedOrderRequest {
            symbol: "ETH".to_string(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(deep_px),
            qty: 0.01,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: None,
            client_order_id: Some("0xcccccccccccccccccccccccccccccccc".to_string()),
            stop_price: None,
        };
        // 私有流只发布 OrderManager 已跟踪的订单：把本次下单注册进去
        // （真实路径中由 AccountPlugin 下单命令完成同样的动作）
        let mut tracked = hftbacktest::types::Order::new(
            0,
            (deep_px / 0.1).round() as i64,
            0.1,
            0.01,
            hftbacktest::types::Side::Buy,
            hftbacktest::types::OrdType::Limit,
            hftbacktest::types::TimeInForce::GTC,
        );
        tracked.status = hftbacktest::types::Status::New;
        connector.track_managed_order("ETH", req.client_order_id.as_deref().unwrap(), &tracked);

        let order = api.submit_order(&req).await.expect("submit_order");
        println!("submitted oid={}", order.order_id);

        // 等私有流推回该订单的 Order 事实
        let submitted_id = order.order_id.clone();
        let ws_submit = wait_private_order(
            &mut rx,
            req.client_order_id.as_deref().unwrap(),
            &submitted_id,
            hftbacktest::types::Status::New,
        )
        .await;
        let rest_submit = api
            .get_order("ETH", Some(&submitted_id), None)
            .await
            .expect("REST get submitted order");

        // 改单（撤旧挂新，oid 会变）
        let amend = api
            .amend_order(&AmendOrderRequest {
                symbol: "ETH".to_string(),
                order_id: Some(submitted_id.clone()),
                client_order_id: None,
                new_price: Some((deep_px * 0.98 * 10.0).round() / 10.0),
                new_qty: Some(0.01),
                new_stop_price: None,
            })
            .await
            .expect("amend_order");
        println!("amended new oid={}", amend.order_id);
        let ws_amend = wait_private_order(
            &mut rx,
            req.client_order_id.as_deref().unwrap(),
            &amend.order_id,
            hftbacktest::types::Status::New,
        )
        .await;
        let rest_amend = api
            .get_order("ETH", Some(&amend.order_id), None)
            .await
            .expect("REST get amended order");

        api.cancel_order(&CancelOrderRequest {
            symbol: "ETH".to_string(),
            order_id: Some(amend.order_id.clone()),
            client_order_id: None,
        })
        .await
        .expect("cancel_order");
        println!("canceled oid={}", amend.order_id);

        let ws_cancel = wait_private_order(
            &mut rx,
            req.client_order_id.as_deref().unwrap(),
            &amend.order_id,
            hftbacktest::types::Status::Canceled,
        )
        .await;
        let rest_cancel = api
            .get_order("ETH", Some(&amend.order_id), None)
            .await
            .expect("REST get canceled order");
        let expected_cloid = req.client_order_id.as_deref().unwrap();
        assert_eq!(rest_submit.client_order_id, expected_cloid);
        assert_eq!(rest_amend.client_order_id, expected_cloid);
        assert_eq!(rest_cancel.client_order_id, expected_cloid);
        assert_rest_ws_fields(&rest_submit, &ws_submit, hftbacktest::types::Status::New);
        assert_rest_ws_fields(&rest_amend, &ws_amend, hftbacktest::types::Status::New);
        assert_rest_ws_fields(
            &rest_cancel,
            &ws_cancel,
            hftbacktest::types::Status::Canceled,
        );
        println!("private streams OK: reconnect + submit/amend/cancel REST/WS fields match");
    }

    /// Takes only the current best ask and cancels the IOC remainder, then immediately closes the
    /// acquired position. The cap defaults to 20 USDC and can be lowered with HL_PARTIAL_MAX_USD.
    #[tokio::test]
    #[ignore]
    async fn live_partial_fill_reconcile_probe() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (key, account) = env_creds();
        let candidates = std::env::var("HL_PARTIAL_SYMBOL")
            .map(|symbol| vec![symbol])
            .unwrap_or_else(|_| {
                [
                    "GAS", "UMA", "BANANA", "STABLE", "RESOLV", "HYPER", "kLUNC", "MANTA", "TRB",
                    "INIT", "MERL", "BSV", "BABY", "BIO",
                ]
                .into_iter()
                .map(str::to_string)
                .collect()
            });
        let max_usd = std::env::var("HL_PARTIAL_MAX_USD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(20.0);
        assert!((10.0..=25.0).contains(&max_usd));
        let config =
            format!("{MAINNET_CFG}private_key = \"{key}\"\naccount_address = \"{account}\"\n");
        let mut connector = Hyperliquid::build_from(&config).unwrap();
        for symbol in &candidates {
            connector.register_account(symbol.clone());
        }

        let (tx, mut rx) = unbounded_channel::<PrivateFact>();
        let publisher = direct_publish_sender(move |publication| match publication {
            DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
                let _ = tx.send(PrivateFact::Ready);
            }
            DirectPublication::Event(PublishEvent::ConnectorError(error)) => {
                let _ = tx.send(PrivateFact::Error(format!("ConnectorError: {error:?}")));
            }
            DirectPublication::Account(AccountPublication::Order {
                client_order_id,
                venue_order_id,
                order,
                ..
            }) => {
                let _ = tx.send(PrivateFact::Order {
                    client_order_id: client_order_id.clone(),
                    venue_order_id: venue_order_id.clone(),
                    order: order.clone(),
                });
            }
            DirectPublication::Account(AccountPublication::Position { symbol, qty, .. }) => {
                let _ = tx.send(PrivateFact::Position(symbol.clone(), *qty));
            }
            DirectPublication::Account(AccountPublication::Error(error)) => {
                let _ = tx.send(PrivateFact::Error(format!("AccountError: {error:?}")));
            }
            _ => {}
        });
        connector.run_account(publisher);
        let api = connector.broker_api().unwrap();
        loop {
            if matches!(
                tokio::time::timeout(Duration::from_secs(30), rx.recv())
                    .await
                    .expect("private stream ready timeout")
                    .expect("publisher channel closed"),
                PrivateFact::Ready
            ) {
                break;
            }
        }

        let before_balance = spot_usdc(&connector.client, &account).await;
        let instruments = api.get_instruments().await.unwrap();
        let scan_deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let (symbol, instrument, ask, qty) = 'scan: loop {
            assert!(
                tokio::time::Instant::now() < scan_deadline,
                "no usable thin best ask appeared within 180 seconds"
            );
            for symbol in &candidates {
                let instrument = instruments
                    .iter()
                    .find(|instrument| instrument.symbol == *symbol)
                    .expect("partial-fill symbol exists");
                let book = api.get_order_book(symbol, 2).await.unwrap();
                let Some(ask) = book.asks.first() else {
                    continue;
                };
                let qty =
                    ((max_usd / ask.price) / instrument.lot_size).floor() * instrument.lot_size;
                let top_ask_notional = ask.price * ask.qty;
                if top_ask_notional >= 10.0 && ask.qty + instrument.lot_size <= qty {
                    break 'scan (symbol.clone(), instrument.clone(), ask.clone(), qty);
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        };
        let significant_tick = 10f64.powi(ask.price.log10().floor() as i32 - 4);
        let decimal_tick = 10f64.powi(-(6_i32 - instrument.qty_precision as i32));
        let tick_size = significant_tick.max(decimal_tick);
        // Cross exactly the current best ask with an IOC whose quantity is larger than that one
        // level. If the book is unchanged at matching time, the first level fills and the
        // remainder cancels without crossing the next price level.
        let price = ask.price;
        let top_ask_notional = ask.price * ask.qty;
        let cloid = "0xf0f0f0f0f0f0f0f0f0f0f0f0f0f00001";
        let mut tracked = hftbacktest::types::Order::new(
            0,
            (price / tick_size).round() as i64,
            tick_size,
            qty,
            hftbacktest::types::Side::Buy,
            hftbacktest::types::OrdType::Limit,
            hftbacktest::types::TimeInForce::IOC,
        );
        tracked.status = hftbacktest::types::Status::New;
        connector.track_managed_order(&symbol, cloid, &tracked);
        let submitted = api
            .submit_order(&UnifiedOrderRequest {
                symbol: symbol.clone(),
                side: ApiSide::Buy,
                order_type: ApiOrderType::Limit,
                price: Some(price),
                qty,
                time_in_force: ApiTimeInForce::IOC,
                reduce_only: false,
                position_side: None,
                client_order_id: Some(cloid.to_string()),
                stop_price: None,
            })
            .await
            .expect("bounded one-level IOC partial-fill order");
        println!(
            "waiting for partial fill: buy {} {} @ {} against top ask qty={} notional={top_ask_notional:.4} (max ${max_usd})",
            qty, symbol, price, ask.qty
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut ws_partial = None;
        while tokio::time::Instant::now() < deadline {
            let fact = match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(fact)) => fact,
                _ => break,
            };
            println!("  passive fact: {fact:?}");
            if let PrivateFact::Order {
                venue_order_id: Some(venue),
                order,
                ..
            } = fact
                && venue == submitted.order_id
                && order.exec_qty > 0.0
            {
                ws_partial = Some(order);
                break;
            }
        }
        // Always remove any unfilled remainder before assertions or reconciliation.
        let _ = api
            .cancel_order(&CancelOrderRequest {
                symbol: symbol.clone(),
                order_id: Some(submitted.order_id.clone()),
                client_order_id: None,
            })
            .await;
        let rest_partial = api
            .get_order(&symbol, Some(&submitted.order_id), None)
            .await
            .expect("REST partial order after cancel");

        if rest_partial.executed_qty > 0.0 {
            let close_cloid = "0xf0f0f0f0f0f0f0f0f0f0f0f0f0f00002";
            let mut close_tracked = hftbacktest::types::Order::new(
                0,
                0,
                tick_size,
                rest_partial.executed_qty,
                hftbacktest::types::Side::Sell,
                hftbacktest::types::OrdType::Market,
                hftbacktest::types::TimeInForce::IOC,
            );
            close_tracked.status = hftbacktest::types::Status::New;
            connector.track_managed_order(&symbol, close_cloid, &close_tracked);
            api.submit_order(&UnifiedOrderRequest {
                symbol: symbol.clone(),
                side: ApiSide::Sell,
                order_type: ApiOrderType::Market,
                price: None,
                qty: rest_partial.executed_qty,
                time_in_force: ApiTimeInForce::IOC,
                reduce_only: true,
                position_side: None,
                client_order_id: Some(close_cloid.to_string()),
                stop_price: None,
            })
            .await
            .expect("close partial-fill position");
        }

        assert!(rest_partial.executed_qty > 0.0);
        assert!(rest_partial.executed_qty < rest_partial.qty);
        let ws_partial = ws_partial.expect("no fill arrived within 30 seconds");
        assert_eq!(rest_partial.order_id, submitted.order_id);
        assert_eq!(rest_partial.client_order_id, cloid);
        assert_rest_ws_fields(
            &rest_partial,
            &ws_partial,
            hftbacktest::types::Status::Filled,
        );
        assert!(ws_partial.leaves_qty > 0.0);
        let fills = api.get_fills(&symbol, 100).await.unwrap();
        let opened: f64 = fills
            .iter()
            .filter(|fill| fill.order_id == submitted.order_id)
            .map(|fill| fill.qty)
            .sum();
        assert!((opened - rest_partial.executed_qty).abs() < 1e-12);

        for _ in 0..30 {
            if api.get_positions(Some(&symbol)).await.unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(api.get_open_orders(&symbol).await.unwrap().is_empty());
        assert!(api.get_positions(Some(&symbol)).await.unwrap().is_empty());
        let after_balance = spot_usdc(&connector.client, &account).await;
        assert!(
            before_balance - after_balance < 1.0,
            "unexpected balance loss"
        );
        println!(
            "partial reconcile OK: symbol={symbol} qty={} filled={} REST=WS=fills, final orders=0 positions=0, balance_delta={:.6}",
            rest_partial.qty,
            rest_partial.executed_qty,
            before_balance - after_balance
        );
    }
}
