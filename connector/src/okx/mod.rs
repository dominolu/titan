#[allow(dead_code)]
mod brokerapi;
mod msg;
mod ordermanager;
mod private_stream;
mod public_stream;
mod rest;

use std::{
    collections::{HashMap, HashSet},
    num::{ParseFloatError, ParseIntError},
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, Order, Value};
use serde::Deserialize;
use thiserror::Error;
use titan_market_plugin::MarketDataKind;
use tokio::sync::{broadcast, broadcast::Sender};
use tracing::error;

use crate::{
    connector::{
        AccountPublication, Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent,
    },
    okx::{
        ordermanager::{OrderManager, SharedOrderManager},
        rest::OkxClient,
    },
    utils::{ExponentialBackoff, Retry},
};

#[derive(Error, Debug)]
pub enum OkxError {
    #[error("AuthError: {code} - {msg}")]
    AuthError { code: String, msg: String },
    #[error("OrderError: {code} - {msg}")]
    OrderError { code: String, msg: String },
    #[error("InvalidPxQty: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("InvalidOrderId: {0}")]
    InvalidOrderId(#[from] ParseIntError),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl OkxError {
    pub fn to_value(&self) -> Value {
        match self {
            OkxError::AuthError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::String(code.clone()));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            OkxError::OrderError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::String(code.clone()));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            _ => Value::String(self.to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    rest_url: String,
    public_ws_url: String,
    private_ws_url: String,
    api_key: String,
    secret: String,
    passphrase: String,
    #[serde(default = "default_td_mode")]
    td_mode: String,
    /// Position side used for order placement: "net", "long" or "short". Required by OKX when the
    /// account is in long/short (hedge) position mode.
    #[serde(default)]
    pos_side: Option<String>,
    /// Demo trading mode: adds the `x-simulated-trading: 1` header to every REST request and uses
    /// the simulated WebSocket endpoints (wss://wspap.okx.com:8443/...).
    #[serde(default)]
    simulated: bool,
    /// Optional proxy for all REST requests, e.g. "socks5h://127.0.0.1:7897". Empty means direct.
    #[serde(default)]
    proxy: String,
    #[serde(default)]
    order_prefix: String,
    /// Exchange-side cancel-all-after timeout. Zero disables the heartbeat.
    #[serde(default = "default_safety_timeout_ms")]
    safety_timeout_ms: u64,
}

fn default_td_mode() -> String {
    "cross".to_string()
}

fn default_safety_timeout_ms() -> u64 {
    30_000
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;
type SharedMarketSubscriptions = Arc<Mutex<HashMap<String, HashSet<MarketDataKind>>>>;

fn all_market_kinds() -> Vec<MarketDataKind> {
    vec![
        MarketDataKind::Depth,
        MarketDataKind::Trades,
        MarketDataKind::Bbo,
        MarketDataKind::FundingRate,
    ]
}
type SharedAssets = Arc<Mutex<HashMap<String, usize>>>;

/// Number of decimal places allowed by the instrument's lot size, e.g. "0.001" -> 3.
fn lot_sz_decimals(lot_sz: &str) -> usize {
    match lot_sz.split('.').nth(1) {
        Some(fraction) => fraction.trim_end_matches('0').len(),
        None => 0,
    }
}

/// Resolves the quantity precision for an instrument, fetching the metadata once and caching it.
async fn ensure_asset_decimals(
    client: &OkxClient,
    assets: &SharedAssets,
    symbol: &str,
) -> Result<usize, OkxError> {
    if let Some(&decimals) = assets.lock().unwrap().get(symbol) {
        return Ok(decimals);
    }
    let instrument = client.get_instruments(symbol).await?;
    let decimals = lot_sz_decimals(&instrument.lot_sz);
    assets.lock().unwrap().insert(symbol.to_string(), decimals);
    Ok(decimals)
}

pub struct Okx {
    config: Config,
    symbols: SharedSymbolSet,
    assets: SharedAssets,
    order_manager: SharedOrderManager,
    client: OkxClient,
    symbol_tx: Sender<String>,
    market_tx: Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
}

impl Okx {
    fn start_safety_heartbeat(&self) {
        let timeout_ms = self.config.safety_timeout_ms;
        if timeout_ms == 0 || self.config.api_key.is_empty() || self.config.secret.is_empty() {
            return;
        }
        let client = self.client.clone();
        tokio::spawn(async move {
            let refresh_ms = (timeout_ms / 3).max(1_000);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(refresh_ms));
            loop {
                interval.tick().await;
                if let Err(error) = client.cancel_all_after(timeout_ms).await {
                    error!(?error, "failed to refresh cancel-all-after safety net");
                }
            }
        });
    }

    fn connect_public_stream(&self, ev_tx: crate::connector::PublishSender) {
        let public_url = self.config.public_ws_url.clone();
        let symbol_tx = self.market_tx.clone();
        let subscriptions = self.market_subscriptions.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: OkxError| {
                    error!(?error, "An error occurred in the public stream connection.");
                    ev_tx
                        .send(PublishEvent::ConnectorError(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        )))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = public_stream::PublicStream::new(
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                        subscriptions.clone(),
                    );
                    if let Err(error) = stream.connect(&public_url).await {
                        error!(?error, "A connection error occurred.");
                        ev_tx
                            .send(PublishEvent::ConnectorError(LiveError::with(
                                ErrorKind::ConnectionInterrupted,
                                error.to_value(),
                            )))
                            .unwrap();
                    } else {
                        ev_tx
                            .send(PublishEvent::ConnectorError(LiveError::new(
                                ErrorKind::ConnectionInterrupted,
                            )))
                            .unwrap();
                    }
                    Err::<(), OkxError>(OkxError::ConnectionInterrupted)
                })
                .await;
        });
    }

    fn connect_private_stream(&self, ev_tx: crate::connector::PublishSender) {
        let private_url = self.config.private_ws_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let passphrase = self.config.passphrase.clone();
        let td_mode = self.config.td_mode.clone();
        let pos_side = self.config.pos_side.clone();
        let order_manager = self.order_manager.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();
        let symbols = self.symbols.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: OkxError| {
                    error!(
                        ?error,
                        "An error occurred in the private stream connection."
                    );
                    ev_tx
                        .send_account(AccountPublication::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        )))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = private_stream::PrivateStream::new(
                        api_key.clone(),
                        secret.clone(),
                        passphrase.clone(),
                        td_mode.clone(),
                        pos_side.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        client.clone(),
                        symbol_tx.subscribe(),
                        symbols.clone(),
                    );
                    stream.connect(&private_url).await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for Okx {
    type Error = OkxError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        if config.order_prefix.len() > 16 {
            return Err(OkxError::InvalidArg(
                "order prefix length should be not greater than 16.",
            ));
        }
        // OKX client order ids only allow ASCII alphanumerics (1-32 chars), so the prefix must too.
        if !config.order_prefix.is_empty()
            && !config
                .order_prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric())
        {
            return Err(OkxError::InvalidArg(
                "order prefix must contain only ASCII alphanumeric characters.",
            ));
        }
        let (symbol_tx, _) = broadcast::channel(500);
        let (market_tx, _) = broadcast::channel(500);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10));
        if !config.proxy.is_empty() {
            builder = builder.proxy(reqwest::Proxy::all(&config.proxy)?);
        }
        let client = OkxClient::with_options(
            &config.rest_url,
            &config.api_key,
            &config.secret,
            &config.passphrase,
            config.simulated,
            builder.build()?,
            config.td_mode.clone(),
        );
        Ok(Okx {
            config,
            symbols: Default::default(),
            assets: Default::default(),
            order_manager,
            client,
            symbol_tx,
            market_tx,
            market_subscriptions: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl Connector for Okx {
    fn register_account(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if symbols.insert(symbol.clone()) {
            let _ = self.symbol_tx.send(symbol);
        }
    }
    fn register(&mut self, symbol: String) {
        if symbol.to_uppercase() != symbol {
            error!("OKX symbol must be uppercase, e.g. BTC-USDT-SWAP.");
        }
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol.clone()).unwrap();
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
        self.connect_public_stream(ev_tx.clone());
        if !self.config.api_key.is_empty() && !self.config.secret.is_empty() {
            self.connect_private_stream(ev_tx);
            self.start_safety_heartbeat();
        }
    }

    fn run_market_data(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_public_stream(ev_tx);
    }

    fn run_account(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_private_stream(ev_tx);
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
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        let mut errors = Vec::new();
        for symbol in symbols {
            if let Err(error) = self
                .client
                .cancel_all_orders(
                    &symbol,
                    &self.config.td_mode,
                    self.config.pos_side.as_deref(),
                )
                .await
            {
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
mod tests;

#[cfg(test)]
mod reconnect_tests {
    use super::*;

    #[tokio::test]
    async fn public_stream_reconnects_and_replays_desired_subscription() {
        let (url, mut subscriptions, server) =
            crate::connector::reconnecting_websocket_server(1).await;
        let config = format!(
            "rest_url = \"http://127.0.0.1:9\"\npublic_ws_url = {url:?}\nprivate_ws_url = {url:?}\napi_key = \"\"\nsecret = \"\"\npassphrase = \"\"\norder_prefix = \"test\"\nsafety_timeout_ms = 0\n"
        );
        let mut connector = Okx::build_from(&config).unwrap();
        connector.subscribe_market_data(
            "BTC-USDT-SWAP".to_owned(),
            vec![MarketDataKind::Depth, MarketDataKind::Trades],
        );
        let (events, _event_receiver) = crate::connector::test_publish_channel();
        connector.run_market_data(events);

        for _ in 0..2 {
            let subscription =
                tokio::time::timeout(std::time::Duration::from_secs(3), subscriptions.recv())
                    .await
                    .expect("connector did not reconnect before deadline")
                    .expect("websocket fixture ended before reconnect");
            assert!(subscription.contains("subscribe"));
            assert!(subscription.contains("BTC-USDT-SWAP"));
            assert!(subscription.contains("books"));
            assert!(subscription.contains("trades"));
        }
        server.await.unwrap();
    }
}
