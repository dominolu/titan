mod brokerapi;
mod market_data_stream;
mod msg;
mod ordermanager;
mod rest;
mod user_data_stream;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, Order, Value};
use serde::Deserialize;
use thiserror::Error;
use titan_market_plugin::MarketDataKind;
use tokio::sync::{broadcast, broadcast::Sender};
use tokio_tungstenite::tungstenite;
use tracing::{debug, error};

use crate::{
    binancefutures::{
        ordermanager::{OrderManager, SharedOrderManager},
        rest::BinanceFuturesClient,
    },
    connector::{
        AccountPublication, Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent,
    },
    utils::{ExponentialBackoff, Retry},
};

#[derive(Error, Debug)]
pub enum BinanceFuturesError {
    #[error("InstrumentNotFound")]
    InstrumentNotFound,
    #[error("InvalidRequest")]
    InvalidRequest,
    #[error("ListenKeyExpired")]
    ListenKeyExpired,
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ReqError: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("OrderError: {code} - {msg})")]
    OrderError { code: i64, msg: String },
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("Tunstenite: {0:?}")]
    Tunstenite(#[from] tungstenite::Error),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<BinanceFuturesError> for Value {
    fn from(value: BinanceFuturesError) -> Value {
        match value {
            BinanceFuturesError::InstrumentNotFound => Value::String(value.to_string()),
            BinanceFuturesError::InvalidRequest => Value::String(value.to_string()),
            BinanceFuturesError::ReqError(error) => {
                let mut map = HashMap::new();
                if let Some(code) = error.status() {
                    map.insert("status_code".to_string(), Value::String(code.to_string()));
                }
                map.insert("msg".to_string(), Value::String(error.to_string()));
                Value::Map(map)
            }
            BinanceFuturesError::OrderError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(code));
                map.insert("msg".to_string(), Value::String(msg));
                map
            }),
            BinanceFuturesError::Tunstenite(error) => Value::String(format!("{error}")),
            BinanceFuturesError::ListenKeyExpired => Value::String(value.to_string()),
            BinanceFuturesError::ConnectionInterrupted => Value::String(value.to_string()),
            BinanceFuturesError::ConnectionAbort(_) => Value::String(value.to_string()),
            BinanceFuturesError::Config(_) => Value::String(value.to_string()),
            BinanceFuturesError::PrefixUnmatched => Value::String(value.to_string()),
            BinanceFuturesError::OrderNotFound => Value::String(value.to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    stream_url: String,
    api_url: String,
    #[serde(default)]
    order_prefix: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    secret: String,
    /// Exchange-side countdown cancel timeout. Zero disables the heartbeat.
    #[serde(default = "default_safety_timeout_ms")]
    safety_timeout_ms: u64,
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
        MarketDataKind::MarkPrice,
        MarketDataKind::FundingRate,
    ]
}

fn private_user_stream_url(stream_url: &str, listen_key: &str) -> String {
    const PRODUCTION_ROOT: &str = "wss://fstream.binance.com";
    if stream_url == PRODUCTION_ROOT || stream_url.starts_with(&format!("{PRODUCTION_ROOT}/")) {
        // Binance split USDⓈ-M futures websocket routes on 2026-03-06 and retired the legacy
        // per-listen-key path (wss://fstream.binance.com/ws/{listenKey}) on 2026-04-23.
        // Private user data now subscribes by events on the dedicated /private route; event
        // names are joined by '/' in the query parameter.
        format!(
            "{PRODUCTION_ROOT}/private/ws?listenKey={listen_key}&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
        )
    } else {
        // Testnet/custom gateways still accept the legacy path on a single connection.
        format!("{stream_url}/{listen_key}")
    }
}

/// A connector for Binance USD-m Futures.
pub struct BinanceFutures {
    config: Config,
    symbols: SharedSymbolSet,
    order_manager: SharedOrderManager,
    client: BinanceFuturesClient,
    symbol_tx: Sender<String>,
    market_tx: Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
}

impl BinanceFutures {
    fn market_stream_endpoints(
        stream_url: &str,
    ) -> Vec<(String, market_data_stream::MarketStreamRoute)> {
        const PRODUCTION_ROOT: &str = "wss://fstream.binance.com";
        if stream_url == PRODUCTION_ROOT || stream_url.starts_with(&format!("{PRODUCTION_ROOT}/")) {
            return vec![
                (
                    format!("{PRODUCTION_ROOT}/public/ws"),
                    market_data_stream::MarketStreamRoute::Public,
                ),
                (
                    format!("{PRODUCTION_ROOT}/market/ws"),
                    market_data_stream::MarketStreamRoute::Market,
                ),
            ];
        }
        vec![(
            stream_url.to_owned(),
            market_data_stream::MarketStreamRoute::All,
        )]
    }

    fn start_safety_heartbeat(&self) {
        let timeout_ms = self.config.safety_timeout_ms;
        if timeout_ms == 0 || self.config.api_key.is_empty() || self.config.secret.is_empty() {
            return;
        }
        let client = self.client.clone();
        let symbols = self.symbols.clone();
        tokio::spawn(async move {
            let refresh_ms = (timeout_ms / 3).max(1_000);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(refresh_ms));
            loop {
                interval.tick().await;
                let registered: Vec<String> = symbols.lock().unwrap().iter().cloned().collect();
                for symbol in registered {
                    if let Err(error) = client.countdown_cancel_all(&symbol, timeout_ms).await {
                        error!(?error, %symbol, "failed to refresh countdown cancel safety net");
                    }
                }
            }
        });
    }

    pub fn connect_market_data_stream(&mut self, ev_tx: crate::connector::PublishSender) {
        for (base_url, route) in Self::market_stream_endpoints(&self.config.stream_url) {
            let client = self.client.clone();
            let symbol_tx = self.market_tx.clone();
            let market_subscriptions = self.market_subscriptions.clone();
            let ev_tx = ev_tx.clone();
            tokio::spawn(async move {
                let _ = Retry::new(ExponentialBackoff::default())
                    .error_handler(|error: BinanceFuturesError| {
                        error!(
                            ?error,
                            "An error occurred in the market data stream connection."
                        );
                        ev_tx
                            .send(PublishEvent::ConnectorError(LiveError::with(
                                ErrorKind::ConnectionInterrupted,
                                error.into(),
                            )))
                            .unwrap();
                        Ok(())
                    })
                    .retry(|| async {
                        let mut stream = market_data_stream::MarketDataStream::new(
                            client.clone(),
                            ev_tx.clone(),
                            symbol_tx.subscribe(),
                            market_subscriptions.clone(),
                            route,
                        );
                        debug!(?route, %base_url, "Connecting to the market data stream...");
                        stream.connect(&base_url).await?;
                        debug!("The market data stream connection is permanently closed.");
                        Ok(())
                    })
                    .await;
            });
        }
    }

    pub fn connect_user_data_stream(&self, ev_tx: crate::connector::PublishSender) {
        let stream_url = self.config.stream_url.clone();
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let instruments = self.symbols.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BinanceFuturesError| {
                    error!(
                        ?error,
                        "An error occurred in the user data stream connection."
                    );
                    ev_tx
                        .send_account(AccountPublication::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        )))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = user_data_stream::UserDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        instruments.clone(),
                        symbol_tx.subscribe(),
                    );

                    debug!("Requesting the listen key for the user data stream...");
                    let listen_key = stream.get_listen_key().await?;
                    let url = private_user_stream_url(&stream_url, &listen_key);

                    debug!(%url, "Connecting to the user data stream...");
                    stream.connect(&url).await?;
                    debug!("The user data stream connection is permanently closed.");
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for BinanceFutures {
    type Error = BinanceFuturesError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;

        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let symbols: SharedSymbolSet = Default::default();
        let client = BinanceFuturesClient::new(&config.api_url, &config.api_key, &config.secret)
            .with_registered_symbols(symbols.clone());
        let (symbol_tx, _) = broadcast::channel(500);
        let (market_tx, _) = broadcast::channel(500);

        Ok(BinanceFutures {
            config,
            symbols,
            order_manager,
            client,
            symbol_tx,
            market_tx,
            market_subscriptions: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl Connector for BinanceFutures {
    fn register_account(&mut self, symbol: String) {
        let symbol = symbol.to_lowercase();
        let mut symbols = self.symbols.lock().unwrap();
        if symbols.insert(symbol.clone()) {
            let _ = self.symbol_tx.send(symbol);
        }
    }
    fn register(&mut self, symbol: String) {
        // Binance futures symbols must be lowercase to subscribe to the WebSocket stream.
        if symbol.to_lowercase() != symbol {
            error!("Binance Futures symbol must be lowercase.");
        }
        let symbol = symbol.to_lowercase();
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol.clone()).unwrap();
        }
        drop(symbols);
        self.subscribe_market_data(symbol, all_market_kinds());
    }

    fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        let symbol = symbol.to_lowercase();
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
        let symbol = symbol.to_lowercase();
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
        let symbol = symbol.to_lowercase();
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
        let _ = self.market_tx.send(MarketDataCommand::Snapshot {
            symbol: symbol.to_lowercase(),
        });
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
        self.connect_market_data_stream(ev_tx.clone());
        // Connects to the user stream only if the API key and secret are provided.
        if !self.config.api_key.is_empty() && !self.config.secret.is_empty() {
            self.connect_user_data_stream(ev_tx.clone());
            self.start_safety_heartbeat();
        }
    }

    fn run_market_data(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_market_data_stream(ev_tx);
    }

    fn run_account(&mut self, ev_tx: crate::connector::PublishSender) {
        self.connect_user_data_stream(ev_tx);
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
            if let Err(error) = self.client.cancel_all_orders(&symbol).await {
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
mod reconnect_tests {
    use super::*;

    #[test]
    fn production_market_stream_is_split_by_binance_route() {
        let endpoints = BinanceFutures::market_stream_endpoints("wss://fstream.binance.com/ws");
        assert_eq!(
            endpoints,
            vec![
                (
                    "wss://fstream.binance.com/public/ws".to_owned(),
                    market_data_stream::MarketStreamRoute::Public,
                ),
                (
                    "wss://fstream.binance.com/market/ws".to_owned(),
                    market_data_stream::MarketStreamRoute::Market,
                ),
            ]
        );
        assert_eq!(
            BinanceFutures::market_stream_endpoints("ws://127.0.0.1:1234"),
            vec![(
                "ws://127.0.0.1:1234".to_owned(),
                market_data_stream::MarketStreamRoute::All,
            )]
        );
    }

    #[test]
    fn private_user_stream_uses_binance_events_route_on_production() {
        assert_eq!(
            private_user_stream_url("wss://fstream.binance.com/ws", "listen-key-abc"),
            "wss://fstream.binance.com/private/ws?listenKey=listen-key-abc&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
        );
        assert_eq!(
            private_user_stream_url("wss://fstream.binance.com", "listen-key-abc"),
            "wss://fstream.binance.com/private/ws?listenKey=listen-key-abc&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
        );
        // Non-production gateways keep the legacy path.
        assert_eq!(
            private_user_stream_url("wss://fstream.binancefuture.com/ws", "listen-key-abc"),
            "wss://fstream.binancefuture.com/ws/listen-key-abc"
        );
    }

    #[tokio::test]
    async fn public_stream_reconnects_and_replays_desired_subscription() {
        let (url, mut subscriptions, server) =
            crate::connector::reconnecting_websocket_server(1).await;
        let config = format!(
            "stream_url = {url:?}\napi_url = \"http://127.0.0.1:9\"\norder_prefix = \"test\"\napi_key = \"\"\nsecret = \"\"\nsafety_timeout_ms = 0\n"
        );
        let mut connector = BinanceFutures::build_from(&config).unwrap();
        connector.subscribe_market_data(
            "btcusdt".to_owned(),
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
            assert!(subscription.contains("SUBSCRIBE"));
            assert!(subscription.contains("btcusdt@depth@0ms"));
            assert!(subscription.contains("btcusdt@trade"));
        }
        server.await.unwrap();
    }
}
