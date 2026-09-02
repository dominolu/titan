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

use hftbacktest::{
    prelude::get_precision,
    types::{ErrorKind, LiveError, LiveEvent, OrdType, Order, Side, Status, TimeInForce, Value},
};
use serde::Deserialize;
use thiserror::Error;
use titan_market_plugin::MarketDataKind;
use tokio::sync::{broadcast, broadcast::Sender};
use tracing::{error, warn};

use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent},
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
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
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
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::ConnectionInterrupted,
                                error.to_value(),
                            ))))
                            .unwrap();
                    } else {
                        ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::new(
                                ErrorKind::ConnectionInterrupted,
                            ))))
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
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
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
        let mut builder = reqwest::Client::builder();
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

    fn broker_api(&self) -> Option<Arc<dyn crate::api::BrokerApi>> {
        Some(Arc::new(self.client.clone()))
    }

    fn submit(&self, symbol: String, mut order: Order, tx: crate::connector::PublishSender) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let assets = self.assets.clone();
        let td_mode = self.config.td_mode.clone();
        let pos_side = self.config.pos_side.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .prepare_client_order_id(symbol.clone(), order.clone());

            match client_order_id {
                Some(client_order_id) => {
                    let side = match order.side {
                        Side::Buy => "buy",
                        Side::Sell => "sell",
                        Side::None | Side::Unsupported => {
                            submit_fail(
                                &client_order_id,
                                &order_manager,
                                &symbol,
                                &tx,
                                OkxError::InvalidArg("side"),
                            );
                            return;
                        }
                    };
                    let (ord_type, px) = match order.order_type {
                        OrdType::Limit => (
                            match order.time_in_force {
                                TimeInForce::GTC => "limit",
                                TimeInForce::GTX => "post_only",
                                TimeInForce::IOC => "ioc",
                                TimeInForce::FOK => "fok",
                                TimeInForce::Unsupported => {
                                    submit_fail(
                                        &client_order_id,
                                        &order_manager,
                                        &symbol,
                                        &tx,
                                        OkxError::InvalidArg("time_in_force"),
                                    );
                                    return;
                                }
                            },
                            Some(format!(
                                "{:.prec$}",
                                order.price_tick as f64 * order.tick_size,
                                prec = get_precision(order.tick_size)
                            )),
                        ),
                        OrdType::Market => ("market", None),
                        OrdType::Unsupported => {
                            submit_fail(
                                &client_order_id,
                                &order_manager,
                                &symbol,
                                &tx,
                                OkxError::InvalidArg("order_type"),
                            );
                            return;
                        }
                    };

                    let sz_decimals = match ensure_asset_decimals(&client, &assets, &symbol).await {
                        Ok(decimals) => decimals,
                        Err(error) => {
                            submit_fail(&client_order_id, &order_manager, &symbol, &tx, error);
                            return;
                        }
                    };

                    let result = client
                        .submit_order(
                            &symbol,
                            &td_mode,
                            pos_side.as_deref(),
                            &client_order_id,
                            side,
                            ord_type,
                            px.as_deref(),
                            &format!("{:.prec$}", order.qty, prec = sz_decimals),
                        )
                        .await;
                    match result {
                        Ok(resp) => {
                            if let Ok(Some(order)) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest_submit(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                            if resp.s_code != "0" {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Error(
                                    LiveError::with(
                                        ErrorKind::OrderError,
                                        OkxError::OrderError {
                                            code: resp.s_code,
                                            msg: resp.s_msg,
                                        }
                                        .to_value(),
                                    ),
                                )))
                                .unwrap();
                            }
                        }
                        Err(error) => {
                            submit_fail(&client_order_id, &order_manager, &symbol, &tx, error);
                        }
                    }
                }
                None => {
                    warn!(
                        ?order,
                        "Coincidentally, creates a duplicated client order id. \
                        This order request will be expired."
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                        .unwrap();
                }
            }
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: crate::connector::PublishSender) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .get_client_order_id(&symbol, order.order_id);

            match client_order_id {
                Some(client_order_id) => {
                    let result = client.cancel_order(&symbol, &client_order_id).await;
                    match result {
                        Ok(()) => {
                            if let Ok(Some(order)) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest_cancel(&client_order_id)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                        }
                        Err(error) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_cancel_fail(&client_order_id, &error)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }

                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.to_value(),
                            ))))
                            .unwrap();
                        }
                    }
                }
                None => {
                    warn!(
                        order_id = order.order_id,
                        "client_order_id corresponding to order_id is not found; \
                        this may be due to the order already being canceled or filled."
                    );
                }
            }
        });
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

fn submit_fail(
    client_order_id: &String,
    order_manager: &SharedOrderManager,
    symbol: &str,
    tx: &crate::connector::PublishSender,
    error: OkxError,
) {
    if let Some(order) = order_manager
        .lock()
        .unwrap()
        .update_submit_fail(client_order_id, &error)
    {
        tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
            symbol: symbol.to_string(),
            order,
        }))
        .unwrap();
    }
    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
        ErrorKind::OrderError,
        error.to_value(),
    ))))
    .unwrap();
}

#[cfg(test)]
mod tests;
