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
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tracing::{error, warn};

use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
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
}

fn default_td_mode() -> String {
    "cross".to_string()
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;
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
}

impl Okx {
    fn connect_public_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let public_url = self.config.public_ws_url.clone();
        let symbol_tx = self.symbol_tx.clone();
        let symbols = self.symbols.clone();

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
                        symbols.clone(),
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

    fn connect_private_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
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
        })
    }
}

impl Connector for Okx {
    fn register(&mut self, symbol: String) {
        if symbol.to_uppercase() != symbol {
            error!("OKX symbol must be uppercase, e.g. BTC-USDT-SWAP.");
        }
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_public_stream(ev_tx.clone());
        if !self.config.api_key.is_empty() && !self.config.secret.is_empty() {
            self.connect_private_stream(ev_tx);
        }
    }

    fn submit(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
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

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
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
}

fn submit_fail(
    client_order_id: &String,
    order_manager: &SharedOrderManager,
    symbol: &str,
    tx: &UnboundedSender<PublishEvent>,
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
