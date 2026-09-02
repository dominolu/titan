mod brokerapi;
mod client;
#[allow(dead_code)]
mod msg;
mod ordermanager;
mod signing;
mod ws;

use std::{
    collections::{HashMap, HashSet},
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
    api::BrokerApi,
    connector::{Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent},
    hyperliquid::{
        client::HyperliquidClient,
        msg::{
            CancelAction, CancelActionWire, CancelByCloidAction, CancelByCloidWire, CancelStatus,
            CancelWire, ExchangeResponse, Meta, OrderAction, OrderStatus, OrderTypeWire, OrderWire,
            Tif,
        },
        ordermanager::{OrderManager, SharedOrderManager},
        signing::{derive_address, sign_l1_action},
    },
    utils::{ExponentialBackoff, Retry, next_nonce},
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
    nonce_counter: Arc<Mutex<u64>>,
    symbols: SharedSymbolSet,
    assets: SharedAssets,
    order_manager: SharedOrderManager,
    client: HyperliquidClient,
    market_tx: Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
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

/// Resolves the asset metadata for a symbol, refreshing the universe once if the asset was listed
/// after the initial load.
async fn ensure_asset(
    client: &HyperliquidClient,
    assets: &SharedAssets,
    symbol: &str,
) -> Result<AssetInfo, HyperliquidError> {
    if assets.lock().unwrap().is_empty() {
        ensure_assets(client, assets).await?;
    }
    if let Some(info) = assets.lock().unwrap().get(symbol).cloned() {
        return Ok(info);
    }
    // The asset may have been listed after the initial load; refresh the universe once.
    assets.lock().unwrap().clear();
    ensure_assets(client, assets).await?;
    assets
        .lock()
        .unwrap()
        .get(symbol)
        .cloned()
        .ok_or_else(|| HyperliquidError::AssetNotFound(symbol.to_string()))
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
        let nonce_counter = self.nonce_counter.clone();
        let account_address = self.account_address.clone();
        let private_key = self.private_key;
        let is_mainnet = self.config.is_mainnet;
        let client = self.client.clone();
        let market_tx = self.market_tx.clone();
        let market_subscriptions = self.market_subscriptions.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: HyperliquidError| {
                    error!(?error, "An error occurred in the WebSocket connection.");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = ws::HyperliquidWs::new(
                        ev_tx.clone(),
                        order_manager.clone(),
                        assets.clone(),
                        symbols.clone(),
                        nonce_counter.clone(),
                        account_address.clone(),
                        private_key,
                        is_mainnet,
                        client.clone(),
                        market_tx.subscribe(),
                        market_subscriptions.clone(),
                        private_channels,
                    );
                    if let Err(error) = stream.connect(&ws_url).await {
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
        let config: Config = toml::from_str(config)?;
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
            nonce_counter: Default::default(),
            symbols: Default::default(),
            assets: Default::default(),
            order_manager,
            client,
            market_tx,
            market_subscriptions: Default::default(),
        })
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

    fn broker_api(&self) -> Option<Arc<dyn crate::api::BrokerApi>> {
        Some(Arc::new(self.client.clone()))
    }

    fn submit(&self, symbol: String, mut order: Order, tx: crate::connector::PublishSender) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let assets = self.assets.clone();
        let private_key = self.private_key;
        let is_mainnet = self.config.is_mainnet;
        let nonce_counter = self.nonce_counter.clone();

        tokio::spawn(async move {
            let asset_info = match ensure_asset(&client, &assets, &symbol).await {
                Ok(info) => info,
                Err(error) => {
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                        symbol: symbol.clone(),
                        order,
                    }))
                    .unwrap();
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
                    return;
                }
            };

            let cloid = order_manager
                .lock()
                .unwrap()
                .prepare_cloid(symbol.clone(), order.clone());
            let Some(cloid) = cloid else {
                warn!(
                    ?order,
                    "Coincidentally, creates a duplicated client order id. \
                    This order request will be expired."
                );
                order.req = Status::None;
                order.status = Status::Expired;
                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                    .unwrap();
                return;
            };

            let result = build_order_wire(&asset_info, order.clone(), cloid.clone());
            let wire = match result {
                Ok(wire) => wire,
                Err(error) => {
                    submit_fail(&cloid, &order_manager, &symbol, &tx, error);
                    return;
                }
            };

            let action = OrderAction {
                type_: "order".to_string(),
                orders: vec![wire],
                grouping: "na".to_string(),
            };
            let nonce = next_nonce(&nonce_counter);
            let signature = match sign_l1_action(&action, &private_key, nonce, None, is_mainnet) {
                Ok(sig) => sig,
                Err(error) => {
                    submit_fail(&cloid, &order_manager, &symbol, &tx, error);
                    return;
                }
            };
            let resp = client.post_exchange(&action, nonce, &signature).await;
            match resp {
                Ok(resp) => {
                    let statuses = parse_order_statuses(&resp);
                    match statuses {
                        Ok(Some(status)) => {
                            if let Ok(Some(order)) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_exchange_submit(&cloid, &status)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                            if let OrderStatus::Error { error } = &status {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Error(
                                    LiveError::with(
                                        ErrorKind::OrderError,
                                        HyperliquidError::OrderError(error.clone()).to_value(),
                                    ),
                                )))
                                .unwrap();
                            }
                        }
                        Ok(None) => {
                            warn!("The exchange response contains no order status.");
                        }
                        Err(error) => {
                            submit_fail(&cloid, &order_manager, &symbol, &tx, error);
                        }
                    }
                }
                Err(error) => {
                    submit_fail(&cloid, &order_manager, &symbol, &tx, error);
                }
            }
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: crate::connector::PublishSender) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let assets = self.assets.clone();
        let private_key = self.private_key;
        let is_mainnet = self.config.is_mainnet;
        let nonce_counter = self.nonce_counter.clone();

        tokio::spawn(async move {
            let cloid = match order_manager
                .lock()
                .unwrap()
                .get_cloid(&symbol, order.order_id)
            {
                Some(cloid) => cloid,
                None => {
                    warn!(
                        order_id = order.order_id,
                        "cloid corresponding to order_id is not found; \
                        this may be due to the order already being canceled or filled."
                    );
                    return;
                }
            };
            let asset_index = match ensure_asset(&client, &assets, &symbol).await {
                Ok(info) => info.index,
                Err(error) => {
                    warn!(?error, %symbol, "Couldn't resolve the asset index; cancel skipped.");
                    return;
                }
            };

            let oid = order_manager
                .lock()
                .unwrap()
                .get_oid(&symbol, order.order_id);
            let nonce = next_nonce(&nonce_counter);

            let action = if let Some(oid) = oid {
                let action = CancelAction {
                    type_: "cancel".to_string(),
                    cancels: vec![CancelWire {
                        a: asset_index,
                        o: oid,
                    }],
                };
                CancelActionWire::Oid(action)
            } else {
                let action = CancelByCloidAction {
                    type_: "cancelByCloid".to_string(),
                    cancels: vec![CancelByCloidWire {
                        asset: asset_index,
                        cloid: cloid.clone(),
                    }],
                };
                CancelActionWire::Cloid(action)
            };

            let signature = match sign_l1_action(&action, &private_key, nonce, None, is_mainnet) {
                Ok(sig) => sig,
                Err(error) => {
                    if let Some(order) = order_manager.lock().unwrap().update_cancel_fail(&cloid) {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                            .unwrap();
                    }
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
                    return;
                }
            };

            let resp = client.post_exchange(&action, nonce, &signature).await;
            match resp {
                Ok(resp) => {
                    let statuses = parse_cancel_statuses(&resp);
                    match statuses {
                        Ok(Some(CancelStatus::Success(_))) => {
                            if let Ok(Some(order)) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_exchange_cancel(&cloid, true)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                        }
                        Ok(Some(CancelStatus::Error { error })) => {
                            if let Some(order) =
                                order_manager.lock().unwrap().update_cancel_fail(&cloid)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                HyperliquidError::OrderError(error).to_value(),
                            ))))
                            .unwrap();
                        }
                        Ok(None) => {
                            warn!("The exchange response contains no cancel status.");
                        }
                        Err(error) => {
                            if let Some(order) =
                                order_manager.lock().unwrap().update_cancel_fail(&cloid)
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
                Err(error) => {
                    if let Some(order) = order_manager.lock().unwrap().update_cancel_fail(&cloid) {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                            .unwrap();
                    }
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
                }
            }
        });
    }

    async fn shutdown(&self) -> Result<(), String> {
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

/// Hyperliquid rejects prices/sizes with trailing zeros (e.g. "0.00100"). Strips trailing zeros
/// and the decimal point from a fixed-decimal string, matching the official SDK's float_to_wire.
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

fn submit_fail(
    cloid: &String,
    order_manager: &SharedOrderManager,
    symbol: &str,
    tx: &crate::connector::PublishSender,
    error: HyperliquidError,
) {
    if let Some(order) = order_manager
        .lock()
        .unwrap()
        .update_from_exchange_submit(
            cloid,
            &OrderStatus::Error {
                error: error.to_string(),
            },
        )
        .unwrap_or(None)
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
mod e2e_tests {
    use super::*;
    use hftbacktest::types::{OrdType, TimeInForce};
    use std::time::Duration;

    fn testnet_config() -> String {
        let private_key = std::env::var("HYPERLIQUID_TEST_PRIVATE_KEY")
            .expect("HYPERLIQUID_TEST_PRIVATE_KEY must be set to run the e2e test");
        let account = std::env::var("HYPERLIQUID_TEST_ACCOUNT_ADDRESS").unwrap_or_default();
        format!(
            r#"info_url = "https://api.hyperliquid-testnet.xyz/info"
exchange_url = "https://api.hyperliquid-testnet.xyz/exchange"
ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
private_key = "{private_key}"
account_address = "{account}"
is_mainnet = false
"#
        )
    }

    async fn wait_order_event(
        rx: &mut crate::connector::PublishReceiver,
        timeout: Duration,
    ) -> Order {
        tokio::time::timeout(timeout, async {
            loop {
                match rx.recv().await {
                    Some(PublishEvent::LiveEvent(LiveEvent::Order { order, .. })) => return order,
                    Some(PublishEvent::LiveEvent(LiveEvent::Error(error))) => {
                        println!("error event: {error:?}");
                        continue;
                    }
                    Some(_) => continue,
                    None => panic!("event channel closed"),
                }
            }
        })
        .await
        .expect("timed out waiting for an order event")
    }

    /// Places a resting limit order far from the market and cancels it. Requires a funded
    /// Hyperliquid testnet account and the API wallet private key via environment variables.
    #[tokio::test]
    #[ignore = "requires a funded Hyperliquid testnet account and env vars"]
    async fn e2e_testnet_order_roundtrip() {
        let connector = Hyperliquid::build_from(&testnet_config()).unwrap();
        let (tx, mut rx) = crate::connector::publish_channel(64);

        // A resting buy GTC at 63,000 (well below the ~64,200 market), so it rests on the book.
        // Testnet BTC trades with a 1.0 tick; integer prices are always valid.
        let order = Order::new(
            9_990_001,
            63_000,
            1.0,
            0.001,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );

        connector.submit("BTC".to_string(), order.clone(), tx.clone());
        let submitted = wait_order_event(&mut rx, Duration::from_secs(30)).await;
        println!(
            "submit -> status={:?} req={:?} px={} qty={}",
            submitted.status,
            submitted.req,
            submitted.price(),
            submitted.qty
        );
        // The exchange error event (e.g. insufficient balance) is sent after the order event.
        if let Ok(Some(PublishEvent::LiveEvent(LiveEvent::Error(error)))) =
            tokio::time::timeout(Duration::from_secs(2), rx.recv()).await
        {
            println!("error event: {error:?}");
        }
        assert_eq!(
            submitted.status,
            Status::New,
            "GTC order should rest on the testnet book"
        );

        connector.cancel("BTC".to_string(), order.clone(), tx.clone());
        let canceled = wait_order_event(&mut rx, Duration::from_secs(30)).await;
        println!(
            "cancel -> status={:?} req={:?}",
            canceled.status, canceled.req
        );
        assert_eq!(canceled.status, Status::Canceled);
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
