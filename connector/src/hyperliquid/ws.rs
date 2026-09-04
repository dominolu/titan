use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use titan_market_plugin::MarketDataKind;
use tokio::{
    net::TcpStream,
    select,
    sync::broadcast::{Receiver, error::RecvError},
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{debug, error, warn};

use crate::{
    connector::{AccountPublication, MarketDataCommand, MarketStreamMetadata, PublishEvent},
    hyperliquid::{
        HyperliquidError, SharedAssets, SharedMarketSubscriptions, SharedSymbolSet,
        client::HyperliquidClient,
        msg::{
            BboData, CancelAction, CancelWire, L2BookData, OrderUpdate, Trade, UserEvent, WsMsg,
            WsSubscribe,
        },
        ordermanager::SharedOrderManager,
        signing::sign_l1_action,
    },
    utils::next_nonce,
};

/// Classifies an incoming WebSocket channel for message dispatch.
#[derive(Debug, PartialEq, Eq)]
enum MarketChannel {
    L2Book,
    Trades,
    OrderUpdates,
    User,
    Other,
}

fn classify_channel(channel: &str) -> MarketChannel {
    if channel.starts_with("l2Book") {
        MarketChannel::L2Book
    } else if channel.starts_with("trades") {
        MarketChannel::Trades
    } else if channel == "orderUpdates" {
        MarketChannel::OrderUpdates
    } else if channel == "user" {
        MarketChannel::User
    } else {
        MarketChannel::Other
    }
}

/// Hyperliquid marks trades with side "A" (ask/taker sell) or "B" (bid/taker buy).
fn trade_side_is_sell(side: &str) -> bool {
    side == "A"
}

/// Applies a fill to a locally tracked position: "B" increases it, everything else decreases it.
fn apply_fill(position: &mut f64, side: &str, sz: f64) {
    if side == "B" {
        *position += sz;
    } else {
        *position -= sz;
    }
}

pub struct HyperliquidWs {
    ev_tx: crate::connector::PublishSender,
    order_manager: SharedOrderManager,
    assets: SharedAssets,
    symbols: SharedSymbolSet,
    nonce_counter: Arc<Mutex<u64>>,
    positions: Arc<Mutex<HashMap<String, f64>>>,
    stream_epochs: HashMap<String, u64>,
    account_address: String,
    private_key: [u8; 32],
    is_mainnet: bool,
    client: HyperliquidClient,
    command_rx: Receiver<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
    private_channels: bool,
    pending_private_subscriptions: HashSet<String>,
}

impl HyperliquidWs {
    pub fn new(
        ev_tx: crate::connector::PublishSender,
        order_manager: SharedOrderManager,
        assets: SharedAssets,
        symbols: SharedSymbolSet,
        nonce_counter: Arc<Mutex<u64>>,
        account_address: String,
        private_key: [u8; 32],
        is_mainnet: bool,
        client: HyperliquidClient,
        command_rx: Receiver<MarketDataCommand>,
        market_subscriptions: SharedMarketSubscriptions,
        private_channels: bool,
    ) -> Self {
        Self {
            ev_tx,
            order_manager,
            assets,
            symbols,
            nonce_counter,
            positions: Default::default(),
            stream_epochs: Default::default(),
            account_address,
            private_key,
            is_mainnet,
            client,
            command_rx,
            market_subscriptions,
            private_channels,
            pending_private_subscriptions: HashSet::new(),
        }
    }

    fn reset_private_subscriptions(&mut self) {
        self.pending_private_subscriptions =
            HashSet::from(["orderUpdates".to_string(), "userEvents".to_string()]);
    }

    fn subscription_response_type(msg: &WsMsg) -> Option<&str> {
        msg.subscription
            .as_ref()
            .or_else(|| msg.data.as_ref()?.get("subscription"))?
            .get("type")?
            .as_str()
    }

    async fn handle_msg(&mut self, text: &str) -> Result<(), HyperliquidError> {
        let msg: WsMsg = serde_json::from_str(text)?;
        let channel = msg.channel.clone();
        if channel == "subscriptionResponse" {
            debug!(?msg, "subscription response");
            if self.private_channels {
                let ready = Self::subscription_response_type(&msg)
                    .is_some_and(|kind| self.pending_private_subscriptions.remove(kind))
                    && self.pending_private_subscriptions.is_empty();
                if ready {
                    self.ev_tx
                        .send(PublishEvent::PrivateStreamReady)
                        .map_err(|_| HyperliquidError::ConnectionInterrupted)?;
                }
            }
            return Ok(());
        }
        if channel == "pong" {
            return Ok(());
        }
        if channel == "error" {
            error!(?msg, "WebSocket error.");
            return Err(HyperliquidError::ConnectionInterrupted);
        }
        let Some(data) = msg.data.as_ref() else {
            debug!(%channel, "Message without data.");
            return Ok(());
        };
        match classify_channel(&channel) {
            MarketChannel::L2Book => self.handle_l2_book(data).await?,
            MarketChannel::Trades => self.handle_trades(data).await?,
            MarketChannel::OrderUpdates => self.handle_order_updates(data).await?,
            MarketChannel::User => self.handle_user_events(data).await?,
            MarketChannel::Other => {
                self.handle_extra(&channel, data).await?;
            }
        }
        Ok(())
    }

    /// 处理非引擎核心频道（allMids/bbo/candle/userFills/userFundings/activeAssetCtx/
    /// clearinghouseState/openOrders/notification/spotState/twapStates 等）。
    async fn handle_extra(
        &self,
        channel: &str,
        data: &serde_json::Value,
    ) -> Result<(), HyperliquidError> {
        match channel {
            "bbo" => {
                let bbo: BboData = serde_json::from_value(data.clone())?;
                let exch_ts = (bbo.time * 1_000_000) as i64;
                let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                if let Some(levels) = &bbo.bbo {
                    for lvl in &levels.bids {
                        self.ev_tx
                            .send(PublishEvent::FeedBatch {
                                symbol: bbo.coin.clone(),
                                events: vec![Event {
                                    ev: LOCAL_BID_DEPTH_BBO_EVENT,
                                    exch_ts,
                                    local_ts,
                                    order_id: 0,
                                    px: lvl.px.parse().unwrap_or(0.0),
                                    qty: lvl.sz.parse().unwrap_or(0.0),
                                    ival: 0,
                                    fval: 0.0,
                                }],
                                stream: None,
                            })
                            .unwrap();
                    }
                    for lvl in &levels.asks {
                        self.ev_tx
                            .send(PublishEvent::FeedBatch {
                                symbol: bbo.coin.clone(),
                                events: vec![Event {
                                    ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                                    exch_ts,
                                    local_ts,
                                    order_id: 0,
                                    px: lvl.px.parse().unwrap_or(0.0),
                                    qty: lvl.sz.parse().unwrap_or(0.0),
                                    ival: 0,
                                    fval: 0.0,
                                }],
                                stream: None,
                            })
                            .unwrap();
                    }
                }
            }
            "allMids"
            | "activeAssetCtx"
            | "candle"
            | "userFills"
            | "userNonFundingLedgerUpdates"
            | "clearinghouseState"
            | "openOrders"
            | "notification"
            | "spotState"
            | "twapStates"
            | "userTwapSliceFills"
            | "userTwapHistory"
            | "outcomeMetaUpdates"
            | "fastAssetCtxs"
            | "allDexsAssetCtxs"
            | "allDexsClearinghouseState" => {
                debug!(%channel, "Extra channel message received.");
            }
            _ => {
                debug!(%channel, "Unhandled channel.");
            }
        }
        Ok(())
    }

    async fn handle_l2_book(&mut self, data: &serde_json::Value) -> Result<(), HyperliquidError> {
        let book: L2BookData = serde_json::from_value(data.clone())?;
        let epoch = {
            let value = self.stream_epochs.entry(book.coin.clone()).or_insert(0);
            *value = value.saturating_add(1);
            *value
        };
        let exch_ts = (book.time * 1_000_000) as i64;
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();

        let mut events = Vec::new();
        // Hyperliquid l2Book pushes are complete book images; preserve the replacement boundary.
        if let Some(bids) = book.levels.first() {
            for level in bids {
                events.push(Event {
                    ev: LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                    exch_ts,
                    local_ts,
                    order_id: 0,
                    px: level.px.parse().unwrap_or(0.0),
                    qty: level.sz.parse().unwrap_or(0.0),
                    ival: 0,
                    fval: 0.0,
                });
            }
        }
        if let Some(asks) = book.levels.get(1) {
            for level in asks {
                events.push(Event {
                    ev: LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                    exch_ts,
                    local_ts,
                    order_id: 0,
                    px: level.px.parse().unwrap_or(0.0),
                    qty: level.sz.parse().unwrap_or(0.0),
                    ival: 0,
                    fval: 0.0,
                });
            }
        }
        self.ev_tx
            .send(PublishEvent::FeedBatch {
                symbol: book.coin,
                events,
                stream: Some(MarketStreamMetadata {
                    epoch,
                    first_update_sequence: 1,
                    last_update_sequence: 1,
                    snapshot: true,
                }),
            })
            .unwrap();
        Ok(())
    }

    async fn handle_trades(&mut self, data: &serde_json::Value) -> Result<(), HyperliquidError> {
        let trades: Vec<Trade> = match data {
            serde_json::Value::Array(arr) => arr
                .iter()
                .map(|v| serde_json::from_value(v.clone()))
                .collect::<Result<_, _>>()?,
            _ => vec![serde_json::from_value(data.clone())?],
        };
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        for trade in trades {
            self.ev_tx
                .send(PublishEvent::FeedBatch {
                    symbol: trade.coin.clone(),
                    events: vec![Event {
                        ev: if trade_side_is_sell(&trade.side) {
                            LOCAL_SELL_TRADE_EVENT
                        } else {
                            LOCAL_BUY_TRADE_EVENT
                        },
                        exch_ts: (trade.time * 1_000_000) as i64,
                        local_ts,
                        order_id: 0,
                        px: trade.px.parse().unwrap_or(0.0),
                        qty: trade.sz.parse().unwrap_or(0.0),
                        ival: 0,
                        fval: 0.0,
                    }],
                    stream: None,
                })
                .unwrap();
        }
        Ok(())
    }

    async fn handle_order_updates(
        &mut self,
        data: &serde_json::Value,
    ) -> Result<(), HyperliquidError> {
        let updates: Vec<OrderUpdate> = data
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|v| serde_json::from_value(v.clone()))
                    .collect::<Result<_, _>>()
            })
            .unwrap_or_else(|| Ok(vec![serde_json::from_value(data.clone())?]))?;
        for update in updates {
            let symbol = update.order.coin.clone();
            let mut order_manager = self.order_manager.lock().unwrap();
            match order_manager.update_from_ws(&update.order, &update.status) {
                Ok(Some(order)) => {
                    self.ev_tx
                        .send_account(AccountPublication::Order {
                            symbol,
                            client_order_id: update.order.cloid.clone(),
                            venue_order_id: Some(update.order.oid.to_string()),
                            order,
                        })
                        .unwrap();
                }
                Ok(None) => {}
                Err(error) => {
                    debug!(?error, "Couldn't update the order data.");
                }
            }
        }
        Ok(())
    }

    async fn handle_user_events(
        &mut self,
        data: &serde_json::Value,
    ) -> Result<(), HyperliquidError> {
        let events: Vec<UserEvent> = match data {
            serde_json::Value::Array(arr) => arr
                .iter()
                .map(|v| serde_json::from_value(v.clone()))
                .collect::<Result<_, _>>()?,
            _ => vec![serde_json::from_value(data.clone())?],
        };
        for event in events {
            if event.type_ != "fill" {
                continue;
            }
            if let Some(fill) = event.fill {
                let mut positions = self.positions.lock().unwrap();
                let position = positions.entry(fill.coin.clone()).or_insert(0.0);
                let sz: f64 = fill.sz.parse().unwrap_or(0.0);
                apply_fill(position, &fill.side, sz);
                let qty = *position;
                drop(positions);
                self.ev_tx
                    .send_account(AccountPublication::Position {
                        symbol: fill.coin.clone(),
                        qty,
                        exch_ts: (fill.time * 1_000_000) as i64,
                    })
                    .unwrap();
            }
        }
        Ok(())
    }

    /// Seeds the local position map from the REST clearinghouse state before the private
    /// `userEvents` stream starts, so fill events are accumulated on top of the real positions.
    async fn seed_positions(&self) -> Result<(), HyperliquidError> {
        let state = self
            .client
            .get_clearinghouse_state(&self.account_address)
            .await?;
        let mut positions = self.positions.lock().unwrap();
        for asset_position in state.asset_positions {
            let qty: f64 = asset_position.position.szi.parse().unwrap_or(0.0);
            positions.insert(asset_position.position.coin, qty);
        }
        Ok(())
    }

    async fn subscribe_symbol(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), HyperliquidError> {
        self.send_market_command(write, "subscribe", symbol, kinds)
            .await
    }

    async fn unsubscribe_symbol(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), HyperliquidError> {
        self.send_market_command(write, "unsubscribe", symbol, kinds)
            .await
    }

    async fn send_market_command(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        method: &str,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), HyperliquidError> {
        let mut channels = Vec::new();
        for kind in kinds {
            let channel = match kind {
                MarketDataKind::Depth | MarketDataKind::Bbo => "l2Book",
                MarketDataKind::Trades => "trades",
                _ => continue,
            };
            if !channels.contains(&channel) {
                channels.push(channel);
            }
        }
        for channel in channels {
            let request = WsSubscribe {
                method: method.to_string(),
                subscription: serde_json::json!({ "type": channel, "coin": symbol }),
            };
            write
                .send(Message::Text(serde_json::to_string(&request)?.into()))
                .await?;
        }
        Ok(())
    }

    async fn resubscribe_book(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
    ) -> Result<(), HyperliquidError> {
        for method in ["unsubscribe", "subscribe"] {
            self.send_market_command(write, method, symbol.clone(), &[MarketDataKind::Depth])
                .await?;
        }
        Ok(())
    }

    async fn init_symbol(&self, symbol: String) {
        let client = self.client.clone();
        let account_address = self.account_address.clone();
        let order_manager = self.order_manager.clone();
        let ev_tx = self.ev_tx.clone();
        let private_key = self.private_key;
        let is_mainnet = self.is_mainnet;
        let assets = self.assets.clone();
        let nonce_counter = self.nonce_counter.clone();
        let positions = self.positions.clone();

        tokio::spawn(async move {
            // Cancel all open orders for the symbol to start with a clean state.
            if let Err(error) = cancel_open_orders(
                client.clone(),
                account_address.clone(),
                symbol.clone(),
                private_key,
                is_mainnet,
                assets.clone(),
                nonce_counter,
                order_manager.clone(),
                ev_tx.clone(),
            )
            .await
            {
                error!(?error, %symbol, "Couldn't cancel open orders.");
            }

            // Fetches the initial position.
            if let Err(error) =
                get_position(client, account_address, symbol, ev_tx, positions, assets).await
            {
                error!(?error, "Couldn't get the position information.");
            }
        });
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), HyperliquidError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = time::interval(Duration::from_secs(30));
        let mut gc_interval = time::interval(Duration::from_secs(20));

        // Seed the local positions before any fill event can arrive.
        if self.private_channels {
            self.reset_private_subscriptions();
            if let Err(error) = self.seed_positions().await {
                error!(?error, "Couldn't seed the initial positions.");
            }
            for channel in ["orderUpdates", "userEvents"] {
                let subscribe = WsSubscribe {
                    method: "subscribe".to_string(),
                    subscription: serde_json::json!({
                        "type": channel,
                        "user": self.account_address.clone(),
                    }),
                };
                write
                    .send(Message::Text(serde_json::to_string(&subscribe)?.into()))
                    .await?;
            }
        }

        // Replays every registered symbol after (re)connect. The broadcast receiver only delivers
        // symbols registered after subscription, so the shared symbol set is the durable source.
        let subscriptions: Vec<_> = self
            .market_subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|(symbol, kinds)| (symbol.clone(), kinds.iter().copied().collect::<Vec<_>>()))
            .collect();
        for (symbol, kinds) in subscriptions {
            self.subscribe_symbol(&mut write, symbol, &kinds).await?;
        }
        if self.private_channels {
            let trading_symbols: Vec<_> = self.symbols.lock().unwrap().iter().cloned().collect();
            for symbol in trading_symbols {
                self.init_symbol(symbol).await;
            }
        }

        loop {
            select! {
                _ = interval.tick() => {
                    let s = "{\"method\":\"ping\"}".to_string();
                    write.send(Message::Text(s.into())).await?;
                }
                _ = gc_interval.tick() => {
                    self.order_manager.lock().unwrap().gc();
                }
                msg = self.command_rx.recv() => match msg {
                    Ok(MarketDataCommand::Subscribe { symbol, kinds }) => self.subscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => self.unsubscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
                        let _ = self.ev_tx.send(PublishEvent::StreamInvalidated {
                            epoch: self.stream_epochs.get(&symbol).copied().unwrap_or(0),
                            symbol: symbol.clone(),
                        });
                        self.resubscribe_book(&mut write, symbol).await?;
                    }
                    Ok(MarketDataCommand::InitializeTrading { symbol }) if self.private_channels => self.init_symbol(symbol).await,
                    Ok(MarketDataCommand::InitializeTrading { .. }) => {}
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_msg(&text).await?;
                        }
                        Some(Ok(Message::Ping(_))) => {
                            write.send(Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(HyperliquidError::ConnectionAbort(
                                close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                            ));
                        }
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(error)) => {
                            return Err(HyperliquidError::from(error));
                        }
                        None => {
                            return Err(HyperliquidError::ConnectionInterrupted);
                        }
                    }
                }
            }
        }
    }
}

async fn cancel_open_orders(
    client: HyperliquidClient,
    account_address: String,
    symbol: String,
    private_key: [u8; 32],
    is_mainnet: bool,
    assets: SharedAssets,
    nonce_counter: Arc<Mutex<u64>>,
    order_manager: SharedOrderManager,
    ev_tx: crate::connector::PublishSender,
) -> Result<(), HyperliquidError> {
    if assets.lock().unwrap().is_empty() {
        super::ensure_assets(&client, &assets).await?;
    }
    let open_orders = client.get_open_orders(&account_address).await?;
    let mut cancels = Vec::new();
    for order in open_orders {
        if order.coin == symbol {
            match assets
                .lock()
                .unwrap()
                .get(&order.coin)
                .map(|info| info.index)
            {
                Some(asset_index) => cancels.push(CancelWire {
                    a: asset_index,
                    o: order.oid,
                }),
                None => {
                    warn!(coin = %order.coin, "Unknown asset; skipping its open-order cancel.");
                }
            }
        }
    }
    if !cancels.is_empty() {
        let action = CancelAction {
            type_: "cancel".to_string(),
            cancels,
        };
        let nonce = next_nonce(&nonce_counter);
        let signature = sign_l1_action(&action, &private_key, nonce, None, is_mainnet)?;
        let _resp = client.post_exchange(&action, nonce, &signature).await?;
    }
    let orders = order_manager.lock().unwrap().cancel_all(&symbol);
    for order in orders {
        ev_tx
            .send_account(AccountPublication::Order {
                symbol: symbol.clone(),
                client_order_id: None,
                venue_order_id: None,
                order,
            })
            .unwrap();
    }
    Ok(())
}

async fn get_position(
    client: HyperliquidClient,
    account_address: String,
    symbol: String,
    ev_tx: crate::connector::PublishSender,
    positions: Arc<Mutex<HashMap<String, f64>>>,
    _assets: SharedAssets,
) -> Result<(), HyperliquidError> {
    let state = client.get_clearinghouse_state(&account_address).await?;
    for asset_position in state.asset_positions {
        let position = asset_position.position;
        if position.coin != symbol {
            continue;
        }
        let qty: f64 = position.szi.parse().unwrap_or(0.0);
        positions.lock().unwrap().insert(symbol.clone(), qty);
        ev_tx
            .send_account(AccountPublication::Position {
                symbol: position.coin,
                qty,
                exch_ts: (position.update_time * 1_000_000) as i64,
            })
            .unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperliquid::msg::WsUserFundings;

    #[tokio::test]
    async fn l2_book_images_are_snapshots_with_monotonic_epochs() {
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut ws = HyperliquidWs::new(
            events,
            Arc::new(Mutex::new(
                crate::hyperliquid::ordermanager::OrderManager::default(),
            )),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::from(["BTC".to_owned()]))),
            Arc::new(Mutex::new(0)),
            String::new(),
            [0; 32],
            false,
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Arc::new(Mutex::new(HashMap::new())),
            false,
        );
        let image = serde_json::json!({
            "coin": "BTC",
            "time": 1,
            "levels": [
                [{"px": "100", "sz": "2", "n": 1}],
                [{"px": "101", "sz": "3", "n": 1}]
            ]
        });

        for expected_epoch in 1..=2 {
            ws.handle_l2_book(&image).await.unwrap();
            match receiver.recv().await.unwrap() {
                PublishEvent::FeedBatch {
                    symbol,
                    events,
                    stream: Some(stream),
                } => {
                    assert_eq!(symbol, "BTC");
                    assert_eq!(events.len(), 2);
                    assert!(stream.snapshot);
                    assert_eq!(stream.epoch, expected_epoch);
                    assert_eq!(stream.first_update_sequence, 1);
                    assert_eq!(stream.last_update_sequence, 1);
                }
                _ => panic!("expected Hyperliquid depth snapshot"),
            }
        }
    }

    #[tokio::test]
    async fn private_ready_ignores_public_and_duplicate_subscription_responses() {
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut ws = HyperliquidWs::new(
            events,
            Arc::new(Mutex::new(
                crate::hyperliquid::ordermanager::OrderManager::default(),
            )),
            Default::default(),
            Default::default(),
            Default::default(),
            String::new(),
            [0; 32],
            false,
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Default::default(),
            true,
        );
        ws.reset_private_subscriptions();

        for kind in ["l2Book", "orderUpdates", "orderUpdates"] {
            let message = serde_json::json!({
                "channel": "subscriptionResponse",
                "data": {"subscription": {"type": kind}}
            });
            ws.handle_msg(&message.to_string()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(1), receiver.recv())
                    .await
                    .is_err()
            );
        }

        let final_ack = serde_json::json!({
            "channel": "subscriptionResponse",
            "data": {"subscription": {"type": "userEvents"}}
        });
        ws.handle_msg(&final_ack.to_string()).await.unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap(),
            PublishEvent::PrivateStreamReady
        ));

        ws.handle_msg(&final_ack.to_string()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn websocket_error_invalidates_the_connection() {
        let (events, _receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut ws = HyperliquidWs::new(
            events,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            String::new(),
            [0; 32],
            false,
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Default::default(),
            true,
        );
        let error = ws
            .handle_msg(r#"{"channel":"error","error":"bad subscription"}"#)
            .await
            .unwrap_err();
        assert!(matches!(error, HyperliquidError::ConnectionInterrupted));
    }

    #[test]
    fn test_classify_channel() {
        assert_eq!(classify_channel("l2Book"), MarketChannel::L2Book);
        assert_eq!(classify_channel("l2Book:btc"), MarketChannel::L2Book);
        assert_eq!(classify_channel("trades"), MarketChannel::Trades);
        assert_eq!(classify_channel("trades:btc"), MarketChannel::Trades);
        assert_eq!(
            classify_channel("orderUpdates"),
            MarketChannel::OrderUpdates
        );
        assert_eq!(classify_channel("user"), MarketChannel::User);
        assert_eq!(
            classify_channel("subscriptionResponse"),
            MarketChannel::Other
        );
        assert_eq!(classify_channel("pong"), MarketChannel::Other);
    }

    #[test]
    fn test_trade_side_is_sell() {
        assert!(trade_side_is_sell("A"));
        assert!(!trade_side_is_sell("B"));
        assert!(!trade_side_is_sell(""));
    }

    #[test]
    fn test_apply_fill() {
        let mut position = 0.5;
        apply_fill(&mut position, "B", 0.1);
        assert_eq!(position, 0.6);
        apply_fill(&mut position, "A", 0.25);
        assert_eq!(position, 0.35);
        // Unknown sides are treated as sells (conservative for closing positions).
        apply_fill(&mut position, "?", 0.1);
        assert!((position - 0.25).abs() < 1e-9);
    }

    #[test]
    fn test_parse_user_fundings_snapshot() {
        // 官方 WsUserFundings 格式：快照 + 每小时结算推送
        let json = r#"{
            "user": "0x1234",
            "isSnapshot": true,
            "fundings": [
                {"time": 1700000000000, "coin": "BTC", "usdc": "1.234", "szi": "0.5", "fundingRate": "0.00005"},
                {"time": 1700003600000, "coin": "BTC", "usdc": "1.250", "szi": "0.5", "fundingRate": "0.000051"},
                {"time": 1700000000000, "coin": "ETH", "usdc": "0.5", "szi": "5.0", "fundingRate": "0.00001"}
            ]
        }"#;
        let msg: WsUserFundings = serde_json::from_str(json).unwrap();
        assert!(msg.is_snapshot);
        assert_eq!(msg.fundings.len(), 3);
        assert_eq!(msg.fundings[0].coin, "BTC");
        assert_eq!(msg.fundings[0].funding_rate, "0.00005");
        assert_eq!(msg.fundings[0].time, 1_700_000_000_000);
    }

    #[test]
    fn test_parse_user_fundings_streaming() {
        let json = r#"{
            "user": "0x1234",
            "isSnapshot": false,
            "fundings": [
                {"time": 1700007200000, "coin": "BTC", "usdc": "1.3", "szi": "0.5", "fundingRate": "0.000052"}
            ]
        }"#;
        let msg: WsUserFundings = serde_json::from_str(json).unwrap();
        assert!(!msg.is_snapshot);
        assert_eq!(msg.fundings[0].funding_rate, "0.000052");
    }
}
