#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};
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
        msg::{BboData, Fill, L2BookData, OrderUpdate, Trade, UserEvent, WsMsg, WsSubscribe},
        ordermanager::SharedOrderManager,
    },
};

const COMPOSITE_L2_MAX_AGE_MS: u64 = 10_000;

/// Classifies an incoming WebSocket channel for message dispatch.
#[derive(Debug, PartialEq, Eq)]
enum MarketChannel {
    L2Book,
    Bbo,
    Trades,
    OrderUpdates,
    User,
    Other,
}

fn classify_channel(channel: &str) -> MarketChannel {
    if channel.starts_with("l2Book") {
        MarketChannel::L2Book
    } else if channel == "bbo" {
        MarketChannel::Bbo
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

fn position_after_fill(current: f64, fill: &Fill) -> f64 {
    // Hyperliquid supplies the absolute position immediately before every fill. Deriving from
    // startPosition makes replay after reconnect idempotent; accumulating onto the local cache
    // would apply a replayed fill twice and can reverse a freshly closed hedge.
    let start = fill.start_position.parse::<f64>().unwrap_or(current);
    let mut position = start;
    apply_fill(&mut position, &fill.side, fill.sz.parse().unwrap_or(0.0));
    position
}

fn market_channels(kinds: &[MarketDataKind]) -> Vec<&'static str> {
    let mut channels = Vec::new();
    for kind in kinds {
        let required = match kind {
            MarketDataKind::Depth => &["l2Book", "bbo"][..],
            MarketDataKind::Bbo => &["bbo"][..],
            MarketDataKind::Trades => &["trades"][..],
            _ => &[],
        };
        for channel in required {
            if !channels.contains(channel) {
                channels.push(*channel);
            }
        }
    }
    channels
}

fn unsubscribe_channels(
    removed: &[MarketDataKind],
    remaining: &HashSet<MarketDataKind>,
) -> Vec<&'static str> {
    market_channels(removed)
        .into_iter()
        .filter(|channel| {
            !market_channels(&remaining.iter().copied().collect::<Vec<_>>()).contains(channel)
        })
        .collect()
}

fn valid_level(px: &str, sz: &str) -> Option<(f64, f64)> {
    let px = px.parse::<f64>().ok()?;
    let qty = sz.parse::<f64>().ok()?;
    (px.is_finite() && qty.is_finite() && px > 0.0 && qty > 0.0).then_some((px, qty))
}

fn depth_event(ev: u64, exch_ts: i64, local_ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev,
        exch_ts,
        local_ts,
        order_id: 0,
        px,
        qty,
        ival: 0,
        fval: 0.0,
    }
}

/// Builds a full replacement image. A fresh BBO replaces the first level and trims any L2
/// levels that would conflict with it. Old L2 tails are discarded rather than advertised as
/// executable liquidity.
fn composite_depth_events(
    l2: Option<&L2BookData>,
    bbo: Option<&BboData>,
    local_ts: i64,
) -> Vec<Event> {
    let bbo_pair = bbo.and_then(|book| {
        let bid = book
            .bbo
            .first()?
            .as_ref()
            .and_then(|level| valid_level(&level.px, &level.sz))?;
        let ask = book
            .bbo
            .get(1)?
            .as_ref()
            .and_then(|level| valid_level(&level.px, &level.sz))?;
        (bid.0 < ask.0).then_some((book.time, bid, ask))
    });

    if let Some((bbo_time, bid, ask)) = bbo_pair
        && l2.is_none_or(|book| bbo_time >= book.time)
    {
        let exch_ts = (bbo_time * 1_000_000) as i64;
        let mut events = vec![depth_event(
            LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
            exch_ts,
            local_ts,
            bid.0,
            bid.1,
        )];
        if let Some(book) = l2
            && bbo_time.saturating_sub(book.time) <= COMPOSITE_L2_MAX_AGE_MS
        {
            if let Some(bids) = book.levels.first() {
                events.extend(bids.iter().filter_map(|level| {
                    let (px, qty) = valid_level(&level.px, &level.sz)?;
                    (px < bid.0).then(|| {
                        depth_event(LOCAL_BID_DEPTH_SNAPSHOT_EVENT, exch_ts, local_ts, px, qty)
                    })
                }));
            }
        }
        events.push(depth_event(
            LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
            exch_ts,
            local_ts,
            ask.0,
            ask.1,
        ));
        if let Some(book) = l2
            && bbo_time.saturating_sub(book.time) <= COMPOSITE_L2_MAX_AGE_MS
            && let Some(asks) = book.levels.get(1)
        {
            events.extend(asks.iter().filter_map(|level| {
                let (px, qty) = valid_level(&level.px, &level.sz)?;
                (px > ask.0).then(|| {
                    depth_event(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, exch_ts, local_ts, px, qty)
                })
            }));
        }
        return events;
    }

    let Some(book) = l2 else {
        return Vec::new();
    };
    let exch_ts = (book.time * 1_000_000) as i64;
    let mut events = Vec::new();
    if let Some(bids) = book.levels.first() {
        events.extend(bids.iter().filter_map(|level| {
            let (px, qty) = valid_level(&level.px, &level.sz)?;
            Some(depth_event(
                LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                exch_ts,
                local_ts,
                px,
                qty,
            ))
        }));
    }
    if let Some(asks) = book.levels.get(1) {
        events.extend(asks.iter().filter_map(|level| {
            let (px, qty) = valid_level(&level.px, &level.sz)?;
            Some(depth_event(
                LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts,
                local_ts,
                px,
                qty,
            ))
        }));
    }
    events
}

pub struct HyperliquidWs {
    ev_tx: crate::connector::PublishSender,
    order_manager: SharedOrderManager,
    assets: SharedAssets,
    symbols: SharedSymbolSet,
    positions: Arc<Mutex<HashMap<String, f64>>>,
    stream_epochs: HashMap<String, u64>,
    l2_books: HashMap<String, L2BookData>,
    bbo_books: HashMap<String, BboData>,
    wire_channels: HashMap<String, HashSet<&'static str>>,
    account_address: String,
    client: HyperliquidClient,
    command_rx: Receiver<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
    private_channels: bool,
    pending_private_subscriptions: HashSet<String>,
    #[cfg(test)]
    reconnect_fault: Arc<AtomicU8>,
}

impl HyperliquidWs {
    pub fn new(
        ev_tx: crate::connector::PublishSender,
        order_manager: SharedOrderManager,
        assets: SharedAssets,
        symbols: SharedSymbolSet,
        account_address: String,
        client: HyperliquidClient,
        command_rx: Receiver<MarketDataCommand>,
        market_subscriptions: SharedMarketSubscriptions,
        private_channels: bool,
        #[cfg(test)] reconnect_fault: Arc<AtomicU8>,
    ) -> Self {
        Self {
            ev_tx,
            order_manager,
            assets,
            symbols,
            positions: Default::default(),
            stream_epochs: Default::default(),
            l2_books: Default::default(),
            bbo_books: Default::default(),
            wire_channels: Default::default(),
            account_address,
            client,
            command_rx,
            market_subscriptions,
            private_channels,
            pending_private_subscriptions: HashSet::new(),
            #[cfg(test)]
            reconnect_fault,
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
        let msg: WsMsg = serde_json::from_str(text).map_err(|error| {
            warn!(%error, text, "Unparseable websocket message.");
            HyperliquidError::OrderError("unparseable websocket message".to_string())
        })?;
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
                    #[cfg(test)]
                    if self
                        .reconnect_fault
                        .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Err(HyperliquidError::ConnectionInterrupted);
                    }
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
            MarketChannel::Bbo => self.handle_bbo(data).await?,
            MarketChannel::Trades => self.handle_trades(data).await?,
            MarketChannel::OrderUpdates => self.handle_order_updates(data).await?,
            MarketChannel::User => self.handle_user_events(data).await?,
            MarketChannel::Other => {
                self.handle_extra(&channel, data).await?;
            }
        }
        Ok(())
    }

    fn depth_subscribed(&self, symbol: &str) -> bool {
        self.market_subscriptions
            .lock()
            .unwrap()
            .get(symbol)
            .is_some_and(|kinds| kinds.contains(&MarketDataKind::Depth))
    }

    fn bbo_subscribed(&self, symbol: &str) -> bool {
        self.market_subscriptions
            .lock()
            .unwrap()
            .get(symbol)
            .is_some_and(|kinds| kinds.contains(&MarketDataKind::Bbo))
    }

    fn clear_inactive_caches(&mut self, symbol: &str) {
        let active = self
            .market_subscriptions
            .lock()
            .unwrap()
            .get(symbol)
            .cloned()
            .unwrap_or_default();
        if !active.contains(&MarketDataKind::Depth) {
            self.l2_books.remove(symbol);
        }
        if !active.contains(&MarketDataKind::Depth) && !active.contains(&MarketDataKind::Bbo) {
            self.bbo_books.remove(symbol);
        }
    }

    fn publish_composite_depth(&mut self, symbol: &str) -> Result<(), HyperliquidError> {
        if !self.depth_subscribed(symbol) {
            return Ok(());
        }
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        let events = composite_depth_events(
            self.l2_books.get(symbol),
            self.bbo_books.get(symbol),
            local_ts,
        );
        if events.is_empty() {
            return Ok(());
        }
        let epoch = {
            let value = self.stream_epochs.entry(symbol.to_owned()).or_insert(0);
            *value = value.saturating_add(1);
            *value
        };
        self.ev_tx
            .send(PublishEvent::FeedBatch {
                symbol: symbol.to_owned(),
                events,
                stream: Some(MarketStreamMetadata {
                    epoch,
                    first_update_sequence: 1,
                    last_update_sequence: 1,
                    snapshot: true,
                }),
            })
            .map_err(|_| HyperliquidError::ConnectionInterrupted)
    }

    async fn handle_bbo(&mut self, data: &serde_json::Value) -> Result<(), HyperliquidError> {
        let bbo: BboData = serde_json::from_value(data.clone())?;
        let exch_ts = (bbo.time * 1_000_000) as i64;
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        let mut events = Vec::with_capacity(2);
        if let Some(Some(level)) = bbo.bbo.first() {
            events.push(Event {
                ev: LOCAL_BID_DEPTH_BBO_EVENT,
                exch_ts,
                local_ts,
                order_id: 0,
                px: level.px.parse().unwrap_or(0.0),
                qty: level.sz.parse().unwrap_or(0.0),
                ival: 0,
                fval: 0.0,
            });
        }
        if let Some(Some(level)) = bbo.bbo.get(1) {
            events.push(Event {
                ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                exch_ts,
                local_ts,
                order_id: 0,
                px: level.px.parse().unwrap_or(0.0),
                qty: level.sz.parse().unwrap_or(0.0),
                ival: 0,
                fval: 0.0,
            });
        }
        if self.bbo_subscribed(&bbo.coin) && !events.is_empty() {
            self.ev_tx
                .send(PublishEvent::FeedBatch {
                    symbol: bbo.coin.clone(),
                    events,
                    stream: None,
                })
                .map_err(|_| HyperliquidError::ConnectionInterrupted)?;
        }
        let symbol = bbo.coin.clone();
        self.bbo_books.insert(symbol.clone(), bbo);
        self.publish_composite_depth(&symbol)
    }

    /// 处理非引擎核心频道（allMids/candle/userFills/userFundings/activeAssetCtx/
    /// clearinghouseState/openOrders/notification/spotState/twapStates 等）。
    async fn handle_extra(
        &self,
        channel: &str,
        _data: &serde_json::Value,
    ) -> Result<(), HyperliquidError> {
        match channel {
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
        let symbol = book.coin.clone();
        self.l2_books.insert(symbol.clone(), book);
        self.publish_composite_depth(&symbol)
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
            match order_manager.update_from_ws(
                &update.order,
                &update.status,
                update.status_timestamp,
            ) {
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
            if let Some(fills) = event.fills {
                for fill in fills {
                    let mut positions = self.positions.lock().unwrap();
                    let position = positions.entry(fill.coin.clone()).or_insert(0.0);
                    *position = position_after_fill(*position, &fill);
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
        &mut self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), HyperliquidError> {
        let channels: Vec<_> = market_channels(kinds)
            .into_iter()
            .filter(|channel| {
                !self
                    .wire_channels
                    .get(&symbol)
                    .is_some_and(|active| active.contains(channel))
            })
            .collect();
        Self::send_channels(write, "subscribe", symbol.clone(), channels.clone()).await?;
        self.wire_channels
            .entry(symbol)
            .or_default()
            .extend(channels);
        Ok(())
    }

    async fn unsubscribe_symbol(
        &mut self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), HyperliquidError> {
        let remaining = self
            .market_subscriptions
            .lock()
            .unwrap()
            .get(&symbol)
            .cloned()
            .unwrap_or_default();
        let channels: Vec<_> = unsubscribe_channels(kinds, &remaining)
            .into_iter()
            .filter(|channel| {
                self.wire_channels
                    .get(&symbol)
                    .is_some_and(|active| active.contains(channel))
            })
            .collect();
        Self::send_channels(write, "unsubscribe", symbol.clone(), channels.clone()).await?;
        if let Some(active) = self.wire_channels.get_mut(&symbol) {
            for channel in channels {
                active.remove(channel);
            }
            if active.is_empty() {
                self.wire_channels.remove(&symbol);
            }
        }
        Ok(())
    }

    async fn send_channels(
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        method: &str,
        symbol: String,
        channels: Vec<&str>,
    ) -> Result<(), HyperliquidError> {
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
        &mut self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
    ) -> Result<(), HyperliquidError> {
        for method in ["unsubscribe", "subscribe"] {
            Self::send_channels(write, method, symbol.clone(), vec!["l2Book", "bbo"]).await?;
        }
        Ok(())
    }

    async fn init_symbol(&self, symbol: String) {
        let client = self.client.clone();
        let account_address = self.account_address.clone();
        let ev_tx = self.ev_tx.clone();
        let assets = self.assets.clone();
        let positions = self.positions.clone();

        tokio::spawn(async move {
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
        self.wire_channels.clear();
        self.l2_books.clear();
        self.bbo_books.clear();

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
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => {
                        self.unsubscribe_symbol(&mut write, symbol.clone(), &kinds).await?;
                        self.clear_inactive_caches(&symbol);
                    }
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
                        let _ = self.ev_tx.send(PublishEvent::StreamInvalidated {
                            epoch: self.stream_epochs.get(&symbol).copied().unwrap_or(0),
                            symbol: symbol.clone(),
                        });
                        self.l2_books.remove(&symbol);
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

    #[test]
    fn market_kinds_use_distinct_hyperliquid_channels() {
        assert_eq!(
            market_channels(&[MarketDataKind::Depth]),
            vec!["l2Book", "bbo"]
        );
        assert_eq!(market_channels(&[MarketDataKind::Bbo]), vec!["bbo"]);
        assert_eq!(
            market_channels(&[
                MarketDataKind::Depth,
                MarketDataKind::Bbo,
                MarketDataKind::Trades,
                MarketDataKind::Bbo,
            ]),
            vec!["l2Book", "bbo", "trades"]
        );
    }

    #[test]
    fn unsubscribe_preserves_channels_shared_by_remaining_kinds() {
        assert_eq!(
            unsubscribe_channels(
                &[MarketDataKind::Depth],
                &HashSet::from([MarketDataKind::Bbo]),
            ),
            vec!["l2Book"]
        );
        assert_eq!(
            unsubscribe_channels(&[MarketDataKind::Depth], &HashSet::new()),
            vec!["l2Book", "bbo"]
        );
    }

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
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Arc::new(Mutex::new(HashMap::from([(
                "BTC".to_owned(),
                HashSet::from([MarketDataKind::Depth]),
            )]))),
            false,
            Arc::new(AtomicU8::new(0)),
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
    async fn bbo_array_is_published_as_one_atomic_two_sided_batch() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut ws = HyperliquidWs::new(
            events,
            Default::default(),
            Default::default(),
            Default::default(),
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Arc::new(Mutex::new(HashMap::from([(
                "BTC".to_owned(),
                HashSet::from([MarketDataKind::Bbo]),
            )]))),
            false,
            Arc::new(AtomicU8::new(0)),
        );
        let message = serde_json::json!({
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1_700_000_000_000_u64,
                "bbo": [
                    {"px": "50000.0", "sz": "1.5", "n": 2},
                    {"px": "50001.0", "sz": "2.0", "n": 3}
                ]
            }
        });

        ws.handle_msg(&message.to_string()).await.unwrap();
        match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                symbol,
                events,
                stream,
            } => {
                assert_eq!(symbol, "BTC");
                assert!(stream.is_none());
                assert_eq!(events.len(), 2);
                assert!(events[0].is(LOCAL_BID_DEPTH_BBO_EVENT));
                assert!(events[1].is(LOCAL_ASK_DEPTH_BBO_EVENT));
                assert_eq!(events[0].px, 50_000.0);
                assert_eq!(events[1].px, 50_001.0);
            }
            _ => panic!("expected atomic Hyperliquid BBO batch"),
        }
    }

    #[tokio::test]
    async fn depth_subscription_merges_fast_bbo_with_fresh_l2_tails() {
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let subscriptions = Arc::new(Mutex::new(HashMap::from([(
            "BTC".to_owned(),
            HashSet::from([MarketDataKind::Depth]),
        )])));
        let mut ws = HyperliquidWs::new(
            events,
            Default::default(),
            Default::default(),
            Default::default(),
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            subscriptions,
            false,
            Arc::new(AtomicU8::new(0)),
        );
        let l2 = serde_json::json!({
            "coin": "BTC",
            "time": 1_700_000_000_000_u64,
            "levels": [
                [
                    {"px": "100", "sz": "2", "n": 1},
                    {"px": "99", "sz": "3", "n": 1}
                ],
                [
                    {"px": "101", "sz": "4", "n": 1},
                    {"px": "102", "sz": "5", "n": 1}
                ]
            ]
        });
        ws.handle_l2_book(&l2).await.unwrap();
        let _initial_l2 = receiver.recv().await.unwrap();

        let bbo = serde_json::json!({
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1_700_000_000_001_u64,
                "bbo": [
                    {"px": "100.5", "sz": "1.5", "n": 1},
                    {"px": "100.8", "sz": "2.5", "n": 1}
                ]
            }
        });
        ws.handle_msg(&bbo.to_string()).await.unwrap();

        match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                symbol,
                events,
                stream: Some(stream),
            } => {
                assert_eq!(symbol, "BTC");
                assert!(stream.snapshot);
                assert_eq!(stream.epoch, 2);
                assert_eq!(
                    events.iter().map(|event| event.px).collect::<Vec<_>>(),
                    vec![100.5, 100.0, 99.0, 100.8, 101.0, 102.0]
                );
                assert!(
                    events[..3]
                        .iter()
                        .all(|event| event.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT))
                );
                assert!(
                    events[3..]
                        .iter()
                        .all(|event| event.is(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT))
                );
            }
            _ => panic!("expected merged Hyperliquid depth snapshot"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(1), receiver.recv())
                .await
                .is_err(),
            "an internal BBO subscription must not leak a separate BBO batch"
        );
    }

    #[test]
    fn stale_l2_tails_are_not_merged_into_a_newer_bbo() {
        let l2: L2BookData = serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "time": 1_u64,
            "levels": [
                [{"px": "100", "sz": "2", "n": 1}],
                [{"px": "101", "sz": "3", "n": 1}]
            ]
        }))
        .unwrap();
        let bbo: BboData = serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "time": 1_u64 + COMPOSITE_L2_MAX_AGE_MS + 1,
            "bbo": [
                {"px": "100.5", "sz": "1", "n": 1},
                {"px": "100.8", "sz": "1", "n": 1}
            ]
        }))
        .unwrap();

        let events = composite_depth_events(Some(&l2), Some(&bbo), 123);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].px, 100.5);
        assert_eq!(events[1].px, 100.8);
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
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Default::default(),
            true,
            Arc::new(AtomicU8::new(0)),
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
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Default::default(),
            true,
            Arc::new(AtomicU8::new(0)),
        );
        let error = ws
            .handle_msg(r#"{"channel":"error","error":"bad subscription"}"#)
            .await
            .unwrap_err();
        assert!(matches!(error, HyperliquidError::ConnectionInterrupted));
    }

    #[tokio::test]
    async fn websocket_unparseable_message_is_reported_as_connection_error() {
        let (events, _receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut ws = HyperliquidWs::new(
            events,
            Default::default(),
            Default::default(),
            Default::default(),
            String::new(),
            HyperliquidClient::new("http://localhost", "http://localhost"),
            command_rx,
            Default::default(),
            true,
            Arc::new(AtomicU8::new(0)),
        );
        let error = ws.handle_msg("not-json").await.unwrap_err();
        match error {
            HyperliquidError::OrderError(msg) => {
                assert_eq!(msg, "unparseable websocket message");
            }
            _ => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn test_classify_channel() {
        assert_eq!(classify_channel("l2Book"), MarketChannel::L2Book);
        assert_eq!(classify_channel("l2Book:btc"), MarketChannel::L2Book);
        assert_eq!(classify_channel("bbo"), MarketChannel::Bbo);
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
    fn replayed_user_fill_keeps_the_same_absolute_position() {
        let fill = Fill {
            coin: "BTC".to_string(),
            px: "79000".to_string(),
            sz: "0.0002".to_string(),
            side: "B".to_string(),
            time: 1,
            start_position: "-0.0002".to_string(),
        };
        let first = position_after_fill(-0.0002, &fill);
        let replay = position_after_fill(first, &fill);
        assert_eq!(first, 0.0);
        assert_eq!(replay, 0.0);
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
