use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::*;
use serde_json::Value;
use titan_market_plugin::MarketDataKind;
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{Receiver as QueueReceiver, Sender as QueueSender, channel},
    },
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::info;
use tracing::{debug, error, warn};

use crate::{
    binancefutures::{
        BinanceFuturesError, SharedMarketSubscriptions,
        msg::{
            rest, stream,
            stream::{EventStream, Stream},
        },
        rest::BinanceFuturesClient,
    },
    connector::{
        MarketDataCommand, MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch, PublishEvent,
    },
    utils::parse_depth,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketStreamRoute {
    /// Legacy and test endpoints that still accept every stream on one connection.
    All,
    /// Binance's high-frequency public route (depth, trade and book ticker).
    Public,
    /// Binance's regular market route (mark price and funding rate).
    Market,
}

impl MarketStreamRoute {
    fn includes(self, kind: MarketDataKind) -> bool {
        match self {
            Self::All => true,
            Self::Public => matches!(
                kind,
                MarketDataKind::Depth
                    | MarketDataKind::Trades
                    | MarketDataKind::Bbo
                    | MarketDataKind::Ticker
            ),
            Self::Market => {
                matches!(
                    kind,
                    MarketDataKind::MarkPrice | MarketDataKind::FundingRate
                )
            }
        }
    }
}

pub struct MarketDataStream {
    client: BinanceFuturesClient,
    ev_tx: crate::connector::PublishSender,
    command_rx: Receiver<MarketDataCommand>,
    subscriptions: SharedMarketSubscriptions,
    pending_depth_messages: HashMap<String, Vec<(stream::OwnedDepth, i64)>>,
    prev_u: HashMap<String, i64>,
    stream_epochs: HashMap<String, u64>,
    canonical_symbols: HashMap<String, String>,
    rest_tx: QueueSender<(String, rest::Depth)>,
    rest_rx: QueueReceiver<(String, rest::Depth)>,
    route: MarketStreamRoute,
}

#[derive(Clone, Copy, Debug)]
enum MarkPriceStreamMode {
    MarkPriceWith1s,
    MarkPrice,
    Both,
}

impl MarkPriceStreamMode {
    fn from_env() -> Self {
        match std::env::var("BINANCE_FUTURES_MARK_PRICE_STREAM_FORM")
            .unwrap_or_else(|_| "1s".to_string())
            .as_str()
        {
            "markPrice" => Self::MarkPrice,
            "both" => Self::Both,
            "1s" | "markPrice@1s" => Self::MarkPriceWith1s,
            _ => Self::MarkPriceWith1s,
        }
    }

    fn stream_names(self, symbol: &str) -> Vec<String> {
        match self {
            Self::MarkPriceWith1s => vec![format!("{symbol}@markPrice@1s")],
            Self::MarkPrice => vec![format!("{symbol}@markPrice")],
            Self::Both => vec![
                format!("{symbol}@markPrice@1s"),
                format!("{symbol}@markPrice"),
            ],
        }
    }

    fn from_env_name() -> &'static str {
        match std::env::var("BINANCE_FUTURES_MARK_PRICE_STREAM_FORM")
            .unwrap_or_else(|_| "1s".to_string())
            .as_str()
        {
            "markPrice" => "markPrice",
            "both" => "both",
            "1s" | "markPrice@1s" => "1s",
            _ => "1s",
        }
    }
}

impl MarketDataStream {
    pub fn new(
        client: BinanceFuturesClient,
        ev_tx: crate::connector::PublishSender,
        command_rx: Receiver<MarketDataCommand>,
        subscriptions: SharedMarketSubscriptions,
        route: MarketStreamRoute,
    ) -> Self {
        let (rest_tx, rest_rx) = channel::<(String, rest::Depth)>(64);
        Self {
            client,
            ev_tx,
            command_rx,
            subscriptions,
            pending_depth_messages: Default::default(),
            prev_u: Default::default(),
            stream_epochs: Default::default(),
            canonical_symbols: Default::default(),
            rest_tx,
            rest_rx,
            route,
        }
    }

    fn fetch_snapshot(&self, symbol: String) {
        let client = self.client.clone();
        let rest_tx = self.rest_tx.clone();
        tokio::spawn(async move {
            match client.get_depth(&symbol).await {
                Ok(depth) => {
                    let _ = rest_tx.send((symbol, depth)).await;
                }
                Err(error) => {
                    error!(?error, %symbol, "Couldn't get the market depth via REST.");
                }
            }
        });
    }

    fn streams(&self, symbol: &str, kinds: &[MarketDataKind]) -> Vec<String> {
        let mut streams = Vec::new();
        let mark_price_mode = MarkPriceStreamMode::from_env();
        for kind in kinds {
            if !self.route.includes(*kind) {
                continue;
            }
            match kind {
                MarketDataKind::Depth => format!("{symbol}@depth@0ms"),
                MarketDataKind::Trades => format!("{symbol}@trade"),
                MarketDataKind::Bbo | MarketDataKind::Ticker => format!("{symbol}@bookTicker"),
                MarketDataKind::MarkPrice | MarketDataKind::FundingRate => {
                    let signatures = mark_price_mode.stream_names(symbol);
                    for stream in signatures {
                        if !streams.contains(&stream) {
                            streams.push(stream);
                        }
                    }
                    continue;
                }
            };
            let stream = if let MarketDataKind::Depth = kind {
                format!("{symbol}@depth@0ms")
            } else if let MarketDataKind::Trades = kind {
                format!("{symbol}@trade")
            } else {
                format!("{symbol}@bookTicker")
            };
            if !streams.contains(&stream) {
                streams.push(stream);
            }
        }
        streams
    }

    async fn send_request(
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        method: &str,
        params: Vec<String>,
    ) -> Result<String, BinanceFuturesError> {
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        debug!(%method, ?params, "sending Binance market stream request");
        let request = serde_json::json!({
            "method": method,
            "params": params,
            "id": request_id,
        });
        write
            .send(Message::Text(request.to_string().into()))
            .await?;
        Ok(request_id.to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        method: &str,
        symbol: &str,
        kinds: &[MarketDataKind],
    ) -> Result<String, BinanceFuturesError> {
        let params = self.streams(symbol, kinds);
        if params.is_empty() {
            return Ok(String::new());
        }
        Self::send_request(write, method, params).await
    }

    fn maybe_process_mark_price_payload(
        &mut self,
        text: &str,
        value: &Value,
        ws_recv_ts: i64,
    ) -> bool {
        if let Some(event) = value.get("e").and_then(Value::as_str) {
            if event.eq_ignore_ascii_case("markprice")
                || event.eq_ignore_ascii_case("markpriceupdate")
            {
                debug!(
                    event,
                    text = %text,
                "received markPrice-like stream payload that did not match strict parser"
                    );
                match serde_json::from_value::<stream::MarkPriceUpdate>(value.clone()) {
                    Ok(stream) => {
                        self.process_message(EventStream::MarkPriceUpdate(stream), ws_recv_ts);
                        return true;
                    }
                    Err(error) => {
                        warn!(?error, %text, "markPrice payload still failed to parse");
                    }
                }
            }
            return false;
        }

        if let Some(stream_name) = value.get("stream").and_then(Value::as_str) {
            if stream_name.contains("markPrice") {
                if let Some(data) = value.get("data") {
                    debug!(
                        stream = %stream_name,
                        "received wrapped markPrice stream payload for fallback parsing"
                    );
                    match serde_json::from_value::<stream::MarkPriceUpdate>(data.clone()) {
                        Ok(stream) => {
                            self.process_message(EventStream::MarkPriceUpdate(stream), ws_recv_ts);
                            return true;
                        }
                        Err(error) => {
                            warn!(
                                ?error,
                                stream = %stream_name,
                                data = ?data,
                                "wrapped markPrice payload failed to parse"
                            );
                        }
                    }
                }
            }
        }
        false
    }

    fn publish_depth_update(&self, data: stream::OwnedDepth, receive_ts: i64) {
        let stream = MarketStreamMetadata {
            epoch: self.stream_epochs.get(&data.symbol).copied().unwrap_or(0),
            first_update_sequence: u64::try_from(data.first_update_id).unwrap_or(0),
            last_update_sequence: u64::try_from(data.last_update_id).unwrap_or(0),
            snapshot: false,
        };
        if self.ev_tx.try_send_native_market(NativeMarketBatch::Depth {
            symbol: &data.symbol,
            bids: NativeDepthLevels::Owned(&data.bids),
            asks: NativeDepthLevels::Owned(&data.asks),
            exchange_ts: data.transaction_time * 1_000_000,
            receive_ts,
            stream,
        }) {
            return;
        }
        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                let mut events = Vec::with_capacity(bids.len() + asks.len());
                for (px, qty) in bids {
                    events.push(Event {
                        ev: LOCAL_BID_DEPTH_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: receive_ts,
                        order_id: data.prev_update_id as u64,
                        px,
                        qty,
                        ival: data.last_update_id,
                        fval: 0.0,
                    });
                }
                for (px, qty) in asks {
                    events.push(Event {
                        ev: LOCAL_ASK_DEPTH_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: receive_ts,
                        order_id: data.prev_update_id as u64,
                        px,
                        qty,
                        ival: data.last_update_id,
                        fval: 0.0,
                    });
                }
                self.ev_tx
                    .send(PublishEvent::FeedBatch {
                        symbol: data.symbol,
                        events,
                        stream: Some(stream),
                    })
                    .unwrap();
            }
            Err(error) => error!(?error, "Couldn't parse DepthUpdate stream."),
        }
    }

    fn publish_borrowed_depth_update(
        &self,
        symbol: &str,
        data: stream::Depth<'_>,
        receive_ts: i64,
    ) {
        let stream = MarketStreamMetadata {
            epoch: self.stream_epochs.get(symbol).copied().unwrap_or(0),
            first_update_sequence: u64::try_from(data.first_update_id).unwrap_or(0),
            last_update_sequence: u64::try_from(data.last_update_id).unwrap_or(0),
            snapshot: false,
        };
        if self.ev_tx.try_send_native_market(NativeMarketBatch::Depth {
            symbol,
            bids: NativeDepthLevels::Borrowed(data.bids.as_slice()),
            asks: NativeDepthLevels::Borrowed(data.asks.as_slice()),
            exchange_ts: data.transaction_time * 1_000_000,
            receive_ts,
            stream,
        }) {
            return;
        }
        self.publish_depth_update(data.into_owned(symbol.to_owned()), receive_ts);
    }

    /// Processes one decoded WebSocket push.
    ///
    /// `ws_recv_ts` is captured immediately after tungstenite yields the text frame, before JSON
    /// decoding.  Keeping that timestamp on every feed event lets a strategy measure the complete
    /// in-process path from WS ingress, through the connector and IPC, to `on_tick`.
    fn process_message(&mut self, stream: EventStream<'_>, ws_recv_ts: i64) {
        match stream {
            EventStream::DepthUpdate(data) => {
                let Some(symbol) = self.canonical_symbols.get(data.symbol).map(String::as_str)
                else {
                    warn!(
                        symbol = data.symbol,
                        "depth update received for an unknown symbol"
                    );
                    return;
                };
                match self.prev_u.get_mut(symbol) {
                    Some(previous) if data.prev_update_id == *previous => {
                        *previous = data.last_update_id;
                        self.publish_borrowed_depth_update(symbol, data, ws_recv_ts);
                    }
                    Some(previous) => {
                        let previous = *previous;
                        let symbol = symbol.to_owned();
                        warn!(%symbol, previous, received = data.prev_update_id, "depth sequence gap; requesting a fresh snapshot");
                        let _ = self.ev_tx.send(PublishEvent::StreamInvalidated {
                            symbol: symbol.clone(),
                            epoch: self.stream_epochs.get(&symbol).copied().unwrap_or(0),
                        });
                        self.prev_u.remove(&symbol);
                        self.pending_depth_messages
                            .entry(symbol.clone())
                            .or_default()
                            .push((data.into_owned(symbol.clone()), ws_recv_ts));
                        self.fetch_snapshot(symbol);
                    }
                    None => {
                        let symbol = symbol.to_owned();
                        let first = !self.pending_depth_messages.contains_key(&symbol);
                        self.pending_depth_messages
                            .entry(symbol.clone())
                            .or_default()
                            .push((data.into_owned(symbol.clone()), ws_recv_ts));
                        if first {
                            self.fetch_snapshot(symbol);
                        }
                    }
                }
            }
            EventStream::MarkPriceUpdate(data) | EventStream::MarkPrice(data) => {
                let symbol = data.symbol.clone();
                if self
                    .ev_tx
                    .send(PublishEvent::MarkPrice {
                        symbol: symbol.clone(),
                        mark_price: data.mark_price,
                        exch_ts: data.event_time * 1_000_000,
                    })
                    .is_err()
                {
                    warn!(symbol = %symbol, "market data sender closed while publishing mark price");
                    return;
                }
                if self
                    .ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Funding {
                        symbol: symbol.clone(),
                        funding_rate: data.funding_rate,
                        next_funding_time: data.next_funding_time * 1_000_000,
                        exch_ts: data.event_time * 1_000_000,
                    }))
                    .is_err()
                {
                    warn!(symbol = %symbol, "market data sender closed while publishing funding event");
                }
            }
            EventStream::Trade(data) => {
                let symbol = data.symbol.clone();
                if self.ev_tx.try_send_native_market(NativeMarketBatch::Trade {
                    symbol: &data.symbol,
                    price: data.price,
                    quantity: data.qty,
                    sell: data.is_the_buyer_the_market_maker,
                    exchange_ts: data.transaction_time * 1_000_000,
                    receive_ts: ws_recv_ts,
                }) {
                    return;
                }
                if self
                    .ev_tx
                    .send(PublishEvent::FeedBatch {
                        symbol: symbol.clone(),
                        events: vec![Event {
                            ev: {
                                if data.is_the_buyer_the_market_maker {
                                    LOCAL_SELL_TRADE_EVENT
                                } else {
                                    LOCAL_BUY_TRADE_EVENT
                                }
                            },
                            exch_ts: data.transaction_time * 1_000_000,
                            local_ts: ws_recv_ts,
                            order_id: 0,
                            px: data.price,
                            qty: data.qty,
                            ival: 0,
                            fval: 0.0,
                        }],
                        stream: None,
                    })
                    .is_err()
                {
                    warn!(symbol = %symbol, "market data sender closed while publishing trade batch");
                }
            }
            EventStream::BookTicker(data) => {
                let symbol = data.symbol.clone();
                if self.ev_tx.try_send_native_market(NativeMarketBatch::Bbo {
                    symbol: &data.symbol,
                    bid_price: data.bid_price,
                    bid_quantity: data.bid_qty,
                    ask_price: data.ask_price,
                    ask_quantity: data.ask_qty,
                    exchange_ts: data.transaction_time * 1_000_000,
                    receive_ts: ws_recv_ts,
                }) {
                    return;
                }
                let local_ts = ws_recv_ts;
                let mut events = Vec::with_capacity(2);
                if data.bid_price > 0.0 {
                    events.push(Event {
                        ev: LOCAL_BID_DEPTH_BBO_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts,
                        order_id: 0,
                        px: data.bid_price,
                        qty: data.bid_qty,
                        ival: 0,
                        fval: 0.0,
                    });
                }
                if data.ask_price > 0.0 {
                    events.push(Event {
                        ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts,
                        order_id: 0,
                        px: data.ask_price,
                        qty: data.ask_qty,
                        ival: 0,
                        fval: 0.0,
                    });
                }
                if !events.is_empty() {
                    if self
                        .ev_tx
                        .send(PublishEvent::FeedBatch {
                            symbol: symbol.clone(),
                            events,
                            stream: None,
                        })
                        .is_err()
                    {
                        warn!(symbol = %symbol, "market data sender closed while publishing bbo batch");
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn process_snapshot(&mut self, symbol: String, data: rest::Depth) {
        let snapshot_sequence = data.last_update_id;
        let epoch = {
            let value = self.stream_epochs.entry(symbol.clone()).or_insert(0);
            *value = value.saturating_add(1);
            *value
        };
        let snapshot_receive_ts = Utc::now().timestamp_nanos_opt().unwrap();
        if self.ev_tx.try_send_native_market(NativeMarketBatch::Depth {
            symbol: &symbol,
            bids: NativeDepthLevels::Owned(&data.bids),
            asks: NativeDepthLevels::Owned(&data.asks),
            exchange_ts: data.transaction_time * 1_000_000,
            receive_ts: snapshot_receive_ts,
            stream: MarketStreamMetadata {
                epoch,
                first_update_sequence: u64::try_from(snapshot_sequence).unwrap_or(0),
                last_update_sequence: u64::try_from(snapshot_sequence).unwrap_or(0),
                snapshot: true,
            },
        }) {
            self.finish_snapshot_sync(symbol, snapshot_sequence);
            return;
        }
        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                let mut events = Vec::with_capacity(bids.len() + asks.len());
                for (px, qty) in bids {
                    events.push(Event {
                        ev: LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px,
                        qty,
                        ival: data.last_update_id,
                        fval: 0.0,
                    });
                }
                for (px, qty) in asks {
                    events.push(Event {
                        ev: LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px,
                        qty,
                        ival: data.last_update_id,
                        fval: 0.0,
                    });
                }
                if self
                    .ev_tx
                    .send(PublishEvent::FeedBatch {
                        symbol: symbol.clone(),
                        events,
                        stream: Some(MarketStreamMetadata {
                            epoch,
                            first_update_sequence: u64::try_from(snapshot_sequence).unwrap_or(0),
                            last_update_sequence: u64::try_from(snapshot_sequence).unwrap_or(0),
                            snapshot: true,
                        }),
                    })
                    .is_err()
                {
                    warn!(symbol = %symbol, "market data sender closed while publishing snapshot batch");
                    self.pending_depth_messages.remove(&symbol);
                }
            }
            Err(error) => {
                error!(?error, "Couldn't parse Depth response.");
                return;
            }
        }
        self.finish_snapshot_sync(symbol, snapshot_sequence);
    }

    fn finish_snapshot_sync(&mut self, symbol: String, snapshot_sequence: i64) {
        let mut previous = snapshot_sequence;
        let mut synchronized = false;
        if let Some(pending) = self.pending_depth_messages.remove(&symbol) {
            for (update, receive_ts) in pending {
                if update.last_update_id < snapshot_sequence {
                    continue;
                }
                let valid = if synchronized {
                    update.prev_update_id == previous
                } else {
                    update.first_update_id <= snapshot_sequence
                        && update.last_update_id >= snapshot_sequence
                };
                if !valid {
                    warn!(%symbol, previous, first = update.first_update_id, last = update.last_update_id, "pending depth sequence is not contiguous");
                    self.pending_depth_messages
                        .entry(symbol.clone())
                        .or_default()
                        .push((update, receive_ts));
                    self.fetch_snapshot(symbol);
                    return;
                }
                previous = update.last_update_id;
                synchronized = true;
                self.publish_depth_update(update, receive_ts);
            }
        }
        self.prev_u.insert(symbol, previous);
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BinanceFuturesError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();
        let mut pending_requests: HashMap<String, String> = HashMap::new();

        let subscriptions: Vec<_> = self
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|(symbol, kinds)| (symbol.clone(), kinds.iter().copied().collect::<Vec<_>>()))
            .collect();
        for (symbol, kinds) in subscriptions {
            let request_id = self
                .send_subscription(&mut write, "SUBSCRIBE", &symbol, &kinds)
                .await?;
            if !request_id.is_empty() {
                self.canonical_symbols
                    .insert(symbol.to_ascii_uppercase(), symbol.clone());
                pending_requests.insert(
                    request_id,
                    format!("SUBSCRIBE symbol={symbol} kinds={kinds:?}"),
                );
            }
            debug!(
                %symbol,
                kinds = ?kinds,
                mode = %MarkPriceStreamMode::from_env_name(),
                "bootstrap subscription prepared"
            );
        }
        let request_id =
            Self::send_request(&mut write, "LIST_SUBSCRIPTIONS", Vec::<String>::new()).await?;
        pending_requests.insert(request_id, "LIST_SUBSCRIPTIONS".to_string());

        let mark_price_signals: Vec<String> = {
            let profile = MarkPriceStreamMode::from_env();
            let symbols = self.canonical_symbols.values().cloned().collect::<Vec<_>>();
            let mut list = Vec::new();
            for symbol in symbols {
                for mode_stream in profile.stream_names(&symbol) {
                    if mode_stream.contains("markPrice") {
                        list.push(mode_stream);
                    }
                }
            }
            list
        };
        if !mark_price_signals.is_empty() {
            debug!(
                ws_url = %url,
                mark_price_streams = ?mark_price_signals,
                "expected Binance markPrice subscriptions"
            );
        }

        loop {
            select! {
                Some((symbol, data)) = self.rest_rx.recv() => {
                    self.process_snapshot(symbol, data);
                }
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("Ping timeout.");
                        return Err(BinanceFuturesError::ConnectionInterrupted);
                    }
                }
                msg = self.command_rx.recv() => match msg {
                    Ok(MarketDataCommand::Subscribe { symbol, kinds }) => {
                        let request_id =
                            self.send_subscription(&mut write, "SUBSCRIBE", &symbol, &kinds).await?;
                        if !request_id.is_empty() {
                            self.canonical_symbols
                                .insert(symbol.to_ascii_uppercase(), symbol.clone());
                            pending_requests.insert(
                                request_id,
                                format!("SUBSCRIBE symbol={symbol} kinds={kinds:?}"),
                            );
                        }
                    }
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => {
                        let request_id =
                            self.send_subscription(&mut write, "UNSUBSCRIBE", &symbol, &kinds).await?;
                        if !request_id.is_empty() {
                            pending_requests.insert(
                                request_id,
                                format!("UNSUBSCRIBE symbol={symbol} kinds={kinds:?}"),
                            );
                        }
                        if self.route.includes(MarketDataKind::Depth)
                            && kinds.contains(&MarketDataKind::Depth)
                        {
                            self.prev_u.remove(&symbol);
                            self.pending_depth_messages.remove(&symbol);
                            self.canonical_symbols.remove(&symbol.to_ascii_uppercase());
                        }
                    }
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
                        if !self.route.includes(MarketDataKind::Depth) {
                            continue;
                        }
                        let _ = self.ev_tx.send(PublishEvent::StreamInvalidated {
                            epoch: self.stream_epochs.get(&symbol).copied().unwrap_or(0),
                            symbol: symbol.clone(),
                        });
                        self.prev_u.remove(&symbol);
                        self.pending_depth_messages.remove(&symbol);
                        self.fetch_snapshot(symbol);
                    }
                    Ok(MarketDataCommand::InitializeTrading { .. }) => {}
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        let ws_recv_ts = Utc::now().timestamp_nanos_opt().unwrap();
                        let text_contains_mark = text.contains("\"markPrice\"") || text.contains("\"markprice\"");
                        if text_contains_mark {
                            info!(ws_url = %url, stream_key = self.canonical_symbols.keys().next().map(|v| v.as_str()).unwrap_or(""), raw = %text, "raw frame includes markPrice marker");
                        }
                        if text.as_bytes().starts_with(br#"{"e":"depthUpdate"#) {
                            match serde_json::from_str::<stream::Depth>(&text) {
                                Ok(depth) => self.process_message(
                                    EventStream::DepthUpdate(depth),
                                    ws_recv_ts,
                                ),
                                Err(error) => {
                                    error!(?error, %text, "Couldn't parse Depth stream.");
                                }
                            }
                            continue;
                        }
                        if let Ok(value) = serde_json::from_str::<Value>(&text) {
                            if self.maybe_process_mark_price_payload(&text, &value, ws_recv_ts) {
                                continue;
                            }
                        }
                        match serde_json::from_str::<Stream>(&text) {
                            Ok(Stream::EventStream(stream)) => {
                                self.process_message(stream, ws_recv_ts);
                            }
                            Ok(Stream::Result(result)) => {
                                if let Some(label) = pending_requests.remove(&result.id) {
                                    debug!(request_id = %result.id, %label, ?result.result, ?result.error, "subscription response received");
                                } else {
                                    debug!(
                                        request_id = %result.id,
                                        ?result.result,
                                        ?result.error,
                                        "subscription response received (untracked request)"
                                    );
                                }
                                if let Some(error) = result.error {
                                    error!(
                                        request_id = %result.id,
                                        code = error.code,
                                        message = %error.msg,
                                        "subscription response error"
                                    );
                                }
                                if let Some(raw_result) = result.result.as_ref() {
                                    if raw_result.is_array() {
                                        if let Some(streams) = raw_result.as_array() {
                                            let mark_price_streams: Vec<_> = streams
                                                .iter()
                                                .filter_map(|item| item.as_str())
                                                .filter(|item| item.contains("markPrice"))
                                                .collect();
                                            if !mark_price_streams.is_empty() {
                                                debug!(
                                                    request_id = %result.id,
                                                    mark_price_streams = ?mark_price_streams,
                                                    "markPrice appears in LIST_SUBSCRIPTIONS response"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                if text_contains_mark {
                                    warn!(?error, %text, "Couldn't parse stream message for markPrice payload");
                                }
                                error!(?error, %text, "Couldn't parse Stream.");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        return Err(BinanceFuturesError::ConnectionAbort(
                            close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                        ));
                    }
                    Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_)))
                    | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        return Err(BinanceFuturesError::from(error));
                    }
                    None => {
                        return Err(BinanceFuturesError::ConnectionInterrupted);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
    };

    #[test]
    fn split_routes_accept_only_their_assigned_market_kinds() {
        assert!(MarketStreamRoute::Public.includes(MarketDataKind::Depth));
        assert!(MarketStreamRoute::Public.includes(MarketDataKind::Trades));
        assert!(MarketStreamRoute::Public.includes(MarketDataKind::Bbo));
        assert!(!MarketStreamRoute::Public.includes(MarketDataKind::MarkPrice));
        assert!(!MarketStreamRoute::Public.includes(MarketDataKind::FundingRate));
        assert!(MarketStreamRoute::Market.includes(MarketDataKind::MarkPrice));
        assert!(MarketStreamRoute::Market.includes(MarketDataKind::FundingRate));
        assert!(!MarketStreamRoute::Market.includes(MarketDataKind::Depth));
    }

    #[tokio::test]
    async fn mark_price_frame_publishes_price_and_funding_independently() {
        let client = BinanceFuturesClient::new("http://localhost", "", "");
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut market_stream = MarketDataStream::new(
            client,
            events,
            command_rx,
            Arc::new(Mutex::new(HashMap::new())),
            MarketStreamRoute::Market,
        );
        let update = serde_json::from_str::<EventStream<'_>>(
            r#"{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15000000","i":"11793.84535841","P":"11780.83846368","r":"-0.00038100","T":1562306400000}"#,
        )
        .unwrap();
        market_stream.process_message(update, 0);

        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::MarkPrice {
                symbol,
                mark_price,
                exch_ts: 1_562_305_380_000_000_000,
            }) if symbol == "btcusdt" && mark_price == 11_794.15
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::LiveEvent(LiveEvent::Funding {
                symbol,
                funding_rate,
                next_funding_time: 1_562_306_400_000_000_000,
                exch_ts: 1_562_305_380_000_000_000,
            })) if symbol == "btcusdt" && funding_rate == -0.000381
        ));
    }

    #[tokio::test]
    async fn rest_snapshot_and_contiguous_delta_preserve_sequence_and_advance_epoch() {
        let client = BinanceFuturesClient::new("http://localhost", "", "");
        let (events, mut receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = tokio::sync::broadcast::channel(4);
        let mut stream = MarketDataStream::new(
            client,
            events,
            command_rx,
            Arc::new(Mutex::new(HashMap::new())),
            MarketStreamRoute::All,
        );
        stream
            .canonical_symbols
            .insert("BTCUSDT".to_owned(), "btcusdt".to_owned());
        stream.process_snapshot(
            "btcusdt".to_owned(),
            rest::Depth {
                last_update_id: 10,
                event_time: 1,
                transaction_time: 1,
                bids: vec![("100".to_owned(), "1".to_owned())],
                asks: vec![("101".to_owned(), "2".to_owned())],
            },
        );
        let first = match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                stream: Some(stream),
                ..
            } => stream,
            _ => panic!("expected Binance Futures snapshot"),
        };
        assert!(first.snapshot);
        assert_eq!(first.epoch, 1);
        assert_eq!(first.last_update_sequence, 10);

        let delta = serde_json::from_str::<EventStream<'_>>(
            r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":11,"u":11,"pu":10,"b":[["100","3"]],"a":[]}"#,
        )
        .unwrap();
        stream.process_message(delta, 3);
        let delta = match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                stream: Some(stream),
                ..
            } => stream,
            _ => panic!("expected Binance Futures delta"),
        };
        assert!(!delta.snapshot);
        assert_eq!(delta.epoch, 1);
        assert_eq!(delta.first_update_sequence, 11);
        assert_eq!(delta.last_update_sequence, 11);

        let gap = serde_json::from_str::<EventStream<'_>>(
            r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSDT","U":12,"u":12,"pu":9,"b":[],"a":[]}"#,
        )
        .unwrap();
        stream.process_message(gap, 4);
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::StreamInvalidated { epoch: 1, .. })
        ));

        stream.process_snapshot(
            "btcusdt".to_owned(),
            rest::Depth {
                last_update_id: 20,
                event_time: 4,
                transaction_time: 4,
                bids: vec![],
                asks: vec![],
            },
        );
        let recovered = match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                stream: Some(stream),
                ..
            } => stream,
            _ => panic!("expected Binance Futures recovery snapshot"),
        };
        assert!(recovered.snapshot);
        assert_eq!(recovered.epoch, 2);
        assert_eq!(recovered.last_update_sequence, 20);
    }

    /// 实盘冒烟：连接 Binance USD-M 测试网 WS，订阅 btcusdt 的
    /// trade/depth@0ms/markPrice/bookTicker，验证收到深度、成交、资金费与 BBO 事件。
    /// 运行：`cargo test --all-features binancefutures::market_data_stream::tests::live_ws -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_ws() {
        let client = BinanceFuturesClient::new("https://testnet.binancefuture.com", "", "");
        let (ev_tx, mut ev_rx) = crate::connector::test_publish_channel();
        let (symbol_tx, _) = tokio::sync::broadcast::channel(16);
        let subscriptions = std::sync::Arc::new(std::sync::Mutex::new(HashMap::from([(
            "btcusdt".to_string(),
            HashSet::from([
                MarketDataKind::Depth,
                MarketDataKind::Trades,
                MarketDataKind::Bbo,
                MarketDataKind::FundingRate,
            ]),
        )])));
        let mut stream = MarketDataStream::new(
            client,
            ev_tx,
            symbol_tx.subscribe(),
            subscriptions,
            MarketStreamRoute::All,
        );

        let handle = tokio::spawn(async move {
            let mut last_err = None;
            for attempt in 0..3 {
                match stream.connect("wss://fstream.binancefuture.com/ws").await {
                    Ok(()) => return Ok::<(), BinanceFuturesError>(()),
                    Err(e) => {
                        eprintln!("connect attempt {attempt} failed: {e:?}; retrying");
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
            Err(last_err.unwrap())
        });

        let mut feed_depth = 0usize;
        let mut feed_bbo = 0usize;
        let mut feed_trade = 0usize;
        let mut funding = 0usize;
        let mut snapshots = 0usize;
        let mut deltas = 0usize;
        let mut epoch = None;
        let mut last_sequence = None;

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_secs(3), ev_rx.recv()).await {
                Ok(Some(PublishEvent::LiveEvent(LiveEvent::Feed { symbol, event }))) => {
                    assert_eq!(symbol, "btcusdt");
                    if event.is(LOCAL_BID_DEPTH_EVENT) || event.is(LOCAL_ASK_DEPTH_EVENT) {
                        feed_depth += 1;
                    } else if event.is(LOCAL_BID_DEPTH_BBO_EVENT)
                        || event.is(LOCAL_ASK_DEPTH_BBO_EVENT)
                    {
                        feed_bbo += 1;
                    } else if event.is(LOCAL_BUY_TRADE_EVENT) || event.is(LOCAL_SELL_TRADE_EVENT) {
                        feed_trade += 1;
                    }
                }
                Ok(Some(PublishEvent::LiveEvent(LiveEvent::Funding {
                    symbol,
                    funding_rate,
                    ..
                }))) => {
                    assert_eq!(symbol, "btcusdt");
                    assert!(funding_rate.is_finite());
                    funding += 1;
                }
                Ok(Some(PublishEvent::FeedBatch {
                    symbol,
                    events,
                    stream,
                })) => {
                    assert_eq!(symbol, "btcusdt");
                    for event in events {
                        if event.is(LOCAL_BID_DEPTH_EVENT)
                            || event.is(LOCAL_ASK_DEPTH_EVENT)
                            || event.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT)
                            || event.is(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT)
                        {
                            feed_depth += 1;
                        } else if event.is(LOCAL_BID_DEPTH_BBO_EVENT)
                            || event.is(LOCAL_ASK_DEPTH_BBO_EVENT)
                        {
                            feed_bbo += 1;
                        } else if event.is(LOCAL_BUY_TRADE_EVENT)
                            || event.is(LOCAL_SELL_TRADE_EVENT)
                        {
                            feed_trade += 1;
                        }
                    }
                    if let Some(stream) = stream {
                        assert!(stream.epoch > 0);
                        assert!(stream.first_update_sequence <= stream.last_update_sequence);
                        if stream.snapshot {
                            snapshots += 1;
                            epoch = Some(stream.epoch);
                            last_sequence = Some(stream.last_update_sequence);
                        } else {
                            deltas += 1;
                            assert_eq!(epoch, Some(stream.epoch));
                            if let Some(previous) = last_sequence {
                                assert!(
                                    stream.last_update_sequence >= previous,
                                    "depth sequence regressed: previous={previous}, current={stream:?}"
                                );
                            }
                            last_sequence = Some(stream.last_update_sequence);
                        }
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_elapsed) => {
                    eprintln!(
                        "no event for 3s (depth={feed_depth} bbo={feed_bbo} trade={feed_trade} funding={funding})"
                    );
                    if snapshots > 0 && deltas > 0 && feed_bbo > 0 && funding > 0 {
                        break;
                    }
                }
            }
        }

        handle.abort();
        println!(
            "depth={feed_depth} snapshots={snapshots} deltas={deltas} bbo={feed_bbo} trade={feed_trade} funding={funding} epoch={epoch:?} last_sequence={last_sequence:?}"
        );
        assert!(feed_depth > 0, "no depth feed events received");
        assert!(snapshots > 0, "no depth snapshot received");
        assert!(deltas > 0, "no depth delta received after snapshot");
        assert!(
            feed_trade > 0 || feed_bbo > 0,
            "no trade/BBO feed events received"
        );
        assert!(funding > 0, "no markPrice/funding events received");
    }
}
