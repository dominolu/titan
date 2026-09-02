use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::*;
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
    utils::{generate_rand_string, parse_depth},
};

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
}

impl MarketDataStream {
    pub fn new(
        client: BinanceFuturesClient,
        ev_tx: crate::connector::PublishSender,
        command_rx: Receiver<MarketDataCommand>,
        subscriptions: SharedMarketSubscriptions,
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

    fn streams(symbol: &str, kinds: &[MarketDataKind]) -> Vec<String> {
        let mut streams = Vec::new();
        for kind in kinds {
            let stream = match kind {
                MarketDataKind::Depth => format!("{symbol}@depth@0ms"),
                MarketDataKind::Trades => format!("{symbol}@trade"),
                MarketDataKind::Bbo | MarketDataKind::Ticker => format!("{symbol}@bookTicker"),
                MarketDataKind::MarkPrice | MarketDataKind::FundingRate => {
                    format!("{symbol}@markPrice")
                }
            };
            if !streams.contains(&stream) {
                streams.push(stream);
            }
        }
        streams
    }

    async fn send_subscription(
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        method: &str,
        symbol: &str,
        kinds: &[MarketDataKind],
    ) -> Result<(), BinanceFuturesError> {
        let params = Self::streams(symbol, kinds);
        if params.is_empty() {
            return Ok(());
        }
        let request = serde_json::json!({
            "method": method,
            "params": params,
            "id": generate_rand_string(16),
        });
        write
            .send(Message::Text(request.to_string().into()))
            .await?;
        Ok(())
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
            EventStream::MarkPriceUpdate(data) => {
                self.ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Funding {
                        symbol: data.symbol,
                        funding_rate: data.funding_rate,
                        next_funding_time: data.next_funding_time * 1_000_000,
                        exch_ts: data.event_time * 1_000_000,
                    }))
                    .unwrap();
            }
            EventStream::Trade(data) => {
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
                self.ev_tx
                    .send(PublishEvent::FeedBatch {
                        symbol: data.symbol,
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
                    .unwrap();
            }
            EventStream::BookTicker(data) => {
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
                    self.ev_tx
                        .send(PublishEvent::FeedBatch {
                            symbol: data.symbol,
                            events,
                            stream: None,
                        })
                        .unwrap();
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
                self.ev_tx
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
                    .unwrap();
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

        let subscriptions: Vec<_> = self
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|(symbol, kinds)| (symbol.clone(), kinds.iter().copied().collect::<Vec<_>>()))
            .collect();
        for (symbol, kinds) in subscriptions {
            self.canonical_symbols
                .insert(symbol.to_ascii_uppercase(), symbol.clone());
            Self::send_subscription(&mut write, "SUBSCRIBE", &symbol, &kinds).await?;
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
                        self.canonical_symbols
                            .insert(symbol.to_ascii_uppercase(), symbol.clone());
                        Self::send_subscription(&mut write, "SUBSCRIBE", &symbol, &kinds).await?;
                    }
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => {
                        Self::send_subscription(&mut write, "UNSUBSCRIBE", &symbol, &kinds).await?;
                        if kinds.contains(&MarketDataKind::Depth) {
                            self.prev_u.remove(&symbol);
                            self.pending_depth_messages.remove(&symbol);
                            self.canonical_symbols.remove(&symbol.to_ascii_uppercase());
                        }
                    }
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
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
                        match serde_json::from_str::<Stream>(&text) {
                            Ok(Stream::EventStream(stream)) => {
                                self.process_message(stream, ws_recv_ts);
                            }
                            Ok(Stream::Result(result)) => {
                                debug!(?result, "Subscription request response is received.");
                            }
                            Err(error) => {
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
    use std::collections::HashSet;

    /// 实盘冒烟：连接 Binance USD-M 测试网 WS，订阅 btcusdt 的
    /// trade/depth@0ms/markPrice/bookTicker，验证收到深度、成交、资金费与 BBO 事件。
    /// 运行：`cargo test --all-features binancefutures::market_data_stream::tests::live_ws -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_ws() {
        let client = BinanceFuturesClient::new("https://testnet.binancefuture.com", "", "");
        let (ev_tx, mut ev_rx) = crate::connector::publish_channel(64);
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
        let mut stream = MarketDataStream::new(client, ev_tx, symbol_tx.subscribe(), subscriptions);

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
