use std::{collections::HashMap, sync::Mutex, time::Duration};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use titan_domain_types::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use titan_market_service::MarketDataKind;
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    connector::{MarketDataCommand, MarketStreamMetadata, PublishEvent},
    okx::{
        OkxError, SharedMarketSubscriptions,
        msg::{
            rest::Books,
            stream::{BboTbt, Books5, DataMsg, FundingRate, StreamMsg, Trade, WsArg, WsRequest},
        },
    },
};

pub struct PublicStream {
    ev_tx: crate::connector::PublishSender,
    command_rx: Receiver<MarketDataCommand>,
    subscriptions: SharedMarketSubscriptions,
    books: Mutex<HashMap<String, LocalBook>>,
    /// Symbols whose book fell out of sequence. A gap is a lost message, not a broken socket:
    /// resubscribing the one book makes OKX push a fresh snapshot over the same connection,
    /// where dropping the connection would take every other symbol down with it.
    resync_tx: UnboundedSender<String>,
    resync_rx: UnboundedReceiver<String>,
}

#[derive(Default)]
struct LocalBook {
    bids: HashMap<String, String>,
    asks: HashMap<String, String>,
    epoch: u64,
    last_sequence: i64,
}

impl LocalBook {
    /// Applies one side of a push to the local book.
    ///
    /// The string forms are kept exactly as the venue wrote them, because the checksum is computed
    /// over them: a level stored as `"100.0"` when OKX sent `"100"` makes every later checksum
    /// disagree and drives a resynchronization the book did not need.
    ///
    /// A level whose quantity is not a number is skipped and reported. Reading it as zero used to
    /// delete the level instead, which silently changed the book.
    fn apply(side: &mut HashMap<String, String>, levels: &[Vec<String>]) -> bool {
        let mut clean = true;
        for level in levels {
            if level.len() < 2 {
                clean = false;
                continue;
            }
            match level[1].parse::<f64>() {
                Ok(0.0) => {
                    side.remove(&level[0]);
                }
                Ok(qty) if qty.is_finite() && qty > 0.0 => {
                    side.insert(level[0].clone(), level[1].clone());
                }
                _ => clean = false,
            }
        }
        clean
    }

    fn checksum(&self) -> i64 {
        let mut bids: Vec<_> = self.bids.iter().collect();
        let mut asks: Vec<_> = self.asks.iter().collect();
        bids.sort_by(|(left, _), (right, _)| {
            right
                .parse::<f64>()
                .unwrap_or(0.0)
                .total_cmp(&left.parse::<f64>().unwrap_or(0.0))
        });
        asks.sort_by(|(left, _), (right, _)| {
            left.parse::<f64>()
                .unwrap_or(0.0)
                .total_cmp(&right.parse::<f64>().unwrap_or(0.0))
        });
        let mut fields = Vec::with_capacity(100);
        for index in 0..25 {
            if let Some((price, quantity)) = bids.get(index) {
                fields.push(price.as_str());
                fields.push(quantity.as_str());
            }
            if let Some((price, quantity)) = asks.get(index) {
                fields.push(price.as_str());
                fields.push(quantity.as_str());
            }
        }
        crc32fast::hash(fields.join(":").as_bytes()) as i32 as i64
    }
}

impl PublicStream {
    pub fn new(
        ev_tx: crate::connector::PublishSender,
        command_rx: Receiver<MarketDataCommand>,
        subscriptions: SharedMarketSubscriptions,
    ) -> Self {
        let (resync_tx, resync_rx) = unbounded_channel();
        Self {
            ev_tx,
            command_rx,
            subscriptions,
            books: Mutex::new(HashMap::new()),
            resync_tx,
            resync_rx,
        }
    }

    /// The symbol a gap has asked to resynchronize, if one is waiting.
    ///
    /// Exists so a test can assert that a gap asks for a snapshot instead of ending the
    /// connection; the connect loop drains the same channel.
    #[cfg(test)]
    fn resync_pending(&mut self) -> Option<String> {
        self.resync_rx.try_recv().ok()
    }

    async fn subscribe_symbol(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), OkxError> {
        let args = Self::args(&symbol, kinds);
        if args.is_empty() {
            return Ok(());
        }
        let op = WsRequest {
            op: "subscribe".to_string(),
            args,
        };
        let s = serde_json::to_string(&op).unwrap();
        write.send(Message::Text(s.into())).await?;
        Ok(())
    }

    fn args(symbol: &str, kinds: &[MarketDataKind]) -> Vec<WsArg> {
        let mut channels = Vec::new();
        for kind in kinds {
            let channel = match kind {
                MarketDataKind::Depth => "books",
                MarketDataKind::Trades => "trades",
                MarketDataKind::Bbo => "bbo-tbt",
                MarketDataKind::Ticker => "tickers",
                MarketDataKind::MarkPrice => "mark-price",
                MarketDataKind::FundingRate => "funding-rate",
            };
            if !channels.contains(&channel) {
                channels.push(channel);
            }
        }
        channels
            .into_iter()
            .map(|channel| WsArg {
                channel: channel.to_string(),
                inst_id: Some(symbol.to_string()),
                inst_type: None,
            })
            .collect()
    }

    async fn unsubscribe_symbol(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
        kinds: &[MarketDataKind],
    ) -> Result<(), OkxError> {
        let args = Self::args(&symbol, kinds);
        if args.is_empty() {
            return Ok(());
        }
        let request = WsRequest {
            op: "unsubscribe".to_string(),
            args,
        };
        write
            .send(Message::Text(serde_json::to_string(&request)?.into()))
            .await?;
        if kinds.contains(&MarketDataKind::Depth) {
            self.books
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&symbol);
        }
        Ok(())
    }

    async fn resubscribe_book(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
    ) -> Result<(), OkxError> {
        for op in ["unsubscribe", "subscribe"] {
            let request = WsRequest {
                op: op.to_string(),
                args: vec![WsArg {
                    channel: "books".to_string(),
                    inst_id: Some(symbol.clone()),
                    inst_type: None,
                }],
            };
            write
                .send(Message::Text(serde_json::to_string(&request)?.into()))
                .await?;
        }
        Ok(())
    }

    async fn handle_public_stream(&self, text: &str) -> Result<(), OkxError> {
        let stream = serde_json::from_str::<StreamMsg>(text)?;
        match stream {
            StreamMsg::Ack(ack) => {
                debug!(?ack, "Ack");
            }
            StreamMsg::Data(data) => {
                self.handle_data(&data).await?;
            }
        }
        Ok(())
    }

    async fn handle_data(&self, data: &DataMsg) -> Result<(), OkxError> {
        match data.arg.channel.as_str() {
            "books" => {
                let books: Books =
                    serde_json::from_value(data.data.first().cloned().unwrap_or_default())?;
                let exch_ts = books.ts.parse::<i64>().unwrap_or(0) * 1_000_000;
                let local_ts = Utc::now().timestamp_nanos_opt().unwrap();

                let snapshot = data.action.as_deref() == Some("snapshot");
                let epoch = {
                    let symbol = data.arg.inst_id.clone().unwrap_or_default();
                    let mut states = self.books.lock().unwrap_or_else(|p| p.into_inner());
                    let state = states.entry(symbol.clone()).or_default();
                    if snapshot {
                        let epoch = state.epoch.saturating_add(1);
                        *state = LocalBook {
                            epoch,
                            ..LocalBook::default()
                        };
                    } else if state.epoch == 0 || books.prev_seq_id != state.last_sequence {
                        // A gap, not a dead socket: drop this book's state, say so, and ask the
                        // connection loop to resubscribe the one book. OKX answers a subscribe
                        // with a fresh snapshot, so the connection keeps serving every other
                        // symbol while this one recovers.
                        let epoch = state.epoch;
                        *state = LocalBook {
                            epoch,
                            ..LocalBook::default()
                        };
                        self.ev_tx.send(PublishEvent::StreamInvalidated {
                            symbol: symbol.clone(),
                            epoch,
                        });
                        let _ = self.resync_tx.send(symbol);
                        return Ok(());
                    }
                    let clean = LocalBook::apply(&mut state.bids, &books.bids)
                        & LocalBook::apply(&mut state.asks, &books.asks);
                    if !clean {
                        // A level the venue sent unreadable leaves the local book short of a
                        // change the venue has already made; the checksum cannot be trusted after
                        // it, so the book is resynchronized rather than carried forward.
                        let epoch = state.epoch;
                        *state = LocalBook {
                            epoch,
                            ..LocalBook::default()
                        };
                        self.ev_tx.send(PublishEvent::StreamInvalidated {
                            symbol: symbol.clone(),
                            epoch,
                        });
                        let _ = self.resync_tx.send(symbol);
                        return Ok(());
                    }
                    if books.checksum != 0 && state.checksum() != books.checksum {
                        // Same reasoning as a sequence gap: the checksum says this book disagrees
                        // with the venue, which only a fresh snapshot fixes. The connection is fine.
                        let epoch = state.epoch;
                        *state = LocalBook {
                            epoch,
                            ..LocalBook::default()
                        };
                        self.ev_tx.send(PublishEvent::StreamInvalidated {
                            symbol: symbol.clone(),
                            epoch,
                        });
                        let _ = self.resync_tx.send(symbol);
                        return Ok(());
                    }
                    state.last_sequence = books.seq_id;
                    state.epoch
                };
                // The state was advanced above and the frame is a duplicate or an empty push with
                // nothing the consumer can record; the first one is still published, because its
                // sequence and time are real and its emptiness is part of what the venue said.
                let mut events = Vec::with_capacity(books.bids.len() + books.asks.len());
                for level in &books.bids {
                    if level.len() < 2 {
                        continue;
                    }
                    let Ok(px) = level[0].parse::<f64>() else {
                        continue;
                    };
                    let Ok(qty) = level[1].parse::<f64>() else {
                        continue;
                    };
                    events.push(Event {
                        ev: if snapshot {
                            LOCAL_BID_DEPTH_SNAPSHOT_EVENT
                        } else {
                            LOCAL_BID_DEPTH_EVENT
                        },
                        exch_ts,
                        local_ts,
                        order_id: books.prev_seq_id as u64,
                        px,
                        qty,
                        ival: books.seq_id,
                        fval: 0.0,
                    });
                }
                for level in &books.asks {
                    if level.len() < 2 {
                        continue;
                    }
                    let Ok(px) = level[0].parse::<f64>() else {
                        continue;
                    };
                    let Ok(qty) = level[1].parse::<f64>() else {
                        continue;
                    };
                    events.push(Event {
                        ev: if snapshot {
                            LOCAL_ASK_DEPTH_SNAPSHOT_EVENT
                        } else {
                            LOCAL_ASK_DEPTH_EVENT
                        },
                        exch_ts,
                        local_ts,
                        order_id: books.prev_seq_id as u64,
                        px,
                        qty,
                        ival: books.seq_id,
                        fval: 0.0,
                    });
                }
                self.ev_tx.send(PublishEvent::FeedBatch {
                    symbol: data.arg.inst_id.clone().unwrap_or_default(),
                    events,
                    stream: Some(MarketStreamMetadata {
                        epoch,
                        first_update_sequence: u64::try_from(books.seq_id).unwrap_or(0),
                        last_update_sequence: u64::try_from(books.seq_id).unwrap_or(0),
                        snapshot,
                    }),
                });
            }
            "trades" => {
                let trades: Vec<Trade> = data
                    .data
                    .iter()
                    .map(|v| serde_json::from_value(v.clone()))
                    .collect::<Result<_, _>>()?;
                let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                for trade in trades {
                    self.ev_tx.send(PublishEvent::FeedBatch {
                        symbol: trade.inst_id.clone(),
                        events: vec![Event {
                            ev: if trade.side == "sell" {
                                LOCAL_SELL_TRADE_EVENT
                            } else {
                                LOCAL_BUY_TRADE_EVENT
                            },
                            exch_ts: trade.ts.parse().unwrap_or(0) * 1_000_000,
                            local_ts,
                            order_id: 0,
                            px: trade.px.parse().unwrap_or(0.0),
                            qty: trade.sz.parse().unwrap_or(0.0),
                            ival: 0,
                            fval: 0.0,
                        }],
                        stream: None,
                    });
                }
            }
            "funding-rate" => {
                for value in &data.data {
                    let funding: FundingRate = serde_json::from_value(value.clone())?;
                    self.ev_tx.send(PublishEvent::Funding {
                        symbol: funding.inst_id,
                        funding_rate: funding.funding_rate.parse().unwrap_or(0.0),
                        next_funding_time: funding.next_funding_time.parse().unwrap_or(0)
                            * 1_000_000,
                        exch_ts: funding.funding_time.parse().unwrap_or(0) * 1_000_000,
                    });
                }
            }
            "books5" => {
                for value in &data.data {
                    let books: Books5 = serde_json::from_value(value.clone())?;
                    let symbol = data.arg.inst_id.clone().unwrap_or_default();
                    let epoch = {
                        let mut states = self.books.lock().unwrap_or_else(|p| p.into_inner());
                        let state = states.entry(symbol.clone()).or_default();
                        state.epoch = state.epoch.saturating_add(1);
                        state.epoch
                    };
                    let exch_ts = books.ts.parse::<i64>().unwrap_or(0) * 1_000_000;
                    let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                    let mut events = Vec::with_capacity(books.bids.len() + books.asks.len());
                    for level in &books.bids {
                        if level.len() < 2 {
                            continue;
                        }
                        events.push(Event {
                            ev: LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                            exch_ts,
                            local_ts,
                            order_id: 0,
                            px: level[0].parse().unwrap_or(0.0),
                            qty: level[1].parse().unwrap_or(0.0),
                            ival: 0,
                            fval: 0.0,
                        });
                    }
                    for level in &books.asks {
                        if level.len() < 2 {
                            continue;
                        }
                        events.push(Event {
                            ev: LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                            exch_ts,
                            local_ts,
                            order_id: 0,
                            px: level[0].parse().unwrap_or(0.0),
                            qty: level[1].parse().unwrap_or(0.0),
                            ival: 0,
                            fval: 0.0,
                        });
                    }
                    self.ev_tx.send(PublishEvent::FeedBatch {
                        symbol,
                        events,
                        stream: Some(MarketStreamMetadata {
                            epoch,
                            first_update_sequence: 1,
                            last_update_sequence: 1,
                            snapshot: true,
                        }),
                    });
                }
            }
            "bbo-tbt" => {
                for value in &data.data {
                    let bbo: BboTbt = serde_json::from_value(value.clone())?;
                    let symbol = data
                        .arg
                        .inst_id
                        .clone()
                        .filter(|symbol| !symbol.is_empty())
                        .unwrap_or_else(|| bbo.inst_id.clone());
                    let exch_ts = bbo.ts.parse::<i64>().unwrap_or(0) * 1_000_000;
                    let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                    let mut events = Vec::with_capacity(2);
                    if let Some((bid_px, bid_sz)) = bbo.best_bid()
                        && let Ok(bid_px) = bid_px.parse::<f64>()
                        && bid_px > 0.0
                    {
                        events.push(Event {
                            ev: LOCAL_BID_DEPTH_BBO_EVENT,
                            exch_ts,
                            local_ts,
                            order_id: 0,
                            px: bid_px,
                            qty: bid_sz.parse().unwrap_or(0.0),
                            ival: 0,
                            fval: 0.0,
                        });
                    }
                    if let Some((ask_px, ask_sz)) = bbo.best_ask()
                        && let Ok(ask_px) = ask_px.parse::<f64>()
                        && ask_px > 0.0
                    {
                        events.push(Event {
                            ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                            exch_ts,
                            local_ts,
                            order_id: 0,
                            px: ask_px,
                            qty: ask_sz.parse().unwrap_or(0.0),
                            ival: 0,
                            fval: 0.0,
                        });
                    }
                    if !events.is_empty() {
                        self.ev_tx.send(PublishEvent::FeedBatch {
                            symbol,
                            events,
                            stream: None,
                        });
                    }
                }
            }
            "tickers" | "open-interest" | "mark-price" | "index-tickers" | "estimated-price"
            | "liquidation-orders" | "adl-warning" | "status" => {
                debug!(
                    channel = %data.arg.channel,
                    count = data.data.len(),
                    "Extra public channel message."
                );
            }
            channel if channel.starts_with("candle") => {
                debug!(
                    %channel,
                    count = data.data.len(),
                    "Candle stream message."
                );
            }
            channel => {
                debug!(%channel, "Unhandled public channel.");
            }
        }
        Ok(())
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), OkxError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = time::interval_at(
            time::Instant::now() + Duration::from_secs(20),
            Duration::from_secs(20),
        );
        let subscriptions: Vec<_> = self
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|(symbol, kinds)| (symbol.clone(), kinds.iter().copied().collect::<Vec<_>>()))
            .collect();
        for (symbol, kinds) in subscriptions {
            self.subscribe_symbol(&mut write, symbol, &kinds).await?;
        }

        loop {
            select! {
                _ = interval.tick() => {
                    // OKX uses an application-level text heartbeat, not a JSON operation.
                    write.send(Message::Text("ping".into())).await?;
                }
                Some(symbol) = self.resync_rx.recv() => {
                    // A book fell out of sequence or its checksum disagreed. Resubscribing that
                    // one book makes OKX push a fresh snapshot; the connection and every other
                    // symbol on it keep running.
                    self.resubscribe_book(&mut write, symbol).await?;
                }
                msg = self.command_rx.recv() => match msg {
                    Ok(MarketDataCommand::Subscribe { symbol, kinds }) => self.subscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => self.unsubscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
                        let epoch = self.books.lock().unwrap_or_else(|p| p.into_inner())
                            .get(&symbol).map_or(0, |book| book.epoch);
                        self.ev_tx.send(PublishEvent::StreamInvalidated {
                            symbol: symbol.clone(), epoch,
                        });
                        self.resubscribe_book(&mut write, symbol).await?;
                    }
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
                            if text.as_str() == "pong" {
                                continue;
                            }
                            if let Err(error) = self.handle_public_stream(&text).await {
                                error!(?error, %text, "Couldn't handle PublicStreamMsg.");
                                return Err(error);
                            }
                        }
                        Some(Ok(Message::Ping(_))) => {
                            write.send(Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(OkxError::ConnectionAbort(
                                close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                            ));
                        }
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(error)) => {
                            return Err(OkxError::from(error));
                        }
                        None => {
                            return Err(OkxError::ConnectionInterrupted);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::*;
    use crate::connector::{PublishEvent, TestPublishReceiver};
    use tokio::sync::broadcast;

    fn stream() -> (PublicStream, TestPublishReceiver) {
        let (events, receiver) = crate::connector::test_publish_channel();
        let (_commands, command_rx) = broadcast::channel(4);
        (
            PublicStream::new(events, command_rx, Arc::new(Mutex::new(HashMap::new()))),
            receiver,
        )
    }

    fn book(action: &str, sequence: i64, previous: i64, checksum: i64) -> String {
        serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": action,
            "data": [{
                "asks": [["101", "2"]],
                "bids": [["100", "1"]],
                "ts": "1",
                "seqId": sequence,
                "prevSeqId": previous,
                "checksum": checksum
            }]
        })
        .to_string()
    }

    async fn next_stream(receiver: &mut TestPublishReceiver) -> MarketStreamMetadata {
        match receiver.recv().await.unwrap() {
            PublishEvent::FeedBatch {
                stream: Some(stream),
                ..
            } => stream,
            _ => panic!("expected market feed batch"),
        }
    }

    #[tokio::test]
    async fn book_gap_invalidates_and_the_recovery_snapshot_advances_epoch() {
        let (mut stream, mut receiver) = stream();
        stream
            .handle_public_stream(&book("snapshot", 10, -1, 0))
            .await
            .unwrap();
        assert_eq!(
            next_stream(&mut receiver).await,
            MarketStreamMetadata {
                epoch: 1,
                first_update_sequence: 10,
                last_update_sequence: 10,
                snapshot: true,
            }
        );

        stream
            .handle_public_stream(&book("update", 11, 10, 0))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 1);

        // A gap is a lost message, not a broken connection: the handler reports it and asks for
        // a resync instead of tearing the socket down.
        stream
            .handle_public_stream(&book("update", 12, 9, 0))
            .await
            .expect("a sequence gap must not end the connection");
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::StreamInvalidated { epoch: 1, .. })
        ));
        assert_eq!(
            stream.resync_pending(),
            Some("BTC-USDT-SWAP".to_string()),
            "the gap must ask for a fresh snapshot on this connection"
        );

        stream
            .handle_public_stream(&book("snapshot", 20, -1, 0))
            .await
            .unwrap();
        let recovered = next_stream(&mut receiver).await;
        assert!(recovered.snapshot);
        assert_eq!(recovered.epoch, 2);
        assert_eq!(recovered.last_update_sequence, 20);
    }

    #[tokio::test]
    async fn checksum_failure_invalidates_without_reusing_the_epoch() {
        let (mut stream, mut receiver) = stream();
        stream
            .handle_public_stream(&book("snapshot", 10, -1, 1))
            .await
            .expect("a checksum mismatch must not end the connection");
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::StreamInvalidated { epoch: 1, .. })
        ));
        assert_eq!(
            stream.resync_pending(),
            Some("BTC-USDT-SWAP".to_string()),
            "the mismatch must ask for a fresh snapshot on this connection"
        );
        stream
            .handle_public_stream(&book("snapshot", 20, -1, 0))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 2);
    }

    #[tokio::test]
    async fn a_book_update_is_published_unchanged_and_keeps_the_epoch() {
        // The segment a recording writes rotates on an epoch change, so a healthy book that is
        // published frame after frame must never advance the epoch: every frame below is a
        // well-formed update whose predecessor is what the local book holds.
        let (stream, mut receiver) = stream();
        stream
            .handle_public_stream(&book("snapshot", 10, -1, 0))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 1);
        for sequence in 11..16 {
            stream
                .handle_public_stream(&book("update", sequence, sequence - 1, 0))
                .await
                .unwrap();
            let published = next_stream(&mut receiver).await;
            assert_eq!(published.epoch, 1);
            assert!(!published.snapshot);
            assert_eq!(published.last_update_sequence, sequence as u64);
        }
    }

    #[tokio::test]
    async fn an_unreadable_level_resynchronizes_instead_of_shifting_the_book() {
        // A quantity that is not a number used to be read as zero, which *removed* the level: the
        // local book then disagreed with the venue without saying so, and the next checksum
        // invalidated the stream with a cause nobody could see.
        let (mut stream, mut receiver) = stream();
        stream
            .handle_public_stream(&book("snapshot", 10, -1, 0))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 1);
        let broken = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "update",
            "data": [{
                "asks": [["101", "n/a"]],
                "bids": [["100", "1"]],
                "ts": "1",
                "seqId": 11,
                "prevSeqId": 10,
                "checksum": 0
            }]
        })
        .to_string();
        stream.handle_public_stream(&broken).await.unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::StreamInvalidated { epoch: 1, .. })
        ));
        assert_eq!(stream.resync_pending(), Some("BTC-USDT-SWAP".to_string()));
    }

    #[tokio::test]
    async fn a_level_stored_in_the_venues_own_text_keeps_the_checksum_valid() {
        // The checksum is computed over the strings OKX sent. Storing a level in any other form
        // (a parsed and re-rendered float, say) makes the next comparison fail on a book that is
        // actually in step.
        let (stream, mut receiver) = stream();
        stream
            .handle_public_stream(&book("snapshot", 10, -1, 0))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 1);
        let state = stream
            .books
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("BTC-USDT-SWAP")
            .map(|book| (book.bids.clone(), book.asks.clone()))
            .expect("the snapshot's book");
        assert_eq!(state.0.get("100"), Some(&"1".to_string()));
        assert_eq!(state.1.get("101"), Some(&"2".to_string()));
        let checksum = stream
            .books
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("BTC-USDT-SWAP")
            .map(LocalBook::checksum)
            .unwrap();
        // The venue now sends the checksum the local book computes, so nothing is invalidated.
        stream
            .handle_public_stream(&book("update", 11, 10, checksum))
            .await
            .unwrap();
        assert_eq!(next_stream(&mut receiver).await.epoch, 1);
    }

    #[tokio::test]
    async fn a_trade_push_publishes_a_trade_event_with_its_own_side() {
        // The recorder only records trades the adapter recognizes; that starts here, where OKX's
        // `trades` frame becomes an event with the aggressor's side and the venue's time.
        let (stream, mut receiver) = stream();
        // The push names its channel in `arg`; OKX's trade rows carry the instrument id, not the
        // channel name, so a payload without `arg` is not what the handler receives.
        let message = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "tradeId": "1",
                "px": "64757.6",
                "sz": "213.28",
                "side": "sell",
                "ts": "1700000000000"
            }]
        })
        .to_string();
        stream.handle_public_stream(&message).await.unwrap();
        match receiver.recv().await {
            Some(PublishEvent::FeedBatch {
                symbol,
                events,
                stream,
            }) => {
                assert_eq!(symbol, "BTC-USDT-SWAP");
                assert_eq!(events.len(), 1);
                assert!(events[0].is(LOCAL_SELL_TRADE_EVENT));
                assert_eq!(events[0].px, 64_757.6);
                assert_eq!(events[0].qty, 213.28);
                assert_eq!(events[0].exch_ts, 1_700_000_000_000_000_000);
                assert!(stream.is_none(), "a trade advances no book sequence");
            }
            _ => panic!("expected one trade batch"),
        }
    }

    #[tokio::test]
    async fn bbo_uses_the_outer_subscription_symbol_when_rows_omit_inst_id() {
        let (stream, mut receiver) = stream();
        let message = serde_json::json!({
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["101", "2", "0", "1"]],
                "bids": [["100", "1", "0", "1"]],
                "ts": "1"
            }]
        })
        .to_string();
        stream.handle_public_stream(&message).await.unwrap();
        match receiver.recv().await {
            Some(PublishEvent::FeedBatch { symbol, events, .. }) => {
                assert_eq!(symbol, "BTC-USDT-SWAP");
                assert_eq!(events.len(), 2);
                assert!(events[0].is(titan_domain_types::BUY_EVENT));
                assert!(events[1].is(titan_domain_types::SELL_EVENT));
            }
            _ => panic!("expected one atomic BBO batch"),
        }
    }
}
