use std::{collections::HashMap, sync::Mutex, time::Duration};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT, LiveEvent,
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
}

#[derive(Default)]
struct LocalBook {
    bids: HashMap<String, String>,
    asks: HashMap<String, String>,
    epoch: u64,
    last_sequence: i64,
}

impl LocalBook {
    fn apply(side: &mut HashMap<String, String>, levels: &[Vec<String>]) {
        for level in levels {
            if level.len() < 2 {
                continue;
            }
            if level[1].parse::<f64>().unwrap_or(0.0) == 0.0 {
                side.remove(&level[0]);
            } else {
                side.insert(level[0].clone(), level[1].clone());
            }
        }
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
        Self {
            ev_tx,
            command_rx,
            subscriptions,
            books: Mutex::new(HashMap::new()),
        }
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
                        let epoch = state.epoch;
                        states.remove(&symbol);
                        let _ = self
                            .ev_tx
                            .send(PublishEvent::StreamInvalidated { symbol, epoch });
                        return Err(OkxError::ConnectionInterrupted);
                    }
                    LocalBook::apply(&mut state.bids, &books.bids);
                    LocalBook::apply(&mut state.asks, &books.asks);
                    if books.checksum != 0 && state.checksum() != books.checksum {
                        let epoch = state.epoch;
                        states.remove(&symbol);
                        let _ = self
                            .ev_tx
                            .send(PublishEvent::StreamInvalidated { symbol, epoch });
                        return Err(OkxError::ConnectionInterrupted);
                    }
                    state.last_sequence = books.seq_id;
                    state.epoch
                };
                let mut events = Vec::with_capacity(books.bids.len() + books.asks.len());
                for level in &books.bids {
                    if level.len() < 2 {
                        continue;
                    }
                    events.push(Event {
                        ev: if snapshot {
                            LOCAL_BID_DEPTH_SNAPSHOT_EVENT
                        } else {
                            LOCAL_BID_DEPTH_EVENT
                        },
                        exch_ts,
                        local_ts,
                        order_id: books.prev_seq_id as u64,
                        px: level[0].parse().unwrap_or(0.0),
                        qty: level[1].parse().unwrap_or(0.0),
                        ival: books.seq_id,
                        fval: 0.0,
                    });
                }
                for level in &books.asks {
                    if level.len() < 2 {
                        continue;
                    }
                    events.push(Event {
                        ev: if snapshot {
                            LOCAL_ASK_DEPTH_SNAPSHOT_EVENT
                        } else {
                            LOCAL_ASK_DEPTH_EVENT
                        },
                        exch_ts,
                        local_ts,
                        order_id: books.prev_seq_id as u64,
                        px: level[0].parse().unwrap_or(0.0),
                        qty: level[1].parse().unwrap_or(0.0),
                        ival: books.seq_id,
                        fval: 0.0,
                    });
                }
                self.ev_tx
                    .send(PublishEvent::FeedBatch {
                        symbol: data.arg.inst_id.clone().unwrap_or_default(),
                        events,
                        stream: Some(MarketStreamMetadata {
                            epoch,
                            first_update_sequence: u64::try_from(books.seq_id).unwrap_or(0),
                            last_update_sequence: u64::try_from(books.seq_id).unwrap_or(0),
                            snapshot,
                        }),
                    })
                    .unwrap();
            }
            "trades" => {
                let trades: Vec<Trade> = data
                    .data
                    .iter()
                    .map(|v| serde_json::from_value(v.clone()))
                    .collect::<Result<_, _>>()?;
                let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                for trade in trades {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: trade.inst_id.clone(),
                            event: Event {
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
                            },
                        }))
                        .unwrap();
                }
            }
            "funding-rate" => {
                for value in &data.data {
                    let funding: FundingRate = serde_json::from_value(value.clone())?;
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Funding {
                            symbol: funding.inst_id,
                            funding_rate: funding.funding_rate.parse().unwrap_or(0.0),
                            next_funding_time: funding.next_funding_time.parse().unwrap_or(0)
                                * 1_000_000,
                            exch_ts: funding.funding_time.parse().unwrap_or(0) * 1_000_000,
                        }))
                        .unwrap();
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
                    self.ev_tx
                        .send(PublishEvent::FeedBatch {
                            symbol,
                            events,
                            stream: Some(MarketStreamMetadata {
                                epoch,
                                first_update_sequence: 1,
                                last_update_sequence: 1,
                                snapshot: true,
                            }),
                        })
                        .unwrap();
                }
            }
            "bbo-tbt" => {
                for value in &data.data {
                    let bbo: BboTbt = serde_json::from_value(value.clone())?;
                    let exch_ts = bbo.ts.parse::<i64>().unwrap_or(0) * 1_000_000;
                    let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                    if let Ok(bid_px) = bbo.bid_px.parse::<f64>() {
                        if bid_px > 0.0 {
                            self.ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                    symbol: bbo.inst_id.clone(),
                                    event: Event {
                                        ev: LOCAL_BID_DEPTH_BBO_EVENT,
                                        exch_ts,
                                        local_ts,
                                        order_id: 0,
                                        px: bid_px,
                                        qty: bbo.bid_sz.parse().unwrap_or(0.0),
                                        ival: 0,
                                        fval: 0.0,
                                    },
                                }))
                                .unwrap();
                        }
                    }
                    if let Ok(ask_px) = bbo.ask_px.parse::<f64>() {
                        if ask_px > 0.0 {
                            self.ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                    symbol: bbo.inst_id,
                                    event: Event {
                                        ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                                        exch_ts,
                                        local_ts,
                                        order_id: 0,
                                        px: ask_px,
                                        qty: bbo.ask_sz.parse().unwrap_or(0.0),
                                        ival: 0,
                                        fval: 0.0,
                                    },
                                }))
                                .unwrap();
                        }
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
        let mut interval = time::interval(Duration::from_secs(20));
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
                    let op = WsRequest {
                        op: "ping".to_string(),
                        args: vec![],
                    };
                    let s = serde_json::to_string(&op).unwrap();
                    write.send(Message::Text(s.into())).await?;
                }
                msg = self.command_rx.recv() => match msg {
                    Ok(MarketDataCommand::Subscribe { symbol, kinds }) => self.subscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Unsubscribe { symbol, kinds }) => self.unsubscribe_symbol(&mut write, symbol, &kinds).await?,
                    Ok(MarketDataCommand::Snapshot { symbol }) => {
                        let epoch = self.books.lock().unwrap_or_else(|p| p.into_inner())
                            .get(&symbol).map_or(0, |book| book.epoch);
                        let _ = self.ev_tx.send(PublishEvent::StreamInvalidated {
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
