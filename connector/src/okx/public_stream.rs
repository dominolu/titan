use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT, LiveEvent,
};
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    connector::PublishEvent,
    okx::{
        OkxError, SharedSymbolSet,
        msg::{
            rest::Books,
            stream::{BboTbt, Books5, DataMsg, FundingRate, StreamMsg, Trade, WsArg, WsRequest},
        },
    },
};

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    symbols: SharedSymbolSet,
}

impl PublicStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        symbols: SharedSymbolSet,
    ) -> Self {
        Self {
            ev_tx,
            symbol_rx,
            symbols,
        }
    }

    async fn subscribe_symbol(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbol: String,
    ) -> Result<(), OkxError> {
        let op = WsRequest {
            op: "subscribe".to_string(),
            args: vec![
                WsArg {
                    channel: "books".to_string(),
                    inst_id: Some(symbol.clone()),
                    inst_type: None,
                },
                WsArg {
                    channel: "trades".to_string(),
                    inst_id: Some(symbol.clone()),
                    inst_type: None,
                },
                WsArg {
                    channel: "funding-rate".to_string(),
                    inst_id: Some(symbol),
                    inst_type: None,
                },
            ],
        };
        let s = serde_json::to_string(&op).unwrap();
        write.send(Message::Text(s.into())).await?;
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

                for level in &books.bids {
                    if level.len() < 2 {
                        continue;
                    }
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: data.arg.inst_id.clone().unwrap_or_default(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px: level[0].parse().unwrap_or(0.0),
                                qty: level[1].parse().unwrap_or(0.0),
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
                for level in &books.asks {
                    if level.len() < 2 {
                        continue;
                    }
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: data.arg.inst_id.clone().unwrap_or_default(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px: level[0].parse().unwrap_or(0.0),
                                qty: level[1].parse().unwrap_or(0.0),
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
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
                    let exch_ts = books.ts.parse::<i64>().unwrap_or(0) * 1_000_000;
                    let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
                    for level in &books.bids {
                        if level.len() < 2 {
                            continue;
                        }
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.arg.inst_id.clone().unwrap_or_default(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts,
                                    local_ts,
                                    order_id: 0,
                                    px: level[0].parse().unwrap_or(0.0),
                                    qty: level[1].parse().unwrap_or(0.0),
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
                    for level in &books.asks {
                        if level.len() < 2 {
                            continue;
                        }
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.arg.inst_id.clone().unwrap_or_default(),
                                event: Event {
                                    ev: LOCAL_ASK_DEPTH_EVENT,
                                    exch_ts,
                                    local_ts,
                                    order_id: 0,
                                    px: level[0].parse().unwrap_or(0.0),
                                    qty: level[1].parse().unwrap_or(0.0),
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
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

        // Replays every registered symbol after (re)connect: a fresh broadcast receiver only
        // delivers symbols registered after subscription, so the shared set is the durable source.
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        for symbol in symbols {
            self.subscribe_symbol(&mut write, symbol).await?;
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
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        self.subscribe_symbol(&mut write, symbol).await?;
                    }
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
