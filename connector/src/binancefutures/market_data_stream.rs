use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::*;
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, warn};

use crate::{
    binancefutures::{
        BinanceFuturesError,
        msg::{
            rest, stream,
            stream::{EventStream, Stream},
        },
        rest::BinanceFuturesClient,
    },
    connector::PublishEvent,
    utils::{generate_rand_string, parse_depth, parse_px_qty_tup},
};

pub struct MarketDataStream {
    client: BinanceFuturesClient,
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    pending_depth_messages: HashMap<String, Vec<stream::Depth>>,
    prev_u: HashMap<String, i64>,
    rest_tx: UnboundedSender<(String, rest::Depth)>,
    rest_rx: UnboundedReceiver<(String, rest::Depth)>,
}

impl MarketDataStream {
    pub fn new(
        client: BinanceFuturesClient,
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
    ) -> Self {
        let (rest_tx, rest_rx) = unbounded_channel::<(String, rest::Depth)>();
        Self {
            client,
            ev_tx,
            symbol_rx,
            pending_depth_messages: Default::default(),
            prev_u: Default::default(),
            rest_tx,
            rest_rx,
        }
    }

    /// Processes one decoded WebSocket push.
    ///
    /// `ws_recv_ts` is captured immediately after tungstenite yields the text frame, before JSON
    /// decoding.  Keeping that timestamp on every feed event lets a strategy measure the complete
    /// in-process path from WS ingress, through the connector and IPC, to `on_tick`.
    fn process_message(&mut self, stream: EventStream, ws_recv_ts: i64) {
        match stream {
            EventStream::DepthUpdate(data) => {
                let prev_u_val = self.prev_u.get_mut(&data.symbol);
                if prev_u_val.is_none()
                /* fixme: || data.prev_update_id != **prev_u_val.as_ref().unwrap()*/
                {
                    // if !pending_depth_messages.contains_key(&data.symbol) {
                    let client_ = self.client.clone();
                    let symbol = data.symbol.clone();
                    let rest_tx = self.rest_tx.clone();
                    tokio::spawn(async move {
                        let resp = client_.get_depth(&symbol).await;
                        match resp {
                            Ok(depth) => {
                                rest_tx.send((symbol, depth)).unwrap();
                            }
                            Err(error) => {
                                error!(
                                    ?error,
                                    %symbol,
                                    "Couldn't get the market depth via REST."
                                );
                            }
                        }
                    });
                    // }
                    // pending_depth_messages
                    //     .entry(data.symbol.clone())
                    //     .or_insert(Vec::new())
                    //     .push(data);
                    // continue;
                }
                // *prev_u_val.unwrap() = data.last_update_id;
                // fixme: currently supports natural refresh only.
                *self
                    .prev_u
                    .entry(data.symbol.clone())
                    .or_insert(data.last_update_id) = data.last_update_id;

                match parse_depth(data.bids, data.asks) {
                    Ok((bids, asks)) => {
                        let mut events = Vec::with_capacity(bids.len() + asks.len());
                        for (px, qty) in bids {
                            events.push(Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts: data.transaction_time * 1_000_000,
                                local_ts: ws_recv_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            });
                        }
                        for (px, qty) in asks {
                            events.push(Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts: data.transaction_time * 1_000_000,
                                local_ts: ws_recv_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            });
                        }
                        self.ev_tx
                            .send(PublishEvent::FeedBatch {
                                symbol: data.symbol,
                                events,
                            })
                            .unwrap();
                    }
                    Err(error) => {
                        error!(?error, "Couldn't parse DepthUpdate stream.");
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
            EventStream::Trade(data) => match parse_px_qty_tup(data.price, data.qty) {
                Ok((px, qty)) => {
                    if data.type_ != "MARKET" {
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
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            }],
                        })
                        .unwrap();
                }
                Err(e) => {
                    error!(error = ?e, "Couldn't parse trade stream.");
                }
            },
            EventStream::BookTicker(data) => {
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
                        })
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
    }

    fn process_snapshot(&self, symbol: String, data: rest::Depth) {
        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                let mut events = Vec::with_capacity(bids.len() + asks.len());
                for (px, qty) in bids {
                    events.push(Event {
                        ev: LOCAL_BID_DEPTH_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    });
                }
                for (px, qty) in asks {
                    events.push(Event {
                        ev: LOCAL_ASK_DEPTH_EVENT,
                        exch_ts: data.transaction_time * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    });
                }
                self.ev_tx
                    .send(PublishEvent::FeedBatch { symbol, events })
                    .unwrap();
            }
            Err(error) => {
                error!(?error, "Couldn't parse Depth response.");
            }
        }
        // fixme: waits for pending messages without blocking.
        // prev_u.remove(&symbol);
        // let mut new_prev_u: Option<i64> = None;
        // while new_prev_u.is_none() {
        //     if let Some(msg) = pending_depth_messages.get_mut(&symbol) {
        //         for pending_depth in msg.into_iter() {
        //             // https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
        //             // The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
        //             if (
        //                 pending_depth.last_update_id < resp.last_update_id
        //                 || pending_depth.first_update_id > resp.last_update_id
        //             ) && new_prev_u.is_none() {
        //                 continue;
        //             }
        //             if new_prev_u.is_some() && pending_depth.prev_update_id != *new_prev_u.as_ref().unwrap() {
        //                 warn!(%symbol, ?pending_depth, "UpdateId does not match.");
        //             }
        //
        //             // Processes a pending depth message
        //             new_prev_u = Some(pending_depth.last_update_id);
        //             *prev_u.entry(symbol.clone())
        //                 .or_insert(pending_depth.last_update_id) = pending_depth.last_update_id;
        //         }
        //     }
        //     if new_prev_u.is_none() {
        //         // Waits for depth messages.
        //         todo!()
        //     }
        // }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BinanceFuturesError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();

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
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        let id = generate_rand_string(16);
                        write.send(Message::Text(format!(r#"{{
                            "method": "SUBSCRIBE",
                            "params": [
                                "{symbol}@trade",
                                "{symbol}@depth@0ms",
                                "{symbol}@markPrice",
                                "{symbol}@bookTicker"
                            ],
                            "id": "{id}"
                        }}"#).into())).await?;
                    }
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

    /// 实盘冒烟：连接 Binance USD-M 测试网 WS，订阅 btcusdt 的
    /// trade/depth@0ms/markPrice/bookTicker，验证收到深度、成交、资金费与 BBO 事件。
    /// 运行：`cargo test --all-features binancefutures::market_data_stream::tests::live_ws -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_ws() {
        let client = BinanceFuturesClient::new("https://testnet.binancefuture.com", "", "");
        let (ev_tx, mut ev_rx) = unbounded_channel::<PublishEvent>();
        let (symbol_tx, _) = tokio::sync::broadcast::channel(16);
        let mut stream = MarketDataStream::new(client, ev_tx, symbol_tx.subscribe());

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

        // 注册标的，触发订阅
        symbol_tx.send("btcusdt".to_string()).unwrap();

        let mut feed_depth = 0usize;
        let mut feed_bbo = 0usize;
        let mut feed_trade = 0usize;
        let mut funding = 0usize;
        let mut batch = 0usize;

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
                Ok(Some(PublishEvent::BatchStart(_))) | Ok(Some(PublishEvent::BatchEnd(_))) => {
                    batch += 1;
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_elapsed) => {
                    eprintln!(
                        "no event for 3s (depth={feed_depth} bbo={feed_bbo} trade={feed_trade} funding={funding})"
                    );
                    if feed_depth > 0 || funding > 0 {
                        break;
                    }
                }
            }
        }

        handle.abort();
        println!(
            "depth={feed_depth} bbo={feed_bbo} trade={feed_trade} funding={funding} batch={batch}"
        );
        assert!(feed_depth > 0, "no depth feed events received");
        assert!(
            feed_trade > 0 || feed_bbo > 0,
            "no trade/BBO feed events received"
        );
        assert!(funding > 0, "no markPrice/funding events received");
    }
}
