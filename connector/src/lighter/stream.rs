use super::*;
use crate::connector::{AccountPublication, MarketStreamMetadata, PublishEvent, PublishSender};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use titan_market_service::MarketDataKind;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Heartbeat {
    next_ping: tokio::time::Instant,
    last_received: tokio::time::Instant,
}
impl Heartbeat {
    fn new() -> Self {
        let now = tokio::time::Instant::now();
        Self {
            next_ping: now + Duration::from_secs(30),
            last_received: now,
        }
    }
}
async fn receive(ws: &mut Socket, heartbeat: &mut Heartbeat) -> Result<Value, ApiError> {
    loop {
        let now = tokio::time::Instant::now();
        if now.duration_since(heartbeat.last_received) >= Duration::from_secs(70) {
            return Err(error(NAME, "WS heartbeat timeout"));
        }
        if now >= heartbeat.next_ping {
            ws.send(Message::Ping(Vec::new().into()))
                .await
                .map_err(|e| websocket_error(NAME, e))?;
            heartbeat.next_ping = now + Duration::from_secs(30);
        }
        let deadline = heartbeat
            .next_ping
            .min(heartbeat.last_received + Duration::from_secs(70));
        let frame = match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(frame) => frame,
            Err(_) => continue,
        };
        let m = frame
            .ok_or_else(|| ApiError::transport(NAME, "WS closed"))?
            .map_err(|e| websocket_error(NAME, e))?;
        heartbeat.last_received = tokio::time::Instant::now();
        match m {
            Message::Text(raw) => {
                let v: Value =
                    serde_json::from_str(&raw).map_err(|_| error(NAME, "invalid WS JSON"))?;
                if v["type"] == "ping" {
                    ws.send(Message::Text(json!({"type":"pong"}).to_string().into()))
                        .await
                        .map_err(|_| ApiError::transport(NAME, "pong failed"))?;
                    continue;
                }
                if v["type"] == "error" || v.get("error").is_some() {
                    return Err(error(NAME, "WS request rejected"));
                }
                return Ok(v);
            }
            Message::Ping(p) => ws
                .send(Message::Pong(p))
                .await
                .map_err(|_| ApiError::transport(NAME, "pong failed"))?,
            Message::Close(_) => return Err(ApiError::transport(NAME, "WS closed")),
            _ => {}
        }
    }
}
async fn connect(c: &LighterClient, private: bool) -> Result<Socket, ApiError> {
    let url = if private {
        c.config.ws_url.clone()
    } else {
        format!("{}?readonly=true", c.config.ws_url)
    };
    tokio::time::timeout(Duration::from_secs(10), connect_async(&url))
        .await
        .map_err(|_| error(NAME, "WS connect timeout"))?
        .map(|v| v.0)
        .map_err(|e| websocket_error(NAME, e))
}
async fn subscribe(ws: &mut Socket, channel: &str, auth: Option<&str>) -> Result<(), ApiError> {
    let mut v = json!({"type":"subscribe","channel":channel});
    if let Some(auth) = auth {
        v["auth"] = json!(auth);
    }
    ws.send(Message::Text(v.to_string().into()))
        .await
        .map_err(|_| ApiError::transport(NAME, "WS subscription failed"))
}
pub(super) async fn snapshot(
    c: &LighterClient,
    channel: &str,
    index: i64,
) -> Result<Value, ApiError> {
    let mut ws = connect(c, false).await?;
    let mut heartbeat = Heartbeat::new();
    subscribe(&mut ws, &format!("{channel}/{index}"), None).await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let v = receive(&mut ws, &mut heartbeat).await?;
            if v["channel"] == format!("{channel}:{index}")
                && v["type"] == format!("subscribed/{channel}")
            {
                return Ok(v);
            }
        }
    })
    .await
    .map_err(|_| error(NAME, "snapshot confirmation timeout"))?
}
#[derive(Default)]
pub(super) struct DepthSequence {
    pub nonce: Option<u64>,
}
impl DepthSequence {
    pub fn accept(&mut self, snapshot: bool, begin: u64, end: u64) -> Result<(), ApiError> {
        if !snapshot && (self.nonce != Some(begin) || end < begin) {
            return Err(error(NAME, "book nonce gap or missing snapshot"));
        }
        self.nonce = Some(end);
        Ok(())
    }
}
pub(super) async fn session(
    c: &LighterClient,
    desired: Desired,
    orders: Arc<Mutex<Orders>>,
    tx: PublishSender,
    private: bool,
    epoch: u64,
) -> Result<(), ApiError> {
    let auth = if private {
        Some(c.signer()?.auth()?)
    } else {
        None
    };
    session_with_auth(c, desired, orders, tx, auth.as_deref(), epoch).await
}
async fn session_with_auth(
    c: &LighterClient,
    desired: Desired,
    orders: Arc<Mutex<Orders>>,
    tx: PublishSender,
    auth: Option<&str>,
    epoch: u64,
) -> Result<(), ApiError> {
    let private = auth.is_some();
    let markets = c.markets().await?;
    let mut bound = HashMap::new();
    for symbol in if private {
        desired.accounts.iter().collect::<Vec<_>>()
    } else {
        desired.market.keys().collect::<Vec<_>>()
    } {
        let m = markets
            .iter()
            .find(|m| m.info.symbol == *symbol && m.info.tradable)
            .ok_or_else(|| invalid(NAME, "unknown bound perpetual"))?;
        bound.insert(m.index, m.clone());
    }
    let mut ws = connect(c, private).await?;
    let mut heartbeat = Heartbeat::new();
    let mut pending = HashSet::new();
    let mut sequences = HashMap::new();
    if private {
        for channel in [
            "account_all_orders",
            "account_all_trades",
            "account_all_positions",
            "user_stats",
        ] {
            subscribe(
                &mut ws,
                &format!("{channel}/{}", c.config.account_index),
                auth,
            )
            .await?;
            pending.insert(channel.to_owned());
        }
    } else {
        let mut channels = HashSet::new();
        for m in bound.values() {
            for k in desired.market.get(&m.info.symbol).unwrap() {
                let channel = match k {
                    MarketDataKind::Depth => "order_book",
                    MarketDataKind::Bbo => "ticker",
                    MarketDataKind::Trades => "trade",
                    MarketDataKind::MarkPrice | MarketDataKind::FundingRate => "market_stats",
                    _ => return Err(unsupported(NAME, "ticker ABI")),
                };
                channels.insert(format!("{channel}/{}", m.index));
            }
        }
        for channel in channels {
            subscribe(&mut ws, &channel, None).await?;
        }
    }
    let startup = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let v = if private && !pending.is_empty() {
            tokio::time::timeout_at(startup, receive(&mut ws, &mut heartbeat))
                .await
                .map_err(|_| error(NAME, "private stream confirmation incomplete"))??
        } else {
            // Auth belongs to the subscribe request, not a client-side socket lifetime.
            // Keep healthy sessions alive; a real reconnect signs fresh subscriptions.
            receive(&mut ws, &mut heartbeat).await?
        };
        let Some(channel) = v["channel"].as_str() else {
            continue;
        };
        let Some((kind, index)) = channel.split_once(':') else {
            continue;
        };
        let index: i64 = index
            .parse()
            .map_err(|_| error(NAME, "invalid WS channel identity"))?;
        if private {
            if index != c.config.account_index {
                return Err(error(NAME, "private account channel mismatch"));
            }
            private_frame(c, kind, &v, &bound, &orders, &tx)?;
            if v["type"] == format!("subscribed/{kind}")
                && pending.remove(kind)
                && pending.is_empty()
            {
                tx.send(PublishEvent::PrivateStreamReady);
            }
        } else {
            let Some(m) = bound.get(&index) else {
                return Err(error(NAME, "unbound public market"));
            };
            public(kind, &v, m, &desired, &mut sequences, &tx, epoch)?;
        }
    }
}
fn public(
    kind: &str,
    v: &Value,
    m: &Market,
    d: &Desired,
    sequences: &mut HashMap<i64, DepthSequence>,
    tx: &PublishSender,
    epoch: u64,
) -> Result<(), ApiError> {
    let symbol = &m.info.symbol;
    let kinds = &d.market[symbol];
    match kind {
        "order_book" if kinds.contains(&MarketDataKind::Depth) => {
            let b = &v["order_book"];
            let snapshot = v["type"] == "subscribed/order_book";
            let end = integer(b, "nonce", NAME)?
                .try_into()
                .map_err(|_| error(NAME, "negative nonce"))?;
            let begin = if snapshot {
                end
            } else {
                integer(b, "begin_nonce", NAME)?
                    .try_into()
                    .map_err(|_| error(NAME, "negative begin_nonce"))?
            };
            sequences
                .entry(m.index)
                .or_default()
                .accept(snapshot, begin, end)?;
            let parse = |key| -> Result<Vec<(String, String)>, ApiError> {
                rows(&b[key], NAME)?
                    .iter()
                    .map(|r| Ok((text(r, "price", NAME)?, text(r, "size", NAME)?)))
                    .collect()
            };
            publish_depth(
                symbol,
                &parse("bids")?,
                &parse("asks")?,
                timestamp(b, "last_updated_at", 1000, NAME)?,
                MarketStreamMetadata {
                    epoch,
                    first_update_sequence: if snapshot {
                        end
                    } else {
                        begin.saturating_add(1)
                    },
                    last_update_sequence: end,
                    snapshot,
                },
                tx,
            )?;
        }
        "ticker" if kinds.contains(&MarketDataKind::Bbo) => {
            let t = &v["ticker"];
            publish_bbo(
                symbol,
                number(&t["b"], "price", NAME)?,
                number(&t["b"], "size", NAME)?,
                number(&t["a"], "price", NAME)?,
                number(&t["a"], "size", NAME)?,
                timestamp(t, "last_updated_at", 1000, NAME)?,
                tx,
            )?;
        }
        "trade" if kinds.contains(&MarketDataKind::Trades) && v["type"] == "update/trade" => {
            for key in ["trades", "liquidation_trades"] {
                if v[key].is_null() {
                    continue;
                }
                for r in rows(&v[key], NAME)? {
                    if integer(r, "market_id", NAME)? != m.index {
                        return Err(error(NAME, "trade market mismatch"));
                    }
                    publish_trade(
                        symbol,
                        number(r, "price", NAME)?,
                        number(r, "size", NAME)?,
                        !r["is_maker_ask"]
                            .as_bool()
                            .ok_or_else(|| error(NAME, "missing maker side"))?,
                        timestamp(r, "transaction_time", 1000, NAME)?,
                        tx,
                    )?;
                }
            }
        }
        "market_stats" => {
            let s = &v["market_stats"];
            let ts = timestamp(v, "timestamp", 1_000_000, NAME)?;
            if kinds.contains(&MarketDataKind::MarkPrice) {
                tx.send(PublishEvent::MarkPrice {
                    symbol: symbol.clone(),
                    mark_price: number(s, "mark_price", NAME)?,
                    exch_ts: ts,
                });
            }
            if kinds.contains(&MarketDataKind::FundingRate) {
                tx.send(PublishEvent::Funding {
                    symbol: symbol.clone(),
                    funding_rate: number(s, "current_funding_rate", NAME)? / 100.0,
                    next_funding_time: next_funding(s)?
                        .checked_mul(1_000_000)
                        .ok_or_else(|| error(NAME, "funding timestamp overflow"))?,
                    exch_ts: ts,
                });
            }
        }
        _ => {}
    }
    Ok(())
}

fn private_frame(
    c: &LighterClient,
    kind: &str,
    v: &Value,
    bound: &HashMap<i64, Market>,
    orders: &Mutex<Orders>,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    match kind {
        "account_all_orders" => {
            if let Some(all) = v["orders"].as_object() {
                for (index, values) in all {
                    let index = index
                        .parse::<i64>()
                        .map_err(|_| error(NAME, "invalid order market"))?;
                    if let Some(m) = bound.get(&index) {
                        for r in rows(values, NAME)? {
                            let o = c.order(r, m)?;
                            publish_order(&o, m.info.tick_size, orders, tx)?;
                        }
                    }
                }
            } else if !v["orders"].is_null() {
                return Err(error(NAME, "invalid orders image"));
            }
        }
        "account_all_trades" => {
            if let Some(all) = v["trades"].as_object() {
                for (index, values) in all {
                    let index = index
                        .parse::<i64>()
                        .map_err(|_| error(NAME, "invalid trade market"))?;
                    if let Some(m) = bound.get(&index) {
                        for r in rows(values, NAME)? {
                            if integer(r, "market_id", NAME)? != m.index {
                                return Err(error(NAME, "private trade market mismatch"));
                            }
                            tx.send_account(AccountPublication::NativeFill {
                                position_side: ApiPositionSide::Net,
                                fill: c.fill(r, m)?,
                            });
                        }
                    }
                }
            } else if !v["trades"].is_null() && v["trades"] != json!([]) {
                return Err(error(NAME, "invalid trades image"));
            }
        }
        "account_all_positions" => {
            // The initial image may have no executed transaction. Publication time is a
            // different clock, so preserve unknown exchange time as zero.
            let ts = if v["transaction_time"].is_null() {
                0
            } else {
                timestamp(v, "transaction_time", 1, NAME)? / 1000
            };
            if let Some(all) = v["positions"].as_object() {
                for (index, p) in all {
                    let index = index
                        .parse::<i64>()
                        .map_err(|_| error(NAME, "invalid position market"))?;
                    if let Some(m) = bound.get(&index) {
                        let p = position(p, ts)?;
                        if p.symbol != m.info.symbol {
                            return Err(error(NAME, "private position symbol mismatch"));
                        }
                        tx.send_account(AccountPublication::Position {
                            symbol: p.symbol,
                            position_side: ApiPositionSide::Net,
                            qty: p.qty,
                            entry_price: Some(p.entry_price),
                            unrealized_pnl: Some(p.unrealized_pnl),
                            liquidation_price: Some(p.liquidation_price),
                            exch_ts: p
                                .update_time
                                .checked_mul(1_000_000)
                                .ok_or_else(|| error(NAME, "position timestamp overflow"))?,
                        });
                    }
                }
            } else {
                return Err(error(NAME, "invalid position image"));
            }
        }
        "user_stats" => {
            let s = &v["stats"];
            if integer(s, "account_trading_mode", NAME)? != 0 {
                return Err(unsupported(NAME, "unified trading account"));
            }
            let wallet = number(s, "collateral", NAME)?;
            let margin = number(s, "portfolio_value", NAME)?;
            tx.send_account(AccountPublication::Balance {
                currency: "USDC".into(),
                wallet,
                available: Some(number(s, "available_balance", NAME)?),
                margin: Some(margin),
                unrealized_pnl: Some(margin - wallet),
                exch_ts: timestamp(v, "timestamp", 1_000_000, NAME)?,
            });
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{DirectPublication, direct_publish_sender};
    use crate::venue::test_support::http;
    use tokio::{net::TcpListener, sync::mpsc};

    fn market() -> Value {
        json!({"code":200,"order_book_details":[{
            "market_id":0,"symbol":"ETH","market_type":"perp","status":"active",
            "supported_price_decimals":1,"supported_size_decimals":3,
            "size_decimals":3,"price_decimals":1,"min_base_amount":"0.001"
        }]})
    }
    fn stats(kind: &str) -> Value {
        json!({"channel":"user_stats:42","type":kind,"timestamp":1700000000000i64,
            "stats":{"account_trading_mode":0,"collateral":"0","portfolio_value":"0","available_balance":"0"}})
    }
    fn book(step: u64) -> Value {
        json!({"channel":"order_book:0","type":if step==0{"subscribed/order_book"}else{"update/order_book"},
            "order_book":{"nonce":step+1,"begin_nonce":step,"last_updated_at":1700000000000000i64,
            "bids":[{"price":"100.0","size":"1.000"}],"asks":[{"price":"100.1","size":"1.000"}]}})
    }
    async fn healthy_session_past_token_and_old_socket_deadlines(private: bool) {
        let (url, mut requests, http_task) = http(vec![(200, market())]).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = LighterClient::new(Config {
            rest_url: url,
            ws_url: format!("ws://{}/stream", listener.local_addr().unwrap()),
            account_index: 42,
            ..Default::default()
        })
        .unwrap();
        let (poke, mut pokes) = mpsc::channel::<u64>(1);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            if private {
                for channel in [
                    "account_all_orders",
                    "account_all_trades",
                    "account_all_positions",
                    "user_stats",
                ] {
                    let request: Value =
                        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                            .unwrap();
                    assert_eq!(request["channel"], format!("{channel}/42"));
                    assert_eq!(request["auth"], "fixture-auth");
                }
                for (channel, key) in [
                    ("account_all_orders", "orders"),
                    ("account_all_trades", "trades"),
                    ("account_all_positions", "positions"),
                ] {
                    let mut frame = json!({"channel":format!("{channel}:42"),"type":format!("subscribed/{channel}")});
                    frame[key] = json!({}); // No transaction_time for an untouched account.
                    ws.send(Message::Text(frame.to_string().into()))
                        .await
                        .unwrap();
                }
                ws.send(Message::Text(
                    stats("subscribed/user_stats").to_string().into(),
                ))
                .await
                .unwrap();
            } else {
                let request: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(request["channel"], "order_book/0");
                assert!(request["auth"].is_null());
                ws.send(Message::Text(book(0).to_string().into()))
                    .await
                    .unwrap();
            }
            loop {
                tokio::select! {
                    step=pokes.recv()=> {
                        let Some(step)=step else {break};
                        let frame=if private {
                            // A duplicate ack must not reopen/rebootstrap the account gate.
                            stats(if step==1{"subscribed/user_stats"}else{"update/user_stats"})
                        } else {book(step)};
                        ws.send(Message::Text(frame.to_string().into())).await.unwrap();
                    }
                    message=ws.next()=> {
                        match message.unwrap().unwrap() {
                            Message::Ping(p)=>ws.send(Message::Pong(p)).await.unwrap(),
                            Message::Pong(_)=>{},
                            other=>panic!("unexpected client message: {other:?}"),
                        }
                    }
                }
            }
            ws.close(None).await.unwrap();
        });
        let (events, mut observed) = mpsc::unbounded_channel();
        let publisher = direct_publish_sender(move |publication| {
            let event = match publication {
                DirectPublication::Event(PublishEvent::PrivateStreamReady) => "ready",
                DirectPublication::Event(PublishEvent::FeedBatch { .. }) => "depth",
                DirectPublication::NativeMarket(crate::connector::NativeMarketBatch::Depth {
                    ..
                }) => "depth",
                DirectPublication::Account(AccountPublication::Balance { .. }) => "balance",
                _ => return,
            };
            events.send(event).unwrap();
        });
        let mut desired = Desired::default();
        if private {
            desired.accounts.insert("ETH".into());
        } else {
            desired
                .market
                .entry("ETH".into())
                .or_default()
                .insert(MarketDataKind::Depth);
        }
        let running = tokio::spawn(async move {
            session_with_auth(
                &client,
                desired,
                Arc::new(Mutex::new(Orders::default())),
                publisher,
                private.then_some("fixture-auth"),
                1,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            if private {
                assert_eq!(observed.recv().await, Some("balance"));
            }
            assert_eq!(
                observed.recv().await,
                Some(if private { "ready" } else { "depth" })
            );
        })
        .await
        .unwrap();
        requests.recv().await.unwrap();
        http_task.await.unwrap();
        // Advance only after real loopback I/O completes, then resume for each next frame.
        // This exercises 12 minutes without either a slow test or virtual-clock I/O races.
        for step in 1..=24 {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(30)).await;
            tokio::time::resume();
            poke.send(step).await.unwrap();
            let event = tokio::time::timeout(Duration::from_secs(5), observed.recv())
                .await
                .unwrap();
            assert_eq!(event, Some(if private { "balance" } else { "depth" }));
            assert!(
                !running.is_finished(),
                "healthy socket ended after {} seconds",
                step * 30
            );
        }
        drop(poke);
        server.await.unwrap();
        // A genuine server disconnect must still terminate the session for reconciliation.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), running)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
    #[tokio::test]
    async fn public_session_stays_connected_past_500_seconds() {
        healthy_session_past_token_and_old_socket_deadlines(false).await;
    }
    #[tokio::test]
    async fn private_session_stays_ready_past_500_seconds_and_emits_ready_once() {
        healthy_session_past_token_and_old_socket_deadlines(true).await;
    }
    #[tokio::test]
    async fn positions_keep_absent_execution_time_unknown_and_reject_invalid_values() {
        let (url, mut requests, task) = http(vec![(200, market())]).await;
        let client = LighterClient::new(Config {
            rest_url: url,
            ..Default::default()
        })
        .unwrap();
        let m = client.markets().await.unwrap().remove(0);
        let bound = HashMap::from([(m.index, m)]);
        let times = Arc::new(Mutex::new(Vec::new()));
        let captured = times.clone();
        let tx = direct_publish_sender(move |publication| {
            if let DirectPublication::Account(AccountPublication::Position { exch_ts, .. }) =
                publication
            {
                captured.lock().unwrap().push(*exch_ts);
            }
        });
        let orders = Mutex::new(Orders::default());
        let mut frame = json!({"positions":{},"timestamp":1793400000000i64});
        private_frame(
            &client,
            "account_all_positions",
            &frame,
            &bound,
            &orders,
            &tx,
        )
        .unwrap();
        frame["positions"] = json!({"0":{"symbol":"ETH","position":"1","sign":1,
            "avg_entry_price":"100.1","position_value":"100.2","initial_margin_fraction":"500",
            "margin_mode":0,"unrealized_pnl":"0.1","realized_pnl":"0","liquidation_price":"50"}});
        private_frame(
            &client,
            "account_all_positions",
            &frame,
            &bound,
            &orders,
            &tx,
        )
        .unwrap();
        for (value, valid) in [
            (Value::Null, true),
            (json!(0), true),
            (json!(1700000000123456i64), true),
            (json!(-1), false),
            (json!("bad"), false),
        ] {
            frame["transaction_time"] = value;
            assert_eq!(
                private_frame(
                    &client,
                    "account_all_positions",
                    &frame,
                    &bound,
                    &orders,
                    &tx
                )
                .is_ok(),
                valid
            );
        }
        assert_eq!(
            *times.lock().unwrap(),
            vec![0, 0, 0, 1700000000123000000i64]
        );
        requests.recv().await.unwrap();
        task.await.unwrap();
    }
}
