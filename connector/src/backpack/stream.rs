use super::*;
use crate::connector::{AccountPublication, MarketStreamMetadata, PublishEvent, PublishSender};
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, time::Duration};
use titan_market_service::MarketDataKind;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Snapshot bridge: overlap is allowed once; after bridging each U must equal previous u + 1.
#[derive(Clone, Copy)]
pub(super) struct DepthSequence {
    pub last: u64,
    pub bridged: bool,
}
impl DepthSequence {
    pub fn accept(&mut self, first: u64, last: u64) -> Result<bool, ApiError> {
        if first > last {
            return Err(error(NAME, "inverted depth sequence"));
        }
        if last <= self.last {
            return Ok(false);
        }
        let expected = self
            .last
            .checked_add(1)
            .ok_or_else(|| error(NAME, "depth sequence overflow"))?;
        if (self.bridged && first != expected)
            || (!self.bridged && (first > expected || last < expected))
        {
            return Err(error(NAME, "depth gap"));
        }
        self.last = last;
        self.bridged = true;
        Ok(true)
    }
}
pub(super) async fn session(
    client: &BackpackClient,
    desired: Desired,
    orders: Arc<Mutex<Orders>>,
    tx: PublishSender,
    private: bool,
    epoch: u64,
) -> Result<(), ApiError> {
    let mut ticks = HashMap::new();
    let instruments = client.get_instruments().await?;
    for symbol in if private {
        desired.accounts.iter().collect::<Vec<_>>()
    } else {
        desired.market.keys().collect::<Vec<_>>()
    } {
        let m = instruments
            .iter()
            .find(|m| m.symbol == *symbol && m.tradable)
            .ok_or_else(|| invalid(NAME, "unbound USDC perpetual"))?;
        ticks.insert(symbol.clone(), m.tick_size);
    }
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(&client.config.ws_url),
    )
    .await
    .map_err(|_| error(NAME, "WS connect timeout"))?
    .map_err(|e| websocket_error(NAME, e))?;
    let mut params = Vec::new();
    if private {
        params.extend(
            [
                "account.orderUpdate",
                "account.positionUpdate",
                "account.balanceUpdate",
            ]
            .map(String::from),
        );
    } else {
        for (symbol, kinds) in &desired.market {
            for kind in kinds {
                let channel = match kind {
                    MarketDataKind::Depth => "depth",
                    MarketDataKind::Bbo => "bookTicker",
                    MarketDataKind::Trades => "trade",
                    MarketDataKind::MarkPrice | MarketDataKind::FundingRate => "markPrice",
                    _ => return Err(unsupported(NAME, "ticker stream")),
                };
                params.push(format!("{channel}.{symbol}"));
            }
        }
        params.sort();
        params.dedup();
    }
    if params.is_empty() {
        std::future::pending::<()>().await;
    }
    let mut subscribe = json!({"method":"SUBSCRIBE","params":params});
    if private {
        let ts = Utc::now().timestamp_millis();
        subscribe["signature"] = json!([
            client.config.api_key,
            client.signature("subscribe", &json!({}), ts)?,
            ts.to_string(),
            client.config.window_ms.to_string()
        ]);
    }
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .map_err(|_| ApiError::transport(NAME, "subscription failed"))?;
    let mut sequences = HashMap::new();
    if !private {
        for (symbol, kinds) in &desired.market {
            if kinds.contains(&MarketDataKind::Depth) {
                let v = client
                    .get("/api/v1/depth", json!({"symbol":symbol}), None)
                    .await?;
                let seq = text(&v, "lastUpdateId", NAME)?
                    .parse::<u64>()
                    .map_err(|_| error(NAME, "invalid snapshot sequence"))?;
                let bids = levels(&v["bids"], NAME)?;
                let asks = levels(&v["asks"], NAME)?;
                publish_depth(
                    symbol,
                    &bids,
                    &asks,
                    timestamp(&v, "timestamp", 1000, NAME)?,
                    MarketStreamMetadata {
                        epoch,
                        first_update_sequence: seq,
                        last_update_sequence: seq,
                        snapshot: true,
                    },
                    &tx,
                )?;
                sequences.insert(
                    symbol.clone(),
                    DepthSequence {
                        last: seq,
                        bridged: false,
                    },
                );
            }
        }
    }
    let (refresh_error, mut refresh_errors) = tokio::sync::mpsc::channel(1);
    let refresh =
        private.then(|| BalanceRefresh::start(client.clone(), tx.clone(), refresh_error.clone()));
    let mut ready = !private;
    let startup = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let timeout = if ready {
            Duration::from_secs(130)
        } else {
            startup.saturating_duration_since(tokio::time::Instant::now())
        };
        let frame = tokio::select! {
            _ = refresh_errors.recv() => return Err(error(NAME,"authoritative collateral refresh failed")),
            frame = tokio::time::timeout(timeout, ws.next()) => frame
                .map_err(|_| error(NAME,"private confirmation or WS heartbeat timeout"))?
                .ok_or_else(||ApiError::transport(NAME,"WS closed"))?
                .map_err(|_|ApiError::transport(NAME,"WS read failed"))?,
        };
        match frame {
            Message::Ping(p) => {
                ws.send(Message::Pong(p))
                    .await
                    .map_err(|_| ApiError::transport(NAME, "pong failed"))?;
                continue;
            }
            Message::Close(_) => return Err(ApiError::transport(NAME, "WS closed")),
            Message::Text(raw) => {
                let v: Value =
                    serde_json::from_str(&raw).map_err(|_| error(NAME, "invalid WS JSON"))?;
                if v.get("error").is_some() || v.get("code").is_some() {
                    return Err(error(NAME, "WS subscription rejected"));
                }
                let Some(channel) = v["stream"].as_str() else {
                    continue;
                };
                let data = &v["data"];
                if private {
                    let refresh = refresh.as_ref().unwrap();
                    let mut revision = refresh.revision.lock().unwrap_or_else(|p| p.into_inner());
                    *revision = revision.wrapping_add(1);
                    let confirmed =
                        private_frame(client, channel, data, &desired, &ticks, &orders, &tx)?;
                    if confirmed && !ready {
                        ready = true;
                        tx.send(PublishEvent::PrivateStreamReady);
                    }
                    refresh.notify.notify_one();
                } else {
                    public(data, &desired, &mut sequences, &tx, epoch)?;
                }
            }
            _ => {}
        }
    }
}
// This task belongs to one socket generation; cancellation drops and aborts it. A slow REST
// refresh never blocks orders or Ping. The revision lock prevents an older REST image from
// overwriting private facts received during its asynchronous preparation.
struct BalanceRefresh {
    revision: Arc<Mutex<u64>>,
    notify: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for BalanceRefresh {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl BalanceRefresh {
    fn start(
        client: BackpackClient,
        tx: PublishSender,
        failure: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        let revision = Arc::new(Mutex::new(0_u64));
        let notify = Arc::new(tokio::sync::Notify::new());
        let version = revision.clone();
        let signal = notify.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(5),
                Duration::from_secs(5),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _=signal.notified()=>{}, _=interval.tick()=>{} }
                tokio::time::sleep(Duration::from_millis(200)).await;
                let before = *version.lock().unwrap_or_else(|p| p.into_inner());
                let result =
                    tokio::time::timeout(Duration::from_secs(15), client.get_account()).await;
                let Ok(Ok(account)) = result else {
                    let _ = failure.send(()).await;
                    return;
                };
                let current = version.lock().unwrap_or_else(|p| p.into_inner());
                if *current != before {
                    signal.notify_one();
                    continue;
                }
                for balance in account.balances {
                    tx.send_account(AccountPublication::Balance {
                        currency: balance.asset,
                        wallet: balance.wallet_balance,
                        available: Some(balance.available_balance),
                        margin: Some(balance.margin_balance),
                        unrealized_pnl: Some(balance.unrealized_pnl),
                        exch_ts: 0,
                    });
                }
            }
        });
        Self {
            revision,
            notify,
            task,
        }
    }
}
fn private_frame(
    client: &BackpackClient,
    channel: &str,
    data: &Value,
    desired: &Desired,
    ticks: &HashMap<String, f64>,
    orders: &Mutex<Orders>,
    tx: &PublishSender,
) -> Result<bool, ApiError> {
    match channel {
        "account.positionUpdate" => {
            if data.is_array() {
                for p in rows(data, NAME)? {
                    position(p, desired, tx)?;
                }
            } else if data.as_object().is_some_and(|v| v.is_empty()) {
            } else {
                position(data, desired, tx)?;
            }
            // The signed batch is authenticated as one request. A validated position image
            // (including an empty array) proves authentication; silence never opens the gate.
            Ok(true)
        }
        "account.orderUpdate" => {
            order(client, data, desired, ticks, orders, tx)?;
            Ok(false)
        }
        "account.balanceUpdate" => {
            let wallet =
                number(data, "A", NAME)? + number(data, "L", NAME)? + number(data, "S", NAME)?;
            if data["a"] != "USDC" && wallet != 0.0 {
                return Err(unsupported(NAME, "non-USDC balance"));
            }
            tx.send_account(AccountPublication::Balance {
                currency: text(data, "a", NAME)?,
                wallet,
                available: None,
                margin: None,
                unrealized_pnl: None,
                exch_ts: timestamp(data, "T", 1000, NAME)?,
            });
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn public(
    v: &Value,
    desired: &Desired,
    sequences: &mut HashMap<String, DepthSequence>,
    tx: &PublishSender,
    epoch: u64,
) -> Result<(), ApiError> {
    let symbol = text(v, "s", NAME)?;
    let Some(kinds) = desired.market.get(&symbol) else {
        return Ok(());
    };
    let ts = timestamp(v, "T", 1000, NAME)?;
    match v["e"].as_str() {
        Some("depth") if kinds.contains(&MarketDataKind::Depth) => {
            let first = integer(v, "U", NAME)?
                .try_into()
                .map_err(|_| error(NAME, "negative depth sequence"))?;
            let last = integer(v, "u", NAME)?
                .try_into()
                .map_err(|_| error(NAME, "negative depth sequence"))?;
            if sequences
                .get_mut(&symbol)
                .ok_or_else(|| error(NAME, "missing snapshot"))?
                .accept(first, last)?
            {
                publish_depth(
                    &symbol,
                    &levels(&v["b"], NAME)?,
                    &levels(&v["a"], NAME)?,
                    ts,
                    MarketStreamMetadata {
                        epoch,
                        first_update_sequence: first,
                        last_update_sequence: last,
                        snapshot: false,
                    },
                    tx,
                )?;
            }
        }
        Some("bookTicker") if kinds.contains(&MarketDataKind::Bbo) => publish_bbo(
            &symbol,
            number(v, "b", NAME)?,
            number(v, "B", NAME)?,
            number(v, "a", NAME)?,
            number(v, "A", NAME)?,
            ts,
            tx,
        )?,
        Some("trade") if kinds.contains(&MarketDataKind::Trades) => publish_trade(
            &symbol,
            number(v, "p", NAME)?,
            number(v, "q", NAME)?,
            v["m"]
                .as_bool()
                .ok_or_else(|| error(NAME, "missing trade maker side"))?,
            ts,
            tx,
        )?,
        Some("markPrice") => {
            if kinds.contains(&MarketDataKind::MarkPrice) {
                tx.send(PublishEvent::MarkPrice {
                    symbol: symbol.clone(),
                    mark_price: number(v, "p", NAME)?,
                    exch_ts: ts,
                });
            }
            if kinds.contains(&MarketDataKind::FundingRate) {
                tx.send(PublishEvent::Funding {
                    symbol,
                    funding_rate: number(v, "f", NAME)?,
                    next_funding_time: timestamp(v, "n", 1_000_000, NAME)?,
                    exch_ts: ts,
                });
            }
        }
        _ => {}
    }
    Ok(())
}
fn position(v: &Value, d: &Desired, tx: &PublishSender) -> Result<(), ApiError> {
    let symbol = text(v, "s", NAME)?;
    if !d.accounts.contains(&symbol) {
        return Ok(());
    }
    tx.send_account(AccountPublication::Position {
        symbol,
        position_side: ApiPositionSide::Net,
        qty: number(v, "q", NAME)?,
        entry_price: Some(number(v, "B", NAME)?),
        unrealized_pnl: Some(number(v, "P", NAME)?),
        liquidation_price: None,
        exch_ts: timestamp(v, "T", 1000, NAME)?,
    });
    Ok(())
}
pub(super) fn order(
    client: &BackpackClient,
    v: &Value,
    d: &Desired,
    ticks: &HashMap<String, f64>,
    orders: &Mutex<Orders>,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    let symbol = text(v, "s", NAME)?;
    if !d.accounts.contains(&symbol) {
        return Ok(());
    }
    let q = number(v, "q", NAME)?;
    let z = number(v, "z", NAME)?;
    let mut info=client.order(&json!({"symbol":symbol,"id":v["i"],"clientId":v["c"],"quantity":q.to_string(),"executedQuantity":z.to_string(),"executedQuoteQuantity":v["Z"],"side":v["S"],"orderType":v["o"],"status":v["X"],"price":v["p"],"timeInForce":v["f"],"postOnly":v["y"],"reduceOnly":v["r"],"createdAt":0,"triggerPrice":v["P"]}))?;
    info.update_time = integer(v, "T", NAME)? / 1000;
    publish_order(
        &info,
        *ticks
            .get(&symbol)
            .ok_or_else(|| error(NAME, "missing tick"))?,
        orders,
        tx,
    )?;
    if v["e"] == "orderFill" {
        let qty = number(v, "l", NAME)?;
        if qty <= 0.0 {
            return Err(error(NAME, "invalid fill quantity"));
        }
        tx.send_account(AccountPublication::NativeFillWithoutRealizedPnl {
            position_side: ApiPositionSide::Net,
            fill: Fill {
                symbol,
                trade_id: text(v, "t", NAME)?,
                order_id: info.order_id,
                client_order_id: info.client_order_id,
                price: number(v, "L", NAME)?,
                qty,
                side: info.side,
                fee: number(v, "n", NAME)?,
                fee_asset: text(v, "N", NAME)?,
                realized_pnl: 0.0,
                maker: v["m"]
                    .as_bool()
                    .ok_or_else(|| error(NAME, "missing fill maker flag"))?,
                timestamp: info.update_time,
            },
        });
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{Connector, ConnectorBuilder, test_publish_channel};
    use crate::venue::test_support::http;
    use tokio::{net::TcpListener, sync::mpsc};
    #[tokio::test]
    async fn reconnect_invalidates_old_epoch_and_rebuilds_from_rest_snapshot() {
        let markets = json!([{"symbol":"BTC_USDC_PERP","baseSymbol":"BTC","quoteSymbol":"USDC","marketType":"PERP","filters":{"price":{"tickSize":"0.1"},"quantity":{"stepSize":"0.001","minQuantity":"0.001"}},"orderBookState":"Open"}]);
        let book = json!({"bids":[["100.0","1.000"]],"asks":[["100.1","2.000"]],"lastUpdateId":"100","timestamp":1700000000000000i64});
        let (url, mut requests, http_task) = http(vec![
            (200, markets.clone()),
            (200, book.clone()),
            (200, markets),
            (200, book),
        ])
        .await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let (notify, mut seen) = mpsc::channel(2);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (s, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
                let sub = ws.next().await.unwrap().unwrap();
                assert!(sub.into_text().unwrap().contains("depth.BTC_USDC_PERP"));
                notify.send(()).await.unwrap();
                for (first, last) in [(98, 100), (99, 102), (104, 105)] {
                    ws.send(Message::Text(json!({"stream":"depth.BTC_USDC_PERP","data":{"e":"depth","s":"BTC_USDC_PERP","b":[["100.0","1.001"]],"a":[],"U":first,"u":last,"T":1700000000123456i64}}).to_string().into())).await.unwrap();
                }
                let _ = ws.close(None).await;
            }
        });
        let mut c = super::super::Backpack::build_from(&format!(
            "rest_url = {url:?}\nws_url = {ws_url:?}\nreconnect_ms = 1\n"
        ))
        .unwrap();
        c.subscribe_market_data("BTC_USDC_PERP".into(), vec![MarketDataKind::Depth]);
        let (tx, mut rx) = test_publish_channel();
        c.run_market_data(tx);
        let mut snapshots = Vec::new();
        let mut invalidated = false;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match rx.recv().await.unwrap() {
                    PublishEvent::FeedBatch {
                        stream: Some(s), ..
                    } if s.snapshot => {
                        snapshots.push(s.epoch);
                        if snapshots.len() == 2 {
                            break;
                        }
                    }
                    PublishEvent::StreamInvalidated { .. } => invalidated = true,
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert!(invalidated);
        assert!(snapshots[1] > snapshots[0]);
        c.shutdown().await.unwrap();
        seen.recv().await.unwrap();
        seen.recv().await.unwrap();
        for _ in 0..4 {
            requests.recv().await.unwrap();
        }
        http_task.await.unwrap();
        server.await.unwrap();
    }
}
#[cfg(test)]
mod confirmation_tests {
    use super::*;
    use crate::connector::test_publish_channel;
    #[test]
    fn private_ready_requires_a_validated_position_image() {
        let c = BackpackClient::new(Config::default()).unwrap();
        let (tx, _) = test_publish_channel();
        let d = Desired::default();
        let ticks = HashMap::new();
        let orders = Mutex::new(Orders::default());
        assert!(
            private_frame(
                &c,
                "account.positionUpdate",
                &json!([]),
                &d,
                &ticks,
                &orders,
                &tx
            )
            .unwrap()
        );
        assert!(
            private_frame(
                &c,
                "account.positionUpdate",
                &json!({"bad":"image"}),
                &d,
                &ticks,
                &orders,
                &tx
            )
            .is_err()
        );
        assert!(!private_frame(&c, "unrelated", &json!({}), &d, &ticks, &orders, &tx).unwrap());
    }
}
#[cfg(test)]
mod funding_units_tests {
    use super::*;
    use crate::connector::test_publish_channel;
    #[tokio::test]
    async fn ws_funding_time_and_engine_time_have_distinct_native_units() {
        let (tx, mut rx) = test_publish_channel();
        let mut desired = Desired::default();
        desired.market.insert(
            "BTC_USDC_PERP".into(),
            [MarketDataKind::FundingRate].into_iter().collect(),
        );
        public(&json!({"e":"markPrice","s":"BTC_USDC_PERP","f":"0.0001","n":1700003600000i64,"T":1700000000123456i64}),&desired,&mut HashMap::new(),&tx,1).unwrap();
        if let PublishEvent::Funding {
            next_funding_time,
            exch_ts,
            ..
        } = rx.recv().await.unwrap()
        {
            assert_eq!(next_funding_time, 1700003600000000000);
            assert_eq!(exch_ts, 1700000000123456000);
        } else {
            panic!()
        }
    }
}
