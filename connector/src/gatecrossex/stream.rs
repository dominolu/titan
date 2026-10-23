use super::{
    Config, Desired, OrderManager,
    brokerapi::{order_info, position_side},
    rest::*,
};
use crate::{
    api::*,
    connector::{
        AccountPublication, MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch,
        PublishEvent, PublishSender,
    },
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::types::{
    ErrorKind, Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT,
    LOCAL_SELL_TRADE_EVENT, LiveError, OrdType, Order, Side, Status, TimeInForce,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use titan_market_service::MarketDataKind;
use tokio::{net::TcpStream, sync::watch, time};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
fn transport(e: impl std::fmt::Display) -> ApiError {
    ApiError::transport(EXCHANGE, e)
}
fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}
fn ns(ms: i64) -> Result<i64, ApiError> {
    ms.checked_mul(1_000_000)
        .ok_or_else(|| protocol("timestamp overflow"))
}
fn live_error() -> LiveError {
    LiveError::new(ErrorKind::ConnectionInterrupted)
}
pub(super) fn check_response(v: &Value) -> Result<(), ApiError> {
    if v.get("error").is_some_and(|e| !e.is_null()) {
        return Err(protocol(format!("WebSocket error: {}", v["error"])));
    }
    if let Some(code) = v["result"]["code"].as_str()
        && code != "100000"
    {
        return Err(protocol(format!("WebSocket response code {code}")));
    }
    Ok(())
}
async fn send(ws: &mut Socket, value: Value) -> Result<(), ApiError> {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .map_err(transport)
}
async fn subscribe(ws: &mut Socket, channel: &str, symbols: Vec<String>) -> Result<(), ApiError> {
    send(ws,json!({"time":Utc::now().timestamp(),"event":"subscribe","channel":channel,"payload":symbols})).await
}
pub(super) async fn receive(ws: &mut Socket) -> Result<Value, ApiError> {
    loop {
        let message = ws
            .next()
            .await
            .ok_or_else(|| transport("WebSocket closed"))?
            .map_err(transport)?;
        match message {
            Message::Text(raw) => {
                let value =
                    serde_json::from_str(&raw).map_err(|_| protocol("invalid WebSocket JSON"))?;
                check_response(&value)?;
                return Ok(value);
            }
            Message::Ping(p) => ws.send(Message::Pong(p)).await.map_err(transport)?,
            Message::Close(_) => return Err(transport("WebSocket closed")),
            _ => {}
        }
    }
}
fn channels(kind: MarketDataKind, depth: u32) -> Result<String, ApiError> {
    Ok(match kind {
        MarketDataKind::Depth => format!("order_book_{depth}"),
        MarketDataKind::Bbo => "ticker".into(),
        MarketDataKind::Trades => "trade".into(),
        MarketDataKind::MarkPrice => "mark_price".into(),
        MarketDataKind::FundingRate => "funding_rate".into(),
        MarketDataKind::Ticker => return Err(unsupported("Ticker ABI; subscribe Bbo instead")),
    })
}
pub(super) fn validate_depth(symbol: &str, depth: u32) -> Result<(), ApiError> {
    let (exchange, _, _) = symbol_parts(symbol)?;
    let supported = match exchange {
        "BINANCE" => matches!(depth, 5 | 10 | 20),
        "OKX" => matches!(depth, 1 | 5),
        "GATE" => matches!(depth, 1 | 5 | 10 | 20 | 50 | 100),
        "BYBIT" => depth == 1,
        "HYPERLIQUID" | "LIGHTER" => matches!(depth, 1 | 5 | 10 | 20 | 30 | 50 | 100 | 400),
        "DERIBIT" => matches!(depth, 1 | 5 | 10 | 20),
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(unsupported(
            "requested snapshot depth for this exchange (Kraken needs incremental books)",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    config: Config,
    client: GateCrossExClient,
    desired: Arc<Mutex<Desired>>,
    orders: Arc<Mutex<OrderManager>>,
    mut revision: watch::Receiver<u64>,
    tx: PublishSender,
    private: bool,
) {
    let mut epoch = 0_u64;
    loop {
        revision.borrow_and_update();
        epoch = epoch.saturating_add(1);
        let result = tokio::select! {
            result=session(&config,&client,&desired,&orders,&tx,private,epoch) => result,
            _=revision.changed() => Ok(()),
        };
        if let Err(error) = result {
            tracing::warn!(code=%error.code,message=%error.message,private,"CrossEx stream interrupted");
        }
        if private {
            tx.send_account(AccountPublication::Error(live_error()));
        } else {
            tx.send(PublishEvent::ConnectorError(live_error()));
            let symbols: Vec<_> = desired
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .market
                .keys()
                .cloned()
                .collect();
            for symbol in symbols {
                tx.send(PublishEvent::StreamInvalidated {
                    symbol,
                    epoch: epoch.saturating_add(1),
                });
            }
        }
        tokio::select! { _=time::sleep(Duration::from_millis(config.reconnect_ms))=>{}, _=revision.changed()=>{} }
    }
}
async fn session(
    config: &Config,
    client: &GateCrossExClient,
    desired: &Mutex<Desired>,
    orders: &Mutex<OrderManager>,
    tx: &PublishSender,
    private: bool,
    epoch: u64,
) -> Result<(), ApiError> {
    let mut multipliers = HashMap::new();
    if !private {
        let subscriptions = desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .market
            .clone();
        for (symbol, kinds) in subscriptions {
            if kinds.iter().any(|k| {
                matches!(
                    k,
                    MarketDataKind::Depth | MarketDataKind::Bbo | MarketDataKind::Trades
                )
            }) {
                multipliers.insert(
                    symbol.clone(),
                    client.market_quantity_multiplier(&symbol).await?,
                );
            }
        }
    }
    let exact_multipliers = multipliers
        .iter()
        .map(|(symbol, value)| {
            crate::decimal::DecimalMultiplier::from_f64(*value)
                .map(|factor| (symbol.clone(), factor))
                .map_err(protocol)
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let url = if private {
        &config.private_ws_url
    } else {
        &config.public_ws_url
    };
    let (mut ws, _) = time::timeout(Duration::from_secs(10), connect_async(url))
        .await
        .map_err(|_| transport("WebSocket connect timeout"))?
        .map_err(transport)?;
    let mut pending = HashSet::new();
    let mut ticks = HashMap::new();
    if private {
        if client.key.is_empty() || client.secret.is_empty() {
            return Err(invalid("CrossEx credentials are missing"));
        }
        let timestamp = Utc::now().timestamp();
        send(&mut ws,json!({"time":timestamp,"event":"login","payload":{"method":"api_key","api_key":client.key,
            "sign":sign(&client.secret,&format!("channel=&event=login&time={timestamp}"))}})).await?;
        let ack = time::timeout(Duration::from_secs(10), receive(&mut ws))
            .await
            .map_err(|_| transport("login timeout"))??;
        if ack["event"] != "login" || ack["result"]["code"] != "100000" {
            return Err(protocol("CrossEx login was not confirmed"));
        }
        for instrument in client.get_instruments().await? {
            ticks.insert(instrument.symbol, instrument.tick_size);
        }
        let margin = desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .iter()
            .any(|s| s.contains("_MARGIN_"));
        let mut private_channels = vec!["order", "position", "asset"];
        if margin {
            private_channels.extend(["margin_position", "margin_interest"]);
        }
        for channel in private_channels {
            subscribe(&mut ws, channel, vec!["!all".into()]).await?;
            pending.insert(channel.to_owned());
        }
    } else {
        let subscriptions = desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .market
            .clone();
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (symbol, kinds) in subscriptions {
            symbol_parts(&symbol)?;
            for kind in kinds {
                if kind == MarketDataKind::Depth {
                    validate_depth(&symbol, config.depth_levels)?;
                }
                grouped
                    .entry(channels(kind, config.depth_levels)?)
                    .or_default()
                    .push(symbol.clone());
            }
        }
        for (channel, mut symbols) in grouped {
            symbols.sort();
            symbols.dedup();
            for chunk in symbols.chunks(100) {
                subscribe(&mut ws, &channel, chunk.to_vec()).await?;
            }
        }
    }
    let deadline = time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    let mut ping = time::interval(Duration::from_secs(15));
    let mut last_receive = time::Instant::now();
    let mut refresh: Option<tokio::task::JoinHandle<Result<BalanceRefresh, ApiError>>> = None;
    let mut refresh_dirty = false;
    // A session owns this worker; aborting a session must never publish an old generation.
    struct AbortRefresh<'a>(
        &'a mut Option<tokio::task::JoinHandle<Result<BalanceRefresh, ApiError>>>,
    );
    impl Drop for AbortRefresh<'_> {
        fn drop(&mut self) {
            if let Some(task) = self.0.take() {
                task.abort();
            }
        }
    }
    let refresh = AbortRefresh(&mut refresh);
    loop {
        tokio::select! {
            result = async { refresh.0.as_mut().expect("active refresh").await }, if refresh.0.is_some() => {
                refresh.0.take();
                let (account,positions) = result.map_err(transport)??;
                publish_balances(tx, &account)?;
                for position in positions {
                    let spot=position.symbol.contains("_SPOT_");
                    tx.send_account(AccountPublication::Position {symbol:position.symbol,position_side:position.position_side,qty:position.qty,
                        entry_price:(!spot).then_some(position.entry_price),unrealized_pnl:(!spot).then_some(position.unrealized_pnl),
                        liquidation_price:(!spot).then_some(position.liquidation_price),exch_ts:ns(position.update_time)?});
                }
                if refresh_dirty {
                    refresh_dirty = false;
                    *refresh.0 = Some(start_balance_refresh(client.clone()));
                }
            }
            _=&mut deadline, if private && !pending.is_empty() => return Err(transport("private subscription confirmation timeout")),
            _=ping.tick() => {
                if last_receive.elapsed()>Duration::from_secs(45) { return Err(transport("WebSocket heartbeat timeout")); }
                ws.send(Message::Ping(Vec::new().into())).await.map_err(transport)?;
            }
            frame=ws.next() => {
                let message=frame.ok_or_else(|| transport("WebSocket closed"))?.map_err(transport)?;
                // Local WS-frame completion, before JSON decoding. Never use an exchange clock
                // to measure receive-to-strategy latency.
                let receive_ns=now_ns();
                last_receive=time::Instant::now();
                match message {
                    Message::Text(raw) => {
                        if !private && config.borrowed_depth && super::depth::publish(&raw,desired,tx,epoch,&exact_multipliers,receive_ns)? {continue;}
                        let value:Value=serde_json::from_str(&raw).map_err(|_| protocol("invalid WebSocket JSON"))?;
                        check_response(&value)?;
                        if private {
                            // Private facts use payload and event=subscribe; public updates use result.
                            // Process a fact even when its envelope also confirms subscription success.
                            if (value["channel"] == "asset" && value["payload"].get("coin").is_some()) || (value["channel"] == "margin_interest" && value["payload"].is_object()) {
                                if refresh.0.is_some() { refresh_dirty = true; }
                                else { *refresh.0 = Some(start_balance_refresh(client.clone())); }
                            } else { private_frame(client,desired,orders,&ticks,tx,&value)?; }
                            if value["event"]=="subscribe" && value["result"]["code"]=="100000"
                                && let Some(channel)=value["channel"].as_str()
                                && pending.remove(channel) && pending.is_empty() {
                                tx.send(PublishEvent::PrivateStreamReady);
                            }
                        } else { public_frame_at(desired,tx,&value,epoch,&multipliers,receive_ns)?; }
                    }
                    Message::Ping(p)=>ws.send(Message::Pong(p)).await.map_err(transport)?,
                    Message::Close(_)=>return Err(transport("WebSocket closed")),
                    _=>{}
                }
            }
        }
    }
}
fn levels(value: &Value) -> Result<Vec<(String, String)>, ApiError> {
    rows(value)?
        .iter()
        .map(|level| {
            let row = level
                .as_array()
                .ok_or_else(|| protocol("invalid depth level"))?;
            if row.len() != 2 {
                return Err(protocol("invalid depth level length"));
            }
            let object = json!({"price":row[0],"qty":row[1]});
            if number(&object, "price")? <= 0.0 || number(&object, "qty")? < 0.0 {
                return Err(protocol("invalid depth price or qty"));
            }
            Ok((text(&object, "price")?, text(&object, "qty")?))
        })
        .collect()
}
#[cfg(test)]
pub(super) fn public_frame(
    desired: &Mutex<Desired>,
    tx: &PublishSender,
    frame: &Value,
    epoch: u64,
) -> Result<(), ApiError> {
    let symbol = frame["result"]["s"].as_str().unwrap_or("").to_owned();
    public_frame_scaled(desired, tx, frame, epoch, &HashMap::from([(symbol, 1.0)]))
}
#[cfg(test)]
pub(super) fn public_frame_scaled(
    desired: &Mutex<Desired>,
    tx: &PublishSender,
    frame: &Value,
    epoch: u64,
    multipliers: &HashMap<String, f64>,
) -> Result<(), ApiError> {
    public_frame_at(desired, tx, frame, epoch, multipliers, now_ns())
}
fn public_frame_at(
    desired: &Mutex<Desired>,
    tx: &PublishSender,
    frame: &Value,
    epoch: u64,
    multipliers: &HashMap<String, f64>,
    receive_ns: i64,
) -> Result<(), ApiError> {
    if frame["event"] != "update" {
        return Ok(());
    }
    let value = &frame["result"];
    let symbol = text(value, "s")?;
    let subscriptions = desired.lock().unwrap_or_else(|p| p.into_inner());
    let Some(kinds) = subscriptions.market.get(&symbol) else {
        return Err(protocol("unqualified or unbound public symbol"));
    };
    let kinds = kinds.clone();
    drop(subscriptions);
    let channel = frame["channel"]
        .as_str()
        .ok_or_else(|| protocol("missing channel"))?;
    let timestamp = if value.get("ts").is_some() {
        integer(value, "ts")?
    } else {
        integer(frame, "time_ms")?
    };
    let exch_ts = ns(timestamp)?;
    let local_ts = receive_ns;
    let multiplier = multipliers.get(&symbol).copied();
    let quantity = |value: f64| {
        super::market_units::scale_quantity(
            value,
            multiplier.ok_or_else(|| protocol("missing public quantity multiplier"))?,
        )
    };
    let mut events = Vec::new();
    if channel.starts_with("order_book_") && kinds.contains(&MarketDataKind::Depth) {
        let scaled_levels = |value: &Value| -> Result<Vec<(String, String)>, ApiError> {
            let multiplier =
                multiplier.ok_or_else(|| protocol("missing public quantity multiplier"))?;
            levels(value)?
                .into_iter()
                .map(|(p, q)| Ok((p, super::market_units::scale_decimal(&q, multiplier)?)))
                .collect()
        };
        let bids = scaled_levels(&value["b"])?;
        let asks = scaled_levels(&value["a"])?;
        // Limited-depth messages are complete images. There is no venue sequence; zero is explicit.
        let stream = MarketStreamMetadata {
            epoch,
            first_update_sequence: 0,
            last_update_sequence: 0,
            snapshot: true,
        };
        if tx.try_send_native_market(NativeMarketBatch::Depth {
            symbol: &symbol,
            bids: NativeDepthLevels::Owned(&bids),
            asks: NativeDepthLevels::Owned(&asks),
            exchange_ts: exch_ts,
            receive_ts: local_ts,
            stream,
        }) {
            return Ok(());
        }
        for (side, levels) in [
            (LOCAL_BID_DEPTH_SNAPSHOT_EVENT, bids),
            (LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, asks),
        ] {
            for (price, qty) in levels {
                events.push(Event {
                    ev: side,
                    exch_ts,
                    local_ts,
                    px: price.parse().map_err(|_| protocol("invalid price"))?,
                    qty: qty.parse().map_err(|_| protocol("invalid qty"))?,
                    order_id: 0,
                    ival: 0,
                    fval: 0.0,
                });
            }
        }
        tx.send(PublishEvent::FeedBatch {
            symbol,
            events,
            stream: Some(stream),
        });
        return Ok(());
    }
    if channel == "ticker" && kinds.contains(&MarketDataKind::Bbo) {
        let bp = number(value, "bp")?;
        let bq = quantity(number(value, "bs")?)?;
        let ap = number(value, "ap")?;
        let aq = quantity(number(value, "as")?)?;
        if bp < 0.0 || bq < 0.0 || ap < 0.0 || aq < 0.0 {
            return Err(protocol("negative BBO"));
        }
        if tx.try_send_native_market(NativeMarketBatch::Bbo {
            symbol: &symbol,
            bid_price: bp,
            bid_quantity: bq,
            ask_price: ap,
            ask_quantity: aq,
            exchange_ts: exch_ts,
            receive_ts: local_ts,
        }) {
            return Ok(());
        }
        events.push(Event {
            ev: LOCAL_BID_DEPTH_BBO_EVENT,
            exch_ts,
            local_ts,
            px: bp,
            qty: bq,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        });
        events.push(Event {
            ev: LOCAL_ASK_DEPTH_BBO_EVENT,
            exch_ts,
            local_ts,
            px: ap,
            qty: aq,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        });
    } else if channel == "trade" && kinds.contains(&MarketDataKind::Trades) {
        let price = number(value, "p")?;
        let qty = quantity(number(value, "q")?)?;
        let side = text(value, "S")?;
        if price <= 0.0 || qty <= 0.0 || !matches!(side.as_str(), "BUY" | "SELL") {
            return Err(protocol("invalid public trade"));
        }
        if tx.try_send_native_market(NativeMarketBatch::Trade {
            symbol: &symbol,
            price,
            quantity: qty,
            sell: side == "SELL",
            exchange_ts: exch_ts,
            receive_ts: local_ts,
        }) {
            return Ok(());
        }
        events.push(Event {
            ev: if side == "BUY" {
                LOCAL_BUY_TRADE_EVENT
            } else {
                LOCAL_SELL_TRADE_EVENT
            },
            exch_ts,
            local_ts,
            px: price,
            qty,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        });
    } else if channel == "funding_rate" && kinds.contains(&MarketDataKind::FundingRate) {
        tx.send(PublishEvent::Funding {
            symbol,
            funding_rate: number(value, "r")?,
            next_funding_time: ns(integer(value, "T")?)?,
            exch_ts,
        });
        return Ok(());
    } else if channel == "mark_price" && kinds.contains(&MarketDataKind::MarkPrice) {
        tx.send(PublishEvent::MarkPrice {
            symbol,
            mark_price: number(value, "mp")?,
            exch_ts,
        });
        return Ok(());
    }
    if !events.is_empty() {
        tx.send(PublishEvent::FeedBatch {
            symbol,
            events,
            stream: None,
        });
    }
    Ok(())
}
type BalanceRefresh = (AccountInfo, Vec<PositionInfo>);
pub(super) fn start_balance_refresh(
    client: GateCrossExClient,
) -> tokio::task::JoinHandle<Result<BalanceRefresh, ApiError>> {
    tokio::spawn(async move {
        time::timeout(Duration::from_secs(15), async {
            let payload = client.account_payload().await?;
            let account = client.account_valued(&payload).await?;
            let symbols = client
                .bound_symbols
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let mut positions = Vec::new();
            for symbol in symbols.iter().filter(|s| s.contains("_SPOT_")) {
                positions.push(super::brokerapi::spot_inventory(&payload, symbol)?);
            }
            if symbols.iter().any(|s| s.contains("_MARGIN_")) {
                let mut query = Vec::new();
                if let Some(exchange) = &client.exchange_type {
                    query.push(("exchange_type", exchange.clone()));
                }
                let values = client
                    .get("/crossex/margin_positions", &query, true)
                    .await?;
                for value in rows(&values)?
                    .iter()
                    .filter(|v| v["symbol"].as_str().is_some_and(|s| symbols.contains(s)))
                {
                    positions.push(super::brokerapi::margin_position_info(value)?);
                }
            }
            Ok((account, positions))
        })
        .await
        .map_err(|_| transport("account refresh exceeded 15 seconds"))?
    })
}
pub(super) fn publish_balances(tx: &PublishSender, account: &AccountInfo) -> Result<(), ApiError> {
    let timestamp = ns(account.timestamp)?;
    for b in &account.balances {
        tx.send_account(AccountPublication::Balance {
            currency: b.asset.clone(),
            wallet: b.wallet_balance,
            available: Some(b.available_balance),
            margin: Some(b.margin_balance),
            unrealized_pnl: Some(b.unrealized_pnl),
            exch_ts: timestamp,
        });
    }
    Ok(())
}

pub(super) fn private_frame(
    client: &GateCrossExClient,
    desired: &Mutex<Desired>,
    orders: &Mutex<OrderManager>,
    ticks: &HashMap<String, f64>,
    tx: &PublishSender,
    frame: &Value,
) -> Result<(), ApiError> {
    tracing::debug!(
        channel = frame["channel"].as_str(),
        has_payload = frame["payload"].is_object(),
        "CrossEx private frame received"
    );
    let Some(value) = frame.get("payload").filter(|v| v.is_object()) else {
        return Ok(());
    };
    let channel = frame["channel"].as_str().unwrap_or("");
    if !matches!(channel, "order" | "position" | "margin_position") || value.get("symbol").is_none()
    {
        return Ok(());
    }
    let symbol = text(value, "symbol")?;
    tracing::debug!(
        symbol = symbol.as_str(),
        bound = desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .contains(&symbol),
        "CrossEx private symbol routing"
    );
    if !desired
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .accounts
        .contains(&symbol)
    {
        return Ok(());
    }
    if matches!(channel, "position" | "margin_position") {
        let side = position_side(&value["position_side"])?;
        let qty = if channel == "margin_position" {
            number(
                value,
                if side == ApiPositionSide::Short {
                    "liability"
                } else {
                    "asset_qty"
                },
            )?
        } else {
            number(value, "position_qty")?
        };
        let optional = |key: &str| {
            if value.get(key).is_some() {
                number(value, key).map(Some)
            } else {
                Ok(None)
            }
        };
        tx.send_account(AccountPublication::Position {
            symbol,
            position_side: side,
            qty: match side {
                ApiPositionSide::Short => -qty.abs(),
                ApiPositionSide::Long => qty.abs(),
                _ => qty,
            },
            entry_price: optional("entry_price")?,
            unrealized_pnl: optional("upnl")?,
            liquidation_price: optional("liq_price")?,
            exch_ts: ns(integer(value, "update_time")?)?,
        });
    } else {
        let info = order_info(value)?;
        let tick = *ticks
            .get(&symbol)
            .ok_or_else(|| protocol("missing order instrument metadata"))?;
        let order = update_order(orders, &info, tick)?;
        tracing::debug!(
            symbol = symbol.as_str(),
            publish = order.is_some(),
            "CrossEx order fact candidate"
        );
        if let Some(mut order) = order {
            if order.exec_qty > 0.0 {
                if value.get("last_executed_qty").is_some()
                    && (number(value, "last_executed_qty")? - order.exec_qty).abs() < 1e-10
                    && number(value, "last_executed_price")? > 0.0
                {
                    order.exec_price_tick = crate::units::integer_units(
                        number(value, "last_executed_price")?,
                        tick,
                        false,
                    )
                    .map_err(protocol)?;
                }
                if order.exec_price_tick <= 0 {
                    return Err(protocol(
                        "incremental execution price requires reconciliation",
                    ));
                }
            }
            client.record_order(&info);
            if info.status == ApiOrderStatus::Rejected {
                tracing::warn!(order_id=%info.order_id,reason=%value["reason"],"CrossEx order rejected");
            }
            tx.send_account(AccountPublication::OrderWithExecution {
                symbol,
                position_side: info.position_side,
                client_order_id: Some(info.client_order_id),
                venue_order_id: Some(info.order_id),
                order,
                average_price: info.avg_price,
            });
        }
    }
    Ok(())
}
pub(super) fn update_order(
    manager: &Mutex<OrderManager>,
    info: &OrderInfo,
    tick: f64,
) -> Result<Option<Order>, ApiError> {
    let mut manager = manager.lock().unwrap_or_else(|p| p.into_inner());
    let key = if info.client_order_id.is_empty() {
        format!("venue/{}", info.order_id)
    } else {
        info.client_order_id.clone()
    };
    if manager.orders.len() >= 4096 {
        manager.orders.retain(|_, (_, o)| o.active() || o.pending());
        let retained: HashSet<_> = manager.orders.keys().cloned().collect();
        manager.notionals.retain(|key, _| retained.contains(key));
    }
    if manager.orders.len() >= 4096 && !manager.orders.contains_key(&key) {
        return Err(protocol("order tracking capacity exceeded"));
    }
    let OrderManager { orders, notionals } = &mut *manager;
    let (symbol, order) = orders.entry(key.clone()).or_insert_with(|| {
        (
            info.symbol.clone(),
            Order::new(
                info.order_id.parse().unwrap_or(0),
                0,
                tick,
                info.qty,
                if info.side == ApiSide::Buy {
                    Side::Buy
                } else {
                    Side::Sell
                },
                if info.order_type == ApiOrderType::Limit {
                    OrdType::Limit
                } else {
                    OrdType::Market
                },
                match info.time_in_force {
                    ApiTimeInForce::IOC => TimeInForce::IOC,
                    ApiTimeInForce::FOK => TimeInForce::FOK,
                    ApiTimeInForce::GTX => TimeInForce::GTX,
                    _ => TimeInForce::GTC,
                },
            ),
        )
    });
    if symbol != &info.symbol {
        return Err(protocol("client order id reused for a different symbol"));
    }
    let timestamp = ns(info.update_time)?;
    let previous = (order.qty - order.leaves_qty).max(0.0);
    if timestamp < order.exch_timestamp || info.executed_qty < previous {
        return Ok(None);
    }
    if !order.active() && !order.pending() && order.status != Status::None {
        if info.executed_qty <= previous {
            return Ok(None);
        }
        if matches!(
            info.status,
            ApiOrderStatus::New | ApiOrderStatus::PartiallyFilled
        ) {
            return Err(protocol(
                "terminal order received an active fill correction; reconcile",
            ));
        }
    }
    order.qty = info.qty;
    order.leaves_qty = info.leaves_qty;
    order.exec_qty = (info.executed_qty - previous).max(0.0);
    order.price_tick =
        crate::units::integer_units(info.price, order.tick_size, false).map_err(protocol)?;
    let cumulative_notional = info.avg_price * info.executed_qty;
    let previous_notional = if previous == 0.0 {
        Some(0.0)
    } else {
        notionals.get(&key).copied().flatten()
    };
    let incremental_price = if order.exec_qty > 0.0 {
        previous_notional
            .map(|amount| (cumulative_notional - amount) / order.exec_qty)
            .unwrap_or(0.0)
    } else {
        0.0
    };
    order.exec_price_tick =
        crate::units::integer_units(incremental_price, order.tick_size, false).map_err(protocol)?;
    notionals.insert(key, Some(cumulative_notional));
    order.exch_timestamp = timestamp;
    order.local_timestamp = now_ns();
    order.req = Status::None;
    order.status = match info.status {
        ApiOrderStatus::New => Status::New,
        ApiOrderStatus::PartiallyFilled => Status::PartiallyFilled,
        ApiOrderStatus::Filled => Status::Filled,
        ApiOrderStatus::Canceled => Status::Canceled,
        ApiOrderStatus::Rejected => Status::Rejected,
        ApiOrderStatus::Expired => Status::Expired,
        _ => return Err(protocol("unsupported order status")),
    };
    // exec_qty is the cumulative difference, so coalesced WS updates never lose a fill quantity.
    Ok(Some(order.clone()))
}
