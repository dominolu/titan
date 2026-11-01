//! Shared transport, durable numeric client IDs and connector lifecycle for native venues.
use crate::{api::*, connector::*};
use async_trait::async_trait;
use chrono::Utc;
use hftbacktest::types::{ErrorKind, Event, LiveError, OrdType, Order, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use titan_market_service::MarketDataKind;
use tokio::{sync::watch, task::JoinHandle};

pub(crate) fn error(venue: &'static str, message: impl Into<String>) -> ApiError {
    ApiError::new(venue, "PROTOCOL", message)
}
pub(crate) fn invalid(venue: &'static str, message: impl Into<String>) -> ApiError {
    ApiError::new(venue, "INVALID", message)
}
pub(crate) fn unsupported(venue: &'static str, operation: &str) -> ApiError {
    ApiError::new(
        venue,
        "UNSUPPORTED",
        format!("{operation} is not supported by this adapter"),
    )
}
pub(crate) fn websocket_error(
    venue: &'static str,
    cause: tokio_tungstenite::tungstenite::Error,
) -> ApiError {
    use tokio_tungstenite::tungstenite::Error;
    match cause {
        Error::Http(response) => ApiError::http(
            venue,
            response.status().as_u16(),
            "WebSocket handshake rejected",
        ),
        Error::Io(cause) => ApiError::transport(venue, format!("WebSocket I/O: {}", cause.kind())),
        Error::Tls(_) => ApiError::transport(venue, "WebSocket TLS handshake failed"),
        _ => ApiError::transport(venue, "WebSocket protocol or connection failed"),
    }
}
pub(crate) fn text(v: &Value, key: &str, venue: &'static str) -> Result<String, ApiError> {
    match &v[key] {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(error(venue, format!("missing {key}"))),
    }
}
pub(crate) fn number(v: &Value, key: &str, venue: &'static str) -> Result<f64, ApiError> {
    let n = match &v[key] {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
    .filter(|n: &f64| n.is_finite());
    n.ok_or_else(|| error(venue, format!("invalid {key}")))
}
pub(crate) fn optional_number(
    v: &Value,
    key: &str,
    venue: &'static str,
) -> Result<Option<f64>, ApiError> {
    if v[key].is_null() {
        Ok(None)
    } else {
        number(v, key, venue).map(Some)
    }
}
pub(crate) fn integer(v: &Value, key: &str, venue: &'static str) -> Result<i64, ApiError> {
    text(v, key, venue)?
        .parse()
        .map_err(|_| error(venue, format!("invalid integer {key}")))
}
pub(crate) fn rows<'a>(v: &'a Value, venue: &'static str) -> Result<&'a Vec<Value>, ApiError> {
    v.as_array().ok_or_else(|| error(venue, "expected array"))
}
pub(crate) fn timestamp(
    v: &Value,
    key: &str,
    scale: i64,
    venue: &'static str,
) -> Result<i64, ApiError> {
    integer(v, key, venue)?
        .checked_mul(scale)
        .filter(|n| *n >= 0)
        .ok_or_else(|| error(venue, "timestamp overflow"))
}
pub(crate) fn endpoint(raw: &str, ws: bool, venue: &'static str) -> Result<(), ApiError> {
    let url = reqwest::Url::parse(raw).map_err(|_| invalid(venue, "invalid endpoint"))?;
    if !(if ws {
        matches!(url.scheme(), "ws" | "wss")
    } else {
        matches!(url.scheme(), "http" | "https")
    }) || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(venue, "invalid endpoint"));
    }
    if matches!(url.scheme(), "http" | "ws")
        && !matches!(
            url.host_str(),
            Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
        )
    {
        return Err(invalid(
            venue,
            "insecure endpoint is only allowed on loopback",
        ));
    }
    Ok(())
}
pub(crate) fn http_client(venue: &'static str) -> Result<reqwest::Client, ApiError> {
    crate::ensure_rustls_crypto_provider();
    reqwest::Client::builder()
        .user_agent(concat!("titan-connector/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| ApiError::transport(venue, e))
}
pub(crate) async fn response(
    r: Result<reqwest::Response, reqwest::Error>,
    venue: &'static str,
) -> Result<Value, ApiError> {
    let r = r.map_err(|e| ApiError::transport(venue, e.without_url()))?;
    let status = r.status();
    // Never include arbitrary response bodies: authenticated errors may echo credentials.
    if !status.is_success() {
        return Err(ApiError::http(
            venue,
            status.as_u16(),
            "exchange request failed",
        ));
    }
    r.json()
        .await
        .map_err(|_| error(venue, "invalid response JSON"))
}
pub(crate) fn decimal(value: f64, step: f64, venue: &'static str) -> Result<String, ApiError> {
    let units = crate::units::integer_units(value, step, true).map_err(|e| invalid(venue, e))?;
    if value < 0.0 || step <= 0.0 || !step.is_finite() {
        return Err(invalid(venue, "invalid decimal"));
    }
    let precision = (0..=18)
        .find(|p| {
            let n = step * 10f64.powi(*p);
            (n - n.round()).abs() < 1e-8 && n >= 1.0
        })
        .ok_or_else(|| invalid(venue, "unsupported decimal precision"))?;
    Ok(format!("{:.*}", precision as usize, units as f64 * step))
}
pub(crate) fn precision(step: f64) -> u32 {
    (0..=18)
        .find(|p| {
            let n = step * 10f64.powi(*p as i32);
            n >= 1.0 && (n - n.round()).abs() < 1e-8
        })
        .unwrap_or(18)
}

#[derive(Serialize, Deserialize)]
struct IdRecord {
    native: u64,
    original: String,
    #[serde(default)]
    reduce_only: Option<bool>,
}
/// A locked append-only mapping: allocation is fsynced before any exchange mutation. No hashes,
/// truncation or numeric ID reuse. Losing this file requires reconciliation, never a fresh reset.
pub(crate) struct ClientIds {
    file: Option<File>,
    by_native: HashMap<u64, String>,
    by_original: HashMap<String, u64>,
    reduce_only: HashMap<u64, bool>,
    next: u64,
    max: u64,
    venue: &'static str,
}
struct JournalEntry {
    identity: String,
    max: u64,
    ids: Weak<Mutex<ClientIds>>,
}
static JOURNALS: OnceLock<Mutex<HashMap<String, JournalEntry>>> = OnceLock::new();
impl ClientIds {
    pub(crate) fn open(
        path: Option<&str>,
        identity: &str,
        max: u64,
        venue: &'static str,
    ) -> Result<Arc<Mutex<Self>>, ApiError> {
        // AccountService bootstraps a replacement before dropping its predecessor. Generations
        // of the same in-process account share one journal/lock; other processes remain excluded.
        let mut journals = JOURNALS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        journals.retain(|_, entry| entry.ids.strong_count() > 0);
        if let Some(path) = path
            && let Some(entry) = journals.get(path)
        {
            if entry.identity != identity || entry.max != max {
                return Err(invalid(
                    venue,
                    "journal is in use by a different account identity",
                ));
            }
            if let Some(ids) = entry.ids.upgrade() {
                return Ok(ids);
            }
        }
        let mut ids = Self {
            file: None,
            by_native: HashMap::new(),
            by_original: HashMap::new(),
            reduce_only: HashMap::new(),
            next: 1,
            max,
            venue,
        };
        if let Some(path) = path {
            if !Path::new(path).is_absolute() {
                return Err(invalid(venue, "client_id_journal must be absolute"));
            }
            let mut options = OpenOptions::new();
            options.read(true).append(true).create(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(path)
                .map_err(|_| invalid(venue, "cannot open client ID journal"))?;
            file.try_lock()
                .map_err(|_| invalid(venue, "client ID journal is already in use"))?;
            let mut content = String::new();
            file.read_to_string(&mut content)
                .map_err(|_| error(venue, "cannot read client ID journal"))?;
            let header = format!("titan-client-ids-v1 {identity}\n");
            if content.is_empty() {
                file.write_all(header.as_bytes())
                    .and_then(|_| file.sync_all())
                    .map_err(|_| error(venue, "cannot commit client ID journal"))?;
                if let Some(parent) = Path::new(path).parent() {
                    File::open(parent)
                        .and_then(|f| f.sync_all())
                        .map_err(|_| error(venue, "cannot sync journal directory"))?;
                }
            } else {
                if !content.starts_with(&header) || !content.ends_with('\n') {
                    return Err(error(venue, "client ID journal identity or tail mismatch"));
                }
                for line in content[header.len()..].lines() {
                    let r: IdRecord = serde_json::from_str(line)
                        .map_err(|_| error(venue, "corrupt client ID journal"))?;
                    if r.native != ids.next
                        || r.native > max
                        || r.original.is_empty()
                        || ids.by_original.contains_key(&r.original)
                    {
                        return Err(error(venue, "conflicting client ID journal"));
                    }
                    ids.next += 1;
                    ids.by_original.insert(r.original.clone(), r.native);
                    ids.by_native.insert(r.native, r.original);
                    if let Some(reduce_only) = r.reduce_only {
                        ids.reduce_only.insert(r.native, reduce_only);
                    }
                }
            }
            ids.file = Some(file);
        }
        let ids = Arc::new(Mutex::new(ids));
        if let Some(path) = path {
            journals.insert(
                path.into(),
                JournalEntry {
                    identity: identity.into(),
                    max,
                    ids: Arc::downgrade(&ids),
                },
            );
        }
        Ok(ids)
    }
    #[cfg(test)]
    pub(crate) fn allocate(&mut self, original: &str) -> Result<u64, ApiError> {
        self.allocate_order(original, None)
    }
    pub(crate) fn allocate_order(
        &mut self,
        original: &str,
        reduce_only: Option<bool>,
    ) -> Result<u64, ApiError> {
        if original.is_empty() || original.len() > 128 {
            return Err(invalid(
                self.venue,
                "client order ID must contain 1..128 bytes",
            ));
        }
        if self.by_original.contains_key(original) {
            return Err(invalid(
                self.venue,
                "client order ID already allocated; query original identity",
            ));
        }
        if self.next > self.max || self.by_native.len() >= 1_000_000 {
            return Err(error(self.venue, "client ID capacity exhausted"));
        }
        let n = self.next;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| invalid(self.venue, "private execution requires client_id_journal"))?;
        let mut line = serde_json::to_vec(&IdRecord {
            native: n,
            original: original.into(),
            reduce_only,
        })
        .map_err(|_| error(self.venue, "ID serialization failed"))?;
        line.push(b'\n');
        // A failed append makes this instance unusable; reopening rejects a partial record.
        if file
            .write_all(&line)
            .and_then(|_| file.sync_data())
            .is_err()
        {
            self.next = self.max.saturating_add(1);
            return Err(error(self.venue, "client ID commit failed"));
        }
        self.next += 1;
        self.by_original.insert(original.into(), n);
        self.by_native.insert(n, original.into());
        if let Some(reduce_only) = reduce_only {
            self.reduce_only.insert(n, reduce_only);
        }
        Ok(n)
    }
    pub(crate) fn reduce_only(&self, native: u64) -> Option<bool> {
        self.reduce_only.get(&native).copied()
    }
    pub(crate) fn native(&self, original: &str) -> Result<u64, ApiError> {
        self.by_original
            .get(original)
            .copied()
            .ok_or_else(|| invalid(self.venue, "client order ID absent from durable journal"))
    }
    pub(crate) fn original(&self, native: u64) -> String {
        self.by_native
            .get(&native)
            .cloned()
            .unwrap_or_else(|| format!("external/{}/{native}", self.venue))
    }
}

#[derive(Default, Clone)]
pub struct Desired {
    pub market: HashMap<String, HashSet<MarketDataKind>>,
    pub accounts: HashSet<String>,
}
#[derive(Default)]
pub struct Orders(pub HashMap<String, (String, Order)>);
impl GetOrders for Orders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.0
            .values()
            .filter(|(s, o)| o.active() && symbol.as_ref().is_none_or(|v| v == s))
            .map(|(_, o)| o.clone())
            .collect()
    }
}
#[async_trait]
pub trait Venue: BrokerApi + Clone + Sized + 'static {
    const NAME: &'static str;
    fn build(config: &str) -> Result<Self, ApiError>;
    fn reconnect_ms(&self) -> u64;
    async fn session(
        &self,
        desired: Desired,
        orders: Arc<Mutex<Orders>>,
        tx: PublishSender,
        private: bool,
        epoch: u64,
    ) -> Result<(), ApiError>;
}
pub struct VenueConnector<V: Venue> {
    pub(crate) client: V,
    desired: Arc<Mutex<Desired>>,
    orders: Arc<Mutex<Orders>>,
    revision: watch::Sender<u64>,
    tasks: Vec<JoinHandle<()>>,
}
impl<V: Venue> ConnectorBuilder for VenueConnector<V> {
    type Error = ApiError;
    fn build_from(config: &str) -> Result<Self, ApiError> {
        let client = V::build(config)?;
        let (revision, _) = watch::channel(0);
        Ok(Self {
            client,
            desired: Default::default(),
            orders: Default::default(),
            revision,
            tasks: vec![],
        })
    }
}
impl<V: Venue> VenueConnector<V> {
    fn changed(&self) {
        self.revision.send_modify(|r| *r = r.wrapping_add(1));
    }
    fn start(&mut self, tx: PublishSender, private: bool) {
        let client = self.client.clone();
        let desired = self.desired.clone();
        let orders = self.orders.clone();
        let mut revision = self.revision.subscribe();
        self.tasks.push(tokio::spawn(async move {
            let mut epoch = 0_u64;
            loop {
                revision.borrow_and_update();
                epoch = epoch.saturating_add(1);
                let state = desired.lock().unwrap_or_else(|p| p.into_inner()).clone();
                let symbols: Vec<_> = state.market.keys().cloned().collect();
                let result = tokio::select! {
                    result = client.session(state, orders.clone(), tx.clone(), private, epoch) => result,
                    _ = revision.changed() => Ok(()),
                };
                if let Err(error) = result {
                    tracing::warn!(venue=V::NAME, code=%error.code, private, "venue stream interrupted");
                }
                let error = LiveError::new(ErrorKind::ConnectionInterrupted);
                if private {
                    tx.send_account(AccountPublication::Error(error));
                } else {
                    tx.send(PublishEvent::ConnectorError(error));
                    for symbol in symbols {
                        tx.send(PublishEvent::StreamInvalidated { symbol, epoch: epoch.saturating_add(1) });
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(client.reconnect_ms())) => {},
                    _ = revision.changed() => {},
                }
            }
        }));
    }
}
#[async_trait]
impl<V: Venue> Connector for VenueConnector<V> {
    fn register_account(&mut self, symbol: String) {
        self.desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .insert(symbol);
        self.changed();
    }
    fn register(&mut self, symbol: String) {
        self.register_account(symbol.clone());
        self.subscribe_market_data(
            symbol,
            vec![
                MarketDataKind::Depth,
                MarketDataKind::Bbo,
                MarketDataKind::Trades,
                MarketDataKind::MarkPrice,
                MarketDataKind::FundingRate,
            ],
        );
    }
    fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        self.desired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .market
            .entry(symbol)
            .or_default()
            .extend(kinds);
        self.changed();
    }
    fn unsubscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        let mut d = self.desired.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(active) = d.market.get_mut(&symbol) {
            for kind in kinds {
                active.remove(&kind);
            }
            if active.is_empty() {
                d.market.remove(&symbol);
            }
        }
        drop(d);
        self.changed();
    }
    fn unregister(&mut self, symbol: String) {
        let mut d = self.desired.lock().unwrap_or_else(|p| p.into_inner());
        d.market.remove(&symbol);
        d.accounts.remove(&symbol);
        drop(d);
        self.changed();
    }
    fn request_snapshot(&mut self, _: String) {
        self.changed();
    }
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.orders.clone()
    }
    fn run(&mut self, tx: PublishSender) {
        self.run_market_data(tx.clone());
        self.run_account(tx);
    }
    fn run_market_data(&mut self, tx: PublishSender) {
        self.start(tx, false);
    }
    fn run_account(&mut self, tx: PublishSender) {
        self.start(tx, true);
    }
    fn broker_api(&self) -> Option<Arc<dyn BrokerApi>> {
        Some(Arc::new(self.client.clone()))
    }
    fn track_managed_order(&self, symbol: &str, id: &str, order: &Order) {
        self.orders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .insert(id.into(), (symbol.into(), order.clone()));
    }
    fn reject_pending_order(&self, id: &str) {
        if let Some((_, o)) = self
            .orders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .get_mut(id)
        {
            crate::utils::reject_pending_order(o);
        }
    }
    fn shutdown(&self) -> ShutdownFuture {
        let handles: Vec<_> = self.tasks.iter().map(|t| t.abort_handle()).collect();
        Box::pin(async move {
            for handle in handles {
                handle.abort();
            }
            Ok(())
        })
    }
}
impl<V: Venue> Drop for VenueConnector<V> {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub(crate) fn publish_order(
    info: &OrderInfo,
    tick: f64,
    orders: &Mutex<Orders>,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    let mut manager = orders.lock().unwrap_or_else(|p| p.into_inner());
    let key = if info.client_order_id.is_empty() {
        format!("venue/{}", info.order_id)
    } else {
        info.client_order_id.clone()
    };
    if manager.0.len() >= 8192 {
        manager.0.retain(|_, (_, o)| o.active() || o.pending());
        if manager.0.len() >= 8192 && !manager.0.contains_key(&key) {
            return Err(error("venue", "order capacity exceeded"));
        }
    }
    let price = if info.price == 0.0 {
        0
    } else {
        crate::units::integer_units(info.price, tick, false).map_err(|e| error("venue", e))?
    };
    let order = manager
        .0
        .entry(key)
        .or_insert_with(|| {
            (
                info.symbol.clone(),
                Order::new(
                    0,
                    price,
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
        })
        .1
        .clone();
    let mut order = order;
    let ns = info
        .update_time
        .checked_mul(1_000_000)
        .ok_or_else(|| error("venue", "order timestamp overflow"))?;
    if order.exch_timestamp > ns || order.qty - order.leaves_qty > info.executed_qty + 1e-12 {
        return Ok(());
    }
    order.price_tick = price;
    order.qty = info.qty;
    order.leaves_qty = info.leaves_qty;
    order.exch_timestamp = ns;
    order.exec_qty = 0.0;
    order.req = Status::None;
    order.status = match info.status {
        ApiOrderStatus::New => Status::New,
        ApiOrderStatus::PartiallyFilled => Status::PartiallyFilled,
        ApiOrderStatus::Filled => Status::Filled,
        ApiOrderStatus::Canceled => Status::Canceled,
        ApiOrderStatus::Expired => Status::Expired,
        ApiOrderStatus::Rejected => Status::Rejected,
        _ => return Err(error("venue", "unsupported order state")),
    };
    manager.0.insert(
        if info.client_order_id.is_empty() {
            format!("venue/{}", info.order_id)
        } else {
            info.client_order_id.clone()
        },
        (info.symbol.clone(), order.clone()),
    );
    drop(manager);
    tx.send_account(AccountPublication::OrderWithNativeFills {
        position_side: info.position_side,
        symbol: info.symbol.clone(),
        client_order_id: (!info.client_order_id.is_empty()).then(|| info.client_order_id.clone()),
        venue_order_id: Some(info.order_id.clone()),
        order,
        average_price: info.avg_price,
        reduce_only: info.reduce_only,
    });
    Ok(())
}
pub(crate) fn publish_depth(
    symbol: &str,
    bids: &[(String, String)],
    asks: &[(String, String)],
    ts: i64,
    stream: MarketStreamMetadata,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    for (p, q) in bids.iter().chain(asks) {
        let p = p.parse::<f64>().ok().filter(|p| p.is_finite() && *p > 0.0);
        let q = q.parse::<f64>().ok().filter(|q| q.is_finite() && *q >= 0.0);
        if p.is_none() || q.is_none() {
            return Err(error("venue", "invalid depth level"));
        }
    }
    let recv = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    if !tx.try_send_native_market(NativeMarketBatch::Depth {
        symbol,
        bids: NativeDepthLevels::Owned(bids),
        asks: NativeDepthLevels::Owned(asks),
        exchange_ts: ts,
        receive_ts: recv,
        stream,
    }) {
        let mut events = Vec::new();
        for (levels, bid) in [(bids, true), (asks, false)] {
            for (p, q) in levels {
                events.push(Event {
                    ev: match (bid, stream.snapshot) {
                        (true, true) => hftbacktest::types::LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                        (false, true) => hftbacktest::types::LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                        (true, false) => hftbacktest::types::LOCAL_BID_DEPTH_EVENT,
                        (false, false) => hftbacktest::types::LOCAL_ASK_DEPTH_EVENT,
                    },
                    exch_ts: ts,
                    local_ts: recv,
                    px: p.parse().unwrap(),
                    qty: q.parse().unwrap(),
                    order_id: 0,
                    ival: 0,
                    fval: 0.0,
                });
            }
        }
        tx.send(PublishEvent::FeedBatch {
            symbol: symbol.into(),
            events,
            stream: Some(stream),
        });
    }
    Ok(())
}
pub(crate) fn levels(v: &Value, venue: &'static str) -> Result<Vec<(String, String)>, ApiError> {
    rows(v, venue)?
        .iter()
        .map(|r| {
            let a = rows(r, venue)?;
            if a.len() != 2 {
                return Err(error(venue, "invalid price level"));
            }
            let s = |v: &Value| match v {
                Value::String(s) => Ok(s.clone()),
                _ => Err(error(venue, "decimal string required")),
            };
            let price = s(&a[0])?;
            let quantity = s(&a[1])?;
            if !price.parse::<f64>().is_ok_and(|v| v.is_finite() && v > 0.0)
                || !quantity
                    .parse::<f64>()
                    .is_ok_and(|v| v.is_finite() && v >= 0.0)
            {
                return Err(error(venue, "invalid price or quantity"));
            }
            Ok((price, quantity))
        })
        .collect()
}
pub(crate) fn publish_bbo(
    symbol: &str,
    bid: f64,
    bq: f64,
    ask: f64,
    aq: f64,
    ts: i64,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    if ![bid, bq, ask, aq].iter().all(|v| v.is_finite())
        || bid <= 0.0
        || ask <= 0.0
        || bq < 0.0
        || aq < 0.0
        || bid > ask
    {
        return Err(error("venue", "invalid BBO"));
    }
    let recv = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    if !tx.try_send_native_market(NativeMarketBatch::Bbo {
        symbol,
        bid_price: bid,
        bid_quantity: bq,
        ask_price: ask,
        ask_quantity: aq,
        exchange_ts: ts,
        receive_ts: recv,
    }) {
        tx.send(PublishEvent::FeedBatch {
            symbol: symbol.into(),
            events: vec![
                Event {
                    ev: hftbacktest::types::LOCAL_BID_DEPTH_BBO_EVENT,
                    exch_ts: ts,
                    local_ts: recv,
                    px: bid,
                    qty: bq,
                    order_id: 0,
                    ival: 0,
                    fval: 0.0,
                },
                Event {
                    ev: hftbacktest::types::LOCAL_ASK_DEPTH_BBO_EVENT,
                    exch_ts: ts,
                    local_ts: recv,
                    px: ask,
                    qty: aq,
                    order_id: 0,
                    ival: 0,
                    fval: 0.0,
                },
            ],
            stream: None,
        });
    }
    Ok(())
}
pub(crate) fn publish_trade(
    symbol: &str,
    price: f64,
    qty: f64,
    sell: bool,
    ts: i64,
    tx: &PublishSender,
) -> Result<(), ApiError> {
    if !price.is_finite() || price <= 0.0 || !qty.is_finite() || qty <= 0.0 {
        return Err(error("venue", "invalid trade"));
    }
    let recv = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    if !tx.try_send_native_market(NativeMarketBatch::Trade {
        symbol,
        price,
        quantity: qty,
        sell,
        exchange_ts: ts,
        receive_ts: recv,
    }) {
        tx.send(PublishEvent::FeedBatch {
            symbol: symbol.into(),
            events: vec![Event {
                ev: if sell {
                    hftbacktest::types::LOCAL_SELL_TRADE_EVENT
                } else {
                    hftbacktest::types::LOCAL_BUY_TRADE_EVENT
                },
                exch_ts: ts,
                local_ts: recv,
                px: price,
                qty,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            }],
            stream: None,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc,
    };
    #[derive(Debug)]
    pub struct Request {
        pub head: String,
        pub body: String,
    }
    pub async fn http(
        responses: Vec<(u16, Value)>,
    ) -> (String, mpsc::Receiver<Request>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(responses.len() + 1);
        let task = tokio::spawn(async move {
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let mut chunk = [0; 4096];
                let mut boundary = None;
                let mut length = 0;
                loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "HTTP client closed early");
                    data.extend_from_slice(&chunk[..n]);
                    if boundary.is_none()
                        && let Some(pos) = data.windows(4).position(|s| s == b"\r\n\r\n")
                    {
                        let header = String::from_utf8_lossy(&data[..pos]).to_lowercase();
                        length = header
                            .lines()
                            .find_map(|s| {
                                s.strip_prefix("content-length:")
                                    .and_then(|s| s.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        boundary = Some(pos + 4);
                    }
                    if let Some(pos) = boundary
                        && data.len() >= pos + length
                    {
                        let head = String::from_utf8(data[..pos].to_vec()).unwrap();
                        let body = String::from_utf8(data[pos..pos + length].to_vec()).unwrap();
                        tx.send(Request { head, body }).await.unwrap();
                        break;
                    }
                    assert!(data.len() < 100_000);
                }
                if status == 0 {
                    std::future::pending::<()>().await;
                }
                let raw = body.to_string();
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}",raw.len()).as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), rx, task)
    }
    pub fn path(label: &str) -> String {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "{}/titan-{label}-{}-{}-{}.jsonl",
            std::env::temp_dir().display(),
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_ids_survive_restart_and_reject_reuse_lock_conflict_and_torn_tail() {
        let path = test_support::path("ids");
        let ids = ClientIds::open(Some(&path), "account1", 3, "test").unwrap();
        assert_eq!(
            ids.lock()
                .unwrap()
                .allocate("00112233445566778899aabbccddeeff")
                .unwrap(),
            1
        );
        assert!(
            ids.lock()
                .unwrap()
                .allocate("00112233445566778899aabbccddeeff")
                .is_err()
        );
        let candidate = ClientIds::open(Some(&path), "account1", 3, "test").unwrap();
        assert!(Arc::ptr_eq(&ids, &candidate));
        let competing = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .unwrap();
        assert!(competing.try_lock().is_err());
        drop(competing);
        drop(candidate);
        drop(ids);
        let ids = ClientIds::open(Some(&path), "account1", 3, "test").unwrap();
        assert_eq!(
            ids.lock().unwrap().original(1),
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(ids.lock().unwrap().allocate("second").unwrap(), 2);
        drop(ids);
        assert!(ClientIds::open(Some(&path), "different-account", 3, "test").is_err());
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"native\":3")
            .unwrap();
        assert!(ClientIds::open(Some(&path), "account1", 3, "test").is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn allocation_never_uses_hashes_or_wraps() {
        let path = test_support::path("capacity");
        let ids = ClientIds::open(Some(&path), "test", 1, "test").unwrap();
        assert_eq!(ids.lock().unwrap().allocate("a").unwrap(), 1);
        assert!(ids.lock().unwrap().allocate("b").is_err());
        assert!(ids.lock().unwrap().native("b").is_err());
        drop(ids);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn request_values_cannot_round_silently_or_escape_endpoints() {
        assert_eq!(decimal(0.000003, 0.000001, "test").unwrap(), "0.000003");
        assert!(decimal(1.001, 0.01, "test").is_err());
        for n in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(decimal(n, 0.1, "test").is_err());
        }
        for u in [
            "http://example.com",
            "https://user:pass@example.com",
            "https://example.com?key=secret",
        ] {
            assert!(endpoint(u, false, "test").is_err());
        }
        assert!(endpoint("http://127.0.0.1:1234", false, "test").is_ok());
    }
    #[tokio::test]
    async fn transport_errors_preserve_outcome_unknown_without_echoing_body() {
        let (url, mut rx, task) =
            test_support::http(vec![(429, serde_json::json!({"secret":"never-echo"}))]).await;
        let e = response(http_client("test").unwrap().get(url).send().await, "test")
            .await
            .unwrap_err();
        rx.recv().await.unwrap();
        task.await.unwrap();
        assert!(e.outcome_unknown());
        assert!(!e.to_string().contains("never-echo"));
    }
}
