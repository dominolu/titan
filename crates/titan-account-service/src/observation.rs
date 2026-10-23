//! Durable observation of account facts. This never projects orders into positions or executes trades.
use crate::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use titan_core_types::{CoreError, EventHandler, EventView};
use titan_domain_types::json_numbers_as_strings as wire;

pub type ObservationResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AccountLedgerRecord {
    pub id: String,
    pub kind: String,
    pub currency: String,
    pub amount: String,
    pub occurred_ms: i64,
    pub instrument: String,
    pub exchange: String,
}
pub type AccountLedgerFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<AccountLedgerRecord>, AccountConnectorError>>
            + Send,
    >,
>;
const CAPACITY: usize = 4096;
#[derive(Default)]
struct Health {
    failed: AtomicBool,
    closing: AtomicBool,
    pending: AtomicUsize,
    dropped: AtomicU64,
}
struct Frame {
    bytes: [u8; 256],
    len: usize,
    schema: u32,
    source_id: u32,
    sequence: u64,
}
struct Sample {
    account: String,
    body: Value,
}
#[derive(Clone, Debug)]
pub struct CashMovement {
    pub id: String,
    pub account: String,
    pub currency_id: u32,
    pub amount_units: i64,
    pub funding: bool,
    pub occurred_at_ns: i64,
    pub provenance: String,
}
struct LedgerBatch {
    account: String,
    from_ms: i64,
    to_ms: i64,
    records: Vec<AccountLedgerRecord>,
    complete: bool,
    error: Option<String>,
}
enum Message {
    Event(Frame),
    Cash(CashMovement),
    Ledger(LedgerBatch),
}

#[derive(Clone, Default)]
struct FillSummary {
    row_id: i64,
    count: u64,
    fees: BTreeMap<(String, String), i128>,
}
pub struct AccountFactJournal {
    path: PathBuf,
    source: String,
    sender: mpsc::SyncSender<Message>,
    health: Arc<Health>,
    sample: Arc<Mutex<BTreeMap<String, Sample>>>,
    writer: Mutex<Option<thread::JoinHandle<()>>>,
    definitions: Arc<BTreeMap<u32, AccountDefinition>>,
    summaries: Mutex<BTreeMap<String, FillSummary>>,
}
impl AccountFactJournal {
    pub fn open(
        path: impl AsRef<Path>,
        source: String,
        accounts: &[AccountDefinition],
    ) -> ObservationResult<Arc<Self>> {
        let path = path.as_ref().to_owned();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(&path)?;
        db.busy_timeout(Duration::from_secs(1))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let version: u32 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > 1 {
            return Err("account observation database is newer than this program".into());
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY,opened_ns TEXT NOT NULL,clean INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS facts(id INTEGER PRIMARY KEY,source TEXT NOT NULL,account TEXT NOT NULL,kind TEXT NOT NULL,event_key TEXT NOT NULL UNIQUE,digest BLOB NOT NULL,body TEXT NOT NULL,raw BLOB);
            CREATE INDEX IF NOT EXISTS facts_query ON facts(account,kind,id);
            CREATE INDEX IF NOT EXISTS facts_source_query ON facts(account,source,kind,id);
            CREATE TABLE IF NOT EXISTS trades(trade_key TEXT PRIMARY KEY,account TEXT NOT NULL,digest BLOB NOT NULL,body TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS trades_account ON trades(account);
            CREATE TABLE IF NOT EXISTS movements(movement_key TEXT PRIMARY KEY,account TEXT NOT NULL,digest BLOB NOT NULL,body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS issues(code TEXT PRIMARY KEY,detail TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS equity_windows(source TEXT NOT NULL,account TEXT NOT NULL,currency TEXT NOT NULL,unit TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(source,account,currency,unit));
            CREATE TABLE IF NOT EXISTS ledger(id TEXT PRIMARY KEY,account TEXT NOT NULL,body TEXT NOT NULL,digest BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS ledger_account ON ledger(account);
            CREATE INDEX IF NOT EXISTS ledger_time ON ledger(account,CAST(json_extract(body,'$.ledger.occurred_ms') AS INTEGER));
            CREATE TABLE IF NOT EXISTS ledger_coverage(account TEXT PRIMARY KEY,body TEXT NOT NULL);
            PRAGMA user_version=1;")?;
        let unclean: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE clean=0)",
            [],
            |r| r.get(0),
        )?;
        if unclean {
            issue(
                &db,
                "RECORDING_GAP",
                "an earlier recorder did not confirm a clean drain",
            )?;
        }
        db.execute(
            "INSERT INTO sources VALUES (?1,?2,0)",
            params![source, now()],
        )?;
        let definitions: Arc<BTreeMap<u32, AccountDefinition>> = Arc::new(
            accounts
                .iter()
                .map(|a| (a.account_id.0, a.clone()))
                .collect(),
        );
        let health = Arc::new(Health::default());
        let sample = Arc::new(Mutex::new(BTreeMap::new()));
        let (sender, receiver) = mpsc::sync_channel(CAPACITY);
        let (h, s, d, identity) = (
            health.clone(),
            sample.clone(),
            definitions.clone(),
            source.clone(),
        );
        let writer = thread::Builder::new()
            .name("account-fact-writer".into())
            .spawn(move || {
                if let Err(error) = record(db, receiver, &identity, &d, &h, &s) {
                    h.failed.store(true, Ordering::Release);
                    tracing::error!(error=%error,"account recording failed; history is incomplete");
                }
            })?;
        Ok(Arc::new(Self {
            path,
            source,
            sender,
            health,
            sample,
            writer: Mutex::new(Some(writer)),
            definitions,
            summaries: Default::default(),
        }))
    }
    fn enqueue(&self, message: Message) -> bool {
        if self.health.closing.load(Ordering::Acquire) {
            self.health.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.health.pending.fetch_add(1, Ordering::AcqRel);
        if self.sender.try_send(message).is_err() {
            self.health.pending.fetch_sub(1, Ordering::AcqRel);
            self.health.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }
    /// Called by the owning platform provider, never by a management HTTP request.
    pub fn cash_movement(&self, movement: CashMovement) -> ObservationResult<()> {
        if movement.id.is_empty()
            || movement.id.len() > 128
            || movement.provenance.len() > 512
            || !self
                .definitions
                .values()
                .any(|d| d.account_key.as_ref() == movement.account)
        {
            return Err("invalid cash movement provenance".into());
        }
        if !self.enqueue(Message::Cash(movement)) {
            return Err("account recorder queue is full or closed".into());
        }
        Ok(())
    }
    pub fn ledger_batch(
        &self,
        account: &str,
        from_ms: i64,
        to_ms: i64,
        records: Vec<AccountLedgerRecord>,
        complete: bool,
        error: Option<String>,
    ) -> ObservationResult<()> {
        if from_ms < 0
            || to_ms < from_ms
            || records.len() > 8000
            || !self
                .definitions
                .values()
                .any(|d| d.account_key.as_ref() == account)
        {
            return Err("invalid ledger batch".into());
        }
        if !self.enqueue(Message::Ledger(LedgerBatch {
            account: account.into(),
            from_ms,
            to_ms,
            records,
            complete,
            error,
        })) {
            return Err("ledger queue unavailable".into());
        }
        Ok(())
    }
    pub fn ledger_cursor(&self, account: &str) -> ObservationResult<Option<i64>> {
        let db =
            Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_millis(200))?;
        let body: Option<String> = db
            .query_row(
                "SELECT body FROM ledger_coverage WHERE account=?1",
                [account],
                |r| r.get(0),
            )
            .optional()?;
        Ok(body
            .map(|body| serde_json::from_str::<Value>(&body))
            .transpose()?
            .and_then(|v| v["to_ms"].as_str().and_then(|s| s.parse().ok())))
    }
    /// Equity sampling uses one coherent account image; private-stream facts use the independent queue.
    pub fn observe_account(
        &self,
        key: &str,
        image: &AccountRecoverySnapshot,
    ) -> ObservationResult<Value> {
        let definition = self
            .definitions
            .get(&image.orders.account.account_id.0)
            .ok_or("unknown observed account")?;
        if definition.account_key.as_ref() != key {
            return Err("account image identity mismatch".into());
        }
        let coherent = AccountRecoverySnapshot::coherent(
            image.orders.clone(),
            image.positions.clone(),
            image.balances.clone(),
        )?;
        let body = account_image(definition, &coherent)?;
        let sampled = json!({"account":key,"source":self.source,"epoch":body["epoch"],"version":body["version"],"captured_at_ns":body["captured_at_ns"],"equity":body["equity"],"completeness":body["completeness"]});
        self.sample
            .lock()
            .map_err(|_| "observation sample lock poisoned")?
            .insert(
                key.into(),
                Sample {
                    account: key.into(),
                    body: sampled,
                },
            );
        Ok(body)
    }
    pub fn health(&self) -> Value {
        json!({"source":self.source,"writer_failed":self.health.failed.load(Ordering::Acquire),"backlog":self.health.pending.load(Ordering::Acquire).to_string(),"dropped":self.health.dropped.load(Ordering::Acquire).to_string(),"history_complete":false,"missing":["pre_observation_history","strategy_attribution"],"ledger_coverage_source":"ledger_coverage"})
    }
    pub fn history(
        &self,
        account: &str,
        kind: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> ObservationResult<Vec<Value>> {
        Self::read_history(&self.path, account, kind, offset, limit)
    }
    pub fn read_history(
        path: &Path,
        account: &str,
        kind: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> ObservationResult<Vec<Value>> {
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_millis(200))?;
        let sql = if kind == Some("ledger") {
            "SELECT body FROM ledger WHERE account=?1 AND ?2='ledger' ORDER BY rowid DESC LIMIT ?3 OFFSET ?4"
        } else if kind == Some("fill") {
            "SELECT body FROM trades WHERE account=?1 AND ?2='fill' ORDER BY rowid DESC LIMIT ?3 OFFSET ?4"
        } else {
            "SELECT body FROM facts WHERE account=?1 AND (?2 IS NULL OR kind=?2) ORDER BY id DESC LIMIT ?3 OFFSET ?4"
        };
        let mut query = db.prepare(sql)?;
        let mut out = Vec::new();
        for row in query.query_map(
            params![account, kind, limit.clamp(1, 200), offset.min(1_000_000)],
            |r| r.get::<_, String>(0),
        )? {
            out.push(wire(serde_json::from_str(&row?)?));
        }
        Ok(out)
    }
    fn summarize_fills(
        &self,
        db: &Connection,
        account: &str,
    ) -> ObservationResult<(FillSummary, bool)> {
        if !self
            .definitions
            .values()
            .any(|d| d.account_key.as_ref() == account)
        {
            return Err("unknown summary account".into());
        }
        let mut cache = self
            .summaries
            .lock()
            .map_err(|_| "fill summary cache is unavailable")?;
        let mut next = cache.get(account).cloned().unwrap_or_default();
        let mut query=db.prepare("SELECT rowid,body FROM trades INDEXED BY trades_account WHERE account=?1 AND rowid>?2 ORDER BY rowid LIMIT 4097")?;
        let mut caught_up = true;
        for (index, row) in query
            .query_map(params![account, next.row_id], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?
            .enumerate()
        {
            if index == 4096 {
                caught_up = false;
                break;
            }
            let (id, body) = row?;
            let trade: Value = serde_json::from_str(&body)?;
            let d = &trade["details"];
            if let (Some(currency), Some(unit), Some(units)) = (
                d["fee_currency"].as_str(),
                d["fee_amount_unit"].as_str(),
                d["fee_amount_units"].as_str(),
            ) {
                let sum = next.fees.entry((currency.into(), unit.into())).or_default();
                *sum = sum
                    .checked_add(units.parse::<i128>()?)
                    .ok_or("observed fee sum overflow")?;
            }
            if next.fees.len() > 4096 {
                return Err("historical fee currency/unit budget exceeded".into());
            }
            next.count = next
                .count
                .checked_add(1)
                .ok_or("observed fill count overflow")?;
            next.row_id = id;
        }
        cache.insert(account.into(), next.clone());
        Ok((next, caught_up))
    }
    pub fn summary(&self, account: &str) -> ObservationResult<Value> {
        let db =
            Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_millis(200))?;
        let (fills, caught_up) = self.summarize_fills(&db, account)?;
        let mut fees = Vec::new();
        for ((currency, unit), units) in fills.fees {
            fees.push(json!({"currency":currency,"amount_unit":unit,"amount_units":units.to_string(),"amount":exact(units,unit.parse::<DecimalUnit>()?)?,"coverage_complete":false}));
        }
        let mut issues = Vec::new();
        let mut stmt = db.prepare("SELECT code,detail FROM issues ORDER BY code")?;
        for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (code, detail) = row?;
            issues.push(json!({"code":code,"detail":detail,"active":true}));
        }
        let mut native = Vec::new();
        let mut stmt = db.prepare(
            "SELECT body FROM equity_windows WHERE account=?1 AND source=?2 ORDER BY currency,unit",
        )?;
        for row in stmt.query_map(params![account, self.source], |r| r.get::<_, String>(0))? {
            native.push(serde_json::from_str::<Value>(&row?)?);
        }
        let coverage: Option<String> = db
            .query_row(
                "SELECT body FROM ledger_coverage WHERE account=?1",
                [account],
                |r| r.get(0),
            )
            .optional()?;
        let coverage = coverage
            .map(|s| serde_json::from_str::<Value>(&s))
            .transpose()?;
        let performance = if issues.is_empty()
            && !self.health.failed.load(Ordering::Acquire)
            && self.health.dropped.load(Ordering::Acquire) == 0
        {
            covered_performance(&db, &self.source, account, coverage.as_ref())
                .unwrap_or_else(|error| json!({"available":false,"reason":error.to_string()}))
        } else {
            json!({"available":false,"reason":"recording continuity or financial identity has unresolved diagnostics"})
        };
        for (code, active, detail) in [
            (
                "WRITER_FAILED",
                self.health.failed.load(Ordering::Acquire),
                "account fact writer failed",
            ),
            (
                "RECORDING_OVERFLOW",
                self.health.dropped.load(Ordering::Acquire) > 0,
                "bounded account queue lost facts",
            ),
            (
                "RECORDING_BACKLOG",
                self.health.pending.load(Ordering::Acquire) > CAPACITY * 3 / 4,
                "account recorder backlog exceeded its observation threshold",
            ),
            (
                "FEE_SUMMARY_PENDING",
                !caught_up,
                "historical fill summary is catching up within its bounded batch budget",
            ),
        ] {
            if !issues.iter().any(|v| v["code"] == code) {
                issues.push(json!({"code":code,"active":active,"detail":detail}));
            }
        }
        Ok(
            json!({"recording":self.health(),"ledger_coverage":coverage,"fee_summary_caught_up":caught_up,"processed_fill_count":fills.count.to_string(),"independent_fills_observed":if caught_up{Some(fills.count.to_string())}else{None},"observed_fees":if caught_up{Some(fees)}else{None},"native_performance":native,"performance":performance,"net_pnl":performance.get("net_pnl"),"max_drawdown":performance.get("max_drawdown"),"attribution":"account","diagnostics":issues}),
        )
    }
    pub fn close(&self, deadline: Instant) -> ObservationResult<()> {
        self.health.closing.store(true, Ordering::Release);
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| "recorder join lock poisoned")?;
        while writer.as_ref().is_some_and(|w| !w.is_finished()) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        if writer.as_ref().is_some_and(|w| !w.is_finished()) {
            return Err("account recorder drain deadline exceeded".into());
        }
        if let Some(writer) = writer.take() {
            writer.join().map_err(|_| "account recorder panicked")?;
        }
        if self.health.failed.load(Ordering::Acquire) {
            return Err("account recorder failed".into());
        }
        Ok(())
    }
}
impl EventHandler for AccountFactJournal {
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError> {
        if event.payload.len() > 256 || decode_account_event_header(event.payload).is_err() {
            self.health.failed.store(true, Ordering::Release);
            return Ok(());
        }
        let mut frame = Frame {
            bytes: [0; 256],
            len: event.payload.len(),
            schema: event.schema_version,
            source_id: event.metadata.source_id,
            sequence: event.metadata.source_sequence,
        };
        frame.bytes[..frame.len].copy_from_slice(event.payload);
        self.enqueue(Message::Event(frame));
        Ok(())
    }
}
impl Drop for AccountFactJournal {
    fn drop(&mut self) {
        self.health.closing.store(true, Ordering::Release);
    }
}
fn now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn issue(db: &Connection, code: &str, detail: &str) -> ObservationResult<()> {
    db.execute(
        "INSERT OR IGNORE INTO issues VALUES (?1,?2)",
        params![code, detail],
    )?;
    Ok(())
}
fn exact(units: i128, unit: DecimalUnit) -> ObservationResult<String> {
    let scaled = units
        .checked_mul(i128::from(unit.coefficient()))
        .ok_or("observation decimal overflow")?;
    let digits = scaled.unsigned_abs().to_string();
    let scale = usize::from(unit.scale());
    let sign = if scaled < 0 { "-" } else { "" };
    if scale == 0 {
        return Ok(format!("{sign}{digits}"));
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let split = padded.len() - scale;
    let fraction = padded[split..].trim_end_matches('0');
    Ok(if fraction.is_empty() {
        format!("{sign}{}", &padded[..split])
    } else {
        format!("{sign}{}.{fraction}", &padded[..split])
    })
}
fn account_image(
    definition: &AccountDefinition,
    image: &AccountRecoverySnapshot,
) -> ObservationResult<Value> {
    let mut equity = Vec::new();
    for balance in image.balances.items.iter() {
        let currency = definition
            .currencies
            .iter()
            .find(|c| c.currency_id == balance.currency_id);
        let amount = currency
            .map(|c| {
                exact(
                    i128::from(balance.wallet_units) + i128::from(balance.unrealized_pnl_units),
                    c.amount_unit,
                )
            })
            .transpose()?;
        equity.push(json!({"currency_id":balance.currency_id.0.to_string(),"currency":currency.map(|c|c.native_currency.as_ref()),"amount":amount,"amount_units":(i128::from(balance.wallet_units)+i128::from(balance.unrealized_pnl_units)).to_string(),"amount_unit":currency.map(|c|c.amount_unit.to_string()),"valuation_complete":currency.is_some()}));
    }
    Ok(wire(
        json!({"account":definition.account_key,"account_id":definition.account_id.0.to_string(),"account_generation":image.orders.account.generation.to_string(),"provider":definition.connector_type,"captured_at_ns":image.orders.captured_at.to_string(),"epoch":image.orders.committed_epoch.map(|v|v.to_string()),"version":image.orders.committed_version.map(|v|v.to_string()),"orders":image.orders.items,"positions":image.positions.items,"balances":image.balances.items,"instruments":definition.instruments,"currencies":definition.currencies,"equity":equity,"risk":known_account_quantity_bound(image)?,"attribution":"account","net_pnl":null,"max_drawdown":null,"max_exposure":null,"completeness":{"account_image":true,"performance":false,"risk":false,"missing":["funding_coverage","cashflow_coverage","mark_price","unsettled_fill_reconciliation"]}}),
    ))
}
fn known_account_quantity_bound(image: &AccountRecoverySnapshot) -> ObservationResult<Value> {
    let mut amounts: BTreeMap<u32, (i128, i128)> = BTreeMap::new();
    for position in image.positions.items.iter() {
        let value = amounts.entry(position.asset_id.0).or_default();
        value.0 = value
            .0
            .checked_add(i128::from(position.quantity_lots).abs())
            .ok_or("position lot overflow")?;
    }
    for order in image
        .orders
        .items
        .iter()
        .filter(|o| matches!(o.status, 1 | 5))
    {
        let remaining = order
            .quantity_lots
            .checked_sub(order.filled_quantity_lots)
            .filter(|q| *q >= 0)
            .ok_or("invalid cumulative order quantity")?;
        let value = amounts.entry(order.asset_id.0).or_default();
        value.1 = value
            .1
            .checked_add(i128::from(remaining))
            .ok_or("pending lot overflow")?;
    }
    let mut assets = Vec::new();
    for (asset, (actual, pending)) in amounts {
        assets.push(json!({"asset_id":asset.to_string(),"actual_gross_lots":actual.to_string(),"known_order_remaining_lots":pending.to_string(),"known_quantity_bound_lots":actual.checked_add(pending).ok_or("quantity bound overflow")?.to_string()}));
    }
    Ok(
        json!({"scope":"coherent_account_image","assets":assets,"notional":null,"complete":false,"missing":["mark_price","unacknowledged_intents","unsettled_fill_reconciliation"]}),
    )
}
/// Native-currency observations include external cash flows. They are not net strategy returns.
fn equity_window(
    db: &Connection,
    source: &str,
    account: &str,
    sample: &Value,
) -> ObservationResult<()> {
    for value in sample["equity"]
        .as_array()
        .ok_or("equity sample has no values")?
    {
        let (Some(currency), Some(unit)) =
            (value["currency"].as_str(), value["amount_unit"].as_str())
        else {
            continue;
        };
        let current = value["amount_units"]
            .as_str()
            .ok_or("equity units missing")?
            .parse::<i128>()?;
        let old:Option<String>=db.query_row("SELECT body FROM equity_windows WHERE source=?1 AND account=?2 AND currency=?3 AND unit=?4",params![source,account,currency,unit],|r|r.get(0)).optional()?;
        let old: Option<Value> = old.map(|body| serde_json::from_str(&body)).transpose()?;
        let get = |key: &str| -> ObservationResult<i128> {
            Ok(old
                .as_ref()
                .and_then(|v| v[key].as_str())
                .ok_or("equity window is corrupt")?
                .parse()?)
        };
        let (first, peak, drawdown) = if old.is_some() {
            (
                get("first_units")?,
                get("peak_units")?.max(current),
                get("max_observed_decline_units")?,
            )
        } else {
            (current, current, 0)
        };
        let decline = peak
            .checked_sub(current)
            .ok_or("equity decline overflow")?
            .max(drawdown);
        let change = current.checked_sub(first).ok_or("equity change overflow")?;
        let amount_unit: DecimalUnit = unit.parse()?;
        let body = json!({"source":source,"account":account,"currency":currency,"amount_unit":unit,"first_units":first.to_string(),"current_units":current.to_string(),"peak_units":peak.to_string(),"max_observed_decline_units":decline.to_string(),"observed_equity_change":exact(change,amount_unit)?,"max_observed_equity_decline":exact(decline,amount_unit)?,"includes_external_flows":true,"net_pnl":null,"coverage_complete":false,"first_at_ns":old.as_ref().map(|v|&v["first_at_ns"]).unwrap_or(&sample["captured_at_ns"]),"last_at_ns":sample["captured_at_ns"]});
        db.execute("INSERT INTO equity_windows VALUES (?1,?2,?3,?4,?5) ON CONFLICT(source,account,currency,unit) DO UPDATE SET body=excluded.body",params![source,account,currency,unit,serde_json::to_string(&body)?])?;
    }
    Ok(())
}
fn record(
    mut db: Connection,
    receiver: mpsc::Receiver<Message>,
    source: &str,
    definitions: &BTreeMap<u32, AccountDefinition>,
    health: &Health,
    samples: &Mutex<BTreeMap<String, Sample>>,
) -> ObservationResult<()> {
    loop {
        let mut batch = Vec::new();
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(item) => batch.push(item),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        batch.extend(receiver.try_iter().take(255));
        let count = batch.len();
        let images = std::mem::take(
            &mut *samples
                .lock()
                .map_err(|_| "observation sample lock poisoned")?,
        );
        if count > 0 || !images.is_empty() {
            let tx = db.transaction()?;
            for message in batch {
                match message {
                    Message::Event(frame) => event(&tx, source, definitions, frame)?,
                    Message::Cash(cash) => cash_fact(&tx, source, definitions, cash)?,
                    Message::Ledger(batch) => ledger_fact(&tx, source, definitions, batch)?,
                }
            }
            for sample in images.into_values() {
                let key = format!(
                    "{source}:image:{}:{}:{}",
                    sample.account, sample.body["epoch"], sample.body["version"]
                );
                let content = serde_json::to_string(&sample.body)?;
                let hash = Sha256::digest(content.as_bytes());
                if tx.execute("INSERT OR IGNORE INTO facts(source,account,kind,event_key,digest,body) VALUES (?1,?2,'equity_sample',?3,?4,?5)",params![source,sample.account,key,&hash[..],content])?>0{equity_window(&tx,source,&sample.account,&sample.body)?;}
            }
            tx.commit()?;
            health.pending.fetch_sub(count, Ordering::AcqRel);
        }
        if health.closing.load(Ordering::Acquire) && health.pending.load(Ordering::Acquire) == 0 {
            break;
        }
    }
    if health.dropped.load(Ordering::Acquire) > 0 {
        issue(
            &db,
            "RECORDING_OVERFLOW",
            "bounded recorder queue lost facts; continuity is not restored by a later snapshot",
        )?;
    }
    if !health.failed.load(Ordering::Acquire) {
        db.execute("UPDATE sources SET clean=1 WHERE id=?1", [source])?;
    }
    Ok(())
}
fn event(
    db: &Connection,
    source: &str,
    definitions: &BTreeMap<u32, AccountDefinition>,
    frame: Frame,
) -> ObservationResult<()> {
    let bytes = &frame.bytes[..frame.len];
    let h = decode_account_event_header(bytes)?;
    let definition = definitions
        .get(&h.account_id)
        .ok_or("unregistered observation account")?;
    let mut body = json!({"source":source,"schema_version":frame.schema.to_string(),"account":definition.account_key,"provider":definition.connector_type,"account_id":h.account_id.to_string(),"generation":h.account_generation.to_string(),"epoch":h.account_epoch.to_string(),"version":h.account_version.to_string(),"exchange_ns":h.exchange_ts.to_string(),"receive_ns":h.receive_ts.to_string(),"source_id":frame.source_id.to_string(),"sequence":frame.sequence.to_string(),"flags":h.flags.to_string()});
    let (kind, details) = match h.kind {
        event_kind::ORDER_CHANGED => {
            let v = OrderChangedV1::decode(bytes)?;
            (
                "order",
                wire(
                    json!({"asset_id":v.asset_id,"instrument_binding":definition.instruments.iter().find(|i|i.asset_id.0==v.asset_id),"side":v.side,"position_side":event_flags::position_side(h.flags),"status":v.status,"price_ticks":v.price_ticks,"quantity_lots":v.quantity_lots,"filled_quantity_lots":v.filled_quantity_lots,"venue_order_id":hex(&v.venue_order_id.0),"client_order_id":hex(&v.client_order_id.0),"command_id":hex(&v.command_id.0)}),
                ),
            )
        }
        event_kind::FILL if frame.schema == FILL_EVENT_SCHEMA_VERSION => {
            let v = FillV2::decode(bytes)?;
            let binding = definition
                .instruments
                .iter()
                .find(|i| i.asset_id.0 == v.asset_id);
            let symbol = binding
                .map(|i| i.native_symbol.as_ref())
                .unwrap_or("UNKNOWN");
            let currency = definition
                .currencies
                .iter()
                .find(|c| c.currency_id.0 == v.fee_currency_id);
            if binding.is_none() {
                issue(
                    db,
                    "INSTRUMENT_UNITS_UNAVAILABLE",
                    "execution units have no immutable instrument binding",
                )?;
            }
            if currency.is_none() {
                issue(
                    db,
                    "FEE_CURRENCY_UNAVAILABLE",
                    "execution fee currency or its amount unit is unavailable",
                )?;
            }
            let fee = currency
                .map(|c| exact(i128::from(v.fee_amount_units), c.amount_unit))
                .transpose()?;
            let details = wire(
                json!({"asset_id":v.asset_id,"instrument":symbol,"instrument_binding":binding,"side":v.side,"position_side":event_flags::position_side(h.flags),"price_ticks":v.price_ticks,"last_fill_quantity_lots":v.last_fill_quantity_lots,"cumulative_filled_quantity_lots":v.cumulative_filled_quantity_lots,"trade_id":hex(&v.trade_id.0),"venue_order_id":hex(&v.venue_order_id.0),"client_order_id":hex(&v.client_order_id.0),"command_id":hex(&v.command_id.0),"fee_amount_units":v.fee_amount_units,"fee_currency_id":v.fee_currency_id,"fee_currency":currency.map(|c|c.native_currency.as_ref()),"fee_amount_unit":currency.map(|c|c.amount_unit.to_string()),"observed_fee_amount":fee,"realized_pnl_units":v.realized_pnl_units}),
            );
            if v.trade_id.0 == [0; 16] {
                issue(
                    db,
                    "MISSING_TRADE_ID",
                    "independent execution identity is missing; no quantity inferred from order totals",
                )?;
            } else {
                let key = serde_json::to_string(&(
                    definition.connector_type.as_ref(),
                    definition.account_key.as_ref(),
                    symbol,
                    hex(&v.trade_id.0),
                ))?;
                let fingerprint = serde_json::to_vec(&json!([
                    v.asset_id,
                    v.side,
                    v.price_ticks,
                    v.last_fill_quantity_lots,
                    v.fee_amount_units,
                    v.fee_currency_id,
                    v.realized_pnl_units,
                    event_flags::position_side(h.flags),
                    binding,
                    currency.map(|c| (c.native_currency.as_ref(), c.amount_unit.to_string())),
                    hex(&v.venue_order_id.0)
                ]))?;
                let hash = Sha256::digest(fingerprint);
                let previous: Option<Vec<u8>> = db
                    .query_row(
                        "SELECT digest FROM trades WHERE trade_key=?1",
                        [&key],
                        |r| r.get(0),
                    )
                    .optional()?;
                if previous.as_deref().is_some_and(|old| old != &hash[..]) {
                    issue(
                        db,
                        "TRADE_ID_CONFLICT",
                        "the same execution identity has contradictory financial facts",
                    )?;
                }
                let mut trade = body.clone();
                trade["details"] = details.clone();
                trade["kind"] = "fill".into();
                db.execute(
                    "INSERT OR IGNORE INTO trades VALUES (?1,?2,?3,?4)",
                    params![
                        key,
                        definition.account_key.as_ref(),
                        &hash[..],
                        serde_json::to_string(&trade)?
                    ],
                )?;
            }
            ("fill", details)
        }
        event_kind::POSITION_CHANGED => {
            let v = PositionChangedV1::decode(bytes)?;
            (
                "position",
                wire(
                    json!({"asset_id":v.asset_id,"instrument_binding":definition.instruments.iter().find(|i|i.asset_id.0==v.asset_id),"position_side":v.position_side,"quantity_lots":v.quantity_lots,"entry_price_ticks":v.entry_price_ticks,"margin_currency_id":v.margin_currency_id,"currency_binding":definition.currencies.iter().find(|c|c.currency_id.0==v.margin_currency_id),"realized_pnl_units":v.realized_pnl_units,"unrealized_pnl_units":v.unrealized_pnl_units}),
                ),
            )
        }
        event_kind::BALANCE_CHANGED => {
            let v = BalanceChangedV1::decode(bytes)?;
            (
                "balance",
                wire(
                    json!({"currency_id":v.currency_id,"currency_binding":definition.currencies.iter().find(|c|c.currency_id.0==v.currency_id),"wallet_units":v.wallet_units,"available_units":v.available_units,"margin_units":v.margin_units,"unrealized_pnl_units":v.unrealized_pnl_units}),
                ),
            )
        }
        event_kind::STREAM_INVALIDATED => {
            let v = StreamInvalidatedV1::decode(bytes)?;
            issue(
                db,
                "ACCOUNT_STREAM_GAP",
                "account stream invalidated; historical completeness needs provider evidence",
            )?;
            (
                "stream",
                wire(json!({"state":v.0.state,"reason_code":v.0.reason_code})),
            )
        }
        event_kind::STREAM_STATE_CHANGED => {
            let v = StreamStateChangedV1::decode(bytes)?;
            (
                "stream",
                wire(json!({"state":v.0.state,"reason_code":v.0.reason_code})),
            )
        }
        _ => {
            issue(
                db,
                "UNSUPPORTED_EVENT_SCHEMA",
                "raw fact retained; financial projection unavailable",
            )?;
            ("unsupported", json!({"schema":frame.schema.to_string()}))
        }
    };
    body["kind"] = kind.into();
    body["details"] = details;
    let key = format!("{source}:{}:{}", frame.source_id, frame.sequence);
    let hash = Sha256::digest(bytes);
    let previous: Option<Vec<u8>> = db
        .query_row("SELECT digest FROM facts WHERE event_key=?1", [&key], |r| {
            r.get(0)
        })
        .optional()?;
    if previous.as_deref().is_some_and(|old| old != &hash[..]) {
        issue(
            db,
            "SOURCE_SEQUENCE_CONFLICT",
            "publication identity was reused with different bytes",
        )?;
    }
    db.execute("INSERT OR IGNORE INTO facts(source,account,kind,event_key,digest,body,raw) VALUES (?1,?2,?3,?4,?5,?6,?7)",params![source,definition.account_key.as_ref(),kind,key,&hash[..],serde_json::to_string(&body)?,bytes])?;
    Ok(())
}
fn ledger_fact(
    db: &Connection,
    source: &str,
    definitions: &BTreeMap<u32, AccountDefinition>,
    batch: LedgerBatch,
) -> ObservationResult<()> {
    let definition = definitions
        .values()
        .find(|d| d.account_key.as_ref() == batch.account)
        .ok_or("ledger account is not bound")?;
    let mut complete = batch.complete;
    for record in batch.records {
        if record.id.is_empty()
            || [
                &record.id,
                &record.kind,
                &record.currency,
                &record.amount,
                &record.instrument,
                &record.exchange,
            ]
            .iter()
            .any(|s| s.len() > 256)
            || record.occurred_ms < batch.from_ms
            || record.occurred_ms > batch.to_ms
        {
            return Err("ledger record violates provider contract".into());
        }
        let kind = match record.kind.as_str() {
            "FUNDING_FEE" => "funding",
            "TRANSFER_IN" | "TRANSFER_OUT" => "cashflow",
            _ => "ledger",
        };
        let known = matches!(
            record.kind.as_str(),
            "FUNDING_FEE"
                | "TRANSFER_IN"
                | "TRANSFER_OUT"
                | "TRANSACTION"
                | "TRADING_FEE"
                | "LIQUIDATION_FEE"
                | "BANKRUPT_COMPENSATION"
                | "AUTO_REPAY"
                | "INTEREST_ISOLATED"
        );
        if !known {
            complete = false;
            issue(
                db,
                "UNCLASSIFIED_LEDGER_ENTRY",
                "unknown ledger semantics prevents a complete performance claim",
            )?;
        }
        let key = serde_json::to_string(&(
            definition.connector_type.as_ref(),
            batch.account.as_str(),
            record.exchange.as_str(),
            record.id.as_str(),
        ))?;
        let body = json!({"source":source,"account":batch.account,"provider":definition.connector_type,"kind":kind,"ledger":record});
        // Source identity is provenance, not part of the exchange financial identity.
        let hash = Sha256::digest(serde_json::to_vec(&record)?);
        let previous: Option<Vec<u8>> = db
            .query_row("SELECT digest FROM ledger WHERE id=?1", [&key], |r| {
                r.get(0)
            })
            .optional()?;
        if previous.as_deref().is_some_and(|old| old != &hash[..]) {
            complete = false;
            issue(
                db,
                "LEDGER_ID_CONFLICT",
                "same ledger identity has contradictory facts",
            )?;
        }
        let content = serde_json::to_string(&body)?;
        if db.execute(
            "INSERT OR IGNORE INTO ledger VALUES (?1,?2,?3,?4)",
            params![key, batch.account, content, &hash[..]],
        )? > 0
        {
            db.execute("INSERT OR IGNORE INTO facts(source,account,kind,event_key,digest,body) VALUES (?1,?2,?3,?4,?5,?6)",params![source,batch.account,kind,format!("ledger:{key}"),&hash[..],content])?;
        }
    }
    let old: Option<String> = db
        .query_row(
            "SELECT body FROM ledger_coverage WHERE account=?1",
            [&batch.account],
            |r| r.get(0),
        )
        .optional()?;
    let old: Option<Value> = old.map(|s| serde_json::from_str(&s)).transpose()?;
    let previous = old
        .as_ref()
        .and_then(|v| v["to_ms"].as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let from = if previous.is_some_and(|end| end >= batch.from_ms) {
        old.as_ref()
            .and_then(|v| v["from_ms"].as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(batch.from_ms)
    } else {
        batch.from_ms
    };
    let unresolved:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM issues WHERE code IN ('LEDGER_ID_CONFLICT','UNCLASSIFIED_LEDGER_ENTRY'))",[],|r|r.get(0))?;
    complete &= !unresolved;
    let coverage = json!({"source":source,"from_ms":from.to_string(),"to_ms":if complete{batch.to_ms}else{previous.unwrap_or(batch.from_ms)}.to_string(),"complete":complete,"last_attempt_to_ms":batch.to_ms.to_string(),"error":batch.error,"scope":"account_ledger","strategy_attribution":false});
    db.execute("INSERT INTO ledger_coverage VALUES (?1,?2) ON CONFLICT(account) DO UPDATE SET body=excluded.body",params![batch.account,serde_json::to_string(&coverage)?])?;
    Ok(())
}
fn decimal_units(text: &str, unit: DecimalUnit) -> ObservationResult<i128> {
    if text.len() > 80 {
        return Err("ledger decimal is too long".into());
    }
    let (negative, text) = if let Some(text) = text.strip_prefix('-') {
        (true, text)
    } else {
        (false, text)
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty()
        || fraction.len() > 18
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err("invalid exact ledger decimal".into());
    }
    let digits = format!("{whole}{fraction}").parse::<i128>()?;
    let numerator = digits
        .checked_mul(10i128.pow(u32::from(unit.scale())))
        .ok_or("ledger amount overflow")?;
    let denominator = 10i128
        .pow(fraction.len() as u32)
        .checked_mul(i128::from(unit.coefficient()))
        .ok_or("ledger unit overflow")?;
    if numerator % denominator != 0 {
        return Err("ledger amount is not representable in account currency units".into());
    }
    let value = numerator / denominator;
    Ok(if negative { -value } else { value })
}
/// Sampled account performance, net of external transfers. Fees/funding already affect equity.
fn covered_performance(
    db: &Connection,
    source: &str,
    account: &str,
    coverage: Option<&Value>,
) -> ObservationResult<Value> {
    let coverage = coverage.ok_or("ledger coverage is unavailable")?;
    if coverage["complete"] != true || coverage["source"].as_str() != Some(source) {
        return Err("ledger interval is incomplete or belongs to another source".into());
    }
    let from = coverage["from_ms"]
        .as_str()
        .ok_or("missing ledger start")?
        .parse::<i64>()?
        .checked_mul(1_000_000)
        .ok_or("ledger time overflow")?;
    let to = coverage["to_ms"]
        .as_str()
        .ok_or("missing ledger end")?
        .parse::<i64>()?
        .checked_mul(1_000_000)
        .ok_or("ledger time overflow")?;
    let mut query=db.prepare("SELECT body FROM facts INDEXED BY facts_source_query WHERE account=?1 AND source=?2 AND kind='equity_sample' AND CAST(json_extract(body,'$.captured_at_ns') AS INTEGER) BETWEEN ?3 AND ?4 ORDER BY id LIMIT 8193")?;
    let samples: Vec<Value> = query
        .query_map(params![account, source, from, to], |r| {
            r.get::<_, String>(0)
        })?
        .map(|row| Ok(serde_json::from_str::<Value>(&row?)?))
        .collect::<ObservationResult<_>>()?;
    if samples.len() < 2 {
        return Err("not enough equity samples inside the covered ledger interval".into());
    }
    if samples.len() > 8192 {
        return Err("performance sample budget exceeded".into());
    }
    let first = samples[0]["captured_at_ns"]
        .as_str()
        .ok_or("equity time missing")?
        .parse::<i64>()?;
    let last = samples.last().unwrap()["captured_at_ns"]
        .as_str()
        .ok_or("equity time missing")?
        .parse::<i64>()?;
    let mut query=db.prepare("SELECT body FROM ledger INDEXED BY ledger_time WHERE account=?1 AND json_extract(body,'$.ledger.kind') IN ('TRANSFER_IN','TRANSFER_OUT') AND CAST(json_extract(body,'$.ledger.occurred_ms') AS INTEGER) BETWEEN ?2 AND ?3 ORDER BY CAST(json_extract(body,'$.ledger.occurred_ms') AS INTEGER) LIMIT 8193")?;
    let flows: Vec<AccountLedgerRecord> = query
        .query_map(params![account, first / 1_000_000, last / 1_000_000], |r| {
            r.get::<_, String>(0)
        })?
        .map(|row| {
            let body: Value = serde_json::from_str(&row?)?;
            Ok(serde_json::from_value(body["ledger"].clone())?)
        })
        .collect::<ObservationResult<_>>()?;
    if flows.len() > 8192 {
        return Err("performance cashflow budget exceeded".into());
    }
    struct NativePerformance {
        first: i128,
        peak: i128,
        decline: i128,
        pnl: i128,
        points: usize,
    }
    let mut metrics: BTreeMap<(String, String), NativePerformance> = BTreeMap::new();
    let mut flow_totals: BTreeMap<(String, String), i128> = BTreeMap::new();
    let mut cursor = 0;
    let mut previous = 0;
    for sample in &samples {
        let at = sample["captured_at_ns"]
            .as_str()
            .ok_or("equity sample timestamp missing")?
            .parse::<i64>()?;
        if at < previous {
            return Err("equity sample clock moved backwards".into());
        }
        previous = at;
        let equities = sample["equity"].as_array().ok_or("equity values missing")?;
        while cursor < flows.len() && flows[cursor].occurred_ms <= at / 1_000_000 {
            let flow = &flows[cursor];
            cursor += 1;
            if flow.occurred_ms == first / 1_000_000 || flow.occurred_ms == at / 1_000_000 {
                return Err(
                    "cashflow millisecond timestamp overlaps an equity sample boundary".into(),
                );
            }
            if flow.occurred_ms < first / 1_000_000 {
                continue;
            }
            let value = equities
                .iter()
                .find(|v| v["currency"].as_str() == Some(flow.currency.as_str()))
                .ok_or("cashflow currency has no account equity valuation")?;
            let unit = value["amount_unit"]
                .as_str()
                .ok_or("currency unit missing")?;
            let delta = decimal_units(&flow.amount, unit.parse()?)?;
            if (flow.kind == "TRANSFER_IN" && delta < 0)
                || (flow.kind == "TRANSFER_OUT" && delta > 0)
            {
                return Err("external cashflow direction contradicts its signed amount".into());
            }
            let total = flow_totals
                .entry((flow.currency.clone(), unit.into()))
                .or_default();
            *total = total.checked_add(delta).ok_or("cashflow sum overflow")?;
        }
        for equity in equities {
            let currency = equity["currency"]
                .as_str()
                .ok_or("equity currency unavailable")?;
            let unit = equity["amount_unit"]
                .as_str()
                .ok_or("equity unit unavailable")?;
            let key = (currency.to_owned(), unit.to_owned());
            let current = equity["amount_units"]
                .as_str()
                .ok_or("equity units missing")?
                .parse::<i128>()?
                .checked_sub(*flow_totals.get(&key).unwrap_or(&0))
                .ok_or("normalized equity overflow")?;
            let row = metrics.entry(key).or_insert(NativePerformance {
                first: current,
                peak: current,
                decline: 0,
                pnl: 0,
                points: 0,
            });
            row.peak = row.peak.max(current);
            row.decline = row
                .decline
                .max(row.peak.checked_sub(current).ok_or("drawdown overflow")?);
            row.pnl = current
                .checked_sub(row.first)
                .ok_or("net equity change overflow")?;
            row.points += 1;
        }
    }
    let mut net = Vec::new();
    let mut drawdown = Vec::new();
    for ((currency, unit), metric) in metrics {
        if metric.points != samples.len() {
            return Err("currency coverage changed inside the performance window".into());
        }
        let unit: DecimalUnit = unit.parse()?;
        net.push(json!({"currency":currency,"amount":exact(metric.pnl,unit)?}));
        drawdown.push(json!({"currency":currency,"amount":exact(metric.decline,unit)?}));
    }
    if net.is_empty() {
        return Err("equity valuation is empty".into());
    }
    Ok(
        json!({"available":true,"source":"sampled_account_equity_and_provider_ledger","scope":"account","strategy_attribution":false,"from_ns":first.to_string(),"as_of_ns":last.to_string(),"sample_count":samples.len().to_string(),"net_pnl":net,"max_drawdown":drawdown,"coverage_complete":false,"ledger_interval_complete":true,"missing":["account_ledger_watermark_link","strategy_attribution"],"fees_and_funding":"already included in equity; never deducted twice","drawdown_method":"maximum observed decline of equity after external transfers"}),
    )
}
fn cash_fact(
    db: &Connection,
    source: &str,
    definitions: &BTreeMap<u32, AccountDefinition>,
    cash: CashMovement,
) -> ObservationResult<()> {
    let definition = definitions
        .values()
        .find(|d| d.account_key.as_ref() == cash.account)
        .ok_or("unknown cash movement account")?;
    let currency = definition
        .currencies
        .iter()
        .find(|c| c.currency_id.0 == cash.currency_id)
        .ok_or("unknown cash movement currency")?;
    let kind = if cash.funding { "funding" } else { "cashflow" };
    let body = json!({"source":source,"kind":kind,"id":cash.id,"account":cash.account,"currency":currency.native_currency,"amount_unit":currency.amount_unit.to_string(),"amount_units":cash.amount_units.to_string(),"amount":exact(i128::from(cash.amount_units),currency.amount_unit)?,"occurred_at_ns":cash.occurred_at_ns.to_string(),"provenance":cash.provenance});
    let content = serde_json::to_string(&body)?;
    let mut identity = body.clone();
    identity.as_object_mut().unwrap().remove("source");
    identity.as_object_mut().unwrap().remove("provenance");
    let hash = Sha256::digest(serde_json::to_vec(&identity)?);
    let key = serde_json::to_string(&(
        definition.connector_type.as_ref(),
        cash.account.as_str(),
        kind,
        cash.id.as_str(),
    ))?;
    let previous: Option<Vec<u8>> = db
        .query_row(
            "SELECT digest FROM movements WHERE movement_key=?1",
            [&key],
            |r| r.get(0),
        )
        .optional()?;
    if previous.as_deref().is_some_and(|old| old != &hash[..]) {
        issue(
            db,
            "CASH_ID_CONFLICT",
            "cash/funding identity has contradictory values",
        )?;
    }
    if db.execute(
        "INSERT OR IGNORE INTO movements VALUES (?1,?2,?3,?4)",
        params![key, cash.account, &hash[..], content],
    )? > 0
    {
        db.execute("INSERT INTO facts(source,account,kind,event_key,digest,body) VALUES (?1,?2,?3,?4,?5,?6)",params![source,cash.account,kind,key,&hash[..],serde_json::to_string(&body)?])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_fill_history_is_summarized_in_bounded_batches_without_partial_totals() {
        let tmp = tempfile::tempdir().unwrap();
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[definition()])
                .unwrap();
        let mut db = Connection::open(&journal.path).unwrap();
        let tx = db.transaction().unwrap();
        let body=json!({"details":{"fee_currency":"USDT","fee_amount_unit":"0.000001","fee_amount_units":"1"}}).to_string();
        for id in 0..4100 {
            tx.execute(
                "INSERT INTO trades VALUES (?1,'gate',X'00',?2)",
                params![id.to_string(), body],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let first = journal.summary("gate").unwrap();
        assert_eq!(first["fee_summary_caught_up"], false);
        assert_eq!(first["processed_fill_count"], "4096");
        assert!(first["independent_fills_observed"].is_null());
        assert!(first["observed_fees"].is_null());
        let second = journal.summary("gate").unwrap();
        assert_eq!(second["fee_summary_caught_up"], true);
        assert_eq!(second["independent_fills_observed"], "4100");
        assert_eq!(second["observed_fees"][0]["amount"], "0.0041");
        assert_eq!(
            journal.summary("gate").unwrap()["observed_fees"],
            second["observed_fees"]
        );
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
    #[test]
    fn historical_execution_units_remain_immutable_and_reinterpreted_replays_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("facts.db");
        let journal = AccountFactJournal::open(&path, "one".into(), &[definition()]).unwrap();
        publish(&journal, fill(1, 1), 1);
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let original = journal.history("gate", Some("fill"), 0, 20).unwrap();
        assert_eq!(
            original[0]["details"]["instrument_binding"]["price_tick"]["scale"],
            "1"
        );
        drop(journal);
        let mut changed = definition();
        let mut instruments = changed.instruments.to_vec();
        instruments[0].price_tick = "0.01".parse().unwrap();
        changed.instruments = instruments.into();
        let journal = AccountFactJournal::open(&path, "two".into(), &[changed]).unwrap();
        publish(&journal, fill(1, 2), 1);
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            journal.history("gate", Some("fill"), 0, 20).unwrap(),
            original
        );
        assert!(
            journal.summary("gate").unwrap()["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["code"] == "TRADE_ID_CONFLICT")
        );
    }
    #[test]
    fn cash_replay_keeps_original_source_without_changing_economic_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("facts.db");
        for source in ["one", "two"] {
            let journal = AccountFactJournal::open(&path, source.into(), &[definition()]).unwrap();
            journal
                .cash_movement(CashMovement {
                    id: "transfer:1".into(),
                    account: "gate".into(),
                    currency_id: 0,
                    amount_units: 17,
                    funding: false,
                    occurred_at_ns: 10,
                    provenance: "provider-record".into(),
                })
                .unwrap();
            journal
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
            assert_eq!(
                journal
                    .history("gate", Some("cashflow"), 0, 20)
                    .unwrap()
                    .len(),
                1
            );
            assert!(
                !journal.summary("gate").unwrap()["diagnostics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v["code"] == "CASH_ID_CONFLICT")
            );
        }
        let items =
            AccountFactJournal::read_history(&path, "gate", Some("cashflow"), 0, 20).unwrap();
        assert_eq!(items[0]["source"], "one");
        assert_eq!(items[0]["amount_unit"], "0.000001");
    }
    #[test]
    fn cash_ids_cannot_collide_between_accounts_containing_separator_characters() {
        let tmp = tempfile::tempdir().unwrap();
        let mut a = definition();
        a.account_key = Arc::from("a:cashflow:b");
        let mut b = definition();
        b.account_key = Arc::from("a");
        b.account_id = AccountId(2);
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[a, b]).unwrap();
        for (account, id) in [("a:cashflow:b", "x"), ("a", "b:cashflow:x")] {
            journal
                .cash_movement(CashMovement {
                    id: id.into(),
                    account: account.into(),
                    currency_id: 0,
                    amount_units: 17,
                    funding: false,
                    occurred_at_ns: 10,
                    provenance: "provider-record".into(),
                })
                .unwrap();
        }
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            journal
                .history("a:cashflow:b", Some("cashflow"), 0, 20)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            journal.history("a", Some("cashflow"), 0, 20).unwrap().len(),
            1
        );
    }
    #[test]
    fn native_account_performance_adjusts_transfers_without_deducting_fees_or_funding_twice() {
        let tmp = tempfile::tempdir().unwrap();
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[definition()])
                .unwrap();
        let db = Connection::open(&journal.path).unwrap();
        for (second, units) in [(1, "100000000"), (2, "115000000"), (3, "106000000")] {
            let body = json!({"captured_at_ns":(second*1_000_000_000i64).to_string(),"equity":[{"currency":"USDT","amount_unit":"0.000001","amount_units":units}]});
            db.execute("INSERT INTO facts(source,account,kind,event_key,digest,body) VALUES ('run','gate','equity_sample',?1,X'00',?2)",params![second.to_string(),body.to_string()]).unwrap();
        }
        let transfer = AccountLedgerRecord {
            id: "transfer".into(),
            kind: "TRANSFER_IN".into(),
            currency: "USDT".into(),
            amount: "10".into(),
            occurred_ms: 1500,
            instrument: String::new(),
            exchange: "GATE".into(),
        };
        let funding = AccountLedgerRecord {
            id: "funding".into(),
            kind: "FUNDING_FEE".into(),
            amount: "-1".into(),
            occurred_ms: 2500,
            ..transfer.clone()
        };
        journal
            .ledger_batch("gate", 0, 4000, vec![transfer, funding], true, None)
            .unwrap();
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let summary = journal.summary("gate").unwrap();
        assert_eq!(summary["net_pnl"][0]["amount"], "-4");
        assert_eq!(summary["max_drawdown"][0]["amount"], "9");
        assert_eq!(summary["performance"]["ledger_interval_complete"], true);
        assert_eq!(summary["performance"]["coverage_complete"], false);
        assert_eq!(
            decimal_units("-9007199254740993.000001", "0.000001".parse().unwrap()).unwrap(),
            -9007199254740993000001
        );
        assert!(decimal_units("0.0000001", "0.000001".parse().unwrap()).is_err());
    }
    #[test]
    fn account_quantity_bound_counts_only_unfilled_remainder_and_keeps_hedge_gross() {
        fn image<T>(items: Vec<T>) -> AccountStateSnapshot<T> {
            AccountStateSnapshot {
                account: AccountHandle {
                    account_id: AccountId(1),
                    generation: 1,
                },
                state: AccountSnapshotState::Ready,
                committed_epoch: Some(1),
                committed_version: Some(7),
                captured_at: 1700000000000000000,
                items: items.into(),
            }
        }
        let mut orders = Vec::new();
        for (id, quantity, filled) in [(1, 10, 4), (2, 3, 3)] {
            orders.push(OrderSnapshot {
                position_side: 0,
                asset_id: AssetId(1),
                side: 1,
                order_type: 0,
                time_in_force: 0,
                status: 1,
                price_ticks: 1000000,
                quantity_lots: quantity,
                filled_quantity_lots: filled,
                reduce_only: false,
                client_order_id: Id128([id; 16]),
                venue_order_id: Id128([id; 16]),
                command_id: Default::default(),
            });
        }
        let positions = vec![
            PositionSnapshot {
                asset_id: AssetId(1),
                position_side: 1,
                margin_type: 0,
                quantity_lots: 7,
                entry_price_ticks: 1000000,
                liquidation_price_ticks: 0,
                realized_pnl_units: 0,
                unrealized_pnl_units: 0,
                margin_currency_id: CurrencyId(0),
            },
            PositionSnapshot {
                asset_id: AssetId(1),
                position_side: 2,
                margin_type: 0,
                quantity_lots: -2,
                entry_price_ticks: 1000000,
                liquidation_price_ticks: 0,
                realized_pnl_units: 0,
                unrealized_pnl_units: 0,
                margin_currency_id: CurrencyId(0),
            },
        ];
        let snapshot = AccountRecoverySnapshot::coherent(
            image(orders),
            image(positions),
            image(vec![BalanceSnapshot {
                currency_id: CurrencyId(0),
                wallet_units: 123456789,
                available_units: 100000000,
                margin_units: 1000000,
                unrealized_pnl_units: -17,
            }]),
        )
        .unwrap();
        let view = account_image(&definition(), &snapshot).unwrap();
        assert_eq!(view["risk"]["assets"][0]["actual_gross_lots"], "9");
        assert_eq!(view["risk"]["assets"][0]["known_order_remaining_lots"], "6");
        assert_eq!(view["risk"]["assets"][0]["known_quantity_bound_lots"], "15");
        assert!(view["risk"]["notional"].is_null());
        assert_eq!(view["equity"][0]["amount"], "123.456772");
        if let Ok(path) = std::env::var("TITAN_OBSERVATION_FIXTURE") {
            std::fs::write(path,serde_json::to_vec_pretty(&json!({"available":true,"source":"test_account_service","run_id":"fixture","generation":"one","account":"gate","instrument":"BTC_USDT","mode":"observe","readonly":true,"process_state":"running","captured_at_ns":"1700000000000000000","account_health":{"state":"Ready"},"account_snapshot":{"available":true,"image":view},"financial":{"recording":{"history_complete":false,"missing":["strategy_attribution","cashflow_coverage"]},"net_pnl":null,"max_drawdown":null,"attribution":"account","independent_fills_observed":"0"},"strategies":[]})).unwrap()).unwrap();
        }
    }
    #[test]
    fn recorder_storage_failure_is_visible_and_does_not_acknowledge_clean_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[definition()])
                .unwrap();
        let db = Connection::open(&journal.path).unwrap();
        db.execute_batch("CREATE TRIGGER fail_fact BEFORE INSERT ON facts BEGIN SELECT RAISE(FAIL,'injected disk failure'); END;").unwrap();
        publish(&journal, fill(1, 1), 1);
        assert!(
            journal
                .close(Instant::now() + Duration::from_secs(2))
                .is_err()
        );
        assert_eq!(journal.health()["writer_failed"], true);
        assert_eq!(
            db.query_row::<i64, _, _>("SELECT clean FROM sources WHERE id='run'", [], |r| r.get(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn ledger_overlap_and_restart_deduplicate_by_venue_record_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("facts.db");
        let record = AccountLedgerRecord {
            id: "121".into(),
            kind: "FUNDING_FEE".into(),
            currency: "USDT".into(),
            amount: "-0.000001".into(),
            occurred_ms: 10,
            instrument: "BTC_USDT".into(),
            exchange: "GATE".into(),
        };
        for source in ["one", "two"] {
            let journal = AccountFactJournal::open(&path, source.into(), &[definition()]).unwrap();
            journal
                .ledger_batch(
                    "gate",
                    0,
                    20,
                    vec![record.clone(), record.clone()],
                    true,
                    None,
                )
                .unwrap();
            journal
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
            assert_eq!(
                journal
                    .history("gate", Some("ledger"), 0, 20)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                journal
                    .history("gate", Some("funding"), 0, 20)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                journal.summary("gate").unwrap()["ledger_coverage"]["complete"],
                true
            );
        }
        let journal = AccountFactJournal::open(&path, "three".into(), &[definition()]).unwrap();
        let mut conflicting = record;
        conflicting.amount = "-100".into();
        journal
            .ledger_batch("gate", 0, 30, vec![conflicting], true, None)
            .unwrap();
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            journal.summary("gate").unwrap()["ledger_coverage"]["complete"],
            false
        );
        assert_eq!(
            journal.history("gate", Some("ledger"), 0, 20).unwrap()[0]["ledger"]["amount"],
            "-0.000001"
        );
        assert_eq!(
            journal.history("gate", Some("ledger"), 0, 20).unwrap()[0]["ledger"]["occurred_ms"],
            "10"
        );
    }
    #[test]
    fn sampled_equity_keeps_currency_units_and_does_not_claim_trading_returns() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = AccountFactJournal::open(
            tmp.path().join("facts.db"),
            "window".into(),
            &[definition()],
        )
        .unwrap();
        let db = Connection::open(&journal.path).unwrap();
        for (at, units) in [(1, "100000000"), (2, "110000000"), (3, "90000000")] {
            equity_window(&db,"window","gate",&json!({"captured_at_ns":at.to_string(),"equity":[{"currency":"USDT","amount_unit":"0.000001","amount_units":units}]})).unwrap();
        }
        let view = journal.summary("gate").unwrap();
        assert_eq!(
            view["native_performance"][0]["observed_equity_change"],
            "-10"
        );
        assert_eq!(
            view["native_performance"][0]["max_observed_equity_decline"],
            "20"
        );
        assert!(view["net_pnl"].is_null());
        journal
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
    fn definition() -> AccountDefinition {
        AccountDefinition {
            account_key: Arc::from("gate"),
            account_id: AccountId(1),
            connector_type: Arc::from("test-provider"),
            credential_ref: SecretRef(Arc::from("not-a-credential")),
            connector_config: Arc::from([]),
            instruments: Arc::from([AccountInstrumentBinding {
                native_symbol: Arc::from("BTC_USDT"),
                asset_id: AssetId(1),
                price_tick: "0.1".parse().unwrap(),
                quantity_lot: "0.0001".parse().unwrap(),
                contract_multiplier: "1".parse().unwrap(),
            }]),
            currencies: Arc::from([AccountCurrencyBinding {
                native_currency: Arc::from("USDT"),
                currency_id: CurrencyId(0),
                amount_unit: "0.000001".parse().unwrap(),
            }]),
            ownership: OrderOwnershipPolicy::ObserveAll,
            shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        }
    }
    fn publish<T: AccountEventPayload>(journal: &AccountFactJournal, event: T, sequence: u64) {
        let mut bytes = vec![0; T::ENCODED_LEN];
        event.encode_into(&mut bytes).unwrap();
        journal
            .handle(EventView {
                event_type: T::EVENT_TYPE,
                schema_version: T::SCHEMA_VERSION,
                payload: &bytes,
                metadata: titan_core_types::EventPublishMetadata {
                    source_id: 3,
                    source_sequence: sequence,
                    ..Default::default()
                },
                trace: Default::default(),
            })
            .unwrap();
    }
    fn fill(trade: u8, version: u64) -> FillV2 {
        FillV2 {
            header: AccountEventHeaderV1 {
                account_id: 1,
                kind: event_kind::FILL,
                account_generation: 1,
                account_epoch: 2,
                account_version: version,
                ..Default::default()
            },
            asset_id: 1,
            side: 0,
            price_ticks: 1000000,
            last_fill_quantity_lots: 2,
            cumulative_filled_quantity_lots: 2,
            fee_amount_units: 17,
            fee_currency_id: 0,
            trade_id: Id128([trade; 16]),
            venue_order_id: Id128([8; 16]),
            ..Default::default()
        }
    }
    #[test]
    fn terminal_orders_do_not_fabricate_fills_and_reconnects_do_not_repeat_fees() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("facts.db");
        let journal = AccountFactJournal::open(&path, "run-1".into(), &[definition()]).unwrap();
        publish(
            &journal,
            OrderChangedV1 {
                header: AccountEventHeaderV1 {
                    account_id: 1,
                    kind: event_kind::ORDER_CHANGED,
                    account_generation: 1,
                    account_epoch: 2,
                    account_version: 30,
                    ..Default::default()
                },
                asset_id: 1,
                status: 5,
                quantity_lots: 999,
                filled_quantity_lots: 999,
                venue_order_id: Id128([8; 16]),
                ..Default::default()
            },
            1,
        );
        publish(&journal, fill(1, 31), 2);
        publish(&journal, fill(1, 40), 3);
        publish(&journal, fill(2, 41), 4);
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            journal.history("gate", Some("fill"), 0, 200).unwrap().len(),
            2
        );
        let summary = journal.summary("gate").unwrap();
        assert_eq!(summary["independent_fills_observed"], "2");
        assert_eq!(summary["observed_fees"][0]["amount"], "0.000034");
        assert!(summary["net_pnl"].is_null());
        drop(journal);
        let journal = AccountFactJournal::open(&path, "run-2".into(), &[definition()]).unwrap();
        let mut duplicate = fill(1, 1);
        duplicate.header.account_generation = 2;
        duplicate.header.account_epoch = 3;
        publish(&journal, duplicate, 1);
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            journal.summary("gate").unwrap()["independent_fills_observed"],
            "2"
        );
        assert_eq!(
            journal.summary("gate").unwrap()["observed_fees"][0]["amount"],
            "0.000034"
        );
    }
    #[test]
    fn contradictory_trade_identity_remains_incomplete_without_overwriting_original() {
        let tmp = tempfile::tempdir().unwrap();
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[definition()])
                .unwrap();
        publish(&journal, fill(1, 1), 1);
        let mut conflict = fill(1, 2);
        conflict.fee_amount_units = 100;
        publish(&journal, conflict, 2);
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let summary = journal.summary("gate").unwrap();
        assert_eq!(summary["independent_fills_observed"], "1");
        assert_eq!(summary["observed_fees"][0]["amount"], "0.000017");
        assert!(
            summary["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["code"] == "TRADE_ID_CONFLICT")
        );
    }
    #[test]
    fn funding_and_external_cash_are_deduplicated_with_exact_signed_amounts() {
        let tmp = tempfile::tempdir().unwrap();
        let journal =
            AccountFactJournal::open(tmp.path().join("facts.db"), "run".into(), &[definition()])
                .unwrap();
        let movement = CashMovement {
            id: "settlement-1".into(),
            account: "gate".into(),
            currency_id: 0,
            amount_units: -37,
            funding: true,
            occurred_at_ns: i64::MAX,
            provenance: "provider history id".into(),
        };
        journal.cash_movement(movement.clone()).unwrap();
        journal.cash_movement(movement.clone()).unwrap();
        journal
            .cash_movement(CashMovement {
                id: "transfer-1".into(),
                funding: false,
                amount_units: 100,
                ..movement
            })
            .unwrap();
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let funding = journal.history("gate", Some("funding"), 0, 200).unwrap();
        assert_eq!(funding.len(), 1);
        assert_eq!(funding[0]["amount"], "-0.000037");
        assert_eq!(funding[0]["occurred_at_ns"], i64::MAX.to_string());
        assert_eq!(
            journal
                .history("gate", Some("cashflow"), 0, 200)
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn interrupted_recording_is_not_repaired_by_opening_a_new_source() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("facts.db");
        let journal = AccountFactJournal::open(&path, "run-1".into(), &[definition()]).unwrap();
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        drop(journal);
        Connection::open(&path)
            .unwrap()
            .execute("UPDATE sources SET clean=0", [])
            .unwrap();
        let journal = AccountFactJournal::open(&path, "run-2".into(), &[definition()]).unwrap();
        journal
            .close(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert!(
            journal.summary("gate").unwrap()["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["code"] == "RECORDING_GAP")
        );
    }
    #[test]
    fn queue_pressure_is_bounded_and_reported_without_waiting_for_the_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let (sender, _receiver) = mpsc::sync_channel(1);
        let journal = AccountFactJournal {
            path: tmp.path().into(),
            source: "run".into(),
            sender,
            health: Arc::new(Health::default()),
            sample: Default::default(),
            writer: Mutex::new(None),
            definitions: Arc::new(BTreeMap::from([(1, definition())])),
            summaries: Default::default(),
        };
        publish(&journal, fill(1, 1), 1);
        publish(&journal, fill(2, 2), 2);
        assert_eq!(journal.health()["backlog"], "1");
        assert_eq!(journal.health()["dropped"], "1");
    }
    #[test]
    fn monetary_formatting_never_passes_through_binary_floating_point() {
        assert_eq!(
            exact(i128::from(i64::MAX) * 2, "0.000001".parse().unwrap()).unwrap(),
            "18446744073709.551614"
        );
        assert_eq!(
            exact(-37, "0.000001".parse().unwrap()).unwrap(),
            "-0.000037"
        );
    }
}
