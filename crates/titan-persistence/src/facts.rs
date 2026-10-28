use crate::{Result, failure, sha256};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

pub const FACT_SCHEMA: u32 = 1;
pub const MAX_FACT_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactEnvironment {
    Live,
    Simulation,
    Backtest,
}
impl FactEnvironment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Simulation => "simulation",
            Self::Backtest => "backtest",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactScope {
    pub environment: FactEnvironment,
    pub node_id: String,
    pub account: String,
    pub strategy_id: Option<String>,
    pub run_id: Option<String>,
    pub run_generation: Option<String>,
}
impl FactScope {
    pub fn account(environment: FactEnvironment, account: impl Into<String>) -> Self {
        Self {
            environment,
            node_id: String::new(),
            account: account.into(),
            strategy_id: None,
            run_id: None,
            run_generation: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactEnvelope {
    pub schema_version: u32,
    pub scope: FactScope,
    pub recording_id: String,
    pub stream_id: String,
    pub source_epoch: String,
    #[serde(with = "u64_text")]
    pub source_sequence: u64,
    pub kind: String,
    #[serde(with = "i64_text")]
    pub exchange_at_ns: i64,
    #[serde(with = "i64_text")]
    pub received_at_ns: i64,
    pub payload: Value,
    /// Native semantic identity, e.g. venue/account/instrument/trade id, never a cumulative fill.
    pub business_key: Option<String>,
    /// Digest of native content/units only, excluding recording provenance and valuation policy.
    pub business_sha256: Option<String>,
    pub sampled: bool,
}
impl FactEnvelope {
    pub fn new(
        scope: FactScope,
        recording: impl Into<String>,
        stream: impl Into<String>,
        sequence: u64,
        kind: impl Into<String>,
        timestamp: i64,
        payload: Value,
    ) -> Self {
        Self {
            schema_version: FACT_SCHEMA,
            scope,
            recording_id: recording.into(),
            stream_id: stream.into(),
            source_epoch: "1".into(),
            source_sequence: sequence,
            kind: kind.into(),
            exchange_at_ns: timestamp,
            received_at_ns: timestamp,
            payload,
            business_key: None,
            business_sha256: None,
            sampled: false,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != FACT_SCHEMA
            || self.recording_id.is_empty()
            || self.stream_id.is_empty()
            || self.source_epoch.is_empty()
            || self.kind.is_empty()
            || [
                &self.recording_id,
                &self.stream_id,
                &self.source_epoch,
                &self.kind,
                &self.scope.account,
                &self.scope.node_id,
            ]
            .iter()
            .any(|v| v.len() > 256)
            || self
                .scope
                .strategy_id
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 128)
            || self
                .scope
                .run_id
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 256)
            || matches!(self.scope.environment, FactEnvironment::Backtest)
                && self.scope.run_id.is_none()
            || self
                .scope
                .run_generation
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 64)
            || self
                .business_key
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 2048)
            || self.business_key.is_some() != self.business_sha256.is_some()
            || self
                .business_sha256
                .as_ref()
                .is_some_and(|v| v.len() != 64 || !v.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(failure("fact_envelope_invalid"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub inserted: u64,
    pub replayed: u64,
    pub conflicts: u64,
    pub durable_row: u64,
}

pub trait FactArchive: Send + Sync {
    fn append(&self, records: &[FactEnvelope]) -> Result<AppendReceipt>;
    fn finish(&self, recording: &str, complete: bool) -> Result<()>;
}
pub trait HistoryQuery: Send + Sync {
    fn page(&self, request: &HistoryRequest) -> Result<HistoryPage>;
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryFilter {
    pub environment: FactEnvironment,
    pub account: String,
    pub strategy_id: Option<String>,
    pub run_id: Option<String>,
    pub kind: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCursor {
    pub store_id: String,
    pub filter_sha256: String,
    #[serde(with = "u64_text")]
    pub upper_row: u64,
    #[serde(with = "u64_text")]
    pub after_row: u64,
}
#[derive(Clone, Debug)]
pub struct HistoryRequest {
    pub filter: HistoryFilter,
    pub cursor: Option<HistoryCursor>,
    pub limit: usize,
    pub newest_first: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredFact {
    pub row_id: String,
    pub payload_sha256: String,
    pub fact: FactEnvelope,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryPage {
    pub items: Vec<StoredFact>,
    pub next_cursor: Option<HistoryCursor>,
    pub upper_row: String,
    pub recording: Value,
}

/// Adds the archive role to an existing account history DB without changing its legacy schema.
pub fn initialize_archive(db: &Connection) -> Result<()> {
    if rusqlite::version_number() < 3_051_003 {
        return Err(failure("history_sqlite_wal_fix_required"));
    }
    let wrong_role: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name IN ('recovery_points','resources','object_catalog','storage_control_meta'))", [], |r| r.get(0))?;
    if wrong_role {
        return Err(failure("history_database_role_conflict"));
    }
    let has_meta: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_meta')",
        [],
        |r| r.get(0),
    )?;
    if has_meta {
        let version: u32 = db.query_row(
            "SELECT schema_version FROM archive_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if version != FACT_SCHEMA {
            return Err(failure("history_schema_unsupported"));
        }
    }
    db.execute_batch("CREATE TABLE IF NOT EXISTS archive_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL,store_id TEXT NOT NULL);
        INSERT OR IGNORE INTO archive_meta VALUES(1,1,lower(hex(randomblob(16))));
        CREATE TABLE IF NOT EXISTS archive_sources(recording TEXT PRIMARY KEY,environment TEXT NOT NULL,clean INTEGER NOT NULL,complete INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS archive_source_scopes(recording TEXT NOT NULL,environment TEXT NOT NULL,account TEXT NOT NULL,run TEXT NOT NULL,PRIMARY KEY(recording,environment,account,run));
        CREATE TABLE IF NOT EXISTS archive_records(id INTEGER PRIMARY KEY,recording TEXT NOT NULL,stream TEXT NOT NULL,epoch TEXT NOT NULL,sequence BLOB NOT NULL,environment TEXT NOT NULL,account TEXT NOT NULL,strategy TEXT,run TEXT,kind TEXT NOT NULL,payload BLOB NOT NULL,sha256 TEXT NOT NULL,replay INTEGER NOT NULL,UNIQUE(recording,stream,epoch,sequence));
        CREATE INDEX IF NOT EXISTS archive_query ON archive_records(environment,account,kind,id);
        CREATE INDEX IF NOT EXISTS archive_run_query ON archive_records(environment,account,strategy,run,id);
        CREATE TABLE IF NOT EXISTS archive_watermarks(recording TEXT NOT NULL,stream TEXT NOT NULL,epoch TEXT NOT NULL,sequence BLOB NOT NULL,row_id INTEGER NOT NULL,PRIMARY KEY(recording,stream,epoch));
        CREATE TABLE IF NOT EXISTS archive_business(scope TEXT NOT NULL,business_key TEXT NOT NULL,digest TEXT NOT NULL,row_id INTEGER NOT NULL,PRIMARY KEY(scope,business_key));
        CREATE TABLE IF NOT EXISTS archive_conflicts(id INTEGER PRIMARY KEY,recording TEXT NOT NULL,account TEXT NOT NULL,code TEXT NOT NULL,original_row INTEGER,new_payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS archive_gaps(recording TEXT NOT NULL,account TEXT NOT NULL,code TEXT NOT NULL,detail TEXT NOT NULL,PRIMARY KEY(recording,account,code));")?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS archive_sequence(id INTEGER PRIMARY KEY CHECK(id=1),last_row INTEGER NOT NULL); INSERT OR IGNORE INTO archive_sequence SELECT 1,max(coalesce((SELECT max(id) FROM archive_records),0),coalesce((SELECT max(row_id) FROM archive_watermarks),0)); CREATE TABLE IF NOT EXISTS archive_retention(target TEXT PRIMARY KEY,through_row INTEGER NOT NULL,records INTEGER NOT NULL,digest TEXT NOT NULL);")?;
    let version: u32 = db.query_row(
        "SELECT schema_version FROM archive_meta WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if version != FACT_SCHEMA {
        return Err(failure("history_schema_unsupported"));
    }
    Ok(())
}
pub fn begin_source(db: &Connection, recording: &str, environment: &FactEnvironment) -> Result<()> {
    if recording.is_empty() || recording.len() > 256 {
        return Err(failure("recording_identity_invalid"));
    }
    db.execute(
        "INSERT INTO archive_sources VALUES(?1,?2,0,1) ON CONFLICT(recording) DO NOTHING",
        params![recording, environment.as_str()],
    )?;
    let (old, closed): (String, bool) = db.query_row(
        "SELECT environment,clean FROM archive_sources WHERE recording=?1",
        [recording],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if old != environment.as_str() {
        return Err(failure("recording_environment_changed"));
    }
    if closed {
        return Err(failure("recording_already_closed"));
    }
    Ok(())
}
pub fn mark_gap(
    db: &Connection,
    recording: &str,
    account: &str,
    code: &str,
    detail: &str,
) -> Result<()> {
    db.execute(
        "INSERT OR IGNORE INTO archive_gaps VALUES(?1,?2,?3,?4)",
        params![recording, account, code, detail],
    )?;
    db.execute(
        "UPDATE archive_sources SET complete=0 WHERE recording=?1",
        [recording],
    )?;
    Ok(())
}
pub fn finish_source(db: &Connection, recording: &str, complete: bool) -> Result<()> {
    if db.execute(
        "UPDATE archive_sources SET clean=1,complete=complete AND ?2 WHERE recording=?1",
        params![recording, complete],
    )? != 1
    {
        return Err(failure("recording_source_missing"));
    }
    Ok(())
}
pub fn bind_source_scope(
    db: &Connection,
    recording: &str,
    environment: &FactEnvironment,
    account: &str,
    run: Option<&str>,
) -> Result<()> {
    db.execute(
        "INSERT OR IGNORE INTO archive_source_scopes VALUES(?1,?2,?3,?4)",
        params![recording, environment.as_str(), account, run.unwrap_or("")],
    )?;
    Ok(())
}
/// Caller MUST use the same transaction as its provider's raw fact and financial projection.
pub fn append_in_transaction(db: &Connection, record: &FactEnvelope) -> Result<AppendReceipt> {
    record.validate()?;
    let bytes = serde_json::to_vec(record)?;
    if bytes.len() > MAX_FACT_BYTES {
        return Err(failure("fact_payload_too_large"));
    }
    let digest = sha256(&bytes);
    let seq = record.source_sequence.to_be_bytes();
    let previous:Option<(i64,String)>=db.query_row("SELECT id,sha256 FROM archive_records WHERE recording=?1 AND stream=?2 AND epoch=?3 AND sequence=?4",params![record.recording_id,record.stream_id,record.source_epoch,seq.as_slice()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((row, old)) = previous {
        if old == digest {
            return Ok(AppendReceipt {
                replayed: 1,
                durable_row: row as u64,
                ..Default::default()
            });
        }
        db.execute("INSERT INTO archive_conflicts(recording,account,code,original_row,new_payload) VALUES(?1,?2,'source_identity_conflict',?3,?4)",params![record.recording_id,record.scope.account,row,bytes])?;
        mark_gap(
            db,
            &record.recording_id,
            &record.scope.account,
            "source_identity_conflict",
            "same source position contained different bytes",
        )?;
        return Ok(AppendReceipt {
            conflicts: 1,
            durable_row: row as u64,
            ..Default::default()
        });
    }
    begin_source(db, &record.recording_id, &record.scope.environment)?;
    bind_source_scope(
        db,
        &record.recording_id,
        &record.scope.environment,
        &record.scope.account,
        record.scope.run_id.as_deref(),
    )?;
    let last: Option<Vec<u8>> = db
        .query_row(
            "SELECT sequence FROM archive_watermarks WHERE recording=?1 AND stream=?2 AND epoch=?3",
            params![record.recording_id, record.stream_id, record.source_epoch],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(last) = last {
        let last = u64::from_be_bytes(last.try_into().map_err(|_| failure("watermark_corrupt"))?);
        if !record.sampled && last.checked_add(1) != Some(record.source_sequence) {
            mark_gap(
                db,
                &record.recording_id,
                &record.scope.account,
                "source_sequence_gap",
                "source order is missing or regressed; local continuity does not prove venue coverage",
            )?;
        }
    }
    let business_scope = serde_json::to_string(&(
        record.scope.environment.as_str(),
        &record.scope.account,
        if matches!(record.scope.environment, FactEnvironment::Backtest) {
            record.scope.run_id.as_deref()
        } else {
            None
        },
    ))?;
    let mut replay = false;
    let mut conflicts = 0;
    if let (Some(key), Some(hash)) = (&record.business_key, &record.business_sha256) {
        let old: Option<(String, i64)> = db
            .query_row(
                "SELECT digest,row_id FROM archive_business WHERE scope=?1 AND business_key=?2",
                params![business_scope, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((old, row)) = old {
            replay = true;
            if old != *hash {
                conflicts = 1;
                db.execute("INSERT INTO archive_conflicts(recording,account,code,original_row,new_payload) VALUES(?1,?2,'business_identity_conflict',?3,?4)",params![record.recording_id,record.scope.account,row,bytes])?;
                mark_gap(
                    db,
                    &record.recording_id,
                    &record.scope.account,
                    "business_identity_conflict",
                    "native identity has contradictory content; first fact preserved",
                )?;
            }
        }
    }
    let last: i64 = db.query_row(
        "SELECT last_row FROM archive_sequence WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let next = last
        .checked_add(1)
        .ok_or_else(|| failure("history_row_id_exhausted"))?;
    db.execute("INSERT INTO archive_records(id,recording,stream,epoch,sequence,environment,account,strategy,run,kind,payload,sha256,replay) VALUES(?13,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![record.recording_id,record.stream_id,record.source_epoch,seq.as_slice(),record.scope.environment.as_str(),record.scope.account,record.scope.strategy_id,record.scope.run_id,record.kind,bytes,digest,replay,next])?;
    db.execute("UPDATE archive_sequence SET last_row=?1 WHERE id=1", [next])?;
    let row = db.last_insert_rowid();
    if let (Some(key), Some(hash)) = (&record.business_key, &record.business_sha256) {
        db.execute(
            "INSERT OR IGNORE INTO archive_business VALUES(?1,?2,?3,?4)",
            params![business_scope, key, hash, row],
        )?;
    }
    db.execute("INSERT INTO archive_watermarks VALUES(?1,?2,?3,?4,?5) ON CONFLICT(recording,stream,epoch) DO UPDATE SET sequence=max(sequence,excluded.sequence),row_id=excluded.row_id",params![record.recording_id,record.stream_id,record.source_epoch,seq.as_slice(),row])?;
    Ok(AppendReceipt {
        inserted: 1,
        replayed: u64::from(replay),
        conflicts,
        durable_row: row as u64,
    })
}

pub struct SqliteFactArchive {
    path: PathBuf,
    db: Mutex<Connection>,
    leases: Mutex<std::collections::BTreeMap<String, std::fs::File>>,
}
impl SqliteFactArchive {
    pub fn begin_recording(&self, recording: &str, scope: &FactScope) -> Result<()> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| failure("recording_lease_poisoned"))?;
        if !leases.contains_key(recording) {
            let directory = self.path.with_extension("archive-sources");
            std::fs::create_dir_all(&directory)?;
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(directory.join(sha256(recording.as_bytes())))?;
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                    return Err(failure("recording_source_owned"));
                }
            }
            #[cfg(not(unix))]
            return Err(failure("recording_source_owner_unsupported"));
            leases.insert(recording.into(), lock);
        }
        let mut db = self
            .db
            .lock()
            .map_err(|_| failure("history_writer_poisoned"))?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        begin_source(&tx, recording, &scope.environment)?;
        bind_source_scope(
            &tx,
            recording,
            &scope.environment,
            &scope.account,
            scope.run_id.as_deref(),
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let db = Connection::open(&path)?;
        db.busy_timeout(Duration::from_millis(1000))?;
        let recovery: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='recovery_points')",
            [],
            |r| r.get(0),
        )?;
        if recovery {
            return Err(failure("history_recovery_role_conflict"));
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        initialize_archive(&db)?;
        let path = std::fs::canonicalize(path)?;
        Ok(Self {
            path,
            db: Mutex::new(db),
            leases: Mutex::new(Default::default()),
        })
    }
    pub fn query(&self) -> SqliteHistoryQuery {
        SqliteHistoryQuery::new(self.path.clone())
    }
}
impl FactArchive for SqliteFactArchive {
    fn append(&self, records: &[FactEnvelope]) -> Result<AppendReceipt> {
        if records.len() > MAX_BATCH {
            return Err(failure("fact_batch_too_large"));
        }
        crate::check_history_budget(&self.path)?;
        let mut db = self
            .db
            .lock()
            .map_err(|_| failure("history_writer_poisoned"))?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut receipt = AppendReceipt::default();
        for record in records {
            let r = append_in_transaction(&tx, record)?;
            receipt.inserted += r.inserted;
            receipt.replayed += r.replayed;
            receipt.conflicts += r.conflicts;
            receipt.durable_row = receipt.durable_row.max(r.durable_row);
        }
        tx.commit()?;
        Ok(receipt)
    }
    fn finish(&self, recording: &str, complete: bool) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|_| failure("history_writer_poisoned"))?;
        finish_source(&db, recording, complete)
    }
}

pub struct SqliteHistoryQuery {
    path: PathBuf,
}
impl SqliteHistoryQuery {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}
impl HistoryQuery for SqliteHistoryQuery {
    fn page(&self, request: &HistoryRequest) -> Result<HistoryPage> {
        if request.limit == 0 || request.limit > 200 || request.filter.account.len() > 256 {
            return Err(failure("history_query_invalid"));
        }
        // Authorization has already produced this exact filter; never filter after pagination.
        let mut db = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_millis(200))?;
        let tx = db.transaction()?;
        let version: u32 = tx.query_row(
            "SELECT schema_version FROM archive_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if version != FACT_SCHEMA {
            return Err(failure("history_schema_unsupported"));
        }
        let store: String =
            tx.query_row("SELECT store_id FROM archive_meta WHERE id=1", [], |r| {
                r.get(0)
            })?;
        let has_retention: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_retention')",
            [],
            |r| r.get(0),
        )?;
        if has_retention
            && tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM archive_retention WHERE through_row>0)",
                [],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Err(failure("local_history_pruned_use_routed_query"));
        }
        let filter = if request.newest_first {
            sha256(&serde_json::to_vec(&(&request.filter, "descending"))?)
        } else {
            sha256(&serde_json::to_vec(&request.filter)?)
        };
        let upper:u64=tx.query_row("SELECT coalesce(max(id),0) FROM archive_records WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR strategy=?3) AND (?4 IS NULL OR run=?4) AND (?5 IS NULL OR kind=?5) AND replay=0",params![request.filter.environment.as_str(),request.filter.account,request.filter.strategy_id,request.filter.run_id,request.filter.kind],|r|r.get(0))?;
        let (upper, after) = if let Some(c) = &request.cursor {
            if c.store_id != store
                || c.filter_sha256 != filter
                || c.after_row > c.upper_row
                || c.upper_row > upper
            {
                return Err(failure("history_cursor_scope_changed"));
            }
            (c.upper_row, c.after_row)
        } else {
            (upper, 0)
        };
        let sql = if request.newest_first {
            "SELECT id,payload,sha256 FROM archive_records WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR strategy=?3) AND (?4 IS NULL OR run=?4) AND (?5 IS NULL OR kind=?5) AND replay=0 AND (?6=0 OR id<?6) AND id<=?7 ORDER BY id DESC LIMIT ?8"
        } else {
            "SELECT id,payload,sha256 FROM archive_records WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR strategy=?3) AND (?4 IS NULL OR run=?4) AND (?5 IS NULL OR kind=?5) AND replay=0 AND id>?6 AND id<=?7 ORDER BY id LIMIT ?8"
        };
        let mut query = tx.prepare(sql)?;
        let rows = query.query_map(
            params![
                request.filter.environment.as_str(),
                request.filter.account,
                request.filter.strategy_id,
                request.filter.run_id,
                request.filter.kind,
                after,
                upper,
                request.limit + 1
            ],
            |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for row in rows {
            let (id, payload, digest) = row?;
            if items.len() == request.limit || bytes + payload.len() > 4 * MAX_FACT_BYTES {
                more = true;
                break;
            }
            if sha256(&payload) != digest {
                return Err(failure("history_payload_corrupt"));
            }
            let fact: FactEnvelope = serde_json::from_slice(&payload)?;
            fact.validate()?;
            if fact.scope.environment != request.filter.environment
                || fact.scope.account != request.filter.account
                || request
                    .filter
                    .strategy_id
                    .as_ref()
                    .is_some_and(|v| fact.scope.strategy_id.as_ref() != Some(v))
                || request
                    .filter
                    .run_id
                    .as_ref()
                    .is_some_and(|v| fact.scope.run_id.as_ref() != Some(v))
                || request
                    .filter
                    .kind
                    .as_ref()
                    .is_some_and(|v| fact.kind != *v)
            {
                return Err(failure("history_scope_metadata_corrupt"));
            }
            bytes += payload.len();
            items.push(StoredFact {
                row_id: id.to_string(),
                payload_sha256: digest,
                fact,
            });
        }
        let next_cursor = if more {
            items.last().map(|r| HistoryCursor {
                store_id: store,
                filter_sha256: filter,
                upper_row: upper,
                after_row: r.row_id.parse().unwrap_or(0),
            })
        } else {
            None
        };
        let gaps:u64=tx.query_row("SELECT count(*) FROM archive_gaps WHERE (account=?1 OR account='') AND recording IN (SELECT recording FROM archive_source_scopes WHERE environment=?2 AND account=?1 AND (?3 IS NULL OR run=?3))",params![request.filter.account,request.filter.environment.as_str(),request.filter.run_id],|r|r.get(0))?;
        let unclean:u64=tx.query_row("SELECT count(*) FROM archive_sources WHERE clean=0 AND recording IN (SELECT recording FROM archive_source_scopes WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR run=?3))",params![request.filter.environment.as_str(),request.filter.account,request.filter.run_id],|r|r.get(0))?;
        let incomplete:u64=tx.query_row("SELECT count(*) FROM archive_sources WHERE complete=0 AND recording IN (SELECT recording FROM archive_source_scopes WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR run=?3))",params![request.filter.environment.as_str(),request.filter.account,request.filter.run_id],|r|r.get(0))?;
        let source_count:u64=tx.query_row("SELECT count(DISTINCT recording) FROM archive_source_scopes WHERE environment=?1 AND account=?2 AND (?3 IS NULL OR run=?3)",params![request.filter.environment.as_str(),request.filter.account,request.filter.run_id],|r|r.get(0))?;
        Ok(HistoryPage {
            items,
            next_cursor,
            upper_row: upper.to_string(),
            recording: json!({"source_count":source_count.to_string(),"gap_count":gaps.to_string(),"unclosed_sources":unclean.to_string(),"incomplete_sources":incomplete.to_string(),"received_segment_complete":source_count>0&&gaps==0&&unclean==0&&incomplete==0,"venue_history_complete":false,"durable_upper_row":upper.to_string()}),
        })
    }
}

/// Synchronous bounded chunks provide backpressure to a simulator, never silent audit loss.
pub struct FactStream {
    archive: Box<dyn FactArchive>,
    recording: String,
    buffer: Vec<FactEnvelope>,
    capacity: usize,
    failed: bool,
    finished: bool,
    buffer_bytes: usize,
}
macro_rules! text_number {
    ($name:ident,$t:ty) => {
        pub(crate) mod $name {
            use serde::{Deserialize, Deserializer, Serialize, Serializer};
            pub fn serialize<S: Serializer>(v: &$t, s: S) -> std::result::Result<S::Ok, S::Error> {
                v.to_string().serialize(s)
            }
            pub fn deserialize<'de, D: Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<$t, D::Error> {
                #[derive(Deserialize)]
                #[serde(untagged)]
                enum Input {
                    Text(String),
                    Number($t),
                }
                match Input::deserialize(d)? {
                    Input::Number(v) => Ok(v),
                    Input::Text(v) => v.parse().map_err(serde::de::Error::custom),
                }
            }
        }
    };
}
text_number!(u64_text, u64);
text_number!(i64_text, i64);
impl FactStream {
    pub fn new(archive: Box<dyn FactArchive>, recording: String, capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > MAX_BATCH {
            return Err(failure("fact_chunk_capacity_invalid"));
        }
        Ok(Self {
            archive,
            recording,
            buffer: Vec::with_capacity(capacity),
            capacity,
            failed: false,
            finished: false,
            buffer_bytes: 0,
        })
    }
    pub fn record(&mut self, fact: FactEnvelope) -> Result<()> {
        if self.failed || self.finished || fact.recording_id != self.recording {
            return Err(failure("fact_stream_closed"));
        }
        fact.validate()?;
        let bytes = serde_json::to_vec(&fact)?.len();
        if bytes > MAX_FACT_BYTES {
            return Err(failure("fact_payload_too_large"));
        }
        if self.buffer.len() == self.capacity || self.buffer_bytes + bytes > 4 * MAX_FACT_BYTES {
            self.flush()?;
        }
        self.buffer_bytes += bytes;
        self.buffer.push(fact);
        Ok(())
    }
    pub fn flush(&mut self) -> Result<()> {
        if self.failed {
            return Err(failure("fact_stream_failed"));
        }
        match self.archive.append(&self.buffer) {
            Ok(r) if r.conflicts == 0 => {
                self.buffer.clear();
                self.buffer_bytes = 0;
                Ok(())
            }
            Ok(_) => {
                self.failed = true;
                Err(failure("fact_stream_conflict"))
            }
            Err(e) => {
                self.failed = true;
                Err(e)
            }
        }
    }
    pub fn finish(&mut self, complete: bool) -> Result<()> {
        self.flush()?;
        self.archive.finish(&self.recording, complete)?;
        self.finished = true;
        Ok(())
    }
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }
}
