//! Platform-owned recovery persistence. Strategy ABI and private state layouts are unchanged.
use crate::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions as FileOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const SCHEMA: i64 = 1;
const MAX_PAYLOAD: usize = 32 * 1024 * 1024;
/// Legacy reads keep their original bound. New runtime submissions use the measured profile.
const MAX_RUNTIME_PAYLOAD: usize = 2 * 1024 * 1024;
pub const SQLITE_MIN_HANDLER_BUDGET_MS: u64 = 750;
const MAX_GENERATION: u64 = u32::MAX as u64 + 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPointRef {
    pub store_id: String,
    pub runtime_strategy_no: RuntimeStrategyNo,
    pub record_id: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverySubmitToken {
    pub session: String,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteRecoveryConfig {
    #[serde(default = "sqlite_driver")]
    pub driver: String,
    pub path: PathBuf,
    #[serde(default = "queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "busy_ms")]
    pub busy_timeout_ms: u64,
}

fn queue_capacity() -> usize {
    16
}
fn busy_ms() -> u64 {
    1000
}
fn sqlite_driver() -> String {
    "sqlite".into()
}

/// A legacy directory must be migrated explicitly, never treated as an empty fresh database.
pub fn reject_legacy_recovery_directory(root: &Path) -> LocalResult<()> {
    validate_legacy_recovery_imports(root, &root.join("runtime.db"))
}

pub fn validate_legacy_recovery_imports(root: &Path, database: &Path) -> LocalResult<()> {
    if root.is_dir() {
        for entry in
            fs::read_dir(root).map_err(|_| storage_error("recovery_directory_unavailable"))?
        {
            let entry = entry.map_err(|_| storage_error("recovery_directory_unavailable"))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("strategy-")
                && (name.ends_with(".json") || name.ends_with(".generation"))
            {
                let no = name
                    .strip_prefix("strategy-")
                    .and_then(|n| n.split('.').next())
                    .and_then(|n| n.parse::<u32>().ok())
                    .ok_or_else(|| storage_error("recovery_legacy_migration_required"))?;
                let db = connection(database, busy_ms(), false)
                    .map_err(|_| storage_error("recovery_legacy_migration_required"))?;
                let bytes = fs::read(entry.path())
                    .map_err(|_| storage_error("recovery_legacy_migration_required"))?;
                let valid = if name.ends_with(".json") {
                    let count:i64=sql(db.query_row("SELECT count(*) FROM migration_receipts WHERE strategy_no=?1 AND sha256=?2",params![no,recovery_payload_sha256(&bytes)],|r|r.get(0)))?;
                    count == 1
                } else {
                    let value: [u8; 8] = bytes
                        .try_into()
                        .map_err(|_| storage_error("recovery_legacy_migration_required"))?;
                    let floor: Option<u64> = sql(db
                        .query_row(
                            "SELECT generation FROM strategy_generations WHERE strategy_no=?1",
                            [no],
                            |r| r.get(0),
                        )
                        .optional())?;
                    let registered:bool=sql(db.query_row("SELECT EXISTS(SELECT 1 FROM generation_imports WHERE strategy_no=?1 AND source_sha256=?2)",params![no,recovery_payload_sha256(&value)],|r|r.get(0)))?;
                    registered && floor.is_some_and(|floor| floor >= u64::from_le_bytes(value))
                };
                if !valid {
                    return Err(storage_error("recovery_legacy_migration_required"));
                }
            }
        }
    }
    Ok(())
}

impl SqliteRecoveryConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            driver: sqlite_driver(),
            path,
            queue_capacity: queue_capacity(),
            busy_timeout_ms: busy_ms(),
        }
    }
    pub fn validate(&self) -> LocalResult<()> {
        if self.driver != "sqlite"
            || self.path.file_name().is_none()
            || self.path.as_os_str().is_empty()
            || self.path == Path::new(":memory:")
            || self.queue_capacity == 0
            || self.queue_capacity > 4096
            || self.busy_timeout_ms == 0
            || self.busy_timeout_ms > 5000
        {
            return Err(storage_error("recovery_config_invalid"));
        }
        Ok(())
    }
}

pub(crate) fn storage_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::CheckpointFailed,
        "recovery_storage",
        code,
        "recovery storage operation failed",
    )
}
fn sql<T>(value: rusqlite::Result<T>) -> LocalResult<T> {
    value.map_err(|error| {
        storage_error(match error {
            rusqlite::Error::SqliteFailure(code, _) => match code.code {
                rusqlite::ffi::ErrorCode::DatabaseBusy
                | rusqlite::ffi::ErrorCode::DatabaseLocked => "recovery_database_busy",
                rusqlite::ffi::ErrorCode::DiskFull => "recovery_database_full",
                rusqlite::ffi::ErrorCode::ReadOnly => "recovery_database_read_only",
                rusqlite::ffi::ErrorCode::DatabaseCorrupt
                | rusqlite::ffi::ErrorCode::NotADatabase => "recovery_database_corrupt",
                _ => "recovery_database_failed",
            },
            _ => "recovery_database_failed",
        })
    })
}
pub fn recovery_payload_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn checked_payload(bytes: &[u8]) -> LocalResult<StrategyRecoveryPoint> {
    if bytes.is_empty() || bytes.len() > MAX_PAYLOAD {
        return Err(storage_error("recovery_payload_size"));
    }
    let point: StrategyRecoveryPoint =
        serde_json::from_slice(bytes).map_err(|_| storage_error("checkpoint_decode_failed"))?;
    let legacy_orders = point.orders_list.is_empty() && point.orders_list_checksum == [0; 32];
    if point.strategy.runtime_strategy_no.0 == 0
        || point.strategy.generation != point.generation
        || point.generation == 0
        || point.generation > MAX_GENERATION
        || !point.native_snapshot().checksum_valid()
        || (!legacy_orders
            && crate::runtime_v13::orders_list_checksum(&point.orders_list)
                != point.orders_list_checksum)
        || point.recovery.as_ref().is_some_and(|baseline| {
            !baseline.integrity_valid(
                point.checksum,
                point.orders_list_checksum,
                point.public_state_identity,
            )
        })
    {
        return Err(storage_error("checkpoint_identity_or_checksum_invalid"));
    }
    Ok(point)
}

fn connection(path: &Path, busy: u64, create: bool) -> LocalResult<Connection> {
    // SQLite's upstream WAL-reset fix is required for our multi-connection writer model.
    if rusqlite::version_number() < 3_051_003 {
        return Err(storage_error("recovery_sqlite_wal_fix_required"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path.exists()
            && fs::metadata(path)
                .map_err(|_| storage_error("recovery_path_invalid"))?
                .nlink()
                != 1
        {
            return Err(storage_error("recovery_hardlink_unsupported"));
        }
    }
    let flags = if create {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let db = sql(Connection::open_with_flags(path, flags))?;
    sql(db.busy_timeout(Duration::from_millis(busy)))?;
    sql(db.pragma_update(None, "foreign_keys", true))?;
    sql(db.pragma_update(None, "synchronous", "FULL"))?;
    let mode: String = sql(db.pragma_query_value(None, "journal_mode", |r| r.get(0)))?;
    if !mode.eq_ignore_ascii_case("wal") {
        sql(db.pragma_update(None, "journal_mode", "WAL"))?;
    }
    let full: i64 = sql(db.pragma_query_value(None, "synchronous", |r| r.get(0)))?;
    let mode: String = sql(db.pragma_query_value(None, "journal_mode", |r| r.get(0)))?;
    if full != 2 || !mode.eq_ignore_ascii_case("wal") {
        return Err(storage_error("recovery_durability_unavailable"));
    }
    Ok(db)
}

fn initialize(db: &mut Connection, path: &Path) -> LocalResult<String> {
    let version: i64 = sql(db.pragma_query_value(None, "user_version", |r| r.get(0)))?;
    if version != 0 && version != SCHEMA {
        return Err(storage_error("recovery_schema_unsupported"));
    }
    if version == 0 {
        let existing: i64 = sql(db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        ))?;
        if existing != 0 {
            return Err(storage_error("recovery_database_role_conflict"));
        }
        sql(db.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),store_id TEXT NOT NULL,activation_allowed INTEGER NOT NULL CHECK(activation_allowed IN (0,1)),bound_path TEXT NOT NULL);
            INSERT INTO storage_meta VALUES(1,lower(hex(randomblob(16))),1,'');
            CREATE TABLE strategy_generations(strategy_no INTEGER PRIMARY KEY CHECK(strategy_no>0),generation INTEGER NOT NULL CHECK(generation>0),owner_session TEXT NOT NULL);
            CREATE TABLE recovery_points(id TEXT PRIMARY KEY,strategy_no INTEGER NOT NULL REFERENCES strategy_generations(strategy_no),generation INTEGER NOT NULL,checkpoint_sequence TEXT NOT NULL,sha256 TEXT NOT NULL,payload BLOB NOT NULL,compiled_digest BLOB NOT NULL,binding_digest BLOB NOT NULL,state_schema_hash BLOB NOT NULL,UNIQUE(strategy_no,sha256),UNIQUE(id,strategy_no));
            CREATE TABLE recovery_heads(strategy_no INTEGER PRIMARY KEY,generation INTEGER NOT NULL,record_id TEXT NOT NULL,revision INTEGER NOT NULL,FOREIGN KEY(record_id,strategy_no) REFERENCES recovery_points(id,strategy_no));
            CREATE TABLE recovery_pins(record_id TEXT NOT NULL,operation_id TEXT NOT NULL,PRIMARY KEY(record_id,operation_id),FOREIGN KEY(record_id) REFERENCES recovery_points(id));
            CREATE TABLE recovery_commits(session TEXT NOT NULL,sequence INTEGER NOT NULL,record_id TEXT NOT NULL,PRIMARY KEY(session,sequence),FOREIGN KEY(record_id) REFERENCES recovery_points(id));
            CREATE TABLE migration_receipts(strategy_no INTEGER NOT NULL,sha256 TEXT NOT NULL,generation_floor INTEGER NOT NULL,record_id TEXT NOT NULL,PRIMARY KEY(strategy_no,sha256),FOREIGN KEY(record_id) REFERENCES recovery_points(id));
            CREATE TABLE generation_imports(strategy_no INTEGER NOT NULL,generation_floor INTEGER NOT NULL,source_sha256 TEXT NOT NULL,PRIMARY KEY(strategy_no,source_sha256));
            PRAGMA user_version=1;"))?;
        sql(db.execute(
            "UPDATE storage_meta SET bound_path=?1",
            [path.to_string_lossy().as_ref()],
        ))?;
        sql(db.execute_batch("COMMIT"))?;
    }
    sql(
        db.query_row("SELECT store_id FROM storage_meta WHERE id=1", [], |r| {
            r.get(0)
        }),
    )
}

#[derive(Default)]
struct Progress {
    submitted: u64,
    committed: u64,
    failed: bool,
    stopped: bool,
}
struct Submission {
    token: RecoverySubmitToken,
    point: StrategyRecoveryPoint,
}

struct WriterCompletion {
    finished: Arc<AtomicBool>,
    progress: Arc<(Mutex<Progress>, Condvar)>,
}
impl Drop for WriterCompletion {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        self.progress.1.notify_all();
    }
}

/// One bounded writer per instance; namespace leases fence concurrent execution owners.
pub struct SqliteRecoveryStore {
    path: PathBuf,
    db: Mutex<Connection>,
    store_id: String,
    session: String,
    sender: Mutex<Option<mpsc::SyncSender<Submission>>>,
    progress: Arc<(Mutex<Progress>, Condvar)>,
    writer_failed: Arc<AtomicBool>,
    writer_finished: Arc<AtomicBool>,
    writer: Mutex<Option<thread::JoinHandle<()>>>,
    leases: Mutex<BTreeMap<u32, File>>,
    fail_closed: AtomicBool,
}

impl SqliteRecoveryStore {
    pub fn open(config: SqliteRecoveryConfig) -> LocalResult<Arc<Self>> {
        Self::open_mode(config, true)
    }
    pub fn open_existing(config: SqliteRecoveryConfig) -> LocalResult<Arc<Self>> {
        Self::open_mode(config, false)
    }
    fn open_mode(config: SqliteRecoveryConfig, create: bool) -> LocalResult<Arc<Self>> {
        config.validate()?;
        if create {
            fs::create_dir_all(config.path.parent().unwrap_or(Path::new(".")))
                .map_err(|_| storage_error("recovery_directory_unavailable"))?;
            let mut options = FileOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&config.path) {
                Ok(file) => file
                    .sync_all()
                    .map_err(|_| storage_error("recovery_create_failed"))?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(storage_error("recovery_create_failed")),
            }
        }
        let mut db = connection(&config.path, config.busy_timeout_ms, create)?;
        if !create {
            let version: i64 = sql(db.pragma_query_value(None, "user_version", |r| r.get(0)))?;
            if version != SCHEMA {
                return Err(storage_error("recovery_schema_unsupported"));
            }
        }
        let path =
            fs::canonicalize(&config.path).map_err(|_| storage_error("recovery_path_invalid"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if fs::metadata(&path)
                .map_err(|_| storage_error("recovery_path_invalid"))?
                .nlink()
                != 1
            {
                return Err(storage_error("recovery_hardlink_unsupported"));
            }
        }
        let store_id = initialize(&mut db, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .map_err(|_| storage_error("recovery_permissions_failed"))?;
        }
        let session: String =
            sql(db.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0)))?;
        let writer_db = connection(&path, config.busy_timeout_ms, false)?;
        let (sender, receiver) = mpsc::sync_channel::<Submission>(config.queue_capacity);
        let progress = Arc::new((Mutex::new(Progress::default()), Condvar::new()));
        let shared = progress.clone();
        let writer_failed = Arc::new(AtomicBool::new(false));
        let writer_finished = Arc::new(AtomicBool::new(false));
        let failed_flag = writer_failed.clone();
        let completion = WriterCompletion {
            finished: writer_finished.clone(),
            progress: shared.clone(),
        };
        let writer = thread::Builder::new().name("sqlite-recovery-writer".into()).spawn(move || {
            let _completion=completion;
            let mut db = writer_db;
            while let Ok(item) = receiver.recv() {
                let failed = shared.0.lock().unwrap_or_else(|p| p.into_inner()).failed;
                let result = if failed { Err(storage_error("checkpoint_writer_failed")) }
                    else { commit_point(&mut db, &item) };
                let mut p = shared.0.lock().unwrap_or_else(|p| p.into_inner());
                match &result {
                    Ok(()) => p.committed = item.token.sequence,
                    Err(_) => {
                        p.failed = true;
                        failed_flag.store(true,Ordering::Release);
                    }
                }
                shared.1.notify_all();
                drop(p);
                if let Err(error)=result {tracing::error!(reason_code=%error.reason_code, "SQLite recovery commit failed");}
            }
            let mut p = shared.0.lock().unwrap_or_else(|p| p.into_inner());
            p.stopped = true;
            shared.1.notify_all();
        }).map_err(|_| storage_error("checkpoint_writer_start_failed"))?;
        Ok(Arc::new(Self {
            path,
            db: Mutex::new(db),
            store_id,
            session,
            sender: Mutex::new(Some(sender)),
            progress,
            writer_failed,
            writer_finished,
            writer: Mutex::new(Some(writer)),
            leases: Mutex::new(BTreeMap::new()),
            fail_closed: AtomicBool::new(false),
        }))
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    fn lease(&self, no: RuntimeStrategyNo) -> LocalResult<()> {
        let mut leases = self.leases.lock().unwrap_or_else(|p| p.into_inner());
        if leases.contains_key(&no.0) {
            return Ok(());
        }
        let path = self.path.with_extension(format!("db.{}.owner.lock", no.0));
        let mut options = FileOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut lock = options
            .open(path)
            .map_err(|_| storage_error("recovery_owner_lock_failed"))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(storage_error("recovery_namespace_owned"));
            }
        }
        #[cfg(not(unix))]
        return Err(storage_error("recovery_owner_lock_unsupported"));
        // This is an ownership witness, not a second generation/state store. It detects an
        // accidentally deleted/recreated database at a previously used namespace path.
        let mut witness = Vec::new();
        (&mut lock)
            .take(128)
            .read_to_end(&mut witness)
            .map_err(|_| storage_error("recovery_owner_lock_failed"))?;
        if witness.is_empty() {
            lock.write_all(self.store_id.as_bytes())
                .and_then(|_| lock.sync_all())
                .map_err(|_| storage_error("recovery_owner_lock_failed"))?;
            File::open(self.path.parent().unwrap_or(Path::new(".")))
                .and_then(|f| f.sync_all())
                .map_err(|_| storage_error("recovery_owner_lock_failed"))?;
        } else if witness != self.store_id.as_bytes() {
            return Err(storage_error("recovery_store_identity_changed"));
        }
        leases.insert(no.0, lock);
        Ok(())
    }

    pub fn reference_by_sha(
        &self,
        no: RuntimeStrategyNo,
        sha: &str,
    ) -> LocalResult<RecoveryPointRef> {
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let id: Option<String> = sql(db
            .query_row(
                "SELECT id FROM recovery_points WHERE strategy_no=?1 AND sha256=?2",
                params![no.0, sha],
                |r| r.get(0),
            )
            .optional())?;
        let reference = RecoveryPointRef {
            store_id: self.store_id.clone(),
            runtime_strategy_no: no,
            record_id: id.ok_or_else(|| storage_error("checkpoint_reference_changed"))?,
            payload_sha256: sha.into(),
        };
        read_point(&db, &self.store_id, &reference)?;
        Ok(reference)
    }
    pub fn head_reference(&self, no: RuntimeStrategyNo) -> LocalResult<Option<RecoveryPointRef>> {
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        head_reference(&db, &self.store_id, no)
    }
    pub fn payload(&self, reference: &RecoveryPointRef) -> LocalResult<Vec<u8>> {
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        read_point(&db, &self.store_id, reference).map(|(bytes, _)| bytes)
    }
    pub fn inspect(&self) -> LocalResult<serde_json::Value> {
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let integrity: String = sql(db.query_row("PRAGMA integrity_check", [], |r| r.get(0)))?;
        if integrity != "ok" {
            return Err(storage_error("recovery_integrity_failed"));
        }
        let foreign_key_errors: i64 = sql(db.query_row(
            "SELECT count(*) FROM pragma_foreign_key_check",
            [],
            |r| r.get(0),
        ))?;
        if foreign_key_errors != 0 {
            return Err(storage_error("recovery_integrity_failed"));
        }
        let mut records_query =
            sql(db.prepare("SELECT id,strategy_no,sha256 FROM recovery_points"))?;
        let references = sql(records_query.query_map([], |r| {
            Ok(RecoveryPointRef {
                store_id: self.store_id.clone(),
                record_id: r.get(0)?,
                runtime_strategy_no: RuntimeStrategyNo(r.get(1)?),
                payload_sha256: r.get(2)?,
            })
        }))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|_| storage_error("recovery_database_failed"))?;
        for reference in references {
            read_point(&db, &self.store_id, &reference)?;
        }
        let mut query =
            sql(db.prepare("SELECT strategy_no FROM recovery_heads ORDER BY strategy_no"))?;
        let numbers = sql(query.query_map([], |r| r.get::<_, u32>(0)))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|_| storage_error("recovery_database_failed"))?;
        let mut heads = Vec::new();
        for no in numbers {
            let reference = head_reference(&db, &self.store_id, RuntimeStrategyNo(no))?
                .ok_or_else(|| storage_error("checkpoint_read_failed"))?;
            let (_, point) = read_point(&db, &self.store_id, &reference)?;
            heads.push(serde_json::json!({"reference":reference,"generation":point.generation.to_string()}));
        }
        let records: i64 =
            sql(db.query_row("SELECT count(*) FROM recovery_points", [], |r| r.get(0)))?;
        let pins: i64 = sql(db.query_row("SELECT count(*) FROM recovery_pins", [], |r| r.get(0)))?;
        let allowed: bool = sql(db.query_row(
            "SELECT activation_allowed FROM storage_meta",
            [],
            |r| r.get(0),
        ))?;
        let mut generations_query = sql(db.prepare(
            "SELECT strategy_no,generation FROM strategy_generations ORDER BY strategy_no",
        ))?;
        let generations=sql(generations_query.query_map([],|r|Ok(serde_json::json!({"runtime_strategy_no":r.get::<_,u32>(0)?,"generation":r.get::<_,u64>(1)?.to_string()}))))?
            .collect::<rusqlite::Result<Vec<_>>>().map_err(|_|storage_error("recovery_database_failed"))?;
        Ok(
            serde_json::json!({"schema_version":SCHEMA,"store_id":self.store_id,"integrity":"ok",
            "records":records,"pins":pins,"activation_allowed":allowed,"heads":heads,"generation_watermarks":generations,
            "sqlite_version":rusqlite::version(),"sqlite_version_number":rusqlite::version_number()}),
        )
    }

    /// Preserve failed-start reservations even when no legacy checkpoint was ever written.
    pub fn import_generation(
        &self,
        no: RuntimeStrategyNo,
        generation_floor: u64,
    ) -> LocalResult<()> {
        if no.0 == 0 || generation_floor == 0 || generation_floor > MAX_GENERATION {
            return Err(storage_error("recovery_generation_floor_invalid"));
        }
        self.lease(no)?;
        let mut db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
        sql(tx.execute("INSERT INTO strategy_generations VALUES(?1,?2,?3) ON CONFLICT(strategy_no) DO UPDATE SET generation=max(generation,excluded.generation),owner_session=excluded.owner_session",params![no.0,generation_floor,self.session]))?;
        sql(tx.execute(
            "INSERT OR IGNORE INTO generation_imports VALUES(?1,?2,?3)",
            params![
                no.0,
                generation_floor,
                recovery_payload_sha256(&generation_floor.to_le_bytes())
            ],
        ))?;
        sql(tx.commit())
    }

    /// Cold, explicit import preserving original checkpoint bytes and generation reservations.
    pub fn import_legacy(
        &self,
        bytes: &[u8],
        generation_floor: u64,
    ) -> LocalResult<RecoveryPointRef> {
        let point = checked_payload(bytes)?;
        if generation_floor < point.generation || generation_floor > MAX_GENERATION {
            return Err(storage_error("recovery_generation_floor_invalid"));
        }
        // This independent transaction cannot roll back with a failed payload import.
        self.import_generation(point.strategy.runtime_strategy_no, generation_floor)?;
        let sha = recovery_payload_sha256(bytes);
        let mut db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
        let existing: Option<(String, u64)> = sql(tx.query_row("SELECT record_id,generation_floor FROM migration_receipts WHERE strategy_no=?1 AND sha256=?2",
            params![point.strategy.runtime_strategy_no.0, sha], |r| Ok((r.get(0)?,r.get(1)?))).optional())?;
        let id = if let Some((id, floor)) = existing {
            if floor != generation_floor {
                return Err(storage_error("recovery_migration_conflict"));
            }
            id
        } else {
            let old = head_reference(&tx, &self.store_id, point.strategy.runtime_strategy_no)?;
            if old.is_some() {
                return Err(storage_error("recovery_migration_target_not_empty"));
            }
            let id = insert_point(&tx, &point, bytes, &sha)?;
            sql(tx.execute(
                "INSERT INTO recovery_heads VALUES(?1,?2,?3,1)",
                params![point.strategy.runtime_strategy_no.0, point.generation, id],
            ))?;
            sql(tx.execute(
                "INSERT INTO migration_receipts VALUES(?1,?2,?3,?4)",
                params![
                    point.strategy.runtime_strategy_no.0,
                    sha,
                    generation_floor,
                    id
                ],
            ))?;
            id
        };
        let reference = RecoveryPointRef {
            store_id: self.store_id.clone(),
            runtime_strategy_no: point.strategy.runtime_strategy_no,
            record_id: id,
            payload_sha256: sha,
        };
        read_point(&tx, &self.store_id, &reference)?;
        sql(tx.commit())?;
        Ok(reference)
    }

    /// SQLite-consistent backup. Copies are quarantined against automatic trading activation.
    pub fn backup(&self, destination: &Path) -> LocalResult<()> {
        self.flush(Instant::now() + Duration::from_secs(5))?;
        if destination.exists() {
            return Err(storage_error("recovery_backup_target_exists"));
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = tempfile::NamedTempFile::new_in(parent)
            .map_err(|_| storage_error("recovery_backup_target_unavailable"))?;
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let mut target = sql(Connection::open(temporary.path()))?;
        {
            let backup = sql(rusqlite::backup::Backup::new(&db, &mut target))?;
            sql(backup.run_to_completion(128, Duration::from_millis(5), None))?;
        }
        sql(target.pragma_update(None, "synchronous", "FULL"))?;
        sql(target.execute("UPDATE storage_meta SET activation_allowed=0", []))?;
        sql(target.close().map_err(|(_, e)| e))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|_| storage_error("recovery_backup_sync_failed"))?;
        temporary
            .persist_noclobber(destination)
            .map_err(|_| storage_error("recovery_backup_publish_failed"))?;
        File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(|_| storage_error("recovery_backup_sync_failed"))?;
        Ok(())
    }

    /// Only unpinned, non-head records outside the retained window can be removed.
    pub fn prune(&self, no: RuntimeStrategyNo, retain: usize) -> LocalResult<usize> {
        if retain == 0 || retain > 100_000 {
            return Err(storage_error("recovery_retention_invalid"));
        }
        self.lease(no)?;
        self.flush(Instant::now() + Duration::from_secs(5))?;
        let mut db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
        sql(tx.execute("DELETE FROM recovery_commits WHERE record_id IN (SELECT id FROM recovery_points WHERE strategy_no=?1 AND id NOT IN (SELECT record_id FROM recovery_heads) AND id NOT IN (SELECT record_id FROM recovery_pins) AND id NOT IN (SELECT record_id FROM migration_receipts) AND id NOT IN (SELECT id FROM recovery_points WHERE strategy_no=?1 ORDER BY rowid DESC LIMIT ?2))",params![no.0,retain as u64]))?;
        let removed = sql(tx.execute("DELETE FROM recovery_points WHERE strategy_no=?1 AND id NOT IN (SELECT record_id FROM recovery_heads) AND id NOT IN (SELECT record_id FROM recovery_pins) AND id NOT IN (SELECT record_id FROM migration_receipts) AND id NOT IN (SELECT record_id FROM recovery_commits) AND id NOT IN (SELECT id FROM recovery_points WHERE strategy_no=?1 ORDER BY rowid DESC LIMIT ?2)",params![no.0,retain as u64]))?;
        sql(tx.commit())?;
        Ok(removed)
    }
}

fn head_reference(
    db: &Connection,
    store: &str,
    no: RuntimeStrategyNo,
) -> LocalResult<Option<RecoveryPointRef>> {
    let row=sql(db.query_row("SELECT p.id,p.sha256,h.generation,p.generation,p.strategy_no FROM recovery_heads h LEFT JOIN recovery_points p ON p.id=h.record_id WHERE h.strategy_no=?1",
        [no.0], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?,r.get::<_,u32>(4)?))).optional())?;
    row.map(
        |(id, sha, head_generation, point_generation, strategy_no)| {
            if head_generation != point_generation || strategy_no != no.0 {
                return Err(storage_error("checkpoint_metadata_mismatch"));
            }
            Ok(RecoveryPointRef {
                store_id: store.into(),
                runtime_strategy_no: no,
                record_id: id,
                payload_sha256: sha,
            })
        },
    )
    .transpose()
}
fn read_point(
    db: &Connection,
    store: &str,
    reference: &RecoveryPointRef,
) -> LocalResult<(Vec<u8>, StrategyRecoveryPoint)> {
    if reference.store_id != store {
        return Err(storage_error("checkpoint_store_changed"));
    }
    let row = sql(db.query_row("SELECT payload,sha256,strategy_no,generation,checkpoint_sequence,compiled_digest,binding_digest,state_schema_hash FROM recovery_points WHERE id=?1",
        [&reference.record_id], |r| Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,String>(1)?,r.get::<_,u32>(2)?,r.get::<_,u64>(3)?,r.get::<_,String>(4)?,r.get::<_,Vec<u8>>(5)?,r.get::<_,Vec<u8>>(6)?,r.get::<_,Vec<u8>>(7)?))).optional())?
        .ok_or_else(|| storage_error("checkpoint_reference_changed"))?;
    if row.1 != reference.payload_sha256
        || recovery_payload_sha256(&row.0) != row.1
        || row.2 != reference.runtime_strategy_no.0
    {
        return Err(storage_error("checkpoint_reference_changed"));
    }
    let point = checked_payload(&row.0)?;
    if point.strategy.runtime_strategy_no.0 != row.2
        || point.generation != row.3
        || point.checkpoint_id.to_string() != row.4
        || point.compiled_strategy_digest.as_slice() != row.5
        || point.binding_digest.as_slice() != row.6
        || point.state_schema_hash.as_slice() != row.7
    {
        return Err(storage_error("checkpoint_metadata_mismatch"));
    }
    Ok((row.0, point))
}
fn insert_point(
    db: &Connection,
    point: &StrategyRecoveryPoint,
    bytes: &[u8],
    sha: &str,
) -> LocalResult<String> {
    sql(db.execute("INSERT INTO recovery_points VALUES(lower(hex(randomblob(16))),?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(strategy_no,sha256) DO NOTHING",
        params![point.strategy.runtime_strategy_no.0,point.generation,point.checkpoint_id.to_string(),sha,bytes,point.compiled_strategy_digest.as_slice(),point.binding_digest.as_slice(),point.state_schema_hash.as_slice()]))?;
    sql(db.query_row(
        "SELECT id FROM recovery_points WHERE strategy_no=?1 AND sha256=?2",
        params![point.strategy.runtime_strategy_no.0, sha],
        |r| r.get(0),
    ))
}
fn commit_point(db: &mut Connection, item: &Submission) -> LocalResult<()> {
    let bytes =
        serde_json::to_vec(&item.point).map_err(|_| storage_error("checkpoint_encode_failed"))?;
    let point = checked_payload(&bytes)?;
    let sha = recovery_payload_sha256(&bytes);
    let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
    let (floor, owner): (u64, String) = sql(tx.query_row(
        "SELECT generation,owner_session FROM strategy_generations WHERE strategy_no=?1",
        [point.strategy.runtime_strategy_no.0],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ))?;
    let head: Option<(u64, u64)> = sql(tx
        .query_row(
            "SELECT generation,revision FROM recovery_heads WHERE strategy_no=?1",
            [point.strategy.runtime_strategy_no.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional())?;
    if floor != point.generation
        || owner != item.token.session
        || head.is_some_and(|h| h.0 > point.generation)
    {
        return Err(storage_error("checkpoint_generation_changed"));
    }
    let id = insert_point(&tx, &point, &bytes, &sha)?;
    let revision = head
        .map_or(0, |h| h.1)
        .checked_add(1)
        .ok_or_else(|| storage_error("checkpoint_revision_overflow"))?;
    let updated = sql(tx.execute("INSERT INTO recovery_heads VALUES(?1,?2,?3,?4) ON CONFLICT(strategy_no) DO UPDATE SET generation=excluded.generation,record_id=excluded.record_id,revision=excluded.revision WHERE recovery_heads.revision=?5",
        params![point.strategy.runtime_strategy_no.0,point.generation,id,revision,revision-1]))?;
    if updated != 1 {
        return Err(storage_error("checkpoint_head_changed"));
    }
    sql(tx.execute(
        "INSERT INTO recovery_commits VALUES(?1,?2,?3)",
        params![item.token.session, item.token.sequence, id],
    ))?;
    sql(tx.commit())
}

impl StrategyRecoveryStore for SqliteRecoveryStore {
    fn minimum_handler_budget(&self) -> Option<Duration> {
        Some(Duration::from_millis(SQLITE_MIN_HANDLER_BUDGET_MS))
    }
    fn durable(&self) -> bool {
        true
    }
    fn load_pinned(
        &self,
        no: RuntimeStrategyNo,
        sha: Option<&str>,
    ) -> LocalResult<Option<StrategyRecoveryPoint>> {
        let reference = if let Some(sha) = sha {
            Some(self.reference_by_sha(no, sha)?)
        } else {
            self.head_reference(no)?
        };
        reference.map(|r| self.load_reference(&r)).transpose()
    }
    fn load_reference(&self, reference: &RecoveryPointRef) -> LocalResult<StrategyRecoveryPoint> {
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        read_point(&db, &self.store_id, reference).map(|(_, p)| p)
    }
    fn reserve_generation(&self, no: RuntimeStrategyNo, minimum: u64) -> LocalResult<u64> {
        self.check_health()?;
        self.lease(no)?;
        let mut db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
        let allowed: bool = sql(tx.query_row(
            "SELECT activation_allowed FROM storage_meta",
            [],
            |r| r.get(0),
        ))?;
        if !allowed {
            return Err(storage_error("recovery_backup_requires_reconciliation"));
        }
        let bound: String =
            sql(tx.query_row("SELECT bound_path FROM storage_meta", [], |r| r.get(0)))?;
        if bound != self.path.to_string_lossy() {
            return Err(storage_error("recovery_database_location_changed"));
        }
        let previous: Option<u64> = sql(tx
            .query_row(
                "SELECT generation FROM strategy_generations WHERE strategy_no=?1",
                [no.0],
                |r| r.get(0),
            )
            .optional())?;
        let head_generation: Option<u64> = sql(tx
            .query_row(
                "SELECT generation FROM recovery_heads WHERE strategy_no=?1",
                [no.0],
                |r| r.get(0),
            )
            .optional())?;
        if head_generation.is_some_and(|generation| previous.is_none_or(|floor| floor < generation))
        {
            return Err(storage_error("recovery_generation_watermark_invalid"));
        }
        let next = previous
            .unwrap_or(0)
            .max(minimum)
            .checked_add(1)
            .filter(|v| *v <= MAX_GENERATION)
            .ok_or_else(|| storage_error("generation_overflow"))?;
        sql(tx.execute("INSERT INTO strategy_generations VALUES(?1,?2,?3) ON CONFLICT(strategy_no) DO UPDATE SET generation=excluded.generation,owner_session=excluded.owner_session",params![no.0,next,self.session]))?;
        sql(tx.commit())?;
        Ok(next)
    }
    fn latest(&self, strategy: StrategyHandle) -> LocalResult<Option<StrategyRecoveryPoint>> {
        let point = self.load_pinned(strategy.runtime_strategy_no, None)?;
        if point.as_ref().is_some_and(|p| p.strategy != strategy) {
            return Err(storage_error("checkpoint_generation_changed"));
        }
        Ok(point)
    }
    fn submit(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()> {
        self.submit_token(snapshot).map(|_| ())
    }
    fn submit_token(&self, point: StrategyRecoveryPoint) -> LocalResult<RecoverySubmitToken> {
        self.check_health()?;
        let size = serde_json::to_vec(&point)
            .map_err(|_| storage_error("checkpoint_encode_failed"))?
            .len();
        if size > MAX_RUNTIME_PAYLOAD {
            self.fail_closed.store(true, Ordering::Release);
            return Err(storage_error("recovery_runtime_payload_budget"));
        }
        if !self
            .leases
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&point.strategy.runtime_strategy_no.0)
        {
            return Err(storage_error("recovery_namespace_not_owned"));
        }
        let sender = self.sender.lock().unwrap_or_else(|p| p.into_inner());
        let mut p = self.progress.0.lock().unwrap_or_else(|p| p.into_inner());
        let sequence = p
            .submitted
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| storage_error("checkpoint_sequence_overflow"))?;
        let token = RecoverySubmitToken {
            session: self.session.clone(),
            sequence,
        };
        sender
            .as_ref()
            .ok_or_else(|| storage_error("checkpoint_writer_stopped"))?
            .try_send(Submission {
                token: token.clone(),
                point,
            })
            .map_err(|_| storage_error("checkpoint_queue_full"))?;
        p.submitted = sequence;
        Ok(token)
    }
    fn flush_through(&self, token: &RecoverySubmitToken, deadline: Instant) -> LocalResult<()> {
        if token.session != self.session {
            return Err(storage_error("checkpoint_token_session_changed"));
        }
        let mut p = self.progress.0.lock().unwrap_or_else(|p| p.into_inner());
        if token.sequence > p.submitted {
            return Err(storage_error("checkpoint_token_invalid"));
        }
        loop {
            if p.failed
                || p.stopped
                || self.fail_closed.load(Ordering::Acquire)
                || self.writer_finished.load(Ordering::Acquire)
            {
                return Err(storage_error("checkpoint_writer_failed"));
            }
            if p.committed >= token.sequence {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.fail_closed.store(true, Ordering::Release);
                return Err(storage_error("checkpoint_flush_deadline_exceeded"));
            }
            let (next, _) = self
                .progress
                .1
                .wait_timeout(p, remaining.min(Duration::from_millis(50)))
                .unwrap_or_else(|p| p.into_inner());
            p = next;
        }
    }
    fn flush(&self, deadline: Instant) -> LocalResult<()> {
        let sequence = self
            .progress
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .submitted;
        self.flush_through(
            &RecoverySubmitToken {
                session: self.session.clone(),
                sequence,
            },
            deadline,
        )
    }
    fn pin(&self, reference: &RecoveryPointRef, operation: &str) -> LocalResult<()> {
        if operation.is_empty() || operation.len() > 256 {
            return Err(storage_error("recovery_pin_invalid"));
        }
        let mut db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        let tx = sql(db.transaction_with_behavior(TransactionBehavior::Immediate))?;
        read_point(&tx, &self.store_id, reference)?;
        let count: i64 = sql(tx.query_row("SELECT count(*) FROM recovery_pins", [], |r| r.get(0)))?;
        let exists: bool = sql(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM recovery_pins WHERE record_id=?1 AND operation_id=?2)",
            params![reference.record_id, operation],
            |r| r.get(0),
        ))?;
        if count >= 4096 && !exists {
            return Err(storage_error("recovery_pin_capacity"));
        }
        sql(tx.execute(
            "INSERT OR IGNORE INTO recovery_pins VALUES(?1,?2)",
            params![reference.record_id, operation],
        ))?;
        sql(tx.commit())
    }
    fn unpin(&self, reference: &RecoveryPointRef, operation: &str) -> LocalResult<()> {
        if reference.store_id != self.store_id {
            return Err(storage_error("checkpoint_store_changed"));
        }
        let db = self.db.lock().unwrap_or_else(|p| p.into_inner());
        sql(db.execute(
            "DELETE FROM recovery_pins WHERE record_id=?1 AND operation_id=?2",
            params![reference.record_id, operation],
        ))
        .map(|_| ())
    }
    fn check_health(&self) -> LocalResult<()> {
        if self.writer_failed.load(Ordering::Acquire)
            || self.writer_finished.load(Ordering::Acquire)
            || self.fail_closed.load(Ordering::Acquire)
        {
            Err(storage_error("checkpoint_writer_failed"))
        } else {
            Ok(())
        }
    }
    fn close(&self, deadline: Instant) -> LocalResult<()> {
        self.sender.lock().unwrap_or_else(|p| p.into_inner()).take();
        while !self.writer_finished.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                return Err(storage_error("recovery_close_deadline_exceeded"));
            }
            thread::park_timeout(Duration::from_millis(1));
        }
        if let Some(writer) = self.writer.lock().unwrap_or_else(|p| p.into_inner()).take() {
            writer
                .join()
                .map_err(|_| storage_error("checkpoint_writer_failed"))?;
        }
        self.leases
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        if self.writer_failed.load(Ordering::Acquire) || self.fail_closed.load(Ordering::Acquire) {
            return Err(storage_error("checkpoint_writer_failed"));
        }
        Ok(())
    }
}

impl Drop for SqliteRecoveryStore {
    fn drop(&mut self) {
        self.sender
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(writer) = self
            .writer
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = writer.join();
        }
        // Leases are released only after the writer has stopped.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(generation: u64, value: u8) -> StrategyRecoveryPoint {
        let mut point = StrategyRecoveryPoint {
            checkpoint_id: u64::MAX,
            strategy: StrategyHandle {
                runtime_strategy_no: RuntimeStrategyNo(77),
                generation,
            },
            strategy_instance_id: 77,
            strategy_type: "test".into(),
            source_version: "1.0.0".into(),
            generation,
            event_committed_sequence: u64::MAX,
            compiled_strategy_digest: [1; 32],
            binding_digest: [2; 32],
            abi_version: STRATEGY_ABI_V13,
            state_schema_version: 1,
            state_schema_hash: [3; 32],
            state_alignment: 8,
            state_bytes: vec![value; 8].into(),
            orders_list: Arc::from([]),
            orders_list_checksum: [0; 32],
            public_state_identity: [4; 32],
            checksum: [0; 32],
            recovery: None,
        };
        point.checksum = crate::v13::snapshot_checksum(&point.native_snapshot());
        point
    }
    fn store(root: &Path) -> Arc<SqliteRecoveryStore> {
        SqliteRecoveryStore::open(SqliteRecoveryConfig::new(root.join("runtime.db"))).unwrap()
    }
    fn commit(store: &SqliteRecoveryStore, generation: u64, value: u8) -> RecoveryPointRef {
        let token = store.submit_token(point(generation, value)).unwrap();
        store
            .flush_through(&token, Instant::now() + Duration::from_secs(3))
            .unwrap();
        store
            .head_reference(RuntimeStrategyNo(77))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn generations_survive_restart_failed_start_and_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 1);
        drop(a);
        let a = store(dir.path());
        assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
        assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 8).unwrap(), 9);
        assert!(
            a.reserve_generation(RuntimeStrategyNo(77), MAX_GENERATION)
                .is_err()
        );
    }
    #[test]
    fn immutable_reference_survives_head_changes_and_preserves_u64() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let original = commit(&a, generation, 1);
        let bytes = a.payload(&original).unwrap();
        a.pin(&original, "operation-1").unwrap();
        a.pin(&original, "operation-1").unwrap();
        let second = commit(&a, generation, 2);
        assert_ne!(original.record_id, second.record_id);
        assert_eq!(a.payload(&original).unwrap(), bytes);
        assert_eq!(
            a.load_reference(&original)
                .unwrap()
                .event_committed_sequence,
            u64::MAX
        );
        a.prune(RuntimeStrategyNo(77), 1).unwrap();
        assert_eq!(a.payload(&original).unwrap(), bytes);
        a.unpin(&original, "operation-1").unwrap();
        assert_eq!(a.prune(RuntimeStrategyNo(77), 1).unwrap(), 1);
        assert!(a.payload(&original).is_err());
        assert!(a.payload(&second).is_ok());
    }
    #[test]
    fn owner_lease_rejects_competing_store_and_releases_after_writer() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let b = store(dir.path());
        a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        assert_eq!(
            b.reserve_generation(RuntimeStrategyNo(77), 0)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_namespace_owned"
        );
        drop(a);
        assert_eq!(b.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
    }
    #[test]
    fn consecutive_tokens_and_stale_generation_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SqliteRecoveryConfig::new(dir.path().join("runtime.db"));
        config.queue_capacity = 256;
        let a = SqliteRecoveryStore::open(config).unwrap();
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let tokens = (0..64)
            .map(|v| a.submit_token(point(generation, v)).unwrap())
            .collect::<Vec<_>>();
        a.flush_through(&tokens[0], Instant::now() + Duration::from_secs(3))
            .unwrap();
        a.flush_through(
            tokens.last().unwrap(),
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
        let next = a
            .reserve_generation(RuntimeStrategyNo(77), generation)
            .unwrap();
        commit(&a, next, 65);
        let stale = a.submit_token(point(generation, 66)).unwrap();
        assert!(
            a.flush_through(&stale, Instant::now() + Duration::from_secs(3))
                .is_err()
        );
        assert!(a.check_health().is_err());
        assert_eq!(
            a.load_pinned(RuntimeStrategyNo(77), None)
                .unwrap()
                .unwrap()
                .generation,
            next
        );
    }
    #[test]
    fn failed_head_write_rolls_back_point_but_not_reserved_generation() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let db = Connection::open(a.path()).unwrap();
        db.execute_batch("CREATE TRIGGER fail_head BEFORE INSERT ON recovery_heads BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        let token = a.submit_token(point(generation, 1)).unwrap();
        assert!(
            a.flush_through(&token, Instant::now() + Duration::from_secs(3))
                .is_err()
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM recovery_points", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(a);
        db.execute_batch("DROP TRIGGER fail_head").unwrap();
        assert_eq!(
            store(dir.path())
                .reserve_generation(RuntimeStrategyNo(77), 0)
                .unwrap(),
            2
        );
    }
    #[test]
    fn reservation_fences_previous_generation_before_next_head_exists() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let original = commit(&a, generation, 1);
        assert_eq!(
            a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(),
            generation + 1
        );
        let stale = a.submit_token(point(generation, 2)).unwrap();
        assert_eq!(
            a.flush_through(&stale, Instant::now() + Duration::from_secs(3))
                .unwrap_err()
                .reason_code
                .as_ref(),
            "checkpoint_writer_failed"
        );
        assert_eq!(
            a.head_reference(RuntimeStrategyNo(77)).unwrap(),
            Some(original)
        );
        assert!(a.check_health().is_err());
    }
    #[test]
    fn ignored_head_update_cannot_acknowledge_an_unpublished_point() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let db = Connection::open(a.path()).unwrap();
        db.execute_batch("CREATE TRIGGER ignore_head BEFORE INSERT ON recovery_heads BEGIN SELECT RAISE(IGNORE); END;").unwrap();
        let token = a.submit_token(point(generation, 1)).unwrap();
        assert_eq!(
            a.flush_through(&token, Instant::now() + Duration::from_secs(3))
                .unwrap_err()
                .reason_code
                .as_ref(),
            "checkpoint_writer_failed"
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM recovery_points", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM recovery_commits", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none());
    }
    #[test]
    fn busy_writer_timeout_never_reopens_after_late_commit() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let db = Connection::open(a.path()).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        let token = a.submit_token(point(generation, 1)).unwrap();
        assert_eq!(
            a.flush_through(&token, Instant::now() + Duration::from_millis(20))
                .unwrap_err()
                .reason_code
                .as_ref(),
            "checkpoint_flush_deadline_exceeded"
        );
        db.execute_batch("ROLLBACK").unwrap();
        assert!(a.check_health().is_err());
        drop(a);
        let b = store(dir.path());
        // A late commit may exist, but no acknowledgement was returned and a new generation is required.
        assert_eq!(b.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
    }
    #[test]
    fn bad_payload_metadata_store_and_reference_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let reference = commit(&a, generation, 1);
        let mut wrong = reference.clone();
        wrong.store_id = "different".into();
        assert!(a.payload(&wrong).is_err());
        assert!(
            a.reference_by_sha(RuntimeStrategyNo(77), &"0".repeat(64))
                .is_err()
        );
        let db = Connection::open(a.path()).unwrap();
        db.execute(
            "UPDATE recovery_points SET generation=generation+1 WHERE id=?1",
            [&reference.record_id],
        )
        .unwrap();
        assert!(a.load_reference(&reference).is_err());
        db.execute(
            "UPDATE recovery_points SET generation=generation-1,payload=x'00' WHERE id=?1",
            [&reference.record_id],
        )
        .unwrap();
        assert!(a.load_reference(&reference).is_err());
        let mut bad = point(generation, 2);
        bad.state_bytes = vec![9; 8].into();
        assert!(
            a.import_legacy(&serde_json::to_vec(&bad).unwrap(), generation)
                .is_err()
        );
    }
    #[test]
    fn legacy_import_preserves_original_bytes_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = serde_json::to_string_pretty(&point(7, 3))
            .unwrap()
            .replace("runtime_strategy_no", "strategy_id")
            .into_bytes();
        fs::write(dir.path().join("strategy-77.json"), &bytes).unwrap();
        fs::write(
            dir.path().join("strategy-77.generation"),
            8_u64.to_le_bytes(),
        )
        .unwrap();
        assert!(reject_legacy_recovery_directory(dir.path()).is_err());
        let a = store(dir.path());
        let reference = a.import_legacy(&bytes, 8).unwrap();
        assert_eq!(a.payload(&reference).unwrap(), bytes);
        assert_eq!(a.import_legacy(&bytes, 8).unwrap(), reference);
        assert!(a.import_legacy(&bytes, 6).is_err());
        reject_legacy_recovery_directory(dir.path()).unwrap();
        assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 9);
        commit(&a, 9, 5);
        assert_eq!(a.import_legacy(&bytes, 8).unwrap(), reference);
        assert_eq!(
            a.head_reference(RuntimeStrategyNo(77))
                .unwrap()
                .unwrap()
                .payload_sha256,
            recovery_payload_sha256(&serde_json::to_vec(&point(9, 5)).unwrap())
        );
        assert_eq!(
            fs::read(dir.path().join("strategy-77.json")).unwrap(),
            bytes
        );
    }
    #[test]
    fn backup_is_consistent_immutable_and_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let reference = commit(&a, generation, 1);
        let backup = dir.path().join("backup.db");
        a.backup(&backup).unwrap();
        assert!(a.backup(&backup).is_err());
        let b = SqliteRecoveryStore::open_existing(SqliteRecoveryConfig::new(backup)).unwrap();
        assert_eq!(
            b.payload(&reference).unwrap(),
            a.payload(&reference).unwrap()
        );
        assert_eq!(
            b.reserve_generation(RuntimeStrategyNo(77), 0)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_backup_requires_reconciliation"
        );
        assert_eq!(b.inspect().unwrap()["activation_allowed"], false);
    }
    #[test]
    fn invalid_configuration_database_role_and_future_schema_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SqliteRecoveryConfig::new(dir.path().join("history.db"));
        config.driver = "postgres".into();
        assert!(config.validate().is_err());
        config.driver = "sqlite".into();
        let db = Connection::open(&config.path).unwrap();
        db.execute_batch("CREATE TABLE facts(id INTEGER)").unwrap();
        assert!(SqliteRecoveryStore::open(config.clone()).is_err());
        db.execute_batch("PRAGMA user_version=999").unwrap();
        assert!(SqliteRecoveryStore::open(config).is_err());
    }
    #[test]
    fn queue_overflow_does_not_forge_or_skip_a_commit_token() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SqliteRecoveryConfig::new(dir.path().join("runtime.db"));
        config.queue_capacity = 1;
        let a = SqliteRecoveryStore::open(config).unwrap();
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let blocker = Connection::open(a.path()).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut last = None;
        let mut rejected = false;
        for value in 0..128 {
            match a.submit_token(point(generation, value)) {
                Ok(token) => last = Some(token),
                Err(error) => {
                    assert_eq!(error.reason_code.as_ref(), "checkpoint_queue_full");
                    rejected = true;
                    break;
                }
            }
        }
        assert!(rejected);
        blocker.execute_batch("ROLLBACK").unwrap();
        a.flush_through(&last.unwrap(), Instant::now() + Duration::from_secs(3))
            .unwrap();
    }
    #[test]
    fn head_metadata_and_non_head_corruption_are_not_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let old = commit(&a, generation, 1);
        commit(&a, generation, 2);
        let db = Connection::open(a.path()).unwrap();
        db.execute("UPDATE recovery_heads SET generation=generation+1", [])
            .unwrap();
        assert!(a.head_reference(RuntimeStrategyNo(77)).is_err());
        db.execute("UPDATE recovery_heads SET generation=generation-1", [])
            .unwrap();
        db.execute(
            "UPDATE recovery_points SET payload=x'00' WHERE id=?1",
            [old.record_id],
        )
        .unwrap();
        assert!(a.inspect().is_err());
    }
    #[test]
    fn copied_database_cannot_activate_at_another_location() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        commit(&a, generation, 1);
        let destination = dir.path().join("copied.db");
        Connection::open(a.path())
            .unwrap()
            .execute("VACUUM INTO ?1", [destination.to_string_lossy().as_ref()])
            .unwrap();
        let b = SqliteRecoveryStore::open_existing(SqliteRecoveryConfig::new(destination)).unwrap();
        assert_eq!(
            b.reserve_generation(RuntimeStrategyNo(77), 0)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_database_location_changed"
        );
        #[cfg(unix)]
        {
            let link = dir.path().join("hardlink.db");
            fs::hard_link(a.path(), &link).unwrap();
            assert!(SqliteRecoveryStore::open_existing(SqliteRecoveryConfig::new(link)).is_err());
        }
    }
    #[test]
    fn deleted_database_cannot_silently_reset_generation_at_existing_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let path = a.path().to_owned();
        drop(a);
        fs::remove_file(path).unwrap();
        let b = store(dir.path());
        assert_eq!(
            b.reserve_generation(RuntimeStrategyNo(77), 0)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_store_identity_changed"
        );
    }
    #[test]
    fn runtime_payload_budget_is_fail_closed_without_advancing_head() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let mut large = point(generation, 1);
        large.state_bytes = vec![255; 600000].into();
        large.checksum = crate::v13::snapshot_checksum(&large.native_snapshot());
        assert_eq!(
            a.submit_token(large).unwrap_err().reason_code.as_ref(),
            "recovery_runtime_payload_budget"
        );
        assert!(a.check_health().is_err());
        assert!(a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none());
        assert_eq!(a.minimum_handler_budget(), Some(Duration::from_millis(750)));
    }
    #[test]
    fn sqlite_new_runtime_refuses_unmeasured_five_ms_default_without_extending_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let config = StrategyCoreConfig {
            recovery_store: Some(a),
            ..Default::default()
        };
        let definition = StrategyDefinition {
            strategy_key: "budget".into(),
            runtime_strategy_no: RuntimeStrategyNo(77),
            strategy_file: StrategyFileRef {
                loader_type: "native-v13".into(),
                uri: "file:///pinned.titan".into(),
                expected_digest: [1; 32],
                signature_ref: None,
            },
            markets: Arc::from([]),
            accounts: Arc::from([]),
            risk_scope: RiskScopeRef("budget".into()),
            runtime: StrategyRuntimeSpec::default(),
            recovery: StrategyRecoveryPolicy::Fresh,
            enabled: true,
            run_config_revision: 1,
        };
        assert_eq!(
            definition.runtime.max_handler_duration,
            Duration::from_millis(5)
        );
        assert_eq!(
            crate::service_core::validate_definition(&config, &definition)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_handler_budget_too_small"
        );
        assert_eq!(definition.runtime.stop_timeout, Duration::from_secs(10));
    }
    #[test]
    fn recovery_commit_latency_probe() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let mut micros = Vec::new();
        for value in 0..64 {
            let start = Instant::now();
            commit(&a, generation, value);
            micros.push(start.elapsed().as_micros());
        }
        micros.sort_unstable();
        println!(
            "sqlite_full_commit_us samples=64 p50={} p95={} p99={} max={}",
            micros[32], micros[60], micros[63], micros[63]
        );
    }
    #[test]
    #[ignore = "explicit build-server contention/performance gate"]
    fn recovery_contention_budget_probe() {
        let output = PathBuf::from(
            std::env::var("TITAN_M3_PERFORMANCE").expect("explicit performance output"),
        );
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut scenarios = Vec::new();
        for mode in [
            "idle",
            "history_writer_long_reader",
            "object_io_and_online_backup",
        ] {
            let shared = stop.clone();
            shared.store(false, Ordering::Release);
            let pressure = dir.path().join(format!("{mode}.db"));
            let workdir = dir.path().to_owned();
            let worker = if mode == "idle" {
                None
            } else {
                Some(thread::spawn(move || {
                    let db = Connection::open(&pressure).unwrap();
                    db.pragma_update(None, "journal_mode", "WAL").unwrap();
                    db.pragma_update(None, "synchronous", "FULL").unwrap();
                    db.execute_batch("CREATE TABLE data(id INTEGER PRIMARY KEY,body BLOB);INSERT INTO data VALUES(0,zeroblob(65536));").unwrap();
                    let reader = Connection::open(&pressure).unwrap();
                    reader
                        .execute_batch("BEGIN;SELECT count(*) FROM data;")
                        .unwrap();
                    let mut index = 0;
                    let mut maximum = 0_u64;
                    while !shared.load(Ordering::Acquire) {
                        db.execute(
                            "INSERT OR REPLACE INTO data VALUES(?1,randomblob(65536))",
                            [index % 256],
                        )
                        .unwrap();
                        if mode == "object_io_and_online_backup" {
                            let path = workdir.join("cold-object");
                            let mut file = File::create(&path).unwrap();
                            file.write_all(&vec![37_u8; 1024 * 1024]).unwrap();
                            file.sync_all().unwrap();
                        }
                        maximum = maximum.max(
                            fs::metadata(format!("{}-wal", pressure.display()))
                                .map_or(0, |m| m.len()),
                        );
                        index += 1;
                        thread::sleep(Duration::from_millis(1));
                    }
                    reader.execute_batch("ROLLBACK").unwrap();
                    (index, maximum)
                }))
            };
            let backup = if mode == "object_io_and_online_backup" {
                let source = a.clone();
                let destination = dir.path().join("contended-backup.db");
                Some(thread::spawn(move || {
                    let start = Instant::now();
                    source.backup(&destination).unwrap();
                    start.elapsed().as_micros() as u64
                }))
            } else {
                None
            };
            let mut samples = Vec::new();
            let mut sizes = Vec::new();
            for n in 0..96 {
                let mut p = point(generation, n);
                let size = match n % 4 {
                    0 => 8,
                    1 => 32 * 1024,
                    2 => 256 * 1024,
                    _ => 500000,
                };
                p.state_bytes = vec![255; size].into();
                p.checksum = crate::v13::snapshot_checksum(&p.native_snapshot());
                sizes.push(serde_json::to_vec(&p).unwrap().len());
                let start = Instant::now();
                let token = a.submit_token(p).unwrap();
                a.flush_through(&token, start + Duration::from_millis(750))
                    .unwrap();
                samples.push(start.elapsed().as_micros() as u64);
            }
            let began = Instant::now();
            let mut tokens = Vec::new();
            for n in 0..16 {
                tokens.push(a.submit_token(point(generation, n)).unwrap());
            }
            a.flush_through(tokens.last().unwrap(), began + Duration::from_secs(5))
                .unwrap();
            let burst_us = began.elapsed().as_micros() as u64;
            stop.store(true, Ordering::Release);
            let (pressure_commits, pressure_wal_peak) =
                worker.map(|w| w.join().unwrap()).unwrap_or((0, 0));
            let backup_us = backup.map(|b| b.join().unwrap());
            samples.sort_unstable();
            sizes.sort_unstable();
            assert!(samples[95] < 750000);
            assert!(burst_us < 5000000);
            scenarios.push(serde_json::json!({"scenario":mode,"samples":samples.len(),"p50_us":samples[48],"p95_us":samples[91],"p99_us":samples[95],"max_us":samples[95],"payload_bytes_p50":sizes[48],"payload_bytes_max":sizes[95],"burst16_us":burst_us,"pressure_commits":pressure_commits,"pressure_wal_peak_bytes":pressure_wal_peak,"online_backup_us":backup_us}));
        }
        fs::write(output,serde_json::to_vec_pretty(&serde_json::json!({"passed":true,"scenarios":scenarios,"sqlite":rusqlite::version(),"synchronous":"FULL","single_submission_budget_ms":750,"burst16_budget_ms":5000,"existing_runtime_handler_ms":5000,"existing_stop_ms":10000,"default_handler_ms":5,"default_handler_admitted":false,"hardware_power_loss_tested":false})).unwrap()).unwrap();
        a.close(Instant::now() + Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn explicit_close_releases_owner_even_if_runtime_keeps_store_alive() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        commit(&a, generation, 1);
        let b = store(dir.path());
        a.close(Instant::now() + Duration::from_secs(2)).unwrap();
        assert_eq!(b.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
        assert!(a.submit_token(point(generation, 2)).is_err());
    }
    #[test]
    fn close_timeout_keeps_owner_until_writer_has_drained() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let blocker = Connection::open(a.path()).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        a.submit_token(point(generation, 1)).unwrap();
        assert!(a.close(Instant::now() + Duration::from_millis(20)).is_err());
        let b = store(dir.path());
        assert!(b.reserve_generation(RuntimeStrategyNo(77), 0).is_err());
        blocker.execute_batch("ROLLBACK").unwrap();
        a.close(Instant::now() + Duration::from_secs(3)).unwrap();
        assert_eq!(b.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
    }
    #[test]
    fn generation_only_legacy_migration_preserves_failed_start_reservations() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("strategy-77.generation"),
            8_u64.to_le_bytes(),
        )
        .unwrap();
        assert!(reject_legacy_recovery_directory(dir.path()).is_err());
        let a = store(dir.path());
        a.import_generation(RuntimeStrategyNo(77), 8).unwrap();
        a.import_generation(RuntimeStrategyNo(77), 8).unwrap();
        assert!(a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none());
        reject_legacy_recovery_directory(dir.path()).unwrap();
        assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 9);
        assert!(a.import_generation(RuntimeStrategyNo(77), 0).is_err());
    }
    #[test]
    fn actual_sqlite_full_and_read_only_fail_without_partial_head() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let mut db = connection(a.path(), 1000, false).unwrap();
        let pages: u32 = db
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        db.pragma_update(None, "max_page_count", pages).unwrap();
        let mut large = point(generation, 1);
        large.state_bytes = vec![1; 65536].into();
        large.checksum = crate::v13::snapshot_checksum(&large.native_snapshot());
        let submission = Submission {
            token: RecoverySubmitToken {
                session: a.session.clone(),
                sequence: 1,
            },
            point: large,
        };
        assert_eq!(
            commit_point(&mut db, &submission)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_database_full"
        );
        assert!(a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none());
        let mut readonly =
            Connection::open_with_flags(a.path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            commit_point(&mut readonly, &submission)
                .unwrap_err()
                .reason_code
                .as_ref(),
            "recovery_database_read_only"
        );
        assert!(a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none());
    }
    #[test]
    fn sqlite_runtime_includes_upstream_wal_reset_fix() {
        assert!(rusqlite::version_number() >= 3_051_003);
    }
    #[test]
    fn foreign_and_unaccepted_tokens_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path());
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(
            a.flush_through(
                &RecoverySubmitToken {
                    session: "other".into(),
                    sequence: 0
                },
                deadline
            )
            .is_err()
        );
        assert!(
            a.flush_through(
                &RecoverySubmitToken {
                    session: a.session.clone(),
                    sequence: 1
                },
                deadline
            )
            .is_err()
        );
    }

    #[test]
    #[ignore = "subprocess fixture invoked only by sqlite_kill_boundaries"]
    fn crash_child() {
        let root = PathBuf::from(std::env::var("TITAN_SQLITE_CRASH_ROOT").unwrap());
        let a = store(&root);
        let generation = a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap();
        let blocker = Connection::open(a.path()).unwrap();
        let before = std::env::var("TITAN_SQLITE_CRASH_BEFORE").unwrap() == "1";
        if before {
            blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        }
        let token = a.submit_token(point(generation, 1)).unwrap();
        if !before {
            a.flush_through(&token, Instant::now() + Duration::from_secs(3))
                .unwrap();
        }
        fs::write(root.join("ready"), b"ready").unwrap();
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }
    #[test]
    fn sqlite_kill_boundaries() {
        for before in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "recovery_storage::tests::crash_child",
                ])
                .env("TITAN_SQLITE_CRASH_ROOT", dir.path())
                .env("TITAN_SQLITE_CRASH_BEFORE", if before { "1" } else { "0" })
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !dir.path().join("ready").exists() {
                if Instant::now() > deadline || child.try_wait().unwrap().is_some() {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("crash fixture failed");
                }
                thread::sleep(Duration::from_millis(5));
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let a = store(dir.path());
            assert_eq!(
                a.head_reference(RuntimeStrategyNo(77)).unwrap().is_none(),
                before
            );
            assert_eq!(a.reserve_generation(RuntimeStrategyNo(77), 0).unwrap(), 2);
        }
    }
}
