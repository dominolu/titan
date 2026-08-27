use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use sysinfo::{Pid, System};

const STARTUP_GRACE_NS: i64 = 30_000_000_000;
const HEARTBEAT_TIMEOUT_NS: i64 = 3_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerProcess {
    pub pid: u32,
    pub start_time: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopAction {
    Signal(WorkerProcess),
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub id: String,
    pub strategy_id: String,
    pub strategy_version: String,
    pub environment: String,
    pub event_mode: String,
    pub state: String,
    pub health: String,
    pub pid: Option<u32>,
    pub process_start_time: Option<u64>,
    pub spec_path: PathBuf,
    pub config_path: PathBuf,
    pub config_sha256: String,
    pub started_at_ns: i64,
    pub heartbeat_ns: i64,
    pub finished_at_ns: Option<i64>,
    pub exit_code: Option<i32>,
    pub result_path: PathBuf,
    pub log_path: PathBuf,
    pub report_state: String,
    pub report_path: Option<PathBuf>,
    pub market_event_count: u64,
    pub order_count: u64,
    pub fill_count: u64,
    pub error: Option<String>,
}

pub struct Registry {
    connection: Connection,
}

impl Registry {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let journal_mode: String =
            connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            connection.pragma_update(None, "journal_mode", "WAL")?;
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                owner_token TEXT NOT NULL,
                strategy_id TEXT NOT NULL DEFAULT '',
                strategy_version TEXT NOT NULL DEFAULT '',
                environment TEXT NOT NULL DEFAULT '',
                event_mode TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,
                health TEXT NOT NULL DEFAULT 'HEALTHY',
                pid INTEGER,
                process_start_time INTEGER,
                spec_path TEXT NOT NULL,
                config_path TEXT NOT NULL DEFAULT '',
                config_sha256 TEXT NOT NULL DEFAULT '',
                started_at_ns INTEGER NOT NULL,
                heartbeat_ns INTEGER NOT NULL,
                finished_at_ns INTEGER,
                exit_code INTEGER,
                result_path TEXT NOT NULL,
                log_path TEXT NOT NULL,
                report_state TEXT NOT NULL DEFAULT 'NONE',
                report_path TEXT,
                report_token TEXT,
                market_event_count INTEGER NOT NULL DEFAULT 0,
                order_count INTEGER NOT NULL DEFAULT 0,
                fill_count INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS runs_started ON runs(started_at_ns DESC);",
        )?;
        if !has_column(&connection, "runs", "process_start_time")? {
            connection.execute("ALTER TABLE runs ADD COLUMN process_start_time INTEGER", [])?;
        }
        let migrations = [
            (
                "strategy_id",
                "ALTER TABLE runs ADD COLUMN strategy_id TEXT NOT NULL DEFAULT ''",
            ),
            (
                "strategy_version",
                "ALTER TABLE runs ADD COLUMN strategy_version TEXT NOT NULL DEFAULT ''",
            ),
            (
                "environment",
                "ALTER TABLE runs ADD COLUMN environment TEXT NOT NULL DEFAULT ''",
            ),
            (
                "event_mode",
                "ALTER TABLE runs ADD COLUMN event_mode TEXT NOT NULL DEFAULT ''",
            ),
            (
                "health",
                "ALTER TABLE runs ADD COLUMN health TEXT NOT NULL DEFAULT 'HEALTHY'",
            ),
            (
                "config_path",
                "ALTER TABLE runs ADD COLUMN config_path TEXT NOT NULL DEFAULT ''",
            ),
            (
                "config_sha256",
                "ALTER TABLE runs ADD COLUMN config_sha256 TEXT NOT NULL DEFAULT ''",
            ),
            (
                "report_state",
                "ALTER TABLE runs ADD COLUMN report_state TEXT NOT NULL DEFAULT 'NONE'",
            ),
            (
                "report_path",
                "ALTER TABLE runs ADD COLUMN report_path TEXT",
            ),
            (
                "report_token",
                "ALTER TABLE runs ADD COLUMN report_token TEXT",
            ),
            (
                "market_event_count",
                "ALTER TABLE runs ADD COLUMN market_event_count INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "order_count",
                "ALTER TABLE runs ADD COLUMN order_count INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "fill_count",
                "ALTER TABLE runs ADD COLUMN fill_count INTEGER NOT NULL DEFAULT 0",
            ),
        ];
        for (column, sql) in migrations {
            if !has_column(&connection, "runs", column)? {
                connection.execute(sql, [])?;
            }
        }
        Ok(Self { connection })
    }

    pub fn create(
        &self,
        id: &str,
        owner_token: &str,
        strategy_id: &str,
        strategy_version: &str,
        environment: &str,
        event_mode: &str,
        spec_path: &Path,
        config_path: &Path,
        config_sha256: &str,
        result_path: &Path,
        log_path: &Path,
    ) -> rusqlite::Result<()> {
        let now = now_ns();
        self.connection.execute(
            "INSERT INTO runs(id, owner_token, strategy_id, strategy_version, environment,
             event_mode, state, spec_path, config_path, config_sha256, started_at_ns, heartbeat_ns,
             result_path, log_path) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'STARTING', ?7, ?8, ?9,
             ?10, ?10, ?11, ?12)",
            params![
                id,
                owner_token,
                strategy_id,
                strategy_version,
                environment,
                event_mode,
                spec_path.to_string_lossy(),
                config_path.to_string_lossy(),
                config_sha256,
                now,
                result_path.to_string_lossy(),
                log_path.to_string_lossy()
            ],
        )?;
        Ok(())
    }

    pub fn spawned(&self, id: &str, token: &str, pid: u32) -> rusqlite::Result<bool> {
        if pid == 0 || pid > i32::MAX as u32 {
            return Ok(false);
        }
        let Some(start_time) = process_start_time(pid) else {
            return Ok(false);
        };
        Ok(self.connection.execute(
            "UPDATE runs SET pid=?3, process_start_time=?4, heartbeat_ns=?5
             WHERE id=?1 AND owner_token=?2 AND state='STARTING' AND pid IS NULL",
            params![id, token, pid, start_time, now_ns()],
        )? == 1)
    }

    pub fn running(&self, id: &str, token: &str, pid: u32) -> rusqlite::Result<bool> {
        if pid == 0 || pid > i32::MAX as u32 {
            return Ok(false);
        }
        let Some(start_time) = process_start_time(pid) else {
            return Ok(false);
        };
        Ok(self.connection.execute(
            "UPDATE runs SET state='LOADING', pid=?3, process_start_time=?4, heartbeat_ns=?5
             WHERE id=?1 AND owner_token=?2 AND state='STARTING'
               AND (pid IS NULL OR (pid=?3 AND process_start_time=?4))",
            params![id, token, pid, start_time, now_ns()],
        )? == 1)
    }

    pub fn transition(&self, id: &str, token: &str, state: &str) -> rusqlite::Result<bool> {
        Ok(self.connection.execute(
            "UPDATE runs SET state=?3, heartbeat_ns=?4 WHERE id=?1 AND owner_token=?2
             AND state IN ('LOADING','COMPILING','READY','RUNNING')",
            params![id, token, state, now_ns()],
        )? == 1)
    }

    pub fn finish(
        &self,
        id: &str,
        token: &str,
        state: &str,
        exit_code: i32,
        error: Option<&str>,
    ) -> rusqlite::Result<bool> {
        let now = now_ns();
        Ok(self.connection.execute(
            "UPDATE runs SET state=?3, heartbeat_ns=?4, finished_at_ns=?4,
             exit_code=?5, error=?6, pid=NULL, process_start_time=NULL WHERE id=?1 AND owner_token=?2
             AND state IN ('STARTING','LOADING','COMPILING','READY','RUNNING','STOP_REQUESTED')",
            params![id, token, state, now, exit_code, error],
        )? == 1)
    }

    pub fn heartbeat(&self, id: &str, token: &str) -> rusqlite::Result<bool> {
        Ok(self.connection.execute(
            "UPDATE runs SET heartbeat_ns=?3 WHERE id=?1 AND owner_token=?2
             AND state IN ('LOADING','COMPILING','READY','RUNNING')",
            params![id, token, now_ns()],
        )? == 1)
    }

    pub fn reconcile(&self) -> rusqlite::Result<()> {
        let mut statement = self.connection.prepare(
            "SELECT id,state,pid,process_start_time,started_at_ns,heartbeat_ns FROM runs
             WHERE state IN ('STARTING','LOADING','COMPILING','READY','RUNNING','STOP_REQUESTED')",
        )?;
        let active: Vec<(String, String, Option<u32>, Option<u64>, i64, i64)> = statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        drop(statement);
        let now = now_ns();
        for (id, state, pid, expected_start_time, started_at_ns, heartbeat_ns) in active {
            if state == "STARTING"
                && pid.is_none()
                && now.saturating_sub(started_at_ns) < STARTUP_GRACE_NS
            {
                continue;
            }
            let alive = pid
                .zip(expected_start_time)
                .is_some_and(|(pid, expected)| process_start_time(pid) == Some(expected));
            if alive {
                let health = if now.saturating_sub(heartbeat_ns) > HEARTBEAT_TIMEOUT_NS {
                    "UNRESPONSIVE"
                } else {
                    "HEALTHY"
                };
                self.connection.execute(
                    "UPDATE runs SET health=?2 WHERE id=?1 AND state IN
                     ('STARTING','LOADING','COMPILING','READY','RUNNING','STOP_REQUESTED')",
                    params![id, health],
                )?;
            } else {
                self.connection.execute(
                    "UPDATE runs SET state='STALE', health='STALE', finished_at_ns=?2, heartbeat_ns=?2,
                     error='worker process identity is no longer alive' WHERE id=?1
                     AND state IN ('STARTING','LOADING','COMPILING','READY','RUNNING','STOP_REQUESTED')",
                    params![id, now],
                )?;
            }
        }
        Ok(())
    }

    pub fn request_stop(&self, id: &str) -> rusqlite::Result<Option<StopAction>> {
        let active = self
            .connection
            .query_row(
                "SELECT state,pid,process_start_time FROM runs
                 WHERE id=?1 AND state IN ('STARTING','LOADING','COMPILING','READY','RUNNING')",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<u32>>(1)?,
                        row.get::<_, Option<u64>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((state, pid, start_time)) = active else {
            return Ok(None);
        };
        if state == "STARTING" && pid.is_none() {
            let now = now_ns();
            self.connection.execute(
                "UPDATE runs SET state='CANCELLED', heartbeat_ns=?2, finished_at_ns=?2,
                 exit_code=0 WHERE id=?1 AND state='STARTING' AND pid IS NULL",
                params![id, now],
            )?;
            return Ok(Some(StopAction::Cancelled));
        }
        let Some((pid, start_time)) = pid.zip(start_time) else {
            return Ok(None);
        };
        self.connection.execute(
            "UPDATE runs SET state='STOP_REQUESTED', heartbeat_ns=?2 WHERE id=?1
             AND state IN ('STARTING','LOADING','COMPILING','READY','RUNNING')",
            params![id, now_ns()],
        )?;
        Ok(Some(StopAction::Signal(WorkerProcess { pid, start_time })))
    }

    pub fn update_metrics(
        &self,
        id: &str,
        token: &str,
        market_event_count: u64,
        order_count: u64,
        fill_count: u64,
    ) -> rusqlite::Result<bool> {
        Ok(self.connection.execute(
            "UPDATE runs SET market_event_count=?3, order_count=?4, fill_count=?5,
             heartbeat_ns=?6 WHERE id=?1 AND owner_token=?2
             AND state IN ('RUNNING','STOP_REQUESTED')",
            params![
                id,
                token,
                market_event_count,
                order_count,
                fill_count,
                now_ns()
            ],
        )? == 1)
    }

    pub fn report_started(&self, id: &str, token: &str, path: &Path) -> rusqlite::Result<bool> {
        Ok(self.connection.execute(
            "UPDATE runs SET report_state='GENERATING', report_path=?3, report_token=?2 WHERE id=?1
             AND state IN ('COMPLETED','STOPPED') AND report_state != 'GENERATING'",
            params![id, token, path.to_string_lossy()],
        )? == 1)
    }

    pub fn report_finished(&self, id: &str, token: &str, success: bool) -> rusqlite::Result<bool> {
        let state = if success { "READY" } else { "FAILED" };
        Ok(self.connection.execute(
            "UPDATE runs SET report_state=?3, report_token=NULL WHERE id=?1
             AND report_state='GENERATING' AND report_token=?2",
            params![id, token, state],
        )? == 1)
    }

    pub fn get(&self, id: &str) -> rusqlite::Result<Option<RunRecord>> {
        self.connection
            .query_row(
                "SELECT id,strategy_id,strategy_version,environment,event_mode,state,health,pid,
             process_start_time,spec_path,config_path,config_sha256,started_at_ns,heartbeat_ns,
             finished_at_ns,exit_code,result_path,log_path,report_state,report_path,
             market_event_count,order_count,fill_count,error FROM runs WHERE id=?1",
                [id],
                row_to_record,
            )
            .optional()
    }

    pub fn list(&self) -> rusqlite::Result<Vec<RunRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT id,strategy_id,strategy_version,environment,event_mode,state,health,pid,
             process_start_time,spec_path,config_path,config_sha256,started_at_ns,heartbeat_ns,
             finished_at_ns,exit_code,result_path,log_path,report_state,report_path,
             market_event_count,order_count,fill_count,error FROM runs ORDER BY started_at_ns DESC",
        )?;
        statement.query_map([], row_to_record)?.collect()
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: row.get(0)?,
        strategy_id: row.get(1)?,
        strategy_version: row.get(2)?,
        environment: row.get(3)?,
        event_mode: row.get(4)?,
        state: row.get(5)?,
        health: row.get(6)?,
        pid: row.get(7)?,
        process_start_time: row.get(8)?,
        spec_path: PathBuf::from(row.get::<_, String>(9)?),
        config_path: PathBuf::from(row.get::<_, String>(10)?),
        config_sha256: row.get(11)?,
        started_at_ns: row.get(12)?,
        heartbeat_ns: row.get(13)?,
        finished_at_ns: row.get(14)?,
        exit_code: row.get(15)?,
        result_path: PathBuf::from(row.get::<_, String>(16)?),
        log_path: PathBuf::from(row.get::<_, String>(17)?),
        report_state: row.get(18)?,
        report_path: row.get::<_, Option<String>>(19)?.map(PathBuf::from),
        market_event_count: row.get(20)?,
        order_count: row.get(21)?,
        fill_count: row.get(22)?,
        error: row.get(23)?,
    })
}

pub fn process_start_time(pid: u32) -> Option<u64> {
    if pid == 0 || pid > i32::MAX as u32 {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let fields = stat.rsplit_once(')')?.1.split_whitespace();
        // After the closing command parenthesis, index 0 is field 3 (`state`); field 22 is the
        // kernel start tick. Unlike wall-clock seconds, this token cannot collide for a reused
        // PID during the same second.
        return fields.into_iter().nth(19)?.parse().ok();
    }
    #[cfg(target_os = "macos")]
    {
        let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
        let size = std::mem::size_of::<libc::proc_bsdinfo>();
        // Safety: `info` is a correctly sized writable PROC_PIDTBSDINFO buffer and `pid` has
        // already been checked to fit pid_t.
        let read = unsafe {
            libc::proc_pidinfo(
                pid as i32,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size as i32,
            )
        };
        return (read == size as i32).then(|| {
            info.pbi_start_tvsec
                .saturating_mul(1_000_000)
                .saturating_add(info.pbi_start_tvusec)
        });
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    System::new_all()
        .process(Pid::from_u32(pid))
        .map(|process| process.start_time())
}

fn has_column(connection: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names.iter().any(|name| name == column))
}

pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "titan-registry-{name}-{}-{}.sqlite3",
            std::process::id(),
            now_ns()
        ))
    }

    #[test]
    fn owner_token_guards_state_and_dead_workers_reconcile_to_stale() {
        let path = path("owner");
        let registry = Registry::open(&path).unwrap();
        let artifact = path.with_extension("json");
        registry
            .create(
                "run",
                "correct",
                "strategy",
                "1.0.0",
                "backtest",
                "bar",
                Path::new("spec.json"),
                Path::new("config.toml"),
                "digest",
                &artifact,
                &artifact,
            )
            .unwrap();
        assert!(!registry.running("run", "wrong", u32::MAX).unwrap());
        assert!(!registry.running("run", "correct", u32::MAX).unwrap());
        assert!(
            registry
                .running("run", "correct", std::process::id())
                .unwrap()
        );
        assert!(!registry.heartbeat("run", "wrong").unwrap());
        registry
            .connection
            .execute(
                "UPDATE runs SET process_start_time=process_start_time+1 WHERE id='run'",
                [],
            )
            .unwrap();
        registry.reconcile().unwrap();
        assert_eq!(registry.get("run").unwrap().unwrap().state, "STALE");
    }

    #[test]
    fn terminal_state_cannot_be_reclaimed_by_a_reused_pid() {
        let path = path("pid-reuse");
        let registry = Registry::open(&path).unwrap();
        let artifact = path.with_extension("json");
        registry
            .create(
                "run",
                "owner",
                "strategy",
                "1.0.0",
                "backtest",
                "bar",
                Path::new("spec.json"),
                Path::new("config.toml"),
                "digest",
                &artifact,
                &artifact,
            )
            .unwrap();
        assert!(
            registry
                .running("run", "owner", std::process::id())
                .unwrap()
        );
        assert!(
            registry
                .finish("run", "owner", "COMPLETED", 0, None)
                .unwrap()
        );
        assert!(
            !registry
                .running("run", "owner", std::process::id())
                .unwrap()
        );
        assert!(
            !registry
                .finish("run", "owner", "FAILED", 30, Some("late error"))
                .unwrap()
        );
        assert_eq!(registry.get("run").unwrap().unwrap().state, "COMPLETED");
    }

    #[test]
    fn fresh_starting_run_survives_reconciliation() {
        let path = path("starting-grace");
        let registry = Registry::open(&path).unwrap();
        let artifact = path.with_extension("json");
        registry
            .create(
                "run",
                "owner",
                "strategy",
                "1.0.0",
                "backtest",
                "bar",
                Path::new("spec.json"),
                Path::new("config.toml"),
                "digest",
                &artifact,
                &artifact,
            )
            .unwrap();
        registry.reconcile().unwrap();
        assert_eq!(registry.get("run").unwrap().unwrap().state, "STARTING");
        registry
            .connection
            .execute(
                "UPDATE runs SET started_at_ns=started_at_ns-?1 WHERE id='run'",
                [STARTUP_GRACE_NS + 1],
            )
            .unwrap();
        registry.reconcile().unwrap();
        assert_eq!(registry.get("run").unwrap().unwrap().state, "STALE");
    }

    #[test]
    fn report_owner_token_serializes_generation_and_guards_completion() {
        let path = path("report-owner");
        let registry = Registry::open(&path).unwrap();
        let artifact = path.with_extension("json");
        registry
            .create(
                "run",
                "owner",
                "strategy",
                "1.0.0",
                "backtest",
                "bar",
                Path::new("spec.json"),
                Path::new("config.toml"),
                "digest",
                &artifact,
                &artifact,
            )
            .unwrap();
        registry
            .connection
            .execute("UPDATE runs SET state='COMPLETED' WHERE id='run'", [])
            .unwrap();

        assert!(
            registry
                .report_started("run", "report-1", Path::new("one.html"))
                .unwrap()
        );
        assert!(
            !registry
                .report_started("run", "report-2", Path::new("two.html"))
                .unwrap()
        );
        assert!(!registry.report_finished("run", "report-2", true).unwrap());
        assert_eq!(
            registry.get("run").unwrap().unwrap().report_state,
            "GENERATING"
        );
        assert!(registry.report_finished("run", "report-1", true).unwrap());
        assert_eq!(registry.get("run").unwrap().unwrap().report_state, "READY");
    }

    #[test]
    fn legacy_registry_is_migrated_without_losing_existing_runs() {
        let path = path("migration");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE runs (
                    id TEXT PRIMARY KEY,
                    owner_token TEXT NOT NULL,
                    state TEXT NOT NULL,
                    pid INTEGER,
                    process_start_time INTEGER,
                    spec_path TEXT NOT NULL,
                    started_at_ns INTEGER NOT NULL,
                    heartbeat_ns INTEGER NOT NULL,
                    finished_at_ns INTEGER,
                    exit_code INTEGER,
                    result_path TEXT NOT NULL,
                    log_path TEXT NOT NULL,
                    error TEXT
                );
                INSERT INTO runs(id,owner_token,state,spec_path,started_at_ns,heartbeat_ns,
                    result_path,log_path) VALUES
                    ('legacy','owner','COMPLETED','old.json',1,1,'result.json','worker.log');",
            )
            .unwrap();
        drop(connection);

        let registry = Registry::open(&path).unwrap();
        let legacy = registry.get("legacy").unwrap().unwrap();
        assert_eq!(legacy.state, "COMPLETED");
        assert_eq!(legacy.health, "HEALTHY");
        assert_eq!(legacy.report_state, "NONE");
        assert_eq!(legacy.market_event_count, 0);
    }

    #[test]
    fn unspawned_starting_run_can_be_cancelled() {
        let path = path("cancel");
        let registry = Registry::open(&path).unwrap();
        let artifact = path.with_extension("json");
        registry
            .create(
                "run",
                "owner",
                "strategy",
                "1.0.0",
                "backtest",
                "bar",
                Path::new("spec.json"),
                Path::new("config.toml"),
                "digest",
                &artifact,
                &artifact,
            )
            .unwrap();
        assert_eq!(
            registry.request_stop("run").unwrap(),
            Some(StopAction::Cancelled)
        );
        assert_eq!(registry.get("run").unwrap().unwrap().state, "CANCELLED");
        assert!(
            !registry
                .finish("run", "owner", "FAILED", 1, Some("late worker"))
                .unwrap()
        );
    }
}
