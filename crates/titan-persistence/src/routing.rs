use crate::*;
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub(crate) fn file_lock(path: &Path, exclusive: bool) -> Result<File> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if unsafe {
                libc::flock(
                    file.as_raw_fd(),
                    (if exclusive {
                        libc::LOCK_EX
                    } else {
                        libc::LOCK_SH
                    }) | libc::LOCK_NB,
                )
            } == 0
            {
                return Ok(file);
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(error.into());
            }
            if Instant::now() >= until {
                return Err(failure("storage_maintenance_in_progress"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    #[cfg(not(unix))]
    {
        Err(failure("storage_owner_lock_unsupported"))
    }
}
pub(crate) fn directory_bytes(root: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_owned()];
    let mut visited = 0;
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            visited += 1;
            if visited > 1_000_000 {
                return Err(failure("object_inventory_budget_exceeded"));
            }
            if metadata.file_type().is_symlink() {
                return Err(failure("object_path_escape"));
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| failure("object_size_overflow"))?;
            }
        }
    }
    Ok(total)
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryBackend {
    Sqlite,
    Postgres,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectBackend {
    Filesystem,
    S3,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryRoute {
    pub source_id: String,
    pub path: PathBuf,
    pub revision: i64,
    pub backend: HistoryBackend,
    pub target_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectRoute {
    pub revision: i64,
    pub backend: ObjectBackend,
    pub target_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub id: String,
    pub domain: String,
    pub source_id: String,
    pub from_revision: i64,
    pub target: String,
    pub target_id: Option<String>,
    pub phase: String,
    pub proof: serde_json::Value,
}
#[derive(Clone)]
pub struct StorageControl {
    root: PathBuf,
    pub control_id: String,
    pub profile: StorageProfile,
    // Open connection, no long read transaction: avoids last-connection WAL cleanup races.
    _keepalive: std::sync::Arc<std::sync::Mutex<Connection>>,
}
impl StorageControl {
    pub fn exists(root: &Path) -> bool {
        root.join("control.db").is_file() || root.join(".configuration.lock").exists()
    }
    pub fn initialize(root: &Path, profile: StorageProfile) -> Result<Self> {
        profile.validate()?;
        fs::create_dir_all(root)?;
        let previously_configured = root.join(".configuration.lock").exists();
        let _owner = file_lock(&root.join(".configuration.lock"), true)?;
        if previously_configured && !root.join("control.db").is_file() {
            return Err(failure("storage_control_missing_requires_reconciliation"));
        }
        let mut db = Connection::open(root.join("control.db"))?;
        db.busy_timeout(Duration::from_millis(200))?;
        let other:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name IN ('recovery_points','archive_records','object_catalog','resources'))",[],|r|r.get(0))?;
        if other {
            return Err(failure("storage_control_role_conflict"));
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS storage_control_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL,control_id TEXT NOT NULL,profile BLOB NOT NULL,profile_sha256 TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS history_routes(source_id TEXT PRIMARY KEY,path TEXT NOT NULL,revision INTEGER NOT NULL,backend TEXT NOT NULL,target_id TEXT);
            CREATE TABLE IF NOT EXISTS object_route(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL,backend TEXT NOT NULL,target_id TEXT);
            CREATE TABLE IF NOT EXISTS storage_plans(id TEXT PRIMARY KEY,domain TEXT NOT NULL,source_id TEXT NOT NULL,from_revision INTEGER NOT NULL,target TEXT NOT NULL,target_id TEXT,phase TEXT NOT NULL,proof TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS storage_targets(name TEXT PRIMARY KEY,target_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS storage_worker_health(source_id TEXT PRIMARY KEY,status TEXT NOT NULL,detail TEXT NOT NULL,updated_ns TEXT NOT NULL);")?;
        let bytes = serde_json::to_vec(&profile)?;
        let hash = sha256(&bytes);
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO storage_control_meta VALUES(1,1,?1,?2,?3)",
            params![uuid::Uuid::new_v4().to_string(), bytes, hash],
        )?;
        let (version, old): (u32, String) = tx.query_row(
            "SELECT schema_version,profile_sha256 FROM storage_control_meta WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if version != 1 || old != hash {
            return Err(failure("storage_profile_identity_changed"));
        }
        tx.execute(
            "INSERT OR IGNORE INTO object_route VALUES(1,1,'filesystem',NULL)",
            [],
        )?;
        tx.commit()?;
        let result = Self::open(root)?;
        if let Some(pg) = &profile.postgres {
            let pg = PostgresProjection::open(pg.clone())?;
            result.pin_target("postgres", &pg.target_id)?;
        }
        if let Some(s3) = &profile.s3 {
            let store = S3ObjectStore::connect(
                s3.clone(),
                profile.budget.clone(),
                &root.join(".s3-stage"),
                result.target("s3")?.as_deref(),
            )?;
            store.claim_namespace(&result.control_id)?;
            result.pin_target("s3", &store.store_id)?;
        }
        Ok(result)
    }
    pub fn open(root: &Path) -> Result<Self> {
        sqlite_cold_retry(|| Self::open_once(root))
    }
    fn open_once(root: &Path) -> Result<Self> {
        let db =
            Connection::open_with_flags(root.join("control.db"), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_millis(200))?;
        let (version,control_id,bytes,hash):(u32,String,Vec<u8>,String)=db.query_row("SELECT schema_version,control_id,profile,profile_sha256 FROM storage_control_meta WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        if version != 1 || sha256(&bytes) != hash {
            return Err(failure("storage_profile_corrupt"));
        }
        let profile: StorageProfile = serde_json::from_slice(&bytes)?;
        profile.validate()?;
        Ok(Self {
            root: fs::canonicalize(root)?,
            control_id,
            profile,
            _keepalive: std::sync::Arc::new(std::sync::Mutex::new(db)),
        })
    }
    fn db(&self) -> Result<Connection> {
        let db = Connection::open_with_flags(
            self.root.join("control.db"),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        db.busy_timeout(Duration::from_millis(1000))?;
        db.pragma_update(None, "synchronous", "FULL")?;
        Ok(db)
    }
    fn read_db(&self) -> Result<Connection> {
        let db = Connection::open_with_flags(
            self.root.join("control.db"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        db.busy_timeout(Duration::from_millis(200))?;
        Ok(db)
    }
    fn target(&self, name: &str) -> Result<Option<String>> {
        use rusqlite::OptionalExtension;
        Ok(self
            .read_db()?
            .query_row(
                "SELECT target_id FROM storage_targets WHERE name=?1",
                [name],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn pin_target(&self, name: &str, id: &str) -> Result<()> {
        let db = self.db()?;
        db.execute(
            "INSERT OR IGNORE INTO storage_targets VALUES(?1,?2)",
            params![name, id],
        )?;
        if self.target(name)?.as_deref() != Some(id) {
            return Err(failure("storage_target_identity_changed"));
        }
        Ok(())
    }
    pub fn register_source(&self, path: &Path) -> Result<HistoryRoute> {
        let path = crate::snapshot::projectable_history_path(path)?;
        sqlite_cold_retry(|| self.register_source_once(&path))
    }
    fn register_source_once(&self, path: &Path) -> Result<HistoryRoute> {
        let snapshot = ArchiveSnapshot::open(path)?;
        let path = fs::canonicalize(path)?;
        self.check_source_path(&path, &snapshot.metadata.store_id)?;
        let exists: bool = self.read_db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM history_routes WHERE source_id=?1)",
            [&snapshot.metadata.store_id],
            |r| r.get(0),
        )?;
        if !exists {
            self.db()?.execute(
                "INSERT OR IGNORE INTO history_routes VALUES(?1,?2,1,'sqlite',NULL)",
                params![
                    snapshot.metadata.store_id,
                    path.to_str()
                        .ok_or_else(|| failure("storage_path_invalid"))?
                ],
            )?;
        }
        let route = self.history_route(&snapshot.metadata.store_id)?;
        if route.path != path {
            return Err(failure("history_source_path_changed"));
        }
        Ok(route)
    }
    fn check_source_path(&self, path: &Path, source: &str) -> Result<()> {
        let changed: bool = self.read_db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM history_routes WHERE path=?1 AND source_id<>?2)",
            params![
                path.to_str()
                    .ok_or_else(|| failure("storage_path_invalid"))?,
                source
            ],
            |r| r.get(0),
        )?;
        if changed {
            return Err(failure("history_source_identity_changed"));
        }
        Ok(())
    }
    pub fn history_route(&self, source: &str) -> Result<HistoryRoute> {
        let db = self.read_db()?;
        let (path, revision, backend, target_id): (String, i64, String, Option<String>) = db
            .query_row(
                "SELECT path,revision,backend,target_id FROM history_routes WHERE source_id=?1",
                [source],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
        let backend = match backend.as_str() {
            "sqlite" => HistoryBackend::Sqlite,
            "postgres" => HistoryBackend::Postgres,
            _ => return Err(failure("history_backend_unsupported")),
        };
        Ok(HistoryRoute {
            source_id: source.into(),
            path: path.into(),
            revision,
            backend,
            target_id,
        })
    }
    pub fn sources(&self) -> Result<Vec<HistoryRoute>> {
        let db = self.read_db()?;
        let mut q =
            db.prepare("SELECT source_id FROM history_routes ORDER BY source_id LIMIT 4097")?;
        let ids = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ids.len() > 4096 {
            return Err(failure("projection_source_budget_exceeded"));
        }
        ids.iter().map(|s| self.history_route(s)).collect()
    }
    pub fn object_route(&self) -> Result<ObjectRoute> {
        let db = self.read_db()?;
        let (revision, backend, target_id): (i64, String, Option<String>) = db.query_row(
            "SELECT revision,backend,target_id FROM object_route WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let backend = match backend.as_str() {
            "filesystem" => ObjectBackend::Filesystem,
            "s3" => ObjectBackend::S3,
            _ => return Err(failure("object_backend_unsupported")),
        };
        Ok(ObjectRoute {
            revision,
            backend,
            target_id,
        })
    }
    fn postgres(&self, expected: Option<&str>) -> Result<PostgresProjection> {
        let config = self
            .profile
            .postgres
            .clone()
            .ok_or_else(|| failure("postgres_not_configured"))?;
        let pinned = self.target("postgres")?;
        let target = expected.or(pinned.as_deref());
        let pg = if let Some(target) = target {
            PostgresProjection::attach(config, target.into())?
        } else {
            PostgresProjection::open(config)?
        };
        if expected.is_some_and(|id| id != pg.target_id) {
            return Err(failure("postgres_target_identity_changed"));
        }
        Ok(pg)
    }
    fn s3(&self, expected: Option<&str>) -> Result<S3ObjectStore> {
        let pinned = self.target("s3")?;
        let s3 = S3ObjectStore::connect(
            self.profile
                .s3
                .clone()
                .ok_or_else(|| failure("s3_not_configured"))?,
            self.profile.budget.clone(),
            &self.root.join(".s3-stage"),
            expected.or(pinned.as_deref()),
        )?;
        s3.claim_namespace(&self.control_id)?;
        Ok(s3)
    }
    pub fn sync_source(&self, path: &Path) -> Result<ProjectionProgress> {
        let route = self.register_source(path)?;
        self.postgres(route.target_id.as_deref())?
            .sync_source(&route.path, self.profile.budget.projection_batch_rows)
    }
    pub fn sync_all(&self) -> Result<Vec<serde_json::Value>> {
        let mut results = Vec::new();
        let mut failed = false;
        let pg = self.postgres(None)?;
        for route in self.sources()? {
            let result = if route
                .target_id
                .as_deref()
                .is_some_and(|id| id != pg.target_id)
            {
                Err(failure("postgres_target_identity_changed"))
            } else {
                pg.sync_source(&route.path, self.profile.budget.projection_batch_rows)
            };
            failed |= result.is_err();
            let (status, detail, value) = match result {
                Ok(p) => ("healthy", "", serde_json::to_value(p)?),
                Err(e) => (
                    "failed",
                    "projection_unavailable",
                    serde_json::json!({"source_id":route.source_id,"error":e.to_string()}),
                ),
            };
            self.db()?.execute("INSERT INTO storage_worker_health VALUES(?1,?2,?3,?4) ON CONFLICT(source_id) DO UPDATE SET status=excluded.status,detail=excluded.detail,updated_ns=excluded.updated_ns",params![route.source_id,status,detail,now_ns()])?;
            results.push(value);
        }
        if failed {
            return Err(failure("projection_one_or_more_sources_failed"));
        }
        Ok(results)
    }
    pub fn inspect(&self) -> Result<serde_json::Value> {
        let db = self.read_db()?;
        let mut q=db.prepare("SELECT source_id,status,detail,updated_ns FROM storage_worker_health ORDER BY source_id")?;
        let health=q.query_map([],|r|Ok(serde_json::json!({"source_id":r.get::<_,String>(0)?,"status":r.get::<_,String>(1)?,"detail":r.get::<_,String>(2)?,"updated_ns":r.get::<_,String>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            serde_json::json!({"schema_version":1,"control_id":self.control_id,"profile_sha256":sha256(&serde_json::to_vec(&self.profile)?),"objects":self.object_route()?,"history":self.sources()?,"worker":health,"budget":self.profile.budget,"recovery_backend":"local_sqlite"}),
        )
    }
    fn save_plan(
        &self,
        domain: &str,
        source: &str,
        revision: i64,
        target: &str,
        target_id: Option<&str>,
    ) -> Result<MigrationPlan> {
        let plan = MigrationPlan {
            id: uuid::Uuid::new_v4().to_string(),
            domain: domain.into(),
            source_id: source.into(),
            from_revision: revision,
            target: target.into(),
            target_id: target_id.map(str::to_owned),
            phase: "copying".into(),
            proof: serde_json::json!({}),
        };
        self.db()?.execute(
            "INSERT INTO storage_plans VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                plan.id,
                domain,
                source,
                revision,
                target,
                target_id,
                plan.phase,
                plan.proof.to_string()
            ],
        )?;
        Ok(plan)
    }
    pub fn plan(&self, id: &str) -> Result<MigrationPlan> {
        self.read_db()?.query_row("SELECT id,domain,source_id,from_revision,target,target_id,phase,proof FROM storage_plans WHERE id=?1",[id],|r|Ok(MigrationPlan{id:r.get(0)?,domain:r.get(1)?,source_id:r.get(2)?,from_revision:r.get(3)?,target:r.get(4)?,target_id:r.get(5)?,phase:r.get(6)?,proof:serde_json::from_str(&r.get::<_,String>(7)?).map_err(|e|rusqlite::Error::FromSqlConversionFailure(7,rusqlite::types::Type::Text,Box::new(e)))?})).map_err(Into::into)
    }
    pub fn prepare_history(&self, path: &Path, target: HistoryBackend) -> Result<MigrationPlan> {
        let route = self.register_source(path)?;
        if route.backend == target {
            return Err(failure("history_backend_already_active"));
        }
        let pg = self.postgres(route.target_id.as_deref())?;
        let plan = self.save_plan(
            "history",
            &route.source_id,
            route.revision,
            if target == HistoryBackend::Postgres {
                "postgres"
            } else {
                "sqlite"
            },
            Some(&pg.target_id),
        )?;
        self.resume(&plan.id)
    }
    fn catch_up(&self, pg: &PostgresProjection, path: &Path) -> Result<i64> {
        let upper = ArchiveSnapshot::open(path)?.metadata.upper_row;
        let until = Instant::now() + Duration::from_secs(120);
        loop {
            let p = pg.sync_source(path, self.profile.budget.projection_batch_rows)?;
            if p.committed_row.parse::<i64>()? >= upper {
                return Ok(upper);
            }
            if Instant::now() >= until {
                return Err(failure("projection_catchup_budget_exceeded"));
            }
        }
    }
    pub fn resume(&self, id: &str) -> Result<MigrationPlan> {
        let plan = self.plan(id)?;
        if plan.phase == "committed" {
            return Ok(plan);
        }
        if !matches!(
            (plan.domain.as_str(), plan.target.as_str()),
            ("history", "sqlite" | "postgres") | ("objects", "filesystem" | "s3")
        ) || plan.from_revision < 0
            || plan.from_revision == i64::MAX
        {
            return Err(failure("storage_plan_invalid"));
        }
        let _lock = file_lock(
            &self.root.join("locks").join(format!(
                "{}-{}",
                plan.domain,
                sha256(plan.source_id.as_bytes())
            )),
            true,
        )?;
        let proof = if plan.domain == "history" {
            let route = self.history_route(&plan.source_id)?;
            if route.revision != plan.from_revision {
                return Err(failure("storage_plan_revision_changed"));
            }
            let pg = self.postgres(plan.target_id.as_deref())?;
            let upper = self.catch_up(&pg, &route.path)?;
            let expected = pg.digest(&route.source_id, upper)?;
            if plan.target == "sqlite" {
                pg.restore_source(&route.path, &route.source_id, upper)?;
            }
            let actual = ArchiveSnapshot::open(&route.path)?.digest(upper)?;
            if actual != expected {
                return Err(failure("history_cutover_digest_mismatch"));
            }
            serde_json::json!({"range":expected,"source_upper_at_verification":upper.to_string()})
        } else if plan.domain == "objects" {
            if self.object_route()?.revision != plan.from_revision {
                return Err(failure("storage_plan_revision_changed"));
            }
            self.copy_objects(&plan)?
        } else {
            return Err(failure("storage_plan_domain_invalid"));
        };
        self.db()?.execute(
            "UPDATE storage_plans SET phase='verified',proof=?2 WHERE id=?1 AND phase<>'committed'",
            params![id, proof.to_string()],
        )?;
        self.plan(id)
    }
    pub fn activate(&self, id: &str) -> Result<MigrationPlan> {
        let original = self.plan(id)?;
        if original.phase == "committed" {
            return Ok(original);
        }
        let _fence = if original.domain == "objects" {
            Some(file_lock(&self.root.join(".publication.lock"), true)?)
        } else {
            None
        };
        let plan = self.resume(id)?;
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let affected = if plan.domain == "history" {
            tx.execute("UPDATE history_routes SET revision=revision+1,backend=?2,target_id=?3 WHERE source_id=?1 AND revision=?4",params![plan.source_id,plan.target,plan.target_id,plan.from_revision])?
        } else {
            tx.execute("UPDATE object_route SET revision=revision+1,backend=?1,target_id=?2 WHERE id=1 AND revision=?3",params![plan.target,plan.target_id,plan.from_revision])?
        };
        if affected != 1 {
            return Err(failure("storage_plan_revision_changed"));
        }
        tx.execute(
            "UPDATE storage_plans SET phase='committed' WHERE id=?1",
            [id],
        )?;
        tx.commit()?;
        self.plan(id)
    }
    pub fn prepare_objects(&self, target: ObjectBackend) -> Result<MigrationPlan> {
        let route = self.object_route()?;
        if route.backend == target {
            return Err(failure("object_backend_already_active"));
        }
        let s3 = self.s3(if route.backend == ObjectBackend::S3 {
            route.target_id.as_deref()
        } else {
            None
        })?;
        let plan = self.save_plan(
            "objects",
            "objects",
            route.revision,
            if target == ObjectBackend::S3 {
                "s3"
            } else {
                "filesystem"
            },
            Some(&s3.store_id),
        )?;
        drop(s3);
        self.resume(&plan.id)
    }
    fn copy_objects(&self, plan: &MigrationPlan) -> Result<serde_json::Value> {
        let local =
            LocalObjectStore::with_budget(self.root.join("objects"), self.profile.budget.clone())?;
        let s3 = self.s3(plan.target_id.as_deref())?;
        let (source, target): (&dyn ObjectStore, &dyn ObjectStore) = if plan.target == "s3" {
            (&local, &s3)
        } else {
            (&s3, &local)
        };
        let refs = if plan.target == "s3" {
            local_inventory(&self.root.join("objects"))?
        } else {
            s3.inventory()?
        };
        let mut hash = sha2::Sha256::new();
        use sha2::Digest;
        let mut bytes = 0_u64;
        for r in &refs {
            let mut file = source.open_verified(r)?;
            let stored = target.put(&mut file)?;
            if stored != *r {
                return Err(failure("object_cutover_digest_mismatch"));
            }
            hash.update(r.sha256.as_bytes());
            hash.update(r.bytes.to_be_bytes());
            bytes = bytes
                .checked_add(r.bytes)
                .ok_or_else(|| failure("object_size_overflow"))?;
        }
        let entries = ResultCatalog::entries(&self.root.join("catalog.db"))?;
        for (id, kind, reference) in &entries {
            let manifest = PublishedManifest::load(source, reference)?;
            if manifest.logical_id != *id || manifest.kind != *kind {
                return Err(failure("publication_catalog_identity_changed"));
            }
            let copied = PublishedManifest::load(target, reference)?;
            if serde_json::to_vec(&manifest)? != serde_json::to_vec(&copied)? {
                return Err(failure("publication_manifest_changed"));
            }
            manifest.verify_objects(source)?;
            copied.verify_objects(target)?;
        }
        Ok(
            serde_json::json!({"published_roots":entries.len().to_string(),"objects":refs.len().to_string(),"bytes":bytes.to_string(),"inventory_sha256":format!("{:x}",hash.finalize()),"target_id":s3.store_id}),
        )
    }
    pub fn prune_history(&self, path: &Path, through: i64) -> Result<RangeDigest> {
        let route = self.register_source(path)?;
        let path = route.path.as_path();
        if route.backend != HistoryBackend::Postgres {
            return Err(failure("history_prune_requires_postgres_route"));
        }
        let _lock = file_lock(
            &self
                .root
                .join("locks")
                .join(format!("history-{}", sha256(route.source_id.as_bytes()))),
            true,
        )?;
        let pg = self.postgres(route.target_id.as_deref())?;
        let progress = pg
            .progress(&route.source_id)?
            .ok_or_else(|| failure("projection_source_unavailable"))?;
        if through < 0 || through > progress.committed_row.parse()? {
            return Err(failure("history_prune_range_not_durable"));
        }
        let local = ArchiveSnapshot::open(path)?;
        if local.metadata.pruned_through > 0 {
            return Err(failure("history_prune_restore_before_next_retention"));
        }
        let expected = pg.digest(&route.source_id, through)?;
        if local.digest(through)? != expected {
            return Err(failure("history_prune_digest_mismatch"));
        }
        drop(local);
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_millis(200))?;
        initialize_archive(&db)?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let active:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM archive_records r JOIN archive_sources s ON s.recording=r.recording WHERE r.id<=?1 AND s.clean=0)",[through],|r|r.get(0))?;
        if active {
            return Err(failure("history_prune_source_still_open"));
        }
        tx.execute("DELETE FROM archive_records WHERE id<=?1", [through])?;
        tx.execute(
            "INSERT INTO archive_retention VALUES(?1,?2,?3,?4)",
            params![
                pg.target_id,
                through,
                expected.records.parse::<i64>()?,
                expected.sha256
            ],
        )?;
        tx.commit()?;
        Ok(expected)
    }
    pub fn evict_local_objects(&self) -> Result<usize> {
        let _fence = file_lock(&self.root.join(".publication.lock"), true)?;
        let route = self.object_route()?;
        if route.backend != ObjectBackend::S3 {
            return Err(failure("object_eviction_requires_s3_route"));
        }
        let s3 = self.s3(route.target_id.as_deref())?;
        let refs = local_inventory(&self.root.join("objects"))?;
        for r in &refs {
            s3.open_verified(r)?;
        }
        for r in &refs {
            fs::remove_file(
                self.root
                    .join("objects")
                    .join(&r.sha256[..2])
                    .join(&r.sha256),
            )?;
        }
        Ok(refs.len())
    }
}
fn now_ns() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
fn local_inventory(root: &Path) -> Result<Vec<ObjectRef>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for shard in fs::read_dir(root)? {
        let shard = shard?;
        let name = shard.file_name().to_string_lossy().to_string();
        if name.len() != 2 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        if !fs::symlink_metadata(shard.path())?.is_dir() {
            return Err(failure("object_path_escape"));
        }
        for entry in fs::read_dir(shard.path())? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                return Err(failure("object_path_escape"));
            }
            let r = ObjectRef {
                sha256: entry.file_name().to_string_lossy().to_string(),
                bytes: metadata.len(),
            };
            crate::s3::validate_reference(&r)?;
            if !r.sha256.starts_with(&name) {
                return Err(failure("object_path_escape"));
            }
            result.push(r);
            if result.len() > 100000 {
                return Err(failure("object_inventory_budget_exceeded"));
            }
        }
    }
    result.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    Ok(result)
}
pub struct RoutedHistoryQuery {
    root: PathBuf,
    path: PathBuf,
}
impl RoutedHistoryQuery {
    pub fn new(root: PathBuf, path: PathBuf) -> Self {
        Self { root, path }
    }
}
impl HistoryQuery for RoutedHistoryQuery {
    fn page(&self, request: &HistoryRequest) -> Result<HistoryPage> {
        let path = history_query_path(&self.path);
        if !StorageControl::exists(&self.root) {
            return SqliteHistoryQuery::new(path.clone()).page(request);
        }
        let control = StorageControl::open(&self.root)?;
        let snapshot = ArchiveSnapshot::open(&path)?;
        let source_id = snapshot.metadata.store_id.clone();
        control.check_source_path(&fs::canonicalize(&path)?, &source_id)?;
        drop(snapshot);
        let exists: bool = control.read_db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM history_routes WHERE source_id=?1)",
            [&source_id],
            |r| r.get(0),
        )?;
        let route = if exists {
            control.history_route(&source_id)?
        } else {
            HistoryRoute {
                source_id,
                path: fs::canonicalize(&path)?,
                // Registering the initial local source is not a backend cutover.
                revision: 1,
                backend: HistoryBackend::Sqlite,
                target_id: None,
            }
        };
        if route.path != fs::canonicalize(&path)? {
            return Err(failure("history_source_path_changed"));
        }
        let prefix = format!("route:{}:{}:", control.control_id, route.revision);
        let mut input = request.clone();
        if let Some(cursor) = input.cursor.as_mut() {
            cursor.store_id = cursor
                .store_id
                .strip_prefix(&prefix)
                .ok_or_else(|| failure("history_cursor_scope_changed"))?
                .into();
        }
        let mut page = match route.backend {
            HistoryBackend::Sqlite => SqliteHistoryQuery::new(path.clone()).page(&input)?,
            HistoryBackend::Postgres => control
                .postgres(route.target_id.as_deref())?
                .query(&route.source_id)
                .page(&input)?,
        };
        if let Some(cursor) = page.next_cursor.as_mut() {
            cursor.store_id = format!("{prefix}{}", cursor.store_id);
        }
        page.recording["backend_revision"] = route.revision.to_string().into();
        page.recording["backend"] = serde_json::to_value(&route.backend)?;
        if route.backend == HistoryBackend::Postgres {
            let source = ArchiveSnapshot::open(&path)?;
            let local_upper = source.metadata.upper_row;
            let committed = page.recording["projection_committed_row"]
                .as_str()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            page.recording["local_durable_upper_row"] = local_upper.to_string().into();
            page.recording["projection_pending_rows_upper_bound"] =
                (local_upper - committed).max(0).to_string().into();
            let metadata_pending = page.recording["projection_metadata_sha256"].as_str()
                != Some(sha256(&serde_json::to_vec(&source.metadata)?).as_str());
            page.recording["projection_metadata_pending"] = metadata_pending.into();
            if committed < local_upper || metadata_pending {
                page.recording["received_segment_complete"] = false.into();
            }
        }
        Ok(page)
    }
}
struct RoutedObjectStore {
    root: PathBuf,
    revision: i64,
    inner: Box<dyn ObjectStore>,
    _control: Option<StorageControl>,
}
impl RoutedObjectStore {
    fn guard(&self) -> Result<File> {
        let guard = file_lock(&self.root.join(".publication.lock"), false)?;
        let revision = if StorageControl::exists(&self.root) {
            let current = StorageControl::open(&self.root)?;
            if self
                ._control
                .as_ref()
                .is_none_or(|old| old.control_id != current.control_id)
            {
                return Err(failure("object_backend_epoch_changed"));
            }
            current.object_route()?.revision
        } else {
            0
        };
        if revision != self.revision {
            return Err(failure("object_backend_epoch_changed"));
        }
        Ok(guard)
    }
}
impl ObjectStore for RoutedObjectStore {
    fn minimum_free_bytes(&self) -> u64 {
        self.inner.minimum_free_bytes()
    }
    fn put(&self, source: &mut dyn Read) -> Result<ObjectRef> {
        let _guard = self.guard()?;
        self.inner.put(source)
    }
    fn open_verified(&self, reference: &ObjectRef) -> Result<File> {
        let _guard = self.guard()?;
        self.inner.open_verified(reference)
    }
    fn publication_guard(&self) -> Result<Option<File>> {
        Ok(Some(self.guard()?))
    }
}
pub fn object_store(root: &Path) -> Result<Box<dyn ObjectStore>> {
    fs::create_dir_all(root)?;
    let (revision, inner, control): (i64, Box<dyn ObjectStore>, Option<StorageControl>) =
        if StorageControl::exists(root) {
            let control = StorageControl::open(root)?;
            let route = control.object_route()?;
            let inner: Box<dyn ObjectStore> = match route.backend {
                ObjectBackend::Filesystem => Box::new(LocalObjectStore::with_budget(
                    root.join("objects"),
                    control.profile.budget.clone(),
                )?),
                ObjectBackend::S3 => Box::new(control.s3(route.target_id.as_deref())?),
            };
            (route.revision, inner, Some(control))
        } else {
            (
                0,
                Box::new(LocalObjectStore::new(root.join("objects"))?),
                None,
            )
        };
    Ok(Box::new(RoutedObjectStore {
        root: root.into(),
        revision,
        inner,
        _control: control,
    }))
}

/// The projection thread has no recovery store, strategy handle or execution capability.
pub struct ProjectionWorker {
    stop: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl ProjectionWorker {
    pub fn start(root: PathBuf, account_source: Option<PathBuf>) -> Result<Option<Self>> {
        if !StorageControl::exists(&root) {
            return Ok(None);
        }
        let control = StorageControl::open(&root)?;
        if control.profile.postgres.is_none() {
            return Ok(None);
        }
        let owner = file_lock(&root.join(".projection-worker.lock"), true)?;
        let stop = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let shared = stop.clone();
        let thread=std::thread::Builder::new().name("history-projection".into()).spawn(move|| {
            let _owner=owner;let mut index=0;
            loop {
                if *shared.0.lock().unwrap_or_else(|e|e.into_inner()){break;}
                if let Some(path)=&account_source&&path.is_file(){let _=control.register_source(path);}
                if let Ok(sources)=control.sources()&&!sources.is_empty() {
                    index%=sources.len();let route=&sources[index];index+=1;
                    let result=control.postgres(route.target_id.as_deref()).and_then(|p|p.sync_source(&route.path,control.profile.budget.projection_batch_rows));
                    let (status,detail)=if result.is_ok(){("healthy","")}else{("failed","projection_unavailable")};
                    if let Ok(db)=control.db(){let _=db.execute("INSERT INTO storage_worker_health VALUES(?1,?2,?3,?4) ON CONFLICT(source_id) DO UPDATE SET status=excluded.status,detail=excluded.detail,updated_ns=excluded.updated_ns",params![route.source_id,status,detail,now_ns()]);}
                }
                let guard=shared.0.lock().unwrap_or_else(|e|e.into_inner());
                if *guard{break;}
                let _unused=shared.1.wait_timeout(guard,Duration::from_millis(control.profile.budget.projection_interval_ms));
            }
        })?;
        Ok(Some(Self {
            stop,
            thread: Some(thread),
        }))
    }
}
impl Drop for ProjectionWorker {
    fn drop(&mut self) {
        *self.stop.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.stop.1.notify_all();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Retry only SQLite lock/lifecycle contention on the cold path. Never retry I/O/unknown commits.
pub(crate) fn sqlite_cold_retry<T>(mut work: impl FnMut() -> Result<T>) -> Result<T> {
    let until = Instant::now() + Duration::from_secs(1);
    loop {
        match work() {
        Err(error) if error.downcast_ref::<rusqlite::Error>().is_some_and(|e|matches!(e,rusqlite::Error::SqliteFailure(code,_) if matches!(code.code,rusqlite::ffi::ErrorCode::DatabaseBusy|rusqlite::ffi::ErrorCode::DatabaseLocked)))&&Instant::now()<until=>std::thread::sleep(Duration::from_millis(5)),
        result=>return result,
    }
    }
}
