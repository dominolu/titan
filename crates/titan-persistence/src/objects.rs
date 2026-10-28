use crate::{Result, failure, sha256};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub sha256: String,
    #[serde(with = "crate::facts::u64_text")]
    pub bytes: u64,
}
pub trait ObjectStore: Send + Sync {
    fn put(&self, source: &mut dyn Read) -> Result<ObjectRef>;
    fn open_verified(&self, reference: &ObjectRef) -> Result<File>;
    fn publication_guard(&self) -> Result<Option<File>> {
        Ok(None)
    }
    fn put_file(&self, path: &Path) -> Result<ObjectRef> {
        self.put(&mut File::open(path)?)
    }
    fn put_bytes(&self, bytes: &[u8]) -> Result<ObjectRef> {
        {
            let mut source = bytes;
            self.put(&mut source)
        }
    }
    fn minimum_free_bytes(&self) -> u64 {
        crate::StorageBudget::default().min_free_bytes
    }
    fn materialize(&self, r: &ObjectRef, destination: &Path) -> Result<()> {
        let parent = destination
            .parent()
            .ok_or_else(|| failure("object_destination_invalid"))?;
        fs::create_dir_all(parent)?;
        let mut source = self.open_verified(r)?;
        let min = self.minimum_free_bytes();
        crate::profile::check_free_space(parent, min.saturating_add(r.bytes))?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        let mut hash = Sha256::new();
        let mut bytes = 0_u64;
        let mut chunk = [0; 65536];
        loop {
            let n = source.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            bytes = bytes
                .checked_add(n as u64)
                .ok_or_else(|| failure("object_size_overflow"))?;
            if bytes > r.bytes {
                return Err(failure("object_size_or_type_changed"));
            }
            crate::profile::check_stream_space(parent, min, bytes, n)?;
            temp.write_all(&chunk[..n])?;
            hash.update(&chunk[..n]);
        }
        if bytes != r.bytes || format!("{:x}", hash.finalize()) != r.sha256 {
            return Err(failure("object_digest_changed"));
        }
        temp.as_file().sync_all()?;
        temp.persist_noclobber(destination).map_err(|e| e.error)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}
pub struct LocalObjectStore {
    root: PathBuf,
    budget: crate::StorageBudget,
}
impl LocalObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: fs::canonicalize(root)?,
            budget: crate::StorageBudget::default(),
        })
    }
    pub fn with_budget(root: impl AsRef<Path>, budget: crate::StorageBudget) -> Result<Self> {
        let mut store = Self::new(root)?;
        store.budget = budget;
        Ok(store)
    }
    fn path(&self, r: &ObjectRef) -> Result<PathBuf> {
        if r.sha256.len() != 64
            || !r
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(failure("object_reference_invalid"));
        }
        let directory = self.root.join(&r.sha256[..2]);
        fs::create_dir_all(&directory)?;
        let canonical = fs::canonicalize(&directory)?;
        if !canonical.starts_with(&self.root) {
            return Err(failure("object_path_escape"));
        }
        Ok(canonical.join(&r.sha256))
    }
    pub fn put_bytes(&self, bytes: &[u8]) -> Result<ObjectRef> {
        let mut source = bytes;
        self.put(&mut source)
    }
    pub fn put_file(&self, path: &Path) -> Result<ObjectRef> {
        self.put(&mut File::open(path)?)
    }
    pub fn materialize(&self, r: &ObjectRef, destination: &Path) -> Result<()> {
        <Self as ObjectStore>::materialize(self, r, destination)
    }
}
impl ObjectStore for LocalObjectStore {
    fn minimum_free_bytes(&self) -> u64 {
        self.budget.min_free_bytes
    }
    fn put(&self, source: &mut dyn Read) -> Result<ObjectRef> {
        let _quota = crate::routing::file_lock(&self.root.join(".quota.lock"), true)?;
        crate::profile::check_free_space(&self.root, self.budget.min_free_bytes)?;
        let used = crate::routing::directory_bytes(&self.root)?;
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root)?;
        let mut hash = Sha256::new();
        let mut bytes = 0_u64;
        let mut chunk = [0; 65536];
        loop {
            let count = source.read(&mut chunk)?;
            if count == 0 {
                break;
            }
            crate::profile::check_stream_space(
                &self.root,
                self.budget.min_free_bytes,
                bytes.saturating_add(count as u64),
                count,
            )?;
            temporary.write_all(&chunk[..count])?;
            hash.update(&chunk[..count]);
            bytes = bytes
                .checked_add(count as u64)
                .ok_or_else(|| failure("object_size_overflow"))?;
            if bytes > self.budget.max_object_bytes
                || used.saturating_add(bytes) > self.budget.max_local_object_bytes
            {
                return Err(failure("local_object_quota_exhausted"));
            }
        }
        let reference = ObjectRef {
            sha256: format!("{:x}", hash.finalize()),
            bytes,
        };
        let path = self.path(&reference)?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => {}
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.open_verified(&reference)?;
            }
            Err(e) => return Err(e.error.into()),
        }
        File::open(path.parent().unwrap())?.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(reference)
    }
    fn open_verified(&self, reference: &ObjectRef) -> Result<File> {
        let path = self.path(reference)?;
        let meta = fs::symlink_metadata(&path)?;
        if !meta.is_file() || meta.len() != reference.bytes {
            return Err(failure("object_size_or_type_changed"));
        }
        let mut file = File::open(&path)?;
        let mut hash = Sha256::new();
        let mut chunk = [0; 65536];
        loop {
            let n = file.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            hash.update(&chunk[..n]);
        }
        if format!("{:x}", hash.finalize()) != reference.sha256 {
            return Err(failure("object_digest_changed"));
        }
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishedFile {
    pub path: String,
    pub object: ObjectRef,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedManifest {
    pub schema_version: u32,
    pub logical_id: String,
    pub kind: String,
    pub files: Vec<PublishedFile>,
    pub metadata: serde_json::Value,
}
/// Publication index. Retrying registration never repeats engine work.
pub struct ResultCatalog {
    db: std::sync::Mutex<rusqlite::Connection>,
}
impl ResultCatalog {
    pub fn open(path: &Path) -> Result<Self> {
        crate::routing::sqlite_cold_retry(|| Self::open_once(path))
    }
    fn open_once(path: &Path) -> Result<Self> {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p)?;
        }
        let db = rusqlite::Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_millis(1000))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let wrong_role:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name IN ('recovery_points','archive_records','facts','resources','storage_control_meta'))",[],|r|r.get(0))?;
        if wrong_role {
            return Err(failure("object_catalog_role_conflict"));
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS object_catalog(logical_id TEXT PRIMARY KEY,manifest_sha256 TEXT NOT NULL,manifest_bytes INTEGER NOT NULL,kind TEXT NOT NULL);")?;
        Ok(Self {
            db: std::sync::Mutex::new(db),
        })
    }
    pub fn entries(path: &Path) -> Result<Vec<(String, String, ObjectRef)>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let db = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        db.busy_timeout(std::time::Duration::from_millis(200))?;
        let mut q=db.prepare("SELECT logical_id,kind,manifest_sha256,manifest_bytes FROM object_catalog ORDER BY logical_id LIMIT 100001")?;
        let rows = q
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    ObjectRef {
                        sha256: r.get(2)?,
                        bytes: r.get(3)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.len() > 100000 {
            return Err(failure("object_catalog_budget_exceeded"));
        }
        Ok(rows)
    }
    pub fn lookup(path: &Path, id: &str) -> Result<ObjectRef> {
        use rusqlite::OptionalExtension;
        let db = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        db.busy_timeout(std::time::Duration::from_millis(200))?;
        db.query_row(
            "SELECT manifest_sha256,manifest_bytes FROM object_catalog WHERE logical_id=?1",
            [id],
            |r| {
                Ok(ObjectRef {
                    sha256: r.get(0)?,
                    bytes: r.get(1)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| failure("publication_not_registered"))
    }
    pub fn register(&self, id: &str, kind: &str, manifest: &ObjectRef) -> Result<()> {
        crate::routing::sqlite_cold_retry(|| self.register_once(id, kind, manifest))
    }
    fn register_once(&self, id: &str, kind: &str, manifest: &ObjectRef) -> Result<()> {
        use rusqlite::{OptionalExtension, params};
        let mut db = self
            .db
            .lock()
            .map_err(|_| failure("result_catalog_poisoned"))?;
        let db = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old:Option<(String,u64,String)>=db.query_row("SELECT manifest_sha256,manifest_bytes,kind FROM object_catalog WHERE logical_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some(old) = old {
            if old != (manifest.sha256.clone(), manifest.bytes, kind.into()) {
                return Err(failure("result_publication_conflict"));
            }
            return Ok(());
        }
        db.execute(
            "INSERT INTO object_catalog VALUES(?1,?2,?3,?4)",
            params![id, manifest.sha256, manifest.bytes, kind],
        )?;
        db.commit()?;
        Ok(())
    }
}
impl PublishedManifest {
    pub fn load(store: &dyn ObjectStore, reference: &ObjectRef) -> Result<Self> {
        if reference.bytes > 4 * 1024 * 1024 {
            return Err(failure("publication_manifest_too_large"));
        }
        let mut bytes = Vec::new();
        store
            .open_verified(reference)?
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        let manifest: Self = serde_json::from_slice(&bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }
    pub fn verify_objects(&self, store: &dyn ObjectStore) -> Result<()> {
        self.validate()?;
        for file in &self.files {
            store.open_verified(&file.object)?;
        }
        if self.kind == "result_bundle_v1" {
            let original: ObjectRef =
                serde_json::from_value(self.metadata["original_manifest"].clone())?;
            if original.bytes > 4 * 1024 * 1024 {
                return Err(failure("bundle_manifest_too_large"));
            }
            let mut bytes = Vec::new();
            store
                .open_verified(&original)?
                .take(4 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            let files = value["files"]
                .as_array()
                .ok_or_else(|| failure("bundle_files_missing"))?;
            if value["schema_version"] != 1
                || value["run_id"] != self.logical_id
                || files.len() != self.files.len()
                || self.files.iter().any(|f| {
                    !files.iter().any(|v| {
                        v["path"] == f.path
                            && v["sha256"] == f.object.sha256
                            && v["bytes"].as_u64() == Some(f.object.bytes)
                    })
                })
            {
                return Err(failure("publication_files_changed"));
            }
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || self.logical_id.is_empty()
            || self.logical_id.len() > 256
            || self.files.is_empty()
            || self.files.len() > 4096
        {
            return Err(failure("publication_manifest_invalid"));
        }
        let mut names = std::collections::BTreeSet::new();
        for f in &self.files {
            let path = Path::new(&f.path);
            if f.path.is_empty()
                || path.is_absolute()
                || !path
                    .components()
                    .all(|p| matches!(p, std::path::Component::Normal(_)))
                || !names.insert(&f.path)
            {
                return Err(failure("publication_path_invalid"));
            }
        }
        Ok(())
    }
    pub fn publish(&self, store: &dyn ObjectStore, catalog: &ResultCatalog) -> Result<ObjectRef> {
        let _guard = store.publication_guard()?;
        self.validate()?;
        self.verify_objects(store)?;
        let bytes = serde_json::to_vec(self)?;
        let reference = store.put(&mut bytes.as_slice())?;
        catalog.register(&self.logical_id, &self.kind, &reference)?;
        Ok(reference)
    }
}
pub fn publish_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| failure("publication_destination_invalid"))?;
    fs::create_dir_all(parent)?;
    if path.exists() {
        if sha256(&fs::read(path)?) == sha256(bytes) {
            return Ok(());
        }
        return Err(failure("publication_target_exists"));
    }
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(path) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            if sha256(&fs::read(path)?) != sha256(bytes) {
                return Err(failure("publication_target_exists"));
            }
        }
        Err(e) => return Err(e.error.into()),
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Rehydrate one allow-listed host file into its fixed operation directory.
pub fn materialize_published_file(
    root: &Path,
    id: &str,
    name: &str,
    destination: &Path,
) -> Result<()> {
    let reference = ResultCatalog::lookup(&root.join("catalog.db"), id)?;
    let store = crate::object_store(root)?;
    let _guard = store.publication_guard()?;
    let manifest = PublishedManifest::load(store.as_ref(), &reference)?;
    if manifest.logical_id != id {
        return Err(failure("publication_catalog_identity_changed"));
    }
    manifest.verify_objects(store.as_ref())?;
    let file = manifest
        .files
        .iter()
        .find(|f| f.path == name)
        .ok_or_else(|| failure("publication_file_missing"))?;
    store.materialize(&file.object, destination)
}
pub fn publish_host_files(
    root: &Path,
    id: &str,
    kind: &str,
    directory: &Path,
    names: &[&str],
) -> Result<ObjectRef> {
    let store = crate::object_store(root)?;
    let _guard = store.publication_guard()?;
    let mut files = Vec::new();
    for name in names {
        files.push(PublishedFile {
            path: (*name).into(),
            object: store.put_file(&directory.join(name))?,
        });
    }
    let manifest = PublishedManifest {
        schema_version: 1,
        logical_id: id.into(),
        kind: kind.into(),
        files,
        metadata: serde_json::json!({}),
    };
    publish_file(
        &directory.join("publication.pending.json"),
        &serde_json::to_vec(&manifest)?,
    )?;
    manifest.publish(
        store.as_ref(),
        &ResultCatalog::open(&root.join("catalog.db"))?,
    )
}
