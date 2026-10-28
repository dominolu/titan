use crate::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceState {
    pub recording: String,
    pub environment: String,
    pub clean: bool,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceScope {
    pub recording: String,
    pub environment: String,
    pub account: String,
    pub run: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceGap {
    pub recording: String,
    pub account: String,
    pub code: String,
    pub detail: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceWatermark {
    pub recording: String,
    pub stream: String,
    pub epoch: String,
    pub sequence: Vec<u8>,
    pub row_id: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceConflict {
    pub id: i64,
    pub recording: String,
    pub account: String,
    pub code: String,
    pub original_row: Option<i64>,
    pub payload: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiveMetadata {
    pub schema_version: u32,
    pub store_id: String,
    pub upper_row: i64,
    pub pruned_through: i64,
    pub sources: Vec<SourceState>,
    pub scopes: Vec<SourceScope>,
    pub gaps: Vec<SourceGap>,
    pub watermarks: Vec<SourceWatermark>,
    pub conflicts: Vec<SourceConflict>,
}
#[derive(Clone, Debug)]
pub struct ArchiveRow {
    pub id: i64,
    pub payload: Vec<u8>,
    pub sha256: String,
    pub replay: bool,
}
impl ArchiveRow {
    pub fn fact(&self) -> Result<FactEnvelope> {
        if self.id <= 0
            || self.payload.len() > MAX_FACT_BYTES
            || sha256(&self.payload) != self.sha256
        {
            return Err(failure("projection_source_payload_corrupt"));
        }
        let fact: FactEnvelope = serde_json::from_slice(&self.payload)?;
        fact.validate()?;
        Ok(fact)
    }
    pub(crate) fn hash(&self, hash: &mut Sha256) {
        hash.update(self.id.to_be_bytes());
        hash.update([u8::from(self.replay)]);
        hash.update((self.payload.len() as u64).to_be_bytes());
        hash.update(&self.payload);
    }
}
pub struct ArchiveSnapshot {
    db: Connection,
    pub metadata: ArchiveMetadata,
    pub path: PathBuf,
}
impl ArchiveSnapshot {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(std::time::Duration::from_millis(200))?;
        db.execute_batch("BEGIN")?;
        let (version, store_id): (u32, String) = db.query_row(
            "SELECT schema_version,store_id FROM archive_meta WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if version != FACT_SCHEMA {
            return Err(failure("history_schema_unsupported"));
        }
        let upper_row: i64 =
            db.query_row("SELECT coalesce(max(id),0) FROM archive_records", [], |r| {
                r.get(0)
            })?;
        let counter: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_sequence')",
            [],
            |r| r.get(0),
        )?;
        let upper_row = if counter {
            upper_row.max(db.query_row(
                "SELECT last_row FROM archive_sequence WHERE id=1",
                [],
                |r| r.get(0),
            )?)
        } else {
            upper_row
        };
        let pruned: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_retention')",
            [],
            |r| r.get(0),
        )?;
        let pruned_through = if pruned {
            db.query_row(
                "SELECT coalesce(max(through_row),0) FROM archive_retention",
                [],
                |r| r.get(0),
            )?
        } else {
            0
        };
        let sources = {
            let mut s=db.prepare("SELECT recording,environment,clean,complete FROM archive_sources ORDER BY recording LIMIT 4097")?;
            s.query_map([], |r| {
                Ok(SourceState {
                    recording: r.get(0)?,
                    environment: r.get(1)?,
                    clean: r.get(2)?,
                    complete: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let scopes = {
            let mut s=db.prepare("SELECT recording,environment,account,run FROM archive_source_scopes ORDER BY recording,environment,account,run LIMIT 16385")?;
            s.query_map([], |r| {
                Ok(SourceScope {
                    recording: r.get(0)?,
                    environment: r.get(1)?,
                    account: r.get(2)?,
                    run: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let gaps = {
            let mut s=db.prepare("SELECT recording,account,code,detail FROM archive_gaps ORDER BY recording,account,code LIMIT 4097")?;
            s.query_map([], |r| {
                Ok(SourceGap {
                    recording: r.get(0)?,
                    account: r.get(1)?,
                    code: r.get(2)?,
                    detail: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let watermarks = {
            let mut s=db.prepare("SELECT recording,stream,epoch,sequence,row_id FROM archive_watermarks ORDER BY recording,stream,epoch LIMIT 16385")?;
            s.query_map([], |r| {
                Ok(SourceWatermark {
                    recording: r.get(0)?,
                    stream: r.get(1)?,
                    epoch: r.get(2)?,
                    sequence: r.get(3)?,
                    row_id: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut conflicts = Vec::new();
        let mut conflict_bytes = 0;
        {
            let mut s=db.prepare("SELECT id,recording,account,code,original_row,new_payload FROM archive_conflicts ORDER BY id LIMIT 4097")?;
            let rows = s.query_map([], |r| {
                Ok(SourceConflict {
                    id: r.get(0)?,
                    recording: r.get(1)?,
                    account: r.get(2)?,
                    code: r.get(3)?,
                    original_row: r.get(4)?,
                    payload: r.get(5)?,
                })
            })?;
            for row in rows {
                let row = row?;
                conflict_bytes += row.payload.len();
                if conflict_bytes > 8 * MAX_FACT_BYTES {
                    return Err(failure("projection_metadata_budget_exceeded"));
                }
                conflicts.push(row);
            }
        }
        if sources.len() > 4096
            || scopes.len() > 16384
            || gaps.len() > 4096
            || watermarks.len() > 16384
            || conflicts.len() > 4096
        {
            return Err(failure("projection_metadata_budget_exceeded"));
        }
        let metadata = ArchiveMetadata {
            schema_version: version,
            store_id,
            upper_row,
            pruned_through,
            sources,
            scopes,
            gaps,
            watermarks,
            conflicts,
        };
        if serde_json::to_vec(&metadata)?.len() > 16 * MAX_FACT_BYTES {
            return Err(failure("projection_metadata_budget_exceeded"));
        }
        Ok(Self {
            db,
            metadata,
            path: path.into(),
        })
    }
    pub fn rows(&self, after: i64, through: i64, limit: usize) -> Result<Vec<ArchiveRow>> {
        if after < 0 || through > self.metadata.upper_row || limit == 0 || limit > MAX_BATCH {
            return Err(failure("projection_range_invalid"));
        }
        let mut s=self.db.prepare("SELECT id,payload,sha256,replay,recording,stream,epoch,sequence,environment,account,strategy,run,kind FROM archive_records WHERE id>?1 AND id<=?2 ORDER BY id LIMIT ?3")?;
        let mut query = s.query(params![after, through, limit])?;
        let mut items = Vec::new();
        let mut bytes = 0;
        while let Some(row) = query.next()? {
            let item = ArchiveRow {
                id: row.get(0)?,
                payload: row.get(1)?,
                sha256: row.get(2)?,
                replay: row.get(3)?,
            };
            if !items.is_empty() && bytes + item.payload.len() > 4 * MAX_FACT_BYTES {
                break;
            }
            let fact = item.fact()?;
            if row.get::<_, String>(4)? != fact.recording_id
                || row.get::<_, String>(5)? != fact.stream_id
                || row.get::<_, String>(6)? != fact.source_epoch
                || row.get::<_, Vec<u8>>(7)? != fact.source_sequence.to_be_bytes()
                || row.get::<_, String>(8)? != fact.scope.environment.as_str()
                || row.get::<_, String>(9)? != fact.scope.account
                || row.get::<_, Option<String>>(10)? != fact.scope.strategy_id
                || row.get::<_, Option<String>>(11)? != fact.scope.run_id
                || row.get::<_, String>(12)? != fact.kind
            {
                return Err(failure("history_scope_metadata_corrupt"));
            }
            bytes += item.payload.len();
            items.push(item);
        }
        Ok(items)
    }
    pub fn digest(&self, through: i64) -> Result<RangeDigest> {
        let mut hash = Sha256::new();
        let mut count = 0;
        let mut after = 0;
        loop {
            let rows = self.rows(after, through, MAX_BATCH)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                after = row.id;
                row.hash(&mut hash);
                count += 1;
            }
        }
        Ok(RangeDigest {
            through_row: through.to_string(),
            records: count.to_string(),
            sha256: format!("{:x}", hash.finalize()),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigest {
    pub through_row: String,
    pub records: String,
    pub sha256: String,
}
pub(crate) fn restore_row(db: &Connection, row: &ArchiveRow) -> Result<()> {
    let f = row.fact()?;
    let existing: Option<(Vec<u8>, String, bool)> = db
        .query_row(
            "SELECT payload,sha256,replay FROM archive_records WHERE id=?1",
            [row.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    if let Some(old) = existing {
        if old != (row.payload.clone(), row.sha256.clone(), row.replay) {
            return Err(failure("history_restore_conflict"));
        }
        return Ok(());
    }
    db.execute("INSERT INTO archive_records(id,recording,stream,epoch,sequence,environment,account,strategy,run,kind,payload,sha256,replay) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![row.id,f.recording_id,f.stream_id,f.source_epoch,f.source_sequence.to_be_bytes().as_slice(),f.scope.environment.as_str(),f.scope.account,f.scope.strategy_id,f.scope.run_id,f.kind,row.payload,row.sha256,row.replay])?;
    if let (Some(key), Some(digest)) = (&f.business_key, &f.business_sha256) {
        let scope = serde_json::to_string(&(
            f.scope.environment.as_str(),
            &f.scope.account,
            if matches!(f.scope.environment, FactEnvironment::Backtest) {
                f.scope.run_id.as_deref()
            } else {
                None
            },
        ))?;
        db.execute(
            "INSERT OR IGNORE INTO archive_business VALUES(?1,?2,?3,?4)",
            params![scope, key, digest, row.id],
        )?;
    }
    db.execute(
        "UPDATE archive_sequence SET last_row=max(last_row,?1) WHERE id=1",
        [row.id],
    )?;
    Ok(())
}
pub(crate) fn restore_metadata(db: &Connection, meta: &ArchiveMetadata) -> Result<()> {
    for s in &meta.sources {
        db.execute(
            "INSERT OR IGNORE INTO archive_sources VALUES(?1,?2,?3,?4)",
            params![s.recording, s.environment, s.clean, s.complete],
        )?;
    }
    for s in &meta.scopes {
        db.execute(
            "INSERT OR IGNORE INTO archive_source_scopes VALUES(?1,?2,?3,?4)",
            params![s.recording, s.environment, s.account, s.run],
        )?;
    }
    for s in &meta.gaps {
        db.execute(
            "INSERT OR IGNORE INTO archive_gaps VALUES(?1,?2,?3,?4)",
            params![s.recording, s.account, s.code, s.detail],
        )?;
    }
    for s in &meta.watermarks {
        db.execute("INSERT INTO archive_watermarks VALUES(?1,?2,?3,?4,?5) ON CONFLICT(recording,stream,epoch) DO UPDATE SET sequence=max(sequence,excluded.sequence),row_id=max(row_id,excluded.row_id)",params![s.recording,s.stream,s.epoch,s.sequence,s.row_id])?;
    }
    for s in &meta.conflicts {
        db.execute("INSERT INTO archive_conflicts(id,recording,account,code,original_row,new_payload) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(id) DO NOTHING",params![s.id,s.recording,s.account,s.code,s.original_row,s.payload])?;
    }
    Ok(())
}

/// Published v1 files remain byte-immutable. Projection retention uses a consistent working copy.
pub fn history_query_path(path: &Path) -> std::path::PathBuf {
    let working = path.with_file_name("history-source.db");
    if path.file_name().is_some_and(|n| n == "history.db") && working.is_file() {
        working
    } else {
        path.to_owned()
    }
}
pub(crate) fn projectable_history_path(path: &Path) -> Result<std::path::PathBuf> {
    let effective = history_query_path(path);
    if effective != path {
        return Ok(effective);
    }
    if path.file_name().is_none_or(|n| n != "history.db") {
        return Ok(effective);
    }
    let parent = path
        .parent()
        .ok_or_else(|| failure("history_path_invalid"))?;
    if !parent.join("manifest.json").is_file() && !parent.join("manifest.pending.json").is_file() {
        return Ok(effective);
    }
    let snapshot = ArchiveSnapshot::open(path)?;
    if snapshot
        .metadata
        .sources
        .iter()
        .any(|s| s.environment != "backtest")
    {
        return Ok(effective);
    }
    let source = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let account: bool = source.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_account_role')",
        [],
        |r| r.get(0),
    )?;
    if account {
        return Ok(effective);
    }
    let pages: u64 = source.pragma_query_value(None, "page_count", |r| r.get(0))?;
    let page_size: u64 = source.pragma_query_value(None, "page_size", |r| r.get(0))?;
    crate::profile::check_free_space(
        parent,
        crate::budget_for_history(path)?
            .min_free_bytes
            .saturating_add(pages.saturating_mul(page_size)),
    )?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut target = Connection::open(temporary.path())?;
    target.pragma_update(None, "synchronous", "FULL")?;
    {
        let backup = rusqlite::backup::Backup::new(&source, &mut target)?;
        backup.run_to_completion(128, std::time::Duration::from_millis(5), None)?;
    }
    target.pragma_update(None, "journal_mode", "WAL")?;
    drop(target);
    temporary.as_file().sync_all()?;
    let destination = parent.join("history-source.db");
    match temporary.persist_noclobber(&destination) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }
    std::fs::File::open(parent)?.sync_all()?;
    if ArchiveSnapshot::open(&destination)?.metadata.store_id != snapshot.metadata.store_id {
        return Err(failure("history_working_source_identity_changed"));
    }
    Ok(destination)
}
