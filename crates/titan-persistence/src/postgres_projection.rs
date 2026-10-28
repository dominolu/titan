use crate::profile::postgres_error;
use crate::*;
use futures_util::{TryStreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio_postgres::{Client, IsolationLevel, types::ToSql};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectionProgress {
    pub target_id: String,
    pub source_id: String,
    pub committed_row: String,
    pub observed_upper_row: String,
    pub copied_rows: usize,
    pub metadata_sha256: String,
}
#[derive(Clone)]
pub struct PostgresProjection {
    config: PostgresConfig,
    pub target_id: String,
}
impl PostgresProjection {
    pub fn attach(config: PostgresConfig, target_id: String) -> Result<Self> {
        let result = Self { config, target_id };
        result.with_client(|_| Box::pin(async { Ok(()) }))?;
        Ok(result)
    }
    pub fn open(config: PostgresConfig) -> Result<Self> {
        let settings = config.clone();
        execute(&settings, None, move |db| {
            Box::pin(async move {
                let tx = db.transaction().await.map_err(postgres_error)?;
                tx.batch_execute("SELECT pg_advisory_xact_lock(727105934072); CREATE TABLE IF NOT EXISTS titan_persistence_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL,store_id TEXT NOT NULL);").await.map_err(postgres_error)?;
                tx.execute(
                    "INSERT INTO titan_persistence_meta VALUES(1,1,$1) ON CONFLICT(id) DO NOTHING",
                    &[&uuid::Uuid::new_v4().to_string()],
                )
                .await
                .map_err(postgres_error)?;
                let row = tx
                    .query_one(
                        "SELECT schema_version,store_id FROM titan_persistence_meta WHERE id=1",
                        &[],
                    )
                    .await
                    .map_err(postgres_error)?;
                if row.get::<_, i32>(0) != 1 {
                    return Err(failure("postgres_schema_unsupported"));
                }
                let target_id: String = row.get(1);
                tx.batch_execute("CREATE TABLE IF NOT EXISTS titan_projection_sources(source_id TEXT PRIMARY KEY,committed_row BIGINT NOT NULL,observed_upper BIGINT NOT NULL,metadata BYTEA NOT NULL,metadata_sha256 TEXT NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
            CREATE TABLE IF NOT EXISTS titan_fact_records(source_id TEXT NOT NULL,row_id BIGINT NOT NULL,recording TEXT NOT NULL,stream TEXT NOT NULL,epoch TEXT NOT NULL,sequence BYTEA NOT NULL,environment TEXT NOT NULL,account TEXT NOT NULL,strategy TEXT,run TEXT,kind TEXT NOT NULL,payload BYTEA NOT NULL,sha256 TEXT NOT NULL,replay BOOLEAN NOT NULL,PRIMARY KEY(source_id,row_id),UNIQUE(source_id,recording,stream,epoch,sequence));
            CREATE INDEX IF NOT EXISTS titan_fact_query ON titan_fact_records(source_id,environment,account,kind,row_id);
            CREATE INDEX IF NOT EXISTS titan_fact_run_query ON titan_fact_records(source_id,environment,account,run,row_id);").await.map_err(postgres_error)?;
                tx.commit().await.map_err(postgres_error)?;
                Ok(Self { config, target_id })
            })
        })
    }
    fn with_client<T>(
        &self,
        work: impl for<'a> FnOnce(&'a mut Client) -> PgFuture<'a, T>,
    ) -> Result<T> {
        execute(&self.config, Some(&self.target_id), work)
    }

    pub fn progress(&self, source: &str) -> Result<Option<ProjectionProgress>> {
        let source = source.to_owned();
        let target_id = self.target_id.clone();
        self.with_client(move|db|Box::pin(async move {

        Ok(db.query_opt("SELECT committed_row,observed_upper,metadata_sha256 FROM titan_projection_sources WHERE source_id=$1",&[&source]).await.map_err(postgres_error)?.map(|r|ProjectionProgress{target_id:target_id.clone(),source_id:source,committed_row:r.get::<_,i64>(0).to_string(),observed_upper_row:r.get::<_,i64>(1).to_string(),copied_rows:0,metadata_sha256:r.get(2)}))
            }))
    }
    pub fn sync_source(&self, path: &Path, batch: usize) -> Result<ProjectionProgress> {
        let snapshot = ArchiveSnapshot::open(path)?;
        let target_id = self.target_id.clone();
        self.with_client(move|db|Box::pin(async move {
        if batch == 0 || batch > MAX_BATCH {
            return Err(failure("projection_batch_invalid"));
        }

        let meta = &snapshot.metadata;
        let bytes = serde_json::to_vec(meta)?;
        let digest = sha256(&bytes);

        let tx = db.transaction().await.map_err(postgres_error)?;
        tx.execute("INSERT INTO titan_projection_sources(source_id,committed_row,observed_upper,metadata,metadata_sha256) VALUES($1,0,0,$2,$3) ON CONFLICT(source_id) DO NOTHING",&[&meta.store_id,&bytes,&digest]).await.map_err(postgres_error)?;
        let prior = tx
            .query_one(
                "SELECT committed_row FROM titan_projection_sources WHERE source_id=$1 FOR UPDATE",
                &[&meta.store_id],
            )
            .await.map_err(postgres_error)?
            .get::<_, i64>(0);
        if prior > meta.upper_row {
            return Err(failure("projection_source_regressed"));
        }
        if prior < meta.pruned_through {
            return Err(failure("projection_source_history_missing"));
        }
        let rows = snapshot.rows(prior, meta.upper_row, batch)?;
        let mut through = prior;
        let statement=tx.prepare("INSERT INTO titan_fact_records VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(source_id,row_id) DO NOTHING").await.map_err(postgres_error)?;
        for row in &rows {
            let f = row.fact()?;
            tx.execute(
                &statement,
                &[
                    &meta.store_id,
                    &row.id,
                    &f.recording_id,
                    &f.stream_id,
                    &f.source_epoch,
                    &&f.source_sequence.to_be_bytes()[..],
                    &f.scope.environment.as_str(),
                    &f.scope.account,
                    &f.scope.strategy_id,
                    &f.scope.run_id,
                    &f.kind,
                    &row.payload,
                    &row.sha256,
                    &row.replay,
                ],
            )
            .await.map_err(postgres_error)?;
            let stored=tx.query_one("SELECT sha256,payload,replay FROM titan_fact_records WHERE source_id=$1 AND row_id=$2",&[&meta.store_id,&row.id]).await.map_err(postgres_error)?;
            if stored.get::<_, String>(0) != row.sha256
                || stored.get::<_, Vec<u8>>(1) != row.payload
                || stored.get::<_, bool>(2) != row.replay
            {
                return Err(failure("projection_record_conflict"));
            }
            through = row.id;
        }
        if rows.is_empty() && prior < meta.upper_row {
            return Err(failure("projection_source_history_missing"));
        }
        tx.execute("UPDATE titan_projection_sources SET committed_row=$2,observed_upper=$3,metadata=$4,metadata_sha256=$5,updated_at=clock_timestamp() WHERE source_id=$1",&[&meta.store_id,&through,&meta.upper_row,&bytes,&digest]).await.map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(ProjectionProgress {
            target_id: target_id.clone(),
            source_id: meta.store_id.clone(),
            committed_row: through.to_string(),
            observed_upper_row: meta.upper_row.to_string(),
            copied_rows: rows.len(),
            metadata_sha256: digest,
        })
            }))
    }
    pub fn query(&self, source: &str) -> PostgresHistoryQuery {
        PostgresHistoryQuery {
            projection: self.clone(),
            source: source.into(),
        }
    }
    pub fn rows(
        &self,
        source: &str,
        after: i64,
        through: i64,
        limit: usize,
    ) -> Result<Vec<ArchiveRow>> {
        let source = source.to_owned();
        self.with_client(move|db|Box::pin(async move {
        if after < 0 || through < after || limit == 0 || limit > MAX_BATCH {
            return Err(failure("projection_range_invalid"));
        }

        let limit = limit as i64;
        let args: [&(dyn ToSql + Sync); 4] = [&source, &after, &through, &limit];
        let rows=db.query_raw("SELECT row_id,payload,sha256,replay FROM titan_fact_records WHERE source_id=$1 AND row_id>$2 AND row_id<=$3 ORDER BY row_id LIMIT $4",args).await.map_err(postgres_error)?;
        pin_mut!(rows);
        let mut result = Vec::new();
        let mut size = 0;
        while let Some(row) = rows.try_next().await.map_err(postgres_error)? {
            let r = ArchiveRow {
                id: row.get(0),
                payload: row.get(1),
                sha256: row.get(2),
                replay: row.get(3),
            };
            if !result.is_empty() && size + r.payload.len() > 4 * MAX_FACT_BYTES {
                break;
            }
            r.fact()?;
            size += r.payload.len();
            result.push(r);
        }
        Ok(result)
            }))
    }
    pub fn metadata(&self, source: &str) -> Result<ArchiveMetadata> {
        let source = source.to_owned();
        self.with_client(move |db| {
            Box::pin(async move {
                let row = db
            .query_opt(
                "SELECT metadata,metadata_sha256 FROM titan_projection_sources WHERE source_id=$1",
                &[&source],
            )
            .await.map_err(postgres_error)?
            .ok_or_else(|| failure("projection_source_unavailable"))?;
                let bytes: Vec<u8> = row.get(0);
                if bytes.len() > 16 * MAX_FACT_BYTES || sha256(&bytes) != row.get::<_, String>(1) {
                    return Err(failure("projection_metadata_corrupt"));
                }
                let m: ArchiveMetadata = serde_json::from_slice(&bytes)?;
                if m.schema_version != FACT_SCHEMA || m.store_id != source {
                    return Err(failure("projection_metadata_corrupt"));
                }
                Ok(m)
            })
        })
    }
    pub fn digest(&self, source: &str, through: i64) -> Result<RangeDigest> {
        let mut hash = Sha256::new();
        let mut after = 0;
        let mut count = 0;
        loop {
            let rows = self.rows(source, after, through, MAX_BATCH)?;
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
    pub fn restore_source(&self, path: &Path, source: &str, through: i64) -> Result<RangeDigest> {
        let metadata = self.metadata(source)?;
        let committed = self
            .progress(source)?
            .ok_or_else(|| failure("projection_source_unavailable"))?
            .committed_row
            .parse::<i64>()?;
        if through > committed || through < 0 {
            return Err(failure("projection_range_not_durable"));
        }
        let mut db = rusqlite::Connection::open(path)?;
        crate::initialize_archive(&db)?;
        let local: String =
            db.query_row("SELECT store_id FROM archive_meta WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if local != source {
            return Err(failure("history_restore_store_identity_mismatch"));
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let mut after = 0;
        loop {
            let rows = self.rows(source, after, through, MAX_BATCH)?;
            if rows.is_empty() {
                break;
            }
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            for row in &rows {
                crate::snapshot::restore_row(&tx, row)?;
                after = row.id;
            }
            tx.commit()?;
        }
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::snapshot::restore_metadata(&tx, &metadata)?;
        tx.execute(
            "DELETE FROM archive_retention WHERE through_row<=?1",
            [through],
        )?;
        tx.commit()?;
        let actual = ArchiveSnapshot::open(path)?.digest(through)?;
        let expected = self.digest(source, through)?;
        if actual != expected {
            return Err(failure("history_restore_digest_mismatch"));
        }
        Ok(actual)
    }
}
pub struct PostgresHistoryQuery {
    projection: PostgresProjection,
    source: String,
}
impl HistoryQuery for PostgresHistoryQuery {
    fn page(&self, request: &HistoryRequest) -> Result<HistoryPage> {
        let request = request.clone();
        let source = self.source.clone();
        let target_id = self.projection.target_id.clone();
        self.projection.with_client(move|db|Box::pin(async move {
        if request.limit == 0 || request.limit > 200 || request.filter.account.len() > 256 {
            return Err(failure("history_query_invalid"));
        }

        let tx = db
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await.map_err(postgres_error)?;
        let progress=tx.query_opt("SELECT committed_row,observed_upper,metadata,metadata_sha256,extract(epoch FROM clock_timestamp()-updated_at)::double precision FROM titan_projection_sources WHERE source_id=$1",&[&source]).await.map_err(postgres_error)?.ok_or_else(||failure("projection_source_unavailable"))?;
        let committed: i64 = progress.get(0);
        let observed: i64 = progress.get(1);
        let metadata: Vec<u8> = progress.get(2);
        if metadata.len() > 16 * MAX_FACT_BYTES || sha256(&metadata) != progress.get::<_, String>(3)
        {
            return Err(failure("projection_metadata_corrupt"));
        }
        let meta: ArchiveMetadata = serde_json::from_slice(&metadata)?;
        if meta.store_id != source || meta.schema_version != FACT_SCHEMA {
            return Err(failure("projection_metadata_corrupt"));
        }
        let store = format!("pg:{}:{}", target_id, source);
        let filter = if request.newest_first {
            sha256(&serde_json::to_vec(&(&request.filter, "descending"))?)
        } else {
            sha256(&serde_json::to_vec(&request.filter)?)
        };
        let f = &request.filter;
        let upper=tx.query_one("SELECT coalesce(max(row_id),0) FROM titan_fact_records WHERE source_id=$1 AND environment=$2 AND account=$3 AND ($4::TEXT IS NULL OR strategy=$4) AND ($5::TEXT IS NULL OR run=$5) AND ($6::TEXT IS NULL OR kind=$6) AND replay=false AND row_id<=$7",&[&source,&f.environment.as_str(),&f.account,&f.strategy_id,&f.run_id,&f.kind,&committed]).await.map_err(postgres_error)?.get::<_,i64>(0) as u64;
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
            "SELECT row_id,payload,sha256 FROM titan_fact_records WHERE source_id=$1 AND environment=$2 AND account=$3 AND ($4::TEXT IS NULL OR strategy=$4) AND ($5::TEXT IS NULL OR run=$5) AND ($6::TEXT IS NULL OR kind=$6) AND replay=false AND ($7::BIGINT=0 OR row_id<$7) AND row_id<=$8 ORDER BY row_id DESC LIMIT $9"
        } else {
            "SELECT row_id,payload,sha256 FROM titan_fact_records WHERE source_id=$1 AND environment=$2 AND account=$3 AND ($4::TEXT IS NULL OR strategy=$4) AND ($5::TEXT IS NULL OR run=$5) AND ($6::TEXT IS NULL OR kind=$6) AND replay=false AND row_id>$7 AND row_id<=$8 ORDER BY row_id LIMIT $9"
        };
        let env = f.environment.as_str();
        let after = after as i64;
        let upper_sql = upper as i64;
        let limit = (request.limit + 1) as i64;
        let args: [&(dyn ToSql + Sync); 9] = [
            &source,
            &env,
            &f.account,
            &f.strategy_id,
            &f.run_id,
            &f.kind,
            &after,
            &upper_sql,
            &limit,
        ];
        let rows = tx.query_raw(sql, args).await.map_err(postgres_error)?;
        pin_mut!(rows);
        let mut items = Vec::new();
        let mut size = 0;
        let mut more = false;
        while let Some(r) = rows.try_next().await.map_err(postgres_error)? {
            let bytes: Vec<u8> = r.get(1);
            if items.len() == request.limit || size + bytes.len() > 4 * MAX_FACT_BYTES {
                more = true;
                break;
            }
            let digest: String = r.get(2);
            if sha256(&bytes) != digest || bytes.len() > MAX_FACT_BYTES {
                return Err(failure("history_payload_corrupt"));
            }
            let fact: FactEnvelope = serde_json::from_slice(&bytes)?;
            fact.validate()?;
            if fact.scope.environment != f.environment
                || fact.scope.account != f.account
                || f.strategy_id
                    .as_ref()
                    .is_some_and(|v| fact.scope.strategy_id.as_ref() != Some(v))
                || f.run_id
                    .as_ref()
                    .is_some_and(|v| fact.scope.run_id.as_ref() != Some(v))
                || f.kind.as_ref().is_some_and(|v| fact.kind != *v)
            {
                return Err(failure("history_scope_metadata_corrupt"));
            }
            size += bytes.len();
            items.push(StoredFact {
                row_id: r.get::<_, i64>(0).to_string(),
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
        let matched: std::collections::BTreeSet<_> = meta
            .scopes
            .iter()
            .filter(|s| {
                s.environment == f.environment.as_str()
                    && s.account == f.account
                    && f.run_id.as_ref().is_none_or(|r| *r == s.run)
            })
            .map(|s| s.recording.as_str())
            .collect();
        let sources = matched.len();
        let gaps = meta
            .gaps
            .iter()
            .filter(|s| {
                matched.contains(s.recording.as_str())
                    && (s.account == f.account || s.account.is_empty())
            })
            .count();
        let unclean = meta
            .sources
            .iter()
            .filter(|s| matched.contains(s.recording.as_str()) && !s.clean)
            .count();
        let incomplete = meta
            .sources
            .iter()
            .filter(|s| matched.contains(s.recording.as_str()) && !s.complete)
            .count();
        Ok(HistoryPage {
            items,
            next_cursor,
            upper_row: upper.to_string(),
            recording: serde_json::json!({"source_count":sources.to_string(),"gap_count":gaps.to_string(),"unclosed_sources":unclean.to_string(),"incomplete_sources":incomplete.to_string(),"received_segment_complete":sources>0&&gaps==0&&unclean==0&&incomplete==0&&committed>=observed,"venue_history_complete":false,"durable_upper_row":upper.to_string(),"projection_committed_row":committed.to_string(),"projection_observed_upper_row":observed.to_string(),"projection_age_seconds":progress.get::<_,f64>(4),"projection_metadata_sha256":progress.get::<_,String>(3),"backend":"postgres"}),
        })
            }))
    }
}

type PgFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + 'a>>;
/// Client deadlines include connect/authentication, transaction and response streaming.
/// Dropping the dedicated runtime closes the transport; no detached blocked worker accumulates.
fn execute<T>(
    config: &PostgresConfig,
    expected: Option<&str>,
    work: impl for<'a> FnOnce(&'a mut Client) -> PgFuture<'a, T>,
) -> Result<T> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| failure("postgres_runtime_unavailable"))?;
    runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_millis(
                config.connect_timeout_ms + config.statement_timeout_ms,
            ),
            async {
                let mut db = config.connect().await?;
                if let Some(expected) = expected {
                    let row = db
                        .query_one(
                            "SELECT schema_version,store_id FROM titan_persistence_meta WHERE id=1",
                            &[],
                        )
                        .await
                        .map_err(postgres_error)?;
                    if row.get::<_, i32>(0) != 1 || row.get::<_, String>(1) != expected {
                        return Err(failure("postgres_target_identity_changed"));
                    }
                }
                work(&mut db).await
            },
        )
        .await
        .map_err(|_| failure("postgres_client_deadline_exceeded"))?
    })
}
