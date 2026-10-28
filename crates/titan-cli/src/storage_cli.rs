//! Local storage maintenance. No account connector, strategy execution or financial calculation.
use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand};
use std::path::PathBuf;
use titan_persistence::*;

#[derive(Args)]
pub struct FactSelection {
    #[arg(long)]
    pub database: PathBuf,
    #[arg(long)]
    pub account: String,
    #[arg(long, default_value = "backtest")]
    pub environment: String,
    #[arg(long)]
    pub run_id: Option<String>,
    #[arg(long)]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
}
impl FactSelection {
    fn filter(self) -> Result<(PathBuf, HistoryFilter)> {
        let environment = match self.environment.as_str() {
            "live" => FactEnvironment::Live,
            "simulation" => FactEnvironment::Simulation,
            "backtest" => FactEnvironment::Backtest,
            _ => anyhow::bail!("unsupported fact environment"),
        };
        Ok((
            self.database,
            HistoryFilter {
                environment,
                account: self.account,
                strategy_id: self.strategy_id,
                run_id: self.run_id,
                kind: self.kind,
            },
        ))
    }
}
#[derive(Subcommand)]
pub enum StorageCommand {
    /// Clean unowned interrupted S3 staging files, preserving all content objects.
    CleanStaging {
        #[arg(long)]
        storage_root: PathBuf,
    },
    /// Initialize immutable host storage configuration; secret values are read only from env refs.
    Initialize {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        profile: PathBuf,
    },
    /// Inspect routing, projection health and fixed capacity budgets without starting a strategy.
    Inspect {
        #[arg(long)]
        storage_root: PathBuf,
    },
    /// Project durable local facts. --follow is an explicit cold-path service, stopped by SIGINT.
    Project {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        database: Option<PathBuf>,
        #[arg(long)]
        follow: bool,
    },
    PrepareHistory {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        target: String,
    },
    PrepareObjects {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        target: String,
    },
    Resume {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        plan: String,
    },
    Activate {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        plan: String,
    },
    PruneHistory {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        through_row: i64,
    },
    EvictLocalObjects {
        #[arg(long)]
        storage_root: PathBuf,
    },
    /// Publish/register an already frozen manifest after storage failure; never runs business code.
    PublishManifest {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    /// Validate an exact archive schema and every envelope; return count and ordered digest.
    InspectParquet {
        #[arg(long)]
        input: PathBuf,
    },
    /// Retry catalog registration from an existing immutable publication, including datasets.
    RegisterPublication {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        reference: String,
    },
    /// Read a fixed upper bound through the same history port used by the management API.
    Facts {
        #[command(flatten)]
        selection: FactSelection,
        #[arg(long)]
        storage_root: Option<PathBuf>,
        #[arg(long)]
        newest_first: bool,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Re-register a completed immutable bundle; never repeats the engine run.
    RegisterBundle {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        manifest: Option<PathBuf>,
    },
    /// Materialize original verified v1 bytes from an immutable publication into a fresh directory.
    MaterializeBundle {
        #[arg(long)]
        storage_root: PathBuf,
        #[arg(long)]
        reference: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Stream authorized-by-local-operator facts to a new Parquet file with fixed source bounds.
    ExportFacts {
        #[command(flatten)]
        selection: FactSelection,
        #[arg(long)]
        storage_root: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },
}
pub fn run(command: StorageCommand) -> Result<()> {
    fn map<T>(r: titan_persistence::Result<T>) -> Result<T> {
        r.map_err(|e| anyhow::anyhow!(e.to_string()))
    }
    let value = match command {
        StorageCommand::CleanStaging { storage_root } => {
            serde_json::json!({"removed_sessions":map(cleanup_s3_staging(&storage_root))?})
        }
        StorageCommand::Initialize {
            storage_root,
            profile,
        } => {
            let profile: StorageProfile = serde_json::from_slice(&std::fs::read(profile)?)?;
            map(StorageControl::initialize(&storage_root, profile))?;
            serde_json::json!({"initialized":true,"state":map(StorageControl::open(&storage_root))?.inspect().map_err(|e|anyhow::anyhow!(e.to_string()))?})
        }
        StorageCommand::Inspect { storage_root } => {
            map(map(StorageControl::open(&storage_root))?.inspect())?
        }
        StorageCommand::Project {
            storage_root,
            database,
            follow,
        } => {
            let control = map(StorageControl::open(&storage_root))?;
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            if follow {
                signal_hook::flag::register(signal_hook::consts::SIGINT, stop.clone())?;
                signal_hook::flag::register(signal_hook::consts::SIGTERM, stop.clone())?;
            }
            let mut latest;
            let started = std::time::Instant::now();
            loop {
                if let Some(path) = &database {
                    let progress = map(control.sync_source(path))?;
                    let caught = progress.committed_row == progress.observed_upper_row;
                    latest = serde_json::to_value(&progress)?;
                    if !follow && caught {
                        break;
                    }
                } else {
                    latest = serde_json::to_value(map(control.sync_all())?)?;
                    if !follow {
                        break;
                    }
                }
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                if !follow && started.elapsed() > std::time::Duration::from_secs(120) {
                    anyhow::bail!("projection catch-up budget exceeded");
                }
                if follow {
                    std::thread::sleep(std::time::Duration::from_millis(
                        control.profile.budget.projection_interval_ms,
                    ));
                }
            }
            latest
        }
        StorageCommand::PrepareHistory {
            storage_root,
            database,
            target,
        } => {
            let target = match target.as_str() {
                "sqlite" => HistoryBackend::Sqlite,
                "postgres" => HistoryBackend::Postgres,
                _ => anyhow::bail!("unsupported history backend"),
            };
            serde_json::to_value(map(
                map(StorageControl::open(&storage_root))?.prepare_history(&database, target)
            )?)?
        }
        StorageCommand::PrepareObjects {
            storage_root,
            target,
        } => {
            let target = match target.as_str() {
                "filesystem" => ObjectBackend::Filesystem,
                "s3" => ObjectBackend::S3,
                _ => anyhow::bail!("unsupported object backend"),
            };
            serde_json::to_value(map(
                map(StorageControl::open(&storage_root))?.prepare_objects(target)
            )?)?
        }
        StorageCommand::Resume { storage_root, plan } => {
            serde_json::to_value(map(map(StorageControl::open(&storage_root))?.resume(&plan))?)?
        }
        StorageCommand::Activate { storage_root, plan } => serde_json::to_value(map(map(
            StorageControl::open(&storage_root),
        )?
        .activate(&plan))?)?,
        StorageCommand::PruneHistory {
            storage_root,
            database,
            through_row,
        } => serde_json::to_value(map(
            map(StorageControl::open(&storage_root))?.prune_history(&database, through_row)
        )?)?,
        StorageCommand::EvictLocalObjects { storage_root } => {
            serde_json::json!({"evicted_objects":map(map(StorageControl::open(&storage_root))?.evict_local_objects())?})
        }
        StorageCommand::PublishManifest {
            storage_root,
            manifest,
        } => {
            let bytes = std::fs::read(manifest)?;
            ensure!(
                bytes.len() <= 4 * MAX_FACT_BYTES,
                "publication manifest too large"
            );
            let publication: PublishedManifest = serde_json::from_slice(&bytes)?;
            let objects =
                object_store(&storage_root).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let catalog = ResultCatalog::open(&storage_root.join("catalog.db"))
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let reference = publication
                .publish(objects.as_ref(), &catalog)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            serde_json::json!({"registered":true,"reference":reference})
        }
        StorageCommand::InspectParquet { input } => {
            use sha2::{Digest, Sha256};
            let mut hash = Sha256::new();
            let count = columnar::scan_facts(&input, |fact| {
                let bytes = serde_json::to_vec(&fact)?;
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
                Ok(())
            })
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            serde_json::json!({"valid":true,"schema":"titan_facts_v1","records":count.to_string(),"ordered_payload_sha256":format!("{:x}",hash.finalize())})
        }
        StorageCommand::RegisterPublication {
            storage_root,
            reference,
        } => {
            use std::io::Read;
            let reference: ObjectRef = serde_json::from_str(&reference)?;
            ensure!(
                reference.bytes <= 4 * MAX_FACT_BYTES as u64,
                "publication manifest too large"
            );
            let objects =
                object_store(&storage_root).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let mut bytes = Vec::new();
            objects
                .open_verified(&reference)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?
                .read_to_end(&mut bytes)?;
            let publication: PublishedManifest = serde_json::from_slice(&bytes)?;
            let catalog = ResultCatalog::open(&storage_root.join("catalog.db"))
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let actual = publication
                .publish(objects.as_ref(), &catalog)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            ensure!(actual == reference, "publication bytes changed");
            serde_json::json!({"registered":true,"reference":reference})
        }
        StorageCommand::Facts {
            selection,
            storage_root,
            newest_first,
            cursor,
            limit,
        } => {
            let (path, filter) = selection.filter()?;
            let query: Box<dyn HistoryQuery> = if let Some(root) = storage_root {
                Box::new(RoutedHistoryQuery::new(root, path))
            } else {
                Box::new(SqliteHistoryQuery::new(path))
            };
            serde_json::to_value(
                query
                    .page(&HistoryRequest {
                        filter,
                        cursor: cursor.map(|c| serde_json::from_str(&c)).transpose()?,
                        limit,
                        newest_first,
                    })
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?,
            )?
        }
        StorageCommand::RegisterBundle {
            bundle,
            storage_root,
            manifest,
        } => {
            let bytes = std::fs::read(manifest.unwrap_or_else(|| {
                if bundle.join("manifest.json").exists() {
                    bundle.join("manifest.json")
                } else {
                    bundle.join("manifest.pending.json")
                }
            }))?;
            let reference = commit_result_bundle(&bundle, &storage_root, &bytes)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            serde_json::json!({"published":true,"reference":reference})
        }
        StorageCommand::MaterializeBundle {
            storage_root,
            reference,
            output,
        } => {
            let reference: ObjectRef = serde_json::from_str(&reference)?;
            materialize_result_bundle(&storage_root, &reference, &output)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            serde_json::json!({"materialized":true,"output":output,"reference":reference})
        }
        StorageCommand::ExportFacts {
            selection,
            output,
            storage_root,
        } => {
            let (path, filter) = selection.filter()?;
            let query: Box<dyn HistoryQuery> = if let Some(root) = &storage_root {
                Box::new(RoutedHistoryQuery::new(root.clone(), path))
            } else {
                Box::new(SqliteHistoryQuery::new(path))
            };
            ensure!(
                !output.with_extension("manifest.json").exists(),
                "archive manifest already exists"
            );
            let mut writer = columnar::FactParquetWriter::create(&output)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let mut cursor = None;
            let mut count = 0_u64;
            let first = query
                .page(&HistoryRequest {
                    filter: filter.clone(),
                    cursor: None,
                    limit: 200,
                    newest_first: false,
                })
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let upper = first.upper_row.clone();
            let recording = first.recording.clone();
            let mut page = Some(first);
            loop {
                let result = if let Some(first) = page.take() {
                    first
                } else {
                    query
                        .page(&HistoryRequest {
                            filter: filter.clone(),
                            cursor: cursor.take(),
                            limit: 200,
                            newest_first: false,
                        })
                        .map_err(|e| anyhow::anyhow!(e.to_string()))?
                };
                let records = result.items.into_iter().map(|i| i.fact).collect::<Vec<_>>();
                count += records.len() as u64;
                writer
                    .write_chunk(&records)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                cursor = result.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            writer
                .finish()
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let root = output.parent().context("missing export directory")?;
            let store: Box<dyn ObjectStore> = if let Some(root) = &storage_root {
                map(object_store(root))?
            } else {
                Box::new(map(LocalObjectStore::new(root.join("archive-objects")))?)
            };
            let object = store
                .put_file(&output)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let metadata = serde_json::json!({"schema_version":1,"format":"titan_facts_v1","records":count.to_string(),"filter":filter,"upper_row":upper,"recording":recording,"object":object});
            publish_file(
                &output.with_extension("manifest.json"),
                &serde_json::to_vec_pretty(&metadata)?,
            )
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            metadata
        }
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}
