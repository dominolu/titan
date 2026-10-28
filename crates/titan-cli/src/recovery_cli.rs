//! Explicit cold-path maintenance; no account connectors or strategy execution.
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use titan_strategy_runtime::{
    RecoveryPointRef, RuntimeStrategyNo, SqliteRecoveryConfig, SqliteRecoveryStore,
    StrategyRecoveryStore, recovery_payload_sha256,
};

#[derive(Subcommand)]
pub enum RecoveryCommand {
    /// Verify database integrity, recovery bytes and public metadata.
    Inspect,
    /// Import original JSON bytes using the original 8-byte generation watermark.
    ImportLegacy {
        #[arg(long)]
        point: PathBuf,
        #[arg(long)]
        generation_watermark: PathBuf,
        #[arg(long)]
        expected_sha256: String,
    },
    /// Preserve an original generation watermark when no checkpoint exists; creates no recovery point.
    ImportGeneration {
        #[arg(long)]
        strategy_no: u32,
        #[arg(long)]
        generation_watermark: PathBuf,
        #[arg(long)]
        expected_sha256: String,
    },
    /// Export exactly the bytes identified by a fixed reference. Never overwrites a file.
    Export {
        #[arg(long)]
        reference: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Produce a consistent SQLite backup quarantined against trading activation.
    Backup {
        #[arg(long)]
        output: PathBuf,
    },
    /// Remove unpinned, non-head recovery points outside a retention window.
    Prune {
        #[arg(long)]
        strategy_no: u32,
        #[arg(long, default_value_t = 64)]
        retain: usize,
    },
    /// Protect a verified reference from retention cleanup.
    Pin {
        #[arg(long)]
        reference: String,
        #[arg(long)]
        operation_id: String,
    },
    /// Explicitly release a reference after its operation has been reconciled.
    Unpin {
        #[arg(long)]
        reference: String,
        #[arg(long)]
        operation_id: String,
    },
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).context("output must be a new file")?;
    file.write_all(bytes)?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        fs::File::open(if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        })?
        .sync_all()?;
    }
    Ok(())
}

fn legacy_owner_lock(point: &Path, watermark: &Path) -> Result<fs::File> {
    let parent = point.parent().unwrap_or(Path::new("."));
    ensure!(
        fs::canonicalize(parent)?
            == fs::canonicalize(watermark.parent().unwrap_or(Path::new(".")))?,
        "legacy point and generation watermark must share one namespace directory"
    );
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(parent.join("owner.lock"))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "legacy namespace is still owned; stop and reconcile its worker before migration"
        );
    }
    #[cfg(not(unix))]
    anyhow::bail!("legacy owner locks are unavailable on this platform");
    Ok(file)
}

pub fn run(database: PathBuf, command: RecoveryCommand) -> Result<()> {
    let config = SqliteRecoveryConfig::new(database);
    let importing = matches!(
        &command,
        RecoveryCommand::ImportLegacy { .. } | RecoveryCommand::ImportGeneration { .. }
    );
    let store = if importing {
        SqliteRecoveryStore::open(config)?
    } else {
        SqliteRecoveryStore::open_existing(config)?
    };
    let value = match command {
        RecoveryCommand::Inspect => store.inspect()?,
        RecoveryCommand::ImportGeneration {
            strategy_no,
            generation_watermark,
            expected_sha256,
        } => {
            let _owner = legacy_owner_lock(&generation_watermark, &generation_watermark)?;
            ensure!(
                generation_watermark.file_name().and_then(|n| n.to_str())
                    == Some(format!("strategy-{strategy_no}.generation").as_str()),
                "generation watermark namespace mismatch"
            );
            let bytes = fs::read(&generation_watermark)?;
            ensure!(
                recovery_payload_sha256(&bytes) == expected_sha256,
                "generation watermark reference changed"
            );
            let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                anyhow::anyhow!("generation watermark must contain exactly 8 bytes")
            })?;
            let floor = u64::from_le_bytes(raw);
            store.import_generation(RuntimeStrategyNo(strategy_no), floor)?;
            serde_json::json!({"imported":true,"generation_floor":floor.to_string(),"recovery_point_created":false})
        }
        RecoveryCommand::ImportLegacy {
            point,
            generation_watermark,
            expected_sha256,
        } => {
            let _legacy_owner = legacy_owner_lock(&point, &generation_watermark)?;
            let bytes = fs::read(&point)?;
            ensure!(
                recovery_payload_sha256(&bytes) == expected_sha256,
                "legacy checkpoint reference changed"
            );
            let watermark = fs::read(&generation_watermark)?;
            let watermark: [u8; 8] = watermark.try_into().map_err(|_| {
                anyhow::anyhow!("generation watermark must contain exactly 8 bytes")
            })?;
            let reference = store.import_legacy(&bytes, u64::from_le_bytes(watermark))?;
            serde_json::json!({"imported":true,"reference":reference,"source_unchanged":true})
        }
        RecoveryCommand::Export { reference, output } => {
            let reference: RecoveryPointRef = serde_json::from_str(&reference)?;
            let bytes = store.payload(&reference)?;
            write_new(&output, &bytes)?;
            serde_json::json!({"exported":true,"reference":reference,"bytes":bytes.len(),"output":output})
        }
        RecoveryCommand::Backup { output } => {
            store.backup(&output)?;
            serde_json::json!({"backed_up":true,"activation_allowed":false,"output":output})
        }
        RecoveryCommand::Prune {
            strategy_no,
            retain,
        } => {
            serde_json::json!({"removed":store.prune(RuntimeStrategyNo(strategy_no),retain)?})
        }
        RecoveryCommand::Pin {
            reference,
            operation_id,
        } => {
            let reference: RecoveryPointRef = serde_json::from_str(&reference)?;
            store.pin(&reference, &operation_id)?;
            serde_json::json!({"pinned":true,"reference":reference,"operation_id":operation_id})
        }
        RecoveryCommand::Unpin {
            reference,
            operation_id,
        } => {
            let reference: RecoveryPointRef = serde_json::from_str(&reference)?;
            store.unpin(&reference, &operation_id)?;
            serde_json::json!({"unpinned":true,"reference":reference,"operation_id":operation_id})
        }
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}
