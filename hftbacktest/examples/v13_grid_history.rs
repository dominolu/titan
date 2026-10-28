//! Historical public BBO/trade replay of the actual ABI V13 grid bundle.
//! The execution/account/fee coordinator is shared with HftBacktest; no Python strategy surrogate.
#![recursion_limit = "256"]
#[path = "grid_history/data.rs"]
mod data;
#[path = "grid_history/engine.rs"]
mod engine;

use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Prepare {
        #[arg(long)]
        trades: PathBuf,
        #[arg(long)]
        quotes: PathBuf,
        #[arg(long)]
        funding: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Run {
        #[arg(long)]
        run_id: Option<String>,
        #[arg(
            long = "strategy-file",
            alias = "strategy",
            alias = "compiled-strategy",
            alias = "artifact"
        )]
        compiled_strategy: PathBuf,
        #[arg(long)]
        layout: PathBuf,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        stress: bool,
        #[arg(long)]
        storage_root: Option<PathBuf>,
    },
}
fn main() -> Result<()> {
    match Args::parse().command {
        Command::Prepare {
            trades,
            quotes,
            funding,
            output,
        } => data::prepare(&trades, &quotes, &funding, &output),
        Command::Run {
            run_id,
            compiled_strategy,
            layout,
            data,
            output,
            stress,
            storage_root,
        } => {
            let result = engine::run(
                &compiled_strategy,
                &layout,
                &data,
                &output,
                stress,
                run_id.as_deref(),
            )?;
            write_chart(&output)?;
            if let Some(run_id) = run_id {
                use sha2::{Digest, Sha256};
                let inspected = titan_strategy_runtime::NativeStrategyLoaderV13::new(
                    output.join("inspect-cache"),
                    titan_strategy_runtime::V13TrustPolicy {
                        require_signature: false,
                        ed25519_keys: Default::default(),
                    },
                )
                .inspect(&compiled_strategy)?;
                let mut files = Vec::new();
                for entry in std::fs::read_dir(&output)? {
                    let entry = entry?;
                    let path = entry.path();
                    if !path.is_file() || path.file_name().is_some_and(|v| v == "manifest.json") {
                        continue;
                    }
                    use std::io::Read;
                    let mut file = std::fs::File::open(&path)?;
                    let mut hash = Sha256::new();
                    let mut buffer = [0; 65536];
                    loop {
                        let n = file.read(&mut buffer)?;
                        if n == 0 {
                            break;
                        }
                        hash.update(&buffer[..n]);
                    }
                    files.push(serde_json::json!({"path":entry.file_name().to_string_lossy(),"bytes":file.metadata()?.len(),"sha256":format!("{:x}",hash.finalize())}));
                }
                let fingerprint = titan_strategy_runtime::V13_ABI_FINGERPRINT
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                let committed_at_ns = i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_nanos(),
                )?;
                // ResultBundle v1 freezes these two historical source keys.
                // Keep report compatibility while sourcing canonical strategy metadata.
                let manifest = serde_json::to_vec_pretty(
                    &serde_json::json!({"schema_version":1,"run_id":run_id,"strategy_id":inspected.strategy_type,"strategy_version":inspected.source_version,"abi_fingerprint":fingerprint,"committed_at_ns":committed_at_ns,"files":files}),
                )?;
                titan_persistence::commit_result_bundle(
                    &output,
                    &storage_root.unwrap_or_else(|| {
                        output
                            .parent()
                            .unwrap_or(std::path::Path::new("."))
                            .join("storage")
                    }),
                    &manifest,
                )
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            }
            println!("{}", serde_json::to_string(&result)?);
            ensure!(
                result["success"] == true,
                "historical replay failed; see result and journal"
            );
            Ok(())
        }
    }
}

// Bounded display projection of the engine's existing observations. No metric is recomputed.
fn write_chart(output: &std::path::Path) -> Result<()> {
    use std::io::BufRead;
    let mut rows = std::io::BufReader::new(std::fs::File::open(output.join("equity.csv"))?).lines();
    let columns = rows
        .next()
        .transpose()?
        .unwrap_or_default()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut points = Vec::new();
    let mut stride = 1usize;
    let mut last = None;
    for (index, row) in rows.enumerate() {
        let values = row?.split(',').map(str::to_owned).collect::<Vec<_>>();
        last = Some(values.clone());
        if index % stride == 0 {
            points.push(values);
        }
        if points.len() >= 2048 {
            points = points.into_iter().step_by(2).collect();
            stride *= 2;
        }
    }
    if let Some(last) = last
        && points.last() != Some(&last)
    {
        points.push(last);
    }
    std::fs::write(
        output.join("chart.json"),
        serde_json::to_vec(
            &serde_json::json!({"columns":columns,"points":points,"display_stride":stride,"metrics_source":"result.json"}),
        )?,
    )?;
    Ok(())
}
