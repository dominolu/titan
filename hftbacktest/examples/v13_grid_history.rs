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
        artifact: PathBuf,
        #[arg(long)]
        layout: PathBuf,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        stress: bool,
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
            artifact,
            layout,
            data,
            output,
            stress,
        } => {
            let result = engine::run(&artifact, &layout, &data, &output, stress)?;
            println!("{}", serde_json::to_string(&result)?);
            ensure!(
                result["success"] == true,
                "historical replay failed; see result and journal"
            );
            Ok(())
        }
    }
}
