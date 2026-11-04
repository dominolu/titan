//! Lifecycle adapters: the management application owns admission and business policy.
use super::{
    client::Client,
    contract::Action,
    managed_operation::{WriteOptions, submit},
    managed_strategy::{StrategyPage, checked_id, list_resources, resource},
};
use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum DeploymentCommand {
    #[command(alias = "list")]
    Ls(StrategyPage),
    Show {
        id: String,
    },
    /// Create a deployment from a fixed strategy version and binding.
    Create {
        #[arg(long)]
        strategy_version_id: String,
        #[arg(long)]
        binding_id: String,
        #[command(flatten)]
        write: WriteOptions,
    },
}

#[derive(Debug, Subcommand)]
pub enum RunCommand {
    #[command(alias = "list")]
    Ls(StrategyPage),
    Show {
        id: String,
    },
    Start {
        #[arg(long)]
        deployment_id: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    Pause {
        id: String,
        #[arg(long)]
        expected_run_generation: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    Resume {
        id: String,
        #[arg(long)]
        expected_run_generation: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    Stop {
        id: String,
        #[arg(long)]
        expected_run_generation: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    #[command(alias = "recover")]
    Restore {
        id: String,
        #[arg(long)]
        expected_run_generation: String,
        #[arg(long)]
        recovery_point_sha256: String,
        #[command(flatten)]
        write: WriteOptions,
    },
}

pub(super) async fn run_deployment(client: &Client, command: DeploymentCommand) -> Result<()> {
    let kind = match &command {
        DeploymentCommand::Create { .. } => "operation",
        _ => "result",
    };
    let value = match command {
        DeploymentCommand::Ls(scope) => list_resources(client, "deployment", &scope).await?,
        DeploymentCommand::Show { id } => resource(client, &id, "deployment").await?,
        DeploymentCommand::Create {
            strategy_version_id,
            binding_id,
            write,
        } => {
            checked_id(&strategy_version_id)?;
            checked_id(&binding_id)?;
            submit(
                client,
                client.node()?,
                Action::Deploy {
                    strategy_version_id,
                    binding_id,
                },
                write,
            )
            .await?
        }
    };
    client.print_kind(kind, &value)
}

pub(super) async fn run_run(client: &Client, command: RunCommand) -> Result<()> {
    let (action, write) = match command {
        RunCommand::Ls(scope) => {
            return client.print(&list_resources(client, "run", &scope).await?);
        }
        RunCommand::Show { id } => {
            return client.print(&resource(client, &id, "run").await?);
        }
        RunCommand::Start {
            deployment_id,
            write,
        } => {
            checked_id(&deployment_id)?;
            (Action::Start { deployment_id }, write)
        }
        RunCommand::Pause {
            id,
            expected_run_generation,
            write,
        } => {
            checked_id(&id)?;
            (
                Action::Pause {
                    run_id: id,
                    generation: expected_run_generation,
                },
                write,
            )
        }
        RunCommand::Resume {
            id,
            expected_run_generation,
            write,
        } => {
            checked_id(&id)?;
            (
                Action::Resume {
                    run_id: id,
                    generation: expected_run_generation,
                },
                write,
            )
        }
        RunCommand::Stop {
            id,
            expected_run_generation,
            write,
        } => {
            checked_id(&id)?;
            (
                Action::Stop {
                    run_id: id,
                    generation: expected_run_generation,
                },
                write,
            )
        }
        RunCommand::Restore {
            id,
            expected_run_generation,
            recovery_point_sha256,
            write,
        } => {
            checked_id(&id)?;
            (
                Action::Restore {
                    run_id: id,
                    generation: expected_run_generation,
                    checkpoint_id: recovery_point_sha256,
                },
                write,
            )
        }
    };
    let value = submit(client, client.node()?, action, write).await?;
    client.print_kind("operation", &value)
}
