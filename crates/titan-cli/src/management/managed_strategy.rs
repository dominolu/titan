//! Strategy directory commands. Validation and execution stay in the management API.
use super::{
    client::{Client, ClientError, read_json},
    contract::{Action, SaveVersion},
    managed_operation::{WriteOptions, submit},
};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct PageOptions {
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1_000_000))]
    pub offset: u32,
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=200))]
    pub limit: u32,
}

#[derive(Debug, Args)]
pub struct StrategyPage {
    #[arg(long)]
    pub strategy_id: String,
    #[command(flatten)]
    pub page: PageOptions,
}

#[derive(Debug, Subcommand)]
pub enum StrategyCommand {
    /// List authorized strategies with server-side pagination.
    #[command(alias = "list")]
    Ls {
        #[command(flatten)]
        page: PageOptions,
    },
    Show {
        strategy_id: String,
    },
    /// List parameter presets belonging to a strategy.
    #[command(alias = "templates")]
    Presets(StrategyPage),
    Versions(StrategyPage),
    Version {
        strategy_id: String,
        id: String,
    },
    Backtests(StrategyPage),
    Reports(StrategyPage),
    Deployments(StrategyPage),
    /// Query one resource kind within the authorized strategy history.
    History {
        #[arg(long)]
        kind: String,
        #[command(flatten)]
        scope: StrategyPage,
    },
    /// Validate parameters and describe the strategy through an Operation.
    #[command(alias = "validate")]
    Describe {
        #[arg(long)]
        parameter_preset_id: String,
        #[arg(long)]
        parameters: PathBuf,
        #[command(flatten)]
        write: WriteOptions,
    },
    #[command(alias = "build")]
    Compile {
        #[arg(long)]
        parameter_version_id: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    /// Prepare a shared dataset through the existing Operation pipeline.
    Prepare {
        #[arg(long)]
        dataset_id: String,
        #[command(flatten)]
        write: WriteOptions,
    },
    Backtest {
        #[arg(long)]
        strategy_version_id: String,
        #[arg(long)]
        dataset_id: String,
        #[arg(long)]
        stress: bool,
        #[command(flatten)]
        write: WriteOptions,
    },
    Report {
        #[arg(long)]
        backtest_id: String,
        #[command(flatten)]
        write: WriteOptions,
    },
}

#[derive(Debug, Subcommand)]
pub enum ParameterCommand {
    #[command(alias = "list")]
    Ls(StrategyPage),
    Show {
        id: String,
    },
    /// Create a draft parameter version from a JSON object.
    Create {
        #[arg(long)]
        parameter_preset_id: String,
        #[arg(long)]
        parameters: PathBuf,
    },
    /// Copy a saved version to a new draft; frozen evidence is not reused.
    Copy {
        id: String,
    },
    /// Replace draft parameters with an explicit optimistic revision.
    Update {
        id: String,
        #[arg(long)]
        parameters: PathBuf,
        #[arg(long)]
        expected_revision: String,
    },
    /// Freeze a saved draft using the matching successful describe operation.
    Freeze {
        id: String,
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        freeze_operation_id: String,
    },
}

pub(super) fn add_scope<'a>(client: &'a Client, query: &mut Vec<(&'a str, &'a str)>) {
    if let Some(node) = client.options().node.as_deref() {
        query.push(("node_id", node));
    }
    if let Some(environment) = client.environment() {
        query.push(("environment", environment));
    }
}

fn resource_scope(client: &Client, value: &Value) -> Result<()> {
    if client
        .options()
        .node
        .as_deref()
        .is_some_and(|node| value["node_id"].as_str() != Some(node))
    {
        return Err(invalid_input("resource does not match the selected node"));
    }
    if client
        .environment()
        .is_some_and(|environment| value["body"]["environment"].as_str() != Some(environment))
    {
        return Err(invalid_input(
            "resource does not match the selected environment",
        ));
    }
    Ok(())
}

pub(super) async fn list_resources(
    client: &Client,
    kind: &str,
    scope: &StrategyPage,
) -> Result<Value> {
    checked_id(&scope.strategy_id)?;
    let offset = scope.page.offset.to_string();
    let limit = scope.page.limit.to_string();
    let mut query = vec![
        ("kind", kind),
        ("strategy_id", scope.strategy_id.as_str()),
        ("offset", offset.as_str()),
        ("limit", limit.as_str()),
    ];
    add_scope(client, &mut query);
    client.get("/api/v2/resources", &query).await
}

async fn strategy_history(client: &Client, endpoint: &str, scope: &StrategyPage) -> Result<Value> {
    checked_id(&scope.strategy_id)?;
    let offset = scope.page.offset.to_string();
    let limit = scope.page.limit.to_string();
    let mut query = vec![("offset", offset.as_str()), ("limit", limit.as_str())];
    add_scope(client, &mut query);
    client
        .get(
            &format!("/api/v2/strategies/{}/{endpoint}", scope.strategy_id),
            &query,
        )
        .await
}

pub(super) async fn resource(client: &Client, id: &str, kind: &str) -> Result<Value> {
    checked_id(id)?;
    let value = client.get(&format!("/api/v2/resources/{id}"), &[]).await?;
    if value["kind"].as_str() != Some(kind) {
        return Err(invalid_input("resource has an unexpected type"));
    }
    resource_scope(client, &value)?;
    Ok(value)
}

pub(super) fn checked_id(id: &str) -> Result<()> {
    super::checked_id(id)
        .map_err(|_| ClientError::new("INVALID_INPUT", "invalid resource identity").into())
}

fn invalid_input(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_INPUT", message).into()
}

fn parameters(path: &std::path::Path) -> Result<Value> {
    let value = read_json(path)?;
    if !value.is_object() {
        return Err(invalid_input("parameters must be a JSON object"));
    }
    Ok(value)
}

pub(super) async fn run_strategy(client: &Client, command: StrategyCommand) -> Result<()> {
    let kind = match &command {
        StrategyCommand::Describe { .. }
        | StrategyCommand::Compile { .. }
        | StrategyCommand::Prepare { .. }
        | StrategyCommand::Backtest { .. }
        | StrategyCommand::Report { .. } => "operation",
        _ => "result",
    };
    let value = match command {
        StrategyCommand::Ls { page } => {
            let offset = page.offset.to_string();
            let limit = page.limit.to_string();
            let mut query = vec![("offset", offset.as_str()), ("limit", limit.as_str())];
            add_scope(client, &mut query);
            client.get("/api/v2/strategies", &query).await?
        }
        StrategyCommand::Show { strategy_id } => {
            checked_id(&strategy_id)?;
            let mut query = Vec::new();
            add_scope(client, &mut query);
            client
                .get(&format!("/api/v2/strategies/{strategy_id}"), &query)
                .await?
        }
        StrategyCommand::Presets(scope) => {
            list_resources(client, "parameter_preset", &scope).await?
        }
        StrategyCommand::Versions(scope) => strategy_history(client, "versions", &scope).await?,
        StrategyCommand::Version { strategy_id, id } => {
            checked_id(&strategy_id)?;
            checked_id(&id)?;
            let mut query = Vec::new();
            add_scope(client, &mut query);
            let value = client
                .get(
                    &format!("/api/v2/strategies/{strategy_id}/versions/{id}"),
                    &query,
                )
                .await?;
            resource_scope(client, &value)?;
            value
        }
        StrategyCommand::Backtests(scope) => list_resources(client, "backtest", &scope).await?,
        StrategyCommand::Reports(scope) => list_resources(client, "report", &scope).await?,
        StrategyCommand::Deployments(scope) => list_resources(client, "deployment", &scope).await?,
        StrategyCommand::History { kind, scope } => {
            if !matches!(
                kind.as_str(),
                "parameter_preset"
                    | "parameter_version"
                    | "strategy_version"
                    | "backtest"
                    | "report"
                    | "deployment"
                    | "run"
            ) {
                return Err(invalid_input("unsupported strategy history kind"));
            }
            list_resources(client, &kind, &scope).await?
        }
        StrategyCommand::Describe {
            parameter_preset_id,
            parameters: path,
            write,
        } => {
            checked_id(&parameter_preset_id)?;
            submit(
                client,
                client.node()?,
                Action::Describe {
                    parameter_preset_id,
                    parameters: parameters(&path)?,
                },
                write,
            )
            .await?
        }
        StrategyCommand::Compile {
            parameter_version_id,
            write,
        } => {
            checked_id(&parameter_version_id)?;
            submit(
                client,
                client.node()?,
                Action::Compile {
                    parameter_version_id,
                },
                write,
            )
            .await?
        }
        StrategyCommand::Prepare { dataset_id, write } => {
            checked_id(&dataset_id)?;
            submit(
                client,
                client.node()?,
                Action::Prepare { dataset_id },
                write,
            )
            .await?
        }
        StrategyCommand::Backtest {
            strategy_version_id,
            dataset_id,
            stress,
            write,
        } => {
            checked_id(&strategy_version_id)?;
            checked_id(&dataset_id)?;
            submit(
                client,
                client.node()?,
                Action::Backtest {
                    strategy_version_id,
                    dataset_id,
                    stress,
                },
                write,
            )
            .await?
        }
        StrategyCommand::Report { backtest_id, write } => {
            checked_id(&backtest_id)?;
            submit(
                client,
                client.node()?,
                Action::Report { backtest_id },
                write,
            )
            .await?
        }
    };
    client.print_kind(kind, &value)
}

fn saved_input(value: &Value) -> Result<SaveVersion> {
    Ok(SaveVersion {
        node_id: value["node_id"]
            .as_str()
            .context("parameter version has no node")?
            .to_owned(),
        parameter_preset_id: value["body"]["parameter_preset_id"]
            .as_str()
            .context("parameter version has no preset")?
            .to_owned(),
        parameters: value["body"]["parameters"].clone(),
        expected_revision: None,
        freeze_operation_id: None,
    })
}

pub(super) async fn run_parameter(client: &Client, command: ParameterCommand) -> Result<()> {
    let value = match command {
        ParameterCommand::Ls(scope) => {
            strategy_history(client, "parameter-versions", &scope).await?
        }
        ParameterCommand::Show { id } => resource(client, &id, "parameter_version").await?,
        ParameterCommand::Create {
            parameter_preset_id,
            parameters: path,
        } => {
            checked_id(&parameter_preset_id)?;
            if client
                .environment()
                .is_some_and(|environment| environment != "backtest")
            {
                return Err(invalid_input(
                    "parameter versions require the backtest environment",
                ));
            }
            let input = SaveVersion {
                node_id: client.node()?.to_owned(),
                parameter_preset_id,
                parameters: parameters(&path)?,
                expected_revision: None,
                freeze_operation_id: None,
            };
            client
                .post("/api/v2/parameter-versions", serde_json::to_value(input)?)
                .await?
        }
        ParameterCommand::Copy { id } => {
            let old = resource(client, &id, "parameter_version").await?;
            let input = saved_input(&old)?;
            client
                .post("/api/v2/parameter-versions", serde_json::to_value(input)?)
                .await?
        }
        ParameterCommand::Update {
            id,
            parameters: path,
            expected_revision,
        } => {
            let old = resource(client, &id, "parameter_version").await?;
            let mut input = saved_input(&old)?;
            input.parameters = parameters(&path)?;
            input.expected_revision = Some(expected_revision);
            client
                .patch(
                    &format!("/api/v2/parameter-versions/{id}"),
                    serde_json::to_value(input)?,
                )
                .await?
        }
        ParameterCommand::Freeze {
            id,
            expected_revision,
            freeze_operation_id,
        } => {
            checked_id(&freeze_operation_id)?;
            let old = resource(client, &id, "parameter_version").await?;
            let mut input = saved_input(&old)?;
            input.expected_revision = Some(expected_revision);
            input.freeze_operation_id = Some(freeze_operation_id);
            client
                .patch(
                    &format!("/api/v2/parameter-versions/{id}"),
                    serde_json::to_value(input)?,
                )
                .await?
        }
    };
    client.print(&value)
}
