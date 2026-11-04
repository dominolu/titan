//! The everyday client tree. Server and compatibility commands live elsewhere.
use super::{
    client::{Client, ClientOptions},
    contract::Action,
    frontend_config,
    frontend_research::{self, ExecutionOptions, ResearchCommand},
    frontend_targets::{self as targets, invalid, text},
    managed_observe,
};
use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "titan_cli",
    version,
    about = "Inspect accounts, control fixed strategy deployments, and run research",
    after_help = "Examples:\n  titan_cli check\n  titan_cli strategy ls\n  titan_cli strategy start grid\n  titan_cli account balance account-alias\n  titan_cli research build grid\n\nNames must identify one authorized target. Use an exact ID or profile alias when ambiguous.\nCommands wait by default; --detach returns acceptance. JSON/NDJSON use versioned envelopes.\nNo arguments enter the interactive shell only in a terminal."
)]
pub struct Cli {
    #[command(flatten)]
    pub options: ClientOptions,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check the configured management connection; never starts a strategy.
    Check,
    /// Persist the selected local client profile without contacting the manager.
    #[command(
        after_help = "Example: titan_cli use research\nThis changes the local default profile. It does not contact the manager or control any strategy."
    )]
    Use {
        #[arg(help = "Existing client profile name to persist as the default")]
        profile: String,
    },
    /// List strategies and control a unique deployment or fixed run.
    #[command(
        after_help = "Examples: strategy ls; strategy show grid; strategy start grid; strategy stop RUN_ID\nMissing targets show usage. No command chooses the newest run or deployment."
    )]
    Strategy {
        #[command(subcommand)]
        command: Option<StrategyCommand>,
    },
    /// Read account directory entries and platform facts; missing facts remain missing.
    #[command(
        after_help = "Examples: account ls; account show ACCOUNT_ID; account balance account-alias\nDirectory profiles do not imply a runtime connection. Ambiguous names require an exact ID."
    )]
    Account {
        #[command(subcommand)]
        command: Option<AccountCommand>,
    },
    /// Build, backtest and report using the existing operation pipeline.
    #[command(
        after_help = "Examples: research build grid; research backtest VERSION_ID --dataset DATASET_ID; research report BACKTEST_ID\nInputs must select unique fixed resources. Private journals retain every stage; --journal ID reconciles without resubmitting unknown writes."
    )]
    Research {
        #[command(subcommand)]
        command: Option<ResearchCommand>,
    },
    /// Configure or inspect local database settings without starting a service.
    Config {
        #[command(subcommand)]
        command: Option<super::cli_database::ConfigCommand>,
    },
}

#[derive(Debug, Subcommand)]
pub enum StrategyCommand {
    /// List an authorized strategy page. Use --offset to continue.
    #[command(alias = "list")]
    Ls {
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1_000_000))]
        offset: u32,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
    },
    /// Show a strategy, deployment or run using an exact ID, alias or unique strategy name.
    #[command(
        alias = "status",
        after_help = "Example: titan_cli strategy show grid\nThe strategy view lists fixed deployments and runs. Ambiguous names require an exact ID."
    )]
    Show {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
    },
    /// Start one bound deployment; a run ID may restore only its fixed recovery evidence.
    #[command(
        after_help = "Example: titan_cli strategy start grid\nStarts only one bound deployment. --detach returns acceptance; waiting never retries a write."
    )]
    Start {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Pause a unique run, fixing its current generation before submission.
    #[command(
        after_help = "Example: titan_cli strategy pause RUN_ID\nThe current run generation is fixed once before submission. Interrupting observation does not cancel the operation."
    )]
    Pause {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Resume a unique run at its fixed current generation.
    #[command(
        after_help = "Example: titan_cli strategy resume RUN_ID\nUses the same run and its fixed current generation; it does not create a fresh deployment."
    )]
    Resume {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Stop a unique run through its existing engine stop policy.
    Stop {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[command(flatten)]
        execution: ExecutionOptions,
    },
    /// Read one fixed run's bounded log; --follow observes without controlling it.
    Logs {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u16).range(1..=1000))]
        samples: u16,
    },
}

#[derive(Debug, Subcommand)]
pub enum AccountCommand {
    /// List account observations, profiles and configured bindings.
    #[command(alias = "list")]
    Ls {
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1_000_000))]
        offset: u32,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
    },
    /// Show an authorized account directory or observation resource.
    #[command(
        after_help = "Example: titan_cli account show account-alias\nA directory profile alone does not prove a connected runtime."
    )]
    Show {
        #[arg(help = "Exact account resource ID, fixed alias, or unique authorized account name")]
        target: Option<String>,
    },
    /// Show the platform balance projection without currency conversion or zero filling.
    #[command(
        alias = "balances",
        after_help = "Example: titan_cli account balance account-alias\nValues retain their original currencies. Missing or stale observations are identified explicitly."
    )]
    Balance {
        #[arg(help = "Exact account observation resource ID, alias, or unique account name")]
        target: Option<String>,
    },
    /// Show positions from the platform's account snapshot.
    #[command(
        after_help = "Example: titan_cli account positions ACCOUNT_RESOURCE_ID\nPosition sides and original units are preserved; missing evidence is not a zero position."
    )]
    Positions {
        #[arg(help = "Exact account observation resource ID, alias, or unique account name")]
        target: Option<String>,
    },
    /// Show platform account order facts, without submitting or cancelling orders.
    #[command(
        after_help = "Example: titan_cli account orders ACCOUNT_RESOURCE_ID\nUse the exact resource ID when several bindings share a display name."
    )]
    Orders {
        #[arg(help = "Exact account observation resource ID, alias, or unique account name")]
        target: Option<String>,
    },
    /// Show the platform risk projection and its observation availability.
    #[command(
        after_help = "Example: titan_cli account risk account-alias\nThe client does not recalculate exposure, limits or financial values."
    )]
    Risk {
        #[arg(help = "Exact account observation resource ID, alias, or unique account name")]
        target: Option<String>,
    },
    /// Read a bounded page of persisted fill facts with proven source identity.
    Fills {
        #[arg(
            value_name = "TARGET",
            help = "Exact resource ID, fixed profile alias, or unique authorized display name"
        )]
        target: Option<String>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
        #[arg(long)]
        cursor: Option<String>,
    },
}

pub fn print_group_help(group: &str) -> Result<()> {
    let mut command = Cli::command();
    if let Some(command) = command.find_subcommand_mut(group) {
        command.print_help()?;
        println!();
    }
    Ok(())
}

fn missing_target(options: &ClientOptions, group: &str) -> Result<()> {
    if options.json || options.ndjson {
        return Err(invalid(
            "command requires a target or a recorded journal identity; use help for syntax",
        ));
    }
    print_group_help(group)
}

fn strategy_target(command: &StrategyCommand) -> Option<&str> {
    match command {
        StrategyCommand::Ls { .. } => Some(""),
        StrategyCommand::Show { target } | StrategyCommand::Logs { target, .. } => {
            target.as_deref()
        }
        StrategyCommand::Start { target, execution }
        | StrategyCommand::Pause { target, execution }
        | StrategyCommand::Resume { target, execution }
        | StrategyCommand::Stop { target, execution } => {
            target.as_deref().or(execution.journal.as_deref())
        }
    }
}
fn account_target(command: &AccountCommand) -> Option<&str> {
    match command {
        AccountCommand::Ls { .. } => Some(""),
        AccountCommand::Show { target }
        | AccountCommand::Balance { target }
        | AccountCommand::Positions { target }
        | AccountCommand::Orders { target }
        | AccountCommand::Risk { target }
        | AccountCommand::Fills { target, .. } => target.as_deref(),
    }
}

/// Shell and one-shot entrypoints share exactly this dispatcher. Missing groups
/// and targets are handled before any connection or automatic service startup.
pub async fn dispatch(options: ClientOptions, command: Command) -> Result<()> {
    match command {
        Command::Use { profile } => frontend_config::select_profile(&options, &profile),
        Command::Config {
            command: Some(command),
        } => super::cli_database::run(&options, command).await,
        Command::Config { command: None } => missing_target(&options, "config"),
        Command::Strategy { command: None } => missing_target(&options, "strategy"),
        Command::Account { command: None } => missing_target(&options, "account"),
        Command::Research { command: None } => missing_target(&options, "research"),
        Command::Strategy {
            command: Some(command),
        } => {
            if strategy_target(&command).is_none() {
                return missing_target(&options, "strategy");
            }
            let client = super::daemon::connect_ready(&options).await?;
            strategy(&client, command).await
        }
        Command::Account {
            command: Some(command),
        } => {
            if account_target(&command).is_none() {
                return missing_target(&options, "account");
            }
            let client = super::daemon::connect_ready(&options).await?;
            account(&client, command).await
        }
        Command::Research {
            command: Some(command),
        } => {
            if !command.has_target() {
                return missing_target(&options, "research");
            }
            let client = super::daemon::connect_ready(&options).await?;
            frontend_research::run(&client, command).await
        }
        Command::Check => {
            let client = super::daemon::connect_ready(&options).await?;
            let session = client.get("/api/v2/session", &[]).await?;
            client.print_kind("connection", &json!({"identity":session["identity"],"api_version":session["api_version"],"build":session["build"],"expires_at_ns":session["expires_at_ns"]}))
        }
    }
}

async fn strategy(client: &Client, command: StrategyCommand) -> Result<()> {
    let replay = match &command {
        StrategyCommand::Start { target, execution } => {
            Some(("start", target.as_deref(), execution))
        }
        StrategyCommand::Pause { target, execution } => {
            Some(("pause", target.as_deref(), execution))
        }
        StrategyCommand::Resume { target, execution } => {
            Some(("resume", target.as_deref(), execution))
        }
        StrategyCommand::Stop { target, execution } => Some(("stop", target.as_deref(), execution)),
        _ => None,
    };
    if let Some((verb, target, execution)) = replay
        && execution.journal.is_some()
    {
        return frontend_research::resume_control(client, verb, target, execution).await;
    }
    match command {
        StrategyCommand::Ls { offset, limit } => {
            let offset = offset.to_string();
            let limit = limit.to_string();
            let mut query = vec![("offset", offset.as_str()), ("limit", limit.as_str())];
            super::managed_strategy::add_scope(client, &mut query);
            client.print_kind(
                "strategies",
                &client.get("/api/v2/strategies", &query).await?,
            )
        }
        StrategyCommand::Show {
            target: Some(target),
        } => {
            // A strategy view exposes its complete bounded directory rather than
            // pretending the newest deployment or run is the selected one.
            match targets::strategy(client, &target).await {
                Ok(owner) => {
                    let id = text(&owner, "id")?;
                    let runs = targets::resources(client, "run", Some(id)).await?;
                    let deployments = targets::resources(client, "deployment", Some(id)).await?;
                    client.print_kind(
                        "strategy",
                        &json!({"strategy":owner,"runs":runs,"deployments":deployments}),
                    )
                }
                Err(error)
                    if error
                        .downcast_ref::<super::client::ClientError>()
                        .is_some_and(|e| e.code == "NOT_FOUND" || e.status == Some(404)) =>
                {
                    let value = targets::resolve(client, &target, &["run", "deployment"]).await?;
                    client.print_kind("strategy", &value)
                }
                Err(error) => Err(error),
            }
        }
        StrategyCommand::Start {
            target: Some(target),
            execution,
        } => {
            let value = targets::start(client, &target).await?;
            let id = text(&value, "id")?.to_owned();
            let node = text(&value, "node_id")?.to_owned();
            let action = if value["kind"] == "deployment" {
                Action::Start { deployment_id: id }
            } else {
                let body = &value["body"];
                let generation = text(body, "run_generation")?.to_owned();
                let hash = text(body, "recovery_point_sha256")?.to_owned();
                let advertised = body["allowed_actions"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|v| v == "restore"));
                if !advertised
                    || hash.len() != 64
                    || !hash.bytes().all(|v| v.is_ascii_hexdigit())
                    || !body["recovery_reference"].is_object()
                    || body["expires_at_ns"].as_str().is_none()
                {
                    return Err(invalid(
                        "run has no complete fixed recovery evidence; use a bound deployment ID for a fresh start",
                    ));
                }
                Action::Restore {
                    run_id: id,
                    generation,
                    checkpoint_id: hash,
                }
            };
            frontend_research::control(client, &node, action, &execution).await
        }
        StrategyCommand::Pause {
            target: Some(target),
            execution,
        } => control_run(client, &target, "pause", &execution).await,
        StrategyCommand::Resume {
            target: Some(target),
            execution,
        } => control_run(client, &target, "resume", &execution).await,
        StrategyCommand::Stop {
            target: Some(target),
            execution,
        } => control_run(client, &target, "stop", &execution).await,
        StrategyCommand::Logs {
            target: Some(target),
            follow,
            samples,
        } => {
            let value = targets::resolve(client, &target, &["run"]).await?;
            managed_observe::execute_logs(
                client,
                managed_observe::LogsArgs {
                    id: text(&value, "id")?.to_owned(),
                    offset: 0,
                    generation: Some(text(&value["body"], "run_generation")?.to_owned()),
                    follow,
                    samples,
                    interval_ms: 1000,
                },
            )
            .await
        }
        _ => print_group_help("strategy"),
    }
}

async fn control_run(
    client: &Client,
    target: &str,
    kind: &str,
    execution: &ExecutionOptions,
) -> Result<()> {
    let value = targets::resolve(client, target, &["run"]).await?;
    let run_id = text(&value, "id")?.to_owned();
    let generation = text(&value["body"], "run_generation")?.to_owned();
    let action = match kind {
        "pause" => Action::Pause { run_id, generation },
        "resume" => Action::Resume { run_id, generation },
        _ => Action::Stop { run_id, generation },
    };
    frontend_research::control(client, text(&value, "node_id")?, action, execution).await
}

async fn account(client: &Client, command: AccountCommand) -> Result<()> {
    use managed_observe::{AccountCommand as Existing, ResourceArgs};
    if let AccountCommand::Ls { offset, limit } = command {
        return managed_observe::execute_account(
            client,
            Existing::Ls(managed_observe::PageArgs { offset, limit }),
        )
        .await;
    }
    let target = account_target(&command).ok_or_else(|| invalid("account target is required"))?;
    let value = targets::account(client, target).await?;
    let id = text(&value, "id")?.to_owned();
    let command = match command {
        AccountCommand::Show { .. } => Existing::Show(ResourceArgs { id }),
        AccountCommand::Balance { .. } => Existing::Balances(ResourceArgs { id }),
        AccountCommand::Positions { .. } => Existing::Positions(ResourceArgs { id }),
        AccountCommand::Orders { .. } => Existing::Orders(ResourceArgs { id }),
        AccountCommand::Risk { .. } => Existing::Risk(ResourceArgs { id }),
        AccountCommand::Fills { limit, cursor, .. } => {
            Existing::Fills(managed_observe::FactsArgs {
                history: managed_observe::HistoryArgs {
                    id,
                    page: managed_observe::PageArgs { offset: 0, limit },
                    kind: Some("fill".into()),
                    generation: None,
                    run_generation: None,
                    from_ns: None,
                    to_ns: None,
                },
                cursor,
                newest_first: false,
                max_pages: 1,
            })
        }
        AccountCommand::Ls { .. } => unreachable!("list was handled before resolution"),
    };
    managed_observe::execute_account(client, command).await
}
