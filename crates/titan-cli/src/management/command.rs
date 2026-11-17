use super::{ConsoleConfig, contract::*};
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use std::{io::Read, path::PathBuf};

#[derive(Debug, Subcommand)]
pub enum ConsoleCommand {
    /// Authenticate for this process and check the management connection.
    #[command(alias = "check")]
    Login,
    /// Authenticate, revoke this process's session, and exit.
    Logout,
    Account {
        #[command(subcommand)]
        command: super::managed_observe::AccountCommand,
    },
    Binding {
        #[command(subcommand)]
        command: super::managed_observe::BindingCommand,
    },
    Strategy {
        #[command(subcommand)]
        command: super::managed_strategy::StrategyCommand,
    },
    Parameter {
        #[command(subcommand)]
        command: super::managed_strategy::ParameterCommand,
    },
    Deployment {
        #[command(subcommand)]
        command: super::managed_run::DeploymentCommand,
    },
    Run {
        #[command(subcommand)]
        command: super::managed_run::RunCommand,
    },
    Operation {
        #[command(subcommand)]
        command: super::managed_operation::OperationCommand,
    },
    Status(super::managed_observe::StatusArgs),
    Watch(super::managed_observe::WatchArgs),
    Logs(super::managed_observe::LogsArgs),
    History(super::managed_observe::HistoryArgs),
    Facts(super::managed_observe::FactsArgs),
    #[command(hide = true)]
    ManagedWorker {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        run: String,
        #[arg(long)]
        owner_token: String,
    },
    /// Run the management application (HTTP, SSE and static console).
    Manager {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run one authenticated execution node.
    Node {
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        command: Option<super::managed_observe::NodeCommand>,
    },
    #[command(hide = true)]
    NodeWorker {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        operation: String,
        #[arg(long)]
        owner_token: String,
    },
    #[command(hide = true)]
    ObserveWorker {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        run: String,
        #[arg(long)]
        owner_token: String,
    },
    /// Generate the browser contract from the Rust DTOs. Run on the build server.
    ExportTypes {
        #[arg(long)]
        output: PathBuf,
    },
    /// Read a password from stdin and emit an Argon2id configuration hash.
    PasswordHash,
    /// Submit a fixed operation through the same API used by the console.
    Request {
        #[arg(long)]
        request: PathBuf,
    },
}
fn async_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?)
}
pub fn run(command: ConsoleCommand, options: super::client::ClientOptions) -> Result<()> {
    match command {
        ConsoleCommand::ManagedWorker {
            config,
            run,
            owner_token,
        } => {
            let config = ConsoleConfig::load(&config)?
                .node
                .context("node configuration missing")?;
            crate::runtime_host::run_runtime_owner(move || {
                super::control::managed_worker(config, run, owner_token)
            })
        }
        ConsoleCommand::ObserveWorker {
            config,
            run,
            owner_token,
        } => {
            let config = ConsoleConfig::load(&config)?
                .node
                .context("node configuration missing")?;
            crate::runtime_host::run_runtime_owner(move || {
                super::live::observe_worker(config, run, owner_token)
            })
        }
        ConsoleCommand::Manager { config } => {
            let config = ConsoleConfig::load(&config)?
                .manager
                .context("manager configuration missing")?;
            async_runtime()?.block_on(super::serve_manager(config))
        }
        ConsoleCommand::Node {
            config: Some(config),
            command: None,
        } => {
            let node = ConsoleConfig::load(&config)?
                .node
                .context("node configuration missing")?;
            async_runtime()?.block_on(super::serve_node(node, config))
        }
        ConsoleCommand::Node {
            config: Some(_),
            command: Some(_),
        } => {
            anyhow::bail!("node --config cannot be combined with a management query")
        }
        ConsoleCommand::NodeWorker {
            config,
            operation,
            owner_token,
        } => {
            let node = ConsoleConfig::load(&config)?
                .node
                .context("node configuration missing")?;
            async_runtime()?.block_on(super::node::worker(node, operation, owner_token))
        }
        ConsoleCommand::ExportTypes { output } => super::contract::export_types(&output),
        ConsoleCommand::PasswordHash => {
            use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
            let mut password = String::new();
            std::io::stdin().read_to_string(&mut password)?;
            let password = password.trim_end_matches(['\r', '\n']);
            ensure!(
                password.len() >= 12,
                "password must contain at least 12 characters"
            );
            let salt = SaltString::generate(&mut rand::rngs::OsRng);
            let hash = Argon2::default()
                .hash_password(password.as_bytes(), &salt)
                .map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))?;
            println!("{hash}");
            Ok(())
        }
        ConsoleCommand::Request { request } => async_runtime()?.block_on(async move {
            let client = super::client::Client::connect(&options).await?;
            let request: SubmitOperation =
                serde_json::from_value(super::client::read_json(&request)?).map_err(|_| {
                    super::client::ClientError::new(
                        "INVALID_INPUT",
                        "invalid fixed operation request",
                    )
                })?;
            super::checked_id(&request.node_id).map_err(|_| {
                super::client::ClientError::new("INVALID_INPUT", "invalid node identity")
            })?;
            if client
                .options()
                .node
                .as_deref()
                .is_some_and(|node| node != request.node_id)
                || client
                    .environment()
                    .is_some_and(|environment| environment != request.action.environment())
            {
                return Err(super::client::ClientError::new(
                    "INVALID_INPUT",
                    "request does not match selected node or environment",
                )
                .into());
            }
            if request.idempotency_key.is_empty()
                || request.idempotency_key.len() > 128
                || !request
                    .idempotency_key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
            {
                return Err(super::client::ClientError::new(
                    "INVALID_INPUT",
                    "invalid idempotency key",
                )
                .into());
            }
            {
                use std::io::Write;
                let mut stderr = std::io::stderr().lock();
                writeln!(stderr, "idempotency_key={}", request.idempotency_key)?;
                stderr.flush()?;
            }
            let body = client
                .post("/api/v1/operations", serde_json::to_value(request)?)
                .await?;
            if options.json || options.ndjson {
                client.print_kind("operation", &body)?;
            } else {
                println!("{body}");
            }
            Ok(())
        }),
        command => {
            async_runtime()?.block_on(async move {
                let client = super::client::Client::connect(&options).await?;
                match command {
                    ConsoleCommand::Login => {
                        let session = client.get("/api/v2/session", &[]).await?;
                        client.print_kind("connection", &serde_json::json!({
                        "identity": session["identity"], "api_version": session["api_version"],
                        "build": session["build"], "expires_at_ns": session["expires_at_ns"]
                    }))
                    }
                    ConsoleCommand::Logout => {
                        client.delete("/api/v2/session").await?;
                        client
                            .print_kind("connection", &serde_json::json!({"session_revoked": true}))
                    }
                    ConsoleCommand::Account { command } => {
                        super::managed_observe::execute_account(&client, command).await
                    }
                    ConsoleCommand::Binding { command } => {
                        super::managed_observe::execute_binding(&client, command).await
                    }
                    ConsoleCommand::Node {
                        config: None,
                        command: Some(command),
                    } => super::managed_observe::execute_node(&client, command).await,
                    ConsoleCommand::Node {
                        config: None,
                        command: None,
                    } => anyhow::bail!("node requires --config or a query subcommand"),
                    ConsoleCommand::Strategy { command } => {
                        super::managed_strategy::run_strategy(&client, command).await
                    }
                    ConsoleCommand::Parameter { command } => {
                        super::managed_strategy::run_parameter(&client, command).await
                    }
                    ConsoleCommand::Deployment { command } => {
                        super::managed_run::run_deployment(&client, command).await
                    }
                    ConsoleCommand::Run { command } => {
                        super::managed_run::run_run(&client, command).await
                    }
                    ConsoleCommand::Operation { command } => {
                        super::managed_operation::execute(&client, command).await
                    }
                    ConsoleCommand::Status(args) => {
                        super::managed_observe::execute_status(&client, args).await
                    }
                    ConsoleCommand::Watch(args) => {
                        super::managed_observe::execute_watch(&client, args).await
                    }
                    ConsoleCommand::Logs(args) => {
                        super::managed_observe::execute_logs(&client, args).await
                    }
                    ConsoleCommand::History(args) => {
                        super::managed_observe::execute_history(&client, args).await
                    }
                    ConsoleCommand::Facts(args) => {
                        super::managed_observe::execute_facts(&client, args).await
                    }
                    _ => unreachable!("server commands were handled before authentication"),
                }
            })
        }
    }
}
