use super::{ConsoleConfig, contract::*};
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use std::{io::Read, path::PathBuf};

#[derive(Debug, Subcommand)]
pub enum ConsoleCommand {
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
        config: PathBuf,
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
        url: String,
        #[arg(long)]
        username: String,
        #[arg(long)]
        password_env: String,
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
pub fn run(command: ConsoleCommand) -> Result<()> {
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
        ConsoleCommand::Node { config } => {
            let node = ConsoleConfig::load(&config)?
                .node
                .context("node configuration missing")?;
            async_runtime()?.block_on(super::serve_node(node, config))
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
        ConsoleCommand::Request {
            url,
            username,
            password_env,
            request,
        } => async_runtime()?.block_on(async move {
            let origin = url.trim_end_matches('/');
            let client = reqwest::Client::builder().cookie_store(true).build()?;
            let password =
                std::env::var(password_env).context("password environment reference is missing")?;
            let session: Session = client
                .post(format!("{origin}/api/v1/session"))
                .header("Origin", origin)
                .json(&serde_json::json!({"username":username,"password":password}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let request: SubmitOperation = serde_json::from_slice(&std::fs::read(request)?)?;
            let response = client
                .post(format!("{origin}/api/v1/operations"))
                .header("Origin", origin)
                .header("X-CSRF-Token", session.csrf_token)
                .json(&request)
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            println!("{body}");
            ensure!(
                status.is_success(),
                "management request rejected ({status})"
            );
            Ok(())
        }),
    }
}
