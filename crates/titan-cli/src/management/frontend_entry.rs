//! Thin client entry: parse/help are offline; only explicit actions connect to the service.
use super::{
    client,
    frontend::{Cli, Command},
    shell,
};
use clap::{CommandFactory, Parser};
use std::io::IsTerminal;

pub fn run() -> i32 {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    let arguments = std::env::args_os().collect::<Vec<_>>();
    let machine = arguments
        .iter()
        .any(|value| value == "--json" || value == "--ndjson");
    let cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let code = error.exit_code();
            if machine && code != 0 {
                client::render_error(
                    &client::ClientError::new(
                        "INVALID_INPUT",
                        "invalid command syntax; use --help to inspect supported arguments",
                    )
                    .into(),
                    true,
                );
            } else {
                let _ = error.print();
            }
            return code;
        }
    };
    let group = match &cli.command {
        None => Some(None),
        Some(Command::Strategy { command: None }) => Some(Some("strategy")),
        Some(Command::Account { command: None }) => Some(Some("account")),
        Some(Command::Research { command: None }) => Some(Some("research")),
        Some(Command::Config { command: None }) => Some(Some("config")),
        _ => None,
    };
    let result = if let Some(group) = group {
        if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() && !machine {
            shell::run(cli.options, group)
        } else if machine {
            Err(client::ClientError::new(
                "INVALID_INPUT",
                "select an action for machine output; use --help to list commands",
            )
            .into())
        } else {
            let mut command = Cli::command();
            let selected = match group {
                Some(name) => command
                    .find_subcommand_mut(name)
                    .expect("declared command group"),
                None => &mut command,
            };
            selected
                .print_help()
                .map_err(anyhow::Error::from)
                .map(|_| println!())
        }
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(anyhow::Error::from)
            .and_then(|runtime| {
                runtime.block_on(frontend_dispatch(
                    cli.options,
                    cli.command.expect("non-group action"),
                ))
            })
    };
    match result {
        Ok(()) => 0,
        Err(error) => {
            client::render_error(&error, machine);
            client::exit_code(&error)
        }
    }
}

async fn frontend_dispatch(options: client::ClientOptions, command: Command) -> anyhow::Result<()> {
    super::frontend::dispatch(options, command).await
}
