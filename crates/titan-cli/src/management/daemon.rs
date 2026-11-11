//! Explicit local manager bootstrap. Never start nodes, workers or strategies.
use super::{
    client::{Client, ClientError, ClientOptions},
    frontend_config::{self, FileLock},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::os::unix::process::CommandExt;
use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

fn invalid(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_CONFIG", message).into()
}
fn startup_error(message: &str) -> anyhow::Error {
    ClientError::new("LOCAL_SERVER_UNAVAILABLE", message).into()
}

#[derive(Serialize, Deserialize)]
struct ProcessRecord {
    pid: u32,
    start_time: u64,
    boot_id: String,
    program: PathBuf,
    manager_config: PathBuf,
    origin: String,
}
fn boot_id() -> Result<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map(|v| v.trim().to_owned())
            .map_err(|_| startup_error("cannot establish local process boot identity"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(String::new())
    }
}
fn read_record(path: &Path) -> Result<Option<ProcessRecord>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = frontend_config::private_file(path, false)?;
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| startup_error("cannot read local manager process identity"))?;
    if bytes.len() > 65536 {
        return Err(startup_error(
            "local manager process identity exceeds its size limit",
        ));
    }
    serde_json::from_slice(&bytes).map(Some).map_err(|_| {
        startup_error(
            "local manager process identity is invalid; inspect private state before retrying",
        )
    })
}
fn save_record(path: &Path, record: &ProcessRecord) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| startup_error("invalid local manager state directory"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| startup_error("cannot stage local manager identity"))?;
    use std::os::unix::fs::PermissionsExt;
    temporary
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|_| startup_error("cannot restrict local manager identity"))?;
    serde_json::to_writer(&mut temporary, record)
        .map_err(|_| startup_error("cannot encode local manager identity"))?;
    temporary
        .flush()
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|_| startup_error("cannot persist local manager identity"))?;
    temporary
        .persist(path)
        .map_err(|_| startup_error("cannot publish local manager identity"))?;
    std::fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|_| startup_error("cannot sync local manager identity directory"))?;
    Ok(())
}
async fn listening(address: SocketAddr) -> bool {
    tokio::time::timeout(
        Duration::from_millis(200),
        tokio::net::TcpStream::connect(address),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}
fn spawn_manager(program: &Path, manager_config: &Path) -> Result<Child> {
    let mut command = Command::new(program);
    command
        .args(["serve", "manager", "--config"])
        .arg(manager_config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // No pipe survives the CLI. Service output is intentionally discarded, so an
    // unattended manager cannot grow a CLI-owned log without a rotation budget.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
        .spawn()
        .map_err(|_| startup_error("cannot launch the configured local manager program"))
}
fn reap(child: Child) {
    // A shell may keep running after bootstrap; reap the child when it eventually
    // exits without blocking shell commands or changing service lifetime.
    let _ = std::thread::Builder::new()
        .name("titan-manager-reaper".into())
        .spawn(move || {
            let mut child = child;
            let _ = child.wait();
        });
}
fn verified(client: Client, expected: &str) -> Result<Option<Client>> {
    if client.service_identity() != Some(expected) {
        return Err(ClientError::new("SERVICE_IDENTITY_MISMATCH", "local manager identity is missing or does not match its configured database and origin; existing services were retained").into());
    }
    Ok(Some(client))
}

async fn prepare(options: &ClientOptions) -> Result<Option<Client>> {
    let options = frontend_config::resolved_options(options)?;
    let Some(server) = frontend_config::local_server(&options)? else {
        return Ok(None);
    };
    let url = reqwest::Url::parse(
        options
            .url
            .as_deref()
            .ok_or_else(|| invalid("local server profile requires a management URL"))?,
    )
    .map_err(|_| invalid("invalid management URL"))?;
    // An overridden remote origin never falls back to the configured local host.
    let Some(ip) = url
        .host_str()
        .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
    else {
        return Ok(None);
    };
    if !ip.is_loopback() {
        return Ok(None);
    }
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(invalid(
            "local manager bootstrap requires a plain loopback HTTP origin",
        ));
    }
    let manager_config = server
        .manager_config
        .canonicalize()
        .map_err(|_| invalid("cannot open configured local manager configuration"))?;
    let config = super::ConsoleConfig::load(&manager_config)
        .map_err(|_| invalid("invalid local manager configuration"))?
        .manager
        .ok_or_else(|| invalid("local server configuration has no manager"))?;
    let address: SocketAddr = config
        .listen
        .parse()
        .map_err(|_| invalid("local manager must listen on an explicit loopback socket"))?;
    let origin = url.origin().ascii_serialization();
    let expected = reqwest::Url::parse(&config.public_origin)
        .map_err(|_| invalid("invalid local manager public origin"))?;
    if !address.ip().is_loopback()
        || address.ip() != ip
        || address.port() != url.port_or_known_default().unwrap_or(0)
        || expected.origin() != url.origin()
        || config.public_origin != origin
        || expected.path() != "/"
        || expected.query().is_some()
        || expected.fragment().is_some()
        || !expected.username().is_empty()
        || expected.password().is_some()
        || config.secure_cookie
    {
        return Err(invalid(
            "local manager origin, listener and client URL must match",
        ));
    }
    let expected_identity = super::manager_identity(&config);
    let program = match server.program {
        Some(path) => path,
        None => std::env::current_exe()
            .map_err(|_| startup_error("cannot resolve the manager program"))?
            .parent()
            .ok_or_else(|| startup_error("client program has no parent directory"))?
            .join("titan"),
    };
    // A valid already-running manager does not require a local launch binary.
    // Spawn still fails closed if this path is unavailable when startup is needed.
    let program = program.canonicalize().unwrap_or(program);
    let root = manager_config
        .parent()
        .ok_or_else(|| invalid("local manager configuration has no parent directory"))?
        .join(".titan-daemon");
    frontend_config::private_directory(&root)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| startup_error("cannot restrict local manager state directory"))?;
    let key = super::digest(
        &[
            manager_config.as_os_str().as_encoded_bytes(),
            b"\0",
            origin.as_bytes(),
        ]
        .concat(),
    );
    let lock_path = root.join(format!("{key}.lock"));
    let record_path = root.join(format!("{key}.json"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(server.startup_seconds);
    let _lock = loop {
        if let Some(lock) = FileLock::try_acquire(&lock_path)? {
            break lock;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(startup_error("local manager startup lock deadline elapsed"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let boot = boot_id()?;
    let existing = read_record(&record_path)?;
    let live = existing.as_ref().is_some_and(|record| {
        record.boot_id == boot
            && crate::registry::process_start_time(record.pid) == Some(record.start_time)
    });
    let running = live
        && existing.as_ref().is_some_and(|record| {
            record.manager_config == manager_config
                && record.program == program
                && record.origin == origin
        });
    if live && !running {
        return Err(startup_error(
            "a live manager identity conflicts with local configuration; inspect private state before retrying",
        ));
    }
    if existing.is_some() && !config.database.is_file() {
        return Err(startup_error(
            "previously initialized management database is unavailable or not a regular file; restore or explicitly initialize it before reconnecting",
        ));
    }
    if listening(address).await {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let client = tokio::time::timeout(remaining, Client::connect(&options))
            .await
            .map_err(|_| {
                startup_error("local manager authentication readiness deadline elapsed")
            })??;
        return verified(client, &expected_identity);
    }
    if options.no_auto_start {
        return Err(startup_error(
            "configured local manager is unavailable and automatic startup is disabled",
        ));
    }
    let mut child = if running {
        None
    } else {
        // A live PID with mismatching identity is never killed or assumed to be
        // our service. A new manager still has to acquire its configured socket.
        let mut child = spawn_manager(&program, &manager_config)?;
        let pid = child.id();
        let start_time = match crate::registry::process_start_time(pid) {
            Some(start_time) => start_time,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(startup_error(
                    "cannot establish spawned manager identity; bootstrap stopped",
                ));
            }
        };
        let record = ProcessRecord {
            pid,
            start_time,
            boot_id: boot,
            program,
            manager_config,
            origin,
        };
        if let Err(error) = save_record(&record_path, &record) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Some(child)
    };
    let result = loop {
        if let Some(process) = &mut child
            && process
                .try_wait()
                .map_err(|_| startup_error("cannot inspect spawned manager"))?
                .is_some()
        {
            break Err(startup_error(
                "local manager exited before readiness; check its configuration",
            ));
        }
        if listening(address).await {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            break match tokio::time::timeout(remaining, Client::connect(&options)).await {
                Ok(result) => result.and_then(|client| verified(client, &expected_identity)),
                Err(_) => Err(startup_error(
                    "local manager authentication readiness deadline elapsed",
                )),
            };
        }
        if tokio::time::Instant::now() >= deadline {
            break Err(startup_error(
                "local manager readiness deadline elapsed; the existing process is retained",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if let Some(child) = child {
        reap(child);
    }
    result
}
pub(super) async fn connect_ready(options: &ClientOptions) -> Result<Client> {
    match prepare(options).await? {
        Some(client) => Ok(client),
        None => Client::connect(options).await,
    }
}
