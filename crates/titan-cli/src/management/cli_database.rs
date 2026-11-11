//! Offline database configuration. Only history may use PostgreSQL.
use super::{
    client::{ClientError, ClientOptions},
    frontend_config,
};
use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, IsTerminal, Read, Write},
    path::{Path, PathBuf},
};
use titan_persistence::{PostgresConfig, StorageBudget, StorageControl, StorageProfile};

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Configure local/node storage offline. Existing stores require validated migration.
    #[command(
        after_help = "Examples:\n  titan_cli config database --history sqlite --storage-root ./storage\n  titan_cli config database --history postgres --dsn-env TITAN_HISTORY_DSN --storage-root ./storage --save-only\n\nOnly environment references are saved. Management and recovery remain local SQLite.\nRun on the machine owning the storage; this command never writes remote paths."
    )]
    Database(DatabaseArgs),
    /// Show a fixed, redacted configuration summary without connecting to a service.
    Show,
    /// Check references, local access and history connection/schema without modifying storage.
    Check,
}
#[derive(Clone, Copy, ValueEnum)]
pub enum HistoryBackend {
    Sqlite,
    Postgres,
}
impl HistoryBackend {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}
#[derive(Args)]
pub struct DatabaseArgs {
    /// History backend; SQLite is offered by the interactive wizard.
    #[arg(long, value_enum)]
    pub history: Option<HistoryBackend>,
    /// Name of an environment variable containing the PostgreSQL DSN, never the DSN itself.
    #[arg(long)]
    pub dsn_env: Option<String>,
    /// StorageControl directory on this machine/owning node; never a recovery database file.
    #[arg(long)]
    pub storage_root: Option<PathBuf>,
    /// Permit plaintext PostgreSQL only when every configured host is loopback.
    #[arg(long)]
    pub allow_insecure_loopback: bool,
    /// Additional PEM root certificate for PostgreSQL TLS.
    #[arg(long)]
    pub root_certificate: Option<PathBuf>,
    /// Save a candidate without initializing even an empty storage directory.
    #[arg(long)]
    pub save_only: bool,
}
impl std::fmt::Debug for DatabaseArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseArgs")
            .field("save_only", &self.save_only)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Database {
    schema_version: u32,
    storage_root: PathBuf,
    history: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dsn_env: Option<String>,
    #[serde(default)]
    allow_insecure_loopback: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_certificate: Option<PathBuf>,
    state: String,
}
fn invalid(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_CONFIG", message).into()
}
fn storage_error(message: &str) -> anyhow::Error {
    ClientError::new("STORAGE_UNAVAILABLE", message).into()
}
impl Database {
    fn profile(&self) -> Result<StorageProfile> {
        if self.schema_version != 1
            || !matches!(self.history.as_str(), "sqlite" | "postgres")
            || !matches!(self.state.as_str(), "candidate" | "active")
            || !self.storage_root.is_absolute()
        {
            return Err(invalid(
                "invalid database settings; storage_root must be absolute",
            ));
        }
        if self.history == "sqlite"
            && (self.dsn_env.is_some()
                || self.root_certificate.is_some()
                || self.allow_insecure_loopback)
        {
            return Err(invalid(
                "PostgreSQL connection options require --history postgres",
            ));
        }
        let postgres = if self.history == "postgres" {
            Some(PostgresConfig {
                dsn_env: self.dsn_env.clone().ok_or_else(|| {
                    invalid("PostgreSQL requires --dsn-env ENVIRONMENT_REFERENCE")
                })?,
                allow_insecure_loopback: self.allow_insecure_loopback,
                root_certificate: self.root_certificate.clone(),
                statement_timeout_ms: 2000,
                connect_timeout_ms: 2000,
            })
        } else {
            None
        };
        let profile = StorageProfile {
            schema_version: 1,
            postgres,
            s3: None,
            budget: StorageBudget::default(),
        };
        profile
            .validate()
            .map_err(|_| invalid("invalid database settings or environment reference"))?;
        Ok(profile)
    }
    fn summary(&self) -> Value {
        // Construct an allowlist. Do not serialize profile fields, paths, DSNs or references.
        json!({"configured":true,"schema_version":self.schema_version,"requested_history":self.history,
            "saved_state":self.state,"storage_root_configured":!self.storage_root.as_os_str().is_empty(),
            "credential_reference_configured":self.dsn_env.is_some(),"tls_root_configured":self.root_certificate.is_some(),
            "allow_insecure_loopback":self.allow_insecure_loopback,"management_backend":"local_sqlite","recovery_backend":"local_sqlite"})
    }
}
fn prompt(label: &str, default: Option<&str>) -> Result<String> {
    let mut stderr = std::io::stderr().lock();
    if let Some(default) = default {
        write!(stderr, "{label} [{default}]: ")
    } else {
        write!(stderr, "{label}: ")
    }
    .and_then(|_| stderr.flush())
    .map_err(|_| invalid("cannot write configuration prompt"))?;
    let mut answer = String::new();
    if std::io::stdin()
        .lock()
        .take(4097)
        .read_line(&mut answer)
        .map_err(|_| invalid("cannot read configuration response"))?
        == 0
    {
        return Err(invalid("configuration cancelled at end of input"));
    }
    if answer.len() > 4096 {
        return Err(invalid("configuration response exceeds the size limit"));
    }
    let answer = answer.trim();
    Ok(if answer.is_empty() {
        default.unwrap_or("").to_owned()
    } else {
        answer.to_owned()
    })
}
fn absolute(path: PathBuf) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(invalid("storage_root must not be empty"));
    }
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|_| invalid("cannot resolve local storage directory"))?
            .join(path)
    };
    if path
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(invalid(
            "use a storage directory without parent traversal components",
        ));
    }
    Ok(path)
}
fn database_args(mut args: DatabaseArgs, interactive: bool) -> Result<Database> {
    if args.history.is_none() && interactive {
        args.history = Some(
            match prompt("History backend (sqlite/postgres)", Some("sqlite"))?.as_str() {
                "sqlite" => HistoryBackend::Sqlite,
                "postgres" => HistoryBackend::Postgres,
                _ => return Err(invalid("choose sqlite or postgres")),
            },
        );
    }
    let history = args.history.ok_or_else(|| {
        invalid("non-interactive configuration requires --history sqlite|postgres")
    })?;
    if args.storage_root.is_none() && interactive {
        args.storage_root = Some(PathBuf::from(prompt(
            "Local storage directory (run on the owning machine)",
            None,
        )?));
    }
    if matches!(history, HistoryBackend::Postgres) && args.dsn_env.is_none() && interactive {
        args.dsn_env = Some(prompt("DSN environment variable name (not the DSN)", None)?);
    }
    if interactive && !args.save_only {
        let answer = prompt(
            "Initialize only if the storage directory is new and empty? (yes/no)",
            Some("yes"),
        )?;
        match answer.as_str() {
            "yes" | "y" => {}
            "no" | "n" => args.save_only = true,
            _ => return Err(invalid("choose yes or no")),
        }
    }
    let database = Database {
        schema_version: 1,
        storage_root: absolute(
            args.storage_root
                .ok_or_else(|| invalid("non-interactive configuration requires --storage-root"))?,
        )?,
        history: history.name().into(),
        dsn_env: args.dsn_env,
        allow_insecure_loopback: args.allow_insecure_loopback,
        root_certificate: args.root_certificate.map(absolute).transpose()?,
        // The caller saves candidate first, then derives active state from the immutable store.
        state: if args.save_only {
            "candidate"
        } else {
            "active"
        }
        .into(),
    };
    database.profile()?;
    Ok(database)
}
fn load_for_database(options: &ClientOptions) -> Result<toml::Value> {
    let path = frontend_config::config_path(options)?;
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut document = toml::map::Map::new();
            document.insert("schema_version".into(), toml::Value::Integer(1));
            document.insert("profiles".into(), toml::Value::Table(Default::default()));
            Ok(toml::Value::Table(document))
        }
        _ => frontend_config::load_document(options),
    }
}
fn selected_for_save(options: &ClientOptions, document: &toml::Value) -> Result<String> {
    if let Some(name) = &options.profile {
        return Ok(name.clone());
    }
    if let Ok(name) = std::env::var("TITAN_PROFILE")
        && !name.is_empty()
    {
        return Ok(name);
    }
    if document
        .get("profiles")
        .and_then(toml::Value::as_table)
        .is_some_and(|profiles| profiles.is_empty())
    {
        return Ok("default".into());
    }
    frontend_config::selected_profile(options)
}
fn put_database(document: &mut toml::Value, name: &str, database: &Database) -> Result<()> {
    let root = document
        .as_table_mut()
        .ok_or_else(|| invalid("invalid client configuration document"))?;
    root.insert("schema_version".into(), toml::Value::Integer(1));
    if !root.contains_key("default_profile") {
        root.insert("default_profile".into(), toml::Value::String(name.into()));
    }
    let profiles = root
        .entry("profiles")
        .or_insert_with(|| toml::Value::Table(Default::default()))
        .as_table_mut()
        .ok_or_else(|| invalid("invalid profile table"))?;
    let profile = profiles
        .entry(name)
        .or_insert_with(|| toml::Value::Table(Default::default()))
        .as_table_mut()
        .ok_or_else(|| invalid("invalid profile settings"))?;
    profile.insert(
        "database".into(),
        toml::Value::try_from(database)
            .map_err(|_| invalid("cannot serialize database settings"))?,
    );
    Ok(())
}
fn configured_database(options: &ClientOptions) -> Result<Database> {
    let profile = frontend_config::profile_value(options)?;
    let value = profile
        .get("database")
        .ok_or_else(|| invalid("configure storage with config database first"))?;
    let database: Database = value
        .clone()
        .try_into()
        .map_err(|_| invalid("invalid database settings"))?;
    database.profile()?;
    Ok(database)
}
fn writable_ancestor(path: &Path) -> Result<bool> {
    let ancestor = path
        .ancestors()
        .find(|part| part.exists())
        .ok_or_else(|| invalid("storage has no accessible parent directory"))?;
    if !ancestor.is_dir() {
        return Err(invalid(
            "storage root or its existing parent must be a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(ancestor.as_os_str().as_bytes())
            .map_err(|_| invalid("invalid storage path"))?;
        Ok(unsafe { libc::access(name.as_ptr(), libc::W_OK | libc::X_OK) } == 0)
    }
    #[cfg(not(unix))]
    {
        Ok(!std::fs::metadata(ancestor)
            .map_err(|_| invalid("cannot inspect storage parent"))?
            .permissions()
            .readonly())
    }
}
fn empty_or_missing(root: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(_) => Err(storage_error("cannot inspect storage directory")),
        Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => Err(invalid(
            "storage root must be a directory, not a file or symbolic link",
        )),
        Ok(_) => Ok(std::fs::read_dir(root)
            .map_err(|_| storage_error("cannot inspect storage directory"))?
            .next()
            .is_none()),
    }
}
fn inspect_local(database: &Database) -> Result<Value> {
    let writable = writable_ancestor(&database.storage_root)?;
    if !StorageControl::exists(&database.storage_root) {
        return Ok(
            json!({"profile_state":"uninitialized","application_state":"candidate","history_state":"uninitialized","history_route_count":0,"local_writable":writable,"safe_to_initialize":empty_or_missing(&database.storage_root)?}),
        );
    }
    let control = StorageControl::open(&database.storage_root).map_err(|_| {
        storage_error("existing storage control cannot be opened; reconciliation is required")
    })?;
    let requested = database.profile()?;
    let matches = serde_json::to_value(&control.profile)
        .map_err(|_| invalid("cannot inspect requested profile"))?
        == serde_json::to_value(&requested)
            .map_err(|_| invalid("cannot inspect active profile"))?;
    let inspect = control
        .inspect()
        .map_err(|_| storage_error("cannot inspect existing storage routes"))?;
    let routes = inspect["history"]
        .as_array()
        .ok_or_else(|| storage_error("invalid storage route metadata"))?;
    let selected = routes
        .iter()
        .all(|route| route["backend"] == database.history);
    let history_active = selected && (database.history == "sqlite" || !routes.is_empty());
    Ok(
        json!({"profile_state":if matches {"active"}else{"different_immutable_profile"},
        "application_state":if matches && history_active && database.history == "sqlite" {"active"}else{"candidate"},
        "history_state":if history_active && database.history == "postgres" {"verification_required"}else if history_active {"active"}else{"migration_required"},
        "history_route_count":routes.len(),"local_writable":writable,"safe_to_initialize":false}),
    )
}
async fn check(database: &Database) -> Result<Value> {
    let profile = database.profile()?;
    let mut result = inspect_local(database)?;
    result["configuration_valid"] = json!(true);
    result["management_backend"] = json!("local_sqlite");
    result["recovery_backend"] = json!("local_sqlite");
    if let Some(pg) = &profile.postgres {
        let expected_target = if StorageControl::exists(&database.storage_root) {
            let control = StorageControl::open(&database.storage_root)
                .map_err(|_| storage_error("cannot inspect fixed history target"))?;
            let pinned = control
                .postgres_target_id()
                .map_err(|_| storage_error("cannot inspect fixed history target"))?;
            if control.profile.postgres.is_some() && pinned.is_none() {
                return Err(storage_error(
                    "fixed history target is unavailable; reconcile storage before use",
                ));
            }
            pinned
        } else {
            None
        };
        let checked = pg.check_connection_expected(expected_target.as_deref()).await.map_err(|_| storage_error("history connection, fixed target identity, reference, TLS, permission or schema check failed; private values are hidden"))?;
        result["target_identity_verified"] =
            json!(expected_target.is_some() && checked.schema_state == "initialized");
        if result["profile_state"] == "active"
            && result["history_state"] == "verification_required"
            && expected_target.is_some()
        {
            result["application_state"] = json!("active");
            result["history_state"] = json!("active");
        }
        result["postgres"] = serde_json::to_value(&checked)
            .map_err(|_| storage_error("cannot format history connection status"))?;
        result["connection_ready"] = json!(if checked.schema_state == "initialized" {
            checked.can_read && checked.can_write
        } else {
            checked.can_initialize
        });
    } else {
        result["connection_ready"] = json!(true);
    }
    result["permissions_ready"] =
        json!(result["local_writable"] == true && result["connection_ready"] == true);
    Ok(result)
}
fn print(options: &ClientOptions, kind: &str, value: &Value) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    if options.json || options.ndjson {
        serde_json::to_writer(&mut stdout, &json!({"schema_version":1,"api_version":super::contract::API_VERSION,"kind":kind,"data":value}))
            .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write configuration output"))?;
    } else {
        super::human::write(&mut stdout, kind, value)
            .and_then(|_| stdout.flush())
            .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write configuration output"))?;
        return Ok(());
    }
    writeln!(stdout)
        .and_then(|_| stdout.flush())
        .map_err(|_| ClientError::new("OUTPUT_ERROR", "cannot write configuration output"))?;
    Ok(())
}
fn maintenance() -> Value {
    json!({"automatic_migration":false,"guidance":[
        "Existing storage profiles are immutable. Saving a candidate does not change a running service or its storage.",
        "Inspect the active store with titan storage inspect --storage-root <local-root>.",
        "When the active immutable profile already declares the intended PostgreSQL target, use titan storage prepare-history --storage-root <local-root> --database <existing-history-db> --target postgres (or sqlite).",
        "Only activate the validated plan using titan storage activate --storage-root <local-root> --plan <validated-plan-id>; interrupted plans use titan storage resume.",
        "If the immutable profile lacks the intended target, reconcile the node's storage configuration through its maintenance procedure; never edit control.db or replace recovery databases.",
        "Run configuration on the machine owning the storage and point its node storage_root to the same initialized directory. No remote service is changed by this command."
    ]})
}
pub async fn run(options: &ClientOptions, command: ConfigCommand) -> Result<()> {
    match command {
        ConfigCommand::Show => {
            let profile = frontend_config::profile_value(options)?;
            let mut value = json!({"profile_selected":true,"management_connection_configured":profile.get("url").is_some(),"local_server_configured":profile.get("local_server").is_some(),"management_backend":"local_sqlite","recovery_backend":"local_sqlite"});
            value["database"] = match profile.get("database") {
                Some(_) => configured_database(options)?.summary(),
                None => json!({"configured":false}),
            };
            print(options, "configuration", &value)
        }
        ConfigCommand::Check => {
            let database = configured_database(options)?;
            let value = check(&database).await?;
            print(options, "configuration_check", &value)?;
            if value["permissions_ready"] != true {
                return Err(storage_error(
                    "configured storage permissions are insufficient",
                ));
            }
            Ok(())
        }
        ConfigCommand::Database(args) => {
            let interactive = std::io::stdin().is_terminal()
                && std::io::stderr().is_terminal()
                && !options.json
                && !options.ndjson;
            let mut database = database_args(args, interactive)?;
            let initialize = database.state == "active";
            database.state = "candidate".into();
            let old = load_for_database(options)?;
            let selected = selected_for_save(options, &old)?;
            let mut candidate = old.clone();
            put_database(&mut candidate, &selected, &database)?;
            frontend_config::save_document(options, &old, &candidate)?;
            let mut state = inspect_local(&database)?;
            let mut initialized = false;
            if initialize && state["safe_to_initialize"] == true && state["local_writable"] == true
            {
                let checked = check(&database).await.map_err(|_| storage_error("candidate saved; history connection, reference, permission or schema check failed; storage was not initialized"))?;
                if checked["permissions_ready"] != true {
                    return Err(storage_error(
                        "candidate saved, but initialization requires storage permissions",
                    ));
                }
                let root = database.storage_root.clone();
                let profile = database.profile()?;
                tokio::task::spawn_blocking(move || {
                    // Recheck immediately before creating only the storage control database.
                    if !empty_or_missing(&root)? { return Err(storage_error("candidate saved; storage directory changed before initialization")); }
                    StorageControl::initialize(&root, profile).map(|_| ()).map_err(|_| storage_error("candidate saved; storage initialization failed and requires inspection"))
                }).await.map_err(|_| storage_error("candidate saved; storage initialization task failed"))??;
                initialized = true;
                state = inspect_local(&database)?;
            }
            if initialize
                && database.history == "postgres"
                && StorageControl::exists(&database.storage_root)
            {
                state = check(&database).await.map_err(|_| {
                    storage_error(
                        "candidate saved; fixed history target identity or connection check failed",
                    )
                })?;
            }
            if state["application_state"] == "active" {
                database.state = "active".into();
                let mut active = candidate.clone();
                put_database(&mut active, &selected, &database)?;
                frontend_config::save_document(options, &candidate, &active)?;
            }
            let mut value = database.summary();
            value["storage"] = state;
            value["initialized"] = json!(initialized);
            value["service_configuration_changed"] = json!(false);
            value["maintenance"] = maintenance();
            print(options, "database_configuration", &value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings(root: PathBuf) -> Database {
        Database {
            schema_version: 1,
            storage_root: root,
            history: "sqlite".into(),
            dsn_env: None,
            allow_insecure_loopback: false,
            root_certificate: None,
            state: "candidate".into(),
        }
    }
    #[test]
    fn database_summary_does_not_expose_paths_or_environment_reference() {
        let mut database = settings(PathBuf::from("/private/storage-marker"));
        database.history = "postgres".into();
        database.dsn_env = Some("PRIVATE_DSN_REFERENCE".into());
        database.root_certificate = Some("/private/certificate-marker".into());
        let text = database.summary().to_string();
        for marker in [
            "storage-marker",
            "PRIVATE_DSN_REFERENCE",
            "certificate-marker",
        ] {
            assert!(!text.contains(marker));
        }
        assert_eq!(database.summary()["credential_reference_configured"], true);
    }
    #[test]
    fn database_noninteractive_requires_explicit_values_and_rejects_dsn_literals() {
        let args = || DatabaseArgs {
            history: None,
            dsn_env: None,
            storage_root: None,
            allow_insecure_loopback: false,
            root_certificate: None,
            save_only: true,
        };
        assert!(database_args(args(), false).is_err());
        let mut provided = args();
        provided.history = Some(HistoryBackend::Postgres);
        provided.storage_root = Some("/storage".into());
        provided.dsn_env = Some("postgres://secret@localhost/db".into());
        assert!(database_args(provided, false).is_err());
    }
    #[tokio::test]
    async fn first_configuration_creates_profile_without_guessing_service_settings() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("new-client.toml");
        let root = temp.path().join("storage");
        let options = ClientOptions {
            config: Some(config),
            profile: Some("fresh-local".into()),
            json: true,
            ..Default::default()
        };
        run(
            &options,
            ConfigCommand::Database(DatabaseArgs {
                history: Some(HistoryBackend::Sqlite),
                dsn_env: None,
                storage_root: Some(root.clone()),
                allow_insecure_loopback: false,
                root_certificate: None,
                save_only: false,
            }),
        )
        .await
        .unwrap();
        let saved = frontend_config::load_document(&options).unwrap();
        assert_eq!(saved["default_profile"].as_str(), Some("fresh-local"));
        let profile = &saved["profiles"]["fresh-local"];
        assert_eq!(profile.as_table().unwrap().len(), 1);
        assert_eq!(profile["database"]["state"].as_str(), Some("active"));
        assert!(StorageControl::exists(&root));
        assert!(!root.join("recovery.db").exists());
        assert!(!root.join("manager.db").exists());
        assert_eq!(
            inspect_local(&configured_database(&options).unwrap()).unwrap()["application_state"],
            "active"
        );
    }

    #[tokio::test]
    async fn save_only_is_offline_and_preserves_profile_fields() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("client.toml");
        std::fs::write(&config, "schema_version=1\ndefault_profile='local'\n[profiles.local]\nusername='reader'\n[profiles.local.aliases]\ngrid='run-grid'\n").unwrap();
        let options = ClientOptions {
            config: Some(config.clone()),
            json: true,
            ..Default::default()
        };
        let root = temp.path().join("new-storage");
        run(
            &options,
            ConfigCommand::Database(DatabaseArgs {
                history: Some(HistoryBackend::Sqlite),
                dsn_env: None,
                storage_root: Some(root.clone()),
                allow_insecure_loopback: false,
                root_certificate: None,
                save_only: true,
            }),
        )
        .await
        .unwrap();
        assert!(!root.exists());
        let saved = frontend_config::profile_value(&options).unwrap();
        assert_eq!(saved["username"].as_str(), Some("reader"));
        assert_eq!(saved["aliases"]["grid"].as_str(), Some("run-grid"));
        assert_eq!(saved["database"]["state"].as_str(), Some("candidate"));
        let checked = check(&configured_database(&options).unwrap())
            .await
            .unwrap();
        assert_eq!(checked["profile_state"], "uninitialized");
        assert!(!root.exists());
    }

    #[test]
    fn readonly_inspection_preserves_existing_data_and_identifies_profile_changes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("storage");
        std::fs::create_dir(&root).unwrap();
        let protected = root.join("recovery.db");
        std::fs::write(&protected, b"protected recovery bytes").unwrap();
        let database = settings(root.clone());
        let result = inspect_local(&database).unwrap();
        assert_eq!(result["safe_to_initialize"], false);
        assert_eq!(
            std::fs::read(&protected).unwrap(),
            b"protected recovery bytes"
        );
        assert!(!root.join("control.db").exists());
        let clean = temp.path().join("clean");
        let mut database = settings(clean.clone());
        StorageControl::initialize(&clean, database.profile().unwrap()).unwrap();
        assert_eq!(
            inspect_local(&database).unwrap()["application_state"],
            "active"
        );
        database.history = "postgres".into();
        database.dsn_env = Some("UNRESOLVED_HISTORY_DSN".into());
        assert_eq!(
            inspect_local(&database).unwrap()["application_state"],
            "candidate"
        );
        let control = StorageControl::open(&clean).unwrap();
        assert!(control.profile.postgres.is_none());
    }
}
