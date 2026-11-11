//! Offline client configuration. Secrets are references, and writes use locked CAS.
use super::client::{ClientError, ClientOptions};
use anyhow::Result;
use serde::Deserialize;
use std::os::{
    fd::AsRawFd,
    unix::fs::{OpenOptionsExt, PermissionsExt},
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MAX_CONFIG: u64 = 1024 * 1024;
fn invalid(message: &str) -> anyhow::Error {
    ClientError::new("INVALID_CONFIG", message).into()
}
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: Option<u32>,
    default_profile: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, Profile>,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    url: Option<String>,
    username: Option<String>,
    password_env: Option<String>,
    node: Option<String>,
    environment: Option<String>,
    local_server: Option<LocalServer>,
    database: Option<Database>,
    #[serde(default)]
    aliases: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Database {
    schema_version: u32,
    storage_root: PathBuf,
    history: String,
    dsn_env: Option<String>,
    #[serde(default)]
    allow_insecure_loopback: bool,
    root_certificate: Option<PathBuf>,
    state: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalServer {
    pub manager_config: PathBuf,
    pub program: Option<PathBuf>,
    #[serde(default = "startup_seconds")]
    pub startup_seconds: u64,
}
fn startup_seconds() -> u64 {
    15
}
fn validate(value: &toml::Value) -> Result<Document> {
    let document: Document = value
        .clone()
        .try_into()
        .map_err(|_| invalid("invalid client configuration fields"))?;
    if document.schema_version.is_some_and(|v| v != 1) {
        return Err(invalid("unsupported client configuration schema"));
    }
    if let Some(name) = &document.default_profile
        && !document.profiles.contains_key(name)
    {
        return Err(invalid("default client profile does not exist"));
    }
    for (name, profile) in &document.profiles {
        super::checked_id(name).map_err(|_| invalid("invalid client profile name"))?;
        if let Some(server) = &profile.local_server
            && (!(1..=60).contains(&server.startup_seconds)
                || server.manager_config.as_os_str().is_empty())
        {
            return Err(invalid("invalid local server configuration"));
        }
        for (alias, id) in &profile.aliases {
            if alias.is_empty() || alias.len() > 128 || alias.chars().any(char::is_control) {
                return Err(invalid("invalid target alias"));
            }
            super::checked_id(id)
                .map_err(|_| invalid("alias must refer to an exact resource identity"))?;
        }
        if let Some(database) = &profile.database {
            if database.schema_version != 1
                || database.storage_root.as_os_str().is_empty()
                || !matches!(database.history.as_str(), "sqlite" | "postgres")
                || !matches!(database.state.as_str(), "candidate" | "active")
                || database.dsn_env.as_deref().is_some_and(|s| {
                    s.is_empty() || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
                || (database.history == "postgres" && database.dsn_env.is_none())
            {
                return Err(invalid(
                    "invalid database configuration; credentials must use environment references",
                ));
            }
            // These fields are validated by the database command against its backend.
            let _ = (&database.root_certificate, database.allow_insecure_loopback);
        }
    }
    Ok(document)
}
pub(super) fn config_path(options: &ClientOptions) -> Result<PathBuf> {
    if let Some(path) = options
        .config
        .clone()
        .or_else(|| variable("TITAN_CLIENT_CONFIG").map(PathBuf::from))
        .or_else(|| variable("TITAN_CONFIG").map(PathBuf::from))
    {
        return absolute(path);
    }
    let root = if let Some(path) = variable("XDG_CONFIG_HOME") {
        PathBuf::from(path)
    } else {
        PathBuf::from(variable("HOME").ok_or_else(|| {
            invalid("configure --client-config or a user configuration directory")
        })?)
        .join(".config")
    };
    absolute(root.join("titan/client.toml"))
}
fn absolute(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| invalid("cannot resolve client configuration directory"))?
            .join(path))
    }
}
fn empty_document() -> toml::Value {
    let mut root = toml::map::Map::new();
    root.insert("schema_version".into(), toml::Value::Integer(1));
    root.insert("profiles".into(), toml::Value::Table(Default::default()));
    toml::Value::Table(root)
}
fn read_document(path: &Path, missing_ok: bool) -> Result<toml::Value> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_ok => {
            return Ok(empty_document());
        }
        Err(_) => return Err(invalid("cannot open client configuration")),
    };
    if !file
        .metadata()
        .map_err(|_| invalid("cannot inspect client configuration"))?
        .is_file()
    {
        return Err(invalid("client configuration must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read client configuration"))?;
    if bytes.len() as u64 > MAX_CONFIG {
        return Err(invalid("client configuration exceeds its size limit"));
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| invalid("client configuration is not UTF-8"))?;
    let value = toml::from_str(text).map_err(|_| invalid("invalid client configuration syntax"))?;
    validate(&value)?;
    Ok(value)
}
pub(super) fn load_document(options: &ClientOptions) -> Result<toml::Value> {
    let explicit = options.config.is_some()
        || variable("TITAN_CLIENT_CONFIG").is_some()
        || variable("TITAN_CONFIG").is_some();
    read_document(&config_path(options)?, !explicit)
}
pub(super) fn selected_profile(options: &ClientOptions) -> Result<String> {
    let value = load_document(options)?;
    let document = validate(&value)?;
    let name = options
        .profile
        .clone()
        .or_else(|| variable("TITAN_PROFILE"))
        .or(document.default_profile)
        .ok_or_else(|| invalid("select a client profile with use or --profile"))?;
    if !document.profiles.contains_key(&name) {
        return Err(invalid("selected client profile does not exist"));
    }
    Ok(name)
}
pub(super) fn profile_value(options: &ClientOptions) -> Result<toml::Value> {
    let value = load_document(options)?;
    let name = selected_profile(options)?;
    value
        .get("profiles")
        .and_then(|v| v.get(&name))
        .cloned()
        .ok_or_else(|| invalid("selected client profile does not exist"))
}
pub(super) fn resolved_options(options: &ClientOptions) -> Result<ClientOptions> {
    let value = load_document(options)?;
    let document = validate(&value)?;
    let name = options
        .profile
        .clone()
        .or_else(|| variable("TITAN_PROFILE"))
        .or(document.default_profile);
    let empty = Profile::default();
    let profile = match name.as_ref() {
        Some(name) => document
            .profiles
            .get(name)
            .ok_or_else(|| invalid("selected client profile does not exist"))?,
        None => &empty,
    };
    let mut result = options.clone();
    result.profile = name;
    result.url = result
        .url
        .or_else(|| variable("TITAN_URL"))
        .or_else(|| profile.url.clone());
    result.username = result
        .username
        .or_else(|| variable("TITAN_USERNAME"))
        .or_else(|| profile.username.clone());
    result.password_env = result
        .password_env
        .or_else(|| variable("TITAN_PASSWORD_ENV"))
        .or_else(|| profile.password_env.clone());
    result.node = result
        .node
        .or_else(|| variable("TITAN_NODE"))
        .or_else(|| profile.node.clone());
    result.environment = result
        .environment
        .or_else(|| variable("TITAN_ENVIRONMENT"))
        .or_else(|| profile.environment.clone());
    result.no_auto_start |=
        variable("TITAN_NO_AUTO_START").is_some_and(|v| matches!(v.as_str(), "1" | "true"));
    Ok(result)
}

pub(super) fn private_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        let mut builder = std::fs::DirBuilder::new();
        use std::os::unix::fs::DirBuilderExt;
        builder
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(|_| invalid("cannot create private client directory"))?;
    }
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| invalid("cannot inspect private client directory"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid(
            "private client directory must not be a symbolic link",
        ));
    }
    Ok(())
}
pub(super) fn journal_root(options: &ClientOptions) -> Result<PathBuf> {
    let options = resolved_options(options)?;
    let config = config_path(&options)?;
    let state = if let Some(path) = variable("XDG_STATE_HOME") {
        PathBuf::from(path)
    } else {
        PathBuf::from(
            variable("HOME").ok_or_else(|| invalid("configure a private user state directory"))?,
        )
        .join(".local/state")
    };
    if !state.is_absolute() {
        return Err(invalid("user state directory must be absolute"));
    }
    let identity = super::digest(
        &[
            config.as_os_str().as_encoded_bytes(),
            b"\0",
            options.profile.as_deref().unwrap_or("direct").as_bytes(),
            b"\0",
            options.url.as_deref().unwrap_or("").as_bytes(),
        ]
        .concat(),
    );
    let parent = state.join("titan/journals");
    private_directory(&parent)?;
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| invalid("cannot restrict private client directory"))?;
    let root = parent.join(identity);
    private_directory(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| invalid("cannot restrict private client directory"))?;
    Ok(root)
}
pub(super) fn private_file(path: &Path, append: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .append(append)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| invalid("cannot open private client state"))?;
    use std::os::unix::fs::MetadataExt;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("cannot inspect private client state"))?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(invalid(
            "client state must be an owned regular file with one link",
        ));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|_| invalid("cannot restrict private client state"))?;
    Ok(file)
}
pub(super) struct FileLock(File);
impl FileLock {
    pub(super) fn try_acquire(path: &Path) -> Result<Option<Self>> {
        let file = private_file(path, false)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(Self(file)));
        }
        if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
            Ok(None)
        } else {
            Err(invalid("cannot lock private client state"))
        }
    }
}
impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
pub(super) fn save_document(
    options: &ClientOptions,
    old: &toml::Value,
    new: &toml::Value,
) -> Result<()> {
    validate(new)?;
    let path = config_path(options)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("client configuration has no parent directory"))?;
    private_directory(parent)?;
    let lock_path = parent.join(format!(
        ".{}.lock",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("client")
    ));
    let _lock = FileLock::try_acquire(&lock_path)?.ok_or_else(|| {
        ClientError::new(
            "VERSION_CONFLICT",
            "client configuration is being edited; reload before saving",
        )
    })?;
    if read_document(&path, true)? != *old {
        return Err(ClientError::new(
            "VERSION_CONFLICT",
            "client configuration changed; reload before saving",
        )
        .into());
    }
    let text = toml::to_string_pretty(new)
        .map_err(|_| invalid("cannot serialize client configuration"))?;
    if text.len() as u64 > MAX_CONFIG {
        return Err(invalid("client configuration exceeds its size limit"));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| invalid("cannot stage client configuration"))?;
    temporary
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|_| invalid("cannot restrict staged client configuration"))?;
    temporary
        .write_all(text.as_bytes())
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|_| invalid("cannot persist staged client configuration"))?;
    temporary
        .persist(&path)
        .map_err(|_| invalid("cannot atomically replace client configuration"))?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|_| invalid("cannot sync client configuration directory"))?;
    Ok(())
}
pub(super) fn select_profile(options: &ClientOptions, name: &str) -> Result<()> {
    let old = load_document(options)?;
    if old.get("profiles").and_then(|v| v.get(name)).is_none() {
        return Err(invalid("selected client profile does not exist"));
    }
    let mut new = old.clone();
    new.as_table_mut()
        .ok_or_else(|| invalid("invalid client configuration"))?
        .insert("default_profile".into(), toml::Value::String(name.into()));
    save_document(options, &old, &new)
}
pub(super) fn local_server(options: &ClientOptions) -> Result<Option<LocalServer>> {
    let value = load_document(options)?;
    let document = validate(&value)?;
    let name = options
        .profile
        .clone()
        .or_else(|| variable("TITAN_PROFILE"))
        .or(document.default_profile);
    let Some(name) = name else {
        return Ok(None);
    };
    let mut server = document
        .profiles
        .get(&name)
        .ok_or_else(|| invalid("selected client profile does not exist"))?
        .local_server
        .clone();
    if let Some(server) = &mut server {
        let config = config_path(options)?;
        let parent = config
            .parent()
            .ok_or_else(|| invalid("client configuration has no parent directory"))?;
        if server.manager_config.is_relative() {
            server.manager_config = parent.join(&server.manager_config);
        }
        if let Some(program) = &mut server.program
            && program.is_relative()
        {
            *program = parent.join(&*program);
        }
    }
    Ok(server)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_selection_is_private_and_stale_saves_preserve_the_newer_document() {
        let temporary = tempfile::tempdir().unwrap();
        let options = ClientOptions {
            config: Some(temporary.path().join("client.toml")),
            ..Default::default()
        };
        let initial: toml::Value = toml::from_str("schema_version=1\ndefault_profile='a'\n[profiles.a]\n[profiles.b]\n[profiles.a.aliases]\ngrid='exact-run-id'\n").unwrap();
        save_document(&options, &empty_document(), &initial).unwrap();
        select_profile(&options, "b").unwrap();
        let current = load_document(&options).unwrap();
        assert_eq!(current["default_profile"].as_str(), Some("b"));
        assert_eq!(
            current["profiles"]["a"]["aliases"]["grid"].as_str(),
            Some("exact-run-id")
        );
        let error = save_document(&options, &initial, &initial).unwrap_err();
        assert_eq!(
            error.downcast_ref::<ClientError>().unwrap().code,
            "VERSION_CONFLICT"
        );
        assert_eq!(load_document(&options).unwrap(), current);
        assert_eq!(
            std::fs::metadata(config_path(&options).unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let state = temporary.path().join("state");
        private_directory(&state).unwrap();
        assert_eq!(
            std::fs::metadata(state).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn unknown_fields_and_invalid_aliases_are_rejected_without_echoing_values() {
        let mut value: toml::Value =
            toml::from_str("schema_version=1\n[profiles.local]\n").unwrap();
        value["profiles"]["local"].as_table_mut().unwrap().insert(
            "credential".into(),
            toml::Value::String("synthetic-private-value".into()),
        );
        let error = validate(&value).err().unwrap();
        assert!(!error.to_string().contains("synthetic-private-value"));
        assert!(validate(&value).is_err());
        let invalid_alias: toml::Value =
            toml::from_str("schema_version=1\n[profiles.local.aliases]\ngrid='../not-an-id'\n")
                .unwrap();
        assert!(validate(&invalid_alias).is_err());
    }

    #[test]
    fn private_state_rejects_symbolic_links_and_shared_inodes() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        std::fs::write(&target, "unchanged").unwrap();
        let symbolic = temporary.path().join("symbolic");
        std::os::unix::fs::symlink(&target, &symbolic).unwrap();
        assert!(private_file(&symbolic, true).is_err());
        let hard = temporary.path().join("hard");
        std::fs::hard_link(&target, &hard).unwrap();
        assert!(private_file(&hard, true).is_err());
        assert_eq!(std::fs::read_to_string(target).unwrap(), "unchanged");
    }
}
