use crate::{Result, failure};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub dsn_env: String,
    #[serde(default)]
    pub allow_insecure_loopback: bool,
    pub root_certificate: Option<PathBuf>,
    #[serde(default = "query_timeout")]
    pub statement_timeout_ms: u64,
    #[serde(default = "connect_timeout")]
    pub connect_timeout_ms: u64,
}
fn query_timeout() -> u64 {
    2000
}
fn connect_timeout() -> u64 {
    2000
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub session_token_env: Option<String>,
    #[serde(default)]
    pub allow_insecure_loopback: bool,
    #[serde(default = "s3_timeout")]
    pub timeout_ms: u64,
}
fn s3_timeout() -> u64 {
    30000
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StorageBudget {
    pub max_database_bytes: u64,
    pub min_free_bytes: u64,
    pub max_object_bytes: u64,
    pub max_local_object_bytes: u64,
    pub projection_batch_rows: usize,
    pub projection_interval_ms: u64,
}
impl Default for StorageBudget {
    fn default() -> Self {
        Self {
            max_database_bytes: 20 * 1024 * 1024 * 1024,
            min_free_bytes: 1024 * 1024 * 1024,
            max_object_bytes: 5_000_000_000,
            max_local_object_bytes: 100 * 1024 * 1024 * 1024,
            projection_batch_rows: 200,
            projection_interval_ms: 1000,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageProfile {
    pub schema_version: u32,
    pub postgres: Option<PostgresConfig>,
    pub s3: Option<S3Config>,
    #[serde(default)]
    pub budget: StorageBudget,
}
impl StorageProfile {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || self.budget.projection_batch_rows == 0
            || self.budget.projection_batch_rows > 256
            || !(100..=60000).contains(&self.budget.projection_interval_ms)
            || self.budget.max_object_bytes == 0
            || self.budget.max_object_bytes > 5_000_000_000
            || self.budget.max_database_bytes < 4096
            || self.budget.max_local_object_bytes < self.budget.max_object_bytes
        {
            return Err(failure("storage_profile_invalid"));
        }
        if let Some(pg) = &self.postgres {
            validate_env(&pg.dsn_env)?;
            if !(50..=60000).contains(&pg.statement_timeout_ms)
                || !(50..=30000).contains(&pg.connect_timeout_ms)
            {
                return Err(failure("postgres_budget_invalid"));
            }
        }
        if let Some(s3) = &self.s3 {
            validate_env(&s3.access_key_env)?;
            validate_env(&s3.secret_key_env)?;
            if let Some(e) = &s3.session_token_env {
                validate_env(e)?;
            }
            let url = url::Url::parse(&s3.endpoint).map_err(|_| failure("s3_endpoint_invalid"))?;
            let local = url.host_str().is_some_and(is_loopback_host);
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
                || (url.scheme() != "https"
                    && !(url.scheme() == "http" && local && s3.allow_insecure_loopback))
                || s3.region.is_empty()
                || s3.bucket.is_empty()
                || s3.prefix.len() > 128
                || !s3
                    .prefix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_/".contains(&b))
                || s3.prefix.split('/').any(|p| p == "..")
                || !(100..=120000).contains(&s3.timeout_ms)
            {
                return Err(failure("s3_profile_invalid"));
            }
        }
        Ok(())
    }
}
pub(crate) fn validate_env(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        Err(failure("secret_environment_reference_invalid"))
    } else {
        Ok(())
    }
}
pub(crate) fn secret(name: &str) -> Result<String> {
    validate_env(name)?;
    std::env::var(name).map_err(|_| failure("storage_secret_unavailable"))
}
pub(crate) fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host == "::1"
        || host == "[::1]"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|p| p.is_loopback())
}
pub(crate) fn postgres_error(error: postgres::Error) -> crate::Error {
    failure(&format!(
        "postgres_operation_failed:{}",
        error.code().map_or("transport", |c| c.code())
    ))
}
impl PostgresConfig {
    pub(crate) async fn connect(&self) -> Result<tokio_postgres::Client> {
        let mut config: tokio_postgres::Config = secret(&self.dsn_env)?
            .parse()
            .map_err(|_| failure("postgres_configuration_invalid"))?;
        config.connect_timeout(Duration::from_millis(self.connect_timeout_ms));
        config.tcp_user_timeout(Duration::from_millis(self.connect_timeout_ms));
        let hosts = config.get_hosts();
        let local = !hosts.is_empty()
            && hosts
                .iter()
                .all(|h| matches!(h,postgres::config::Host::Tcp(v) if is_loopback_host(v)));
        let client = if self.allow_insecure_loopback && local {
            config.ssl_mode(postgres::config::SslMode::Disable);
            let (client, connection) = config
                .connect(postgres::NoTls)
                .await
                .map_err(postgres_error)?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        } else {
            config.ssl_mode(postgres::config::SslMode::Require);
            let mut tls = native_tls::TlsConnector::builder();
            if let Some(path) = &self.root_certificate {
                tls.add_root_certificate(
                    native_tls::Certificate::from_pem(&std::fs::read(path)?)
                        .map_err(|_| failure("postgres_certificate_invalid"))?,
                );
            }
            let (client, connection) = config
                .connect(postgres_native_tls::MakeTlsConnector::new(
                    tls.build()
                        .map_err(|_| failure("postgres_tls_unavailable"))?,
                ))
                .await
                .map_err(postgres_error)?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        };
        client.batch_execute(&format!("SET statement_timeout={};SET lock_timeout={};SET idle_in_transaction_session_timeout={};SET application_name='titan-persistence';",self.statement_timeout_ms,self.statement_timeout_ms.min(200),self.statement_timeout_ms+1000)).await.map_err(postgres_error)?;
        Ok(client)
    }
}
pub fn check_storage_budget(path: &Path, budget: &StorageBudget) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| failure("storage_path_invalid"))?;
    std::fs::create_dir_all(parent)?;
    let size = [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
    ]
    .iter()
    .filter_map(|p| std::fs::metadata(p).ok())
    .map(|m| m.len())
    .sum::<u64>();
    if size >= budget.max_database_bytes {
        return Err(failure("local_spool_quota_exhausted"));
    }
    check_free_space(parent, budget.min_free_bytes)
}
pub(crate) fn check_free_space(path: &Path, min: u64) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut state = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(c.as_ptr(), state.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let state = unsafe { state.assume_init() };
        let free = (state.f_bavail as u128) * (state.f_frsize as u128);
        if free < u128::from(min) {
            return Err(failure("storage_free_space_budget_exhausted"));
        }
    }
    Ok(())
}

/// Local admission check. Resolving a profile never opens an external driver.
pub fn budget_for_history(path: &Path) -> Result<StorageBudget> {
    for parent in path.ancestors().skip(1) {
        for root in [parent.to_path_buf(), parent.join("storage")] {
            if crate::StorageControl::exists(&root) {
                return Ok(crate::StorageControl::open(&root)?.profile.budget);
            }
        }
    }
    Ok(StorageBudget::default())
}
pub fn check_history_budget(path: &Path) -> Result<()> {
    check_storage_budget(path, &budget_for_history(path)?)
}

pub(crate) fn check_stream_space(path: &Path, min: u64, written: u64, chunk: usize) -> Result<()> {
    let chunk = chunk as u64;
    if written == chunk || written / (1024 * 1024) != (written - chunk) / (1024 * 1024) {
        check_free_space(path, min.saturating_add(chunk))?;
    }
    Ok(())
}
