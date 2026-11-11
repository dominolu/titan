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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresConfigurationCheck {
    pub schema_state: String,
    pub can_initialize: bool,
    pub can_read: bool,
    pub can_write: bool,
}

impl PostgresConfig {
    /// Inspect a candidate without installing tables or changing persistent state.
    /// Diagnostics intentionally contain no DSN, server name or credential value.
    pub async fn check_connection(&self) -> Result<PostgresConfigurationCheck> {
        self.check_connection_expected(None).await
    }
    pub async fn check_connection_expected(
        &self,
        expected_target: Option<&str>,
    ) -> Result<PostgresConfigurationCheck> {
        StorageProfile {
            schema_version: 1,
            postgres: Some(self.clone()),
            s3: None,
            budget: StorageBudget::default(),
        }
        .validate()?;
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut client = self.connect().await?;
            let tx = client.build_transaction().read_only(true).start().await.map_err(postgres_error)?;
            let privileges = tx.query_one(
                "SELECT has_database_privilege(current_database(),'CONNECT'), COALESCE(has_schema_privilege(current_schema(),'USAGE'),false), COALESCE(has_schema_privilege(current_schema(),'CREATE'),false), to_regclass('titan_persistence_meta') IS NOT NULL, to_regclass('titan_projection_sources') IS NOT NULL, to_regclass('titan_fact_records') IS NOT NULL",
                &[],
            ).await.map_err(postgres_error)?;
            if !privileges.get::<_, bool>(0) || !privileges.get::<_, bool>(1) {
                return Err(failure("postgres_permissions_insufficient"));
            }
            let can_initialize = privileges.get::<_, bool>(2);
            let tables = [privileges.get::<_, bool>(3), privileges.get::<_, bool>(4), privileges.get::<_, bool>(5)];
            if tables.iter().all(|exists| !exists) {
                if expected_target.is_some() { return Err(failure("postgres_target_identity_changed")); }
                tx.rollback().await.map_err(postgres_error)?;
                return Ok(PostgresConfigurationCheck { schema_state: "uninitialized".into(), can_initialize, can_read: false, can_write: false });
            }
            if !tables.iter().all(|exists| *exists) {
                return Err(failure("postgres_schema_incomplete"));
            }
            let access = tx.query_one(
                "SELECT bool_and(has_table_privilege(name,'SELECT')), bool_and(has_table_privilege(name,'INSERT') AND has_table_privilege(name,'UPDATE') AND has_table_privilege(name,'DELETE')) FROM unnest(ARRAY['titan_persistence_meta','titan_projection_sources','titan_fact_records']) AS names(name)",
                &[],
            ).await.map_err(postgres_error)?;
            let can_read = access.get::<_, bool>(0);
            let can_write = access.get::<_, bool>(1);
            if !can_read {
                return Err(failure("postgres_permissions_insufficient"));
            }
            let row = tx.query_opt("SELECT schema_version,store_id FROM titan_persistence_meta WHERE id=1", &[]).await.map_err(postgres_error)?
                .ok_or_else(|| failure("postgres_schema_incomplete"))?;
            if row.try_get::<_, i32>(0).map_err(postgres_error)? != 1 {
                return Err(failure("postgres_schema_unsupported"));
            }
            let target_id = row.try_get::<_, String>(1).map_err(postgres_error)?;
            if target_id.is_empty() || expected_target.is_some_and(|expected| expected != target_id) {
                return Err(failure("postgres_target_identity_changed"));
            }
            tx.prepare("SELECT source_id,committed_row,observed_upper,metadata,metadata_sha256,updated_at FROM titan_projection_sources LIMIT 0").await.map_err(postgres_error)?;
            tx.prepare("SELECT source_id,row_id,recording,stream,epoch,sequence,environment,account,strategy,run,kind,payload,sha256,replay FROM titan_fact_records LIMIT 0").await.map_err(postgres_error)?;
            tx.rollback().await.map_err(postgres_error)?;
            Ok(PostgresConfigurationCheck { schema_state: "initialized".into(), can_initialize, can_read, can_write })
        }).await.map_err(|_| failure("postgres_configuration_check_timeout"))?
    }

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
                tls.add_root_certificate(read_postgres_certificate(path)?);
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
fn read_postgres_certificate(path: &Path) -> Result<native_tls::Certificate> {
    use std::io::Read;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| failure("postgres_certificate_unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| failure("postgres_certificate_unavailable"))?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(failure("postgres_certificate_invalid"));
    }
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| failure("postgres_certificate_unavailable"))?;
    if bytes.len() > 1024 * 1024 {
        return Err(failure("postgres_certificate_invalid"));
    }
    native_tls::Certificate::from_pem(&bytes).map_err(|_| failure("postgres_certificate_invalid"))
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

#[cfg(test)]
mod configuration_check_tests {
    use super::*;

    #[test]
    fn certificate_reader_is_bounded_and_does_not_report_private_paths() {
        let temp = tempfile::tempdir().unwrap();
        let oversized = temp.path().join("private-oversized-marker.pem");
        std::fs::File::create(&oversized)
            .unwrap()
            .set_len(1024 * 1024 + 1)
            .unwrap();
        assert_eq!(
            read_postgres_certificate(&oversized)
                .err()
                .expect("expected rejection")
                .to_string(),
            "postgres_certificate_invalid"
        );
        let missing = temp.path().join("private-missing-marker.pem");
        let error = read_postgres_certificate(&missing)
            .err()
            .expect("expected rejection")
            .to_string();
        assert_eq!(error, "postgres_certificate_unavailable");
        assert!(!error.contains("private-missing-marker"));
        let malformed = temp.path().join("private-malformed-marker.pem");
        std::fs::write(&malformed, b"invalid certificate").unwrap();
        assert_eq!(
            read_postgres_certificate(&malformed)
                .err()
                .expect("expected rejection")
                .to_string(),
            "postgres_certificate_invalid"
        );
        assert!(read_postgres_certificate(temp.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn certificate_reader_rejects_fifo_without_opening_a_writer() {
        use std::os::unix::ffi::OsStrExt;
        let temp = tempfile::tempdir().unwrap();
        let fifo = temp.path().join("certificate.fifo");
        let path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        assert_eq!(
            read_postgres_certificate(&fifo)
                .err()
                .expect("expected rejection")
                .to_string(),
            "postgres_certificate_invalid"
        );
    }

    #[test]
    fn certificate_reader_accepts_small_public_root_certificate() {
        // Public COMODO ECC Certification Authority root; contains no private key.
        let pem = b"-----BEGIN CERTIFICATE-----\nMIICiTCCAg+gAwIBAgIQH0evqmIAcFBUTAGem2OZKjAKBggqhkjOPQQDAzCBhTEL\nMAkGA1UEBhMCR0IxGzAZBgNVBAgTEkdyZWF0ZXIgTWFuY2hlc3RlcjEQMA4GA1UE\nBxMHU2FsZm9yZDEaMBgGA1UEChMRQ09NT0RPIENBIExpbWl0ZWQxKzApBgNVBAMT\nIkNPTU9ETyBFQ0MgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMDgwMzA2MDAw\nMDAwWhcNMzgwMTE4MjM1OTU5WjCBhTELMAkGA1UEBhMCR0IxGzAZBgNVBAgTEkdy\nZWF0ZXIgTWFuY2hlc3RlcjEQMA4GA1UEBxMHU2FsZm9yZDEaMBgGA1UEChMRQ09N\nT0RPIENBIExpbWl0ZWQxKzApBgNVBAMTIkNPTU9ETyBFQ0MgQ2VydGlmaWNhdGlv\nbiBBdXRob3JpdHkwdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAQDR3svdcmCFYX7deSR\nFtSrYpn1PlILBs5BAH+X4QokPB0BBO490o0JlwzgdeT6+3eKKvUDYEs2ixYjFq0J\ncfRK9ChQtP6IHG4/bC8vCVlbpVsLM5niwz2J+Wos77LTBumjQjBAMB0GA1UdDgQW\nBBR1cacZSBm8nZ3qQUfflMRId5nTeTAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/\nBAUwAwEB/zAKBggqhkjOPQQDAwNoADBlAjEA7wNbeqy3eApyt4jf/7VGFAkK+qDm\nfQjGGoe9GKhzvSbKYAydzpmfz1wPMOG+FDHqAjAU9JM8SaczepBGR7NjfRObTrdv\nGDeAU/7dIOA1mjbRxwG55tzd8/8dLDoWV9mSOdY=\n-----END CERTIFICATE-----\n";
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("public-root.pem");
        std::fs::write(&path, pem).unwrap();
        assert!(read_postgres_certificate(&path).is_ok());
    }

    #[test]
    #[ignore = "requires an explicitly provisioned empty loopback PostgreSQL database"]
    fn real_postgres_configuration_check_pins_target_identity() {
        let reference = "TITAN_TEST_POSTGRES_DSN";
        let dsn =
            std::env::var(reference).expect("explicit isolated PostgreSQL reference required");
        let parsed: postgres::Config = dsn
            .parse()
            .expect("isolated PostgreSQL reference must be valid");
        assert!(!parsed.get_hosts().is_empty());
        assert!(parsed.get_hosts().iter().all(
            |host| matches!(host, postgres::config::Host::Tcp(host) if is_loopback_host(host))
        ));
        let config = PostgresConfig {
            dsn_env: reference.into(),
            allow_insecure_loopback: true,
            root_certificate: None,
            statement_timeout_ms: 2000,
            connect_timeout_ms: 2000,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let empty = runtime.block_on(config.check_connection()).unwrap();
        assert_eq!(
            empty.schema_state, "uninitialized",
            "use a fresh isolated database for this test"
        );
        assert!(empty.can_initialize);
        assert!(!empty.can_read && !empty.can_write);
        let error = runtime
            .block_on(config.check_connection_expected(Some("missing-fixed-target")))
            .unwrap_err()
            .to_string();
        assert_eq!(error, "postgres_target_identity_changed");
        assert!(!error.contains(&dsn));
        let projection = crate::PostgresProjection::open(config.clone()).unwrap();
        let initialized = runtime
            .block_on(config.check_connection_expected(Some(&projection.target_id)))
            .unwrap();
        assert_eq!(initialized.schema_state, "initialized");
        assert!(initialized.can_read && initialized.can_write);
        let error = runtime
            .block_on(config.check_connection_expected(Some("different-fixed-target")))
            .unwrap_err()
            .to_string();
        assert_eq!(error, "postgres_target_identity_changed");
        assert!(!error.contains("different-fixed-target") && !error.contains(&dsn));
        // An optional separately provisioned SELECT-only role verifies privilege reporting.
        if let Some(readonly_dsn) = std::env::var_os("TITAN_TEST_POSTGRES_READONLY_DSN") {
            let parsed: postgres::Config = readonly_dsn
                .to_str()
                .expect("read-only reference must be UTF-8")
                .parse()
                .expect("read-only reference must be valid");
            assert!(!parsed.get_hosts().is_empty() && parsed.get_hosts().iter().all(|host| matches!(host, postgres::config::Host::Tcp(host) if is_loopback_host(host))));
            let readonly = PostgresConfig {
                dsn_env: "TITAN_TEST_POSTGRES_READONLY_DSN".into(),
                ..config
            };
            let checked = runtime
                .block_on(readonly.check_connection_expected(Some(&projection.target_id)))
                .unwrap();
            assert!(checked.can_read && !checked.can_write && !checked.can_initialize);
        }
    }

    #[test]
    fn postgres_check_rejects_literal_credentials_before_connecting() {
        let config = PostgresConfig {
            dsn_env: "postgresql://literal-marker/history".into(),
            allow_insecure_loopback: false,
            root_certificate: None,
            statement_timeout_ms: 2000,
            connect_timeout_ms: 2000,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(config.check_connection())
            .unwrap_err()
            .to_string();
        assert_eq!(error, "secret_environment_reference_invalid");
        assert!(!error.contains("literal-marker"));
    }

    #[test]
    fn postgres_check_requires_bounded_budgets_before_connecting() {
        let config = PostgresConfig {
            dsn_env: "TITAN_TEST_HISTORY_REFERENCE".into(),
            allow_insecure_loopback: false,
            root_certificate: None,
            statement_timeout_ms: 120_000,
            connect_timeout_ms: 2000,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(
            runtime
                .block_on(config.check_connection())
                .unwrap_err()
                .to_string(),
            "postgres_budget_invalid"
        );
    }
}
