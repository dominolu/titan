//! Startup-only loader for venue ConnectorFactory plugin packages.

use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    sync::Arc,
};

use sha2::{Digest, Sha256};

use titan_account_plugin::{
    AccountConnectorFactory, AccountPluginFactory, DynamicAccountConnectorFactory,
};
use titan_market_plugin::{
    DynamicMarketConnectorFactory, MarketConnectorFactory, MarketPluginFactory,
};
use titan_plugin_engine::{
    DynamicPluginLoader, DynamicPluginSession, LoadedDynamicPackage, PluginError,
};

pub struct LoadedConnectorPlugin {
    pub package_version: semver::Version,
    pub source: Arc<str>,
    pub market_factory: Arc<dyn MarketConnectorFactory>,
    pub account_factory: Arc<dyn AccountConnectorFactory>,
    session: DynamicPluginSession,
}

pub fn load_connector_plugins(
    manifests: impl IntoIterator<Item = impl AsRef<Path>>,
) -> Result<Vec<LoadedConnectorPlugin>, PluginError> {
    manifests
        .into_iter()
        .map(LoadedConnectorPlugin::load)
        .collect()
}

pub fn market_plugin_factory(plugins: &[LoadedConnectorPlugin]) -> MarketPluginFactory {
    plugins
        .iter()
        .fold(MarketPluginFactory::new(), |factory, plugin| {
            factory.with_factory(plugin.market_factory.clone())
        })
}

pub fn account_plugin_factory(plugins: &[LoadedConnectorPlugin]) -> AccountPluginFactory {
    plugins
        .iter()
        .fold(AccountPluginFactory::new(), |factory, plugin| {
            factory.with_factory(plugin.account_factory.clone())
        })
}

impl LoadedConnectorPlugin {
    pub fn load(manifest_path: impl AsRef<Path>) -> Result<Self, PluginError> {
        let package = DynamicPluginLoader::default().load_package(manifest_path)?;
        Self::from_package(package)
    }

    pub fn from_package(package: LoadedDynamicPackage) -> Result<Self, PluginError> {
        let package_version = package.package_version.clone();
        let source: Arc<str> = Arc::from(package.manifest_path.to_string_lossy().as_ref());
        let manifest_version = package
            .code
            .manifest()
            .get("version")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                connector_error("load_connector_plugin", "plugin version is missing".into())
            })?;
        let manifest_version = semver::Version::parse(manifest_version)
            .map_err(|error| connector_error("load_connector_plugin", error.to_string()))?;
        if manifest_version != package_version {
            return Err(connector_error(
                "load_connector_plugin",
                "package version and plugin manifest version differ".into(),
            ));
        }
        let session = DynamicPluginSession::start(package.code, &serde_json::json!({}))?;
        let market_factory = DynamicMarketConnectorFactory::from_session(session.clone())
            .map_err(|error| connector_error("load_market_factory", error.to_string()))?;
        let account_factory = DynamicAccountConnectorFactory::from_session(session.clone())
            .map_err(|error| connector_error("load_account_factory", error.to_string()))?;
        if market_factory.connector_type().is_empty() || account_factory.connector_type().is_empty()
        {
            return Err(connector_error(
                "load_connector_plugin",
                "connector factory type must not be empty".into(),
            ));
        }
        Ok(Self {
            package_version,
            source,
            market_factory: Arc::new(market_factory),
            account_factory: Arc::new(account_factory),
            session,
        })
    }

    pub fn code_path(&self) -> std::path::PathBuf {
        self.session.code_lease().path().to_path_buf()
    }
}

fn connector_error(operation: &'static str, message: String) -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        titan_plugin_engine::PluginIdentity::new("titan.connector-loader", "startup"),
        titan_plugin_engine::LifecycleState::Discovered,
        operation,
        message,
    )
}

/// Copies a built venue library into a self-contained plugin package and writes its verified
/// package manifest. Packaging is a build/deployment operation, never a runtime hot-path action.
pub fn package_plugin_library(
    library_path: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<std::path::PathBuf, PluginError> {
    let code = DynamicPluginLoader::default().load_library(library_path.as_ref())?;
    let version = code
        .manifest()
        .get("version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| connector_error("package_plugin", "plugin version is missing".into()))?;
    semver::Version::parse(version)
        .map_err(|error| connector_error("package_plugin", error.to_string()))?;
    let destination = destination.as_ref();
    fs::create_dir_all(destination)
        .map_err(|error| connector_error("package_plugin", error.to_string()))?;
    let filename = code
        .path()
        .file_name()
        .ok_or_else(|| connector_error("package_plugin", "library filename is missing".into()))?;
    let packaged_library = destination.join(filename);
    fs::copy(code.path(), &packaged_library)
        .map_err(|error| connector_error("package_plugin", error.to_string()))?;
    let digest = sha256_file(&packaged_library)?;
    let manifest_path = destination.join("plugin.json");
    let manifest = serde_json::json!({
        "schema_major": titan_plugin_engine::TITAN_MANIFEST_SCHEMA_MAJOR,
        "schema_minor": titan_plugin_engine::TITAN_MANIFEST_SCHEMA_MINOR,
        "package_version": version,
        "library": filename.to_string_lossy(),
        "sha256": digest,
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| connector_error("package_plugin", error.to_string()))?,
    )
    .map_err(|error| connector_error("package_plugin", error.to_string()))?;
    Ok(manifest_path)
}

fn sha256_file(path: &Path) -> Result<String, PluginError> {
    let mut file =
        File::open(path).map_err(|error| connector_error("package_plugin", error.to_string()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| connector_error("package_plugin", error.to_string()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
