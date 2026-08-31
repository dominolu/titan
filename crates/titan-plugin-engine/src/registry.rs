use std::{collections::BTreeMap, sync::Arc};

use crate::{
    ApiVersion, ErrorKind, PluginError, PluginFactory, PluginIdentity, PluginManifest, engine_error,
};

pub struct RegisteredPlugin {
    pub factory: Arc<dyn PluginFactory>,
    pub package_version: semver::Version,
    pub source: Arc<str>,
}

#[derive(Default)]
pub struct PluginRegistry {
    entries: BTreeMap<Arc<str>, RegisteredPlugin>,
}

impl PluginRegistry {
    pub fn register(
        &mut self,
        factory: Arc<dyn PluginFactory>,
        package_version: semver::Version,
        source: impl Into<Arc<str>>,
        host_api: ApiVersion,
        host_abi: ApiVersion,
    ) -> Result<(), PluginError> {
        let manifest = factory.manifest();
        validate_manifest(manifest, host_api, host_abi)?;
        if self.entries.contains_key(&manifest.plugin_type) {
            return Err(engine_error(
                ErrorKind::ManifestInvalid,
                "register",
                format!("duplicate plugin type {}", manifest.plugin_type),
            ));
        }
        self.entries.insert(
            manifest.plugin_type.clone(),
            RegisteredPlugin {
                factory,
                package_version,
                source: source.into(),
            },
        );
        Ok(())
    }

    pub fn get(&self, plugin_type: &str) -> Option<&RegisteredPlugin> {
        self.entries.get(plugin_type)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &RegisteredPlugin)> {
        self.entries.iter()
    }
}

pub fn validate_manifest(
    manifest: &PluginManifest,
    host_api: ApiVersion,
    host_abi: ApiVersion,
) -> Result<(), PluginError> {
    let identity = PluginIdentity::new(manifest.plugin_type.clone(), "<manifest>");
    if manifest.plugin_type.is_empty() || manifest.name.is_empty() {
        return Err(PluginError::new(
            ErrorKind::ManifestInvalid,
            identity,
            crate::LifecycleState::Discovered,
            "validate_manifest",
            "plugin_type and name must be non-empty",
        ));
    }
    if !host_api.supports(manifest.engine_api_version) {
        return Err(PluginError::new(
            ErrorKind::ApiVersionMismatch,
            identity,
            crate::LifecycleState::Discovered,
            "validate_manifest",
            format!(
                "host API {}.{} does not support {}.{}",
                host_api.major,
                host_api.minor,
                manifest.engine_api_version.major,
                manifest.engine_api_version.minor
            ),
        ));
    }
    if !host_abi.supports(manifest.abi_version) {
        return Err(PluginError::new(
            ErrorKind::AbiVersionMismatch,
            identity,
            crate::LifecycleState::Discovered,
            "validate_manifest",
            "plugin ABI is incompatible with the host",
        ));
    }
    let mut provided = std::collections::BTreeSet::new();
    for service in &manifest.provides {
        if !provided.insert(service.id.clone()) {
            return Err(engine_error(
                ErrorKind::ManifestInvalid,
                "validate_manifest",
                format!("service {} is provided more than once", service.id),
            ));
        }
    }
    Ok(())
}
