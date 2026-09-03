use std::path::PathBuf;

use titan_connector_loader::{LoadedConnectorPlugin, package_plugin_library};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    if arguments.len() != 6 {
        return Err("expected three pairs: <library> <venue>".into());
    }
    for pair in arguments.chunks_exact(2) {
        let expected = pair[1]
            .to_str()
            .ok_or("connector type is not valid UTF-8")?;
        let package_dir = std::env::temp_dir().join(format!(
            "titan-connector-smoke-{}-{}",
            std::process::id(),
            expected
        ));
        let manifest = package_plugin_library(&pair[0], &package_dir)?;
        let plugin = LoadedConnectorPlugin::load(manifest)?;
        if plugin.market_factory.connector_type() != expected {
            return Err(format!(
                "market connector mismatch: expected {expected}, got {}",
                plugin.market_factory.connector_type()
            )
            .into());
        }
        let expected_account = format!("{expected}-account");
        if plugin.account_factory.connector_type() != expected_account {
            return Err(format!(
                "account connector mismatch: expected {expected_account}, got {}",
                plugin.account_factory.connector_type()
            )
            .into());
        }
        drop(plugin);
        std::fs::remove_dir_all(package_dir)?;
    }
    Ok(())
}
