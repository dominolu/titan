use std::{
    io::{BufRead, BufReader, Read},
    path::PathBuf,
    process::{Command, ExitCode, Stdio},
};

use titan_cli::{
    APPLICATION_CONFIG_SCHEMA_VERSION, ApplicationConfig, ConfigurationAdapter,
    ConfiguredCoreRuntime, PluginConfig, PluginExecutionConfig, PluginSubscriptionConfig,
};
use titan_connector_loader::package_plugin_library;
use titan_event_engine::EventEngineConfig;
use titan_market_plugin::{
    AssetId, MarketApi, MarketInstrumentBinding, MarketRequest, MarketResponse,
    MarketSourceDefinition,
};
use titan_plugin_engine::{ServiceId, ServiceKey, ServiceScope, StopReason, TraceContext};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("dynamic Core Runtime smoke failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let libraries = std::env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if libraries.len() != 3 {
        return Err("expected Binance Futures, OKX and Hyperliquid library paths".into());
    }
    let root = std::env::temp_dir().join(format!(
        "titan-dynamic-core-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir(&root)?;
    let result = (|| {
        let mut packages = Vec::new();
        for (index, library) in libraries.iter().enumerate() {
            packages.push(package_plugin_library(
                library,
                root.join(format!("plugin-{index}")),
            )?);
        }
        let config = ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            connector_plugin_packages: packages,
            plugins: vec![
                plugin("market-provider", "titan.market"),
                plugin("account-provider", "titan.account"),
            ],
            market_sources: vec![
                market_source(
                    "binance",
                    "binance-futures",
                    1,
                    "btcusdt",
                    r#"stream_url = "ws://localhost"
api_url = "http://localhost"
"#,
                ),
                market_source(
                    "okx",
                    "okx",
                    2,
                    "BTC-USDT-SWAP",
                    r#"rest_url = "http://localhost"
public_ws_url = "ws://localhost/public"
private_ws_url = "ws://localhost/private"
api_key = ""
secret = ""
passphrase = ""
"#,
                ),
                market_source(
                    "hyperliquid",
                    "hyperliquid",
                    3,
                    "BTC",
                    r#"info_url = "http://localhost/info"
exchange_url = "http://localhost/exchange"
ws_url = "ws://localhost/ws"
"#,
                ),
            ],
            accounts: vec![],
            strategies: vec![],
        };
        let adapted = ConfigurationAdapter::adapt(config.clone(), &root)?;
        let mut runtime = ConfiguredCoreRuntime::start(adapted)?;
        let diagnostics = runtime.core().plugins().diagnostics();
        if diagnostics.len() != 2
            || diagnostics
                .iter()
                .any(|item| item.lifecycle_state != titan_plugin_engine::LifecycleState::Running)
        {
            return Err("Market/Account plugins did not enter RUNNING".into());
        }
        let market = runtime
            .core()
            .plugins()
            .services()
            .bind_typed::<MarketApi>(&ServiceKey {
                id: ServiceId::new("titan.market", "market"),
                version: semver::Version::new(1, 0, 0),
                scope: ServiceScope::Global,
            })
            .ok_or("Market service is unavailable")?;
        for source_key in ["binance", "okx", "hyperliquid"] {
            let MarketResponse::Handle(handle) = market.call(
                MarketRequest::Resolve(source_key.into()),
                TraceContext::default(),
            )??
            else {
                return Err("Market resolve returned an unexpected response".into());
            };
            let MarketResponse::Instruments(instruments) =
                market.call(MarketRequest::Instruments(handle), TraceContext::default())??
            else {
                return Err("Market instruments returned an unexpected response".into());
            };
            if instruments.len() != 1 {
                return Err(format!("{source_key} dynamic connector lost its instrument").into());
            }
        }
        runtime.shutdown(StopReason::Shutdown)?;

        let config_path = root.join("runtime.toml");
        std::fs::write(&config_path, toml::to_string(&config)?)?;
        let titan = std::env::current_exe()?
            .parent()
            .and_then(|examples| examples.parent())
            .ok_or("cannot locate target directory")?
            .join(if cfg!(windows) { "titan.exe" } else { "titan" });
        let mut child = Command::new(titan)
            .args(["core-run", "--config"])
            .arg(&config_path)
            .arg("--json")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("missing core-run stdout")?);
        let mut running = String::new();
        if stdout.read_line(&mut running)? == 0 {
            let status = child.wait()?;
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .ok_or("missing core-run stderr")?
                .read_to_string(&mut stderr)?;
            return Err(format!("core-run exited before readiness with {status}: {stderr}").into());
        }
        let running: serde_json::Value = serde_json::from_str(&running)?;
        if running.get("status").and_then(|value| value.as_str()) != Some("RUNNING") {
            return Err("core-run did not report RUNNING readiness".into());
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(child.id() as i32, libc::SIGINT);
        }
        #[cfg(not(unix))]
        child.kill()?;
        let mut stopped = String::new();
        stdout.read_to_string(&mut stopped)?;
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .ok_or("missing core-run stderr")?
            .read_to_string(&mut stderr)?;
        let status = child.wait()?;
        if !status.success() {
            return Err(format!("core-run failed with {status}: {stderr}").into());
        }
        let response: serde_json::Value = serde_json::from_str(stopped.trim())?;
        if response.get("status").and_then(|value| value.as_str()) != Some("STOPPED") {
            return Err("core-run did not report a clean SIGINT shutdown".into());
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    result
}

fn market_source(
    source_key: &str,
    connector_type: &str,
    asset_id: u32,
    native_symbol: &str,
    connector_config: &str,
) -> MarketSourceDefinition {
    MarketSourceDefinition {
        source_key: source_key.into(),
        connector_type: connector_type.into(),
        connector_config: connector_config.as_bytes().into(),
        instruments: vec![MarketInstrumentBinding {
            native_symbol: native_symbol.into(),
            asset_id: AssetId(asset_id),
            price_tick: 0.1,
            quantity_lot: 0.001,
        }]
        .into(),
        enabled: false,
        definition_version: 1,
    }
}

fn plugin(instance_id: &str, plugin_type: &str) -> PluginConfig {
    PluginConfig {
        instance_id: instance_id.into(),
        plugin_type: plugin_type.into(),
        config_schema_version: 1,
        config_version: 1,
        config: serde_json::json!({}),
        enabled: true,
        execution: PluginExecutionConfig::default(),
        subscription: PluginSubscriptionConfig::default(),
    }
}
