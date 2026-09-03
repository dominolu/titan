use std::{
    collections::BTreeMap,
    env,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use connector::account_plugin::venue_account_factories;
use serde_json::json;
use titan_account_plugin::{
    AccountConnectorContext, AccountConnectorFactory, AccountCurrencyBinding, AccountDefinition,
    AccountEventPublisher, AccountEventSink, AccountHandle, AccountId, AccountInstrumentBinding,
    AccountLifecycle, AccountSnapshotState, AssetId, CurrencyId, DecimalUnit, OperationState,
    OrderOwnershipPolicy, PositionFilter, RECONCILE_COMPLETED_EVENT, ReconcileScope,
    STREAM_STATE_CHANGED_EVENT, ScopedSecretResolver, SecretProvider, SecretRef, SecretValue,
    ShutdownOrderPolicy, SourceStreamId,
};
use titan_connector_loader::{load_connector_plugins, package_plugin_library};
use titan_plugin_engine::{PluginError, PluginIdentity, ResourceScope, TraceContext};

struct EnvironmentSecret {
    value: String,
}

impl SecretProvider for EnvironmentSecret {
    fn resolve(
        &self,
        _: &SecretRef,
    ) -> Result<SecretValue, titan_account_plugin::AccountConnectorError> {
        Ok(SecretValue::new(self.value.as_bytes().to_vec()))
    }
}

#[derive(Default)]
struct RecordingSink {
    counts: Mutex<BTreeMap<String, usize>>,
}

impl AccountEventSink for RecordingSink {
    fn publish(&self, event_type: &str, _: &[u8], _: TraceContext) -> Result<(), PluginError> {
        *self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(event_type.to_owned())
            .or_default() += 1;
        Ok(())
    }
}

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .try_init();
    let api_key = required_secret("BINANCE_FUTURES_API_KEY")?;
    let api_secret = required_secret("BINANCE_FUTURES_API_SECRET")?;
    let secret_ref = SecretRef::new("env://binance-futures-probe");
    let secret_provider: Arc<dyn SecretProvider> = Arc::new(EnvironmentSecret {
        value: format!("api_key = {api_key:?}\nsecret = {api_secret:?}\n"),
    });
    let package_root = env::var_os("BINANCE_FUTURES_PLUGIN_LIBRARY")
        .map(|library| {
            let root =
                env::temp_dir().join(format!("titan-binance-account-live-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            let manifest = package_plugin_library(library, &root)?;
            Ok::<_, titan_plugin_engine::PluginError>((root, manifest))
        })
        .transpose()?;
    let loaded_plugins = package_root
        .as_ref()
        .map(|(_, manifest)| load_connector_plugins([manifest]))
        .transpose()?;
    let factory: Arc<dyn AccountConnectorFactory> = if let Some(plugins) = &loaded_plugins {
        plugins
            .first()
            .context("dynamic Binance plugin was not loaded")?
            .account_factory
            .clone()
    } else {
        venue_account_factories()
            .into_iter()
            .find(|factory| factory.connector_type() == "binance-futures-account")
            .context("Binance Futures account factory is unavailable")?
    };
    let mut cycles = Vec::new();
    for generation in 1..=2 {
        cycles.push(run_cycle(
            generation,
            secret_ref.clone(),
            secret_provider.clone(),
            factory.clone(),
        )?);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "exchange": "binance-usdm",
            "cycles": cycles,
            "reconnected": true,
            "dynamic_plugin": loaded_plugins.is_some(),
        }))?
    );
    drop(factory);
    drop(loaded_plugins);
    if let Some((root, _)) = package_root {
        let _ = std::fs::remove_dir_all(root);
    }
    Ok(())
}

fn run_cycle(
    generation: u64,
    secret_ref: SecretRef,
    secret_provider: Arc<dyn SecretProvider>,
    factory: Arc<dyn AccountConnectorFactory>,
) -> Result<serde_json::Value> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _guard = runtime.enter();
    let definition = AccountDefinition {
        account_key: Arc::from("binance-live-probe"),
        account_id: AccountId(1),
        connector_type: Arc::from("binance-futures-account"),
        credential_ref: secret_ref.clone(),
        connector_config: Arc::from(
            b"stream_url = \"wss://fstream.binance.com/ws\"\napi_url = \"https://fapi.binance.com\"\norder_prefix = \"titanrt\"\nsafety_timeout_ms = 0\n"
                .as_slice(),
        ),
        instruments: Arc::from([AccountInstrumentBinding {
            native_symbol: Arc::from("XRPUSDT"),
            asset_id: AssetId(1),
            price_tick: DecimalUnit::from_str("0.0001").unwrap(),
            quantity_lot: DecimalUnit::from_str("0.1").unwrap(),
            contract_multiplier: DecimalUnit::from_str("1").unwrap(),
        }]),
        currencies: Arc::from([AccountCurrencyBinding {
            native_currency: Arc::from("USDT"),
            currency_id: CurrencyId(1),
            amount_unit: DecimalUnit::from_str("0.00000001").unwrap(),
        }]),
        ownership: OrderOwnershipPolicy::ManagedOnly {
            client_id_prefix: Arc::from("titanrt"),
        },
        shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
        enabled: true,
        definition_version: generation,
    };
    let sink = Arc::new(RecordingSink::default());
    let scope = ResourceScope::new(PluginIdentity::new(
        "titan.account.live-probe",
        format!("generation-{generation}"),
    ));
    let connector = factory
        .create(
            &definition,
            AccountConnectorContext {
                account: AccountHandle {
                    account_id: AccountId(1),
                    generation,
                },
                instruments: definition.instruments.clone(),
                currencies: definition.currencies.clone(),
                ownership: definition.ownership.clone(),
                account_stream: SourceStreamId(10),
                control_stream: SourceStreamId(11),
                event_publisher: AccountEventPublisher::from_sink(
                    AccountHandle {
                        account_id: AccountId(1),
                        generation,
                    },
                    sink.clone(),
                ),
                resources: scope.handle(),
                secrets: ScopedSecretResolver::scoped(secret_ref, secret_provider),
                command_queue_capacity: 64,
            },
        )
        .context("create account connector")?;
    connector.start().context("start account connector")?;
    wait_until(Duration::from_secs(20), || {
        connector.health().state == AccountLifecycle::Ready
    })
    .context("private stream and initial reconcile did not become ready")?;

    let operation = connector
        .reconcile(ReconcileScope::Full)
        .context("schedule explicit full reconcile")?;
    wait_until(Duration::from_secs(15), || {
        connector.operation(operation).state != OperationState::Pending
    })
    .context("explicit reconcile did not complete")?;
    ensure!(
        connector.operation(operation).state == OperationState::Succeeded,
        "explicit reconcile failed"
    );
    wait_until(Duration::from_secs(5), || {
        connector.health().state == AccountLifecycle::Ready
    })
    .context("account did not return to ready after reconcile")?;

    let positions = connector
        .positions(PositionFilter::default())
        .context("position snapshot")?;
    let balances = connector.balances().context("balance snapshot")?;
    ensure!(
        positions.state == AccountSnapshotState::Ready,
        "position snapshot is not ready"
    );
    ensure!(
        balances.state == AccountSnapshotState::Ready,
        "balance snapshot is not ready"
    );
    ensure!(
        positions
            .items
            .iter()
            .all(|position| position.quantity_lots == 0),
        "non-zero position after live roundtrip"
    );
    let diagnostics = connector.diagnostics();
    connector
        .stop(Instant::now() + Duration::from_secs(5))
        .context("stop account connector")?;

    let event_counts = sink
        .counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    ensure!(
        event_counts
            .get(STREAM_STATE_CHANGED_EVENT)
            .copied()
            .unwrap_or(0)
            >= 1,
        "ready stream-state event was not published"
    );
    ensure!(
        event_counts
            .get(RECONCILE_COMPLETED_EVENT)
            .copied()
            .unwrap_or(0)
            >= 1,
        "reconcile completion event was not published"
    );
    drop(scope);
    Ok(json!({
        "generation": generation,
        "ready": true,
        "stopped": connector.health().state == AccountLifecycle::Stopped,
        "position_count": positions.items.len(),
        "balance_count": balances.items.len(),
        "account_epoch": diagnostics.account_epoch,
        "account_version": diagnostics.account_version,
        "event_counts": event_counts,
    }))
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("deadline exceeded")
}

fn required_secret(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}
