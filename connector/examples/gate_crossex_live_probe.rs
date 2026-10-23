//! Controlled mainnet acceptance probe. Credentials are read from a private file on the live
//! server. The default action is read-only; no startup code submits orders or changes modes.
#[path = "gate_crossex_live_probe/passive.rs"]
mod passive;

use anyhow::{Context, Result, ensure};
use connector::{
    api::BrokerApi,
    connector::{
        AccountPublication, Connector, ConnectorBuilder, DirectPublication, PublishEvent,
        direct_publish_sender,
    },
    gatecrossex::{Config, GateCrossEx, GateCrossExClient, GateCrossExWebSocket, Query},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, time::Duration};
use tokio::sync::mpsc;

fn checkpoint(name: &str, result: &Result<Value>) {
    match result {
        Ok(value) => println!("{}", json!({"check":name,"ok":true,"result":value})),
        Err(error) => println!(
            "{}",
            json!({"check":name,"ok":false,"error":error.to_string()})
        ),
    }
}
fn summarize(value: &Value) -> Value {
    match value {
        Value::Array(v) => json!({"rows":v.len()}),
        Value::Object(v) => json!({"fields":v.keys().collect::<Vec<_>>()}),
        _ => value.clone(),
    }
}
async fn private_ready(rx: &mut mpsc::UnboundedReceiver<Value>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(value) = rx.recv().await {
            if value["kind"] == "ready" {
                return Ok(());
            }
            if value["kind"] == "error" {
                println!("{}", json!({"private_stream_event":value}));
            }
        }
        anyhow::bail!("private stream stopped")
    })
    .await
    .context("private ready timeout")?
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .try_init();
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        (2..=4).contains(&args.len()),
        "usage: gate_crossex_live_probe PRIVATE_CONFIG [NATIVE_SYMBOL]"
    );
    ensure!(
        args.get(3)
            .is_none_or(|mode| mode == "--plan" || mode == "--passive"),
        "unknown probe action"
    );
    let path = &args[1];
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "credential file must be private"
        );
    }
    let config_text = fs::read_to_string(path).context("read private config")?;
    let config: Config =
        toml::from_str(&config_text).map_err(|_| anyhow::anyhow!("invalid private config"))?;
    ensure!(
        config.rest_url == "https://api.gateio.ws/api/v4",
        "unexpected mainnet endpoint"
    );
    let mut api = GateCrossExClient::new(&config.rest_url, &config.api_key, &config.secret)?
        .with_websocket_urls(&config.public_ws_url, &config.private_ws_url)?;
    api = api.with_market_quantity_multipliers(&config.market_quantity_multipliers)?;
    if let Some(exchange) = &config.exchange_type {
        api = api.with_exchange_type(exchange)?;
    }
    let symbol = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "GATE_FUTURE_BTC_USDT".into());
    let mut failures = Vec::new();
    let mut report = |name: &str, result: Result<Value>| {
        checkpoint(name, &result);
        if result.is_err() {
            failures.push(name.to_owned());
        }
    };
    let raw = api.get_account_raw(&Query::new()).await?;
    let assets = raw["assets"].as_array().context("assets array missing")?;
    report(
        "account_raw",
        Ok(
            json!({"account_mode":raw["account_mode"],"position_mode":raw["position_mode"],"available_margin":raw["available_margin"],"margin_balance":raw["margin_balance"],"assets":assets.iter().map(|a|json!({"exchange_type":a["exchange_type"],"coin":a["coin"],"balance":a["balance"],"equity":a["equity"]})).collect::<Vec<_>>()}),
        ),
    );
    report("account_unified",api.get_account().await.map(|v|json!({"wallet":v.total_wallet_balance,"margin":v.total_margin_balance,"available":v.available_balance,"currencies":v.balances.iter().map(|b|&b.asset).collect::<Vec<_>>()})).map_err(Into::into));
    let position = api.list_positions(&Query::new()).await?;
    report("positions_raw", Ok(position.clone()));
    report(
        "margin_positions",
        api.list_margin_positions(&Query::new())
            .await
            .map(|v| summarize(&v))
            .map_err(Into::into),
    );
    let opened = api.list_open_orders(&Query::new()).await?;
    report(
        "open_orders",
        Ok(
            json!({"rows":opened.as_array().map_or(0,Vec::len),"by_symbol":opened.as_array().unwrap_or(&Vec::new()).iter().fold(BTreeMap::<String,usize>::new(),|mut out,v|{*out.entry(v["symbol"].as_str().unwrap_or("").into()).or_default()+=1;out})}),
        ),
    );
    let mut connector = GateCrossEx::build_from(&config_text)?;
    connector.register_account(symbol.clone());
    let broker = connector.broker_api().context("broker api missing")?;
    report(
        "positions_unified",
        broker
            .get_positions(None)
            .await
            .map(|v| json!(v))
            .map_err(Into::into),
    );
    report(
        "leverage",
        api.get_leverage(&symbol)
            .await
            .map(|v| json!(v))
            .map_err(Into::into),
    );
    report(
        "fee",
        api.get_fee_rates(&symbol)
            .await
            .map(|v| json!(v))
            .map_err(Into::into),
    );
    report(
        "transfer_coins",
        api.list_transfer_coins(&Query::new())
            .await
            .map(|v| summarize(&v))
            .map_err(Into::into),
    );
    report(
        "risk_limits",
        api.list_rule_risk_limits(std::slice::from_ref(&symbol))
            .await
            .map(|v| summarize(&v))
            .map_err(Into::into),
    );
    let query = vec![("limit".into(), "3".into()), ("page".into(), "1".into())];
    report(
        "transfers",
        api.list_transfers(&query)
            .await
            .map(|v| summarize(&v))
            .map_err(Into::into),
    );
    report(
        "account_book",
        api.list_account_book(&query)
            .await
            .map(|v| summarize(&v))
            .map_err(Into::into),
    );
    report(
        "order_history",
        api.get_order_history(&symbol, 3)
            .await
            .map(|v| json!({"rows":v.len()}))
            .map_err(Into::into),
    );
    report(
        "fills",
        api.get_fills(&symbol, 3)
            .await
            .map(|v| json!({"rows":v.len()}))
            .map_err(Into::into),
    );
    report(
        "income",
        api.get_income_history(&symbol, 3)
            .await
            .map(|v| json!({"rows":v.len()}))
            .map_err(Into::into),
    );
    let ticker = api.get_ticker(&symbol).await?;
    let book = api.get_order_book(&symbol, 5).await?;
    let rules = api.list_rule_symbols().await?;
    let rule = rules
        .as_array()
        .context("rules missing")?
        .iter()
        .find(|v| v["symbol"] == symbol)
        .context("target rule missing")?;
    report(
        "market_and_rule",
        Ok(json!({"ticker":ticker,"book":book,"rule":rule})),
    );
    report(
        "funding_rate",
        api.get_funding_rate(&symbol)
            .await
            .map(|v| json!(v))
            .map_err(Into::into),
    );
    report(
        "open_interest",
        api.get_open_interest(&symbol)
            .await
            .map(|v| json!(v))
            .map_err(Into::into),
    );
    report(
        "account_runtime_bootstrap",
        runtime_bootstrap(path, &symbol, &api).await,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    connector.run_account(direct_publish_sender(move |publication| {
        let event = match publication {
            DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
                Some(json!({"kind":"ready"}))
            }
            DirectPublication::Account(AccountPublication::Error(_)) => {
                Some(json!({"kind":"error"}))
            }
            _ => None,
        };
        if let Some(value) = event {
            let _ = tx.send(value);
        }
    }));
    report(
        "private_factory_ready",
        private_ready(&mut rx).await.map(|_| json!({"ready":true})),
    );
    connector.request_snapshot(symbol.clone());
    report(
        "private_factory_reconnect",
        private_ready(&mut rx)
            .await
            .map(|_| json!({"ready_after_reconnect":true})),
    );
    drop(connector); // Never invoke the legacy shutdown hook, which can cancel venue-wide orders.
    let native = GateCrossExWebSocket::connect(&api, true).await?;
    for channel in [
        "order",
        "position",
        "asset",
        "margin_position",
        "margin_interest",
    ] {
        report(
            &format!("native_private_{channel}"),
            native
                .subscribe_confirmed(channel, &["!all".into()])
                .await
                .map(|v| json!({"code":v.result["code"]}))
                .map_err(Into::into),
        );
    }
    drop(native);
    println!(
        "{}",
        json!({"action":"read_only","symbol":symbol,"mutations":0,"failed_checks":failures})
    );
    ensure!(
        failures.is_empty(),
        "some read-only acceptance checks failed"
    );
    if let Some(action) = args.get(3) {
        passive::run(&api, &config_text, path, &symbol, action == "--passive").await?;
    }
    Ok(())
}

fn decimal_unit(value: f64) -> Result<titan_account_service::DecimalUnit> {
    ensure!(value.is_finite() && value > 0.0, "invalid instrument unit");
    let text = value.to_string();
    let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let coefficient = format!("{whole}{fraction}").parse()?;
    titan_account_service::DecimalUnit::new(coefficient, fraction.len().try_into()?)
        .map_err(|_| anyhow::anyhow!("invalid decimal unit"))
}
async fn runtime_bootstrap(path: &str, symbol: &str, api: &GateCrossExClient) -> Result<Value> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;
    use titan_account_service as account;
    use titan_core_types::{ComponentIdentity, CoreError, ResourceScope, TraceContext};
    struct Sink(AtomicUsize);
    impl account::AccountEventSink for Sink {
        fn publish(
            &self,
            _: &str,
            _: &[u8],
            _: TraceContext,
        ) -> std::result::Result<(), CoreError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    let instrument = api
        .get_instruments()
        .await?
        .into_iter()
        .find(|v| v.symbol == symbol)
        .context("instrument missing")?;
    let handle = account::AccountHandle {
        account_id: account::AccountId(900001),
        generation: 1,
    };
    let binding = account::AccountInstrumentBinding {
        native_symbol: Arc::from(symbol),
        asset_id: account::AssetId(1),
        price_tick: decimal_unit(instrument.tick_size)?,
        quantity_lot: decimal_unit(instrument.lot_size)?,
        contract_multiplier: account::DecimalUnit::new(1, 0).unwrap(),
        linear_quote_currency: None,
    };
    let currency = account::AccountCurrencyBinding {
        native_currency: Arc::from("USDT"),
        currency_id: account::CurrencyId(1),
        amount_unit: account::DecimalUnit::new(1, 8).unwrap(),
    };
    let credential_path = fs::canonicalize(path)?;
    let reference = account::SecretRef::new(format!(
        "secret://file/{}",
        credential_path
            .file_name()
            .context("credential filename")?
            .to_str()
            .context("UTF8 filename")?
    ));
    let sink = Arc::new(Sink(AtomicUsize::new(0)));
    let resources = ResourceScope::new(ComponentIdentity::new(
        "connector",
        "gate-crossex-live-probe",
    ));
    let definition = account::AccountDefinition {
        account_key: Arc::from("gate-crossex-live-readonly"),
        account_id: handle.account_id,
        connector_type: Arc::from("gate-crossex-account"),
        credential_ref: reference.clone(),
        connector_config: Arc::from(&b""[..]),
        instruments: vec![binding].into(),
        currencies: vec![currency].into(),
        ownership: account::OrderOwnershipPolicy::ObserveAll,
        shutdown_order_policy: account::ShutdownOrderPolicy::LeaveOpen,
        enabled: true,
        definition_version: 1,
    };
    let context = account::AccountConnectorContext {
        account: handle,
        instruments: definition.instruments.clone(),
        currencies: definition.currencies.clone(),
        ownership: definition.ownership.clone(),
        account_stream: account::SourceStreamId(1),
        control_stream: account::SourceStreamId(2),
        event_publisher: account::AccountEventPublisher::from_sink(handle, sink.clone()),
        resources: resources.handle(),
        secrets: account::ScopedSecretResolver::scoped(
            reference,
            Arc::new(account::DirectorySecretProvider::new(
                credential_path.parent().context("credential directory")?,
            )?),
        ),
    };
    let factory = connector::account_runtime::venue_account_factories()
        .into_iter()
        .find(|v| v.connector_type() == "gate-crossex-account")
        .context("account factory missing")?;
    let runtime = factory.create(&definition, context)?;
    runtime.start()?;
    let result=tokio::time::timeout(Duration::from_secs(40),async {
        loop {
            let health=runtime.health();
            if health.state==account::AccountLifecycle::Ready {
                let snapshot=runtime.recovery_snapshot()?;
                return Ok(json!({"state":"Ready","orders":snapshot.orders.items.len(),"positions":snapshot.positions.items.len(),"balances":snapshot.balances.items.len(),"events":sink.0.load(Ordering::Relaxed)}));
            }
            if health.state==account::AccountLifecycle::Failed {anyhow::bail!("{}",health.message);}
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.context("AccountRuntime bootstrap timeout").and_then(|v:Result<Value>|v);
    let stopped = runtime.stop(Instant::now() + Duration::from_secs(10));
    result.and_then(|v| {
        stopped?;
        Ok(v)
    })
}
