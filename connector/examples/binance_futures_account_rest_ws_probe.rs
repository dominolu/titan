//! Binance USD-M mainnet REST -> private-WebSocket field-semantics probe.
//!
//! Places one real post-only sell far above market (no fill expected), then observes both the
//! synchronous REST OrderChanged and the asynchronous private-stream OrderChanged carrying the
//! same client order id, cancels it, and confirms the terminal WebSocket fact. The probe exits
//! with zero open orders/positions on the symbol and prints every decoded account fact so the
//! exact wire field semantics can be inspected.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use connector::account_plugin::venue_account_factories;
use titan_account_plugin::*;
use titan_plugin_engine::{PluginError, PluginIdentity, ResourceScope, TraceContext};

use titan_account_plugin::event_flags;

/// Orders surfaced by a REST submit/cancel command carry the originating command id; full
/// reconcile snapshots carry the SNAPSHOT flag; anything else with a zero command id and no
/// SNAPSHOT flag must have arrived through the private user-data stream.
fn event_source(event: &OrderChangedV1) -> &'static str {
    if event.command_id != Id128::default() {
        "rest-command"
    } else if event.header.flags & event_flags::SNAPSHOT != 0 {
        "reconcile-snapshot"
    } else {
        "private-stream"
    }
}

fn is_private_stream_event(event: &OrderChangedV1) -> bool {
    event_source(event) == "private-stream"
}

struct EnvironmentSecret {
    value: String,
}

impl SecretProvider for EnvironmentSecret {
    fn resolve(
        &self,
        _: &SecretRef,
    ) -> Result<SecretValue, AccountConnectorError> {
        Ok(SecretValue::new(self.value.as_bytes().to_vec()))
    }
}

#[derive(Clone, Debug)]
struct RecordedEvent {
    event_type: String,
    payload: Vec<u8>,
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RecordedEvent>>,
}

impl AccountEventSink for RecordingSink {
    fn publish(&self, event_type: &str, payload: &[u8], _: TraceContext) -> Result<(), PluginError> {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(RecordedEvent {
                event_type: event_type.to_owned(),
                payload: payload.to_vec(),
            });
        Ok(())
    }
}

fn required_secret(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        anyhow::bail!("{name} must not be empty");
    }
    Ok(value)
}

fn id_text(id: Id128) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn client_hex_equal(id: Id128, text: &str) -> bool {
    id_text(id) == text.strip_prefix("0x").unwrap_or(text)
}

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .try_init();
    let api_key = required_secret("BINANCE_FUTURES_API_KEY")?;
    let api_secret = required_secret("BINANCE_FUTURES_API_SECRET")?;
    let symbol = std::env::var("BINANCE_FUTURES_PROBE_SYMBOL")
        .unwrap_or_else(|_| "XRPUSDT".to_owned())
        .to_uppercase();
    ensure!(
        symbol == "XRPUSDT",
        "probe is hard-configured for XRPUSDT precision"
    );

    let secret_ref = SecretRef::new("env://binance-futures-rest-ws-probe");
    let secret_provider: Arc<dyn SecretProvider> = Arc::new(EnvironmentSecret {
        value: format!("api_key = {api_key:?}\nsecret = {api_secret:?}\n"),
    });
    let factory = venue_account_factories()
        .into_iter()
        .find(|factory| factory.connector_type() == "binance-futures-account")
        .context("Binance Futures account factory is unavailable")?;

    let definition = AccountDefinition {
        account_key: Arc::from("binance-rest-ws-probe"),
        account_id: AccountId(1),
        connector_type: Arc::from("binance-futures-account"),
        credential_ref: secret_ref.clone(),
        connector_config: Arc::from(
            b"stream_url = \"wss://fstream.binance.com/ws\"\napi_url = \"https://fapi.binance.com\"\norder_prefix = \"\"\nsafety_timeout_ms = 0\n"
                .as_slice(),
        ),
        instruments: Arc::from([AccountInstrumentBinding {
            native_symbol: Arc::from(symbol.as_str()),
            asset_id: AssetId(1),
            price_tick: "0.0001".parse().unwrap(),
            quantity_lot: "0.1".parse().unwrap(),
            contract_multiplier: "1".parse().unwrap(),
        }]),
        currencies: Arc::from([AccountCurrencyBinding {
            native_currency: Arc::from("USDT"),
            currency_id: CurrencyId(1),
            amount_unit: "0.00000001".parse().unwrap(),
        }]),
        // ObserveAll keeps the REST reconcile from filtering the deterministic owner client id;
        // external-order handling is exercised by separate unit tests.
        ownership: OrderOwnershipPolicy::ObserveAll,
        shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
        enabled: true,
        definition_version: 1,
    };

    let sink = Arc::new(RecordingSink::default());
    let scope = ResourceScope::new(PluginIdentity::new(
        "titan.account.rest-ws-probe",
        "binance-generation-1",
    ));
    let connector = factory
        .create(
            &definition,
            AccountConnectorContext {
                account: AccountHandle {
                    account_id: AccountId(1),
                    generation: 1,
                },
                instruments: definition.instruments.clone(),
                currencies: definition.currencies.clone(),
                ownership: definition.ownership.clone(),
                account_stream: SourceStreamId(20),
                control_stream: SourceStreamId(21),
                event_publisher: AccountEventPublisher::from_sink(
                    AccountHandle {
                        account_id: AccountId(1),
                        generation: 1,
                    },
                    sink.clone(),
                ),
                resources: scope.handle(),
                secrets: ScopedSecretResolver::scoped(secret_ref, secret_provider),
                command_queue_capacity: 64,
            },
        )
        .context("create account connector")?;

    let result = run_probe(&symbol, &connector, sink.clone());
    let stop_result = connector.stop(Instant::now() + Duration::from_secs(5));
    drop(scope);
    result?;
    stop_result.context("stop account connector")?;
    Ok(())
}

fn run_probe(
    symbol: &str,
    connector: &Arc<dyn AccountConnector>,
    sink: Arc<RecordingSink>,
) -> Result<()> {
    connector.start().context("start account connector")?;
    wait_until(Duration::from_secs(20), || {
        connector.health().state == AccountLifecycle::Ready
    })
    .context("private stream and initial reconcile did not become ready")?;

    // GTX sell at 100 USDT, XRP mark ~1.44: post-only so it cannot fill.
    let asset_id = AssetId(1);
    let command_id = Id128([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    ]);
    let client_order_id = Id128([
        16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
    ]);
    let client_hex = id_text(client_order_id);
    let submit_receipt = connector
        .submit(SubmitOrderCommand {
            command_id,
            client_order_id: Some(client_order_id),
            asset_id,
            side: 2,
            order_type: 0,
            time_in_force: 1, // GTX post-only
            price_ticks: 1_000_000, // 100 / 0.0001
            quantity_lots: 1,        // 0.1 XRP
            trace: TraceContext {
                trace_id: 9001,
                causation_id: 0,
            },
        })
        .context("submit GTX order")?;
    println!("rest_receipt={submit_receipt:?}");
    let submit_ns = submit_receipt.accepted_at;

    let cancel_command_id = Id128([
        32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17,
    ]);
    let wait_for_stream = wait_until(Duration::from_secs(90), || {
        recorded_orders(&sink, &client_hex)
            .iter()
            .any(|event| {
                is_private_stream_event(event)
                    && event.header.exchange_ts >= submit_ns - 1_000_000_000
            })
    });
    if let Err(error) = wait_for_stream {
        // Fail-safe cleanup: never leave a resting order behind when the stream path is broken.
        let _ = connector.cancel(CancelOrderCommand {
            command_id: cancel_command_id,
            asset_id,
            client_order_id: Some(client_order_id),
            venue_order_id: None,
            trace: TraceContext {
                trace_id: 9002,
                causation_id: 9001,
            },
        });
        eprintln!("decoded_events={}", debug_dump_events(&sink));
        return Err(error).context(
            "private-stream OrderChanged carrying client id was not observed",
        );
    }

    let before_cancel = decode_order_summary(&sink, &client_hex);
    println!("order_facts_before_cancel={}", serde_json::to_string_pretty(&before_cancel)?);
    verify_rest_stream_agreement(&sink, &client_hex, 1)
        .context("NEW order facts disagree between REST and private stream")?;

    let cancel_receipt = connector
        .cancel(CancelOrderCommand {
            command_id: cancel_command_id,
            asset_id,
            client_order_id: Some(client_order_id),
            venue_order_id: None,
            trace: TraceContext {
                trace_id: 9002,
                causation_id: 9001,
            },
        })
        .context("cancel GTX order")?;
    println!("cancel_receipt={cancel_receipt:?}");
    let cancel_ns = cancel_receipt.accepted_at;

    let wait_for_cancel = wait_until(Duration::from_secs(90), || {
        recorded_orders(&sink, &client_hex)
            .iter()
            .any(|event| {
                event.status == 4
                    && is_private_stream_event(event)
                    && event.header.exchange_ts >= cancel_ns - 1_000_000_000
            })
    });
    if let Err(error) = wait_for_cancel {
        eprintln!("decoded_events={}", debug_dump_events(&sink));
        return Err(error).context(
            "private-stream terminal CANCELED OrderChanged was not observed",
        );
    }
    verify_rest_stream_agreement(&sink, &client_hex, 4)
        .context("CANCELED order facts disagree between REST and private stream")?;

    let operation = connector
        .reconcile(ReconcileScope::Full)
        .context("schedule final full reconcile")?;
    wait_until(Duration::from_secs(15), || {
        connector.operation(operation).state != OperationState::Pending
    })
    .context("final reconcile did not complete")?;
    ensure!(
        connector.operation(operation).state == OperationState::Succeeded,
        "final reconcile failed"
    );

    let positions = connector.positions(PositionFilter::default())?;
    ensure!(
        positions.items.iter().all(|item| item.quantity_lots == 0),
        "non-zero position after cancel"
    );
    let orders = connector.orders(OrderFilter {
        asset_id: None,
        include_final: true,
    })?;
    ensure!(
        orders.items.iter().all(|item| item.status != 1 && item.status != 5),
        "open order still present after cancel"
    );

    let after_cancel = decode_order_summary(&sink, &client_hex);
    println!(
        "order_facts_after_cancel={}",
        serde_json::to_string_pretty(&after_cancel)?
    );
    println!(
        "final={}",
        serde_json::to_string_pretty(&serde_json::json!({
            "symbol": symbol,
            "ready": connector.health().state == AccountLifecycle::Ready,
            "open_order_count": orders.items.len(),
            "position_lots": positions.items.iter().map(|i| i.quantity_lots).sum::<i64>(),
            "ws_order_fact_count": recorded_orders(&sink, &client_hex).len(),
        }))?
    );
    Ok(())
}

fn verify_rest_stream_agreement(
    sink: &RecordingSink,
    client_hex: &str,
    status: u8,
) -> Result<()> {
    let orders = recorded_orders(sink, client_hex);
    let stream = orders
        .iter()
        .find(|event| is_private_stream_event(event) && event.status == status)
        .context("matching private-stream fact not recorded")?;
    let rest = orders
        .iter()
        .find(|event| event_source(event) == "rest-command" && event.status == status)
        .context("matching REST command fact not recorded")?;
    ensure!(
        rest.client_order_id == stream.client_order_id,
        "client order id mismatch: REST {} vs stream {}",
        id_text(rest.client_order_id),
        id_text(stream.client_order_id)
    );
    ensure!(
        rest.venue_order_id == stream.venue_order_id,
        "venue order id mismatch: REST {} vs stream {}",
        id_text(rest.venue_order_id),
        id_text(stream.venue_order_id)
    );
    ensure!(
        rest.header.exchange_ts == stream.header.exchange_ts,
        "exchange ts mismatch: REST {} vs stream {}",
        rest.header.exchange_ts,
        stream.header.exchange_ts
    );
    ensure!(
        stream.header.exchange_ts >= 1_700_000_000_000_000_000,
        "stream exchange_ts does not look like ns epoch ({})",
        stream.header.exchange_ts
    );
    Ok(())
}

fn recorded_orders(
    sink: &RecordingSink,
    client_hex: &str,
) -> Vec<OrderChangedV1> {
    sink.events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|event| event.event_type == ORDER_CHANGED_EVENT)
        .filter_map(|event| OrderChangedV1::decode(&event.payload).ok())
        .filter(|event| client_hex_equal(event.client_order_id, client_hex))
        .collect()
}

fn decode_order_summary(
    sink: &RecordingSink,
    client_hex: &str,
) -> Vec<serde_json::Value> {
    recorded_orders(sink, client_hex)
        .into_iter()
        .map(|event| {
            serde_json::json!({
                "schema": "order-changed-v1",
                "source": event_source(&event),
                "account_version": event.header.account_version,
                "exchange_ts_ns": event.header.exchange_ts,
                "receive_ts_ns": event.header.receive_ts,
                "client_order_id": id_text(event.client_order_id),
                "venue_order_id_hex": id_text(event.venue_order_id),
                "flags": event.header.flags,
                "status": event.status,
                "side": event.side,
                "order_type": event.order_type,
                "time_in_force": event.time_in_force,
                "price_ticks": event.price_ticks,
                "quantity_lots": event.quantity_lots,
                "filled_quantity_lots": event.filled_quantity_lots,
                "average_price_ticks": event.average_price_ticks,
            })
        })
        .collect()
}

fn debug_dump_events(sink: &RecordingSink) -> serde_json::Value {
    let events = sink.events.lock().unwrap_or_else(|p| p.into_inner());
    serde_json::json!({
        "by_type": events.iter().fold(BTreeMap::<String, usize>::new(), |mut counts, event| {
            *counts.entry(event.event_type.clone()).or_default() += 1;
            counts
        }),
        "order_client_ids": events.iter()
            .filter(|event| event.event_type == ORDER_CHANGED_EVENT)
            .filter_map(|event| OrderChangedV1::decode(&event.payload).ok())
            .map(|event| serde_json::json!({
                "client_order_id": id_text(event.client_order_id),
                "status": event.status,
                "source": event_source(&event),
                "flags": event.header.flags,
            }))
            .collect::<Vec<_>>(),
    })
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
