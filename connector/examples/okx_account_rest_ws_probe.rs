//! OKX mainnet REST -> private-WebSocket field-semantics probe.
//!
//! Mirrors `binance_futures_account_rest_ws_probe.rs` for the OKX V5 swap
//! account path (B-02 acceptance): opens the account connector through the
//! real plugin factory, waits for private-stream READY plus Full reconcile,
//! places one real post-only sell far above market (no fill expected), then
//! cancels it. Every decoded account fact is printed so REST and private-stream
//! field semantics can be compared directly:
//!
//! - client order id (32-hex clOrdId) must be identical on both paths;
//! - venue order id (ordId) from the REST submit response must match the
//!   private-stream fact;
//! - private-stream exchange_ts (OKX `uTime`, ms) must equal the REST order
//!   record `uTime` observed through a Full reconcile snapshot. OKX order
//!   submit/cancel responses carry no timestamp, so the REST *command* fact
//!   has exchange_ts == 0 by design; parity is judged against the reconcile
//!   snapshot instead.

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

fn is_reconcile_snapshot_event(event: &OrderChangedV1) -> bool {
    event_source(event) == "reconcile-snapshot"
}

struct EnvironmentSecret {
    value: String,
}

impl SecretProvider for EnvironmentSecret {
    fn resolve(&self, _: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
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
    fn publish(
        &self,
        event_type: &str,
        payload: &[u8],
        _: TraceContext,
    ) -> Result<(), PluginError> {
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

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn id_text(id: Id128) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn client_hex_equal(id: Id128, text: &str) -> bool {
    id_text(id) == text.strip_prefix("0x").unwrap_or(text)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .try_init();
    let api_key = required_env("OKX_API_KEY")?;
    let api_secret = required_env("OKX_SECRET_KEY")?;
    let passphrase = required_env("OKX_PASSPHRASE")?;
    let symbol = std::env::var("OKX_PROBE_SYMBOL")
        .unwrap_or_else(|_| "XRP-USDT-SWAP".to_owned())
        .to_uppercase();
    // Post-only sell above the best ask so it rests. OKX price limits are per-instrument
    // (buyLmt/sellLmt from /public/price-limit), so the resting price is derived from live
    // metadata below instead of assuming any fixed percentage.
    let price_margin_pct: f64 = std::env::var("OKX_PROBE_PRICE_MARGIN_PCT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.8);
    let quantity_lots: i64 = std::env::var("OKX_PROBE_QUANTITY_LOTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let td_mode = std::env::var("OKX_PROBE_TD_MODE").unwrap_or_else(|_| "cross".to_owned());
    let pos_side = std::env::var("OKX_PROBE_POS_SIDE").unwrap_or_default();
    let simulated = std::env::var("OKX_PROBE_SIMULATED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Live instrument metadata: tick/lot precision, per-instrument price limit and the
    // current best ask. Credentials stay empty — these are all public no-auth endpoints.
    let public_client = connector::okx::rest::OkxClient::new("https://www.okx.com", "", "", "");
    let instrument = public_client
        .get_instruments(&symbol)
        .await
        .context("fetch OKX instrument metadata")?;
    let price_tick: f64 = instrument
        .tick_sz
        .parse()
        .context("instrument tickSz is not a number")?;
    ensure!(price_tick > 0.0, "instrument tickSz must be positive");
    let quantity_lot = instrument.lot_sz.clone();
    let min_qty: f64 = instrument.min_sz.parse().unwrap_or(0.0);
    ensure!(
        quantity_lots as f64 * quantity_lot.parse::<f64>()? >= min_qty,
        "order size {quantity_lots} x {quantity_lot} is below OKX minSz {min_qty}"
    );
    let price_limit = public_client
        .get_price_limit(&symbol)
        .await
        .context("fetch OKX price limit")?;
    let buy_lmt: f64 = price_limit
        .buy_lmt
        .parse()
        .context("buyLmt is not a number")?;
    let book = public_client
        .get_books(&symbol, 1)
        .await
        .context("fetch OKX order book")?;
    let best_ask: f64 = book
        .asks
        .first()
        .and_then(|level| level.first())
        .and_then(|px| px.parse().ok())
        .context("order book has no best ask")?;
    // Resting price: a small configurable margin over the best ask, capped by the
    // per-instrument buyLmt (conservative upper bound for resting sells), floored to the
    // instrument tick grid.
    let raw_price = best_ask * (1.0 + price_margin_pct / 100.0);
    let price_ticks = (((raw_price.min(buy_lmt - price_tick)) / price_tick).floor() as i64)
        .max(1);
    let price_usdt = price_ticks as f64 * price_tick;
    ensure!(price_usdt > best_ask, "derived resting price crosses the book");
    println!(
        "derived_order_price symbol={symbol} best_ask={best_ask} buy_lmt={buy_lmt} tick={price_tick} margin_pct={price_margin_pct} price={price_usdt} quantity_lot={quantity_lot}"
    );

    let secret_ref = SecretRef::new("env://okx-rest-ws-probe");
    let secret_provider: Arc<dyn SecretProvider> = Arc::new(EnvironmentSecret {
        value: format!(
            "api_key = {api_key:?}\nsecret = {api_secret:?}\npassphrase = {passphrase:?}\n"
        ),
    });
    let factory = venue_account_factories()
        .into_iter()
        .find(|factory| factory.connector_type() == "okx-account")
        .context("OKX account factory is unavailable")?;

    let mut connector_config = format!(
        "rest_url = \"https://www.okx.com\"\npublic_ws_url = \"wss://ws.okx.com:8443/ws/v5/public\"\nprivate_ws_url = \"wss://ws.okx.com:8443/ws/v5/private\"\nsimulated = {simulated}\ntd_mode = {td_mode:?}\norder_prefix = \"\"\nsafety_timeout_ms = 0\n"
    );
    if !pos_side.is_empty() {
        connector_config.push_str(&format!("pos_side = {pos_side:?}\n"));
    }

    let definition = AccountDefinition {
        account_key: Arc::from("okx-rest-ws-probe"),
        account_id: AccountId(1),
        connector_type: Arc::from("okx-account"),
        credential_ref: secret_ref.clone(),
        connector_config: Arc::from(connector_config.as_bytes()),
        instruments: Arc::from([AccountInstrumentBinding {
            native_symbol: Arc::from(symbol.as_str()),
            asset_id: AssetId(1),
            price_tick: format!("{price_tick}").parse().unwrap(),
            quantity_lot: quantity_lot.parse().unwrap(),
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
        "titan.account.okx-rest-ws-probe",
        "okx-generation-1",
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
                account_stream: SourceStreamId(30),
                control_stream: SourceStreamId(31),
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

    let result = run_probe(
        &symbol,
        price_ticks,
        quantity_lots,
        &connector,
        sink.clone(),
    );
    let stop_result = connector.stop(Instant::now() + Duration::from_secs(5));
    drop(scope);
    result?;
    stop_result.context("stop account connector")?;
    Ok(())
}

fn run_probe(
    symbol: &str,
    price_ticks: i64,
    quantity_lots: i64,
    connector: &Arc<dyn AccountConnector>,
    sink: Arc<RecordingSink>,
) -> Result<()> {
    connector.start().context("start account connector")?;
    wait_until(Duration::from_secs(30), || {
        connector.health().state == AccountLifecycle::Ready
    })
    .context("private stream and initial reconcile did not become ready")?;
    println!("ready state reached");

    let asset_id = AssetId(1);
    let command_id = Id128([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let client_order_id = Id128([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
    let client_hex = id_text(client_order_id);
    let submit_receipt = connector
        .submit(SubmitOrderCommand {
            command_id,
            client_order_id: Some(client_order_id),
            asset_id,
            side: 2,
            order_type: 0,
            time_in_force: 1, // GTX post-only
            price_ticks,
            quantity_lots,
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
        recorded_orders(&sink, &client_hex).iter().any(|event| {
            is_private_stream_event(event) && event.header.exchange_ts >= submit_ns - 1_000_000_000
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
        return Err(error)
            .context("private-stream OrderChanged carrying client id was not observed");
    }

    let before_cancel = decode_order_summary(&sink, &client_hex);
    println!(
        "order_facts_before_cancel={}",
        serde_json::to_string_pretty(&before_cancel)?
    );

    // REST order-record parity: a Full reconcile snapshots the resting order through
    // GET /api/v5/trade/order (or open-orders) and must agree with the private-stream fact.
    let reconcile_operation = connector
        .reconcile(ReconcileScope::Full)
        .context("schedule pre-cancel full reconcile")?;
    wait_until(Duration::from_secs(30), || {
        connector.operation(reconcile_operation).state != OperationState::Pending
    })
    .context("pre-cancel reconcile did not complete")?;
    ensure!(
        connector.operation(reconcile_operation).state == OperationState::Succeeded,
        "pre-cancel reconcile failed"
    );
    verify_rest_stream_agreement(&sink, &client_hex, 1)
        .context("NEW order facts disagree between REST reconcile and private stream")?;

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
        recorded_orders(&sink, &client_hex).iter().any(|event| {
            event.status == 4
                && is_private_stream_event(event)
                && event.header.exchange_ts >= cancel_ns - 1_000_000_000
        })
    });
    if let Err(error) = wait_for_cancel {
        eprintln!("decoded_events={}", debug_dump_events(&sink));
        return Err(error)
            .context("private-stream terminal CANCELED OrderChanged was not observed");
    }

    let reconcile_operation = connector
        .reconcile(ReconcileScope::Full)
        .context("schedule post-cancel full reconcile")?;
    wait_until(Duration::from_secs(30), || {
        connector.operation(reconcile_operation).state != OperationState::Pending
    })
    .context("post-cancel reconcile did not complete")?;
    ensure!(
        connector.operation(reconcile_operation).state == OperationState::Succeeded,
        "post-cancel reconcile failed"
    );
    verify_rest_stream_agreement(&sink, &client_hex, 4)
        .context("CANCELED order facts disagree between REST reconcile and private stream")?;

    let positions = connector.positions(PositionFilter::default())?;
    let position_lots: i64 = positions
        .items
        .iter()
        .filter(|item| item.quantity_lots != 0)
        .map(|item| item.quantity_lots)
        .sum();
    if position_lots != 0 {
        // The post-only sell may legitimately fill while the probe waits (OKX price limits
        // force the resting order near the touch); close the residual short with a market
        // buy so the account ends flat.
        println!("position_lots_after_cancel={position_lots}; closing with market buy");
        let close_receipt = connector
            .submit(SubmitOrderCommand {
                command_id: Id128([64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79]),
                client_order_id: Some(Id128([
                    79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64,
                ])),
                asset_id,
                side: 1,
                order_type: 1, // market
                time_in_force: 0,
                price_ticks: 0,
                quantity_lots: position_lots,
                trace: TraceContext {
                    trace_id: 9003,
                    causation_id: 9002,
                },
            })
            .context("submit market close order")?;
        println!("close_receipt={close_receipt:?}");
        let close_operation = connector
            .reconcile(ReconcileScope::Full)
            .context("schedule post-close full reconcile")?;
        wait_until(Duration::from_secs(30), || {
            connector.operation(close_operation).state != OperationState::Pending
        })
        .context("post-close reconcile did not complete")?;
        ensure!(
            connector.operation(close_operation).state == OperationState::Succeeded,
            "post-close reconcile failed"
        );
        let positions = connector.positions(PositionFilter::default())?;
        ensure!(
            positions.items.iter().all(|item| item.quantity_lots == 0),
            "non-zero position after market close"
        );
    }
    let orders = connector.orders(OrderFilter {
        asset_id: None,
        include_final: true,
    })?;
    ensure!(
        orders
            .items
            .iter()
            .all(|item| item.status != 1 && item.status != 5),
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

fn verify_rest_stream_agreement(sink: &RecordingSink, client_hex: &str, status: u8) -> Result<()> {
    let orders = recorded_orders(sink, client_hex);
    let stream = orders
        .iter()
        .find(|event| is_private_stream_event(event) && event.status == status)
        .context("matching private-stream fact not recorded")?;
    // NEW orders are provable through a Full reconcile snapshot (the order is still open);
    // a CANCELED order no longer appears in open-order snapshots, so the REST-side fact is
    // the cancel command response (whose exchange_ts is the OKX cancel response `ts`).
    let rest = orders
        .iter()
        .find(|event| is_reconcile_snapshot_event(event) && event.status == status)
        .or_else(|| {
            orders
                .iter()
                .find(|event| event_source(event) == "rest-command" && event.status == status)
        })
        .context("matching REST fact (reconcile snapshot or command response) not recorded")?;
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
        stream.venue_order_id != Id128::default(),
        "private-stream venue order id is empty (OKX ordId backfill missing)"
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
    ensure!(
        stream.header.exchange_ts % 1_000_000 == 0,
        "stream exchange_ts is not millisecond-aligned ({})",
        stream.header.exchange_ts
    );
    Ok(())
}

fn recorded_orders(sink: &RecordingSink, client_hex: &str) -> Vec<OrderChangedV1> {
    sink.events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|event| event.event_type == ORDER_CHANGED_EVENT)
        .filter_map(|event| OrderChangedV1::decode(&event.payload).ok())
        .filter(|event| client_hex_equal(event.client_order_id, client_hex))
        .collect()
}

fn decode_order_summary(sink: &RecordingSink, client_hex: &str) -> Vec<serde_json::Value> {
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
