//! A single post-only order, an amendment farther from the market, and exact-order cleanup.
//! No market order, transfer, leverage change, venue-wide cancellation or strategy is started.
use anyhow::{Context, Result, ensure};
use connector::{
    api::*,
    connector::{
        AccountPublication, Connector, ConnectorBuilder, DirectPublication, PublishEvent,
        direct_publish_sender,
    },
    gatecrossex::{GateCrossEx, GateCrossExClient},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const MAX_NOTIONAL: f64 = 10.0;
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Plan {
    symbol: String,
    client_order_id: String,
    quantity: f64,
    price: f64,
    amended_price: f64,
    tick: f64,
    reference_ask: f64,
    max_notional_usdt: f64,
    created_ms: i64,
}
fn plan_values(
    symbol: &str,
    min_qty: f64,
    lot: f64,
    tick: f64,
    min_notional: f64,
    bid: f64,
    ask: f64,
) -> Result<Plan> {
    ensure!(
        [min_qty, lot, tick, bid, ask]
            .iter()
            .all(|v| v.is_finite() && *v > 0.0)
            && min_notional.is_finite()
            && min_notional >= 0.0,
        "invalid live rule or book"
    );
    ensure!(ask >= bid, "crossed book");
    let round = |v: f64, unit: f64| (v / unit).floor() * unit;
    let price = round(bid * 0.98, tick);
    let amended_price = round(bid * 0.979, tick);
    let quantity = (min_qty.max(min_notional / price) / lot).ceil() * lot;
    ensure!(
        price > 0.0 && amended_price > 0.0 && amended_price < price && price < bid * 0.99,
        "post-only price protection failed"
    );
    ensure!(
        quantity.is_finite()
            && quantity > 0.0
            && quantity * ask <= MAX_NOTIONAL
            && quantity * price >= min_notional,
        "minimum order exceeds the 10 USDT test cap"
    );
    Ok(Plan {
        symbol: symbol.into(),
        client_order_id: format!("titanlive-{:016x}", rand::random::<u64>()),
        quantity,
        price,
        amended_price,
        tick,
        reference_ask: ask,
        max_notional_usdt: MAX_NOTIONAL,
        created_ms: chrono::Utc::now().timestamp_millis(),
    })
}
async fn plan(api: &GateCrossExClient, symbol: &str) -> Result<Plan> {
    ensure!(
        symbol.ends_with("_USDT") && symbol.contains("_FUTURE_"),
        "probe only supports USDT linear futures"
    );
    ensure!(
        api.get_account_position_mode().await? == ApiPositionMode::Net,
        "this probe requires the existing SINGLE mode; modes will not be changed"
    );
    ensure!(
        api.get_positions(Some(symbol))
            .await?
            .iter()
            .all(|p| p.qty == 0.0),
        "target symbol already has a position"
    );
    ensure!(
        api.get_open_orders(symbol).await?.is_empty(),
        "target symbol already has open orders"
    );
    ensure!(
        api.get_account().await?.available_balance >= MAX_NOTIONAL * 2.0,
        "insufficient available margin for controlled probe"
    );
    let rules = api.list_rule_symbols().await?;
    let rule = rules
        .as_array()
        .context("rules missing")?
        .iter()
        .find(|v| v["symbol"] == symbol)
        .context("target missing")?;
    ensure!(rule["state"] == "live", "target is not live");
    let number = |key: &str| -> Result<f64> {
        rule[key]
            .as_str()
            .context("missing rule field")?
            .parse()
            .context("invalid rule number")
    };
    let min_notional = rule["min_notional"]
        .as_str()
        .map(str::parse)
        .transpose()?
        .unwrap_or(0.0);
    let book = api.get_order_book(symbol, 5).await?;
    ensure!(
        chrono::Utc::now()
            .timestamp_millis()
            .abs_diff(book.timestamp)
            < 5_000,
        "stale reference book"
    );
    plan_values(
        symbol,
        number("min_size")?,
        number("lot_size")?,
        number("tick_size")?,
        min_notional,
        book.bids.first().context("empty bids")?.price,
        book.asks.first().context("empty asks")?.price,
    )
}
fn journal(path: &Path, plan: &Plan) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer(&mut file, plan)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
async fn fact(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    id: &str,
    price: f64,
    tick: f64,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(value) = rx.recv().await {
            if value["client_order_id"] == id {
                ensure!(
                    value["executed_qty"].as_f64() == Some(0.0),
                    "unexpected execution; stop and reconcile"
                );
                ensure!(
                    !matches!(
                        value["status"].as_str(),
                        Some("Rejected" | "Expired" | "Canceled" | "Filled")
                    ),
                    "order became terminal before expected resting fact"
                );
                if value["price"]
                    .as_f64()
                    .is_some_and(|p| (p - price).abs() < tick / 2.0)
                {
                    return Ok(value);
                }
            }
            if value["kind"] == "error" {
                anyhow::bail!("private stream interrupted");
            }
        }
        anyhow::bail!("private fact stream stopped")
    })
    .await
    .context("private order fact timeout")?
}
async fn protected(api: &GateCrossExClient, plan: &Plan) -> Result<()> {
    let book = api.get_order_book(&plan.symbol, 5).await?;
    ensure!(
        chrono::Utc::now()
            .timestamp_millis()
            .abs_diff(book.timestamp)
            < 5_000,
        "stale guard book"
    );
    let bid = book.bids.first().context("empty bid")?.price;
    ensure!(
        plan.price < bid * 0.99 && plan.amended_price < bid * 0.99,
        "market moved into the protected order band"
    );
    Ok(())
}
async fn cleanup(api: &GateCrossExClient, plan: &Plan) -> Result<Value> {
    // Read the exact client id first. A rejected/not-sent order can be absent. Never cancel by
    // symbol or cancel another application order, even when this exercise has failed.
    let mut order = None;
    for _ in 0..20 {
        match api
            .get_order(&plan.symbol, None, Some(&plan.client_order_id))
            .await
        {
            Ok(value) => {
                order = Some(value);
                break;
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "TRADE_ORDER_NOT_FOUND_ERROR" | "ORDER_NOT_FOUND" | "404"
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    if let Some(value) = &order {
        if matches!(
            value.status,
            ApiOrderStatus::New | ApiOrderStatus::PartiallyFilled
        ) {
            // A cancel response may be acceptance only; follow with authoritative polling.
            if let Err(error) = api
                .cancel_order(&CancelOrderRequest {
                    symbol: plan.symbol.clone(),
                    order_id: Some(value.order_id.clone()),
                    client_order_id: None,
                })
                .await
            {
                println!(
                    "{}",
                    json!({"cleanup_cancel_error":error.code,"outcome_unknown":error.outcome_unknown()})
                );
            }
        }
        for _ in 0..30 {
            let value = api
                .get_order(&plan.symbol, None, Some(&plan.client_order_id))
                .await?;
            let terminal = matches!(
                value.status,
                ApiOrderStatus::Canceled
                    | ApiOrderStatus::Filled
                    | ApiOrderStatus::Rejected
                    | ApiOrderStatus::Expired
            );
            order = Some(value);
            if terminal {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let remaining = api.get_open_orders(&plan.symbol).await?;
    ensure!(
        remaining
            .iter()
            .all(|v| v.client_order_id != plan.client_order_id),
        "probe order remains open; manual reconciliation required"
    );
    let positions = api.get_positions(Some(&plan.symbol)).await?;
    ensure!(
        positions.iter().all(|v| v.qty == 0.0),
        "unexpected nonzero position after passive probe; stop and reconcile"
    );
    if let Some(value) = &order {
        ensure!(
            value.executed_qty == 0.0,
            "probe order unexpectedly traded; stop and reconcile"
        );
    }
    Ok(json!({"own_order_absent_from_open_orders":true,"positions_zero":true,"final_order":order}))
}

pub(super) async fn run(
    api: &GateCrossExClient,
    config: &str,
    credential_path: &str,
    symbol: &str,
    execute: bool,
) -> Result<()> {
    let plan = plan(api, symbol).await?;
    println!(
        "{}",
        json!({"action":"passive_plan","execute":execute,"plan":plan,"side":"BUY","type":"LIMIT","time_in_force":"POC","account_id":1,"account_name":"gate","environment":"mainnet","initial_position":0,"initial_open_orders":0})
    );
    if !execute {
        return Ok(());
    }
    let parent = Path::new(credential_path)
        .parent()
        .context("credential directory")?;
    journal(
        &parent.join(format!("intent-{}.json", plan.client_order_id)),
        &plan,
    )?;
    let mut connector = GateCrossEx::build_from(config)?;
    connector.register_account(symbol.into());
    let (tx, mut rx) = mpsc::unbounded_channel();
    connector.run_account(direct_publish_sender(move|publication| {
        let value=match publication {
            DirectPublication::Event(PublishEvent::PrivateStreamReady)=>Some(json!({"kind":"ready"})),
            DirectPublication::Account(AccountPublication::Error(_))=>Some(json!({"kind":"error"})),
            DirectPublication::Account(AccountPublication::OrderWithNativeFills{client_order_id,venue_order_id,order,..})=>Some(json!({"client_order_id":client_order_id,"order_id":venue_order_id,"status":format!("{:?}",order.status),"price":order.price_tick as f64*order.tick_size,"executed_qty":order.qty-order.leaves_qty})),
            _=>None,
        };if let Some(value)=value {let _=tx.send(value);}
    }));
    super::private_ready(&mut rx).await?;
    protected(api, &plan).await?;
    let exercise = async {
        let start = Instant::now();
        let value = api
            .submit_order(&UnifiedOrderRequest {
                symbol: plan.symbol.clone(),
                side: ApiSide::Buy,
                order_type: ApiOrderType::Limit,
                price: Some(plan.price),
                qty: plan.quantity,
                time_in_force: ApiTimeInForce::GTX,
                reduce_only: false,
                position_side: Some(ApiPositionSide::Net),
                client_order_id: Some(plan.client_order_id.clone()),
                stop_price: None,
            })
            .await?;
        println!(
            "{}",
            json!({"action":"submit_ack","elapsed_ms":start.elapsed().as_millis(),"order":value})
        );
        ensure!(
            value.executed_qty == 0.0 && value.status == ApiOrderStatus::New,
            "submit did not produce an unfilled resting order"
        );
        println!(
            "{}",
            json!({"action":"submit_ws","fact":fact(&mut rx,&plan.client_order_id,plan.price,plan.tick).await?})
        );
        protected(api, &plan).await?;
        let start = Instant::now();
        let amended = api
            .amend_order(&AmendOrderRequest {
                symbol: plan.symbol.clone(),
                order_id: Some(value.order_id),
                client_order_id: None,
                new_price: Some(plan.amended_price),
                new_qty: None,
                new_stop_price: None,
            })
            .await?;
        println!(
            "{}",
            json!({"action":"amend_ack","elapsed_ms":start.elapsed().as_millis(),"order":amended})
        );
        ensure!(
            amended.executed_qty == 0.0
                && (amended.price - plan.amended_price).abs() < plan.tick / 2.0,
            "amend result differs from protected intent"
        );
        println!(
            "{}",
            json!({"action":"amend_ws","fact":fact(&mut rx,&plan.client_order_id,plan.amended_price,plan.tick).await?})
        );
        Ok::<_, anyhow::Error>(())
    };
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let interrupted = async {
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    };
    #[cfg(not(unix))]
    let interrupted = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let result = tokio::select! {result=exercise=>result,_=interrupted=>Err(anyhow::anyhow!("probe interrupted; cleaning own order")),_=tokio::time::sleep(Duration::from_secs(45))=>Err(anyhow::anyhow!("probe timed out; cleaning own order"))};
    if let Err(error) = &result {
        println!("{}", json!({"exercise_error":error.to_string()}));
    }
    let cleaned = cleanup(api, &plan).await;
    println!(
        "{}",
        json!({"action":"passive_cleanup","ok":cleaned.is_ok(),"result":cleaned.as_ref().ok(),"error":cleaned.as_ref().err().map(ToString::to_string)})
    );
    let cleaned = cleaned?;
    if result.is_ok() {
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(value) = rx.recv().await {
                if value["client_order_id"] == plan.client_order_id && value["status"] == "Canceled"
                {
                    return Ok(value);
                }
            }
            anyhow::bail!("private stream ended before cancel fact")
        })
        .await
        .context("cancel websocket fact timeout")??;
        println!("{}", json!({"action":"cancel_ws","fact":observed}));
    }
    fs::write(
        parent.join(format!("completed-{}.json", plan.client_order_id)),
        serde_json::to_vec(&cleaned)?,
    )?;
    drop(connector); // No venue-wide shutdown/cancellation.
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn passive_plan_is_small_far_and_price_amendment_moves_away() {
        let p = plan_values(
            "GATE_FUTURE_BTC_USDT",
            0.0001,
            0.0001,
            0.1,
            0.0,
            82665.0,
            82665.1,
        )
        .unwrap();
        assert_eq!(p.quantity, 0.0001);
        assert!(p.quantity * p.reference_ask < 10.0);
        assert!(p.price < 82665.0 * 0.99);
        assert!(p.amended_price < p.price);
    }
    #[test]
    fn oversized_or_invalid_minimum_never_produces_a_test_order() {
        assert!(plan_values("x", 0.001, 0.001, 0.1, 0.0, 82665.0, 82665.1).is_err());
        assert!(plan_values("x", 0.0001, 0.0001, 0.1, 100.0, 82665.0, 82665.1).is_err());
        assert!(plan_values("x", 0.0001, 0.0, 0.1, 0.0, 82665.0, 82665.1).is_err());
        assert!(plan_values("x", 0.0001, 0.0001, 0.1, 0.0, f64::NAN, 82665.1).is_err());
    }
}
