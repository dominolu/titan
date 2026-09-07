//! Hyperliquid mainnet live latency probe: REST ack vs private-WS push, per action.
//!
//! Measures, for resting-GTC submit/cancel and market open/close, both:
//!   - REST ack latency (submit start -> HTTP response with result)
//!   - WS fact latency (submit start -> orderUpdates fact pushed, timestamped at publish)
//! The WS fact may arrive before the REST response; the delta quantifies the push advantage.
//!
//! Env: HL_PRIVATE_KEY (agent key), HL_ACCOUNT_ADDRESS (main wallet).
//! Cost: one 0.01 ETH market round trip (~$0.02 fees) + transient resting orders.

use anyhow::{Context, Result};
use connector::api::{
    ApiOrderType, ApiSide, ApiTimeInForce, CancelOrderRequest, UnifiedOrderRequest,
};
use connector::connector::{
    AccountPublication, Connector, ConnectorBuilder, DirectPublication, PublishEvent,
    direct_publish_sender,
};
use connector::hyperliquid::Hyperliquid;
use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn load_key() -> Result<String> {
    let hex = std::env::var("HL_PRIVATE_KEY").context("HL_PRIVATE_KEY is required")?;
    let hex = hex
        .trim()
        .strip_prefix("0x")
        .unwrap_or(hex.trim())
        .to_string();
    let mut key = [0u8; 32];
    hex::decode_to_slice(&hex, &mut key).context("HL_PRIVATE_KEY must be 32-byte hex")?;
    Ok(hex)
}

#[derive(Clone)]
#[allow(dead_code)]
struct TrackedOrder {
    cloid: String,
    qty: f64,
    price: f64,
    side: Side,
}

/// Registers the order so the private stream publishes facts for it. Must run before
/// the WS fact can arrive (facts are only published for tracked cloids).
fn track(connector: &Hyperliquid, cloid: &str, qty: f64, price: f64, side: Side) {
    let mut order = hftbacktest::types::Order::new(
        0,
        (price / 0.1).round() as i64,
        0.1,
        qty,
        side,
        OrdType::Limit,
        TimeInForce::GTC,
    );
    order.status = Status::New;
    connector.track_managed_order("ETH", cloid, &order);
}

/// Scans facts published so far for the first one matching (cloid, status-contains)
/// and returns its publish timestamp relative to `t0`. Facts are timestamped at push
/// time inside the publisher closure, so late scanning does not skew the measurement.
async fn wait_fact(
    rx: &Receiver<(String, Instant)>,
    t0: Instant,
    cloid: &str,
    status_contains: &str,
    timeout: Duration,
) -> Result<f64> {
    let deadline = Instant::now() + timeout;
    loop {
        while let Ok((msg, ts)) = rx.try_recv() {
            println!("    [fact] +{:?} {msg}", ts - t0);
            if msg.contains(cloid) && (status_contains.is_empty() || msg.contains(status_contains))
            {
                let fact_ms = (ts - t0).as_secs_f64() * 1000.0;
                println!("    ws fact @+{fact_ms:.1}ms: {msg}");
                return Ok(fact_ms);
            }
        }
        if Instant::now() > deadline {
            anyhow::bail!("timeout waiting for ws fact cloid={cloid} status~{status_contains}");
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // workspace 同时启用 aws-lc-rs 与 ring 两个 rustls provider，需显式选择
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let key = load_key()?;
    let account = std::env::var("HL_ACCOUNT_ADDRESS").context("HL_ACCOUNT_ADDRESS is required")?;
    let config = format!(
        "info_url = \"https://api.hyperliquid.xyz/info\"\n\
         exchange_url = \"https://api.hyperliquid.xyz/exchange\"\n\
         ws_url = \"wss://api.hyperliquid.xyz/ws\"\n\
         safety_timeout_ms = 0\n\
         is_mainnet = true\n\
         private_key = \"{key}\"\n\
         account_address = \"{account}\"\n"
    );
    let mut connector = Hyperliquid::build_from(&config).context("build connector")?;
    connector.register_account("ETH".to_string());

    // publisher: timestamp every account fact at push time
    let (tx, rx) = std::sync::mpsc::channel::<(String, Instant)>();
    let publisher = direct_publish_sender(move |publication| match publication {
        DirectPublication::Event(e) => {
            if let PublishEvent::PrivateStreamReady = e {
                let _ = tx.send(("PrivateStreamReady".to_string(), Instant::now()));
            }
        }
        DirectPublication::Account(a) => match a {
            AccountPublication::Order {
                client_order_id,
                venue_order_id,
                order,
                ..
            } => {
                let _ = tx.send((
                    format!(
                        "Order cloid={client_order_id:?} venue={venue_order_id:?} status={:?}",
                        order.status
                    ),
                    Instant::now(),
                ));
            }
            AccountPublication::Position { symbol, qty, .. } => {
                let _ = tx.send((format!("Position {symbol}={qty}"), Instant::now()));
            }
            AccountPublication::Error(e) => {
                let _ = tx.send((format!("AccountError: {e:?}"), Instant::now()));
            }
        },
        DirectPublication::NativeMarket(_) => {}
    });
    connector.run_account(publisher);

    let api = connector.broker_api().context("broker api")?;

    // wait for private stream ready
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match rx.try_recv() {
            Ok((m, _)) if m == "PrivateStreamReady" => break,
            Ok((m, _)) => println!("  pre-ready: {m}"),
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if Instant::now() > deadline {
                    anyhow::bail!("private stream not ready in 30s");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                anyhow::bail!("publisher channel closed")
            }
        }
    }
    println!("[setup] private stream READY");

    let acct: connector::api::AccountInfo = api.get_account().await.context("get_account")?;
    println!(
        "[setup] perp available = {:.4} USDC",
        acct.available_balance
    );
    let ticker = api.get_ticker("ETH").await.context("get_ticker")?;
    let mark = ticker.mark_price.unwrap_or(ticker.last_price);
    println!("[setup] ETH mark = {mark}");

    let mut results: Vec<(String, f64, f64)> = Vec::new(); // label, ack_ms, ws_ms

    // ---------- 1. resting GTC submit + cancel ----------
    let deep = (mark * 0.5 * 10.0).round() / 10.0;
    let cloid = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01";
    track(&connector, cloid, 0.01, deep, Side::Buy);
    let t0 = Instant::now();
    let ack = api
        .submit_order(&UnifiedOrderRequest {
            symbol: "ETH".into(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(deep),
            qty: 0.01,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: None,
            client_order_id: Some(cloid.into()),
            stop_price: None,
        })
        .await
        .context("resting submit")?;
    let ack_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ws_ms = wait_fact(&rx, t0, cloid, "", Duration::from_secs(10)).await?;
    println!(
        "[resting-submit] REST ack {ack_ms:.1}ms | WS fact {ws_ms:.1}ms | delta(WS-REST) {:+.1}ms",
        ws_ms - ack_ms
    );
    results.push(("resting-submit".into(), ack_ms, ws_ms));

    let t0 = Instant::now();
    api.cancel_order(&CancelOrderRequest {
        symbol: "ETH".into(),
        order_id: Some(ack.order_id.clone()),
        client_order_id: None,
    })
    .await
    .context("resting cancel")?;
    let ack_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ws_ms = wait_fact(&rx, t0, cloid, "Canceled", Duration::from_secs(10)).await?;
    println!(
        "[resting-cancel] REST ack {ack_ms:.1}ms | WS fact {ws_ms:.1}ms | delta(WS-REST) {:+.1}ms",
        ws_ms - ack_ms
    );
    results.push(("resting-cancel".into(), ack_ms, ws_ms));

    // ---------- 2. market open ----------
    let cloid = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeee02";
    track(&connector, cloid, 0.01, mark, Side::Buy);
    let t0 = Instant::now();
    let ack = api
        .submit_order(&UnifiedOrderRequest {
            symbol: "ETH".into(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Market,
            price: None,
            qty: 0.01,
            time_in_force: ApiTimeInForce::IOC,
            reduce_only: false,
            position_side: None,
            client_order_id: Some(cloid.into()),
            stop_price: None,
        })
        .await
        .context("market buy")?;
    let ack_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ws_ms = wait_fact(&rx, t0, cloid, "", Duration::from_secs(10)).await?;
    println!(
        "[market-open] REST ack {ack_ms:.1}ms (status={:?}) | WS fact {ws_ms:.1}ms | delta {:+.1}ms",
        ack.status,
        ws_ms - ack_ms
    );
    results.push(("market-open".into(), ack_ms, ws_ms));

    // ---------- 3. market close (reduce-only) ----------
    let cloid = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeee03";
    track(&connector, cloid, 0.01, mark, Side::Sell);
    let t0 = Instant::now();
    let ack = api
        .submit_order(&UnifiedOrderRequest {
            symbol: "ETH".into(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::Market,
            price: None,
            qty: 0.01,
            time_in_force: ApiTimeInForce::IOC,
            reduce_only: true,
            position_side: None,
            client_order_id: Some(cloid.into()),
            stop_price: None,
        })
        .await
        .context("market close")?;
    let ack_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ws_ms = wait_fact(&rx, t0, cloid, "", Duration::from_secs(10)).await?;
    println!(
        "[market-close] REST ack {ack_ms:.1}ms (status={:?}) | WS fact {ws_ms:.1}ms | delta {:+.1}ms",
        ack.status,
        ws_ms - ack_ms
    );
    results.push(("market-close".into(), ack_ms, ws_ms));

    // ---------- summary ----------
    let pos = api.get_positions(Some("ETH")).await?;
    println!("\n===== REST ack vs WS push (ms) =====");
    println!(
        "{:<16}{:>10}{:>10}{:>12}",
        "action", "REST-ack", "WS-push", "delta"
    );
    for (label, a, w) in &results {
        println!("{:<16}{:>10.1}{:>10.1}{:>+12.1}", label, a, w, w - a);
    }
    println!(
        "[end] positions: {:?}",
        pos.iter()
            .map(|p| (p.symbol.clone(), p.qty))
            .collect::<Vec<_>>()
    );
    Ok(())
}
