//! OKX mainnet public-stream probe.
//!
//! Connects the real OKX connector to production public WebSocket, subscribes
//! Depth (books), Trades (trades), Bbo (bbo-tbt) and FundingRate (funding-rate)
//! for one swap instrument, and verifies every stream kind is observed inside
//! the window. This mirrors `binance_futures_market_stream_probe.rs`; OKX has
//! no MarkPrice publication today (the `mark-price` channel is subscribed but
//! its payloads are not decoded), so mark price is intentionally not asserted.

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, ensure};
use connector::{
    connector::{Connector, ConnectorBuilder, DirectPublication, PublishEvent, direct_publish_sender},
    okx::Okx,
};
use hftbacktest::prelude::{
    LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use titan_market_plugin::MarketDataKind;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .try_init();
    let symbol = std::env::var("OKX_PROBE_SYMBOL")
        .unwrap_or_else(|_| "XRP-USDT-SWAP".to_owned())
        .to_uppercase();
    let public_ws_url =
        std::env::var("OKX_TEST_PUBLIC_WS_URL").unwrap_or_else(|_| "wss://ws.okx.com:8443/ws/v5/public".to_owned());
    let window_seconds = std::env::var("OKX_MARKET_WINDOW_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(45);

    println!("probe target symbol={symbol} public_ws_url={public_ws_url} window={window_seconds}s");

    let kinds = vec![
        MarketDataKind::Depth,
        MarketDataKind::Trades,
        MarketDataKind::Bbo,
        MarketDataKind::FundingRate,
    ];
    let config = format!(
        "rest_url = \"https://www.okx.com\"\npublic_ws_url = {public_ws_url:?}\nprivate_ws_url = {public_ws_url:?}\napi_key = \"\"\nsecret = \"\"\npassphrase = \"\"\nsimulated = false\nsafety_timeout_ms = 0\n"
    );
    let mut connector = Okx::build_from(&config).context("build OKX connector")?;
    connector.subscribe_market_data(symbol.clone(), kinds.clone());

    let (tx, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sender = direct_publish_sender(move |publication| {
        if let DirectPublication::Event(event) = publication {
            let _ = tx.send(event.clone());
        }
    });
    connector.run_market_data(sender);

    let mut observed = BTreeSet::new();
    let mut depth_snapshot_count = 0usize;
    let mut depth_update_count = 0usize;
    let mut trade_count = 0usize;
    let mut bbo_count = 0usize;
    let mut funding_count = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(window_seconds);
    while tokio::time::Instant::now() < deadline && observed.len() < 5 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let event = match tokio::time::timeout(remaining, receiver.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) | Err(_) => break,
        };
        match event {
            PublishEvent::FeedBatch { events, stream, .. } => {
                if stream.is_some_and(|metadata| metadata.snapshot) {
                    observed.insert("depth_snapshot");
                    depth_snapshot_count += 1;
                }
                for event in events {
                    match event.ev {
                        LOCAL_BID_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT => {
                            observed.insert("depth_update");
                            depth_update_count += 1;
                        }
                        LOCAL_BID_DEPTH_SNAPSHOT_EVENT | LOCAL_ASK_DEPTH_SNAPSHOT_EVENT => {
                            observed.insert("depth_snapshot");
                        }
                        LOCAL_BID_DEPTH_BBO_EVENT | LOCAL_ASK_DEPTH_BBO_EVENT => {
                            observed.insert("bbo");
                            bbo_count += 1;
                        }
                        LOCAL_BUY_TRADE_EVENT | LOCAL_SELL_TRADE_EVENT => {
                            observed.insert("trade");
                            trade_count += 1;
                        }
                        _ => {}
                    }
                }
            }
            PublishEvent::Funding { symbol, funding_rate, .. } => {
                observed.insert("funding");
                funding_count += 1;
                println!("funding_event symbol={symbol} funding_rate={funding_rate}");
            }
            PublishEvent::StreamInvalidated { symbol, epoch } => {
                println!("stream_invalidated symbol={symbol} epoch={epoch}");
            }
            _ => {}
        }
    }

    connector.unsubscribe_market_data(symbol.to_owned(), kinds);

    let summary = serde_json::json!({
        "symbol": symbol,
        "depth_snapshot": observed.contains("depth_snapshot"),
        "depth_update": observed.contains("depth_update"),
        "trade": observed.contains("trade"),
        "bbo": observed.contains("bbo"),
        "funding": observed.contains("funding"),
        "counts": {
            "depth_snapshot": depth_snapshot_count,
            "depth_update": depth_update_count,
            "trade": trade_count,
            "bbo": bbo_count,
            "funding": funding_count,
        },
        "window_seconds": window_seconds,
    });
    println!("final={}", serde_json::to_string_pretty(&summary)?);

    for required in ["depth_snapshot", "depth_update", "trade", "bbo", "funding"] {
        ensure!(
            observed.contains(required),
            "OKX public stream did not observe {required} within {window_seconds}s"
        );
    }
    Ok(())
}
