//! Public-data-only CrossEx probe. This example never reads credentials or starts an account stream.
use anyhow::{Context, Result, ensure};
use connector::{
    connector::{
        Connector, ConnectorBuilder, DirectPublication, NativeMarketBatch, PublishEvent,
        direct_publish_sender,
    },
    gatecrossex::GateCrossEx,
};
use std::{collections::BTreeSet, time::Duration};
use titan_market_service::MarketDataKind;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let symbol = std::env::var("GATE_CROSSEX_PROBE_SYMBOL")
        .unwrap_or_else(|_| "BINANCE_FUTURE_BTC_USDT".into());
    let mut connector = GateCrossEx::build_from("").context("build public CrossEx connector")?;
    connector.subscribe_market_data(
        symbol.clone(),
        vec![
            MarketDataKind::Depth,
            MarketDataKind::Bbo,
            MarketDataKind::Trades,
            MarketDataKind::MarkPrice,
            MarketDataKind::FundingRate,
        ],
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    connector.run_market_data(direct_publish_sender(move |publication| {
        let kind = match publication {
            DirectPublication::NativeMarket(NativeMarketBatch::Depth { .. }) => Some("depth"),
            DirectPublication::NativeMarket(NativeMarketBatch::Bbo { .. }) => Some("bbo"),
            DirectPublication::NativeMarket(NativeMarketBatch::Trade { .. }) => Some("trade"),
            DirectPublication::Event(PublishEvent::Funding { .. }) => Some("funding"),
            DirectPublication::Event(PublishEvent::MarkPrice { .. }) => Some("mark"),
            _ => None,
        };
        if let Some(kind) = kind {
            let _ = tx.send(kind);
        }
    }));
    let mut observed = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    while observed.len() < 5 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(kind)) => {
                observed.insert(kind);
            }
            _ => break,
        }
    }
    drop(connector);
    println!(
        "{}",
        serde_json::json!({"symbol":symbol,"account_used":false,"observed":observed})
    );
    ensure!(
        observed.len() == 5,
        "not all public CrossEx channels were observed"
    );
    Ok(())
}
