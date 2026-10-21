//! No credentials, account reads or mutations. Run only on the designated build server.
use anyhow::{Result, ensure};
use connector::{api::BrokerApi, gatecrossex::GateCrossExClient};
#[tokio::main]
async fn main() -> Result<()> {
    let api = GateCrossExClient::new("https://api.gateio.ws/api/v4", "", "")?;
    let symbol = std::env::var("GATE_CROSSEX_PROBE_SYMBOL")
        .unwrap_or_else(|_| "BINANCE_FUTURE_BTC_USDT".into());
    let instruments = api.get_instruments().await?;
    ensure!(
        instruments.iter().any(|v| v.symbol == symbol),
        "probe symbol absent from rules"
    );
    let ticker = api.get_ticker(&symbol).await?;
    let book = api.get_order_book(&symbol, 5).await?;
    ensure!(
        ticker.last_price > 0.0 && !book.bids.is_empty() && !book.asks.is_empty(),
        "invalid public snapshot"
    );
    println!(
        "{}",
        serde_json::json!({"account_used":false,"symbol":symbol,"instruments":instruments.len(),"last_price":ticker.last_price,"ticker_time":ticker.timestamp,"bids":book.bids.len(),"asks":book.asks.len(),"book_time":book.timestamp})
    );
    Ok(())
}
