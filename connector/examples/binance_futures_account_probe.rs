use std::env;

use anyhow::{Context, Result, bail};
use connector::{
    binancefutures::BinanceFutures,
    connector::{Connector, ConnectorBuilder},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let api_key = required_secret("BINANCE_FUTURES_API_KEY")?;
    let secret = required_secret("BINANCE_FUTURES_API_SECRET")?;
    let api_url = env::var("BINANCE_FUTURES_API_URL")
        .unwrap_or_else(|_| "https://fapi.binance.com".to_owned());
    let symbol = env::var("BINANCE_FUTURES_TEST_SYMBOL")
        .unwrap_or_else(|_| "BTCUSDT".to_owned())
        .to_uppercase();
    let config = format!(
        "stream_url = \"wss://fstream.binance.com/ws\"\napi_url = {api_url:?}\norder_prefix = \"titanprobe\"\napi_key = {api_key:?}\nsecret = {secret:?}\nsafety_timeout_ms = 0\n"
    );
    let connector = BinanceFutures::build_from(&config).context("build Binance connector")?;
    let api = connector
        .broker_api()
        .context("Binance connector did not expose BrokerApi")?;

    api.ping().await.context("public ping")?;
    let server_time = api.get_server_time().await.context("server time")?;
    let instruments = api.get_instruments().await.context("instrument metadata")?;
    let instrument = instruments
        .iter()
        .find(|value| value.symbol.eq_ignore_ascii_case(&symbol))
        .with_context(|| format!("instrument {symbol} was not found"))?;
    if !instrument.tradable || instrument.min_qty <= 0.0 || instrument.lot_size <= 0.0 {
        bail!("instrument {symbol} is not safely tradable");
    }
    let ticker = api.get_ticker(&symbol).await.context("ticker")?;
    let account = api
        .get_account()
        .await
        .context("authenticated account snapshot")?;
    let positions = api
        .get_positions(Some(&symbol))
        .await
        .context("authenticated positions snapshot")?;
    let open_orders = api
        .get_open_orders(&symbol)
        .await
        .context("authenticated open orders snapshot")?;
    let fees = api
        .get_fee_rates(&symbol)
        .await
        .context("commission rate")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "exchange": "binance-usdm",
            "api_url": api_url,
            "authenticated": true,
            "server_time_ms": server_time,
            "symbol": symbol,
            "instrument": {
                "tradable": instrument.tradable,
                "tick_size": instrument.tick_size,
                "lot_size": instrument.lot_size,
                "min_qty": instrument.min_qty,
                "contract_size": instrument.contract_size,
            },
            "market": {
                "last_price": ticker.last_price,
                "mark_price": ticker.mark_price,
            },
            "account": {
                "available_balance": account.available_balance,
                "wallet_balance": account.total_wallet_balance,
                "margin_balance": account.total_margin_balance,
                "nonzero_balance_assets": account.balances.iter()
                    .filter(|value| value.wallet_balance != 0.0 || value.available_balance != 0.0)
                    .map(|value| value.asset.as_str())
                    .collect::<Vec<_>>(),
            },
            "position": positions.iter().map(|value| json!({
                "side": format!("{:?}", value.position_side),
                "qty": value.qty,
                "entry_price": value.entry_price,
                "mark_price": value.mark_price,
                "leverage": value.leverage,
                "margin_type": format!("{:?}", value.margin_type),
            })).collect::<Vec<_>>(),
            "open_order_count": open_orders.len(),
            "fees": {
                "maker": fees.maker_fee,
                "taker": fees.taker_fee,
            }
        }))?
    );
    Ok(())
}

fn required_secret(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}
