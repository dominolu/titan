//! Public endpoints only. Run on the build server; never reads account state or credentials.
use anyhow::Context;
use connector::{
    api::{ApiError, BrokerApi},
    backpack::{BackpackClient, Config as BackpackConfig},
    lighter::{Config as LighterConfig, LighterClient, LighterOperation},
    native_api::NativeRequest,
    variational::{StatsVersion, VariationalClient, VariationalError},
};
use std::future::Future;

// Only this public, read-only diagnostic retries transport failures. Trading calls never retry.
async fn public_read<T, F, Fut>(operation: &str, mut request: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    for attempt in 1..=3 {
        match request().await {
            Ok(value) => return Ok(value),
            Err(error) if error.code == "TRANSPORT" && attempt < 3 => {
                eprintln!("{operation}: transport failure, public read attempt {attempt}/3");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error).with_context(|| operation.to_string()),
        }
    }
    unreachable!()
}
async fn variational_read<T, F, Fut>(mut request: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, VariationalError>>,
{
    for attempt in 1..=3 {
        match request().await {
            Ok(value) => return Ok(value),
            Err(VariationalError::RateLimited {
                retry_after_seconds,
            }) if attempt < 3 && retry_after_seconds <= 10 => {
                tokio::time::sleep(std::time::Duration::from_secs(retry_after_seconds)).await
            }
            Err(e) => return Err(e.into()),
        }
    }
    unreachable!()
}
async fn probe(name: &str, api: &dyn BrokerApi, symbol: &str) -> anyhow::Result<()> {
    let instruments = public_read("instruments", || api.get_instruments()).await?;
    anyhow::ensure!(
        instruments.iter().any(|m| m.symbol == symbol && m.tradable),
        "missing {name} instrument"
    );
    let book = public_read("book", || api.get_order_book(symbol, 100)).await?;
    anyhow::ensure!(
        !book.bids.is_empty() && !book.asks.is_empty(),
        "empty {name} book"
    );
    let trades = public_read("trades", || api.get_trades(symbol, 3)).await?;
    anyhow::ensure!(!trades.is_empty(), "empty {name} trades");
    let funding = public_read("funding", || api.get_funding_rate(symbol)).await?;
    let oi = public_read("open interest", || api.get_open_interest(symbol)).await?;
    let candles = public_read("candles", || api.get_klines(symbol, "1m", 2)).await?;
    let funding_history = public_read("funding history", || {
        api.get_funding_rate_history(symbol, 2)
    })
    .await?;
    anyhow::ensure!(
        !candles.is_empty() && !funding_history.is_empty(),
        "missing public history"
    );
    let now = chrono::Utc::now().timestamp_millis();
    anyhow::ensure!(
        trades
            .iter()
            .all(|t| t.timestamp > 1_000_000_000_000 && t.timestamp <= now + 60_000),
        "invalid normalized public trade time"
    );
    anyhow::ensure!(
        (now - book.timestamp).abs() < 60_000,
        "stale or incorrectly scaled public book time"
    );
    anyhow::ensure!(
        funding.next_funding_time > now && funding.next_funding_time <= now + 86_400_000,
        "invalid next funding time"
    );
    println!(
        "{}",
        serde_json::json!({"venue":name,"symbol":symbol,"instruments":instruments.len(),"bids":book.bids.len(),"asks":book.asks.len(),"trades":trades.len(),"funding":funding.funding_rate,"next_funding_time_ms":funding.next_funding_time,"open_interest_base":oi.open_interest,"candles":candles.len(),"funding_history":funding_history.len(),"first_trade_time_ms":trades[0].timestamp})
    );
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let selected = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    anyhow::ensure!(
        matches!(
            selected.as_str(),
            "all" | "backpack" | "lighter" | "variational"
        ),
        "unknown venue"
    );
    let backpack = BackpackClient::new(BackpackConfig::default())?;
    let lighter = LighterClient::new(LighterConfig::default())?;
    let mut failed = Vec::new();
    for (name, api, symbol) in [
        ("backpack", &backpack as &dyn BrokerApi, "BTC_USDC_PERP"),
        ("lighter", &lighter as &dyn BrokerApi, "ETH"),
    ] {
        if selected != "all" && selected != name {
            continue;
        }
        if let Err(e) = probe(name, api, symbol).await {
            eprintln!("{name}: {e:#}");
            failed.push(name);
        } else if name == "lighter" {
            let native = lighter.native()?;
            let native = &native;
            for operation in [
                LighterOperation::SystemConfig,
                LighterOperation::AssetDetails,
                LighterOperation::FeeTiers,
                LighterOperation::ExplorerGetMarkets,
            ] {
                match public_read("native public operation", || async move {
                    native
                        .native_request(operation, NativeRequest::default(), None)
                        .await?
                        .json()
                        .await
                })
                .await
                {
                    Ok(value) => {
                        if !(value.is_object() || value.is_array())
                            || value
                                .get("code")
                                .and_then(serde_json::Value::as_i64)
                                .is_some_and(|code| code != 200)
                        {
                            eprintln!(
                                "lighter native {}: unexpected response shape/code",
                                operation.metadata().id
                            );
                            failed.push("lighter-native");
                            continue;
                        }
                        println!(
                            "{}",
                            serde_json::json!({"venue":"lighter","native_operation":operation.metadata().id,"json_object":value.is_object(),"json_array":value.is_array(),"code":value.get("code")})
                        );
                    }
                    Err(e) => {
                        eprintln!("lighter native {}: {e:#}", operation.metadata().id);
                        failed.push("lighter-native");
                    }
                }
            }
        }
    }
    if selected == "all" || selected == "variational" {
        let client = VariationalClient::default();
        match variational_read(|| client.stats()).await {
            Ok(stats) => println!(
                "{}",
                serde_json::json!({"venue":"variational","capability":"statistics_only","listings":stats.stats.listings.len(),"reported_num_markets":stats.stats.num_markets,"received_at_ms":stats.received_at_ms})
            ),
            Err(e) => {
                eprintln!("variational: {e}");
                failed.push("variational");
            }
        }
        match variational_read(|| client.native_stats(StatsVersion::LegacyV1)).await {
            Ok(snapshot) => {
                if !snapshot.body["listings"].is_array() {
                    eprintln!("variational legacy: missing native listings");
                    failed.push("variational-legacy");
                } else {
                    println!(
                        "{}",
                        serde_json::json!({"venue":"variational","native_operation":"legacy_stats","listings":snapshot.body["listings"].as_array().unwrap().len(),"received_at_ms":snapshot.received_at_ms})
                    );
                }
            }
            Err(e) => {
                eprintln!("variational legacy: {e:#}");
                failed.push("variational-legacy");
            }
        }
    }
    anyhow::ensure!(
        failed.is_empty(),
        "public probes failed: {}",
        failed.join(", ")
    );
    Ok(())
}
