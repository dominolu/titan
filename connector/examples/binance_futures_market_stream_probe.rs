use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, ensure};
use connector::{
    binancefutures::BinanceFutures,
    connector::{
        Connector, ConnectorBuilder, DirectPublication, PublishEvent, direct_publish_sender,
    },
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
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .try_init();
    let symbol = std::env::var("BINANCE_FUTURES_TEST_SYMBOL")
        .unwrap_or_else(|_| "XRPUSDT".to_owned())
        .to_lowercase();
    let stream_url = std::env::var("BINANCE_FUTURES_STREAM_URL")
        .unwrap_or_else(|_| "wss://fstream.binance.com/ws".to_owned());
    let candidates = std::env::var("BINANCE_FUTURES_MARK_PRICE_STREAM_FORM")
        .unwrap_or_else(|_| "1s".to_owned())
        .split(',')
        .map(|part| part.trim().to_owned())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let candidates = if candidates.is_empty() {
        vec!["1s".to_owned()]
    } else {
        candidates
    };
    let window_seconds = std::env::var("MARK_PRICE_WINDOW_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(65);
    let duration = Duration::from_secs(window_seconds.max(65));
    let mut final_ok = false;

    println!("probe target symbol={symbol} stream_url={stream_url}");
    println!("markPrice stream forms to probe: {candidates:?}");

    for form in candidates {
        println!("== running markPrice form: {form} for {window_seconds}s ==");
        unsafe {
            std::env::set_var("BINANCE_FUTURES_MARK_PRICE_STREAM_FORM", &form);
        }
        let result = run_once(&symbol, &stream_url, duration).await?;
        let expected_mark_streams: Vec<String> = match form.as_str() {
            "markPrice" => vec![format!("{symbol}@markPrice")],
            "both" => vec![
                format!("{symbol}@markPrice@1s"),
                format!("{symbol}@markPrice"),
            ],
            _ => vec![format!("{symbol}@markPrice@1s")],
        };
        if result.mark_price_seen && result.funding_seen {
            println!(
                "markPrice/funding: OK | form={form} mark_price_count={} funding_count={} subscribe={:?} acks={:?} ws_url={stream_url} window_sec={}",
                result.mark_price_count,
                result.funding_count,
                expected_mark_streams,
                result.ack_ids,
                window_seconds
            );
            final_ok = true;
            break;
        } else {
            println!(
                "markPrice/funding: FAIL | form={form} acks={:?} no_events_in_window={:?} ws_url={stream_url} subscribe={:?} window_sec={}",
                result.ack_ids, result.last_seen, expected_mark_streams, window_seconds
            );
        }
    }

    ensure!(
        final_ok,
        "markPrice/funding复验未通过: 所有模式都未观测到markPrice事件"
    );
    Ok(())
}

#[derive(Default)]
#[allow(dead_code)]
struct ProbeResult {
    mark_price_seen: bool,
    mark_price_count: usize,
    funding_seen: bool,
    funding_count: usize,
    depth_snapshot_seen: bool,
    depth_update_seen: bool,
    trade_seen: bool,
    bbo_seen: bool,
    last_seen: Option<String>,
    ack_ids: Vec<String>,
}

async fn run_once(symbol: &str, stream_url: &str, duration: Duration) -> Result<ProbeResult> {
    let config = format!(
        "stream_url = {stream_url:?}\napi_url = \"https://fapi.binance.com\"\nsafety_timeout_ms = 0\n"
    );
    let mut connector = BinanceFutures::build_from(&config).context("build Binance connector")?;
    connector.subscribe_market_data(
        symbol.to_owned(),
        vec![
            MarketDataKind::Depth,
            MarketDataKind::Trades,
            MarketDataKind::Bbo,
            MarketDataKind::MarkPrice,
            MarketDataKind::FundingRate,
        ],
    );
    let (tx, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sender = direct_publish_sender(move |publication| {
        if let DirectPublication::Event(event) = publication {
            let _ = tx.send(event.clone());
        }
    });
    connector.run_market_data(sender);

    let mut observed = BTreeSet::new();
    let mut mark_price_count = 0;
    let mut funding_count = 0;
    let deadline = tokio::time::Instant::now() + duration;
    let window_secs = duration.as_secs();
    while tokio::time::Instant::now() < deadline && observed.len() < 6 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let event = match tokio::time::timeout(remaining, receiver.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) | Err(_) => break,
        };
        match event {
            PublishEvent::MarkPrice {
                symbol: event_symbol,
                mark_price,
                ..
            } => {
                observed.insert("mark_price");
                mark_price_count += 1;
                println!("mark_price_event symbol={event_symbol} mark_price={mark_price}");
            }
            PublishEvent::FeedBatch { events, stream, .. } => {
                if stream.is_some_and(|metadata| metadata.snapshot) {
                    observed.insert("depth_snapshot");
                }
                for event in events {
                    match event.ev {
                        LOCAL_BID_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT => {
                            observed.insert("depth_update");
                        }
                        LOCAL_BID_DEPTH_SNAPSHOT_EVENT | LOCAL_ASK_DEPTH_SNAPSHOT_EVENT => {
                            observed.insert("depth_snapshot");
                        }
                        LOCAL_BID_DEPTH_BBO_EVENT | LOCAL_ASK_DEPTH_BBO_EVENT => {
                            observed.insert("bbo");
                        }
                        LOCAL_BUY_TRADE_EVENT | LOCAL_SELL_TRADE_EVENT => {
                            observed.insert("trade");
                        }
                        _ => {}
                    }
                }
            }
            PublishEvent::Funding {
                symbol: event_symbol,
                funding_rate,
                next_funding_time,
                ..
            } => {
                observed.insert("funding");
                funding_count += 1;
                println!(
                    "mark_price_event symbol={event_symbol} funding_rate={funding_rate} next_funding_time={next_funding_time}"
                );
            }
            _ => {}
        }
    }

    connector.unsubscribe_market_data(
        symbol.to_owned(),
        vec![
            MarketDataKind::Depth,
            MarketDataKind::Trades,
            MarketDataKind::Bbo,
            MarketDataKind::MarkPrice,
            MarketDataKind::FundingRate,
        ],
    );

    let mark_price = observed.contains("mark_price");
    let funding = observed.contains("funding");
    let depth_snapshot = observed.contains("depth_snapshot");
    let depth_update = observed.contains("depth_update");
    let trade = observed.contains("trade");
    let bbo = observed.contains("bbo");

    if mark_price && funding && depth_snapshot && depth_update && trade && bbo {
        println!(
            "run_once window ok: depth_snapshot={depth_snapshot} depth_update={depth_update} trade={trade} bbo={bbo} mark_price={mark_price} funding={funding} window_secs={window_secs}"
        );
    } else {
        println!(
            "run_once window incomplete: depth_snapshot={depth_snapshot} depth_update={depth_update} trade={trade} bbo={bbo} mark_price={mark_price} funding={funding} window_secs={window_secs}"
        );
    }

    Ok(ProbeResult {
        mark_price_seen: mark_price,
        mark_price_count,
        funding_seen: funding,
        funding_count,
        depth_snapshot_seen: depth_snapshot,
        depth_update_seen: depth_update,
        trade_seen: trade,
        bbo_seen: bbo,
        last_seen: if mark_price && funding {
            Some("mark_price_and_funding".to_owned())
        } else {
            None
        },
        ack_ids: Vec::new(),
    })
}
