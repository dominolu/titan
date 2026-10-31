use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Utf8Bytes;

use crate::binance_transport::{self, Heartbeat};

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::new()
        .get(format!(
            "https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

pub async fn keep_connection(
    streams: Vec<String>,
    symbols: Vec<String>,
    ws_tx: Sender<(DateTime<Utc>, Utf8Bytes)>,
) {
    binance_transport::keep_connection(
        "wss://stream.binance.com:9443/stream",
        Heartbeat {
            check_interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        },
        streams,
        symbols,
        ws_tx,
    )
    .await;
}
