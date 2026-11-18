use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Utf8Bytes;

use collector::transport::{self, Heartbeat, WsTarget};

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

/// The combined-stream URL: one `?streams=` list of every stream of every symbol, symbols
/// lowercased in the order given.
pub(crate) fn stream_url(endpoint: &str, streams: &[String], symbols: &[String]) -> String {
    let mut url = format!("{endpoint}?streams=");
    let mut first = true;
    for symbol in symbols {
        let symbol = symbol.to_lowercase();
        for stream in streams {
            if !first {
                url.push('/');
            }
            first = false;
            url.push_str(&stream.replace("$symbol", &symbol));
        }
    }
    url
}

pub async fn keep_connection(
    streams: Vec<String>,
    symbols: Vec<String>,
    ws_tx: Sender<(DateTime<Utc>, Utf8Bytes)>,
) {
    transport::keep_connection(
        WsTarget::new(
            stream_url("wss://stream.binance.com:9443/stream", &streams, &symbols),
            Heartbeat {
                check_interval: Duration::from_secs(5),
                timeout: Duration::from_secs(30),
            },
        ),
        ws_tx,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::stream_url;

    #[test]
    fn stream_url_preserves_symbol_and_stream_order() {
        let streams = vec!["$symbol@trade".into(), "$symbol@depth@0ms".into()];
        let symbols = vec!["BTCUSDT".into(), "EthUsdt".into()];
        assert_eq!(
            stream_url("wss://fstream.binance.com/stream", &streams, &symbols),
            "wss://fstream.binance.com/stream?streams=btcusdt@trade/btcusdt@depth@0ms/ethusdt@trade/ethusdt@depth@0ms"
        );
        assert_eq!(
            stream_url("ws://localhost/stream", &[], &symbols),
            "ws://localhost/stream?streams="
        );
        assert_eq!(
            stream_url("ws://localhost/stream", &streams, &[]),
            "ws://localhost/stream?streams="
        );
    }
}
