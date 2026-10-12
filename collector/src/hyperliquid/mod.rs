mod http;

use chrono::{DateTime, Utc};
pub use http::keep_connection;

use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::error::ConnectorError;

async fn handle(
    writer_tx: &crate::pipeline::WriterSender,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;

    if let Some(channel) = j.get("channel") {
        let channel_str = channel.as_str().ok_or(ConnectorError::FormatError)?;

        if channel_str == "subscriptionResponse" {
            return Ok(());
        }

        if let Some(data_obj) = j.get("data") {
            let symbol = match channel_str {
                "trades" => {
                    if let Some(trades) = data_obj.as_array() {
                        if let Some(first_trade) = trades.first() {
                            first_trade
                                .get("coin")
                                .and_then(|c| c.as_str())
                                .ok_or(ConnectorError::FormatError)?
                        } else {
                            return Ok(());
                        }
                    } else {
                        return Err(ConnectorError::FormatError);
                    }
                }
                "l2Book" => data_obj
                    .get("coin")
                    .and_then(|c| c.as_str())
                    .ok_or(ConnectorError::FormatError)?,
                "bbo" => data_obj
                    .get("coin")
                    .and_then(|c| c.as_str())
                    .ok_or(ConnectorError::FormatError)?,
                _ => {
                    if let Some(coin) = data_obj.get("coin").and_then(|c| c.as_str()) {
                        coin
                    } else {
                        return Ok(());
                    }
                }
            };

            writer_tx
                .send((recv_time, symbol.to_string(), data.to_string()))
                .await
                .map_err(|_| ConnectorError::FormatError)?;
        }
    }

    Ok(())
}

pub async fn run_collection(
    subscriptions: Vec<String>,
    symbols: Vec<String>,
    writer_tx: crate::pipeline::WriterSender,
) -> Result<(), anyhow::Error> {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(8192);
    let mut h =
        crate::pipeline::AbortOnDrop(tokio::spawn(keep_connection(subscriptions, symbols, ws_tx)));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&writer_tx, recv_time, data).await {
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = (&mut h.0).await;
    Ok(())
}
