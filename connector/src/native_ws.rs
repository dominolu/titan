//! Raw native websocket messages. No account readiness or executable book is inferred here.
use crate::{
    api::ApiError,
    venue::{error, websocket_error},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
pub enum NativeFrame {
    Json(Value),
    Binary(Vec<u8>),
}
pub struct NativeSocket {
    venue: &'static str,
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_ping: Instant,
    last_received: Instant,
}
impl NativeSocket {
    pub(crate) async fn connect(venue: &'static str, url: &str) -> Result<Self, ApiError> {
        crate::ensure_rustls_crypto_provider();
        let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(url))
            .await
            .map_err(|_| ApiError::transport(venue, "native WS connect timeout"))?
            .map_err(|e| websocket_error(venue, e))?;
        let now = Instant::now();
        Ok(Self {
            venue,
            socket,
            next_ping: now + Duration::from_secs(30),
            last_received: now,
        })
    }
    pub async fn send_json(&mut self, value: &Value) -> Result<(), ApiError> {
        tokio::time::timeout(
            Duration::from_secs(15),
            self.socket.send(Message::Text(value.to_string().into())),
        )
        .await
        .map_err(|_| ApiError::transport(self.venue, "native WS send timeout; outcome unknown"))?
        .map_err(|e| websocket_error(self.venue, e))
    }
    pub async fn receive(&mut self) -> Result<NativeFrame, ApiError> {
        loop {
            let now = Instant::now();
            if now.duration_since(self.last_received) >= Duration::from_secs(70) {
                return Err(ApiError::transport(
                    self.venue,
                    "native WS heartbeat timeout",
                ));
            }
            if now >= self.next_ping {
                tokio::time::timeout(
                    Duration::from_secs(15),
                    self.socket.send(Message::Ping(Vec::new().into())),
                )
                .await
                .map_err(|_| ApiError::transport(self.venue, "native WS ping timeout"))?
                .map_err(|e| websocket_error(self.venue, e))?;
                self.next_ping = now + Duration::from_secs(30);
            }
            let deadline = self
                .next_ping
                .min(self.last_received + Duration::from_secs(70));
            let frame = match tokio::time::timeout_at(deadline, self.socket.next()).await {
                Ok(frame) => frame,
                Err(_) => continue,
            };
            let frame = frame
                .ok_or_else(|| ApiError::transport(self.venue, "native WS closed"))?
                .map_err(|e| websocket_error(self.venue, e))?;
            self.last_received = Instant::now();
            match frame {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text)
                        .map_err(|_| error(self.venue, "invalid native WS JSON"))?;
                    if self.venue == "lighter" && value["type"] == "ping" {
                        self.send_json(&serde_json::json!({"type":"pong"})).await?;
                        continue;
                    }
                    return Ok(NativeFrame::Json(value));
                }
                Message::Binary(bytes) => return Ok(NativeFrame::Binary(bytes.to_vec())),
                Message::Ping(bytes) => {
                    tokio::time::timeout(
                        Duration::from_secs(15),
                        self.socket.send(Message::Pong(bytes)),
                    )
                    .await
                    .map_err(|_| ApiError::transport(self.venue, "native WS pong timeout"))?
                    .map_err(|e| websocket_error(self.venue, e))?;
                }
                Message::Close(_) => {
                    return Err(ApiError::transport(self.venue, "native WS closed"));
                }
                _ => {}
            }
        }
    }
    pub async fn close(&mut self) -> Result<(), ApiError> {
        tokio::time::timeout(Duration::from_secs(5), self.socket.close(None))
            .await
            .map_err(|_| ApiError::transport(self.venue, "native WS close timeout"))?
            .map_err(|e| websocket_error(self.venue, e))
    }
}
