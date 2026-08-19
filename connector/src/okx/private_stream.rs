use std::time::Duration;

use base64::Engine as _;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use hftbacktest::prelude::LiveEvent;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    connector::PublishEvent,
    okx::{
        OkxError, SharedSymbolSet,
        msg::{
            rest::Position,
            stream::{DataMsg, LoginArg, LoginRequest, OrderUpdate, StreamMsg, WsArg, WsRequest},
        },
        ordermanager::SharedOrderManager,
        rest::OkxClient,
    },
};

pub struct PrivateStream {
    api_key: String,
    secret: String,
    passphrase: String,
    td_mode: String,
    pos_side: Option<String>,
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    client: OkxClient,
    symbol_rx: Receiver<String>,
    symbols: SharedSymbolSet,
}

impl PrivateStream {
    pub fn new(
        api_key: String,
        secret: String,
        passphrase: String,
        td_mode: String,
        pos_side: Option<String>,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        client: OkxClient,
        symbol_rx: Receiver<String>,
        symbols: SharedSymbolSet,
    ) -> Self {
        Self {
            api_key,
            secret,
            passphrase,
            td_mode,
            pos_side,
            ev_tx,
            order_manager,
            client,
            symbol_rx,
            symbols,
        }
    }

    async fn handle_private_stream(
        &self,
        text: &str,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<(), OkxError> {
        let stream = serde_json::from_str::<StreamMsg>(text)?;
        match stream {
            StreamMsg::Ack(ack) => {
                debug!(?ack, "Ack");
                if ack.event.as_deref() == Some("login") {
                    if ack.code.as_deref() == Some("0") {
                        let op = WsRequest {
                            op: "subscribe".to_string(),
                            args: vec![
                                WsArg {
                                    channel: "orders".to_string(),
                                    inst_id: None,
                                    inst_type: Some("SWAP".to_string()),
                                },
                                WsArg {
                                    channel: "positions".to_string(),
                                    inst_id: None,
                                    inst_type: Some("SWAP".to_string()),
                                },
                            ],
                        };
                        let s = serde_json::to_string(&op).unwrap();
                        write.send(Message::Text(s.into())).await?;

                        // Replays every registered symbol after (re)connect so their cancel-all and
                        // position initialization run again on the fresh connection.
                        let symbols: Vec<String> =
                            self.symbols.lock().unwrap().iter().cloned().collect();
                        for symbol in symbols {
                            self.init_symbol(symbol).await;
                        }
                    } else {
                        return Err(OkxError::AuthError {
                            code: ack.code.unwrap_or_default(),
                            msg: ack.msg.unwrap_or_default(),
                        });
                    }
                }
            }
            StreamMsg::Data(data) => {
                self.handle_data(&data).await?;
            }
        }
        Ok(())
    }

    async fn handle_data(&self, data: &DataMsg) -> Result<(), OkxError> {
        match data.arg.channel.as_str() {
            "orders" => {
                for value in &data.data {
                    let order_update: OrderUpdate = serde_json::from_value(value.clone())?;
                    let mut order_manager = self.order_manager.lock().unwrap();
                    match order_manager.update_from_ws(&order_update) {
                        Ok(Some(order)) => {
                            let symbol = order_update.inst_id.clone();
                            self.ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                .unwrap();
                        }
                        Ok(None) => {}
                        Err(OkxError::PrefixUnmatched) => {
                            // The order is not created by this connector.
                        }
                        Err(error) => {
                            error!(?error, "Couldn't update the order data");
                        }
                    }
                }
            }
            "positions" => {
                for value in &data.data {
                    let position: Position = serde_json::from_value(value.clone())?;
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Position {
                            symbol: position.inst_id.clone(),
                            qty: position_qty(&position),
                            exch_ts: position.u_time.parse().unwrap_or(0) * 1_000_000,
                        }))
                        .unwrap();
                }
            }
            "account" | "balance-and-position" | "orders-algo" | "algo-advance" => {
                debug!(
                    channel = %data.arg.channel,
                    count = data.data.len(),
                    "Extra private channel message."
                );
            }
            channel => {
                debug!(%channel, "Unhandled private channel.");
            }
        }
        Ok(())
    }

    async fn init_symbol(&self, symbol: String) {
        let client = self.client.clone();
        let td_mode = self.td_mode.clone();
        let pos_side = self.pos_side.clone();
        let order_manager = self.order_manager.clone();
        let ev_tx = self.ev_tx.clone();

        tokio::spawn(async move {
            // Cancel all orders in order to start with a clean state.
            if let Err(error) = cancel_all(
                client.clone(),
                td_mode.clone(),
                pos_side.clone(),
                symbol.clone(),
                order_manager.clone(),
                ev_tx.clone(),
            )
            .await
            {
                error!(?error, %symbol, "Couldn't cancel all orders.");
            }
            // Fetches the initial position.
            if let Err(error) = get_position(client, symbol.clone(), ev_tx).await {
                error!(?error, %symbol, "Couldn't get the position information.");
            }
        });
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), OkxError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = time::interval(Duration::from_secs(20));
        let mut gc_interval = time::interval(Duration::from_secs(20));

        // OKX WebSocket login expects a Unix-epoch timestamp in SECONDS (unlike REST, which uses
        // ISO 8601). The signature covers timestamp + method + path with an empty body.
        let timestamp = Utc::now().timestamp().to_string();
        let sign = sign_login(&self.secret, &timestamp);
        let login = LoginRequest {
            op: "login".to_string(),
            args: vec![LoginArg {
                api_key: self.api_key.clone(),
                passphrase: self.passphrase.clone(),
                timestamp: timestamp.clone(),
                sign,
            }],
        };
        let s = serde_json::to_string(&login).unwrap();
        write.send(Message::Text(s.into())).await?;

        loop {
            select! {
                _ = interval.tick() => {
                    let op = WsRequest {
                        op: "ping".to_string(),
                        args: vec![],
                    };
                    let s = serde_json::to_string(&op).unwrap();
                    write.send(Message::Text(s.into())).await?;
                }
                _ = gc_interval.tick() => {
                    self.order_manager.lock().unwrap().gc();
                }
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        self.init_symbol(symbol).await;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(error) = self.handle_private_stream(&text, &mut write).await {
                                error!(%text, ?error, "Couldn't properly handle PrivateStreamMsg");
                            }
                        }
                        Some(Ok(Message::Ping(_))) => {
                            write.send(Message::Pong(Bytes::default())).await?;
                        }
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(OkxError::ConnectionAbort(
                                close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                            ));
                        }
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(error)) => {
                            return Err(OkxError::from(error));
                        }
                        None => {
                            return Err(OkxError::ConnectionInterrupted);
                        }
                    }
                }
            }
        }
    }
}

pub fn position_qty(position: &Position) -> f64 {
    // OKX already returns a signed quantity for FUTURES/SWAP/OPTION (positive = long,
    // negative = short) in both net and long/short position modes.
    position.pos.parse().unwrap_or(0.0)
}

/// WS login signature: Base64(HMAC-SHA256(timestamp + "GET" + "/users/self/verify", secret)).
pub(crate) fn sign_login(secret: &str, timestamp: &str) -> String {
    let s = format!("{timestamp}GET/users/self/verify");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(s.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

pub async fn get_position(
    client: OkxClient,
    symbol: String,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), OkxError> {
    let positions = client.get_positions(&symbol).await?;
    for position in positions {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol: position.inst_id.clone(),
                qty: position_qty(&position),
                exch_ts: position.u_time.parse().unwrap_or(0) * 1_000_000,
            }))
            .unwrap();
    }
    Ok(())
}

pub async fn cancel_all(
    client: OkxClient,
    td_mode: String,
    pos_side: Option<String>,
    symbol: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), OkxError> {
    client
        .cancel_all_orders(&symbol, &td_mode, pos_side.as_deref())
        .await?;
    let orders = order_manager
        .lock()
        .unwrap()
        .cancel_all(&symbol, pos_side.as_deref());
    for order in orders {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order {
                symbol: symbol.clone(),
                order,
            }))
            .unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(pos: &str, pos_side: &str) -> Position {
        Position {
            inst_id: "BTC-USDT-SWAP".to_string(),
            pos_side: pos_side.to_string(),
            pos: pos.to_string(),
            u_time: "0".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_position_qty_signed_net_mode() {
        assert_eq!(position_qty(&position("-1.5", "net")), -1.5);
        assert_eq!(position_qty(&position("2.0", "net")), 2.0);
    }

    #[test]
    fn test_position_qty_signed_hedge_mode() {
        // OKX returns a negative pos for short positions in long/short mode too.
        assert_eq!(position_qty(&position("-1.5", "short")), -1.5);
        assert_eq!(position_qty(&position("1.5", "long")), 1.5);
    }
}
