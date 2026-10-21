//! Native CrossEx websocket access, including channels absent from Titan's fixed market ABI.
use super::{
    GateCrossExClient,
    rest::{EXCHANGE, invalid, protocol, sign},
};
use crate::api::ApiError;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Debug)]
pub struct CrossExMessage {
    pub channel: String,
    pub event: String,
    pub payload: Value,
    pub result: Value,
    pub time_ms: i64,
    pub raw: Value,
}
impl CrossExMessage {
    pub(super) fn parse(raw: Value) -> Self {
        Self {
            channel: raw["channel"].as_str().unwrap_or("").into(),
            event: raw["event"].as_str().unwrap_or("").into(),
            payload: raw["payload"].clone(),
            result: raw["result"].clone(),
            time_ms: raw["time_ms"].as_i64().unwrap_or_else(|| {
                raw["time"]
                    .as_i64()
                    .and_then(|v| v.checked_mul(1000))
                    .unwrap_or(0)
            }),
            raw,
        }
    }
    pub fn data(&self) -> &Value {
        if self.payload.is_object() {
            &self.payload
        } else {
            &self.result
        }
    }
}
type AckReply = oneshot::Sender<Result<CrossExMessage, ApiError>>;
type PendingRequest = (String, String, AckReply);

struct Command {
    channel: String,
    event: String,
    payload: Value,
    reply: Reply,
}
enum Reply {
    Sent(oneshot::Sender<Result<(), ApiError>>),
    Acknowledged(AckReply),
}
/// One authenticated or public connection. A background pump owns Ping/Pong, bounded delivery
/// and request acknowledgements. On interruption `next` fails; callers explicitly reconnect and
/// replay subscriptions, or use GateCrossEx's reconnecting Market/Account factories.
pub struct GateCrossExWebSocket {
    command: mpsc::Sender<Command>,
    events: mpsc::Receiver<Result<CrossExMessage, ApiError>>,
    task: JoinHandle<()>,
}
impl Drop for GateCrossExWebSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl GateCrossExWebSocket {
    pub async fn connect(client: &GateCrossExClient, private: bool) -> Result<Self, ApiError> {
        let endpoint = if private {
            &client.private_ws_url
        } else {
            &client.public_ws_url
        };
        let (mut ws, _) = time::timeout(Duration::from_secs(10), connect_async(endpoint))
            .await
            .map_err(|_| protocol("websocket connect timeout"))?
            .map_err(|e| ApiError::transport(EXCHANGE, e))?;
        if private {
            if client.key.is_empty() || client.secret.is_empty() {
                return Err(invalid("CrossEx credentials are missing"));
            }
            let timestamp = Utc::now().timestamp();
            ws.send(Message::Text(json!({"time":timestamp,"event":"login","payload":{"method":"api_key","api_key":client.key,
                "sign":sign(&client.secret,&format!("channel=&event=login&time={timestamp}"))}}).to_string().into())).await.map_err(|e|ApiError::transport(EXCHANGE,e))?;
            let login = time::timeout(Duration::from_secs(10), super::stream::receive(&mut ws))
                .await
                .map_err(|_| protocol("websocket login timeout"))??;
            if login["event"] != "login" || login["result"]["code"] != "100000" {
                return Err(protocol("websocket login failed"));
            }
        }
        let (command, mut commands) = mpsc::channel::<Command>(128);
        let (events, receiver) = mpsc::channel(4096);
        let task = tokio::spawn(async move {
            let mut pending: BTreeMap<String, PendingRequest> = BTreeMap::new();
            let mut sequence = 0_u64;
            let mut ping = time::interval(Duration::from_secs(15));
            let mut last = time::Instant::now();
            let outcome=async {
                loop { tokio::select! {
                    _=ping.tick()=>{
                        if last.elapsed()>Duration::from_secs(45) { return Err(protocol("websocket heartbeat timeout")); }
                        ws.send(Message::Ping(Vec::new().into())).await.map_err(|e|ApiError::transport(EXCHANGE,e))?;
                    }
                    command=commands.recv()=>{
                        pending.retain(|_,(_,_,reply)|!reply.is_closed());
                        if pending.len()>=128 {return Err(protocol("websocket pending request capacity exceeded"));}
                        let Some(command)=command else {return Ok(());};
                        sequence=sequence.checked_add(1).ok_or_else(||protocol("websocket request id exhausted"))?;
                        let id=sequence.to_string();
                        ws.send(Message::Text(json!({"time":Utc::now().timestamp(),"request_id":id,"event":command.event,
                            "channel":command.channel,"payload":command.payload}).to_string().into())).await.map_err(|e|ApiError::transport(EXCHANGE,e))?;
                        match command.reply {
                            Reply::Acknowledged(reply) => {pending.insert(id,(command.channel,command.event,reply));}
                            Reply::Sent(reply) => {let _=reply.send(Ok(()));}
                        }
                    }
                    frame=ws.next()=>{
                        let message=frame.ok_or_else(||protocol("websocket closed"))?.map_err(|e|ApiError::transport(EXCHANGE,e))?;
                        last=time::Instant::now();
                        match message {
                            Message::Text(raw)=>{
                                let raw:Value=serde_json::from_str(&raw).map_err(|_|protocol("invalid websocket JSON"))?;
                                super::stream::check_response(&raw)?;
                                let frame=CrossExMessage::parse(raw);
                                if frame.result.get("code").is_some() && (frame.event=="api" || !frame.payload.is_object()) {
                                    let id=if let Some(id)=frame.raw["request_id"].as_str() {
                                        pending.contains_key(id).then(||id.to_owned())
                                    } else {
                                        let mut candidates=pending.iter().filter(|(_, (channel,event,_))|channel==&frame.channel && event==&frame.event);
                                        let first=candidates.next().map(|(id,_)|id.clone());
                                        if candidates.next().is_some() {return Err(protocol("ambiguous websocket acknowledgement without request_id"));}
                                        first
                                    };
                                    if let Some(id)=id && let Some((_,_,reply))=pending.remove(&id) { let _=reply.send(Ok(frame.clone())); }
                                }
                                events.try_send(Ok(frame)).map_err(|_|protocol("websocket delivery capacity exceeded"))?;
                            }
                            Message::Ping(bytes)=>ws.send(Message::Pong(bytes)).await.map_err(|e|ApiError::transport(EXCHANGE,e))?,
                            Message::Close(_)=>return Err(protocol("websocket closed")),
                            _=>{}
                        }
                    }
                } }
            }.await;
            if let Err(error) = outcome {
                for (_, (_, _, reply)) in pending {
                    let failure = if error.outcome_unknown() {
                        error.clone()
                    } else {
                        ApiError::transport(
                            EXCHANGE,
                            format!(
                                "websocket stopped with pending request; reconcile API mutations ({})",
                                error.code
                            ),
                        )
                    };
                    let _ = reply.send(Err(failure));
                }
                let _ = events.try_send(Err(error));
            }
        });
        Ok(Self {
            command,
            events: receiver,
            task,
        })
    }
    pub async fn request(
        &self,
        event: &str,
        channel: &str,
        payload: Value,
    ) -> Result<CrossExMessage, ApiError> {
        if !["subscribe", "unsubscribe", "api"].contains(&event) || channel.is_empty() {
            return Err(invalid("invalid websocket command"));
        }
        let (reply, result) = oneshot::channel();
        self.command
            .send(Command {
                channel: channel.into(),
                event: event.into(),
                payload,
                reply: Reply::Acknowledged(reply),
            })
            .await
            .map_err(|_| protocol("websocket stopped"))?;
        time::timeout(Duration::from_secs(10), result)
            .await
            .map_err(|_| {
                ApiError::transport(
                    EXCHANGE,
                    "websocket acknowledgement timeout; API mutation outcome may be unknown",
                )
            })?
            .map_err(|_| {
                ApiError::transport(
                    EXCHANGE,
                    "websocket stopped with request pending; outcome unknown",
                )
            })?
    }
    /// Public channels do not promise acknowledgement frames. This confirms transmission;
    /// `next` returns the actual server acknowledgements and data without synthesizing a reply.
    pub async fn subscribe(&self, channel: &str, symbols: &[String]) -> Result<(), ApiError> {
        self.send_subscription("subscribe", channel, symbols).await
    }
    pub async fn unsubscribe(&self, channel: &str, symbols: &[String]) -> Result<(), ApiError> {
        self.send_subscription("unsubscribe", channel, symbols)
            .await
    }
    /// For private channels that explicitly acknowledge subscriptions.
    pub async fn subscribe_confirmed(
        &self,
        channel: &str,
        symbols: &[String],
    ) -> Result<CrossExMessage, ApiError> {
        self.request("subscribe", channel, json!(symbols)).await
    }
    async fn send_subscription(
        &self,
        event: &str,
        channel: &str,
        symbols: &[String],
    ) -> Result<(), ApiError> {
        if channel.is_empty() || symbols.is_empty() {
            return Err(invalid("channel and symbols are required"));
        }
        let (reply, result) = oneshot::channel();
        self.command
            .send(Command {
                channel: channel.into(),
                event: event.into(),
                payload: json!(symbols),
                reply: Reply::Sent(reply),
            })
            .await
            .map_err(|_| protocol("websocket stopped"))?;
        time::timeout(Duration::from_secs(10), result)
            .await
            .map_err(|_| protocol("websocket send timeout"))?
            .map_err(|_| protocol("websocket stopped"))?
    }
    pub async fn api(&self, channel: &str, payload: Value) -> Result<CrossExMessage, ApiError> {
        self.request("api", channel, payload).await
    }
    pub async fn next(&mut self) -> Result<CrossExMessage, ApiError> {
        self.events
            .recv()
            .await
            .unwrap_or_else(|| Err(protocol("websocket stopped; reconnect required")))
    }
}
