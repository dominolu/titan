//! A websocket market stream, kept open.
//!
//! Reading, pings, heartbeats, reconnection and the reasons a connection ends are the same for
//! every venue; only the URL and how its frames are read differ. This module owns the former, so a
//! venue's `keep_connection` is a URL and a heartbeat, and its message handling starts once a frame
//! is already decoded text.
//!
//! Frames are delivered as `(received_at, text)` on a bounded channel: the reader never blocks on
//! the handler, and a handler that falls behind backs the socket up rather than losing frames.

use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::{Sender, unbounded_channel},
    time::{Instant as HeartbeatInstant, interval},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        Bytes, Message, Utf8Bytes, client::IntoClientRequest, protocol::WebSocketConfig,
    },
};
use tracing::{error, warn};

/// How often a market stream is checked, and how long it may go without a ping.
#[derive(Clone, Copy)]
pub struct Heartbeat {
    pub check_interval: Duration,
    pub timeout: Duration,
}

/// What a stream connection accepts and buffers.
///
/// Set explicitly rather than left to the library, so that a size is a decision: a frame past the
/// limit ends the connection with a logged error instead of growing memory, and a bounded write
/// buffer makes a peer that stops reading visible as `WriteBufferFull`.
#[derive(Clone, Copy)]
pub struct WsConfig {
    pub max_message_size: usize,
    pub max_frame_size: usize,
    pub max_write_buffer: usize,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            // A depth frame carries every level a window changed: a few kilobytes at BTC's pace.
            max_message_size: 8 << 20,
            max_frame_size: 2 << 20,
            // Streams are read-only apart from pongs; a few frames is all the buffer needs.
            max_write_buffer: 1 << 20,
        }
    }
}

impl WsConfig {
    fn websocket_config(&self) -> WebSocketConfig {
        WebSocketConfig::default()
            .max_message_size(Some(self.max_message_size))
            .max_frame_size(Some(self.max_frame_size))
            .max_write_buffer_size(self.max_write_buffer)
    }
}

/// A stream to keep open: where, how often it is checked, and what it accepts.
#[derive(Clone)]
pub struct WsTarget {
    pub url: String,
    pub heartbeat: Heartbeat,
    pub config: WsConfig,
    /// Messages sent once the connection is open.
    ///
    /// A venue that subscribes over the connection (Bybit, OKX, Hyperliquid send an `op` request)
    /// names them here; one that subscribes in the URL (Binance) leaves this empty. Either way the
    /// subscription is part of the connection, so a reconnect subscribes again without the broker
    /// handling it.
    pub subscribe: Vec<String>,
}

impl WsTarget {
    pub fn new(url: String, heartbeat: Heartbeat) -> Self {
        Self {
            url,
            heartbeat,
            config: WsConfig::default(),
            subscribe: Vec::new(),
        }
    }

    /// Adds the messages this connection sends once it is open.
    pub fn subscribing(mut self, messages: Vec<String>) -> Self {
        self.subscribe = messages;
        self
    }
}

// Preserve the effective policy: the fourth short failure starts a one-second
// delay; a connection lasting strictly more than 30 seconds resets the count.
fn reconnect_delay(error_count: &mut usize, connected_for: Duration) -> Duration {
    *error_count += 1;
    if connected_for > Duration::from_secs(30) {
        *error_count = 0;
    }
    if *error_count > 3 {
        Duration::from_secs(1)
    } else {
        Duration::ZERO
    }
}

async fn connect(
    target: &WsTarget,
    ws_tx: Sender<(DateTime<Utc>, Utf8Bytes)>,
) -> Result<(), anyhow::Error> {
    let request = target.url.as_str().into_client_request()?;
    let (ws_stream, _) =
        connect_async_with_config(request, Some(target.config.websocket_config()), false).await?;
    let heartbeat = target.heartbeat;
    let (mut write, mut read) = ws_stream.split();
    // A venue that subscribes over the connection gets its requests now that it is open; they go
    // before the read loop so the first frames already belong to a subscription.
    for message in &target.subscribe {
        write.send(Message::Text(message.as_str().into())).await?;
    }
    let (tx, mut rx) = unbounded_channel::<Bytes>();

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if write.send(Message::Pong(data)).await.is_err() {
                let _ = write.close().await;
                return;
            }
        }
    });

    let mut last_ping = HeartbeatInstant::now();
    let mut checker = interval(heartbeat.check_interval);

    loop {
        select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    if ws_tx.send((recv_time, text)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                Some(Ok(Message::Ping(data))) => {
                    if tx.send(data).is_err() {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::ConnectionAborted,
                            "closed",
                        )));
                    }
                    last_ping = HeartbeatInstant::now();
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, "closed");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "closed",
                    )));
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(e)) => {
                    return Err(Error::from(e));
                }
                None => {
                    break;
                }
            },
            _ = checker.tick() => {
                if last_ping.elapsed() > heartbeat.timeout {
                    warn!("Ping timeout.");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        "Ping",
                    )));
                }
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(target: WsTarget, ws_tx: Sender<(DateTime<Utc>, Utf8Bytes)>) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(error) = connect(&target, ws_tx.clone()).await {
            error!(?error, "websocket error");
            let delay = reconnect_delay(&mut error_count, connect_time.elapsed());
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};
    use tokio::{net::TcpListener, sync::mpsc::channel, time::advance};
    use tokio_tungstenite::accept_async;

    const HEARTBEAT: Heartbeat = Heartbeat {
        check_interval: Duration::from_millis(20),
        timeout: Duration::from_millis(500),
    };

    /// Aborts the task it holds when dropped, so a test cannot leak a connection.
    struct AbortOnDropGuard<T>(tokio::task::JoinHandle<T>);

    impl<T> AbortOnDropGuard<T> {
        fn new(handle: tokio::task::JoinHandle<T>) -> Self {
            Self(handle)
        }
    }

    impl<T> Drop for AbortOnDropGuard<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    fn target(url: &str) -> WsTarget {
        WsTarget {
            url: url.to_owned(),
            heartbeat: HEARTBEAT,
            config: WsConfig::default(),
            subscribe: Vec::new(),
        }
    }

    // Keep this task runnable while socket I/O is pending, so Tokio cannot
    // auto-advance the paused clock. Only explicit advance() calls move time.
    // The wall-clock bound detects hangs; it does not decide heartbeat behavior.
    async fn with_manual_time<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let started = Instant::now();
        std::future::poll_fn(|cx| {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "loopback test stalled"
            );
            match future.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        })
        .await
    }

    #[test]
    fn reconnect_delay_preserves_failure_and_reset_boundaries() {
        let mut count = 0;
        for failure in 1..=24 {
            let expected = if failure > 3 {
                Duration::from_secs(1)
            } else {
                Duration::ZERO
            };
            assert_eq!(reconnect_delay(&mut count, Duration::ZERO), expected);
            assert_eq!(count, failure);
        }
        // Exactly 30s is still a short failure, including the fourth failure.
        count = 3;
        assert_eq!(
            reconnect_delay(&mut count, Duration::from_secs(30)),
            Duration::from_secs(1)
        );
        assert_eq!(count, 4);
        assert_eq!(
            reconnect_delay(
                &mut count,
                Duration::from_secs(30) + Duration::from_nanos(1)
            ),
            Duration::ZERO
        );
        assert_eq!(count, 0);
        for _ in 0..3 {
            assert_eq!(reconnect_delay(&mut count, Duration::ZERO), Duration::ZERO);
        }
        assert_eq!(
            reconnect_delay(&mut count, Duration::ZERO),
            Duration::from_secs(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn loopback_forwards_text_replies_to_ping_and_reports_close() {
        with_manual_time(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}/stream", listener.local_addr().unwrap());
            let (tx, mut rx) = channel(8);
            let mut client =
                AbortOnDropGuard::new(tokio::spawn(
                    async move { connect(&target(&url), tx).await },
                ));
            let (tcp, _) = listener.accept().await.unwrap();
            let mut server = accept_async(tcp).await.unwrap();
            server
                .send(Message::Binary(Bytes::from_static(b"ignored")))
                .await
                .unwrap();
            server
                .send(Message::Pong(Bytes::from_static(b"ignored")))
                .await
                .unwrap();
            let before = Utc::now();
            server
                .send(Message::Text("market text".into()))
                .await
                .unwrap();
            let (received_at, text) = rx.recv().await.unwrap();
            assert_eq!(text.as_str(), "market text");
            assert!(received_at >= before && received_at <= Utc::now());
            server
                .send(Message::Ping(Bytes::from_static(b"heartbeat")))
                .await
                .unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Pong(Bytes::from_static(b"heartbeat"))
            );
            assert!(rx.try_recv().is_err());
            server.send(Message::Close(None)).await.unwrap();
            let error = (&mut client.0).await.unwrap().unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                ErrorKind::ConnectionAborted
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn loopback_ping_refreshes_deadline_then_missing_ping_times_out() {
        with_manual_time(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}/stream", listener.local_addr().unwrap());
            let (tx, mut rx) = channel(8);
            let mut client =
                AbortOnDropGuard::new(tokio::spawn(
                    async move { connect(&target(&url), tx).await },
                ));
            let (tcp, _) = listener.accept().await.unwrap();
            let mut server = accept_async(tcp).await.unwrap();
            server.send(Message::Text("ready".into())).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().1.as_str(), "ready");

            // Move beyond the initial deadline, confirming each distinct Ping
            // was processed before advancing the clock again.
            for id in 0..4 {
                advance(Duration::from_millis(200)).await;
                let payload = Bytes::from(format!("ping-{id}"));
                server.send(Message::Ping(payload.clone())).await.unwrap();
                loop {
                    match server.next().await.unwrap().unwrap() {
                        Message::Pong(data) if data == payload => break,
                        // The WS implementation may also flush an automatic Pong.
                        Message::Pong(_) => {}
                        message => panic!("expected Pong, received {message:?}"),
                    }
                }
                assert!(!client.0.is_finished());
            }

            // Text provides an ordered receive barrier after each unsolicited
            // Pong. Neither frame may move the last-Ping deadline.
            for id in 0..4 {
                advance(Duration::from_millis(100)).await;
                server
                    .send(Message::Pong(Bytes::from_static(b"not a ping")))
                    .await
                    .unwrap();
                let text = format!("text-{id}");
                server
                    .send(Message::Text(text.clone().into()))
                    .await
                    .unwrap();
                assert_eq!(rx.recv().await.unwrap().1.as_str(), text);
                assert!(!client.0.is_finished());
            }
            // 520ms since the last Ping, but only 120ms since the last Text/Pong.
            advance(Duration::from_millis(120)).await;
            let error = (&mut client.0).await.unwrap().unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                ErrorKind::TimedOut
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn loopback_reconnects_and_stops_when_receiver_closes() {
        with_manual_time(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/stream", listener.local_addr().unwrap());
            let (tx, rx) = channel(8);
            let mut client = AbortOnDropGuard::new(tokio::spawn(async move {
                keep_connection(target(&endpoint), tx).await;
            }));
            for attempt in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut server = accept_async(tcp).await.unwrap();
                if attempt == 0 {
                    server.send(Message::Close(None)).await.unwrap();
                } else {
                    drop(rx);
                    server
                        .send(Message::Text("receiver closed".into()))
                        .await
                        .unwrap();
                    (&mut client.0).await.unwrap();
                    break;
                }
            }
        })
        .await;
    }
}
