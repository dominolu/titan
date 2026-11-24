//! Public entry point for a venue's market data stream.
//!
//! The connector assembles a market stream from several pieces: the HTTP client that fetches
//! depth snapshots, the subscriptions it must carry, the endpoint route that decides which of
//! Binance's websocket hosts a given stream belongs on, and the retry loop that keeps it open.
//! A recorder needs the same stream and none of those decisions, so this module is the one call
//! it makes: a symbol, the kinds to subscribe, and where the events should go.
//!
//! Nothing here is specific to recording. The live runtime assembles the same pieces internally;
//! this is the supported way for another binary to do so without reaching into connector internals.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::connector::MarketDataCommand;
use titan_market_service::MarketDataKind;
use tokio::sync::broadcast;
use tracing::error;

use crate::binancefutures::{
    BinanceFuturesClient, BinanceFuturesError,
    market_data_stream::{MarketDataStream, MarketStreamRoute},
};
use crate::connector::{PublishEvent, PublishSender};
use titan_domain_types::{ErrorKind, LiveError};

/// Where a Binance USDⓈ-M market stream connects, and what to subscribe to.
#[derive(Clone, Debug)]
pub struct BinanceMarketStreamConfig {
    /// The REST base the depth snapshot is fetched from.
    pub api_url: String,
    /// The websocket root. Binance's production root is expanded into its two route hosts.
    pub stream_url: String,
    /// The symbol whose depth, trades and (futures) funding this stream carries.
    pub symbol: String,
    /// Which market data kinds to subscribe to.
    pub kinds: Vec<MarketDataKind>,
    /// How many levels a depth snapshot requests.
    pub depth_snapshot_limit: u16,
}

impl BinanceMarketStreamConfig {
    /// Production Binance USDⓈ-M, subscribing to depth and trades for `symbol`.
    pub fn new(symbol: impl Into<String>, kinds: Vec<MarketDataKind>) -> Self {
        Self {
            api_url: "https://fapi.binance.com".to_owned(),
            stream_url: "wss://fstream.binance.com".to_owned(),
            symbol: symbol.into(),
            kinds,
            depth_snapshot_limit: 1000,
        }
    }
}

/// A handle to a running market stream.
///
/// Dropping it stops the stream: the retry loop and the socket it holds are aborted, so a caller
/// that goes away does not leave a connection publishing into a closed channel.
pub struct MarketStreamHandle {
    shutdown: tokio::sync::watch::Sender<bool>,
    /// Kept alive for the life of the stream: `MarketDataStream` treats a closed command channel
    /// as "stop", so dropping the last sender would end every connection immediately.
    _commands: broadcast::Sender<MarketDataCommand>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl MarketStreamHandle {
    /// Stops the stream and waits for its tasks to finish.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

/// The reason a market stream could not be started.
#[derive(Debug, thiserror::Error)]
pub enum MarketStreamError {
    #[error("unsupported market stream configuration: {0}")]
    Config(&'static str),
    #[error("{0}")]
    Client(#[from] BinanceFuturesError),
}

/// Starts a Binance USDⓈ-M market stream, publishing into `ev_tx`, and returns its handle.
///
/// The stream retries on its own and reports a connection failure as a `ConnectorError` event
/// followed by a `StreamInvalidated` for each symbol, which is what a consumer needs to record a
/// gap. It never decides for the caller whether to stop.
pub async fn start_binance_market_stream(
    config: BinanceMarketStreamConfig,
    ev_tx: PublishSender,
) -> Result<MarketStreamHandle, MarketStreamError> {
    if config.symbol.is_empty() {
        return Err(MarketStreamError::Config("empty symbol"));
    }
    if config.kinds.is_empty() {
        return Err(MarketStreamError::Config("no market data kinds requested"));
    }
    let client = BinanceFuturesClient::new(&config.api_url, "", "")
        .with_depth_snapshot_limit(config.depth_snapshot_limit)?;
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut guard = subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert(
            config.symbol.clone(),
            config.kinds.iter().copied().collect::<HashSet<_>>(),
        );
    }
    // The receiver itself is never read: the handle owns the sender so the channel stays open.
    let (command_tx, _command_rx) = broadcast::channel(64);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    // A fresh receiver reports its first `changed()` immediately; acknowledge it once here so the
    // per-stream loops only ever wake on a real shutdown signal.
    let _ = shutdown_rx.borrow_and_update();
    let mut tasks = Vec::new();
    for (base_url, route) in market_stream_endpoints(&config.stream_url) {
        let client = client.clone();
        let ev_tx = ev_tx.clone();
        let subscriptions = subscriptions.clone();
        let command_rx = command_tx.subscribe();
        let mut task_shutdown = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                if *task_shutdown.borrow() {
                    return;
                }
                let mut stream = MarketDataStream::new(
                    client.clone(),
                    ev_tx.clone(),
                    command_rx.resubscribe(),
                    subscriptions.clone(),
                    route,
                );
                tracing::info!(?route, %base_url, "connecting the market data stream");
                // `changed()` resolves immediately once for a receiver that has not yet seen the
                // current value, so the shutdown arm is biased below: a stale wake-up must not
                // pre-empt the connection it was meant to interrupt.
                let result: Result<(), BinanceFuturesError> = tokio::select! {
                    biased;
                    result = stream.connect(&base_url) => result,
                    changed = task_shutdown.changed() => {
                        if changed.is_err() || *task_shutdown.borrow() {
                            return;
                        }
                        Ok(())
                    }
                };
                if let Err(error) = &result {
                    error!(?error, %base_url, "the market data stream ended");
                    stream.invalidate_depth();
                    ev_tx.send(PublishEvent::ConnectorError(LiveError::with(
                        ErrorKind::ConnectionInterrupted,
                        connector_io_value(error),
                    )));
                }
                // A stream that was refused waits the delay the venue asked for; anything else
                // backs off from a second up to a minute so a flapping connection stays cheap.
                tokio::select! {
                    changed = task_shutdown.changed() => {
                        if changed.is_err() || *task_shutdown.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }));
    }
    Ok(MarketStreamHandle {
        shutdown,
        _commands: command_tx,
        tasks,
    })
}

/// The value a `LiveError` carries for a stream failure.
///
/// `Value` has no `From<String>`, so the text is wrapped in its string variant explicitly.
fn connector_io_value(error: &BinanceFuturesError) -> titan_domain_types::Value {
    titan_domain_types::Value::String(error.to_string())
}

/// The websocket endpoints and route for a stream root.
///
/// Binance serves high-frequency depth and trade frames on `/public/ws` and mark price, funding
/// and book ticker on `/market/ws`; a test or simulation root takes everything on one connection.
fn market_stream_endpoints(stream_url: &str) -> Vec<(String, MarketStreamRoute)> {
    const PRODUCTION_ROOT: &str = "wss://fstream.binance.com";
    if stream_url == PRODUCTION_ROOT || stream_url.starts_with(&format!("{PRODUCTION_ROOT}/")) {
        return vec![
            (
                format!("{PRODUCTION_ROOT}/public/ws"),
                MarketStreamRoute::Public,
            ),
            (
                format!("{PRODUCTION_ROOT}/market/ws"),
                MarketStreamRoute::Market,
            ),
        ];
    }
    vec![(stream_url.to_owned(), MarketStreamRoute::All)]
}
