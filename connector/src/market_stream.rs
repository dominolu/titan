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

use titan_market_service::MarketDataKind;
use tokio::sync::broadcast;

use crate::binancefutures::{
    BinanceFuturesClient, BinanceFuturesError,
    market_data_stream::{MarketDataStream, MarketStreamRoute},
};
use crate::connector::{MarketDataCommand, PublishEvent, PublishSender};
#[cfg(feature = "hyperliquid")]
use crate::hyperliquid::client::HyperliquidClient;
#[cfg(feature = "hyperliquid")]
use crate::hyperliquid::ordermanager::OrderManager as HyperliquidOrderManager;
#[cfg(feature = "hyperliquid")]
use crate::hyperliquid::ws::{HyperliquidWs, HyperliquidWsConfig};
#[cfg(feature = "okx")]
use crate::okx::public_stream::PublicStream as OkxPublicStream;
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
    run_stream_loop(ev_tx, command_tx, move |command_rx, ev_tx| {
        let client = client.clone();
        let subscriptions = subscriptions.clone();
        let endpoints = market_stream_endpoints(&config.stream_url);
        async move {
            let mut last_error = None;
            for (base_url, route) in endpoints {
                let mut stream = MarketDataStream::new(
                    client.clone(),
                    ev_tx.clone(),
                    command_rx.resubscribe(),
                    subscriptions.clone(),
                    route,
                );
                tracing::info!(?route, %base_url, "connecting the market data stream");
                if let Err(error) = stream.connect(&base_url).await {
                    stream.invalidate_depth();
                    last_error = Some(error.to_string());
                } else {
                    return Ok(());
                }
            }
            Err(last_error.unwrap_or_else(|| "no market stream endpoint".to_owned()))
        }
    })
    .await
}

/// Runs one venue stream until it returns or the handle is dropped.
///
/// Every venue answers the same questions the same way: reconnect when the connection ends, back
/// off from a second up to a minute so a flapping socket stays cheap, report the failure as a
/// `ConnectorError`, and stop only when the handle says so. Only *how* a venue connects differs,
/// which is the `connect` closure's whole job.
async fn run_stream_loop<C, F>(
    ev_tx: PublishSender,
    command_tx: broadcast::Sender<MarketDataCommand>,
    mut connect: C,
) -> Result<MarketStreamHandle, MarketStreamError>
where
    C: FnMut(broadcast::Receiver<MarketDataCommand>, PublishSender) -> F + Send + 'static,
    F: std::future::Future<Output = Result<(), String>> + Send,
{
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    // A fresh receiver reports its first `changed()` immediately; acknowledge it once here so the
    // per-stream loops only ever wake on a real shutdown signal.
    let _ = shutdown_rx.borrow_and_update();
    let command_rx = command_tx.subscribe();
    let mut task_shutdown = shutdown_rx.clone();
    let task = tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if *task_shutdown.borrow() {
                return;
            }
            // `changed()` resolves immediately once for a receiver that has not yet seen the
            // current value, so the shutdown arm is biased: a stale wake-up must not pre-empt the
            // connection it was meant to interrupt.
            let result: Result<(), String> = tokio::select! {
                biased;
                result = connect(command_rx.resubscribe(), ev_tx.clone()) => result,
                changed = task_shutdown.changed() => {
                    if changed.is_err() || *task_shutdown.borrow() {
                        return;
                    }
                    Ok(())
                }
            };
            if let Err(error) = &result {
                tracing::error!(%error, "the market data stream ended");
                ev_tx.send(PublishEvent::ConnectorError(LiveError::with(
                    ErrorKind::ConnectionInterrupted,
                    titan_domain_types::Value::String(error.clone()),
                )));
            }
            // A stream that was refused waits the delay the venue asked for; anything else backs
            // off from a second up to a minute so a flapping connection stays cheap.
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
    });
    Ok(MarketStreamHandle {
        shutdown,
        _commands: command_tx,
        tasks: vec![task],
    })
}

/// Where an OKX public market stream connects, and what to subscribe to.
#[cfg(feature = "okx")]
#[derive(Clone, Debug)]
pub struct OkxMarketStreamConfig {
    /// OKX's public websocket endpoint.
    pub stream_url: String,
    /// The instrument id, e.g. `BTC-USDT-SWAP`.
    pub symbol: String,
    /// Which market data kinds to subscribe to.
    pub kinds: Vec<MarketDataKind>,
}

#[cfg(feature = "okx")]
impl OkxMarketStreamConfig {
    /// Production OKX public websocket, subscribing to depth and trades for `symbol`.
    pub fn new(symbol: impl Into<String>, kinds: Vec<MarketDataKind>) -> Self {
        Self {
            stream_url: "wss://ws.okx.com:8443/ws/v5/public".to_owned(),
            symbol: symbol.into(),
            kinds,
        }
    }
}

/// Starts an OKX public market stream, publishing into `ev_tx`, and returns its handle.
///
/// OKX answers a resubscribe with a fresh snapshot, so a book that falls out of sequence recovers
/// on the same connection; the stream only reconnects when the socket itself ends.
#[cfg(feature = "okx")]
pub async fn start_okx_market_stream(
    config: OkxMarketStreamConfig,
    ev_tx: PublishSender,
) -> Result<MarketStreamHandle, MarketStreamError> {
    if config.symbol.is_empty() {
        return Err(MarketStreamError::Config("empty symbol"));
    }
    if config.kinds.is_empty() {
        return Err(MarketStreamError::Config("no market data kinds requested"));
    }
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut guard = subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert(
            config.symbol.clone(),
            config.kinds.iter().copied().collect::<HashSet<_>>(),
        );
    }
    let (command_tx, _command_rx) = broadcast::channel(64);
    run_stream_loop(ev_tx, command_tx, move |command_rx, ev_tx| {
        let subscriptions = subscriptions.clone();
        let stream_url = config.stream_url.clone();
        async move {
            let mut stream = OkxPublicStream::new(ev_tx, command_rx, subscriptions);
            stream
                .connect(&stream_url)
                .await
                .map_err(|error| error.to_string())
        }
    })
    .await
}

/// Where a Hyperliquid market stream connects, and what to subscribe to.
#[cfg(feature = "hyperliquid")]
#[derive(Clone, Debug)]
pub struct HyperliquidMarketStreamConfig {
    /// The websocket endpoint.
    pub stream_url: String,
    /// The coin name, e.g. `BTC`.
    pub symbol: String,
    /// Which market data kinds to subscribe to.
    pub kinds: Vec<MarketDataKind>,
    /// Whether to use the fast `l2Book` subscription.
    pub l2_fast: bool,
}

#[cfg(feature = "hyperliquid")]
impl HyperliquidMarketStreamConfig {
    /// Production Hyperliquid websocket, subscribing to `l2Book` and trades for `symbol`.
    pub fn new(symbol: impl Into<String>, kinds: Vec<MarketDataKind>) -> Self {
        Self {
            stream_url: "wss://api.hyperliquid.xyz/ws".to_owned(),
            symbol: symbol.into(),
            kinds,
            l2_fast: true,
        }
    }
}

/// Starts a Hyperliquid market stream, publishing into `ev_tx`, and returns its handle.
///
/// Only public channels are subscribed: `l2Book` carries a whole book per message, so there is no
/// increment to lose, and no account channel is opened. The order manager and asset table the
/// websocket type carries are for the trading path and stay empty here.
#[cfg(feature = "hyperliquid")]
pub async fn start_hyperliquid_market_stream(
    config: HyperliquidMarketStreamConfig,
    ev_tx: PublishSender,
) -> Result<MarketStreamHandle, MarketStreamError> {
    if config.symbol.is_empty() {
        return Err(MarketStreamError::Config("empty symbol"));
    }
    if config.kinds.is_empty() {
        return Err(MarketStreamError::Config("no market data kinds requested"));
    }
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut guard = subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert(
            config.symbol.clone(),
            config.kinds.iter().copied().collect::<HashSet<_>>(),
        );
    }
    let (command_tx, _command_rx) = broadcast::channel(64);
    run_stream_loop(ev_tx, command_tx, move |command_rx, ev_tx| {
        let subscriptions = subscriptions.clone();
        let stream_url = config.stream_url.clone();
        let l2_fast = config.l2_fast;
        async move {
            let ws_config = HyperliquidWsConfig {
                ev_tx,
                order_manager: Arc::new(Mutex::new(HyperliquidOrderManager::new())),
                assets: Arc::new(Mutex::new(HashMap::new())),
                symbols: Arc::new(Mutex::new(HashSet::new())),
                // No account is subscribed on this path.
                account_address: String::new(),
                client: HyperliquidClient::new(
                    "https://api.hyperliquid.xyz",
                    "https://api.hyperliquid.xyz",
                ),
                command_rx,
                market_subscriptions: subscriptions,
                l2_fast,
                private_channels: false,
                // Only the reconnect tests drive this handle; a real stream never sets it.
                #[cfg(test)]
                reconnect_fault: Arc::new(std::sync::atomic::AtomicU8::new(0)),
            };
            let mut stream = HyperliquidWs::new(ws_config);
            stream
                .connect(&stream_url)
                .await
                .map_err(|error| error.to_string())
        }
    })
    .await
}

/// Where a Bybit V5 public market stream connects, and what to subscribe to.
#[cfg(feature = "bybit")]
#[derive(Clone, Debug)]
pub struct BybitMarketStreamConfig {
    /// The websocket endpoint. Built from the category by [`Self::new`].
    pub stream_url: String,
    /// The REST base the orderbook image and open interest are read from.
    pub api_url: String,
    /// Which Bybit product the symbol belongs to.
    pub category: crate::bybit::BybitCategory,
    /// The venue's symbol, e.g. `BTCUSDT`.
    pub symbol: String,
    /// The depth subscription: Bybit publishes 1, 50, 200 and 500 levels and nothing else.
    pub depth: u16,
    /// How often open interest is polled, for a derivative market that records it.
    pub open_interest_interval: Duration,
}

#[cfg(feature = "bybit")]
impl BybitMarketStreamConfig {
    /// Production Bybit public data for `symbol` in `category`, recording `depth_levels` levels.
    ///
    /// The subscription depth is the smallest published depth that covers `depth_levels`, which is
    /// also what the recorder reports as `depth_available`.
    pub fn new(
        category: crate::bybit::BybitCategory,
        symbol: impl Into<String>,
        depth_levels: u16,
    ) -> Self {
        let config = crate::bybit::BybitMarketStreamConfig::new(category, symbol, depth_levels);
        Self {
            stream_url: config.stream_url,
            api_url: config.api_url,
            category,
            symbol: config.symbol,
            depth: config.depth,
            open_interest_interval: Duration::from_secs(0),
        }
    }

    /// Sets how often open interest is polled; zero disables the poll.
    pub fn with_open_interest_interval(mut self, interval: Duration) -> Self {
        self.open_interest_interval = interval;
        self
    }
}

/// Starts a Bybit public market stream, publishing into `ev_tx`, and returns its handle.
///
/// The stream checks the orderbook's update id itself. A lost increment publishes a
/// `StreamInvalidated` and requests the book again over the same connection, so a break costs the
/// trade stream nothing; only a connection that actually ends is retried by [`run_stream_loop`].
#[cfg(feature = "bybit")]
pub async fn start_bybit_market_stream(
    config: BybitMarketStreamConfig,
    ev_tx: PublishSender,
) -> Result<MarketStreamHandle, MarketStreamError> {
    if config.symbol.is_empty() {
        return Err(MarketStreamError::Config("empty symbol"));
    }
    if config.depth == 0 {
        return Err(MarketStreamError::Config("no depth levels requested"));
    }
    let (command_tx, _command_rx) = broadcast::channel(64);
    run_stream_loop(ev_tx, command_tx, move |_command_rx, ev_tx| {
        let config = config.clone();
        async move {
            let inner = crate::bybit::BybitMarketStreamConfig {
                stream_url: config.stream_url,
                api_url: config.api_url,
                category: config.category,
                symbol: config.symbol,
                depth: config.depth,
                open_interest_interval: config.open_interest_interval,
                heartbeat: crate::transport::Heartbeat {
                    check_interval: Duration::from_secs(15),
                    timeout: Duration::from_secs(60),
                },
            };
            // One connection attempt: `run_stream_loop` owns the retry policy that every venue
            // shares, so this stream must not run a second retry loop of its own inside it.
            let mut stream = crate::bybit::run::MarketStream::new(inner, ev_tx);
            stream.connect().await.map_err(|error| error.to_string())
        }
    })
    .await
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
