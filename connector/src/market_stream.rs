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

use chrono::Utc;
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
    /// How often open interest is polled; zero disables the poll.
    ///
    /// Binance publishes open interest only over REST. The recorder used to poll it itself, with
    /// an endpoint table that knew Binance's two hosts and nothing else; the poll lives beside the
    /// stream it belongs to now, as it already does for the other venues.
    pub open_interest_interval: Duration,
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
            open_interest_interval: Duration::from_secs(0),
        }
    }

    /// Sets how often open interest is polled; zero disables the poll.
    pub fn with_open_interest_interval(mut self, interval: Duration) -> Self {
        self.open_interest_interval = interval;
        self
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
    /// Attaches a task that lives and dies with the stream.
    ///
    /// A venue's REST poll (open interest) is not part of the socket loop but must stop with it;
    /// holding the task here is what makes that true, and `shutdown` waits for it.
    fn add_task(&mut self, task: tokio::task::JoinHandle<()>) {
        self.tasks.push(task);
    }

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
    let mut handle = run_stream_loop(ev_tx.clone(), command_tx, move |command_rx, ev_tx| {
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
    .await?;
    if !config.open_interest_interval.is_zero() {
        // Binance answers open interest only over REST. USDⓈ-M serves it from `fapi`, COIN-M from
        // `dapi`, which is exactly the base the depth snapshot already uses for each of them.
        let endpoint = binance_open_interest_endpoint(&config.api_url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| MarketStreamError::Config("open interest HTTP client"))?;
        let symbol = config.symbol.clone();
        let mut shutdown = handle.shutdown.subscribe();
        let interval = config.open_interest_interval;
        let task = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let row: serde_json::Value = match client
                            .get(&endpoint)
                            .query(&[("symbol", symbol.as_str())])
                            .send()
                            .await
                        {
                            // A missing sample, not a broken book: the next tick asks again.
                            Err(error) => {
                                tracing::debug!(%symbol, %error, "open interest poll failed");
                                continue;
                            }
                            Ok(response) => match response.error_for_status() {
                                Err(error) => {
                                    tracing::debug!(%symbol, %error, "open interest poll refused");
                                    continue;
                                }
                                Ok(response) => match response.json().await {
                                    Ok(row) => row,
                                    Err(error) => {
                                        tracing::debug!(
                                            %symbol,
                                            %error,
                                            "open interest response unreadable"
                                        );
                                        continue;
                                    }
                                },
                            },
                        };
                        let Some(open_interest) = row.get("openInterest").and_then(|v| v.as_str())
                        else {
                            tracing::debug!(%symbol, "open interest sample without a value");
                            continue;
                        };
                        let exch_ts = row
                            .get("time")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(|ms| ms.checked_mul(1_000_000))
                            .unwrap_or_else(|| Utc::now().timestamp_nanos_opt().unwrap_or(0));
                        ev_tx.send(PublishEvent::OpenInterest {
                            symbol: symbol.clone(),
                            open_interest: open_interest.to_owned(),
                            // Binance reports open interest in contracts.
                            unit: crate::connector::OpenInterestUnit::Contracts,
                            exch_ts,
                        });
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        });
        handle.add_task(task);
    }
    Ok(handle)
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
    /// The REST base the open interest is read from.
    pub api_url: String,
    /// The instrument id, e.g. `BTC-USDT-SWAP`.
    pub symbol: String,
    /// Which market data kinds to subscribe to.
    pub kinds: Vec<MarketDataKind>,
    /// How often open interest is polled; zero disables the poll.
    ///
    /// OKX does publish an `open-interest` websocket channel, but it is not part of the connector's
    /// market stream and the data recorder needs a sample per cadence rather than a push when the
    /// venue chooses to send one.
    pub open_interest_interval: Duration,
    /// The OKX product the symbol belongs to (`SWAP`, `FUTURES`, ...). The open interest query
    /// requires it beside the instrument id.
    pub open_interest_inst_type: String,
}

#[cfg(feature = "okx")]
impl OkxMarketStreamConfig {
    /// Production OKX public websocket, subscribing to depth and trades for `symbol`.
    pub fn new(symbol: impl Into<String>, kinds: Vec<MarketDataKind>) -> Self {
        let symbol = symbol.into();
        Self {
            stream_url: "wss://ws.okx.com:8443/ws/v5/public".to_owned(),
            api_url: "https://www.okx.com".to_owned(),
            open_interest_inst_type: okx_inst_type(&symbol).to_owned(),
            symbol,
            kinds,
            open_interest_interval: Duration::from_secs(0),
        }
    }

    /// Sets how often open interest is polled; zero disables the poll.
    pub fn with_open_interest_interval(mut self, interval: Duration) -> Self {
        self.open_interest_interval = interval;
        self
    }
}

/// The OKX `instType` of an instrument id, from the suffix OKX itself defines: `BTC-USDT-SWAP`
/// and `BTC-USDT-SWAP_...` are swaps, `BTC-USD-260327` futures, `BTC-USDT` a spot pair.
#[cfg(feature = "okx")]
fn okx_inst_type(symbol: &str) -> &'static str {
    let last = symbol.rsplit('-').next().unwrap_or_default();
    if last.starts_with("SWAP") {
        "SWAP"
    } else if symbol.matches('-').count() >= 2 {
        // `BASE-QUOTE-EXPIRY`: a dated contract.
        "FUTURES"
    } else {
        "SPOT"
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
    // The open interest poll is a second, REST-only task beside the stream. It runs on the stream's
    // own shutdown signal so a stopped stream stops asking OKX for samples too.
    let mut handle = run_stream_loop(ev_tx.clone(), command_tx, move |command_rx, ev_tx| {
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
    .await?;
    if !config.open_interest_interval.is_zero() {
        let client = crate::okx::rest::OkxClient::new(&config.api_url, "", "", "");
        let symbol = config.symbol.clone();
        let inst_type = config.open_interest_inst_type.clone();
        let mut shutdown = handle.shutdown.subscribe();
        let interval = config.open_interest_interval;
        let task = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            // A stream that was stalled does not owe OKX a burst of polls when it returns.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match client.get_open_interest_raw(&inst_type, &symbol).await {
                            // A missing sample, not a broken book: the next tick asks again.
                            Err(error) => {
                                tracing::warn!(%symbol, ?error, "OKX open interest poll failed");
                            }
                            Ok((open_interest, exch_ts)) => match open_interest.parse::<f64>() {
                                Ok(value) if value > 0.0 => {
                                    ev_tx.send(PublishEvent::OpenInterest {
                                        symbol: symbol.clone(),
                                        open_interest,
                                        // OKX's `oi` is a contract count.
                                        unit: crate::connector::OpenInterestUnit::Contracts,
                                        exch_ts: exch_ts.saturating_mul(1_000_000),
                                    });
                                }
                                _ => {
                                    tracing::debug!(%symbol, "open interest sample without a value")
                                }
                            },
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        });
        handle.add_task(task);
    }
    Ok(handle)
}

/// The open interest endpoint for a Binance REST base.
///
/// USDⓈ-M answers on `fapi` and COIN-M on `dapi`; the base already carries that choice, so the
/// endpoint follows it rather than a second table that could disagree with it.
fn binance_open_interest_endpoint(api_url: &str) -> String {
    let base = api_url.trim_end_matches('/');
    let family = if base.contains("dapi") {
        "dapi"
    } else {
        "fapi"
    };
    format!("{base}/{family}/v1/openInterest")
}

/// Where a Hyperliquid market stream connects, and what to subscribe to.
#[cfg(feature = "hyperliquid")]
#[derive(Clone, Debug)]
pub struct HyperliquidMarketStreamConfig {
    /// The websocket endpoint.
    pub stream_url: String,
    /// The `info` endpoint funding rate and open interest are read from — the full path, as
    /// `HyperliquidClient` posts to it directly. The bare host answers 404.
    pub info_url: String,
    /// The coin name, e.g. `BTC`.
    pub symbol: String,
    /// Which market data kinds to subscribe to.
    pub kinds: Vec<MarketDataKind>,
    /// Whether to use the fast `l2Book` subscription.
    pub l2_fast: bool,
    /// How often funding rate and open interest are polled; zero disables the poll.
    ///
    /// Hyperliquid has no public funding or open interest channel on the market stream: both are
    /// part of `metaAndAssetCtxs`, read over REST.
    pub derivative_poll_interval: Duration,
}

#[cfg(feature = "hyperliquid")]
impl HyperliquidMarketStreamConfig {
    /// Production Hyperliquid websocket, subscribing to `l2Book` and trades for `symbol`.
    pub fn new(symbol: impl Into<String>, kinds: Vec<MarketDataKind>) -> Self {
        Self {
            stream_url: "wss://api.hyperliquid.xyz/ws".to_owned(),
            info_url: "https://api.hyperliquid.xyz/info".to_owned(),
            symbol: symbol.into(),
            kinds,
            l2_fast: true,
            derivative_poll_interval: Duration::from_secs(0),
        }
    }

    /// Sets how often funding rate and open interest are polled; zero disables the poll.
    pub fn with_derivative_poll_interval(mut self, interval: Duration) -> Self {
        self.derivative_poll_interval = interval;
        self
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
    let info_url = config.info_url.clone();
    let mut handle = run_stream_loop(ev_tx.clone(), command_tx, move |command_rx, ev_tx| {
        let subscriptions = subscriptions.clone();
        let stream_url = config.stream_url.clone();
        let client_info_url = config.info_url.clone();
        let l2_fast = config.l2_fast;
        async move {
            let ws_config = HyperliquidWsConfig {
                ev_tx,
                order_manager: Arc::new(Mutex::new(HyperliquidOrderManager::new())),
                assets: Arc::new(Mutex::new(HashMap::new())),
                symbols: Arc::new(Mutex::new(HashSet::new())),
                // No account is subscribed on this path.
                account_address: String::new(),
                // The websocket path never posts to the exchange, but its type requires both URLs.
                client: HyperliquidClient::new(
                    &client_info_url,
                    "https://api.hyperliquid.xyz/exchange",
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
    .await?;
    if !config.derivative_poll_interval.is_zero() {
        // Hyperliquid publishes no funding or open interest channel on the market stream; both
        // come from `metaAndAssetCtxs`, so one poll feeds both records.
        let client = HyperliquidClient::new(&info_url, "https://api.hyperliquid.xyz/exchange");
        let symbol = config.symbol.clone();
        let mut shutdown = handle.shutdown.subscribe();
        let interval = config.derivative_poll_interval;
        let task = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // `metaAndAssetCtxs` carries no observation time of its own; the receive
                        // time is the only time the sample is known to hold for.
                        let observed_at = Utc::now().timestamp_nanos_opt().unwrap_or_default();
                        match client.get_funding_and_open_interest(&symbol).await {
                            Ok(Some((funding, open_interest))) => {
                                ev_tx.send(PublishEvent::Funding {
                                    symbol: symbol.clone(),
                                    funding_rate: funding,
                                    // Hyperliquid pays funding hourly and the next hour is not in
                                    // this response; zero states that, it is not a timestamp.
                                    next_funding_time: 0,
                                    exch_ts: observed_at,
                                });
                                ev_tx.send(PublishEvent::OpenInterest {
                                    symbol: symbol.clone(),
                                    open_interest,
                                    // `openInterest` is a coin quantity, not a contract count.
                                    unit: crate::connector::OpenInterestUnit::Base,
                                    exch_ts: observed_at,
                                });
                            }
                            Ok(None) => tracing::debug!(
                                %symbol,
                                "no funding or open interest sample for this coin"
                            ),
                            Err(error) => {
                                tracing::warn!(
                                    %symbol,
                                    ?error,
                                    "Hyperliquid derivative poll failed"
                                );
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        });
        handle.add_task(task);
    }
    Ok(handle)
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

#[cfg(test)]
mod binance_open_interest_tests {
    use super::binance_open_interest_endpoint;

    #[test]
    fn the_coin_margined_base_keeps_its_own_endpoint() {
        // COIN-M answers on `dapi`; sending it a `fapi` path would ask USDⓈ-M for a symbol it does
        // not have, which is the mistake the recorder's old endpoint table had to be written to
        // avoid.
        assert_eq!(
            binance_open_interest_endpoint("https://fapi.binance.com"),
            "https://fapi.binance.com/fapi/v1/openInterest"
        );
        assert_eq!(
            binance_open_interest_endpoint("https://dapi.binance.com/"),
            "https://dapi.binance.com/dapi/v1/openInterest"
        );
    }
}

#[cfg(all(test, feature = "okx"))]
mod okx_inst_type_tests {
    use super::okx_inst_type;

    #[test]
    fn the_okx_product_is_read_from_the_instrument_id() {
        // The open interest query requires `instType` beside `instId`; getting it wrong returns an
        // empty result rather than an error, so the classification is what decides whether a
        // derivative market has any open interest sample at all.
        assert_eq!(okx_inst_type("BTC-USDT-SWAP"), "SWAP");
        assert_eq!(okx_inst_type("BTC-USD-SWAP"), "SWAP");
        assert_eq!(okx_inst_type("BTC-USDT-SWAP_UMCBL"), "SWAP");
        assert_eq!(okx_inst_type("BTC-USD-260327"), "FUTURES");
        assert_eq!(okx_inst_type("BTC-USDT"), "SPOT");
    }

    #[test]
    fn every_derivative_symbol_the_recorder_uses_is_classified_as_one() {
        for symbol in ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "BTC-USD-260327"] {
            assert_ne!(okx_inst_type(symbol), "SPOT", "{symbol}");
        }
    }
}

#[cfg(all(test, feature = "okx", feature = "hyperliquid"))]
mod derivative_poll_tests {
    //! The open interest (and, for Hyperliquid, funding) polls are the only source of those
    //! records for these venues, so each is exercised against a REST stub that answers what the
    //! venue answers. Without these the polls could be wired to nothing without any test noticing.

    use std::{sync::Arc, time::Duration};

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::*;

    /// Whether a publication is the stream loop reporting that its socket ended.
    ///
    /// These tests point the websocket at the REST stub, so a failed connection is expected and
    /// must not be mistaken for a poll sample; the distinction was worth spelling out because
    /// reading it from a printed discriminant was not obvious the first time.
    fn is_connector_error(event: &PublishEvent) -> bool {
        matches!(event, PublishEvent::ConnectorError(_))
    }

    /// The next publication that is not the stream loop's own connection report.
    async fn next_sample(receiver: &mut mpsc::UnboundedReceiver<PublishEvent>) -> PublishEvent {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
                .await
                .expect("a derivative sample")
                .expect("a publication");
            if !is_connector_error(&event) {
                return event;
            }
        }
    }

    /// Records what a poll publishes, for a test that only counts how often one runs.
    #[allow(clippy::type_complexity)]
    fn counting_sender() -> (
        PublishSender,
        Arc<std::sync::atomic::AtomicUsize>,
        mpsc::UnboundedReceiver<PublishEvent>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = count.clone();
        let sender = crate::connector::test_publish_sender(Arc::new(move |event| {
            // Counts what a poll publishes, so a test can show a stopped poll publishes nothing.
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = tx.send(event);
        }));
        (sender, count, rx)
    }

    #[tokio::test]
    async fn okx_poll_publishes_the_open_interest_sample_the_record_stores() {
        let (root, mut requests, _server) = crate::venue::test_support::http(vec![
            // The websocket handshake cannot succeed against this stub; see the note in the
            // Hyperliquid poll test. It consumes the first response.
            (200, json!({})),
            (
                200,
                json!({
                    "code": "0",
                    "msg": "",
                    "data": [{"instId": "BTC-USDT-SWAP", "oi": "1234567.89", "oiCcy": "1234.56789", "ts": "1700000000123"}]
                }),
            ),
        ])
        .await;
        let (sender, _count, mut events) = counting_sender();
        let config = OkxMarketStreamConfig {
            api_url: root,
            ..OkxMarketStreamConfig::new("BTC-USDT-SWAP", vec![MarketDataKind::Depth])
        }
        .with_open_interest_interval(Duration::from_millis(50));
        let handle = start_okx_market_stream(config, sender)
            .await
            .expect("the REST poll starts even when the socket cannot connect");
        let _handshake = tokio::time::timeout(Duration::from_secs(10), requests.recv())
            .await
            .expect("the websocket handshake reaches the stub")
            .expect("a request");
        let sample = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("an open interest sample")
            .expect("a publication");
        handle.shutdown().await;
        match sample {
            PublishEvent::OpenInterest {
                symbol,
                open_interest,
                unit,
                exch_ts,
            } => {
                assert_eq!(unit, crate::connector::OpenInterestUnit::Contracts);
                assert_eq!(symbol, "BTC-USDT-SWAP");
                // The venue's own text is kept: rounding it here would lose the decimals the record
                // stores and leave the quality report disagreeing with what OKX said.
                assert_eq!(open_interest, "1234567.89");
                assert_eq!(exch_ts, 1_700_000_000_123_000_000);
            }
            other => panic!(
                "expected an open interest sample, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[tokio::test]
    async fn hyperliquid_poll_publishes_funding_and_open_interest_from_one_response() {
        // Hyperliquid's market stream carries no funding or open interest channel, so this poll is
        // the only source of both records. One `metaAndAssetCtxs` answer feeds both.
        //
        // The poll and the websocket get separate stubs: the websocket one is wrapped and then
        // fails to speak websocket, which is a connection report, not a sample, and sharing one
        // stub would make which request sees which answer depend on timing.
        let (api, _api_requests, _api_server) = crate::venue::test_support::http(vec![(
            200,
            // Written as JSON text: `json!` cannot express a top-level array that begins with an
            // object, and `metaAndAssetCtxs` is exactly `[meta, [ctx, ...]]`.
            serde_json::from_str(
                r#"[{"universe":[{"name":"BTC"},{"name":"ETH"}]},
                    [{"funding":"0.0000125","openInterest":"1234.56","markPx":"75000"},
                     {"funding":"0.00002","openInterest":"99","markPx":"3000"}]]"#,
            )
            .expect("the fixture is a JSON array"),
        )])
        .await;
        let (stream_url, _stream_requests, _stream_server) =
            crate::venue::test_support::http(vec![(200, json!({}))]).await;
        let (sender, _count, mut events) = counting_sender();
        let config = HyperliquidMarketStreamConfig {
            // The client posts to this URL as given, so the stub's root is what a real deployment
            // must spell out: production is `https://api.hyperliquid.xyz/info`, not the host.
            info_url: format!("{api}/info"),
            stream_url,
            ..HyperliquidMarketStreamConfig::new("BTC", vec![MarketDataKind::Depth])
        }
        .with_derivative_poll_interval(Duration::from_millis(50));
        let handle = start_hyperliquid_market_stream(config, sender)
            .await
            .expect("the REST poll starts even when the socket cannot connect");
        let first = next_sample(&mut events).await;
        let second = next_sample(&mut events).await;
        handle.shutdown().await;
        match (first, second) {
            (
                PublishEvent::Funding {
                    symbol,
                    funding_rate,
                    next_funding_time,
                    exch_ts,
                },
                PublishEvent::OpenInterest {
                    symbol: oi_symbol,
                    open_interest,
                    unit,
                    exch_ts: oi_ts,
                },
            ) => {
                assert_eq!(symbol, "BTC");
                assert_eq!(oi_symbol, "BTC");
                assert!(funding_rate > 0.0 && funding_rate < 0.001, "{funding_rate}");
                // The venue does not state the next funding time here; zero says unknown.
                assert_eq!(next_funding_time, 0);
                // Open interest is the venue's own string, and this coin's, not ETH's.
                assert_eq!(open_interest, "1234.56");
                // Hyperliquid quotes coin quantity; recording it as a contract count would be a
                // different number under the same label.
                assert_eq!(unit, crate::connector::OpenInterestUnit::Base);
                // Both records come from one observation, so they carry the same time.
                assert_eq!(exch_ts, oi_ts);
            }
            (first, second) => panic!(
                "expected funding then open interest, got error={} and error={}",
                is_connector_error(&first),
                is_connector_error(&second)
            ),
        }
    }

    #[test]
    fn the_hyperliquid_poll_posts_to_the_info_path() {
        // `HyperliquidClient` posts to `info_url` verbatim, and the bare host answers 404: the
        // first live deployment of this poll did exactly that and recorded no sample. The default
        // must therefore be the full path, and it must match the one the trading path uses.
        let config = HyperliquidMarketStreamConfig::new("BTC", vec![MarketDataKind::Depth]);
        assert_eq!(config.info_url, "https://api.hyperliquid.xyz/info");
        assert!(config.info_url.ends_with("/info"));
    }

    #[tokio::test]
    async fn hyperliquid_publishes_nothing_for_a_coin_the_universe_does_not_have() {
        // A misspelt or delisted coin must not produce a zero record that the quality report would
        // count as a real sample.
        //
        // The websocket root is unroutable on purpose: its connection report is the only other
        // publication this setup can make, and a fast-refused connection keeps it out of the way
        // instead of racing the poll's timeout.
        let (api, _api_requests, _api_server) = crate::venue::test_support::http(vec![(
            200,
            serde_json::from_str(
                r#"[{"universe":[{"name":"ETH"}]},
                    [{"funding":"0.00002","openInterest":"99","markPx":"3000"}]]"#,
            )
            .expect("the fixture is a JSON array"),
        )])
        .await;
        let (sender, _count, mut events) = counting_sender();
        let config = HyperliquidMarketStreamConfig {
            info_url: format!("{api}/info"),
            stream_url: "ws://127.0.0.1:1".to_owned(),
            ..HyperliquidMarketStreamConfig::new("BTC", vec![MarketDataKind::Depth])
        }
        .with_derivative_poll_interval(Duration::from_millis(20));
        let handle = start_hyperliquid_market_stream(config, sender)
            .await
            .expect("the REST poll starts");
        // Give the poll every chance to publish.
        let published = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                let event = events.recv().await.expect("a publication");
                if !is_connector_error(&event) {
                    return event;
                }
            }
        })
        .await;
        handle.shutdown().await;
        assert!(
            published.is_err(),
            "a coin the universe does not have must publish no sample"
        );
    }

    #[tokio::test]
    async fn okx_stops_polling_when_the_stream_handle_is_dropped() {
        // A poll that outlives its stream keeps spending the venue's rate limit for a market
        // nothing records. The stub never answers, so a poll that survives the drop is the only
        // way a sample could appear after the handle is gone.
        let (root, _requests, _server) =
            crate::venue::test_support::http(vec![(0, json!({"code": "0", "data": []}))]).await;
        let (sender, count, _events) = counting_sender();
        let config = OkxMarketStreamConfig {
            api_url: root,
            ..OkxMarketStreamConfig::new("BTC-USDT-SWAP", vec![MarketDataKind::Depth])
        }
        .with_open_interest_interval(Duration::from_millis(20));
        let handle = start_okx_market_stream(config, sender)
            .await
            .expect("the REST poll starts");
        handle.shutdown().await;
        let after_shutdown = count.load(std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            count.load(std::sync::atomic::Ordering::Relaxed),
            after_shutdown,
            "a stopped stream must not keep polling"
        );
    }
}
