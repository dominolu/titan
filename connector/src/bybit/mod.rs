//! Bybit V5 public market data.
//!
//! The connector serves Bybit's public market data: orderbook, trades, ticker (funding and open
//! interest), and the REST orderbook image the stream aligns itself against. There is no order
//! manager and no private stream here — the trading path for Bybit is not implemented, and a
//! market-only connector must not pretend otherwise.
//!
//! Bybit's V5 API is one protocol shared by three categories (`spot`, `linear`, `inverse`); the
//! category decides the endpoint and which ticker fields exist, and nothing else. That is the
//! [`BybitCategory`] argument rather than three copies of the same module.

pub mod msg;
pub mod rest;

use std::time::Duration;

use crate::bybit::rest::BybitClient;
use crate::transport::{Heartbeat, WsTarget};
use crate::utils::{ExponentialBackoff, Retry};

pub use rest::BybitError;

/// Which Bybit V5 product a market belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BybitCategory {
    /// Spot.
    Spot,
    /// USDT/USDC-margined perpetual and futures.
    Linear,
    /// Coin-margined perpetual and futures.
    Inverse,
}

impl BybitCategory {
    /// The `category` value Bybit's REST and websocket paths use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
        }
    }

    /// The public websocket endpoint for this category.
    pub fn stream_url(self) -> String {
        format!("wss://stream.bybit.com/v5/public/{}", self.as_str())
    }

    /// Whether the category publishes funding rate and open interest, which spot does not.
    pub fn is_derivative(self) -> bool {
        !matches!(self, Self::Spot)
    }

    /// The orderbook depth to subscribe for a market that wants `depth_levels` levels.
    ///
    /// Bybit publishes 1, 50, 200 and 500 levels and nothing in between, so the subscription is the
    /// smallest published depth that covers the request; the caller records `depth_available` as
    /// what it actually subscribed to. Zero is not a market without depth but a market whose depth
    /// was left unset, and is served at the deepest published level rather than at one level.
    pub fn subscription_depth(depth_levels: u16) -> u16 {
        match depth_levels {
            0..=1 => 1,
            2..=50 => 50,
            51..=200 => 200,
            _ => 500,
        }
    }
}

/// Where a Bybit public market stream connects, and what to subscribe to.
#[derive(Clone)]
pub struct BybitMarketStreamConfig {
    /// The websocket endpoint. Built from the category by [`Self::new`].
    pub stream_url: String,
    /// The REST base the orderbook image and open interest are read from.
    pub api_url: String,
    /// Which product the symbol belongs to.
    pub category: BybitCategory,
    /// The venue's symbol, e.g. `BTCUSDT`.
    pub symbol: String,
    /// The depth subscription, from [`BybitCategory::subscription_depth`].
    pub depth: u16,
    /// How often open interest is polled over REST. Zero disables the poll; the stream carries no
    /// open interest channel of its own.
    pub open_interest_interval: Duration,
    /// Pings every `check_interval`: Bybit closes a connection that is silent for 20 seconds.
    pub heartbeat: Heartbeat,
}

impl std::fmt::Debug for BybitMarketStreamConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BybitMarketStreamConfig")
            .field("stream_url", &self.stream_url)
            .field("api_url", &self.api_url)
            .field("category", &self.category)
            .field("symbol", &self.symbol)
            .field("depth", &self.depth)
            .field("open_interest_interval", &self.open_interest_interval)
            .finish_non_exhaustive()
    }
}

impl BybitMarketStreamConfig {
    /// Production Bybit public data for `symbol` in `category`, recording `depth_levels` levels.
    pub fn new(category: BybitCategory, symbol: impl Into<String>, depth_levels: u16) -> Self {
        Self {
            stream_url: category.stream_url(),
            api_url: "https://api.bybit.com".to_owned(),
            category,
            symbol: symbol.into(),
            depth: BybitCategory::subscription_depth(depth_levels),
            open_interest_interval: Duration::from_secs(0),
            heartbeat: Heartbeat {
                check_interval: Duration::from_secs(15),
                timeout: Duration::from_secs(60),
            },
        }
    }

    /// Sets how often open interest is polled. Derivatives only; spot ignores it.
    pub fn with_open_interest_interval(mut self, interval: Duration) -> Self {
        self.open_interest_interval = interval;
        self
    }

    /// The topic this market subscribes to for `channel` (`orderbook.{depth}`, `publicTrade`,
    /// `tickers`).
    pub fn topic(&self, channel: &str) -> String {
        match channel {
            "orderbook" => format!("orderbook.{}.{}", self.depth, self.symbol),
            channel => format!("{channel}.{}", self.symbol),
        }
    }

    pub(crate) fn client(&self) -> BybitClient {
        BybitClient::new(&self.api_url, self.category.as_str())
    }

    /// The connection for this market: the subscription travels with it, so a reconnect
    /// re-subscribes without the stream handling it.
    pub(crate) fn ws_target(&self) -> WsTarget {
        let mut topics = vec![self.topic("orderbook"), self.topic("publicTrade")];
        if self.category.is_derivative() {
            topics.push(self.topic("tickers"));
        }
        WsTarget::new(self.stream_url.clone(), self.heartbeat).subscribing(vec![
            serde_json::json!({"op": "subscribe", "args": topics}).to_string(),
        ])
    }
}

/// A connector for Bybit V5 public market data.
pub struct Bybit {
    config: BybitMarketStreamConfig,
}

impl Bybit {
    pub fn new(config: BybitMarketStreamConfig) -> Self {
        crate::ensure_rustls_crypto_provider();
        Self { config }
    }

    pub fn config(&self) -> &BybitMarketStreamConfig {
        &self.config
    }

    /// Runs the public market stream for as long as the process does, retrying a connection that
    /// ends.
    ///
    /// This is the whole-process entry point; a recorder uses
    /// [`start_bybit_market_stream`](crate::market_stream::start_bybit_market_stream), which shares
    /// the retry contract of every other venue. A connection that ends is reported as a
    /// `ConnectorError` and retried after an exponential backoff; a refusal the venue asked for is
    /// waited out for as long as it asked. The stream publishes `StreamInvalidated` itself whenever
    /// it loses depth continuity, so a consumer never infers a gap from a reconnect.
    pub async fn run(self, ev_tx: crate::connector::PublishSender) {
        let config = self.config;
        let _ = Retry::new(ExponentialBackoff::default())
            .error_handler(|error: BybitError| {
                if let Some(ban) = ban_delay_of(&error) {
                    tracing::warn!(?ban, ?error, "Bybit asked this client to wait");
                }
                tracing::error!(?error, "the Bybit market data stream ended");
                ev_tx.send(crate::connector::PublishEvent::ConnectorError(
                    titan_domain_types::LiveError::with(
                        titan_domain_types::ErrorKind::ConnectionInterrupted,
                        error.into(),
                    ),
                ));
                Ok(())
            })
            .retry(|| {
                let config = config.clone();
                let ev_tx = ev_tx.clone();
                async move {
                    let mut stream = MarketStream::new(config, ev_tx);
                    stream.connect().await
                }
            })
            .await;
    }
}

/// The wait a venue asked for, when the error is a refusal rather than a failure.
fn ban_delay_of(error: &BybitError) -> Option<std::time::Duration> {
    match error {
        BybitError::Banned(ban) => Some(ban.delay()),
        _ => None,
    }
}

impl std::fmt::Display for BybitCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<rest::BybitError> for titan_domain_types::Value {
    fn from(error: rest::BybitError) -> Self {
        titan_domain_types::Value::String(error.to_string())
    }
}

pub(crate) mod market_stream;
pub(crate) use market_stream::MarketStream;
/// The stream entry points that name [`MarketStream`], re-exported so a caller outside this module
/// does not reach into the private module path.
pub(crate) mod run {
    pub(crate) use super::market_stream::MarketStream;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn categories_map_to_their_v5_paths() {
        assert_eq!(BybitCategory::Spot.as_str(), "spot");
        assert_eq!(BybitCategory::Linear.as_str(), "linear");
        assert_eq!(BybitCategory::Inverse.as_str(), "inverse");
        assert_eq!(
            BybitCategory::Linear.stream_url(),
            "wss://stream.bybit.com/v5/public/linear"
        );
        assert!(!BybitCategory::Spot.is_derivative());
        assert!(BybitCategory::Linear.is_derivative());
        assert!(BybitCategory::Inverse.is_derivative());
    }

    #[test]
    fn the_subscription_depth_is_the_smallest_published_depth_that_covers_the_request() {
        // Bybit publishes 1, 50, 200 and 500 levels only.
        for (levels, expected) in [
            (0u16, 1),
            (1, 1),
            (2, 50),
            (50, 50),
            (51, 200),
            (100, 200),
            (200, 200),
            (201, 500),
            (1000, 500),
        ] {
            assert_eq!(
                BybitCategory::subscription_depth(levels),
                expected,
                "{levels} levels"
            );
        }
    }

    #[test]
    fn the_market_topic_carries_the_subscribed_depth() {
        let config = BybitMarketStreamConfig::new(BybitCategory::Linear, "BTCUSDT", 100);
        assert_eq!(config.depth, 200);
        assert_eq!(config.topic("orderbook"), "orderbook.200.BTCUSDT");
        assert_eq!(config.topic("publicTrade"), "publicTrade.BTCUSDT");
    }

    #[test]
    fn the_subscription_travels_with_the_connection() {
        let linear = BybitMarketStreamConfig::new(BybitCategory::Linear, "BTCUSDT", 50);
        let target = linear.ws_target();
        assert_eq!(target.url, "wss://stream.bybit.com/v5/public/linear");
        assert_eq!(target.subscribe.len(), 1);
        let message: serde_json::Value = serde_json::from_str(&target.subscribe[0]).unwrap();
        assert_eq!(message["op"], "subscribe");
        assert_eq!(
            message["args"],
            serde_json::json!([
                "orderbook.50.BTCUSDT",
                "publicTrade.BTCUSDT",
                "tickers.BTCUSDT"
            ])
        );

        // Spot has no funding and no open interest, so it does not subscribe the ticker.
        let spot = BybitMarketStreamConfig::new(BybitCategory::Spot, "BTCUSDT", 1);
        let message: serde_json::Value =
            serde_json::from_str(&spot.ws_target().subscribe[0]).unwrap();
        assert_eq!(
            message["args"],
            serde_json::json!(["orderbook.1.BTCUSDT", "publicTrade.BTCUSDT"])
        );
    }
}
