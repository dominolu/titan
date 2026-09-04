use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use hftbacktest::types::{Event, LiveError, LiveEvent, Order};
use titan_market_plugin::MarketDataKind;
use tokio::sync::mpsc::{self, error::TrySendError};

/// Exchange-owned stream coordinates attached by the concrete market-data connector. The plugin
/// adapter transports these values unchanged; it must not infer gaps or create epochs itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketStreamMetadata {
    pub epoch: u64,
    pub first_update_sequence: u64,
    pub last_update_sequence: u64,
    pub snapshot: bool,
}

#[derive(Clone, Debug)]
pub enum MarketDataCommand {
    Subscribe {
        symbol: String,
        kinds: Vec<MarketDataKind>,
    },
    Unsubscribe {
        symbol: String,
        kinds: Vec<MarketDataKind>,
    },
    Snapshot {
        symbol: String,
    },
    InitializeTrading {
        symbol: String,
    },
}

#[derive(Clone, Copy)]
pub enum NativeMarketBatch<'a> {
    Depth {
        symbol: &'a str,
        bids: NativeDepthLevels<'a>,
        asks: NativeDepthLevels<'a>,
        exchange_ts: i64,
        receive_ts: i64,
        stream: MarketStreamMetadata,
    },
    Trade {
        symbol: &'a str,
        price: f64,
        quantity: f64,
        sell: bool,
        exchange_ts: i64,
        receive_ts: i64,
    },
    Bbo {
        symbol: &'a str,
        bid_price: f64,
        bid_quantity: f64,
        ask_price: f64,
        ask_quantity: f64,
        exchange_ts: i64,
        receive_ts: i64,
    },
}

#[derive(Clone, Copy)]
pub enum NativeDepthLevels<'a> {
    Owned(&'a [(String, String)]),
    Borrowed(&'a [(&'a str, &'a str)]),
}

impl NativeDepthLevels<'_> {
    pub fn len(self) -> usize {
        match self {
            Self::Owned(levels) => levels.len(),
            Self::Borrowed(levels) => levels.len(),
        }
    }
}

impl<'a> NativeMarketBatch<'a> {
    pub fn symbol(&self) -> &'a str {
        match self {
            Self::Depth { symbol, .. } | Self::Trade { symbol, .. } | Self::Bbo { symbol, .. } => {
                symbol
            }
        }
    }
}

pub enum DirectPublication<'a> {
    Event(&'a PublishEvent),
    NativeMarket(NativeMarketBatch<'a>),
    Account(&'a AccountPublication),
}

/// Account-owned facts emitted by authenticated venue streams. The AccountPlugin direct path
/// consumes this type synchronously and encodes the stable account ABI without wrapping the fact
/// in the legacy bot-wide `LiveEvent` transport. Queued legacy runners are adapted at the sender
/// boundary until that runner is retired.
#[derive(Clone, Debug)]
pub enum AccountPublication {
    Order {
        symbol: String,
        /// Venue client order id when the private stream can associate one. REST-submitted
        /// AccountPlugin orders carry the deterministic 32-hex owner id; legacy connector orders
        /// carry the venue prefix id. It must survive into the account ABI so Order/Fill facts can
        /// be correlated back to the strategy command that created them.
        client_order_id: Option<String>,
        /// Exchange order id as reported by the private stream. AccountPlugin facts must surface
        /// the exchange id (not the hftbacktest local order id) so a WS OrderChanged can be
        /// correlated and later amended/canceled by venue id. Falls back to `order.order_id` when
        /// the private stream cannot associate an exchange id (e.g. venue-wide cancel-all).
        venue_order_id: Option<String>,
        order: Order,
    },
    Position {
        symbol: String,
        qty: f64,
        exch_ts: i64,
    },
    Error(LiveError),
}

type DirectPublisher = dyn for<'a> Fn(DirectPublication<'a>) + Send + Sync + 'static;

#[derive(Clone)]
pub struct PublishSender {
    publish: Arc<DirectPublisher>,
    native_supported: bool,
}

impl PublishSender {
    pub fn send(&self, event: PublishEvent) -> Result<(), TrySendError<PublishEvent>> {
        (self.publish)(DirectPublication::Event(&event));
        Ok(())
    }

    /// Publishes a borrowed venue batch without constructing normalized `Event` vectors.
    pub fn try_send_native_market(&self, batch: NativeMarketBatch<'_>) -> bool {
        if !self.native_supported {
            return false;
        }
        (self.publish)(DirectPublication::NativeMarket(batch));
        true
    }

    /// Publishes an authenticated account fact directly into the AccountPlugin encoder.
    pub fn send_account(
        &self,
        publication: AccountPublication,
    ) -> Result<(), TrySendError<AccountPublication>> {
        (self.publish)(DirectPublication::Account(&publication));
        Ok(())
    }
}

pub fn direct_publish_sender(
    publish: impl for<'a> Fn(DirectPublication<'a>) + Send + Sync + 'static,
) -> PublishSender {
    PublishSender {
        publish: Arc::new(publish),
        native_supported: true,
    }
}

/// A message will be received by the publisher thread and then published to the bots.
#[derive(Clone)]
pub enum PublishEvent {
    /// The authenticated private stream has connected and confirmed its account subscriptions.
    /// AccountPlugin uses this as the barrier before running reconciliation and declaring READY.
    PrivateStreamReady,
    LiveEvent(LiveEvent),
    /// A normalized mark-price update. This remains separate from `LiveEvent::Funding` because
    /// one Binance mark-price frame carries both values and consumers may subscribe independently.
    MarkPrice {
        symbol: String,
        mark_price: f64,
        exch_ts: i64,
    },
    /// All normalized feed records produced by one exchange message. Keeping the symbol once per
    /// batch avoids one MPSC allocation and one symbol clone per price level.
    FeedBatch {
        symbol: String,
        events: Vec<Event>,
        stream: Option<MarketStreamMetadata>,
    },
    /// The concrete connector detected that consumers must discard their current stream image.
    StreamInvalidated {
        symbol: String,
        epoch: u64,
    },
}

impl PublishEvent {
    pub(crate) fn lossy_market_symbol(&self) -> Option<&str> {
        match self {
            Self::FeedBatch { symbol, .. }
            | Self::MarkPrice { symbol, .. }
            | Self::LiveEvent(LiveEvent::Feed { symbol, .. })
            | Self::LiveEvent(LiveEvent::Funding { symbol, .. }) => Some(symbol),
            _ => None,
        }
    }
}

#[cfg(test)]
pub(crate) struct TestPublishReceiver {
    receiver: mpsc::UnboundedReceiver<PublishEvent>,
}

#[cfg(test)]
impl TestPublishReceiver {
    pub(crate) async fn recv(&mut self) -> Option<PublishEvent> {
        self.receiver.recv().await
    }
}

/// Direct test channel: captures every `PublishEvent` sent through a `PublishSender`.
#[cfg(test)]
pub(crate) fn test_publish_channel() -> (PublishSender, TestPublishReceiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        PublishSender {
            publish: Arc::new(move |publication| {
                if let DirectPublication::Event(event) = publication {
                    let _ = sender.send(event.clone());
                }
            }),
            // Keep the historical test behavior: native batches are declined so venue code
            // falls back to the normalized FeedBatch path that the test receiver can inspect.
            native_supported: false,
        },
        TestPublishReceiver { receiver },
    )
}

/// Provides a build function for the Connector.
pub trait ConnectorBuilder {
    type Error: Debug;

    fn build_from(config: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Provides an interface for connecting with an exchange or broker for a live bot.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Registers an instrument to be traded through this connector.
    fn register(&mut self, symbol: String);

    /// Registers an instrument for private account state without enabling public market data.
    fn register_account(&mut self, symbol: String) {
        self.register(symbol);
    }

    /// Updates the venue market-data subscription without invoking trading/account initialization.
    fn subscribe_market_data(&mut self, symbol: String, _kinds: Vec<MarketDataKind>) {
        self.register(symbol);
    }

    /// Releases a market-data registration. Venues that cannot unsubscribe an individual channel
    /// may keep the socket alive, but must remove the symbol from reconnect state.
    fn unregister(&mut self, _symbol: String) {}

    fn unsubscribe_market_data(&mut self, symbol: String, _kinds: Vec<MarketDataKind>) {
        self.unregister(symbol);
    }

    /// Returns an [`OrderManager`].
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>>;

    /// Runs the connector, establishing the connection and preparing to exchange information such
    /// as data feed and orders. This method should not block, and any response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn run(&mut self, tx: PublishSender);

    /// Starts only public market-data resources for MarketPlugin adapters.
    fn run_market_data(&mut self, tx: PublishSender) {
        self.run(tx);
    }

    /// Starts private account resources without enabling public market data.
    fn run_account(&mut self, tx: PublishSender) {
        self.run(tx);
    }

    /// Returns the authenticated REST facade used for reconciliation and command execution.
    fn broker_api(&self) -> Option<Arc<dyn crate::api::BrokerApi>> {
        None
    }

    /// Submits a new order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn submit(&self, symbol: String, order: Order, tx: PublishSender);

    /// Registers a REST-submitted AccountPlugin order in the connector's private-stream identity
    /// table before the exchange request is sent.
    ///
    /// `client_order_id` is the canonical identifier the REST facade will send (32 lowercase hex
    /// for AccountPlugin orders). Venue implementations translate it to their own private-stream
    /// key (e.g. Hyperliquid's `0x` cloid). Without registration, private-stream Order/Fill
    /// updates for orders submitted through `BrokerApi` cannot be correlated back to the order.
    fn track_managed_order(&self, symbol: &str, client_order_id: &str, order: &Order) {
        let _ = (symbol, client_order_id, order);
    }

    /// Cancels an open order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn cancel(&self, symbol: String, order: Order, tx: PublishSender);

    /// Requests a fresh exchange-owned snapshot without running trading initialization.
    fn request_snapshot(&mut self, _symbol: String) {}

    /// Recovers venue market state after local backpressure dropped one or more updates. Concrete
    /// connectors own the resubscribe/snapshot policy because only they understand stream state.
    fn recover_market_data(&mut self, symbols: Vec<String>) {
        for symbol in symbols {
            self.request_snapshot(symbol);
        }
    }

    /// Cancels every open order managed by this connector before process shutdown.
    /// Implementations must wait for the exchange response before returning.
    async fn shutdown(&self) -> Result<(), String>;
}

/// Provides `orders` method to get the current working orders.
pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order>;
}

#[cfg(test)]
pub(crate) async fn reconnecting_websocket_server(
    text_frames_per_connection: usize,
) -> (
    String,
    tokio::sync::mpsc::Receiver<String>,
    tokio::task::JoinHandle<()>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local websocket fixture");
    let address = listener
        .local_addr()
        .expect("read websocket fixture address");
    assert!(text_frames_per_connection > 0);
    let (subscriptions, receiver) = tokio::sync::mpsc::channel(2 * text_frames_per_connection);
    let task = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.expect("accept connector socket");
            let mut websocket = accept_async(socket)
                .await
                .expect("complete connector websocket handshake");
            let mut observed = 0;
            while observed < text_frames_per_connection {
                let Some(message) = websocket.next().await else {
                    panic!("connector socket ended before all subscription frames arrived");
                };
                match message.expect("read connector websocket frame") {
                    Message::Text(text) => {
                        subscriptions
                            .send(text.to_string())
                            .await
                            .expect("publish observed subscription");
                        observed += 1;
                    }
                    Message::Ping(value) => {
                        websocket
                            .send(Message::Pong(value))
                            .await
                            .expect("reply to connector ping");
                    }
                    _ => {}
                }
            }
            // Force an actual peer-side disconnect. The connector must reconnect and rebuild the
            // subscription from its shared desired state rather than relying on an in-flight
            // command from the first socket.
            websocket.close(None).await.expect("close connector socket");
        }
    });
    (format!("ws://{address}"), receiver, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn publish_event_is_delivered_once_through_the_direct_sender() {
        let (sender, mut receiver) = test_publish_channel();
        let delta = PublishEvent::FeedBatch {
            symbol: "BTC".to_string(),
            events: Vec::new(),
            stream: Some(MarketStreamMetadata {
                epoch: 1,
                first_update_sequence: 1,
                last_update_sequence: 1,
                snapshot: false,
            }),
        };
        sender.send(delta.clone()).unwrap();
        sender.send(delta.clone()).unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::FeedBatch { symbol, .. }) if symbol == "BTC"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::FeedBatch { symbol, .. }) if symbol == "BTC"
        ));
    }

    #[test]
    fn account_publication_is_delivered_directly_without_a_live_event_bridge() {
        let observed = Arc::new(AtomicUsize::new(0));
        let capture = observed.clone();
        let sender = direct_publish_sender(move |publication| {
            if let DirectPublication::Account(AccountPublication::Position {
                symbol,
                qty,
                exch_ts,
            }) = publication
            {
                assert_eq!(symbol, "BTC");
                assert_eq!(*qty, -2.0);
                assert_eq!(*exch_ts, 99);
                capture.fetch_add(1, Ordering::Relaxed);
            }
        });
        sender
            .send_account(AccountPublication::Position {
                symbol: "BTC".to_owned(),
                qty: -2.0,
                exch_ts: 99,
            })
            .unwrap();
        assert_eq!(observed.load(Ordering::Relaxed), 1);
    }
}
