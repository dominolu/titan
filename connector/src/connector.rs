use std::{
    collections::HashSet,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use hftbacktest::types::{Event, LiveEvent, Order};
use titan_market_plugin::MarketDataKind;
use tokio::sync::mpsc::{self, error::TrySendError};

pub const DEFAULT_PUBLISH_QUEUE_CAPACITY: usize = 4_096;

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

/// Bounded, non-blocking connector output. Producers observe congestion immediately instead of
/// accumulating an unbounded market-data backlog.
#[derive(Clone)]
pub struct PublishSender {
    transport: PublishTransport,
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
}

type DirectPublisher = dyn for<'a> Fn(DirectPublication<'a>) + Send + Sync + 'static;

#[derive(Clone)]
enum PublishTransport {
    Queued {
        market_sender: mpsc::Sender<PublishEvent>,
        critical_sender: mpsc::UnboundedSender<PublishEvent>,
        overflowed_symbols: Arc<Mutex<HashSet<String>>>,
    },
    Direct(Arc<DirectPublisher>),
}

impl PublishSender {
    pub fn send(&self, event: PublishEvent) -> Result<(), TrySendError<PublishEvent>> {
        let PublishTransport::Queued {
            market_sender,
            critical_sender,
            overflowed_symbols,
        } = &self.transport
        else {
            let PublishTransport::Direct(publish) = &self.transport else {
                unreachable!()
            };
            publish(DirectPublication::Event(&event));
            return Ok(());
        };
        let Some(symbol) = event.lossy_market_symbol().map(str::to_owned) else {
            return critical_sender
                .send(event)
                .map_err(|error| TrySendError::Closed(error.0));
        };
        match market_sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                overflowed_symbols
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(symbol);
                Ok(())
            }
            Err(error @ TrySendError::Closed(_)) => Err(error),
        }
    }

    /// Publishes a borrowed venue batch without constructing normalized `Event` vectors. Returns
    /// `false` when this sender uses the compatibility queue and the caller must use `send`.
    pub fn try_send_native_market(&self, batch: NativeMarketBatch<'_>) -> bool {
        let PublishTransport::Direct(publish) = &self.transport else {
            return false;
        };
        publish(DirectPublication::NativeMarket(batch));
        true
    }
}

pub struct PublishReceiver {
    market_receiver: mpsc::Receiver<PublishEvent>,
    critical_receiver: mpsc::UnboundedReceiver<PublishEvent>,
    overflowed_symbols: Arc<Mutex<HashSet<String>>>,
    invalidated_symbols: HashSet<String>,
}

impl PublishReceiver {
    fn take_overflow(&mut self) -> Option<PublishEvent> {
        let mut overflowed = {
            let mut value = self
                .overflowed_symbols
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *value)
        };
        if overflowed.is_empty() {
            return None;
        }
        while let Ok(event) = self.market_receiver.try_recv() {
            if let Some(symbol) = event.lossy_market_symbol() {
                overflowed.insert(symbol.to_string());
            }
        }
        self.invalidated_symbols.extend(overflowed.iter().cloned());
        Some(PublishEvent::QueueOverflow {
            symbols: overflowed.into_iter().collect(),
        })
    }

    pub async fn recv(&mut self) -> Option<PublishEvent> {
        if let Some(overflow) = self.take_overflow() {
            return Some(overflow);
        }
        loop {
            let (value, market) = tokio::select! {
                biased;
                value = self.critical_receiver.recv() => (value, false),
                value = self.market_receiver.recv() => (value, true),
            };
            let value = value?;
            if market {
                if let Some(overflow) = self.take_overflow() {
                    return Some(overflow);
                }
            }
            let Some(symbol) = value.lossy_market_symbol() else {
                return Some(value);
            };
            if !self.invalidated_symbols.contains(symbol) {
                return Some(value);
            }
            if value.is_market_snapshot() {
                self.invalidated_symbols.remove(symbol);
                return Some(value);
            }
        }
    }
}

pub fn publish_channel(capacity: usize) -> (PublishSender, PublishReceiver) {
    let (market_sender, market_receiver) = mpsc::channel(capacity);
    let (critical_sender, critical_receiver) = mpsc::unbounded_channel();
    let overflowed_symbols = Arc::new(Mutex::new(HashSet::new()));
    (
        PublishSender {
            transport: PublishTransport::Queued {
                market_sender,
                critical_sender,
                overflowed_symbols: overflowed_symbols.clone(),
            },
        },
        PublishReceiver {
            market_receiver,
            critical_receiver,
            overflowed_symbols,
            invalidated_symbols: HashSet::new(),
        },
    )
}

pub(crate) fn direct_publish_sender(
    publish: impl for<'a> Fn(DirectPublication<'a>) + Send + Sync + 'static,
) -> PublishSender {
    PublishSender {
        transport: PublishTransport::Direct(Arc::new(publish)),
    }
}

/// A message will be received by the publisher thread and then published to the bots.
pub enum PublishEvent {
    QueueOverflow {
        symbols: Vec<String>,
    },
    BatchStart(u64),
    BatchEnd(u64),
    /// The authenticated private stream has connected and confirmed its account subscriptions.
    /// AccountPlugin uses this as the barrier before running reconciliation and declaring READY.
    PrivateStreamReady,
    LiveEvent(LiveEvent),
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
    RegisterInstrument {
        id: u64,
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    },
}

impl PublishEvent {
    pub(crate) fn lossy_market_symbol(&self) -> Option<&str> {
        match self {
            Self::FeedBatch { symbol, .. }
            | Self::LiveEvent(LiveEvent::Feed { symbol, .. })
            | Self::LiveEvent(LiveEvent::Funding { symbol, .. }) => Some(symbol),
            _ => None,
        }
    }

    fn is_market_snapshot(&self) -> bool {
        matches!(
            self,
            Self::FeedBatch {
                stream: Some(MarketStreamMetadata { snapshot: true, .. }),
                ..
            }
        )
    }
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_publish_channel_reports_overflow_before_buffered_data() {
        let (sender, mut receiver) = publish_channel(1);
        let delta = || PublishEvent::FeedBatch {
            symbol: "BTC".to_string(),
            events: Vec::new(),
            stream: Some(MarketStreamMetadata {
                epoch: 1,
                first_update_sequence: 1,
                last_update_sequence: 1,
                snapshot: false,
            }),
        };
        sender.send(delta()).unwrap();
        sender.send(delta()).unwrap();
        sender.send(PublishEvent::BatchStart(1)).unwrap();

        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::QueueOverflow { .. })
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::BatchStart(1))
        ));

        sender.send(delta()).unwrap();
        sender.send(delta()).unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::QueueOverflow { .. })
        ));

        sender.send(delta()).unwrap();
        let snapshot_sender = sender.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            snapshot_sender
                .send(PublishEvent::FeedBatch {
                    symbol: "BTC".to_string(),
                    events: Vec::new(),
                    stream: Some(MarketStreamMetadata {
                        epoch: 2,
                        first_update_sequence: 10,
                        last_update_sequence: 10,
                        snapshot: true,
                    }),
                })
                .unwrap();
        });
        assert!(matches!(
            receiver.recv().await,
            Some(PublishEvent::FeedBatch {
                stream: Some(MarketStreamMetadata { snapshot: true, .. }),
                ..
            })
        ));
    }
}
