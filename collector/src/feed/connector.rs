//! Adapter from the connector's market stream to the recorder's feed contract.
//!
//! The connector owns everything a venue's market data needs: the socket, the ping and heartbeat,
//! the sequence check, the REST snapshot and its alignment, and the epoch a gap starts. The
//! recorder owns everything a recording needs: the UTC grid, the window, the block layout and the
//! integrity markers.
//!
//! This module is the only place the two meet. It turns what the connector publishes as
//! [`PublishEvent`]s into the [`FeedMessage`]s the recorder already knows how to write, so neither
//! side has to learn the other's vocabulary. The sequence chain is deliberately *not* repeated
//! here: the connector decides whether there is a gap, and this only reports where the recording
//! had got to when it happened.

use std::collections::VecDeque;

use crate::book::{
    Change, DepthMessage, Feed, FeedMessage, GapMessage, GapReason, Level, MarketConfig, Side,
    SnapshotMessage, decimal_steps,
};
use crate::file::{AuxRecord, AuxSource, TradeRecord, TradeSide};
use connector::connector::{
    DirectPublication, MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch, PublishEvent,
    PublishSender,
};
use titan_domain_types::{BUY_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, Event};

/// One thing the connector published, captured so it outlives the callback.
///
/// The connector hands native depth and trade batches straight to a synchronous callback to avoid
/// building an `Event` vector per frame, so a consumer that keeps messages has to copy them. The
/// alternative — forcing every batch through `FeedBatch` — would add that cost back to the live
/// path for the recorder's benefit alone.
enum Publication {
    Event(PublishEvent),
    Depth {
        symbol: String,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
        exchange_ts: i64,
        receive_ts: i64,
        stream: MarketStreamMetadata,
    },
    Trade {
        symbol: String,
        price: f64,
        quantity: f64,
        sell: bool,
        exchange_ts: i64,
        receive_ts: i64,
    },
}

impl Publication {
    /// Copies a native batch into an owned publication, or returns `None` for a kind the
    /// recorder does not store.
    fn from_native(batch: NativeMarketBatch<'_>) -> Option<Self> {
        match batch {
            NativeMarketBatch::Depth {
                symbol,
                bids,
                asks,
                exchange_ts,
                receive_ts,
                stream,
            } => Some(Self::Depth {
                symbol: symbol.to_owned(),
                bids: collect_levels(bids),
                asks: collect_levels(asks),
                exchange_ts,
                receive_ts,
                stream,
            }),
            NativeMarketBatch::Trade {
                symbol,
                price,
                quantity,
                sell,
                exchange_ts,
                receive_ts,
            } => Some(Self::Trade {
                symbol: symbol.to_owned(),
                price,
                quantity,
                sell,
                exchange_ts,
                receive_ts,
            }),
            // The recorder does not store a best-quote frame on its own.
            NativeMarketBatch::Bbo { .. } => None,
        }
    }
}

/// The levels of a native batch as owned strings, whatever form the connector borrowed them in.
fn collect_levels(levels: NativeDepthLevels<'_>) -> Vec<(String, String)> {
    match levels {
        NativeDepthLevels::Owned(levels) => levels.to_vec(),
        NativeDepthLevels::Borrowed(levels) | NativeDepthLevels::ScaledBorrowed { levels, .. } => {
            levels
                .iter()
                .map(|(price, qty)| ((*price).to_owned(), (*qty).to_owned()))
                .collect()
        }
    }
}

/// The recorder's view of one connected connector stream.
///
/// A connector stream publishes for one symbol at a time; a recorder market is also one symbol,
/// so the two line up and no routing table is needed.
pub struct ConnectorFeed {
    events: tokio::sync::mpsc::UnboundedReceiver<Publication>,
    symbol: String,
    /// The tick and quantity units the venue's decimal text is scaled by. The connector publishes
    /// prices as the venue wrote them, so the recorder's integer form is derived here.
    market: MarketConfig,
    pending: VecDeque<FeedMessage>,
    /// The last sequence handed to the recorder. The connector decides *whether* there is a gap;
    /// this only records where the recording had reached so the gap can say so.
    last_seq: u64,
}

impl ConnectorFeed {
    /// Builds a feed for `symbol` and the sender the connector stream must publish through.
    ///
    /// The sender is returned rather than used internally so the caller keeps ownership of the
    /// venue connection: this adapter never opens or closes a socket.
    pub fn channel(market: MarketConfig) -> (Self, PublishSender) {
        let symbol = market.symbol.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = connector::connector::direct_publish_sender(move |publication| {
            let captured = match publication {
                DirectPublication::Event(event) => Some(Publication::Event(event.clone())),
                DirectPublication::NativeMarket(batch) => Publication::from_native(batch),
                DirectPublication::Account(_) => None,
            };
            if let Some(publication) = captured {
                let _ = tx.send(publication);
            }
        });
        (
            Self {
                events: rx,
                symbol,
                market,
                pending: VecDeque::new(),
                last_seq: 0,
            },
            sender,
        )
    }

    /// Whether a publication is for this feed's market.
    ///
    /// The connector lower-cases the symbol on some paths (mark price and trades are decoded
    /// through a lower-casing deserializer) while depth keeps the venue's own casing, so the same
    /// market arrives under both forms. Comparing case-sensitively dropped one of them.
    fn matches(&self, symbol: &str) -> bool {
        symbol.eq_ignore_ascii_case(&self.symbol)
    }

    /// Turns one native depth batch into a recorder message.
    ///
    /// The connector publishes a snapshot as a native batch whose levels carry the whole book, so
    /// it replaces the recorder's window; anything else is a net change against the previous one.
    fn native_depth(
        &mut self,
        symbol: &str,
        bids: &[(String, String)],
        asks: &[(String, String)],
        exchange_ts: i64,
        receive_ts: i64,
        stream: MarketStreamMetadata,
    ) -> Vec<FeedMessage> {
        if !self.matches(symbol) {
            tracing::debug!(got = %symbol, want = %self.symbol, "depth for another symbol, dropped");
            return Vec::new();
        }
        if stream.snapshot {
            self.last_seq = stream.last_update_sequence;
            let mut snapshot_bids = parse_levels(bids, &self.market);
            let mut snapshot_asks = parse_levels(asks, &self.market);
            snapshot_bids.sort_by(|a, b| b.tick.cmp(&a.tick));
            snapshot_asks.sort_by_key(|level| level.tick);
            return vec![FeedMessage::Snapshot(SnapshotMessage {
                exch_ts_ns: exchange_ts,
                local_ts_ns: receive_ts,
                bids: snapshot_bids,
                asks: snapshot_asks,
                last_seq: Some(self.last_seq),
            })];
        }
        let mut changes = parse_changes(bids, Side::Bid, &self.market);
        changes.extend(parse_changes(asks, Side::Ask, &self.market));
        if changes.is_empty() {
            tracing::warn!(%symbol, bids = bids.len(), asks = asks.len(), "depth changes empty after scaling");
            return Vec::new();
        }
        // The connector publishes only a delta it has accepted, having checked `pu` itself. The
        // metadata carries no predecessor, and deriving one here would state a continuity this
        // layer cannot see: it would be checked again below and could only drop messages the
        // connector already accepted. Leaving it empty asks the recorder to apply the change.
        self.last_seq = stream.last_update_sequence;
        vec![FeedMessage::Depth(DepthMessage {
            exch_ts_ns: exchange_ts,
            local_ts_ns: receive_ts,
            first_prev_seq: None,
            first_seq: Some(stream.first_update_sequence),
            last_seq: stream.last_update_sequence,
            changes,
        })]
    }

    /// Turns one native trade into a recorder trade record.
    fn native_trade(
        &mut self,
        symbol: &str,
        price: f64,
        quantity: f64,
        sell: bool,
        exchange_ts: i64,
        receive_ts: i64,
    ) -> Vec<FeedMessage> {
        if !self.matches(symbol) {
            return Vec::new();
        }
        vec![FeedMessage::Trade(TradeRecord {
            exch_ts_ns: exchange_ts,
            local_ts_ns: receive_ts,
            // `sell` describes the aggressor: a buyer moving the market is a buy.
            side: if sell {
                TradeSide::Sell
            } else {
                TradeSide::Buy
            },
            price: format_quantity(price),
            qty: format_quantity(quantity),
            trade_id: None,
        })]
    }

    /// Turns one normalized event batch into recorder messages.
    ///
    /// A batch marked `snapshot` carries the whole book as snapshot levels and replaces whatever
    /// the recorder held; anything else is a net change against the previous state.
    fn event_batch(
        &mut self,
        symbol: &str,
        events: &[Event],
        stream: Option<MarketStreamMetadata>,
    ) -> Vec<FeedMessage> {
        if !self.matches(symbol) {
            return Vec::new();
        }
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut changes = Vec::new();
        let mut first_seq = None;
        let mut last_seq = 0;
        let mut exch_ts = 0;
        let mut local_ts = 0;
        let mut snapshot = false;
        for event in events {
            let side = if event.is(BUY_EVENT) {
                Side::Bid
            } else {
                Side::Ask
            };
            // The kind lives in the low byte: `BUY_EVENT`, `SELL_EVENT` and `LOCAL_EVENT` are
            // flags beside it, so only those bits may be compared against a kind constant.
            let kind = event.ev & 0xff;
            // A `FeedBatch` carries prices and quantities already parsed to `f64`, in the venue's
            // own units. The recorder stores ticks and quantity steps, so the same scaling the
            // native path does has to happen here; truncating the float would put a BTC price in
            // the low thousands and a quantity at zero.
            let Some((tick, qty)) = scale_level(event.px, event.qty, &self.market) else {
                continue;
            };
            if kind == DEPTH_SNAPSHOT_EVENT {
                snapshot = true;
                match side {
                    Side::Bid => bids.push(Level { tick, qty }),
                    Side::Ask => asks.push(Level { tick, qty }),
                }
                exch_ts = event.exch_ts;
                local_ts = event.local_ts;
            } else if kind == DEPTH_EVENT {
                first_seq.get_or_insert(event.ival as u64);
                last_seq = last_seq.max(event.ival as u64);
                exch_ts = event.exch_ts;
                local_ts = event.local_ts;
                changes.push(Change { side, tick, qty });
            }
        }
        let mut out = Vec::new();
        if snapshot {
            self.last_seq = stream.map_or(last_seq, |meta| meta.last_update_sequence);
            out.push(FeedMessage::Snapshot(SnapshotMessage {
                exch_ts_ns: exch_ts,
                local_ts_ns: local_ts,
                bids,
                asks,
                last_seq: Some(self.last_seq),
            }));
        } else if !changes.is_empty() {
            self.last_seq = last_seq;
            out.push(FeedMessage::Depth(DepthMessage {
                exch_ts_ns: exch_ts,
                local_ts_ns: local_ts,
                first_prev_seq: None,
                first_seq,
                last_seq,
                changes,
            }));
        }
        out
    }
}

impl Feed for ConnectorFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        if let Some(message) = self.pending.pop_front() {
            return Some(message);
        }
        loop {
            let event = self.events.recv().await?;
            let produced: Vec<FeedMessage> = match event {
                Publication::Depth {
                    symbol,
                    bids,
                    asks,
                    exchange_ts,
                    receive_ts,
                    stream,
                } => self.native_depth(&symbol, &bids, &asks, exchange_ts, receive_ts, stream),
                Publication::Trade {
                    symbol,
                    price,
                    quantity,
                    sell,
                    exchange_ts,
                    receive_ts,
                } => self.native_trade(&symbol, price, quantity, sell, exchange_ts, receive_ts),
                Publication::Event(PublishEvent::FeedBatch {
                    symbol,
                    events,
                    stream,
                }) => self.event_batch(&symbol, &events, stream),
                Publication::Event(PublishEvent::StreamInvalidated { symbol, epoch }) => {
                    if !self.matches(&symbol) {
                        Vec::new()
                    } else {
                        let now = now_ns();
                        vec![FeedMessage::Gap(GapMessage {
                            start_local_ns: now,
                            start_exch_ns: 0,
                            end_local_ns: now,
                            end_exch_ns: 0,
                            reason: GapReason::SequenceBreak,
                            last_seq: self.last_seq,
                            next_seq: 0,
                            detail: format!("connector invalidated the stream at epoch {epoch}"),
                        })]
                    }
                }
                Publication::Event(PublishEvent::ConnectorError(error)) => {
                    let now = now_ns();
                    vec![FeedMessage::Gap(GapMessage {
                        start_local_ns: now,
                        start_exch_ns: 0,
                        end_local_ns: now,
                        end_exch_ns: 0,
                        reason: GapReason::Disconnect,
                        last_seq: self.last_seq,
                        next_seq: 0,
                        detail: format!("{:?}", error),
                    })]
                }
                Publication::Event(PublishEvent::Funding {
                    symbol,
                    funding_rate,
                    next_funding_time,
                    exch_ts,
                }) => {
                    if !self.matches(&symbol) {
                        Vec::new()
                    } else {
                        vec![FeedMessage::Aux(AuxRecord::Funding {
                            exch_ts_ns: exch_ts,
                            local_ts_ns: now_ns(),
                            rate: funding_rate.to_string(),
                            next_funding_ts_ns: Some(next_funding_time),
                            mark_px: None,
                            index_px: None,
                            source: AuxSource::Stream,
                        })]
                    }
                }
                // The recorder does not store mark price on its own; a booking is made from the
                // funding frame that carries the same values.
                Publication::Event(
                    PublishEvent::MarkPrice { .. } | PublishEvent::PrivateStreamReady,
                ) => Vec::new(),
            };
            self.pending.extend(produced);
            if let Some(message) = self.pending.pop_front() {
                return Some(message);
            }
        }
    }
}

/// The levels of a native batch as recorder levels, in ticks and quantity steps.
/// Scales one parsed price and quantity into the recorder's tick and quantity step.
///
/// Returns `None` when the value is not a finite positive number or does not land on the market's
/// grid; the connector has already checked the book, so the level is skipped rather than guessed.
fn scale_level(px: f64, qty: f64, market: &MarketConfig) -> Option<(i64, u64)> {
    if !px.is_finite() || !qty.is_finite() || px <= 0.0 || qty <= 0.0 {
        return None;
    }
    let scale = 10f64.powi(i32::from(market.price_decimals));
    let tick = (px * scale).round();
    if !tick.is_finite() || tick <= 0.0 {
        return None;
    }
    let tick = tick as u64;
    if tick % market.tick_units != 0 {
        return None;
    }
    let qty_scale = 10f64.powi(i32::from(market.qty_decimals));
    let steps = (qty * qty_scale).round();
    if !steps.is_finite() || steps < 0.0 {
        return None;
    }
    let steps = steps as u64;
    if steps % market.qty_step_units != 0 {
        return None;
    }
    Some(((tick / market.tick_units) as i64, steps / market.qty_step_units))
}

fn parse_levels(levels: &[(String, String)], market: &MarketConfig) -> Vec<Level> {
    parse_changes(levels, Side::Bid, market)
        .into_iter()
        .filter(|change| change.qty > 0)
        .map(|change| Level {
            tick: change.tick,
            qty: change.qty,
        })
        .collect()
}

/// The levels of a native batch as recorder changes on one side.
///
/// A level the venue wrote in a form the market's units cannot express is dropped rather than
/// guessed at; the connector has already checked the book, and a fabricated tick would be worse
/// than a missing one.
fn parse_changes(levels: &[(String, String)], side: Side, market: &MarketConfig) -> Vec<Change> {
    levels
        .iter()
        .filter_map(|(price, qty)| {
            let tick = decimal_steps(price, market.price_decimals, market.tick_units).ok()?;
            let qty = decimal_steps(qty, market.qty_decimals, market.qty_step_units).ok()?;
            Some(Change {
                side,
                tick: i64::try_from(tick).ok()?,
                qty,
            })
        })
        .collect()
}

/// The shortest decimal text that round-trips `value`, so a price keeps the venue's precision.
fn format_quantity(value: f64) -> String {
    let mut text = format!("{value}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use titan_domain_types::{LOCAL_EVENT, SELL_EVENT};

    /// A market whose tick is 0.01 and quantity step is 0.001, so venue text keeps its decimals.
    fn market() -> MarketConfig {
        MarketConfig {
            market_id: 1,
            venue_id: 1,
            venue: "binance_futures".into(),
            instrument: crate::book::InstrumentType::Perpetual,
            symbol: "BTCUSDT".into(),
            feed_mode: crate::book::FeedMode::Diff,
            // Spot BTCUSDT as the recorder is configured in production: prices carry 8 decimals
            // and a quantity step of 1e-8, so venue text keeps every digit it was written with.
            tick_units: 1_000_000,
            price_decimals: 8,
            qty_step_units: 1,
            qty_decimals: 8,
            depth_available: 100,
        }
    }

    fn metadata(epoch: u64, last: u64, snapshot: bool) -> MarketStreamMetadata {
        MarketStreamMetadata {
            epoch,
            first_update_sequence: last,
            last_update_sequence: last,
            snapshot,
        }
    }

    fn depth_event(px: f64, qty: f64, seq: i64, bid: bool) -> Event {
        Event {
            ev: DEPTH_EVENT | LOCAL_EVENT | if bid { BUY_EVENT } else { SELL_EVENT },
            exch_ts: 1,
            local_ts: 2,
            px,
            qty,
            order_id: 0,
            ival: seq,
            fval: 0.0,
        }
    }

    #[tokio::test]
    async fn funding_for_the_market_is_recorded_despite_symbol_case() {
        // The connector lower-cases the symbol on the mark price and trade paths, so a feed built
        // for "BTCUSDT" still has to recognise a publication for "btcusdt"; a case-sensitive
        // comparison silently dropped every funding record.
        let (mut feed, sender) = ConnectorFeed::channel(market());
        sender.send(PublishEvent::Funding {
            symbol: "btcusdt".into(),
            funding_rate: 0.0001,
            next_funding_time: 1_000,
            exch_ts: 2_000,
        });
        match feed.next().await.expect("a funding record") {
            FeedMessage::Aux(AuxRecord::Funding { rate, .. }) => {
                assert!(rate.starts_with("0.0001"), "rate was {rate}");
            }
            other => panic!("expected a funding record, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_native_depth_batch_reaches_the_feed() {
        // The connector hands depth to the callback as a native batch, never as `FeedBatch`, so a
        // test that only constructs the latter would miss the path a live stream actually uses.
        let (mut feed, sender) = ConnectorFeed::channel(market());
        let bids = vec![("75000.50000000".to_owned(), "0.01500000".to_owned())];
        let asks = vec![("75001.25000000".to_owned(), "0.02500000".to_owned())];
        assert!(sender.try_send_native_market(NativeMarketBatch::Depth {
            symbol: "BTCUSDT",
            bids: NativeDepthLevels::Owned(&bids),
            asks: NativeDepthLevels::Owned(&asks),
            exchange_ts: 1_000,
            receive_ts: 2_000,
            stream: metadata(1, 55, false),
        }));
        match feed.next().await.expect("a depth message") {
            FeedMessage::Depth(depth) => {
                assert_eq!(depth.last_seq, 55);
                assert_eq!(depth.first_seq, Some(55));
                assert_eq!(depth.changes.len(), 2);
                assert_eq!(depth.exch_ts_ns, 1_000);
            }
            other => panic!("expected a depth message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_native_trade_batch_reaches_the_feed() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        assert!(sender.try_send_native_market(NativeMarketBatch::Trade {
            symbol: "BTCUSDT",
            price: 75_000.5,
            quantity: 0.25,
            sell: false,
            exchange_ts: 5_000,
            receive_ts: 6_000,
        }));
        match feed.next().await.expect("a trade") {
            FeedMessage::Trade(trade) => {
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.price, "75000.5");
                assert_eq!(trade.qty, "0.25");
            }
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_native_snapshot_replaces_the_book() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        let bids = vec![
            ("75000.50000000".to_owned(), "0.00100000".to_owned()),
            ("75000.00000000".to_owned(), "0.00200000".to_owned()),
        ];
        let asks = vec![("75001.25000000".to_owned(), "0.00300000".to_owned())];
        assert!(sender.try_send_native_market(NativeMarketBatch::Depth {
            symbol: "BTCUSDT",
            bids: NativeDepthLevels::Owned(&bids),
            asks: NativeDepthLevels::Owned(&asks),
            exchange_ts: 1,
            receive_ts: 2,
            stream: metadata(1, 90, true),
        }));
        match feed.next().await.expect("a snapshot") {
            FeedMessage::Snapshot(snapshot) => {
                assert_eq!(snapshot.last_seq, Some(90));
                assert_eq!(snapshot.bids.len(), 2);
                // Bids descend from the best price, as the recorder expects.
                assert!(snapshot.bids[0].tick > snapshot.bids[1].tick);
                assert_eq!(snapshot.asks.len(), 1);
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_depth_batch_becomes_one_depth_message_with_its_sequence_range() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        sender.send(PublishEvent::FeedBatch {
            symbol: "BTCUSDT".into(),
            events: vec![
                depth_event(10.0, 1.0, 7, true),
                depth_event(11.0, 2.0, 8, false),
            ],
            stream: Some(metadata(1, 8, false)),
        });
        match feed.next().await.expect("a message") {
            FeedMessage::Depth(depth) => {
                assert_eq!(depth.first_seq, Some(7));
                assert_eq!(depth.last_seq, 8);
                assert_eq!(depth.changes.len(), 2);
                assert_eq!(depth.changes[0].side, Side::Bid);
                assert_eq!(depth.changes[1].side, Side::Ask);
            }
            other => panic!("expected a depth message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_snapshot_batch_replaces_the_book() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        let levels = vec![
            Event {
                ev: DEPTH_SNAPSHOT_EVENT | LOCAL_EVENT | BUY_EVENT,
                exch_ts: 1,
                local_ts: 2,
                px: 10.0,
                qty: 3.0,
                order_id: 0,
                ival: 100,
                fval: 0.0,
            },
            Event {
                ev: DEPTH_SNAPSHOT_EVENT | LOCAL_EVENT | SELL_EVENT,
                exch_ts: 1,
                local_ts: 2,
                px: 11.0,
                qty: 4.0,
                order_id: 0,
                ival: 100,
                fval: 0.0,
            },
        ];
        sender.send(PublishEvent::FeedBatch {
            symbol: "BTCUSDT".into(),
            events: levels,
            stream: Some(metadata(3, 100, true)),
        });
        match feed.next().await.expect("a message") {
            FeedMessage::Snapshot(snapshot) => {
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 1);
                assert_eq!(snapshot.last_seq, Some(100));
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalidation_reports_a_gap_at_the_last_recorded_sequence() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        sender.send(PublishEvent::FeedBatch {
            symbol: "BTCUSDT".into(),
            events: vec![depth_event(10.0, 1.0, 42, true)],
            stream: Some(metadata(1, 42, false)),
        });
        assert!(matches!(feed.next().await, Some(FeedMessage::Depth(_))));
        sender.send(PublishEvent::StreamInvalidated {
            symbol: "BTCUSDT".into(),
            epoch: 2,
        });
        match feed.next().await.expect("a gap") {
            FeedMessage::Gap(gap) => {
                assert_eq!(gap.reason, GapReason::SequenceBreak);
                assert_eq!(gap.last_seq, 42);
            }
            other => panic!("expected a gap, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn another_symbols_events_are_ignored() {
        let (mut feed, sender) = ConnectorFeed::channel(market());
        sender.send(PublishEvent::FeedBatch {
            symbol: "ETHUSDT".into(),
            events: vec![depth_event(10.0, 1.0, 1, true)],
            stream: Some(metadata(1, 1, false)),
        });
        sender.send(PublishEvent::FeedBatch {
            symbol: "BTCUSDT".into(),
            events: vec![depth_event(10.0, 1.0, 9, true)],
            stream: Some(metadata(1, 9, false)),
        });
        // The ETH batch must not surface; the first message is the BTC one.
        match feed.next().await.expect("the BTC batch") {
            FeedMessage::Depth(depth) => assert_eq!(depth.last_seq, 9),
            other => panic!("expected the BTC depth message, got {other:?}"),
        }
    }
}
