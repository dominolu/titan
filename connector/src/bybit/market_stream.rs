//! The Bybit orderbook stream: sequence check, image alignment and recovery.
//!
//! Bybit publishes the whole book when a subscription opens and increments afterwards, each
//! carrying an update id `u`. Two consequences decide this module's shape, and both are the reason
//! the check lives in the connector rather than where a frame is parsed:
//!
//! * The stream tracks `u`. A delta whose `u` is not the previous `u` + 1 means a lost message,
//!   which is not a broken socket: the stream publishes `StreamInvalidated`, asks the venue for the
//!   book again, and buffers what arrives until a new image carries it forward. A break must not
//!   cost the connection — reconnecting takes the trade stream down with it, and every reconnect
//!   restarts `u` below what the stream had reached.
//! * Every image is checked against what was published. An image older than the consumer's book is
//!   refused rather than preferred, and an image that cannot be proven to continue it is published
//!   only after the consumer was told to discard what it holds.
//!
//! Only a message the stream accepted is published, so a consumer either receives the continuation
//! of the image it holds or an explicit invalidation, never a silently different view.

use std::collections::VecDeque;

use chrono::Utc;
use titan_domain_types::{
    Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_EVENT,
    LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::bybit::{
    BybitError, BybitMarketStreamConfig,
    msg::{OrderbookData, RestOrderbook, StreamEnvelope, Ticker, Trade},
    rest::BybitClient,
};
use crate::connector::{
    MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch, PublishEvent, PublishSender,
};
use crate::transport;

/// Frames a connection may queue before the transport backs the socket up.
const CONNECTION_FRAMES: usize = 8192;
/// Increments held while waiting for the image that aligns them.
///
/// A published book is at most 500 levels and an image answers a subscription within one round
/// trip, so this is far past what a normal break produces. A full buffer means the image is not
/// coming — a rejected subscription, or a venue that stopped answering — and the stream says so
/// rather than growing without bound.
const MAX_BUFFERED_DELTAS: usize = 256;

/// One orderbook frame: its update id and the level pairs it carried, as the venue wrote them.
pub(crate) type Frame = (u64, Vec<(String, String)>, Vec<(String, String)>);

/// The update id of the book a consumer holds, and whether that was an image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StreamMark {
    pub sequence: u64,
    pub snapshot: bool,
}

/// What the stream knows about the book a consumer holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Nothing published yet: waiting for the subscription's image. Any image is taken, because
    /// there is no earlier view for an older one to contradict.
    Unprimed,
    /// Continuity holds. A delta is accepted iff `u == sequence + 1`.
    Streaming(StreamMark),
    /// A break was reported and the consumer discarded its book. Deltas are buffered until an image
    /// arrives; only an image newer than `sequence` is taken.
    AwaitingSnapshot(StreamMark),
    /// The connection ended. Bybit restarts `u` below the value this stream had reached, so no
    /// image arriving now can be proven to continue it: the next connection's image is the whole
    /// new book and the stream starts again from there.
    Disconnected,
}

/// The sequence rules for one orderbook, with no I/O.
///
/// Kept apart from the connection so the rules can be tested exactly: the frames the tests feed to
/// this state are the frames a live socket delivers.
pub(crate) struct BookState {
    phase: Phase,
    /// Frames received since continuity was lost, oldest first.
    buffered: VecDeque<Frame>,
}

/// What applying a delta decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DeltaDecision {
    /// The frame continues the published book and is published.
    Publish,
    /// A message was lost: the consumer must be invalidated, the frame buffered, and a new image
    /// requested.
    Invalidated { previous: u64, received: u64 },
    /// The frame was held until an image aligns it.
    Buffered,
    /// The frame is not newer than the published book and is dropped.
    Stale,
    /// The connection is not open; nothing can be accepted.
    Disconnected,
}

/// What applying an image decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ImageDecision {
    /// The frames to publish, the image first and the increments it carries forward after it.
    Publish {
        /// The consumer's current book cannot be extended by this image, so it must be told to
        /// discard it before the image is published.
        discard_previous: bool,
        frames: Vec<Frame>,
    },
    /// The image is not newer than the consumer's book and must not move it backwards.
    Stale,
}

impl BookState {
    pub(crate) fn new() -> Self {
        Self {
            phase: Phase::Unprimed,
            buffered: VecDeque::new(),
        }
    }

    /// The book a consumer holds, or `None` when it holds nothing this stream published.
    #[cfg(test)]
    pub(crate) fn published(&self) -> Option<StreamMark> {
        match self.phase {
            Phase::Streaming(mark) => Some(mark),
            Phase::Unprimed | Phase::AwaitingSnapshot(_) | Phase::Disconnected => None,
        }
    }

    /// The connection ended. Returns whether the consumer must be invalidated.
    ///
    /// A break already reported is not repeated: the consumer discarded its book either way, and
    /// a second report of the same interruption gives it nothing different to do.
    pub(crate) fn disconnected(&mut self) -> bool {
        let report = matches!(self.phase, Phase::Streaming(_));
        self.phase = Phase::Disconnected;
        self.buffered.clear();
        report
    }

    /// Applies one delta.
    pub(crate) fn delta(&mut self, frame: Frame) -> DeltaDecision {
        let sequence = frame.0;
        match self.phase {
            Phase::Streaming(mark) if sequence == mark.sequence + 1 => {
                self.phase = Phase::Streaming(StreamMark {
                    sequence,
                    snapshot: false,
                });
                DeltaDecision::Publish
            }
            Phase::Streaming(mark) if sequence > mark.sequence => {
                // A lost message. The frame that revealed it is kept: the image may bridge it.
                let previous = mark.sequence;
                self.phase = Phase::AwaitingSnapshot(mark);
                self.buffered.clear();
                self.push(frame);
                DeltaDecision::Invalidated {
                    previous,
                    received: sequence,
                }
            }
            Phase::Streaming(mark) => {
                // At or behind the published book: a repeated or reordered frame. Applying it again
                // would restate an update the consumer has or move its book backwards.
                debug!(
                    previous = mark.sequence,
                    received = sequence,
                    "Bybit delta is not newer than the published book"
                );
                DeltaDecision::Stale
            }
            Phase::AwaitingSnapshot(_) | Phase::Unprimed => {
                self.push(frame);
                DeltaDecision::Buffered
            }
            Phase::Disconnected => DeltaDecision::Disconnected,
        }
    }

    /// Applies one image.
    pub(crate) fn image(&mut self, frame: Frame) -> ImageDecision {
        let sequence = frame.0;
        let mark = match self.phase {
            Phase::Unprimed | Phase::Disconnected => None,
            Phase::Streaming(mark) | Phase::AwaitingSnapshot(mark) => Some(mark),
        };
        if let Some(mark) = mark
            && sequence <= mark.sequence
        {
            // Older than the consumer's book, or the same one: a delta carries that forward.
            return ImageDecision::Stale;
        }
        // The image is newer than the consumer's book. It continues it only if a buffered delta
        // carries the image's update id forward — the image itself is a new base, and any frame
        // received after it is already newer than the book.
        let discard_previous = mark.is_some();
        let frames = self.bridge(sequence, frame);
        let last = frames.last().map_or(sequence, |(sequence, _, _)| *sequence);
        self.phase = Phase::Streaming(StreamMark {
            sequence: last,
            snapshot: last == sequence,
        });
        ImageDecision::Publish {
            discard_previous,
            frames,
        }
    }

    /// Takes the image, then the buffered frames that continue it, in order.
    fn bridge(&mut self, sequence: u64, image: Frame) -> Vec<Frame> {
        let mut frames = vec![image];
        let mut previous = sequence;
        while let Some((next, _, _)) = self.buffered.front() {
            let next = *next;
            if next <= previous {
                // Covered by the image or by an earlier buffered frame.
                self.buffered.pop_front();
                continue;
            }
            if next != previous + 1 {
                // The chain is broken again. Nothing past this point can be published; the caller
                // asks for another image, and the book stays at the last contiguous update id.
                warn!(
                    previous,
                    received = next,
                    "the Bybit image does not carry the buffered depth forward"
                );
                self.buffered.clear();
                break;
            }
            previous = next;
            if let Some(frame) = self.buffered.pop_front() {
                frames.push(frame);
            }
        }
        frames
    }

    fn push(&mut self, frame: Frame) {
        if self.buffered.len() >= MAX_BUFFERED_DELTAS {
            let dropped = self.buffered.pop_front().map(|(sequence, _, _)| sequence);
            warn!(
                ?dropped,
                received = frame.0,
                "the Bybit depth buffer is full; the oldest increment was dropped"
            );
        }
        self.buffered.push_back(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(sequence: u64) -> Frame {
        (
            sequence,
            vec![("100.0".to_owned(), "1".to_owned())],
            vec![("101.0".to_owned(), "1".to_owned())],
        )
    }

    fn ids(frames: &[Frame]) -> Vec<u64> {
        frames.iter().map(|(sequence, _, _)| *sequence).collect()
    }

    fn mark(sequence: u64, snapshot: bool) -> Option<StreamMark> {
        Some(StreamMark { sequence, snapshot })
    }

    #[test]
    fn the_first_image_is_published_before_anything_else() {
        let mut state = BookState::new();
        assert_eq!(
            state.image(frame(10)),
            ImageDecision::Publish {
                discard_previous: false,
                frames: vec![frame(10)],
            }
        );
        assert_eq!(state.published(), mark(10, true));
    }

    #[test]
    fn contiguous_deltas_are_published_and_a_gap_invalidates_without_losing_the_connection() {
        let mut state = BookState::new();
        state.image(frame(10));
        assert_eq!(state.delta(frame(11)), DeltaDecision::Publish);
        assert_eq!(state.delta(frame(12)), DeltaDecision::Publish);
        assert_eq!(state.published(), mark(12, false));

        // 13 was lost. The break is reported and the frame that revealed it is kept.
        assert_eq!(
            state.delta(frame(14)),
            DeltaDecision::Invalidated {
                previous: 12,
                received: 14
            }
        );
        assert_eq!(state.published(), None);

        // The image the venue answers with is newer than the book, so it is taken after the
        // consumer was told to discard it, and it carries the buffered 14 forward with its levels.
        assert_eq!(
            state.image(frame(13)),
            ImageDecision::Publish {
                discard_previous: true,
                frames: vec![frame(13), frame(14)],
            }
        );
        assert_eq!(state.published(), mark(14, false));
        // Continuity holds again from the replayed update id.
        assert_eq!(state.delta(frame(15)), DeltaDecision::Publish);
    }

    #[test]
    fn a_snapshot_older_than_the_stream_does_not_move_the_book_backwards() {
        let mut state = BookState::new();
        state.image(frame(10));
        state.delta(frame(11));
        state.delta(frame(12));
        // The image can arrive after the deltas that overtook it; taking it would lose 11 and 12.
        assert_eq!(state.image(frame(11)), ImageDecision::Stale);
        // An equal image adds nothing either: the book already has 12.
        assert_eq!(state.image(frame(12)), ImageDecision::Stale);
        assert_eq!(state.published(), mark(12, false));
    }

    #[test]
    fn a_repeated_frame_is_not_published_twice() {
        let mut state = BookState::new();
        state.image(frame(10));
        assert_eq!(state.delta(frame(11)), DeltaDecision::Publish);
        assert_eq!(state.delta(frame(11)), DeltaDecision::Stale);
        assert_eq!(state.delta(frame(5)), DeltaDecision::Stale);
        assert_eq!(state.published(), mark(11, false));
    }

    #[test]
    fn a_reconnect_starts_a_fresh_book_even_below_the_previous_update_id() {
        let mut state = BookState::new();
        state.image(frame(100));
        state.delta(frame(101));
        // The connection ended: the consumer's book is invalidated exactly once.
        assert!(state.disconnected());
        assert!(!state.disconnected());
        assert_eq!(state.published(), None);
        // Bybit restarts `u` on the new connection; that image is the whole new book.
        assert_eq!(
            state.image(frame(7)),
            ImageDecision::Publish {
                discard_previous: false,
                frames: vec![frame(7)],
            }
        );
        assert_eq!(state.delta(frame(8)), DeltaDecision::Publish);
    }

    #[test]
    fn a_break_already_reported_is_not_reported_again_when_the_connection_ends() {
        let mut state = BookState::new();
        state.image(frame(10));
        assert!(matches!(
            state.delta(frame(12)),
            DeltaDecision::Invalidated { .. }
        ));
        // The consumer was already told to discard its book; the socket ending changes nothing.
        assert!(!state.disconnected());
    }

    #[test]
    fn deltas_before_the_first_image_are_buffered_and_replayed() {
        let mut state = BookState::new();
        assert_eq!(state.delta(frame(40)), DeltaDecision::Buffered);
        assert_eq!(state.delta(frame(41)), DeltaDecision::Buffered);
        assert_eq!(
            state.image(frame(40)),
            ImageDecision::Publish {
                discard_previous: false,
                frames: vec![frame(40), frame(41)],
            }
        );
        assert_eq!(state.delta(frame(42)), DeltaDecision::Publish);
    }

    #[test]
    fn a_buffer_that_does_not_continue_the_image_is_discarded_not_published() {
        let mut state = BookState::new();
        assert_eq!(state.delta(frame(40)), DeltaDecision::Buffered);
        // 41 never arrived. The image lands at 42, so 40 is behind it and nothing ahead is known.
        assert_eq!(
            state.image(frame(42)),
            ImageDecision::Publish {
                discard_previous: false,
                frames: vec![frame(42)],
            }
        );
        assert_eq!(state.published(), mark(42, true));
    }

    #[test]
    fn an_image_that_cannot_carry_the_buffer_forward_publishes_only_itself() {
        let mut state = BookState::new();
        state.image(frame(10));
        assert!(matches!(
            state.delta(frame(12)),
            DeltaDecision::Invalidated { .. }
        ));
        // 13 never arrives; the buffer holds 14 alone, so the image at 13 cannot reach it.
        assert_eq!(
            state.image(frame(13)),
            ImageDecision::Publish {
                discard_previous: true,
                frames: vec![frame(13)],
            }
        );
        assert_eq!(state.published(), mark(13, true));
        // The next frame is a gap against 13, and is reported as one.
        assert_eq!(
            state.delta(frame(15)),
            DeltaDecision::Invalidated {
                previous: 13,
                received: 15
            }
        );
    }

    #[test]
    fn the_buffer_is_bounded_and_keeps_the_newest_frames() {
        let mut state = BookState::new();
        let newest = MAX_BUFFERED_DELTAS as u64 + 10;
        for sequence in 1..=newest {
            assert_eq!(state.delta(frame(sequence)), DeltaDecision::Buffered);
        }
        // The oldest frames were evicted, so an image at 1 cannot reach the buffer: only a
        // contiguous run from the image is published, never a jump to what survived.
        match state.image(frame(1)) {
            ImageDecision::Publish { frames, .. } => assert_eq!(ids(&frames), vec![1]),
            other => panic!("expected the image to publish, got {other:?}"),
        }
        // From the newest edge the buffer still replays the frames it kept.
        let mut state = BookState::new();
        for sequence in 1..=newest {
            state.delta(frame(sequence));
        }
        match state.image(frame(newest - 1)) {
            ImageDecision::Publish { frames, .. } => {
                assert_eq!(ids(&frames), vec![newest - 1, newest])
            }
            other => panic!("expected the image to publish, got {other:?}"),
        }
    }
}

/// One Bybit connection: reads frames, owns the book state, and publishes what it accepted.
pub(crate) struct MarketStream {
    config: BybitMarketStreamConfig,
    client: BybitClient,
    ev_tx: PublishSender,
    book: BookState,
}

impl MarketStream {
    pub(crate) fn new(config: BybitMarketStreamConfig, ev_tx: PublishSender) -> Self {
        let client = config.client();
        Self {
            config,
            client,
            ev_tx,
            book: BookState::new(),
        }
    }

    /// Opens the connection and serves it until it ends.
    ///
    /// The image that recovers a break is fetched by a task and returns through a channel, so a
    /// REST round trip never stops the socket being read: frames arriving meanwhile are buffered by
    /// the state machine exactly as frames after any other break are.
    pub(crate) async fn connect(&mut self) -> Result<(), BybitError> {
        let (frames_tx, mut frames) = mpsc::channel(CONNECTION_FRAMES);
        let (snapshots_tx, mut snapshots) = mpsc::channel::<RestOrderbook>(SNAPSHOT_CHANNEL);
        // Owned by this connection: dropping its receiver ends the transport's send loop, and
        // aborting the task closes the socket.
        let connection = tokio::spawn(transport::keep_connection(
            self.config.ws_target(),
            frames_tx,
        ));
        let mut open_interest = self.open_interest_timer();
        // One iteration handles one event; the loop ends only when the socket does.
        //
        // The open interest arm is a plain `if` guard rather than an optional future: a `select!`
        // with every arm disabled returns immediately, which would end the connection as soon as
        // the first frame was handled.
        let error = loop {
            let poll_enabled = open_interest.is_some();
            tokio::select! {
                frame = frames.recv() => match frame {
                    Some((_, text)) => {
                        if let Err(error) = self.handle_text(text.as_str(), &snapshots_tx) {
                            break error;
                        }
                    }
                    // The transport is reconnecting; nothing more comes from this socket.
                    None => break BybitError::ConnectionInterrupted,
                },
                // The image task only ends with the connection, where the socket arm decides.
                Some(snapshot) = snapshots.recv() => {
                    let bids = level_pairs(&snapshot.bids);
                    let asks = level_pairs(&snapshot.asks);
                    // A REST image carries its own exchange time; without one the image is still
                    // the venue's state as of now, and its own time is unknown.
                    let exch_ts = snapshot.ts.unwrap_or_default().saturating_mul(1_000_000);
                    self.apply_image(snapshot.update_id, bids, asks, exch_ts);
                }
                // Without a configured poll this arm is disabled, and the other two always keep
                // the connection alive: a `select!` with nothing to wait on ends the stream, which
                // is why the poll is a guard rather than an optional future in the select body.
                _ = poll_tick(open_interest.as_mut()), if poll_enabled => {
                    self.poll_open_interest().await;
                }
            }
        };
        connection.abort();
        // The book stops being extended. A consumer that holds one is told so; the run loop
        // reports the interruption either way.
        if self.book.disconnected() {
            self.publish_invalidation();
        }
        Err(error)
    }

    /// The open interest poll, when the category and the configuration ask for one.
    ///
    /// The first tick is delayed rather than immediate: an image and an open interest request in
    /// the same breath spend request weight before the book has started. A missed tick is not
    /// caught up either, so a stalled connection does not pay a burst of polls when it returns.
    fn open_interest_timer(&self) -> Option<tokio::time::Interval> {
        let interval = self.config.open_interest_interval;
        if interval.is_zero() || !self.config.category.is_derivative() {
            return None;
        }
        let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Some(timer)
    }

    async fn poll_open_interest(&self) {
        match self.client.get_open_interest(&self.config.symbol).await {
            Ok(row) if !row.open_interest.is_empty() => {
                let Ok(exch_ts) = row.timestamp.parse::<i64>() else {
                    debug!(symbol = %self.config.symbol, "open interest sample without a time");
                    return;
                };
                self.ev_tx.send(PublishEvent::OpenInterest {
                    symbol: self.config.symbol.clone(),
                    open_interest: row.open_interest,
                    // Bybit reports open interest in contract counts.
                    unit: crate::connector::OpenInterestUnit::Contracts,
                    exch_ts: exch_ts.saturating_mul(1_000_000),
                });
            }
            Ok(_) => debug!(symbol = %self.config.symbol, "open interest sample without a value"),
            Err(error) => {
                // A missing sample, not a broken book: the orderbook is whole and the next tick
                // asks again.
                warn!(symbol = %self.config.symbol, ?error, "Bybit open interest poll failed");
            }
        }
    }

    /// Starts the image request for the current break; at most one is in flight.
    fn request_snapshot(&self, snapshots: &mpsc::Sender<RestOrderbook>) {
        if snapshots.capacity() == 0 {
            debug!(symbol = %self.config.symbol, "a Bybit image request is already in flight");
            return;
        }
        let client = self.client.clone();
        let symbol = self.config.symbol.clone();
        let depth = self.config.depth;
        let snapshots = snapshots.clone();
        tokio::spawn(async move {
            match client.get_orderbook(&symbol, depth).await {
                Ok(book) => {
                    let _ = snapshots.send(book).await;
                }
                Err(error) => {
                    // The break stays reported, so the consumer never extends a book the image
                    // never aligned; the next delta that reveals the gap asks again.
                    warn!(%symbol, ?error, "Bybit orderbook image request failed");
                }
            }
        });
    }

    fn publish_invalidation(&self) {
        self.ev_tx.send(PublishEvent::StreamInvalidated {
            symbol: self.config.symbol.clone(),
            epoch: 0,
        });
    }

    fn handle_text(
        &mut self,
        text: &str,
        snapshots: &mpsc::Sender<RestOrderbook>,
    ) -> Result<(), BybitError> {
        let envelope: StreamEnvelope = serde_json::from_str(text)?;
        if envelope.success == Some(false) {
            // A rejected subscription is not fixed by waiting, and an open connection that
            // publishes nothing looks healthy. It ends the connection so the run loop reports it.
            return Err(BybitError::Rejected {
                message: envelope
                    .ret_msg
                    .unwrap_or_else(|| "subscription rejected".to_owned()),
            });
        }
        let Some(topic) = envelope.topic.as_deref() else {
            debug!(?envelope.op, "Bybit subscription acknowledged");
            return Ok(());
        };
        let exch_ts = envelope.ts.unwrap_or_default().saturating_mul(1_000_000);
        if topic.starts_with("orderbook.") {
            let book: OrderbookData = envelope.decode()?;
            self.handle_orderbook(envelope.kind.as_deref(), book, exch_ts, snapshots);
        } else if topic.starts_with("publicTrade.") {
            let trades: Vec<Trade> = envelope.decode()?;
            self.publish_trades(trades);
        } else if topic.starts_with("tickers.") {
            let ticker: Ticker = envelope.decode()?;
            self.publish_ticker(ticker, exch_ts);
        }
        Ok(())
    }

    fn handle_orderbook(
        &mut self,
        kind: Option<&str>,
        book: OrderbookData,
        exch_ts: i64,
        snapshots: &mpsc::Sender<RestOrderbook>,
    ) {
        let bids = level_pairs(&book.b);
        let asks = level_pairs(&book.a);
        if kind == Some("snapshot") {
            // A reconnect or a resubscribe answers with the whole book over the socket itself.
            self.apply_image(book.u, bids, asks, exch_ts);
            return;
        }
        let Some(sequence) = u64::try_from(book.u).ok().filter(|id| *id > 0) else {
            warn!(
                update_id = book.u,
                "Bybit orderbook frame without a usable update id"
            );
            return;
        };
        match self.book.delta((sequence, bids.clone(), asks.clone())) {
            DeltaDecision::Publish => self.publish_depth(&bids, &asks, exch_ts, false, sequence),
            DeltaDecision::Invalidated { previous, received } => {
                warn!(
                    symbol = %self.config.symbol,
                    previous, received, "Bybit orderbook update id gap; requesting a new image"
                );
                self.publish_invalidation();
                self.request_snapshot(snapshots);
            }
            DeltaDecision::Buffered => {}
            DeltaDecision::Stale => debug!(
                symbol = %self.config.symbol,
                update_id = book.u, "Bybit delta is not newer than the published book; ignored"
            ),
            DeltaDecision::Disconnected => {}
        }
    }

    /// Applies a whole book, from the socket or from REST.
    fn apply_image(
        &mut self,
        update_id: i64,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
        exch_ts: i64,
    ) {
        let Some(sequence) = u64::try_from(update_id).ok().filter(|id| *id > 0) else {
            warn!(update_id, "Bybit image without a usable update id");
            return;
        };
        match self.book.image((sequence, bids.clone(), asks.clone())) {
            ImageDecision::Publish {
                discard_previous,
                frames,
            } => {
                if discard_previous {
                    self.publish_invalidation();
                }
                // The image replaces the book; the increments it carries forward are deltas, with
                // the levels they arrived with.
                for (index, (sequence, bids, asks)) in frames.iter().enumerate() {
                    self.publish_depth(bids, asks, exch_ts, index == 0, *sequence);
                }
            }
            ImageDecision::Stale => debug!(
                symbol = %self.config.symbol,
                update_id, "Bybit image is not newer than the published book; ignored"
            ),
        }
    }

    fn publish_depth(
        &self,
        bids: &[(String, String)],
        asks: &[(String, String)],
        exch_ts: i64,
        snapshot: bool,
        sequence: u64,
    ) {
        let bid_levels = level_refs(bids);
        let ask_levels = level_refs(asks);
        let metadata = MarketStreamMetadata {
            epoch: 0,
            first_update_sequence: sequence,
            last_update_sequence: sequence,
            snapshot,
        };
        if self.ev_tx.try_send_native_market(NativeMarketBatch::Depth {
            symbol: &self.config.symbol,
            bids: NativeDepthLevels::Borrowed(&bid_levels),
            asks: NativeDepthLevels::Borrowed(&ask_levels),
            exchange_ts: exch_ts,
            receive_ts: now_ns(),
            stream: metadata,
        }) {
            return;
        }
        self.ev_tx.send(PublishEvent::FeedBatch {
            symbol: self.config.symbol.clone(),
            events: depth_events(&bid_levels, &ask_levels, exch_ts, snapshot),
            stream: Some(metadata),
        });
    }

    fn publish_trades(&self, trades: Vec<Trade>) {
        for trade in trades {
            let (Ok(price), Ok(quantity)) = (trade.p.parse::<f64>(), trade.v.parse::<f64>()) else {
                continue;
            };
            // `Buy` is the taker buying, which moves the market up.
            let sell = trade.side == "Sell";
            let exch_ts = trade.time_ms.saturating_mul(1_000_000);
            if self.ev_tx.try_send_native_market(NativeMarketBatch::Trade {
                symbol: &self.config.symbol,
                price,
                quantity,
                sell,
                exchange_ts: exch_ts,
                receive_ts: now_ns(),
            }) {
                continue;
            }
            self.ev_tx.send(PublishEvent::FeedBatch {
                symbol: self.config.symbol.clone(),
                events: vec![Event {
                    ev: if sell {
                        LOCAL_SELL_TRADE_EVENT
                    } else {
                        LOCAL_BUY_TRADE_EVENT
                    },
                    exch_ts,
                    local_ts: now_ns(),
                    order_id: 0,
                    px: price,
                    qty: quantity,
                    ival: 0,
                    fval: 0.0,
                }],
                stream: None,
            });
        }
    }

    /// Publishes funding and, beside it, the mark price the same ticker carried.
    fn publish_ticker(&self, ticker: Ticker, exch_ts: i64) {
        let (Some(rate), Some(next)) = (ticker.funding_rate, ticker.next_funding_time) else {
            return;
        };
        let (Ok(rate), Ok(next)) = (rate.parse::<f64>(), next.parse::<i64>()) else {
            return;
        };
        self.ev_tx.send(PublishEvent::Funding {
            symbol: self.config.symbol.clone(),
            funding_rate: rate,
            next_funding_time: next.saturating_mul(1_000_000),
            exch_ts,
        });
        if let Some(mark) = ticker.mark_price
            && let Ok(mark) = mark.parse::<f64>()
        {
            self.ev_tx.send(PublishEvent::MarkPrice {
                symbol: self.config.symbol.clone(),
                mark_price: mark,
                exch_ts,
            });
        }
    }
}

/// Waits for the next open interest poll.
///
/// The timer is optional because most streams do not poll open interest at all; the caller guards
/// the arm, so a `None` here is never polled.
async fn poll_tick(timer: Option<&mut tokio::time::Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// How many image answers may be queued for a connection: one, because a second request while the
/// first is outstanding asks the same question again.
const SNAPSHOT_CHANNEL: usize = 1;

fn level_pairs(rows: &[Vec<String>]) -> Vec<(String, String)> {
    rows.iter()
        .filter_map(|row| match row.as_slice() {
            [price, quantity, ..] => Some((price.clone(), quantity.clone())),
            _ => None,
        })
        .collect()
}

fn level_refs(levels: &[(String, String)]) -> Vec<(&str, &str)> {
    levels
        .iter()
        .map(|(price, quantity)| (price.as_str(), quantity.as_str()))
        .collect()
}

fn depth_events(
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
    exch_ts: i64,
    snapshot: bool,
) -> Vec<Event> {
    let (bid_kind, ask_kind) = if snapshot {
        (
            LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
            LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
        )
    } else {
        (LOCAL_BID_DEPTH_EVENT, LOCAL_ASK_DEPTH_EVENT)
    };
    let local_ts = now_ns();
    let mut events = Vec::with_capacity(bids.len() + asks.len());
    for (kind, side) in [(bid_kind, bids), (ask_kind, asks)] {
        for (price, quantity) in side.iter() {
            let (Ok(price), Ok(quantity)) = (price.parse::<f64>(), quantity.parse::<f64>()) else {
                continue;
            };
            events.push(Event {
                ev: kind,
                exch_ts,
                local_ts,
                order_id: 0,
                px: price,
                qty: quantity,
                ival: 0,
                fval: 0.0,
            });
        }
    }
    events
}

fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::connector::DirectPublication;
    use futures_util::{SinkExt, StreamExt};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio_tungstenite::tungstenite::Message;

    fn topic(kind: &str, update_id: i64) -> String {
        serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": kind,
            "ts": 1_700_000_000_000_i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["100.0", "1"]],
                "a": [["101.0", "1"]],
                "u": update_id,
            }
        })
        .to_string()
    }

    /// Serves one scripted connection: the frames are sent in order after the subscription, then
    /// the socket stays open, so a stream that ended early is visible as a return before then.
    async fn serve(frames: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut received = Vec::new();
            // Wait for the subscription before answering, as the venue does; a ping or pong in
            // between is not the subscription and must not be mistaken for it.
            while received.is_empty() {
                let Ok(message) =
                    tokio::time::timeout(Duration::from_millis(500), websocket.next()).await
                else {
                    break;
                };
                match message {
                    Some(Ok(Message::Text(text))) => received.push(text.to_string()),
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    _ => break,
                }
            }
            for frame in frames {
                websocket.send(Message::Text(frame.into())).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
            received
        });
        (format!("ws://{address}"), task)
    }

    fn config(url: String) -> BybitMarketStreamConfig {
        let mut config =
            BybitMarketStreamConfig::new(crate::bybit::BybitCategory::Linear, "BTCUSDT", 50);
        config.stream_url = url;
        // No REST image is served here: the sequence state machine is what these tests exercise.
        config.api_url = "http://127.0.0.1:9".to_owned();
        config.open_interest_interval = Duration::ZERO;
        config
    }

    /// Runs the stream over `frames` and returns what it published as one line per publication.
    async fn run(frames: Vec<String>) -> (Vec<String>, bool) {
        let (url, server) = serve(frames).await;
        let published = Arc::new(Mutex::new(Vec::new()));
        let sink = published.clone();
        let sender = crate::connector::direct_publish_sender(move |publication| {
            let line = match publication {
                DirectPublication::NativeMarket(NativeMarketBatch::Depth { stream, .. }) => {
                    format!("depth {}", stream.last_update_sequence)
                }
                DirectPublication::Event(PublishEvent::StreamInvalidated { symbol, epoch }) => {
                    format!("invalidated {symbol} {epoch}")
                }
                DirectPublication::Event(PublishEvent::ConnectorError(_)) => "error".to_owned(),
                DirectPublication::Event(_) => "event".to_owned(),
                DirectPublication::NativeMarket(_) => "market".to_owned(),
                DirectPublication::Account(_) => "account".to_owned(),
            };
            sink.lock().unwrap().push(line);
        });
        let mut stream = MarketStream::new(config(url), sender);
        // The socket outlives the window, so a stream that ended inside it ended for its own
        // reason: that is exactly what this flag reports.
        let ended = tokio::time::timeout(Duration::from_millis(80), stream.connect())
            .await
            .is_ok();
        let _subscription = server.await.unwrap();
        let published = published.lock().unwrap().clone();
        (published, ended)
    }

    fn depths(published: &[String]) -> Vec<u64> {
        published
            .iter()
            .filter_map(|line| line.strip_prefix("depth ")?.parse().ok())
            .collect()
    }

    #[tokio::test]
    async fn a_sequence_break_publishes_an_invalidation_and_keeps_the_connection() {
        // 11 and 14 arrive: 12 and 13 are lost. The break is reported, and the connection is still
        // being read when the test window closes.
        let (published, ended) = run(vec![
            topic("snapshot", 10),
            topic("delta", 11),
            topic("delta", 14),
        ])
        .await;
        assert!(!ended, "a sequence break must not end the connection");
        assert!(
            published.iter().any(|line| line.starts_with("invalidated")),
            "the break must be reported: {published:?}"
        );
        // Only what the stream could prove: the image and the increment that continued it.
        assert_eq!(depths(&published), vec![10, 11], "{published:?}");
    }

    #[tokio::test]
    async fn a_repeated_delta_is_published_once() {
        let (published, _) = run(vec![
            topic("snapshot", 10),
            topic("delta", 11),
            topic("delta", 11),
            topic("delta", 12),
        ])
        .await;
        assert_eq!(depths(&published), vec![10, 11, 12], "{published:?}");
        assert!(
            published.iter().all(|line| line.starts_with("depth")),
            "a repeat is not a gap: {published:?}"
        );
    }

    #[tokio::test]
    async fn an_image_behind_the_published_book_does_not_move_it_backwards() {
        let (published, _) = run(vec![
            topic("snapshot", 10),
            topic("delta", 11),
            topic("snapshot", 10),
        ])
        .await;
        assert_eq!(depths(&published), vec![10, 11], "{published:?}");
    }

    #[tokio::test]
    async fn the_subscription_is_sent_when_the_socket_opens() {
        let (url, server) = serve(vec![topic("snapshot", 10)]).await;
        let (events, _receiver) = crate::connector::test_publish_channel();
        let mut stream = MarketStream::new(config(url), events);
        let _ = tokio::time::timeout(Duration::from_millis(80), stream.connect()).await;
        let received = server.await.unwrap();
        assert!(
            received
                .iter()
                .any(|text| text.contains("orderbook.50.BTCUSDT")),
            "{received:?}"
        );
    }
}
