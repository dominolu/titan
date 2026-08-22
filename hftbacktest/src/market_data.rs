//! Canonical Bar/Tick data types shared by backtest, live trading and foreign runtimes.
//!
//! A [`Bar`] contains market-time data only. Delivery/receive time belongs to the runtime
//! event envelope and is deliberately not part of the bar payload.

use crate::types::{BUY_EVENT, Event};

/// The bar is complete and may be delivered to a strategy.
pub const BAR_COMPLETE: u64 = 1 << 0;
/// The bar contains no trades.
pub const BAR_EMPTY: u64 = 1 << 1;
/// The bar was synthesized by a configured empty-bar policy.
pub const BAR_SYNTHETIC: u64 = 1 << 2;
/// The bar came from an exchange-native candle source.
pub const BAR_NATIVE: u64 = 1 << 3;
/// The bar is partial and must not be delivered as a normal close event.
pub const BAR_PARTIAL: u64 = 1 << 4;

/// Canonical closed OHLCV bar.
///
/// The covered interval is always `[open_ts, close_ts)`. A trade at exactly
/// `close_ts` belongs to the next bar. Timestamps are Unix epoch nanoseconds.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub open_ts: i64,
    pub close_ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Base-asset quantity (or the venue's documented contract quantity).
    pub volume: f64,
    pub quote_volume: f64,
    /// Quantity initiated by buyers.
    pub buy_volume: f64,
    pub trade_count: u64,
    pub flags: u64,
}

impl Default for Bar {
    fn default() -> Self {
        Self {
            open_ts: 0,
            close_ts: 0,
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            volume: 0.0,
            quote_volume: 0.0,
            buy_volume: 0.0,
            trade_count: 0,
            flags: 0,
        }
    }
}

impl Bar {
    #[inline(always)]
    pub fn is_complete(&self) -> bool {
        self.flags & BAR_COMPLETE != 0 && self.flags & BAR_PARTIAL == 0
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.flags & BAR_EMPTY != 0
    }
}

/// How missing fixed intervals are represented by a canonical builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyBarPolicy {
    /// Do not emit a bar for an interval without trades.
    Omit,
    /// Emit a synthetic bar whose OHLC is the previous close and whose volume is zero.
    PreviousClose,
    /// Emit an empty bar whose OHLC values are NaN and whose volume is zero.
    Nan,
}

/// Time used to assign live trades to intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarClock {
    ExchangeTime,
    LocalReceiveTime,
}

/// Deterministic fixed-duration bar construction rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarSpec {
    pub timeframe_ns: i64,
    /// Offset from Unix epoch used for interval alignment.
    pub alignment_offset_ns: i64,
    pub clock: BarClock,
    pub empty_policy: EmptyBarPolicy,
    /// How long exchange-time input may arrive late before a bar is finalized.
    pub allowed_lateness_ns: i64,
}

impl BarSpec {
    pub fn validate(&self) -> Result<(), BarError> {
        if self.timeframe_ns <= 0 {
            return Err(BarError::InvalidTimeframe(self.timeframe_ns));
        }
        if self.allowed_lateness_ns < 0 {
            return Err(BarError::InvalidLateness(self.allowed_lateness_ns));
        }
        Ok(())
    }

    #[inline(always)]
    pub fn open_ts(&self, timestamp: i64) -> i64 {
        (timestamp - self.alignment_offset_ns).div_euclid(self.timeframe_ns) * self.timeframe_ns
            + self.alignment_offset_ns
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BarError {
    #[error("bar timeframe must be positive, got {0}")]
    InvalidTimeframe(i64),
    #[error("allowed lateness cannot be negative, got {0}")]
    InvalidLateness(i64),
    #[error("trade timestamp {trade_ts} is older than finalized boundary {finalized_before}")]
    LateTrade {
        trade_ts: i64,
        finalized_before: i64,
    },
}

/// Streaming and offline implementation of the canonical fixed-duration bar rules.
///
/// The same implementation is intended for historical materialization and live input.
/// Closed bars are returned to the caller; no callback or Python code runs here.
#[derive(Debug, Clone)]
pub struct CanonicalBarBuilder {
    spec: BarSpec,
    current: Option<Bar>,
    previous_close: Option<f64>,
    finalized_before: i64,
}

impl CanonicalBarBuilder {
    pub fn new(spec: BarSpec) -> Result<Self, BarError> {
        spec.validate()?;
        Ok(Self {
            spec,
            current: None,
            previous_close: None,
            finalized_before: i64::MIN,
        })
    }

    pub fn spec(&self) -> BarSpec {
        self.spec
    }

    /// Pushes a normalized trade and appends every newly closed bar to `out`.
    ///
    /// Input must be ordered by the configured bar clock. A caller that accepts
    /// out-of-order exchange events must hold them until its watermark is safe and then
    /// pass them here in timestamp order.
    pub fn push_trade(&mut self, trade: &Event, out: &mut Vec<Bar>) -> Result<(), BarError> {
        let ts = match self.spec.clock {
            BarClock::ExchangeTime => trade.exch_ts,
            BarClock::LocalReceiveTime => trade.local_ts,
        };
        if ts < self.finalized_before {
            return Err(BarError::LateTrade {
                trade_ts: ts,
                finalized_before: self.finalized_before,
            });
        }

        let open_ts = self.spec.open_ts(ts);
        self.advance_to(open_ts, out);
        let bar = self.current.get_or_insert_with(|| Bar {
            open_ts,
            close_ts: open_ts + self.spec.timeframe_ns,
            open: trade.px,
            high: trade.px,
            low: trade.px,
            close: trade.px,
            volume: 0.0,
            quote_volume: 0.0,
            buy_volume: 0.0,
            trade_count: 0,
            flags: 0,
        });
        bar.high = bar.high.max(trade.px);
        bar.low = bar.low.min(trade.px);
        bar.close = trade.px;
        bar.volume += trade.qty;
        bar.quote_volume += trade.px * trade.qty;
        if trade.is(BUY_EVENT) {
            bar.buy_volume += trade.qty;
        }
        bar.trade_count += 1;
        Ok(())
    }

    /// Finalizes intervals whose close is not later than `watermark_ts`.
    pub fn advance_watermark(&mut self, watermark_ts: i64, out: &mut Vec<Bar>) {
        let Some(current) = self.current else {
            return;
        };
        if watermark_ts < current.close_ts + self.spec.allowed_lateness_ns {
            return;
        }
        let safe_ts = watermark_ts.saturating_sub(self.spec.allowed_lateness_ns);
        self.advance_to(self.spec.open_ts(safe_ts), out);
    }

    /// Returns the unfinished current bar with `BAR_PARTIAL`; it is not emitted normally.
    pub fn partial(&self) -> Option<Bar> {
        self.current.map(|mut bar| {
            bar.flags |= BAR_PARTIAL;
            bar
        })
    }

    /// Finalizes the current non-empty interval without inventing future empty bars.
    /// Offline materialization calls this at end of input; live runtimes normally close
    /// through [`Self::advance_watermark`].
    pub fn finish(&mut self, out: &mut Vec<Bar>) {
        if let Some(mut current) = self.current.take() {
            current.flags |= BAR_COMPLETE;
            current.flags &= !BAR_PARTIAL;
            self.previous_close = Some(current.close);
            self.finalized_before = current.close_ts;
            out.push(current);
        }
    }

    fn advance_to(&mut self, target_open: i64, out: &mut Vec<Bar>) {
        let Some(mut current) = self.current.take() else {
            return;
        };
        if target_open <= current.open_ts {
            self.current = Some(current);
            return;
        }

        current.flags |= BAR_COMPLETE;
        self.previous_close = Some(current.close);
        self.finalized_before = current.close_ts;
        let mut next_open = current.close_ts;
        out.push(current);

        while next_open < target_open {
            if let Some(empty) = self.empty_bar(next_open) {
                out.push(empty);
            }
            self.finalized_before = next_open + self.spec.timeframe_ns;
            next_open += self.spec.timeframe_ns;
        }
        self.current = None;
    }

    fn empty_bar(&self, open_ts: i64) -> Option<Bar> {
        let mut bar = Bar {
            open_ts,
            close_ts: open_ts + self.spec.timeframe_ns,
            flags: BAR_COMPLETE | BAR_EMPTY,
            ..Bar::default()
        };
        match self.spec.empty_policy {
            EmptyBarPolicy::Omit => None,
            EmptyBarPolicy::PreviousClose => {
                let close = self.previous_close?;
                bar.open = close;
                bar.high = close;
                bar.low = close;
                bar.close = close;
                bar.flags |= BAR_SYNTHETIC;
                Some(bar)
            }
            EmptyBarPolicy::Nan => Some(bar),
        }
    }
}

/// Fixed-capacity closed-bar history. The current callback's bar is committed only after
/// the callback, so `get(-1)` means the bar immediately preceding the current callback.
#[derive(Debug, Clone)]
pub struct BarHistory {
    bars: Vec<Bar>,
    capacity: usize,
    next: usize,
    len: usize,
}

impl BarHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            bars: vec![Bar::default(); capacity],
            capacity,
            next: 0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn next_index(&self) -> usize {
        self.next
    }

    pub fn as_ptr(&self) -> *const Bar {
        self.bars.as_ptr()
    }

    pub fn push(&mut self, bar: Bar) {
        if self.capacity == 0 {
            return;
        }
        self.bars[self.next] = bar;
        self.next = (self.next + 1) % self.capacity;
        self.len = (self.len + 1).min(self.capacity);
    }

    /// Python-style negative indexing over closed history: `-1` is the latest committed
    /// bar, `-2` the one before it. Non-negative indices count from the oldest retained bar.
    pub fn get(&self, index: isize) -> Option<&Bar> {
        if self.len == 0 {
            return None;
        }
        let logical = if index < 0 {
            self.len as isize + index
        } else {
            index
        };
        if logical < 0 || logical >= self.len as isize {
            return None;
        }
        let oldest = (self.next + self.capacity - self.len) % self.capacity;
        self.bars.get((oldest + logical as usize) % self.capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EXCH_EVENT, LOCAL_EVENT, TRADE_EVENT};

    fn trade(exch_ts: i64, local_ts: i64, px: f64, qty: f64, buy: bool) -> Event {
        Event {
            ev: TRADE_EVENT | EXCH_EVENT | LOCAL_EVENT | if buy { BUY_EVENT } else { 0 },
            exch_ts,
            local_ts,
            px,
            qty,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }
    }

    fn spec(empty_policy: EmptyBarPolicy) -> BarSpec {
        BarSpec {
            timeframe_ns: 10,
            alignment_offset_ns: 0,
            clock: BarClock::ExchangeTime,
            empty_policy,
            allowed_lateness_ns: 0,
        }
    }

    #[test]
    fn fixed_intervals_are_epoch_aligned_and_half_open() {
        let mut builder = CanonicalBarBuilder::new(spec(EmptyBarPolicy::Omit)).unwrap();
        let mut out = Vec::new();
        builder
            .push_trade(&trade(1, 1, 100.0, 2.0, true), &mut out)
            .unwrap();
        builder
            .push_trade(&trade(9, 9, 101.0, 1.0, false), &mut out)
            .unwrap();
        builder
            .push_trade(&trade(10, 10, 102.0, 3.0, true), &mut out)
            .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!((out[0].open_ts, out[0].close_ts), (0, 10));
        assert_eq!((out[0].open, out[0].close), (100.0, 101.0));
        assert_eq!(out[0].volume, 3.0);
        assert_eq!(out[0].buy_volume, 2.0);
        assert_eq!(builder.partial().unwrap().open_ts, 10);
    }

    #[test]
    fn missing_intervals_are_not_merged() {
        let mut builder = CanonicalBarBuilder::new(spec(EmptyBarPolicy::PreviousClose)).unwrap();
        let mut out = Vec::new();
        builder
            .push_trade(&trade(1, 1, 100.0, 1.0, true), &mut out)
            .unwrap();
        builder
            .push_trade(&trade(31, 31, 103.0, 1.0, true), &mut out)
            .unwrap();

        assert_eq!(out.len(), 3);
        assert_eq!(out[0].open_ts, 0);
        assert_eq!(out[1].open_ts, 10);
        assert_eq!(out[2].open_ts, 20);
        assert!(out[1].is_empty());
        assert_eq!(out[1].close, 100.0);
    }

    #[test]
    fn history_supports_negative_indices() {
        let mut history = BarHistory::new(2);
        for open_ts in [0, 10, 20] {
            history.push(Bar {
                open_ts,
                close_ts: open_ts + 10,
                ..Bar::default()
            });
        }
        assert_eq!(history.get(-1).unwrap().open_ts, 20);
        assert_eq!(history.get(-2).unwrap().open_ts, 10);
        assert_eq!(history.get(0).unwrap().open_ts, 10);
        assert!(history.get(-3).is_none());
    }

    #[test]
    fn offline_and_incremental_construction_are_identical() {
        let trades = [
            trade(1, 2, 100.0, 1.0, true),
            trade(9, 10, 101.0, 2.0, false),
            trade(10, 11, 99.0, 3.0, true),
            trade(35, 36, 105.0, 4.0, false),
        ];
        let spec = spec(EmptyBarPolicy::PreviousClose);

        let mut offline = CanonicalBarBuilder::new(spec).unwrap();
        let mut offline_out = Vec::new();
        for event in &trades {
            offline.push_trade(event, &mut offline_out).unwrap();
        }
        offline.finish(&mut offline_out);

        let mut streaming = CanonicalBarBuilder::new(spec).unwrap();
        let mut streaming_out = Vec::new();
        for chunk in trades.chunks(2) {
            for event in chunk {
                streaming.push_trade(event, &mut streaming_out).unwrap();
            }
        }
        streaming.finish(&mut streaming_out);

        assert_eq!(offline_out, streaming_out);
    }
}
