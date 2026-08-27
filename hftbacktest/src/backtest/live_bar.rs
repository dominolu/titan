use std::collections::BTreeMap;

use crate::{
    backtest::execution::InstrumentId,
    market_data::{BAR_COMPLETE, BAR_EMPTY, BAR_SYNTHETIC, Bar},
};
use titan_runtime_abi::TimedBarItem;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveBarOrigin {
    Native,
    Canonical,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LiveBarKey {
    pub instrument_id: InstrumentId,
    pub timeframe_ns: i64,
    pub open_ts: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveBarDiagnosticKind {
    Duplicate,
    LateCorrection,
    LowerPriorityIgnored,
    LateTradeDropped,
    OutOfOrderTrade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveBarDiagnostic {
    pub key: LiveBarKey,
    pub kind: LiveBarDiagnosticKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmptyBarPolicy {
    Skip,
    Emit,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CanonicalTrade {
    pub event_ts: i64,
    pub price: f64,
    pub qty: f64,
    pub quote_qty: f64,
    pub buy_qty: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct BarAccumulator {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    quote_volume: f64,
    buy_volume: f64,
    trade_count: u64,
    first_ts: i64,
    last_ts: i64,
}

/// Watermark-based local live Bar builder. It accepts bounded out-of-order trades, finalizes only
/// buckets behind the watermark, and never mutates an already emitted complete Bar.
pub struct CanonicalLiveBarBuilder {
    instrument_id: InstrumentId,
    asset_no: u64,
    timeframe_ns: i64,
    allowed_lateness_ns: i64,
    empty_policy: EmptyBarPolicy,
    max_event_ts: i64,
    watermark_ts: i64,
    last_ingest_ts: i64,
    next_open_ts: Option<i64>,
    pending: BTreeMap<i64, BarAccumulator>,
    diagnostics: Vec<LiveBarDiagnostic>,
}

impl CanonicalLiveBarBuilder {
    pub fn new(
        instrument_id: InstrumentId,
        asset_no: u64,
        timeframe_ns: i64,
        allowed_lateness_ns: i64,
        empty_policy: EmptyBarPolicy,
    ) -> Result<Self, LiveBarError> {
        if timeframe_ns <= 0 || allowed_lateness_ns < 0 {
            return Err(LiveBarError::InvalidBar);
        }
        Ok(Self {
            instrument_id,
            asset_no,
            timeframe_ns,
            allowed_lateness_ns,
            empty_policy,
            max_event_ts: i64::MIN,
            watermark_ts: i64::MIN,
            last_ingest_ts: i64::MIN,
            next_open_ts: None,
            pending: BTreeMap::new(),
            diagnostics: Vec::new(),
        })
    }

    fn bucket_open(&self, event_ts: i64) -> i64 {
        event_ts.div_euclid(self.timeframe_ns) * self.timeframe_ns
    }

    fn key(&self, open_ts: i64) -> LiveBarKey {
        LiveBarKey {
            instrument_id: self.instrument_id,
            timeframe_ns: self.timeframe_ns,
            open_ts,
        }
    }

    pub fn ingest(&mut self, trade: CanonicalTrade) -> Result<bool, LiveBarError> {
        if !trade.price.is_finite()
            || trade.price <= 0.0
            || !trade.qty.is_finite()
            || trade.qty < 0.0
            || !trade.quote_qty.is_finite()
            || trade.quote_qty < 0.0
            || !trade.buy_qty.is_finite()
            || trade.buy_qty < 0.0
        {
            return Err(LiveBarError::InvalidBar);
        }
        let open_ts = self.bucket_open(trade.event_ts);
        if open_ts.saturating_add(self.timeframe_ns) <= self.watermark_ts {
            self.diagnostics.push(LiveBarDiagnostic {
                key: self.key(open_ts),
                kind: LiveBarDiagnosticKind::LateTradeDropped,
            });
            return Ok(false);
        }
        if trade.event_ts < self.last_ingest_ts {
            self.diagnostics.push(LiveBarDiagnostic {
                key: self.key(open_ts),
                kind: LiveBarDiagnosticKind::OutOfOrderTrade,
            });
        }
        self.last_ingest_ts = self.last_ingest_ts.max(trade.event_ts);
        self.max_event_ts = self.max_event_ts.max(trade.event_ts);
        self.watermark_ts = self.max_event_ts.saturating_sub(self.allowed_lateness_ns);
        self.next_open_ts = Some(self.next_open_ts.map_or(open_ts, |next| next.min(open_ts)));
        let bar = self.pending.entry(open_ts).or_insert(BarAccumulator {
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            first_ts: trade.event_ts,
            last_ts: trade.event_ts,
            ..Default::default()
        });
        bar.high = bar.high.max(trade.price);
        bar.low = bar.low.min(trade.price);
        if trade.event_ts < bar.first_ts {
            bar.first_ts = trade.event_ts;
            bar.open = trade.price;
        }
        if trade.event_ts >= bar.last_ts {
            bar.last_ts = trade.event_ts;
            bar.close = trade.price;
        }
        bar.volume += trade.qty;
        bar.quote_volume += trade.quote_qty;
        bar.buy_volume += trade.buy_qty;
        bar.trade_count += 1;
        Ok(true)
    }

    pub fn watermark_ts(&self) -> i64 {
        self.watermark_ts
    }

    pub fn drain_ready(&mut self, out: &mut Vec<TimedBarItem>) {
        let Some(mut open_ts) = self.next_open_ts else {
            return;
        };
        while open_ts.saturating_add(self.timeframe_ns) <= self.watermark_ts {
            if let Some(value) = self.pending.remove(&open_ts) {
                out.push(TimedBarItem {
                    asset_no: self.asset_no,
                    timeframe_ns: self.timeframe_ns,
                    bar: Bar {
                        open_ts,
                        close_ts: open_ts + self.timeframe_ns,
                        open: value.open,
                        high: value.high,
                        low: value.low,
                        close: value.close,
                        volume: value.volume,
                        quote_volume: value.quote_volume,
                        buy_volume: value.buy_volume,
                        trade_count: value.trade_count,
                        flags: BAR_COMPLETE | BAR_SYNTHETIC,
                    },
                });
            } else if self.empty_policy == EmptyBarPolicy::Emit {
                out.push(TimedBarItem {
                    asset_no: self.asset_no,
                    timeframe_ns: self.timeframe_ns,
                    bar: Bar {
                        open_ts,
                        close_ts: open_ts + self.timeframe_ns,
                        open: f64::NAN,
                        high: f64::NAN,
                        low: f64::NAN,
                        close: f64::NAN,
                        volume: 0.0,
                        quote_volume: 0.0,
                        buy_volume: 0.0,
                        trade_count: 0,
                        flags: BAR_COMPLETE | BAR_EMPTY | BAR_SYNTHETIC,
                    },
                });
            }
            open_ts = open_ts.saturating_add(self.timeframe_ns);
        }
        self.next_open_ts = Some(open_ts);
    }

    pub fn diagnostics(&self) -> &[LiveBarDiagnostic] {
        &self.diagnostics
    }

    pub fn reset(&mut self) {
        self.max_event_ts = i64::MIN;
        self.watermark_ts = i64::MIN;
        self.last_ingest_ts = i64::MIN;
        self.next_open_ts = None;
        self.pending.clear();
        self.diagnostics.clear();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
pub enum LiveBarError {
    #[error("live Bar must be complete and interval-valid")]
    InvalidBar,
}

#[derive(Clone, Copy, Debug)]
struct PendingLiveBar {
    record: TimedBarItem,
    origin: LiveBarOrigin,
}

/// Unifies native closed candles, local canonical Bars and REST recovery. A complete Bar is
/// delivered once through the normal runtime path and can never be silently rewritten.
pub struct RecoveringLiveBarSource {
    native_priority: u16,
    canonical_priority: u16,
    recovery_priority: u16,
    pending: BTreeMap<LiveBarKey, PendingLiveBar>,
    delivered: BTreeMap<LiveBarKey, TimedBarItem>,
    diagnostics: Vec<LiveBarDiagnostic>,
}

impl RecoveringLiveBarSource {
    pub fn new(native_priority: u16, canonical_priority: u16, recovery_priority: u16) -> Self {
        Self {
            native_priority,
            canonical_priority,
            recovery_priority,
            pending: BTreeMap::new(),
            delivered: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn priority(&self, origin: LiveBarOrigin) -> u16 {
        match origin {
            LiveBarOrigin::Native => self.native_priority,
            LiveBarOrigin::Canonical => self.canonical_priority,
            LiveBarOrigin::Recovery => self.recovery_priority,
        }
    }

    pub fn ingest(
        &mut self,
        instrument_id: InstrumentId,
        origin: LiveBarOrigin,
        record: TimedBarItem,
    ) -> Result<bool, LiveBarError> {
        if record.timeframe_ns <= 0
            || record.bar.close_ts - record.bar.open_ts != record.timeframe_ns
            || record.bar.flags & BAR_COMPLETE == 0
        {
            return Err(LiveBarError::InvalidBar);
        }
        let key = LiveBarKey {
            instrument_id,
            timeframe_ns: record.timeframe_ns,
            open_ts: record.bar.open_ts,
        };
        if let Some(delivered) = self.delivered.get(&key) {
            self.diagnostics.push(LiveBarDiagnostic {
                key,
                kind: if delivered == &record {
                    LiveBarDiagnosticKind::Duplicate
                } else {
                    LiveBarDiagnosticKind::LateCorrection
                },
            });
            return Ok(false);
        }
        if let Some(existing) = self.pending.get(&key).copied() {
            if existing.record == record {
                self.diagnostics.push(LiveBarDiagnostic {
                    key,
                    kind: LiveBarDiagnosticKind::Duplicate,
                });
                return Ok(false);
            }
            if self.priority(origin) >= self.priority(existing.origin) {
                self.diagnostics.push(LiveBarDiagnostic {
                    key,
                    kind: LiveBarDiagnosticKind::LowerPriorityIgnored,
                });
                return Ok(false);
            }
        }
        self.pending.insert(key, PendingLiveBar { record, origin });
        Ok(true)
    }

    pub fn drain_ready(&mut self, watermark_ts: i64, out: &mut Vec<TimedBarItem>) {
        let mut ready: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.record.bar.close_ts <= watermark_ts)
            .map(|(key, _)| *key)
            .collect();
        ready.sort_by_key(|key| {
            let record = self.pending[key].record;
            (record.bar.close_ts, record.timeframe_ns, record.asset_no)
        });
        for key in ready {
            let pending = self.pending.remove(&key).unwrap();
            self.delivered.insert(key, pending.record);
            out.push(pending.record);
        }
    }

    pub fn diagnostics(&self) -> &[LiveBarDiagnostic] {
        &self.diagnostics
    }

    pub fn reset(&mut self) {
        self.pending.clear();
        self.delivered.clear();
        self.diagnostics.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::{BAR_NATIVE, Bar};

    fn record(close: f64) -> TimedBarItem {
        TimedBarItem {
            asset_no: 0,
            timeframe_ns: 60,
            bar: Bar {
                open_ts: 0,
                close_ts: 60,
                open: 1.0,
                high: 2.0,
                low: 1.0,
                close,
                volume: 1.0,
                quote_volume: 1.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE | BAR_NATIVE,
            },
        }
    }

    #[test]
    fn recovery_deduplicates_and_never_rewrites_delivered_bar() {
        let mut source = RecoveringLiveBarSource::new(0, 1, 2);
        let id = InstrumentId(1);
        source
            .ingest(id, LiveBarOrigin::Recovery, record(1.5))
            .unwrap();
        source
            .ingest(id, LiveBarOrigin::Native, record(1.6))
            .unwrap();
        let mut out = Vec::new();
        source.drain_ready(60, &mut out);
        assert_eq!(out[0].bar.close, 1.6);
        assert!(
            !source
                .ingest(id, LiveBarOrigin::Recovery, record(1.7))
                .unwrap()
        );
        assert_eq!(
            source.diagnostics().last().unwrap().kind,
            LiveBarDiagnosticKind::LateCorrection
        );
    }

    #[test]
    fn canonical_builder_uses_watermark_accepts_bounded_reordering_and_emits_gaps() {
        let mut builder =
            CanonicalLiveBarBuilder::new(InstrumentId(4), 2, 60, 10, EmptyBarPolicy::Emit).unwrap();
        builder
            .ingest(CanonicalTrade {
                event_ts: 65,
                price: 101.0,
                qty: 1.0,
                quote_qty: 101.0,
                buy_qty: 1.0,
            })
            .unwrap();
        builder
            .ingest(CanonicalTrade {
                event_ts: 62,
                price: 100.0,
                qty: 2.0,
                quote_qty: 200.0,
                buy_qty: 0.0,
            })
            .unwrap();
        builder
            .ingest(CanonicalTrade {
                event_ts: 190,
                price: 103.0,
                qty: 1.0,
                quote_qty: 103.0,
                buy_qty: 1.0,
            })
            .unwrap();
        let mut out = Vec::new();
        builder.drain_ready(&mut out);
        assert_eq!(out.len(), 2);
        assert_eq!((out[0].bar.open, out[0].bar.close), (100.0, 101.0));
        assert_eq!(out[0].bar.volume, 3.0);
        assert!(out[1].bar.flags & BAR_EMPTY != 0);
        assert_eq!(
            builder.diagnostics()[0].kind,
            LiveBarDiagnosticKind::OutOfOrderTrade
        );
        assert!(
            !builder
                .ingest(CanonicalTrade {
                    event_ts: 70,
                    price: 99.0,
                    qty: 1.0,
                    quote_qty: 99.0,
                    buy_qty: 0.0,
                })
                .unwrap()
        );
        assert_eq!(
            builder.diagnostics().last().unwrap().kind,
            LiveBarDiagnosticKind::LateTradeDropped
        );
    }
}
