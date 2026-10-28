//! Bounded host-owned L2 reconstruction. Unknown prices outside the snapshot's
//! covered range never become evidence that a requested Top-N is complete.
use std::collections::BTreeMap;

use crate::TitanDepthView;

pub const DEPTH_BATCH_CAPACITY: usize = 2_730;
pub const HOST_DEPTH_LEVELS: usize = 100;
const BOOK_CAPACITY: usize = 16_384;

/// Versioned event payload for HOST_DEPTH_100, deliberately separate from the
/// frozen V13 context/item layouts. One payload accompanies every Depth callback.
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
#[repr(C)]
pub struct DepthBatchInfoV1 {
    pub version: u32,
    pub asset_no: u32,
    pub stream_epoch: u64,
    pub first_update_sequence: u64,
    pub last_update_sequence: u64,
    pub publication_sequence: u64,
    pub valid: u8,
    pub reset: u8,
    pub reserved: [u8; 6],
}
const _: () = assert!(std::mem::size_of::<DepthBatchInfoV1>() == 48);

#[derive(Default)]
pub(crate) struct HostDepthBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    bid_floor: i64,
    ask_ceiling: i64,
    epoch: u64,
    publication_sequence: u64,
    sequence: u64,
    initialized: bool,
    pub ready: bool,
    pub snapshot_pending: bool,
}

impl HostDepthBook {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn stale(&self, epoch: u64, publication_sequence: u64) -> bool {
        epoch < self.epoch
            || (publication_sequence != 0 && publication_sequence <= self.publication_sequence)
    }
    pub fn invalidate_at(&mut self, epoch: u64, publication_sequence: u64) -> bool {
        if self.stale(epoch, publication_sequence) {
            return false;
        }
        self.invalidate(epoch);
        self.publication_sequence = self.publication_sequence.max(publication_sequence);
        true
    }
    pub fn invalidate(&mut self, epoch: u64) {
        self.epoch = self.epoch.max(epoch);
        self.initialized = false;
        self.ready = false;
        self.bids.clear();
        self.asks.clear();
    }

    pub fn apply(
        &mut self,
        info: &mut DepthBatchInfoV1,
        snapshot: bool,
        items: &[TitanDepthView],
        output: &mut Vec<TitanDepthView>,
    ) -> Result<bool, &'static str> {
        output.clear();
        info.valid = 0;
        info.reset = u8::from(snapshot);
        if self.stale(info.stream_epoch, info.publication_sequence) {
            return Ok(false);
        }
        self.publication_sequence = info.publication_sequence;
        if info.first_update_sequence > info.last_update_sequence {
            return Err("depth_sequence_range");
        }
        if snapshot {
            self.invalidate(info.stream_epoch);
            self.snapshot_pending = false;
            self.sequence = info.last_update_sequence;
        } else if !self.initialized || self.epoch != info.stream_epoch {
            self.invalidate(info.stream_epoch);
            return Ok(true);
        } else if info.last_update_sequence != 0 && info.last_update_sequence < self.sequence {
            return Err("depth_sequence_regressed");
        }
        for item in items {
            if item.price_ticks <= 0
                || item.qty_lots < 0
                || !matches!(item.side, 1 | 2)
                || !matches!(item.action, 1 | 2)
                || (item.action == 2 && item.qty_lots != 0)
            {
                return Err("depth_item_invalid");
            }
            // Outside the initial snapshot range there may be unchanged,
            // unobserved levels. Do not silently extend the trusted range.
            if !snapshot
                && ((item.side == 1 && item.price_ticks < self.bid_floor)
                    || (item.side == 2 && item.price_ticks > self.ask_ceiling))
            {
                continue;
            }
            let side = if item.side == 1 {
                &mut self.bids
            } else {
                &mut self.asks
            };
            if item.qty_lots == 0 {
                side.remove(&item.price_ticks);
            } else {
                side.insert(item.price_ticks, item.qty_lots);
            }
        }
        if self.bids.len() + self.asks.len() > BOOK_CAPACITY {
            self.invalidate(info.stream_epoch);
            return Err("depth_book_capacity");
        }
        if snapshot {
            self.bid_floor = self.bids.first_key_value().map_or(0, |(p, _)| *p);
            self.ask_ceiling = self.asks.last_key_value().map_or(0, |(p, _)| *p);
            self.initialized = true;
        }
        self.sequence = info.last_update_sequence;
        self.ready = self.bids.len() >= HOST_DEPTH_LEVELS
            && self.asks.len() >= HOST_DEPTH_LEVELS
            && self.bids.last_key_value().unwrap().0 < self.asks.first_key_value().unwrap().0;
        info.valid = u8::from(self.ready);
        if !self.ready {
            return Ok(true);
        }
        let sample = items.first().copied().unwrap_or_default();
        for (side, levels) in [
            (
                1,
                self.bids
                    .iter()
                    .rev()
                    .take(HOST_DEPTH_LEVELS)
                    .collect::<Vec<_>>(),
            ),
            (
                2,
                self.asks.iter().take(HOST_DEPTH_LEVELS).collect::<Vec<_>>(),
            ),
        ] {
            for (level, (price, qty)) in levels.into_iter().enumerate() {
                output.push(TitanDepthView {
                    asset_no: info.asset_no,
                    level: level as u32,
                    exchange_ts_ns: sample.exchange_ts_ns,
                    receive_ts_ns: sample.receive_ts_ns,
                    price_ticks: *price,
                    qty_lots: *qty,
                    source_sequence: info.last_update_sequence,
                    side,
                    action: 1,
                    is_snapshot: u8::from(snapshot),
                    reserved: [0; 5],
                });
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(n: i64) -> Vec<TitanDepthView> {
        (0..n)
            .flat_map(|i| [(1, 1000 - i), (2, 1001 + i)])
            .map(|(side, price)| TitanDepthView {
                side,
                price_ticks: price,
                qty_lots: 1,
                action: 1,
                ..Default::default()
            })
            .collect()
    }
    #[test]
    fn deletion_backfills_unchanged_level_and_coverage_exhaustion_invalidates() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            last_update_sequence: 10,
            ..Default::default()
        };
        let mut out = Vec::new();
        book.apply(&mut info, true, &snapshot(101), &mut out)
            .unwrap();
        assert_eq!(out.len(), 200);
        let delete = TitanDepthView {
            side: 1,
            action: 2,
            price_ticks: 1000,
            ..Default::default()
        };
        info.last_update_sequence = 11;
        book.apply(&mut info, false, &[delete], &mut out).unwrap();
        assert_eq!(out[99].price_ticks, 900);
        assert_eq!(info.valid, 1);
        book.apply(
            &mut info,
            false,
            &[TitanDepthView {
                price_ticks: 999,
                ..delete
            }],
            &mut out,
        )
        .unwrap();
        assert_eq!(info.valid, 0);
        assert!(out.is_empty());
    }
    #[test]
    fn resnapshot_removes_ghosts_and_old_epoch_cannot_restore_readiness() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        let mut out = Vec::new();
        book.apply(&mut info, true, &snapshot(101), &mut out)
            .unwrap();
        book.invalidate(2);
        book.apply(&mut info, true, &snapshot(101), &mut out)
            .unwrap();
        assert!(!book.ready);
        info.stream_epoch = 2;
        book.apply(&mut info, true, &snapshot(100), &mut out)
            .unwrap();
        assert!(!book.bids.contains_key(&900));
        assert!(book.ready);
    }
    #[test]
    fn small_snapshot_and_delta_before_snapshot_stay_invalid() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1::default();
        let mut out = Vec::new();
        book.apply(&mut info, false, &snapshot(101), &mut out)
            .unwrap();
        assert_eq!(info.valid, 0);
        book.apply(&mut info, true, &snapshot(20), &mut out)
            .unwrap();
        assert_eq!(info.valid, 0);
    }
    #[test]
    fn invalidation_watermark_rejects_queued_old_snapshot_and_delta() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            publication_sequence: 10,
            ..Default::default()
        };
        let mut out = Vec::new();
        book.apply(&mut info, true, &snapshot(101), &mut out)
            .unwrap();
        assert!(book.invalidate_at(1, 12));
        info.publication_sequence = 11;
        assert!(
            !book
                .apply(&mut info, true, &snapshot(101), &mut out)
                .unwrap()
        );
        assert!(!book.ready);
        info.publication_sequence = 13;
        info.stream_epoch = 2;
        assert!(
            book.apply(&mut info, true, &snapshot(101), &mut out)
                .unwrap()
        );
        assert!(!book.invalidate_at(1, 14));
        assert!(book.ready);
    }
    #[test]
    fn large_delta_preserves_untouched_prices_and_is_never_a_reset() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        let mut out = Vec::new();
        book.apply(&mut info, true, &snapshot(500), &mut out)
            .unwrap();
        let updates: Vec<_> = snapshot(300)
            .into_iter()
            .filter(|item| item.price_ticks != 1000)
            .collect();
        book.apply(&mut info, false, &updates, &mut out).unwrap();
        assert_eq!(info.reset, 0);
        assert_eq!(out[0].price_ticks, 1000);
        assert!(out.iter().all(|item| item.is_snapshot == 0));
    }
}
