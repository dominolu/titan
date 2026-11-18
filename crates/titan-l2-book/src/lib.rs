//! Host-owned L2 reconstruction.
//!
//! The book is shared by two readers with the same rules and different needs: the live strategy
//! runtime, which publishes a complete, coverage-checked Top-N, and the market data recorder, which
//! takes whichever levels it has to store a window per slot. Merging, sequence checking and the
//! coverage rule live here once; what a reader does with the result stays with the reader.

use std::collections::BTreeMap;

/// One depth update as the strategies' ABI sees it.
///
/// The layout is frozen: this struct is handed to compiled strategies through
/// `TitanDepthView`, so field order, widths and `reserved` are part of the ABI fingerprint.
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
#[repr(C)]
pub struct TitanDepthView {
    pub asset_no: u32,
    pub level: u32,
    pub exchange_ts_ns: i64,
    pub receive_ts_ns: i64,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub source_sequence: u64,
    pub side: u8,
    pub action: u8,
    pub is_snapshot: u8,
    pub reserved: [u8; 5],
}

/// Levels the live path publishes a side.
pub const HOST_DEPTH_LEVELS: usize = 100;

/// Versioned event payload for HOST_DEPTH_100, deliberately separate from the
/// frozen compiled strategy context/item layouts. One payload accompanies every Depth callback.
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

pub struct HostDepthBook {
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
    /// Levels a side must hold before the book is complete, and how many each side publishes.
    ///
    /// The live path asks for `HOST_DEPTH_LEVELS`; a recorder publishing a deeper window passes
    /// its own, so one book maintains every depth and the coverage rule stays the same.
    levels: usize,
    /// Whether an incomplete book publishes nothing (`Strict`, the live path) or publishes the
    /// levels it holds (`Lenient`, a recorder, which wants the traffic of a thin market too).
    publish_incomplete: bool,
}

impl Default for HostDepthBook {
    fn default() -> Self {
        Self::with_levels(HOST_DEPTH_LEVELS, false)
    }
}

impl HostDepthBook {
    /// A book publishing `levels` a side. `publish_incomplete` decides what an incomplete book
    /// does: `true` publishes what it holds, `false` publishes nothing and reports `ready = 0`.
    pub fn with_levels(levels: usize, publish_incomplete: bool) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            bid_floor: 0,
            ask_ceiling: 0,
            epoch: 0,
            publication_sequence: 0,
            sequence: 0,
            initialized: false,
            ready: false,
            snapshot_pending: false,
            levels: levels.max(1),
            publish_incomplete,
        }
    }

    /// The levels this book maintains a side.
    pub fn levels(&self) -> usize {
        self.levels
    }

    /// Applies one depth update from a market feed: `changes` are (side, price, quantity) triples
    /// with `side` 1 for bids and 2 for asks, a zero quantity removing the level.
    ///
    /// A recorder reads the book through `retained` rather than `apply`'s publication, and this
    /// keeps it off the ABI view layout: the sequence fields are the ones the feed reports, the
    /// publication sequence is the update's own so that ordering is checked by sequence alone.
    pub fn apply_changes(
        &mut self,
        epoch: u64,
        first_update_sequence: u64,
        last_update_sequence: u64,
        changes: &[(u8, i64, i64)],
    ) -> Result<bool, &'static str> {
        // The publication sequence counts publications, so this message is always newer than the
        // last one; the update's own sequence decides its order, and a regressed one is reported
        // rather than quietly dropped as stale.
        let publication_sequence = self.publication_sequence.saturating_add(1);
        let mut info = DepthBatchInfoV1 {
            version: 1,
            stream_epoch: epoch,
            first_update_sequence,
            last_update_sequence,
            publication_sequence,
            ..Default::default()
        };
        let items: Vec<TitanDepthView> = changes
            .iter()
            .map(|(side, price_ticks, qty_lots)| TitanDepthView {
                side: *side,
                price_ticks: *price_ticks,
                qty_lots: *qty_lots,
                action: 1,
                ..Default::default()
            })
            .collect();
        let mut publication = Vec::new();
        self.apply(&mut info, false, &items, &mut publication)
    }

    /// Installs a snapshot from the feed and checks it the way the live path does.
    pub fn apply_snapshot(
        &mut self,
        epoch: u64,
        last_update_sequence: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<bool, &'static str> {
        let publication_sequence = self.publication_sequence.saturating_add(1);
        let mut info = DepthBatchInfoV1 {
            version: 1,
            stream_epoch: epoch,
            first_update_sequence: last_update_sequence,
            last_update_sequence,
            publication_sequence,
            ..Default::default()
        };
        let items: Vec<TitanDepthView> = bids
            .iter()
            .map(|(price_ticks, qty_lots)| TitanDepthView {
                side: 1,
                price_ticks: *price_ticks,
                qty_lots: *qty_lots,
                action: 1,
                ..Default::default()
            })
            .chain(asks.iter().map(|(price_ticks, qty_lots)| TitanDepthView {
                side: 2,
                price_ticks: *price_ticks,
                qty_lots: *qty_lots,
                action: 1,
                ..Default::default()
            }))
            .collect();
        let mut publication = Vec::new();
        self.apply(&mut info, true, &items, &mut publication)
    }

    /// The retained bids and asks, deepest price first on both sides. A reader that wants its own
    /// view (a recorder taking the window to diff) reads them here instead of paying for a
    /// publication it would discard.
    pub fn retained(&self) -> (&BTreeMap<i64, i64>, &BTreeMap<i64, i64>) {
        (&self.bids, &self.asks)
    }
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
        if snapshot {
            self.bid_floor = self.bids.first_key_value().map_or(0, |(p, _)| *p);
            self.ask_ceiling = self.asks.last_key_value().map_or(0, |(p, _)| *p);
            self.initialized = true;
        }
        self.sequence = info.last_update_sequence;
        self.ready = self.bids.len() >= self.levels
            && self.asks.len() >= self.levels
            && self.bids.last_key_value().unwrap().0 < self.asks.first_key_value().unwrap().0;
        info.valid = u8::from(self.ready);
        // A book that publishes what it holds gives a thin market's levels to a reader that wants
        // them (a recorder); one that publishes only a complete window reports the gap instead.
        if !self.ready && !self.publish_incomplete {
            return Ok(true);
        }
        let sample = items.first().copied().unwrap_or_default();
        for (side, level, (price, qty)) in self
            .bids
            .iter()
            .rev()
            .take(self.levels)
            .enumerate()
            .map(|(level, value)| (1, level, value))
            .chain(
                self.asks
                    .iter()
                    .take(self.levels)
                    .enumerate()
                    .map(|(level, value)| (2, level, value)),
            )
        {
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
        // Keep every received price, but partial updates outside the snapshot
        // cannot prove that intervening, unchanged prices are known.
        // A complete book is only trustworthy where the snapshot covered it: an unchanged price
        // outside the snapshot's range is not known to be there.
        if self.ready {
            self.ready = output[self.levels - 1].price_ticks >= self.bid_floor
                && output[2 * self.levels - 1].price_ticks <= self.ask_ceiling;
        }
        info.valid = u8::from(self.ready);
        if !self.ready && !self.publish_incomplete {
            output.clear();
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
    fn feed_shaped_updates_reach_the_retained_book() {
        // The entry points a recorder uses: a snapshot, then an update, read back from `retained`
        // without paying for a publication it would discard.
        let mut book = HostDepthBook::with_levels(5, true);
        book.apply_snapshot(1, 100, &[(1000, 7), (999, 3)], &[(1001, 5)])
            .unwrap();
        assert_eq!(
            book.retained()
                .0
                .iter()
                .map(|(p, q)| (*p, *q))
                .collect::<Vec<_>>(),
            vec![(999, 3), (1000, 7)]
        );
        book.apply_changes(1, 101, 101, &[(1, 1000, 9), (2, 1001, 0), (1, 998, 4)])
            .unwrap();
        assert_eq!(
            book.retained()
                .0
                .iter()
                .map(|(p, q)| (*p, *q))
                .collect::<Vec<_>>(),
            vec![(998, 4), (999, 3), (1000, 9)]
        );
        assert!(
            book.retained().1.is_empty(),
            "a zero quantity removes the level"
        );
        // A sequence that regresses is refused the way the live path refuses it.
        assert_eq!(
            book.apply_changes(1, 50, 50, &[(1, 1000, 1)]),
            Err("depth_sequence_regressed")
        );
    }

    #[test]
    fn configured_levels_publish_a_deeper_window_than_the_live_path() {
        // A recorder publishes 500 a side; the coverage rule is the same one the live path uses.
        let mut book = HostDepthBook::with_levels(500, false);
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        let mut out = Vec::new();
        let items: Vec<_> = (0..600)
            .flat_map(|i| [(1, 10_000 - i), (2, 20_000 + i)])
            .map(|(side, price_ticks)| TitanDepthView {
                side,
                price_ticks,
                qty_lots: 1,
                action: 1,
                ..Default::default()
            })
            .collect();
        assert!(book.apply(&mut info, true, &items, &mut out).unwrap());
        assert!(book.ready);
        assert_eq!(out.len(), 1000, "500 levels a side are published");
        assert_eq!(out[499].price_ticks, 10_000 - 499);
        assert_eq!(out[500].side, 2);
        assert_eq!(book.levels(), 500);
    }

    #[test]
    fn a_thin_book_publishes_what_it_holds_only_when_asked_to() {
        // Ten levels a side, a book wanting a hundred: the live path publishes nothing and says
        // so; a recorder that asked for the levels it holds gets them.
        let items: Vec<_> = (0..10)
            .flat_map(|i| [(1, 1000 - i), (2, 1001 + i)])
            .map(|(side, price_ticks)| TitanDepthView {
                side,
                price_ticks,
                qty_lots: 2,
                action: 1,
                ..Default::default()
            })
            .collect();
        let mut strict = HostDepthBook::with_levels(100, false);
        let mut slenient = HostDepthBook::with_levels(100, true);
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        let mut out = Vec::new();
        strict.apply(&mut info, true, &items, &mut out).unwrap();
        assert!(!strict.ready);
        assert!(out.is_empty(), "an incomplete live book publishes nothing");
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        slenient.apply(&mut info, true, &items, &mut out).unwrap();
        assert!(!slenient.ready);
        assert_eq!(out.len(), 20, "a recorder still gets the ten levels a side");
        assert_eq!(slenient.retained().0.len(), 10);
    }

    #[test]
    fn large_book_and_outside_updates_are_retained_without_claiming_coverage() {
        let mut book = HostDepthBook::default();
        let mut info = DepthBatchInfoV1 {
            stream_epoch: 1,
            ..Default::default()
        };
        let mut out = Vec::with_capacity(200);
        let items: Vec<_> = (0..20_000)
            .flat_map(|i| [(1, 100_000 - i), (2, 100_001 + i)])
            .map(|(side, price_ticks)| TitanDepthView {
                side,
                price_ticks,
                qty_lots: 1,
                action: 1,
                ..Default::default()
            })
            .collect();
        book.apply(&mut info, true, &items, &mut out).unwrap();
        assert_eq!(book.bids.len() + book.asks.len(), 40_000);
        assert_eq!(info.valid, 1);
        assert_eq!(out.len(), 200);
        book.apply(&mut info, true, &snapshot(100), &mut out)
            .unwrap();
        let outside = TitanDepthView {
            side: 1,
            action: 1,
            price_ticks: 800,
            qty_lots: 3,
            ..Default::default()
        };
        book.apply(&mut info, false, &[outside], &mut out).unwrap();
        assert_eq!(book.bids.get(&800), Some(&3));
        assert_eq!(info.valid, 1);
        let delete = TitanDepthView {
            price_ticks: 1000,
            qty_lots: 0,
            action: 2,
            ..outside
        };
        book.apply(&mut info, false, &[delete], &mut out).unwrap();
        assert_eq!(info.valid, 0);
        assert!(out.is_empty());
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
