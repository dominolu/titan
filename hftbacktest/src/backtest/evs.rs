use std::mem;

use crate::utils::{AlignedArray, CACHE_LINE_SIZE};

#[derive(Clone, Copy)]
#[repr(C, align(32))]
pub struct EventIntent {
    pub timestamp: i64,
    pub asset_no: usize,
    pub kind: EventIntentKind,
}

/// This is constructed by using transmute in `EventSet::next`.
#[allow(dead_code)]
#[derive(Eq, PartialEq, Clone, Copy)]
#[repr(usize)]
pub enum EventIntentKind {
    LocalData = 0,
    LocalOrder = 1,
    ExchData = 2,
    ExchOrder = 3,
}

/// Manages the event timestamps to determine the next event to be processed.
pub struct EventSet {
    timestamp: AlignedArray<i64, CACHE_LINE_SIZE>,
}

impl EventSet {
    /// Constructs an instance of `EventSet`.
    pub fn new(num_assets: usize) -> Self {
        if num_assets == 0 {
            panic!();
        }
        let mut timestamp = AlignedArray::<i64, CACHE_LINE_SIZE>::new(num_assets * 4);
        for i in 0..(num_assets * 4) {
            timestamp[i] = i64::MAX;
        }
        Self { timestamp }
    }

    /// Returns the next event to be processed, which has the earliest timestamp.
    pub fn next(&self) -> Option<EventIntent> {
        let mut evst_no = 0;
        let mut timestamp = unsafe { *self.timestamp.get_unchecked(0) };
        for (i, &ev_timestamp) in self.timestamp[1..].iter().enumerate() {
            if ev_timestamp < timestamp {
                timestamp = ev_timestamp;
                evst_no = i + 1;
            }
        }
        // Returns None if no valid events are found.
        if timestamp == i64::MAX {
            return None;
        }
        let asset_no = evst_no >> 2;
        let kind = unsafe { mem::transmute::<usize, EventIntentKind>(evst_no & 3) };
        Some(EventIntent {
            timestamp,
            asset_no,
            kind,
        })
    }

    pub fn reset(&mut self) {
        self.timestamp.fill(i64::MAX);
    }

    #[inline]
    fn update(&mut self, evst_no: usize, timestamp: i64) {
        let item = unsafe { self.timestamp.get_unchecked_mut(evst_no) };
        *item = timestamp;
    }

    #[inline]
    pub fn update_local_data(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no, timestamp);
    }

    #[inline]
    pub fn update_local_order(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 1, timestamp);
    }

    #[inline]
    pub fn update_exch_data(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 2, timestamp);
    }

    #[inline]
    pub fn update_exch_order(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 3, timestamp);
    }

    #[inline]
    fn invalidate(&mut self, evst_no: usize) {
        let item = unsafe { self.timestamp.get_unchecked_mut(evst_no) };
        *item = i64::MAX;
    }

    #[inline]
    pub fn invalidate_local_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no);
    }

    #[inline]
    pub fn invalidate_exch_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no + 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_timestamp_order_is_asset_then_legacy_event_kind() {
        let mut events = EventSet::new(2);
        for asset_no in 0..2 {
            events.update_local_data(asset_no, 10);
            events.update_local_order(asset_no, 10);
            events.update_exch_data(asset_no, 10);
            events.update_exch_order(asset_no, 10);
        }

        let expected = [
            (0, EventIntentKind::LocalData),
            (0, EventIntentKind::LocalOrder),
            (0, EventIntentKind::ExchData),
            (0, EventIntentKind::ExchOrder),
            (1, EventIntentKind::LocalData),
            (1, EventIntentKind::LocalOrder),
            (1, EventIntentKind::ExchData),
            (1, EventIntentKind::ExchOrder),
        ];
        for (asset_no, kind) in expected {
            let event = events.next().unwrap();
            assert_eq!(event.asset_no, asset_no);
            assert!(event.kind == kind);
            events.invalidate(4 * asset_no + kind as usize);
        }
        assert!(events.next().is_none());
    }
}
