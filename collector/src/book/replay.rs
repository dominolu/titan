use std::collections::BTreeMap;

use super::{BookError, Change, Level, Side};

/// Compact replay-side price ladder. The v1 recorder stores integer ticks and
/// quantities, so reconstruction never depends on floating point arithmetic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ladder {
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
}

impl Ladder {
    pub fn from_snapshot(bids: &[Level], asks: &[Level]) -> Result<Self, BookError> {
        let mut ladder = Self::default();
        for level in bids {
            ladder.insert(Side::Bid, level.tick, level.qty)?;
        }
        for level in asks {
            ladder.insert(Side::Ask, level.tick, level.qty)?;
        }
        Ok(ladder)
    }

    pub fn apply(&mut self, changes: &[Change]) -> Result<(), BookError> {
        for change in changes {
            let book = match change.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if change.qty == 0 {
                if book.remove(&change.tick).is_none() {
                    return Err(BookError::InvalidField("delete missing level"));
                }
            } else {
                book.insert(change.tick, change.qty);
            }
        }
        Ok(())
    }

    pub fn top_n(&self, side: Side, n: usize) -> Vec<Level> {
        match side {
            Side::Bid => self
                .bids
                .iter()
                .rev()
                .take(n)
                .map(|(&tick, &qty)| Level { tick, qty })
                .collect(),
            Side::Ask => self
                .asks
                .iter()
                .take(n)
                .map(|(&tick, &qty)| Level { tick, qty })
                .collect(),
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .last_key_value()
            .map(|(&tick, &qty)| Level { tick, qty })
    }
    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .first_key_value()
            .map(|(&tick, &qty)| Level { tick, qty })
    }

    fn insert(&mut self, side: Side, tick: i64, qty: u64) -> Result<(), BookError> {
        if qty == 0 {
            return Err(BookError::InvalidField("snapshot quantity"));
        }
        match side {
            Side::Bid => self.bids.insert(tick, qty),
            Side::Ask => self.asks.insert(tick, qty),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_level_falls_back_after_delete() {
        let mut ladder = Ladder::from_snapshot(
            &[Level { tick: 100, qty: 2 }, Level { tick: 99, qty: 3 }],
            &[Level { tick: 101, qty: 4 }, Level { tick: 102, qty: 5 }],
        )
        .unwrap();
        ladder
            .apply(&[
                Change {
                    side: Side::Bid,
                    tick: 100,
                    qty: 0,
                },
                Change {
                    side: Side::Ask,
                    tick: 101,
                    qty: 0,
                },
            ])
            .unwrap();
        assert_eq!(ladder.best_bid(), Some(Level { tick: 99, qty: 3 }));
        assert_eq!(ladder.best_ask(), Some(Level { tick: 102, qty: 5 }));
        assert_eq!(ladder.top_n(Side::Bid, 1), vec![Level { tick: 99, qty: 3 }]);
    }

    #[test]
    fn deleting_unknown_price_is_rejected() {
        let mut ladder = Ladder::default();
        assert!(
            ladder
                .apply(&[Change {
                    side: Side::Bid,
                    tick: 9,
                    qty: 0
                }])
                .is_err()
        );
    }
}
