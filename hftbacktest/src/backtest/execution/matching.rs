use super::{ExecutionOrder, ExecutionReason};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProposedFill {
    pub exchange_ts: i64,
    pub price: f64,
    pub qty: f64,
    pub maker: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MatchOutcome {
    Accepted {
        exchange_ts: i64,
    },
    Rejected {
        exchange_ts: i64,
        reason: ExecutionReason,
    },
    Fill(ProposedFill),
    Canceled {
        exchange_ts: i64,
    },
    Expired {
        exchange_ts: i64,
    },
}

/// Reusable output buffer. One matching operation may emit multiple independent fills.
#[derive(Debug, Default)]
pub struct MatchOutcomeSink {
    outcomes: Vec<MatchOutcome>,
}

impl MatchOutcomeSink {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            outcomes: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, outcome: MatchOutcome) {
        self.outcomes.push(outcome);
    }

    pub fn as_slice(&self) -> &[MatchOutcome] {
        &self.outcomes
    }

    pub fn clear(&mut self) {
        self.outcomes.clear();
    }
}

/// Market-specific matching only. Implementations must not mutate accounts or call strategies.
pub trait MatchingModel<MarketEvent> {
    type Error;

    fn on_order(
        &mut self,
        order: &ExecutionOrder,
        exchange_ts: i64,
        out: &mut MatchOutcomeSink,
    ) -> Result<(), Self::Error>;

    fn on_market(
        &mut self,
        event: &MarketEvent,
        out: &mut MatchOutcomeSink,
    ) -> Result<(), Self::Error>;

    fn reset(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_preserves_every_partial_fill() {
        let mut sink = MatchOutcomeSink::with_capacity(2);
        sink.push(MatchOutcome::Fill(ProposedFill {
            exchange_ts: 1,
            price: 100.0,
            qty: 3.0,
            maker: false,
        }));
        sink.push(MatchOutcome::Fill(ProposedFill {
            exchange_ts: 1,
            price: 101.0,
            qty: 2.0,
            maker: false,
        }));
        assert_eq!(sink.as_slice().len(), 2);
        assert_ne!(sink.as_slice()[0], sink.as_slice()[1]);
    }
}
