use std::collections::{BTreeMap, BTreeSet};

use super::execution::InstrumentId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionSource {
    Tick,
    Bar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingTickPolicy {
    Error,
    NoLiquidity,
    BarFallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutedExecutionSource {
    Tick,
    Bar,
    NoLiquidity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HybridRoutingError {
    #[error("instrument execution source is not configured")]
    MissingExecutionSource,
    #[error("tick execution was configured but the interval contains no Tick data")]
    MissingTickData,
    #[error("duplicate instrument execution-source configuration")]
    DuplicateInstrument,
}

/// Selects exactly one fill-producing source for every instrument. Bars may still be delivered as
/// signals when Tick is the execution source, but the Bar matcher is not authorized to fill.
pub struct HybridExecutionRouter {
    sources: BTreeMap<InstrumentId, ExecutionSource>,
    missing_tick_policy: MissingTickPolicy,
    tick_intervals: BTreeSet<(InstrumentId, i64, i64)>,
}

impl HybridExecutionRouter {
    pub fn new(missing_tick_policy: MissingTickPolicy) -> Self {
        Self {
            sources: BTreeMap::new(),
            missing_tick_policy,
            tick_intervals: BTreeSet::new(),
        }
    }

    pub fn configure(
        &mut self,
        instrument_id: InstrumentId,
        source: ExecutionSource,
    ) -> Result<(), HybridRoutingError> {
        if self.sources.insert(instrument_id, source).is_some() {
            return Err(HybridRoutingError::DuplicateInstrument);
        }
        Ok(())
    }

    pub fn mark_tick_interval(&mut self, instrument_id: InstrumentId, open_ts: i64, close_ts: i64) {
        self.tick_intervals
            .insert((instrument_id, open_ts, close_ts));
    }

    pub fn execution_for_interval(
        &self,
        instrument_id: InstrumentId,
        open_ts: i64,
        close_ts: i64,
    ) -> Result<RoutedExecutionSource, HybridRoutingError> {
        match self.sources.get(&instrument_id).copied() {
            Some(ExecutionSource::Bar) => Ok(RoutedExecutionSource::Bar),
            Some(ExecutionSource::Tick)
                if self
                    .tick_intervals
                    .contains(&(instrument_id, open_ts, close_ts)) =>
            {
                Ok(RoutedExecutionSource::Tick)
            }
            Some(ExecutionSource::Tick) => match self.missing_tick_policy {
                MissingTickPolicy::Error => Err(HybridRoutingError::MissingTickData),
                MissingTickPolicy::NoLiquidity => Ok(RoutedExecutionSource::NoLiquidity),
                MissingTickPolicy::BarFallback => Ok(RoutedExecutionSource::Bar),
            },
            None => Err(HybridRoutingError::MissingExecutionSource),
        }
    }

    pub fn reset(&mut self) {
        self.tick_intervals.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bar_signal_and_tick_execution_never_double_match() {
        let id = InstrumentId(1);
        let mut router = HybridExecutionRouter::new(MissingTickPolicy::Error);
        router.configure(id, ExecutionSource::Tick).unwrap();
        router.mark_tick_interval(id, 0, 60);
        assert_eq!(
            router.execution_for_interval(id, 0, 60).unwrap(),
            RoutedExecutionSource::Tick
        );
        assert_eq!(
            router.execution_for_interval(id, 60, 120),
            Err(HybridRoutingError::MissingTickData)
        );
        router.reset();
        assert_eq!(
            router.execution_for_interval(id, 0, 60),
            Err(HybridRoutingError::MissingTickData)
        );
    }
}
