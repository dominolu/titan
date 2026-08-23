use std::collections::BTreeMap;

use super::{InstrumentId, InstrumentSpec};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketStatus {
    PreOpen,
    Open,
    Halted,
    Closed,
    Expired,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InstrumentEvent {
    SpecUpdate {
        effective_ts: i64,
        spec: InstrumentSpec,
    },
    MarketStatus {
        effective_ts: i64,
        instrument_id: InstrumentId,
        status: MarketStatus,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentEventError {
    #[error("instrument update version is not increasing")]
    StaleVersion,
    #[error("instrument update is not found")]
    UnknownInstrument,
}

#[derive(Default)]
pub struct InstrumentRegistry {
    initial_specs: BTreeMap<InstrumentId, InstrumentSpec>,
    initial_statuses: BTreeMap<InstrumentId, MarketStatus>,
    specs: BTreeMap<InstrumentId, InstrumentSpec>,
    statuses: BTreeMap<InstrumentId, MarketStatus>,
}

impl InstrumentRegistry {
    pub fn insert_initial(&mut self, spec: InstrumentSpec, status: MarketStatus) -> bool {
        let id = spec.instrument_id;
        self.initial_statuses.insert(id, status);
        self.initial_specs.insert(id, spec.clone());
        self.statuses.insert(id, status);
        self.specs.insert(id, spec).is_none()
    }

    pub fn apply(&mut self, event: InstrumentEvent) -> Result<(), InstrumentEventError> {
        match event {
            InstrumentEvent::SpecUpdate { spec, .. } => {
                let Some(current) = self.specs.get(&spec.instrument_id) else {
                    return Err(InstrumentEventError::UnknownInstrument);
                };
                if spec.version <= current.version {
                    return Err(InstrumentEventError::StaleVersion);
                }
                self.specs.insert(spec.instrument_id, spec);
            }
            InstrumentEvent::MarketStatus {
                instrument_id,
                status,
                ..
            } => {
                if !self.specs.contains_key(&instrument_id) {
                    return Err(InstrumentEventError::UnknownInstrument);
                }
                self.statuses.insert(instrument_id, status);
            }
        }
        Ok(())
    }

    pub fn spec(&self, id: InstrumentId) -> Option<&InstrumentSpec> {
        self.specs.get(&id)
    }

    pub fn status(&self, id: InstrumentId) -> Option<MarketStatus> {
        self.statuses.get(&id).copied()
    }

    pub fn reset(&mut self) {
        self.specs.clone_from(&self.initial_specs);
        self.statuses.clone_from(&self.initial_statuses);
    }
}

/// Deterministic cursor for timestamped specification and market-status changes. The global
/// scheduler supplies `now`; this component never advances an independent clock.
#[derive(Default)]
pub struct ScheduledInstrumentRegistry {
    registry: InstrumentRegistry,
    configured: Vec<InstrumentEvent>,
    cursor: usize,
}

impl ScheduledInstrumentRegistry {
    pub fn new(registry: InstrumentRegistry) -> Self {
        Self {
            registry,
            configured: Vec::new(),
            cursor: 0,
        }
    }

    pub fn schedule(&mut self, event: InstrumentEvent) {
        let timestamp = event.effective_ts();
        let instrument = event.instrument_id();
        let index = self
            .configured
            .iter()
            .position(|queued| {
                (queued.effective_ts(), queued.instrument_id()) > (timestamp, instrument)
            })
            .unwrap_or(self.configured.len());
        self.configured.insert(index, event);
    }

    pub fn next_timestamp(&self) -> Option<i64> {
        self.configured
            .get(self.cursor)
            .map(InstrumentEvent::effective_ts)
    }

    pub fn advance_to(&mut self, now: i64) -> Result<usize, InstrumentEventError> {
        let start = self.cursor;
        while self
            .configured
            .get(self.cursor)
            .is_some_and(|event| event.effective_ts() <= now)
        {
            self.registry.apply(self.configured[self.cursor].clone())?;
            self.cursor += 1;
        }
        Ok(self.cursor - start)
    }

    pub fn registry(&self) -> &InstrumentRegistry {
        &self.registry
    }

    pub fn reset(&mut self) {
        self.registry.reset();
        self.cursor = 0;
    }
}

impl InstrumentEvent {
    pub fn effective_ts(&self) -> i64 {
        match self {
            Self::SpecUpdate { effective_ts, .. } | Self::MarketStatus { effective_ts, .. } => {
                *effective_ts
            }
        }
    }

    pub fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::SpecUpdate { spec, .. } => spec.instrument_id,
            Self::MarketStatus { instrument_id, .. } => *instrument_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::execution::{CashFlowMode, CurrencyId, InstrumentType, VenueId};

    fn spec(version: u32) -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(1),
            asset_no: 0,
            venue_id: VenueId(1),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 10.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: CashFlowMode::DerivativePnl,
            version,
        }
    }

    #[test]
    fn timestamped_updates_are_versioned_and_reset_to_frozen_configuration() {
        let mut registry = InstrumentRegistry::default();
        assert!(registry.insert_initial(spec(1), MarketStatus::Open));
        registry
            .apply(InstrumentEvent::MarketStatus {
                effective_ts: 10,
                instrument_id: InstrumentId(1),
                status: MarketStatus::Halted,
            })
            .unwrap();
        registry
            .apply(InstrumentEvent::SpecUpdate {
                effective_ts: 11,
                spec: spec(2),
            })
            .unwrap();
        assert_eq!(registry.status(InstrumentId(1)), Some(MarketStatus::Halted));
        assert_eq!(registry.spec(InstrumentId(1)).unwrap().version, 2);
        registry.reset();
        assert_eq!(registry.status(InstrumentId(1)), Some(MarketStatus::Open));
        assert_eq!(registry.spec(InstrumentId(1)).unwrap().version, 1);
    }

    #[test]
    fn scheduled_status_changes_use_global_time_and_replay_after_reset() {
        let mut registry = InstrumentRegistry::default();
        registry.insert_initial(spec(1), MarketStatus::Open);
        let mut scheduled = ScheduledInstrumentRegistry::new(registry);
        scheduled.schedule(InstrumentEvent::SpecUpdate {
            effective_ts: 20,
            spec: spec(2),
        });
        scheduled.schedule(InstrumentEvent::MarketStatus {
            effective_ts: 10,
            instrument_id: InstrumentId(1),
            status: MarketStatus::Halted,
        });
        assert_eq!(scheduled.next_timestamp(), Some(10));
        assert_eq!(scheduled.advance_to(15).unwrap(), 1);
        assert_eq!(
            scheduled.registry().status(InstrumentId(1)),
            Some(MarketStatus::Halted)
        );
        assert_eq!(scheduled.advance_to(20).unwrap(), 1);
        assert_eq!(
            scheduled.registry().spec(InstrumentId(1)).unwrap().version,
            2
        );
        scheduled.reset();
        assert_eq!(scheduled.next_timestamp(), Some(10));
        assert_eq!(
            scheduled.registry().status(InstrumentId(1)),
            Some(MarketStatus::Open)
        );
    }
}
