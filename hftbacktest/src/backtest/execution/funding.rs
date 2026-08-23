use super::{
    AccountDelta, AccountError, AccountReport, CurrencyId, ExchangeAccountState, InstrumentId,
    InstrumentSpec, InstrumentType, LocalAccountView, VenueId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingBoundary {
    BeforeSettlementEvents,
    AfterSettlementEvents,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingRoundingMode {
    Nearest,
    TowardZero,
    Floor,
    Ceil,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingPriceSource {
    Mark,
    Index,
    External(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingPositionSnapshot {
    BeforeSettlementEvents,
    AfterSettlementEvents,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingFormula {
    /// Linear notional is `qty * contract_size * price`; inverse is
    /// `qty * contract_size / price`.
    InstrumentNotional,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FundingRounding {
    pub increment: f64,
    pub mode: FundingRoundingMode,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FundingConfig {
    pub price_source: FundingPriceSource,
    pub position_snapshot: FundingPositionSnapshot,
    pub formula: FundingFormula,
    pub currency: CurrencyId,
    pub rounding: FundingRounding,
    pub boundary: FundingBoundary,
}

impl FundingConfig {
    pub fn stable_hash(self) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for value in [
            match self.price_source {
                FundingPriceSource::Mark => 1,
                FundingPriceSource::Index => 2,
                FundingPriceSource::External(id) => 0x1_0000_0000 | u64::from(id),
            },
            match self.position_snapshot {
                FundingPositionSnapshot::BeforeSettlementEvents => 1,
                FundingPositionSnapshot::AfterSettlementEvents => 2,
            },
            self.formula as u64,
            u64::from(self.currency.0),
            self.rounding.increment.to_bits(),
            self.rounding.mode as u64,
            self.boundary as u64,
        ] {
            hash ^= value;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FundingEvent {
    pub event_id: u64,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub currency: CurrencyId,
    pub publication_ts: i64,
    pub effective_ts: i64,
    pub settlement_ts: i64,
    pub rate: f64,
    pub mark_price: f64,
    pub boundary: FundingBoundary,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScheduledFunding {
    pub asset_no: u32,
    pub event: FundingEvent,
    pub delivery_ts: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FundingReport {
    pub event: FundingEvent,
    pub delivery_ts: i64,
    pub sequence: u64,
    pub position_qty: f64,
    /// Positive is a credit to the account; negative is a payment.
    pub amount: f64,
    pub account_report: AccountReport,
}

#[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
pub enum FundingError {
    #[error("funding timestamps are not publication <= effective <= settlement")]
    InvalidTimestamps,
    #[error("funding rate, price or rounding is invalid")]
    InvalidValue,
    #[error("funding event does not match instrument")]
    InstrumentMismatch,
    #[error(transparent)]
    Account(#[from] AccountError),
}

pub struct FundingEngine {
    config: FundingConfig,
    enforce_config: bool,
    settled_count: u64,
    total: f64,
}

impl FundingEngine {
    pub fn new(rounding: FundingRounding) -> Result<Self, FundingError> {
        let mut engine = Self::new_with_config(FundingConfig {
            price_source: FundingPriceSource::Mark,
            position_snapshot: FundingPositionSnapshot::BeforeSettlementEvents,
            formula: FundingFormula::InstrumentNotional,
            currency: CurrencyId(0),
            rounding,
            boundary: FundingBoundary::BeforeSettlementEvents,
        })?;
        engine.enforce_config = false;
        Ok(engine)
    }

    pub fn new_with_config(config: FundingConfig) -> Result<Self, FundingError> {
        let rounding = config.rounding;
        if !rounding.increment.is_finite() || rounding.increment <= 0.0 {
            return Err(FundingError::InvalidValue);
        }
        if !matches!(
            (config.position_snapshot, config.boundary),
            (
                FundingPositionSnapshot::BeforeSettlementEvents,
                FundingBoundary::BeforeSettlementEvents
            ) | (
                FundingPositionSnapshot::AfterSettlementEvents,
                FundingBoundary::AfterSettlementEvents
            )
        ) {
            return Err(FundingError::InvalidValue);
        }
        Ok(Self {
            config,
            enforce_config: true,
            settled_count: 0,
            total: 0.0,
        })
    }

    pub fn settle(
        &mut self,
        event: FundingEvent,
        position_qty: f64,
        spec: &InstrumentSpec,
        exchange: &mut ExchangeAccountState,
        delivery_ts: i64,
        sequence: u64,
    ) -> Result<FundingReport, FundingError> {
        if event.publication_ts > event.effective_ts
            || event.effective_ts > event.settlement_ts
            || delivery_ts < event.settlement_ts
        {
            return Err(FundingError::InvalidTimestamps);
        }
        if event.venue_id != spec.venue_id
            || event.instrument_id != spec.instrument_id
            || event.currency != spec.settlement_currency
        {
            return Err(FundingError::InstrumentMismatch);
        }
        // CurrencyId(0) preserves the legacy constructor while explicit configurations freeze
        // currency and boundary semantics before a run starts.
        if self.enforce_config
            && (event.currency != self.config.currency || event.boundary != self.config.boundary)
        {
            return Err(FundingError::InstrumentMismatch);
        }
        if !event.rate.is_finite()
            || !event.mark_price.is_finite()
            || event.mark_price <= 0.0
            || !position_qty.is_finite()
        {
            return Err(FundingError::InvalidValue);
        }
        let notional = match spec.instrument_type {
            InstrumentType::Spot
            | InstrumentType::LinearFuture
            | InstrumentType::LinearPerpetual => {
                position_qty * spec.contract_size * event.mark_price
            }
            InstrumentType::InverseFuture | InstrumentType::InversePerpetual => {
                position_qty * spec.contract_size / event.mark_price
            }
        };
        let raw = -notional * event.rate;
        let units = raw / self.config.rounding.increment;
        let rounded_units = match self.config.rounding.mode {
            FundingRoundingMode::Nearest => units.round(),
            FundingRoundingMode::TowardZero => units.trunc(),
            FundingRoundingMode::Floor => units.floor(),
            FundingRoundingMode::Ceil => units.ceil(),
        };
        let amount = rounded_units * self.config.rounding.increment;
        let delta = AccountDelta {
            instrument_id: spec.instrument_id,
            position_delta: 0.0,
            trade_qty: 0.0,
            trade_value: 0.0,
            currency: event.currency,
            cash_delta: 0.0,
            fee: 0.0,
            funding: amount,
            execution_price: 0.0,
            realized_pnl: 0.0,
        };
        let account_report =
            exchange.apply_and_report(delta, event.settlement_ts, delivery_ts, sequence)?;
        self.settled_count += 1;
        self.total += amount;
        Ok(FundingReport {
            event,
            delivery_ts,
            sequence,
            position_qty,
            amount,
            account_report,
        })
    }

    pub fn deliver(
        report: FundingReport,
        local: &mut LocalAccountView,
    ) -> Result<(), AccountError> {
        local.deliver(report.account_report)
    }

    pub fn total(&self) -> f64 {
        self.total
    }

    pub fn settled_count(&self) -> u64 {
        self.settled_count
    }

    pub fn config(&self) -> FundingConfig {
        self.config
    }

    pub fn reset(&mut self) {
        self.settled_count = 0;
        self.total = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(2),
            asset_no: 0,
            venue_id: VenueId(1),
            tick_size: 0.01,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(7),
            settlement_currency: CurrencyId(7),
            margin_currency: CurrencyId(7),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    #[test]
    fn settlement_updates_exchange_then_local_at_delivery_and_resets() {
        let mut engine = FundingEngine::new(FundingRounding {
            increment: 0.01,
            mode: FundingRoundingMode::Nearest,
        })
        .unwrap();
        let mut exchange = ExchangeAccountState::new(VenueId(1));
        let mut local = LocalAccountView::new(VenueId(1));
        let report = engine
            .settle(
                FundingEvent {
                    event_id: 8,
                    venue_id: VenueId(1),
                    instrument_id: InstrumentId(2),
                    currency: CurrencyId(7),
                    publication_ts: 80,
                    effective_ts: 90,
                    settlement_ts: 100,
                    rate: 0.001,
                    mark_price: 100.0,
                    boundary: FundingBoundary::BeforeSettlementEvents,
                },
                2.0,
                &spec(),
                &mut exchange,
                120,
                0,
            )
            .unwrap();
        assert_eq!(report.amount, -0.2);
        assert_eq!(exchange.account().funding(CurrencyId(7)), -0.2);
        assert_eq!(local.account().funding(CurrencyId(7)), 0.0);
        FundingEngine::deliver(report, &mut local).unwrap();
        assert_eq!(local.account().funding(CurrencyId(7)), -0.2);
        engine.reset();
        assert_eq!((engine.settled_count(), engine.total()), (0, 0.0));
    }

    #[test]
    fn explicit_configuration_freezes_sources_boundary_and_hash() {
        let config = FundingConfig {
            price_source: FundingPriceSource::Index,
            position_snapshot: FundingPositionSnapshot::AfterSettlementEvents,
            formula: FundingFormula::InstrumentNotional,
            currency: CurrencyId(7),
            rounding: FundingRounding {
                increment: 0.01,
                mode: FundingRoundingMode::Floor,
            },
            boundary: FundingBoundary::AfterSettlementEvents,
        };
        let engine = FundingEngine::new_with_config(config).unwrap();
        assert_eq!(engine.config(), config);
        assert_ne!(
            config.stable_hash(),
            FundingConfig {
                price_source: FundingPriceSource::Mark,
                ..config
            }
            .stable_hash()
        );
        assert!(
            FundingEngine::new_with_config(FundingConfig {
                position_snapshot: FundingPositionSnapshot::BeforeSettlementEvents,
                ..config
            })
            .is_err()
        );
    }
}
