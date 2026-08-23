use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct VenueId(pub u32);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct InstrumentId(pub u32);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct CurrencyId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentType {
    Spot,
    LinearFuture,
    InverseFuture,
    LinearPerpetual,
    InversePerpetual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CashFlowMode {
    /// Compatibility mode used by the frozen Tick golden: every fill exchanges full notional.
    LegacyNotional,
    /// Derivative mode: opening notional does not move cash; only realized PnL does.
    DerivativePnl,
}

/// Canonical execution constraints for one instrument.
#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentSpec {
    pub instrument_id: InstrumentId,
    pub asset_no: u32,
    pub venue_id: VenueId,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    pub min_notional: f64,
    pub contract_size: f64,
    pub price_currency: CurrencyId,
    pub settlement_currency: CurrencyId,
    pub margin_currency: CurrencyId,
    pub instrument_type: InstrumentType,
    pub cash_flow_mode: CashFlowMode,
    /// Version of the specification. Dynamic updates will carry a new version and effective time.
    pub version: u32,
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum InstrumentSpecError {
    #[error("{field} must be finite and positive")]
    InvalidPositiveField { field: &'static str },
    #[error("min_qty must not exceed max_qty")]
    InvalidQuantityRange,
    #[error("price must be finite and positive")]
    InvalidPrice,
    #[error("quantity must be finite and positive")]
    InvalidQuantity,
    #[error("price is not aligned to tick_size")]
    PricePrecision,
    #[error("quantity is not aligned to lot_size")]
    QuantityPrecision,
    #[error("quantity is below min_qty")]
    QuantityBelowMinimum,
    #[error("quantity exceeds max_qty")]
    QuantityAboveMaximum,
    #[error("order notional is below min_notional")]
    NotionalBelowMinimum,
}

impl InstrumentSpec {
    pub fn validate(&self) -> Result<(), InstrumentSpecError> {
        for (field, value) in [
            ("tick_size", self.tick_size),
            ("lot_size", self.lot_size),
            ("min_qty", self.min_qty),
            ("max_qty", self.max_qty),
            ("contract_size", self.contract_size),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(InstrumentSpecError::InvalidPositiveField { field });
            }
        }
        if !self.min_notional.is_finite() || self.min_notional < 0.0 {
            return Err(InstrumentSpecError::InvalidPositiveField {
                field: "min_notional",
            });
        }
        if self.min_qty > self.max_qty {
            return Err(InstrumentSpecError::InvalidQuantityRange);
        }
        Ok(())
    }

    /// Validates a limit-order price and quantity. Market orders use `validate_quantity` at submit
    /// time and validate notional against the actual execution price.
    pub fn validate_limit_order(&self, price: f64, qty: f64) -> Result<(), InstrumentSpecError> {
        self.validate()?;
        if !price.is_finite() || price <= 0.0 {
            return Err(InstrumentSpecError::InvalidPrice);
        }
        self.validate_quantity(qty)?;
        if !is_step_aligned(price, self.tick_size) {
            return Err(InstrumentSpecError::PricePrecision);
        }
        if price * qty * self.contract_size < self.min_notional {
            return Err(InstrumentSpecError::NotionalBelowMinimum);
        }
        Ok(())
    }

    pub fn validate_quantity(&self, qty: f64) -> Result<(), InstrumentSpecError> {
        if !qty.is_finite() || qty <= 0.0 {
            return Err(InstrumentSpecError::InvalidQuantity);
        }
        if !is_step_aligned(qty, self.lot_size) {
            return Err(InstrumentSpecError::QuantityPrecision);
        }
        if qty < self.min_qty {
            return Err(InstrumentSpecError::QuantityBelowMinimum);
        }
        if qty > self.max_qty {
            return Err(InstrumentSpecError::QuantityAboveMaximum);
        }
        Ok(())
    }
}

fn is_step_aligned(value: f64, step: f64) -> bool {
    let scaled = value / step;
    let tolerance = f64::EPSILON * scaled.abs().max(1.0) * 8.0;
    (scaled - scaled.round()).abs() <= tolerance
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(7),
            asset_no: 0,
            venue_id: VenueId(2),
            tick_size: 0.01,
            lot_size: 0.001,
            min_qty: 0.001,
            max_qty: 100.0,
            min_notional: 10.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    #[test]
    fn validates_price_quantity_and_notional() {
        let spec = spec();
        assert_eq!(spec.validate_limit_order(100.01, 0.1), Ok(()));
        assert_eq!(
            spec.validate_limit_order(100.005, 0.1),
            Err(InstrumentSpecError::PricePrecision)
        );
        assert_eq!(
            spec.validate_limit_order(100.0, 0.1005),
            Err(InstrumentSpecError::QuantityPrecision)
        );
        assert_eq!(
            spec.validate_limit_order(100.0, 0.01),
            Err(InstrumentSpecError::NotionalBelowMinimum)
        );
    }

    #[test]
    fn rejects_invalid_spec_before_order_validation() {
        let mut spec = spec();
        spec.tick_size = 0.0;
        assert_eq!(
            spec.validate_limit_order(100.0, 1.0),
            Err(InstrumentSpecError::InvalidPositiveField { field: "tick_size" })
        );
    }
}
