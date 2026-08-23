use thiserror::Error;

use super::{CurrencyId, InstrumentId, PositionLedger, VenueId};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DenseCurrencyLedger {
    pub balance: f64,
    pub fee: f64,
    pub funding: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DenseAccountDelta {
    pub asset_no: usize,
    pub currency_slot: usize,
    pub position_delta: f64,
    pub trade_qty: f64,
    pub trade_value: f64,
    pub cash_delta: f64,
    pub fee: f64,
    pub funding: f64,
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum DenseAccountError {
    #[error("asset slot is out of range")]
    InvalidAssetSlot,
    #[error("currency slot is out of range")]
    InvalidCurrencySlot,
    #[error("dense account delta contains a non-finite value")]
    NonFinite,
    #[error("trade quantity or value is negative")]
    NegativeTrade,
    #[error("instrument is not registered at the requested asset slot")]
    InstrumentMismatch,
    #[error("currency is not registered")]
    CurrencyNotFound,
}

/// Cache-friendly venue account for execution hot paths.
///
/// Instrument access uses `asset_no` directly and the common single-currency case takes one
/// branch, avoiding maps, strings, allocations and dynamic dispatch per fill.
#[derive(Clone, Debug, PartialEq)]
pub struct DenseVenueAccount {
    venue_id: VenueId,
    instrument_ids: Vec<Option<InstrumentId>>,
    positions: Vec<PositionLedger>,
    currency_ids: Vec<CurrencyId>,
    currencies: Vec<DenseCurrencyLedger>,
}

impl DenseVenueAccount {
    pub fn new(
        venue_id: VenueId,
        instruments: &[(usize, InstrumentId)],
        currency_ids: &[CurrencyId],
    ) -> Result<Self, DenseAccountError> {
        let num_assets = instruments
            .iter()
            .map(|(asset_no, _)| asset_no + 1)
            .max()
            .unwrap_or(0);
        let mut instrument_ids = vec![None; num_assets];
        for &(asset_no, instrument_id) in instruments {
            if instrument_ids[asset_no].replace(instrument_id).is_some() {
                return Err(DenseAccountError::InstrumentMismatch);
            }
        }
        let mut deduped_currencies = currency_ids.to_vec();
        deduped_currencies.sort_unstable();
        deduped_currencies.dedup();
        Ok(Self {
            venue_id,
            positions: vec![PositionLedger::default(); num_assets],
            instrument_ids,
            currencies: vec![DenseCurrencyLedger::default(); deduped_currencies.len()],
            currency_ids: deduped_currencies,
        })
    }

    pub const fn venue_id(&self) -> VenueId {
        self.venue_id
    }

    pub fn currency_slot(&self, currency_id: CurrencyId) -> Result<usize, DenseAccountError> {
        if self.currency_ids.len() == 1 && self.currency_ids[0] == currency_id {
            return Ok(0);
        }
        self.currency_ids
            .binary_search(&currency_id)
            .map_err(|_| DenseAccountError::CurrencyNotFound)
    }

    pub fn instrument_id(&self, asset_no: usize) -> Option<InstrumentId> {
        self.instrument_ids.get(asset_no).copied().flatten()
    }

    pub fn position(&self, asset_no: usize) -> Option<&PositionLedger> {
        self.positions.get(asset_no)
    }

    pub fn currency(&self, currency_slot: usize) -> Option<&DenseCurrencyLedger> {
        self.currencies.get(currency_slot)
    }

    #[inline]
    pub fn apply(&mut self, delta: DenseAccountDelta) -> Result<(), DenseAccountError> {
        if delta.asset_no >= self.positions.len() {
            return Err(DenseAccountError::InvalidAssetSlot);
        }
        if delta.currency_slot >= self.currencies.len() {
            return Err(DenseAccountError::InvalidCurrencySlot);
        }
        if self.instrument_ids[delta.asset_no].is_none() {
            return Err(DenseAccountError::InstrumentMismatch);
        }
        if ![
            delta.position_delta,
            delta.trade_qty,
            delta.trade_value,
            delta.cash_delta,
            delta.fee,
            delta.funding,
        ]
        .iter()
        .all(|value| value.is_finite())
        {
            return Err(DenseAccountError::NonFinite);
        }
        if delta.trade_qty < 0.0 || delta.trade_value < 0.0 {
            return Err(DenseAccountError::NegativeTrade);
        }

        // Bounds were checked above. Keeping the two independent arrays makes the common fill
        // update compact while avoiding a hash/tree lookup.
        let position = &mut self.positions[delta.asset_no];
        position.qty += delta.position_delta;
        if delta.trade_qty > 0.0 {
            position.num_trades += 1;
            position.trading_volume += delta.trade_qty;
            position.trading_value += delta.trade_value;
        }
        let currency = &mut self.currencies[delta.currency_slot];
        currency.balance += delta.cash_delta - delta.fee + delta.funding;
        currency.fee += delta.fee;
        currency.funding += delta.funding;
        Ok(())
    }

    pub fn reset(&mut self) {
        self.positions.fill(PositionLedger::default());
        self.currencies.fill(DenseCurrencyLedger::default());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_currency_ledger_without_map_lookup_per_instrument() {
        let mut account = DenseVenueAccount::new(
            VenueId(1),
            &[(0, InstrumentId(10)), (1, InstrumentId(11))],
            &[CurrencyId(7)],
        )
        .unwrap();
        let currency_slot = account.currency_slot(CurrencyId(7)).unwrap();
        for (asset_no, position_delta, cash_delta) in
            [(0, 2.0_f64, -200.0_f64), (1, -1.0_f64, 110.0_f64)]
        {
            account
                .apply(DenseAccountDelta {
                    asset_no,
                    currency_slot,
                    position_delta,
                    trade_qty: position_delta.abs(),
                    trade_value: cash_delta.abs(),
                    cash_delta,
                    fee: 0.1,
                    funding: 0.0,
                })
                .unwrap();
        }
        assert_eq!(account.position(0).unwrap().qty, 2.0);
        assert_eq!(account.position(1).unwrap().qty, -1.0);
        assert!((account.currency(0).unwrap().balance + 90.2).abs() < 1e-12);
        assert!((account.currency(0).unwrap().fee - 0.2).abs() < 1e-12);
    }

    #[test]
    fn reset_preserves_layout_and_clears_values() {
        let mut account =
            DenseVenueAccount::new(VenueId(1), &[(3, InstrumentId(10))], &[CurrencyId(7)]).unwrap();
        account
            .apply(DenseAccountDelta {
                asset_no: 3,
                currency_slot: 0,
                position_delta: 1.0,
                trade_qty: 1.0,
                trade_value: 1.0,
                cash_delta: -1.0,
                fee: 0.0,
                funding: 0.0,
            })
            .unwrap();
        account.reset();
        assert_eq!(account.instrument_id(3), Some(InstrumentId(10)));
        assert_eq!(account.position(3), Some(&PositionLedger::default()));
        assert_eq!(account.currency_slot(CurrencyId(7)), Ok(0));
    }
}
