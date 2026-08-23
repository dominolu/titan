use std::collections::BTreeMap;

use thiserror::Error;

use super::{CurrencyId, InstrumentId, VenueId};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PositionLedger {
    pub qty: f64,
    pub avg_entry_price: f64,
    pub realized_pnl: f64,
    pub num_trades: u64,
    pub trading_volume: f64,
    pub trading_value: f64,
}

/// A precomputed, replayable account mutation produced exactly once at exchange event time.
///
/// `cash_delta` is the trade cash flow before fees and funding. A positive fee is a cost, a
/// negative fee is a rebate, and positive funding is a credit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AccountDelta {
    pub instrument_id: InstrumentId,
    pub position_delta: f64,
    pub trade_qty: f64,
    pub trade_value: f64,
    pub currency: CurrencyId,
    pub cash_delta: f64,
    pub fee: f64,
    pub funding: f64,
    pub execution_price: f64,
    pub realized_pnl: f64,
}

impl AccountDelta {
    pub const fn zero(instrument_id: InstrumentId, currency: CurrencyId) -> Self {
        Self {
            instrument_id,
            position_delta: 0.0,
            trade_qty: 0.0,
            trade_value: 0.0,
            currency,
            cash_delta: 0.0,
            fee: 0.0,
            funding: 0.0,
            execution_price: 0.0,
            realized_pnl: 0.0,
        }
    }

    fn validate(self) -> Result<(), AccountError> {
        for (field, value) in [
            ("position_delta", self.position_delta),
            ("trade_qty", self.trade_qty),
            ("trade_value", self.trade_value),
            ("cash_delta", self.cash_delta),
            ("fee", self.fee),
            ("funding", self.funding),
            ("execution_price", self.execution_price),
            ("realized_pnl", self.realized_pnl),
        ] {
            if !value.is_finite() {
                return Err(AccountError::NonFinite { field });
            }
        }
        if self.trade_qty < 0.0 {
            return Err(AccountError::NegativeTradeQuantity);
        }
        if self.trade_value < 0.0 {
            return Err(AccountError::NegativeTradeValue);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AccountReport {
    pub venue_id: VenueId,
    pub exchange_ts: i64,
    pub delivery_ts: i64,
    pub sequence: u64,
    pub delta: AccountDelta,
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum AccountError {
    #[error("account delta field {field} must be finite")]
    NonFinite { field: &'static str },
    #[error("trade quantity must not be negative")]
    NegativeTradeQuantity,
    #[error("trade value must not be negative")]
    NegativeTradeValue,
    #[error("account report venue does not match account venue")]
    VenueMismatch,
    #[error("account report delivery time precedes exchange time")]
    InvalidDeliveryTime,
}

/// Account ledger shared by every instrument belonging to one venue/account.
#[derive(Clone, Debug, PartialEq)]
pub struct VenueAccount {
    venue_id: VenueId,
    balances: BTreeMap<CurrencyId, f64>,
    fees: BTreeMap<CurrencyId, f64>,
    funding: BTreeMap<CurrencyId, f64>,
    positions: BTreeMap<InstrumentId, PositionLedger>,
}

impl VenueAccount {
    pub fn new(venue_id: VenueId) -> Self {
        Self {
            venue_id,
            balances: BTreeMap::new(),
            fees: BTreeMap::new(),
            funding: BTreeMap::new(),
            positions: BTreeMap::new(),
        }
    }

    pub const fn venue_id(&self) -> VenueId {
        self.venue_id
    }

    pub fn set_balance(&mut self, currency: CurrencyId, value: f64) -> Result<(), AccountError> {
        if !value.is_finite() {
            return Err(AccountError::NonFinite { field: "balance" });
        }
        self.balances.insert(currency, value);
        Ok(())
    }

    pub fn balance(&self, currency: CurrencyId) -> f64 {
        self.balances.get(&currency).copied().unwrap_or(0.0)
    }

    pub fn fee(&self, currency: CurrencyId) -> f64 {
        self.fees.get(&currency).copied().unwrap_or(0.0)
    }

    pub fn funding(&self, currency: CurrencyId) -> f64 {
        self.funding.get(&currency).copied().unwrap_or(0.0)
    }

    pub fn position(&self, instrument_id: InstrumentId) -> PositionLedger {
        self.positions
            .get(&instrument_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn apply(&mut self, delta: AccountDelta) -> Result<(), AccountError> {
        delta.validate()?;

        let position = self.positions.entry(delta.instrument_id).or_default();
        let old_qty = position.qty;
        let new_qty = old_qty + delta.position_delta;
        if delta.trade_qty > 0.0 {
            if old_qty == 0.0 || old_qty.signum() == delta.position_delta.signum() {
                let old_abs = old_qty.abs();
                let added = delta.position_delta.abs();
                position.avg_entry_price = if old_abs + added > 0.0 {
                    (position.avg_entry_price * old_abs + delta.execution_price * added)
                        / (old_abs + added)
                } else {
                    0.0
                };
            } else if new_qty == 0.0 {
                position.avg_entry_price = 0.0;
            } else if new_qty.signum() != old_qty.signum() {
                position.avg_entry_price = delta.execution_price;
            }
        }
        position.qty += delta.position_delta;
        position.realized_pnl += delta.realized_pnl;
        if delta.trade_qty > 0.0 {
            position.num_trades += 1;
            position.trading_volume += delta.trade_qty;
            position.trading_value += delta.trade_value;
        }

        *self.balances.entry(delta.currency).or_default() +=
            delta.cash_delta - delta.fee + delta.funding;
        *self.fees.entry(delta.currency).or_default() += delta.fee;
        *self.funding.entry(delta.currency).or_default() += delta.funding;
        Ok(())
    }

    pub fn reset(&mut self) {
        self.balances.clear();
        self.fees.clear();
        self.funding.clear();
        self.positions.clear();
    }
}

/// Venue state used by exchange-side checks immediately after an exchange event.
#[derive(Clone, Debug, PartialEq)]
pub struct ExchangeAccountState(VenueAccount);

impl ExchangeAccountState {
    pub fn new(venue_id: VenueId) -> Self {
        Self(VenueAccount::new(venue_id))
    }

    pub fn account(&self) -> &VenueAccount {
        &self.0
    }

    /// Seeds frozen run-start collateral before any delayed report is projected.
    pub fn seed_balance(&mut self, currency: CurrencyId, balance: f64) -> Result<(), AccountError> {
        self.0.set_balance(currency, balance)
    }

    pub fn account_mut(&mut self) -> &mut VenueAccount {
        &mut self.0
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }

    /// Applies exchange state now and returns the immutable delta to deliver locally later.
    pub fn apply_and_report(
        &mut self,
        delta: AccountDelta,
        exchange_ts: i64,
        delivery_ts: i64,
        sequence: u64,
    ) -> Result<AccountReport, AccountError> {
        if delivery_ts < exchange_ts {
            return Err(AccountError::InvalidDeliveryTime);
        }
        self.0.apply(delta)?;
        Ok(AccountReport {
            venue_id: self.0.venue_id,
            exchange_ts,
            delivery_ts,
            sequence,
            delta,
        })
    }
}

/// Strategy-visible venue state. It changes only when a report reaches local delivery time.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalAccountView(VenueAccount);

impl LocalAccountView {
    pub fn new(venue_id: VenueId) -> Self {
        Self(VenueAccount::new(venue_id))
    }

    pub fn account(&self) -> &VenueAccount {
        &self.0
    }

    /// Seeds frozen run-start collateral before any delayed report is projected.
    pub fn seed_balance(&mut self, currency: CurrencyId, balance: f64) -> Result<(), AccountError> {
        self.0.set_balance(currency, balance)
    }

    pub fn deliver(&mut self, report: AccountReport) -> Result<(), AccountError> {
        if report.venue_id != self.0.venue_id {
            return Err(AccountError::VenueMismatch);
        }
        if report.delivery_ts < report.exchange_ts {
            return Err(AccountError::InvalidDeliveryTime);
        }
        self.0.apply(report.delta)
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }
}

/// Local-visible portfolio aggregated by venue. It never reads exchange-only state.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PortfolioLedger {
    venues: BTreeMap<VenueId, LocalAccountView>,
}

/// Exchange-authoritative accounts keyed by Venue. This is the single cross-instrument owner
/// consumed by venue risk and funding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExchangePortfolio {
    venues: BTreeMap<VenueId, ExchangeAccountState>,
}

impl ExchangePortfolio {
    pub fn venue(&self, venue_id: VenueId) -> Option<&ExchangeAccountState> {
        self.venues.get(&venue_id)
    }

    pub fn venue_mut_or_insert(&mut self, venue_id: VenueId) -> &mut ExchangeAccountState {
        self.venues
            .entry(venue_id)
            .or_insert_with(|| ExchangeAccountState::new(venue_id))
    }

    pub fn apply(&mut self, venue_id: VenueId, delta: AccountDelta) -> Result<(), AccountError> {
        self.venue_mut_or_insert(venue_id)
            .account_mut()
            .apply(delta)
    }

    pub fn reset(&mut self) {
        self.venues.clear();
    }
}

impl PortfolioLedger {
    pub fn insert(&mut self, account: LocalAccountView) -> Option<LocalAccountView> {
        self.venues.insert(account.account().venue_id(), account)
    }

    pub fn venue(&self, venue_id: VenueId) -> Option<&LocalAccountView> {
        self.venues.get(&venue_id)
    }

    pub fn venue_mut_or_insert(&mut self, venue_id: VenueId) -> &mut LocalAccountView {
        self.venues
            .entry(venue_id)
            .or_insert_with(|| LocalAccountView::new(venue_id))
    }

    pub fn deliver(&mut self, report: AccountReport) -> Result<(), AccountError> {
        let account = self
            .venues
            .entry(report.venue_id)
            .or_insert_with(|| LocalAccountView::new(report.venue_id));
        account.deliver(report)
    }

    pub fn total_balance(&self, currency: CurrencyId) -> f64 {
        self.venues
            .values()
            .map(|account| account.account().balance(currency))
            .sum()
    }

    pub fn reset(&mut self) {
        self.venues.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill_delta(instrument_id: InstrumentId, currency: CurrencyId) -> AccountDelta {
        AccountDelta {
            instrument_id,
            position_delta: 2.0,
            trade_qty: 2.0,
            trade_value: 200.0,
            currency,
            cash_delta: -200.0,
            fee: 0.2,
            funding: 0.0,
            execution_price: 100.0,
            realized_pnl: 0.0,
        }
    }

    #[test]
    fn exchange_changes_before_local_delivery() {
        let venue = VenueId(1);
        let instrument = InstrumentId(10);
        let currency = CurrencyId(7);
        let mut exchange = ExchangeAccountState::new(venue);
        let mut local = LocalAccountView::new(venue);

        let report = exchange
            .apply_and_report(fill_delta(instrument, currency), 100, 150, 0)
            .unwrap();

        assert_eq!(exchange.account().position(instrument).qty, 2.0);
        assert_eq!(exchange.account().balance(currency), -200.2);
        assert_eq!(local.account().position(instrument).qty, 0.0);
        assert_eq!(local.account().balance(currency), 0.0);

        local.deliver(report).unwrap();
        assert_eq!(local.account(), exchange.account());
    }

    #[test]
    fn venue_account_shares_balance_across_instruments() {
        let venue = VenueId(1);
        let currency = CurrencyId(7);
        let mut account = VenueAccount::new(venue);
        account
            .apply(fill_delta(InstrumentId(10), currency))
            .unwrap();
        let mut second = fill_delta(InstrumentId(11), currency);
        second.position_delta = -1.0;
        second.trade_qty = 1.0;
        second.trade_value = 110.0;
        second.cash_delta = 110.0;
        second.fee = 0.1;
        account.apply(second).unwrap();

        assert_eq!(account.position(InstrumentId(10)).qty, 2.0);
        assert_eq!(account.position(InstrumentId(11)).qty, -1.0);
        assert!((account.balance(currency) + 90.3).abs() < 1e-12);
        assert!((account.fee(currency) - 0.3).abs() < 1e-12);
    }

    #[test]
    fn portfolio_aggregates_only_delivered_reports() {
        let currency = CurrencyId(7);
        let mut portfolio = PortfolioLedger::default();
        let report = AccountReport {
            venue_id: VenueId(2),
            exchange_ts: 10,
            delivery_ts: 20,
            sequence: 0,
            delta: AccountDelta {
                funding: 5.0,
                ..AccountDelta::zero(InstrumentId(3), currency)
            },
        };
        assert_eq!(portfolio.total_balance(currency), 0.0);
        portfolio.deliver(report).unwrap();
        assert_eq!(portfolio.total_balance(currency), 5.0);
    }
}
