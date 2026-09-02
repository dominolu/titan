use std::collections::{BTreeMap, btree_map::Entry};

use thiserror::Error;

use crate::types::{OrderId, Side, Status};

use super::{
    AccountDelta, AccountError, CashFlowMode, ExchangeAccountState, ExecutionFeeModel,
    ExecutionOrder, ExecutionOrderRequest, ExecutionReason, ExecutionReport, ExecutionReportKind,
    FeeCharge, FillFeeContext, InstrumentSpec, InstrumentSpecError, InstrumentType, OrderState,
    OrderStateError, OrderTransition, ProposedFill, VenueId,
};

#[derive(Debug, Error, PartialEq)]
pub enum ExecutionError {
    #[error("order ID already exists")]
    DuplicateOrderId,
    #[error("order is not found")]
    OrderNotFound,
    #[error("order venue does not match coordinator venue")]
    VenueMismatch,
    #[error("order instrument does not match instrument specification")]
    InstrumentMismatch,
    #[error(transparent)]
    InvalidInstrument(#[from] InstrumentSpecError),
    #[error(transparent)]
    InvalidState(#[from] OrderStateError),
    #[error(transparent)]
    InvalidAccount(#[from] AccountError),
    #[error("fee model returned a non-finite charge")]
    InvalidFee,
}

/// Shared state/report/account coordinator. A matcher supplies outcomes; this type owns the
/// resulting order transitions and applies every fill exactly once.
pub struct ExecutionCoordinator<F> {
    venue_id: VenueId,
    orders: BTreeMap<OrderId, ExecutionOrder>,
    fee_model: F,
    next_venue_order_id: u64,
    next_report_sequence: u64,
}

impl<F> ExecutionCoordinator<F>
where
    F: ExecutionFeeModel,
{
    pub fn new(venue_id: VenueId, fee_model: F) -> Self {
        Self {
            venue_id,
            orders: BTreeMap::new(),
            fee_model,
            next_venue_order_id: 1,
            next_report_sequence: 0,
        }
    }

    pub fn order(&self, order_id: OrderId) -> Option<&ExecutionOrder> {
        self.orders.get(&order_id)
    }

    pub fn submit(
        &mut self,
        request: ExecutionOrderRequest,
        spec: &InstrumentSpec,
    ) -> Result<(), ExecutionError> {
        if request.venue_id != self.venue_id || spec.venue_id != self.venue_id {
            return Err(ExecutionError::VenueMismatch);
        }
        if request.instrument_id != spec.instrument_id {
            return Err(ExecutionError::InstrumentMismatch);
        }
        match request.order_type {
            crate::types::OrdType::Limit => {
                spec.validate_limit_order(request.price, request.qty)?
            }
            crate::types::OrdType::Market => spec.validate_quantity(request.qty)?,
            crate::types::OrdType::Unsupported => {
                return Err(ExecutionError::InvalidInstrument(
                    InstrumentSpecError::InvalidPrice,
                ));
            }
        }
        match self.orders.entry(request.client_order_id) {
            Entry::Occupied(_) => Err(ExecutionError::DuplicateOrderId),
            Entry::Vacant(entry) => {
                let mut order = ExecutionOrder::new(request);
                order.transition(OrderTransition::Submit)?;
                entry.insert(order);
                Ok(())
            }
        }
    }

    /// Registers a request solely so a local validation/risk rejection can traverse the same
    /// state machine and report projector. Market constraints are intentionally not revalidated.
    pub fn submit_for_rejection(
        &mut self,
        request: ExecutionOrderRequest,
        spec: &InstrumentSpec,
    ) -> Result<(), ExecutionError> {
        if request.venue_id != self.venue_id || spec.venue_id != self.venue_id {
            return Err(ExecutionError::VenueMismatch);
        }
        if request.instrument_id != spec.instrument_id {
            return Err(ExecutionError::InstrumentMismatch);
        }
        match self.orders.entry(request.client_order_id) {
            Entry::Occupied(_) => Err(ExecutionError::DuplicateOrderId),
            Entry::Vacant(entry) => {
                let mut order = ExecutionOrder::new(request);
                order.transition(OrderTransition::Submit)?;
                entry.insert(order);
                Ok(())
            }
        }
    }

    pub fn accept(
        &mut self,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        order.transition(OrderTransition::Accept)?;
        if order.venue_order_id.is_none() {
            order.venue_order_id = Some(self.next_venue_order_id);
            self.next_venue_order_id = self
                .next_venue_order_id
                .checked_add(1)
                .expect("venue order ID overflow");
        }
        order.exchange_arrival_ts = exchange_ts;
        order.last_exchange_ts = exchange_ts;
        self.make_report(
            order_id,
            ExecutionReportKind::Accepted,
            ExecutionReason::None,
            exchange_ts,
            delivery_ts,
            0.0,
            0.0,
            false,
            None,
        )
    }

    /// Emits a rejection without inserting or changing an order. This is used for a duplicate
    /// client ID rejected by the local gateway while preserving the original lifecycle.
    pub fn reject_unstored_request(
        &mut self,
        request: ExecutionOrderRequest,
        exchange_ts: i64,
        delivery_ts: i64,
        reason: ExecutionReason,
    ) -> ExecutionReport {
        let sequence = self.next_report_sequence;
        self.next_report_sequence = self
            .next_report_sequence
            .checked_add(1)
            .expect("execution report sequence overflow");
        ExecutionReport {
            kind: ExecutionReportKind::Rejected,
            reason,
            venue_id: request.venue_id,
            instrument_id: request.instrument_id,
            asset_no: 0,
            order_id: request.client_order_id,
            venue_order_id: 0,
            exchange_ts,
            delivery_ts,
            sequence,
            status: Status::Rejected,
            side: request.side,
            order_price: request.price,
            order_qty: request.qty,
            exec_price: 0.0,
            exec_qty: 0.0,
            cumulative_filled_qty: 0.0,
            maker: false,
            account_delta: None,
        }
    }

    pub fn request_cancel(&mut self, order_id: OrderId) -> Result<(), ExecutionError> {
        self.orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?
            .transition(OrderTransition::RequestCancel)?;
        Ok(())
    }

    pub fn reject(
        &mut self,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
        reason: ExecutionReason,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        let transition = if order.state == OrderState::PendingCancel {
            OrderTransition::CancelReject
        } else {
            OrderTransition::Reject
        };
        order.transition(transition)?;
        order.last_exchange_ts = exchange_ts;
        self.make_report(
            order_id,
            ExecutionReportKind::Rejected,
            reason,
            exchange_ts,
            delivery_ts,
            0.0,
            0.0,
            false,
            None,
        )
    }

    /// Rejects an operation against an order whose lifecycle must remain unchanged (for example,
    /// a cancel which loses a fill race after the order is already Filled).
    pub fn reject_request(
        &mut self,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
        reason: ExecutionReason,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        order.last_exchange_ts = exchange_ts;
        self.make_report(
            order_id,
            ExecutionReportKind::Rejected,
            reason,
            exchange_ts,
            delivery_ts,
            0.0,
            0.0,
            false,
            None,
        )
    }

    pub fn cancel(
        &mut self,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        order.transition(OrderTransition::Cancel)?;
        order.last_exchange_ts = exchange_ts;
        self.make_report(
            order_id,
            ExecutionReportKind::Canceled,
            ExecutionReason::UserCanceled,
            exchange_ts,
            delivery_ts,
            0.0,
            0.0,
            false,
            None,
        )
    }

    pub fn expire(
        &mut self,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        order.transition(OrderTransition::Expire)?;
        order.last_exchange_ts = exchange_ts;
        self.make_report(
            order_id,
            ExecutionReportKind::Expired,
            ExecutionReason::Expired,
            exchange_ts,
            delivery_ts,
            0.0,
            0.0,
            false,
            None,
        )
    }

    pub fn apply_outcome(
        &mut self,
        order_id: OrderId,
        outcome: super::MatchOutcome,
        delivery_ts: i64,
        spec: &InstrumentSpec,
        exchange_account: &mut ExchangeAccountState,
    ) -> Result<ExecutionReport, ExecutionError> {
        match outcome {
            super::MatchOutcome::Accepted { exchange_ts } => {
                self.accept(order_id, exchange_ts, delivery_ts)
            }
            super::MatchOutcome::Rejected {
                exchange_ts,
                reason,
            } => self.reject(order_id, exchange_ts, delivery_ts, reason),
            super::MatchOutcome::Fill(fill) => {
                self.fill(order_id, fill, delivery_ts, spec, exchange_account)
            }
            super::MatchOutcome::Canceled { exchange_ts } => {
                self.cancel(order_id, exchange_ts, delivery_ts)
            }
            super::MatchOutcome::Expired { exchange_ts } => {
                self.expire(order_id, exchange_ts, delivery_ts)
            }
        }
    }

    pub fn fill(
        &mut self,
        order_id: OrderId,
        fill: ProposedFill,
        delivery_ts: i64,
        spec: &InstrumentSpec,
        exchange_account: &mut ExchangeAccountState,
    ) -> Result<ExecutionReport, ExecutionError> {
        if spec.venue_id != self.venue_id {
            return Err(ExecutionError::VenueMismatch);
        }
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        if order.request.instrument_id != spec.instrument_id {
            return Err(ExecutionError::InstrumentMismatch);
        }
        order.transition(OrderTransition::Fill { qty: fill.qty })?;
        order.last_exchange_ts = fill.exchange_ts;

        let trade_value = trade_value(spec, fill.price, fill.qty);
        let FeeCharge {
            amount: fee,
            currency,
        } = self.fee_model.charge(&FillFeeContext {
            order,
            fill,
            instrument: spec,
            trade_value,
        });
        if !fee.is_finite() {
            return Err(ExecutionError::InvalidFee);
        }
        let side = match order.request.side {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
            _ => unreachable!("validated execution request side"),
        };
        if exchange_account.account().venue_id() != self.venue_id {
            return Err(ExecutionError::VenueMismatch);
        }
        let current = exchange_account.account().position(spec.instrument_id);
        let realized_pnl = realized_pnl(
            spec,
            current.qty,
            current.avg_entry_price,
            fill.price,
            fill.qty * side,
        );
        let cash_delta = match spec.cash_flow_mode {
            CashFlowMode::LegacyNotional => -trade_value * side,
            CashFlowMode::DerivativePnl => match spec.instrument_type {
                InstrumentType::Spot => -trade_value * side,
                InstrumentType::LinearFuture
                | InstrumentType::InverseFuture
                | InstrumentType::LinearPerpetual
                | InstrumentType::InversePerpetual => realized_pnl,
            },
        };
        let delta = AccountDelta {
            instrument_id: spec.instrument_id,
            position_delta: fill.qty * side,
            trade_qty: fill.qty,
            trade_value,
            currency,
            cash_delta,
            fee,
            funding: 0.0,
            execution_price: fill.price,
            realized_pnl,
        };
        exchange_account.apply_and_report(
            delta,
            fill.exchange_ts,
            delivery_ts,
            self.next_report_sequence,
        )?;
        self.make_report(
            order_id,
            ExecutionReportKind::Fill,
            ExecutionReason::None,
            fill.exchange_ts,
            delivery_ts,
            fill.price,
            fill.qty,
            fill.maker,
            Some(delta),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn make_report(
        &mut self,
        order_id: OrderId,
        kind: ExecutionReportKind,
        reason: ExecutionReason,
        exchange_ts: i64,
        delivery_ts: i64,
        exec_price: f64,
        exec_qty: f64,
        maker: bool,
        account_delta: Option<AccountDelta>,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(ExecutionError::OrderNotFound)?;
        order.last_delivery_ts = delivery_ts;
        let status = match order.state {
            OrderState::Initialized | OrderState::Submitted => Status::None,
            OrderState::Accepted | OrderState::PendingCancel => Status::New,
            OrderState::PartiallyFilled => Status::PartiallyFilled,
            OrderState::Filled => Status::Filled,
            OrderState::Rejected => Status::Rejected,
            OrderState::Canceled => Status::Canceled,
            OrderState::Expired => Status::Expired,
        };
        let sequence = self.next_report_sequence;
        self.next_report_sequence = self
            .next_report_sequence
            .checked_add(1)
            .expect("execution report sequence overflow");
        Ok(ExecutionReport {
            kind,
            reason,
            venue_id: self.venue_id,
            instrument_id: order.request.instrument_id,
            asset_no: 0,
            order_id,
            venue_order_id: order.venue_order_id.unwrap_or(0),
            exchange_ts,
            delivery_ts,
            sequence,
            status,
            side: order.request.side,
            order_price: order.request.price,
            order_qty: order.request.qty,
            exec_price,
            exec_qty,
            cumulative_filled_qty: order.filled_qty,
            maker,
            account_delta,
        })
    }

    pub fn reset(&mut self) {
        self.orders.clear();
        self.fee_model.reset();
        self.next_venue_order_id = 1;
        self.next_report_sequence = 0;
    }
}

fn trade_value(spec: &InstrumentSpec, price: f64, qty: f64) -> f64 {
    match spec.instrument_type {
        InstrumentType::Spot | InstrumentType::LinearFuture | InstrumentType::LinearPerpetual => {
            spec.contract_size * price * qty
        }
        InstrumentType::InverseFuture | InstrumentType::InversePerpetual => {
            spec.contract_size * qty / price
        }
    }
}

fn realized_pnl(
    spec: &InstrumentSpec,
    old_qty: f64,
    average_entry: f64,
    exit_price: f64,
    position_delta: f64,
) -> f64 {
    if old_qty == 0.0 || average_entry <= 0.0 || old_qty.signum() == position_delta.signum() {
        return 0.0;
    }
    let closed_qty = old_qty.abs().min(position_delta.abs());
    match spec.instrument_type {
        InstrumentType::Spot => 0.0,
        InstrumentType::LinearFuture | InstrumentType::LinearPerpetual => {
            closed_qty * spec.contract_size * (exit_price - average_entry) * old_qty.signum()
        }
        InstrumentType::InverseFuture | InstrumentType::InversePerpetual => {
            closed_qty
                * spec.contract_size
                * (1.0 / average_entry - 1.0 / exit_price)
                * old_qty.signum()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{CurrencyId, InstrumentId, OrderOrigin, RateFeeModel},
        types::{OrdType, Side, TimeInForce},
    };

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(4),
            asset_no: 7,
            venue_id: VenueId(2),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    fn request() -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            client_order_id: 8,
            venue_id: VenueId(2),
            instrument_id: InstrumentId(4),
            price: 102.0,
            qty: 5.0,
            side: Side::Buy,
            time_in_force: TimeInForce::IOC,
            order_type: OrdType::Limit,
            reduce_only: false,
            origin: OrderOrigin::Strategy,
            local_submit_ts: 10,
        }
    }

    #[test]
    fn multiple_fills_each_create_report_fee_and_account_delta() {
        let spec = spec();
        let mut account = ExchangeAccountState::new(spec.venue_id);
        let mut coordinator = ExecutionCoordinator::new(
            VenueId(2),
            RateFeeModel {
                maker_rate: 0.0,
                taker_rate: 0.001,
                currency: CurrencyId(1),
            },
        );
        coordinator.submit(request(), &spec).unwrap();
        let accepted = coordinator.accept(8, 20, 25).unwrap();
        assert_eq!(accepted.venue_order_id, 1);
        let first = coordinator
            .fill(
                8,
                ProposedFill {
                    exchange_ts: 20,
                    price: 100.0,
                    qty: 3.0,
                    maker: false,
                },
                25,
                &spec,
                &mut account,
            )
            .unwrap();
        let second = coordinator
            .fill(
                8,
                ProposedFill {
                    exchange_ts: 20,
                    price: 101.0,
                    qty: 2.0,
                    maker: false,
                },
                25,
                &spec,
                &mut account,
            )
            .unwrap();

        assert_eq!(first.status, Status::PartiallyFilled);
        assert_eq!(first.exec_qty, 3.0);
        assert_eq!(second.status, Status::Filled);
        assert_eq!(second.exec_qty, 2.0);
        assert_ne!(first.sequence, second.sequence);
        assert_eq!(first.venue_order_id, accepted.venue_order_id);
        assert_eq!(second.venue_order_id, accepted.venue_order_id);
        assert_eq!(coordinator.order(8).unwrap().exchange_arrival_ts, 20);
        assert_eq!(coordinator.order(8).unwrap().last_exchange_ts, 20);
        assert_eq!(coordinator.order(8).unwrap().last_delivery_ts, 25);
        let venue_account = account.account();
        assert_eq!(venue_account.position(spec.instrument_id).qty, 5.0);
        assert_eq!(venue_account.position(spec.instrument_id).num_trades, 2);
        assert!((venue_account.balance(CurrencyId(1)) + 502.502).abs() < 1e-12);
        assert!((venue_account.fee(CurrencyId(1)) - 0.502).abs() < 1e-12);
    }

    #[test]
    fn cancel_reject_restores_active_state_and_cancel_accept_is_terminal() {
        let spec = spec();
        let mut coordinator = ExecutionCoordinator::new(
            VenueId(2),
            RateFeeModel {
                maker_rate: 0.0,
                taker_rate: 0.0,
                currency: CurrencyId(1),
            },
        );
        coordinator.submit(request(), &spec).unwrap();
        coordinator.accept(8, 20, 25).unwrap();
        coordinator.request_cancel(8).unwrap();
        let rejected = coordinator
            .reject(8, 30, 35, ExecutionReason::Unknown(9))
            .unwrap();
        assert_eq!(rejected.kind, ExecutionReportKind::Rejected);
        assert_eq!(coordinator.order(8).unwrap().state, OrderState::Accepted);

        coordinator.request_cancel(8).unwrap();
        let canceled = coordinator.cancel(8, 40, 45).unwrap();
        assert_eq!(canceled.status, Status::Canceled);
        assert!(coordinator.order(8).unwrap().state.is_terminal());
    }

    #[test]
    fn derivative_cash_flow_uses_realized_pnl_instead_of_opening_notional() {
        let mut spec = spec();
        spec.cash_flow_mode = CashFlowMode::DerivativePnl;
        let mut account = ExchangeAccountState::new(spec.venue_id);
        let mut coordinator = ExecutionCoordinator::new(
            VenueId(2),
            RateFeeModel {
                maker_rate: 0.0,
                taker_rate: 0.0,
                currency: CurrencyId(1),
            },
        );
        let mut buy = request();
        buy.qty = 2.0;
        coordinator.submit(buy, &spec).unwrap();
        coordinator.accept(8, 20, 20).unwrap();
        coordinator
            .fill(
                8,
                ProposedFill {
                    exchange_ts: 20,
                    price: 100.0,
                    qty: 2.0,
                    maker: false,
                },
                20,
                &spec,
                &mut account,
            )
            .unwrap();
        assert_eq!(account.account().balance(CurrencyId(1)), 0.0);

        let mut sell = buy;
        sell.client_order_id = 9;
        sell.side = Side::Sell;
        coordinator.submit(sell, &spec).unwrap();
        coordinator.accept(9, 30, 30).unwrap();
        coordinator
            .fill(
                9,
                ProposedFill {
                    exchange_ts: 30,
                    price: 110.0,
                    qty: 2.0,
                    maker: false,
                },
                30,
                &spec,
                &mut account,
            )
            .unwrap();
        let position = account.account().position(spec.instrument_id);
        assert_eq!(position.qty, 0.0);
        assert_eq!(position.avg_entry_price, 0.0);
        assert_eq!(position.realized_pnl, 20.0);
        assert_eq!(account.account().balance(CurrencyId(1)), 20.0);
    }
}
