use thiserror::Error;

use super::{
    ExecutionCoordinator, ExecutionError, ExecutionFeeModel, ExecutionOrderRequest,
    ExecutionReport, InstrumentSpec, LegacyOrderSnapshot, MatchOutcome, ObservedOutcome,
    OrderOrigin, OrderState,
};

#[derive(Debug, Error, PartialEq)]
pub enum TickCoordinatorError {
    #[error("the observed Tick outcome does not contain its legacy order snapshot")]
    MissingOrderSnapshot,
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

pub struct SharedTickExecutionConfig {
    pub spec: InstrumentSpec,
    pub fee_model: Box<dyn ExecutionFeeModel>,
}

impl SharedTickExecutionConfig {
    pub fn new<F>(spec: InstrumentSpec, fee_model: F) -> Self
    where
        F: ExecutionFeeModel + 'static,
    {
        Self {
            spec,
            fee_model: Box::new(fee_model),
        }
    }
}

/// Exchange-time adapter which drives the shared state/account/report coordinator from the
/// current monomorphized L2/L3 matchers.
///
/// Legacy market orders can fill immediately without a separate Accepted response. In that case
/// this adapter emits the canonical Accepted report immediately before the Fill report at the
/// same exchange timestamp. Likewise, the legacy cancel request is normalized into the shared
/// PendingCancel transition before its exchange result is applied.
pub struct TickOutcomeCoordinator<F> {
    spec: InstrumentSpec,
    coordinator: ExecutionCoordinator<F>,
}

impl<F> TickOutcomeCoordinator<F>
where
    F: ExecutionFeeModel,
{
    pub fn new(spec: InstrumentSpec, fee_model: F) -> Self {
        Self {
            coordinator: ExecutionCoordinator::new(spec.venue_id, fee_model),
            spec,
        }
    }

    pub fn spec(&self) -> &InstrumentSpec {
        &self.spec
    }

    pub fn coordinator(&self) -> &ExecutionCoordinator<F> {
        &self.coordinator
    }

    /// Exchange-time position maintained by the coordinator which consumed the matcher outcome.
    pub fn exchange_position(&self) -> f64 {
        self.coordinator
            .exchange_account()
            .account()
            .position(self.spec.instrument_id)
            .qty
    }

    pub fn reset(&mut self) {
        self.coordinator.reset();
    }

    /// Applies one observed matcher result. `reports` is caller-owned and reusable; it receives
    /// one report normally or Accepted+Fill for an immediate fill.
    pub fn apply(
        &mut self,
        observed: ObservedOutcome,
        delivery_ts: i64,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), TickCoordinatorError> {
        reports.clear();
        let snapshot = observed
            .order
            .ok_or(TickCoordinatorError::MissingOrderSnapshot)?;
        if self.coordinator.order(observed.order_id).is_none() {
            self.coordinator
                .submit(self.request(snapshot), &self.spec)?;
        }

        let state = self.coordinator.order(observed.order_id).unwrap().state;
        if let MatchOutcome::Rejected {
            exchange_ts,
            reason,
        } = observed.outcome
            && state.is_terminal()
        {
            let mut report = self.coordinator.reject_request(
                observed.order_id,
                exchange_ts,
                delivery_ts,
                reason,
            )?;
            report.asset_no = self.spec.asset_no;
            reports.push(report);
            return Ok(());
        }
        match observed.outcome {
            MatchOutcome::Accepted { .. } if state != OrderState::Submitted => return Ok(()),
            MatchOutcome::Fill(fill) if state == OrderState::Submitted => {
                reports.push(self.coordinator.accept(
                    observed.order_id,
                    fill.exchange_ts,
                    delivery_ts,
                )?);
            }
            MatchOutcome::Canceled { .. }
                if matches!(state, OrderState::Accepted | OrderState::PartiallyFilled) =>
            {
                self.coordinator.request_cancel(observed.order_id)?;
            }
            MatchOutcome::Rejected { .. }
                if matches!(state, OrderState::Accepted | OrderState::PartiallyFilled) =>
            {
                self.coordinator.request_cancel(observed.order_id)?;
            }
            _ => {}
        }

        reports.push(self.coordinator.apply_outcome(
            observed.order_id,
            observed.outcome,
            delivery_ts,
            &self.spec,
        )?);
        for report in reports.iter_mut() {
            report.asset_no = self.spec.asset_no;
        }
        Ok(())
    }

    fn request(&self, snapshot: LegacyOrderSnapshot) -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            client_order_id: snapshot.order_id,
            venue_id: self.spec.venue_id,
            instrument_id: self.spec.instrument_id,
            price: snapshot.price,
            qty: snapshot.qty,
            side: snapshot.side,
            time_in_force: snapshot.time_in_force,
            order_type: snapshot.order_type,
            reduce_only: false,
            origin: OrderOrigin::Strategy,
            local_submit_ts: snapshot.local_submit_ts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{
            CurrencyId, InstrumentId, InstrumentType, NoFee, ProposedFill, VenueId,
        },
        types::{OrdType, Order, Side, Status, TimeInForce},
    };

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(10),
            asset_no: 0,
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
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    fn observed(order: &Order, outcome: MatchOutcome) -> ObservedOutcome {
        ObservedOutcome {
            order_id: order.order_id,
            order: Some(order.into()),
            outcome,
        }
    }

    #[test]
    fn immediate_fill_synthesizes_accept_and_updates_shared_exchange_account() {
        let mut adapter = TickOutcomeCoordinator::new(
            spec(),
            NoFee {
                currency: CurrencyId(1),
            },
        );
        let order = Order::new(7, 0, 1.0, 2.0, Side::Buy, OrdType::Market, TimeInForce::IOC);
        let mut reports = Vec::with_capacity(2);
        adapter
            .apply(
                observed(
                    &order,
                    MatchOutcome::Fill(ProposedFill {
                        exchange_ts: 100,
                        price: 101.0,
                        qty: 2.0,
                        maker: false,
                    }),
                ),
                120,
                &mut reports,
            )
            .unwrap();

        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].status, Status::New);
        assert_eq!(reports[1].status, Status::Filled);
        assert_eq!(
            adapter
                .coordinator()
                .exchange_account()
                .account()
                .position(InstrumentId(10))
                .qty,
            2.0
        );
    }

    #[test]
    fn cancel_result_enters_pending_cancel_before_terminal_transition() {
        let mut adapter = TickOutcomeCoordinator::new(
            spec(),
            NoFee {
                currency: CurrencyId(1),
            },
        );
        let order = Order::new(
            8,
            100,
            1.0,
            1.0,
            Side::Sell,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        let mut reports = Vec::with_capacity(2);
        adapter
            .apply(
                observed(&order, MatchOutcome::Accepted { exchange_ts: 10 }),
                12,
                &mut reports,
            )
            .unwrap();
        adapter
            .apply(
                observed(&order, MatchOutcome::Canceled { exchange_ts: 20 }),
                23,
                &mut reports,
            )
            .unwrap();
        assert_eq!(reports[0].status, Status::Canceled);
        assert_eq!(
            adapter.coordinator().order(8).unwrap().state,
            OrderState::Canceled
        );
    }
}
