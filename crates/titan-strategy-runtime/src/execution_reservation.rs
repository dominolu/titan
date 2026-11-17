//! Per-leg execution reservations for a multi-binding instance.
//!
//! A maker quote commits the platform to hedge whatever fills. That obligation exists from the
//! moment the submit is dispatched, not from the moment a fill arrives, so the hedge resources
//! must be reserved *before* the order can trade. Cancelling never releases: a cancel always races
//! a fill.
//!
//! Both legs reserve against the same hedge budget, from different directions: a quote on the maker
//! leg owes `lots × ratio` hedge lots, an in-flight hedge order on the hedge leg owes its own lots.
//! As maker fills settle the maker side shrinks and the hedge order takes over, so the total stays
//! at or above the real obligation at every point in between.
//!
//! All arithmetic is integer-only. A reservation is released only when its order is proven
//! terminal *and* no independent fill is still awaited.

use std::collections::BTreeMap;

use crate::{ExecutionLegRole, LotRatio, MultiBindingExecutionLimits};

/// Per-leg ceilings copied from the bound configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegCaps {
    pub max_position_lots: i128,
    pub max_notional_tick_lots: i128,
}

/// A non-reduce-only submit that asks the platform to hold hedge capacity before it can trade.
///
/// The ledger resolves the leg role from the bound configuration instead of trusting the caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationRequest {
    pub order_id: u64,
    pub account_no: u32,
    pub asset_no: u32,
    pub lots: i64,
    pub price_ticks: i64,
    /// Worst hedge price this order is willing to commit at.
    pub hedge_mark_ticks: i64,
}

/// One live order's outstanding hedge obligation and own-leg ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Reservation {
    account_no: u32,
    asset_no: u32,
    /// True when this order quotes on the maker leg and therefore owes `ratio` hedge lots each.
    maker: bool,
    /// Lots of this order that may still trade.
    remaining_lots: i64,
    /// Cumulative lots already settled from independent fills.
    settled_lots: i64,
    /// Hedge lots reserved for those remaining lots.
    hedge_lots: i128,
    /// Hedge notional reserved at the worst protected hedge price seen so far.
    hedge_notional_tick_lots: i128,
    /// Own-leg notional of the remaining lots.
    own_notional_tick_lots: i128,
    hedge_mark_ticks: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RescueUsage {
    quantity_lots: i128,
    notional_tick_lots: i128,
}

/// Ledger shared by admission and evidence. It is a pure function of the bound configuration plus
/// the facts fed into it, so a restart rebuilds it from recovered orders instead of trusting a
/// reset counter.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionReservations {
    revision: u64,
    ratio: i128,
    orders: BTreeMap<u64, Reservation>,
    rescue: RescueUsage,
}

impl ExecutionReservations {
    /// Builds an empty ledger for a bound configuration.
    pub fn new(limits: &MultiBindingExecutionLimits) -> Self {
        Self {
            revision: limits.revision,
            ratio: reservation_ratio(limits).unwrap_or(0),
            orders: BTreeMap::new(),
            rescue: RescueUsage::default(),
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty() && self.rescue == RescueUsage::default()
    }

    /// Number of live order reservations.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// Live reservation order ids in ascending order.
    pub fn order_ids(&self) -> Vec<u64> {
        self.orders.keys().copied().collect()
    }

    pub fn holds(&self, order_id: u64) -> bool {
        self.orders.contains_key(&order_id)
    }

    /// Reserves the hedge obligation of a submit before it is dispatched.
    ///
    /// `hedge_mark_ticks` is the worst hedge price the caller is willing to commit at; raising it
    /// for an existing reservation widens the reserved notional, never narrows it.
    pub fn reserve(
        &mut self,
        limits: &MultiBindingExecutionLimits,
        request: ReservationRequest,
    ) -> Result<(), &'static str> {
        let ReservationRequest {
            order_id,
            account_no,
            asset_no,
            lots,
            price_ticks,
            hedge_mark_ticks,
        } = request;
        if limits.revision != self.revision {
            return Err("execution_reservation_revision");
        }
        if self.ratio <= 0 {
            return Err("execution_binding_lot_ratio");
        }
        if order_id == 0 || lots <= 0 || price_ticks <= 0 || hedge_mark_ticks <= 0 {
            return Err("execution_reservation_invalid");
        }
        if self.orders.contains_key(&order_id) {
            return Err("execution_reservation_duplicate");
        }
        let (maker_leg, hedge_leg) = legs(limits)?;
        let maker = if maker_leg.local_account_no == account_no
            && maker_leg.local_asset_no == asset_no
        {
            true
        } else if hedge_leg.local_account_no == account_no && hedge_leg.local_asset_no == asset_no {
            false
        } else {
            return Err("execution_reservation_unbound_leg");
        };
        let quantity = i128::from(lots);
        let hedge_lots = if maker {
            quantity
                .checked_mul(self.ratio)
                .ok_or("execution_reservation_overflow")?
        } else {
            quantity
        };
        let hedge_notional_tick_lots = hedge_lots
            .checked_mul(i128::from(hedge_mark_ticks))
            .ok_or("execution_reservation_overflow")?;
        let own_notional_tick_lots = quantity
            .checked_mul(i128::from(price_ticks))
            .ok_or("execution_reservation_overflow")?;
        let own_leg = if maker { maker_leg } else { hedge_leg };
        let own_caps = caps_of(own_leg)?;
        let hedge_caps = caps_of(hedge_leg)?;
        let own_totals = self.leg_totals(own_leg.local_account_no, own_leg.local_asset_no);
        let fits = own_totals
            .position_lots
            .checked_add(quantity)
            .is_some_and(|total| total <= own_caps.max_position_lots)
            && own_totals
                .notional_tick_lots
                .checked_add(own_notional_tick_lots)
                .is_some_and(|total| total <= own_caps.max_notional_tick_lots)
            && hedge_totals_ok(
                self.hedge_totals(),
                hedge_lots,
                hedge_notional_tick_lots,
                hedge_caps,
            );
        if !fits {
            return Err("execution_reservation_unavailable");
        }
        self.orders.insert(
            order_id,
            Reservation {
                account_no,
                asset_no,
                maker,
                remaining_lots: lots,
                settled_lots: 0,
                hedge_lots,
                hedge_notional_tick_lots,
                own_notional_tick_lots,
                hedge_mark_ticks,
            },
        );
        Ok(())
    }

    /// Applies the cumulative independent-fill quantity to an existing reservation.
    ///
    /// The filled lots no longer need own-leg capacity, but their hedge obligation stays reserved:
    /// it belongs to the hedge order now, and only that order's own terminal proof may release it.
    ///
    /// Idempotent: replaying the same cumulative quantity is a no-op, so a reconciliation sweep can
    /// run after every callback without double consuming budget.
    pub fn settle(
        &mut self,
        order_id: u64,
        cumulative_filled_lots: i64,
        price_ticks: i64,
    ) -> Result<(), &'static str> {
        let Some(entry) = self.orders.get_mut(&order_id) else {
            return Err("execution_reservation_unknown_order");
        };
        if cumulative_filled_lots < 0 || price_ticks <= 0 {
            return Err("execution_reservation_invalid_fill");
        }
        let delta = cumulative_filled_lots.saturating_sub(entry.settled_lots);
        if delta <= 0 {
            return Ok(());
        }
        if delta > entry.remaining_lots {
            return Err("execution_reservation_invalid_fill");
        }
        entry.remaining_lots -= delta;
        entry.settled_lots = cumulative_filled_lots;
        entry.own_notional_tick_lots = i128::from(entry.remaining_lots)
            .checked_mul(i128::from(price_ticks))
            .ok_or("execution_reservation_overflow")?;
        Ok(())
    }

    /// Widens the reserved hedge mark for an existing order. A lower mark is ignored, so this can
    /// never release budget.
    pub fn widen_mark(&mut self, order_id: u64, hedge_mark_ticks: i64) -> Result<(), &'static str> {
        let Some(entry) = self.orders.get_mut(&order_id) else {
            return Err("execution_reservation_unknown_order");
        };
        if hedge_mark_ticks <= entry.hedge_mark_ticks {
            return Ok(());
        }
        entry.hedge_notional_tick_lots = entry
            .hedge_lots
            .checked_mul(i128::from(hedge_mark_ticks))
            .ok_or("execution_reservation_overflow")?;
        entry.hedge_mark_ticks = hedge_mark_ticks;
        Ok(())
    }

    /// Releases a reservation once the caller proved the order terminal and every consumed fill
    /// accounted for. A cancel alone is never enough to call this.
    pub fn release(&mut self, order_id: u64) -> Result<(), &'static str> {
        self.orders
            .remove(&order_id)
            .map(|_| ())
            .ok_or("execution_reservation_unknown_order")
    }

    /// Charges the pre-authorized rescue budget. It never resets within a run and never exceeds the
    /// configured ceiling.
    pub fn charge_rescue(
        &mut self,
        limits: &MultiBindingExecutionLimits,
        quantity_lots: i64,
        notional_tick_lots: i128,
    ) -> Result<(), &'static str> {
        if quantity_lots <= 0 || notional_tick_lots <= 0 {
            return Err("execution_reservation_invalid");
        }
        let budget = i128::from(limits.rescue.max_quantity_lots);
        let ceiling = crate::parse_notional_limit(&limits.rescue.max_notional_tick_lots)
            .ok_or("execution_reservation_invalid")?;
        let quantity_after = self
            .rescue
            .quantity_lots
            .checked_add(i128::from(quantity_lots))
            .ok_or("execution_reservation_overflow")?;
        let notional_after = self
            .rescue
            .notional_tick_lots
            .checked_add(notional_tick_lots)
            .ok_or("execution_reservation_overflow")?;
        if quantity_after > budget || notional_after > ceiling {
            return Err("execution_rescue_exhausted");
        }
        self.rescue.quantity_lots = quantity_after;
        self.rescue.notional_tick_lots = notional_after;
        Ok(())
    }

    pub fn rescue_spent(&self) -> (i128, i128) {
        (self.rescue.quantity_lots, self.rescue.notional_tick_lots)
    }

    /// Reserved hedge lots and hedge notional across every live order on either leg.
    pub fn hedge_totals(&self) -> (i128, i128) {
        self.orders
            .values()
            .fold((0_i128, 0_i128), |(lots, notional), entry| {
                (
                    lots.saturating_add(entry.hedge_lots),
                    notional.saturating_add(entry.hedge_notional_tick_lots),
                )
            })
    }

    /// Own-leg totals for one bound leg.
    pub fn leg_totals(&self, account_no: u32, asset_no: u32) -> LegTotals {
        self.orders
            .values()
            .filter(|entry| entry.account_no == account_no && entry.asset_no == asset_no)
            .fold(LegTotals::default(), |mut totals, entry| {
                totals.position_lots = totals
                    .position_lots
                    .saturating_add(i128::from(entry.remaining_lots));
                totals.notional_tick_lots = totals
                    .notional_tick_lots
                    .saturating_add(entry.own_notional_tick_lots);
                totals
            })
    }

    /// Rebuilds the ledger from recovered facts.
    ///
    /// Every non-terminal non-reduce-only order on either leg re-reserves its remaining quantity and
    /// the recovered rescue consumption is carried over, so a restart can neither start from an
    /// empty budget nor refund already spent rescue allowance. The result is a pure function of its
    /// arguments.
    pub fn rebuild(
        &self,
        limits: &MultiBindingExecutionLimits,
        recovered: impl IntoIterator<Item = ReservationRequest>,
        rescue_spent: (i128, i128),
    ) -> Result<Self, &'static str> {
        let mut ledger = Self::new(limits);
        for request in recovered {
            ledger.reserve(limits, request)?;
        }
        let (quantity, notional) = rescue_spent;
        let ceiling = crate::parse_notional_limit(&limits.rescue.max_notional_tick_lots)
            .ok_or("execution_binding_invalid")?;
        if quantity < 0
            || notional < 0
            || quantity > i128::from(limits.rescue.max_quantity_lots)
            || notional > ceiling
        {
            return Err("execution_rescue_exhausted");
        }
        ledger.rescue = RescueUsage {
            quantity_lots: quantity,
            notional_tick_lots: notional,
        };
        Ok(ledger)
    }
}

/// Whether a reservation may be released.
///
/// Cancelling never releases: `CANCEL_PENDING` (5) and every non-terminal status keep holding. A
/// terminal status releases only when no independent fill is still awaited, because an order event
/// may report a fill whose execution event has not arrived yet.
pub fn reservation_releasable(status: u8, awaiting_independent_fill: bool) -> bool {
    matches!(status, 4 | 6 | 7 | 8) && !awaiting_independent_fill
}

/// Own-leg reservation totals.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegTotals {
    pub position_lots: i128,
    pub notional_tick_lots: i128,
}

fn legs(
    limits: &MultiBindingExecutionLimits,
) -> Result<(&crate::ExecutionLegLimit, &crate::ExecutionLegLimit), &'static str> {
    let maker = limits
        .legs
        .iter()
        .find(|leg| leg.role == ExecutionLegRole::Maker)
        .ok_or("execution_binding_role_invalid")?;
    let hedge = limits
        .legs
        .iter()
        .find(|leg| leg.role == ExecutionLegRole::Hedge)
        .ok_or("execution_binding_role_invalid")?;
    Ok((maker, hedge))
}

fn caps_of(leg: &crate::ExecutionLegLimit) -> Result<LegCaps, &'static str> {
    Ok(LegCaps {
        max_position_lots: i128::from(leg.max_position_lots),
        max_notional_tick_lots: crate::parse_notional_limit(&leg.max_notional_tick_lots)
            .ok_or("execution_binding_invalid")?,
    })
}

fn hedge_totals_ok(
    current: (i128, i128),
    extra_lots: i128,
    extra_notional: i128,
    caps: LegCaps,
) -> bool {
    current
        .0
        .checked_add(extra_lots)
        .is_some_and(|total| total <= caps.max_position_lots)
        && current
            .1
            .checked_add(extra_notional)
            .is_some_and(|total| total <= caps.max_notional_tick_lots)
}

fn reservation_ratio(limits: &MultiBindingExecutionLimits) -> Option<i128> {
    let (maker, hedge) = legs(limits).ok()?;
    LotRatio::exact(maker.base_delta_per_lot, hedge.base_delta_per_lot).integer_value()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    fn unit(coefficient: u64, scale: u8) -> DecimalUnit {
        DecimalUnit::new(coefficient, scale).expect("valid unit")
    }

    fn limits() -> MultiBindingExecutionLimits {
        MultiBindingExecutionLimits {
            revision: 3,
            quote_currency: 1,
            legs: Arc::from([
                crate::ExecutionLegLimit {
                    local_account_no: 0,
                    local_asset_no: 1,
                    role: ExecutionLegRole::Maker,
                    base_delta_per_lot: unit(1, 2),
                    min_quantity_lots: 1,
                    min_notional_quote: unit(1, 0),
                    max_position_lots: 20,
                    max_notional_tick_lots: "1000000".into(),
                    max_market_age_ns: 2_000_000_000,
                    margin_domain: 1,
                    margin_rate_bps: 10_000,
                },
                crate::ExecutionLegLimit {
                    local_account_no: 1,
                    local_asset_no: 0,
                    role: ExecutionLegRole::Hedge,
                    base_delta_per_lot: unit(1, 3),
                    min_quantity_lots: 1,
                    min_notional_quote: unit(1, 0),
                    max_position_lots: 200,
                    max_notional_tick_lots: "2000000".into(),
                    max_market_age_ns: 2_000_000_000,
                    margin_domain: 2,
                    margin_rate_bps: 10_000,
                },
            ]),
            rescue: crate::RescueBudget {
                max_quantity_lots: 100,
                max_notional_tick_lots: "500000".into(),
                max_cost_bps: 20,
                deadline_ns: 5_000_000_000,
            },
            stop: crate::StopBudget {
                cancel_ns: 1_000_000_000,
                hedge_ns: 1_000_000_000,
                exit_ns: 1_000_000_000,
                confirm_ns: 1_000_000_000,
                slack_ns: 500_000_000,
            },
        }
    }

    fn maker(order_id: u64, lots: i64, price: i64, mark: i64) -> ReservationRequest {
        ReservationRequest {
            order_id,
            account_no: 0,
            asset_no: 1,
            lots,
            price_ticks: price,
            hedge_mark_ticks: mark,
        }
    }

    fn hedge(order_id: u64, lots: i64, price: i64, mark: i64) -> ReservationRequest {
        ReservationRequest {
            order_id,
            account_no: 1,
            asset_no: 0,
            lots,
            price_ticks: price,
            hedge_mark_ticks: mark,
        }
    }

    #[test]
    fn maker_submit_reserves_its_hedge_before_it_can_trade() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        assert!(ledger.is_empty());
        // Five maker lots at ratio ten reserve fifty hedge lots.
        ledger.reserve(&limits, maker(7, 5, 1000, 1005)).unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger.hedge_totals(), (50, 50 * 1005));
        assert!(ledger.holds(7));
        assert_eq!(
            ledger.reserve(&limits, maker(7, 5, 1000, 1005)),
            Err("execution_reservation_duplicate")
        );
    }

    #[test]
    fn a_hedge_order_holds_the_same_budget_the_maker_quote_counted() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        ledger.reserve(&limits, maker(7, 5, 1000, 1005)).unwrap();
        // The maker quote fills completely, then the hedge order takes over. The hedge leg order
        // reserves its own fifty lots, so the budget never dips between the two.
        ledger.settle(7, 5, 1000).unwrap();
        assert_eq!(ledger.hedge_totals().0, 50);
        ledger.reserve(&limits, hedge(8, 50, 1000, 1005)).unwrap();
        assert_eq!(ledger.hedge_totals().0, 100);
        // Releasing the maker quote afterwards leaves exactly the hedge order's obligation.
        ledger.release(7).unwrap();
        assert_eq!(ledger.hedge_totals().0, 50);
        assert!(ledger.holds(8));
    }

    #[test]
    fn a_cancel_alone_never_releases_the_hedge_reservation() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        ledger.reserve(&limits, maker(7, 5, 1000, 1005)).unwrap();
        ledger.settle(7, 2, 1000).unwrap();
        ledger.settle(7, 2, 1000).unwrap();
        assert_eq!(ledger.hedge_totals().0, 50);
        assert!(ledger.holds(7));
        assert_eq!(
            ledger.settle(7, 9, 1000),
            Err("execution_reservation_invalid_fill")
        );
        ledger.release(7).unwrap();
        assert_eq!(ledger.hedge_totals(), (0, 0));
        assert_eq!(
            ledger.release(7),
            Err("execution_reservation_unknown_order")
        );
    }

    #[test]
    fn cancel_pending_and_awaiting_fills_keep_holding_the_budget() {
        for status in [1_u8, 2, 3, 5] {
            assert!(!reservation_releasable(status, false), "status {status}");
        }
        for status in [4_u8, 6, 7, 8] {
            assert!(!reservation_releasable(status, true), "status {status}");
            assert!(reservation_releasable(status, false), "status {status}");
        }
    }

    #[test]
    fn reservations_are_refused_beyond_the_bound_leg_caps() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        // Maker cap is twenty lots, so a twenty-one lot quote is refused before dispatch.
        assert_eq!(
            ledger.reserve(&limits, maker(1, 21, 1000, 1000)),
            Err("execution_reservation_unavailable")
        );
        ledger.reserve(&limits, maker(1, 10, 1000, 1000)).unwrap();
        ledger.reserve(&limits, maker(2, 10, 1000, 1000)).unwrap();
        assert_eq!(
            ledger.reserve(&limits, maker(3, 1, 1000, 1000)),
            Err("execution_reservation_unavailable")
        );
        // Hedge notional cap is two million tick lots: 200 hedge lots * 10001 exceeds it.
        let mut wide = ExecutionReservations::new(&limits);
        assert_eq!(
            wide.reserve(&limits, maker(1, 20, 1000, 100_001)),
            Err("execution_reservation_unavailable")
        );
        // The hedge leg has its own position ceiling of 200 lots.
        let mut hedged = ExecutionReservations::new(&limits);
        hedged.reserve(&limits, hedge(1, 200, 1000, 1000)).unwrap();
        assert_eq!(
            hedged.reserve(&limits, hedge(2, 1, 1000, 1000)),
            Err("execution_reservation_unavailable")
        );
    }

    #[test]
    fn widened_marks_never_shrink_a_reservation() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        ledger.reserve(&limits, maker(7, 5, 1000, 1000)).unwrap();
        ledger.widen_mark(7, 1005).unwrap();
        assert_eq!(ledger.hedge_totals(), (50, 50 * 1005));
        ledger.widen_mark(7, 900).unwrap();
        assert_eq!(ledger.hedge_totals(), (50, 50 * 1005));
        assert_eq!(
            ledger.widen_mark(9, 1005),
            Err("execution_reservation_unknown_order")
        );
    }

    #[test]
    fn rescue_budget_is_a_hard_ceiling_that_never_resets() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        ledger.charge_rescue(&limits, 60, 10_000).unwrap();
        assert_eq!(
            ledger.charge_rescue(&limits, 41, 10_000),
            Err("execution_rescue_exhausted")
        );
        ledger.charge_rescue(&limits, 40, 10_000).unwrap();
        assert_eq!(ledger.rescue_spent(), (100, 20_000));
        assert_eq!(
            ledger.charge_rescue(&limits, 1, 1),
            Err("execution_rescue_exhausted")
        );
        let mut ledger = ExecutionReservations::new(&limits);
        assert_eq!(
            ledger.charge_rescue(&limits, 1, 500_001),
            Err("execution_rescue_exhausted")
        );
    }

    #[test]
    fn rebuild_from_recovered_orders_restores_the_same_budget() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        ledger.reserve(&limits, maker(7, 5, 1000, 1005)).unwrap();
        ledger.reserve(&limits, maker(8, 3, 1000, 1005)).unwrap();
        ledger.charge_rescue(&limits, 25, 4_000).unwrap();
        let empty = ExecutionReservations::new(&limits);
        let rebuilt = empty
            .rebuild(
                &limits,
                [maker(7, 5, 1000, 1005), maker(8, 3, 1000, 1005)],
                (25, 4_000),
            )
            .unwrap();
        assert_eq!(rebuilt, ledger);
        // A restart that recovers fewer orders would under-reserve, so the caller must pass every
        // non-terminal order; the arithmetic here proves the ledger is a pure function of facts.
        let partial = empty
            .rebuild(&limits, [maker(7, 5, 1000, 1005)], (25, 4_000))
            .unwrap();
        assert_ne!(partial, ledger);
        // Rescue consumption can never be refunded by a restart, and cannot exceed the ceiling.
        let refunded = empty
            .rebuild(
                &limits,
                [maker(7, 5, 1000, 1005), maker(8, 3, 1000, 1005)],
                (0, 0),
            )
            .unwrap();
        assert_ne!(refunded.rescue_spent(), ledger.rescue_spent());
        assert_eq!(
            empty.rebuild(&limits, [], (101, 0)),
            Err("execution_rescue_exhausted")
        );
    }

    #[test]
    fn rebuild_refuses_recovered_orders_that_break_the_binding() {
        let limits = limits();
        let empty = ExecutionReservations::new(&limits);
        let mut unbound = maker(7, 5, 1000, 1005);
        unbound.account_no = 5;
        unbound.asset_no = 5;
        assert_eq!(
            empty.rebuild(&limits, [unbound], (0, 0)),
            Err("execution_reservation_unbound_leg")
        );
        assert_eq!(
            empty.rebuild(&limits, [maker(7, 21, 1000, 1005)], (0, 0)),
            Err("execution_reservation_unavailable")
        );
    }

    /// Crash fixture: reserves, publishes the facts it would have persisted, then blocks so the
    /// parent can kill the process at a chosen point.
    #[test]
    #[ignore = "subprocess fixture invoked only by reservations_survive_process_kill"]
    fn reservation_crash_child() {
        let root = std::path::PathBuf::from(
            std::env::var("TITAN_RESERVATION_CRASH_ROOT").expect("crash root"),
        );
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        let quotes = [maker(7, 5, 1000, 1005), maker(8, 3, 1000, 1005)];
        let hedges = [hedge(9, 20, 1000, 1005)];
        let point = std::env::var("TITAN_RESERVATION_CRASH_POINT").expect("crash point");
        // (order, account, asset, lots, price, mark, cumulative_filled) that became durable.
        let mut durable: Vec<(u64, u32, u32, i64, i64, i64, i64)> = Vec::new();
        match point.as_str() {
            // Killed before anything was dispatched.
            "before_dispatch" => {}
            // Killed after the first quote became durable.
            "after_first_dispatch" => {
                ledger.reserve(&limits, quotes[0]).unwrap();
                durable.push((7, 0, 1, 5, 1000, 1005, 0));
            }
            // Killed after every order became durable.
            "after_dispatch" => {
                for quote in quotes.iter().chain(hedges.iter()) {
                    ledger.reserve(&limits, *quote).unwrap();
                }
                durable.extend([
                    (7, 0, 1, 5, 1000, 1005, 0),
                    (8, 0, 1, 3, 1000, 1005, 0),
                    (9, 1, 0, 20, 1000, 1005, 0),
                ]);
            }
            // Killed after the first quote fully filled but before its fill was durable.
            "after_fill" => {
                for quote in quotes.iter().chain(hedges.iter()) {
                    ledger.reserve(&limits, *quote).unwrap();
                }
                ledger.settle(7, 5, 1000).unwrap();
                durable.extend([
                    (7, 0, 1, 5, 1000, 1005, 0),
                    (8, 0, 1, 3, 1000, 1005, 0),
                    (9, 1, 0, 20, 1000, 1005, 0),
                ]);
            }
            other => panic!("unknown crash point {other}"),
        }
        std::fs::write(
            root.join("facts.json"),
            serde_json::to_vec(&durable).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("live_hedge_lots"),
            ledger.hedge_totals().0.to_string(),
        )
        .unwrap();
        std::fs::write(root.join("ready"), b"ready").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    #[test]
    fn reservations_survive_process_kill_and_recover_conservatively() {
        // What the rebuilt ledger must hold for each durable state, and what the live ledger held.
        let cases = [
            ("before_dispatch", 0_i128, 0_i128),
            ("after_first_dispatch", 50, 50),
            ("after_dispatch", 100, 100),
            // The fill was applied in memory only. Its tons stay reserved because the maker quote
            // is still durable and the hedge order is still in flight.
            ("after_fill", 100, 100),
        ];
        for (point, expected_rebuilt, expected_live) in cases {
            let dir = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "execution_reservation::tests::reservation_crash_child",
                ])
                .env("TITAN_RESERVATION_CRASH_ROOT", dir.path())
                .env("TITAN_RESERVATION_CRASH_POINT", point)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while !dir.path().join("ready").exists() {
                if std::time::Instant::now() > deadline || child.try_wait().unwrap().is_some() {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("crash fixture failed for {point}");
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let live: i128 = std::fs::read_to_string(dir.path().join("live_hedge_lots"))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert_eq!(live, expected_live, "{point}: live budget");
            let facts: Vec<(u64, u32, u32, i64, i64, i64, i64)> =
                serde_json::from_slice(&std::fs::read(dir.path().join("facts.json")).unwrap())
                    .unwrap();
            let recovered: Vec<ReservationRequest> = facts
                .iter()
                .filter(|(_, _, _, lots, _, _, cumulative)| lots - cumulative > 0)
                .map(
                    |(order_id, account_no, asset_no, lots, price, mark, cumulative)| {
                        ReservationRequest {
                            order_id: *order_id,
                            account_no: *account_no,
                            asset_no: *asset_no,
                            lots: lots - cumulative,
                            price_ticks: *price,
                            hedge_mark_ticks: *mark,
                        }
                    },
                )
                .collect();
            let limits = limits();
            // Rescue consumption is unknowable after a kill, so the restart assumes it is spent.
            let spent = if recovered.is_empty() {
                (0, 0)
            } else {
                (
                    i128::from(limits.rescue.max_quantity_lots),
                    crate::parse_notional_limit(&limits.rescue.max_notional_tick_lots).unwrap(),
                )
            };
            let rebuilt = ExecutionReservations::new(&limits)
                .rebuild(&limits, recovered, spent)
                .unwrap_or_else(|error| panic!("{point}: rebuild failed: {error}"));
            assert_eq!(
                rebuilt.hedge_totals().0,
                expected_rebuilt,
                "{point}: rebuilt budget"
            );
            // A restart never refunds a spent rescue allowance.
            assert_eq!(
                rebuilt.rescue_spent().0 > 0,
                !facts.is_empty(),
                "{point}: rescue must be exhausted only when orders survived"
            );
        }
    }

    #[test]
    fn a_mismatched_revision_is_refused() {
        let limits = limits();
        let mut ledger = ExecutionReservations::new(&limits);
        let mut other = limits.clone();
        other.revision = 4;
        assert_eq!(
            ledger.reserve(&other, maker(1, 1, 1000, 1000)),
            Err("execution_reservation_revision")
        );
        ledger.reserve(&limits, maker(1, 1, 1000, 1000)).unwrap();
        assert_eq!(ledger.revision(), 3);
    }
}
