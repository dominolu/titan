//! Strategy-independent reconciliation. Never infer missing fills from net positions.
use crate::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoveryBaseline {
    pub version: u32,
    pub pending_execution: bool,
    pub public: StrategyPublicStateSeedV13,
    pub checksum: [u8; 32],
}

impl RecoveryBaseline {
    pub(crate) fn capture(
        public: StrategyPublicStateSeedV13,
        snapshot: [u8; 32],
        orders: [u8; 32],
    ) -> Self {
        let mut baseline = Self {
            version: 1,
            pending_execution: false,
            public,
            checksum: [0; 32],
        };
        baseline.checksum = baseline.digest(snapshot, orders);
        baseline
    }
    pub(crate) fn with_pending_execution(
        mut self,
        pending: bool,
        snapshot: [u8; 32],
        orders: [u8; 32],
    ) -> Self {
        self.pending_execution = pending;
        self.checksum = self.digest(snapshot, orders);
        self
    }
    fn digest(&self, snapshot: [u8; 32], orders: [u8; 32]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"titan.recovery.baseline.v1");
        digest.update(self.version.to_le_bytes());
        digest.update([u8::from(self.pending_execution)]);
        digest.update(snapshot);
        digest.update(orders);
        // Typed vectors have deterministic field order and no maps/padding bytes.
        digest.update(serde_json::to_vec(&self.public).expect("integer recovery views serialize"));
        digest.finalize().into()
    }
    pub(crate) fn validate(
        &self,
        snapshot: [u8; 32],
        orders: [u8; 32],
        identity: [u8; 32],
    ) -> Result<(), &'static str> {
        if self.version != 1
            || self.checksum != self.digest(snapshot, orders)
            || public_state_identity_v13(
                &self.public.accounts,
                &self.public.active_orders,
                &self.public.positions,
                &self.public.balances,
            ) != identity
        {
            return Err("recovery_baseline_invalid");
        }
        if self.pending_execution {
            return Err("recovery_checkpoint_has_pending_fill");
        }
        Ok(())
    }
}

/// Complete known-order results for unchanged executions. Changed fills need a future
/// complete, deduplicated execution-history provider; current REST images cannot prove them.
pub(crate) fn reconcile_recovery(
    baseline: &RecoveryBaseline,
    current: &StrategyPublicStateSeedV13,
    records: &mut BTreeMap<u64, StrategyOrderRecordV13>,
) -> Result<(), &'static str> {
    if baseline.pending_execution {
        return Err("recovery_checkpoint_has_pending_fill");
    }
    let old = &baseline.public;
    let accounts: BTreeMap<_, _> = current.accounts.iter().map(|v| (v.account_no, v)).collect();
    if accounts.len() != current.accounts.len()
        || accounts.len() != old.accounts.len()
        || old.accounts.iter().any(|v| {
            accounts
                .get(&v.account_no)
                .is_none_or(|now| now.state != 4 || now.account_epoch == 0)
        })
    {
        return Err("recovery_account_not_ready");
    }
    let positions: BTreeMap<_, _> = current
        .positions
        .iter()
        .map(|v| ((v.account_no, v.asset_no, v.position_side), v))
        .collect();
    if positions.len() != current.positions.len()
        || positions.len() != old.positions.len()
        || old.positions.iter().any(|v| {
            positions
                .get(&(v.account_no, v.asset_no, v.position_side))
                .is_none_or(|now| {
                    (
                        now.qty_lots,
                        now.average_price_ticks,
                        now.realized_pnl_ticks,
                    ) != (v.qty_lots, v.average_price_ticks, v.realized_pnl_ticks)
                })
        })
    {
        return Err("recovery_position_change_requires_history");
    }
    let balances: BTreeMap<_, _> = current
        .balances
        .iter()
        .map(|v| ((v.account_no, v.currency_no), v))
        .collect();
    if balances.len() != current.balances.len()
        || balances.len() != old.balances.len()
        || old
            .balances
            .iter()
            .any(|v| !balances.contains_key(&(v.account_no, v.currency_no)))
    {
        return Err("recovery_balance_binding_changed");
    }
    let results: BTreeMap<_, _> = current
        .recovery_orders
        .iter()
        .map(|v| ((v.account_no, v.order_id), v))
        .collect();
    if results.len() != current.recovery_orders.len() {
        return Err("recovery_duplicate_order");
    }
    for now in current.active_orders.iter() {
        if records.get(&now.order_id).is_none_or(|prior| {
            prior.account_no != now.account_no
                || prior.asset_no != now.asset_no
                || matches!(prior.status, 4 | 6 | 7 | 8)
        }) {
            return Err("recovery_unknown_active_order");
        }
    }
    for now in current.active_orders.iter() {
        let result = results
            .get(&(now.account_no, now.order_id))
            .ok_or("recovery_seed_not_stable")?;
        if (
            now.status,
            now.cumulative_filled_lots,
            now.qty_lots,
            now.price_ticks,
        ) != (
            result.status,
            result.cumulative_filled_lots,
            result.qty_lots,
            result.price_ticks,
        ) {
            return Err("recovery_seed_not_stable");
        }
    }
    for prior in records.values_mut() {
        let result = results.get(&(prior.account_no, prior.order_id));
        if result.is_none() && matches!(prior.status, 4 | 6 | 7 | 8) {
            continue;
        }
        let now = result.ok_or("recovery_order_result_missing")?;
        if (
            now.account_no,
            now.asset_no,
            now.side,
            now.order_type,
            now.time_in_force,
            now.price_ticks,
            now.qty_lots,
            now.reduce_only,
            now.position_side,
        ) != (
            prior.account_no,
            prior.asset_no,
            prior.side,
            prior.order_type,
            prior.time_in_force,
            prior.price_ticks,
            prior.qty_lots,
            prior.reduce_only,
            prior.position_side,
        ) {
            return Err("recovery_order_identity_changed");
        }
        if now.cumulative_filled_lots != prior.cumulative_filled_lots {
            return Err("recovery_fill_change_requires_history");
        }
        if now.qty_lots <= 0
            || now.cumulative_filled_lots < 0
            || now.cumulative_filled_lots > now.qty_lots
            || !matches!(now.status, 2 | 3 | 4 | 6 | 7 | 8)
            || (matches!(prior.status, 4 | 6 | 7 | 8) && now.status != prior.status)
        {
            return Err("recovery_order_outcome_unknown");
        }
        prior.status = now.status;
        prior.account_sequence = now.account_sequence;
        prior.last_event_ts_ns = now.updated_ts_ns;
    }
    Ok(())
}

pub(crate) fn recovery_seed(
    accounts: &[TitanAccountView],
    positions: &[TitanPositionView],
    balances: &[TitanBalanceView],
    active_orders: &[TitanActiveOrderView],
    recovery_orders: &[TitanActiveOrderView],
) -> StrategyPublicStateSeedV13 {
    StrategyPublicStateSeedV13 {
        accounts: Arc::from(accounts),
        positions: Arc::from(positions),
        balances: Arc::from(balances),
        active_orders: Arc::from(active_orders),
        recovery_orders: Arc::from(recovery_orders),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (
        RecoveryBaseline,
        StrategyPublicStateSeedV13,
        BTreeMap<u64, StrategyOrderRecordV13>,
    ) {
        let order = TitanActiveOrderView {
            order_id: 7,
            account_no: 0,
            asset_no: 0,
            price_ticks: 100,
            qty_lots: 2,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            status: 2,
            account_sequence: 5,
            ..Default::default()
        };
        let public = recovery_seed(
            &[TitanAccountView {
                state: 4,
                account_epoch: 1,
                account_sequence: 5,
                ..Default::default()
            }],
            &[TitanPositionView::default()],
            &[TitanBalanceView {
                total_units: 100,
                available_units: 100,
                ..Default::default()
            }],
            &[order],
            &[order],
        );
        let baseline = RecoveryBaseline::capture(public.clone(), [1; 32], [2; 32]);
        let record = StrategyOrderRecordV13 {
            order_id: 7,
            price_ticks: 100,
            qty_lots: 2,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            status: 2,
            ..Default::default()
        };
        (baseline, public, BTreeMap::from([(7, record)]))
    }
    #[test]
    fn session_and_balance_changes_reconcile_without_replaying_trades() {
        let (baseline, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.accounts)[0].account_epoch = 2;
        Arc::make_mut(&mut current.accounts)[0].account_sequence = 99;
        Arc::make_mut(&mut current.balances)[0].total_units = 120;
        reconcile_recovery(&baseline, &current, &mut records).unwrap();
    }
    #[test]
    fn confirmed_cancellation_is_consumed_by_the_common_order_history() {
        let (baseline, mut current, mut records) = fixture();
        current.active_orders = Arc::from([]);
        Arc::make_mut(&mut current.recovery_orders)[0].status = 6;
        reconcile_recovery(&baseline, &current, &mut records).unwrap();
        assert_eq!(records[&7].status, 6);
    }
    #[test]
    fn missing_order_is_never_inferred_to_be_canceled() {
        let (baseline, mut current, mut records) = fixture();
        current.active_orders = Arc::from([]);
        current.recovery_orders = Arc::from([]);
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_order_result_missing")
        );
    }
    #[test]
    fn changed_fills_require_complete_execution_history_even_with_same_net_position() {
        let (baseline, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.recovery_orders)[0].cumulative_filled_lots = 1;
        Arc::make_mut(&mut current.active_orders)[0].cumulative_filled_lots = 1;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_fill_change_requires_history")
        );
    }
    #[test]
    fn unknown_orders_and_changed_order_identity_are_rejected() {
        let (baseline, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.active_orders)[0].order_id = 8;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_unknown_active_order")
        );
        let (_, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.recovery_orders)[0].qty_lots = 3;
        Arc::make_mut(&mut current.active_orders)[0].qty_lots = 3;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_order_identity_changed")
        );
    }
    #[test]
    fn hedge_recovery_preserves_sides_even_when_net_exposure_is_unchanged() {
        let (mut baseline, mut current, mut records) = fixture();
        current.positions = Arc::from([
            TitanPositionView {
                position_side: 1,
                qty_lots: 2,
                ..Default::default()
            },
            TitanPositionView {
                position_side: 2,
                qty_lots: -1,
                ..Default::default()
            },
        ]);
        baseline.public.positions = current.positions.clone();
        assert!(reconcile_recovery(&baseline, &current, &mut records).is_ok());
        Arc::make_mut(&mut current.positions)[0].qty_lots = 3;
        Arc::make_mut(&mut current.positions)[1].qty_lots = -2;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_position_change_requires_history")
        );
    }

    #[test]
    fn changed_positions_and_unready_accounts_do_not_reopen_trading() {
        let (baseline, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.positions)[0].qty_lots = 1;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_position_change_requires_history")
        );
        let (_, mut current, mut records) = fixture();
        Arc::make_mut(&mut current.accounts)[0].state = 3;
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_account_not_ready")
        );
    }
    #[test]
    fn incomplete_checkpoint_does_not_hide_a_fill_delivery_gap() {
        let (baseline, current, mut records) = fixture();
        let baseline = baseline.with_pending_execution(true, [1; 32], [2; 32]);
        let identity = public_state_identity_v13(
            &baseline.public.accounts,
            &baseline.public.active_orders,
            &baseline.public.positions,
            &baseline.public.balances,
        );
        assert_eq!(
            baseline.validate([1; 32], [2; 32], identity),
            Err("recovery_checkpoint_has_pending_fill")
        );
        assert_eq!(
            reconcile_recovery(&baseline, &current, &mut records),
            Err("recovery_checkpoint_has_pending_fill")
        );
    }
    #[test]
    fn baseline_checksum_binds_public_facts_private_snapshot_and_order_history() {
        let (mut baseline, _, _) = fixture();
        let identity = public_state_identity_v13(
            &baseline.public.accounts,
            &baseline.public.active_orders,
            &baseline.public.positions,
            &baseline.public.balances,
        );
        baseline.validate([1; 32], [2; 32], identity).unwrap();
        assert!(baseline.validate([3; 32], [2; 32], identity).is_err());
        assert!(baseline.validate([1; 32], [3; 32], identity).is_err());
        Arc::make_mut(&mut baseline.public.balances)[0].total_units += 1;
        assert!(baseline.validate([1; 32], [2; 32], identity).is_err());
    }
}
