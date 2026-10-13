
#[cfg(test)]
mod review_probes {
    use super::*;
    use account::AccountService;
    struct StableProjected(AccountProjection);
    impl StableProjected {
        fn snapshot<T>(&self, version: u64, items: Arc<[T]>) -> account::AccountStateSnapshot<T> {
            account::AccountStateSnapshot {
                account: account::AccountHandle { account_id: account::AccountId(1), generation: 1 },
                state: account::AccountSnapshotState::Ready,
                committed_epoch: Some(self.0.epoch), committed_version: Some(version),
                captured_at: 0, items,
            }
        }
    }
    impl AccountService for StableProjected {
        fn resolve(&self, _: &str) -> account::LocalResult<account::AccountHandle> { unreachable!() }
        fn orders(&self, _: account::AccountHandle, filter: account::OrderFilter) -> account::LocalResult<account::AccountStateSnapshot<account::OrderSnapshot>> {
            let (version, items) = self.0.orders(filter); Ok(self.snapshot(version, items))
        }
        fn positions(&self, _: account::AccountHandle, filter: account::PositionFilter) -> account::LocalResult<account::AccountStateSnapshot<account::PositionSnapshot>> {
            let (version, items) = self.0.positions(filter); Ok(self.snapshot(version, items))
        }
        fn balances(&self, _: account::AccountHandle) -> account::LocalResult<account::AccountStateSnapshot<account::BalanceSnapshot>> {
            let (version, items) = self.0.balances(); Ok(self.snapshot(version, items))
        }
        fn health(&self, _: account::AccountHandle) -> account::LocalResult<account::AccountConnectorHealthSnapshot> { unreachable!() }
        fn diagnostics(&self, _: account::AccountHandle) -> account::LocalResult<account::AccountConnectorDiagnosticSnapshot> { unreachable!() }
    }
    #[test]
    fn review_stable_real_projection_must_be_recoverable() {
        let mut image = AccountProjection::default();
        image.order(&account::OrderChangedV1 {
            header: account::AccountEventHeaderV1 { account_epoch: 1, account_version: 1, ..Default::default() },
            status: 1, ..Default::default()
        });
        image.position(&account::PositionChangedV1 {
            header: account::AccountEventHeaderV1 { account_epoch: 1, account_version: 2, ..Default::default() },
            ..Default::default()
        });
        image.balance(&account::BalanceChangedV1 {
            header: account::AccountEventHeaderV1 { account_epoch: 1, account_version: 3, ..Default::default() },
            ..Default::default()
        });
        println!("Stable committed real projection: orders={}, positions={}, balances={}", image.orders(account::OrderFilter::default()).0, image.positions(account::PositionFilter::default()).0, image.balances().0);
        let service = StableProjected(image);
        assert!(service.recovery_snapshot(account::AccountHandle { account_id: account::AccountId(1), generation: 1 }).is_ok(), "stable real projection is incorrectly rejected");
    }
    #[test]
    fn review_missing_rest_position_must_not_force_net_mode() {
        let mut image = AccountProjection::default();
        image.position(&account::PositionChangedV1 { asset_id: 7, position_side: 0, quantity_lots: 0, ..Default::default() });
        assert!(image.position_mode_matches(7, 1), "synthetic NET zero forbids the first real LONG update");
    }
}
