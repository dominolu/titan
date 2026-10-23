use std::{sync::Arc, time::Instant};

use crate::*;

pub trait AccountAdminService: Send + Sync {
    fn create(&self, definition: AccountDefinition) -> LocalResult<AccountHandle>;
    fn start(&self, account: AccountHandle) -> LocalResult<OperationId>;
    fn stop(&self, account: AccountHandle, deadline: Instant) -> LocalResult<OperationId>;
    fn remove(&self, account: AccountHandle) -> LocalResult<OperationId>;
    fn replace(
        &self,
        account: AccountHandle,
        definition: AccountDefinition,
    ) -> LocalResult<AccountHandle>;
    fn list(&self) -> Arc<[AccountInstanceSnapshot]>;
    fn operation(&self, id: OperationId) -> AccountOperationSnapshot;
}

pub trait AccountService: Send + Sync {
    fn order_evidence(
        &self,
        _account: AccountHandle,
        _asset: AssetId,
        _client: Id128,
    ) -> OrderEvidenceFuture {
        Box::pin(async {
            Err(AccountConnectorError::rejected(
                "order evidence provider unavailable",
            ))
        })
    }
    fn pin_binding(&self, _account: AccountHandle) -> LocalResult<titan_core_types::BindingLease> {
        Ok(Default::default())
    }
    fn resolve(&self, account_key: &str) -> LocalResult<AccountHandle>;
    fn orders(
        &self,
        account: AccountHandle,
        filter: OrderFilter,
    ) -> LocalResult<AccountStateSnapshot<OrderSnapshot>>;
    fn positions(
        &self,
        account: AccountHandle,
        filter: PositionFilter,
    ) -> LocalResult<AccountStateSnapshot<PositionSnapshot>>;
    fn balances(
        &self,
        account: AccountHandle,
    ) -> LocalResult<AccountStateSnapshot<BalanceSnapshot>>;
    /// Retry boundedly until all images represent the same committed account boundary.
    fn recovery_snapshot(&self, account: AccountHandle) -> LocalResult<AccountRecoverySnapshot> {
        for _ in 0..8 {
            let orders = self.orders(
                account,
                OrderFilter {
                    asset_id: None,
                    include_final: true,
                },
            )?;
            let positions = self.positions(account, PositionFilter::default())?;
            let balances = self.balances(account)?;
            if orders.account == account
                && positions.account == account
                && balances.account == account
                && let Ok(snapshot) = AccountRecoverySnapshot::coherent(orders, positions, balances)
            {
                return Ok(snapshot);
            }
        }
        Err(AccountError::new(
            AccountErrorKind::NotReady,
            "recovery account boundary is unstable",
        ))
    }
    fn health(&self, account: AccountHandle) -> LocalResult<AccountConnectorHealthSnapshot>;
    fn diagnostics(
        &self,
        account: AccountHandle,
    ) -> LocalResult<AccountConnectorDiagnosticSnapshot>;
    fn execution_connector(
        &self,
        account: AccountHandle,
    ) -> LocalResult<Arc<dyn DirectExecutionConnector>> {
        let _ = account;
        Err(AccountError::new(
            AccountErrorKind::RuntimeNotActive,
            "direct execution is unavailable through a service adapter",
        ))
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Images {
        incoherent: bool,
        calls: AtomicU64,
    }
    impl Images {
        fn snapshot<T>(&self, account: AccountHandle) -> AccountStateSnapshot<T> {
            let read = self.calls.fetch_add(1, Ordering::Relaxed);
            AccountStateSnapshot {
                account,
                state: AccountSnapshotState::Ready,
                committed_epoch: Some(2),
                committed_version: Some(if self.incoherent { read } else { 17 }),
                captured_at: 0,
                items: Arc::from([]),
            }
        }
    }
    impl AccountService for Images {
        fn resolve(&self, _: &str) -> LocalResult<AccountHandle> {
            unreachable!()
        }
        fn orders(
            &self,
            account: AccountHandle,
            filter: OrderFilter,
        ) -> LocalResult<AccountStateSnapshot<OrderSnapshot>> {
            assert!(filter.include_final);
            Ok(self.snapshot(account))
        }
        fn positions(
            &self,
            account: AccountHandle,
            _: PositionFilter,
        ) -> LocalResult<AccountStateSnapshot<PositionSnapshot>> {
            Ok(self.snapshot(account))
        }
        fn balances(
            &self,
            account: AccountHandle,
        ) -> LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
            Ok(self.snapshot(account))
        }
        fn health(&self, _: AccountHandle) -> LocalResult<AccountConnectorHealthSnapshot> {
            unreachable!()
        }
        fn diagnostics(&self, _: AccountHandle) -> LocalResult<AccountConnectorDiagnosticSnapshot> {
            unreachable!()
        }
    }
    #[test]
    fn recovery_captures_one_committed_account_boundary() {
        let service = Images {
            incoherent: false,
            calls: AtomicU64::new(0),
        };
        let snapshot = service
            .recovery_snapshot(AccountHandle {
                account_id: AccountId(5),
                generation: 1,
            })
            .unwrap();
        assert_eq!(snapshot.orders.committed_version, Some(17));
        assert_eq!(
            snapshot.positions.committed_version,
            snapshot.balances.committed_version
        );
    }
    #[test]
    fn changing_account_versions_fail_instead_of_splicing_different_images() {
        let service = Images {
            incoherent: true,
            calls: AtomicU64::new(0),
        };
        let result = service.recovery_snapshot(AccountHandle {
            account_id: AccountId(5),
            generation: 1,
        });
        assert!(matches!(
            result,
            Err(AccountError {
                kind: AccountErrorKind::NotReady,
                ..
            })
        ));
        assert!(service.calls.load(Ordering::Relaxed) <= 24);
    }
}
