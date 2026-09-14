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
