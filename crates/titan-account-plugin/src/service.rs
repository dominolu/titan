use std::{sync::Arc, time::Instant};

use titan_plugin_engine::{Service, TraceContext, TypedServiceEndpoint};

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
    fn reconcile(&self, account: AccountHandle, scope: ReconcileScope) -> LocalResult<OperationId>;
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
}

pub trait AccountExecutionService: Send + Sync {
    fn submit(
        &self,
        account: AccountHandle,
        command: SubmitOrderCommand,
    ) -> LocalResult<AccountCommandReceipt>;
    fn amend(
        &self,
        account: AccountHandle,
        command: AmendOrderCommand,
    ) -> LocalResult<AccountCommandReceipt>;
    fn cancel(
        &self,
        account: AccountHandle,
        command: CancelOrderCommand,
    ) -> LocalResult<AccountCommandReceipt>;
    fn cancel_all(
        &self,
        account: AccountHandle,
        command: CancelAllCommand,
    ) -> LocalResult<AccountCommandReceipt>;
    fn cancel_all_after(
        &self,
        account: AccountHandle,
        command: CancelAllAfterCommand,
    ) -> LocalResult<AccountCommandReceipt>;
}

#[derive(Debug)]
pub enum AccountAdminRequest {
    Create(AccountDefinition),
    Start(AccountHandle),
    Stop(AccountHandle, Instant),
    Remove(AccountHandle),
    Replace(AccountHandle, AccountDefinition),
    Reconcile(AccountHandle, ReconcileScope),
    List,
    Operation(OperationId),
}
#[derive(Debug)]
pub enum AccountAdminResponse {
    Handle(AccountHandle),
    OperationId(OperationId),
    Accounts(Arc<[AccountInstanceSnapshot]>),
    Operation(AccountOperationSnapshot),
}
pub struct AccountAdminApi;
impl Service for AccountAdminApi {
    type Request = AccountAdminRequest;
    type Response = LocalResult<AccountAdminResponse>;
}

#[derive(Debug)]
pub enum AccountRequest {
    Resolve(Arc<str>),
    Orders(AccountHandle, OrderFilter),
    Positions(AccountHandle, PositionFilter),
    Balances(AccountHandle),
    Health(AccountHandle),
    Diagnostics(AccountHandle),
}
#[derive(Debug)]
pub enum AccountResponse {
    Handle(AccountHandle),
    Orders(AccountStateSnapshot<OrderSnapshot>),
    Positions(AccountStateSnapshot<PositionSnapshot>),
    Balances(AccountStateSnapshot<BalanceSnapshot>),
    Health(AccountConnectorHealthSnapshot),
    Diagnostics(AccountConnectorDiagnosticSnapshot),
}
pub struct AccountApi;
impl Service for AccountApi {
    type Request = AccountRequest;
    type Response = LocalResult<AccountResponse>;
}

#[derive(Debug)]
pub enum AccountExecutionRequest {
    Submit(AccountHandle, SubmitOrderCommand),
    Amend(AccountHandle, AmendOrderCommand),
    Cancel(AccountHandle, CancelOrderCommand),
    CancelAll(AccountHandle, CancelAllCommand),
    CancelAllAfter(AccountHandle, CancelAllAfterCommand),
}
#[derive(Debug)]
pub struct AccountExecutionResponse(pub AccountCommandReceipt);
pub struct AccountExecutionApi;
impl Service for AccountExecutionApi {
    type Request = AccountExecutionRequest;
    type Response = LocalResult<AccountExecutionResponse>;
}

pub(crate) struct AdminEndpoint(pub Arc<dyn AccountAdminService>);
impl TypedServiceEndpoint<AccountAdminApi> for AdminEndpoint {
    fn call(
        &self,
        r: AccountAdminRequest,
        _: TraceContext,
    ) -> Result<LocalResult<AccountAdminResponse>, titan_plugin_engine::PluginError> {
        Ok(match r {
            AccountAdminRequest::Create(v) => self.0.create(v).map(AccountAdminResponse::Handle),
            AccountAdminRequest::Start(v) => self.0.start(v).map(AccountAdminResponse::OperationId),
            AccountAdminRequest::Stop(v, d) => {
                self.0.stop(v, d).map(AccountAdminResponse::OperationId)
            }
            AccountAdminRequest::Remove(v) => {
                self.0.remove(v).map(AccountAdminResponse::OperationId)
            }
            AccountAdminRequest::Replace(h, v) => {
                self.0.replace(h, v).map(AccountAdminResponse::Handle)
            }
            AccountAdminRequest::Reconcile(h, s) => self
                .0
                .reconcile(h, s)
                .map(AccountAdminResponse::OperationId),
            AccountAdminRequest::List => Ok(AccountAdminResponse::Accounts(self.0.list())),
            AccountAdminRequest::Operation(id) => {
                Ok(AccountAdminResponse::Operation(self.0.operation(id)))
            }
        })
    }
}

pub(crate) struct QueryEndpoint(pub Arc<dyn AccountService>);
impl TypedServiceEndpoint<AccountApi> for QueryEndpoint {
    fn call(
        &self,
        r: AccountRequest,
        _: TraceContext,
    ) -> Result<LocalResult<AccountResponse>, titan_plugin_engine::PluginError> {
        Ok(match r {
            AccountRequest::Resolve(k) => self.0.resolve(&k).map(AccountResponse::Handle),
            AccountRequest::Orders(h, f) => self.0.orders(h, f).map(AccountResponse::Orders),
            AccountRequest::Positions(h, f) => {
                self.0.positions(h, f).map(AccountResponse::Positions)
            }
            AccountRequest::Balances(h) => self.0.balances(h).map(AccountResponse::Balances),
            AccountRequest::Health(h) => self.0.health(h).map(AccountResponse::Health),
            AccountRequest::Diagnostics(h) => {
                self.0.diagnostics(h).map(AccountResponse::Diagnostics)
            }
        })
    }
}

pub(crate) struct ExecutionEndpoint(pub Arc<dyn AccountExecutionService>);
impl TypedServiceEndpoint<AccountExecutionApi> for ExecutionEndpoint {
    fn call(
        &self,
        r: AccountExecutionRequest,
        trace: TraceContext,
    ) -> Result<LocalResult<AccountExecutionResponse>, titan_plugin_engine::PluginError> {
        Ok(match r {
            AccountExecutionRequest::Submit(h, mut c) => {
                c.trace = trace;
                self.0.submit(h, c)
            }
            AccountExecutionRequest::Amend(h, mut c) => {
                c.trace = trace;
                self.0.amend(h, c)
            }
            AccountExecutionRequest::Cancel(h, mut c) => {
                c.trace = trace;
                self.0.cancel(h, c)
            }
            AccountExecutionRequest::CancelAll(h, mut c) => {
                c.trace = trace;
                self.0.cancel_all(h, c)
            }
            AccountExecutionRequest::CancelAllAfter(h, mut c) => {
                c.trace = trace;
                self.0.cancel_all_after(h, c)
            }
        }
        .map(AccountExecutionResponse))
    }
}
