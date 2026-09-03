//! In-process typed-service adapters used when StrategyPlugin is assembled by PluginEngine.
//!
//! These adapters do not perform RPC. They retain a generation-aware local `ServiceHandle` and
//! translate the typed endpoint response into the domain service trait expected by StrategyPlugin.

use std::sync::{Arc, RwLock};

use titan_account_plugin::{
    AccountApi, AccountError, AccountErrorKind, AccountExecutionApi, AccountExecutionRequest,
    AccountExecutionService, AccountHandle, AccountRequest, AccountResponse, AccountService,
    AccountStateSnapshot, AmendOrderCommand, BalanceSnapshot, CancelAllAfterCommand,
    CancelAllCommand, CancelOrderCommand, OrderFilter, OrderSnapshot, PositionFilter,
    PositionSnapshot, SubmitOrderCommand,
};
use titan_market_plugin::{
    ConnectorHealthSnapshot, ConnectorOperationSnapshot, InstrumentSnapshot, MarketApi,
    MarketError, MarketErrorKind, MarketRequest, MarketResponse, MarketService, MarketSourceHandle,
    MarketSubscribeRequest, MarketSubscription, OperationId,
};
use titan_plugin_engine::{
    BoundServices, ErrorKind, LifecycleState, PluginError, PluginIdentity, ServiceHandle,
    ServiceId, ServiceKey, ServiceScope, TraceContext,
};

#[derive(Clone)]
pub struct PluginMarketService(pub ServiceHandle<MarketApi>);

impl PluginMarketService {
    fn call(&self, request: MarketRequest) -> titan_market_plugin::LocalResult<MarketResponse> {
        self.0
            .call(request, TraceContext::default())
            .map_err(|error| {
                MarketError::new(MarketErrorKind::ConnectorRejected, error.to_string())
            })?
    }

    fn unexpected(operation: &'static str) -> MarketError {
        MarketError::new(
            MarketErrorKind::ConnectorRejected,
            format!("unexpected MarketService response during {operation}"),
        )
    }
}

impl MarketService for PluginMarketService {
    fn resolve(&self, key: &str) -> titan_market_plugin::LocalResult<MarketSourceHandle> {
        match self.call(MarketRequest::Resolve(Arc::from(key)))? {
            MarketResponse::Handle(value) => Ok(value),
            _ => Err(Self::unexpected("resolve")),
        }
    }

    fn subscribe(
        &self,
        source: MarketSourceHandle,
        request: MarketSubscribeRequest,
    ) -> titan_market_plugin::LocalResult<MarketSubscription> {
        match self.call(MarketRequest::Subscribe(source, request))? {
            MarketResponse::Subscription(value) => Ok(value),
            _ => Err(Self::unexpected("subscribe")),
        }
    }

    fn unsubscribe(
        &self,
        source: MarketSourceHandle,
        subscription: MarketSubscription,
    ) -> titan_market_plugin::LocalResult<OperationId> {
        match self.call(MarketRequest::Unsubscribe(source, subscription))? {
            MarketResponse::OperationId(value) => Ok(value),
            _ => Err(Self::unexpected("unsubscribe")),
        }
    }

    fn request_snapshot(
        &self,
        source: MarketSourceHandle,
        asset: titan_market_plugin::AssetId,
    ) -> titan_market_plugin::LocalResult<OperationId> {
        match self.call(MarketRequest::Snapshot(source, asset))? {
            MarketResponse::OperationId(value) => Ok(value),
            _ => Err(Self::unexpected("request_snapshot")),
        }
    }

    fn instruments(
        &self,
        source: MarketSourceHandle,
    ) -> titan_market_plugin::LocalResult<Arc<[InstrumentSnapshot]>> {
        match self.call(MarketRequest::Instruments(source))? {
            MarketResponse::Instruments(value) => Ok(value),
            _ => Err(Self::unexpected("instruments")),
        }
    }

    fn health(
        &self,
        source: MarketSourceHandle,
    ) -> titan_market_plugin::LocalResult<ConnectorHealthSnapshot> {
        match self.call(MarketRequest::Health(source))? {
            MarketResponse::Health(value) => Ok(value),
            _ => Err(Self::unexpected("health")),
        }
    }

    fn operation(
        &self,
        source: MarketSourceHandle,
        operation: OperationId,
    ) -> titan_market_plugin::LocalResult<ConnectorOperationSnapshot> {
        match self.call(MarketRequest::Operation(source, operation))? {
            MarketResponse::Operation(value) => Ok(value),
            _ => Err(Self::unexpected("operation")),
        }
    }
}

#[derive(Clone)]
pub struct PluginAccountService(pub ServiceHandle<AccountApi>);

impl PluginAccountService {
    fn call(&self, request: AccountRequest) -> titan_account_plugin::LocalResult<AccountResponse> {
        self.0
            .call(request, TraceContext::default())
            .map_err(|error| {
                AccountError::new(AccountErrorKind::ConnectorRejected, error.to_string())
            })?
    }

    fn unexpected(operation: &'static str) -> AccountError {
        AccountError::new(
            AccountErrorKind::ConnectorRejected,
            format!("unexpected AccountService response during {operation}"),
        )
    }
}

impl AccountService for PluginAccountService {
    fn resolve(&self, key: &str) -> titan_account_plugin::LocalResult<AccountHandle> {
        match self.call(AccountRequest::Resolve(Arc::from(key)))? {
            AccountResponse::Handle(value) => Ok(value),
            _ => Err(Self::unexpected("resolve")),
        }
    }

    fn orders(
        &self,
        account: AccountHandle,
        filter: OrderFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<OrderSnapshot>> {
        match self.call(AccountRequest::Orders(account, filter))? {
            AccountResponse::Orders(value) => Ok(value),
            _ => Err(Self::unexpected("orders")),
        }
    }

    fn positions(
        &self,
        account: AccountHandle,
        filter: PositionFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<PositionSnapshot>> {
        match self.call(AccountRequest::Positions(account, filter))? {
            AccountResponse::Positions(value) => Ok(value),
            _ => Err(Self::unexpected("positions")),
        }
    }

    fn balances(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
        match self.call(AccountRequest::Balances(account))? {
            AccountResponse::Balances(value) => Ok(value),
            _ => Err(Self::unexpected("balances")),
        }
    }

    fn health(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountConnectorHealthSnapshot>
    {
        match self.call(AccountRequest::Health(account))? {
            AccountResponse::Health(value) => Ok(value),
            _ => Err(Self::unexpected("health")),
        }
    }

    fn diagnostics(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountConnectorDiagnosticSnapshot>
    {
        match self.call(AccountRequest::Diagnostics(account))? {
            AccountResponse::Diagnostics(value) => Ok(value),
            _ => Err(Self::unexpected("diagnostics")),
        }
    }
}

#[derive(Clone)]
pub struct PluginAccountExecutionService(pub ServiceHandle<AccountExecutionApi>);

impl PluginAccountExecutionService {
    fn call(
        &self,
        request: AccountExecutionRequest,
        trace: TraceContext,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.0
            .call(request, trace)
            .map_err(|error| {
                AccountError::new(AccountErrorKind::ConnectorRejected, error.to_string())
            })?
            .map(|response| response.0)
    }
}

impl AccountExecutionService for PluginAccountExecutionService {
    fn submit(
        &self,
        account: AccountHandle,
        command: SubmitOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        let trace = command.trace;
        self.call(AccountExecutionRequest::Submit(account, command), trace)
    }

    fn amend(
        &self,
        account: AccountHandle,
        command: AmendOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        let trace = command.trace;
        self.call(AccountExecutionRequest::Amend(account, command), trace)
    }

    fn cancel(
        &self,
        account: AccountHandle,
        command: CancelOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        let trace = command.trace;
        self.call(AccountExecutionRequest::Cancel(account, command), trace)
    }

    fn cancel_all(
        &self,
        account: AccountHandle,
        command: CancelAllCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        let trace = command.trace;
        self.call(AccountExecutionRequest::CancelAll(account, command), trace)
    }

    fn cancel_all_after(
        &self,
        account: AccountHandle,
        command: CancelAllAfterCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        let trace = command.trace;
        self.call(
            AccountExecutionRequest::CancelAllAfter(account, command),
            trace,
        )
    }
}

#[derive(Clone)]
struct BoundStrategyServices {
    market: PluginMarketService,
    account: PluginAccountService,
    execution: PluginAccountExecutionService,
}

/// Late-bound in-process service facade used by `StrategyPluginFactory`.
///
/// Plugin factories are created before a PluginPlan stages its service slots. The lifecycle binds
/// the generation-aware handles supplied by PluginEngine during `start`, before any StrategyAdmin
/// endpoint can pass its ActivationGate.
#[derive(Default)]
pub struct PluginStrategyServices {
    bound: RwLock<Option<BoundStrategyServices>>,
}

impl PluginStrategyServices {
    pub fn bind(&self, services: &BoundServices) -> Result<(), PluginError> {
        let bound = BoundStrategyServices {
            market: PluginMarketService(services.require(&service_key("titan.market", "market"))?),
            account: PluginAccountService(
                services.require(&service_key("titan.account", "query"))?,
            ),
            execution: PluginAccountExecutionService(
                services.require(&service_key("titan.account", "execution"))?,
            ),
        };
        *self
            .bound
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(bound);
        Ok(())
    }

    pub fn clear(&self) {
        *self
            .bound
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn services(&self) -> Result<BoundStrategyServices, PluginError> {
        self.bound
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| {
                PluginError::new(
                    ErrorKind::ServiceUnavailable,
                    PluginIdentity::new("titan.strategy", "service-bindings"),
                    LifecycleState::Starting,
                    "bind_strategy_services",
                    "strategy dependencies are unavailable",
                )
            })
    }
}

impl MarketService for PluginStrategyServices {
    fn resolve(&self, key: &str) -> titan_market_plugin::LocalResult<MarketSourceHandle> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .resolve(key)
    }

    fn subscribe(
        &self,
        source: MarketSourceHandle,
        request: MarketSubscribeRequest,
    ) -> titan_market_plugin::LocalResult<MarketSubscription> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .subscribe(source, request)
    }

    fn unsubscribe(
        &self,
        source: MarketSourceHandle,
        subscription: MarketSubscription,
    ) -> titan_market_plugin::LocalResult<OperationId> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .unsubscribe(source, subscription)
    }

    fn request_snapshot(
        &self,
        source: MarketSourceHandle,
        asset: titan_market_plugin::AssetId,
    ) -> titan_market_plugin::LocalResult<OperationId> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .request_snapshot(source, asset)
    }

    fn instruments(
        &self,
        source: MarketSourceHandle,
    ) -> titan_market_plugin::LocalResult<Arc<[InstrumentSnapshot]>> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .instruments(source)
    }

    fn health(
        &self,
        source: MarketSourceHandle,
    ) -> titan_market_plugin::LocalResult<ConnectorHealthSnapshot> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .health(source)
    }

    fn operation(
        &self,
        source: MarketSourceHandle,
        operation: OperationId,
    ) -> titan_market_plugin::LocalResult<ConnectorOperationSnapshot> {
        self.services()
            .map_err(market_binding_error)?
            .market
            .operation(source, operation)
    }
}

impl AccountService for PluginStrategyServices {
    fn resolve(&self, key: &str) -> titan_account_plugin::LocalResult<AccountHandle> {
        self.services()
            .map_err(account_binding_error)?
            .account
            .resolve(key)
    }

    fn orders(
        &self,
        account: AccountHandle,
        filter: OrderFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<OrderSnapshot>> {
        self.services()
            .map_err(account_binding_error)?
            .account
            .orders(account, filter)
    }

    fn positions(
        &self,
        account: AccountHandle,
        filter: PositionFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<PositionSnapshot>> {
        self.services()
            .map_err(account_binding_error)?
            .account
            .positions(account, filter)
    }

    fn balances(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
        self.services()
            .map_err(account_binding_error)?
            .account
            .balances(account)
    }

    fn health(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountConnectorHealthSnapshot>
    {
        self.services()
            .map_err(account_binding_error)?
            .account
            .health(account)
    }

    fn diagnostics(
        &self,
        account: AccountHandle,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountConnectorDiagnosticSnapshot>
    {
        self.services()
            .map_err(account_binding_error)?
            .account
            .diagnostics(account)
    }
}

impl AccountExecutionService for PluginStrategyServices {
    fn submit(
        &self,
        account: AccountHandle,
        command: SubmitOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.services()
            .map_err(account_binding_error)?
            .execution
            .submit(account, command)
    }

    fn amend(
        &self,
        account: AccountHandle,
        command: AmendOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.services()
            .map_err(account_binding_error)?
            .execution
            .amend(account, command)
    }

    fn cancel(
        &self,
        account: AccountHandle,
        command: CancelOrderCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.services()
            .map_err(account_binding_error)?
            .execution
            .cancel(account, command)
    }

    fn cancel_all(
        &self,
        account: AccountHandle,
        command: CancelAllCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.services()
            .map_err(account_binding_error)?
            .execution
            .cancel_all(account, command)
    }

    fn cancel_all_after(
        &self,
        account: AccountHandle,
        command: CancelAllAfterCommand,
    ) -> titan_account_plugin::LocalResult<titan_account_plugin::AccountCommandReceipt> {
        self.services()
            .map_err(account_binding_error)?
            .execution
            .cancel_all_after(account, command)
    }
}

fn service_key(namespace: &str, name: &str) -> ServiceKey {
    ServiceKey {
        id: ServiceId::new(namespace, name),
        version: semver::Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}

fn market_binding_error(error: PluginError) -> MarketError {
    MarketError::new(MarketErrorKind::ConnectorRejected, error.to_string())
}

fn account_binding_error(error: PluginError) -> AccountError {
    AccountError::new(AccountErrorKind::ConnectorRejected, error.to_string())
}
