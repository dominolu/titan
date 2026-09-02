use std::{sync::Arc, time::Instant};

use titan_plugin_engine::{Service, TraceContext, TypedServiceEndpoint};

use crate::*;

pub trait StrategyAdminService: Send + Sync {
    fn create(&self, definition: StrategyDefinition) -> LocalResult<StrategyHandle>;
    fn prepare(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId>;
    fn start(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId>;
    fn pause(
        &self,
        strategy: StrategyHandle,
        reason: PauseReason,
    ) -> LocalResult<StrategyOperationId>;
    fn resume(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId>;
    fn stop(&self, strategy: StrategyHandle, deadline: Instant)
    -> LocalResult<StrategyOperationId>;
    fn replace(
        &self,
        strategy: StrategyHandle,
        definition: StrategyDefinition,
    ) -> LocalResult<StrategyHandle>;
    fn remove(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId>;
    fn checkpoint(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId>;
    fn list(&self) -> Arc<[StrategyInstanceSnapshot]>;
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot;
}

pub trait StrategyService: Send + Sync {
    fn resolve(&self, strategy_key: &str) -> LocalResult<StrategyHandle>;
    fn state(&self, strategy: StrategyHandle) -> LocalResult<StrategyRuntimeStateSnapshot>;
    fn health(&self, strategy: StrategyHandle) -> LocalResult<StrategyRuntimeHealthSnapshot>;
    fn diagnostics(
        &self,
        strategy: StrategyHandle,
    ) -> LocalResult<StrategyRuntimeDiagnosticSnapshot>;
}

#[derive(Clone)]
pub enum StrategyAdminRequest {
    Create(StrategyDefinition),
    Prepare(StrategyHandle),
    Start(StrategyHandle),
    Pause(StrategyHandle, PauseReason),
    Resume(StrategyHandle),
    Stop(StrategyHandle, Instant),
    Replace(StrategyHandle, StrategyDefinition),
    Remove(StrategyHandle),
    Checkpoint(StrategyHandle),
    List,
    Operation(StrategyOperationId),
}

pub enum StrategyAdminResponse {
    Handle(StrategyHandle),
    OperationId(StrategyOperationId),
    Instances(Arc<[StrategyInstanceSnapshot]>),
    Operation(StrategyOperationSnapshot),
}

pub struct StrategyAdminApi;
impl Service for StrategyAdminApi {
    type Request = StrategyAdminRequest;
    type Response = LocalResult<StrategyAdminResponse>;
}

#[derive(Clone)]
pub enum StrategyQueryRequest {
    Resolve(Arc<str>),
    State(StrategyHandle),
    Health(StrategyHandle),
    Diagnostics(StrategyHandle),
}

pub enum StrategyQueryResponse {
    Handle(StrategyHandle),
    State(StrategyRuntimeStateSnapshot),
    Health(StrategyRuntimeHealthSnapshot),
    Diagnostics(StrategyRuntimeDiagnosticSnapshot),
}

pub struct StrategyQueryApi;
impl Service for StrategyQueryApi {
    type Request = StrategyQueryRequest;
    type Response = LocalResult<StrategyQueryResponse>;
}

pub struct StrategyAdminEndpoint(pub Arc<dyn StrategyAdminService>);
impl TypedServiceEndpoint<StrategyAdminApi> for StrategyAdminEndpoint {
    fn call(
        &self,
        request: StrategyAdminRequest,
        _: TraceContext,
    ) -> Result<LocalResult<StrategyAdminResponse>, titan_plugin_engine::PluginError> {
        Ok(match request {
            StrategyAdminRequest::Create(value) => {
                self.0.create(value).map(StrategyAdminResponse::Handle)
            }
            StrategyAdminRequest::Prepare(value) => self
                .0
                .prepare(value)
                .map(StrategyAdminResponse::OperationId),
            StrategyAdminRequest::Start(value) => {
                self.0.start(value).map(StrategyAdminResponse::OperationId)
            }
            StrategyAdminRequest::Pause(value, reason) => self
                .0
                .pause(value, reason)
                .map(StrategyAdminResponse::OperationId),
            StrategyAdminRequest::Resume(value) => {
                self.0.resume(value).map(StrategyAdminResponse::OperationId)
            }
            StrategyAdminRequest::Stop(value, deadline) => self
                .0
                .stop(value, deadline)
                .map(StrategyAdminResponse::OperationId),
            StrategyAdminRequest::Replace(handle, value) => self
                .0
                .replace(handle, value)
                .map(StrategyAdminResponse::Handle),
            StrategyAdminRequest::Remove(value) => {
                self.0.remove(value).map(StrategyAdminResponse::OperationId)
            }
            StrategyAdminRequest::Checkpoint(value) => self
                .0
                .checkpoint(value)
                .map(StrategyAdminResponse::OperationId),
            StrategyAdminRequest::List => Ok(StrategyAdminResponse::Instances(self.0.list())),
            StrategyAdminRequest::Operation(id) => {
                Ok(StrategyAdminResponse::Operation(self.0.operation(id)))
            }
        })
    }
}

pub struct StrategyQueryEndpoint(pub Arc<dyn StrategyService>);
impl TypedServiceEndpoint<StrategyQueryApi> for StrategyQueryEndpoint {
    fn call(
        &self,
        request: StrategyQueryRequest,
        _: TraceContext,
    ) -> Result<LocalResult<StrategyQueryResponse>, titan_plugin_engine::PluginError> {
        Ok(match request {
            StrategyQueryRequest::Resolve(key) => {
                self.0.resolve(&key).map(StrategyQueryResponse::Handle)
            }
            StrategyQueryRequest::State(handle) => {
                self.0.state(handle).map(StrategyQueryResponse::State)
            }
            StrategyQueryRequest::Health(handle) => {
                self.0.health(handle).map(StrategyQueryResponse::Health)
            }
            StrategyQueryRequest::Diagnostics(handle) => self
                .0
                .diagnostics(handle)
                .map(StrategyQueryResponse::Diagnostics),
        })
    }
}
