use std::{sync::Arc, time::Instant};

use titan_plugin_engine::{Service, TraceContext, TypedServiceEndpoint};

use crate::{
    AssetId, ConnectorHealthSnapshot, ConnectorOperationSnapshot, InstrumentSnapshot, LocalResult,
    MarketOperationSnapshot, MarketSourceDefinition, MarketSourceHandle, MarketSourceSnapshot,
    MarketSubscribeRequest, MarketSubscription, OperationId,
};

pub trait MarketAdminService: Send + Sync {
    fn create(&self, definition: MarketSourceDefinition) -> LocalResult<MarketSourceHandle>;
    fn start(&self, source: MarketSourceHandle) -> LocalResult<OperationId>;
    fn stop(&self, source: MarketSourceHandle, deadline: Instant) -> LocalResult<OperationId>;
    fn remove(&self, source: MarketSourceHandle) -> LocalResult<OperationId>;
    fn replace(
        &self,
        source: MarketSourceHandle,
        definition: MarketSourceDefinition,
    ) -> LocalResult<MarketSourceHandle>;
    fn list(&self) -> Arc<[MarketSourceSnapshot]>;
    fn operation(&self, id: OperationId) -> MarketOperationSnapshot;
}

pub trait MarketService: Send + Sync {
    fn resolve(&self, source_key: &str) -> LocalResult<MarketSourceHandle>;
    fn subscribe(
        &self,
        source: MarketSourceHandle,
        request: MarketSubscribeRequest,
    ) -> LocalResult<MarketSubscription>;
    fn unsubscribe(
        &self,
        source: MarketSourceHandle,
        subscription: MarketSubscription,
    ) -> LocalResult<OperationId>;
    fn request_snapshot(
        &self,
        source: MarketSourceHandle,
        asset_id: AssetId,
    ) -> LocalResult<OperationId>;
    fn instruments(&self, source: MarketSourceHandle) -> LocalResult<Arc<[InstrumentSnapshot]>>;
    fn health(&self, source: MarketSourceHandle) -> LocalResult<ConnectorHealthSnapshot>;
    fn operation(
        &self,
        source: MarketSourceHandle,
        id: OperationId,
    ) -> LocalResult<ConnectorOperationSnapshot>;
}

#[derive(Debug)]
pub enum MarketAdminRequest {
    Create(MarketSourceDefinition),
    Start(MarketSourceHandle),
    Stop(MarketSourceHandle, Instant),
    Remove(MarketSourceHandle),
    Replace(MarketSourceHandle, MarketSourceDefinition),
    List,
    Operation(OperationId),
}

#[derive(Debug)]
pub enum MarketAdminResponse {
    Handle(MarketSourceHandle),
    OperationId(OperationId),
    Sources(Arc<[MarketSourceSnapshot]>),
    Operation(MarketOperationSnapshot),
}

pub struct MarketAdminApi;
impl Service for MarketAdminApi {
    type Request = MarketAdminRequest;
    type Response = LocalResult<MarketAdminResponse>;
}

#[derive(Debug)]
pub enum MarketRequest {
    Resolve(Arc<str>),
    Subscribe(MarketSourceHandle, MarketSubscribeRequest),
    Unsubscribe(MarketSourceHandle, MarketSubscription),
    Snapshot(MarketSourceHandle, AssetId),
    Instruments(MarketSourceHandle),
    Health(MarketSourceHandle),
    Operation(MarketSourceHandle, OperationId),
}

#[derive(Debug)]
pub enum MarketResponse {
    Handle(MarketSourceHandle),
    Subscription(MarketSubscription),
    OperationId(OperationId),
    Instruments(Arc<[InstrumentSnapshot]>),
    Health(ConnectorHealthSnapshot),
    Operation(ConnectorOperationSnapshot),
}

pub struct MarketApi;
impl Service for MarketApi {
    type Request = MarketRequest;
    type Response = LocalResult<MarketResponse>;
}

pub(crate) struct AdminEndpoint(pub Arc<dyn MarketAdminService>);
impl TypedServiceEndpoint<MarketAdminApi> for AdminEndpoint {
    fn call(
        &self,
        request: MarketAdminRequest,
        _: TraceContext,
    ) -> Result<LocalResult<MarketAdminResponse>, titan_plugin_engine::PluginError> {
        Ok(match request {
            MarketAdminRequest::Create(value) => {
                self.0.create(value).map(MarketAdminResponse::Handle)
            }
            MarketAdminRequest::Start(value) => {
                self.0.start(value).map(MarketAdminResponse::OperationId)
            }
            MarketAdminRequest::Stop(value, deadline) => self
                .0
                .stop(value, deadline)
                .map(MarketAdminResponse::OperationId),
            MarketAdminRequest::Remove(value) => {
                self.0.remove(value).map(MarketAdminResponse::OperationId)
            }
            MarketAdminRequest::Replace(handle, value) => self
                .0
                .replace(handle, value)
                .map(MarketAdminResponse::Handle),
            MarketAdminRequest::List => Ok(MarketAdminResponse::Sources(self.0.list())),
            MarketAdminRequest::Operation(id) => {
                Ok(MarketAdminResponse::Operation(self.0.operation(id)))
            }
        })
    }
}

pub(crate) struct MarketEndpoint(pub Arc<dyn MarketService>);
impl TypedServiceEndpoint<MarketApi> for MarketEndpoint {
    fn call(
        &self,
        request: MarketRequest,
        _: TraceContext,
    ) -> Result<LocalResult<MarketResponse>, titan_plugin_engine::PluginError> {
        Ok(match request {
            MarketRequest::Resolve(key) => self.0.resolve(&key).map(MarketResponse::Handle),
            MarketRequest::Subscribe(handle, req) => self
                .0
                .subscribe(handle, req)
                .map(MarketResponse::Subscription),
            MarketRequest::Unsubscribe(handle, sub) => self
                .0
                .unsubscribe(handle, sub)
                .map(MarketResponse::OperationId),
            MarketRequest::Snapshot(handle, asset) => self
                .0
                .request_snapshot(handle, asset)
                .map(MarketResponse::OperationId),
            MarketRequest::Instruments(handle) => {
                self.0.instruments(handle).map(MarketResponse::Instruments)
            }
            MarketRequest::Health(handle) => self.0.health(handle).map(MarketResponse::Health),
            MarketRequest::Operation(handle, id) => {
                self.0.operation(handle, id).map(MarketResponse::Operation)
            }
        })
    }
}
