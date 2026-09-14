use std::{sync::Arc, time::Instant};

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
