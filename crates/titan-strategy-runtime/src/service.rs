use std::{sync::Arc, time::Instant};

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
