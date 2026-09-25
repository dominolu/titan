//! Bridge types for running the same signed ABI V13 artifact in HftBacktest and live Titan.
//!
//! A backtest driver updates the adapter's public projections from its local/exchange processors,
//! dispatches events at `EventPhase::StrategyCallback`, then translates returned staged commands
//! into `ExecutionOrderRequest`s. No Python callback or legacy strategy context is involved.

pub use titan_strategy_runtime::{
    OfflineV13Adapter, StagedCommandV13, TitanAccountStateEventView, TitanAccountView,
    TitanActiveOrderView, TitanBalanceEventView, TitanBalanceView, TitanCancelEventView,
    TitanDepthView, TitanFillView, TitanMarketView, TitanOrderEventView, TitanPositionEventView,
    TitanPositionView, TitanTickView, TitanTimerView,
};
