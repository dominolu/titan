//! Shared execution-domain primitives.
//!
//! Tick and Bar matchers retain their market-specific, monomorphized behavior and route outcomes
//! through these shared account, report, risk and projection components via compatibility
//! adapters.

mod account;
mod capabilities;
mod command;
mod conditional;
mod coordinator;
mod core;
mod dense_account;
mod fee;
mod funding;
mod instrument;
mod instrument_event;
mod live_adapter;
mod margin;
mod matching;
mod observer;
mod projector;
mod quality;
mod risk;
mod state_machine;
mod tick_adapter;
mod tick_coordinator;
mod transport;

pub use account::{
    AccountDelta, AccountError, AccountReport, ExchangeAccountState, ExchangePortfolio,
    LocalAccountView, PortfolioLedger, PositionLedger, VenueAccount,
};
pub use capabilities::{
    Capability, CapabilityError, CapabilitySet, ModelDescriptor, validate_capabilities,
};
pub use command::{CancelRequest, ExecutionCommand, ExecutionOrderRequest, OrderOrigin};
pub use conditional::{ConditionalAction, ConditionalOrder, ConditionalOrderBook};
pub use coordinator::{ExecutionCoordinator, ExecutionError};
pub use core::{InstrumentExecutionCore, SharedExecutionEngine, VenueExecutionCore};
pub use dense_account::{
    DenseAccountDelta, DenseAccountError, DenseCurrencyLedger, DenseVenueAccount,
};
pub use fee::{
    ExecutionFeeModel, FeeCharge, FeeRoundingMode, FillFeeContext, LegacyExecutionFeeAdapter,
    NoFee, RateFeeModel, RoundedFeeModel,
};
pub use funding::{
    FundingBoundary, FundingEngine, FundingError, FundingEvent, FundingReport, FundingRounding,
    FundingRoundingMode, ScheduledFunding,
};
pub use instrument::{
    CashFlowMode, CurrencyId, InstrumentId, InstrumentSpec, InstrumentSpecError, InstrumentType,
    VenueId,
};
pub use instrument_event::{
    InstrumentEvent, InstrumentEventError, InstrumentRegistry, MarketStatus,
    ScheduledInstrumentRegistry,
};
pub use live_adapter::{
    LIVE_EXECUTION_ABI_VERSION, LiveAdapterError, LiveExecutionAdapter, LiveExecutionEvent,
    LiveOrderStatus,
};
pub use margin::{CrossMarginRisk, MarginParameters};
pub use matching::{MatchOutcome, MatchOutcomeSink, MatchingModel, ProposedFill};
pub use observer::{
    BufferedExecutionObserver, ExecutionObserver, LegacyOrderSnapshot, NoopExecutionObserver,
    ObservedOutcome, OutcomeBus,
};
pub use projector::{
    ExecutionEventProjector, ExecutionReason, ExecutionReport, ExecutionReportKind, ProjectedEvent,
    ProjectedEventKind, ProjectedFundingEvent,
};
pub use quality::{
    DisabledLiquidityConsumption, ExecutionQualityIdentity, ExecutionQualityModel,
    ExecutionRealityPipeline, FillQualityContext, HistoricalLiquidityConsumption,
    IdentityExecutionQuality, InstrumentExecutionReality, LiquidityConsumptionModel, LiquidityKey,
    SeededExecutionQuality, TickExecutionReality,
};
pub use risk::{
    AllowAllRisk, ExchangeRisk, InstrumentRiskMetrics, LocalPreTradeRisk, MarketStatusRisk,
    PostTradeRisk, RiskAction, RiskActionSink, RiskDecision, RiskPipeline, RiskReason, VenueRisk,
};
pub use state_machine::{
    ExecutionOrder, OrderExtensions, OrderState, OrderStateError, OrderTransition,
    TransitionResult, TriggerKind, TriggerState,
};
pub use tick_adapter::{LegacyTickAdapterError, LegacyTickOutcomeAdapter};
pub use tick_coordinator::{
    SharedTickExecutionConfig, TickCoordinatorError, TickOutcomeCoordinator,
};
pub use transport::{
    ConstantExecutionLatency, ExecutionLatencyModel, OrderTransport, RequestTiming,
};
