use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Error as IoError,
    ops::{Deref, DerefMut},
};

pub use data::DataSource;
use data::Reader;
use models::FeeModel;
use thiserror::Error;

pub use crate::backtest::{
    models::L3QueueModel,
    proc::{L3Local, L3NoPartialFillExchange},
};
use crate::{
    backtest::{
        assettype::AssetType,
        data::{Data, FeedLatencyAdjustment, NpyDTyped},
        evs::{EventIntentKind, EventSet},
        execution::{
            AccountError, AllowAllRisk, CurrencyId, ExchangePortfolio, ExecutionEventProjector,
            ExecutionOrderRequest, ExecutionReason, ExecutionReport, FundingEngine, FundingError,
            FundingReport, InstrumentId, InstrumentSpec, InstrumentSpecError,
            LegacyExecutionFeeAdapter, LocalPreTradeRisk, NoFee, ObservedOutcome, OrderOrigin,
            OutcomeBus, PortfolioLedger, ProjectedEvent, RiskAction, RiskActionSink, RiskDecision,
            RiskReason, ScheduledFunding, SharedTickExecutionConfig, TickCoordinatorError,
            TickOutcomeCoordinator, VenueId, VenueRisk,
        },
        models::{LatencyModel, QueueModel},
        order::order_bus,
        proc::{Local, LocalProcessor, NoPartialFillExchange, PartialFillExchange, Processor},
        result::{
            AccountSnapshot, AuditKind, AuditRecord, AuditRecorder, BacktestResult, EndPolicy,
            ReproducibilityMetadata, RunTermination,
        },
        scheduler::{EventKey, EventPhase},
        state::State,
    },
    depth::{L2MarketDepth, L3MarketDepth, MarketDepth},
    prelude::{
        Bot, OrdType, Order, OrderId, OrderRequest, Side, StateValues, Status, TimeInForce,
        UNTIL_END_OF_DATA, WaitOrderResponse,
    },
    types::{BuildError, ElapseResult, Event},
};

/// Provides asset types.
pub mod assettype;

pub mod models;

pub mod bar;

pub mod hybrid;

pub mod live_bar;

pub mod platform;

/// OrderBus implementation
pub mod order;

/// Local and exchange models
pub mod proc;

/// Trading state.
pub mod state;

/// Recorder for a bot's trading statistics.
pub mod recorder;
pub mod result;

pub mod data;
mod evs;

/// Shared execution domain used by Tick, Bar, Hybrid and live event adapters.
pub mod execution;

/// Deterministic global event scheduling primitives.
pub mod scheduler;

/// Errors that can occur during backtesting.
#[derive(Error, Debug)]
pub enum BacktestError {
    #[error("Order related to a given order id already exists")]
    OrderIdExist,
    #[error("Order request is in process")]
    OrderRequestInProcess,
    #[error("Order not found")]
    OrderNotFound,
    #[error("order request is invalid")]
    InvalidOrderRequest,
    #[error("order status is invalid to proceed the request")]
    InvalidOrderStatus,
    #[error("unsupported operation: {0}")]
    UnsupportedOperation(&'static str),
    #[error("end of data")]
    EndOfData,
    #[error("data error: {0:?}")]
    DataError(#[from] IoError),
    #[error("shared Tick execution error: {0}")]
    SharedTickExecution(#[from] TickCoordinatorError),
    #[error("shared Tick InstrumentSpec asset_no is out of range or duplicated")]
    InvalidSharedTickSpec,
    #[error("shared local account error: {0}")]
    SharedAccount(#[from] AccountError),
    #[error("shared Funding error: {0}")]
    SharedFunding(#[from] FundingError),
}

fn execution_reason_from_risk(reason: RiskReason) -> ExecutionReason {
    match reason {
        RiskReason::InvalidInstrument => ExecutionReason::InvalidInstrument,
        RiskReason::InvalidPrice => ExecutionReason::InvalidPrice,
        RiskReason::InvalidQuantity => ExecutionReason::InvalidQuantity,
        RiskReason::DuplicateOrderId => ExecutionReason::DuplicateOrderId,
        RiskReason::PositionLimit => ExecutionReason::PositionLimit,
        RiskReason::NotionalLimit => ExecutionReason::NotionalLimit,
        RiskReason::InsufficientBalance => ExecutionReason::InsufficientBalance,
        RiskReason::InsufficientMargin => ExecutionReason::InsufficientMargin,
        RiskReason::ReduceOnlyViolation => ExecutionReason::ReduceOnlyViolation,
        RiskReason::MarketClosed => ExecutionReason::MarketClosed,
        RiskReason::Custom(code) => ExecutionReason::Unknown(code),
    }
}

fn execution_reason_from_spec(error: InstrumentSpecError) -> ExecutionReason {
    match error {
        InstrumentSpecError::InvalidPrice | InstrumentSpecError::PricePrecision => {
            ExecutionReason::InvalidPrice
        }
        InstrumentSpecError::InvalidQuantity
        | InstrumentSpecError::QuantityPrecision
        | InstrumentSpecError::QuantityBelowMinimum
        | InstrumentSpecError::QuantityAboveMaximum => ExecutionReason::InvalidQuantity,
        InstrumentSpecError::NotionalBelowMinimum
        | InstrumentSpecError::InvalidPositiveField { .. }
        | InstrumentSpecError::InvalidQuantityRange => ExecutionReason::InvalidInstrument,
    }
}

/// Backtesting Asset
pub struct Asset<L: ?Sized, E: ?Sized, D: NpyDTyped + Clone /* todo: ugly bounds */> {
    pub local: Box<L>,
    pub exch: Box<E>,
    pub reader: Reader<D>,
    pub outcome_bus: Option<OutcomeBus>,
    pub shared_execution: Option<SharedTickExecutionConfig>,
}

impl<L, E, D: NpyDTyped + Clone> Asset<L, E, D> {
    /// Constructs an instance of `Asset`. Use this method if a custom local processor or an
    /// exchange processor is needed.
    pub fn new(local: L, exch: E, reader: Reader<D>) -> Self {
        Self {
            local: Box::new(local),
            exch: Box::new(exch),
            reader,
            outcome_bus: None,
            shared_execution: None,
        }
    }

    /// Returns an `L2AssetBuilder`.
    pub fn l2_builder<LM, AT, QM, MD, FM>() -> L2AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L2MarketDepth + 'static,
        QM: QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
    {
        L2AssetBuilder::new()
    }

    /// Returns an `L3AssetBuilder`.
    pub fn l3_builder<LM, AT, QM, MD, FM>() -> L3AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L3MarketDepth + 'static,
        QM: L3QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
        BacktestError: From<<MD as L3MarketDepth>::Error>,
    {
        L3AssetBuilder::new()
    }
}

fn default_shared_execution<AT, FM, MD>(
    asset_type: &AT,
    fee_model: FM,
    depth: &MD,
) -> SharedTickExecutionConfig
where
    AT: AssetType,
    FM: FeeModel + 'static,
    MD: MarketDepth,
{
    let currency = CurrencyId(0);
    SharedTickExecutionConfig::new(
        InstrumentSpec {
            instrument_id: InstrumentId(0),
            asset_no: 0,
            venue_id: VenueId(0),
            tick_size: depth.tick_size(),
            lot_size: depth.lot_size(),
            min_qty: depth.lot_size(),
            max_qty: f64::MAX,
            min_notional: 0.0,
            contract_size: asset_type.contract_size_hint(),
            price_currency: currency,
            settlement_currency: currency,
            margin_currency: currency,
            instrument_type: asset_type.execution_instrument_type(),
            cash_flow_mode: execution::CashFlowMode::LegacyNotional,
            version: 1,
        },
        LegacyExecutionFeeAdapter::new(fee_model, currency),
    )
}

/// Exchange model kind.
pub enum ExchangeKind {
    /// Uses [NoPartialFillExchange](`NoPartialFillExchange`).
    NoPartialFillExchange,
    /// Uses [PartialFillExchange](`PartialFillExchange`).
    PartialFillExchange,
}

/// A level-2 asset builder.
pub struct L2AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
    execution_reality: Option<Box<dyn execution::TickExecutionReality>>,
}

impl<LM, AT, QM, MD, FM> L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    /// Constructs an `L2AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
            execution_reality: None,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// Installs historical-liquidity/execution-quality adjustment for PartialFillExchange.
    /// NoPartialFillExchange rejects this configuration because it cannot preserve partial
    /// liquidity semantics.
    pub fn execution_reality<R>(self, model: R) -> Self
    where
        R: execution::TickExecutionReality + 'static,
    {
        Self {
            execution_reality: Some(Box::new(model)),
            ..self
        }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = order_bus(order_latency);

        let local_depth = create_depth();
        let shared_execution =
            default_shared_execution(&asset_type, fee_model.clone(), &local_depth);
        let local = Local::new_external_accounting(
            local_depth,
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                if self.execution_reality.is_some() {
                    return Err(BuildError::InvalidArgument(
                        "execution_reality requires PartialFillExchange",
                    ));
                }
                let outcome_bus = OutcomeBus::new();
                let exch = NoPartialFillExchange::new_with_observer(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                    outcome_bus.clone(),
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    outcome_bus: Some(outcome_bus),
                    shared_execution: Some(shared_execution),
                })
            }
            ExchangeKind::PartialFillExchange => {
                let outcome_bus = OutcomeBus::new();
                let mut exch = PartialFillExchange::new_with_observer(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                    outcome_bus.clone(),
                );
                if let Some(model) = self.execution_reality {
                    exch.set_execution_reality(model);
                }

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    outcome_bus: Some(outcome_bus),
                    shared_execution: Some(shared_execution),
                })
            }
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A level-3 asset builder.
pub struct L3AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
}

impl<LM, AT, QM, MD, FM> L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    /// Constructs an `L3AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = order_bus(order_latency);

        let local_depth = create_depth();
        let shared_execution =
            default_shared_execution(&asset_type, fee_model.clone(), &local_depth);
        let local = L3Local::new_external_accounting(
            local_depth,
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                let outcome_bus = OutcomeBus::new();
                let exch = L3NoPartialFillExchange::new_with_observer(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                    outcome_bus.clone(),
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    outcome_bus: Some(outcome_bus),
                    shared_execution: Some(shared_execution),
                })
            }
            ExchangeKind::PartialFillExchange => Err(BuildError::InvalidArgument(
                "L3 PartialFillExchange is not supported; choose NoPartialFillExchange",
            )),
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn default() -> Self {
        Self::new()
    }
}

/// [`Backtest`] builder.
pub struct BacktestBuilder<MD> {
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
    outcome_buses: Vec<Option<OutcomeBus>>,
    shared_execution: Vec<Option<SharedTickExecutionConfig>>,
}

impl<MD> BacktestBuilder<MD> {
    /// Adds [`Asset`], which will undergo simulation within the backtester.
    pub fn add_asset(self, asset: Asset<dyn LocalProcessor<MD>, dyn Processor, Event>) -> Self {
        let mut self_ = Self { ..self };
        let asset_no = self_.local.len();
        let mut shared_execution = asset.shared_execution;
        if let Some(config) = shared_execution.as_mut() {
            config.spec.asset_no = asset_no as u32;
            if config.spec.instrument_id == InstrumentId(0) {
                config.spec.instrument_id = InstrumentId(asset_no as u32 + 1);
            }
        }
        self_.local.push(BacktestProcessorState::new(
            asset.local,
            asset.reader.clone(),
        ));
        self_
            .exch
            .push(BacktestProcessorState::new(asset.exch, asset.reader));
        self_.outcome_buses.push(asset.outcome_bus);
        self_.shared_execution.push(shared_execution);
        self_
    }

    /// Builds [`Backtest`].
    pub fn build(self) -> Result<Backtest<MD>, BuildError> {
        let num_assets = self.local.len();
        if self.local.len() != num_assets || self.exch.len() != num_assets {
            panic!();
        }
        let mut shared_tick_coordinators: Vec<_> = (0..num_assets).map(|_| None).collect();
        for config in self.shared_execution.into_iter().flatten() {
            let asset_no = config.spec.asset_no as usize;
            shared_tick_coordinators[asset_no] =
                Some(TickOutcomeCoordinator::new(config.spec, config.fee_model));
        }
        Ok(Backtest {
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets),
            local: self.local,
            exch: self.exch,
            outcome_buses: self.outcome_buses,
            runtime_feed_events: Vec::new(),
            runtime_order_events: Vec::new(),
            runtime_match_outcomes: Vec::new(),
            shared_tick_coordinators,
            shared_exchange_reports: Vec::new(),
            shared_report_scratch: Vec::with_capacity(2),
            shared_pending_reports: VecDeque::with_capacity(64),
            shared_exchange_portfolio: ExchangePortfolio::default(),
            shared_initial_balances: HashMap::new(),
            shared_venue_risks: HashMap::new(),
            shared_local_risk: Box::new(AllowAllRisk),
            shared_risk_actions: RiskActionSink::with_capacity(4),
            runtime_reduce_only: HashMap::new(),
            next_risk_order_id: u64::MAX,
            pending_liquidations: HashSet::new(),
            risk_order_instruments: HashMap::new(),
            shared_local_portfolio: PortfolioLedger::default(),
            shared_projector: ExecutionEventProjector::with_capacity(3),
            shared_projected_events: Vec::new(),
            shared_delivery_scratch: Vec::with_capacity(4),
            shared_state_values: vec![StateValues::default(); num_assets],
            audit: AuditRecorder::disabled(),
            audit_run_id: 0,
            audit_sequence: 0,
            runtime_capture_enabled: false,
        })
    }
}

/// This backtester provides multi-asset and multi-exchange model backtesting, allowing you to
/// configure different setups such as queue models or asset types for each asset. However, this may
/// result in slightly slower performance compared to [`Backtest`].
pub struct Backtest<MD> {
    cur_ts: i64,
    evs: EventSet,
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
    outcome_buses: Vec<Option<OutcomeBus>>,
    runtime_feed_events: Vec<(usize, Event)>,
    runtime_order_events: Vec<(usize, i64, Order)>,
    runtime_match_outcomes: Vec<(usize, ObservedOutcome)>,
    shared_tick_coordinators:
        Vec<Option<TickOutcomeCoordinator<Box<dyn execution::ExecutionFeeModel>>>>,
    shared_exchange_reports: Vec<(usize, ExecutionReport)>,
    shared_report_scratch: Vec<ExecutionReport>,
    shared_pending_reports: VecDeque<(usize, ExecutionReport)>,
    shared_exchange_portfolio: ExchangePortfolio,
    shared_initial_balances: HashMap<(VenueId, CurrencyId), f64>,
    shared_venue_risks: HashMap<VenueId, Box<dyn VenueRisk>>,
    shared_local_risk: Box<dyn LocalPreTradeRisk>,
    shared_risk_actions: RiskActionSink,
    runtime_reduce_only: HashMap<(usize, OrderId), bool>,
    next_risk_order_id: OrderId,
    pending_liquidations: HashSet<(VenueId, InstrumentId)>,
    risk_order_instruments: HashMap<OrderId, (VenueId, InstrumentId)>,
    shared_local_portfolio: PortfolioLedger,
    shared_projector: ExecutionEventProjector,
    shared_projected_events: Vec<(usize, ProjectedEvent)>,
    shared_delivery_scratch: Vec<(OrderId, i64)>,
    shared_state_values: Vec<StateValues>,
    audit: AuditRecorder,
    audit_run_id: u64,
    audit_sequence: u64,
    runtime_capture_enabled: bool,
}

impl<MD> Backtest<MD>
where
    MD: MarketDepth,
{
    /// Feed events processed locally since the runtime last cleared its batch.
    pub fn runtime_feed_events(&self) -> &[(usize, Event)] {
        &self.runtime_feed_events
    }

    pub fn clear_runtime_feed_events(&mut self) {
        self.runtime_feed_events.clear();
    }

    /// Order responses received locally, preserving individual partial fills.
    pub fn runtime_order_events(&self) -> &[(usize, i64, Order)] {
        &self.runtime_order_events
    }

    pub fn clear_runtime_order_events(&mut self) {
        self.runtime_order_events.clear();
    }

    /// Exchange-time normalized outcomes since the runtime last cleared its batch.
    pub fn runtime_match_outcomes(&self) -> &[(usize, ObservedOutcome)] {
        &self.runtime_match_outcomes
    }

    pub fn clear_runtime_match_outcomes(&mut self) {
        self.runtime_match_outcomes.clear();
    }

    /// Rewinds a Tick/L3 backtest in place while preserving immutable data/model configuration.
    pub fn reset(&mut self) {
        self.cur_ts = i64::MAX;
        self.evs.reset();
        for state in &mut self.local {
            state.reset();
        }
        for state in &mut self.exch {
            state.reset();
        }
        for bus in self.outcome_buses.iter_mut().flatten() {
            bus.clear();
        }
        for coordinator in self.shared_tick_coordinators.iter_mut().flatten() {
            coordinator.reset();
        }
        self.runtime_feed_events.clear();
        self.runtime_order_events.clear();
        self.runtime_match_outcomes.clear();
        self.shared_exchange_reports.clear();
        self.shared_report_scratch.clear();
        self.shared_pending_reports.clear();
        self.shared_exchange_portfolio.reset();
        self.shared_local_portfolio.reset();
        self.restore_shared_exchange_balances();
        self.shared_projector.reset();
        self.shared_projected_events.clear();
        self.shared_delivery_scratch.clear();
        self.shared_state_values.fill(StateValues::default());
        self.shared_venue_risks
            .values_mut()
            .for_each(|risk| risk.reset_all());
        self.shared_local_risk.reset();
        self.shared_risk_actions.clear();
        self.runtime_reduce_only.clear();
        self.next_risk_order_id = u64::MAX;
        self.pending_liquidations.clear();
        self.risk_order_instruments.clear();
        self.audit.reset();
        self.audit_sequence = 0;
    }

    pub fn enable_audit(&mut self, run_id: u64, capacity: usize) {
        self.audit = AuditRecorder::bounded(capacity);
        self.audit_run_id = run_id;
        self.audit_sequence = 0;
    }

    pub fn audit(&self) -> &AuditRecorder {
        &self.audit
    }

    fn record_audit(
        &mut self,
        timestamp: i64,
        phase: EventPhase,
        asset_no: usize,
        kind: AuditKind,
        order_id: u64,
        code: u32,
        value0: f64,
        value1: f64,
    ) {
        let venue_no = self
            .shared_tick_coordinators
            .get(asset_no)
            .and_then(Option::as_ref)
            .map_or(0, |coordinator| coordinator.spec().venue_id.0);
        self.audit.record(AuditRecord {
            run_id: self.audit_run_id,
            schema_version: 1,
            key: EventKey {
                timestamp,
                phase,
                source_priority: 0,
                venue_no,
                asset_no: asset_no as u32,
                sequence: self.audit_sequence,
            },
            kind,
            order_id,
            code,
            value0,
            value1,
        });
        self.audit_sequence = self.audit_sequence.wrapping_add(1);
    }

    fn restore_shared_exchange_balances(&mut self) {
        for (&(venue_id, currency), &balance) in &self.shared_initial_balances {
            self.shared_exchange_portfolio
                .venue_mut_or_insert(venue_id)
                .account_mut()
                .set_balance(currency, balance)
                .expect("validated initial balance must remain valid during reset");
            self.shared_local_portfolio
                .venue_mut_or_insert(venue_id)
                .seed_balance(currency, balance)
                .expect("validated initial balance must remain valid during reset");
        }
    }

    /// Enables exchange-time shared state/account coordination for the configured Tick assets.
    /// The legacy local processor remains the strategy-visible state until B06 delivery migration.
    pub fn configure_shared_tick_execution(
        &mut self,
        specs: impl IntoIterator<Item = InstrumentSpec>,
    ) -> Result<(), BacktestError> {
        self.shared_tick_coordinators
            .iter_mut()
            .for_each(|slot| *slot = None);
        for spec in specs {
            let asset_no = spec.asset_no as usize;
            let Some(slot) = self.shared_tick_coordinators.get_mut(asset_no) else {
                return Err(BacktestError::InvalidSharedTickSpec);
            };
            if slot.is_some() {
                return Err(BacktestError::InvalidSharedTickSpec);
            }
            let currency = spec.settlement_currency;
            *slot = Some(TickOutcomeCoordinator::new(
                spec,
                Box::new(NoFee { currency }) as Box<dyn execution::ExecutionFeeModel>,
            ));
        }
        self.shared_exchange_reports.clear();
        self.shared_pending_reports.clear();
        self.shared_exchange_portfolio.reset();
        self.shared_local_portfolio.reset();
        self.restore_shared_exchange_balances();
        self.shared_projected_events.clear();
        self.shared_state_values.fill(StateValues::default());
        self.shared_venue_risks
            .values_mut()
            .for_each(|risk| risk.reset_all());
        self.shared_local_risk.reset();
        self.shared_risk_actions.clear();
        self.runtime_reduce_only.clear();
        self.next_risk_order_id = u64::MAX;
        self.pending_liquidations.clear();
        self.risk_order_instruments.clear();
        self.audit.reset();
        self.audit_sequence = 0;
        Ok(())
    }

    pub fn configure_shared_tick_execution_with_fees(
        &mut self,
        configs: impl IntoIterator<Item = SharedTickExecutionConfig>,
    ) -> Result<(), BacktestError> {
        self.shared_tick_coordinators
            .iter_mut()
            .for_each(|slot| *slot = None);
        for config in configs {
            let asset_no = config.spec.asset_no as usize;
            let Some(slot) = self.shared_tick_coordinators.get_mut(asset_no) else {
                return Err(BacktestError::InvalidSharedTickSpec);
            };
            if slot.is_some() {
                return Err(BacktestError::InvalidSharedTickSpec);
            }
            *slot = Some(TickOutcomeCoordinator::new(config.spec, config.fee_model));
        }
        self.shared_exchange_reports.clear();
        self.shared_pending_reports.clear();
        self.shared_exchange_portfolio.reset();
        self.shared_local_portfolio.reset();
        self.restore_shared_exchange_balances();
        self.shared_projected_events.clear();
        self.shared_state_values.fill(StateValues::default());
        self.shared_venue_risks
            .values_mut()
            .for_each(|risk| risk.reset_all());
        self.shared_local_risk.reset();
        self.shared_risk_actions.clear();
        self.runtime_reduce_only.clear();
        self.next_risk_order_id = u64::MAX;
        self.pending_liquidations.clear();
        self.risk_order_instruments.clear();
        self.audit.reset();
        self.audit_sequence = 0;
        Ok(())
    }

    pub fn shared_exchange_reports(&self) -> &[(usize, ExecutionReport)] {
        &self.shared_exchange_reports
    }

    pub fn clear_shared_exchange_reports(&mut self) {
        self.shared_exchange_reports.clear();
    }

    pub fn shared_local_portfolio(&self) -> &PortfolioLedger {
        &self.shared_local_portfolio
    }

    pub fn shared_exchange_portfolio(&self) -> &ExchangePortfolio {
        &self.shared_exchange_portfolio
    }

    /// Returns the authoritative exchange-final and report-delivered local-final states using
    /// the same result schema as the Bar prepared runtime.
    pub fn shared_account_snapshots(&self) -> (Vec<AccountSnapshot>, Vec<AccountSnapshot>) {
        let mut exchange = Vec::new();
        let mut local = Vec::new();
        for coordinator in self.shared_tick_coordinators.iter().flatten() {
            let spec = coordinator.spec();
            let risk = self.shared_venue_risks.get(&spec.venue_id);
            let snapshot = |account: &execution::VenueAccount| {
                let metrics = risk.map_or_else(Default::default, |risk| {
                    risk.instrument_metrics(account, spec.instrument_id)
                });
                AccountSnapshot {
                    venue_no: spec.venue_id.0,
                    asset_no: spec.asset_no,
                    currency: spec.settlement_currency,
                    position: account.position(spec.instrument_id).qty,
                    balance: account.balance(spec.settlement_currency),
                    fee: account.fee(spec.settlement_currency),
                    funding: account.funding(spec.settlement_currency),
                    realized_pnl: account.position(spec.instrument_id).realized_pnl,
                    unrealized_pnl: metrics.unrealized_pnl,
                    margin: metrics.initial_margin,
                }
            };
            exchange.push(
                self.shared_exchange_portfolio
                    .venue(spec.venue_id)
                    .map(|account| snapshot(account.account()))
                    .unwrap_or(AccountSnapshot {
                        venue_no: spec.venue_id.0,
                        asset_no: spec.asset_no,
                        currency: spec.settlement_currency,
                        ..Default::default()
                    }),
            );
            local.push(
                self.shared_local_portfolio
                    .venue(spec.venue_id)
                    .map(|account| snapshot(account.account()))
                    .unwrap_or(AccountSnapshot {
                        venue_no: spec.venue_id.0,
                        asset_no: spec.asset_no,
                        currency: spec.settlement_currency,
                        ..Default::default()
                    }),
            );
        }
        (exchange, local)
    }

    /// Builds a Tick/L2/L3 `BacktestResult` directly from captured canonical reports and account
    /// ledgers. No strategy code is rerun to derive statistics.
    pub fn shared_backtest_result(
        &self,
        run_id: u64,
        metadata: ReproducibilityMetadata,
        end_policy: EndPolicy,
        termination: RunTermination,
    ) -> BacktestResult {
        let mut result = BacktestResult::empty(metadata);
        result.run_id = run_id;
        result.end_policy = end_policy;
        result.termination = termination;
        let mut order_ids = HashSet::new();
        let reports = self
            .shared_exchange_reports
            .iter()
            .map(|(_, report)| *report);
        for report in reports {
            order_ids.insert((report.venue_id, report.order_id));
            if result.order_count == 0 {
                result.start_exchange_ts = report.exchange_ts;
                result.start_delivery_ts = report.delivery_ts;
            } else {
                result.start_exchange_ts = result.start_exchange_ts.min(report.exchange_ts);
                result.start_delivery_ts = result.start_delivery_ts.min(report.delivery_ts);
            }
            result.end_exchange_ts = result.end_exchange_ts.max(report.exchange_ts);
            result.end_delivery_ts = result.end_delivery_ts.max(report.delivery_ts);
            match report.kind {
                execution::ExecutionReportKind::Rejected => result.reject_count += 1,
                execution::ExecutionReportKind::Canceled => result.cancel_count += 1,
                execution::ExecutionReportKind::Expired => result.expire_count += 1,
                execution::ExecutionReportKind::Fill => result.fill_count += 1,
                execution::ExecutionReportKind::Accepted => {}
            }
            result.order_count = order_ids.len() as u64;
        }
        (result.exchange_final, result.local_delivered_final) = self.shared_account_snapshots();
        result
    }

    /// Installs the venue-scoped exchange-arrival and post-trade risk model used by Tick matching.
    pub fn configure_shared_tick_venue_risk<R>(&mut self, venue_id: VenueId, risk: R)
    where
        R: VenueRisk + 'static,
    {
        self.shared_venue_risks.insert(venue_id, Box::new(risk));
    }

    pub fn configure_shared_tick_local_risk<R>(&mut self, risk: R)
    where
        R: LocalPreTradeRisk + 'static,
    {
        self.shared_local_risk = Box::new(risk);
    }

    /// Seeds authoritative exchange collateral before the first order arrives.
    pub fn set_shared_exchange_balance(
        &mut self,
        venue_id: VenueId,
        currency: CurrencyId,
        balance: f64,
    ) -> Result<(), BacktestError> {
        self.shared_exchange_portfolio
            .venue_mut_or_insert(venue_id)
            .account_mut()
            .set_balance(currency, balance)?;
        self.shared_local_portfolio
            .venue_mut_or_insert(venue_id)
            .seed_balance(currency, balance)?;
        self.shared_initial_balances
            .insert((venue_id, currency), balance);
        Ok(())
    }

    /// Records execution-domain flags absent from the legacy monomorphized Order ABI.
    pub fn register_runtime_order_extensions(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        reduce_only: bool,
    ) {
        if reduce_only {
            self.runtime_reduce_only.insert((asset_no, order_id), true);
        } else {
            self.runtime_reduce_only.remove(&(asset_no, order_id));
        }
    }

    pub fn shared_risk_actions(&self) -> &[RiskAction] {
        self.shared_risk_actions.as_slice()
    }

    pub fn clear_shared_risk_actions(&mut self) {
        self.shared_risk_actions.clear();
    }

    pub fn settle_runtime_funding(
        &mut self,
        scheduled: ScheduledFunding,
        engine: &mut FundingEngine,
        sequence: u64,
    ) -> Result<FundingReport, BacktestError> {
        let asset_no = scheduled.asset_no as usize;
        let coordinator = self
            .shared_tick_coordinators
            .get(asset_no)
            .and_then(Option::as_ref)
            .ok_or(BacktestError::InvalidSharedTickSpec)?;
        let spec = coordinator.spec().clone();
        let exchange = self
            .shared_exchange_portfolio
            .venue_mut_or_insert(scheduled.event.venue_id);
        let position = exchange.account().position(spec.instrument_id).qty;
        let report = engine
            .settle(
                scheduled.event,
                position,
                &spec,
                exchange,
                scheduled.delivery_ts,
                sequence,
            )
            .map_err(BacktestError::from)?;
        self.record_audit(
            report.event.settlement_ts,
            EventPhase::ExchangeState,
            asset_no,
            AuditKind::Funding,
            0,
            0,
            report.event.rate,
            report.amount,
        );
        Ok(report)
    }

    pub fn deliver_runtime_funding(&mut self, report: FundingReport) -> Result<(), BacktestError> {
        self.shared_projector.project_funding(
            report,
            self.shared_local_portfolio
                .venue_mut_or_insert(report.event.venue_id),
        )?;
        Ok(())
    }

    pub fn shared_projected_events(&self) -> &[(usize, ProjectedEvent)] {
        &self.shared_projected_events
    }

    pub fn clear_shared_projected_events(&mut self) {
        self.shared_projected_events.clear();
    }

    pub fn drain_shared_projected_events(&mut self, output: &mut Vec<(usize, ProjectedEvent)>) {
        output.append(&mut self.shared_projected_events);
    }

    pub fn set_runtime_capture(&mut self, enabled: bool) {
        self.runtime_capture_enabled = enabled;
        if !enabled {
            self.runtime_feed_events.clear();
            self.runtime_order_events.clear();
            self.runtime_match_outcomes.clear();
        }
    }

    #[inline]
    fn drain_exchange_outcomes(&mut self, asset_no: usize) -> Result<(), BacktestError> {
        let Some(bus) = self.outcome_buses.get(asset_no).and_then(Option::as_ref) else {
            return Ok(());
        };
        // Market events overwhelmingly produce no order outcome. Avoid taking the mutable
        // VecDeque path on every Tick; this branch is highly predictable and keeps the disabled
        // matching hot path close to the zero-sized observer baseline.
        if bus.is_empty() {
            return Ok(());
        }
        let mut generated_risk_actions = Vec::new();
        loop {
            let outcome = self.outcome_buses[asset_no]
                .as_mut()
                .and_then(OutcomeBus::pop_front);
            let Some(outcome) = outcome else {
                break;
            };
            if let Some(coordinator) = self
                .shared_tick_coordinators
                .get_mut(asset_no)
                .and_then(Option::as_mut)
            {
                let exchange_ts = match outcome.outcome {
                    execution::MatchOutcome::Accepted { exchange_ts }
                    | execution::MatchOutcome::Rejected { exchange_ts, .. }
                    | execution::MatchOutcome::Canceled { exchange_ts }
                    | execution::MatchOutcome::Expired { exchange_ts } => exchange_ts,
                    execution::MatchOutcome::Fill(fill) => fill.exchange_ts,
                };
                let venue_id = coordinator.spec().venue_id;
                let apply_result = coordinator.apply(
                    outcome,
                    exchange_ts,
                    self.shared_exchange_portfolio.venue_mut_or_insert(venue_id),
                    &mut self.shared_report_scratch,
                );
                if apply_result.is_err() {
                    self.record_audit(
                        exchange_ts,
                        EventPhase::ExchangeState,
                        asset_no,
                        AuditKind::Diagnostic,
                        outcome.order_id,
                        1,
                        0.0,
                        0.0,
                    );
                }
                apply_result?;
                for index in 0..self.shared_report_scratch.len() {
                    let report = self.shared_report_scratch[index];
                    self.record_audit(
                        report.exchange_ts,
                        EventPhase::ExchangeState,
                        asset_no,
                        AuditKind::ExecutionReport,
                        report.order_id,
                        crate::runtime::execution_reason_code(report.reason),
                        report.exec_price,
                        report.exec_qty,
                    );
                    self.record_audit(
                        report.exchange_ts,
                        EventPhase::ExchangeState,
                        asset_no,
                        AuditKind::OrderTransition,
                        report.order_id,
                        report.status as u32,
                        report.order_qty,
                        report.exec_qty,
                    );
                    if let Some(delta) = report.account_delta {
                        self.record_audit(
                            report.exchange_ts,
                            EventPhase::Matching,
                            asset_no,
                            AuditKind::Fill,
                            report.order_id,
                            0,
                            report.exec_price,
                            report.exec_qty,
                        );
                        self.record_audit(
                            report.exchange_ts,
                            EventPhase::ExchangeState,
                            asset_no,
                            AuditKind::AccountDelta,
                            report.order_id,
                            0,
                            delta.position_delta,
                            delta.cash_delta - delta.fee + delta.funding,
                        );
                        if self
                            .shared_exchange_portfolio
                            .venue(report.venue_id)
                            .is_some_and(|account| {
                                account.account().position(delta.instrument_id).qty == 0.0
                            })
                        {
                            self.pending_liquidations
                                .remove(&(report.venue_id, delta.instrument_id));
                        }
                        if let (Some(risk), Some(account)) = (
                            self.shared_venue_risks.get_mut(&report.venue_id),
                            self.shared_exchange_portfolio.venue(report.venue_id),
                        ) {
                            let mut sink = RiskActionSink::with_capacity(2);
                            risk.on_account_change(account, &mut sink);
                            generated_risk_actions.extend_from_slice(sink.as_slice());
                        }
                    }
                    if matches!(
                        report.kind,
                        execution::ExecutionReportKind::Canceled
                            | execution::ExecutionReportKind::Rejected
                            | execution::ExecutionReportKind::Expired
                    ) {
                        self.runtime_reduce_only
                            .remove(&(asset_no, report.order_id));
                        if let Some(key) = self.risk_order_instruments.remove(&report.order_id) {
                            self.pending_liquidations.remove(&key);
                        }
                    }
                }
                self.shared_exchange_reports
                    .extend(self.shared_report_scratch.drain(..).map(|report| {
                        self.shared_pending_reports.push_back((asset_no, report));
                        (asset_no, report)
                    }));
            }
            if self.runtime_capture_enabled {
                self.runtime_match_outcomes.push((asset_no, outcome));
            }
        }
        for action in generated_risk_actions {
            self.shared_risk_actions.push(action);
            match action {
                RiskAction::Cancel {
                    instrument_id,
                    order_id,
                    ..
                } => {
                    if let Some(action_asset) = self.asset_for_instrument(instrument_id) {
                        self.record_audit(
                            self.cur_ts,
                            EventPhase::PostTradeRisk,
                            action_asset,
                            AuditKind::RiskDecision,
                            order_id,
                            0,
                            0.0,
                            0.0,
                        );
                        self.exch[action_asset].cancel_from_risk(self.cur_ts, order_id)?;
                        self.evs.update_local_order(
                            action_asset,
                            self.exch[action_asset].earliest_send_order_timestamp(),
                        );
                    }
                }
                RiskAction::Liquidate {
                    venue_id,
                    instrument_id,
                    ..
                } => {
                    let Some(action_asset) = self.asset_for_instrument(instrument_id) else {
                        continue;
                    };
                    self.record_audit(
                        self.cur_ts,
                        EventPhase::PostTradeRisk,
                        action_asset,
                        AuditKind::Liquidation,
                        0,
                        0,
                        0.0,
                        0.0,
                    );
                    let qty = self
                        .shared_exchange_portfolio
                        .venue(venue_id)
                        .map(|account| account.account().position(instrument_id).qty)
                        .unwrap_or(0.0);
                    if qty == 0.0 {
                        continue;
                    }
                    if !self.pending_liquidations.insert((venue_id, instrument_id)) {
                        continue;
                    }
                    let order_id = self.next_risk_order_id;
                    self.next_risk_order_id = self.next_risk_order_id.saturating_sub(1);
                    self.risk_order_instruments
                        .insert(order_id, (venue_id, instrument_id));
                    let side = if qty > 0.0 { Side::Sell } else { Side::Buy };
                    self.register_runtime_order_extensions(action_asset, order_id, true);
                    self.local[action_asset].submit_order(
                        order_id,
                        side,
                        0.0,
                        qty.abs(),
                        OrdType::Market,
                        TimeInForce::IOC,
                        self.cur_ts,
                    )?;
                    self.evs.update_exch_order(
                        action_asset,
                        self.exch[action_asset].earliest_recv_order_timestamp(),
                    );
                }
            }
        }
        Ok(())
    }

    fn asset_for_instrument(&self, instrument_id: InstrumentId) -> Option<usize> {
        self.shared_tick_coordinators
            .iter()
            .position(|coordinator| {
                coordinator
                    .as_ref()
                    .is_some_and(|coordinator| coordinator.spec().instrument_id == instrument_id)
            })
    }

    fn deliver_shared_reports(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        exchange_ts: i64,
        delivery_ts: i64,
    ) -> Result<(), BacktestError> {
        let pending_len = self.shared_pending_reports.len();
        for _ in 0..pending_len {
            let (pending_asset, mut report) = self.shared_pending_reports.pop_front().unwrap();
            if pending_asset == asset_no
                && report.order_id == order_id
                && report.exchange_ts <= exchange_ts
            {
                report.delivery_ts = delivery_ts;
                let local = self
                    .shared_local_portfolio
                    .venue_mut_or_insert(report.venue_id);
                let projected = self.shared_projector.project(report, local)?;
                if self.runtime_capture_enabled {
                    self.shared_projected_events
                        .extend(projected.iter().copied().map(|event| (asset_no, event)));
                }
                let spec = self.shared_tick_coordinators[asset_no]
                    .as_ref()
                    .unwrap()
                    .spec();
                let account = self
                    .shared_local_portfolio
                    .venue(spec.venue_id)
                    .unwrap()
                    .account();
                let position = account.position(spec.instrument_id);
                let fee = account.fee(spec.settlement_currency);
                let funding = account.funding(spec.settlement_currency);
                self.shared_state_values[asset_no] = StateValues {
                    position: position.qty,
                    // Preserve the legacy ABI's gross-cash definition while canonical account
                    // balance remains net of fee/funding.
                    balance: account.balance(spec.settlement_currency) + fee - funding,
                    fee,
                    num_trades: position.num_trades as i64,
                    trading_volume: position.trading_volume,
                    trading_value: position.trading_value,
                };
            } else {
                self.shared_pending_reports
                    .push_back((pending_asset, report));
            }
        }
        Ok(())
    }
}

impl<P: Processor> Deref for BacktestProcessorState<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.processor
    }
}

impl<P: Processor> DerefMut for BacktestProcessorState<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.processor
    }
}

/// Per asset backtesting state used internally to advance event buffers.
pub struct BacktestProcessorState<P: Processor> {
    data: Data<Event>,
    processor: P,
    reader: Reader<Event>,
    row: Option<usize>,
}

impl<P: Processor> BacktestProcessorState<P> {
    fn new(processor: P, reader: Reader<Event>) -> BacktestProcessorState<P> {
        Self {
            data: Data::empty(),
            processor,
            reader,
            row: None,
        }
    }

    fn reset(&mut self) {
        if !self.data.is_empty() {
            self.reader
                .release(std::mem::replace(&mut self.data, Data::empty()));
        }
        self.reader.reset();
        self.row = None;
        self.processor.reset();
    }

    /// Get the index of the next available row, only advancing the reader if there's no
    /// row currently available.
    fn next_row(&mut self) -> Result<usize, BacktestError> {
        if self.row.is_none() {
            let _ = self.advance()?;
        }

        self.row.ok_or(BacktestError::EndOfData)
    }

    /// Advance the state of this processor to the next available event and return the
    /// timestamp it occurred at, if any.
    fn advance(&mut self) -> Result<i64, BacktestError> {
        loop {
            let start = self.row.map(|rn| rn + 1).unwrap_or(0);

            for rn in start..self.data.len() {
                if let Some(ts) = self.processor.event_seen_timestamp(&self.data[rn]) {
                    self.row = Some(rn);
                    return Ok(ts);
                }
            }

            let next = self.reader.next_data()?;

            self.reader.release(std::mem::replace(&mut self.data, next));
            self.row = None;
        }
    }
}

impl<MD> Backtest<MD>
where
    MD: MarketDepth,
{
    fn execution_request(
        &self,
        asset_no: usize,
        order_id: OrderId,
        side: Side,
        price: f64,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Option<ExecutionOrderRequest> {
        let coordinator = self
            .shared_tick_coordinators
            .get(asset_no)
            .and_then(Option::as_ref)?;
        let spec = coordinator.spec();
        Some(ExecutionOrderRequest {
            client_order_id: order_id,
            venue_id: spec.venue_id,
            instrument_id: spec.instrument_id,
            price,
            qty,
            side,
            time_in_force,
            order_type,
            reduce_only: self
                .runtime_reduce_only
                .get(&(asset_no, order_id))
                .copied()
                .unwrap_or(false),
            origin: OrderOrigin::Strategy,
            local_submit_ts: self.cur_ts,
        })
    }

    fn local_rejection_reason(
        &mut self,
        asset_no: usize,
        request: &ExecutionOrderRequest,
    ) -> Option<ExecutionReason> {
        let spec = self.shared_tick_coordinators[asset_no]
            .as_ref()
            .unwrap()
            .spec();
        let validation = match request.order_type {
            OrdType::Limit => spec.validate_limit_order(request.price, request.qty),
            OrdType::Market => spec.validate_quantity(request.qty),
            OrdType::Unsupported => return Some(ExecutionReason::InvalidInstrument),
        };
        if let Err(error) = validation {
            return Some(execution_reason_from_spec(error));
        }
        match self
            .shared_local_risk
            .check(request, &self.shared_local_portfolio)
        {
            RiskDecision::Allow => None,
            RiskDecision::Reject { reason } => Some(execution_reason_from_risk(reason)),
        }
    }

    fn emit_local_rejection(
        &mut self,
        asset_no: usize,
        request: ExecutionOrderRequest,
        reason: ExecutionReason,
    ) -> Result<(), BacktestError> {
        let order = self.local[asset_no].reject_order(
            request.client_order_id,
            request.side,
            request.price,
            request.qty,
            request.order_type,
            request.time_in_force,
            request.local_submit_ts,
        )?;
        if self.runtime_capture_enabled {
            self.runtime_order_events
                .push((asset_no, request.local_submit_ts, order));
        }
        self.shared_tick_coordinators[asset_no]
            .as_mut()
            .unwrap()
            .reject_local(request, reason, &mut self.shared_report_scratch)?;
        for report in self.shared_report_scratch.drain(..) {
            self.audit.record(AuditRecord {
                run_id: self.audit_run_id,
                schema_version: 1,
                key: EventKey {
                    timestamp: report.exchange_ts,
                    phase: EventPhase::StrategyCallback,
                    source_priority: 0,
                    venue_no: report.venue_id.0,
                    asset_no: asset_no as u32,
                    sequence: self.audit_sequence,
                },
                kind: AuditKind::ExecutionReport,
                order_id: report.order_id,
                code: crate::runtime::execution_reason_code(report.reason),
                value0: 0.0,
                value1: 0.0,
            });
            self.audit_sequence = self.audit_sequence.wrapping_add(1);
            self.shared_exchange_reports.push((asset_no, report));
            let local = self
                .shared_local_portfolio
                .venue_mut_or_insert(report.venue_id);
            let projected = self.shared_projector.project(report, local)?;
            if self.runtime_capture_enabled {
                self.shared_projected_events
                    .extend(projected.iter().copied().map(|event| (asset_no, event)));
            }
        }
        self.runtime_reduce_only
            .remove(&(asset_no, request.client_order_id));
        Ok(())
    }

    fn submit_with_local_risk(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        side: Side,
        price: f64,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Result<bool, BacktestError> {
        self.record_audit(
            self.cur_ts,
            EventPhase::StrategyCallback,
            asset_no,
            AuditKind::Command,
            order_id,
            1,
            price,
            qty,
        );
        let Some(request) = self.execution_request(
            asset_no,
            order_id,
            side,
            price,
            qty,
            order_type,
            time_in_force,
        ) else {
            self.local[asset_no].submit_order(
                order_id,
                side,
                price,
                qty,
                order_type,
                time_in_force,
                self.cur_ts,
            )?;
            return Ok(false);
        };
        if self.shared_tick_coordinators[asset_no]
            .as_ref()
            .unwrap()
            .coordinator()
            .order(order_id)
            .is_some()
        {
            self.record_audit(
                self.cur_ts,
                EventPhase::StrategyCallback,
                asset_no,
                AuditKind::RiskDecision,
                order_id,
                crate::runtime::execution_reason_code(ExecutionReason::DuplicateOrderId),
                0.0,
                0.0,
            );
            self.shared_tick_coordinators[asset_no]
                .as_mut()
                .unwrap()
                .reject_duplicate_local(request, &mut self.shared_report_scratch);
            for report in self.shared_report_scratch.drain(..) {
                self.audit.record(AuditRecord {
                    run_id: self.audit_run_id,
                    schema_version: 1,
                    key: EventKey {
                        timestamp: report.exchange_ts,
                        phase: EventPhase::StrategyCallback,
                        source_priority: 0,
                        venue_no: report.venue_id.0,
                        asset_no: asset_no as u32,
                        sequence: self.audit_sequence,
                    },
                    kind: AuditKind::ExecutionReport,
                    order_id: report.order_id,
                    code: crate::runtime::execution_reason_code(report.reason),
                    value0: 0.0,
                    value1: 0.0,
                });
                self.audit_sequence = self.audit_sequence.wrapping_add(1);
                self.shared_exchange_reports.push((asset_no, report));
                let local = self
                    .shared_local_portfolio
                    .venue_mut_or_insert(report.venue_id);
                let projected = self.shared_projector.project(report, local)?;
                if self.runtime_capture_enabled {
                    self.shared_projected_events
                        .extend(projected.iter().copied().map(|event| (asset_no, event)));
                }
            }
            return Ok(true);
        }
        if let Some(reason) = self.local_rejection_reason(asset_no, &request) {
            self.record_audit(
                self.cur_ts,
                EventPhase::StrategyCallback,
                asset_no,
                AuditKind::RiskDecision,
                order_id,
                crate::runtime::execution_reason_code(reason),
                0.0,
                0.0,
            );
            self.emit_local_rejection(asset_no, request, reason)?;
            return Ok(true);
        }
        self.record_audit(
            self.cur_ts,
            EventPhase::StrategyCallback,
            asset_no,
            AuditKind::RiskDecision,
            order_id,
            0,
            0.0,
            0.0,
        );
        self.local[asset_no].submit_order(
            order_id,
            side,
            price,
            qty,
            order_type,
            time_in_force,
            self.cur_ts,
        )?;
        Ok(false)
    }

    pub fn builder() -> BacktestBuilder<MD> {
        BacktestBuilder {
            local: vec![],
            exch: vec![],
            outcome_buses: vec![],
            shared_execution: vec![],
        }
    }

    pub fn new(
        local: Vec<Box<dyn LocalProcessor<MD>>>,
        exch: Vec<Box<dyn Processor>>,
        reader: Vec<Reader<Event>>,
    ) -> Self {
        let num_assets = local.len();
        if local.len() != num_assets || exch.len() != num_assets || reader.len() != num_assets {
            panic!();
        }

        let local = local
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();
        let exch = exch
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();

        Self {
            local,
            exch,
            outcome_buses: (0..num_assets).map(|_| None).collect(),
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets),
            runtime_feed_events: Vec::new(),
            runtime_order_events: Vec::new(),
            runtime_match_outcomes: Vec::new(),
            shared_tick_coordinators: (0..num_assets).map(|_| None).collect(),
            shared_exchange_reports: Vec::new(),
            shared_report_scratch: Vec::with_capacity(2),
            shared_pending_reports: VecDeque::with_capacity(64),
            shared_exchange_portfolio: ExchangePortfolio::default(),
            shared_initial_balances: HashMap::new(),
            shared_venue_risks: HashMap::new(),
            shared_local_risk: Box::new(AllowAllRisk),
            shared_risk_actions: RiskActionSink::with_capacity(4),
            runtime_reduce_only: HashMap::new(),
            next_risk_order_id: u64::MAX,
            pending_liquidations: HashSet::new(),
            risk_order_instruments: HashMap::new(),
            shared_local_portfolio: PortfolioLedger::default(),
            shared_projector: ExecutionEventProjector::with_capacity(3),
            shared_projected_events: Vec::new(),
            shared_delivery_scratch: Vec::with_capacity(4),
            shared_state_values: vec![StateValues::default(); num_assets],
            audit: AuditRecorder::disabled(),
            audit_run_id: 0,
            audit_sequence: 0,
            runtime_capture_enabled: false,
        }
    }

    fn initialize_evs(&mut self) -> Result<(), BacktestError> {
        for (asset_no, local) in self.local.iter_mut().enumerate() {
            match local.advance() {
                Ok(ts) => self.evs.update_local_data(asset_no, ts),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_local_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        for (asset_no, exch) in self.exch.iter_mut().enumerate() {
            match exch.advance() {
                Ok(ts) => self.evs.update_exch_data(asset_no, ts),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_exch_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn goto_end(&mut self) -> Result<ElapseResult, BacktestError> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(UNTIL_END_OF_DATA, WaitOrderResponse::None)
    }

    fn goto<const WAIT_NEXT_FEED: bool>(
        &mut self,
        timestamp: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BacktestError> {
        let mut result = ElapseResult::Ok;
        let mut timestamp = timestamp;
        for (asset_no, local) in self.local.iter().enumerate() {
            self.evs
                .update_exch_order(asset_no, local.earliest_send_order_timestamp());
            self.evs
                .update_local_order(asset_no, local.earliest_recv_order_timestamp());
        }
        loop {
            match self.evs.next() {
                Some(ev) => {
                    if ev.timestamp > timestamp {
                        self.cur_ts = timestamp;
                        return Ok(result);
                    }
                    match ev.kind {
                        EventIntentKind::LocalData => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let mut processed = None;
                            let next = local.next_row().and_then(|row| {
                                local.processor.process(&local.data[row])?;
                                processed = Some(local.data[row].clone());
                                local.advance()
                            });
                            if self.runtime_capture_enabled
                                && let Some(event) = processed
                            {
                                self.runtime_feed_events.push((ev.asset_no, event));
                            }

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_local_data(ev.asset_no, next_ts);
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_local_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            if WAIT_NEXT_FEED {
                                timestamp = ev.timestamp;
                                result = ElapseResult::MarketFeed;
                            }
                        }
                        EventIntentKind::LocalOrder => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let wait_order_resp_id = match wait_order_response {
                                WaitOrderResponse::Specified {
                                    asset_no: wait_order_asset_no,
                                    order_id: wait_order_id,
                                } if ev.asset_no == wait_order_asset_no => Some(wait_order_id),
                                _ => None,
                            };
                            let capture = self.runtime_capture_enabled;
                            let shared_enabled =
                                self.shared_tick_coordinators[ev.asset_no].is_some();
                            let order_events = &mut self.runtime_order_events;
                            let delivery_keys = &mut self.shared_delivery_scratch;
                            delivery_keys.clear();
                            if local.process_recv_order_with_handler(
                                ev.timestamp,
                                wait_order_resp_id,
                                &mut |order| {
                                    if capture {
                                        order_events.push((
                                            ev.asset_no,
                                            ev.timestamp,
                                            order.clone(),
                                        ))
                                    }
                                    if shared_enabled {
                                        delivery_keys.push((order.order_id, order.exch_timestamp));
                                    }
                                },
                            )? || wait_order_response == WaitOrderResponse::Any
                            {
                                timestamp = ev.timestamp;
                                if WAIT_NEXT_FEED {
                                    result = ElapseResult::OrderResponse;
                                }
                            }
                            let next_local_order_ts = local.earliest_recv_order_timestamp();
                            while let Some((order_id, exchange_ts)) =
                                self.shared_delivery_scratch.pop()
                            {
                                self.deliver_shared_reports(
                                    ev.asset_no,
                                    order_id,
                                    exchange_ts,
                                    ev.timestamp,
                                )?;
                            }
                            self.evs
                                .update_local_order(ev.asset_no, next_local_order_ts);
                        }
                        EventIntentKind::ExchData => {
                            let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                            let next = exch.next_row().and_then(|row| {
                                exch.processor.process(&exch.data[row])?;
                                exch.advance()
                            });

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_exch_data(ev.asset_no, next_ts);
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_exch_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            self.evs.update_local_order(
                                ev.asset_no,
                                exch.earliest_send_order_timestamp(),
                            );
                            self.drain_exchange_outcomes(ev.asset_no)?;
                        }
                        EventIntentKind::ExchOrder => {
                            let arriving_order_id = self.exch[ev.asset_no]
                                .peek_recv_order(ev.timestamp)
                                .filter(|order| order.req == Status::New)
                                .map(|order| order.order_id);
                            let risk_rejection = self.exch[ev.asset_no]
                                .peek_recv_order(ev.timestamp)
                                .filter(|order| order.req == Status::New)
                                .and_then(|order| {
                                    let coordinator = self
                                        .shared_tick_coordinators
                                        .get(ev.asset_no)
                                        .and_then(Option::as_ref)?;
                                    let spec = coordinator.spec();
                                    let request = ExecutionOrderRequest {
                                        client_order_id: order.order_id,
                                        venue_id: spec.venue_id,
                                        instrument_id: spec.instrument_id,
                                        price: order.price(),
                                        qty: order.qty,
                                        side: order.side,
                                        time_in_force: order.time_in_force,
                                        order_type: order.order_type,
                                        reduce_only: self
                                            .runtime_reduce_only
                                            .get(&(ev.asset_no, order.order_id))
                                            .copied()
                                            .unwrap_or(false),
                                        origin: OrderOrigin::Strategy,
                                        local_submit_ts: order.local_timestamp,
                                    };
                                    if request.reduce_only {
                                        let signed_qty = match request.side {
                                            Side::Buy => request.qty,
                                            Side::Sell => -request.qty,
                                            _ => return Some(ExecutionReason::ReduceOnlyViolation),
                                        };
                                        let old_qty = self
                                            .shared_exchange_portfolio
                                            .venue(spec.venue_id)
                                            .map_or(0.0, |account| {
                                                account.account().position(spec.instrument_id).qty
                                            });
                                        let new_qty = old_qty + signed_qty;
                                        if old_qty == 0.0 || new_qty.abs() >= old_qty.abs() {
                                            return Some(ExecutionReason::ReduceOnlyViolation);
                                        }
                                    }
                                    let risk = self.shared_venue_risks.get_mut(&spec.venue_id)?;
                                    let account = self
                                        .shared_exchange_portfolio
                                        .venue_mut_or_insert(spec.venue_id);
                                    match risk.check_arrival(&request, account) {
                                        RiskDecision::Allow => None,
                                        RiskDecision::Reject { reason } => {
                                            Some(execution_reason_from_risk(reason))
                                        }
                                    }
                                });
                            let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                            if let Some(reason) = risk_rejection {
                                let _ = exch.reject_recv_order(ev.timestamp, reason)?;
                            } else {
                                let _ = exch.process_recv_order(ev.timestamp, None)?;
                            }
                            if let Some(order_id) = arriving_order_id {
                                self.runtime_reduce_only.remove(&(ev.asset_no, order_id));
                            }
                            self.evs.update_exch_order(
                                ev.asset_no,
                                exch.earliest_recv_order_timestamp(),
                            );
                            self.evs.update_local_order(
                                ev.asset_no,
                                exch.earliest_send_order_timestamp(),
                            );
                            self.drain_exchange_outcomes(ev.asset_no)?;
                        }
                    }
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
    }
}

impl<MD> Bot<MD> for Backtest<MD>
where
    MD: MarketDepth,
{
    type Error = BacktestError;

    #[inline]
    fn current_timestamp(&self) -> i64 {
        self.cur_ts
    }

    #[inline]
    fn num_assets(&self) -> usize {
        self.local.len()
    }

    #[inline]
    fn position(&self, asset_no: usize) -> f64 {
        if self.shared_tick_coordinators[asset_no].is_some() {
            self.shared_state_values[asset_no].position
        } else {
            self.local.get(asset_no).unwrap().position()
        }
    }

    #[inline]
    fn state_values(&self, asset_no: usize) -> &StateValues {
        if self.shared_tick_coordinators[asset_no].is_some() {
            &self.shared_state_values[asset_no]
        } else {
            self.local.get(asset_no).unwrap().state_values()
        }
    }

    fn depth(&self, asset_no: usize) -> &MD {
        self.local.get(asset_no).unwrap().depth()
    }

    fn last_trades(&self, asset_no: usize) -> &[Event] {
        self.local.get(asset_no).unwrap().last_trades()
    }

    #[inline]
    fn clear_last_trades(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(an) => {
                let local = self.local.get_mut(an).unwrap();
                local.clear_last_trades();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_last_trades();
                }
            }
        }
    }

    #[inline]
    fn orders(&self, asset_no: usize) -> &HashMap<u64, Order> {
        self.local.get(asset_no).unwrap().orders()
    }

    #[inline]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let rejected = self.submit_with_local_risk(
            asset_no,
            order_id,
            Side::Buy,
            price,
            qty,
            order_type,
            time_in_force,
        )?;
        if rejected {
            return Ok(if wait {
                ElapseResult::OrderResponse
            } else {
                ElapseResult::Ok
            });
        }

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let rejected = self.submit_with_local_risk(
            asset_no,
            order_id,
            Side::Sell,
            price,
            qty,
            order_type,
            time_in_force,
        )?;
        if rejected {
            return Ok(if wait {
                ElapseResult::OrderResponse
            } else {
                ElapseResult::Ok
            });
        }

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let rejected = self.submit_with_local_risk(
            asset_no,
            order.order_id,
            order.side,
            order.price,
            order.qty,
            order.order_type,
            order.time_in_force,
        )?;
        if rejected {
            return Ok(if wait {
                ElapseResult::OrderResponse
            } else {
                ElapseResult::Ok
            });
        }

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified {
                    asset_no,
                    order_id: order.order_id,
                },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn modify(
        &mut self,
        _asset_no: usize,
        _order_id: OrderId,
        _price: f64,
        _qty: f64,
        _wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        Err(BacktestError::UnsupportedOperation(
            "order modification is disabled; cancel and submit a replacement order",
        ))
    }

    #[inline]
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.cancel(order_id, self.cur_ts)?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(asset_no) => {
                self.local
                    .get_mut(asset_no)
                    .unwrap()
                    .clear_inactive_orders();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_inactive_orders();
                }
            }
        }
    }

    #[inline]
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, BacktestError> {
        self.goto::<false>(
            self.cur_ts + timeout,
            WaitOrderResponse::Specified { asset_no, order_id },
        )
    }

    #[inline]
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        if include_order_resp {
            self.goto::<true>(self.cur_ts + timeout, WaitOrderResponse::Any)
        } else {
            self.goto::<true>(self.cur_ts + timeout, WaitOrderResponse::None)
        }
    }

    #[inline]
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(self.cur_ts + duration, WaitOrderResponse::None)
    }

    #[inline]
    fn elapse_bt(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        self.elapse(duration)
    }

    #[inline]
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline]
    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.local.get(asset_no).unwrap().feed_latency()
    }

    #[inline]
    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.local.get(asset_no).unwrap().order_latency()
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use crate::{
        backtest::{
            Backtest, BacktestError, DataSource,
            ExchangeKind::{NoPartialFillExchange, PartialFillExchange},
            L2AssetBuilder, L3AssetBuilder,
            assettype::LinearAsset,
            data::Data,
            execution::{
                CashFlowMode, CrossMarginRisk, CurrencyId, ExchangeAccountState, ExchangeRisk,
                ExecutionOrderRequest, ExecutionReason, ExecutionReportKind, InstrumentId,
                InstrumentSpec, InstrumentType, LocalPreTradeRisk, MarginParameters, MatchOutcome,
                PortfolioLedger, PostTradeRisk, RateFeeModel, RiskAction, RiskActionSink,
                RiskDecision, RiskReason, SharedTickExecutionConfig, VenueId,
            },
            models::{
                CommonFees, ConstantLatency, L3FIFOQueueModel, PowerProbQueueFunc3, ProbQueueModel,
                RiskAdverseQueueModel, TradingValueFeeModel,
            },
            result::AuditKind,
        },
        depth::HashMapMarketDepth,
        prelude::{Bot, Event, OrdType, Status, TimeInForce},
        types::{
            ADD_ORDER_EVENT, BUY_EVENT, EXCH_ASK_DEPTH_EVENT, EXCH_BID_DEPTH_EVENT, EXCH_EVENT,
            EXCH_SELL_TRADE_EVENT, FILL_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT,
            LOCAL_EVENT, LOCAL_SELL_TRADE_EVENT, SELL_EVENT,
        },
    };

    struct LiquidateAfterFill {
        venue_id: VenueId,
        instrument_id: InstrumentId,
        cancel_order_id: Option<u64>,
    }

    struct RejectLocalRisk;

    impl LocalPreTradeRisk for RejectLocalRisk {
        fn check(
            &mut self,
            _request: &ExecutionOrderRequest,
            _portfolio: &PortfolioLedger,
        ) -> RiskDecision {
            RiskDecision::Reject {
                reason: RiskReason::PositionLimit,
            }
        }
    }

    impl ExchangeRisk for LiquidateAfterFill {
        fn check_arrival(
            &mut self,
            _request: &ExecutionOrderRequest,
            _account: &ExchangeAccountState,
        ) -> RiskDecision {
            RiskDecision::Allow
        }
    }

    impl PostTradeRisk for LiquidateAfterFill {
        fn on_account_change(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink) {
            if account.account().position(self.instrument_id).qty > 0.0 {
                if let Some(order_id) = self.cancel_order_id {
                    out.push(RiskAction::Cancel {
                        venue_id: self.venue_id,
                        instrument_id: self.instrument_id,
                        order_id,
                        reason: RiskReason::Custom(98),
                    });
                }
                out.push(RiskAction::Liquidate {
                    venue_id: self.venue_id,
                    instrument_id: self.instrument_id,
                    reason: RiskReason::Custom(99),
                });
            }
        }
    }

    #[test]
    fn skips_unseen_events() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT | EXCH_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(50, 50))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(0.01, 1.0))
                    .build()?,
            )
            .build()?;

        // Process first events and advance a single timestep
        backtester.elapse_bt(1)?;
        assert_eq!(1, backtester.cur_ts);

        // Check that we correctly skip past events that aren't seen by a given processor
        backtester.elapse_bt(1)?;
        assert_eq!(2, backtester.cur_ts);
        assert_eq!(Some(3), backtester.local[0].row);
        assert_eq!(Some(2), backtester.exch[0].row);

        backtester.elapse_bt(1)?;
        assert_eq!(3, backtester.cur_ts);
        assert!(backtester.runtime_feed_events().is_empty());

        backtester.set_runtime_capture(true);
        backtester.elapse_bt(1)?;
        assert_eq!(1, backtester.runtime_feed_events().len());
        backtester.set_runtime_capture(false);
        assert!(backtester.runtime_feed_events().is_empty());
        assert!(matches!(
            backtester.modify(0, 1, 1.0, 1.0, false),
            Err(BacktestError::UnsupportedOperation(_))
        ));

        backtester.configure_shared_tick_local_risk(RejectLocalRisk);
        backtester.enable_audit(9, 16);
        backtester.set_runtime_capture(true);
        assert_eq!(
            backtester.submit_buy_order(0, 77, 1.0, 1.0, TimeInForce::GTC, OrdType::Limit, true,)?,
            crate::types::ElapseResult::OrderResponse
        );
        assert_eq!(backtester.orders(0)[&77].status, Status::Rejected);
        assert_eq!(backtester.runtime_order_events().len(), 1);
        assert!(
            backtester
                .shared_exchange_reports()
                .iter()
                .any(|(_, report)| {
                    report.order_id == 77 && report.reason == ExecutionReason::PositionLimit
                })
        );
        assert!(backtester.audit().records().iter().any(|record| {
            record.run_id == 9
                && record.order_id == 77
                && record.kind == AuditKind::RiskDecision
                && record.code
                    == crate::runtime::execution_reason_code(ExecutionReason::PositionLimit)
        }));

        Ok(())
    }

    #[test]
    fn tick_reset_replays_identically_one_hundred_times() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 100,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 100,
                px: 101.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT,
                exch_ts: 1_000,
                local_ts: 1_000,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 10))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(0.01, 1.0))
                    .build()?,
            )
            .build()?;
        backtester.set_shared_exchange_balance(VenueId(0), CurrencyId(0), 100.0)?;
        let mut expected = None;
        let mut expected_result = None;
        for run in 0..100 {
            backtester.elapse_bt(1)?;
            backtester.submit_buy_order(
                0,
                42,
                0.0,
                2.0,
                TimeInForce::IOC,
                OrdType::Market,
                false,
            )?;
            backtester.elapse_bt(100)?;
            let reports: Vec<_> = backtester
                .shared_exchange_reports()
                .iter()
                .map(|(_, report)| {
                    (
                        report.kind,
                        report.reason,
                        report.order_id,
                        report.venue_order_id,
                        report.exchange_ts,
                        report.delivery_ts,
                        report.sequence,
                        report.status,
                        report.exec_price.to_bits(),
                        report.exec_qty.to_bits(),
                    )
                })
                .collect();
            assert_eq!(reports.len(), 2);
            assert_eq!(backtester.position(0), 2.0);
            let identity = crate::backtest::result::ModelIdentity::new("test", 1);
            let result = backtester.shared_backtest_result(
                run + 1,
                crate::backtest::result::ReproducibilityMetadata {
                    engine_version: env!("CARGO_PKG_VERSION").into(),
                    git_revision: "test".into(),
                    strategy_id: "tick-reset".into(),
                    strategy_version: "1".into(),
                    runtime_abi_version: crate::runtime::STRATEGY_ABI_VERSION,
                    phase_contract_version: crate::backtest::scheduler::PHASE_CONTRACT_VERSION,
                    data_manifest_hash: 1,
                    config_hash: 2,
                    matching: identity.clone(),
                    fee: identity.clone(),
                    latency: identity.clone(),
                    risk: identity.clone(),
                    execution_quality: identity,
                    random_seed: 3,
                },
                crate::backtest::result::EndPolicy::DrainAll,
                crate::backtest::result::RunTermination::DataEnd,
            );
            let result_core = (
                result.order_count,
                result.fill_count,
                result.reject_count,
                result.exchange_final.clone(),
                result.local_delivered_final.clone(),
            );
            if let Some(expected) = &expected_result {
                assert_eq!(&result_core, expected);
            } else {
                expected_result = Some(result_core);
            }
            if let Some(expected) = &expected {
                assert_eq!(&reports, expected);
            } else {
                expected = Some(reports);
            }
            if run != 99 {
                backtester.reset();
                assert_eq!(backtester.current_timestamp(), i64::MAX);
                assert!(backtester.shared_exchange_reports().is_empty());
                assert!(backtester.runtime_order_events().is_empty());
                assert!(backtester.audit().records().is_empty());
                assert_eq!(
                    backtester
                        .shared_exchange_portfolio()
                        .venue(VenueId(0))
                        .unwrap()
                        .account()
                        .balance(CurrencyId(0)),
                    100.0
                );
                assert_eq!(
                    backtester
                        .shared_local_portfolio()
                        .total_balance(CurrencyId(0)),
                    100.0
                );
            }
        }
        Ok(())
    }

    /// Characterizes the legacy Tick path before shared-execution adapters are connected.
    /// Keep the exact timestamps and account values stable during P0-B migration.
    #[test]
    fn legacy_tick_market_fill_latency_and_account_golden() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 101.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            // Keeps the legacy event loop open beyond the order response horizon.
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.001)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        backtester.configure_shared_tick_execution_with_fees([SharedTickExecutionConfig::new(
            InstrumentSpec {
                instrument_id: InstrumentId(1),
                asset_no: 0,
                venue_id: VenueId(1),
                tick_size: 1.0,
                lot_size: 1.0,
                min_qty: 1.0,
                max_qty: 1_000_000.0,
                min_notional: 0.0,
                contract_size: 1.0,
                price_currency: CurrencyId(1),
                settlement_currency: CurrencyId(1),
                margin_currency: CurrencyId(1),
                instrument_type: InstrumentType::LinearPerpetual,
                cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
                version: 1,
            },
            RateFeeModel {
                maker_rate: 0.0,
                taker_rate: 0.001,
                currency: CurrencyId(1),
            },
        )])?;

        backtester.elapse_bt(6)?;
        assert_eq!(backtester.current_timestamp(), 106);
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 42, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;

        // Entry reaches exchange at 116, but the strategy-visible account cannot change yet.
        backtester.elapse_bt(10)?;
        assert_eq!(backtester.current_timestamp(), 116);
        assert_eq!(backtester.position(0), 0.0);
        assert!(backtester.runtime_order_events().is_empty());
        assert_eq!(backtester.runtime_match_outcomes().len(), 1);
        assert_eq!(backtester.runtime_match_outcomes()[0].0, 0);
        assert_eq!(backtester.runtime_match_outcomes()[0].1.order_id, 42);
        assert!(matches!(
            backtester.runtime_match_outcomes()[0].1.outcome,
            MatchOutcome::Fill(fill)
                if fill.exchange_ts == 116 && fill.price == 101.0 && fill.qty == 1.0
        ));
        assert_eq!(backtester.shared_exchange_reports().len(), 2);
        assert_eq!(backtester.shared_exchange_reports()[0].1.exchange_ts, 116);
        assert_eq!(backtester.shared_exchange_reports()[1].1.exec_qty, 1.0);
        assert_eq!(
            backtester.shared_exchange_reports()[1]
                .1
                .account_delta
                .unwrap()
                .fee,
            0.101
        );
        assert!(
            backtester
                .shared_local_portfolio()
                .venue(VenueId(1))
                .is_none()
        );

        // Response arrives at 136 and applies the one immutable fill to local state.
        backtester.elapse_bt(20)?;
        assert_eq!(backtester.current_timestamp(), 136);
        assert_eq!(backtester.position(0), 1.0);
        let state = backtester.state_values(0);
        assert_eq!(state.balance, -101.0);
        assert!((state.fee - 0.101).abs() < 1e-12);
        assert_eq!(state.num_trades, 1);
        assert_eq!(state.trading_volume, 1.0);
        assert_eq!(state.trading_value, 101.0);
        let shared_local = backtester
            .shared_local_portfolio()
            .venue(VenueId(1))
            .unwrap()
            .account();
        assert_eq!(shared_local.position(InstrumentId(1)).qty, 1.0);
        assert!((shared_local.balance(CurrencyId(1)) + 101.101).abs() < 1e-12);
        assert_eq!(
            shared_local.position(InstrumentId(1)).num_trades,
            state.num_trades as u64
        );
        assert_eq!(
            shared_local.position(InstrumentId(1)).trading_volume,
            state.trading_volume
        );
        assert_eq!(
            shared_local.position(InstrumentId(1)).trading_value,
            state.trading_value
        );
        assert_eq!(shared_local.fee(CurrencyId(1)), state.fee);
        // Legacy balance is gross cash flow with fee reported separately; the canonical ledger
        // stores net cash while retaining the same fee audit field.
        assert!((shared_local.balance(CurrencyId(1)) + state.fee - state.balance).abs() < 1e-12);
        assert_eq!(backtester.shared_projected_events().len(), 4);
        assert!(
            backtester
                .shared_projected_events()
                .iter()
                .all(|(_, event)| event.report.delivery_ts == 136)
        );
        let mut golden_hash = 0xcbf29ce484222325_u64;
        for (_, report) in backtester.shared_exchange_reports() {
            for value in [
                report.order_id,
                report.exchange_ts as u64,
                report.sequence,
                report.exec_price.to_bits(),
                report.exec_qty.to_bits(),
                report.account_delta.map_or(0, |delta| delta.fee.to_bits()),
            ] {
                for byte in value.to_le_bytes() {
                    golden_hash ^= u64::from(byte);
                    golden_hash = golden_hash.wrapping_mul(0x100000001b3);
                }
            }
        }
        assert_eq!(golden_hash, 14_703_535_995_109_120_096);
        assert_eq!(backtester.order_latency(0), Some((106, 116, 136)));

        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, 0);
        assert_eq!(events[0].1, 136);
        assert_eq!(events[0].2.order_id, 42);
        assert_eq!(events[0].2.status, Status::Filled);
        assert_eq!(events[0].2.exch_timestamp, 116);
        // Legacy Order keeps local submit time; the capture tuple carries response delivery time.
        assert_eq!(events[0].2.local_timestamp, 106);
        assert_eq!(events[0].2.exec_price(), 101.0);
        assert_eq!(events[0].2.exec_qty, 1.0);
        assert!(!events[0].2.maker);

        Ok(())
    }

    #[test]
    fn partial_ioc_delivers_every_fill_and_terminal_expiry() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 101.0,
                qty: 3.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 102.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        backtester.elapse_bt(6)?;
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 77, 102.0, 7.0, TimeInForce::IOC, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;

        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].2.status, Status::PartiallyFilled);
        assert_eq!(events[0].2.exec_price(), 101.0);
        assert_eq!(events[0].2.exec_qty, 3.0);
        assert_eq!(events[0].2.leaves_qty, 4.0);
        assert_eq!(events[1].2.status, Status::PartiallyFilled);
        assert_eq!(events[1].2.exec_price(), 102.0);
        assert_eq!(events[1].2.exec_qty, 2.0);
        assert_eq!(events[1].2.leaves_qty, 2.0);
        assert_eq!(events[2].2.status, Status::Expired);
        assert_eq!(events[2].2.leaves_qty, 2.0);
        assert!(events.iter().all(|event| event.1 == 136));

        assert_eq!(backtester.position(0), 5.0);
        let state = backtester.state_values(0);
        assert_eq!(state.balance, -507.0);
        assert_eq!(state.num_trades, 2);
        assert_eq!(state.trading_volume, 5.0);
        assert_eq!(state.trading_value, 507.0);
        assert_eq!(backtester.orders(0)[&77].status, Status::Expired);
        assert_eq!(backtester.shared_exchange_reports().len(), 4);
        let shared = backtester
            .shared_local_portfolio()
            .venue(VenueId(0))
            .unwrap()
            .account();
        assert_eq!(shared.position(InstrumentId(1)).qty, state.position);
        assert_eq!(shared.position(InstrumentId(1)).num_trades, 2);
        assert_eq!(shared.position(InstrumentId(1)).trading_value, 507.0);

        backtester.clear_runtime_order_events();
        backtester.submit_buy_order(0, 78, 102.0, 5.0, TimeInForce::FOK, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;
        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].2.status, Status::PartiallyFilled);
        assert_eq!(events[0].2.exec_qty, 3.0);
        assert_eq!(events[1].2.status, Status::Filled);
        assert_eq!(events[1].2.exec_qty, 2.0);
        assert_eq!(backtester.position(0), 10.0);
        assert_eq!(backtester.state_values(0).num_trades, 4);
        assert_eq!(backtester.state_values(0).trading_value, 1_014.0);
        assert_eq!(backtester.shared_exchange_reports().len(), 7);
        assert_eq!(
            backtester
                .shared_local_portfolio()
                .venue(VenueId(0))
                .unwrap()
                .account()
                .position(InstrumentId(1))
                .qty,
            10.0
        );
        Ok(())
    }

    #[test]
    fn legacy_tick_same_time_fill_precedes_cancel_arrival() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 101.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_SELL_TRADE_EVENT | LOCAL_SELL_TRADE_EVENT,
                exch_ts: 150,
                local_ts: 155,
                px: 98.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        backtester.elapse_bt(6)?;
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 90, 99.0, 1.0, TimeInForce::GTC, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;
        assert_eq!(backtester.current_timestamp(), 136);
        assert_eq!(backtester.orders(0)[&90].status, Status::New);
        backtester.clear_runtime_order_events();

        backtester.elapse_bt(4)?;
        assert_eq!(backtester.current_timestamp(), 140);
        backtester.cancel(0, 90, false)?;
        backtester.elapse_bt(30)?;

        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].1, 170);
        assert_eq!(events[0].2.status, Status::Filled);
        assert_eq!(events[0].2.exec_price(), 99.0);
        assert_eq!(events[1].1, 170);
        assert_eq!(events[1].2.status, Status::New);
        assert_eq!(events[1].2.req, Status::Rejected);
        assert_eq!(backtester.position(0), 1.0);
        assert_eq!(backtester.orders(0)[&90].status, Status::Filled);
        assert_eq!(backtester.orders(0)[&90].req, Status::None);

        backtester.clear_runtime_order_events();
        backtester.submit_buy_order(0, 91, 101.0, 1.0, TimeInForce::GTX, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;
        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].2.order_id, 91);
        assert_eq!(events[0].2.status, Status::Expired);
        assert_eq!(events[0].2.exec_qty, 0.0);
        Ok(())
    }

    #[test]
    fn legacy_l3_fifo_full_order_lifecycle() -> Result<(), Box<dyn Error>> {
        let both = EXCH_EVENT | LOCAL_EVENT;
        let data = Data::from_data(&[
            Event {
                ev: both | BUY_EVENT | ADD_ORDER_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 1.0,
                order_id: 1_000,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: both | SELL_EVENT | ADD_ORDER_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 101.0,
                qty: 1.0,
                order_id: 2_000,
                ival: 0,
                fval: 0.0,
            },
            // Arrives after the backtest order and is therefore behind it in FIFO.
            Event {
                ev: both | BUY_EVENT | ADD_ORDER_EVENT,
                exch_ts: 130,
                local_ts: 135,
                px: 99.0,
                qty: 1.0,
                order_id: 1_001,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: both | BUY_EVENT | FILL_EVENT,
                exch_ts: 140,
                local_ts: 145,
                px: 99.0,
                qty: 1.0,
                order_id: 1_000,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: both | BUY_EVENT | FILL_EVENT,
                exch_ts: 150,
                local_ts: 155,
                px: 99.0,
                qty: 1.0,
                order_id: 1_001,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: both | BUY_EVENT | ADD_ORDER_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 98.0,
                qty: 1.0,
                order_id: 1_002,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let mut backtester = Backtest::builder()
            .add_asset(
                L3AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(L3FIFOQueueModel::new())
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        backtester.elapse_bt(6)?;
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 92, 99.0, 1.0, TimeInForce::GTC, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;
        assert_eq!(backtester.orders(0)[&92].status, Status::New);
        backtester.clear_runtime_order_events();
        backtester.elapse_bt(34)?;

        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, 170);
        assert_eq!(events[0].2.status, Status::Filled);
        assert_eq!(events[0].2.exec_price(), 99.0);
        assert!(events[0].2.maker);
        assert_eq!(backtester.position(0), 1.0);
        assert_eq!(backtester.shared_exchange_reports().len(), 2);
        let shared = backtester
            .shared_local_portfolio()
            .venue(VenueId(0))
            .unwrap()
            .account();
        assert_eq!(shared.position(InstrumentId(1)).qty, 1.0);
        assert_eq!(shared.position(InstrumentId(1)).num_trades, 1);
        assert_eq!(shared.position(InstrumentId(1)).trading_value, 99.0);
        Ok(())
    }

    #[test]
    fn legacy_l2_queue_advances_only_after_ahead_quantity_trades() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 101.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_SELL_TRADE_EVENT | LOCAL_SELL_TRADE_EVENT,
                exch_ts: 140,
                local_ts: 145,
                px: 99.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_SELL_TRADE_EVENT | LOCAL_SELL_TRADE_EVENT,
                exch_ts: 150,
                local_ts: 155,
                px: 99.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 99.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        backtester.elapse_bt(6)?;
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 93, 99.0, 1.0, TimeInForce::GTC, OrdType::Limit, false)?;
        backtester.elapse_bt(30)?;
        backtester.clear_runtime_order_events();
        backtester.elapse_bt(24)?;
        assert_eq!(backtester.current_timestamp(), 160);
        assert_eq!(backtester.position(0), 0.0);
        assert!(backtester.runtime_order_events().is_empty());
        backtester.elapse_bt(10)?;
        assert_eq!(backtester.position(0), 1.0);
        let events = backtester.runtime_order_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].2.status, Status::Filled);
        assert_eq!(events[0].2.exec_price(), 99.0);
        assert!(events[0].2.maker);
        Ok(())
    }

    #[test]
    fn tick_exchange_risk_checks_each_same_time_order_against_authoritative_account()
    -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_EVENT | LOCAL_ASK_DEPTH_EVENT,
                exch_ts: 100,
                local_ts: 105,
                px: 100.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_BID_DEPTH_EVENT | LOCAL_BID_DEPTH_EVENT,
                exch_ts: 1_000,
                local_ts: 1_005,
                px: 99.0,
                qty: 10.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);
        let spec = InstrumentSpec {
            instrument_id: InstrumentId(1),
            asset_no: 0,
            venue_id: VenueId(7),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 1_000.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: CashFlowMode::DerivativePnl,
            version: 1,
        };
        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(10, 20))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;
        backtester.configure_shared_tick_execution([spec.clone()])?;
        let mut risk = CrossMarginRisk::new(VenueId(7), CurrencyId(1));
        risk.register(
            spec,
            MarginParameters {
                initial_margin_rate: 0.6,
                maintenance_margin_rate: 0.3,
                max_leverage: 2.0,
            },
            100.0,
        )
        .unwrap();
        backtester.configure_shared_tick_venue_risk(VenueId(7), risk);
        backtester.set_shared_exchange_balance(VenueId(7), CurrencyId(1), 100.0)?;

        backtester.elapse_bt(6)?;
        backtester.set_runtime_capture(true);
        backtester.submit_buy_order(0, 501, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
        backtester.submit_buy_order(0, 502, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
        backtester.elapse_bt(40)?;

        assert_eq!(
            backtester
                .shared_exchange_portfolio()
                .venue(VenueId(7))
                .unwrap()
                .account()
                .position(InstrumentId(1))
                .qty,
            1.0
        );
        assert!(
            backtester
                .shared_exchange_reports()
                .iter()
                .any(|(_, report)| {
                    report.order_id == 502 && report.reason == ExecutionReason::InsufficientMargin
                })
        );
        assert_eq!(backtester.orders(0)[&501].status, Status::Filled);
        assert_eq!(backtester.orders(0)[&502].status, Status::Rejected);

        // reduce-only is evaluated against the exchange position, not the delayed local view.
        backtester.register_runtime_order_extensions(0, 503, true);
        backtester.submit_buy_order(0, 503, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
        backtester.register_runtime_order_extensions(0, 504, true);
        backtester.submit_sell_order(0, 504, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
        backtester.elapse_bt(40)?;
        assert_eq!(backtester.orders(0)[&503].status, Status::Rejected);
        assert_eq!(backtester.orders(0)[&504].status, Status::Filled);
        assert!(
            backtester
                .shared_exchange_reports()
                .iter()
                .any(|(_, report)| {
                    report.order_id == 503 && report.reason == ExecutionReason::ReduceOnlyViolation
                })
        );
        assert_eq!(
            backtester
                .shared_exchange_portfolio()
                .venue(VenueId(7))
                .unwrap()
                .account()
                .position(InstrumentId(1))
                .qty,
            0.0
        );

        backtester.configure_shared_tick_venue_risk(
            VenueId(7),
            LiquidateAfterFill {
                venue_id: VenueId(7),
                instrument_id: InstrumentId(1),
                cancel_order_id: Some(506),
            },
        );
        backtester.submit_sell_order(
            0,
            506,
            110.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            false,
        )?;
        backtester.elapse_bt(40)?;
        assert_eq!(backtester.orders(0)[&506].status, Status::New);
        backtester.submit_buy_order(0, 505, 0.0, 1.0, TimeInForce::IOC, OrdType::Market, false)?;
        backtester.elapse_bt(80)?;
        assert!(matches!(
            backtester.shared_risk_actions().last(),
            Some(RiskAction::Liquidate {
                reason: RiskReason::Custom(99),
                ..
            })
        ));
        assert_eq!(backtester.orders(0)[&506].status, Status::Canceled);
        assert_eq!(
            backtester
                .shared_exchange_portfolio()
                .venue(VenueId(7))
                .unwrap()
                .account()
                .position(InstrumentId(1))
                .qty,
            0.0
        );
        assert!(
            backtester
                .shared_exchange_reports()
                .iter()
                .any(|(_, report)| {
                    report.order_id == u64::MAX && report.kind == ExecutionReportKind::Fill
                })
        );
        Ok(())
    }
}
