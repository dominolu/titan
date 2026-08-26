use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    mem::size_of,
    path::Path,
};

use thiserror::Error;

use crate::{
    backtest::execution::{
        AccountError, AllowAllRisk, CurrencyId, ExchangePortfolio, ExecutionEventProjector,
        ExecutionFeeModel, ExecutionOrderRequest, ExecutionReason, ExecutionReport, FundingConfig,
        FundingEngine, FundingError, FundingReport, FundingRounding, FundingRoundingMode,
        InstrumentId, InstrumentSpec, InstrumentSpecError, InstrumentType, LegacyOrderSnapshot,
        LocalPreTradeRisk, MatchOutcome, ObservedOutcome, OrderOrigin, PortfolioLedger,
        ProjectedEvent, ProposedFill, RateFeeModel, RiskAction, RiskActionSink, RiskDecision,
        RiskReason, ScheduledFunding, SharedTickExecutionConfig, TickCoordinatorError,
        TickOutcomeCoordinator, VenueId, VenueRisk,
    },
    backtest::{
        result::{AuditKind, AuditRecord, AuditRecorder},
        scheduler::{EventKey, EventPhase, GlobalScheduler},
    },
    market_data::{BAR_COMPLETE, BAR_EMPTY, BAR_PARTIAL},
    runtime::{BarItem, FillEvent, OrderCommand, TimedBarItem},
    types::{OrdType, Side, Status, TimeInForce},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BarBatchMeta {
    pub close_ts: i64,
    pub timeframe_ns: i64,
}

/// Default Bar-runtime commission charged on every fill for both maker and taker executions.
pub const DEFAULT_BAR_FEE_RATE: f64 = 0.001;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BarFeedError {
    #[error("bar timeframe must be positive")]
    InvalidTimeframe,
    #[error("bar interval does not match its timeframe")]
    IntervalMismatch,
    #[error("partial bars cannot be delivered to on_bar")]
    PartialBar,
    #[error("on_bar accepts only bars marked complete")]
    IncompleteBar,
    #[error("invalid OHLCV values")]
    InvalidOhlcv,
    #[error("bar input must be sorted by (close_ts, timeframe_ns, asset_no)")]
    Unsorted,
    #[error("duplicate asset in one bar batch")]
    DuplicateAsset,
    #[error("Bar feed I/O failed")]
    Io,
    #[error("Bar NPY schema is invalid")]
    NpySchema,
}

/// Event-jumping Bar feed. Implementations own ingestion/chunking only; they do not match orders
/// or mutate accounts.
pub trait BarFeed {
    type Error;

    fn next_batch(&mut self) -> Result<Option<BarBatchMeta>, Self::Error>;
    fn peek_open_ts(&mut self) -> Result<Option<i64>, Self::Error>;
    fn batch(&self) -> &[BarItem];
    fn reset(&mut self) -> Result<(), Self::Error>;
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PendingBarOrder {
    pub command: OrderCommand,
    pub local_submit_ts: i64,
    pub eligible_after: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BarMatchOutcome {
    Fill {
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_ts: i64,
        price: f64,
        qty: f64,
    },
    Expired {
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_ts: i64,
    },
}

pub trait BarMatchingModel {
    fn submit(&mut self, order: PendingBarOrder) -> bool;
    fn cancel(&mut self, asset_no: u64, order_id: u64) -> bool;
    fn on_batch(&mut self, meta: BarBatchMeta, bars: &[BarItem]);
    fn outcomes(&self) -> &[BarMatchOutcome];
    fn reset(&mut self);
}

/// Conservative NextOpen matcher: an order submitted after Bar close becomes eligible only at a
/// subsequent executable Bar open. It never synthesizes an intrabar Tick path.
pub struct NextOpenBarMatcher {
    execution_timeframe_ns: i64,
    execution_assets: Vec<bool>,
    orders: Vec<PendingBarOrder>,
    outcomes: Vec<BarMatchOutcome>,
}

/// Same-close matcher for close-of-Bar strategies. Orders submitted from `on_bar` become
/// eligible at that Bar's close and execute at its close price. This intentionally models a
/// close-auction/same-close assumption and must be selected explicitly because it is not
/// conservative for ordinary continuous-market data.
pub struct SignalCloseBarMatcher {
    execution_timeframe_ns: i64,
    execution_assets: Vec<bool>,
    orders: Vec<PendingBarOrder>,
    outcomes: Vec<BarMatchOutcome>,
}

/// Transitional Bar execution adapter. It is deliberately separate from feed and matching; P0-C
/// replaces its position-only accounting with the shared execution coordinator without changing
/// either component.
pub struct BarExecutionState {
    positions: Vec<f64>,
    fills: Vec<FillEvent>,
    coordinators: Vec<TickOutcomeCoordinator<Box<dyn ExecutionFeeModel>>>,
    exchange_accounts: ExchangePortfolio,
    initial_balances: BTreeMap<(VenueId, CurrencyId), f64>,
    local_accounts: PortfolioLedger,
    projector: ExecutionEventProjector,
    reports: Vec<ExecutionReport>,
    projected: Vec<(usize, ProjectedEvent)>,
    report_scratch: Vec<ExecutionReport>,
    response_latency_ns: i64,
    deliveries: GlobalScheduler<BarDelivery>,
    funding_engines: Vec<FundingEngine>,
    funding_configured: Vec<bool>,
    funding_reports: Vec<FundingReport>,
    next_funding_sequence: u64,
    local_risk: Box<dyn LocalPreTradeRisk>,
    venue_risk: BTreeMap<VenueId, Box<dyn VenueRisk>>,
    risk_actions: RiskActionSink,
    audit: AuditRecorder,
    audit_run_id: u64,
    audit_sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum BarDelivery {
    Execution {
        asset_no: usize,
        report: ExecutionReport,
    },
    Funding {
        asset_no: usize,
        report: FundingReport,
    },
}

#[derive(Debug, Error)]
pub enum BarExecutionError {
    #[error(transparent)]
    Coordinator(#[from] TickCoordinatorError),
    #[error(transparent)]
    Account(#[from] AccountError),
    #[error(transparent)]
    Funding(#[from] FundingError),
    #[error(transparent)]
    Instrument(#[from] InstrumentSpecError),
    #[error("invalid Bar execution configuration")]
    InvalidConfiguration,
}

impl BarExecutionState {
    pub fn new(num_assets: usize) -> Self {
        let configs = (0..num_assets)
            .map(|asset_no| {
                let currency = CurrencyId(0);
                SharedTickExecutionConfig::new(
                    InstrumentSpec {
                        instrument_id: InstrumentId(asset_no as u32 + 1),
                        asset_no: asset_no as u32,
                        venue_id: VenueId(0),
                        tick_size: 1e-12,
                        lot_size: 1e-12,
                        min_qty: 1e-12,
                        max_qty: f64::MAX,
                        min_notional: 0.0,
                        contract_size: 1.0,
                        price_currency: currency,
                        settlement_currency: currency,
                        margin_currency: currency,
                        instrument_type: InstrumentType::LinearPerpetual,
                        cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
                        version: 1,
                    },
                    RateFeeModel {
                        maker_rate: DEFAULT_BAR_FEE_RATE,
                        taker_rate: DEFAULT_BAR_FEE_RATE,
                        currency,
                    },
                )
            })
            .collect();
        Self::new_with_configs(num_assets, configs, 0)
            .expect("default Bar execution configuration is valid")
    }

    pub fn new_with_configs(
        num_assets: usize,
        configs: Vec<SharedTickExecutionConfig>,
        response_latency_ns: i64,
    ) -> Result<Self, BarExecutionError> {
        if response_latency_ns < 0 || configs.len() != num_assets {
            return Err(BarExecutionError::InvalidConfiguration);
        }
        let mut coordinators: Vec<Option<_>> = (0..num_assets).map(|_| None).collect();
        for config in configs {
            let asset_no = config.spec.asset_no as usize;
            if asset_no >= num_assets || coordinators[asset_no].is_some() {
                return Err(BarExecutionError::InvalidConfiguration);
            }
            coordinators[asset_no] =
                Some(TickOutcomeCoordinator::new(config.spec, config.fee_model));
        }
        Ok(Self {
            positions: vec![0.0; num_assets],
            fills: Vec::new(),
            coordinators: coordinators
                .into_iter()
                .map(|coordinator| coordinator.unwrap())
                .collect(),
            exchange_accounts: ExchangePortfolio::default(),
            initial_balances: BTreeMap::new(),
            local_accounts: PortfolioLedger::default(),
            projector: ExecutionEventProjector::with_capacity(3),
            reports: Vec::new(),
            projected: Vec::new(),
            report_scratch: Vec::with_capacity(2),
            response_latency_ns,
            deliveries: GlobalScheduler::new(),
            funding_engines: (0..num_assets)
                .map(|_| {
                    FundingEngine::new(FundingRounding {
                        increment: 1e-12,
                        mode: FundingRoundingMode::Nearest,
                    })
                    .unwrap()
                })
                .collect(),
            funding_configured: vec![false; num_assets],
            funding_reports: Vec::with_capacity(8),
            next_funding_sequence: 0,
            local_risk: Box::new(AllowAllRisk),
            venue_risk: BTreeMap::new(),
            risk_actions: RiskActionSink::with_capacity(8),
            audit: AuditRecorder::disabled(),
            audit_run_id: 0,
            audit_sequence: 0,
        })
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
        asset_no: u32,
        kind: AuditKind,
        order_id: u64,
        code: u32,
        value0: f64,
        value1: f64,
    ) {
        self.audit.record(AuditRecord {
            run_id: self.audit_run_id,
            schema_version: 1,
            key: EventKey {
                timestamp,
                phase,
                source_priority: 0,
                venue_no: self
                    .coordinators
                    .get(asset_no as usize)
                    .map_or(0, |coordinator| coordinator.spec().venue_id.0),
                asset_no,
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

    pub fn configure_local_risk<R>(&mut self, risk: R)
    where
        R: LocalPreTradeRisk + 'static,
    {
        self.local_risk = Box::new(risk);
    }

    pub fn configure_venue_risk<R>(&mut self, venue_id: VenueId, risk: R)
    where
        R: VenueRisk + 'static,
    {
        self.venue_risk.insert(venue_id, Box::new(risk));
    }

    pub fn set_exchange_balance(
        &mut self,
        venue_id: VenueId,
        currency: CurrencyId,
        balance: f64,
    ) -> Result<(), BarExecutionError> {
        self.exchange_accounts
            .venue_mut_or_insert(venue_id)
            .account_mut()
            .set_balance(currency, balance)?;
        self.local_accounts
            .venue_mut_or_insert(venue_id)
            .seed_balance(currency, balance)?;
        self.initial_balances.insert((venue_id, currency), balance);
        Ok(())
    }

    pub fn check_local_submit(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
    ) -> Result<RiskDecision, BarExecutionError> {
        self.record_audit(
            local_submit_ts,
            EventPhase::StrategyCallback,
            command.asset_no as u32,
            AuditKind::Command,
            command.order_id,
            command.kind as u32,
            command.price,
            command.qty,
        );
        let request = self.request(command, local_submit_ts)?;
        let spec = self.coordinators[command.asset_no as usize].spec();
        let validation = match request.order_type {
            OrdType::Limit => spec.validate_limit_order(request.price, request.qty),
            OrdType::Market => spec.validate_quantity(request.qty),
            OrdType::Unsupported => {
                Err(crate::backtest::execution::InstrumentSpecError::InvalidPrice)
            }
        };
        let decision = match validation {
            Ok(()) => self.local_risk.check(&request, &self.local_accounts),
            Err(error) => RiskDecision::Reject {
                reason: risk_reason_from_spec(error),
            },
        };
        self.record_audit(
            local_submit_ts,
            EventPhase::StrategyCallback,
            command.asset_no as u32,
            AuditKind::RiskDecision,
            command.order_id,
            u32::from(matches!(decision, RiskDecision::Reject { .. })),
            0.0,
            0.0,
        );
        Ok(decision)
    }

    pub fn reject_local(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
        reason: RiskReason,
    ) -> Result<(), BarExecutionError> {
        let asset_no = command.asset_no as usize;
        let request = self.request(command, local_submit_ts)?;
        self.coordinators[asset_no].reject_local(
            request,
            execution_reason(reason),
            &mut self.report_scratch,
        )?;
        self.collect_reports(asset_no);
        Ok(())
    }

    pub fn reject_duplicate_local(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
    ) -> Result<(), BarExecutionError> {
        let asset_no = command.asset_no as usize;
        let request = self.request(command, local_submit_ts)?;
        self.coordinators[asset_no].reject_duplicate_local(request, &mut self.report_scratch);
        self.collect_reports(asset_no);
        Ok(())
    }

    /// Applies exchange-side risk against the authoritative venue account. A rejection is
    /// reported through normal response latency and returns false so the caller can remove the
    /// order from its matcher.
    pub fn arrive(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_arrival_ts: i64,
    ) -> Result<bool, BarExecutionError> {
        let request = self.request(command, local_submit_ts)?;
        let decision =
            self.venue_risk
                .get_mut(&request.venue_id)
                .map_or(RiskDecision::Allow, |risk| {
                    let account = self.exchange_accounts.venue_mut_or_insert(request.venue_id);
                    risk.check_arrival(&request, account)
                });
        match decision {
            RiskDecision::Allow => {
                self.accept(command, local_submit_ts, exchange_arrival_ts)?;
                Ok(true)
            }
            RiskDecision::Reject { reason } => {
                let asset_no = command.asset_no as usize;
                self.apply_observed(
                    asset_no,
                    ObservedOutcome {
                        order_id: command.order_id,
                        order: Some(snapshot(command, local_submit_ts)),
                        outcome: MatchOutcome::Rejected {
                            exchange_ts: exchange_arrival_ts,
                            reason: execution_reason(reason),
                        },
                    },
                    exchange_arrival_ts + self.response_latency_ns,
                )?;
                Ok(false)
            }
        }
    }

    fn request(
        &self,
        command: OrderCommand,
        local_submit_ts: i64,
    ) -> Result<ExecutionOrderRequest, BarExecutionError> {
        let spec = self
            .coordinators
            .get(command.asset_no as usize)
            .ok_or(BarExecutionError::InvalidConfiguration)?
            .spec();
        let request = ExecutionOrderRequest {
            client_order_id: command.order_id,
            venue_id: spec.venue_id,
            instrument_id: spec.instrument_id,
            price: command.price,
            qty: command.qty,
            side: if command.side == 1 {
                Side::Buy
            } else {
                Side::Sell
            },
            time_in_force: match command.time_in_force {
                1 => TimeInForce::GTX,
                2 => TimeInForce::FOK,
                3 => TimeInForce::IOC,
                _ => TimeInForce::GTC,
            },
            order_type: if command.order_type == 1 {
                OrdType::Market
            } else {
                OrdType::Limit
            },
            reduce_only: command._reserved[0] & 1 != 0,
            origin: match command._reserved[2] {
                1 => OrderOrigin::ExecutionAlgorithm,
                2 => OrderOrigin::Liquidation,
                _ => OrderOrigin::Strategy,
            },
            local_submit_ts,
        };
        Ok(request)
    }

    pub fn apply(&mut self, outcomes: &[BarMatchOutcome]) -> Result<(), BarExecutionError> {
        self.fills.clear();
        for outcome in outcomes {
            let (command, local_submit_ts, exchange_ts, canonical) = match *outcome {
                BarMatchOutcome::Fill {
                    command,
                    local_submit_ts,
                    exchange_ts,
                    price,
                    qty,
                } => (
                    command,
                    local_submit_ts,
                    exchange_ts,
                    MatchOutcome::Fill(ProposedFill {
                        exchange_ts,
                        price,
                        qty,
                        maker: false,
                    }),
                ),
                BarMatchOutcome::Expired {
                    command,
                    local_submit_ts,
                    exchange_ts,
                } => (
                    command,
                    local_submit_ts,
                    exchange_ts,
                    MatchOutcome::Expired { exchange_ts },
                ),
            };
            let asset_no = command.asset_no as usize;
            self.apply_observed(
                asset_no,
                ObservedOutcome {
                    order_id: command.order_id,
                    order: Some(snapshot(command, local_submit_ts)),
                    outcome: canonical,
                },
                exchange_ts + self.response_latency_ns,
            )?;
        }
        Ok(())
    }

    /// Applies exchange arrival independently of matching so even a resting GTC receives an
    /// Accepted report at the correct exchange time.
    pub fn accept(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_arrival_ts: i64,
    ) -> Result<(), BarExecutionError> {
        let asset_no = command.asset_no as usize;
        self.apply_observed(
            asset_no,
            ObservedOutcome {
                order_id: command.order_id,
                order: Some(snapshot(command, local_submit_ts)),
                outcome: MatchOutcome::Accepted {
                    exchange_ts: exchange_arrival_ts,
                },
            },
            exchange_arrival_ts + self.response_latency_ns,
        )?;
        Ok(())
    }

    pub fn cancel(
        &mut self,
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_arrival_ts: i64,
        canceled: bool,
    ) -> Result<(), BarExecutionError> {
        let asset_no = command.asset_no as usize;
        let outcome = if canceled {
            MatchOutcome::Canceled {
                exchange_ts: exchange_arrival_ts,
            }
        } else {
            MatchOutcome::Rejected {
                exchange_ts: exchange_arrival_ts,
                reason: crate::backtest::execution::ExecutionReason::Unknown(1),
            }
        };
        self.apply_observed(
            asset_no,
            ObservedOutcome {
                order_id: command.order_id,
                order: Some(snapshot(command, local_submit_ts)),
                outcome,
            },
            exchange_arrival_ts + self.response_latency_ns,
        )?;
        Ok(())
    }

    fn apply_observed(
        &mut self,
        asset_no: usize,
        observed: ObservedOutcome,
        delivery_ts: i64,
    ) -> Result<(), BarExecutionError> {
        let venue_id = self.coordinators[asset_no].spec().venue_id;
        let order_id = observed.order_id;
        let result = self.coordinators[asset_no].apply(
            observed,
            delivery_ts,
            self.exchange_accounts.venue_mut_or_insert(venue_id),
            &mut self.report_scratch,
        );
        if result.is_err() {
            self.record_audit(
                delivery_ts,
                EventPhase::ExchangeState,
                asset_no as u32,
                AuditKind::Diagnostic,
                order_id,
                1,
                0.0,
                0.0,
            );
        }
        result?;
        self.collect_reports(asset_no);
        Ok(())
    }

    fn collect_reports(&mut self, asset_no: usize) {
        for index in 0..self.report_scratch.len() {
            let report = self.report_scratch[index];
            self.record_audit(
                report.exchange_ts,
                EventPhase::ExchangeState,
                report.asset_no,
                AuditKind::ExecutionReport,
                report.order_id,
                execution_report_code(report.kind),
                report.exec_price,
                report.exec_qty,
            );
            self.record_audit(
                report.exchange_ts,
                EventPhase::ExchangeState,
                report.asset_no,
                AuditKind::OrderTransition,
                report.order_id,
                report.status as u32,
                report.order_qty,
                report.exec_qty,
            );
            if let Some(delta) = report.account_delta {
                if let Some(risk) = self.venue_risk.get_mut(&report.venue_id) {
                    let before = self.risk_actions.as_slice().len();
                    risk.on_account_change(
                        self.exchange_accounts.venue(report.venue_id).unwrap(),
                        &mut self.risk_actions,
                    );
                    for index in before..self.risk_actions.as_slice().len() {
                        let action = self.risk_actions.as_slice()[index];
                        self.record_audit(
                            report.exchange_ts,
                            EventPhase::PostTradeRisk,
                            report.asset_no,
                            match action {
                                RiskAction::Liquidate { .. } => AuditKind::Liquidation,
                                RiskAction::Cancel { .. } => AuditKind::RiskDecision,
                            },
                            report.order_id,
                            0,
                            0.0,
                            0.0,
                        );
                    }
                }
                self.record_audit(
                    report.exchange_ts,
                    EventPhase::ExchangeState,
                    report.asset_no,
                    AuditKind::AccountDelta,
                    report.order_id,
                    0,
                    delta.position_delta,
                    delta.cash_delta - delta.fee + delta.funding,
                );
                self.record_audit(
                    report.exchange_ts,
                    EventPhase::Matching,
                    report.asset_no,
                    AuditKind::Fill,
                    report.order_id,
                    0,
                    report.exec_price,
                    report.exec_qty,
                );
            }
            self.reports.push(report);
            self.deliveries.schedule(
                report.delivery_ts,
                response_delivery_phase(report.exchange_ts, report.delivery_ts),
                0,
                report.venue_id.0,
                report.asset_no,
                BarDelivery::Execution { asset_no, report },
            );
        }
        self.report_scratch.clear();
    }

    pub fn next_delivery_ts(&self) -> Option<i64> {
        self.deliveries.peek_key().map(|key| key.timestamp)
    }

    pub fn next_delivery_key(&self) -> Option<crate::backtest::scheduler::EventKey> {
        self.deliveries.peek_key()
    }

    pub fn deliver_next(&mut self) -> Result<bool, BarExecutionError> {
        if !self
            .deliveries
            .peek()
            .is_some_and(|(_, event)| matches!(event, BarDelivery::Execution { .. }))
        {
            return Ok(false);
        }
        let BarDelivery::Execution { asset_no, report } = self.deliveries.pop().unwrap().payload
        else {
            unreachable!();
        };
        let events = self.projector.project(
            report,
            self.local_accounts.venue_mut_or_insert(report.venue_id),
        )?;
        self.projected
            .extend(events.iter().copied().map(|event| (asset_no, event)));
        let instrument_id = self.coordinators[asset_no].spec().instrument_id;
        self.positions[asset_no] = self
            .local_accounts
            .venue(report.venue_id)
            .unwrap()
            .account()
            .position(instrument_id)
            .qty;
        if report.exec_qty > 0.0 {
            self.fills.push(FillEvent {
                asset_no: asset_no as u64,
                order_id: report.order_id,
                venue_order_id: report.venue_order_id,
                exch_ts: report.exchange_ts,
                local_ts: report.delivery_ts,
                sequence: report.sequence,
                price: report.exec_price,
                qty: report.exec_qty,
                venue_no: report.venue_id.0,
                instrument_id: report.instrument_id.0,
                reason: crate::runtime::execution_reason_code(report.reason),
                side: report.side as i8,
                maker: u8::from(report.maker),
                _reserved: [0; 2],
            });
        }
        Ok(true)
    }

    pub fn settle_funding(&mut self, scheduled: ScheduledFunding) -> Result<(), BarExecutionError> {
        let asset_no = scheduled.asset_no as usize;
        let spec = self
            .coordinators
            .get(asset_no)
            .ok_or(BarExecutionError::InvalidConfiguration)?
            .spec()
            .clone();
        let exchange = self
            .exchange_accounts
            .venue_mut_or_insert(scheduled.event.venue_id);
        let position = exchange.account().position(spec.instrument_id).qty;
        let report = self.funding_engines[asset_no].settle(
            scheduled.event,
            position,
            &spec,
            exchange,
            scheduled.delivery_ts,
            self.next_funding_sequence,
        )?;
        self.next_funding_sequence += 1;
        self.funding_reports.push(report);
        self.record_audit(
            report.event.settlement_ts,
            EventPhase::ExchangeState,
            scheduled.asset_no,
            AuditKind::Funding,
            0,
            0,
            report.event.rate,
            report.amount,
        );
        self.deliveries.schedule(
            report.delivery_ts,
            response_delivery_phase(report.event.settlement_ts, report.delivery_ts),
            0,
            report.event.venue_id.0,
            scheduled.asset_no,
            BarDelivery::Funding { asset_no, report },
        );
        Ok(())
    }

    pub fn configure_funding(
        &mut self,
        asset_no: usize,
        config: FundingConfig,
    ) -> Result<(), BarExecutionError> {
        let engine = self
            .funding_engines
            .get_mut(asset_no)
            .ok_or(BarExecutionError::InvalidConfiguration)?;
        if self.funding_configured[asset_no] {
            if engine.config() != config {
                return Err(BarExecutionError::InvalidConfiguration);
            }
            return Ok(());
        }
        *engine = FundingEngine::new_with_config(config)?;
        self.funding_configured[asset_no] = true;
        Ok(())
    }

    pub fn deliver_next_funding(
        &mut self,
    ) -> Result<Option<(usize, FundingReport)>, BarExecutionError> {
        if !self
            .deliveries
            .peek()
            .is_some_and(|(_, event)| matches!(event, BarDelivery::Funding { .. }))
        {
            return Ok(None);
        }
        let BarDelivery::Funding { asset_no, report } = self.deliveries.pop().unwrap().payload
        else {
            unreachable!();
        };
        self.projector.project_funding(
            report,
            self.local_accounts
                .venue_mut_or_insert(report.event.venue_id),
        )?;
        Ok(Some((asset_no, report)))
    }

    pub fn next_is_funding(&self) -> bool {
        self.deliveries
            .peek()
            .is_some_and(|(_, event)| matches!(event, BarDelivery::Funding { .. }))
    }

    pub fn positions(&self) -> &[f64] {
        &self.positions
    }

    pub fn fills(&self) -> &[FillEvent] {
        &self.fills
    }

    pub fn reports(&self) -> &[ExecutionReport] {
        &self.reports
    }

    pub fn funding_reports(&self) -> &[FundingReport] {
        &self.funding_reports
    }

    pub fn account_snapshots(
        &self,
    ) -> (
        Vec<crate::backtest::result::AccountSnapshot>,
        Vec<crate::backtest::result::AccountSnapshot>,
    ) {
        let mut exchange = Vec::with_capacity(self.coordinators.len());
        let mut local = Vec::with_capacity(self.coordinators.len());
        for (asset_no, coordinator) in self.coordinators.iter().enumerate() {
            let spec = coordinator.spec();
            let exchange_account = self
                .exchange_accounts
                .venue(spec.venue_id)
                .map(|account| account.account());
            let local_account = self
                .local_accounts
                .venue(spec.venue_id)
                .map(|account| account.account());
            let risk = self.venue_risk.get(&spec.venue_id);
            let snapshot = |account: &crate::backtest::execution::VenueAccount| {
                let metrics = risk.map_or_else(Default::default, |risk| {
                    risk.instrument_metrics(account, spec.instrument_id)
                });
                crate::backtest::result::AccountSnapshot {
                    venue_no: spec.venue_id.0,
                    asset_no: asset_no as u32,
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
            exchange.push(exchange_account.map_or_else(
                || crate::backtest::result::AccountSnapshot {
                    venue_no: spec.venue_id.0,
                    asset_no: asset_no as u32,
                    currency: spec.settlement_currency,
                    ..Default::default()
                },
                snapshot,
            ));
            local.push(local_account.map_or_else(
                || crate::backtest::result::AccountSnapshot {
                    venue_no: spec.venue_id.0,
                    asset_no: asset_no as u32,
                    currency: spec.settlement_currency,
                    ..Default::default()
                },
                snapshot,
            ));
        }
        (exchange, local)
    }

    pub fn projected(&self) -> &[(usize, ProjectedEvent)] {
        &self.projected
    }

    pub fn project_timer(
        &mut self,
        event: crate::backtest::scheduler::TimerEvent,
    ) -> crate::backtest::scheduler::TimerEvent {
        self.projector.project_timer(event)[0]
    }

    pub fn take_risk_actions(&mut self) -> Vec<RiskAction> {
        let actions = self.risk_actions.as_slice().to_vec();
        self.risk_actions.clear();
        self.audit.reset();
        self.audit_sequence = 0;
        actions
    }

    pub fn asset_for_instrument(
        &self,
        venue_id: VenueId,
        instrument_id: InstrumentId,
    ) -> Option<usize> {
        self.coordinators.iter().position(|coordinator| {
            coordinator.spec().venue_id == venue_id
                && coordinator.spec().instrument_id == instrument_id
        })
    }

    pub fn exchange_position(&self, asset_no: usize) -> Option<f64> {
        let spec = self.coordinators.get(asset_no)?.spec();
        Some(
            self.exchange_accounts
                .venue(spec.venue_id)
                .map_or(0.0, |account| {
                    account.account().position(spec.instrument_id).qty
                }),
        )
    }

    pub fn set_response_latency(&mut self, response_latency_ns: i64) -> bool {
        if response_latency_ns < 0 {
            return false;
        }
        self.response_latency_ns = response_latency_ns;
        true
    }

    pub fn reset(&mut self) {
        for coordinator in &mut self.coordinators {
            coordinator.reset();
        }
        self.exchange_accounts.reset();
        for (&(venue_id, currency), &balance) in &self.initial_balances {
            self.exchange_accounts
                .venue_mut_or_insert(venue_id)
                .account_mut()
                .set_balance(currency, balance)
                .expect("validated initial balance must remain valid during reset");
        }
        self.local_accounts.reset();
        for (&(venue_id, currency), &balance) in &self.initial_balances {
            self.local_accounts
                .venue_mut_or_insert(venue_id)
                .seed_balance(currency, balance)
                .expect("validated initial balance must remain valid during reset");
        }
        self.projector.reset();
        self.positions.fill(0.0);
        self.fills.clear();
        self.reports.clear();
        self.projected.clear();
        self.report_scratch.clear();
        self.deliveries.reset();
        for engine in &mut self.funding_engines {
            engine.reset();
        }
        self.funding_reports.clear();
        self.next_funding_sequence = 0;
        self.local_risk.reset();
        for risk in self.venue_risk.values_mut() {
            risk.reset_all();
        }
        self.risk_actions.clear();
    }

    pub fn clear_results(&mut self) {
        self.fills.clear();
        self.reports.clear();
        self.projected.clear();
        self.funding_reports.clear();
    }
}

#[inline]
fn response_delivery_phase(exchange_ts: i64, delivery_ts: i64) -> EventPhase {
    if delivery_ts == exchange_ts {
        EventPhase::ZeroLatencyResponse
    } else {
        EventPhase::OldResponseDelivery
    }
}

fn execution_reason(reason: RiskReason) -> ExecutionReason {
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

fn risk_reason_from_spec(error: crate::backtest::execution::InstrumentSpecError) -> RiskReason {
    use crate::backtest::execution::InstrumentSpecError;
    match error {
        InstrumentSpecError::InvalidPrice | InstrumentSpecError::PricePrecision => {
            RiskReason::InvalidPrice
        }
        InstrumentSpecError::InvalidQuantity
        | InstrumentSpecError::QuantityPrecision
        | InstrumentSpecError::QuantityBelowMinimum
        | InstrumentSpecError::QuantityAboveMaximum => RiskReason::InvalidQuantity,
        InstrumentSpecError::NotionalBelowMinimum
        | InstrumentSpecError::InvalidPositiveField { .. }
        | InstrumentSpecError::InvalidQuantityRange => RiskReason::InvalidInstrument,
    }
}

fn execution_report_code(kind: crate::backtest::execution::ExecutionReportKind) -> u32 {
    match kind {
        crate::backtest::execution::ExecutionReportKind::Accepted => 1,
        crate::backtest::execution::ExecutionReportKind::Rejected => 2,
        crate::backtest::execution::ExecutionReportKind::Canceled => 3,
        crate::backtest::execution::ExecutionReportKind::Expired => 4,
        crate::backtest::execution::ExecutionReportKind::Fill => 5,
    }
}

fn snapshot(command: OrderCommand, local_submit_ts: i64) -> LegacyOrderSnapshot {
    LegacyOrderSnapshot {
        order_id: command.order_id,
        price: command.price,
        qty: command.qty,
        leaves_qty: command.qty,
        local_submit_ts,
        side: if command.side == 1 {
            Side::Buy
        } else {
            Side::Sell
        },
        order_type: if command.order_type == 1 {
            OrdType::Market
        } else {
            OrdType::Limit
        },
        time_in_force: match command.time_in_force {
            1 => TimeInForce::GTX,
            2 => TimeInForce::FOK,
            3 => TimeInForce::IOC,
            _ => TimeInForce::GTC,
        },
        request: Status::New,
    }
}

impl NextOpenBarMatcher {
    pub fn new(execution_timeframe_ns: i64, execution_assets: Vec<bool>) -> Self {
        Self {
            execution_timeframe_ns,
            execution_assets,
            orders: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    pub fn supports_asset(&self, asset_no: usize) -> bool {
        self.execution_assets
            .get(asset_no)
            .copied()
            .unwrap_or(false)
    }
}

impl BarMatchingModel for NextOpenBarMatcher {
    fn submit(&mut self, order: PendingBarOrder) -> bool {
        if self.orders.iter().any(|existing| {
            existing.command.asset_no == order.command.asset_no
                && existing.command.order_id == order.command.order_id
        }) {
            return false;
        }
        self.orders.push(order);
        true
    }

    fn cancel(&mut self, asset_no: u64, order_id: u64) -> bool {
        if let Some(index) = self.orders.iter().position(|order| {
            order.command.asset_no == asset_no && order.command.order_id == order_id
        }) {
            self.orders.swap_remove(index);
            true
        } else {
            false
        }
    }

    fn on_batch(&mut self, meta: BarBatchMeta, bars: &[BarItem]) {
        self.outcomes.clear();
        if meta.timeframe_ns != self.execution_timeframe_ns {
            return;
        }
        let mut index = 0;
        while index < self.orders.len() {
            let pending = self.orders[index];
            let command = pending.command;
            let Some(item) = bars.iter().find(|item| item.asset_no == command.asset_no) else {
                index += 1;
                continue;
            };
            if item.bar.flags & BAR_EMPTY != 0 || item.bar.open_ts < pending.eligible_after {
                index += 1;
                continue;
            }
            let open = item.bar.open;
            let executable = command.order_type == 1
                || (command.side == 1 && open <= command.price)
                || (command.side == -1 && open >= command.price);
            if executable {
                self.outcomes.push(BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.open_ts,
                    price: open,
                    qty: command.qty,
                });
                self.orders.remove(index);
            } else if matches!(command.time_in_force, 2 | 3) {
                self.outcomes.push(BarMatchOutcome::Expired {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.open_ts,
                });
                self.orders.remove(index);
            } else {
                index += 1;
            }
        }
    }

    fn outcomes(&self) -> &[BarMatchOutcome] {
        &self.outcomes
    }

    fn reset(&mut self) {
        self.orders.clear();
        self.outcomes.clear();
    }
}

impl SignalCloseBarMatcher {
    pub fn new(execution_timeframe_ns: i64, execution_assets: Vec<bool>) -> Self {
        Self {
            execution_timeframe_ns,
            execution_assets,
            orders: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    pub fn supports_asset(&self, asset_no: usize) -> bool {
        self.execution_assets
            .get(asset_no)
            .copied()
            .unwrap_or(false)
    }
}

impl BarMatchingModel for SignalCloseBarMatcher {
    fn submit(&mut self, order: PendingBarOrder) -> bool {
        if !self.supports_asset(order.command.asset_no as usize)
            || self.orders.iter().any(|existing| {
                existing.command.asset_no == order.command.asset_no
                    && existing.command.order_id == order.command.order_id
            })
        {
            return false;
        }
        self.orders.push(order);
        true
    }

    fn cancel(&mut self, asset_no: u64, order_id: u64) -> bool {
        if let Some(index) = self.orders.iter().position(|order| {
            order.command.asset_no == asset_no && order.command.order_id == order_id
        }) {
            self.orders.remove(index);
            true
        } else {
            false
        }
    }

    fn on_batch(&mut self, meta: BarBatchMeta, bars: &[BarItem]) {
        self.outcomes.clear();
        if meta.timeframe_ns != self.execution_timeframe_ns {
            return;
        }
        let mut index = 0;
        while index < self.orders.len() {
            let pending = self.orders[index];
            let command = pending.command;
            let Some(item) = bars.iter().find(|item| item.asset_no == command.asset_no) else {
                index += 1;
                continue;
            };
            if item.bar.flags & BAR_EMPTY != 0 || item.bar.close_ts < pending.eligible_after {
                index += 1;
                continue;
            }
            let close = item.bar.close;
            let executable = command.order_type == 1
                || (command.side == 1 && close <= command.price)
                || (command.side == -1 && close >= command.price);
            if executable {
                self.outcomes.push(BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.close_ts,
                    price: close,
                    qty: command.qty,
                });
                self.orders.remove(index);
            } else if matches!(command.time_in_force, 2 | 3) {
                self.outcomes.push(BarMatchOutcome::Expired {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.close_ts,
                });
                self.orders.remove(index);
            } else {
                index += 1;
            }
        }
    }

    fn outcomes(&self) -> &[BarMatchOutcome] {
        &self.outcomes
    }

    fn reset(&mut self) {
        self.orders.clear();
        self.outcomes.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OhlcFillAssumption {
    /// A limit fills when the Bar touches its price.
    Touch,
    /// A limit fills only when both the range touches and the close finishes through the price.
    Conservative,
}

/// Intrabar-path-free OHLC matcher with optional volume participation. Orders remain in stable
/// insertion order and every partial fill is emitted independently.
pub struct OhlcBarMatcher {
    execution_timeframe_ns: i64,
    execution_assets: Vec<bool>,
    assumption: OhlcFillAssumption,
    volume_participation: f64,
    orders: Vec<PendingBarOrder>,
    outcomes: Vec<BarMatchOutcome>,
}

pub enum ConfiguredBarMatcher {
    NextOpen(NextOpenBarMatcher),
    SignalClose(SignalCloseBarMatcher),
    Ohlc(OhlcBarMatcher),
}

impl ConfiguredBarMatcher {
    pub fn supports_asset(&self, asset_no: usize) -> bool {
        match self {
            Self::NextOpen(matcher) => matcher.supports_asset(asset_no),
            Self::SignalClose(matcher) => matcher.supports_asset(asset_no),
            Self::Ohlc(matcher) => matcher
                .execution_assets
                .get(asset_no)
                .copied()
                .unwrap_or(false),
        }
    }
}

impl BarMatchingModel for ConfiguredBarMatcher {
    fn submit(&mut self, order: PendingBarOrder) -> bool {
        match self {
            Self::NextOpen(matcher) => matcher.submit(order),
            Self::SignalClose(matcher) => matcher.submit(order),
            Self::Ohlc(matcher) => matcher.submit(order),
        }
    }

    fn cancel(&mut self, asset_no: u64, order_id: u64) -> bool {
        match self {
            Self::NextOpen(matcher) => matcher.cancel(asset_no, order_id),
            Self::SignalClose(matcher) => matcher.cancel(asset_no, order_id),
            Self::Ohlc(matcher) => matcher.cancel(asset_no, order_id),
        }
    }

    fn on_batch(&mut self, meta: BarBatchMeta, bars: &[BarItem]) {
        match self {
            Self::NextOpen(matcher) => matcher.on_batch(meta, bars),
            Self::SignalClose(matcher) => matcher.on_batch(meta, bars),
            Self::Ohlc(matcher) => matcher.on_batch(meta, bars),
        }
    }

    fn outcomes(&self) -> &[BarMatchOutcome] {
        match self {
            Self::NextOpen(matcher) => matcher.outcomes(),
            Self::SignalClose(matcher) => matcher.outcomes(),
            Self::Ohlc(matcher) => matcher.outcomes(),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::NextOpen(matcher) => matcher.reset(),
            Self::SignalClose(matcher) => matcher.reset(),
            Self::Ohlc(matcher) => matcher.reset(),
        }
    }
}

impl OhlcBarMatcher {
    pub fn new(
        execution_timeframe_ns: i64,
        execution_assets: Vec<bool>,
        assumption: OhlcFillAssumption,
        volume_participation: f64,
    ) -> Option<Self> {
        if !(0.0..=1.0).contains(&volume_participation) {
            return None;
        }
        Some(Self {
            execution_timeframe_ns,
            execution_assets,
            assumption,
            volume_participation,
            orders: Vec::new(),
            outcomes: Vec::new(),
        })
    }

    fn touched(&self, command: OrderCommand, bar: &crate::market_data::Bar) -> bool {
        if command.order_type == 1 {
            return true;
        }
        match (command.side, self.assumption) {
            (1, OhlcFillAssumption::Touch) => bar.low <= command.price,
            (-1, OhlcFillAssumption::Touch) => bar.high >= command.price,
            (1, OhlcFillAssumption::Conservative) => {
                bar.low <= command.price && bar.close <= command.price
            }
            (-1, OhlcFillAssumption::Conservative) => {
                bar.high >= command.price && bar.close >= command.price
            }
            _ => false,
        }
    }
}

impl BarMatchingModel for OhlcBarMatcher {
    fn submit(&mut self, order: PendingBarOrder) -> bool {
        if !self
            .execution_assets
            .get(order.command.asset_no as usize)
            .copied()
            .unwrap_or(false)
            || self.orders.iter().any(|existing| {
                existing.command.asset_no == order.command.asset_no
                    && existing.command.order_id == order.command.order_id
            })
        {
            return false;
        }
        self.orders.push(order);
        true
    }

    fn cancel(&mut self, asset_no: u64, order_id: u64) -> bool {
        if let Some(index) = self.orders.iter().position(|order| {
            order.command.asset_no == asset_no && order.command.order_id == order_id
        }) {
            self.orders.remove(index);
            true
        } else {
            false
        }
    }

    fn on_batch(&mut self, meta: BarBatchMeta, bars: &[BarItem]) {
        self.outcomes.clear();
        if meta.timeframe_ns != self.execution_timeframe_ns {
            return;
        }
        let mut remaining_volume: Vec<(u64, f64)> = bars
            .iter()
            .map(|item| (item.asset_no, item.bar.volume * self.volume_participation))
            .collect();
        let mut index = 0;
        while index < self.orders.len() {
            let pending = self.orders[index];
            let command = pending.command;
            let Some(item) = bars.iter().find(|item| item.asset_no == command.asset_no) else {
                index += 1;
                continue;
            };
            if item.bar.flags & BAR_EMPTY != 0 || item.bar.open_ts < pending.eligible_after {
                index += 1;
                continue;
            }
            let available = remaining_volume
                .iter_mut()
                .find(|(asset_no, _)| *asset_no == command.asset_no)
                .unwrap();
            if command.time_in_force == 2
                && self.touched(command, &item.bar)
                && available.1 < command.qty
            {
                self.outcomes.push(BarMatchOutcome::Expired {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.close_ts,
                });
                self.orders.remove(index);
            } else if self.touched(command, &item.bar) && available.1 > 0.0 {
                let qty = command.qty.min(available.1);
                available.1 -= qty;
                let price = if command.order_type == 1 {
                    item.bar.open
                } else {
                    command.price
                };
                self.outcomes.push(BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.close_ts,
                    price,
                    qty,
                });
                if qty >= command.qty {
                    self.orders.remove(index);
                } else if matches!(command.time_in_force, 2 | 3) {
                    self.outcomes.push(BarMatchOutcome::Expired {
                        command,
                        local_submit_ts: pending.local_submit_ts,
                        exchange_ts: item.bar.close_ts,
                    });
                    self.orders.remove(index);
                } else {
                    self.orders[index].command.qty -= qty;
                    index += 1;
                }
            } else if matches!(command.time_in_force, 2 | 3) {
                self.outcomes.push(BarMatchOutcome::Expired {
                    command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: item.bar.close_ts,
                });
                self.orders.remove(index);
            } else {
                index += 1;
            }
        }
    }

    fn outcomes(&self) -> &[BarMatchOutcome] {
        &self.outcomes
    }

    fn reset(&mut self) {
        self.orders.clear();
        self.outcomes.clear();
    }
}

/// In-memory feed for already materialized closed Bars.
pub struct MaterializedBarFeed {
    records: Vec<TimedBarItem>,
    cursor: usize,
    batch: Vec<BarItem>,
}

/// Supplies one owned chunk at a time. File/mmap/Parquet providers can implement this without
/// retaining the complete dataset in Rust memory.
pub trait BarChunkProvider {
    fn next_chunk(&mut self) -> Result<Option<Vec<TimedBarItem>>, BarFeedError>;
    fn reset(&mut self) -> Result<(), BarFeedError>;
}

/// Flat canonical Bar row used by chunked NPY storage.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct FlatTimedBarRow {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub open_ts: i64,
    pub close_ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub buy_volume: f64,
    pub trade_count: u64,
    pub flags: u64,
}

unsafe impl crate::backtest::data::POD for FlatTimedBarRow {}

impl crate::backtest::data::NpyDTyped for FlatTimedBarRow {
    fn descr() -> Vec<crate::backtest::data::Field> {
        let endian = if cfg!(target_endian = "little") {
            "<"
        } else {
            ">"
        };
        [
            ("asset_no", "u8"),
            ("timeframe_ns", "i8"),
            ("open_ts", "i8"),
            ("close_ts", "i8"),
            ("open", "f8"),
            ("high", "f8"),
            ("low", "f8"),
            ("close", "f8"),
            ("volume", "f8"),
            ("quote_volume", "f8"),
            ("buy_volume", "f8"),
            ("trade_count", "u8"),
            ("flags", "u8"),
        ]
        .into_iter()
        .map(|(name, ty)| crate::backtest::data::Field {
            name: name.into(),
            ty: format!("{endian}{ty}"),
        })
        .collect()
    }
}

impl From<FlatTimedBarRow> for TimedBarItem {
    fn from(row: FlatTimedBarRow) -> Self {
        Self {
            asset_no: row.asset_no,
            timeframe_ns: row.timeframe_ns,
            bar: crate::market_data::Bar {
                open_ts: row.open_ts,
                close_ts: row.close_ts,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                quote_volume: row.quote_volume,
                buy_volume: row.buy_volume,
                trade_count: row.trade_count,
                flags: row.flags,
            },
        }
    }
}

pub struct NpyBarChunkProvider {
    file: File,
    data_offset: u64,
    remaining: usize,
    total_rows: usize,
    chunk_rows: usize,
}

impl NpyBarChunkProvider {
    pub fn open(path: impl AsRef<Path>, chunk_rows: usize) -> Result<Self, BarFeedError> {
        if chunk_rows == 0 {
            return Err(BarFeedError::NpySchema);
        }
        let mut file = File::open(path).map_err(|_| BarFeedError::Io)?;
        let mut prefix = [0_u8; 10];
        file.read_exact(&mut prefix).map_err(|_| BarFeedError::Io)?;
        if &prefix[..6] != b"\x93NUMPY" || prefix[6..8] != [1, 0] {
            return Err(BarFeedError::NpySchema);
        }
        let header_len = u16::from_le_bytes([prefix[8], prefix[9]]) as usize;
        let mut header_bytes = vec![0_u8; header_len];
        file.read_exact(&mut header_bytes)
            .map_err(|_| BarFeedError::Io)?;
        let header = crate::backtest::data::NpyHeader::from_header(
            std::str::from_utf8(&header_bytes).map_err(|_| BarFeedError::NpySchema)?,
        )
        .map_err(|_| BarFeedError::NpySchema)?;
        if header.fortran_order
            || header.shape.len() != 1
            || header.descr != <FlatTimedBarRow as crate::backtest::data::NpyDTyped>::descr()
        {
            return Err(BarFeedError::NpySchema);
        }
        let total_rows = header.shape[0];
        let data_offset = (10 + header_len) as u64;
        let expected_len = data_offset + (total_rows * size_of::<FlatTimedBarRow>()) as u64;
        if file.metadata().map_err(|_| BarFeedError::Io)?.len() != expected_len {
            return Err(BarFeedError::NpySchema);
        }
        Ok(Self {
            file,
            data_offset,
            remaining: total_rows,
            total_rows,
            chunk_rows,
        })
    }
}

impl BarChunkProvider for NpyBarChunkProvider {
    fn next_chunk(&mut self) -> Result<Option<Vec<TimedBarItem>>, BarFeedError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let len = self.remaining.min(self.chunk_rows);
        let mut rows = vec![FlatTimedBarRow::default(); len];
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                rows.as_mut_ptr().cast::<u8>(),
                len * size_of::<FlatTimedBarRow>(),
            )
        };
        self.file.read_exact(bytes).map_err(|_| BarFeedError::Io)?;
        self.remaining -= len;
        Ok(Some(rows.into_iter().map(Into::into).collect()))
    }

    fn reset(&mut self) -> Result<(), BarFeedError> {
        self.file
            .seek(SeekFrom::Start(self.data_offset))
            .map_err(|_| BarFeedError::Io)?;
        self.remaining = self.total_rows;
        Ok(())
    }
}

pub struct ChunkedBarFeed<P> {
    provider: P,
    current: std::vec::IntoIter<TimedBarItem>,
    lookahead: Option<TimedBarItem>,
    last_loaded_key: Option<(i64, i64, u64)>,
    batch: Vec<BarItem>,
}

impl<P> ChunkedBarFeed<P>
where
    P: BarChunkProvider,
{
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            current: Vec::new().into_iter(),
            lookahead: None,
            last_loaded_key: None,
            batch: Vec::new(),
        }
    }

    fn pull_record(&mut self) -> Result<Option<TimedBarItem>, BarFeedError> {
        loop {
            if let Some(record) = self.current.next() {
                validate_records(std::slice::from_ref(&record))?;
                let key = (record.bar.close_ts, record.timeframe_ns, record.asset_no);
                if self.last_loaded_key.is_some_and(|previous| key < previous) {
                    return Err(BarFeedError::Unsorted);
                }
                self.last_loaded_key = Some(key);
                return Ok(Some(record));
            }
            let Some(chunk) = self.provider.next_chunk()? else {
                return Ok(None);
            };
            self.current = chunk.into_iter();
        }
    }

    fn ensure_lookahead(&mut self) -> Result<(), BarFeedError> {
        if self.lookahead.is_none() {
            self.lookahead = self.pull_record()?;
        }
        Ok(())
    }
}

impl<P> BarFeed for ChunkedBarFeed<P>
where
    P: BarChunkProvider,
{
    type Error = BarFeedError;

    fn next_batch(&mut self) -> Result<Option<BarBatchMeta>, Self::Error> {
        self.ensure_lookahead()?;
        let Some(first) = self.lookahead.take() else {
            self.batch.clear();
            return Ok(None);
        };
        let meta = BarBatchMeta {
            close_ts: first.bar.close_ts,
            timeframe_ns: first.timeframe_ns,
        };
        self.batch.clear();
        self.batch.push(BarItem {
            asset_no: first.asset_no,
            bar: first.bar,
        });
        while let Some(record) = self.pull_record()? {
            if record.bar.close_ts != meta.close_ts || record.timeframe_ns != meta.timeframe_ns {
                self.lookahead = Some(record);
                break;
            }
            if self
                .batch
                .last()
                .is_some_and(|item| item.asset_no == record.asset_no)
            {
                return Err(BarFeedError::DuplicateAsset);
            }
            self.batch.push(BarItem {
                asset_no: record.asset_no,
                bar: record.bar,
            });
        }
        Ok(Some(meta))
    }

    fn peek_open_ts(&mut self) -> Result<Option<i64>, Self::Error> {
        self.ensure_lookahead()?;
        Ok(self.lookahead.map(|record| record.bar.open_ts))
    }

    fn batch(&self) -> &[BarItem] {
        &self.batch
    }

    fn reset(&mut self) -> Result<(), Self::Error> {
        self.provider.reset()?;
        self.current = Vec::new().into_iter();
        self.lookahead = None;
        self.last_loaded_key = None;
        self.batch.clear();
        Ok(())
    }
}

#[derive(Clone)]
pub struct VecBarChunkProvider {
    chunks: Vec<Vec<TimedBarItem>>,
    cursor: usize,
}

impl VecBarChunkProvider {
    pub fn new(chunks: Vec<Vec<TimedBarItem>>) -> Self {
        Self { chunks, cursor: 0 }
    }
}

impl BarChunkProvider for VecBarChunkProvider {
    fn next_chunk(&mut self) -> Result<Option<Vec<TimedBarItem>>, BarFeedError> {
        let chunk = self.chunks.get(self.cursor).cloned();
        self.cursor += usize::from(chunk.is_some());
        Ok(chunk)
    }

    fn reset(&mut self) -> Result<(), BarFeedError> {
        self.cursor = 0;
        Ok(())
    }
}

impl MaterializedBarFeed {
    pub fn new(records: &[TimedBarItem]) -> Result<Self, BarFeedError> {
        validate_records(records)?;
        Ok(Self {
            records: records.to_vec(),
            cursor: 0,
            batch: Vec::new(),
        })
    }

    pub fn records(&self) -> &[TimedBarItem] {
        &self.records
    }
}

impl BarFeed for MaterializedBarFeed {
    type Error = BarFeedError;

    fn next_batch(&mut self) -> Result<Option<BarBatchMeta>, Self::Error> {
        let Some(first) = self.records.get(self.cursor).copied() else {
            self.batch.clear();
            return Ok(None);
        };
        let meta = BarBatchMeta {
            close_ts: first.bar.close_ts,
            timeframe_ns: first.timeframe_ns,
        };
        self.batch.clear();
        while let Some(record) = self.records.get(self.cursor).copied() {
            if record.bar.close_ts != meta.close_ts || record.timeframe_ns != meta.timeframe_ns {
                break;
            }
            if self
                .batch
                .last()
                .is_some_and(|item| item.asset_no == record.asset_no)
            {
                return Err(BarFeedError::DuplicateAsset);
            }
            self.batch.push(BarItem {
                asset_no: record.asset_no,
                bar: record.bar,
            });
            self.cursor += 1;
        }
        Ok(Some(meta))
    }

    fn batch(&self) -> &[BarItem] {
        &self.batch
    }

    fn peek_open_ts(&mut self) -> Result<Option<i64>, Self::Error> {
        Ok(self
            .records
            .get(self.cursor)
            .map(|record| record.bar.open_ts))
    }

    fn reset(&mut self) -> Result<(), Self::Error> {
        self.cursor = 0;
        self.batch.clear();
        Ok(())
    }
}

fn validate_records(records: &[TimedBarItem]) -> Result<(), BarFeedError> {
    for (index, record) in records.iter().enumerate() {
        if record.timeframe_ns <= 0 {
            return Err(BarFeedError::InvalidTimeframe);
        }
        if record.bar.close_ts - record.bar.open_ts != record.timeframe_ns {
            return Err(BarFeedError::IntervalMismatch);
        }
        if record.bar.flags & BAR_PARTIAL != 0 {
            return Err(BarFeedError::PartialBar);
        }
        if record.bar.flags & BAR_COMPLETE == 0 {
            return Err(BarFeedError::IncompleteBar);
        }
        let prices_valid = record.bar.open.is_finite()
            && record.bar.high.is_finite()
            && record.bar.low.is_finite()
            && record.bar.close.is_finite()
            && record.bar.high >= record.bar.open.max(record.bar.close)
            && record.bar.low <= record.bar.open.min(record.bar.close)
            && record.bar.low <= record.bar.high;
        let nan_empty = record.bar.flags & BAR_EMPTY != 0
            && record.bar.open.is_nan()
            && record.bar.high.is_nan()
            && record.bar.low.is_nan()
            && record.bar.close.is_nan();
        if (!prices_valid && !nan_empty)
            || !record.bar.volume.is_finite()
            || record.bar.volume < 0.0
        {
            return Err(BarFeedError::InvalidOhlcv);
        }
        if index > 0 {
            let previous = records[index - 1];
            if (record.bar.close_ts, record.timeframe_ns, record.asset_no)
                < (
                    previous.bar.close_ts,
                    previous.timeframe_ns,
                    previous.asset_no,
                )
            {
                return Err(BarFeedError::Unsorted);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::{BAR_COMPLETE, Bar};

    fn record(asset_no: u64, close_ts: i64) -> TimedBarItem {
        TimedBarItem {
            asset_no,
            timeframe_ns: 10,
            bar: Bar {
                open_ts: close_ts - 10,
                close_ts,
                open: 1.0,
                high: 2.0,
                low: 0.5,
                close: 1.5,
                volume: 1.0,
                quote_volume: 0.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE,
            },
        }
    }

    #[test]
    fn groups_global_batches_and_resets_without_reallocation_contract_changes() {
        let mut feed =
            MaterializedBarFeed::new(&[record(0, 10), record(1, 10), record(0, 20)]).unwrap();
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 10);
        assert_eq!(feed.batch().len(), 2);
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 20);
        assert_eq!(feed.batch().len(), 1);
        assert!(feed.next_batch().unwrap().is_none());
        feed.reset().unwrap();
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 10);
    }

    #[test]
    fn signal_close_fills_at_the_producing_bar_close() {
        let mut feed = MaterializedBarFeed::new(&[record(0, 10)]).unwrap();
        let meta = feed.next_batch().unwrap().unwrap();
        let command = OrderCommand {
            kind: 1,
            side: 1,
            order_type: 1,
            asset_no: 0,
            order_id: 7,
            qty: 2.0,
            ..OrderCommand::default()
        };
        let mut matcher = SignalCloseBarMatcher::new(10, vec![true]);
        assert!(matcher.submit(PendingBarOrder {
            command,
            local_submit_ts: 10,
            eligible_after: 10,
        }));

        matcher.on_batch(meta, feed.batch());

        assert_eq!(
            matcher.outcomes(),
            &[BarMatchOutcome::Fill {
                command,
                local_submit_ts: 10,
                exchange_ts: 10,
                price: 1.5,
                qty: 2.0,
            }]
        );
    }

    #[test]
    fn chunked_feed_crosses_chunk_boundary_end_to_end() {
        let provider = VecBarChunkProvider::new(vec![
            vec![record(0, 10)],
            vec![record(1, 10), record(0, 20)],
        ]);
        let mut feed = ChunkedBarFeed::new(provider);
        let meta = feed.next_batch().unwrap().unwrap();
        assert_eq!(feed.batch().len(), 2);
        let command = OrderCommand {
            kind: 1,
            side: 1,
            time_in_force: 3,
            order_type: 1,
            asset_no: 0,
            order_id: 50,
            qty: 1.0,
            ..OrderCommand::default()
        };
        let mut matcher = NextOpenBarMatcher::new(10, vec![true, true]);
        matcher.submit(PendingBarOrder {
            command,
            local_submit_ts: 0,
            eligible_after: 0,
        });
        matcher.on_batch(meta, feed.batch());
        let mut execution = BarExecutionState::new(2);
        execution.apply(matcher.outcomes()).unwrap();
        while execution.deliver_next().unwrap() {}
        assert_eq!(execution.positions()[0], 1.0);
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 20);
        assert_eq!(feed.batch().len(), 1);
    }

    #[test]
    fn npy_chunk_provider_streams_and_rewinds_without_materializing_whole_file() {
        let path = std::env::temp_dir().join(format!(
            "titan-bar-chunk-{}-{}.npy",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let rows: Vec<FlatTimedBarRow> = [record(0, 10), record(0, 20), record(0, 30)]
            .into_iter()
            .map(|item| FlatTimedBarRow {
                asset_no: item.asset_no,
                timeframe_ns: item.timeframe_ns,
                open_ts: item.bar.open_ts,
                close_ts: item.bar.close_ts,
                open: item.bar.open,
                high: item.bar.high,
                low: item.bar.low,
                close: item.bar.close,
                volume: item.bar.volume,
                quote_volume: item.bar.quote_volume,
                buy_volume: item.bar.buy_volume,
                trade_count: item.bar.trade_count,
                flags: item.bar.flags,
            })
            .collect();
        let mut file = std::fs::File::create(&path).unwrap();
        crate::backtest::data::write_npy(&mut file, &rows).unwrap();
        drop(file);

        let provider = NpyBarChunkProvider::open(&path, 2).unwrap();
        let mut feed = ChunkedBarFeed::new(provider);
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 10);
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 20);
        feed.reset().unwrap();
        assert_eq!(feed.next_batch().unwrap().unwrap().close_ts, 10);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn next_open_uses_shared_order_account_and_projector_chain() {
        let command = OrderCommand {
            kind: 1,
            side: 1,
            time_in_force: 3,
            order_type: 1,
            asset_no: 0,
            order_id: 9,
            price: 0.0,
            qty: 2.0,
            ..OrderCommand::default()
        };
        let bar = record(0, 20);
        let bars = [BarItem {
            asset_no: 0,
            bar: bar.bar,
        }];
        let mut matcher = NextOpenBarMatcher::new(10, vec![true]);
        assert!(matcher.submit(PendingBarOrder {
            command,
            local_submit_ts: 10,
            eligible_after: 10,
        }));
        matcher.on_batch(
            BarBatchMeta {
                close_ts: 20,
                timeframe_ns: 10,
            },
            &bars,
        );
        let mut execution = BarExecutionState::new(1);
        execution.enable_audit(44, 32);
        execution.apply(matcher.outcomes()).unwrap();
        while execution.deliver_next().unwrap() {}
        assert_eq!(execution.reports().len(), 2);
        assert_eq!(execution.fills()[0].price, 1.0);
        assert_eq!(execution.positions()[0], 2.0);
        assert_eq!(execution.projected().len(), 4);
        let kinds: Vec<_> = execution
            .audit()
            .records()
            .iter()
            .map(|record| record.kind)
            .collect();
        assert!(kinds.contains(&AuditKind::ExecutionReport));
        assert!(kinds.contains(&AuditKind::OrderTransition));
        assert!(kinds.contains(&AuditKind::Fill));
        assert!(kinds.contains(&AuditKind::AccountDelta));
    }

    #[test]
    fn default_bar_execution_charges_one_per_mille_on_every_fill() {
        let command = OrderCommand {
            kind: 1,
            side: 1,
            time_in_force: 3,
            order_type: 1,
            asset_no: 0,
            order_id: 10,
            qty: 2.0,
            ..OrderCommand::default()
        };
        let mut execution = BarExecutionState::new(1);
        execution
            .apply(&[BarMatchOutcome::Fill {
                command,
                local_submit_ts: 5,
                exchange_ts: 10,
                price: 100.0,
                qty: 2.0,
            }])
            .unwrap();
        let fill = execution.reports().last().unwrap();
        assert_eq!(fill.account_delta.unwrap().fee, 0.2);
    }

    #[test]
    fn bar_execution_applies_configured_fee_and_response_latency() {
        use crate::backtest::execution::RateFeeModel;

        let currency = CurrencyId(7);
        let spec = InstrumentSpec {
            instrument_id: InstrumentId(42),
            asset_no: 0,
            venue_id: VenueId(3),
            tick_size: 0.01,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: currency,
            settlement_currency: currency,
            margin_currency: currency,
            instrument_type: InstrumentType::Spot,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        };
        let command = OrderCommand {
            kind: 1,
            side: 1,
            time_in_force: 3,
            order_type: 1,
            asset_no: 0,
            order_id: 11,
            qty: 2.0,
            ..OrderCommand::default()
        };
        let mut execution = BarExecutionState::new_with_configs(
            1,
            vec![SharedTickExecutionConfig::new(
                spec,
                RateFeeModel {
                    maker_rate: 0.0,
                    taker_rate: 0.001,
                    currency,
                },
            )],
            5,
        )
        .unwrap();
        execution
            .apply(&[BarMatchOutcome::Fill {
                command,
                local_submit_ts: 5,
                exchange_ts: 10,
                price: 100.0,
                qty: 2.0,
            }])
            .unwrap();
        let fill = execution.reports().last().unwrap();
        assert_eq!(fill.exchange_ts, 10);
        assert_eq!(fill.delivery_ts, 15);
        assert_eq!(fill.account_delta.unwrap().fee, 0.2);
    }

    #[test]
    fn bar_execution_preserves_independent_partial_fills() {
        let command = OrderCommand {
            kind: 1,
            side: 1,
            time_in_force: 0,
            order_type: 0,
            asset_no: 0,
            order_id: 12,
            price: 101.0,
            qty: 2.0,
            ..OrderCommand::default()
        };
        let mut execution = BarExecutionState::new(1);
        execution
            .apply(&[
                BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: 5,
                    exchange_ts: 10,
                    price: 100.0,
                    qty: 1.0,
                },
                BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: 5,
                    exchange_ts: 10,
                    price: 101.0,
                    qty: 1.0,
                },
            ])
            .unwrap();
        let fills: Vec<_> = execution
            .reports()
            .iter()
            .filter(|report| report.exec_qty > 0.0)
            .collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].exec_qty, 1.0);
        assert_eq!(fills[1].exec_qty, 1.0);
    }

    #[test]
    fn response_latency_can_cross_later_bar_close_without_time_reversal() {
        use crate::runtime::{
            MaterializedBarSource, ORDER_COMMAND_SUBMIT, RuntimeEventSource, StrategyEventKind,
            StrategyRuntimeContext,
        };

        let records = [record(0, 10), record(0, 20)];
        let mut source = MaterializedBarSource::new(&records, 4).unwrap();
        let currency = CurrencyId(0);
        source
            .configure_execution(
                vec![SharedTickExecutionConfig::new(
                    InstrumentSpec {
                        instrument_id: InstrumentId(1),
                        asset_no: 0,
                        venue_id: VenueId(0),
                        tick_size: 1.0,
                        lot_size: 1.0,
                        min_qty: 1.0,
                        max_qty: 10.0,
                        min_notional: 0.0,
                        contract_size: 1.0,
                        price_currency: currency,
                        settlement_currency: currency,
                        margin_currency: currency,
                        instrument_type: InstrumentType::Spot,
                        cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
                        version: 1,
                    },
                    crate::backtest::execution::NoFee { currency },
                )],
                15,
            )
            .unwrap();
        let mut ctx = StrategyRuntimeContext::default();
        source.configure_context(&mut ctx);

        let (first_kind, first_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!((first_kind, first_now), (StrategyEventKind::Bar as u32, 10));
        unsafe {
            *ctx.commands_ptr = OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 1,
                time_in_force: 3,
                order_type: 1,
                asset_no: 0,
                order_id: 99,
                qty: 1.0,
                ..OrderCommand::default()
            };
        }
        ctx.num_commands = 1;
        ctx.now = 10;
        source.after_callback(first_kind, &mut ctx).unwrap();

        let (second_kind, second_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!(
            (second_kind, second_now),
            (StrategyEventKind::Bar as u32, 20)
        );
        source.after_callback(second_kind, &mut ctx).unwrap();
        let response = source.next_event().unwrap().unwrap();
        assert_eq!(response.kind, StrategyEventKind::Order as u32);
        assert_eq!(response.now, 25);
    }

    #[test]
    fn entry_latency_accepts_at_arrival_and_skips_missed_open() {
        use crate::runtime::{
            MaterializedBarSource, ORDER_COMMAND_SUBMIT, RuntimeEventSource, StrategyEventKind,
            StrategyRuntimeContext,
        };

        let records = [record(0, 10), record(0, 20), record(0, 30)];
        let mut source = MaterializedBarSource::new(&records, 4).unwrap();
        source.configure_transport(5, 0).unwrap();
        let mut ctx = StrategyRuntimeContext::default();
        source.configure_context(&mut ctx);
        let (first_kind, first_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!((first_kind, first_now), (StrategyEventKind::Bar as u32, 10));
        unsafe {
            *ctx.commands_ptr = OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 1,
                time_in_force: 3,
                order_type: 1,
                asset_no: 0,
                order_id: 101,
                qty: 1.0,
                ..OrderCommand::default()
            };
        }
        ctx.num_commands = 1;
        ctx.now = 10;
        source.after_callback(first_kind, &mut ctx).unwrap();

        let (second_kind, second_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!(
            (second_kind, second_now),
            (StrategyEventKind::Order as u32, 15)
        );
        source.after_callback(second_kind, &mut ctx).unwrap();
        let (second_kind, second_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!(
            (second_kind, second_now),
            (StrategyEventKind::Bar as u32, 20)
        );
        source.after_callback(second_kind, &mut ctx).unwrap();

        let (fill_kind, fill_now) = {
            let event = source.next_event().unwrap().unwrap();
            (event.kind, event.now)
        };
        assert_eq!((fill_kind, fill_now), (StrategyEventKind::Order as u32, 20));
        let reports = source.execution_reports();
        assert_eq!(reports[0].exchange_ts, 15);
        assert_eq!(reports.last().unwrap().exchange_ts, 20);
    }

    #[test]
    fn ohlc_volume_model_emits_independent_partial_fills_and_fok_is_atomic() {
        let mut matcher =
            OhlcBarMatcher::new(10, vec![true], OhlcFillAssumption::Touch, 0.5).unwrap();
        let mut first = OrderCommand {
            side: 1,
            order_type: 0,
            time_in_force: 0,
            asset_no: 0,
            order_id: 1,
            price: 1.5,
            qty: 0.4,
            ..Default::default()
        };
        matcher.submit(PendingBarOrder {
            command: first,
            local_submit_ts: 0,
            eligible_after: 0,
        });
        first.order_id = 2;
        first.qty = 0.4;
        matcher.submit(PendingBarOrder {
            command: first,
            local_submit_ts: 0,
            eligible_after: 0,
        });
        first.order_id = 3;
        first.qty = 1.0;
        first.time_in_force = 2;
        matcher.submit(PendingBarOrder {
            command: first,
            local_submit_ts: 0,
            eligible_after: 0,
        });
        let item = record(0, 10);
        matcher.on_batch(
            BarBatchMeta {
                close_ts: 10,
                timeframe_ns: 10,
            },
            &[BarItem {
                asset_no: 0,
                bar: item.bar,
            }],
        );
        assert_eq!(matcher.outcomes().len(), 3);
        let BarMatchOutcome::Fill { qty: first, .. } = matcher.outcomes()[0] else {
            panic!("first order must fill");
        };
        let BarMatchOutcome::Fill { qty: second, .. } = matcher.outcomes()[1] else {
            panic!("second order must partially fill");
        };
        let BarMatchOutcome::Expired { command, .. } = matcher.outcomes()[2] else {
            panic!("FOK must expire atomically");
        };
        assert!((first - 0.4).abs() < 1e-12);
        assert!((second - 0.1).abs() < 1e-12);
        assert_eq!(command.order_id, 3);
    }

    #[derive(Default)]
    struct DenyLocal;

    impl LocalPreTradeRisk for DenyLocal {
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

    #[derive(Default)]
    struct DenyVenue;

    impl crate::backtest::execution::ExchangeRisk for DenyVenue {
        fn check_arrival(
            &mut self,
            _request: &ExecutionOrderRequest,
            _account: &crate::backtest::execution::ExchangeAccountState,
        ) -> RiskDecision {
            RiskDecision::Reject {
                reason: RiskReason::InsufficientMargin,
            }
        }
    }

    impl crate::backtest::execution::PostTradeRisk for DenyVenue {
        fn on_account_change(
            &mut self,
            _account: &crate::backtest::execution::ExchangeAccountState,
            _out: &mut RiskActionSink,
        ) {
        }
    }

    fn risk_command(order_id: u64) -> OrderCommand {
        OrderCommand {
            kind: 1,
            side: 1,
            order_type: 0,
            asset_no: 0,
            order_id,
            price: 1.0,
            qty: 1.0,
            ..Default::default()
        }
    }

    #[test]
    fn bar_submit_runs_local_then_exchange_risk_with_distinct_delivery_semantics() {
        let mut state = BarExecutionState::new(1);
        state.configure_local_risk(DenyLocal);
        let command = risk_command(71);
        let RiskDecision::Reject { reason } = state.check_local_submit(command, 10).unwrap() else {
            panic!("local risk must reject");
        };
        state.reject_local(command, 10, reason).unwrap();
        assert_eq!(state.next_delivery_ts(), Some(10));
        state.deliver_next().unwrap();
        assert_eq!(state.reports()[0].reason, ExecutionReason::PositionLimit);

        let mut state = BarExecutionState::new(1);
        state.configure_venue_risk(VenueId(0), DenyVenue);
        assert!(!state.arrive(risk_command(72), 10, 15).unwrap());
        assert_eq!(state.next_delivery_ts(), Some(15));
        state.deliver_next().unwrap();
        assert_eq!(
            state.reports()[0].reason,
            ExecutionReason::InsufficientMargin
        );
    }

    #[test]
    fn invalid_and_duplicate_bar_orders_are_canonical_local_rejections() {
        let currency = CurrencyId(1);
        let spec = InstrumentSpec {
            instrument_id: InstrumentId(1),
            asset_no: 0,
            venue_id: VenueId(2),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 10.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: currency,
            settlement_currency: currency,
            margin_currency: currency,
            instrument_type: InstrumentType::Spot,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        };
        let mut state = BarExecutionState::new_with_configs(
            1,
            vec![SharedTickExecutionConfig::new(
                spec,
                crate::backtest::execution::NoFee { currency },
            )],
            0,
        )
        .unwrap();
        let mut invalid = risk_command(80);
        invalid.price = 1.5;
        let RiskDecision::Reject { reason } = state.check_local_submit(invalid, 10).unwrap() else {
            panic!("invalid precision must reject locally");
        };
        assert_eq!(reason, RiskReason::InvalidPrice);
        state.reject_local(invalid, 10, reason).unwrap();
        assert_eq!(
            state.reports().last().unwrap().reason,
            ExecutionReason::InvalidPrice
        );

        let valid = risk_command(81);
        state.accept(valid, 20, 20).unwrap();
        let original_state = state.coordinators[0].coordinator().order(81).unwrap().state;
        state.reject_duplicate_local(valid, 21).unwrap();
        assert_eq!(
            state.reports().last().unwrap().reason,
            ExecutionReason::DuplicateOrderId
        );
        assert_eq!(
            state.coordinators[0].coordinator().order(81).unwrap().state,
            original_state
        );
    }

    #[test]
    fn invalid_order_transition_stops_and_enters_diagnostic_audit() {
        let mut state = BarExecutionState::new(1);
        state.enable_audit(9, 32);
        let command = risk_command(90);
        state.accept(command, 1, 1).unwrap();
        let fill = BarMatchOutcome::Fill {
            command,
            local_submit_ts: 1,
            exchange_ts: 2,
            price: 1.0,
            qty: 1.0,
        };
        state.apply(&[fill]).unwrap();
        assert!(state.apply(&[fill]).is_err());
        assert!(
            state
                .audit()
                .records()
                .iter()
                .any(|record| record.kind == AuditKind::Diagnostic && record.order_id == 90)
        );
    }
}
