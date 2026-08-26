use super::{
    execution::{CurrencyId, ExecutionReport, FundingReport, InstrumentId, VenueId},
    scheduler::EventKey,
};

/// Monotonic CPU time consumed by the current process. Unsupported targets return zero while
/// Unix targets use the kernel process CPU clock rather than wall time.
pub fn process_cpu_time_ns() -> u64 {
    #[cfg(unix)]
    {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let status = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut value) };
        if status == 0 && value.tv_sec >= 0 && value.tv_nsec >= 0 {
            return (value.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(value.tv_nsec as u64);
        }
    }
    0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndPolicy {
    DrainAll,
    StopAtDataEnd,
    StopAtTime(i64),
}

impl Default for EndPolicy {
    fn default() -> Self {
        Self::DrainAll
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunTermination {
    DataEnd,
    StrategyStop,
    RiskStop,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineComponent {
    Configuration,
    DataSource,
    Scheduler,
    Strategy,
    Risk,
    Matching,
    Account,
    Projector,
    Recorder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum EngineErrorCode {
    InvalidConfiguration,
    InvalidData,
    CallbackFailed,
    InvalidState,
    AccountInvariant,
    CapabilityMismatch,
    RecorderFlush,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("run {run_id} {component:?}/{code:?}: {context}")]
pub struct StructuredEngineError {
    pub run_id: u64,
    pub component: EngineComponent,
    pub event_key: Option<EventKey>,
    pub code: EngineErrorCode,
    pub context: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelIdentity {
    pub id: String,
    pub version: u32,
    pub config_hash: u64,
}

impl ModelIdentity {
    pub fn new(id: impl Into<String>, version: u32) -> Self {
        Self {
            id: id.into(),
            version,
            config_hash: 0,
        }
    }

    pub fn with_config_hash(mut self, config_hash: u64) -> Self {
        self.config_hash = config_hash;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReproducibilityMetadata {
    pub engine_version: String,
    pub git_revision: String,
    pub strategy_id: String,
    pub strategy_version: String,
    pub runtime_abi_version: u32,
    pub phase_contract_version: u32,
    pub data_manifest_hash: u64,
    pub config_hash: u64,
    pub matching: ModelIdentity,
    pub fee: ModelIdentity,
    pub latency: ModelIdentity,
    pub risk: ModelIdentity,
    pub execution_quality: ModelIdentity,
    pub random_seed: u64,
}

impl ReproducibilityMetadata {
    /// Stable identity for result comparison. Every input/model/phase/seed field contributes.
    pub fn run_fingerprint(&self) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        let mut mix = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        };
        for value in [
            u64::from(self.runtime_abi_version),
            u64::from(self.phase_contract_version),
            self.data_manifest_hash,
            self.config_hash,
            self.random_seed,
        ] {
            mix(&value.to_le_bytes());
        }
        for text in [
            &self.engine_version,
            &self.git_revision,
            &self.strategy_id,
            &self.strategy_version,
            &self.matching.id,
            &self.fee.id,
            &self.latency.id,
            &self.risk.id,
            &self.execution_quality.id,
        ] {
            mix(text.as_bytes());
            mix(&[0]);
        }
        for version in [
            self.matching.version,
            self.fee.version,
            self.latency.version,
            self.risk.version,
            self.execution_quality.version,
        ] {
            mix(&version.to_le_bytes());
        }
        for config_hash in [
            self.matching.config_hash,
            self.fee.config_hash,
            self.latency.config_hash,
            self.risk.config_hash,
            self.execution_quality.config_hash,
        ] {
            mix(&config_hash.to_le_bytes());
        }
        hash
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AccountSnapshot {
    pub venue_no: u32,
    pub asset_no: u32,
    pub currency: CurrencyId,
    pub position: f64,
    pub balance: f64,
    pub fee: f64,
    pub funding: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub margin: f64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FundingSnapshot {
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub currency: CurrencyId,
    pub settlement_count: u64,
    pub amount: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BacktestResult {
    pub run_id: u64,
    pub metadata: ReproducibilityMetadata,
    pub end_policy: EndPolicy,
    pub termination: RunTermination,
    pub start_exchange_ts: i64,
    pub end_exchange_ts: i64,
    pub start_delivery_ts: i64,
    pub end_delivery_ts: i64,
    pub wall_time_ns: u64,
    pub cpu_time_ns: u64,
    pub market_event_count: u64,
    pub callback_count: Vec<u64>,
    pub order_count: u64,
    pub fill_count: u64,
    pub reject_count: u64,
    pub cancel_count: u64,
    pub expire_count: u64,
    /// Canonical order lifecycle and fill facts captured by the engine. Each partial fill remains
    /// a distinct `ExecutionReportKind::Fill` record linked to its order ID and sequence.
    pub execution_reports: Vec<ExecutionReport>,
    pub exchange_final: Vec<AccountSnapshot>,
    pub local_delivered_final: Vec<AccountSnapshot>,
    /// Funding totals remain attributable by Venue/Instrument/Currency and are never inferred
    /// from a currency-level account balance.
    pub funding: Vec<FundingSnapshot>,
    pub warnings: Vec<String>,
    pub capability_downgrades: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionReportCounts {
    pub order_count: u64,
    pub fill_count: u64,
    pub reject_count: u64,
    pub cancel_count: u64,
    pub expire_count: u64,
}

/// Counts canonical execution facts using the same Venue/Order composite identity as report
/// validation. Client order IDs are not required to be globally unique across venues.
pub fn execution_report_counts(reports: &[ExecutionReport]) -> ExecutionReportCounts {
    use super::execution::ExecutionReportKind;

    let mut counts = ExecutionReportCounts::default();
    let mut order_ids = std::collections::BTreeSet::new();
    for report in reports {
        order_ids.insert((report.venue_id, report.order_id));
        match report.kind {
            ExecutionReportKind::Rejected => counts.reject_count += 1,
            ExecutionReportKind::Canceled => counts.cancel_count += 1,
            ExecutionReportKind::Expired => counts.expire_count += 1,
            ExecutionReportKind::Fill => counts.fill_count += 1,
            ExecutionReportKind::Accepted => {}
        }
    }
    counts.order_count = order_ids.len() as u64;
    counts
}

impl BacktestResult {
    /// Creates an empty result carrying immutable run identity. Runtime counters and account
    /// snapshots are populated directly by the engine without re-running strategy code.
    pub fn empty(metadata: ReproducibilityMetadata) -> Self {
        Self {
            run_id: 0,
            metadata,
            end_policy: EndPolicy::DrainAll,
            termination: RunTermination::DataEnd,
            start_exchange_ts: 0,
            end_exchange_ts: 0,
            start_delivery_ts: 0,
            end_delivery_ts: 0,
            wall_time_ns: 0,
            cpu_time_ns: 0,
            market_event_count: 0,
            callback_count: Vec::new(),
            order_count: 0,
            fill_count: 0,
            reject_count: 0,
            cancel_count: 0,
            expire_count: 0,
            execution_reports: Vec::new(),
            exchange_final: Vec::new(),
            local_delivered_final: Vec::new(),
            funding: Vec::new(),
            warnings: Vec::new(),
            capability_downgrades: Vec::new(),
        }
    }

    pub fn record_funding(&mut self, report: FundingReport) {
        if let Some(snapshot) = self.funding.iter_mut().find(|snapshot| {
            snapshot.venue_id == report.event.venue_id
                && snapshot.instrument_id == report.event.instrument_id
                && snapshot.currency == report.event.currency
        }) {
            snapshot.settlement_count += 1;
            snapshot.amount += report.amount;
        } else {
            self.funding.push(FundingSnapshot {
                venue_id: report.event.venue_id,
                instrument_id: report.event.instrument_id,
                currency: report.event.currency,
                settlement_count: 1,
                amount: report.amount,
            });
            self.funding.sort_by_key(|snapshot| {
                (snapshot.venue_id, snapshot.instrument_id, snapshot.currency)
            });
        }
    }

    pub fn total_funding(&self, currency: CurrencyId) -> f64 {
        self.funding
            .iter()
            .filter(|snapshot| snapshot.currency == currency)
            .map(|snapshot| snapshot.amount)
            .sum()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditKind {
    Command,
    RiskDecision,
    OrderTransition,
    ExecutionReport,
    Fill,
    AccountDelta,
    Funding,
    Liquidation,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AuditRecord {
    pub run_id: u64,
    pub schema_version: u32,
    pub key: EventKey,
    pub kind: AuditKind,
    pub order_id: u64,
    pub code: u32,
    pub value0: f64,
    pub value1: f64,
}

/// Bounded recorder. `drain_chunk` lets callers write chunks without unbounded memory growth;
/// disabled mode returns before touching the buffer.
pub struct AuditRecorder {
    enabled: bool,
    capacity: usize,
    records: Vec<AuditRecord>,
    dropped: u64,
}

pub trait AuditChunkSink {
    type Error;
    fn write_header(&mut self, schema_version: u32, run_id: u64) -> Result<(), Self::Error>;
    fn write_chunk(&mut self, records: &[AuditRecord]) -> Result<(), Self::Error>;
    fn finish(&mut self) -> Result<(), Self::Error>;
}

/// Stable little-endian audit stream. The format starts with `TITAUDIT`, schema version and run
/// ID; every following chunk is length-prefixed and a zero-length chunk terminates the stream.
pub struct BinaryAuditSink<W> {
    writer: W,
    header_written: bool,
    finished: bool,
}

impl<W> BinaryAuditSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_written: false,
            finished: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: std::io::Write> AuditChunkSink for BinaryAuditSink<W> {
    type Error = std::io::Error;

    fn write_header(&mut self, schema_version: u32, run_id: u64) -> Result<(), Self::Error> {
        if self.header_written {
            return Ok(());
        }
        self.writer.write_all(b"TITAUDIT")?;
        self.writer.write_all(&schema_version.to_le_bytes())?;
        self.writer.write_all(&run_id.to_le_bytes())?;
        self.header_written = true;
        Ok(())
    }

    fn write_chunk(&mut self, records: &[AuditRecord]) -> Result<(), Self::Error> {
        self.writer
            .write_all(&(records.len() as u64).to_le_bytes())?;
        for record in records {
            self.writer.write_all(&record.key.timestamp.to_le_bytes())?;
            self.writer
                .write_all(&(record.key.phase as u16).to_le_bytes())?;
            self.writer
                .write_all(&record.key.source_priority.to_le_bytes())?;
            self.writer.write_all(&record.key.venue_no.to_le_bytes())?;
            self.writer.write_all(&record.key.asset_no.to_le_bytes())?;
            self.writer.write_all(&record.key.sequence.to_le_bytes())?;
            self.writer
                .write_all(&audit_kind_code(record.kind).to_le_bytes())?;
            self.writer.write_all(&record.order_id.to_le_bytes())?;
            self.writer.write_all(&record.code.to_le_bytes())?;
            self.writer.write_all(&record.value0.to_le_bytes())?;
            self.writer.write_all(&record.value1.to_le_bytes())?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        if !self.finished {
            self.writer.write_all(&0_u64.to_le_bytes())?;
            self.writer.flush()?;
            self.finished = true;
        }
        Ok(())
    }
}

fn audit_kind_code(kind: AuditKind) -> u16 {
    match kind {
        AuditKind::Command => 1,
        AuditKind::RiskDecision => 2,
        AuditKind::OrderTransition => 3,
        AuditKind::ExecutionReport => 4,
        AuditKind::Fill => 5,
        AuditKind::AccountDelta => 6,
        AuditKind::Funding => 7,
        AuditKind::Liquidation => 8,
        AuditKind::Diagnostic => 9,
    }
}

impl AuditRecorder {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            capacity: 0,
            records: Vec::new(),
            dropped: 0,
        }
    }

    pub fn bounded(capacity: usize) -> Self {
        Self {
            enabled: true,
            capacity,
            records: Vec::with_capacity(capacity),
            dropped: 0,
        }
    }

    #[inline]
    pub fn record(&mut self, record: AuditRecord) {
        if !self.enabled {
            return;
        }
        if self.records.len() == self.capacity {
            self.dropped += 1;
        } else {
            self.records.push(record);
        }
    }

    /// Records one item and flushes a full chunk before accepting more data. Unlike `record`,
    /// this mode never drops records and keeps resident memory bounded by `capacity`.
    pub fn record_streaming<S: AuditChunkSink>(
        &mut self,
        record: AuditRecord,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if !self.enabled {
            return Ok(());
        }
        sink.write_header(record.schema_version, record.run_id)?;
        if self.records.len() == self.capacity && self.capacity > 0 {
            sink.write_chunk(&self.records)?;
            self.records.clear();
        }
        if self.capacity > 0 {
            self.records.push(record);
        }
        Ok(())
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }

    pub fn drain_chunk(&mut self) -> impl Iterator<Item = AuditRecord> + '_ {
        self.records.drain(..)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn reset(&mut self) {
        self.records.clear();
        self.dropped = 0;
    }

    pub fn flush_to<S: AuditChunkSink>(
        &mut self,
        schema_version: u32,
        run_id: u64,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if !self.enabled {
            return Ok(());
        }
        sink.write_header(schema_version, run_id)?;
        sink.write_chunk(&self.records)?;
        sink.finish()?;
        self.records.clear();
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum PreparedRunnerError<E> {
    #[error("prepared runner has been disposed")]
    Disposed,
    #[error("prepared runtime failed: {0}")]
    Runtime(E),
}

pub trait ReusableRuntime {
    type Error: std::fmt::Display;
    type Output;

    fn run_once(&mut self) -> Result<Self::Output, Self::Error>;
    fn reset(&mut self) -> Result<(), Self::Error>;
    fn clear_results(&mut self);
}

/// Owns a prepared runtime and enforces run/reset/dispose lifecycle semantics.
pub struct PreparedRunner<R> {
    runtime: Option<R>,
    run_count: u64,
}

impl<R> PreparedRunner<R>
where
    R: ReusableRuntime,
{
    pub fn new(runtime: R) -> Self {
        Self {
            runtime: Some(runtime),
            run_count: 0,
        }
    }

    pub fn run(&mut self) -> Result<R::Output, PreparedRunnerError<R::Error>> {
        let runtime = self.runtime.as_mut().ok_or(PreparedRunnerError::Disposed)?;
        let output = runtime.run_once().map_err(PreparedRunnerError::Runtime)?;
        self.run_count += 1;
        Ok(output)
    }

    pub fn reset(&mut self) -> Result<(), PreparedRunnerError<R::Error>> {
        self.runtime
            .as_mut()
            .ok_or(PreparedRunnerError::Disposed)?
            .reset()
            .map_err(PreparedRunnerError::Runtime)
    }

    pub fn clear_results(&mut self) -> Result<(), PreparedRunnerError<R::Error>> {
        self.runtime
            .as_mut()
            .ok_or(PreparedRunnerError::Disposed)?
            .clear_results();
        Ok(())
    }

    pub fn dispose(&mut self) {
        self.runtime = None;
    }

    pub fn run_count(&self) -> u64 {
        self.run_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{
            AccountDelta, AccountReport, ExecutionReason, ExecutionReportKind, FundingBoundary,
            FundingEvent,
        },
        types::{Side, Status},
    };

    struct Counter(u64);

    impl ReusableRuntime for Counter {
        type Error = &'static str;
        type Output = u64;

        fn run_once(&mut self) -> Result<Self::Output, Self::Error> {
            self.0 += 1;
            Ok(self.0)
        }

        fn reset(&mut self) -> Result<(), Self::Error> {
            self.0 = 0;
            Ok(())
        }

        fn clear_results(&mut self) {}
    }

    #[test]
    fn prepared_runner_reset_matches_fresh_and_dispose_is_explicit() {
        let mut runner = PreparedRunner::new(Counter(0));
        assert_eq!(runner.run().unwrap(), 1);
        assert_eq!(runner.run().unwrap(), 2);
        runner.reset().unwrap();
        assert_eq!(runner.run().unwrap(), 1);
        runner.dispose();
        assert_eq!(runner.run(), Err(PreparedRunnerError::Disposed));
    }

    #[test]
    fn execution_counts_use_venue_and_order_composite_identity() {
        let report = |venue, kind, sequence| ExecutionReport {
            kind,
            reason: ExecutionReason::None,
            venue_id: VenueId(venue),
            instrument_id: InstrumentId(venue + 1),
            asset_no: venue,
            order_id: 7,
            venue_order_id: u64::from(venue),
            exchange_ts: 1,
            delivery_ts: 1,
            sequence,
            status: Status::Filled,
            side: Side::Buy,
            order_price: 1.0,
            order_qty: 1.0,
            exec_price: 1.0,
            exec_qty: 1.0,
            maker: false,
            account_delta: None,
        };
        let reports = [
            report(1, ExecutionReportKind::Accepted, 1),
            report(1, ExecutionReportKind::Fill, 2),
            report(2, ExecutionReportKind::Fill, 3),
        ];
        let counts = execution_report_counts(&reports);
        assert_eq!(counts.order_count, 2);
        assert_eq!(counts.fill_count, 2);
    }

    #[test]
    fn audit_is_bounded_and_chunk_draining_is_reusable() {
        let mut recorder = AuditRecorder::bounded(1);
        let record = AuditRecord {
            run_id: 1,
            schema_version: 1,
            key: EventKey {
                timestamp: 1,
                phase: super::super::scheduler::EventPhase::Matching,
                source_priority: 0,
                venue_no: 0,
                asset_no: 0,
                sequence: 0,
            },
            kind: AuditKind::Fill,
            order_id: 2,
            code: 0,
            value0: 1.0,
            value1: 2.0,
        };
        recorder.record(record);
        recorder.record(record);
        assert_eq!(recorder.records(), &[record]);
        assert_eq!(recorder.dropped(), 1);
        assert_eq!(recorder.drain_chunk().count(), 1);
    }

    #[test]
    fn binary_audit_sink_writes_versioned_bounded_chunks() {
        let record = AuditRecord {
            run_id: 4,
            schema_version: 2,
            key: EventKey {
                timestamp: 10,
                phase: super::super::scheduler::EventPhase::Matching,
                source_priority: 1,
                venue_no: 2,
                asset_no: 3,
                sequence: 4,
            },
            kind: AuditKind::ExecutionReport,
            order_id: 5,
            code: 6,
            value0: 7.0,
            value1: 8.0,
        };
        let mut recorder = AuditRecorder::bounded(1);
        let mut sink = BinaryAuditSink::new(Vec::new());
        recorder.record_streaming(record, &mut sink).unwrap();
        recorder.record_streaming(record, &mut sink).unwrap();
        recorder.flush_to(2, 4, &mut sink).unwrap();
        let bytes = sink.into_inner();
        assert_eq!(&bytes[..8], b"TITAUDIT");
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 2);
        assert_eq!(u64::from_le_bytes(bytes[12..20].try_into().unwrap()), 4);
        assert_eq!(u64::from_le_bytes(bytes[20..28].try_into().unwrap()), 1);
        assert!(bytes.ends_with(&0_u64.to_le_bytes()));
    }

    #[test]
    fn funding_result_keeps_instrument_attribution_and_currency_total() {
        let metadata = ReproducibilityMetadata {
            engine_version: "1".into(),
            git_revision: "g".into(),
            strategy_id: "s".into(),
            strategy_version: "1".into(),
            runtime_abi_version: 1,
            phase_contract_version: 1,
            data_manifest_hash: 1,
            config_hash: 1,
            matching: ModelIdentity::new("m", 1),
            fee: ModelIdentity::new("f", 1),
            latency: ModelIdentity::new("l", 1),
            risk: ModelIdentity::new("r", 1),
            execution_quality: ModelIdentity::new("q", 1),
            random_seed: 7,
        };
        let mut result = BacktestResult::empty(metadata);
        let baseline = result.metadata.run_fingerprint();
        let mut changed = result.metadata.clone();
        changed.random_seed += 1;
        assert_ne!(baseline, changed.run_fingerprint());
        changed = result.metadata.clone();
        changed.phase_contract_version += 1;
        assert_ne!(baseline, changed.run_fingerprint());
        changed = result.metadata.clone();
        changed.data_manifest_hash += 1;
        assert_ne!(baseline, changed.run_fingerprint());
        changed = result.metadata.clone();
        changed.fee.version += 1;
        assert_ne!(baseline, changed.run_fingerprint());
        changed = result.metadata.clone();
        changed.fee.config_hash = 99;
        assert_ne!(baseline, changed.run_fingerprint());
        for (event_id, instrument_id, amount) in [(1, 2, -0.2), (2, 3, 0.1), (3, 2, -0.3)] {
            let event = FundingEvent {
                event_id,
                venue_id: VenueId(1),
                instrument_id: InstrumentId(instrument_id),
                currency: CurrencyId(7),
                publication_ts: 1,
                effective_ts: 2,
                settlement_ts: 3,
                rate: 0.001,
                price_source: crate::backtest::execution::FundingPriceSource::Mark,
                mark_price: 100.0,
                boundary: FundingBoundary::BeforeSettlementEvents,
            };
            result.record_funding(FundingReport {
                event,
                delivery_ts: 4,
                sequence: event_id,
                position_qty: 1.0,
                amount,
                account_report: AccountReport {
                    venue_id: VenueId(1),
                    exchange_ts: 3,
                    delivery_ts: 4,
                    sequence: event_id,
                    delta: AccountDelta {
                        instrument_id: InstrumentId(instrument_id),
                        position_delta: 0.0,
                        trade_qty: 0.0,
                        trade_value: 0.0,
                        currency: CurrencyId(7),
                        cash_delta: 0.0,
                        fee: 0.0,
                        funding: amount,
                        execution_price: 0.0,
                        realized_pnl: 0.0,
                    },
                },
            });
        }
        assert_eq!(result.funding.len(), 2);
        assert_eq!(result.funding[0].settlement_count, 2);
        assert!((result.total_funding(CurrencyId(7)) + 0.4).abs() < 1e-12);
    }
}
