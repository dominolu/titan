use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use titan_plugin_engine::{
    EventPublishMetadata, EventPublisher, PluginError, ResourceScopeHandle, TraceContext,
};

use crate::{ConnectorError, MARKET_EVENT_SCHEMA_VERSION, MarketError, MarketErrorKind};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MarketSourceId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct AssetId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MarketSourceHandle {
    pub source_id: MarketSourceId,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketInstrumentBinding {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    /// Exchange price tick represented by one `price_ticks` unit in Market ABI payloads.
    pub price_tick: f64,
    /// Exchange quantity lot represented by one `quantity_lots` unit in Market ABI payloads.
    pub quantity_lot: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketSourceDefinition {
    pub source_key: Arc<str>,
    pub connector_type: Arc<str>,
    pub connector_config: Arc<[u8]>,
    pub instruments: Arc<[MarketInstrumentBinding]>,
    pub enabled: bool,
    pub definition_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SourceStreamId(pub u32);

#[derive(Clone)]
struct CoreMarketEventPublisher {
    inner: EventPublisher,
    market_stream: SourceStreamId,
    control_stream: SourceStreamId,
    source_sequence: Arc<Mutex<u64>>,
}

/// Backend used by dynamically loaded venue plugins. Implementations live inside the venue
/// library and translate these calls to the fixed-layout C callbacks supplied by the host.
pub trait MarketEventSink: Send + Sync + 'static {
    fn publish_market(
        &self,
        event_type: &str,
        payload: &[u8],
        asset_id: AssetId,
        exchange_ts: i64,
        receive_ts: i64,
        trace: TraceContext,
    ) -> Result<(), PluginError>;

    fn publish_control(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError>;
}

#[derive(Clone)]
enum MarketEventPublisherBackend {
    Core(CoreMarketEventPublisher),
    Dynamic(Arc<dyn MarketEventSink>),
}

#[derive(Clone)]
pub struct MarketEventPublisher {
    backend: MarketEventPublisherBackend,
}

impl MarketEventPublisher {
    pub(crate) fn new(
        inner: EventPublisher,
        market_stream: SourceStreamId,
        control_stream: SourceStreamId,
    ) -> Self {
        Self {
            backend: MarketEventPublisherBackend::Core(CoreMarketEventPublisher {
                inner,
                market_stream,
                control_stream,
                source_sequence: Arc::new(Mutex::new(0)),
            }),
        }
    }

    /// Constructs the plugin-side publisher used by a dynamic Connector implementation.
    pub fn from_sink(sink: Arc<dyn MarketEventSink>) -> Self {
        Self {
            backend: MarketEventPublisherBackend::Dynamic(sink),
        }
    }

    pub fn publish_market(
        &self,
        event_type: &str,
        payload: &[u8],
        asset_id: AssetId,
        exchange_ts: i64,
        receive_ts: i64,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let MarketEventPublisherBackend::Core(core) = &self.backend else {
            return match &self.backend {
                MarketEventPublisherBackend::Dynamic(sink) => sink.publish_market(
                    event_type,
                    payload,
                    asset_id,
                    exchange_ts,
                    receive_ts,
                    trace,
                ),
                MarketEventPublisherBackend::Core(_) => unreachable!(),
            };
        };
        let mut committed_sequence = core
            .source_sequence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let source_sequence = committed_sequence.saturating_add(1);
        let result = core.inner.publish_with_metadata(
            event_type,
            MARKET_EVENT_SCHEMA_VERSION,
            payload,
            EventPublishMetadata {
                source_id: core.market_stream.0,
                source_sequence,
                exchange_ts,
                receive_ts,
                routing_key: u64::from(asset_id.0),
                ..EventPublishMetadata::default()
            },
            trace,
        );
        if result.is_ok() {
            *committed_sequence = source_sequence;
        }
        result
    }

    /// Reserves a MarketBatch arena block, lets the connector bridge encode directly into it, and
    /// commits the source sequence only after publication succeeds.
    pub fn publish_market_batch(
        &self,
        event_type: &str,
        payload_length: usize,
        asset_id: AssetId,
        exchange_ts: i64,
        receive_ts: i64,
        trace: TraceContext,
        encode: impl FnOnce(&mut [u8]) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        if let MarketEventPublisherBackend::Dynamic(sink) = &self.backend {
            let mut payload = vec![0_u8; payload_length];
            encode(&mut payload)?;
            return sink
                .publish_market(
                    event_type,
                    &payload,
                    asset_id,
                    exchange_ts,
                    receive_ts,
                    trace,
                )
                .map_err(|error| ConnectorError::new(error.to_string()));
        }
        let MarketEventPublisherBackend::Core(core) = &self.backend else {
            unreachable!()
        };
        let mut committed_sequence = core
            .source_sequence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let source_sequence = committed_sequence.saturating_add(1);
        let mut reservation = core
            .inner
            .reserve_market_batch(
                event_type,
                MARKET_EVENT_SCHEMA_VERSION,
                payload_length,
                EventPublishMetadata {
                    source_id: core.market_stream.0,
                    source_sequence,
                    exchange_ts,
                    receive_ts,
                    routing_key: u64::from(asset_id.0),
                    ..EventPublishMetadata::default()
                },
                trace,
            )
            .map_err(|error| ConnectorError::new(error.to_string()))?;
        encode(reservation.payload_mut())?;
        reservation
            .commit()
            .map_err(|error| ConnectorError::new(error.to_string()))?;
        *committed_sequence = source_sequence;
        Ok(())
    }

    pub fn publish_control(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        if let MarketEventPublisherBackend::Dynamic(sink) = &self.backend {
            return sink.publish_control(event_type, payload, trace);
        }
        let MarketEventPublisherBackend::Core(core) = &self.backend else {
            unreachable!()
        };
        core.inner.publish_with_metadata(
            event_type,
            MARKET_EVENT_SCHEMA_VERSION,
            payload,
            EventPublishMetadata {
                source_id: core.control_stream.0,
                ..EventPublishMetadata::default()
            },
            trace,
        )
    }
}

#[derive(Clone)]
pub struct MarketConnectorContext {
    pub source: MarketSourceHandle,
    pub instruments: Arc<[MarketInstrumentBinding]>,
    pub market_source_stream: SourceStreamId,
    pub control_source_stream: SourceStreamId,
    pub event_publisher: MarketEventPublisher,
    pub resources: ResourceScopeHandle,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum MarketDataKind {
    Depth,
    Trades,
    Bbo,
    Ticker,
    MarkPrice,
    FundingRate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketSubscribeRequest {
    pub asset_id: AssetId,
    pub kinds: Arc<[MarketDataKind]>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MarketSubscription {
    pub id: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOperationSnapshot {
    pub id: OperationId,
    pub state: OperationState,
    pub detail: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketOperationSnapshot {
    pub id: OperationId,
    pub state: OperationState,
    pub detail: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstrumentSnapshot {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    pub available: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConnectorHealth {
    Created,
    Starting,
    Running,
    Degraded,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorHealthSnapshot {
    pub state: ConnectorHealth,
    pub message: Arc<str>,
    pub observed_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorDiagnosticSnapshot {
    pub summary: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorLifecycle {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketSourceSnapshot {
    pub handle: MarketSourceHandle,
    pub source_key: Arc<str>,
    pub connector_type: Arc<str>,
    pub definition_version: u64,
    pub enabled: bool,
    pub lifecycle: ConnectorLifecycle,
}

pub trait MarketConnectorFactory: Send + Sync + 'static {
    fn connector_type(&self) -> &str;
    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError>;
}

pub trait MarketConnector: Send + Sync + 'static {
    fn start(&self) -> Result<(), ConnectorError>;
    fn stop(&self, deadline: std::time::Instant) -> Result<(), ConnectorError>;
    fn subscribe(
        &self,
        request: MarketSubscribeRequest,
    ) -> Result<MarketSubscription, ConnectorError>;
    fn unsubscribe(&self, subscription: MarketSubscription) -> Result<OperationId, ConnectorError>;
    fn request_snapshot(&self, asset_id: AssetId) -> Result<OperationId, ConnectorError>;
    fn instruments(&self) -> Arc<[InstrumentSnapshot]>;
    fn health(&self) -> ConnectorHealthSnapshot;
    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot;
    fn operation(&self, id: OperationId) -> ConnectorOperationSnapshot;
}

pub(crate) fn connector_error(action: &str, error: ConnectorError) -> MarketError {
    MarketError::new(
        MarketErrorKind::ConnectorRejected,
        format!("{action}: {error}"),
    )
}
