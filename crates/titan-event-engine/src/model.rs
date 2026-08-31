use std::sync::Arc;

use titan_plugin_engine::TraceContext;

use crate::OwnedEvent;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum PoolKind {
    SmallEvent = 0,
    MarketBatch = 1,
    Snapshot = 2,
}

impl PoolKind {
    pub(crate) const ALL: [Self; 3] = [Self::SmallEvent, Self::MarketBatch, Self::Snapshot];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EventClass {
    Critical,
    Market,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventDescriptor {
    pub id: u32,
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub class: EventClass,
    pub pool: PoolKind,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventHeader {
    pub source_id: u32,
    pub event_type_id: u32,
    pub schema_version: u32,
    pub flags: u32,
    pub source_sequence: u64,
    pub local_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub trace: TraceContext,
}

#[derive(Clone, Copy, Debug)]
pub struct PublishRequest<'a> {
    pub event_type: &'a str,
    pub schema_version: u32,
    pub source_id: u32,
    pub source_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub flags: u32,
    pub trace: TraceContext,
    pub payload: &'a [u8],
}

impl<'a> PublishRequest<'a> {
    pub fn new(event_type: &'a str, schema_version: u32, payload: &'a [u8]) -> Self {
        Self {
            event_type,
            schema_version,
            source_id: 0,
            source_sequence: 0,
            exchange_ts: 0,
            receive_ts: 0,
            publish_ts: 0,
            routing_key: 0,
            flags: 0,
            trace: TraceContext::default(),
            payload,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReserveRequest<'a> {
    pub event_type: &'a str,
    pub schema_version: u32,
    pub payload_length: usize,
    pub source_id: u32,
    pub source_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub flags: u32,
    pub trace: TraceContext,
}

impl<'a> ReserveRequest<'a> {
    pub fn new(event_type: &'a str, schema_version: u32, payload_length: usize) -> Self {
        Self {
            event_type,
            schema_version,
            payload_length,
            source_id: 0,
            source_sequence: 0,
            exchange_ts: 0,
            receive_ts: 0,
            publish_ts: 0,
            routing_key: 0,
            flags: 0,
            trace: TraceContext::default(),
        }
    }
}

pub(crate) struct EventRecord {
    pub descriptor: Arc<EventDescriptor>,
    pub header: EventHeader,
    pub payload: OwnedEvent,
    pub ingress_at_ns: u64,
}

impl EventRecord {
    pub(crate) fn delivery(&self) -> Delivery {
        Delivery {
            descriptor: self.descriptor.clone(),
            header: self.header,
            payload: self.payload.clone(),
            ingress_at_ns: self.ingress_at_ns,
        }
    }
}

pub(crate) struct Delivery {
    pub descriptor: Arc<EventDescriptor>,
    pub header: EventHeader,
    pub payload: OwnedEvent,
    pub ingress_at_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerSignal {
    pub timer_id: u64,
    pub deadline_ns: u64,
    pub fired_at_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceStage {
    Published,
    EventLoopDequeued,
    Pending,
    Dispatched,
    SubscriberReceived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TracePoint {
    pub trace: TraceContext,
    pub local_sequence: u64,
    pub source_sequence: u64,
    pub subscriber_id: u64,
    pub stage: TraceStage,
    pub timestamp_ns: u64,
}

impl Clone for Delivery {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            header: self.header,
            payload: self.payload.clone(),
            ingress_at_ns: self.ingress_at_ns,
        }
    }
}
