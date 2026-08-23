use crate::types::{OrdType, OrderId, Side, TimeInForce};

use super::{InstrumentId, VenueId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderOrigin {
    Strategy,
    ExecutionAlgorithm,
    Liquidation,
}

/// Execution-domain request produced after decoding the public/FFI command format.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExecutionOrderRequest {
    pub client_order_id: OrderId,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrdType,
    pub reduce_only: bool,
    pub origin: OrderOrigin,
    pub local_submit_ts: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CancelRequest {
    pub client_order_id: OrderId,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub local_submit_ts: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ExecutionCommand {
    Submit(ExecutionOrderRequest),
    Cancel(CancelRequest),
}
