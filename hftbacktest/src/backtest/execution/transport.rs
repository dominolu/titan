use super::{ExecutionOrderRequest, ExecutionReport};

pub trait ExecutionLatencyModel {
    /// Negative latency means a local/technical rejection delivered after `abs(latency)`.
    fn entry_latency(&mut self, request: &ExecutionOrderRequest) -> i64;
    fn response_latency(&mut self, report: &ExecutionReport) -> i64;
    fn reset(&mut self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestTiming {
    ExchangeArrival { exchange_ts: i64 },
    LocalReject { delivery_ts: i64 },
}

pub struct OrderTransport<L> {
    latency: L,
}

impl<L> OrderTransport<L>
where
    L: ExecutionLatencyModel,
{
    pub fn new(latency: L) -> Self {
        Self { latency }
    }

    pub fn request_timing(&mut self, request: &ExecutionOrderRequest) -> RequestTiming {
        let latency = self.latency.entry_latency(request);
        if latency < 0 {
            RequestTiming::LocalReject {
                delivery_ts: request
                    .local_submit_ts
                    .saturating_add(latency.saturating_abs()),
            }
        } else {
            RequestTiming::ExchangeArrival {
                exchange_ts: request.local_submit_ts.saturating_add(latency),
            }
        }
    }

    pub fn response_delivery_ts(&mut self, report: &ExecutionReport) -> i64 {
        let latency = self.latency.response_latency(report);
        assert!(latency >= 0, "response latency must not be negative");
        report.exchange_ts.saturating_add(latency)
    }

    pub fn reset(&mut self) {
        self.latency.reset();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConstantExecutionLatency {
    pub entry: i64,
    pub response: i64,
}

impl ExecutionLatencyModel for ConstantExecutionLatency {
    fn entry_latency(&mut self, _request: &ExecutionOrderRequest) -> i64 {
        self.entry
    }

    fn response_latency(&mut self, _report: &ExecutionReport) -> i64 {
        self.response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{InstrumentId, OrderOrigin, VenueId},
        types::{OrdType, Side, TimeInForce},
    };

    fn request() -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            client_order_id: 1,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(1),
            price: 10.0,
            qty: 1.0,
            side: Side::Buy,
            time_in_force: TimeInForce::GTC,
            order_type: OrdType::Limit,
            reduce_only: false,
            origin: OrderOrigin::Strategy,
            local_submit_ts: 100,
        }
    }

    #[test]
    fn separates_exchange_arrival_from_local_rejection() {
        let mut accepted = OrderTransport::new(ConstantExecutionLatency {
            entry: 10,
            response: 20,
        });
        assert_eq!(
            accepted.request_timing(&request()),
            RequestTiming::ExchangeArrival { exchange_ts: 110 }
        );

        let mut rejected = OrderTransport::new(ConstantExecutionLatency {
            entry: -7,
            response: 20,
        });
        assert_eq!(
            rejected.request_timing(&request()),
            RequestTiming::LocalReject { delivery_ts: 107 }
        );
    }
}
