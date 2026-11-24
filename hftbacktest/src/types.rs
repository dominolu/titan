use std::{collections::HashMap, fmt::Debug};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::{backtest::data::POD, depth::MarketDepth};

/// Values and venue errors carried by the live event transport. Defined in
/// `titan_domain_types` so error handling does not depend on the backtest engine;
/// re-exported here unchanged.
pub use titan_domain_types::{ErrorKind, LiveError, Value};

/// Events occurring in a live bot sent by a [`Connector`](`crate::connector::Connector`).
#[derive(Clone, Debug, Decode, Encode)]
pub enum LiveEvent {
    BatchStart,
    BatchEnd,
    Feed {
        symbol: String,
        event: Event,
    },
    Order {
        symbol: String,
        order: Order,
    },
    Position {
        symbol: String,
        qty: f64,
        exch_ts: i64,
    },
    Funding {
        symbol: String,
        funding_rate: f64,
        /// Next settlement time in nanoseconds since the Unix epoch. 0 when unknown.
        next_funding_time: i64,
        exch_ts: i64,
    },
    Error(LiveError),
    /// Account funding settlement. Unlike `Funding`, this carries the actual cash amount and is
    /// projected to the strategy's `on_funding` callback exactly once. Appended to preserve the
    /// encoded discriminants of existing IPC variants.
    FundingSettlement {
        symbol: String,
        event_id: u64,
        amount: f64,
        position_qty: f64,
        funding_rate: f64,
        mark_price: f64,
        currency: u32,
        exch_ts: i64,
    },
    /// Canonical connector execution event. Unlike the legacy `Order` variant, this carries a
    /// venue-stable event ID, complete identity/reason fields and the immutable account delta
    /// required for reconnect deduplication and backtest/live projector equivalence. Appended to
    /// preserve all existing encoded enum discriminants.
    ExecutionReport {
        symbol: String,
        event_id: u128,
        venue_no: u32,
        instrument_id: u32,
        asset_no: u32,
        order_id: u64,
        venue_order_id: u64,
        exchange_ts: i64,
        delivery_ts: i64,
        sequence: u64,
        /// 1 Accepted, 2 Rejected, 3 Canceled, 4 Expired, 5 PartiallyFilled, 6 Filled.
        status: u32,
        reason: u32,
        side: i8,
        order_price: f64,
        order_qty: f64,
        leaves_qty: f64,
        exec_price: f64,
        exec_qty: f64,
        maker: bool,
        local_submit_ts: i64,
        time_in_force: u8,
        order_type: u8,
        request: u8,
        account_delta: Option<LiveAccountDelta>,
    },
    /// A connector-ingress feed batch shared by connector channels and tests. The variant is kept
    /// to preserve the encoded discriminants used by existing channels.
    FeedBatch {
        instrument_id: u64,
        events: Vec<Event>,
    },
}

/// Wire-safe immutable account mutation attached to a canonical live fill.
#[derive(Clone, Copy, Debug, Decode, Encode)]
pub struct LiveAccountDelta {
    pub instrument_id: u32,
    pub position_delta: f64,
    pub trade_qty: f64,
    pub trade_value: f64,
    pub currency: u32,
    pub cash_delta: f64,
    pub fee: f64,
    pub funding: f64,
    pub execution_price: f64,
    pub realized_pnl: f64,
}

/// Market event and side flags. Defined in `titan_domain_types` alongside [`Event`], so a market
/// data consumer can name them without depending on this crate.
pub use titan_domain_types::{
    BUY_EVENT, DEPTH_BBO_EVENT, DEPTH_CLEAR_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, SELL_EVENT,
    TRADE_EVENT,
};

/// Indicates that an order has been added to the order book.
pub const ADD_ORDER_EVENT: u64 = 10;

/// Indicates that an order in the order book has been canceled.
pub const CANCEL_ORDER_EVENT: u64 = 11;

/// Indicates that an order in the order book has been modified.
pub const MODIFY_ORDER_EVENT: u64 = 12;

/// Indicates that an order in the order book has been filled.
pub const FILL_EVENT: u64 = 13;

/// Indicates that it is a valid event to be handled by the exchange processor at the exchange
/// timestamp.
pub const EXCH_EVENT: u64 = 1 << 31;

/// Indicates that it is a valid event to be handled by the local processor at the local timestamp.
pub use titan_domain_types::LOCAL_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], and [`LOCAL_EVENT`].
pub use titan_domain_types::LOCAL_DEPTH_CLEAR_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | EXCH_EVENT;

/// Bid/ask side local depth, snapshot and best-quote events. Defined in `titan_domain_types`
/// alongside [`Event`], so a market data consumer can name them without depending on this crate.
pub use titan_domain_types::{
    LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
    LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT, LOCAL_TRADE_EVENT,
};

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`BUY_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_BID_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | BUY_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`SELL_EVENT`], and [`LOCAL_EVENT`].
pub const LOCAL_ASK_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | SELL_EVENT | LOCAL_EVENT;

/// Represents a combination of [`DEPTH_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_EVENT: u64 = DEPTH_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_EVENT: u64 = DEPTH_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_CLEAR_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_SNAPSHOT_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`BUY_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_BID_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | BUY_EVENT | EXCH_EVENT;

/// Represents a combination of [`DEPTH_BBO_EVENT`], [`SELL_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_ASK_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | SELL_EVENT | EXCH_EVENT;

/// Represents a combination of [`TRADE_EVENT`], and [`EXCH_EVENT`].
pub const EXCH_TRADE_EVENT: u64 = TRADE_EVENT | EXCH_EVENT;

/// Represents a combination of [`EXCH_TRADE_EVENT`] and [`BUY_EVENT`].
pub const EXCH_BUY_TRADE_EVENT: u64 = EXCH_TRADE_EVENT | BUY_EVENT;

/// Represents a combination of [`EXCH_TRADE_EVENT`] and [`SELL_EVENT`].
pub const EXCH_SELL_TRADE_EVENT: u64 = EXCH_TRADE_EVENT | SELL_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`ADD_ORDER_EVENT`].
pub const LOCAL_ADD_ORDER_EVENT: u64 = LOCAL_EVENT | ADD_ORDER_EVENT;

/// Represents a combination of [`BUY_EVENT`] and [`LOCAL_ADD_ORDER_EVENT`].
pub const LOCAL_BID_ADD_ORDER_EVENT: u64 = BUY_EVENT | LOCAL_ADD_ORDER_EVENT;

/// Represents a combination of [`SELL_EVENT`] and [`LOCAL_ADD_ORDER_EVENT`].
pub const LOCAL_ASK_ADD_ORDER_EVENT: u64 = SELL_EVENT | LOCAL_ADD_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`CANCEL_ORDER_EVENT`].
pub const LOCAL_CANCEL_ORDER_EVENT: u64 = LOCAL_EVENT | CANCEL_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`MODIFY_ORDER_EVENT`].
pub const LOCAL_MODIFY_ORDER_EVENT: u64 = LOCAL_EVENT | MODIFY_ORDER_EVENT;

/// Represents a combination of [`LOCAL_EVENT`] and [`FILL_EVENT`].
pub const LOCAL_FILL_EVENT: u64 = LOCAL_EVENT | FILL_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`ADD_ORDER_EVENT`].
pub const EXCH_ADD_ORDER_EVENT: u64 = EXCH_EVENT | ADD_ORDER_EVENT;

/// Represents a combination of [`BUY_EVENT`] and [`EXCH_ADD_ORDER_EVENT`].
pub const EXCH_BID_ADD_ORDER_EVENT: u64 = BUY_EVENT | EXCH_ADD_ORDER_EVENT;

/// Represents a combination of [`SELL_EVENT`] and [`EXCH_ADD_ORDER_EVENT`].
pub const EXCH_ASK_ADD_ORDER_EVENT: u64 = SELL_EVENT | EXCH_ADD_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`CANCEL_ORDER_EVENT`].
pub const EXCH_CANCEL_ORDER_EVENT: u64 = EXCH_EVENT | CANCEL_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`MODIFY_ORDER_EVENT`].
pub const EXCH_MODIFY_ORDER_EVENT: u64 = EXCH_EVENT | MODIFY_ORDER_EVENT;

/// Represents a combination of [`EXCH_EVENT`] and [`FILL_EVENT`].
pub const EXCH_FILL_EVENT: u64 = EXCH_EVENT | FILL_EVENT;

/// Indicates that one should continue until the end of the data.
pub const UNTIL_END_OF_DATA: i64 = i64::MAX;

pub use titan_domain_types::OrderId;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum WaitOrderResponse {
    None,
    Any,
    Specified { asset_no: usize, order_id: OrderId },
}

pub use titan_domain_types::Event;

unsafe impl POD for Event {}

impl crate::backtest::data::NpyDTyped for Event {
    fn read_parquet(path: &str) -> std::io::Result<crate::backtest::data::Data<Self>> {
        let mut events = Vec::new();
        titan_persistence::columnar::scan_market(std::path::Path::new(path), |event| {
            if events.len() == 1_048_576 {
                return Err(titan_persistence::failure(
                    "Parquet Event shard exceeds 64 MiB; split the dataset",
                ));
            }
            events.push(event);
            Ok(())
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(crate::backtest::data::Data::from_data(&events))
    }
    fn descr() -> Vec<crate::backtest::data::Field> {
        let endian = if cfg!(target_endian = "little") {
            "<"
        } else {
            ">"
        };
        [
            ("ev", "u8"),
            ("exch_ts", "i8"),
            ("local_ts", "i8"),
            ("px", "f8"),
            ("qty", "f8"),
            ("order_id", "u8"),
            ("ival", "i8"),
            ("fval", "f8"),
        ]
        .into_iter()
        .map(|(name, ty)| crate::backtest::data::Field {
            name: name.to_owned(),
            ty: format!("{endian}{ty}"),
        })
        .collect()
    }
}

/// Order, side, order type, status and time-in-force types. Defined in `titan_domain_types`
/// so order handling does not depend on the backtest engine; re-exported here unchanged.
pub use titan_domain_types::{AnyClone, OrdType, Order, Side, Status, TimeInForce};

/// An asynchronous request to [`Connector`](`crate::connector::Connector`).
#[derive(Clone, Debug, Encode, Decode)]
pub enum LiveRequest {
    /// An order request, a tuple consisting of an asset number and an [`Order`].
    Order { symbol: String, order: Order },
    /// A request to add an instrument for trading.
    RegisterInstrument {
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    },
}

/// Provides state values.
///
/// **Note:** In a live bot, currently only `position` value is delivered correctly, and other
/// values are invalid.
#[repr(C)]
#[derive(PartialEq, Clone, Debug, Default)]
pub struct StateValues {
    pub position: f64,
    /// Backtest only
    pub balance: f64,
    /// Backtest only
    pub fee: f64,
    // todo: currently, they are cumulative values, but they need to be values within the record
    //       interval.
    /// Backtest only
    pub num_trades: i64,
    /// Backtest only
    pub trading_volume: f64,
    /// Backtest only
    pub trading_value: f64,
}

/// Provides errors that can occur in builders.
#[derive(Error, Debug)]
pub enum BuildError {
    #[error("`{0}` is required")]
    BuilderIncomplete(&'static str),
    #[error("{0}")]
    InvalidArgument(&'static str),
    #[error("`{0}/{1}` already exists")]
    Duplicate(String, String),
    #[error("`{0}` is not found")]
    ConnectorNotFound(String),
    #[error("{0:?}")]
    Error(#[from] anyhow::Error),
}

/// Used to submit an order in a live bot.
#[derive(Decode, Encode)]
pub struct OrderRequest {
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrdType,
}

/// Provides a bot interface for backtesting and live trading.
pub trait Bot<MD>
where
    MD: MarketDepth,
{
    type Error;

    /// In backtesting, this timestamp reflects the time at which the backtesting is conducted
    /// within the provided data. In a live bot, it's literally the current local timestamp.
    fn current_timestamp(&self) -> i64;

    /// Returns the number of assets.
    fn num_assets(&self) -> usize;

    /// Returns the position you currently hold.
    ///
    /// * `asset_no` - Asset number from which the position will be retrieved.
    fn position(&self, asset_no: usize) -> f64;

    /// Returns the state's values such as balance, fee, and so on.
    fn state_values(&self, asset_no: usize) -> &StateValues;

    /// Returns the [`MarketDepth`].
    ///
    /// * `asset_no` - Asset number from which the market depth will be retrieved.
    fn depth(&self, asset_no: usize) -> &MD;

    /// Returns the last market trades.
    ///
    /// * `asset_no` - Asset number from which the last market trades will be retrieved.
    fn last_trades(&self, asset_no: usize) -> &[Event];

    /// Clears the last market trades from the buffer.
    ///
    /// * `asset_no` - Asset number at which this command will be executed. If `None`, all last
    ///   trades in any assets will be cleared.
    fn clear_last_trades(&mut self, asset_no: Option<usize>);

    /// Returns a hash map of order IDs and their corresponding [`Order`]s.
    ///
    /// * `asset_no` - Asset number from which orders will be retrieved.
    fn orders(&self, asset_no: usize) -> &HashMap<OrderId, Order>;

    /// Places a buy order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - The unique order ID; there should not be any existing order with the same ID
    ///   on both local and exchange sides.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `time_in_force` - Available [`TimeInForce`] options vary depending on the exchange model.
    ///   See to the exchange model for details.
    ///
    /// * `order_type` - Available [`OrdType`] options vary depending on the exchange model. See to
    ///   the exchange model for details.
    ///
    /// * `wait` - If true, wait until the order placement response is received.
    #[allow(clippy::too_many_arguments)]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Places a sell order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - The unique order ID; there should not be any existing order with the same ID
    ///   on both local and exchange sides.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `time_in_force` - Available [`TimeInForce`] options vary depending on the exchange model.
    ///   See to the exchange model for details.
    ///
    /// * `order_type` - Available [`OrdType`] options vary depending on the exchange model. See to
    ///   the exchange model for details.
    ///
    /// * `wait` - If true, wait until the order placement response is received.
    #[allow(clippy::too_many_arguments)]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Places an order.
    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Modifies an open order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - Order ID to modify.
    /// * `price` - Order price.
    /// * `qty` - Quantity to buy.
    /// * `wait` - If true, wait until the order modification response is received.
    fn modify(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Cancels an open order.
    ///
    /// * `asset_no` - Asset number at which this command will be executed.
    /// * `order_id` - Order ID to cancel.
    /// * `wait` - If true, wait until the order placement response is received.
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error>;

    /// Clears inactive orders from the local orders whose status is neither [`Status::New`] nor
    /// [`Status::PartiallyFilled`].
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>);

    /// Waits for the response of the order with the given order ID until timeout.
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error>;

    /// Wait until the next feed is received, or until timeout.
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error>;

    /// Elapses the specified duration.
    ///
    /// Args:
    /// * `duration` - Duration to elapse. Nanoseconds is the default unit. However, unit should be
    ///   the same as the data's timestamp unit.
    ///
    /// Returns:
    ///   `Ok(true)` if the method reaches the specified timestamp within the data. If the end of
    ///   the data is reached before the specified timestamp, it returns `Ok(false)`.
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error>;

    /// Elapses time only in backtesting. In live mode, it is ignored.
    ///
    /// The [elapse()](Self::elapse()) method exclusively manages time during backtesting, meaning
    /// that factors such as computing time are not properly accounted for. So, this method can be
    /// utilized to simulate such processing times.
    ///
    /// Args:
    /// * `duration` - Duration to elapse. Nanoseconds is the default unit. However, unit should be
    ///   the same as the data's timestamp unit.
    ///
    /// Returns:
    ///   `Ok(true)` if the method reaches the specified timestamp within the data. If the end of
    ///   the data is reached before the specified timestamp, it returns `Ok(false)`.
    fn elapse_bt(&mut self, duration: i64) -> Result<ElapseResult, Self::Error>;

    /// Closes this backtester or bot.
    fn close(&mut self) -> Result<(), Self::Error>;

    /// Returns the last feed's exchange timestamp and local receipt timestamp.
    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)>;

    /// Returns the last order's request timestamp, exchange timestamp, and response receipt
    /// timestamp.
    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)>;
}

/// Provides bot statistics and [`StateValues`] recording features for backtesting result analysis
/// or live bot logging.
pub trait Recorder {
    type Error;

    /// Records the current [`StateValues`].
    fn record<MD, I>(&mut self, hbt: &I) -> Result<(), Self::Error>
    where
        I: Bot<MD>,
        MD: MarketDepth;
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ElapseResult {
    Ok,
    EndOfData,
    MarketFeed,
    OrderResponse,
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::LOCAL_EVENT,
        types::{
            BUY_EVENT, Event, LOCAL_BID_DEPTH_CLEAR_EVENT, LOCAL_BID_DEPTH_EVENT,
            LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT,
        },
    };

    #[test]
    fn test_event_is() {
        let event = Event {
            ev: LOCAL_BID_DEPTH_CLEAR_EVENT | (1 << 20),
            exch_ts: 0,
            local_ts: 0,
            order_id: 0,
            px: 0.0,
            qty: 0.0,
            ival: 0,
            fval: 0.0,
        };

        assert!(!event.is(LOCAL_BID_DEPTH_EVENT));
        assert!(!event.is(LOCAL_BUY_TRADE_EVENT));
        assert!(event.is(LOCAL_BID_DEPTH_CLEAR_EVENT));

        let event = Event {
            ev: LOCAL_EVENT | BUY_EVENT | 0xff,
            exch_ts: 0,
            local_ts: 0,
            order_id: 0,
            px: 0.0,
            qty: 0.0,
            ival: 0,
            fval: 0.0,
        };

        assert!(!event.is(LOCAL_BID_DEPTH_EVENT));
        assert!(!event.is(LOCAL_BUY_TRADE_EVENT));
        assert!(!event.is(LOCAL_BID_DEPTH_CLEAR_EVENT));
        assert!(!event.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT));
        assert!(event.is(LOCAL_EVENT));
        assert!(event.is(BUY_EVENT));
    }
}
