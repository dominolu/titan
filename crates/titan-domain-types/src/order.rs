//! Order and side types shared by the live connector and the market data recorder.
//!
//! These used to live in `hftbacktest::types`. They describe an order as the venue and the local
//! order manager see it, so they belong with the domain model rather than with the backtest
//! engine. `hftbacktest` re-exports them, which keeps its public API unchanged.

use std::{
    any::Any,
    fmt::{Debug, Formatter},
};

use bincode::{
    BorrowDecode, Decode, Encode,
    de::{BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use dyn_clone::DynClone;

/// The numeric identifier a venue or the local order manager assigns to an order.
pub type OrderId = u64;

/// Represents a side, which can refer to either the side of an order or the initiator's side in a
/// trade event, with the meaning varying depending on the context.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(i8)]
pub enum Side {
    /// In the market depth event, this indicates the bid side; in the market trade event, it
    /// indicates that the trade initiator is a buyer.
    Buy = 1,
    /// In the market depth event, this indicates the ask side; in the market trade event, it
    /// indicates that the trade initiator is a seller.
    Sell = -1,
    /// No side provided.
    None = 0,
    /// This occurs when the `Connector` receives a side value that
    /// does not have a corresponding enum value.
    Unsupported = 127,
}

impl AsRef<f64> for Side {
    fn as_ref(&self) -> &f64 {
        match self {
            Side::Buy => &1.0f64,
            Side::Sell => &-1.0f64,
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

impl AsRef<str> for Side {
    fn as_ref(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

/// Order status
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum Status {
    None = 0,
    New = 1,
    Expired = 2,
    Filled = 3,
    Canceled = 4,
    PartiallyFilled = 5,
    Rejected = 6,
    Replaced = 7,
    /// This occurs when the `Connector` receives an order status
    /// value that does not have a corresponding enum value.
    Unsupported = 255,
}

/// Time In Force
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum TimeInForce {
    /// Good 'Til Canceled
    GTC = 0,
    /// Post-only
    GTX = 1,
    /// Fill or Kill
    FOK = 2,
    /// Immediate or Cancel
    IOC = 3,
    /// This occurs when the `Connector` receives a time-in-force
    /// value that does not have a corresponding enum value.
    Unsupported = 255,
}

impl AsRef<str> for TimeInForce {
    fn as_ref(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::GTX => "GTX",
            TimeInForce::FOK => "FOK",
            TimeInForce::IOC => "IOC",
            TimeInForce::Unsupported => panic!("TimeInForce::Unsupported"),
        }
    }
}

/// Order type
#[derive(Clone, Copy, Eq, PartialEq, Debug, Decode, Encode)]
#[repr(u8)]
pub enum OrdType {
    Limit = 0,
    Market = 1,
    Unsupported = 255,
}

impl AsRef<str> for OrdType {
    fn as_ref(&self) -> &'static str {
        match self {
            OrdType::Limit => "LIMIT",
            OrdType::Market => "MARKET",
            OrdType::Unsupported => panic!("OrdType::Unsupported"),
        }
    }
}

/// Provides cloning of `Box<dyn Any>`, which is utilized in [Order] for the additional data used in
/// `QueueModel`.
///
/// **Usage:**
/// ```
/// use std::any::Any;
/// use titan_domain_types::AnyClone;
///
/// #[derive(Clone)]
/// struct QueuePos {
///     pos: f64,
/// }
///
/// impl AnyClone for QueuePos {
///     fn as_any(&self) -> &dyn Any {
///         self
///     }
///
///     fn as_any_mut(&mut self) -> &mut dyn Any {
///         self
///     }
/// }
/// ```
pub trait AnyClone: DynClone {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
dyn_clone::clone_trait_object!(AnyClone);

impl AnyClone for () {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// A queue position model can carry a plain `f64`, so the primitive implements `AnyClone` too.
// It lives here because the orphan rule only allows a crate to implement its own trait for a
// foreign primitive.
impl AnyClone for f64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Returns the number of decimal places in `tick_size`'s textual form.
///
/// Venue adapters format order prices with this, so it lives next to [`Order`] rather than with
/// the backtest engine.
pub fn get_precision(tick_size: f64) -> usize {
    let s = tick_size.to_string();
    let mut prec = 0;
    for (i, c) in s.chars().enumerate() {
        if c == '.' {
            prec = s.len() - i - 1;
            break;
        }
    }
    prec
}

/// Order
#[derive(Clone)]
#[repr(C)]
pub struct Order {
    /// Order quantity
    pub qty: f64,
    /// The quantity of this order that has not yet been executed. It represents the remaining
    /// quantity that is still open or active in the market after any partial fills.
    pub leaves_qty: f64,
    /// Executed quantity, only available when this order is executed.
    pub exec_qty: f64,
    /// Executed price in ticks (`executed_price / tick_size`), only available when this order is
    /// executed.
    pub exec_price_tick: i64,
    /// Order price in ticks (`price / tick_size`).
    pub price_tick: i64,
    /// The tick size of the asset associated with this order.
    pub tick_size: f64,
    /// The time at which the exchange processes this order, ideally when the matching engine
    /// processes the order, will be set if the value is available.
    pub exch_timestamp: i64,
    /// The time at which the local receives this order or sent this order to the exchange.
    pub local_timestamp: i64,
    pub order_id: u64,
    /// Additional data used for `QueueModel`.
    /// This is only available in backtesting, and the type `Q` is set to `()` in a live bot.
    pub q: Box<dyn AnyClone + Send>,
    /// Whether the order is executed as a maker, only available when this order is executed.
    pub maker: bool,
    pub order_type: OrdType,
    /// Request status:
    ///   * [`Status::New`]: Request to open a new order.
    ///   * [`Status::Canceled`]: Request to cancel an opened order.
    pub req: Status,
    pub status: Status,
    pub side: Side,
    pub time_in_force: TimeInForce,
}

impl Order {
    /// Constructs an instance of `Order`.
    pub fn new(
        order_id: u64,
        price_tick: i64,
        tick_size: f64,
        qty: f64,
        side: Side,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            qty,
            leaves_qty: qty,
            price_tick,
            tick_size,
            side,
            time_in_force,
            exch_timestamp: 0,
            status: Status::None,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: 0.0,
            order_id,
            q: Box::new(()),
            maker: false,
            order_type,
        }
    }

    /// Returns the order price.
    pub fn price(&self) -> f64 {
        self.price_tick as f64 * self.tick_size
    }

    /// Returns the executed price, only available when this order is executed.
    pub fn exec_price(&self) -> f64 {
        self.exec_price_tick as f64 * self.tick_size
    }

    /// Returns whether this order is cancelable.
    pub fn cancellable(&self) -> bool {
        (self.status == Status::New || self.status == Status::PartiallyFilled)
            && self.req == Status::None
    }

    /// Returns whether this order is active in the market.
    pub fn active(&self) -> bool {
        self.status == Status::New || self.status == Status::PartiallyFilled
    }

    /// Returns whether this order has an ongoing request.
    pub fn pending(&self) -> bool {
        self.req != Status::None
    }

    /// Updates this order with the given order. This is used only by the processor in backtesting
    /// or by a bot in live trading.
    pub fn update(&mut self, order: &Order) {
        //assert!(order.exch_timestamp >= self.exch_timestamp);
        if order.exch_timestamp < self.exch_timestamp {
            println!(
                "Warning: Perhaps an inaccurate order response update occurs: an order previously \
                updated by a later exchange timestamp is updated by an earlier one. \
                This issue is primarily caused by incorrect or inconsistent timestamp ordering \
                across the files.\n \
                order={:?}, \
                response={:?}",
                &self, &order
            );
        }

        self.qty = order.qty;
        self.leaves_qty = order.leaves_qty;
        self.price_tick = order.price_tick;
        self.tick_size = order.tick_size;
        self.side = order.side;
        self.time_in_force = order.time_in_force;

        if order.exch_timestamp > 0 {
            self.exch_timestamp = order.exch_timestamp;
        }
        self.status = order.status;
        // if order.local_timestamp > 0 {
        //     self.local_timestamp = order.local_timestamp;
        // }
        self.req = order.req;
        self.exec_price_tick = order.exec_price_tick;
        self.exec_qty = order.exec_qty;
        self.order_id = order.order_id;
        self.q = order.q.clone();
        self.maker = order.maker;
        self.order_type = order.order_type;
    }
}

impl Debug for Order {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Order")
            .field("qty", &self.qty)
            .field("leaves_qty", &self.leaves_qty)
            .field("price_tick", &self.price_tick)
            .field("tick_size", &self.tick_size)
            .field("side", &self.side)
            .field("time_in_force", &self.time_in_force)
            .field("exch_timestamp", &self.exch_timestamp)
            .field("status", &self.status)
            .field("local_timestamp", &self.local_timestamp)
            .field("req", &self.req)
            .field("exec_price_tick", &self.exec_price_tick)
            .field("exec_qty", &self.exec_qty)
            .field("order_id", &self.order_id)
            .field("maker", &self.maker)
            .field("order_type", &self.order_type)
            .finish()
    }
}

impl<Context> Decode<Context> for Order {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            qty: Decode::decode(decoder)?,
            leaves_qty: Decode::decode(decoder)?,
            exec_qty: Decode::decode(decoder)?,
            exec_price_tick: Decode::decode(decoder)?,
            price_tick: Decode::decode(decoder)?,
            tick_size: Decode::decode(decoder)?,
            exch_timestamp: Decode::decode(decoder)?,
            local_timestamp: Decode::decode(decoder)?,
            order_id: Decode::decode(decoder)?,
            // In a live bot, q isn't used.
            q: Box::new(()),
            maker: Decode::decode(decoder)?,
            order_type: Decode::decode(decoder)?,
            req: Decode::decode(decoder)?,
            status: Decode::decode(decoder)?,
            side: Decode::decode(decoder)?,
            time_in_force: Decode::decode(decoder)?,
        })
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for Order {
    fn borrow_decode<D: BorrowDecoder<'de>>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            qty: Decode::decode(decoder)?,
            leaves_qty: Decode::decode(decoder)?,
            exec_qty: Decode::decode(decoder)?,
            exec_price_tick: Decode::decode(decoder)?,
            price_tick: Decode::decode(decoder)?,
            tick_size: Decode::decode(decoder)?,
            exch_timestamp: Decode::decode(decoder)?,
            local_timestamp: Decode::decode(decoder)?,
            order_id: Decode::decode(decoder)?,
            // In a live bot, q isn't used.
            q: Box::new(()),
            maker: Decode::decode(decoder)?,
            order_type: Decode::decode(decoder)?,
            req: Decode::decode(decoder)?,
            status: Decode::decode(decoder)?,
            side: Decode::decode(decoder)?,
            time_in_force: Decode::decode(decoder)?,
        })
    }
}

impl Encode for Order {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.qty.encode(encoder)?;
        self.leaves_qty.encode(encoder)?;
        self.exec_qty.encode(encoder)?;
        self.exec_price_tick.encode(encoder)?;
        self.price_tick.encode(encoder)?;
        self.tick_size.encode(encoder)?;
        self.exch_timestamp.encode(encoder)?;
        self.local_timestamp.encode(encoder)?;
        self.order_id.encode(encoder)?;
        // In a live bot, q isn't used.
        self.maker.encode(encoder)?;
        self.order_type.encode(encoder)?;
        self.req.encode(encoder)?;
        self.status.encode(encoder)?;
        self.side.encode(encoder)?;
        self.time_in_force.encode(encoder)?;
        Ok(())
    }
}
