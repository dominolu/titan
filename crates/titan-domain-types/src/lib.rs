//! Version-neutral domain types shared by market, account and backtest components.
//!
//! Strategy ABI definitions intentionally do not live here. The only executable strategy
//! contract is Strategy ABI in `titan-strategy-runtime`.

use std::{ffi::c_void, fmt, marker::PhantomData, str::FromStr, sync::Arc};

use bincode::{Decode, Encode};
use serde::{Deserialize, Deserializer, Serialize, de};

mod order;
pub use order::{AnyClone, OrdType, Order, OrderId, Side, Status, TimeInForce, get_precision};

mod live_error;
pub use live_error::{ErrorKind, LiveError, Value};

pub const BAR_COMPLETE: u64 = 1 << 0;
pub const BAR_EMPTY: u64 = 1 << 1;
pub const BAR_SYNTHETIC: u64 = 1 << 2;
pub const BAR_NATIVE: u64 = 1 << 3;
pub const BAR_PARTIAL: u64 = 1 << 4;

/// Deserializes an immutable byte buffer from either a UTF-8 string or an integer sequence.
/// Human-authored runtime TOML can use strings while existing JSON callers retain byte arrays.
pub fn deserialize_arc_bytes<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ArcBytesVisitor(PhantomData<Arc<[u8]>>);

    impl<'de> de::Visitor<'de> for ArcBytesVisitor {
        type Value = Arc<[u8]>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a UTF-8 string or a sequence of bytes")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Arc::from(value.as_bytes()))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Arc::from(value.into_bytes()))
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Arc::from(value))
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Arc::from(value))
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
            while let Some(byte) = sequence.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(Arc::from(bytes))
        }
    }

    deserializer.deserialize_any(ArcBytesVisitor(PhantomData))
}

/// Exact positive decimal unit represented as `coefficient * 10^-scale`. This is the single
/// authoritative tick/lot unit shared by Market and Account definitions.
///
/// Deserialization always goes through [`DecimalUnit::new`], so a configuration or wire payload
/// cannot install a zero unit or more than [`DecimalUnit::MAX_SCALE`] decimal places. Both the
/// `{ coefficient, scale }` table form used by runtime TOML and an exact decimal string
/// (`"0.01"`) are accepted; serialization keeps the table form so existing payloads do not change.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Encode, Decode)]
pub struct DecimalUnit {
    coefficient: u64,
    scale: u8,
}

impl<'de> Deserialize<'de> for DecimalUnit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DecimalUnitVisitor;

        impl<'de> de::Visitor<'de> for DecimalUnitVisitor {
            type Value = DecimalUnit;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a { coefficient, scale } table or an exact decimal string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                DecimalUnit::from_str(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                const FIELDS: &[&str] = &["coefficient", "scale"];
                let mut coefficient = None;
                let mut scale = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "coefficient" => {
                            if coefficient.is_some() {
                                return Err(de::Error::duplicate_field("coefficient"));
                            }
                            coefficient = Some(map.next_value::<u64>()?);
                        }
                        "scale" => {
                            if scale.is_some() {
                                return Err(de::Error::duplicate_field("scale"));
                            }
                            scale = Some(map.next_value::<u64>()?);
                        }
                        other => return Err(de::Error::unknown_field(other, FIELDS)),
                    }
                }
                let coefficient =
                    coefficient.ok_or_else(|| de::Error::missing_field("coefficient"))?;
                let scale = scale.ok_or_else(|| de::Error::missing_field("scale"))?;
                let scale = u8::try_from(scale)
                    .map_err(|_| de::Error::custom("decimal unit scale must fit in 8 bits"))?;
                DecimalUnit::new(coefficient, scale).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(DecimalUnitVisitor)
    }
}

/// Exact reduced ratio `numerator / denominator` between two [`DecimalUnit`] values.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Ratio {
    numerator: u64,
    denominator: u64,
}

impl Ratio {
    /// Reduces `numerator / denominator`; a zero denominator is refused.
    pub fn new(numerator: u64, denominator: u64) -> Option<Self> {
        if denominator == 0 {
            return None;
        }
        let divisor = gcd_u64(numerator, denominator);
        Some(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    pub const fn denominator(self) -> u64 {
        self.denominator
    }

    pub const fn is_integral(self) -> bool {
        self.denominator == 1
    }

    /// The integer quotient when the ratio is integral; `None` for a fractional ratio.
    pub fn as_integral_u64(self) -> Option<u64> {
        self.is_integral().then_some(self.numerator)
    }
}

fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left.max(1)
}
/// Wire projection for authoritative observations; input parameters keep their numeric types.
pub fn json_numbers_as_strings(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(json_numbers_as_strings).collect())
        }
        Value::Object(items) => Value::Object(
            items
                .into_iter()
                .map(|(k, v)| (k, json_numbers_as_strings(v)))
                .collect(),
        ),
        other => other,
    }
}

impl DecimalUnit {
    pub const MAX_SCALE: u8 = 18;

    pub fn new(coefficient: u64, scale: u8) -> Result<Self, &'static str> {
        if coefficient == 0 || scale > Self::MAX_SCALE {
            return Err("decimal unit must be positive and have at most 18 decimal places");
        }
        let mut coefficient = coefficient;
        let mut scale = scale;
        while scale > 0 && coefficient.is_multiple_of(10) {
            coefficient /= 10;
            scale -= 1;
        }
        Ok(Self { coefficient, scale })
    }

    pub const fn coefficient(self) -> u64 {
        self.coefficient
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub fn as_f64(self) -> f64 {
        self.coefficient as f64 / 10_f64.powi(i32::from(self.scale))
    }

    /// Exact product of two units. Returns `None` when the product needs more than
    /// [`DecimalUnit::MAX_SCALE`] decimal places or its coefficient does not fit in `u64`.
    ///
    /// Approximating the product is never allowed: an inexact tick/lot silently changes both the
    /// strategy's arithmetic and the admission identity.
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        let mut coefficient = u128::from(self.coefficient) * u128::from(rhs.coefficient);
        let mut scale = u16::from(self.scale) + u16::from(rhs.scale);
        while scale > u16::from(Self::MAX_SCALE) {
            if !coefficient.is_multiple_of(10) {
                return None;
            }
            coefficient /= 10;
            scale -= 1;
        }
        let coefficient = u64::try_from(coefficient).ok()?;
        Self::new(coefficient, scale as u8).ok()
    }

    /// Exact quotient `self / rhs` as a reduced ratio. A zero right side or an unrepresentable
    /// ratio returns `None`.
    pub fn ratio_to(self, rhs: Self) -> Option<Ratio> {
        let mut numerator = u128::from(self.coefficient);
        let mut denominator = u128::from(rhs.coefficient);
        let exponent = i32::from(rhs.scale) - i32::from(self.scale);
        let factor = 10_u128.checked_pow(exponent.unsigned_abs())?;
        if exponent >= 0 {
            numerator = numerator.checked_mul(factor)?;
        } else {
            denominator = denominator.checked_mul(factor)?;
        }
        Ratio::new(
            u64::try_from(numerator).ok()?,
            u64::try_from(denominator).ok()?,
        )
    }

    /// `ceil(self / denominator)` as an exact integer count (used for minimum-lot admission).
    pub fn ceil_div_by(self, denominator: Self) -> Option<u64> {
        let ratio = self.ratio_to(denominator)?;
        let sum = u128::from(ratio.numerator()).checked_add(u128::from(ratio.denominator()) - 1)?;
        u64::try_from(sum / u128::from(ratio.denominator())).ok()
    }
}

impl fmt::Display for DecimalUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.coefficient);
        }
        let digits = self.coefficient.to_string();
        let scale = usize::from(self.scale);
        if digits.len() <= scale {
            write!(f, "0.{:0>width$}", digits, width = scale)
        } else {
            let split = digits.len() - scale;
            write!(f, "{}.{}", &digits[..split], &digits[split..])
        }
    }
}

impl FromStr for DecimalUnit {
    type Err = &'static str;
    /// Parses a plain decimal (`"0.01"`) or an exact exponent form (`"1e-7"`, `"2.5E-8"`).
    ///
    /// Exchange metadata sometimes publishes steps in scientific notation. Parsing it here keeps a
    /// single exact decimal path: the value is never routed through `f64`.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        if input.is_empty() || input.starts_with('-') || input.starts_with('+') {
            return Err("invalid positive decimal unit");
        }
        let (mantissa, exponent) = match input.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => {
                let exponent = exponent
                    .parse::<i32>()
                    .map_err(|_| "invalid positive decimal unit")?;
                (mantissa, exponent)
            }
            None => (input, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty()
            || fraction.len() > usize::from(Self::MAX_SCALE)
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return Err("invalid positive decimal unit");
        }
        let digits = format!("{whole}{fraction}");
        let coefficient = digits.parse::<u64>().map_err(|_| "decimal unit overflow")?;
        let scale = i64::try_from(fraction.len()).map_err(|_| "decimal unit overflow")?
            - i64::from(exponent);
        if scale < 0 {
            let factor = 10_u64
                .checked_pow(u32::try_from(-scale).map_err(|_| "decimal unit overflow")?)
                .ok_or("decimal unit overflow")?;
            let coefficient = coefficient
                .checked_mul(factor)
                .ok_or("decimal unit overflow")?;
            return Self::new(coefficient, 0);
        }
        let scale = u8::try_from(scale).map_err(|_| "invalid positive decimal unit")?;
        Self::new(coefficient, scale)
    }
}

/// Canonical closed OHLCV bar shared by engine, Runtime and Numba.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
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

impl Default for Bar {
    fn default() -> Self {
        Self {
            open_ts: 0,
            close_ts: 0,
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            volume: 0.0,
            quote_volume: 0.0,
            buy_volume: 0.0,
            trade_count: 0,
            flags: 0,
        }
    }
}

impl Bar {
    #[inline(always)]
    pub fn is_complete(&self) -> bool {
        self.flags & BAR_COMPLETE != 0 && self.flags & BAR_PARTIAL == 0
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.flags & BAR_EMPTY != 0
    }
}

/// Flat fill payload suitable for zero-copy foreign access.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct FillEvent {
    pub asset_no: u64,
    pub local_account_no: u32,
    pub _account_reserved: u32,
    /// Account stream epoch that scopes `sequence` and permits version restart after reconnect.
    pub account_epoch: u64,
    pub order_id: u64,
    pub venue_order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub sequence: u64,
    pub price: f64,
    /// Quantity contributed by this canonical fill fact.
    pub last_fill_qty: f64,
    /// Order cumulative filled quantity after applying this fill.
    pub cumulative_filled_qty: f64,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub reason: u32,
    pub side: i8,
    pub maker: u8,
    pub _reserved: [u8; 2],
}

/// Flat order-response payload. Partial fills are not collapsed into snapshots.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderEvent {
    pub asset_no: u64,
    pub local_account_no: u32,
    pub _account_reserved: u32,
    pub order_id: u64,
    pub venue_order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub sequence: u64,
    pub price: f64,
    pub qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub reason: u32,
    pub side: i8,
    pub status: u8,
    pub request: u8,
    pub maker: u8,
    pub _reserved: [u8; 4],
}

/// One depth level inside a callback-scoped [`DepthBatchEvent`].
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct DepthItemEvent {
    pub price: f64,
    pub qty: f64,
    pub side: u8,
    pub action: u8,
    pub _reserved: [u8; 6],
}

/// Full canonical depth envelope. Unlike the legacy Tick projection this preserves the
/// provider epoch, sequence range, snapshot flags and per-level actions.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct DepthBatchEvent {
    pub asset_no: u64,
    pub market_no: u32,
    pub kind: u32,
    pub flags: u32,
    pub stream_epoch: u64,
    pub first_update_sequence: u64,
    pub last_update_sequence: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub items_ptr: *const DepthItemEvent,
    pub num_items: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct PositionEvent {
    pub asset_no: u64,
    pub local_account_no: u32,
    pub margin_currency_id: u32,
    pub account_epoch: u64,
    pub sequence: u64,
    pub quantity: f64,
    pub entry_price: f64,
    pub liquidation_price: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub position_side: u8,
    pub margin_type: u8,
    pub _reserved: [u8; 6],
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct BalanceEvent {
    pub local_account_no: u32,
    pub currency_id: u32,
    pub account_epoch: u64,
    pub sequence: u64,
    pub wallet: f64,
    pub available: f64,
    pub margin: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct AccountStateEvent {
    pub local_account_no: u32,
    pub reason: u32,
    pub account_epoch: u64,
    pub sequence: u64,
    pub terminal_version: u64,
    pub kind: u32,
    pub flags: u32,
    pub state: u8,
    pub success: u8,
    pub scope: u8,
    pub _reserved: [u8; 5],
}

/// Read-only top-of-book state refreshed before every market callback.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct MarketState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_qty: f64,
    pub best_ask_qty: f64,
    pub tick_size: f64,
    pub lot_size: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct BarItem {
    pub asset_no: u64,
    pub bar: Bar,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct TimedBarItem {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bar: Bar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RuntimeTimer {
    pub deadline_ts: i64,
    pub owner_id: u64,
    pub timer_id: u64,
}

/// Read-only ring metadata exposed to a callback for its duration.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BarHistoryView {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bars_ptr: *const Bar,
    pub capacity: usize,
    pub len: usize,
    pub next: usize,
}

/// Canonical feed event. Its explicit 64-byte alignment is part of the ABI.
#[repr(C, align(64))]
#[derive(Clone, PartialEq, Debug, Decode, Encode)]
pub struct Event {
    pub ev: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub px: f64,
    pub qty: f64,
    pub order_id: u64,
    pub ival: i64,
    pub fval: f64,
}

impl Event {
    #[inline(always)]
    pub fn is(&self, event: u64) -> bool {
        if (self.ev & event) != event {
            false
        } else {
            let event_kind = event & 0xff;
            event_kind == 0 || self.ev & 0xff == event_kind
        }
    }
}

/// Indicates a buy, with specific meaning that can vary depending on the situation. For example,
/// when combined with a depth event, it means a bid-side event, while when combined with a trade
/// event, it means that the trade initiator is a buyer.
pub const BUY_EVENT: u64 = 1 << 29;

/// Indicates a sell, with specific meaning that can vary depending on the situation. For example,
/// when combined with a depth event, it means an ask-side event, while when combined with a trade
/// event, it means that the trade initiator is a seller.
pub const SELL_EVENT: u64 = 1 << 28;

/// Indicates that the market depth is changed.
pub const DEPTH_EVENT: u64 = 1;

/// Indicates that a trade occurs in the market.
pub const TRADE_EVENT: u64 = 2;

/// Indicates that the market depth is cleared.
pub const DEPTH_CLEAR_EVENT: u64 = 3;

/// Indicates that the market depth snapshot is received.
pub const DEPTH_SNAPSHOT_EVENT: u64 = 4;

/// Indicates that the best bid and best ask update event is received.
pub const DEPTH_BBO_EVENT: u64 = 5;

/// Timestamp flag marking an event whose time is the local (receiving) clock.
pub const LOCAL_EVENT: u64 = 1 << 30;

/// [`DEPTH_CLEAR_EVENT`] and [`LOCAL_EVENT`]: the local depth image is cleared.
pub const LOCAL_DEPTH_CLEAR_EVENT: u64 = DEPTH_CLEAR_EVENT | LOCAL_EVENT;

/// [`DEPTH_EVENT`], [`BUY_EVENT`] and [`LOCAL_EVENT`]: a bid-side depth change.
pub const LOCAL_BID_DEPTH_EVENT: u64 = DEPTH_EVENT | BUY_EVENT | LOCAL_EVENT;

/// [`DEPTH_EVENT`], [`SELL_EVENT`] and [`LOCAL_EVENT`]: an ask-side depth change.
pub const LOCAL_ASK_DEPTH_EVENT: u64 = DEPTH_EVENT | SELL_EVENT | LOCAL_EVENT;

/// [`DEPTH_SNAPSHOT_EVENT`], [`BUY_EVENT`] and [`LOCAL_EVENT`]: a bid-side snapshot level.
pub const LOCAL_BID_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | BUY_EVENT | LOCAL_EVENT;

/// [`DEPTH_SNAPSHOT_EVENT`], [`SELL_EVENT`] and [`LOCAL_EVENT`]: an ask-side snapshot level.
pub const LOCAL_ASK_DEPTH_SNAPSHOT_EVENT: u64 = DEPTH_SNAPSHOT_EVENT | SELL_EVENT | LOCAL_EVENT;

/// [`DEPTH_BBO_EVENT`], [`BUY_EVENT`] and [`LOCAL_EVENT`]: a bid-side best-quote update.
pub const LOCAL_BID_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | BUY_EVENT | LOCAL_EVENT;

/// [`DEPTH_BBO_EVENT`], [`SELL_EVENT`] and [`LOCAL_EVENT`]: an ask-side best-quote update.
pub const LOCAL_ASK_DEPTH_BBO_EVENT: u64 = DEPTH_BBO_EVENT | SELL_EVENT | LOCAL_EVENT;

/// [`TRADE_EVENT`] and [`LOCAL_EVENT`].
pub const LOCAL_TRADE_EVENT: u64 = TRADE_EVENT | LOCAL_EVENT;

/// [`LOCAL_TRADE_EVENT`] and [`BUY_EVENT`]: a trade whose initiator is a buyer.
pub const LOCAL_BUY_TRADE_EVENT: u64 = LOCAL_TRADE_EVENT | BUY_EVENT;

/// [`LOCAL_TRADE_EVENT`] and [`SELL_EVENT`]: a trade whose initiator is a seller.
pub const LOCAL_SELL_TRADE_EVENT: u64 = LOCAL_TRADE_EVENT | SELL_EVENT;

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct TickItem {
    pub asset_no: u64,
    pub event: Event,
}

pub const ORDER_COMMAND_SUBMIT: u8 = 1;
pub const ORDER_COMMAND_CANCEL: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderCommand {
    pub kind: u8,
    pub side: i8,
    pub time_in_force: u8,
    pub order_type: u8,
    pub _reserved: [u8; 4],
    pub local_account_no: u32,
    pub _account_reserved: u32,
    pub asset_no: u64,
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
    pub trigger_price: f64,
    pub gtd_expiry_ts: i64,
}

impl Default for OrderCommand {
    fn default() -> Self {
        Self {
            kind: 0,
            side: 0,
            time_in_force: 0,
            order_type: 0,
            _reserved: [0; 4],
            local_account_no: 0,
            _account_reserved: 0,
            asset_no: 0,
            order_id: 0,
            price: 0.0,
            qty: 0.0,
            trigger_price: 0.0,
            gtd_expiry_ts: 0,
        }
    }
}

/// Fixed-layout request copied by the host before a direct submit task is spawned.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct AbiNewOrderRequest {
    pub asset_no: u64,
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
    pub side: i8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub _reserved: [u8; 5],
}

impl Default for AbiNewOrderRequest {
    fn default() -> Self {
        Self {
            asset_no: 0,
            order_id: 0,
            price: 0.0,
            qty: 0.0,
            side: 0,
            order_type: 0,
            time_in_force: 0,
            _reserved: [0; 5],
        }
    }
}

/// Fixed-layout request copied by the host before a direct cancel task is spawned.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[repr(C)]
pub struct AbiCancelOrderRequest {
    pub asset_no: u64,
    pub order_id: u64,
}

pub type AbiExecutionFn = unsafe extern "C" fn(
    context: *mut c_void,
    account_no: u32,
    request: *const c_void,
    task_id_out: *mut u64,
) -> i32;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct RuntimeFunding {
    pub event_id: u64,
    pub asset_no: u32,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub currency: u32,
    pub price_source: u32,
    pub position_snapshot: u8,
    pub formula: u8,
    pub rounding_mode: u8,
    pub boundary: u8,
    pub publication_ts: i64,
    pub effective_ts: i64,
    pub settlement_ts: i64,
    pub delivery_ts: i64,
    pub rate: f64,
    pub mark_price: f64,
    pub position_qty: f64,
    pub amount: f64,
    pub rounding_increment: f64,
}

impl Default for RuntimeFunding {
    fn default() -> Self {
        Self {
            event_id: 0,
            asset_no: 0,
            venue_no: 0,
            instrument_id: 0,
            currency: 0,
            price_source: 0,
            position_snapshot: 0,
            formula: 0,
            rounding_mode: 0,
            boundary: 0,
            publication_ts: 0,
            effective_ts: 0,
            settlement_ts: 0,
            delivery_ts: 0,
            rate: 0.0,
            mark_price: 0.0,
            position_qty: 0.0,
            amount: 0.0,
            rounding_increment: 1e-12,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_unit_deserialization_normalizes_and_rejects_invalid_values() {
        let unit: DecimalUnit = serde_json::from_str(r#"{"coefficient":10,"scale":1}"#).unwrap();
        assert_eq!(unit, DecimalUnit::new(1, 0).unwrap());
        assert_eq!(unit.coefficient(), 1);
        assert_eq!(unit.scale(), 0);

        let from_string: DecimalUnit = serde_json::from_str(r#""0.01""#).unwrap();
        assert_eq!(from_string, DecimalUnit::new(1, 2).unwrap());
        // Exchange metadata publishes steps in scientific notation; parse it exactly.
        assert_eq!(
            "1e-7".parse::<DecimalUnit>(),
            Ok(DecimalUnit::new(1, 7).unwrap())
        );
        assert_eq!(
            "2.5E-8".parse::<DecimalUnit>(),
            Ok(DecimalUnit::new(25, 9).unwrap())
        );
        assert_eq!(
            "1e2".parse::<DecimalUnit>(),
            Ok(DecimalUnit::new(100, 0).unwrap())
        );
        assert!("1e".parse::<DecimalUnit>().is_err());
        assert!("1e-19".parse::<DecimalUnit>().is_err());

        assert!(serde_json::from_str::<DecimalUnit>(r#"{"coefficient":0,"scale":0}"#).is_err());
        assert!(serde_json::from_str::<DecimalUnit>(r#"{"coefficient":1,"scale":19}"#).is_err());
        assert!(
            serde_json::from_str::<DecimalUnit>(r#"{"coefficient":1,"scale":0,"extra":1}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<DecimalUnit>(r#"{"coefficient":1}"#).is_err());
        assert!(serde_json::from_str::<DecimalUnit>(r#""0""#).is_err());
        assert!(serde_json::from_str::<DecimalUnit>(r#""-0.01""#).is_err());
    }

    #[test]
    fn decimal_unit_serialization_keeps_the_table_form() {
        let unit = DecimalUnit::new(25, 2).unwrap();
        assert_eq!(
            serde_json::to_string(&unit).unwrap(),
            r#"{"coefficient":25,"scale":2}"#
        );
    }

    #[test]
    fn checked_multiplication_is_exact_or_refuses() {
        let price_tick = DecimalUnit::new(1, 2).unwrap();
        let base_delta = DecimalUnit::new(1, 3).unwrap();
        assert_eq!(
            price_tick.checked_mul(base_delta),
            Some(DecimalUnit::new(1, 5).unwrap())
        );
        // 0.0000000001 * 0.0000000001 needs 20 decimal places and must not be rounded.
        let tiny = DecimalUnit::new(1, 10).unwrap();
        assert_eq!(tiny.checked_mul(tiny), None);
        // A product that overflows u64 after normalization is refused.
        let large = DecimalUnit::new(u64::MAX, 0).unwrap();
        assert_eq!(large.checked_mul(large), None);
    }

    #[test]
    fn ratio_and_ceiling_division_are_exact() {
        let maker_lot = DecimalUnit::new(1, 2).unwrap();
        let hedge_lot = DecimalUnit::new(1, 3).unwrap();
        let ratio = maker_lot.ratio_to(hedge_lot).unwrap();
        assert_eq!((ratio.numerator(), ratio.denominator()), (10, 1));
        assert!(ratio.is_integral());
        assert_eq!(ratio.as_integral_u64(), Some(10));

        let reversed = hedge_lot.ratio_to(maker_lot).unwrap();
        assert_eq!((reversed.numerator(), reversed.denominator()), (1, 10));
        assert!(!reversed.is_integral());

        let min_quantity = DecimalUnit::new(15, 1).unwrap();
        // 1.5 / 0.01 = 150 whole lots, rounded up.
        assert_eq!(min_quantity.ceil_div_by(maker_lot), Some(150));
        let partial = DecimalUnit::new(11, 1).unwrap();
        assert_eq!(
            partial.ceil_div_by(DecimalUnit::new(1, 0).unwrap()),
            Some(2)
        );
        assert_eq!(
            maker_lot
                .ratio_to(DecimalUnit::new(1, 0).unwrap())
                .unwrap()
                .numerator(),
            1
        );
    }
}
