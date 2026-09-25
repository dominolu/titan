//! Version-neutral domain types shared by market, account and backtest components.
//!
//! Strategy ABI definitions intentionally do not live here. The only executable strategy
//! contract is ABI V13 in `titan-strategy-runtime`.

use std::{ffi::c_void, fmt, marker::PhantomData, str::FromStr, sync::Arc};

use bincode::{Decode, Encode};
use serde::{Deserialize, Deserializer, Serialize, de};

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
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DecimalUnit {
    coefficient: u64,
    scale: u8,
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
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        if input.is_empty() || input.starts_with('-') || input.starts_with('+') {
            return Err("invalid positive decimal unit");
        }
        let (whole, fraction) = input.split_once('.').unwrap_or((input, ""));
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
        Self::new(coefficient, fraction.len() as u8)
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
