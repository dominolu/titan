//! Stable metadata for the Rust/Numba strategy ABI.
//!
//! This crate deliberately starts with the versioned descriptor and event IDs. Pure
//! `repr(C)` payloads are migrated here before the event loop moves to `titan-runtime`,
//! which breaks the current engine/runtime dependency cycle without creating a second
//! executable runtime.

use std::ffi::c_void;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

pub const STRATEGY_ABI_VERSION: u32 = 9;
pub const EVENT_SLOT_COUNT: usize = 32;

pub const BAR_COMPLETE: u64 = 1 << 0;
pub const BAR_EMPTY: u64 = 1 << 1;
pub const BAR_SYNTHETIC: u64 = 1 << 2;
pub const BAR_NATIVE: u64 = 1 << 3;
pub const BAR_PARTIAL: u64 = 1 << 4;

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

/// Context passed as the single strategy callback argument.
#[derive(Debug)]
#[repr(C)]
pub struct StrategyRuntimeContext {
    pub abi_version: u32,
    pub struct_size: u32,
    pub event_kind: u32,
    pub stop_requested: u32,
    pub now: i64,
    pub generation: u64,
    pub user_data: *mut c_void,
    pub bot_ptr: *mut c_void,
    pub ticks_ptr: *const TickItem,
    pub num_ticks: usize,
    pub bars_ptr: *const BarItem,
    pub num_bars: usize,
    pub bar_timeframe_ns: i64,
    pub bar_close_ts: i64,
    pub fills_ptr: *const FillEvent,
    pub num_fills: usize,
    pub orders_ptr: *const OrderEvent,
    pub num_orders: usize,
    pub histories_ptr: *const BarHistoryView,
    pub num_histories: usize,
    pub payload_ptr: *const c_void,
    pub payload_len: usize,
    pub state_f64_ptr: *mut f64,
    pub state_f64_len: usize,
    pub state_i64_ptr: *mut i64,
    pub state_i64_len: usize,
    pub commands_ptr: *mut OrderCommand,
    pub num_commands: usize,
    pub command_capacity: usize,
    pub positions_ptr: *const f64,
    pub num_positions: usize,
    pub markets_ptr: *const MarketState,
    pub num_markets: usize,
    pub last_error: i64,
}

impl Default for StrategyRuntimeContext {
    fn default() -> Self {
        Self {
            abi_version: STRATEGY_ABI_VERSION,
            struct_size: size_of::<Self>() as u32,
            event_kind: StrategyEventKind::Start as u32,
            stop_requested: 0,
            now: 0,
            generation: 0,
            user_data: std::ptr::null_mut(),
            bot_ptr: std::ptr::null_mut(),
            ticks_ptr: std::ptr::null(),
            num_ticks: 0,
            bars_ptr: std::ptr::null(),
            num_bars: 0,
            bar_timeframe_ns: 0,
            bar_close_ts: 0,
            fills_ptr: std::ptr::null(),
            num_fills: 0,
            orders_ptr: std::ptr::null(),
            num_orders: 0,
            histories_ptr: std::ptr::null(),
            num_histories: 0,
            payload_ptr: std::ptr::null(),
            payload_len: 0,
            state_f64_ptr: std::ptr::null_mut(),
            state_f64_len: 0,
            state_i64_ptr: std::ptr::null_mut(),
            state_i64_len: 0,
            commands_ptr: std::ptr::null_mut(),
            num_commands: 0,
            command_capacity: 0,
            positions_ptr: std::ptr::null(),
            num_positions: 0,
            markets_ptr: std::ptr::null(),
            num_markets: 0,
            last_error: 0,
        }
    }
}

impl StrategyRuntimeContext {
    #[inline]
    pub fn request_stop(&mut self) {
        self.stop_requested = 1;
    }

    /// Invalidates callback-scoped borrowed payload views.
    pub fn clear_views(&mut self) {
        self.ticks_ptr = std::ptr::null();
        self.num_ticks = 0;
        self.bars_ptr = std::ptr::null();
        self.num_bars = 0;
        self.bar_timeframe_ns = 0;
        self.bar_close_ts = 0;
        self.fills_ptr = std::ptr::null();
        self.num_fills = 0;
        self.orders_ptr = std::ptr::null();
        self.num_orders = 0;
        self.payload_ptr = std::ptr::null();
        self.payload_len = 0;
    }
}

/// Stable callback identifiers. Existing numeric values are ABI and log commitments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum StrategyEventKind {
    Start = 0,
    Order = 1,
    Filled = 2,
    Position = 3,
    Funding = 4,
    Bar = 5,
    Tick = 6,
    Timer = 7,
    Error = 8,
    Stop = 9,
}

impl StrategyEventKind {
    #[inline(always)]
    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Builds the complete native strategy ABI descriptor from Rust's actual layouts.
/// No offset or size is duplicated in Python or maintained by hand.
pub fn runtime_abi_descriptor() -> RuntimeAbiDescriptor {
    use std::mem::{align_of, offset_of, size_of};

    macro_rules! field {
        ($struct:ty, $name:ident, $kind:expr, $ty:ty) => {
            AbiField::new(
                stringify!($name),
                $kind,
                offset_of!($struct, $name),
                size_of::<$ty>(),
                align_of::<$ty>(),
            )
        };
    }
    macro_rules! abi_struct {
        ($type:ty, $($name:ident: $kind:expr => $field_type:ty),+ $(,)?) => {
            AbiStruct::new::<$type>(
                stringify!($type),
                vec![$(field!($type, $name, $kind, $field_type)),+],
            )
        };
    }

    RuntimeAbiDescriptor::new(vec![
        abi_struct!(Bar,
            open_ts: AbiType::I64 => i64, close_ts: AbiType::I64 => i64,
            open: AbiType::F64 => f64, high: AbiType::F64 => f64,
            low: AbiType::F64 => f64, close: AbiType::F64 => f64,
            volume: AbiType::F64 => f64, quote_volume: AbiType::F64 => f64,
            buy_volume: AbiType::F64 => f64, trade_count: AbiType::U64 => u64,
            flags: AbiType::U64 => u64,
        ),
        abi_struct!(Event,
            ev: AbiType::U64 => u64, exch_ts: AbiType::I64 => i64,
            local_ts: AbiType::I64 => i64, px: AbiType::F64 => f64,
            qty: AbiType::F64 => f64, order_id: AbiType::U64 => u64,
            ival: AbiType::I64 => i64, fval: AbiType::F64 => f64,
        ),
        abi_struct!(FillEvent,
            asset_no: AbiType::U64 => u64, order_id: AbiType::U64 => u64,
            venue_order_id: AbiType::U64 => u64, exch_ts: AbiType::I64 => i64,
            local_ts: AbiType::I64 => i64, sequence: AbiType::U64 => u64,
            price: AbiType::F64 => f64,
            last_fill_qty: AbiType::F64 => f64,
            cumulative_filled_qty: AbiType::F64 => f64,
            venue_no: AbiType::U32 => u32, instrument_id: AbiType::U32 => u32,
            reason: AbiType::U32 => u32, side: AbiType::I8 => i8,
            maker: AbiType::U8 => u8,
            _reserved: AbiType::Array { element: Box::new(AbiType::U8), len: 2 } => [u8; 2],
        ),
        abi_struct!(OrderEvent,
            asset_no: AbiType::U64 => u64, order_id: AbiType::U64 => u64,
            venue_order_id: AbiType::U64 => u64, exch_ts: AbiType::I64 => i64,
            local_ts: AbiType::I64 => i64, sequence: AbiType::U64 => u64,
            price: AbiType::F64 => f64, qty: AbiType::F64 => f64,
            exec_price: AbiType::F64 => f64, exec_qty: AbiType::F64 => f64,
            venue_no: AbiType::U32 => u32, instrument_id: AbiType::U32 => u32,
            reason: AbiType::U32 => u32, side: AbiType::I8 => i8,
            status: AbiType::U8 => u8, request: AbiType::U8 => u8,
            maker: AbiType::U8 => u8,
            _reserved: AbiType::Array { element: Box::new(AbiType::U8), len: 4 } => [u8; 4],
        ),
        abi_struct!(MarketState,
            best_bid: AbiType::F64 => f64, best_ask: AbiType::F64 => f64,
            best_bid_qty: AbiType::F64 => f64, best_ask_qty: AbiType::F64 => f64,
            tick_size: AbiType::F64 => f64, lot_size: AbiType::F64 => f64,
        ),
        abi_struct!(BarItem,
            asset_no: AbiType::U64 => u64, bar: AbiType::Struct("Bar".into()) => Bar,
        ),
        abi_struct!(TimedBarItem,
            asset_no: AbiType::U64 => u64, timeframe_ns: AbiType::I64 => i64,
            bar: AbiType::Struct("Bar".into()) => Bar,
        ),
        abi_struct!(RuntimeTimer,
            deadline_ts: AbiType::I64 => i64, owner_id: AbiType::U64 => u64,
            timer_id: AbiType::U64 => u64,
        ),
        abi_struct!(BarHistoryView,
            asset_no: AbiType::U64 => u64, timeframe_ns: AbiType::I64 => i64,
            bars_ptr: AbiType::Pointer => *const Bar, capacity: AbiType::Usize => usize,
            len: AbiType::Usize => usize, next: AbiType::Usize => usize,
        ),
        abi_struct!(TickItem,
            asset_no: AbiType::U64 => u64, event: AbiType::Struct("Event".into()) => Event,
        ),
        abi_struct!(OrderCommand,
            kind: AbiType::U8 => u8, side: AbiType::I8 => i8,
            time_in_force: AbiType::U8 => u8, order_type: AbiType::U8 => u8,
            _reserved: AbiType::Array { element: Box::new(AbiType::U8), len: 4 } => [u8; 4],
            local_account_no: AbiType::U32 => u32,
            _account_reserved: AbiType::U32 => u32,
            asset_no: AbiType::U64 => u64, order_id: AbiType::U64 => u64,
            price: AbiType::F64 => f64, qty: AbiType::F64 => f64,
            trigger_price: AbiType::F64 => f64, gtd_expiry_ts: AbiType::I64 => i64,
        ),
        abi_struct!(RuntimeFunding,
            event_id: AbiType::U64 => u64, asset_no: AbiType::U32 => u32,
            venue_no: AbiType::U32 => u32, instrument_id: AbiType::U32 => u32,
            currency: AbiType::U32 => u32, price_source: AbiType::U32 => u32,
            position_snapshot: AbiType::U8 => u8, formula: AbiType::U8 => u8,
            rounding_mode: AbiType::U8 => u8, boundary: AbiType::U8 => u8,
            publication_ts: AbiType::I64 => i64, effective_ts: AbiType::I64 => i64,
            settlement_ts: AbiType::I64 => i64, delivery_ts: AbiType::I64 => i64,
            rate: AbiType::F64 => f64, mark_price: AbiType::F64 => f64,
            position_qty: AbiType::F64 => f64, amount: AbiType::F64 => f64,
            rounding_increment: AbiType::F64 => f64,
        ),
        abi_struct!(StrategyRuntimeContext,
            abi_version: AbiType::U32 => u32, struct_size: AbiType::U32 => u32,
            event_kind: AbiType::U32 => u32, stop_requested: AbiType::U32 => u32,
            now: AbiType::I64 => i64, generation: AbiType::U64 => u64,
            user_data: AbiType::Pointer => *mut c_void, bot_ptr: AbiType::Pointer => *mut c_void,
            ticks_ptr: AbiType::Pointer => *const TickItem, num_ticks: AbiType::Usize => usize,
            bars_ptr: AbiType::Pointer => *const BarItem, num_bars: AbiType::Usize => usize,
            bar_timeframe_ns: AbiType::I64 => i64, bar_close_ts: AbiType::I64 => i64,
            fills_ptr: AbiType::Pointer => *const FillEvent, num_fills: AbiType::Usize => usize,
            orders_ptr: AbiType::Pointer => *const OrderEvent, num_orders: AbiType::Usize => usize,
            histories_ptr: AbiType::Pointer => *const BarHistoryView,
            num_histories: AbiType::Usize => usize, payload_ptr: AbiType::Pointer => *const c_void,
            payload_len: AbiType::Usize => usize, state_f64_ptr: AbiType::Pointer => *mut f64,
            state_f64_len: AbiType::Usize => usize, state_i64_ptr: AbiType::Pointer => *mut i64,
            state_i64_len: AbiType::Usize => usize, commands_ptr: AbiType::Pointer => *mut OrderCommand,
            num_commands: AbiType::Usize => usize, command_capacity: AbiType::Usize => usize,
            positions_ptr: AbiType::Pointer => *const f64, num_positions: AbiType::Usize => usize,
            markets_ptr: AbiType::Pointer => *const MarketState, num_markets: AbiType::Usize => usize,
            last_error: AbiType::I64 => i64,
        ),
    ])
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbiType {
    U8,
    I8,
    U32,
    I32,
    U64,
    I64,
    F64,
    Usize,
    Pointer,
    Array { element: Box<AbiType>, len: u64 },
    Struct(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiField {
    pub name: String,
    pub kind: AbiType,
    pub offset: u64,
    pub size: u64,
    pub alignment: u64,
}

impl AbiField {
    pub fn new(
        name: impl Into<String>,
        kind: AbiType,
        offset: usize,
        size: usize,
        alignment: usize,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            offset: offset as u64,
            size: size as u64,
            alignment: alignment as u64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiStruct {
    pub name: String,
    pub size: u64,
    pub alignment: u64,
    pub fields: Vec<AbiField>,
}

impl AbiStruct {
    pub fn new<T>(name: impl Into<String>, fields: Vec<AbiField>) -> Self {
        Self {
            name: name.into(),
            size: size_of::<T>() as u64,
            alignment: align_of::<T>() as u64,
            fields,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiEventSlot {
    pub name: String,
    pub id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAbiDescriptor {
    pub abi_version: u32,
    pub event_slot_count: u64,
    pub pointer_width: u8,
    pub little_endian: bool,
    pub event_slots: Vec<AbiEventSlot>,
    pub structs: Vec<AbiStruct>,
    /// Stable FNV-1a digest of every compatibility-relevant field above.
    pub fingerprint: String,
}

impl RuntimeAbiDescriptor {
    pub fn new(structs: Vec<AbiStruct>) -> Self {
        let mut descriptor = Self {
            abi_version: STRATEGY_ABI_VERSION,
            event_slot_count: EVENT_SLOT_COUNT as u64,
            pointer_width: usize::BITS as u8,
            little_endian: cfg!(target_endian = "little"),
            event_slots: vec![
                event_slot("start", StrategyEventKind::Start),
                event_slot("order", StrategyEventKind::Order),
                event_slot("filled", StrategyEventKind::Filled),
                event_slot("position", StrategyEventKind::Position),
                event_slot("funding", StrategyEventKind::Funding),
                event_slot("bar", StrategyEventKind::Bar),
                event_slot("tick", StrategyEventKind::Tick),
                event_slot("timer", StrategyEventKind::Timer),
                event_slot("error", StrategyEventKind::Error),
                event_slot("stop", StrategyEventKind::Stop),
            ],
            structs,
            fingerprint: String::new(),
        };
        descriptor.fingerprint = format!("fnv1a64:{:016x}", descriptor.calculate_fingerprint());
        descriptor
    }

    pub fn verify_fingerprint(&self) -> bool {
        self.fingerprint == format!("fnv1a64:{:016x}", self.calculate_fingerprint())
    }

    fn calculate_fingerprint(&self) -> u64 {
        let mut digest = Fnv1a64::new();
        digest.u32(self.abi_version);
        digest.u64(self.event_slot_count);
        digest.u8(self.pointer_width);
        digest.u8(u8::from(self.little_endian));
        for slot in &self.event_slots {
            digest.string(&slot.name);
            digest.u32(slot.id);
        }
        for item in &self.structs {
            digest.string(&item.name);
            digest.u64(item.size);
            digest.u64(item.alignment);
            for field in &item.fields {
                digest.string(&field.name);
                digest.abi_type(&field.kind);
                digest.u64(field.offset);
                digest.u64(field.size);
                digest.u64(field.alignment);
            }
        }
        digest.finish()
    }
}

fn event_slot(name: &str, kind: StrategyEventKind) -> AbiEventSlot {
    AbiEventSlot {
        name: name.to_owned(),
        id: kind as u32,
    }
}

struct Fnv1a64(u64);

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn string(&mut self, value: &str) {
        self.u64(value.len() as u64);
        self.bytes(value.as_bytes());
    }

    fn abi_type(&mut self, value: &AbiType) {
        match value {
            AbiType::U8 => self.u8(0),
            AbiType::I8 => self.u8(1),
            AbiType::U32 => self.u8(2),
            AbiType::I32 => self.u8(3),
            AbiType::U64 => self.u8(4),
            AbiType::I64 => self.u8(5),
            AbiType::F64 => self.u8(6),
            AbiType::Usize => self.u8(7),
            AbiType::Pointer => self.u8(8),
            AbiType::Array { element, len } => {
                self.u8(9);
                self.abi_type(element);
                self.u64(*len);
            }
            AbiType::Struct(name) => {
                self.u8(10);
                self.string(name);
            }
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    struct Example {
        first: u32,
        second: i64,
    }

    fn descriptor() -> RuntimeAbiDescriptor {
        RuntimeAbiDescriptor::new(vec![AbiStruct::new::<Example>(
            "Example",
            vec![
                AbiField::new(
                    "first",
                    AbiType::U32,
                    std::mem::offset_of!(Example, first),
                    size_of::<u32>(),
                    align_of::<u32>(),
                ),
                AbiField::new(
                    "second",
                    AbiType::I64,
                    std::mem::offset_of!(Example, second),
                    size_of::<i64>(),
                    align_of::<i64>(),
                ),
            ],
        )])
    }

    #[test]
    fn fingerprint_is_stable_and_self_verifying() {
        let first = descriptor();
        let second = descriptor();
        assert_eq!(first.fingerprint, second.fingerprint);
        assert!(first.verify_fingerprint());
    }

    #[test]
    fn fingerprint_detects_layout_changes() {
        let mut changed = descriptor();
        changed.structs[0].fields[0].offset += 1;
        assert!(!changed.verify_fingerprint());
    }

    #[test]
    fn abi_v9_exposes_dual_fill_quantity_and_account_routing() {
        let descriptor = runtime_abi_descriptor();
        assert_eq!(descriptor.abi_version, 9);
        let fill = descriptor
            .structs
            .iter()
            .find(|value| value.name == "FillEvent")
            .unwrap();
        assert!(
            fill.fields
                .iter()
                .any(|field| field.name == "last_fill_qty")
        );
        assert!(
            fill.fields
                .iter()
                .any(|field| field.name == "cumulative_filled_qty")
        );
        let command = descriptor
            .structs
            .iter()
            .find(|value| value.name == "OrderCommand")
            .unwrap();
        assert!(
            command
                .fields
                .iter()
                .any(|field| field.name == "local_account_no")
        );
    }
}
