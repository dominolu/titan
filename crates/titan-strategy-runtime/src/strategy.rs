//! Standalone native Strategy ABI compiled_strategy and state runtime.

use crate::context::StrategyRuntimeContext;
use crate::observation::{
    BusinessObservationDefinition, CompiledBusinessObservation, compile_business_observation,
};
use std::{
    collections::BTreeSet,
    fs,
    io::{Cursor, Read},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use libloading::Library;
use sha2::{Digest, Sha256};

mod compiler;
pub use compiler::*;

use crate::context::{ABI_FINGERPRINT, STRATEGY_ABI};

/// Number of callbacks in a callback mask word.
pub const CALLBACK_COUNT: usize = 13;

#[derive(Clone, Debug, PartialEq)]
enum CborValue {
    Integer(i128),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<CborValue>),
    Map(Vec<(CborValue, CborValue)>),
    Bool(bool),
    Null,
    Float(f64),
}

impl CborValue {
    fn as_text(&self) -> Option<&str> {
        if let Self::Text(value) = self {
            Some(value)
        } else {
            None
        }
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(value) = self {
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CallbackCode {
    Ok = 0,
    HandlerError = -1,
    InvalidContext = -2,
    StateSchemaMismatch = -3,
    CommandError = -4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum EventKind {
    Start = 0,
    Tick = 1,
    Bar = 2,
    Depth = 3,
    Fill = 4,
    Order = 5,
    Cancel = 6,
    Position = 7,
    Balance = 8,
    AccountState = 9,
    Timer = 10,
    Stop = 11,
    Recover = 12,
}

impl EventKind {
    pub(crate) fn schema_version(self) -> u32 {
        if self == Self::Fill { 2 } else { 1 }
    }
    fn index(self) -> usize {
        self as usize
    }

    fn from_manifest_name(value: &str) -> Result<Self, LoadError> {
        Ok(match value {
            "bbo" => Self::Tick,
            "bar" => Self::Bar,
            "depth" => Self::Depth,
            "fill" => Self::Fill,
            "order" => Self::Order,
            "cancel" => Self::Cancel,
            "position" => Self::Position,
            "balance" => Self::Balance,
            "account_state" => Self::AccountState,
            "timer" => Self::Timer,
            _ => return Err(LoadError::Contract("unknown subscription event")),
        })
    }
}

#[allow(clippy::too_many_arguments)] // frozen compiled strategy-era call shape; V14 wires the table as the last argument
pub(crate) fn callback_context(
    now_ns: i64,
    schema_version: u32,
    markets: &[TitanMarketView],
    positions: &[TitanPositionView],
    balances: &[TitanBalanceView],
    accounts: &[TitanAccountView],
    active_orders: &[TitanActiveOrderView],
    table: &crate::ObjectTable,
) -> StrategyRuntimeContext {
    crate::context::callback_context(
        now_ns,
        schema_version,
        crate::context::CallbackViews {
            markets,
            positions,
            balances,
            accounts,
            active_orders,
        },
        table,
    )
}

pub(crate) fn invoke_staged(
    instance: &mut StrategyInstance,
    staging: &mut CallbackCommandStaging,
    context: &mut StrategyRuntimeContext,
    kind: EventKind,
    gate: bool,
    active_orders: &[TitanActiveOrderView],
    host_stopping: bool,
) -> Result<Vec<StagedCommand>, LoadError> {
    let paused = context.flags & CONTEXT_FLAG_PAUSED != 0;
    staging.begin_callback(
        (gate || paused) && instance.command_gate_open(),
        active_orders,
    );
    staging.reducing_only = paused;
    staging.bind_context(context);
    let gate_open = instance.command_gate_open();
    instance.status_pre_callback(paused, gate_open, host_stopping);
    if let Err(error) = instance.invoke(kind, context) {
        instance.status_post_callback(-1, context.last_error_code);
        staging.finish_callback(-1);
        return Err(error);
    }
    instance.status_post_callback(0, context.last_error_code);
    Ok(staging.finish_callback(0).to_vec())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionQos {
    Latest,
    ReliableOrdered,
    BestEffort,
}

impl SubscriptionQos {
    fn from_manifest_name(value: &str) -> Result<Self, LoadError> {
        Ok(match value {
            "latest" => Self::Latest,
            "reliable_ordered" => Self::ReliableOrdered,
            "best_effort" => Self::BestEffort,
            _ => return Err(LoadError::Contract("unknown subscription QoS")),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSubscription {
    pub event: EventKind,
    pub handler: Arc<str>,
    pub schema_version: u32,
    pub qos: SubscriptionQos,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ManifestCapabilities(pub u64);

impl ManifestCapabilities {
    pub const MARKET_DATA: Self = Self(1 << 0);
    pub const ACCOUNT_DATA: Self = Self(1 << 1);
    pub const ORDER_EXECUTION: Self = Self(1 << 2);
    pub const TIMER: Self = Self(1 << 3);
    /// Stop waits for a callback to explicitly confirm its business work is complete.
    pub const STOP_BARRIER: Self = Self(1 << 4);
    /// Recover receives host-reconciled order results in its typed event payload.
    pub const RECOVERY_ORDER_RESULTS: Self = Self(1 << 5);
    pub const PAUSE_SAFE: Self = Self(1 << 6);
    /// Host reconstructs a covered Top100 and supplies DepthBatchInfoV1.
    pub const HOST_DEPTH_100: Self = Self(1 << 7);
    pub const BUSINESS_FAULT_DRAIN: Self = Self(1 << 8);
    pub const REQUIRES_PERIODIC_TIMER: Self = Self(1 << 9);
    pub const MULTI_VENUE_EXECUTION: Self = Self(1 << 10);
    /// The artifact reads the V14 object table (`object_context` manifest capability).
    ///
    /// compiled strategy manifest encoding of [`crate::OBJECT_CONTEXT_CAPABILITY`]; the V14 runtime counterpart
    /// is [`StrategyCapabilities::OBJECT_CONTEXT`](crate::StrategyCapabilities::OBJECT_CONTEXT)
    /// (`1 << 13`). The mapping lives in [`crate::compiled_strategy::manifest_from_artifact`]; the two bit numbers differ on
    /// purpose because the bit sets are different wire formats.
    pub const OBJECT_CONTEXT: Self = Self(1 << 11);

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// Value written into the deprecated `TitanMarketView.tick_size/lot_size` slots.
///
/// It is a placeholder, not an exchange fact: real units come from
/// `TitanSymbolPayloadV1` via `ctx.symbol(..)`. Both the live runtime and the offline replay
/// write this constant through `apply_deprecated_unit_placeholders`, so the two execution paths
/// cannot disagree about what the deprecated slots contain.
pub const DEPRECATED_UNIT_PLACEHOLDER: i64 = 1;

/// Lowercase hexadecimal form of a 32-byte identity (the form manifests store).
pub fn hex_identity(value: &[u8; 32]) -> String {
    value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

/// Applies [`DEPRECATED_UNIT_PLACEHOLDER`] to every market view with no unit claim of its own.
///
/// This is the only place that writes the deprecated slots. Passing already-derived views (views
/// that carry a non-zero `tick_size`) is a programming error and returns `false` so the caller can
/// fail closed rather than mix real units with placeholders.
pub fn apply_deprecated_unit_placeholders(markets: &mut [TitanMarketView]) -> bool {
    for market in markets.iter_mut() {
        if market.tick_size != 0 || market.lot_size != 0 {
            return false;
        }
        market.tick_size = DEPRECATED_UNIT_PLACEHOLDER;
        market.lot_size = DEPRECATED_UNIT_PLACEHOLDER;
    }
    true
}

// ---------------------------------------------------------------------------------------------
// Reserved fields: the one rule, stated once
// ---------------------------------------------------------------------------------------------
//
// The views below, `StrategyRuntimeContext`, the V14 tail and the V14 object payloads all
// carry fields named `reserved`, `reserved0`, `reserved1`, `reserved2`, `reserved_tail` or
// `reserved_ext`. They all follow the same three rules:
//
// (a) **Placeholder, not business meaning.** A reserved field is padding left in a frozen layout
//     so that a future revision can add meaning without changing the ABI fingerprint. Today they
//     carry no business semantics at all: every "reserved" byte the host ever produces is zero, and
//     every reader must ignore them. They exist only because names, offsets and sizes are part of
//     the ABI fingerprint and therefore cannot be removed or renamed in place.
//
// (b) **One live exception: `TitanSubmitOrderRequest.reserved[0]`.** That single byte carries the
//     submit's [`CommandPurpose`] at [`COMMAND_PURPOSE_INDEX`] (defined in
//     `execution_binding.rs`). It is a real extension slot, not padding: it must never be zeroed
//     by a cleanup pass, never reused for anything else, and never renamed or resized (the whole
//     `reserved: [u8; 2]` array stays as is).
//     Hosts that do not understand it must still treat byte 0 as authored by the strategy. The
//     second byte is an ordinary, always-zero placeholder like every other reserved byte.
//
// (c) **V14 schema-identity fields must stay zero.** The `reserved*` fields of the object payloads
//     (`TitanDecimalV1::reserved`, `TitanSymbolPayloadV1::reserved0`, etc.) are part of the
//     canonical descriptor hashed by `object_schema_hash()` (see
//     [`crate::context::object_schema_hash`]): they have a name, a type, an offset and a size in
//     that descriptor, so they define the V14 payload schema and are not free space. The V14
//     context tail (`StrategyRuntimeContext::reserved0/reserved/reserved_tail/reserved_ext`)
//     pins the root layout instead: `objects_ptr` is at 392, the root is 440 bytes, and the tail
//     offsets are asserted. Writing a non-zero value into either group changes what the schema
//     identity or the layout asserts about the same bytes, so both must be identically zero,
//     always.
//
// The invariant test `reserved_fields_are_zero_except_the_command_purpose_slot` (in this module's
// test submodule) pins these rules for the views and the one exception.
//
// Nothing here changes layout: do not add serde attributes to reserved fields and do not rename
// them — both would move the ABI fingerprint.

macro_rules! view {
    ($name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
        #[serde(default, deny_unknown_fields)]
        #[repr(C)]
        pub struct $name { $(pub $field: $type),* }
    };
}

view!(TitanTickView {
    asset_no: u32,
    kind: u8,
    side: u8,
    reserved: [u8; 2],
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    price_ticks: i64,
    qty_lots: i64,
    source_sequence: u64
});
view!(TitanBarView {
    asset_no: u32,
    reserved: u32,
    timeframe_ns: i64,
    open_ts_ns: i64,
    close_ts_ns: i64,
    open_ticks: i64,
    high_ticks: i64,
    low_ticks: i64,
    close_ticks: i64,
    volume_lots: i64
});
view!(TitanDepthView {
    asset_no: u32,
    level: u32,
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    price_ticks: i64,
    qty_lots: i64,
    source_sequence: u64,
    side: u8,
    action: u8,
    is_snapshot: u8,
    reserved: [u8; 5]
});
view!(TitanFillView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    fill_price_ticks: i64,
    fill_qty_lots: i64,
    cumulative_filled_lots: i64,
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    account_sequence: u64,
    side: u8,
    liquidity: u8,
    final_fill: u8,
    reserved: [u8; 5]
});
view!(TitanOrderEventView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    cumulative_filled_lots: i64,
    event_ts_ns: i64,
    account_sequence: u64,
    status: u8,
    reason: u8,
    position_side: u8,
    reserved: [u8; 5]
});
view!(TitanCancelEventView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    event_ts_ns: i64,
    account_sequence: u64,
    request_result: u8,
    final_status: u8,
    reserved: [u8; 6]
});
view!(TitanPositionEventView {
    asset_no: u32,
    account_no: u32,
    qty_lots: i64,
    average_price_ticks: i64,
    realized_pnl_ticks: i64,
    event_ts_ns: i64,
    account_sequence: u64,
    position_side: u8,
    reserved: [u8; 7]
});
view!(TitanBalanceEventView {
    account_no: u32,
    currency_no: u32,
    total_units: i64,
    available_units: i64,
    event_ts_ns: i64,
    account_sequence: u64
});
view!(TitanAccountStateEventView {
    account_no: u32,
    reserved0: u32,
    account_epoch: u64,
    event_ts_ns: i64,
    account_sequence: u64,
    state: u8,
    reason: u8,
    reserved: [u8; 6]
});
view!(TitanTimerView {
    timer_id: u64,
    scheduled_ts_ns: i64,
    fired_ts_ns: i64
});
// Deprecated placeholders kept byte-compatible for the frozen compiled strategy layout: `tick_size`/`lot_size`
// are always 1 and carry no exchange meaning; authoritative units live in the V14 object table
// (`ctx.symbol`). Removing the fields needs another ABI major, so they stay as reserved slots.
view!(TitanMarketView {
    asset_no: u32,
    flags: u32,
    best_bid_ticks: i64,
    best_bid_qty_lots: i64,
    best_ask_ticks: i64,
    best_ask_qty_lots: i64,
    tick_size: i64,
    lot_size: i64,
    source_sequence: u64
});
view!(TitanPositionView {
    asset_no: u32,
    account_no: u32,
    qty_lots: i64,
    average_price_ticks: i64,
    realized_pnl_ticks: i64,
    account_sequence: u64,
    position_side: u8,
    reserved: [u8; 7]
});
view!(TitanBalanceView {
    account_no: u32,
    currency_no: u32,
    total_units: i64,
    available_units: i64,
    account_sequence: u64
});
view!(TitanAccountView {
    account_no: u32,
    reserved: u32,
    account_epoch: u64,
    account_sequence: u64,
    state: u8,
    reason: u8,
    reserved2: [u8; 6]
});
view!(TitanActiveOrderView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    cumulative_filled_lots: i64,
    created_ts_ns: i64,
    updated_ts_ns: i64,
    account_sequence: u64,
    side: u8,
    order_type: u8,
    time_in_force: u8,
    status: u8,
    reduce_only: u8,
    position_side: u8,
    reserved: [u8; 2]
});
view!(TitanSubmitOrderRequest {
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    trigger_price_ticks: i64,
    gtd_expiry_ns: i64,
    side: u8,
    order_type: u8,
    time_in_force: u8,
    reduce_only: u8,
    trigger_kind: u8,
    position_side: u8,
    reserved: [u8; 2]
});
view!(TitanCancelOrderRequest {
    order_id: u64,
    asset_no: u32,
    account_no: u32
});

pub type TitanSubmitOrderFn =
    unsafe extern "C" fn(*mut std::ffi::c_void, *const TitanSubmitOrderRequest, *mut u64) -> i32;
pub type TitanCancelOrderFn =
    unsafe extern "C" fn(*mut std::ffi::c_void, *const TitanCancelOrderRequest, *mut u64) -> i32;

#[derive(Clone, Copy, Debug)]
pub enum StagedCommand {
    Submit {
        order_id: u64,
        request: TitanSubmitOrderRequest,
    },
    Cancel {
        command_id: u64,
        request: TitanCancelOrderRequest,
    },
}

pub struct CallbackCommandStaging {
    commands: Vec<StagedCommand>,
    capacity: usize,
    next_order_id: u64,
    next_command_id: u64,
    gate_open: bool,
    reducing_only: bool,
    attempted: bool,
    active_orders_ptr: *const TitanActiveOrderView,
    active_orders_len: usize,
}

// The borrowed active-order pointer is installed and cleared on the same single-writer strategy
// lane around a synchronous callback. The staging value is never accessed concurrently.
unsafe impl Send for CallbackCommandStaging {}

impl CallbackCommandStaging {
    pub fn new(
        capacity: usize,
        strategy_instance_id: u64,
        generation: u64,
    ) -> Result<Self, LoadError> {
        if capacity == 0 || strategy_instance_id == 0 || generation == 0 {
            return Err(LoadError::Contract("invalid command staging configuration"));
        }
        let generation_prefix = generation
            .checked_sub(1)
            .filter(|value| *value <= u64::from(u32::MAX))
            .ok_or(LoadError::Contract("strategy generation is too large"))?
            << 32;
        Ok(Self {
            commands: Vec::with_capacity(capacity),
            capacity,
            // Generation one deliberately keeps the historical 1-based identifiers. Later
            // generations occupy disjoint 32-bit ranges so a restart can never reuse an order
            // identifier that may still be live at a venue.
            next_order_id: generation_prefix | 1,
            next_command_id: generation_prefix | 1,
            gate_open: false,
            reducing_only: false,
            attempted: false,
            active_orders_ptr: std::ptr::null(),
            active_orders_len: 0,
        })
    }

    pub fn begin_callback(&mut self, gate_open: bool, active_orders: &[TitanActiveOrderView]) {
        self.commands.clear();
        self.gate_open = gate_open;
        self.reducing_only = false;
        self.attempted = false;
        self.active_orders_ptr = active_orders.as_ptr();
        self.active_orders_len = active_orders.len();
    }

    pub fn bind_context(&mut self, context: &mut StrategyRuntimeContext) {
        context.command_context = (self as *mut Self).cast();
        context.submit_order = Some(stage_submit_order);
        context.cancel_order = Some(stage_cancel_order);
        context.last_error_code = 0;
    }

    pub fn finish_callback(&mut self, callback_code: i32) -> &[StagedCommand] {
        self.active_orders_ptr = std::ptr::null();
        self.active_orders_len = 0;
        if callback_code != 0 {
            self.commands.clear();
        }
        &self.commands
    }

    pub fn clear_committed(&mut self) {
        self.commands.clear();
    }
    pub(crate) fn attempted_command(&self) -> bool {
        self.attempted
    }

    fn active_orders(&self) -> &[TitanActiveOrderView] {
        if self.active_orders_ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.active_orders_ptr, self.active_orders_len) }
        }
    }
}

unsafe extern "C" fn stage_submit_order(
    context: *mut std::ffi::c_void,
    request: *const TitanSubmitOrderRequest,
    order_id_out: *mut u64,
) -> i32 {
    if context.is_null() || request.is_null() || order_id_out.is_null() {
        return -1;
    }
    let staging = unsafe { &mut *context.cast::<CallbackCommandStaging>() };
    staging.attempted = true;
    let mut request = unsafe { *request };
    if !staging.gate_open
        || staging.commands.len() >= staging.capacity
        || request.qty_lots <= 0
        || !matches!(request.side, 1 | 2)
        || !matches!(request.order_type, 1 | 2)
        || !matches!(request.time_in_force, 1..=4)
        || !matches!(request.reduce_only, 0 | 1)
        || request.position_side > 2
        || request.trigger_kind != 0
        || request.trigger_price_ticks != 0
        || request.gtd_expiry_ns != 0
    {
        return -2;
    }
    let closes_side = titan_account_service::is_hedge_close(
        request.position_side,
        if request.side == 1 { 1 } else { -1 },
    );
    if request.reduce_only == 1 && request.position_side != 0 && !closes_side {
        return -2;
    }
    if closes_side {
        request.reduce_only = 1;
    }
    if staging.reducing_only && request.reduce_only == 0 {
        return -2;
    }
    let order_id = staging.next_order_id;
    staging.next_order_id = staging.next_order_id.checked_add(1).unwrap_or(0);
    if order_id == 0 || staging.next_order_id == 0 {
        return -3;
    }
    staging
        .commands
        .push(StagedCommand::Submit { order_id, request });
    unsafe {
        *order_id_out = order_id;
    }
    0
}

unsafe extern "C" fn stage_cancel_order(
    context: *mut std::ffi::c_void,
    request: *const TitanCancelOrderRequest,
    command_id_out: *mut u64,
) -> i32 {
    if context.is_null() || request.is_null() || command_id_out.is_null() {
        return -1;
    }
    let staging = unsafe { &mut *context.cast::<CallbackCommandStaging>() };
    staging.attempted = true;
    let request = unsafe { *request };
    if !staging.gate_open || staging.commands.len() >= staging.capacity {
        return -2;
    }
    if !staging.active_orders().iter().any(|order| {
        order.order_id == request.order_id
            && order.account_no == request.account_no
            && order.asset_no == request.asset_no
    }) {
        return -4;
    }
    let command_id = staging.next_command_id;
    staging.next_command_id = staging.next_command_id.checked_add(1).unwrap_or(0);
    if command_id == 0 || staging.next_command_id == 0 {
        return -3;
    }
    staging.commands.push(StagedCommand::Cancel {
        command_id,
        request,
    });
    unsafe {
        *command_id_out = command_id;
    }
    0
}

/// Callback output flag, honored only for compiled_strategies declaring STOP_BARRIER.
pub const CONTEXT_FLAG_STOP_READY: u32 = 1;
/// Host input: callbacks continue to consume facts, but only cancel/reduce commands are allowed.
pub const CONTEXT_FLAG_PAUSED: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct NativeStrategyDescriptor {
    pub struct_size: u32,
    pub abi_version: u32,
    pub abi_fingerprint: [u8; 32],
    pub state_schema_version: u32,
    pub state_alignment: u32,
    pub state_len: u64,
    pub state_schema_hash: [u8; 32],
    pub callback_mask: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryStateIdentity {
    pub version: u32,
    pub hash: [u8; 32],
    pub byte_len: usize,
    pub alignment: u32,
}
impl RecoveryStateIdentity {
    pub fn validate_description(&self, description: &serde_json::Value) -> Result<(), LoadError> {
        let mut schema = description.clone();
        let object = schema
            .as_object_mut()
            .ok_or(LoadError::Contract("invalid compiler state schema"))?;
        if object.get("itemsize").and_then(|v| v.as_u64()) != Some(self.byte_len as u64)
            || object.get("alignment").and_then(|v| v.as_u64()) != Some(self.alignment.into())
        {
            return Err(LoadError::Contract("state schema layout mismatch"));
        }
        object.insert("schema_version".into(), self.version.into());
        if <[u8; 32]>::from(Sha256::digest(compiler_json(&schema))) != self.hash {
            return Err(LoadError::Contract("compiler state schema hash mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlignedStateMemory {
    words: Vec<u64>,
    byte_len: usize,
    schema: RecoveryStateIdentity,
}

impl AlignedStateMemory {
    pub fn from_bytes(schema: RecoveryStateIdentity, initial: &[u8]) -> Result<Self, LoadError> {
        if schema.byte_len == 0
            || schema.byte_len != initial.len()
            || schema.alignment == 0
            || schema.alignment > 8
            || !schema.alignment.is_power_of_two()
        {
            return Err(LoadError::State("invalid state length or alignment"));
        }
        let mut words = vec![0_u64; schema.byte_len.div_ceil(8)];
        unsafe {
            std::ptr::copy_nonoverlapping(
                initial.as_ptr(),
                words.as_mut_ptr().cast(),
                initial.len(),
            );
        }
        Ok(Self {
            words,
            byte_len: initial.len(),
            schema,
        })
    }

    pub fn schema(&self) -> RecoveryStateIdentity {
        self.schema
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.words.as_mut_ptr().cast()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.byte_len) }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.byte_len) }
    }
    pub fn clone_for_instance(&self) -> Self {
        Self::from_bytes(self.schema, self.as_bytes()).expect("validated state remains valid")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("compiled_strategy I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("compiled_strategy CBOR is invalid: {0}")]
    Cbor(String),
    #[error("compiled_strategy contract violation: {0}")]
    Contract(&'static str),
    #[error("state memory is invalid: {0}")]
    State(&'static str),
    #[error("native library load failed: {0}")]
    Native(#[from] libloading::Error),
    // Structured ABI mismatches: the loader reports a stable reason so an operator is told to
    // recompile instead of seeing a generic "load failed".
    #[error(
        "strategy ABI version mismatch: artifact {artifact}, host {host}; recompile the strategy"
    )]
    AbiVersion { artifact: u32, host: u32 },
    #[error("strategy ABI fingerprint mismatch; recompile the strategy with the current host")]
    AbiFingerprint,
    #[error(
        "strategy ABI context layout mismatch: artifact {artifact} bytes, host {host} bytes; recompile the strategy"
    )]
    AbiLayout { artifact: u64, host: u64 },
    #[error("bundle is invalid: {0}")]
    Bundle(&'static str),
    #[error("native callback {event:?} returned {code}")]
    Callback { event: EventKind, code: i32 },
}

#[derive(Clone, Debug)]
pub struct ArtifactMetadata {
    pub abi_version: u32,
    pub strategy_type: Arc<str>,
    pub source_version: Arc<str>,
    pub target_triple: Arc<str>,
    pub cpu_baseline: Arc<str>,
    pub native_library: Arc<str>,
    pub callback_mask: u64,
    pub parameter_schema: Arc<serde_json::Value>,
    /// Source identity of the compiled strategy; optional for older compiled strategy producers.
    pub source_digest: Option<[u8; 32]>,
    pub parameters_digest: [u8; 32],
    /// V14 object requirements declared by the artifact. `None` for compiled strategy artifacts.
    pub object_manifest: Option<crate::ObjectManifest>,
    pub subscriptions: Arc<[EventSubscription]>,
    pub capabilities: ManifestCapabilities,
    pub state: RecoveryStateIdentity,
    pub recovery_state_schema: Option<Arc<serde_json::Value>>,
    pub business_observation: Option<Arc<CompiledBusinessObservation>>,
    pub initial_state: Arc<[u8]>,
    pub native_library_sha256: [u8; 32],
    pub compiled_strategy_digest: [u8; 32],
}

pub type StrategyCallback = unsafe extern "C" fn(*mut StrategyRuntimeContext) -> i32;

#[derive(Clone)]
pub struct CallbackRegistry {
    callbacks: [Option<StrategyCallback>; CALLBACK_COUNT],
}

impl CallbackRegistry {
    pub fn invoke(
        &self,
        event: EventKind,
        context: &mut StrategyRuntimeContext,
    ) -> Result<(), LoadError> {
        context.event_kind = event as u32;
        let Some(callback) = self.callbacks[event.index()] else {
            return Ok(());
        };
        let code = unsafe { callback(context) };
        if code == 0 {
            Ok(())
        } else {
            Err(LoadError::Callback { event, code })
        }
    }
}

struct NativeLibraryLease {
    _library: Library,
}
unsafe impl Send for NativeLibraryLease {}
unsafe impl Sync for NativeLibraryLease {}

pub struct LoadedNativeStrategy {
    pub manifest: ArtifactMetadata,
    pub descriptor: NativeStrategyDescriptor,
    pub callbacks: CallbackRegistry,
    pub initial_state: AlignedStateMemory,
    _lease: Arc<NativeLibraryLease>,
}

impl LoadedNativeStrategy {
    pub fn instantiate(
        &self,
        strategy_instance_id: u64,
        generation: u64,
    ) -> Result<StrategyInstance, LoadError> {
        if strategy_instance_id == 0 || generation == 0 {
            return Err(LoadError::Contract(
                "instance id and generation must be nonzero",
            ));
        }
        Ok(StrategyInstance {
            strategy_instance_id,
            generation,
            state: self.initial_state.clone_for_instance(),
            callbacks: self.callbacks.clone(),
            business_fault_drain: self
                .manifest
                .capabilities
                .contains(ManifestCapabilities::BUSINESS_FAULT_DRAIN),
            business_fault: None,
            strategy_type: self.manifest.strategy_type.clone(),
            source_version: self.manifest.source_version.clone(),
            compiled_strategy_digest: self.manifest.compiled_strategy_digest,
            binding_digest: [0; 32],
            lifecycle: StrategyInstanceLifecycle::Ready,
            command_gate_open: false,
            event_committed_sequence: 0,
            public_state_identity: [0; 32],
            object_table: crate::ObjectTable::empty(),
            _lease: self._lease.clone(),
        })
    }

    pub fn instantiate_with_config(
        &self,
        config: &StrategyInstanceConfig,
        generation: u64,
    ) -> Result<StrategyInstance, LoadError> {
        if config.max_commands_per_callback == 0 || config.max_handler_duration.is_zero() {
            return Err(LoadError::Contract("runtime limits must be positive"));
        }
        validate_local_numbers(&config.markets)?;
        validate_local_numbers(&config.accounts)?;
        let mut instance = self.instantiate(config.strategy_instance_id, generation)?;
        instance.binding_digest = binding_digest(config);
        Ok(instance)
    }
}

#[derive(Clone, Debug)]
pub struct StrategyInstanceConfig {
    pub strategy_instance_id: u64,
    /// Admission identity of the instance's object table; zero when no table is attached.
    pub object_identity_digest: [u8; 32],
    pub markets: Vec<(u32, u32)>,
    pub accounts: Vec<(u32, u32)>,
    pub routing_keys: Vec<u64>,
    pub max_commands_per_callback: usize,
    pub max_handler_duration: std::time::Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyInstanceLifecycle {
    Ready,
    Running,
    Restoring,
    Stopped,
    Faulted,
}

pub struct StrategyInstance {
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub state: AlignedStateMemory,
    callbacks: CallbackRegistry,
    business_fault_drain: bool,
    business_fault: Option<u32>,
    strategy_type: Arc<str>,
    source_version: Arc<str>,
    compiled_strategy_digest: [u8; 32],
    binding_digest: [u8; 32],
    lifecycle: StrategyInstanceLifecycle,
    command_gate_open: bool,
    event_committed_sequence: u64,
    public_state_identity: [u8; 32],
    object_table: crate::ObjectTable,
    _lease: Arc<NativeLibraryLease>,
}

pub(crate) fn status_lifecycle_value(lifecycle: StrategyInstanceLifecycle, paused: bool) -> u32 {
    if paused {
        return crate::STATUS_LIFECYCLE_PAUSED;
    }
    match lifecycle {
        StrategyInstanceLifecycle::Ready => crate::STATUS_LIFECYCLE_READY,
        StrategyInstanceLifecycle::Running => crate::STATUS_LIFECYCLE_RUNNING,
        StrategyInstanceLifecycle::Restoring => crate::STATUS_LIFECYCLE_RECOVERING,
        StrategyInstanceLifecycle::Stopped => crate::STATUS_LIFECYCLE_STOPPED,
        StrategyInstanceLifecycle::Faulted => crate::STATUS_LIFECYCLE_FAILED,
    }
}

impl StrategyInstance {
    /// Publishes the entry state into the single mutable status object before a callback runs.
    fn status_pre_callback(&mut self, paused: bool, gate_open: bool, stopping: bool) {
        let lifecycle = status_lifecycle_value(self.lifecycle, paused);
        if let Some(status) = self.object_table.try_status_mut() {
            status.lifecycle = lifecycle;
            status.entry_flags = (u32::from(paused) * crate::STATUS_ENTRY_PAUSED)
                | (u32::from(gate_open) * crate::STATUS_ENTRY_COMMAND_GATE_OPEN)
                | (u32::from(stopping) * crate::STATUS_ENTRY_STOPPING);
        }
    }

    /// Publishes the callback outcome; `last_*` intentionally describes the *previous* callback
    /// until this point, which is the documented lagging semantic.
    fn status_post_callback(&mut self, code: i32, last_error_code: i32) {
        if let Some(status) = self.object_table.try_status_mut() {
            status.last_return_code = code;
            status.last_error_code = last_error_code;
        }
    }

    /// The frozen object table for this instance. Empty until the service attaches the table
    /// built from the instance's bound definitions and metadata snapshot.
    pub fn object_table(&self) -> &crate::ObjectTable {
        &self.object_table
    }

    /// Admission identity of the attached object table (zero when no table is attached).
    pub fn object_identity_digest(&self) -> [u8; 32] {
        self.object_table.identity_digest()
    }

    /// Digest of the metadata snapshot the table was built from (zero when none).
    pub fn metadata_snapshot_digest(&self) -> [u8; 32] {
        self.object_table.metadata_snapshot_digest()
    }

    pub fn attach_object_table(&mut self, table: crate::ObjectTable) {
        self.object_table = table;
    }

    pub fn invoke(
        &mut self,
        event: EventKind,
        context: &mut StrategyRuntimeContext,
    ) -> Result<(), LoadError> {
        let allowed = match event {
            EventKind::Recover => self.lifecycle == StrategyInstanceLifecycle::Restoring,
            EventKind::Start => self.lifecycle == StrategyInstanceLifecycle::Ready,
            EventKind::Stop => matches!(
                self.lifecycle,
                StrategyInstanceLifecycle::Ready | StrategyInstanceLifecycle::Running
            ),
            _ => self.lifecycle == StrategyInstanceLifecycle::Running,
        };
        if !allowed {
            return Err(LoadError::Contract(
                "callback is not allowed in the current lifecycle state",
            ));
        }
        self.invoke_callback(event, context)
    }

    fn invoke_callback(
        &mut self,
        event: EventKind,
        context: &mut StrategyRuntimeContext,
    ) -> Result<(), LoadError> {
        context.struct_size = std::mem::size_of::<StrategyRuntimeContext>() as u32;
        context.abi_version = STRATEGY_ABI;
        context.strategy_instance_id = self.strategy_instance_id;
        context.generation = self.generation;
        context.state_ptr = self.state.as_mut_ptr();
        context.state_len = self.state.schema.byte_len as u64;
        context.state_alignment = self.state.schema.alignment;
        context.state_schema_version = self.state.schema.version;
        context.state_schema_hash = self.state.schema.hash;
        if let Err(error) = self.callbacks.invoke(event, context) {
            if let LoadError::Callback {
                code: 1..=65535, ..
            } = &error
                && self.business_fault_drain
                && !matches!(event, EventKind::Start | EventKind::Recover)
            {
                if let LoadError::Callback { code, .. } = error {
                    self.business_fault.get_or_insert(code as u32);
                }
                return Ok(());
            }
            self.command_gate_open = false;
            self.lifecycle = StrategyInstanceLifecycle::Faulted;
            return Err(error);
        }
        Ok(())
    }

    /// A capability-negotiated business fault preserves staged safety commands.
    pub fn business_fault(&self) -> Option<u32> {
        self.business_fault
    }

    pub(crate) fn replay_fill(
        &mut self,
        staging: &mut CallbackCommandStaging,
        context: &mut StrategyRuntimeContext,
    ) -> Result<(), LoadError> {
        if self.lifecycle != StrategyInstanceLifecycle::Restoring || self.command_gate_open {
            return Err(LoadError::Contract(
                "execution replay requires a closed restoring instance",
            ));
        }
        staging.begin_callback(false, &[]);
        staging.bind_context(context);
        let result = self.invoke_callback(EventKind::Fill, context);
        staging.finish_callback(-1);
        if staging.attempted {
            return Err(LoadError::Contract("execution replay attempted a command"));
        }
        result
    }

    pub fn start(&mut self) -> Result<(), LoadError> {
        if self.lifecycle != StrategyInstanceLifecycle::Ready {
            return Err(LoadError::Contract("instance is not ready"));
        }
        self.command_gate_open = true;
        self.lifecycle = StrategyInstanceLifecycle::Running;
        Ok(())
    }

    pub fn stop(&mut self) {
        self.command_gate_open = false;
        self.lifecycle = StrategyInstanceLifecycle::Stopped;
    }

    pub fn command_gate_open(&self) -> bool {
        self.command_gate_open
    }

    pub fn update_checkpoint_boundary(
        &mut self,
        event_committed_sequence: u64,
        public_state_identity: [u8; 32],
    ) {
        self.event_committed_sequence = event_committed_sequence;
        self.public_state_identity = public_state_identity;
    }

    pub fn freeze_state(&self, checkpoint_id: u64) -> Result<StrategyStateSnapshot, LoadError> {
        if checkpoint_id == 0 {
            return Err(LoadError::Contract("checkpoint id must be nonzero"));
        }
        let mut snapshot = StrategyStateSnapshot {
            checkpoint_id,
            strategy_instance_id: self.strategy_instance_id,
            generation: self.generation,
            event_committed_sequence: self.event_committed_sequence,
            strategy_type: self.strategy_type.clone(),
            source_version: self.source_version.clone(),
            compiled_strategy_digest: self.compiled_strategy_digest,
            binding_digest: self.binding_digest,
            abi_version: STRATEGY_ABI,
            state_schema_version: self.state.schema.version,
            state_schema_hash: self.state.schema.hash,
            state_alignment: self.state.schema.alignment,
            state_bytes: Arc::from(self.state.as_bytes()),
            public_state_identity: self.public_state_identity,
            checksum: [0; 32],
        };
        snapshot.checksum = snapshot_checksum(&snapshot);
        Ok(snapshot)
    }

    pub fn restore_state(&mut self, snapshot: &StrategyStateSnapshot) -> Result<(), LoadError> {
        if !matches!(
            self.lifecycle,
            StrategyInstanceLifecycle::Ready | StrategyInstanceLifecycle::Stopped
        ) {
            return Err(LoadError::Contract(
                "restore requires a stopped instance safe point",
            ));
        }
        if !snapshot.checksum_valid()
            || snapshot.strategy_instance_id != self.strategy_instance_id
            || snapshot.strategy_type != self.strategy_type
            || snapshot.source_version != self.source_version
            || snapshot.compiled_strategy_digest != self.compiled_strategy_digest
            || snapshot.binding_digest != self.binding_digest
            || snapshot.abi_version != STRATEGY_ABI
            || snapshot.state_schema_version != self.state.schema.version
            || snapshot.state_schema_hash != self.state.schema.hash
            || snapshot.state_alignment != self.state.schema.alignment
            || snapshot.state_bytes.len() != self.state.schema.byte_len
        {
            return Err(LoadError::Contract("snapshot identity mismatch"));
        }
        self.command_gate_open = false;
        self.lifecycle = StrategyInstanceLifecycle::Restoring;
        // Reconciliation compares the successor against the persisted generation.
        self.generation = snapshot.generation;
        self.state
            .as_bytes_mut()
            .copy_from_slice(&snapshot.state_bytes);
        self.event_committed_sequence = snapshot.event_committed_sequence;
        self.public_state_identity = snapshot.public_state_identity;
        Ok(())
    }

    pub(crate) fn bind_resolved_routes(
        &mut self,
        markets: &[crate::ResolvedMarketBinding],
        accounts: &[crate::ResolvedAccountBinding],
    ) {
        self.binding_digest = resolved_route_digest(self.binding_digest, markets, accounts);
    }
    pub(crate) fn has_callback(&self, event: EventKind) -> bool {
        self.callbacks.callbacks[event.index()].is_some()
    }
    /// Only the host reconciler calls this after generic and strategy validation succeeded.
    pub(crate) fn complete_reconciled_restore(
        &mut self,
        identity: [u8; 32],
        generation: u64,
    ) -> Result<(), LoadError> {
        if self.lifecycle != StrategyInstanceLifecycle::Restoring || generation <= self.generation {
            return Err(LoadError::Contract("invalid reconciliation boundary"));
        }
        self.public_state_identity = identity;
        self.event_committed_sequence = 0;
        self.complete_restore(identity, generation)
    }
    pub fn complete_restore(
        &mut self,
        rebuilt_public_state_identity: [u8; 32],
        new_generation: u64,
    ) -> Result<(), LoadError> {
        if self.lifecycle != StrategyInstanceLifecycle::Restoring
            || rebuilt_public_state_identity != self.public_state_identity
            || new_generation <= self.generation
        {
            self.lifecycle = StrategyInstanceLifecycle::Faulted;
            self.command_gate_open = false;
            return Err(LoadError::Contract("public-state reconcile failed"));
        }
        self.generation = new_generation;
        self.lifecycle = StrategyInstanceLifecycle::Ready;
        self.command_gate_open = false;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StrategyStateSnapshot {
    pub checkpoint_id: u64,
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub event_committed_sequence: u64,
    pub strategy_type: Arc<str>,
    pub source_version: Arc<str>,
    pub compiled_strategy_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub abi_version: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_alignment: u32,
    pub state_bytes: Arc<[u8]>,
    pub public_state_identity: [u8; 32],
    pub checksum: [u8; 32],
}
impl StrategyStateSnapshot {
    pub(crate) fn checksum_valid(&self) -> bool {
        self.checksum == snapshot_checksum(self)
    }
}

fn validate_local_numbers(values: &[(u32, u32)]) -> Result<(), LoadError> {
    let mut numbers: Vec<u32> = values.iter().map(|(number, _)| *number).collect();
    numbers.sort_unstable();
    if numbers.iter().copied().ne(0..numbers.len() as u32) {
        return Err(LoadError::Contract(
            "local bindings must be contiguous from zero",
        ));
    }
    Ok(())
}

fn binding_digest(config: &StrategyInstanceConfig) -> [u8; 32] {
    let mut markets = config.markets.clone();
    let mut accounts = config.accounts.clone();
    let mut routes = config.routing_keys.clone();
    markets.sort_unstable();
    accounts.sort_unstable();
    routes.sort_unstable();
    routes.dedup();
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.binding.v13");
    digest.update(config.object_identity_digest);
    for (number, identity) in markets {
        digest.update(number.to_le_bytes());
        digest.update(identity.to_le_bytes());
    }
    digest.update(u64::MAX.to_le_bytes());
    for (number, identity) in accounts {
        digest.update(number.to_le_bytes());
        digest.update(identity.to_le_bytes());
    }
    digest.update(u64::MAX.to_le_bytes());
    for route in routes {
        digest.update(route.to_le_bytes());
    }
    digest.finalize().into()
}

fn resolved_route_digest(
    seed: [u8; 32],
    markets: &[crate::ResolvedMarketBinding],
    accounts: &[crate::ResolvedAccountBinding],
) -> [u8; 32] {
    let mut market_routes: Vec<_> = markets
        .iter()
        .map(|binding| {
            let (mode, timeframe) = match binding.data_mode {
                crate::StrategyDataMode::Tick => (0, 0),
                crate::StrategyDataMode::Bar { timeframe_ns } => (1, timeframe_ns),
                crate::StrategyDataMode::Hybrid {
                    signal_timeframe_ns,
                } => (2, signal_timeframe_ns),
            };
            [
                i64::from(binding.local_market_no),
                i64::from(binding.local_asset_no),
                i64::from(binding.source.source_id.0),
                i64::from(binding.asset_id),
                mode,
                timeframe,
            ]
        })
        .collect();
    let mut account_routes: Vec<_> = accounts
        .iter()
        .flat_map(|binding| {
            binding.tradable_assets.iter().map(move |asset| {
                [
                    binding.local_account_no,
                    binding.account.account_id.0,
                    asset.local_asset_no,
                    asset.asset_id,
                ]
            })
        })
        .collect();
    market_routes.sort_unstable();
    account_routes.sort_unstable();
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.resolved-routes.v13.1");
    digest.update(seed);
    digest.update((market_routes.len() as u64).to_le_bytes());
    for route in market_routes {
        for value in route {
            digest.update(value.to_le_bytes());
        }
    }
    digest.update((account_routes.len() as u64).to_le_bytes());
    for route in account_routes {
        for value in route {
            digest.update(value.to_le_bytes());
        }
    }
    digest.finalize().into()
}

pub(crate) fn snapshot_checksum(snapshot: &StrategyStateSnapshot) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.snapshot.v13");
    for value in [
        snapshot.checkpoint_id,
        snapshot.strategy_instance_id,
        snapshot.generation,
        snapshot.event_committed_sequence,
    ] {
        digest.update(value.to_le_bytes());
    }
    digest.update((snapshot.strategy_type.len() as u64).to_le_bytes());
    digest.update(snapshot.strategy_type.as_bytes());
    digest.update((snapshot.source_version.len() as u64).to_le_bytes());
    digest.update(snapshot.source_version.as_bytes());
    digest.update(snapshot.compiled_strategy_digest);
    digest.update(snapshot.binding_digest);
    digest.update(snapshot.abi_version.to_le_bytes());
    digest.update(snapshot.state_schema_version.to_le_bytes());
    digest.update(snapshot.state_schema_hash);
    digest.update(snapshot.state_alignment.to_le_bytes());
    digest.update((snapshot.state_bytes.len() as u64).to_le_bytes());
    digest.update(&snapshot.state_bytes);
    digest.update(snapshot.public_state_identity);
    digest.finalize().into()
}

pub fn public_state_identity(
    accounts: &[TitanAccountView],
    active_orders: &[TitanActiveOrderView],
    positions: &[TitanPositionView],
    balances: &[TitanBalanceView],
) -> [u8; 32] {
    let mut accounts = accounts.to_vec();
    accounts.sort_by_key(|value| value.account_no);
    let mut active_orders = active_orders.to_vec();
    active_orders.sort_by_key(|value| (value.account_no, value.order_id));
    let mut positions = positions.to_vec();
    positions.sort_by_key(|value| (value.account_no, value.asset_no, value.position_side));
    let mut balances = balances.to_vec();
    balances.sort_by_key(|value| (value.account_no, value.currency_no));
    let mut digest = Sha256::new();
    digest.update(b"titan.public-state.v13");
    digest.update(1_u32.to_le_bytes());
    digest.update((accounts.len() as u64).to_le_bytes());
    for value in accounts {
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.account_epoch.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
        digest.update([value.state, value.reason]);
    }
    digest.update((active_orders.len() as u64).to_le_bytes());
    for value in active_orders {
        digest.update(value.order_id.to_le_bytes());
        digest.update(value.asset_no.to_le_bytes());
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.price_ticks.to_le_bytes());
        digest.update(value.qty_lots.to_le_bytes());
        digest.update(value.cumulative_filled_lots.to_le_bytes());
        // Snapshot capture timestamps are transport metadata and cannot be reconstructed from a
        // venue snapshot after restart. Reconciliation is based on economic/order state instead.
        digest.update(value.account_sequence.to_le_bytes());
        digest.update([
            value.side,
            value.order_type,
            value.time_in_force,
            value.status,
            value.reduce_only,
            value.position_side,
        ]);
    }
    digest.update((positions.len() as u64).to_le_bytes());
    for value in positions {
        digest.update([value.position_side]);
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.asset_no.to_le_bytes());
        digest.update(value.qty_lots.to_le_bytes());
        digest.update(value.average_price_ticks.to_le_bytes());
        digest.update(value.realized_pnl_ticks.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
    }
    digest.update((balances.len() as u64).to_le_bytes());
    for value in balances {
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.currency_no.to_le_bytes());
        digest.update(value.total_units.to_le_bytes());
        digest.update(value.available_units.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
    }
    digest.finalize().into()
}

pub struct NativeStrategyLoader {
    cache_dir: PathBuf,
    max_bundle_entry_bytes: u64,
}

impl NativeStrategyLoader {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            max_bundle_entry_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn inspect(&self, strategy_path: &Path) -> Result<ArtifactMetadata, LoadError> {
        let (manifest_bytes, library) = self.read_pair_or_bundle(strategy_path)?;
        let mut value = parse_cbor(&manifest_bytes)?;
        // The signature was removed; older producers may still emit an inert `signature`
        // field, which is dropped before the canonical digest is computed.
        let _ = map_remove(&mut value, "signature");
        let canonical = canonical_cbor(&value)?;
        let compiled_strategy_digest: [u8; 32] = Sha256::digest(&canonical).into();
        let manifest = parse_manifest(&value, compiled_strategy_digest)?;
        if <[u8; 32]>::from(Sha256::digest(&library)) != manifest.native_library_sha256 {
            return Err(LoadError::Contract("native digest mismatch"));
        }
        if manifest.target_triple.as_ref() != host_target_triple() {
            return Err(LoadError::Contract(
                "compiled_strategy target does not match host",
            ));
        }
        Ok(manifest)
    }

    pub fn load(&self, strategy_path: &Path) -> Result<LoadedNativeStrategy, LoadError> {
        let manifest = self.inspect(strategy_path)?;
        let (_, library_bytes) = self.read_pair_or_bundle(strategy_path)?;
        fs::create_dir_all(&self.cache_dir)?;
        let cached = self
            .cache_dir
            .join(format!("{}.so", hex(&manifest.native_library_sha256)));
        let cache_valid = match fs::read(&cached) {
            Ok(bytes) => <[u8; 32]>::from(Sha256::digest(&bytes)) == manifest.native_library_sha256,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if !cache_valid {
            let temporary = cached.with_extension(format!("{}.tmp", std::process::id()));
            fs::write(&temporary, &library_bytes)?;
            fs::rename(temporary, &cached)?;
        }
        let library = unsafe { Library::new(&cached)? };
        let abi_version = unsafe {
            library.get::<unsafe extern "C" fn() -> u32>(b"titan_strategy_abi_version\0")?
        };
        if unsafe { abi_version() } != STRATEGY_ABI {
            return Err(LoadError::Contract("native ABI version mismatch"));
        }
        let descriptor_fn = unsafe {
            library.get::<unsafe extern "C" fn() -> *const NativeStrategyDescriptor>(
                b"titan_strategy_descriptor\0",
            )?
        };
        let descriptor_ptr = unsafe { descriptor_fn() };
        if descriptor_ptr.is_null() {
            return Err(LoadError::Contract("native descriptor is null"));
        }
        let descriptor = unsafe { *descriptor_ptr };
        validate_descriptor(&descriptor, &manifest)?;
        let mut callbacks = [None; CALLBACK_COUNT];
        for (index, name) in CALLBACK_NAMES.iter().enumerate() {
            let declared = manifest.callback_mask & (1 << index) != 0;
            let symbol = unsafe {
                library.get::<StrategyCallback>(format!("titan_strategy_{name}\0").as_bytes())
            };
            match (declared, symbol) {
                (true, Ok(value)) => callbacks[index] = Some(*value),
                (true, Err(_)) => {
                    return Err(LoadError::Contract("declared callback symbol is missing"));
                }
                (false, Ok(_)) => {
                    return Err(LoadError::Contract(
                        "undeclared callback symbol is exported",
                    ));
                }
                (false, Err(_)) => {}
            }
        }
        let initial_state =
            AlignedStateMemory::from_bytes(manifest.state, &manifest.initial_state)?;
        Ok(LoadedNativeStrategy {
            manifest,
            descriptor,
            callbacks: CallbackRegistry { callbacks },
            initial_state,
            _lease: Arc::new(NativeLibraryLease { _library: library }),
        })
    }

    fn read_pair_or_bundle(&self, path: &Path) -> Result<(Vec<u8>, Vec<u8>), LoadError> {
        if path.extension().and_then(|value| value.to_str()) == Some("titan") {
            let bytes = fs::read(path)?;
            let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
                .map_err(|_| LoadError::Bundle("invalid zip"))?;
            if archive.len() != 2 {
                return Err(LoadError::Bundle("bundle must contain two files"));
            }
            let mut manifest = None;
            let mut library = None;
            let mut names = BTreeSet::new();
            for index in 0..archive.len() {
                let mut entry = archive
                    .by_index(index)
                    .map_err(|_| LoadError::Bundle("invalid entry"))?;
                let name = entry.name().to_owned();
                if !safe_entry_name(&name)
                    || !names.insert(name.clone())
                    || entry.size() > self.max_bundle_entry_bytes
                {
                    return Err(LoadError::Bundle("unsafe entry"));
                }
                let mut bytes = Vec::with_capacity(entry.size() as usize);
                entry.read_to_end(&mut bytes)?;
                if name.ends_with(".manifest.cbor") {
                    manifest = Some(bytes);
                } else if name.ends_with(".so") {
                    library = Some(bytes);
                } else {
                    return Err(LoadError::Bundle("unknown entry"));
                }
            }
            return Ok((
                manifest.ok_or(LoadError::Bundle("manifest missing"))?,
                library.ok_or(LoadError::Bundle("library missing"))?,
            ));
        }
        let manifest_path = if path.to_string_lossy().ends_with(".manifest.cbor") {
            path.to_path_buf()
        } else {
            path.with_extension("manifest.cbor")
        };
        let manifest = fs::read(&manifest_path)?;
        let value = parse_cbor(&manifest)?;
        let library_name = value_text(map_get(&value, "native_library")?)?;
        if !safe_entry_name(library_name) {
            return Err(LoadError::Contract("unsafe native library name"));
        }
        let library = fs::read(
            manifest_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(library_name),
        )?;
        Ok((manifest, library))
    }
}

const CALLBACK_NAMES: [&str; CALLBACK_COUNT] = [
    "on_start",
    "on_tick",
    "on_bar",
    "on_depth",
    "on_fill",
    "on_order",
    "on_cancel",
    "on_position",
    "on_balance",
    "on_account_state",
    "on_timer",
    "on_stop",
    "on_recover",
];

fn validate_descriptor(
    descriptor: &NativeStrategyDescriptor,
    manifest: &ArtifactMetadata,
) -> Result<(), LoadError> {
    // Check the ABI identity first and separately: a fingerprint change is the normal consequence
    // of a host ABI revision, and the operator must see "recompile", not a generic state mismatch.
    if descriptor.abi_version != STRATEGY_ABI {
        return Err(LoadError::AbiVersion {
            artifact: descriptor.abi_version,
            host: STRATEGY_ABI,
        });
    }
    if descriptor.abi_fingerprint != ABI_FINGERPRINT {
        return Err(LoadError::AbiFingerprint);
    }
    if descriptor.struct_size as usize != std::mem::size_of::<NativeStrategyDescriptor>() {
        return Err(LoadError::AbiLayout {
            artifact: u64::from(descriptor.struct_size),
            host: std::mem::size_of::<NativeStrategyDescriptor>() as u64,
        });
    }
    if descriptor.state_schema_version != manifest.state.version
        || descriptor.state_alignment != manifest.state.alignment
        || descriptor.state_len != manifest.state.byte_len as u64
        || descriptor.state_schema_hash != manifest.state.hash
        || descriptor.callback_mask != manifest.callback_mask
        || descriptor.callback_mask >> CALLBACK_COUNT != 0
    {
        return Err(LoadError::Contract(
            "native descriptor state or callback layout does not match manifest",
        ));
    }
    Ok(())
}

// Verify original signed bytes before adapting historical field names.
fn manifest_field<'a>(value: &'a CborValue, name: &str) -> Result<&'a CborValue, LoadError> {
    let legacy = match name {
        "strategy_format_version" => "artifact_format_version",
        "strategy_type" => "strategy_id",
        "source_version" => "strategy_version",
        "recovery_state_schema" => "state_schema",
        "recovery_state_schema_version" => "state_schema_version",
        "recovery_state_schema_hash" => "state_schema_hash",
        "native_library_sha256" => "native_digest",
        other => other,
    };
    if name != legacy && map_get(value, name).is_ok() && map_get(value, legacy).is_ok() {
        return Err(LoadError::Contract("ambiguous strategy metadata aliases"));
    }
    map_get(value, name).or_else(|_| map_get(value, legacy))
}

fn parse_manifest(
    value: &CborValue,
    compiled_strategy_digest: [u8; 32],
) -> Result<ArtifactMetadata, LoadError> {
    if !matches!(
        value_u64(manifest_field(value, "strategy_format_version")?)?,
        1 | 2
    ) {
        return Err(LoadError::Contract("unsupported strategy format version"));
    }
    let artifact_abi = value_u64(map_get(value, "abi_version")?)?;
    if artifact_abi != u64::from(STRATEGY_ABI) {
        return Err(LoadError::AbiVersion {
            artifact: u32::try_from(artifact_abi).unwrap_or(u32::MAX),
            host: STRATEGY_ABI,
        });
    }
    if value_bytes32(map_get(value, "abi_fingerprint")?)? != ABI_FINGERPRINT {
        return Err(LoadError::AbiFingerprint);
    }
    let state_len = value_u64(map_get(value, "state_len")?)? as usize;
    let initial_state = value_bytes(map_get(value, "initial_state")?)?;
    if initial_state.len() != state_len {
        return Err(LoadError::Contract("initial state length mismatch"));
    }
    let subscriptions = value_array(map_get(value, "subscriptions")?)?
        .iter()
        .map(|subscription| {
            let event =
                EventKind::from_manifest_name(value_text(map_get(subscription, "event")?)?)?;
            let handler: Arc<str> = Arc::from(value_text(map_get(subscription, "handler")?)?);
            let schema_version =
                u32::try_from(value_u64(map_get(subscription, "schema_version")?)?)
                    .map_err(|_| LoadError::Contract("subscription schema version overflow"))?;
            if schema_version == 0 || handler.as_ref() != CALLBACK_NAMES[event.index()] {
                return Err(LoadError::Contract(
                    "subscription handler contract mismatch",
                ));
            }
            let qos =
                SubscriptionQos::from_manifest_name(value_text(map_get(subscription, "qos")?)?)?;
            if matches!(
                event,
                EventKind::Fill
                    | EventKind::Order
                    | EventKind::Cancel
                    | EventKind::Position
                    | EventKind::Balance
                    | EventKind::AccountState
            ) && qos != SubscriptionQos::ReliableOrdered
            {
                return Err(LoadError::Contract(
                    "account subscription must be reliable ordered",
                ));
            }
            Ok(EventSubscription {
                event,
                handler,
                schema_version,
                qos,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    let mut seen = BTreeSet::new();
    if subscriptions
        .iter()
        .any(|item| !seen.insert(item.event as u32))
    {
        return Err(LoadError::Contract("duplicate subscription event"));
    }
    let mut capabilities = ManifestCapabilities::default();
    for capability in value_array(map_get(value, "capabilities")?)? {
        capabilities.0 |= match value_text(capability)? {
            "market_data" => ManifestCapabilities::MARKET_DATA.0,
            "account_data" => ManifestCapabilities::ACCOUNT_DATA.0,
            "order_execution" => ManifestCapabilities::ORDER_EXECUTION.0,
            "timer" => ManifestCapabilities::TIMER.0,
            "stop_barrier" => ManifestCapabilities::STOP_BARRIER.0,
            "recovery_order_results" => ManifestCapabilities::RECOVERY_ORDER_RESULTS.0,
            "pause_safe" => ManifestCapabilities::PAUSE_SAFE.0,
            "host_depth_100" => ManifestCapabilities::HOST_DEPTH_100.0,
            "business_fault_drain" => ManifestCapabilities::BUSINESS_FAULT_DRAIN.0,
            "requires_periodic_timer" => ManifestCapabilities::REQUIRES_PERIODIC_TIMER.0,
            "multi_venue_execution" => ManifestCapabilities::MULTI_VENUE_EXECUTION.0,
            "object_context" => ManifestCapabilities::OBJECT_CONTEXT.0,
            _ => return Err(LoadError::Contract("unknown strategy capability")),
        };
    }
    // The literal above is intentionally spelled out: it is the frozen compiled strategy manifest vocabulary.
    // Everything downstream (bit translation, runtime gate) goes through
    // [`crate::OBJECT_CONTEXT_CAPABILITY`] / the two typed capability constants so that the
    // capability has exactly one documented meaning.
    if capabilities.contains(ManifestCapabilities::HOST_DEPTH_100)
        && !subscriptions
            .iter()
            .any(|s| s.event == EventKind::Depth && s.qos == SubscriptionQos::ReliableOrdered)
    {
        return Err(LoadError::Contract(
            "host depth requires reliable ordered depth subscription",
        ));
    }
    if capabilities.contains(ManifestCapabilities::BUSINESS_FAULT_DRAIN)
        && !capabilities.contains(ManifestCapabilities::STOP_BARRIER)
    {
        return Err(LoadError::Contract(
            "business fault drain requires stop barrier",
        ));
    }
    if capabilities.contains(ManifestCapabilities::REQUIRES_PERIODIC_TIMER)
        && !subscriptions.iter().any(|s| s.event == EventKind::Timer)
    {
        return Err(LoadError::Contract(
            "periodic timer lacks callback subscription",
        ));
    }
    if capabilities.contains(ManifestCapabilities::STOP_BARRIER)
        && value_u64(map_get(value, "callback_mask")?)? & (1 << EventKind::Stop.index()) == 0
    {
        return Err(LoadError::Contract("stop barrier lacks stop callback"));
    }
    if capabilities.contains(ManifestCapabilities::RECOVERY_ORDER_RESULTS)
        && value_u64(map_get(value, "callback_mask")?)? & (1 << EventKind::Recover.index()) == 0
    {
        return Err(LoadError::Contract(
            "recovery results lack recover callback",
        ));
    }
    if subscriptions.iter().any(|item| {
        matches!(
            item.event,
            EventKind::Tick | EventKind::Bar | EventKind::Depth
        )
    }) && !capabilities.contains(ManifestCapabilities::MARKET_DATA)
    {
        return Err(LoadError::Contract("market subscription lacks capability"));
    }
    if subscriptions.iter().any(|item| {
        matches!(
            item.event,
            EventKind::Fill
                | EventKind::Order
                | EventKind::Cancel
                | EventKind::Position
                | EventKind::Balance
                | EventKind::AccountState
        )
    }) && !capabilities.contains(ManifestCapabilities::ACCOUNT_DATA)
    {
        return Err(LoadError::Contract("account subscription lacks capability"));
    }
    let state = RecoveryStateIdentity {
        version: value_u64(manifest_field(value, "recovery_state_schema_version")?)? as u32,
        hash: value_bytes32(manifest_field(value, "recovery_state_schema_hash")?)?,
        byte_len: state_len,
        alignment: value_u64(map_get(value, "state_alignment")?)? as u32,
    };
    let recovery_state_schema = manifest_field(value, "recovery_state_schema")
        .ok()
        .map(cbor_to_json)
        .transpose()?
        .map(Arc::new);
    if let Some(description) = &recovery_state_schema {
        state.validate_description(description)?;
    }
    let observation = map_get(value, "business_observation")
        .ok()
        .filter(|v| !matches!(v, CborValue::Null))
        .map(|value| {
            let definition: BusinessObservationDefinition =
                serde_json::from_value(cbor_to_json(value)?)
                    .map_err(|_| LoadError::Contract("invalid business observation declaration"))?;
            let description = recovery_state_schema.as_ref().ok_or(LoadError::Contract(
                "business observation lacks recovery description",
            ))?;
            compile_business_observation(&definition, description, state).map(Arc::new)
        })
        .transpose()?;
    let object_manifest = match map_get(value, "object_schema_major") {
        Ok(_) => Some(parse_object_manifest(value)?),
        Err(_) => None,
    };
    Ok(ArtifactMetadata {
        abi_version: u32::try_from(artifact_abi).unwrap_or(u32::MAX),
        strategy_type: Arc::from(value_text(manifest_field(value, "strategy_type")?)?),
        source_version: Arc::from(value_text(manifest_field(value, "source_version")?)?),
        target_triple: Arc::from(value_text(map_get(value, "target_triple")?)?),
        cpu_baseline: Arc::from(value_text(map_get(value, "cpu_baseline")?)?),
        native_library: Arc::from(value_text(map_get(value, "native_library")?)?),
        callback_mask: value_u64(map_get(value, "callback_mask")?)?,
        parameter_schema: Arc::new(cbor_to_json(map_get(value, "parameter_schema")?)?),
        source_digest: map_get(value, "source_digest")
            .ok()
            .map(value_bytes32)
            .transpose()?,
        parameters_digest: value_bytes32(map_get(value, "parameters_digest")?)?,
        object_manifest,
        subscriptions: subscriptions.into(),
        capabilities,
        state,
        recovery_state_schema,
        business_observation: observation,
        initial_state: initial_state.into(),
        native_library_sha256: value_bytes32(manifest_field(value, "native_library_sha256")?)?,
        compiled_strategy_digest,
    })
}

fn map(value: &CborValue) -> Result<&Vec<(CborValue, CborValue)>, LoadError> {
    if let CborValue::Map(value) = value {
        Ok(value)
    } else {
        Err(LoadError::Contract("expected map"))
    }
}
fn map_get<'a>(value: &'a CborValue, name: &str) -> Result<&'a CborValue, LoadError> {
    map(value)?
        .iter()
        .find_map(|(key, value)| (key.as_text() == Some(name)).then_some(value))
        .ok_or(LoadError::Contract("manifest field is missing"))
}
fn map_remove(value: &mut CborValue, name: &str) -> Result<(), LoadError> {
    let CborValue::Map(values) = value else {
        return Err(LoadError::Contract("expected map"));
    };
    let index = values
        .iter()
        .position(|(key, _)| key.as_text() == Some(name))
        .ok_or(LoadError::Contract("manifest field is missing"))?;
    values.remove(index);
    Ok(())
}
fn value_text(value: &CborValue) -> Result<&str, LoadError> {
    value.as_text().ok_or(LoadError::Contract("expected text"))
}
fn value_bytes(value: &CborValue) -> Result<Vec<u8>, LoadError> {
    value
        .as_bytes()
        .map(ToOwned::to_owned)
        .ok_or(LoadError::Contract("expected bytes"))
}
fn value_bytes32(value: &CborValue) -> Result<[u8; 32], LoadError> {
    value_bytes(value)?
        .try_into()
        .map_err(|_| LoadError::Contract("expected 32 bytes"))
}
fn value_u64(value: &CborValue) -> Result<u64, LoadError> {
    let CborValue::Integer(value) = value else {
        return Err(LoadError::Contract("expected integer"));
    };
    u64::try_from(*value).map_err(|_| LoadError::Contract("expected uint64"))
}

fn value_bool(value: &CborValue) -> Result<bool, LoadError> {
    match value {
        CborValue::Bool(value) => Ok(*value),
        CborValue::Integer(value) => Ok(*value != 0),
        _ => Err(LoadError::Contract("expected boolean")),
    }
}

fn parse_object_requirement(value: &CborValue) -> Result<crate::ObjectRequirement, LoadError> {
    Ok(crate::ObjectRequirement {
        domain: u32::try_from(value_u64(map_get(value, "domain")?)?)
            .map_err(|_| LoadError::Contract("object domain overflow"))?,
        major: u16::try_from(value_u64(map_get(value, "major")?)?)
            .map_err(|_| LoadError::Contract("object schema major overflow"))?,
        min_minor: u16::try_from(value_u64(map_get(value, "min_minor")?)?)
            .map_err(|_| LoadError::Contract("object schema minor overflow"))?,
        min_payload_len: value_u64(map_get(value, "min_payload_len")?)?,
        min_count: u32::try_from(value_u64(map_get(value, "min_count")?)?)
            .map_err(|_| LoadError::Contract("object min_count overflow"))?,
        required_fields: value_u64(map_get(value, "required_fields")?)?,
    })
}

fn parse_object_manifest(value: &CborValue) -> Result<crate::ObjectManifest, LoadError> {
    let schema_major = u16::try_from(value_u64(map_get(value, "object_schema_major")?)?)
        .map_err(|_| LoadError::Contract("object schema major overflow"))?;
    let schema_minor = u16::try_from(value_u64(map_get(value, "object_schema_minor")?)?)
        .map_err(|_| LoadError::Contract("object schema minor overflow"))?;
    let schema_hash = match map_get(value, "object_schema_hash") {
        Ok(raw) => Some(value_bytes32(raw)?),
        Err(_) => None,
    };
    let identity_version = match map_get(value, "object_identity_version") {
        Ok(raw) => u32::try_from(value_u64(raw)?)
            .map_err(|_| LoadError::Contract("object identity version overflow"))?,
        Err(_) => 1,
    };
    let requires_fee = match map_get(value, "requires_fee") {
        Ok(raw) => value_bool(raw)?,
        Err(_) => false,
    };
    let requirements = match map_get(value, "object_requirements") {
        Ok(raw) => value_array(raw)?
            .iter()
            .map(parse_object_requirement)
            .collect::<Result<Vec<_>, LoadError>>()?,
        Err(_) => Vec::new(),
    };
    Ok(crate::ObjectManifest {
        schema_major,
        schema_minor,
        schema_hash,
        identity_version,
        requires_fee,
        requirements,
    })
}

fn value_array(value: &CborValue) -> Result<&[CborValue], LoadError> {
    if let CborValue::Array(value) = value {
        Ok(value)
    } else {
        Err(LoadError::Contract("expected array"))
    }
}

fn cbor_to_json(value: &CborValue) -> Result<serde_json::Value, LoadError> {
    Ok(match value {
        CborValue::Integer(value) => {
            let number = serde_json::Number::from_i128(*value)
                .ok_or(LoadError::Contract("JSON integer overflow"))?;
            serde_json::Value::Number(number)
        }
        CborValue::Text(value) => serde_json::Value::String(value.clone()),
        CborValue::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(cbor_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        CborValue::Map(values) => {
            let mut output = serde_json::Map::new();
            for (key, value) in values {
                let key = key
                    .as_text()
                    .ok_or(LoadError::Contract("JSON object key must be text"))?;
                output.insert(key.to_owned(), cbor_to_json(value)?);
            }
            serde_json::Value::Object(output)
        }
        CborValue::Bool(value) => serde_json::Value::Bool(*value),
        CborValue::Null => serde_json::Value::Null,
        CborValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or(LoadError::Contract("non-finite JSON float"))?,
        CborValue::Bytes(_) => {
            return Err(LoadError::Contract(
                "bytes are not valid JSON schema values",
            ));
        }
    })
}

fn canonical_cbor(value: &CborValue) -> Result<Vec<u8>, LoadError> {
    let mut output = Vec::new();
    encode_cbor(value, &mut output)?;
    Ok(output)
}
fn cbor_head(major: u8, value: u64, out: &mut Vec<u8>) {
    match value {
        0..=23 => out.push(major << 5 | value as u8),
        24..=0xff => {
            out.extend([major << 5 | 24, value as u8]);
        }
        0x100..=0xffff => {
            out.push(major << 5 | 25);
            out.extend((value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(major << 5 | 26);
            out.extend((value as u32).to_be_bytes());
        }
        _ => {
            out.push(major << 5 | 27);
            out.extend(value.to_be_bytes());
        }
    }
}
fn encode_cbor(value: &CborValue, out: &mut Vec<u8>) -> Result<(), LoadError> {
    match value {
        CborValue::Integer(value) => {
            if let Ok(value) = u64::try_from(*value) {
                cbor_head(0, value, out);
            } else {
                cbor_head(1, (-1 - *value) as u64, out);
            }
        }
        CborValue::Bytes(value) => {
            cbor_head(2, value.len() as u64, out);
            out.extend(value);
        }
        CborValue::Text(value) => {
            cbor_head(3, value.len() as u64, out);
            out.extend(value.as_bytes());
        }
        CborValue::Array(values) => {
            cbor_head(4, values.len() as u64, out);
            for value in values {
                encode_cbor(value, out)?;
            }
        }
        CborValue::Map(values) => {
            let mut encoded = values
                .iter()
                .map(|(key, value)| Ok((canonical_cbor(key)?, canonical_cbor(value)?)))
                .collect::<Result<Vec<_>, LoadError>>()?;
            encoded.sort_by(|left, right| {
                left.0
                    .len()
                    .cmp(&right.0.len())
                    .then_with(|| left.0.cmp(&right.0))
            });
            cbor_head(5, encoded.len() as u64, out);
            for (key, value) in encoded {
                out.extend(key);
                out.extend(value);
            }
        }
        CborValue::Bool(false) => out.push(0xf4),
        CborValue::Bool(true) => out.push(0xf5),
        CborValue::Null => out.push(0xf6),
        CborValue::Float(value) => {
            out.push(0xfb);
            out.extend(value.to_be_bytes());
        }
    }
    Ok(())
}

fn parse_cbor(data: &[u8]) -> Result<CborValue, LoadError> {
    let (value, offset) = parse_cbor_at(data, 0)?;
    if offset != data.len() {
        return Err(LoadError::Cbor("trailing bytes".into()));
    }
    Ok(value)
}

fn cbor_argument(data: &[u8], offset: usize, additional: u8) -> Result<(u64, usize), LoadError> {
    if additional < 24 {
        return Ok((u64::from(additional), offset));
    }
    let width = match additional {
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return Err(LoadError::Cbor("indefinite CBOR is forbidden".into())),
    };
    let end = offset
        .checked_add(width)
        .ok_or_else(|| LoadError::Cbor("CBOR overflow".into()))?;
    let bytes = data
        .get(offset..end)
        .ok_or_else(|| LoadError::Cbor("truncated CBOR".into()))?;
    let mut padded = [0_u8; 8];
    padded[8 - width..].copy_from_slice(bytes);
    let value = u64::from_be_bytes(padded);
    if (additional == 24 && value < 24)
        || (additional == 25 && value <= 0xff)
        || (additional == 26 && value <= 0xffff)
        || (additional == 27 && value <= 0xffff_ffff)
    {
        return Err(LoadError::Cbor("non-minimal integer".into()));
    }
    Ok((value, end))
}

fn parse_cbor_at(data: &[u8], mut offset: usize) -> Result<(CborValue, usize), LoadError> {
    let initial = *data
        .get(offset)
        .ok_or_else(|| LoadError::Cbor("truncated CBOR".into()))?;
    offset += 1;
    let major = initial >> 5;
    let additional = initial & 31;
    if major == 7 {
        return match additional {
            20 => Ok((CborValue::Bool(false), offset)),
            21 => Ok((CborValue::Bool(true), offset)),
            22 => Ok((CborValue::Null, offset)),
            27 => {
                let end = offset + 8;
                let bytes: [u8; 8] = data
                    .get(offset..end)
                    .ok_or_else(|| LoadError::Cbor("truncated float".into()))?
                    .try_into()
                    .unwrap();
                Ok((CborValue::Float(f64::from_be_bytes(bytes)), end))
            }
            _ => Err(LoadError::Cbor("unsupported simple value".into())),
        };
    }
    let (argument, mut offset) = cbor_argument(data, offset, additional)?;
    match major {
        0 => Ok((CborValue::Integer(i128::from(argument)), offset)),
        1 => Ok((CborValue::Integer(-1 - i128::from(argument)), offset)),
        2 | 3 => {
            let size = usize::try_from(argument)
                .map_err(|_| LoadError::Cbor("string too large".into()))?;
            let end = offset
                .checked_add(size)
                .ok_or_else(|| LoadError::Cbor("string overflow".into()))?;
            let bytes = data
                .get(offset..end)
                .ok_or_else(|| LoadError::Cbor("truncated string".into()))?;
            if major == 2 {
                Ok((CborValue::Bytes(bytes.to_vec()), end))
            } else {
                Ok((
                    CborValue::Text(
                        std::str::from_utf8(bytes)
                            .map_err(|_| LoadError::Cbor("invalid UTF-8".into()))?
                            .to_owned(),
                    ),
                    end,
                ))
            }
        }
        4 => {
            let mut values = Vec::new();
            for _ in 0..argument {
                let (value, next) = parse_cbor_at(data, offset)?;
                values.push(value);
                offset = next;
            }
            Ok((CborValue::Array(values), offset))
        }
        5 => {
            let mut values = Vec::new();
            let mut previous: Option<(usize, Vec<u8>)> = None;
            for _ in 0..argument {
                let start = offset;
                let (key, next) = parse_cbor_at(data, offset)?;
                let encoded = data[start..next].to_vec();
                let order = (encoded.len(), encoded.clone());
                if previous.as_ref().is_some_and(|value| order <= *value) {
                    return Err(LoadError::Cbor("map keys are not deterministic".into()));
                }
                previous = Some(order);
                offset = next;
                let (value, next) = parse_cbor_at(data, offset)?;
                values.push((key, value));
                offset = next;
            }
            Ok((CborValue::Map(values), offset))
        }
        _ => Err(LoadError::Cbor("unsupported CBOR major type".into())),
    }
}

fn safe_entry_name(name: &str) -> bool {
    let path = Path::new(name);
    !name.is_empty()
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}
fn host_target_triple() -> &'static str {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        "x86_64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        "aarch64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        "aarch64-apple-darwin"
    }
    #[cfg(not(any(
        all(target_arch = "x86_64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "macos")
    )))]
    {
        "unsupported"
    }
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

const _: () = {
    assert!(std::mem::size_of::<NativeStrategyDescriptor>() == 96);
};

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the reserved-field rules documented above the view definitions: every `reserved*`
    /// field is an always-zero placeholder except `TitanSubmitOrderRequest.reserved[0]`, which
    /// carries the staged submit's [`CommandPurpose`](crate::CommandPurpose).
    #[test]
    fn reserved_fields_are_zero_except_the_command_purpose_slot() {
        // (a) Borrowed views: every reserved field defaults to zero and stays zero after serde.
        assert_eq!(TitanTickView::default().reserved, [0; 2]);
        assert_eq!(TitanBarView::default().reserved, 0);
        assert_eq!(TitanDepthView::default().reserved, [0; 5]);
        assert_eq!(TitanFillView::default().reserved, [0; 5]);
        assert_eq!(TitanOrderEventView::default().reserved, [0; 5]);
        assert_eq!(TitanCancelEventView::default().reserved, [0; 6]);
        assert_eq!(TitanPositionEventView::default().reserved, [0; 7]);
        assert_eq!(TitanAccountStateEventView::default().reserved0, 0);
        assert_eq!(TitanAccountStateEventView::default().reserved, [0; 6]);
        assert_eq!(TitanPositionView::default().reserved, [0; 7]);
        assert_eq!(TitanAccountView::default().reserved, 0);
        assert_eq!(TitanAccountView::default().reserved2, [0; 6]);
        assert_eq!(TitanActiveOrderView::default().reserved, [0; 2]);
        // `TitanMarketView` carries no reserved bytes: its deprecated `tick_size`/`lot_size` slots
        // hold `DEPRECATED_UNIT_PLACEHOLDER`, never zero, and are documented separately above.
        assert_eq!(
            TitanMarketView::default().tick_size,
            TitanMarketView::default().lot_size
        );
        let mut placeholder_market = [TitanMarketView {
            asset_no: 7,
            ..Default::default()
        }];
        assert!(apply_deprecated_unit_placeholders(&mut placeholder_market));
        assert_eq!(placeholder_market[0].tick_size, DEPRECATED_UNIT_PLACEHOLDER);
        assert_eq!(placeholder_market[0].lot_size, DEPRECATED_UNIT_PLACEHOLDER);

        // The compiled strategy root context reserved slot (frozen offset) is zero as well.

        // Reserved bytes are not rejected, rewritten or "repaired" by serde: a producer that
        // (incorrectly) writes into a *placeholder* is not silently reinterpreted. Omitted bytes
        // default back to zero.
        let round_tripped: TitanPositionEventView =
            serde_json::from_str(r#"{"asset_no":1,"reserved":[9,9,9,9,9,9,9]}"#)
                .expect("unknown-free JSON is accepted; reserved is just bytes");
        assert_eq!(round_tripped.reserved, [9; 7]);
        let defaulted: TitanPositionEventView =
            serde_json::from_str(r#"{"asset_no":1}"#).expect("defaulted reserved");
        assert_eq!(defaulted.reserved, [0; 7]);

        // (b) The one live exception. An all-zero request means "no purpose declared", which is
        // exactly `CommandPurpose::Unspecified`; the byte is part of the frozen layout.
        const _: () = assert!(crate::COMMAND_PURPOSE_INDEX == 0);
        const _: () = assert!(crate::COMMAND_PURPOSE_INDEX < 2);
        let plain = TitanSubmitOrderRequest::default();
        assert_eq!(plain.reserved, [0; 2]);
        assert_eq!(
            crate::CommandPurpose::from_byte(plain.reserved[crate::COMMAND_PURPOSE_INDEX]),
            Some(crate::CommandPurpose::Unspecified)
        );
        assert_eq!(crate::CommandPurpose::Unspecified.as_byte(), 0);

        let mut hedge = TitanSubmitOrderRequest {
            side: 1,
            order_type: 1,
            ..Default::default()
        };
        // Same writer the live runtime and the tests use; no new byte API is introduced.
        hedge.reserved[crate::COMMAND_PURPOSE_INDEX] =
            crate::CommandPurpose::KnownFillHedge.as_byte();
        assert_eq!(hedge.reserved[crate::COMMAND_PURPOSE_INDEX], 2);
        assert_eq!(
            crate::CommandPurpose::from_byte(hedge.reserved[crate::COMMAND_PURPOSE_INDEX]),
            Some(crate::CommandPurpose::KnownFillHedge)
        );
        // The purpose byte is the only byte that moves; the trailing placeholder stays zero.
        assert_eq!(hedge.reserved[1], 0);
        // Every other field, reserved ones included, is untouched by the purpose writer.
        assert_eq!(hedge.asset_no, plain.asset_no);
        assert_eq!(hedge.account_no, plain.account_no);
        assert_eq!(hedge.price_ticks, plain.price_ticks);
        assert_eq!(hedge.qty_lots, plain.qty_lots);
        // The purpose survives being copied the way `stage_submit_order` copies a request.
        let staged = hedge;
        assert_eq!(
            crate::CommandPurpose::from_byte(staged.reserved[crate::COMMAND_PURPOSE_INDEX]),
            Some(crate::CommandPurpose::KnownFillHedge)
        );
    }

    #[test]
    fn closed_recovery_staging_remembers_rejected_command_attempts() {
        let mut staging = CallbackCommandStaging::new(2, 5, 2).unwrap();
        staging.begin_callback(false, &[]);
        let mut context = StrategyRuntimeContext::default();
        staging.bind_context(&mut context);
        let request = TitanSubmitOrderRequest {
            qty_lots: 1,
            price_ticks: 100,
            side: 1,
            order_type: 1,
            time_in_force: 3,
            ..Default::default()
        };
        let mut id = 0;
        assert_ne!(
            unsafe { context.submit_order.unwrap()(context.command_context, &request, &mut id) },
            0
        );
        assert!(staging.attempted);
        assert!(staging.finish_callback(0).is_empty());
        assert_eq!(id, 0);
        staging.begin_callback(false, &[]);
        assert!(!staging.attempted);
        assert_ne!(
            unsafe {
                context.cancel_order.unwrap()(
                    context.command_context,
                    &TitanCancelOrderRequest::default(),
                    &mut id,
                )
            },
            0
        );
        assert!(staging.attempted);
        assert!(staging.finish_callback(0).is_empty());
    }

    #[test]
    #[ignore = "requires explicit previous-binary synthetic recovery and signed strategy fixtures"]
    fn historical_recovery_point_loads_original_native_strategy() {
        use crate::*;
        let bytes =
            std::fs::read(std::env::var("TITAN_TEST_PREVIOUS_RECOVERY_POINT").unwrap()).unwrap();
        let legacy: StrategyRecoveryPoint = serde_json::from_slice(&bytes).unwrap();
        let recovery_root = tempfile::tempdir().unwrap();
        let store = SqliteRecoveryStore::open(SqliteRecoveryConfig::new(
            recovery_root.path().join("runtime.db"),
        ))
        .unwrap();
        let reference = store.import_legacy(&bytes, legacy.generation).unwrap();
        assert_eq!(store.payload(&reference).unwrap(), bytes);
        let point = store.load_reference(&reference).unwrap();
        assert!(point.native_snapshot().checksum_valid());
        let root = tempfile::tempdir().unwrap();
        let loader = NativeStrategyLoader::new(root.path().into());
        let compiled = loader
            .load(std::path::Path::new(
                &std::env::var("TITAN_TEST_PREVIOUS_STRATEGY_FILE").unwrap(),
            ))
            .unwrap();
        let config = StrategyInstanceConfig {
            strategy_instance_id: 77,
            object_identity_digest: [0; 32],
            markets: vec![(0, 1)],
            accounts: vec![(0, 1)],
            routing_keys: vec![1, 1],
            max_commands_per_callback: 64,
            max_handler_duration: std::time::Duration::from_secs(5),
        };
        let mut native = compiled
            .instantiate_with_config(&config, point.generation + 1)
            .unwrap();
        native.bind_resolved_routes(
            &[ResolvedMarketBinding {
                source_stream: titan_market_service::SourceStreamId(2),
                local_market_no: 0,
                local_asset_no: 0,
                source: titan_market_service::MarketSourceHandle {
                    source_id: titan_market_service::MarketSourceId(1),
                    generation: 1,
                },
                asset_id: 1,
                data_mode: StrategyDataMode::Tick,
            }],
            &[ResolvedAccountBinding {
                local_account_no: 0,
                account: titan_account_service::AccountHandle {
                    account_id: titan_account_service::AccountId(1),
                    generation: 1,
                },
                tradable_assets: Arc::from([StrategyTradableAsset {
                    local_asset_no: 0,
                    asset_id: 1,
                }]),
            }],
        );
        let snapshot = point.native_snapshot();
        native.restore_state(&snapshot).unwrap();
        assert_eq!(native.state.as_bytes(), point.state_bytes.as_ref());
        assert!(!native.command_gate_open());
        assert_eq!(native.lifecycle, StrategyInstanceLifecycle::Restoring);
        let mut successor = compiled
            .instantiate_with_config(&config, point.generation + 1)
            .unwrap();
        // An unbound instance and a tampered source remain incompatible.
        assert!(successor.restore_state(&snapshot).is_err());
        let mut bad = snapshot;
        let mut state = bad.state_bytes.to_vec();
        state[0] ^= 1;
        bad.state_bytes = state.into();
        assert!(!bad.checksum_valid());
        println!(
            "previous-binary signed grid and recovery bytes loaded unchanged; state schema {}, ABI {}; trading gate remains closed",
            point.state_schema_version, point.abi_version
        );
    }

    #[test]
    fn recovery_bindings_cover_execution_asset_routes_and_ignore_session_generations() {
        let mut account = crate::ResolvedAccountBinding {
            local_account_no: 0,
            account: titan_account_service::AccountHandle {
                account_id: titan_account_service::AccountId(8),
                generation: 1,
            },
            tradable_assets: std::sync::Arc::from([crate::StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 10,
            }]),
        };
        let original = resolved_route_digest([0; 32], &[], &[account.clone()]);
        account.account.generation = 2;
        assert_eq!(
            original,
            resolved_route_digest([0; 32], &[], &[account.clone()])
        );
        account.tradable_assets = std::sync::Arc::from([crate::StrategyTradableAsset {
            local_asset_no: 0,
            asset_id: 11,
        }]);
        assert_ne!(original, resolved_route_digest([0; 32], &[], &[account]));
    }

    #[test]
    fn aligned_state_is_independent_and_hides_tail_padding() {
        let schema = RecoveryStateIdentity {
            version: 1,
            hash: [7; 32],
            byte_len: 13,
            alignment: 8,
        };
        let initial: Vec<u8> = (0..13).collect();
        let state = AlignedStateMemory::from_bytes(schema, &initial).unwrap();
        assert_eq!(state.as_ptr() as usize % 8, 0);
        assert_eq!(state.as_bytes(), initial);
        let mut clone = state.clone_for_instance();
        clone.as_bytes_mut()[0] = 99;
        assert_eq!(state.as_bytes()[0], 0);
    }

    #[test]
    fn hedge_staging_preserves_side_and_normalizes_closing_semantics() {
        let mut staging = CallbackCommandStaging::new(2, 5, 1).unwrap();
        let mut context = StrategyRuntimeContext::default();
        staging.begin_callback(true, &[]);
        staging.bind_context(&mut context);
        let mut request = TitanSubmitOrderRequest {
            qty_lots: 1,
            side: 1,
            position_side: 2,
            order_type: 1,
            time_in_force: 1,
            ..Default::default()
        };
        let mut order_id = 0;
        assert_eq!(
            unsafe {
                context.submit_order.unwrap()(context.command_context, &request, &mut order_id)
            },
            0
        );
        let commands = staging.finish_callback(0);
        match commands[0] {
            StagedCommand::Submit { request, .. } => {
                assert_eq!(request.position_side, 2);
                assert_eq!(request.reduce_only, 1);
            }
            _ => panic!("expected submit"),
        }
        staging.begin_callback(true, &[]);
        request.position_side = 1;
        request.reduce_only = 1;
        assert_eq!(
            unsafe {
                context.submit_order.unwrap()(context.command_context, &request, &mut order_id)
            },
            -2
        );
        assert!(staging.finish_callback(0).is_empty());
    }

    #[test]
    fn paused_command_staging_allows_cancels_and_venue_reduce_only_but_rejects_opening() {
        let active = [TitanActiveOrderView {
            order_id: 9,
            asset_no: 0,
            account_no: 0,
            ..Default::default()
        }];
        let mut staging = CallbackCommandStaging::new(4, 42, 1).unwrap();
        let mut context = StrategyRuntimeContext::default();
        staging.begin_callback(true, &active);
        staging.reducing_only = true;
        staging.bind_context(&mut context);
        let mut request = TitanSubmitOrderRequest {
            qty_lots: 1,
            price_ticks: 100,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            ..Default::default()
        };
        let mut id = 0;
        assert_eq!(
            unsafe { context.submit_order.unwrap()(context.command_context, &request, &mut id) },
            -2
        );
        request.reduce_only = 1;
        assert_eq!(
            unsafe { context.submit_order.unwrap()(context.command_context, &request, &mut id) },
            0
        );
        let cancel = TitanCancelOrderRequest {
            order_id: 9,
            asset_no: 0,
            account_no: 0,
        };
        assert_eq!(
            unsafe { context.cancel_order.unwrap()(context.command_context, &cancel, &mut id) },
            0
        );
        assert_eq!(staging.finish_callback(0).len(), 2);
    }

    #[test]
    fn command_staging_is_atomic_and_cancel_checks_ownership() {
        let active = [TitanActiveOrderView {
            order_id: 9,
            asset_no: 2,
            account_no: 1,
            ..Default::default()
        }];
        let mut staging = CallbackCommandStaging::new(2, 5, 1).unwrap();
        let mut context = StrategyRuntimeContext::default();
        staging.begin_callback(true, &active);
        staging.bind_context(&mut context);
        let request = TitanSubmitOrderRequest {
            account_no: 1,
            asset_no: 2,
            qty_lots: 3,
            price_ticks: 100,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            ..Default::default()
        };
        let mut order_id = 0;
        assert_eq!(
            unsafe {
                context.submit_order.unwrap()(context.command_context, &request, &mut order_id)
            },
            0
        );
        assert_eq!(order_id, 1);
        let cancel = TitanCancelOrderRequest {
            order_id: 9,
            account_no: 1,
            asset_no: 2,
        };
        let mut command_id = 0;
        assert_eq!(
            unsafe {
                context.cancel_order.unwrap()(context.command_context, &cancel, &mut command_id)
            },
            0
        );
        assert_eq!(
            staging.finish_callback(-1).len(),
            0,
            "failed callbacks discard the whole batch"
        );
    }

    #[test]
    fn command_ids_do_not_overlap_between_generations() {
        let first = CallbackCommandStaging::new(2, 5, 1).unwrap();
        let second = CallbackCommandStaging::new(2, 5, 2).unwrap();
        assert_eq!(first.next_order_id, 1);
        assert_eq!(second.next_order_id, (1_u64 << 32) | 1);
        assert_ne!(first.next_order_id, second.next_order_id);
    }

    #[test]
    fn public_identity_is_order_independent_but_fact_sensitive() {
        let first = TitanActiveOrderView {
            order_id: 2,
            account_no: 1,
            qty_lots: 3,
            ..Default::default()
        };
        let second = TitanActiveOrderView {
            order_id: 1,
            account_no: 1,
            qty_lots: 4,
            ..Default::default()
        };
        let left = public_state_identity(&[], &[first, second], &[], &[]);
        let right = public_state_identity(&[], &[second, first], &[], &[]);
        assert_eq!(left, right);
        let changed = TitanActiveOrderView {
            qty_lots: 5,
            ..second
        };
        assert_ne!(
            left,
            public_state_identity(&[], &[first, changed], &[], &[])
        );
    }
}

/// Bidirectional ABI layout guard.
///
/// `ABI_FINGERPRINT` is a hash of the canonical descriptor produced by the Python SDK over numpy
/// dtypes. Until this test existed the guard was one-way: Python hashed its own dtypes and Rust
/// compared the result with a hard-coded constant, so a Rust-side layout edit that left the
/// constant untouched was invisible until a strategy crashed.
///
/// `fixtures/abi_descriptor.json` is the SDK's canonical bytes (a Python test asserts equality
/// with `canonical_abi_descriptor()`). This test proves that
/// 1. the fixture hashes to the Rust constant, and
/// 2. every struct size and field offset in the fixture equals the real Rust layout.
///
/// A one-sided edit therefore fails on the side that was not edited.
#[cfg(test)]
mod layout_tests {
    use super::*;

    fn fixture() -> (&'static str, serde_json::Value) {
        let text = include_str!("../../../fixtures/abi_descriptor.json");
        (
            text,
            serde_json::from_str(text).expect("descriptor fixture is valid JSON"),
        )
    }

    fn field_offset(document: &serde_json::Value, name: &str, field: &str) -> u64 {
        document["structs"][name]["fields"]
            .as_array()
            .unwrap_or_else(|| panic!("{name} has no field list"))
            .iter()
            .find(|entry| entry["name"].as_str() == Some(field))
            .unwrap_or_else(|| panic!("{name} has no field {field}"))["offset"]
            .as_u64()
            .expect("offset is an integer")
    }

    macro_rules! assert_layout {
        ($document:expr, $name:literal, $rust:ty { $($field:ident),* $(,)? }) => {{
            let entry = &$document["structs"][$name];
            assert_eq!(
                entry["size"].as_u64().unwrap(),
                std::mem::size_of::<$rust>() as u64,
                "size drift for {}",
                $name
            );
            $(
                assert_eq!(
                    field_offset(&$document, $name, stringify!($field)),
                    std::mem::offset_of!($rust, $field) as u64,
                    "offset drift for {}.{}",
                    $name,
                    stringify!($field)
                );
            )*
        }};
    }

    #[test]
    fn canonical_descriptor_fixture_matches_rust_fingerprint_and_layout() {
        let (text, document) = fixture();
        // Historical compiled strategy fixture: it pins the retired layout and fingerprint, not the live ABI.
        assert_eq!(
            document["abi_version"].as_u64(),
            Some(u64::from(STRATEGY_ABI))
        );
        let digest: [u8; 32] = Sha256::digest(text.as_bytes()).into();
        assert_eq!(
            digest, ABI_FINGERPRINT,
            "fixture fingerprint != compiled strategy constant: historical layout drifted"
        );

        assert_layout!(
            document,
            "NativeStrategyDescriptor",
            NativeStrategyDescriptor {
                struct_size,
                abi_version,
                abi_fingerprint,
                state_schema_version,
                state_alignment,
                state_len,
                state_schema_hash,
                callback_mask,
            }
        );
        assert_layout!(
            document,
            "TitanTickView",
            TitanTickView {
                asset_no,
                kind,
                side,
                reserved,
                exchange_ts_ns,
                receive_ts_ns,
                price_ticks,
                qty_lots,
                source_sequence,
            }
        );
        assert_layout!(
            document,
            "TitanBarView",
            TitanBarView {
                asset_no,
                reserved,
                timeframe_ns,
                open_ts_ns,
                close_ts_ns,
                open_ticks,
                high_ticks,
                low_ticks,
                close_ticks,
                volume_lots,
            }
        );
        assert_layout!(
            document,
            "TitanDepthView",
            TitanDepthView {
                asset_no,
                level,
                exchange_ts_ns,
                receive_ts_ns,
                price_ticks,
                qty_lots,
                source_sequence,
                side,
                action,
                is_snapshot,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanFillView",
            TitanFillView {
                order_id,
                asset_no,
                account_no,
                fill_price_ticks,
                fill_qty_lots,
                cumulative_filled_lots,
                exchange_ts_ns,
                receive_ts_ns,
                account_sequence,
                side,
                liquidity,
                final_fill,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanOrderEventView",
            TitanOrderEventView {
                order_id,
                asset_no,
                account_no,
                price_ticks,
                qty_lots,
                cumulative_filled_lots,
                event_ts_ns,
                account_sequence,
                status,
                reason,
                position_side,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanCancelEventView",
            TitanCancelEventView {
                order_id,
                asset_no,
                account_no,
                event_ts_ns,
                account_sequence,
                request_result,
                final_status,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanPositionEventView",
            TitanPositionEventView {
                asset_no,
                account_no,
                qty_lots,
                average_price_ticks,
                realized_pnl_ticks,
                event_ts_ns,
                account_sequence,
                position_side,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanBalanceEventView",
            TitanBalanceEventView {
                account_no,
                currency_no,
                total_units,
                available_units,
                event_ts_ns,
                account_sequence,
            }
        );
        assert_layout!(
            document,
            "TitanAccountStateEventView",
            TitanAccountStateEventView {
                account_no,
                account_epoch,
                event_ts_ns,
                account_sequence,
                state,
                reason,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanTimerView",
            TitanTimerView {
                timer_id,
                scheduled_ts_ns,
                fired_ts_ns,
            }
        );
        assert_layout!(
            document,
            "TitanMarketView",
            TitanMarketView {
                asset_no,
                flags,
                best_bid_ticks,
                best_bid_qty_lots,
                best_ask_ticks,
                best_ask_qty_lots,
                tick_size,
                lot_size,
                source_sequence,
            }
        );
        assert_layout!(
            document,
            "TitanPositionView",
            TitanPositionView {
                asset_no,
                account_no,
                qty_lots,
                average_price_ticks,
                realized_pnl_ticks,
                account_sequence,
                position_side,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanBalanceView",
            TitanBalanceView {
                account_no,
                currency_no,
                total_units,
                available_units,
                account_sequence,
            }
        );
        assert_layout!(
            document,
            "TitanAccountView",
            TitanAccountView {
                account_no,
                reserved,
                account_epoch,
                account_sequence,
                state,
                reason,
                reserved2,
            }
        );
        assert_layout!(
            document,
            "TitanActiveOrderView",
            TitanActiveOrderView {
                order_id,
                asset_no,
                account_no,
                price_ticks,
                qty_lots,
                cumulative_filled_lots,
                created_ts_ns,
                updated_ts_ns,
                account_sequence,
                side,
                order_type,
                time_in_force,
                status,
                reduce_only,
                position_side,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanSubmitOrderRequest",
            TitanSubmitOrderRequest {
                asset_no,
                account_no,
                price_ticks,
                qty_lots,
                trigger_price_ticks,
                gtd_expiry_ns,
                side,
                order_type,
                time_in_force,
                reduce_only,
                trigger_kind,
                position_side,
                reserved,
            }
        );
        assert_layout!(
            document,
            "TitanCancelOrderRequest",
            TitanCancelOrderRequest {
                order_id,
                asset_no,
                account_no,
            }
        );
    }
}

#[cfg(test)]
mod object_manifest_tests {
    use super::*;

    fn map(entries: &[(&str, CborValue)]) -> CborValue {
        CborValue::Map(
            entries
                .iter()
                .map(|(key, value)| (CborValue::Text((*key).into()), value.clone()))
                .collect(),
        )
    }

    fn requirement(minor: i128) -> CborValue {
        map(&[
            ("domain", CborValue::Integer(1)),
            ("major", CborValue::Integer(1)),
            ("min_minor", CborValue::Integer(minor)),
            ("min_payload_len", CborValue::Integer(152)),
            ("min_count", CborValue::Integer(1)),
            ("required_fields", CborValue::Integer(0b11)),
        ])
    }

    #[test]
    fn object_manifest_is_absent_without_the_capability() {
        let value = map(&[("abi_version", CborValue::Integer(13))]);
        assert!(map_get(&value, "object_schema_major").is_err());
        assert!(parse_object_manifest(&value).is_err());
    }

    #[test]
    fn object_manifest_parses_and_passes_the_artifact_gate() {
        let value = map(&[
            ("object_schema_major", CborValue::Integer(1)),
            ("object_schema_minor", CborValue::Integer(1)),
            ("object_identity_version", CborValue::Integer(1)),
            ("object_schema_hash", CborValue::Bytes(vec![7; 32])),
            ("requires_fee", CborValue::Bool(true)),
            (
                "object_requirements",
                CborValue::Array(vec![requirement(1)]),
            ),
        ]);
        let manifest = parse_object_manifest(&value).unwrap();
        assert_eq!(manifest.schema_major, 1);
        assert_eq!(manifest.schema_minor, 1);
        assert_eq!(manifest.schema_hash, Some([7; 32]));
        assert!(manifest.requires_fee);
        assert_eq!(manifest.requirements.len(), 1);
        assert_eq!(manifest.requirements[0].min_payload_len, 152);
        assert_eq!(manifest.requirements[0].required_fields, 0b11);
        manifest
            .validate_artifact()
            .expect("host supports schema 1/1");

        let newer = map(&[
            ("object_schema_major", CborValue::Integer(1)),
            ("object_schema_minor", CborValue::Integer(2)),
            (
                "object_requirements",
                CborValue::Array(vec![requirement(2)]),
            ),
        ]);
        let manifest = parse_object_manifest(&newer).unwrap();
        assert_eq!(
            manifest.validate_artifact().unwrap_err().code(),
            crate::OBJECT_GATE_SCHEMA_MINOR
        );

        // Undefined symbol field bits are refused by the artifact gate.
        let mut bad = requirement(1);
        if let CborValue::Map(entries) = &mut bad {
            for (key, value) in entries.iter_mut() {
                if matches!(key, CborValue::Text(name) if name == "required_fields") {
                    *value = CborValue::Integer(1 << 40);
                }
            }
        }
        let value = map(&[
            ("object_schema_major", CborValue::Integer(1)),
            ("object_schema_minor", CborValue::Integer(1)),
            ("object_requirements", CborValue::Array(vec![bad])),
        ]);
        let manifest = parse_object_manifest(&value).unwrap();
        assert_eq!(
            manifest.validate_artifact().unwrap_err().code(),
            crate::OBJECT_GATE_REQUIREMENT_INVALID
        );
    }
}

#[cfg(test)]
mod acceptance_tests {
    use super::*;

    fn map(entries: &[(&str, CborValue)]) -> CborValue {
        CborValue::Map(
            entries
                .iter()
                .map(|(key, value)| (CborValue::Text((*key).into()), value.clone()))
                .collect(),
        )
    }

    #[test]
    fn manifest_from_a_retired_abi_is_refused_with_a_recompile_hint() {
        let value = map(&[
            ("strategy_format_version", CborValue::Integer(1)),
            ("abi_version", CborValue::Integer(13)),
            (
                "abi_fingerprint",
                CborValue::Bytes(ABI_FINGERPRINT.to_vec()),
            ),
        ]);
        let error = parse_manifest(&value, [0; 32]).unwrap_err();
        match &error {
            LoadError::AbiVersion { artifact, host } => {
                assert_eq!(*artifact, 13);
                assert_eq!(*host, STRATEGY_ABI);
            }
            other => panic!("unexpected error for a retired artifact: {other}"),
        }
        assert!(error.to_string().contains("recompile"));
    }

    #[test]
    fn deprecated_market_unit_placeholders_stay_constant() {
        // The frozen compiled strategy layout keeps tick_size/lot_size slots, but they must never carry venue
        // data: the constant is the only writer and it is 1.
        assert_eq!(DEPRECATED_UNIT_PLACEHOLDER, 1);
        let view = TitanMarketView::default();
        assert_eq!(view.tick_size, 0, "default views carry no unit claim");
        assert_eq!(view.lot_size, 0);
        let size = std::mem::size_of::<TitanMarketView>();
        assert_eq!(std::mem::offset_of!(TitanMarketView, tick_size), 40);
        assert_eq!(std::mem::offset_of!(TitanMarketView, lot_size), 48);
        assert_eq!(std::mem::offset_of!(TitanMarketView, source_sequence), 56);
        assert_eq!(
            size, 64,
            "market view layout is frozen by the compiled strategy prefix"
        );
    }

    #[test]
    fn deprecated_market_unit_placeholders_have_one_writer_for_both_paths() {
        // The replay and the live runtime both go through this helper, so a strategy that still
        // reads the deprecated slots observes the same values in the two execution paths.
        let mut markets = vec![
            TitanMarketView::default(),
            TitanMarketView {
                asset_no: 1,
                ..TitanMarketView::default()
            },
        ];
        assert!(apply_deprecated_unit_placeholders(&mut markets));
        for market in &markets {
            assert_eq!(market.tick_size, DEPRECATED_UNIT_PLACEHOLDER);
            assert_eq!(market.lot_size, DEPRECATED_UNIT_PLACEHOLDER);
        }
        assert_eq!(markets[1].asset_no, 1, "asset_no is preserved");
        // Views that already claim real units are refused instead of being overwritten.
        let mut derived = vec![TitanMarketView {
            tick_size: 1,
            lot_size: 10,
            ..TitanMarketView::default()
        }];
        assert!(!apply_deprecated_unit_placeholders(&mut derived));
        let mut empty: [TitanMarketView; 0] = [];
        assert!(apply_deprecated_unit_placeholders(&mut empty));
    }

    #[test]
    fn offline_replay_publishes_the_same_deprecated_slots_as_the_live_runtime() {
        let mut markets = vec![TitanMarketView {
            asset_no: 0,
            best_bid_ticks: 99,
            best_ask_ticks: 101,
            ..TitanMarketView::default()
        }];
        // What OfflineAdapter::new does to the trace's market views.
        assert!(apply_deprecated_unit_placeholders(&mut markets));
        // What the live runtime publishes for the same binding.
        let mut live = vec![TitanMarketView {
            asset_no: 0,
            best_bid_ticks: 99,
            best_ask_ticks: 101,
            ..TitanMarketView::default()
        }];
        assert!(apply_deprecated_unit_placeholders(&mut live));
        for (replay, live) in markets.iter().zip(live.iter()) {
            assert_eq!(replay.asset_no, live.asset_no);
            assert_eq!(replay.best_bid_ticks, live.best_bid_ticks);
            assert_eq!(replay.best_ask_ticks, live.best_ask_ticks);
            assert_eq!(replay.tick_size, live.tick_size);
            assert_eq!(replay.lot_size, live.lot_size);
        }
    }

    #[test]
    fn status_lifecycle_mapping_is_frozen() {
        use StrategyInstanceLifecycle::*;
        assert_eq!(
            status_lifecycle_value(Ready, false),
            crate::STATUS_LIFECYCLE_READY
        );
        assert_eq!(
            status_lifecycle_value(Running, false),
            crate::STATUS_LIFECYCLE_RUNNING
        );
        assert_eq!(
            status_lifecycle_value(Running, true),
            crate::STATUS_LIFECYCLE_PAUSED
        );
        assert_eq!(
            status_lifecycle_value(Restoring, false),
            crate::STATUS_LIFECYCLE_RECOVERING
        );
        assert_eq!(
            status_lifecycle_value(Stopped, false),
            crate::STATUS_LIFECYCLE_STOPPED
        );
        assert_eq!(
            status_lifecycle_value(Faulted, false),
            crate::STATUS_LIFECYCLE_FAILED
        );
        let mut empty = crate::ObjectTable::empty();
        assert!(
            empty.try_status_mut().is_none(),
            "the pre-attach table has no status object"
        );
    }

    #[test]
    fn binding_digest_covers_the_object_identity() {
        let base = StrategyInstanceConfig {
            strategy_instance_id: 77,
            object_identity_digest: [0; 32],
            markets: vec![(0, 1)],
            accounts: vec![(0, 1)],
            routing_keys: vec![1],
            max_commands_per_callback: 64,
            max_handler_duration: std::time::Duration::from_secs(5),
        };
        let first = binding_digest(&base);
        let mut changed = base.clone();
        changed.object_identity_digest = [3; 32];
        let second = binding_digest(&changed);
        assert_ne!(
            first, second,
            "unit identity must change the binding digest"
        );
        assert_eq!(first, binding_digest(&base));
        // Restore compares this digest, so a changed object identity rejects the old checkpoint.
        let mut snapshot = StrategyStateSnapshot {
            checkpoint_id: 1,
            strategy_instance_id: 77,
            generation: 1,
            event_committed_sequence: 0,
            strategy_type: Arc::from("sample"),
            source_version: Arc::from("1.0.0"),
            compiled_strategy_digest: [0; 32],
            binding_digest: first,
            abi_version: STRATEGY_ABI,
            state_schema_version: 1,
            state_schema_hash: [0; 32],
            state_alignment: 8,
            state_bytes: Arc::from([0u8; 8]),
            public_state_identity: [0; 32],
            checksum: [0; 32],
        };
        snapshot.checksum = snapshot_checksum(&snapshot);
        assert_ne!(snapshot.binding_digest, second);
        assert!(snapshot.checksum_valid());
    }

    #[test]
    fn public_state_identity_keeps_its_tag_and_layout() {
        let identity = public_state_identity(&[], &[], &[], &[]);
        assert_eq!(
            hex(&identity),
            "50b9905fa70d4c22816f5d824d6111bef7a9a77030859a8d0ac20b48b5707833"
        );
    }
}
