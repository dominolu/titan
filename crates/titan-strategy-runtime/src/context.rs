//! Strategy ABI object context: frozen root/reference layout and versioned payloads.
//!
//! V14 appends an object-table view to the V13 callback context. Two layers exist:
//!
//! - the **root** ([`StrategyRuntimeContext`]) and every [`TitanObjectRef`] are frozen for the
//!   whole major and are hashed into `ABI_FINGERPRINT`;
//! - each **payload** is versioned per domain with `(schema_major, schema_minor)` and is *not*
//!   hashed into the fingerprint. Appending a field bumps `schema_minor`; readers only require that
//!   `payload_len` covers the prefix they were compiled against.
//!
//! This module is the live layout: the coordinated V13→V14 switch has happened, so
//! [`crate::STRATEGY_ABI`] and [`crate::ABI_FINGERPRINT`] resolve here, the service builds an
//! [`crate::ObjectTable`] per instance and `NativeRuntime` hands it to every callback.

use std::ffi::c_void;

use crate::strategy::{
    TitanAccountView, TitanActiveOrderView, TitanBalanceView, TitanMarketView, TitanPositionView,
};

use titan_domain_types::DecimalUnit;

/// ABI version carried by the V14 root. The live root reports this value since the coordinated
/// switch: [`crate::STRATEGY_ABI`] re-exports it and the loader/descriptor gates compare against it.
pub const STRATEGY_ABI: u32 = 14;

/// Payload schema major the V14 host writes and understands.
pub const OBJECT_SCHEMA_MAJOR: u16 = 1;
/// Highest payload schema minor the V14 host understands (appended fields only within a major).
pub const OBJECT_SCHEMA_MINOR: u16 = 1;

/// ABI fingerprint of the V14 dtype set (root + object ref + V13 views).
///
/// Computed by the Python SDK (`abi.canonical_abi_descriptor`) and pinned here by the layout
/// tests. The live loader compares against it through [`crate::ABI_FINGERPRINT`].
pub const ABI_FINGERPRINT: [u8; 32] = [
    0xcc, 0x15, 0x25, 0x65, 0xae, 0x25, 0x98, 0x94, 0x4b, 0x0e, 0xf3, 0xc3, 0x91, 0x7f, 0x84, 0xcb,
    0x83, 0xe4, 0x3e, 0xc0, 0xc6, 0xfe, 0x12, 0xbf, 0xa1, 0x77, 0xea, 0x77, 0x66, 0x6c, 0x32, 0x7d,
];

/// Object domain numbers are ABI-frozen.
pub const OBJECT_DOMAIN_SYMBOL: u32 = 1;
pub const OBJECT_DOMAIN_ACCOUNT: u32 = 2;
pub const OBJECT_DOMAIN_CURRENCY: u32 = 3;
pub const OBJECT_DOMAIN_PAIR: u32 = 4;
pub const OBJECT_DOMAIN_STATUS: u32 = 5;
pub const OBJECT_DOMAIN_FEE: u32 = 6;

/// `TitanObjectRef.flags`.
pub const OBJECT_REF_VALID: u32 = 1 << 0;
pub const OBJECT_REF_VOLATILE: u32 = 1 << 1;
pub const OBJECT_REF_SYNTHETIC: u32 = 1 << 2;

/// `TitanSymbolPayloadV1.flags`.
pub const SYMBOL_FLAG_TRADABLE: u32 = 1 << 0;
pub const SYMBOL_FLAG_SYNTHETIC: u32 = 1 << 1;
pub const SYMBOL_FLAG_METADATA_COMPLETE: u32 = 1 << 2;

/// `TitanAccountPayloadV1.flags`.
pub const ACCOUNT_FLAG_READY: u32 = 1 << 0;
pub const ACCOUNT_FLAG_SYNTHETIC: u32 = 1 << 1;

/// `TitanCurrencyPayloadV1.flags`.
pub const CURRENCY_FLAG_QUOTE: u32 = 1 << 0;
pub const CURRENCY_FLAG_MARGIN: u32 = 1 << 1;
pub const CURRENCY_FLAG_SETTLEMENT: u32 = 1 << 2;

/// `TitanPairPayloadV1.flags`.
pub const PAIR_FLAG_LINEAR_NET: u32 = 1 << 0;
pub const PAIR_FLAG_ADMISSIBLE: u32 = 1 << 1;

/// `TitanFeePayloadV1.flags`.
pub const FEE_FLAG_VALID: u32 = 1 << 0;
pub const FEE_FLAG_MAKER_REBATE: u32 = 1 << 1;
pub const FEE_FLAG_SOURCE_CONFIG: u32 = 1 << 2;

/// `TitanFeePayloadV1.source`.
pub const FEE_SOURCE_EXCHANGE_PRIVATE: u32 = 0;
pub const FEE_SOURCE_CONFIG_OVERRIDE: u32 = 1;
pub const FEE_SOURCE_UNAVAILABLE: u32 = 2;

/// Valuation kind recorded in `TitanSymbolPayloadV1.valuation`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Valuation {
    /// Linear instrument priced in its quote currency.
    #[default]
    Linear = 0,
    /// Inverse instrument. V14 refuses these for multi-leg admission.
    Inverse = 1,
}

/// Position mode recorded in `TitanAccountPayloadV1.position_mode`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PositionMode {
    #[default]
    Net = 0,
    Hedge = 1,
}

/// Margin mode recorded in `TitanAccountPayloadV1.margin_mode`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MarginMode {
    Cross = 0,
    Isolated = 1,
    #[default]
    Unknown = 2,
}

/// ABI lifecycle values for `TitanStatusPayloadV1.lifecycle`. These are fixed numbers, independent
/// of Rust enum declaration order.
pub const STATUS_LIFECYCLE_DEFINED: u32 = 0;
pub const STATUS_LIFECYCLE_PREPARING: u32 = 1;
pub const STATUS_LIFECYCLE_READY: u32 = 2;
pub const STATUS_LIFECYCLE_RUNNING: u32 = 3;
pub const STATUS_LIFECYCLE_PAUSED: u32 = 4;
pub const STATUS_LIFECYCLE_RECOVERING: u32 = 5;
pub const STATUS_LIFECYCLE_STOPPING: u32 = 6;
pub const STATUS_LIFECYCLE_STOPPED: u32 = 7;
pub const STATUS_LIFECYCLE_FAILED: u32 = 8;

/// `TitanStatusPayloadV1.entry_flags`.
pub const STATUS_ENTRY_PAUSED: u32 = 1 << 0;
pub const STATUS_ENTRY_COMMAND_GATE_OPEN: u32 = 1 << 1;
pub const STATUS_ENTRY_STOPPING: u32 = 1 << 2;

/// `TitanStatusPayloadV1.last_flags`.
pub const STATUS_LAST_STOP_READY: u32 = 1 << 0;
pub const STATUS_LAST_BUSINESS_FAULTED: u32 = 1 << 1;

/// Fixed decimal payload; mirrors [`DecimalUnit`] without Rust layout assumptions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanDecimalV1 {
    pub coefficient: u64,
    pub scale: u8,
    pub reserved: [u8; 7],
}

impl TitanDecimalV1 {
    pub fn from_unit(unit: DecimalUnit) -> Self {
        Self {
            coefficient: unit.coefficient(),
            scale: unit.scale(),
            reserved: [0; 7],
        }
    }

    /// Absent optional decimals are encoded as `coefficient = 0, scale = 0`.
    pub fn absent() -> Self {
        Self::default()
    }

    pub fn unit(self) -> Option<DecimalUnit> {
        if self.coefficient == 0 {
            None
        } else {
            DecimalUnit::new(self.coefficient, self.scale).ok()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct TitanObjectRef {
    pub domain: u32,
    pub object_id: u32,
    pub flags: u32,
    pub schema_major: u16,
    pub schema_minor: u16,
    pub payload: *const u8,
    pub payload_len: u64,
}

/// Versioned object payloads. Only the fields shared by every reader live here; the layout is the
/// ABI and may only grow by appending at the end of a struct.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanSymbolPayloadV1 {
    pub local_asset_no: u32,
    pub asset_id: u32,
    pub quote_currency_no: u32,
    pub margin_currency_no: u32,
    pub valuation: u32,
    pub flags: u32,
    pub instrument_ref: u64,
    pub price_tick: TitanDecimalV1,
    pub quantity_lot: TitanDecimalV1,
    pub contract_multiplier: TitanDecimalV1,
    pub base_delta_per_lot: TitanDecimalV1,
    pub min_quantity: TitanDecimalV1,
    pub min_notional_quote: TitanDecimalV1,
    pub min_qty_lots: u64,
    pub min_notional_tick_lots: u64,
    pub metadata_version: u32,
    pub reserved0: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanAccountPayloadV1 {
    pub local_account_no: u32,
    pub account_id: u32,
    pub quote_currency_no: u32,
    pub margin_currency_no: u32,
    pub position_mode: u32,
    pub margin_mode: u32,
    pub margin_domain: u32,
    pub flags: u32,
    pub currency_start: u32,
    pub currency_count: u32,
    pub reserved0: u32,
    pub reserved1: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanCurrencyPayloadV1 {
    pub local_account_no: u32,
    pub currency_no: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub amount_unit: TitanDecimalV1,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanPairPayloadV1 {
    pub maker_local_asset_no: u32,
    pub hedge_local_asset_no: u32,
    pub maker_local_account_no: u32,
    pub hedge_local_account_no: u32,
    pub quote_currency_no: u32,
    pub flags: u32,
    pub hedge_lots_per_maker_lot: u64,
    pub hedge_ticks_per_maker_tick_num: u64,
    pub hedge_ticks_per_maker_tick_den: u64,
    pub min_hedge_lots: u64,
    pub min_hedge_notional_quote: TitanDecimalV1,
    pub pair_revision: u32,
    pub reserved0: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanStatusPayloadV1 {
    pub lifecycle: u32,
    pub entry_flags: u32,
    pub last_return_code: i32,
    pub last_flags: u32,
    pub last_error_code: i32,
    pub error_category: u32,
    pub stop_deadline_ns: i64,
    pub reserved0: u32,
    pub reserved1: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TitanFeePayloadV1 {
    pub local_account_no: u32,
    pub flags: u32,
    pub maker_fee_bps: i32,
    pub taker_fee_bps: i32,
    pub source: u32,
    pub reserved0: u32,
    pub observed_at_ns: u64,
}

/// Reserved dynamic-object query function pointer. V14 always sets it to `NULL`.
pub type TitanObjectQueryFn = unsafe extern "C" fn(
    object_context: *mut c_void,
    domain: u32,
    object_id: u32,
    max_minor: u32,
    out_ref: *mut TitanObjectRef,
) -> i32;

/// V14 root: V13's field sequence followed by the object-table tail.
///
/// The V13 fields are repeated here rather than nested so that the frozen offsets documented by the
/// V13 layout are preserved byte-for-byte up to the appended tail.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct StrategyRuntimeContext {
    // --- V13 prefix (unchanged offsets) ---
    pub struct_size: u32,
    pub abi_version: u32,
    pub event_kind: u32,
    pub event_schema_version: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub now_ns: i64,
    pub generation: u64,
    pub strategy_instance_id: u64,
    pub state_ptr: *mut u8,
    pub state_len: u64,
    pub state_alignment: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub ticks_ptr: *const crate::TitanTickView,
    pub ticks_len: u64,
    pub bars_ptr: *const crate::TitanBarView,
    pub bars_len: u64,
    pub depth_ptr: *const crate::TitanDepthView,
    pub depth_len: u64,
    pub fills_ptr: *const crate::TitanFillView,
    pub fills_len: u64,
    pub order_events_ptr: *const crate::TitanOrderEventView,
    pub order_events_len: u64,
    pub cancel_events_ptr: *const crate::TitanCancelEventView,
    pub cancel_events_len: u64,
    pub position_events_ptr: *const crate::TitanPositionEventView,
    pub position_events_len: u64,
    pub balance_events_ptr: *const crate::TitanBalanceEventView,
    pub balance_events_len: u64,
    pub account_state_events_ptr: *const crate::TitanAccountStateEventView,
    pub account_state_events_len: u64,
    pub timer_ptr: *const crate::TitanTimerView,
    pub timer_len: u64,
    pub markets_ptr: *const crate::TitanMarketView,
    pub markets_len: u64,
    pub positions_ptr: *const crate::TitanPositionView,
    pub positions_len: u64,
    pub balances_ptr: *const crate::TitanBalanceView,
    pub balances_len: u64,
    pub accounts_ptr: *const crate::TitanAccountView,
    pub accounts_len: u64,
    pub active_orders_ptr: *const crate::TitanActiveOrderView,
    pub active_orders_len: u64,
    pub event_payload_ptr: *const c_void,
    pub event_payload_len: u64,
    pub command_context: *mut c_void,
    pub submit_order: Option<crate::TitanSubmitOrderFn>,
    pub cancel_order: Option<crate::TitanCancelOrderFn>,
    pub last_error_code: i32,
    pub reserved: u32,
    // --- V14 tail (appended; never reordered) ---
    pub objects_ptr: *const TitanObjectRef,
    pub objects_len: u64,
    pub object_schema_minor: u32,
    pub reserved_tail: u32,
    pub object_query: Option<TitanObjectQueryFn>,
    pub object_context: *mut c_void,
    pub reserved_ext: u64,
}

impl Default for StrategyRuntimeContext {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

impl StrategyRuntimeContext {
    /// Fills the frozen identity fields the callback wrapper owns.
    pub fn prepare(&mut self) {
        self.struct_size = std::mem::size_of::<Self>() as u32;
        self.abi_version = STRATEGY_ABI;
    }
}

/// Schema description of one payload field, used by `OBJECT_SCHEMA_HASH` and CI cross-checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectFieldSchema {
    pub name: &'static str,
    pub type_name: &'static str,
    pub offset: usize,
    pub size: usize,
}

/// Schema description of one payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPayloadSchema {
    pub domain: u32,
    pub name: &'static str,
    pub schema_major: u16,
    pub schema_minor: u16,
    pub size: usize,
    pub alignment: usize,
    pub fields: Vec<ObjectFieldSchema>,
}

macro_rules! payload_fields {
    ($ty:ty, $(($name:ident, $field_ty:ty, $type_name:literal)),* $(,)?) => {
        vec![
            $(ObjectFieldSchema {
                name: stringify!($name),
                type_name: $type_name,
                offset: std::mem::offset_of!($ty, $name),
                size: std::mem::size_of::<$field_ty>(),
            }),*
        ]
    };
}

fn payload_schema<T>(
    domain: u32,
    name: &'static str,
    fields: Vec<ObjectFieldSchema>,
) -> ObjectPayloadSchema {
    ObjectPayloadSchema {
        domain,
        name,
        schema_major: 1,
        schema_minor: 1,
        size: std::mem::size_of::<T>(),
        alignment: std::mem::align_of::<T>(),
        fields,
    }
}

/// Canonical payload schema table. This is the single Rust source for
/// [`object_schema_hash`] and the golden-bytes tests.
pub fn object_payload_schemas() -> Vec<ObjectPayloadSchema> {
    vec![
        payload_schema::<TitanSymbolPayloadV1>(
            OBJECT_DOMAIN_SYMBOL,
            "TitanSymbolPayloadV1",
            payload_fields!(
                TitanSymbolPayloadV1,
                (local_asset_no, u32, "u32"),
                (asset_id, u32, "u32"),
                (quote_currency_no, u32, "u32"),
                (margin_currency_no, u32, "u32"),
                (valuation, u32, "u32"),
                (flags, u32, "u32"),
                (instrument_ref, u64, "u64"),
                (price_tick, TitanDecimalV1, "decimal"),
                (quantity_lot, TitanDecimalV1, "decimal"),
                (contract_multiplier, TitanDecimalV1, "decimal"),
                (base_delta_per_lot, TitanDecimalV1, "decimal"),
                (min_quantity, TitanDecimalV1, "decimal"),
                (min_notional_quote, TitanDecimalV1, "decimal"),
                (min_qty_lots, u64, "u64"),
                (min_notional_tick_lots, u64, "u64"),
                (metadata_version, u32, "u32"),
                (reserved0, u32, "u32"),
            ),
        ),
        payload_schema::<TitanAccountPayloadV1>(
            OBJECT_DOMAIN_ACCOUNT,
            "TitanAccountPayloadV1",
            payload_fields!(
                TitanAccountPayloadV1,
                (local_account_no, u32, "u32"),
                (account_id, u32, "u32"),
                (quote_currency_no, u32, "u32"),
                (margin_currency_no, u32, "u32"),
                (position_mode, u32, "u32"),
                (margin_mode, u32, "u32"),
                (margin_domain, u32, "u32"),
                (flags, u32, "u32"),
                (currency_start, u32, "u32"),
                (currency_count, u32, "u32"),
                (reserved0, u32, "u32"),
                (reserved1, u32, "u32"),
            ),
        ),
        payload_schema::<TitanCurrencyPayloadV1>(
            OBJECT_DOMAIN_CURRENCY,
            "TitanCurrencyPayloadV1",
            payload_fields!(
                TitanCurrencyPayloadV1,
                (local_account_no, u32, "u32"),
                (currency_no, u32, "u32"),
                (flags, u32, "u32"),
                (reserved0, u32, "u32"),
                (amount_unit, TitanDecimalV1, "decimal"),
            ),
        ),
        payload_schema::<TitanPairPayloadV1>(
            OBJECT_DOMAIN_PAIR,
            "TitanPairPayloadV1",
            payload_fields!(
                TitanPairPayloadV1,
                (maker_local_asset_no, u32, "u32"),
                (hedge_local_asset_no, u32, "u32"),
                (maker_local_account_no, u32, "u32"),
                (hedge_local_account_no, u32, "u32"),
                (quote_currency_no, u32, "u32"),
                (flags, u32, "u32"),
                (hedge_lots_per_maker_lot, u64, "u64"),
                (hedge_ticks_per_maker_tick_num, u64, "u64"),
                (hedge_ticks_per_maker_tick_den, u64, "u64"),
                (min_hedge_lots, u64, "u64"),
                (min_hedge_notional_quote, TitanDecimalV1, "decimal"),
                (pair_revision, u32, "u32"),
                (reserved0, u32, "u32"),
            ),
        ),
        payload_schema::<TitanStatusPayloadV1>(
            OBJECT_DOMAIN_STATUS,
            "TitanStatusPayloadV1",
            payload_fields!(
                TitanStatusPayloadV1,
                (lifecycle, u32, "u32"),
                (entry_flags, u32, "u32"),
                (last_return_code, i32, "i32"),
                (last_flags, u32, "u32"),
                (last_error_code, i32, "i32"),
                (error_category, u32, "u32"),
                (stop_deadline_ns, i64, "i64"),
                (reserved0, u32, "u32"),
                (reserved1, u32, "u32"),
            ),
        ),
        payload_schema::<TitanFeePayloadV1>(
            OBJECT_DOMAIN_FEE,
            "TitanFeePayloadV1",
            payload_fields!(
                TitanFeePayloadV1,
                (local_account_no, u32, "u32"),
                (flags, u32, "u32"),
                (maker_fee_bps, i32, "i32"),
                (taker_fee_bps, i32, "i32"),
                (source, u32, "u32"),
                (reserved0, u32, "u32"),
                (observed_at_ns, u64, "u64"),
            ),
        ),
    ]
}

/// Canonical descriptor of the payload schema (domain-ordered, fields in declaration order).
///
/// The hash is intentionally independent of the root fingerprint: payloads may grow by appending
/// fields without invalidating compiled strategies, so CI compares this descriptor across Rust and
/// Python instead of rejecting artifacts at load time.
pub fn object_schema_descriptor() -> serde_json::Value {
    use serde_json::json;
    let payloads = object_payload_schemas()
        .into_iter()
        .map(|payload| {
            json!({
                "domain": payload.domain,
                "name": payload.name,
                "schema_major": payload.schema_major,
                "schema_minor": payload.schema_minor,
                "size": payload.size,
                "alignment": payload.alignment,
                "fields": payload.fields.iter().map(|field| json!({
                    "name": field.name,
                    "type": field.type_name,
                    "offset": field.offset,
                    "size": field.size,
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "object_schema_version": 1,
        "object_ref_size": std::mem::size_of::<TitanObjectRef>(),
        "context_size": std::mem::size_of::<StrategyRuntimeContext>(),
        "payloads": payloads,
    })
}

pub fn object_schema_hash() -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&object_schema_descriptor())
        .expect("object schema descriptor is encodable");
    Sha256::digest(bytes).into()
}

const _: () = {
    assert!(std::mem::size_of::<TitanDecimalV1>() == 16);
    assert!(std::mem::align_of::<TitanDecimalV1>() == 8);
    assert!(std::mem::size_of::<TitanObjectRef>() == 32);
    assert!(std::mem::align_of::<TitanObjectRef>() == 8);
    assert!(std::mem::size_of::<TitanSymbolPayloadV1>() == 152);
    assert!(std::mem::size_of::<TitanAccountPayloadV1>() == 48);
    assert!(std::mem::size_of::<TitanCurrencyPayloadV1>() == 32);
    assert!(std::mem::size_of::<TitanPairPayloadV1>() == 80);
    assert!(std::mem::size_of::<TitanStatusPayloadV1>() == 40);
    assert!(std::mem::size_of::<TitanFeePayloadV1>() == 32);
    assert!(std::mem::size_of::<StrategyRuntimeContext>() == 440);
    assert!(std::mem::align_of::<StrategyRuntimeContext>() == 8);
    // The V14 tail must start exactly where V13's 392-byte body ends.
    assert!(std::mem::offset_of!(StrategyRuntimeContext, objects_ptr) == 392);
    assert!(std::mem::offset_of!(StrategyRuntimeContext, reserved_ext) == 432);
};

/// Borrowed per-callback views. The V14 root stores only pointers/lengths, so the views are never
/// copied and the callback path stays allocation-free.
pub(crate) struct CallbackViews<'a> {
    pub markets: &'a [TitanMarketView],
    pub positions: &'a [TitanPositionView],
    pub balances: &'a [TitanBalanceView],
    pub accounts: &'a [TitanAccountView],
    pub active_orders: &'a [TitanActiveOrderView],
}

/// Builds the per-callback V14 root. The object table is borrowed, never rebuilt.
///
/// # Dead code
/// The coordinated V13→V14 switch calls this from the runtime dispatcher; until then only the layout
/// tests exercise it.
#[allow(dead_code)]
pub(crate) fn callback_context(
    now_ns: i64,
    schema_version: u32,
    views: CallbackViews<'_>,
    table: &crate::ObjectTable,
) -> StrategyRuntimeContext {
    let refs = table.refs();
    StrategyRuntimeContext {
        now_ns,
        event_schema_version: schema_version,
        markets_ptr: views.markets.as_ptr(),
        markets_len: views.markets.len() as u64,
        positions_ptr: views.positions.as_ptr(),
        positions_len: views.positions.len() as u64,
        balances_ptr: views.balances.as_ptr(),
        balances_len: views.balances.len() as u64,
        accounts_ptr: views.accounts.as_ptr(),
        accounts_len: views.accounts.len() as u64,
        active_orders_ptr: views.active_orders.as_ptr(),
        active_orders_len: views.active_orders.len() as u64,
        objects_ptr: refs.as_ptr(),
        objects_len: refs.len() as u64,
        object_schema_minor: u32::from(OBJECT_SCHEMA_MINOR),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_appends_the_object_tail_without_moving_offsets() {
        assert_eq!(std::mem::offset_of!(StrategyRuntimeContext, struct_size), 0);
        assert_eq!(std::mem::offset_of!(StrategyRuntimeContext, event_kind), 8);
        assert_eq!(
            std::mem::offset_of!(StrategyRuntimeContext, state_schema_hash),
            72
        );
        assert_eq!(std::mem::offset_of!(StrategyRuntimeContext, ticks_ptr), 104);
        assert_eq!(
            std::mem::offset_of!(StrategyRuntimeContext, objects_ptr),
            392
        );
        assert_eq!(
            std::mem::offset_of!(StrategyRuntimeContext, objects_len),
            400
        );
        assert_eq!(
            std::mem::offset_of!(StrategyRuntimeContext, object_query),
            416
        );
        let mut context = StrategyRuntimeContext::default();
        context.prepare();
        assert_eq!(context.struct_size, 440);
        assert_eq!(context.abi_version, 14);
        assert!(context.object_query.is_none());
        assert!(context.object_context.is_null());
    }

    #[test]
    fn payload_offsets_match_the_frozen_documentation() {
        let payloads = object_payload_schemas();
        let symbol = payloads
            .iter()
            .find(|payload| payload.domain == OBJECT_DOMAIN_SYMBOL)
            .unwrap();
        let field = |name: &str| {
            symbol
                .fields
                .iter()
                .find(|field| field.name == name)
                .unwrap()
                .offset
        };
        assert_eq!(field("price_tick"), 32);
        assert_eq!(field("min_qty_lots"), 128);
        assert_eq!(field("min_notional_tick_lots"), 136);
        assert_eq!(symbol.size, 152);

        let pair = payloads
            .iter()
            .find(|payload| payload.domain == OBJECT_DOMAIN_PAIR)
            .unwrap();
        assert_eq!(pair.size, 80);
        let account = payloads
            .iter()
            .find(|payload| payload.domain == OBJECT_DOMAIN_ACCOUNT)
            .unwrap();
        assert_eq!(account.size, 48);
        let status = payloads
            .iter()
            .find(|payload| payload.domain == OBJECT_DOMAIN_STATUS)
            .unwrap();
        assert_eq!(status.size, 40);
    }

    #[test]
    fn decimal_payload_round_trips_and_marks_absent_values() {
        let unit: DecimalUnit = "0.01".parse().unwrap();
        let payload = TitanDecimalV1::from_unit(unit);
        assert_eq!(payload.reserved, [0; 7]);
        assert_eq!(payload.unit(), Some(unit));
        assert_eq!(TitanDecimalV1::absent().unit(), None);
        // An invalid scale is not silently reinterpreted.
        assert_eq!(
            TitanDecimalV1 {
                coefficient: 1,
                scale: 19,
                reserved: [0; 7]
            }
            .unit(),
            None
        );
    }

    #[test]
    fn schema_hash_covers_every_payload_and_is_stable() {
        let descriptor = object_schema_descriptor();
        assert_eq!(descriptor["payloads"].as_array().unwrap().len(), 6);
        assert_eq!(descriptor["object_ref_size"], 32);
        assert_eq!(descriptor["context_size"], 440);
        let first = object_schema_hash();
        assert_eq!(first, object_schema_hash());
        assert_eq!(first.len(), 32);
    }

    #[test]
    fn v14_descriptor_fixture_matches_fingerprint_and_tail_offsets() {
        use sha2::{Digest, Sha256};
        let text = include_str!("../../../fixtures/abi_descriptor.json");
        let digest: [u8; 32] = Sha256::digest(text.as_bytes()).into();
        assert_eq!(
            digest, ABI_FINGERPRINT,
            "fixture fingerprint != Rust constant: one side changed the ABI alone"
        );
        let document: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(document["abi_version"], 14);
        let context = &document["structs"]["StrategyRuntimeContext"];
        assert_eq!(context["size"], 440);
        let offset = |name: &str| {
            context["fields"]
                .as_array()
                .unwrap()
                .iter()
                .find(|field| field["name"] == name)
                .unwrap_or_else(|| panic!("missing context field {name}"))["offset"]
                .as_u64()
                .unwrap()
        };
        assert_eq!(offset("struct_size"), 0);
        assert_eq!(offset("state_schema_hash"), 72);
        assert_eq!(offset("objects_ptr"), 392);
        assert_eq!(offset("objects_len"), 400);
        assert_eq!(offset("object_schema_minor"), 408);
        assert_eq!(offset("reserved_tail"), 412);
        assert_eq!(offset("object_query"), 416);
        assert_eq!(offset("object_context"), 424);
        assert_eq!(offset("reserved_ext"), 432);

        let reference = &document["structs"]["TitanObjectRef"];
        assert_eq!(reference["size"], 32);
        let offset = |name: &str| {
            reference["fields"]
                .as_array()
                .unwrap()
                .iter()
                .find(|field| field["name"] == name)
                .unwrap_or_else(|| panic!("missing ref field {name}"))["offset"]
                .as_u64()
                .unwrap()
        };
        assert_eq!(offset("domain"), 0);
        assert_eq!(offset("object_id"), 4);
        assert_eq!(offset("flags"), 8);
        assert_eq!(offset("schema_major"), 12);
        assert_eq!(offset("schema_minor"), 14);
        assert_eq!(offset("payload"), 16);
        assert_eq!(offset("payload_len"), 24);
    }

    #[test]
    fn object_schema_fixture_matches_the_rust_descriptor() {
        use sha2::{Digest, Sha256};
        let text = include_str!("../../../fixtures/object_schema.json");
        let fixture: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(
            fixture,
            object_schema_descriptor(),
            "Rust and Python object schema descriptors diverged"
        );
        let digest: [u8; 32] = Sha256::digest(text.as_bytes()).into();
        assert_eq!(digest, object_schema_hash());
    }

    #[test]
    fn v14_callback_context_carries_the_frozen_table_pointer() {
        use crate::context::{MarginMode, PositionMode, Valuation};
        use crate::object_table::{
            AccountObjectInput, CurrencyObjectInput, ObjectTableInput, SymbolObjectInput,
        };
        use std::sync::Arc;
        use titan_domain_types::DecimalUnit;

        fn unit(value: &str) -> DecimalUnit {
            value.parse().unwrap()
        }
        let table = crate::ObjectTable::build(ObjectTableInput {
            symbols: vec![SymbolObjectInput {
                local_asset_no: 0,
                asset_id: 1,
                native_symbol: Arc::from("TEST"),
                source_key: Arc::from("venue"),
                price_tick: unit("0.1"),
                quantity_lot: unit("0.01"),
                contract_multiplier: unit("1"),
                min_quantity: None,
                min_notional_quote: None,
                quote_currency: Arc::from("USDT"),
                margin_currency: Arc::from("USDT"),
                valuation: Valuation::Linear,
                tradable: true,
                synthetic: false,
                metadata_version: 1,
            }],
            accounts: vec![AccountObjectInput {
                local_account_no: 0,
                account_id: 1,
                quote_currency: Arc::from("USDT"),
                margin_currency: Arc::from("USDT"),
                position_mode: PositionMode::Net,
                margin_mode: MarginMode::Cross,
                margin_domain: 0,
                ready: true,
                synthetic: false,
                currencies: vec![CurrencyObjectInput {
                    currency_no: 1,
                    native_currency: Arc::from("USDT"),
                    amount_unit: unit("0.000001"),
                    quote: true,
                    margin: true,
                    settlement: true,
                }],
            }],
            pairs: vec![],
            fees: vec![],
            metadata_snapshot_digest: [0; 32],
        })
        .unwrap();
        let views = CallbackViews {
            markets: &[],
            positions: &[],
            balances: &[],
            accounts: &[],
            active_orders: &[],
        };
        let context = callback_context(7, 1, views, &table);
        assert_eq!(context.now_ns, 7);
        assert_eq!(context.objects_ptr, table.refs().as_ptr());
        assert_eq!(context.objects_len, table.refs().len() as u64);
        assert_eq!(context.object_schema_minor, u32::from(OBJECT_SCHEMA_MINOR));
        assert!(context.object_query.is_none());
        assert!(context.object_context.is_null());
        assert_eq!(context.reserved_tail, 0);
        assert_eq!(context.reserved_ext, 0);
        // The V13 prefix keeps its frozen offsets and semantics.
        assert_eq!(
            context.struct_size, 0,
            "struct_size is filled by the invoke wrapper"
        );
        assert_eq!(std::mem::size_of_val(&context), 440);
    }
}
