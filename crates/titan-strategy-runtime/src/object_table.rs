//! Builds the immutable object table from frozen instance facts.
//!
//! The table is built once per instance (online or offline) and never re-read from configuration or
//! the exchange. Every derived value is computed here with checked integer arithmetic; an
//! unprovable fact refuses the instance instead of producing a plausible number.
//!
//! Layout and versioning live in [`crate::v14`]. This module owns:
//! - admission validation (`synthetic`, inverse, unknown currency, non-integral ratios, missing
//!   minimums) and the derived integers the strategy may use;
//! - payload arena construction in one canonical order `(domain, object_id)` with 8-byte-aligned
//!   payloads and immutable arenas;
//! - `object_identity_digest`, which covers only fields that participate in admission arithmetic.

use std::{collections::BTreeMap, sync::Arc};

use sha2::{Digest, Sha256};
use titan_domain_types::DecimalUnit;

use crate::context::*;

/// Version of the identity digest *format* (which fields participate and in what order).
pub const OBJECT_IDENTITY_VERSION: u32 = 1;
/// Highest payload minor this host understands.
pub const OBJECT_TABLE_SCHEMA_MINOR: u16 = 1;

pub const MAX_OBJECT_SYMBOLS: usize = 64;
pub const MAX_OBJECT_ACCOUNTS: usize = 16;
pub const MAX_OBJECT_CURRENCIES: usize = 128;
pub const MAX_OBJECT_PAIRS: usize = 16;
pub const MAX_OBJECT_TABLE_BYTES: usize = 1024 * 1024;

/// A symbol fact the object table may expose. Field semantics follow [`crate::v14`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolObjectInput {
    pub local_asset_no: u32,
    pub asset_id: u32,
    pub native_symbol: Arc<str>,
    pub source_key: Arc<str>,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,
    pub contract_multiplier: DecimalUnit,
    pub min_quantity: Option<DecimalUnit>,
    pub min_notional_quote: Option<DecimalUnit>,
    pub quote_currency: Arc<str>,
    pub margin_currency: Arc<str>,
    pub valuation: Valuation,
    pub tradable: bool,
    pub synthetic: bool,
    pub metadata_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrencyObjectInput {
    pub currency_no: u32,
    pub native_currency: Arc<str>,
    pub amount_unit: DecimalUnit,
    pub quote: bool,
    pub margin: bool,
    pub settlement: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountObjectInput {
    pub local_account_no: u32,
    pub account_id: u32,
    pub quote_currency: Arc<str>,
    pub margin_currency: Arc<str>,
    pub position_mode: PositionMode,
    pub margin_mode: MarginMode,
    pub margin_domain: u32,
    pub ready: bool,
    pub synthetic: bool,
    pub currencies: Vec<CurrencyObjectInput>,
}

/// One admissible maker/hedge pair. The required ratios are derived from the symbol units; a
/// configuration cannot supply them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairObjectInput {
    pub maker_local_asset_no: u32,
    pub hedge_local_asset_no: u32,
    pub maker_local_account_no: u32,
    pub hedge_local_account_no: u32,
    pub quote_currency_no: u32,
    pub pair_revision: u32,
}

/// Account fee facts. `None` rates mean "not available" and never zero.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeeObjectInput {
    pub local_account_no: u32,
    pub maker_fee_bps: Option<i32>,
    pub taker_fee_bps: Option<i32>,
    pub source_config: bool,
    pub observed_at_ns: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectCounts {
    pub symbols: usize,
    pub accounts: usize,
    pub currencies: usize,
    pub pairs: usize,
    pub fees: usize,
}

/// One refused instance. `code` is stable; `detail` is diagnostic only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectTableError {
    code: &'static str,
    detail: String,
}

impl ObjectTableError {
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for ObjectTableError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for ObjectTableError {}

pub const OBJECT_UNITS_INVALID: &str = "object_units_invalid";
pub const OBJECT_CAPACITY_EXCEEDED: &str = "object_capacity_exceeded";
pub const OBJECT_CURRENCY_UNKNOWN: &str = "object_currency_unknown";
pub const OBJECT_CURRENCY_ID_CONFLICT: &str = "object_currency_id_conflict";
pub const OBJECT_DUPLICATE_OBJECT: &str = "object_duplicate_object";
pub const OBJECT_ADMISSION_UNPROVABLE: &str = "object_admission_unprovable";
pub const OBJECT_PAIR_UNKNOWN: &str = "object_pair_unknown";
pub const OBJECT_PAIR_LOT_RATIO: &str = "object_pair_lot_ratio";
pub const OBJECT_PAIR_TICK_RATIO: &str = "object_pair_tick_ratio";
pub const OBJECT_PAIR_QUOTE_MISMATCH: &str = "object_pair_quote_mismatch";
pub const OBJECT_PAIR_MIN_FILL_UNPROVEN: &str = "object_pair_min_fill_unproven";
pub const OBJECT_ACCOUNT_STATE_INVALID: &str = "object_account_state_invalid";

/// Immutable object table plus the single mutable status payload.
pub struct ObjectTable {
    refs: Box<[TitanObjectRef]>,
    symbols: Box<[u64]>,
    accounts: Box<[u64]>,
    currencies: Box<[u64]>,
    pairs: Box<[u64]>,
    fees: Box<[u64]>,
    status: Box<[u64]>,
    digest: [u8; 32],
    metadata_snapshot_digest: [u8; 32],
    counts: ObjectCounts,
}

// SAFETY: every arena is allocated once during `build` and only read afterwards. The only mutable
// payload is `status`, touched exclusively through `&mut ObjectTable` (single writer, no concurrent
// readers). Pointers inside `TitanObjectRef` have the same lifetime as the table.
// SAFETY: after `build`, every arena and payload is immutable; `&ObjectTable` only exposes reads.
// The single mutable object (`status`) is reachable exclusively through `&mut ObjectTable`, so no
// concurrent reader can observe a torn write. Pointers are valid for the table's lifetime.
unsafe impl Send for ObjectTable {}
unsafe impl Sync for ObjectTable {}

fn write_payload<T: Copy>(arena: &mut Vec<u64>, value: T) -> usize {
    debug_assert!(std::mem::align_of::<T>() <= 8);
    let size = std::mem::size_of::<T>();
    let offset = arena.len() * 8;
    let slots = size.div_ceil(8);
    arena.resize(arena.len() + slots, 0);
    unsafe {
        std::ptr::write_unaligned(
            arena.as_mut_ptr().cast::<u8>().add(offset).cast::<T>(),
            value,
        );
    }
    offset
}

fn arena_bytes(arena: &[u64]) -> usize {
    arena.len() * 8
}

fn instrument_ref(source_key: &str, native_symbol: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"titan-instrument-v1");
    hasher.update(source_key.as_bytes());
    hasher.update([0u8]);
    hasher.update(native_symbol.as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("eight bytes"))
}

fn decimal_bytes(hasher: &mut Sha256, value: Option<DecimalUnit>) {
    match value {
        Some(unit) => {
            hasher.update([1u8]);
            hasher.update(unit.coefficient().to_le_bytes());
            hasher.update([unit.scale()]);
        }
        None => hasher.update([0u8]),
    }
}

impl std::fmt::Debug for ObjectTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print payload pointers: they are process-local and must not leak into logs/records.
        formatter
            .debug_struct("ObjectTable")
            .field("counts", &self.counts)
            .field("digest", &self.digest)
            .field("schema_minor", &OBJECT_TABLE_SCHEMA_MINOR)
            .finish_non_exhaustive()
    }
}

impl ObjectTable {
    /// A table with no objects. Used before the service attaches the instance's built table and by
    /// callers that intentionally run without an object context.
    pub fn empty() -> Self {
        Self {
            refs: Box::new([]),
            symbols: Box::new([]),
            accounts: Box::new([]),
            currencies: Box::new([]),
            pairs: Box::new([]),
            fees: Box::new([]),
            status: Box::new([]),
            digest: [0; 32],
            metadata_snapshot_digest: [0; 32],
            counts: ObjectCounts::default(),
        }
    }
    pub fn build(input: ObjectTableInput) -> Result<Self, ObjectTableError> {
        let ObjectTableInput {
            symbols,
            accounts,
            pairs,
            fees,
            metadata_snapshot_digest,
        } = input;
        if symbols.len() > MAX_OBJECT_SYMBOLS
            || accounts.len() > MAX_OBJECT_ACCOUNTS
            || pairs.len() > MAX_OBJECT_PAIRS
        {
            return Err(ObjectTableError::new(
                OBJECT_CAPACITY_EXCEEDED,
                "symbol/account/pair count exceeds the instance table limit",
            ));
        }
        if symbols.is_empty() || accounts.is_empty() {
            return Err(ObjectTableError::new(
                OBJECT_ACCOUNT_STATE_INVALID,
                "the object table needs at least one symbol and one account",
            ));
        }

        // Currency numbers are account-local; V14 requires the same native currency to have the same
        // number in every account that declares it, otherwise a symbol payload cannot name one id.
        let mut currency_ids: BTreeMap<Arc<str>, u32> = BTreeMap::new();
        for account in &accounts {
            for currency in &account.currencies {
                match currency_ids.get(&currency.native_currency) {
                    Some(existing) if *existing != currency.currency_no => {
                        return Err(ObjectTableError::new(
                            OBJECT_CURRENCY_ID_CONFLICT,
                            format!(
                                "currency {} is {} in one account and {} in another",
                                currency.native_currency, existing, currency.currency_no
                            ),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        currency_ids.insert(currency.native_currency.clone(), currency.currency_no);
                    }
                }
            }
        }
        let currency_no = |name: &Arc<str>| -> Result<u32, ObjectTableError> {
            currency_ids.get(name).copied().ok_or_else(|| {
                ObjectTableError::new(
                    OBJECT_CURRENCY_UNKNOWN,
                    format!("currency {name} is not declared by any bound account"),
                )
            })
        };

        // --- symbols: derive checked integers and the admission-usable payload ---
        let mut symbol_map: BTreeMap<u32, TitanSymbolPayloadV1> = BTreeMap::new();
        let mut symbol_quotes: BTreeMap<u32, u32> = BTreeMap::new();
        for symbol in &symbols {
            if symbol_map.contains_key(&symbol.local_asset_no) {
                return Err(ObjectTableError::new(
                    OBJECT_DUPLICATE_OBJECT,
                    format!("duplicate symbol asset_no {}", symbol.local_asset_no),
                ));
            }
            let base_delta = symbol
                .quantity_lot
                .checked_mul(symbol.contract_multiplier)
                .ok_or_else(|| {
                    ObjectTableError::new(
                        OBJECT_UNITS_INVALID,
                        format!(
                            "{}: quantity_lot * contract_multiplier is not exactly representable",
                            symbol.native_symbol
                        ),
                    )
                })?;
            let min_qty_lots = match symbol.min_quantity {
                Some(quantity) => quantity.ceil_div_by(symbol.quantity_lot).ok_or_else(|| {
                    ObjectTableError::new(
                        OBJECT_UNITS_INVALID,
                        format!(
                            "{}: minimum quantity is not representable in lots",
                            symbol.native_symbol
                        ),
                    )
                })?,
                None => 0,
            };
            let min_notional_tick_lots = match symbol.min_notional_quote {
                Some(notional) => {
                    let denominator =
                        symbol.price_tick.checked_mul(base_delta).ok_or_else(|| {
                            ObjectTableError::new(
                                OBJECT_UNITS_INVALID,
                                format!(
                                    "{}: price_tick * base_delta is not exact",
                                    symbol.native_symbol
                                ),
                            )
                        })?;
                    notional.ceil_div_by(denominator).ok_or_else(|| {
                        ObjectTableError::new(
                            OBJECT_UNITS_INVALID,
                            format!(
                                "{}: minimum notional is not representable",
                                symbol.native_symbol
                            ),
                        )
                    })?
                }
                None => 0,
            };
            let quote = currency_no(&symbol.quote_currency)?;
            let margin = currency_no(&symbol.margin_currency)?;
            let mut flags = 0;
            if symbol.tradable {
                flags |= SYMBOL_FLAG_TRADABLE;
            }
            if symbol.synthetic {
                flags |= SYMBOL_FLAG_SYNTHETIC;
            }
            if symbol.min_quantity.is_some() && symbol.min_notional_quote.is_some() {
                flags |= SYMBOL_FLAG_METADATA_COMPLETE;
            }
            symbol_quotes.insert(symbol.local_asset_no, quote);
            symbol_map.insert(
                symbol.local_asset_no,
                TitanSymbolPayloadV1 {
                    local_asset_no: symbol.local_asset_no,
                    asset_id: symbol.asset_id,
                    quote_currency_no: quote,
                    margin_currency_no: margin,
                    valuation: symbol.valuation as u32,
                    flags,
                    instrument_ref: instrument_ref(&symbol.source_key, &symbol.native_symbol),
                    price_tick: TitanDecimalV1::from_unit(symbol.price_tick),
                    quantity_lot: TitanDecimalV1::from_unit(symbol.quantity_lot),
                    contract_multiplier: TitanDecimalV1::from_unit(symbol.contract_multiplier),
                    base_delta_per_lot: TitanDecimalV1::from_unit(base_delta),
                    min_quantity: symbol
                        .min_quantity
                        .map(TitanDecimalV1::from_unit)
                        .unwrap_or_else(TitanDecimalV1::absent),
                    min_notional_quote: symbol
                        .min_notional_quote
                        .map(TitanDecimalV1::from_unit)
                        .unwrap_or_else(TitanDecimalV1::absent),
                    min_qty_lots,
                    min_notional_tick_lots,
                    metadata_version: symbol.metadata_version,
                    reserved0: 0,
                },
            );
        }

        // --- accounts and their currency blocks ---
        let mut account_map: BTreeMap<u32, TitanAccountPayloadV1> = BTreeMap::new();
        let mut currency_payloads: Vec<TitanCurrencyPayloadV1> = Vec::new();
        for account in &accounts {
            if account_map.contains_key(&account.local_account_no) {
                return Err(ObjectTableError::new(
                    OBJECT_DUPLICATE_OBJECT,
                    format!("duplicate account_no {}", account.local_account_no),
                ));
            }
            if account.currencies.len() > MAX_OBJECT_CURRENCIES {
                return Err(ObjectTableError::new(
                    OBJECT_CAPACITY_EXCEEDED,
                    "account currency block exceeds the instance limit",
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            let mut native = std::collections::BTreeSet::new();
            for currency in &account.currencies {
                if !seen.insert(currency.currency_no)
                    || !native.insert(currency.native_currency.clone())
                {
                    return Err(ObjectTableError::new(
                        OBJECT_DUPLICATE_OBJECT,
                        format!(
                            "account {} declares a duplicate currency {}",
                            account.local_account_no, currency.currency_no
                        ),
                    ));
                }
            }
            let resolve = |name: &Arc<str>, role: &str| -> Result<u32, ObjectTableError> {
                if !account
                    .currencies
                    .iter()
                    .any(|currency| &currency.native_currency == name)
                {
                    return Err(ObjectTableError::new(
                        OBJECT_CURRENCY_UNKNOWN,
                        format!(
                            "account {} does not declare its {role} currency {name}",
                            account.local_account_no
                        ),
                    ));
                }
                currency_ids.get(name).copied().ok_or_else(|| {
                    ObjectTableError::new(
                        OBJECT_CURRENCY_UNKNOWN,
                        format!("currency {name} is not declared by any bound account"),
                    )
                })
            };
            let quote = resolve(&account.quote_currency, "quote")?;
            let margin = resolve(&account.margin_currency, "margin")?;
            let start = u32::try_from(currency_payloads.len()).expect("currency count bounded");
            for currency in &account.currencies {
                let mut flags = 0;
                if currency.quote {
                    flags |= CURRENCY_FLAG_QUOTE;
                }
                if currency.margin {
                    flags |= CURRENCY_FLAG_MARGIN;
                }
                if currency.settlement {
                    flags |= CURRENCY_FLAG_SETTLEMENT;
                }
                currency_payloads.push(TitanCurrencyPayloadV1 {
                    local_account_no: account.local_account_no,
                    currency_no: currency.currency_no,
                    flags,
                    reserved0: 0,
                    amount_unit: TitanDecimalV1::from_unit(currency.amount_unit),
                });
            }
            let count = account.currencies.len() as u32;
            let mut flags = 0;
            if account.ready {
                flags |= ACCOUNT_FLAG_READY;
            }
            if account.synthetic {
                flags |= ACCOUNT_FLAG_SYNTHETIC;
            }
            account_map.insert(
                account.local_account_no,
                TitanAccountPayloadV1 {
                    local_account_no: account.local_account_no,
                    account_id: account.account_id,
                    quote_currency_no: quote,
                    margin_currency_no: margin,
                    position_mode: account.position_mode as u32,
                    margin_mode: account.margin_mode as u32,
                    margin_domain: account.margin_domain,
                    flags,
                    currency_start: start,
                    currency_count: count,
                    reserved0: 0,
                    reserved1: 0,
                },
            );
        }

        // --- pairs: ratios derived from symbol units, never from configuration ---
        let mut pair_payloads: Vec<TitanPairPayloadV1> = Vec::new();
        for pair in &pairs {
            let maker = symbol_map.get(&pair.maker_local_asset_no).ok_or_else(|| {
                ObjectTableError::new(
                    OBJECT_PAIR_UNKNOWN,
                    format!(
                        "pair maker asset {} is not bound",
                        pair.maker_local_asset_no
                    ),
                )
            })?;
            let hedge = symbol_map.get(&pair.hedge_local_asset_no).ok_or_else(|| {
                ObjectTableError::new(
                    OBJECT_PAIR_UNKNOWN,
                    format!(
                        "pair hedge asset {} is not bound",
                        pair.hedge_local_asset_no
                    ),
                )
            })?;
            if !account_map.contains_key(&pair.maker_local_account_no)
                || !account_map.contains_key(&pair.hedge_local_account_no)
            {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_UNKNOWN,
                    "pair references an unbound account",
                ));
            }
            let linear = maker.valuation == Valuation::Linear as u32
                && hedge.valuation == Valuation::Linear as u32
                && maker.flags & SYMBOL_FLAG_SYNTHETIC == 0
                && hedge.flags & SYMBOL_FLAG_SYNTHETIC == 0
                && maker.flags & SYMBOL_FLAG_TRADABLE != 0
                && hedge.flags & SYMBOL_FLAG_TRADABLE != 0;
            if !linear {
                return Err(ObjectTableError::new(
                    OBJECT_ADMISSION_UNPROVABLE,
                    format!(
                        "pair {}->{} is not a proven linear, tradable, non-synthetic pair",
                        pair.maker_local_asset_no, pair.hedge_local_asset_no
                    ),
                ));
            }
            if maker.quote_currency_no != pair.quote_currency_no
                || hedge.quote_currency_no != pair.quote_currency_no
            {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_QUOTE_MISMATCH,
                    "pair quote currency differs from a leg's quote currency",
                ));
            }
            let maker_delta = maker
                .base_delta_per_lot
                .unit()
                .ok_or_else(|| ObjectTableError::new(OBJECT_UNITS_INVALID, "maker base delta"))?;
            let hedge_delta = hedge
                .base_delta_per_lot
                .unit()
                .ok_or_else(|| ObjectTableError::new(OBJECT_UNITS_INVALID, "hedge base delta"))?;
            let lot_ratio = maker_delta.ratio_to(hedge_delta).ok_or_else(|| {
                ObjectTableError::new(OBJECT_PAIR_LOT_RATIO, "lot ratio is not representable")
            })?;
            let hedge_lots_per_maker_lot = lot_ratio.as_integral_u64().filter(|ratio| *ratio > 0).ok_or_else(|| {
                ObjectTableError::new(
                    OBJECT_PAIR_LOT_RATIO,
                    format!(
                        "one maker lot needs {}/{} hedge lots; the ratio must be a positive integer",
                        lot_ratio.numerator(),
                        lot_ratio.denominator()
                    ),
                )
            })?;
            let maker_tick = maker
                .price_tick
                .unit()
                .ok_or_else(|| ObjectTableError::new(OBJECT_UNITS_INVALID, "maker price tick"))?;
            let hedge_tick = hedge
                .price_tick
                .unit()
                .ok_or_else(|| ObjectTableError::new(OBJECT_UNITS_INVALID, "hedge price tick"))?;
            let tick_ratio = hedge_tick.ratio_to(maker_tick).ok_or_else(|| {
                ObjectTableError::new(OBJECT_PAIR_TICK_RATIO, "tick ratio is not representable")
            })?;
            if tick_ratio.numerator() == 0 {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_TICK_RATIO,
                    "hedge price tick is zero",
                ));
            }
            if maker.min_qty_lots == 0 {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_MIN_FILL_UNPROVEN,
                    "maker minimum quantity is not published; a minimal fill cannot be proven",
                ));
            }
            let min_hedge_lots = maker
                .min_qty_lots
                .checked_mul(hedge_lots_per_maker_lot)
                .ok_or_else(|| {
                    ObjectTableError::new(OBJECT_PAIR_MIN_FILL_UNPROVEN, "minimal fill overflows")
                })?;
            let min_hedge_notional = hedge.min_notional_quote.unit().ok_or_else(|| {
                ObjectTableError::new(
                    OBJECT_PAIR_MIN_FILL_UNPROVEN,
                    "hedge minimum notional is not published",
                )
            })?;
            pair_payloads.push(TitanPairPayloadV1 {
                maker_local_asset_no: pair.maker_local_asset_no,
                hedge_local_asset_no: pair.hedge_local_asset_no,
                maker_local_account_no: pair.maker_local_account_no,
                hedge_local_account_no: pair.hedge_local_account_no,
                quote_currency_no: pair.quote_currency_no,
                flags: PAIR_FLAG_LINEAR_NET | PAIR_FLAG_ADMISSIBLE,
                hedge_lots_per_maker_lot,
                hedge_ticks_per_maker_tick_num: tick_ratio.numerator(),
                hedge_ticks_per_maker_tick_den: tick_ratio.denominator(),
                min_hedge_lots,
                min_hedge_notional_quote: TitanDecimalV1::from_unit(min_hedge_notional),
                pair_revision: pair.pair_revision,
                reserved0: 0,
            });
        }

        // --- fee payloads (optional, never fabricated as zero) ---
        let mut fee_payloads: Vec<TitanFeePayloadV1> = Vec::new();
        for fee in &fees {
            if !account_map.contains_key(&fee.local_account_no) {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_UNKNOWN,
                    "fee references an unbound account",
                ));
            }
            let available = fee.maker_fee_bps.is_some() && fee.taker_fee_bps.is_some();
            let mut flags = if available { FEE_FLAG_VALID } else { 0 };
            if fee.source_config {
                flags |= FEE_FLAG_SOURCE_CONFIG;
            }
            if fee.maker_fee_bps.is_some_and(|rate| rate < 0) {
                flags |= FEE_FLAG_MAKER_REBATE;
            }
            fee_payloads.push(TitanFeePayloadV1 {
                local_account_no: fee.local_account_no,
                flags,
                maker_fee_bps: fee.maker_fee_bps.unwrap_or(0),
                taker_fee_bps: fee.taker_fee_bps.unwrap_or(0),
                source: if available {
                    if fee.source_config {
                        FEE_SOURCE_CONFIG_OVERRIDE
                    } else {
                        FEE_SOURCE_EXCHANGE_PRIVATE
                    }
                } else {
                    FEE_SOURCE_UNAVAILABLE
                },
                reserved0: 0,
                observed_at_ns: fee.observed_at_ns,
            });
        }

        // --- arenas: write payloads first, then freeze pointers ---
        let mut symbol_arena: Vec<u64> = Vec::new();
        let mut symbol_offsets = BTreeMap::new();
        for (asset_no, payload) in &symbol_map {
            symbol_offsets.insert(*asset_no, write_payload(&mut symbol_arena, *payload));
        }
        let mut account_arena: Vec<u64> = Vec::new();
        let mut account_offsets = BTreeMap::new();
        for (account_no, payload) in &account_map {
            account_offsets.insert(*account_no, write_payload(&mut account_arena, *payload));
        }
        let mut currency_arena: Vec<u64> = Vec::new();
        let mut currency_offsets = Vec::with_capacity(currency_payloads.len());
        for payload in &currency_payloads {
            currency_offsets.push(write_payload(&mut currency_arena, *payload));
        }
        let mut pair_arena: Vec<u64> = Vec::new();
        let mut pair_offsets = Vec::with_capacity(pair_payloads.len());
        for payload in &pair_payloads {
            pair_offsets.push(write_payload(&mut pair_arena, *payload));
        }
        let mut fee_arena: Vec<u64> = Vec::new();
        let mut fee_offsets = Vec::with_capacity(fee_payloads.len());
        for payload in &fee_payloads {
            fee_offsets.push(write_payload(&mut fee_arena, *payload));
        }
        let mut status_arena: Vec<u64> =
            vec![0; std::mem::size_of::<TitanStatusPayloadV1>().div_ceil(8)];
        unsafe {
            std::ptr::write_unaligned(
                status_arena.as_mut_ptr().cast::<TitanStatusPayloadV1>(),
                TitanStatusPayloadV1 {
                    lifecycle: STATUS_LIFECYCLE_DEFINED,
                    ..TitanStatusPayloadV1::default()
                },
            );
        }

        let table_bytes = arena_bytes(&symbol_arena)
            + arena_bytes(&account_arena)
            + arena_bytes(&currency_arena)
            + arena_bytes(&pair_arena)
            + arena_bytes(&fee_arena)
            + arena_bytes(&status_arena);
        if table_bytes > MAX_OBJECT_TABLE_BYTES {
            return Err(ObjectTableError::new(
                OBJECT_CAPACITY_EXCEEDED,
                format!("object table exceeds {MAX_OBJECT_TABLE_BYTES} bytes"),
            ));
        }

        // Freeze arenas before taking pointers; no further mutation happens afterwards.
        let symbols = symbol_arena.into_boxed_slice();
        let accounts = account_arena.into_boxed_slice();
        let currencies = currency_arena.into_boxed_slice();
        let pairs = pair_arena.into_boxed_slice();
        let fees = fee_arena.into_boxed_slice();
        let status = status_arena.into_boxed_slice();

        let mut refs: Vec<TitanObjectRef> = Vec::with_capacity(
            symbol_map.len()
                + account_map.len()
                + currency_offsets.len()
                + pair_offsets.len()
                + fee_offsets.len()
                + 1,
        );
        for (asset_no, offset) in &symbol_offsets {
            refs.push(TitanObjectRef {
                domain: OBJECT_DOMAIN_SYMBOL,
                object_id: *asset_no,
                flags: OBJECT_REF_VALID,
                schema_major: 1,
                schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
                payload: unsafe { symbols.as_ptr().cast::<u8>().add(*offset) },
                payload_len: std::mem::size_of::<TitanSymbolPayloadV1>() as u64,
            });
        }
        for (account_no, offset) in &account_offsets {
            refs.push(TitanObjectRef {
                domain: OBJECT_DOMAIN_ACCOUNT,
                object_id: *account_no,
                flags: OBJECT_REF_VALID,
                schema_major: 1,
                schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
                payload: unsafe { accounts.as_ptr().cast::<u8>().add(*offset) },
                payload_len: std::mem::size_of::<TitanAccountPayloadV1>() as u64,
            });
        }
        for (index, offset) in currency_offsets.iter().enumerate() {
            refs.push(TitanObjectRef {
                domain: OBJECT_DOMAIN_CURRENCY,
                object_id: index as u32,
                flags: OBJECT_REF_VALID,
                schema_major: 1,
                schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
                payload: unsafe { currencies.as_ptr().cast::<u8>().add(*offset) },
                payload_len: std::mem::size_of::<TitanCurrencyPayloadV1>() as u64,
            });
        }
        for (index, offset) in pair_offsets.iter().enumerate() {
            refs.push(TitanObjectRef {
                domain: OBJECT_DOMAIN_PAIR,
                object_id: index as u32,
                flags: OBJECT_REF_VALID,
                schema_major: 1,
                schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
                payload: unsafe { pairs.as_ptr().cast::<u8>().add(*offset) },
                payload_len: std::mem::size_of::<TitanPairPayloadV1>() as u64,
            });
        }
        // Refs are ordered by `(domain, object_id)`; status (domain 5) precedes fee (domain 6), and
        // a fee is addressed by its account number, matching the SDK accessor.
        refs.push(TitanObjectRef {
            domain: OBJECT_DOMAIN_STATUS,
            object_id: 0,
            flags: OBJECT_REF_VALID | OBJECT_REF_VOLATILE,
            schema_major: 1,
            schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
            payload: status.as_ptr().cast::<u8>(),
            payload_len: std::mem::size_of::<TitanStatusPayloadV1>() as u64,
        });
        let mut fee_refs: Vec<(u32, TitanObjectRef)> = fee_payloads
            .iter()
            .zip(fee_offsets.iter())
            .map(|(payload, offset)| {
                (
                    payload.local_account_no,
                    TitanObjectRef {
                        domain: OBJECT_DOMAIN_FEE,
                        object_id: payload.local_account_no,
                        flags: if payload.flags & FEE_FLAG_VALID != 0 {
                            OBJECT_REF_VALID
                        } else {
                            0
                        },
                        schema_major: 1,
                        schema_minor: OBJECT_TABLE_SCHEMA_MINOR,
                        payload: unsafe { fees.as_ptr().cast::<u8>().add(*offset) },
                        payload_len: std::mem::size_of::<TitanFeePayloadV1>() as u64,
                    },
                )
            })
            .collect();
        fee_refs.sort_by_key(|(account_no, _)| *account_no);
        if let Some(duplicate) = fee_refs
            .windows(2)
            .find(|window| window[0].0 == window[1].0)
            .map(|window| window[0].0)
        {
            return Err(ObjectTableError::new(
                OBJECT_DUPLICATE_OBJECT,
                format!("duplicate fee object for account {duplicate}"),
            ));
        }
        refs.extend(fee_refs.into_iter().map(|(_, reference)| reference));

        let counts = ObjectCounts {
            symbols: symbol_map.len(),
            accounts: account_map.len(),
            currencies: currency_payloads.len(),
            pairs: pair_payloads.len(),
            fees: fee_payloads.len(),
        };
        let digest = identity_digest(&refs, &currency_payloads, &pair_payloads);
        Ok(Self {
            refs: refs.into_boxed_slice(),
            symbols,
            accounts,
            currencies,
            pairs,
            fees,
            status,
            digest,
            metadata_snapshot_digest,
            counts,
        })
    }

    pub fn refs(&self) -> &[TitanObjectRef] {
        &self.refs
    }

    pub fn counts(&self) -> &ObjectCounts {
        &self.counts
    }

    pub fn schema_minor(&self) -> u16 {
        OBJECT_TABLE_SCHEMA_MINOR
    }

    /// Admission identity. Changes only when a field that participates in admission changes.
    pub fn identity_digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn metadata_snapshot_digest(&self) -> [u8; 32] {
        self.metadata_snapshot_digest
    }

    pub fn object_table_bytes(&self) -> usize {
        self.symbols.len() * 8
            + self.accounts.len() * 8
            + self.currencies.len() * 8
            + self.pairs.len() * 8
            + self.fees.len() * 8
            + self.status.len() * 8
            + self.refs.len() * std::mem::size_of::<TitanObjectRef>()
    }

    fn find(&self, domain: u32, object_id: u32) -> Option<&TitanObjectRef> {
        self.refs
            .iter()
            .find(|reference| reference.domain == domain && reference.object_id == object_id)
    }

    fn payload_at<T: Copy>(&self, domain: u32, object_id: u32, require_valid: bool) -> Option<T> {
        let reference = self.find(domain, object_id)?;
        if (require_valid && reference.flags & OBJECT_REF_VALID == 0)
            || reference.payload.is_null()
            || reference.payload_len < std::mem::size_of::<T>() as u64
            || !(reference.payload as usize).is_multiple_of(std::mem::align_of::<T>())
        {
            return None;
        }
        Some(unsafe { std::ptr::read_unaligned(reference.payload.cast::<T>()) })
    }

    fn payload<T: Copy>(&self, domain: u32, object_id: u32) -> Option<T> {
        self.payload_at(domain, object_id, true)
    }

    pub fn symbol(&self, asset_no: u32) -> Option<TitanSymbolPayloadV1> {
        self.payload(OBJECT_DOMAIN_SYMBOL, asset_no)
    }

    pub fn account(&self, account_no: u32) -> Option<TitanAccountPayloadV1> {
        self.payload(OBJECT_DOMAIN_ACCOUNT, account_no)
    }

    pub fn currency(&self, account_no: u32, currency_no: u32) -> Option<TitanCurrencyPayloadV1> {
        let account = self.account(account_no)?;
        let start = account.currency_start;
        let end = start.checked_add(account.currency_count)?;
        (start..end).find_map(|index| {
            let payload = self.payload::<TitanCurrencyPayloadV1>(OBJECT_DOMAIN_CURRENCY, index)?;
            (payload.local_account_no == account_no && payload.currency_no == currency_no)
                .then_some(payload)
        })
    }

    pub fn pair(&self, pair_no: u32) -> Option<TitanPairPayloadV1> {
        self.payload(OBJECT_DOMAIN_PAIR, pair_no)
    }

    /// A fee object may legitimately carry `valid = 0`; the caller must branch instead of
    /// treating a missing rate as zero. Only a missing reference yields `None`.
    pub fn fee(&self, account_no: u32) -> Option<TitanFeePayloadV1> {
        self.payload_at(OBJECT_DOMAIN_FEE, account_no, false)
    }

    /// The status payload is the only mutable object; callers write it before each callback.
    /// Status accessor that tolerates a table without a status object (e.g. the empty pre-attach
    /// table). Returns `None` instead of dereferencing an empty arena.
    pub fn try_status_mut(&mut self) -> Option<&mut TitanStatusPayloadV1> {
        if self.status.is_empty() {
            None
        } else {
            Some(self.status_mut())
        }
    }

    pub fn status_mut(&mut self) -> &mut TitanStatusPayloadV1 {
        unsafe { &mut *self.status.as_mut_ptr().cast::<TitanStatusPayloadV1>() }
    }

    /// Reads the status object. A table without one (the pre-attach empty table) yields zeroes
    /// instead of dereferencing an empty arena.
    pub fn status(&self) -> TitanStatusPayloadV1 {
        if self.status.is_empty() {
            return TitanStatusPayloadV1::default();
        }
        unsafe { std::ptr::read_unaligned(self.status.as_ptr().cast::<TitanStatusPayloadV1>()) }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectTableInput {
    pub symbols: Vec<SymbolObjectInput>,
    pub accounts: Vec<AccountObjectInput>,
    pub pairs: Vec<PairObjectInput>,
    pub fees: Vec<FeeObjectInput>,
    pub metadata_snapshot_digest: [u8; 32],
}

fn identity_digest(
    refs: &[TitanObjectRef],
    currencies: &[TitanCurrencyPayloadV1],
    pairs: &[TitanPairPayloadV1],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"titan-object-identity-v1");
    hasher.update(OBJECT_IDENTITY_VERSION.to_le_bytes());
    hasher.update(1u16.to_le_bytes());
    hasher.update([OBJECT_DOMAIN_SYMBOL as u8]);
    for reference in refs.iter().filter(|r| r.domain == OBJECT_DOMAIN_SYMBOL) {
        let payload: TitanSymbolPayloadV1 =
            unsafe { std::ptr::read_unaligned(reference.payload.cast()) };
        hasher.update(payload.local_asset_no.to_le_bytes());
        decimal_bytes(&mut hasher, payload.price_tick.unit());
        decimal_bytes(&mut hasher, payload.quantity_lot.unit());
        decimal_bytes(&mut hasher, payload.contract_multiplier.unit());
        decimal_bytes(&mut hasher, payload.base_delta_per_lot.unit());
        decimal_bytes(&mut hasher, payload.min_quantity.unit());
        decimal_bytes(&mut hasher, payload.min_notional_quote.unit());
        hasher.update(payload.min_qty_lots.to_le_bytes());
        hasher.update(payload.min_notional_tick_lots.to_le_bytes());
        hasher.update(payload.quote_currency_no.to_le_bytes());
        hasher.update(payload.margin_currency_no.to_le_bytes());
        hasher.update(payload.valuation.to_le_bytes());
        hasher.update([(payload.flags & SYMBOL_FLAG_TRADABLE != 0) as u8]);
    }
    hasher.update([OBJECT_DOMAIN_ACCOUNT as u8]);
    for reference in refs.iter().filter(|r| r.domain == OBJECT_DOMAIN_ACCOUNT) {
        let payload: TitanAccountPayloadV1 =
            unsafe { std::ptr::read_unaligned(reference.payload.cast()) };
        hasher.update(payload.local_account_no.to_le_bytes());
        hasher.update(payload.quote_currency_no.to_le_bytes());
        hasher.update(payload.margin_currency_no.to_le_bytes());
        hasher.update(payload.margin_domain.to_le_bytes());
        hasher.update(payload.position_mode.to_le_bytes());
        hasher.update(payload.margin_mode.to_le_bytes());
    }
    hasher.update([OBJECT_DOMAIN_CURRENCY as u8]);
    for payload in currencies {
        hasher.update(payload.local_account_no.to_le_bytes());
        hasher.update(payload.currency_no.to_le_bytes());
        decimal_bytes(&mut hasher, payload.amount_unit.unit());
    }
    hasher.update([OBJECT_DOMAIN_PAIR as u8]);
    for payload in pairs {
        hasher.update(payload.maker_local_asset_no.to_le_bytes());
        hasher.update(payload.hedge_local_asset_no.to_le_bytes());
        hasher.update(payload.hedge_lots_per_maker_lot.to_le_bytes());
        hasher.update(payload.hedge_ticks_per_maker_tick_num.to_le_bytes());
        hasher.update(payload.hedge_ticks_per_maker_tick_den.to_le_bytes());
        hasher.update(payload.min_hedge_lots.to_le_bytes());
        decimal_bytes(&mut hasher, payload.min_hedge_notional_quote.unit());
        hasher.update(payload.quote_currency_no.to_le_bytes());
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().expect("test decimal")
    }

    fn symbol(
        asset_no: u32,
        lot: &str,
        multiplier: &str,
        min_quantity: Option<&str>,
        min_notional: Option<&str>,
    ) -> SymbolObjectInput {
        SymbolObjectInput {
            local_asset_no: asset_no,
            asset_id: asset_no + 100,
            native_symbol: Arc::from(format!("TEST-{asset_no}")),
            source_key: Arc::from("venue"),
            price_tick: unit("0.1"),
            quantity_lot: unit(lot),
            contract_multiplier: unit(multiplier),
            min_quantity: min_quantity.map(unit),
            min_notional_quote: min_notional.map(unit),
            quote_currency: Arc::from("USDT"),
            margin_currency: Arc::from("USDT"),
            valuation: Valuation::Linear,
            tradable: true,
            synthetic: false,
            metadata_version: 1,
        }
    }

    fn account(account_no: u32, currency_no: u32) -> AccountObjectInput {
        AccountObjectInput {
            local_account_no: account_no,
            account_id: account_no + 200,
            quote_currency: Arc::from("USDT"),
            margin_currency: Arc::from("USDT"),
            position_mode: PositionMode::Net,
            margin_mode: MarginMode::Cross,
            margin_domain: account_no,
            ready: true,
            synthetic: false,
            currencies: vec![CurrencyObjectInput {
                currency_no,
                native_currency: Arc::from("USDT"),
                amount_unit: unit("0.000001"),
                quote: true,
                margin: true,
                settlement: true,
            }],
        }
    }

    fn pair() -> PairObjectInput {
        PairObjectInput {
            maker_local_asset_no: 1,
            hedge_local_asset_no: 0,
            maker_local_account_no: 0,
            hedge_local_account_no: 1,
            quote_currency_no: 1,
            pair_revision: 7,
        }
    }

    fn fee(account_no: u32, maker: Option<i32>, taker: Option<i32>) -> FeeObjectInput {
        FeeObjectInput {
            local_account_no: account_no,
            maker_fee_bps: maker,
            taker_fee_bps: taker,
            source_config: true,
            observed_at_ns: 42,
        }
    }

    fn base_input() -> ObjectTableInput {
        ObjectTableInput {
            symbols: vec![
                symbol(1, "0.01", "1", Some("0.01"), Some("5")),
                symbol(0, "0.001", "1", Some("0.001"), Some("5")),
                // Not part of the pair: exercises objects that exist without admission use.
                symbol(2, "0.01", "1", Some("0.01"), Some("5")),
            ],
            accounts: vec![account(0, 1), account(1, 1)],
            pairs: vec![pair()],
            fees: vec![
                fee(0, Some(-1), Some(5)),
                // Missing rates must be reported as unavailable, never as zero fee.
                fee(1, None, None),
            ],
            metadata_snapshot_digest: [9; 32],
        }
    }

    #[test]
    fn table_derives_units_ratios_and_refs_in_canonical_order() {
        let table = ObjectTable::build(base_input()).expect("valid table");
        assert_eq!(table.counts().symbols, 3);
        assert_eq!(table.counts().accounts, 2);
        assert_eq!(table.counts().currencies, 2);
        assert_eq!(table.counts().pairs, 1);
        assert_eq!(table.counts().fees, 2);
        assert!(table.object_table_bytes() < MAX_OBJECT_TABLE_BYTES);
        assert_eq!(table.metadata_snapshot_digest(), [9; 32]);

        let maker = table.symbol(1).unwrap();
        assert_eq!(maker.base_delta_per_lot.unit(), Some(unit("0.01")));
        assert_eq!(maker.min_qty_lots, 1);
        // ceil(5 / (0.1 * 0.01)) = 5000
        assert_eq!(maker.min_notional_tick_lots, 5000);
        assert_ne!(maker.flags & SYMBOL_FLAG_TRADABLE, 0);
        assert_ne!(maker.flags & SYMBOL_FLAG_METADATA_COMPLETE, 0);

        let hedge = table.symbol(0).unwrap();
        assert_eq!(hedge.base_delta_per_lot.unit(), Some(unit("0.001")));

        let pair = table.pair(0).unwrap();
        assert_eq!(pair.hedge_lots_per_maker_lot, 10);
        assert_eq!(pair.hedge_ticks_per_maker_tick_num, 1);
        assert_eq!(pair.hedge_ticks_per_maker_tick_den, 1);
        assert_eq!(pair.min_hedge_lots, 10);
        assert_eq!(pair.min_hedge_notional_quote.unit(), Some(unit("5")));
        assert_eq!(pair.flags & PAIR_FLAG_ADMISSIBLE, PAIR_FLAG_ADMISSIBLE);

        let account = table.account(0).unwrap();
        assert_eq!(account.quote_currency_no, 1);
        assert_eq!(account.currency_count, 1);
        assert_ne!(account.flags & ACCOUNT_FLAG_READY, 0);

        let currency = table.currency(0, 1).unwrap();
        assert_eq!(currency.amount_unit.unit(), Some(unit("0.000001")));
        assert!(table.currency(0, 2).is_none());

        let maker_fee = table.fee(0).unwrap();
        assert_ne!(maker_fee.flags & FEE_FLAG_VALID, 0);
        assert_ne!(maker_fee.flags & FEE_FLAG_MAKER_REBATE, 0);
        assert_eq!(maker_fee.maker_fee_bps, -1);
        let missing_fee = table.fee(1).unwrap();
        assert_eq!(missing_fee.flags & FEE_FLAG_VALID, 0);
        assert_eq!(missing_fee.source, FEE_SOURCE_UNAVAILABLE);

        // Refs are ordered by domain, then object id; status is volatile and last in domain order.
        let mut previous = (0u32, 0u32);
        for reference in table.refs() {
            assert!((reference.domain, reference.object_id) > previous);
            previous = (reference.domain, reference.object_id);
            assert_eq!(reference.schema_major, 1);
            assert_eq!(reference.schema_minor, 1);
            assert!(!reference.payload.is_null());
            assert_eq!(reference.payload as usize % 8, 0);
        }
        assert_eq!(
            table.refs().last().unwrap().domain,
            OBJECT_DOMAIN_FEE,
            "fee is the highest domain"
        );
        assert_eq!(table.status().lifecycle, STATUS_LIFECYCLE_DEFINED);
    }

    #[test]
    fn identity_covers_admission_fields_only() {
        let base = ObjectTable::build(base_input()).unwrap().identity_digest();
        assert_eq!(
            base,
            ObjectTable::build(base_input()).unwrap().identity_digest(),
            "identity is deterministic"
        );

        // Fees, status and audit fields do not participate.
        let mut fees_changed = base_input();
        fees_changed.fees = vec![fee(0, Some(1), Some(2)), fee(1, Some(3), Some(4))];
        assert_eq!(
            ObjectTable::build(fees_changed).unwrap().identity_digest(),
            base
        );
        let mut audit_changed = base_input();
        // Asset 2 is not part of the pair, so its synthetic flag is not an admission fact here and
        // must not move the identity.
        audit_changed.symbols[2].synthetic = true;
        audit_changed.symbols[0].metadata_version = 99;
        audit_changed.pairs[0].pair_revision = 99;
        assert_eq!(
            ObjectTable::build(audit_changed).unwrap().identity_digest(),
            base
        );

        // A unit or collateral-domain change is an admission change.
        let mut units_changed = base_input();
        units_changed.symbols[0].price_tick = unit("0.2");
        assert_ne!(
            ObjectTable::build(units_changed).unwrap().identity_digest(),
            base
        );
        let mut domain_changed = base_input();
        domain_changed.accounts[1].margin_domain = 9;
        assert_ne!(
            ObjectTable::build(domain_changed)
                .unwrap()
                .identity_digest(),
            base
        );
    }

    #[test]
    fn status_is_the_only_mutable_object() {
        let mut table = ObjectTable::build(base_input()).unwrap();
        let before = table.identity_digest();
        table.status_mut().lifecycle = STATUS_LIFECYCLE_RUNNING;
        table.status_mut().entry_flags = STATUS_ENTRY_COMMAND_GATE_OPEN;
        assert_eq!(table.status().lifecycle, STATUS_LIFECYCLE_RUNNING);
        assert_eq!(table.identity_digest(), before);
    }

    #[test]
    fn non_integral_lot_ratio_is_refused() {
        let mut input = base_input();
        input.symbols[1].quantity_lot = unit("0.003");
        let error = ObjectTable::build(input).expect_err("3/10 ratio");
        assert_eq!(error.code(), OBJECT_PAIR_LOT_RATIO);
    }

    #[test]
    fn synthetic_inverse_or_unpublished_units_refuse_admission() {
        let mut synthetic = base_input();
        synthetic.symbols[0].synthetic = true;
        assert_eq!(
            ObjectTable::build(synthetic).unwrap_err().code(),
            OBJECT_ADMISSION_UNPROVABLE
        );

        let mut inverse = base_input();
        inverse.symbols[1].valuation = Valuation::Inverse;
        assert_eq!(
            ObjectTable::build(inverse).unwrap_err().code(),
            OBJECT_ADMISSION_UNPROVABLE
        );

        let mut missing_maker_min = base_input();
        missing_maker_min.symbols[0].min_quantity = None;
        assert_eq!(
            ObjectTable::build(missing_maker_min).unwrap_err().code(),
            OBJECT_PAIR_MIN_FILL_UNPROVEN
        );

        let mut missing_hedge_notional = base_input();
        missing_hedge_notional.symbols[1].min_notional_quote = None;
        assert_eq!(
            ObjectTable::build(missing_hedge_notional)
                .unwrap_err()
                .code(),
            OBJECT_PAIR_MIN_FILL_UNPROVEN
        );
        let _ = crate::context::object_schema_hash();
    }

    #[test]
    fn currency_identity_must_be_unambiguous() {
        let mut conflict = base_input();
        conflict.accounts[1].currencies[0].currency_no = 2;
        assert_eq!(
            ObjectTable::build(conflict).unwrap_err().code(),
            OBJECT_CURRENCY_ID_CONFLICT
        );

        let mut unknown = base_input();
        unknown.symbols[0].quote_currency = Arc::from("EUR");
        assert_eq!(
            ObjectTable::build(unknown).unwrap_err().code(),
            OBJECT_CURRENCY_UNKNOWN
        );

        // An account must declare the currency it claims as quote/margin.
        let mut undeclared = base_input();
        undeclared.accounts[0].quote_currency = Arc::from("USDC");
        assert_eq!(
            ObjectTable::build(undeclared).unwrap_err().code(),
            OBJECT_CURRENCY_UNKNOWN
        );
    }

    #[test]
    fn capacity_limits_are_enforced() {
        let mut input = base_input();
        input.symbols = (0..=MAX_OBJECT_SYMBOLS as u32)
            .map(|asset| symbol(asset, "0.01", "1", Some("0.01"), Some("5")))
            .collect();
        assert_eq!(
            ObjectTable::build(input).unwrap_err().code(),
            OBJECT_CAPACITY_EXCEEDED
        );
    }

    #[test]
    fn duplicate_fee_or_symbol_objects_are_refused() {
        let mut duplicate_symbol = base_input();
        duplicate_symbol
            .symbols
            .push(symbol(1, "0.01", "1", Some("0.01"), Some("5")));
        assert_eq!(
            ObjectTable::build(duplicate_symbol).unwrap_err().code(),
            OBJECT_DUPLICATE_OBJECT
        );
        let mut duplicate_fee = base_input();
        duplicate_fee.fees.push(fee(0, Some(1), Some(1)));
        assert_eq!(
            ObjectTable::build(duplicate_fee).unwrap_err().code(),
            OBJECT_DUPLICATE_OBJECT
        );
    }
}

#[cfg(test)]
mod hot_path_probe {
    use super::*;
    use crate::context::{
        FEE_FLAG_VALID, MarginMode, OBJECT_DOMAIN_SYMBOL, PositionMode, TitanObjectRef,
        TitanSymbolPayloadV1, Valuation,
    };
    use crate::test_allocations::ALLOCATIONS;
    use std::hint::black_box;
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().unwrap()
    }

    /// Mirrors the generated `ctx.symbol(0)` scan, including the guard conditions.
    fn symbol_scan(refs: &[TitanObjectRef]) -> (u64, u8) {
        let mut observed = (0u64, 0u8);
        for reference in refs {
            if reference.domain == OBJECT_DOMAIN_SYMBOL
                && reference.object_id == 0
                && reference.flags & 1 != 0
                && reference.schema_major == 1
                && reference.schema_minor >= 1
                && !reference.payload.is_null()
                && (reference.payload as usize).is_multiple_of(8)
                && reference.payload_len >= size_of::<TitanSymbolPayloadV1>() as u64
            {
                let payload: TitanSymbolPayloadV1 =
                    unsafe { std::ptr::read_unaligned(reference.payload.cast()) };
                observed = match payload.price_tick.unit() {
                    Some(value) => (value.coefficient(), value.scale()),
                    None => (0, 0),
                };
            }
        }
        observed
    }

    fn probe_table() -> ObjectTable {
        ObjectTable::build(ObjectTableInput {
            symbols: vec![SymbolObjectInput {
                local_asset_no: 0,
                asset_id: 1,
                native_symbol: Arc::from("TEST"),
                source_key: Arc::from("venue"),
                price_tick: unit("0.1"),
                quantity_lot: unit("0.01"),
                contract_multiplier: unit("1"),
                min_quantity: Some(unit("0.01")),
                min_notional_quote: Some(unit("5")),
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
            fees: vec![FeeObjectInput {
                local_account_no: 0,
                maker_fee_bps: Some(-2),
                taker_fee_bps: Some(5),
                source_config: true,
                observed_at_ns: 1,
            }],
            metadata_snapshot_digest: [0; 32],
        })
        .expect("table admits the fixture")
    }

    #[test]
    fn object_access_is_allocation_free_after_build() {
        let table = probe_table();
        let refs = table.refs();
        // Warm-up: the first reads may touch lazily initialised machinery.
        black_box(symbol_scan(black_box(refs)));
        black_box(table.fee(0));

        ALLOCATIONS.with(|value| value.set(Some(0)));
        for _ in 0..1_000 {
            assert_eq!(black_box(symbol_scan(black_box(refs))), (1, 1));
            let fee = black_box(table.fee(0)).expect("fee object exists");
            assert_eq!(fee.maker_fee_bps, -2);
            assert_eq!(fee.flags & FEE_FLAG_VALID, FEE_FLAG_VALID);
        }
        let allocations = ALLOCATIONS.with(|value| value.replace(None).unwrap());
        assert_eq!(allocations, 0, "object access must not allocate");
    }
}

#[cfg(test)]
mod diagnostics_identity_tests {
    use super::*;
    use crate::context::{MarginMode, PositionMode, Valuation};
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().unwrap()
    }

    fn input(price_tick: &str) -> ObjectTableInput {
        ObjectTableInput {
            symbols: vec![SymbolObjectInput {
                local_asset_no: 0,
                asset_id: 1,
                native_symbol: Arc::from("TEST"),
                source_key: Arc::from("venue"),
                price_tick: unit(price_tick),
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
            metadata_snapshot_digest: [7; 32],
        }
    }

    #[test]
    fn operator_digests_track_units_but_not_the_snapshot_digest() {
        let first = object_table_identity_probe(&input("0.1"));
        let same = object_table_identity_probe(&input("0.10"));
        let changed = object_table_identity_probe(&input("0.5"));
        assert_eq!(
            first.0, same.0,
            "equivalent decimals normalise to one identity"
        );
        assert_ne!(
            first.0, changed.0,
            "a different tick must change the identity"
        );
        assert_eq!(first.1, [7; 32]);
        assert_eq!(first.1, changed.1, "snapshot digest is reported separately");

        let mut other_snapshot = input("0.1");
        other_snapshot.metadata_snapshot_digest = [9; 32];
        let probe = object_table_identity_probe(&other_snapshot);
        assert_eq!(
            probe.0, first.0,
            "identity covers admission fields, not the snapshot"
        );
        assert_eq!(probe.1, [9; 32]);
    }

    fn object_table_identity_probe(input: &ObjectTableInput) -> ([u8; 32], [u8; 32]) {
        let table = ObjectTable::build(input.clone()).expect("table admits the fixture");
        (table.identity_digest(), table.metadata_snapshot_digest())
    }
}

#[cfg(test)]
mod status_publication_probe {
    use super::*;
    use crate::context::{MarginMode, PositionMode, Valuation};
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().unwrap()
    }

    fn table() -> ObjectTable {
        ObjectTable::build(ObjectTableInput {
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
        .expect("table admits the fixture")
    }

    /// The written contract: entry state is published before the callback, the outcome after it, so
    /// `last_*` always describes the *previous* callback (lagging semantics, matrix item 11).
    #[test]
    fn status_publishes_entry_state_before_and_outcome_after() {
        let mut table = table();
        // Before any callback the status object is the build-time default.
        assert_eq!(table.status().lifecycle, 0);
        assert_eq!(table.status().last_return_code, 0);

        // Host entry: running, command gate open, not paused (lifecycle values are ABI numbers).
        table.try_status_mut().unwrap().lifecycle = crate::STATUS_LIFECYCLE_RUNNING;
        table.try_status_mut().unwrap().entry_flags = crate::STATUS_ENTRY_COMMAND_GATE_OPEN;
        assert_eq!(table.status().lifecycle, crate::STATUS_LIFECYCLE_RUNNING);

        // Post-callback outcome of the first callback.
        table.try_status_mut().unwrap().last_return_code = 0;
        table.try_status_mut().unwrap().last_error_code = 0;

        // Second callback's entry state must not disturb the recorded outcome.
        table.try_status_mut().unwrap().lifecycle = crate::STATUS_LIFECYCLE_PAUSED;
        table.try_status_mut().unwrap().entry_flags = crate::STATUS_ENTRY_PAUSED;
        let during = table.status();
        assert_eq!(during.lifecycle, crate::STATUS_LIFECYCLE_PAUSED);
        assert_eq!(during.entry_flags, crate::STATUS_ENTRY_PAUSED);
        assert_eq!(
            during.last_return_code, 0,
            "last_* still describes callback #1"
        );

        // Only after the second callback returns does last_* advance.
        table.try_status_mut().unwrap().last_return_code = -4;
        table.try_status_mut().unwrap().last_error_code = -2;
        let after = table.status();
        assert_eq!(after.last_return_code, -4);
        assert_eq!(after.last_error_code, -2);
        assert_eq!(
            after.lifecycle,
            crate::STATUS_LIFECYCLE_PAUSED,
            "entry state persists"
        );
    }

    #[test]
    fn empty_table_has_no_status_object_until_attached() {
        let mut empty = ObjectTable::empty();
        assert!(empty.try_status_mut().is_none());
        assert_eq!(
            empty.status().lifecycle,
            0,
            "reading an absent status yields zeroes"
        );
    }
}
