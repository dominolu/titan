//! Two-layer object requirements: what an artifact demands, and whether the built table proves it.
//!
//! The gate is deliberately split:
//!
//! - **artifact/load layer** has no object table yet. It only compares the artifact's declared
//!   requirements with the *host compile-time* constants (schema major/minor, known domains,
//!   payload sizes, field bitmaps). A newer artifact is refused before any callback could run.
//! - **instance/create layer** runs after [`ObjectTable`] construction and checks the actual
//!   references/payloads (count, major, minor, length, field presence, required fees).
//!
//! Reason codes are stable and are what the CLI/management error surfaces report.

use crate::context::{
    FEE_FLAG_VALID, OBJECT_DOMAIN_ACCOUNT, OBJECT_DOMAIN_CURRENCY, OBJECT_DOMAIN_FEE,
    OBJECT_DOMAIN_PAIR, OBJECT_DOMAIN_STATUS, OBJECT_DOMAIN_SYMBOL, TitanAccountPayloadV1,
    TitanCurrencyPayloadV1, TitanFeePayloadV1, TitanPairPayloadV1, TitanStatusPayloadV1,
    TitanSymbolPayloadV1,
};
use crate::object_table::ObjectTable;

/// Payload schema major the V14 host understands. A requirement with another major is refused.
pub const HOST_OBJECT_SCHEMA_MAJOR: u16 = crate::OBJECT_SCHEMA_MAJOR;
/// Highest payload minor the V14 host understands.
pub const HOST_OBJECT_SCHEMA_MINOR: u16 = crate::OBJECT_SCHEMA_MINOR;
/// Identity digest format version the host understands.
pub const HOST_OBJECT_IDENTITY_VERSION: u32 = 1;
/// Manifest capability granting access to the object table.
///
/// This string is the single source for one conceptual capability that has two wire encodings:
///
/// - manifest/`ManifestCapabilities` bit [`crate::ManifestCapabilities::OBJECT_CONTEXT`]
///   (`1 << 11`, the frozen compiled strategy manifest format);
/// - runtime `StrategyCapabilities` bit [`crate::StrategyCapabilities::OBJECT_CONTEXT`]
///   (`1 << 13`, the V14 runtime format consumed after the compiled strategy→V14 mapping).
///
/// The bit numbers differ because the two bit sets are different wire formats, not because the
/// capability is different: [`crate::compiled_strategy::manifest_from_artifact`] turns the compiled strategy bit into the V14 bit, and the
/// V14 instance gate in `native_runtime.rs` is the only consumer of the V14 bit. Do not add a
/// separate literal for either bit elsewhere; reference this constant or the two typed constants.
pub const OBJECT_CONTEXT_CAPABILITY: &str = "object_context";

/// Symbol `required_fields` bits, matching the symbol payload spec.
pub const SYMBOL_FIELD_PRICE_TICK: u64 = 1 << 0;
pub const SYMBOL_FIELD_QUANTITY_LOT: u64 = 1 << 1;
pub const SYMBOL_FIELD_CONTRACT_MULTIPLIER: u64 = 1 << 2;
pub const SYMBOL_FIELD_BASE_DELTA_PER_LOT: u64 = 1 << 3;
pub const SYMBOL_FIELD_MIN_QUANTITY: u64 = 1 << 4;
pub const SYMBOL_FIELD_MIN_QTY_LOTS: u64 = 1 << 5;
pub const SYMBOL_FIELD_MIN_NOTIONAL_QUOTE: u64 = 1 << 6;
pub const SYMBOL_FIELD_MIN_NOTIONAL_TICK_LOTS: u64 = 1 << 7;
pub const SYMBOL_FIELD_QUOTE_CURRENCY_NO: u64 = 1 << 8;
pub const SYMBOL_FIELD_MARGIN_CURRENCY_NO: u64 = 1 << 9;
pub const SYMBOL_FIELD_VALUATION: u64 = 1 << 10;
pub const SYMBOL_FIELD_TRADABLE: u64 = 1 << 11;
/// Every bit defined for symbol payload v1.
pub const SYMBOL_FIELD_ALL: u64 = 0xFFF;

pub const OBJECT_GATE_SCHEMA_MAJOR: &str = "object_schema_major_unsupported";
pub const OBJECT_GATE_SCHEMA_MINOR: &str = "object_schema_minor_too_new";
pub const OBJECT_GATE_IDENTITY_VERSION: &str = "object_identity_version_too_new";
pub const OBJECT_GATE_REQUIREMENT_INVALID: &str = "object_requirement_invalid";
pub const OBJECT_GATE_MISSING: &str = "object_requirement_missing";
pub const OBJECT_GATE_MAJOR_MISMATCH: &str = "object_major_mismatch";
pub const OBJECT_GATE_PAYLOAD_SHORT: &str = "object_payload_short";
pub const OBJECT_GATE_FIELD_MISSING: &str = "object_field_missing";
pub const OBJECT_GATE_FEE_REQUIRED: &str = "object_fee_required";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectGateError {
    code: &'static str,
    detail: String,
}

impl ObjectGateError {
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

impl std::fmt::Display for ObjectGateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for ObjectGateError {}

/// One domain/field requirement declared by an artifact manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectRequirement {
    pub domain: u32,
    pub major: u16,
    pub min_minor: u16,
    pub min_payload_len: u64,
    pub min_count: u32,
    pub required_fields: u64,
}

/// Everything an artifact declares about the object context it needs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectManifest {
    pub schema_major: u16,
    pub schema_minor: u16,
    pub schema_hash: Option<[u8; 32]>,
    pub identity_version: u32,
    pub requires_fee: bool,
    pub requirements: Vec<ObjectRequirement>,
}

/// Host payload size for a domain at schema major 1, or `None` for an unknown domain.
pub fn host_payload_len(domain: u32) -> Option<usize> {
    match domain {
        OBJECT_DOMAIN_SYMBOL => Some(std::mem::size_of::<TitanSymbolPayloadV1>()),
        OBJECT_DOMAIN_ACCOUNT => Some(std::mem::size_of::<TitanAccountPayloadV1>()),
        OBJECT_DOMAIN_CURRENCY => Some(std::mem::size_of::<TitanCurrencyPayloadV1>()),
        OBJECT_DOMAIN_PAIR => Some(std::mem::size_of::<TitanPairPayloadV1>()),
        OBJECT_DOMAIN_STATUS => Some(std::mem::size_of::<TitanStatusPayloadV1>()),
        OBJECT_DOMAIN_FEE => Some(std::mem::size_of::<TitanFeePayloadV1>()),
        _ => None,
    }
}

impl ObjectManifest {
    /// Artifact/load layer: compares the declaration with host compile-time capabilities.
    ///
    /// The table does not exist yet, so only the shape and the host's supported version/payload
    /// sizes can be checked. Anything a newer host could satisfy must be refused here rather than
    /// failing inside a callback.
    pub fn validate_artifact(&self) -> Result<(), ObjectGateError> {
        if self.schema_major != HOST_OBJECT_SCHEMA_MAJOR {
            return Err(ObjectGateError::new(
                OBJECT_GATE_SCHEMA_MAJOR,
                format!(
                    "artifact requires object schema major {}, host supports {}",
                    self.schema_major, HOST_OBJECT_SCHEMA_MAJOR
                ),
            ));
        }
        if self.schema_minor > HOST_OBJECT_SCHEMA_MINOR {
            return Err(ObjectGateError::new(
                OBJECT_GATE_SCHEMA_MINOR,
                format!(
                    "artifact requires object schema minor {}, host supports {}",
                    self.schema_minor, HOST_OBJECT_SCHEMA_MINOR
                ),
            ));
        }
        if self.identity_version > HOST_OBJECT_IDENTITY_VERSION {
            return Err(ObjectGateError::new(
                OBJECT_GATE_IDENTITY_VERSION,
                format!(
                    "artifact requires object identity version {}, host supports {}",
                    self.identity_version, HOST_OBJECT_IDENTITY_VERSION
                ),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for requirement in &self.requirements {
            let Some(host_len) = host_payload_len(requirement.domain) else {
                return Err(ObjectGateError::new(
                    OBJECT_GATE_REQUIREMENT_INVALID,
                    format!("unknown object domain {}", requirement.domain),
                ));
            };
            if requirement.major != HOST_OBJECT_SCHEMA_MAJOR
                || requirement.min_minor == 0
                || requirement.min_minor > HOST_OBJECT_SCHEMA_MINOR
                || requirement.min_payload_len == 0
                || requirement.min_payload_len > host_len as u64
                || requirement.min_count == 0
                || !seen.insert(requirement.domain)
            {
                return Err(ObjectGateError::new(
                    OBJECT_GATE_REQUIREMENT_INVALID,
                    format!(
                        "domain {} requirement is malformed (major {}, min_minor {}, min_payload_len {}, min_count {})",
                        requirement.domain,
                        requirement.major,
                        requirement.min_minor,
                        requirement.min_payload_len,
                        requirement.min_count
                    ),
                ));
            }
            let field_bits = if requirement.domain == OBJECT_DOMAIN_SYMBOL {
                SYMBOL_FIELD_ALL
            } else {
                0
            };
            if requirement.required_fields & !field_bits != 0 {
                return Err(ObjectGateError::new(
                    OBJECT_GATE_REQUIREMENT_INVALID,
                    format!(
                        "domain {} declares unsupported field bits {:#x}",
                        requirement.domain, requirement.required_fields
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Instance/create layer: proves the requirements against a built [`ObjectTable`].
    pub fn validate_instance(&self, table: &ObjectTable) -> Result<(), ObjectGateError> {
        for requirement in &self.requirements {
            let refs = table
                .refs()
                .iter()
                .filter(|reference| reference.domain == requirement.domain)
                .collect::<Vec<_>>();
            if refs.len() < requirement.min_count as usize {
                return Err(ObjectGateError::new(
                    OBJECT_GATE_MISSING,
                    format!(
                        "domain {} has {} object(s), artifact requires {}",
                        requirement.domain,
                        refs.len(),
                        requirement.min_count
                    ),
                ));
            }
            for reference in refs {
                if reference.schema_major != requirement.major {
                    return Err(ObjectGateError::new(
                        OBJECT_GATE_MAJOR_MISMATCH,
                        format!(
                            "domain {} object {} has schema major {}, artifact requires {}",
                            requirement.domain,
                            reference.object_id,
                            reference.schema_major,
                            requirement.major
                        ),
                    ));
                }
                if reference.schema_minor < requirement.min_minor
                    || reference.payload_len < requirement.min_payload_len
                {
                    return Err(ObjectGateError::new(
                        OBJECT_GATE_PAYLOAD_SHORT,
                        format!(
                            "domain {} object {} exposes minor {}/{} bytes, artifact requires {}/{}+",
                            requirement.domain,
                            reference.object_id,
                            reference.schema_minor,
                            reference.payload_len,
                            requirement.min_minor,
                            requirement.min_payload_len
                        ),
                    ));
                }
            }
            if requirement.domain == OBJECT_DOMAIN_SYMBOL && requirement.required_fields != 0 {
                for reference in table
                    .refs()
                    .iter()
                    .filter(|reference| reference.domain == OBJECT_DOMAIN_SYMBOL)
                {
                    if reference.payload.is_null()
                        || reference.payload_len
                            < std::mem::size_of::<TitanSymbolPayloadV1>() as u64
                    {
                        return Err(ObjectGateError::new(
                            OBJECT_GATE_PAYLOAD_SHORT,
                            format!("symbol {} payload is truncated", reference.object_id),
                        ));
                    }
                    let payload: TitanSymbolPayloadV1 = unsafe {
                        std::ptr::read_unaligned(reference.payload.cast::<TitanSymbolPayloadV1>())
                    };
                    let present = symbol_field_presence(&payload);
                    let missing = requirement.required_fields & !present;
                    if missing != 0 {
                        return Err(ObjectGateError::new(
                            OBJECT_GATE_FIELD_MISSING,
                            format!(
                                "symbol {} lacks required field bits {missing:#x}",
                                reference.object_id
                            ),
                        ));
                    }
                }
            }
        }
        if self.requires_fee {
            for account in table
                .refs()
                .iter()
                .filter(|reference| reference.domain == OBJECT_DOMAIN_ACCOUNT)
            {
                let fee = table.fee(account.object_id);
                let valid = fee.is_some_and(|fee| fee.flags & FEE_FLAG_VALID != 0);
                if !valid {
                    return Err(ObjectGateError::new(
                        OBJECT_GATE_FEE_REQUIRED,
                        format!(
                            "artifact requires fee facts but account {} has none",
                            account.object_id
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Bitmap of fields actually present in one symbol payload.
pub fn symbol_field_presence(payload: &TitanSymbolPayloadV1) -> u64 {
    let mut bits = 0;
    if payload.price_tick.unit().is_some() {
        bits |= SYMBOL_FIELD_PRICE_TICK;
    }
    if payload.quantity_lot.unit().is_some() {
        bits |= SYMBOL_FIELD_QUANTITY_LOT;
    }
    if payload.contract_multiplier.unit().is_some() {
        bits |= SYMBOL_FIELD_CONTRACT_MULTIPLIER;
    }
    if payload.base_delta_per_lot.unit().is_some() {
        bits |= SYMBOL_FIELD_BASE_DELTA_PER_LOT;
    }
    if payload.min_quantity.unit().is_some() {
        bits |= SYMBOL_FIELD_MIN_QUANTITY;
    }
    if payload.min_qty_lots > 0 {
        bits |= SYMBOL_FIELD_MIN_QTY_LOTS;
    }
    if payload.min_notional_quote.unit().is_some() {
        bits |= SYMBOL_FIELD_MIN_NOTIONAL_QUOTE;
    }
    if payload.min_notional_tick_lots > 0 {
        bits |= SYMBOL_FIELD_MIN_NOTIONAL_TICK_LOTS;
    }
    if payload.quote_currency_no > 0 {
        bits |= SYMBOL_FIELD_QUOTE_CURRENCY_NO;
    }
    if payload.margin_currency_no > 0 {
        bits |= SYMBOL_FIELD_MARGIN_CURRENCY_NO;
    }
    // `valuation` and `tradable` are always defined fields; their *value* is the fact, not presence.
    bits |= SYMBOL_FIELD_VALUATION | SYMBOL_FIELD_TRADABLE;
    bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{MarginMode, PositionMode, Valuation};
    use crate::object_table::{
        AccountObjectInput, CurrencyObjectInput, FeeObjectInput, ObjectTableInput,
        SymbolObjectInput,
    };
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().unwrap()
    }

    fn table(fee: bool, min_fields: bool) -> ObjectTable {
        ObjectTable::build(ObjectTableInput {
            symbols: vec![SymbolObjectInput {
                local_asset_no: 0,
                asset_id: 1,
                native_symbol: Arc::from("TEST"),
                source_key: Arc::from("venue"),
                price_tick: unit("0.1"),
                quantity_lot: unit("0.01"),
                contract_multiplier: unit("1"),
                min_quantity: min_fields.then(|| unit("0.01")),
                min_notional_quote: min_fields.then(|| unit("5")),
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
            fees: if fee {
                vec![FeeObjectInput {
                    local_account_no: 0,
                    maker_fee_bps: Some(1),
                    taker_fee_bps: Some(2),
                    source_config: true,
                    observed_at_ns: 1,
                }]
            } else {
                vec![]
            },
            metadata_snapshot_digest: [0; 32],
        })
        .unwrap()
    }

    fn requirement() -> ObjectRequirement {
        ObjectRequirement {
            domain: OBJECT_DOMAIN_SYMBOL,
            major: 1,
            min_minor: 1,
            min_payload_len: std::mem::size_of::<TitanSymbolPayloadV1>() as u64,
            min_count: 1,
            required_fields: SYMBOL_FIELD_PRICE_TICK
                | SYMBOL_FIELD_QUANTITY_LOT
                | SYMBOL_FIELD_MIN_QUANTITY
                | SYMBOL_FIELD_MIN_NOTIONAL_QUOTE,
        }
    }

    fn manifest(requirements: Vec<ObjectRequirement>) -> ObjectManifest {
        ObjectManifest {
            schema_major: 1,
            schema_minor: 1,
            schema_hash: None,
            identity_version: 1,
            requires_fee: false,
            requirements,
        }
    }

    #[test]
    fn artifact_layer_refuses_newer_or_malformed_requirements() {
        manifest(vec![requirement()]).validate_artifact().unwrap();

        let mut newer = manifest(vec![requirement()]);
        newer.schema_minor = 2;
        assert_eq!(
            newer.validate_artifact().unwrap_err().code(),
            OBJECT_GATE_SCHEMA_MINOR
        );

        let mut major = manifest(vec![requirement()]);
        major.schema_major = 2;
        assert_eq!(
            major.validate_artifact().unwrap_err().code(),
            OBJECT_GATE_SCHEMA_MAJOR
        );

        let mut identity = manifest(vec![requirement()]);
        identity.identity_version = 2;
        assert_eq!(
            identity.validate_artifact().unwrap_err().code(),
            OBJECT_GATE_IDENTITY_VERSION
        );

        let mut unknown_domain = requirement();
        unknown_domain.domain = 99;
        assert_eq!(
            manifest(vec![unknown_domain])
                .validate_artifact()
                .unwrap_err()
                .code(),
            OBJECT_GATE_REQUIREMENT_INVALID
        );

        let mut short_payload = requirement();
        short_payload.min_payload_len = 200;
        assert_eq!(
            manifest(vec![short_payload])
                .validate_artifact()
                .unwrap_err()
                .code(),
            OBJECT_GATE_REQUIREMENT_INVALID
        );

        let mut bad_bits = requirement();
        bad_bits.domain = OBJECT_DOMAIN_ACCOUNT;
        bad_bits.required_fields = 1;
        assert_eq!(
            manifest(vec![bad_bits])
                .validate_artifact()
                .unwrap_err()
                .code(),
            OBJECT_GATE_REQUIREMENT_INVALID
        );

        let duplicate = manifest(vec![requirement(), requirement()]);
        assert_eq!(
            duplicate.validate_artifact().unwrap_err().code(),
            OBJECT_GATE_REQUIREMENT_INVALID
        );
    }

    #[test]
    fn instance_layer_proves_counts_fields_and_fees() {
        let complete = table(true, true);
        manifest(vec![requirement()])
            .validate_instance(&complete)
            .unwrap();
        manifest(vec![ObjectRequirement {
            domain: OBJECT_DOMAIN_ACCOUNT,
            major: 1,
            min_minor: 1,
            min_payload_len: std::mem::size_of::<TitanAccountPayloadV1>() as u64,
            min_count: 1,
            required_fields: 0,
        }])
        .validate_instance(&complete)
        .unwrap();

        let sparse = table(true, false);
        assert_eq!(
            manifest(vec![requirement()])
                .validate_instance(&sparse)
                .unwrap_err()
                .code(),
            OBJECT_GATE_FIELD_MISSING
        );

        let mut two_symbols = manifest(vec![ObjectRequirement {
            min_count: 2,
            ..requirement()
        }]);
        two_symbols.requires_fee = false;
        assert_eq!(
            two_symbols.validate_instance(&complete).unwrap_err().code(),
            OBJECT_GATE_MISSING
        );

        let mut fee_required = manifest(vec![requirement()]);
        fee_required.requires_fee = true;
        fee_required
            .validate_instance(&complete)
            .expect("valid fee facts satisfy the requirement");
        assert_eq!(
            fee_required
                .validate_instance(&table(false, true))
                .unwrap_err()
                .code(),
            OBJECT_GATE_FEE_REQUIRED
        );
    }
}
