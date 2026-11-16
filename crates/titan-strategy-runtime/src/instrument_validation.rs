//! Single authoritative validation of instrument units across market, account and execution
//! bindings.
//!
//! One instrument fact must have one source. Before this module existed the runtime compared only
//! `price_tick`/`quantity_lot` between market and account definitions, never `contract_multiplier`,
//! and the multi-leg envelope compared a hand-written `base_delta_per_lot` against nothing at all.
//! A wrong value therefore only produced a silently mis-hedged position.
//!
//! Every check here is a pure function of the in-memory definitions (plus, later, an offline
//! metadata snapshot). It never reads the network or the clock, never uses `f64`, and returns stable
//! reason codes so the CLI, the management path and `scripts/cross_exchange_mm_preflight.py` can all
//! report the same verdict instead of each writing its own arithmetic.

use std::{collections::BTreeMap, fmt, sync::Arc};

use titan_account_service::{AccountDefinition, AccountInstrumentBinding};
use titan_domain_types::DecimalUnit;
use titan_market_service::{MarketInstrumentBinding, MarketSourceDefinition, MetadataSnapshot};

use crate::{ExecutionLegRole, LotRatio, StrategyDefinition};

/// A unit (or a derived value) is unusable: zero coefficient, scale out of range, unrepresentable
/// product, or missing required data.
pub const INSTRUMENT_UNITS_INVALID: &str = "instrument_units_invalid";
/// Two enabled market sources declare the same `asset_id` with different units.
pub const INSTRUMENT_ASSET_CONFLICT: &str = "instrument_asset_conflict";
/// Market and account definitions disagree on tick, lot, multiplier or quote currency.
pub const INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH: &str =
    "instrument_units_market_account_mismatch";
/// Quote currencies differ between the two legs (or between config and resolved account currency).
pub const INSTRUMENT_QUOTE_CURRENCY_MISMATCH: &str = "instrument_quote_currency_mismatch";
/// The multi-leg `base_delta_per_lot` does not equal `quantity_lot * contract_multiplier`.
pub const INSTRUMENT_BASE_DELTA_MISMATCH: &str = "instrument_base_delta_mismatch";
/// The supplied metadata snapshot has no entry for a bound asset.
pub const INSTRUMENT_METADATA_MISSING: &str = "instrument_metadata_missing";
/// The supplied metadata snapshot contradicts the configured binding units.
pub const INSTRUMENT_METADATA_MISMATCH: &str = "instrument_metadata_mismatch";
/// The snapshot's schema version is unknown or its entries are malformed.
pub const INSTRUMENT_METADATA_INVALID: &str = "instrument_metadata_invalid";
/// The snapshot's own digest does not match its canonical bytes.
pub const INSTRUMENT_METADATA_DIGEST_MISMATCH: &str = "instrument_metadata_digest_mismatch";
/// A strategy binds an asset the market source or account does not define.
pub const INSTRUMENT_BINDING_MISSING: &str = "instrument_binding_missing";
/// The smallest maker fill is not a whole number of hedge lots.
pub const INSTRUMENT_LOT_RATIO_NOT_INTEGRAL: &str = "instrument_lot_ratio_not_integral";
/// The smallest maker fill is smaller than the hedge leg can trade.
pub const INSTRUMENT_MIN_FILL_NOT_HEDGEABLE: &str = "instrument_min_fill_not_hedgeable";

/// One rejected unit fact. `code` is stable; `subject` identifies the binding without exposing
/// credentials (source/account keys and native symbols are configuration, not secrets).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentUnitError {
    code: &'static str,
    subject: String,
    detail: String,
}

impl InstrumentUnitError {
    pub fn new(code: &'static str, subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code,
            subject: subject.into(),
            detail: detail.into(),
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for InstrumentUnitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {} ({})",
            self.code, self.subject, self.detail
        )
    }
}

impl std::error::Error for InstrumentUnitError {}

/// Derived unit facts of one bound instrument. These are the values a strategy needs in order to
/// quote or hedge without knowing exchange numbers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentUnits {
    pub source_key: Arc<str>,
    pub native_symbol: Arc<str>,
    pub asset_id: u32,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,
    pub contract_multiplier: DecimalUnit,
    /// `quantity_lot * contract_multiplier`: base-currency delta of one ABI quantity lot.
    pub base_delta_per_lot: DecimalUnit,
    pub min_quantity: Option<DecimalUnit>,
    pub min_notional_quote: Option<DecimalUnit>,
    pub quote_currency: Option<Arc<str>>,
}

impl InstrumentUnits {
    /// `ceil(min_quantity / quantity_lot)` in ABI lots, when the venue publishes a minimum.
    pub fn min_qty_lots(&self) -> Option<u64> {
        self.min_quantity?.ceil_div_by(self.quantity_lot)
    }
}

/// Result of one validation pass. Errors are collected rather than short-circuited so an operator
/// sees every inconsistent binding in one run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InstrumentValidationReport {
    pub instruments: Vec<InstrumentUnits>,
    pub errors: Vec<InstrumentUnitError>,
    /// Digest of the metadata snapshot that was verified, when one was supplied.
    pub metadata_digest: Option<[u8; 32]>,
}

impl InstrumentValidationReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn error_codes(&self) -> Vec<&'static str> {
        self.errors.iter().map(InstrumentUnitError::code).collect()
    }
}

fn valid_unit(unit: &DecimalUnit) -> bool {
    unit.coefficient() > 0 && unit.scale() <= DecimalUnit::MAX_SCALE
}

fn derive_units(
    source_key: &Arc<str>,
    binding: &MarketInstrumentBinding,
) -> Result<InstrumentUnits, InstrumentUnitError> {
    let subject = format!("{}:{}", source_key, binding.native_symbol);
    for (name, unit) in [
        ("price_tick", binding.price_tick),
        ("quantity_lot", binding.quantity_lot),
        ("contract_multiplier", binding.contract_multiplier),
    ] {
        if !valid_unit(&unit) {
            return Err(InstrumentUnitError::new(
                INSTRUMENT_UNITS_INVALID,
                subject,
                format!("{name} is not a usable positive decimal"),
            ));
        }
    }
    for (name, unit) in [
        ("min_quantity", binding.min_quantity),
        ("min_notional_quote", binding.min_notional_quote),
    ] {
        if let Some(unit) = unit
            && !valid_unit(&unit)
        {
            return Err(InstrumentUnitError::new(
                INSTRUMENT_UNITS_INVALID,
                subject,
                format!("{name} is present but not a usable positive decimal"),
            ));
        }
    }
    let base_delta_per_lot = binding
        .quantity_lot
        .checked_mul(binding.contract_multiplier)
        .ok_or_else(|| {
            InstrumentUnitError::new(
                INSTRUMENT_UNITS_INVALID,
                subject.clone(),
                "quantity_lot * contract_multiplier is not exactly representable".to_string(),
            )
        })?;
    Ok(InstrumentUnits {
        source_key: source_key.clone(),
        native_symbol: binding.native_symbol.clone(),
        asset_id: binding.asset_id.0,
        price_tick: binding.price_tick,
        quantity_lot: binding.quantity_lot,
        contract_multiplier: binding.contract_multiplier,
        base_delta_per_lot,
        min_quantity: binding.min_quantity,
        min_notional_quote: binding.min_notional_quote,
        quote_currency: binding.linear_quote_currency.clone(),
    })
}

fn same_units(left: &InstrumentUnits, right: &InstrumentUnits) -> bool {
    left.asset_id == right.asset_id
        && left.price_tick == right.price_tick
        && left.quantity_lot == right.quantity_lot
        && left.contract_multiplier == right.contract_multiplier
        && left.quote_currency == right.quote_currency
}

fn account_currency_name(account: &AccountDefinition, currency_id: u32) -> Option<Arc<str>> {
    account
        .currencies
        .iter()
        .find(|currency| currency.currency_id.0 == currency_id)
        .map(|currency| currency.native_currency.clone())
}

fn account_instrument(
    account: &AccountDefinition,
    asset_id: u32,
) -> Option<&AccountInstrumentBinding> {
    account
        .instruments
        .iter()
        .find(|instrument| instrument.asset_id.0 == asset_id)
}

/// Validates every unit fact that participates in price/quantity conversion or hedging proof.
///
/// Scope:
/// - enabled market sources: derive `base_delta_per_lot`; reject unusable decimals.
/// - the same `asset_id` across enabled sources: identical units, or a conflict.
/// - enabled accounts: tick/lot/multiplier/quote currency must match the market side.
/// - enabled strategies: every bound market/account instrument must exist, multi-leg
///   `base_delta_per_lot` must equal the authoritative derivation, the maker/hedge lot ratio must be
///   a positive integer and one minimal maker fill must be hedgeable.
///
/// The report never fails fast: callers decide whether a non-empty `errors` list refuses startup.
pub fn validate_instrument_units(
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
    strategies: &[StrategyDefinition],
) -> InstrumentValidationReport {
    validate_instrument_units_with_metadata(market_sources, accounts, strategies, None)
}

/// Same validation, additionally proving the configured bindings against an authoritative
/// offline metadata snapshot. A snapshot whose digest does not match its bytes is refused before
/// any comparison; a bound asset with no entry is a hard error.
pub fn validate_instrument_units_with_metadata(
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
    strategies: &[StrategyDefinition],
    metadata: Option<&MetadataSnapshot>,
) -> InstrumentValidationReport {
    let mut report = InstrumentValidationReport::default();
    let mut by_asset: BTreeMap<u32, InstrumentUnits> = BTreeMap::new();
    let mut by_source: BTreeMap<Arc<str>, Vec<InstrumentUnits>> = BTreeMap::new();

    for source in market_sources.iter().filter(|source| source.enabled) {
        let mut source_units = Vec::new();
        for binding in source.instruments.iter() {
            match derive_units(&source.source_key, binding) {
                Ok(units) => {
                    if let Some(existing) = by_asset.get(&units.asset_id) {
                        if !same_units(existing, &units) {
                            report.errors.push(InstrumentUnitError::new(
                                INSTRUMENT_ASSET_CONFLICT,
                                format!("asset {}", units.asset_id),
                                format!(
                                    "{}:{} (tick {} lot {} multiplier {}) differs from {}:{} (tick {} lot {} multiplier {})",
                                    units.source_key,
                                    units.native_symbol,
                                    units.price_tick,
                                    units.quantity_lot,
                                    units.contract_multiplier,
                                    existing.source_key,
                                    existing.native_symbol,
                                    existing.price_tick,
                                    existing.quantity_lot,
                                    existing.contract_multiplier,
                                ),
                            ));
                        }
                    } else {
                        by_asset.insert(units.asset_id, units.clone());
                    }
                    source_units.push(units.clone());
                    report.instruments.push(units);
                }
                Err(error) => report.errors.push(error),
            }
        }
        by_source.insert(source.source_key.clone(), source_units);
    }

    if let Some(metadata) = metadata {
        report.metadata_digest = Some(metadata.digest);
        if metadata.schema_version != titan_market_service::METADATA_SNAPSHOT_SCHEMA_VERSION {
            report.errors.push(InstrumentUnitError::new(
                INSTRUMENT_METADATA_INVALID,
                "metadata_snapshot",
                format!(
                    "unsupported metadata schema_version {}",
                    metadata.schema_version
                ),
            ));
        }
        if !metadata.digest_matches() {
            report.errors.push(InstrumentUnitError::new(
                INSTRUMENT_METADATA_DIGEST_MISMATCH,
                "metadata_snapshot",
                "snapshot digest does not match its canonical bytes",
            ));
        }
        for source in market_sources.iter().filter(|source| source.enabled) {
            for binding in source.instruments.iter() {
                let subject = format!("{}:{}", source.source_key, binding.native_symbol);
                let Some(instrument) =
                    metadata.instrument_by_symbol(&binding.native_symbol, Some(&source.source_key))
                else {
                    report.errors.push(InstrumentUnitError::new(
                        INSTRUMENT_METADATA_MISSING,
                        subject,
                        format!("no metadata entry for asset {}", binding.asset_id.0),
                    ));
                    continue;
                };
                let mut differences = Vec::new();
                for (name, configured, published) in [
                    (
                        "price_tick",
                        Some(binding.price_tick),
                        Some(instrument.price_tick),
                    ),
                    (
                        "quantity_lot",
                        Some(binding.quantity_lot),
                        Some(instrument.quantity_lot),
                    ),
                    (
                        "contract_multiplier",
                        Some(binding.contract_multiplier),
                        Some(instrument.contract_multiplier),
                    ),
                    (
                        "min_quantity",
                        binding.min_quantity,
                        instrument.min_quantity,
                    ),
                    (
                        "min_notional_quote",
                        binding.min_notional_quote,
                        instrument.min_notional_quote,
                    ),
                ] {
                    if configured != published {
                        differences.push(format!(
                            "{name} config {:?} metadata {:?}",
                            configured, published
                        ));
                    }
                }
                if binding.linear_quote_currency.as_deref()
                    != instrument.linear_quote_currency.as_deref()
                {
                    differences.push(format!(
                        "linear_quote_currency config {:?} metadata {:?}",
                        binding.linear_quote_currency, instrument.linear_quote_currency
                    ));
                }
                if !differences.is_empty() {
                    report.errors.push(InstrumentUnitError::new(
                        INSTRUMENT_METADATA_MISMATCH,
                        subject,
                        differences.join("; "),
                    ));
                }
            }
        }
    }

    for account in accounts.iter().filter(|account| account.enabled) {
        for instrument in account.instruments.iter() {
            let Some(market) = by_asset.get(&instrument.asset_id.0) else {
                continue;
            };
            let subject = format!("{}:{}", account.account_key, instrument.native_symbol);
            let mut mismatch = Vec::new();
            if instrument.price_tick != market.price_tick {
                mismatch.push(format!(
                    "price_tick market {} account {}",
                    market.price_tick, instrument.price_tick
                ));
            }
            if instrument.quantity_lot != market.quantity_lot {
                mismatch.push(format!(
                    "quantity_lot market {} account {}",
                    market.quantity_lot, instrument.quantity_lot
                ));
            }
            if instrument.contract_multiplier != market.contract_multiplier {
                mismatch.push(format!(
                    "contract_multiplier market {} account {}",
                    market.contract_multiplier, instrument.contract_multiplier
                ));
            }
            if let Some(currency_id) = instrument.linear_quote_currency {
                let account_currency = account_currency_name(account, currency_id.0);
                match (account_currency, market.quote_currency.as_ref()) {
                    (Some(account_currency), Some(market_currency))
                        if account_currency.as_ref() != market_currency.as_ref() =>
                    {
                        mismatch.push(format!(
                            "quote_currency market {market_currency} account {account_currency}"
                        ));
                    }
                    (None, _) => mismatch.push(format!(
                        "linear_quote_currency {} does not name one of the account's currencies",
                        currency_id.0
                    )),
                    _ => {}
                }
            }
            if !mismatch.is_empty() {
                report.errors.push(InstrumentUnitError::new(
                    INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH,
                    subject,
                    mismatch.join("; "),
                ));
            }
        }
    }

    for strategy in strategies.iter().filter(|strategy| strategy.enabled) {
        for binding in strategy.markets.iter() {
            // A disabled or absent source has no enabled instrument entry; both are reported as a
            // binding miss so an enabled strategy never runs against an unverified definition.
            if by_source
                .get(&binding.source_key)
                .is_some_and(|units| units.iter().any(|units| units.asset_id == binding.asset_id))
            {
                continue;
            }
            report.errors.push(InstrumentUnitError::new(
                INSTRUMENT_BINDING_MISSING,
                format!("{}:{}", strategy.strategy_key, binding.source_key),
                format!(
                    "market source {} has no enabled instrument for asset {}",
                    binding.source_key, binding.asset_id
                ),
            ));
        }

        let account_lookup: BTreeMap<Arc<str>, &AccountDefinition> = accounts
            .iter()
            .map(|account| (account.account_key.clone(), account))
            .collect();
        for binding in strategy.accounts.iter() {
            let account = account_lookup.get(&binding.account_key).copied();
            let Some(account) = account else {
                continue;
            };
            for tradable in binding.tradable_assets.iter() {
                if account_instrument(account, tradable.asset_id).is_none() {
                    report.errors.push(InstrumentUnitError::new(
                        INSTRUMENT_BINDING_MISSING,
                        format!("{}:{}", strategy.strategy_key, binding.account_key),
                        format!(
                            "account {} has no instrument for tradable asset {}",
                            binding.account_key, tradable.asset_id
                        ),
                    ));
                }
            }
        }

        let Some(limits) = strategy.runtime.multi_binding.as_ref() else {
            continue;
        };
        let mut makers = 0usize;
        let mut hedges = 0usize;
        for leg in limits.legs.iter() {
            match leg.role {
                ExecutionLegRole::Maker => makers += 1,
                ExecutionLegRole::Hedge => hedges += 1,
            }
            let subject = format!(
                "{}:leg({},{})",
                strategy.strategy_key, leg.local_account_no, leg.local_asset_no
            );
            let Some(market_binding) = strategy
                .markets
                .iter()
                .find(|binding| binding.local_asset_no == leg.local_asset_no)
            else {
                report.errors.push(InstrumentUnitError::new(
                    INSTRUMENT_BINDING_MISSING,
                    subject,
                    format!(
                        "leg references local_asset_no {} which the strategy does not bind",
                        leg.local_asset_no
                    ),
                ));
                continue;
            };
            let derived = by_source
                .get(&market_binding.source_key)
                .and_then(|units| {
                    units
                        .iter()
                        .find(|units| units.asset_id == market_binding.asset_id)
                })
                .map(|units| units.base_delta_per_lot)
                // The market source may be disabled or unbound; fall back to the account side so a
                // configuration-only check still catches a hand-written delta.
                .or_else(|| {
                    strategy
                        .accounts
                        .iter()
                        .find(|binding| binding.local_account_no == leg.local_account_no)
                        .and_then(|binding| account_lookup.get(&binding.account_key).copied())
                        .and_then(|account| account_instrument(account, market_binding.asset_id))
                        .and_then(|instrument| {
                            instrument
                                .quantity_lot
                                .checked_mul(instrument.contract_multiplier)
                        })
                });
            match derived {
                Some(derived) if derived == leg.base_delta_per_lot => {}
                Some(derived) => report.errors.push(InstrumentUnitError::new(
                    INSTRUMENT_BASE_DELTA_MISMATCH,
                    subject.clone(),
                    format!(
                        "configured base_delta_per_lot {} != quantity_lot * contract_multiplier {}",
                        leg.base_delta_per_lot, derived
                    ),
                )),
                None => report.errors.push(InstrumentUnitError::new(
                    INSTRUMENT_UNITS_INVALID,
                    subject.clone(),
                    "no authoritative units available to verify base_delta_per_lot".to_string(),
                )),
            }
            // The leg's quote currency comes from the account binding, but a configuration that
            // omits `linear_quote_currency` must still be checked against the market side's
            // declared quote currency. Otherwise the "common quote currency" rule silently passes.
            if let Some(account) = strategy
                .accounts
                .iter()
                .find(|binding| binding.local_account_no == leg.local_account_no)
                .and_then(|binding| account_lookup.get(&binding.account_key).copied())
            {
                let declared = account_instrument(account, market_binding.asset_id)
                    .and_then(|instrument| instrument.linear_quote_currency);
                if let Some(currency_id) = declared {
                    if currency_id.0 != limits.quote_currency {
                        report.errors.push(InstrumentUnitError::new(
                            INSTRUMENT_QUOTE_CURRENCY_MISMATCH,
                            subject.clone(),
                            format!(
                                "leg quote currency {} differs from multi-binding quote currency {}",
                                currency_id.0, limits.quote_currency
                            ),
                        ));
                    }
                } else if let Some(name) = by_source
                    .get(&market_binding.source_key)
                    .and_then(|units| {
                        units
                            .iter()
                            .find(|units| units.asset_id == market_binding.asset_id)
                    })
                    .and_then(|units| units.quote_currency.clone())
                {
                    match account
                        .currencies
                        .iter()
                        .find(|currency| currency.native_currency == name)
                    {
                        Some(currency) if currency.currency_id.0 == limits.quote_currency => {}
                        Some(currency) => report.errors.push(InstrumentUnitError::new(
                            INSTRUMENT_QUOTE_CURRENCY_MISMATCH,
                            subject.clone(),
                            format!(
                                "market quote currency {name} is account currency {} but the multi-binding quote currency is {}",
                                currency.currency_id.0, limits.quote_currency
                            ),
                        )),
                        None => report.errors.push(InstrumentUnitError::new(
                            INSTRUMENT_QUOTE_CURRENCY_MISMATCH,
                            subject.clone(),
                            format!(
                                "market quote currency {name} is not declared by account {}",
                                account.account_key
                            ),
                        )),
                    }
                }
            }
        }
        if makers == 1 && hedges == 1 {
            let maker = limits
                .legs
                .iter()
                .find(|leg| leg.role == ExecutionLegRole::Maker);
            let hedge = limits
                .legs
                .iter()
                .find(|leg| leg.role == ExecutionLegRole::Hedge);
            if let (Some(maker), Some(hedge)) = (maker, hedge) {
                let ratio = LotRatio::exact(maker.base_delta_per_lot, hedge.base_delta_per_lot);
                let subject = format!("{}:pair", strategy.strategy_key);
                if !ratio.is_integral() {
                    report.errors.push(InstrumentUnitError::new(
                        INSTRUMENT_LOT_RATIO_NOT_INTEGRAL,
                        subject.clone(),
                        format!(
                            "hedge/maker base delta ratio {}/{} is not a positive integer",
                            ratio.numerator, ratio.denominator
                        ),
                    ));
                } else if let Some(hedge_lots) = ratio.integer_value() {
                    let required = i128::from(maker.min_quantity_lots).checked_mul(hedge_lots);
                    match required {
                        Some(required) if required >= i128::from(hedge.min_quantity_lots) => {}
                        Some(required) => report.errors.push(InstrumentUnitError::new(
                            INSTRUMENT_MIN_FILL_NOT_HEDGEABLE,
                            subject,
                            format!(
                                "one minimal maker fill is {required} hedge lots but the hedge leg minimum is {}",
                                hedge.min_quantity_lots
                            ),
                        )),
                        None => report.errors.push(InstrumentUnitError::new(
                            INSTRUMENT_UNITS_INVALID,
                            subject,
                            "minimal maker fill * hedge ratio overflows".to_string(),
                        )),
                    }
                }
            }
        }
    }

    report
}

/// Convenience wrapper for callers that only need pass/fail with the first error.
pub fn require_instrument_units(
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
    strategies: &[StrategyDefinition],
) -> Result<InstrumentValidationReport, InstrumentUnitError> {
    let report = validate_instrument_units(market_sources, accounts, strategies);
    match report.errors.first() {
        Some(error) => Err(error.clone()),
        None => Ok(report),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExecutionLegLimit, MultiBindingExecutionLimits, RescueBudget, RiskScopeRef,
        RuntimeStrategyNo, StopBudget, StrategyAccountBinding, StrategyDataMode, StrategyFileRef,
        StrategyMarketBinding, StrategyRecoveryPolicy, StrategyRuntimeSpec, StrategyTradableAsset,
    };

    fn unit(value: &str) -> DecimalUnit {
        value.parse().expect("test decimal")
    }

    fn market_source(
        key: &str,
        asset_id: u32,
        lot: &str,
        multiplier: &str,
        quote: Option<&str>,
        enabled: bool,
    ) -> MarketSourceDefinition {
        MarketSourceDefinition {
            source_key: Arc::from(key),
            connector_type: Arc::from("test-connector"),
            connector_config: Arc::from([]),
            instruments: Arc::from([MarketInstrumentBinding {
                native_symbol: Arc::from(format!("{key}-symbol")),
                asset_id: titan_market_service::AssetId(asset_id),
                price_tick: unit("0.1"),
                quantity_lot: unit(lot),
                contract_multiplier: unit(multiplier),
                min_quantity: None,
                min_notional_quote: None,
                linear_quote_currency: quote.map(Arc::from),
            }]),
            enabled,
            definition_version: 1,
        }
    }

    fn account(
        key: &str,
        asset_id: u32,
        lot: &str,
        multiplier: &str,
        quote_currency: Option<u32>,
        enabled: bool,
    ) -> AccountDefinition {
        AccountDefinition {
            account_key: Arc::from(key),
            account_id: titan_account_service::AccountId(1),
            connector_type: Arc::from("test-account"),
            credential_ref: titan_account_service::SecretRef::new("secret://test"),
            connector_config: Arc::from([]),
            instruments: Arc::from([titan_account_service::AccountInstrumentBinding {
                native_symbol: Arc::from(format!("{key}-symbol")),
                asset_id: titan_account_service::AssetId(asset_id),
                price_tick: unit("0.1"),
                quantity_lot: unit(lot),
                contract_multiplier: unit(multiplier),
                linear_quote_currency: quote_currency.map(titan_account_service::CurrencyId),
            }]),
            currencies: Arc::from([
                titan_account_service::AccountCurrencyBinding {
                    native_currency: Arc::from("USDT"),
                    currency_id: titan_account_service::CurrencyId(1),
                    amount_unit: unit("0.000001"),
                },
                titan_account_service::AccountCurrencyBinding {
                    native_currency: Arc::from("USDC"),
                    currency_id: titan_account_service::CurrencyId(2),
                    amount_unit: unit("0.000001"),
                },
            ]),
            ownership: titan_account_service::OrderOwnershipPolicy::ObserveAll,
            shutdown_order_policy: titan_account_service::ShutdownOrderPolicy::LeaveOpen,
            enabled,
            definition_version: 1,
        }
    }

    fn leg(
        account_no: u32,
        asset_no: u32,
        role: ExecutionLegRole,
        base_delta: &str,
        min_quantity_lots: i64,
    ) -> ExecutionLegLimit {
        ExecutionLegLimit {
            local_account_no: account_no,
            local_asset_no: asset_no,
            role,
            base_delta_per_lot: unit(base_delta),
            min_quantity_lots,
            min_notional_quote: unit("5"),
            max_position_lots: 100,
            max_notional_tick_lots: "1000".to_string(),
            max_market_age_ns: 1_000_000_000,
            margin_domain: account_no,
            margin_rate_bps: 10_000,
        }
    }

    fn multi_binding(
        maker: ExecutionLegLimit,
        hedge: ExecutionLegLimit,
        quote_currency: u32,
    ) -> MultiBindingExecutionLimits {
        MultiBindingExecutionLimits {
            revision: 1,
            quote_currency,
            legs: Arc::from([maker, hedge]),
            rescue: RescueBudget {
                max_quantity_lots: 10,
                max_notional_tick_lots: "1000".to_string(),
                max_cost_bps: 100,
                deadline_ns: 1_000_000_000,
            },
            stop: StopBudget {
                cancel_ns: 1,
                hedge_ns: 1,
                exit_ns: 1,
                confirm_ns: 1,
                slack_ns: 1,
            },
        }
    }

    fn strategy(
        markets: Vec<StrategyMarketBinding>,
        accounts: Vec<StrategyAccountBinding>,
        multi_binding: Option<MultiBindingExecutionLimits>,
    ) -> StrategyDefinition {
        StrategyDefinition {
            strategy_key: Arc::from("test-strategy"),
            runtime_strategy_no: RuntimeStrategyNo(1),
            strategy_file: StrategyFileRef {
                loader_type: Arc::from("native"),
                uri: Arc::from("file://test.titan"),
                expected_digest: [0; 32],
                parameters_digest: None,
            },
            markets: markets.into(),
            accounts: accounts.into(),
            risk_scope: RiskScopeRef(Arc::from("test")),
            runtime: StrategyRuntimeSpec {
                multi_binding,
                ..StrategyRuntimeSpec::default()
            },
            recovery: StrategyRecoveryPolicy::Fresh,
            enabled: true,
            run_config_revision: 1,
        }
    }

    fn market_binding(
        local_market_no: u32,
        local_asset_no: u32,
        source: &str,
        asset_id: u32,
    ) -> StrategyMarketBinding {
        StrategyMarketBinding {
            local_market_no,
            local_asset_no,
            source_key: Arc::from(source),
            asset_id,
            data_mode: StrategyDataMode::Tick,
        }
    }

    fn account_binding(
        local_account_no: u32,
        key: &str,
        local_asset_no: u32,
        asset_id: u32,
    ) -> StrategyAccountBinding {
        StrategyAccountBinding {
            local_account_no,
            account_key: Arc::from(key),
            tradable_assets: Arc::from([StrategyTradableAsset {
                local_asset_no,
                asset_id,
            }]),
            fees: None,
        }
    }

    #[test]
    fn consistent_definitions_produce_a_clean_report_with_derived_facts() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            true,
        )];
        let accounts = [account("maker-account", 1001, "0.01", "1", Some(1), true)];
        let strategies = [strategy(
            vec![market_binding(0, 0, "maker", 1001)],
            vec![account_binding(0, "maker-account", 0, 1001)],
            None,
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert!(report.is_ok(), "{:?}", report.errors);
        assert_eq!(report.instruments.len(), 1);
        assert_eq!(report.instruments[0].base_delta_per_lot, unit("0.01"));
    }

    #[test]
    fn market_and_account_multiplier_mismatch_is_reported() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            true,
        )];
        let accounts = [account("maker-account", 1001, "0.01", "2", Some(1), true)];
        let strategies = [strategy(
            vec![market_binding(0, 0, "maker", 1001)],
            vec![account_binding(0, "maker-account", 0, 1001)],
            None,
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert_eq!(
            report.error_codes(),
            vec![INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH]
        );
        assert!(report.errors[0].detail().contains("contract_multiplier"));
    }

    #[test]
    fn conflicting_sources_for_one_asset_are_reported() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1001, "0.001", "1", Some("USDT"), true),
        ];
        let report = validate_instrument_units(&sources, &[], &[]);
        assert_eq!(report.error_codes(), vec![INSTRUMENT_ASSET_CONFLICT]);
    }

    #[test]
    fn quote_currency_mismatch_between_market_and_account_is_reported() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            true,
        )];
        let accounts = [account("maker-account", 1001, "0.01", "1", Some(2), true)];
        let report = validate_instrument_units(&sources, &accounts, &[]);
        assert_eq!(
            report.error_codes(),
            vec![INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH]
        );
        assert!(report.errors[0].detail().contains("quote_currency"));
    }

    fn metadata_snapshot(asset_id: u32, multiplier: &str) -> MetadataSnapshot {
        let mut snapshot = MetadataSnapshot {
            schema_version: titan_market_service::METADATA_SNAPSHOT_SCHEMA_VERSION,
            captured_at_ns: 1_700_000_000_000,
            instruments: vec![titan_market_service::MetadataInstrument {
                asset_no: asset_id,
                native_symbol: Arc::from("maker-symbol"),
                source_key: Some(Arc::from("maker")),
                price_tick: unit("0.1"),
                quantity_lot: unit("0.01"),
                contract_multiplier: unit(multiplier),
                min_quantity: None,
                min_notional_quote: None,
                linear_quote_currency: Some(Arc::from("USDT")),
                synthetic: false,
            }],
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        snapshot
    }

    #[test]
    fn metadata_snapshot_must_cover_and_agree_with_enabled_bindings() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            true,
        )];
        let matching = metadata_snapshot(1001, "1");
        let report = validate_instrument_units_with_metadata(&sources, &[], &[], Some(&matching));
        assert!(report.is_ok(), "{:?}", report.errors);
        assert_eq!(report.metadata_digest, Some(matching.digest));

        let contradictory = metadata_snapshot(1001, "2");
        let report =
            validate_instrument_units_with_metadata(&sources, &[], &[], Some(&contradictory));
        assert_eq!(report.error_codes(), vec![INSTRUMENT_METADATA_MISMATCH]);

        let missing = {
            let mut snapshot = matching.clone();
            snapshot.instruments.clear();
            snapshot.digest = snapshot.compute_digest();
            snapshot
        };
        let report = validate_instrument_units_with_metadata(&sources, &[], &[], Some(&missing));
        assert_eq!(report.error_codes(), vec![INSTRUMENT_METADATA_MISSING]);

        let mut tampered = matching.clone();
        tampered.captured_at_ns += 1;
        let report = validate_instrument_units_with_metadata(&sources, &[], &[], Some(&tampered));
        assert!(
            report
                .error_codes()
                .contains(&INSTRUMENT_METADATA_DIGEST_MISMATCH)
        );
    }

    #[test]
    fn disabled_definitions_are_not_rejected() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            false,
        )];
        let accounts = [account("maker-account", 1001, "0.01", "2", Some(1), false)];
        let report = validate_instrument_units(&sources, &accounts, &[]);
        assert!(report.is_ok(), "{:?}", report.errors);
    }

    #[test]
    fn strategy_binding_without_enabled_instrument_is_reported() {
        let sources = [market_source(
            "maker",
            1001,
            "0.01",
            "1",
            Some("USDT"),
            false,
        )];
        let strategies = [strategy(
            vec![market_binding(0, 0, "maker", 1001)],
            vec![],
            None,
        )];
        let report = validate_instrument_units(&sources, &[], &strategies);
        assert_eq!(report.error_codes(), vec![INSTRUMENT_BINDING_MISSING]);
    }

    #[test]
    fn configured_base_delta_must_equal_authoritative_units() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1002, "0.001", "1", Some("USDT"), true),
        ];
        let accounts = [
            account("maker-account", 1001, "0.01", "1", Some(1), true),
            account("hedge-account", 1002, "0.001", "1", Some(1), true),
        ];
        let strategies = [strategy(
            vec![
                market_binding(0, 0, "hedge", 1002),
                market_binding(1, 1, "maker", 1001),
            ],
            vec![
                account_binding(0, "maker-account", 1, 1001),
                account_binding(1, "hedge-account", 0, 1002),
            ],
            Some(multi_binding(
                // The maker leg claims 0.02 base per lot while the units say 0.01.
                leg(0, 1, ExecutionLegRole::Maker, "0.02", 1),
                leg(1, 0, ExecutionLegRole::Hedge, "0.001", 1),
                1,
            )),
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert_eq!(report.error_codes(), vec![INSTRUMENT_BASE_DELTA_MISMATCH]);
        assert!(report.errors[0].detail().contains("0.02"));
    }

    #[test]
    fn non_integral_lot_ratio_is_reported() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1002, "0.003", "1", Some("USDT"), true),
        ];
        let accounts = [
            account("maker-account", 1001, "0.01", "1", Some(1), true),
            account("hedge-account", 1002, "0.003", "1", Some(1), true),
        ];
        let strategies = [strategy(
            vec![
                market_binding(0, 0, "hedge", 1002),
                market_binding(1, 1, "maker", 1001),
            ],
            vec![
                account_binding(0, "maker-account", 1, 1001),
                account_binding(1, "hedge-account", 0, 1002),
            ],
            Some(multi_binding(
                leg(0, 1, ExecutionLegRole::Maker, "0.01", 1),
                leg(1, 0, ExecutionLegRole::Hedge, "0.003", 1),
                1,
            )),
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert_eq!(
            report.error_codes(),
            vec![INSTRUMENT_LOT_RATIO_NOT_INTEGRAL]
        );
    }

    #[test]
    fn market_quote_currency_is_checked_when_the_account_omits_it() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1002, "0.001", "1", Some("USDT"), true),
        ];
        // Accounts omit linear_quote_currency; their currency table maps USDT to id 2 while the
        // envelope claims id 1.
        let accounts = [
            account("maker-account", 1001, "0.01", "1", None, true),
            account("hedge-account", 1002, "0.001", "1", None, true),
        ];
        let strategies = [strategy(
            vec![
                market_binding(0, 0, "hedge", 1002),
                market_binding(1, 1, "maker", 1001),
            ],
            vec![
                account_binding(0, "maker-account", 1, 1001),
                account_binding(1, "hedge-account", 0, 1002),
            ],
            Some(multi_binding(
                leg(0, 1, ExecutionLegRole::Maker, "0.01", 1),
                leg(1, 0, ExecutionLegRole::Hedge, "0.001", 1),
                2,
            )),
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert!(
            report
                .error_codes()
                .contains(&INSTRUMENT_QUOTE_CURRENCY_MISMATCH)
        );
    }

    #[test]
    fn minimal_maker_fill_must_be_hedgeable() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1002, "0.001", "1", Some("USDT"), true),
        ];
        let accounts = [
            account("maker-account", 1001, "0.01", "1", Some(1), true),
            account("hedge-account", 1002, "0.001", "1", Some(1), true),
        ];
        let strategies = [strategy(
            vec![
                market_binding(0, 0, "hedge", 1002),
                market_binding(1, 1, "maker", 1001),
            ],
            vec![
                account_binding(0, "maker-account", 1, 1001),
                account_binding(1, "hedge-account", 0, 1002),
            ],
            Some(multi_binding(
                // One maker lot needs 10 hedge lots; a 1-lot maker minimum needs 10 hedge lots,
                // which is below a 20-lot hedge minimum.
                leg(0, 1, ExecutionLegRole::Maker, "0.01", 1),
                leg(1, 0, ExecutionLegRole::Hedge, "0.001", 20),
                1,
            )),
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert_eq!(
            report.error_codes(),
            vec![INSTRUMENT_MIN_FILL_NOT_HEDGEABLE]
        );
    }

    #[test]
    fn multi_binding_quote_currency_must_match_the_leg() {
        let sources = [
            market_source("maker", 1001, "0.01", "1", Some("USDT"), true),
            market_source("hedge", 1002, "0.001", "1", Some("USDT"), true),
        ];
        let accounts = [
            account("maker-account", 1001, "0.01", "1", Some(2), true),
            account("hedge-account", 1002, "0.001", "1", Some(2), true),
        ];
        let strategies = [strategy(
            vec![
                market_binding(0, 0, "hedge", 1002),
                market_binding(1, 1, "maker", 1001),
            ],
            vec![
                account_binding(0, "maker-account", 1, 1001),
                account_binding(1, "hedge-account", 0, 1002),
            ],
            Some(multi_binding(
                leg(0, 1, ExecutionLegRole::Maker, "0.01", 1),
                leg(1, 0, ExecutionLegRole::Hedge, "0.001", 1),
                1,
            )),
        )];
        let report = validate_instrument_units(&sources, &accounts, &strategies);
        assert!(
            report
                .error_codes()
                .contains(&INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH)
        );
        assert!(
            report
                .error_codes()
                .contains(&INSTRUMENT_QUOTE_CURRENCY_MISMATCH)
        );
    }
}
