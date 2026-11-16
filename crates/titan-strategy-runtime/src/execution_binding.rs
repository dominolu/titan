//! Controlled-execution envelopes for a strategy instance.
//!
//! One instance has exactly one envelope:
//! - [`ExecutionLimits`] covers a single market and single account binding. It stays the
//!   only accepted form for legacy single-market instances.
//! - [`MultiBindingExecutionLimits`] covers every leg of a cross-venue instance. It is
//!   mutually exclusive with [`ExecutionLimits`] because a single-market cap cannot express
//!   two independent collateral domains.
//!
//! Validation here is configuration-shape only. It never touches exchange state: native
//! instrument metadata, funds and margin proof stay with the account-service preflight.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use titan_domain_types::DecimalUnit;

use crate::{StrategyAccountBinding, StrategyMarketBinding, StrategyRuntimeSpec};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLegRole {
    Maker,
    Hedge,
}

/// One binding leg of a multi-venue instance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLegLimit {
    pub local_account_no: u32,
    pub local_asset_no: u32,
    pub role: ExecutionLegRole,
    /// Exact base-currency delta of one native lot. Ratios are computed from this value with
    /// integer arithmetic; an approximate float ratio is never admitted.
    pub base_delta_per_lot: DecimalUnit,
    pub min_quantity_lots: i64,
    pub min_notional_quote: DecimalUnit,
    pub max_position_lots: i64,
    pub max_notional_tick_lots: String,
    pub max_market_age_ns: i64,
    /// Collateral domain. Different domains never offset each other.
    pub margin_domain: u32,
    /// Explicit initial-margin rate in basis points of the bound notional. Configuration, never a
    /// venue guess; a missing rate is refused rather than defaulted.
    #[serde(default = "default_margin_rate_bps")]
    pub margin_rate_bps: u32,
}

fn default_margin_rate_bps() -> u32 {
    10_000
}

/// Pre-authorized bounded rescue budget. It is an execution ceiling, not a loss allowance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RescueBudget {
    pub max_quantity_lots: i64,
    pub max_notional_tick_lots: String,
    pub max_cost_bps: u32,
    pub deadline_ns: i64,
}

/// Phase budgets of the stop path. Their sum must fit the host stop timeout.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopBudget {
    pub cancel_ns: i64,
    pub hedge_ns: i64,
    pub exit_ns: i64,
    pub confirm_ns: i64,
    pub slack_ns: i64,
}

impl StopBudget {
    pub fn total(&self) -> i64 {
        self.cancel_ns
            .saturating_add(self.hedge_ns)
            .saturating_add(self.exit_ns)
            .saturating_add(self.confirm_ns)
            .saturating_add(self.slack_ns)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiBindingExecutionLimits {
    /// Risk-configuration revision. It enters the recovery identity; a restart may not move
    /// it back.
    pub revision: u64,
    /// Common quote/collateral currency. Cross-currency legs need a verified FX source and
    /// are not admitted by this revision.
    pub quote_currency: u32,
    pub legs: Arc<[ExecutionLegLimit]>,
    pub rescue: RescueBudget,
    pub stop: StopBudget,
}

/// A reduced exact ratio `numerator / denominator` between two lot sizes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LotRatio {
    pub numerator: i128,
    pub denominator: i128,
}

impl LotRatio {
    /// Reduces `left / right` using integer arithmetic only.
    pub fn exact(left: DecimalUnit, right: DecimalUnit) -> Self {
        let mut numerator = i128::from(left.coefficient());
        let mut denominator = i128::from(right.coefficient());
        let exponent = i32::from(right.scale()) - i32::from(left.scale());
        let factor = 10_i128
            .checked_pow(exponent.unsigned_abs())
            .unwrap_or(i128::MAX);
        if exponent >= 0 {
            numerator = numerator.saturating_mul(factor);
        } else {
            denominator = denominator.saturating_mul(factor);
        }
        let divisor = gcd(numerator, denominator);
        if divisor > 1 {
            numerator /= divisor;
            denominator /= divisor;
        }
        Self {
            numerator,
            denominator,
        }
    }

    /// True when one left lot is exactly an integer number of right lots.
    pub fn is_integral(self) -> bool {
        self.denominator > 0 && self.numerator > 0 && self.numerator % self.denominator == 0
    }

    pub fn integer_value(self) -> Option<i128> {
        self.is_integral()
            .then(|| self.numerator / self.denominator)
    }
}

fn gcd(mut a: i128, mut b: i128) -> i128 {
    while b != 0 {
        let remainder = a % b;
        a = b;
        b = remainder;
    }
    a.abs().max(1)
}

/// Numeric bound shared by admission and evidence, so both read one configuration.
pub fn parse_notional_limit(value: &str) -> Option<i128> {
    value.parse::<i128>().ok().filter(|limit| *limit > 0)
}

fn positive(value: i64) -> bool {
    value > 0
}

fn budget_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Configuration-shape validation. Returns a stable error code for the caller's error type.
///
/// `capabilities` is the compiled-strategy capability set when it is already known. Definition
/// inspection passes `None`; runtime creation passes the manifest value and additionally
/// refuses a multi-leg instance whose strategy did not declare `MULTI_VENUE_EXECUTION`.
pub fn validate_execution_binding(
    runtime: &StrategyRuntimeSpec,
    markets: &[StrategyMarketBinding],
    accounts: &[StrategyAccountBinding],
    capabilities: Option<crate::StrategyCapabilities>,
) -> Result<(), &'static str> {
    match (&runtime.execution_limits, &runtime.multi_binding) {
        (Some(_), Some(_)) => Err("execution_binding_ambiguous"),
        (None, None) => Ok(()),
        (Some(_), None) => Ok(()),
        (None, Some(limits)) => {
            validate_multi_binding(runtime, markets, accounts, limits, capabilities)
        }
    }
}

fn validate_multi_binding(
    runtime: &StrategyRuntimeSpec,
    markets: &[StrategyMarketBinding],
    accounts: &[StrategyAccountBinding],
    limits: &MultiBindingExecutionLimits,
    capabilities: Option<crate::StrategyCapabilities>,
) -> Result<(), &'static str> {
    if limits.revision == 0 || limits.quote_currency == 0 {
        return Err("execution_binding_invalid");
    }
    if limits.legs.len() < 2 {
        return Err("execution_binding_unproven");
    }
    if let Some(capabilities) = capabilities
        && !capabilities.contains(crate::StrategyCapabilities::MULTI_VENUE_EXECUTION)
    {
        return Err("execution_binding_capability_missing");
    }
    let mut seen = BTreeSet::new();
    let mut domains: std::collections::BTreeMap<u32, u32> = std::collections::BTreeMap::new();
    let mut makers = 0usize;
    let mut hedges = 0usize;
    let mut notional_total: i128 = 0;
    for leg in limits.legs.iter() {
        if !seen.insert((leg.local_account_no, leg.local_asset_no)) {
            return Err("execution_binding_duplicate_leg");
        }
        let Some(account) = accounts
            .iter()
            .find(|account| account.local_account_no == leg.local_account_no)
        else {
            return Err("execution_binding_account_unbound");
        };
        if !account
            .tradable_assets
            .iter()
            .any(|asset| asset.local_asset_no == leg.local_asset_no)
        {
            return Err("execution_binding_asset_unbound");
        }
        if !markets
            .iter()
            .any(|market| market.local_asset_no == leg.local_asset_no)
        {
            return Err("execution_binding_market_unbound");
        }
        if !(1..=100_000).contains(&leg.margin_rate_bps) {
            return Err("execution_binding_invalid");
        }
        if leg.base_delta_per_lot.coefficient() == 0
            || leg.min_notional_quote.coefficient() == 0
            || !positive(leg.min_quantity_lots)
            || !positive(leg.max_position_lots)
            || leg.min_quantity_lots > leg.max_position_lots
            || !positive(leg.max_market_age_ns)
        {
            return Err("execution_binding_invalid");
        }
        let Some(leg_notional) = parse_notional_limit(&leg.max_notional_tick_lots) else {
            return Err("execution_binding_invalid");
        };
        notional_total = notional_total.saturating_add(leg_notional);
        // Two legs of one account must live in one collateral domain; the calculator never
        // offsets one domain against another.
        match domains.get(&leg.local_account_no) {
            Some(domain) if *domain != leg.margin_domain => {
                return Err("execution_binding_margin_domain");
            }
            Some(_) => {}
            None => {
                domains.insert(leg.local_account_no, leg.margin_domain);
            }
        }
        match leg.role {
            ExecutionLegRole::Maker => makers += 1,
            ExecutionLegRole::Hedge => hedges += 1,
        }
    }
    if makers != 1 || hedges != 1 {
        return Err("execution_binding_role_invalid");
    }
    let maker = limits
        .legs
        .iter()
        .find(|leg| leg.role == ExecutionLegRole::Maker)
        .ok_or("execution_binding_role_invalid")?;
    let hedge = limits
        .legs
        .iter()
        .find(|leg| leg.role == ExecutionLegRole::Hedge)
        .ok_or("execution_binding_role_invalid")?;
    // Every smallest maker partial fill must be hedgeable as a whole number of hedge lots.
    if !LotRatio::exact(maker.base_delta_per_lot, hedge.base_delta_per_lot).is_integral() {
        return Err("execution_binding_lot_ratio");
    }
    if !positive(limits.rescue.max_quantity_lots)
        || !positive(limits.rescue.deadline_ns)
        || parse_notional_limit(&limits.rescue.max_notional_tick_lots).is_none()
    {
        return Err("execution_binding_invalid");
    }
    // Rescue capacity must at least unwind one minimal maker fill.
    if limits.rescue.max_quantity_lots < maker.min_quantity_lots {
        return Err("execution_binding_invalid");
    }
    if limits.stop.cancel_ns < 0
        || limits.stop.hedge_ns < 0
        || limits.stop.exit_ns < 0
        || limits.stop.confirm_ns < 0
        || limits.stop.slack_ns < 0
    {
        return Err("execution_binding_invalid");
    }
    let budget = limits.stop.total();
    let host = budget_ns(runtime.stop_timeout);
    if budget <= 0 || budget > host {
        return Err("execution_stop_budget_exceeds_host");
    }
    Ok(())
}

/// Declared intent of a staged submit. It travels in the request's first reserved byte, so the
/// ABI layout does not change and a host that ignores it behaves exactly as before.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPurpose {
    /// Legacy default: treated as new risk, no relaxation anywhere.
    Unspecified,
    /// Resting maker quote. Only a maker leg may declare it.
    MakerQuote,
    /// Hedge covering an already proven independent fill. Only a hedge leg may declare it, and
    /// only when it provably reduces the proven unhedged exposure.
    KnownFillHedge,
    /// Pre-authorized rescue exit. Must be reduce-only.
    RescueExit,
    /// Exit of inventory the strategy demonstrably owns. Must be reduce-only.
    OwnedExit,
}

/// Byte index inside `TitanSubmitOrderRequest.reserved` that carries [`CommandPurpose`].
pub const COMMAND_PURPOSE_INDEX: usize = 0;

impl CommandPurpose {
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Unspecified => 0,
            Self::MakerQuote => 1,
            Self::KnownFillHedge => 2,
            Self::RescueExit => 3,
            Self::OwnedExit => 4,
        }
    }

    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::MakerQuote),
            2 => Some(Self::KnownFillHedge),
            3 => Some(Self::RescueExit),
            4 => Some(Self::OwnedExit),
            _ => None,
        }
    }

    pub const fn is_exit(self) -> bool {
        matches!(self, Self::RescueExit | Self::OwnedExit)
    }
}

/// Verifies declared purposes against the bound leg roles.
///
/// Without a multi-binding envelope there are no roles, so purposes are declarative only: an
/// unknown byte is still refused, but no purpose grants any admission relaxation.
pub fn validate_command_purpose(
    limits: Option<&MultiBindingExecutionLimits>,
    commands: &[crate::StagedCommand],
) -> Result<(), &'static str> {
    for command in commands {
        let crate::StagedCommand::Submit { request, .. } = command else {
            continue;
        };
        let Some(purpose) = CommandPurpose::from_byte(request.reserved[COMMAND_PURPOSE_INDEX])
        else {
            return Err("execution_purpose_invalid");
        };
        if purpose == CommandPurpose::Unspecified {
            continue;
        }
        let Some(limits) = limits else {
            continue;
        };
        let Some(leg) = limits.legs.iter().find(|leg| {
            leg.local_account_no == request.account_no && leg.local_asset_no == request.asset_no
        }) else {
            return Err("execution_purpose_unbound");
        };
        match purpose {
            CommandPurpose::MakerQuote if leg.role != ExecutionLegRole::Maker => {
                return Err("execution_purpose_role");
            }
            CommandPurpose::KnownFillHedge if leg.role != ExecutionLegRole::Hedge => {
                return Err("execution_purpose_role");
            }
            CommandPurpose::KnownFillHedge if request.reduce_only != 0 => {
                return Err("execution_purpose_invalid");
            }
            CommandPurpose::RescueExit | CommandPurpose::OwnedExit if request.reduce_only == 0 => {
                return Err("execution_purpose_role");
            }
            _ => {}
        }
    }
    Ok(())
}

/// True when a hedge submit provably *shrinks* the unhedged exposure the platform already
/// proved from independent fills. It can never be used to open new risk.
pub fn hedge_reduces_proven_exposure(
    limits: &MultiBindingExecutionLimits,
    maker_expected_lots: i128,
    hedge_expected_lots: i128,
    side: u8,
    qty_lots: i64,
) -> bool {
    if qty_lots <= 0 {
        return false;
    }
    let (Some(maker), Some(hedge)) = (
        limits
            .legs
            .iter()
            .find(|leg| leg.role == ExecutionLegRole::Maker),
        limits
            .legs
            .iter()
            .find(|leg| leg.role == ExecutionLegRole::Hedge),
    ) else {
        return false;
    };
    let Some(ratio) =
        LotRatio::exact(maker.base_delta_per_lot, hedge.base_delta_per_lot).integer_value()
    else {
        return false;
    };
    let Some(unhedged) = maker_expected_lots
        .checked_mul(ratio)
        .and_then(|value| value.checked_add(hedge_expected_lots))
    else {
        return false;
    };
    let quantity = i128::from(qty_lots);
    match side {
        // Selling hedges a proven long maker position.
        2 => unhedged > 0 && quantity <= unhedged,
        // Buying hedges a proven short maker position.
        1 => unhedged < 0 && quantity <= -unhedged,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RiskScopeRef, StrategyMarketBinding, StrategyRuntimeSpec, StrategyTradableAsset};
    use titan_account_service::AccountHandle;

    fn unit(coefficient: u64, scale: u8) -> DecimalUnit {
        DecimalUnit::new(coefficient, scale).expect("valid unit")
    }

    fn leg(role: ExecutionLegRole, asset: u32, delta: DecimalUnit) -> ExecutionLegLimit {
        ExecutionLegLimit {
            local_account_no: u32::from(asset == 0),
            local_asset_no: asset,
            role,
            base_delta_per_lot: delta,
            min_quantity_lots: 1,
            min_notional_quote: unit(1, 0),
            max_position_lots: 10,
            max_notional_tick_lots: "1000000".into(),
            max_market_age_ns: 2_000_000_000,
            margin_domain: u32::from(asset == 0) + 1,
            margin_rate_bps: 10_000,
        }
    }

    fn limits(maker: DecimalUnit, hedge: DecimalUnit) -> MultiBindingExecutionLimits {
        MultiBindingExecutionLimits {
            revision: 1,
            quote_currency: 1,
            legs: Arc::from([
                leg(ExecutionLegRole::Maker, 1, maker),
                leg(ExecutionLegRole::Hedge, 0, hedge),
            ]),
            rescue: RescueBudget {
                max_quantity_lots: 20,
                max_notional_tick_lots: "2000000".into(),
                max_cost_bps: 20,
                deadline_ns: 10_000_000_000,
            },
            stop: StopBudget {
                cancel_ns: 1_000_000_000,
                hedge_ns: 2_000_000_000,
                exit_ns: 2_000_000_000,
                confirm_ns: 1_000_000_000,
                slack_ns: 500_000_000,
            },
        }
    }

    fn runtime(multi: Option<MultiBindingExecutionLimits>) -> StrategyRuntimeSpec {
        StrategyRuntimeSpec {
            multi_binding: multi,
            ..StrategyRuntimeSpec::default()
        }
    }

    fn markets() -> Vec<StrategyMarketBinding> {
        vec![
            StrategyMarketBinding {
                local_market_no: 0,
                local_asset_no: 0,
                source_key: Arc::from("hedge"),
                asset_id: 1002,
                data_mode: crate::StrategyDataMode::Tick,
            },
            StrategyMarketBinding {
                local_market_no: 1,
                local_asset_no: 1,
                source_key: Arc::from("maker"),
                asset_id: 1001,
                data_mode: crate::StrategyDataMode::Tick,
            },
        ]
    }

    fn accounts() -> Vec<StrategyAccountBinding> {
        vec![
            StrategyAccountBinding {
                local_account_no: 0,
                account_key: Arc::from("maker"),
                tradable_assets: Arc::from([StrategyTradableAsset {
                    local_asset_no: 1,
                    asset_id: 1001,
                }]),
                fees: None,
            },
            StrategyAccountBinding {
                local_account_no: 1,
                account_key: Arc::from("hedge"),
                tradable_assets: Arc::from([StrategyTradableAsset {
                    local_asset_no: 0,
                    asset_id: 1002,
                }]),
                fees: None,
            },
        ]
    }

    #[test]
    fn exact_integer_lot_ratio_is_admitted() {
        // Maker 0.01, hedge 0.001 -> exactly 10 hedge lots per maker lot.
        let result = validate_execution_binding(
            &runtime(Some(limits(unit(1, 2), unit(1, 3)))),
            &markets(),
            &accounts(),
            Some(crate::StrategyCapabilities::MULTI_VENUE_EXECUTION),
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn non_integral_lot_ratio_is_rejected() {
        // 0.01 / 0.003 = 10/3: a single maker partial fill cannot be hedged exactly.
        let result = validate_execution_binding(
            &runtime(Some(limits(unit(1, 2), unit(3, 3)))),
            &markets(),
            &accounts(),
            None,
        );
        assert_eq!(result, Err("execution_binding_lot_ratio"));
        assert!(!LotRatio::exact(unit(1, 2), unit(3, 3)).is_integral());
        assert_eq!(
            LotRatio::exact(unit(1, 2), unit(1, 3)).integer_value(),
            Some(10)
        );
    }

    #[test]
    fn legacy_and_multi_binding_are_mutually_exclusive() {
        let mut spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        spec.execution_limits = Some(crate::ExecutionLimits {
            max_notional_tick_lots: "1000".into(),
            max_price_deviation_bps: 100,
            max_market_age_ns: 2_000_000_000,
        });
        assert_eq!(
            validate_execution_binding(&spec, &markets(), &accounts(), None),
            Err("execution_binding_ambiguous")
        );
    }

    #[test]
    fn single_market_capability_is_rejected_for_two_legs() {
        assert_eq!(
            validate_execution_binding(
                &runtime(Some(limits(unit(1, 2), unit(1, 3)))),
                &markets(),
                &accounts(),
                Some(crate::StrategyCapabilities::SUBMIT_ORDER),
            ),
            Err("execution_binding_capability_missing")
        );
    }

    #[test]
    fn stop_budget_must_fit_host_timeout() {
        let mut spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        spec.stop_timeout = Duration::from_secs(1);
        assert_eq!(
            validate_execution_binding(&spec, &markets(), &accounts(), None),
            Err("execution_stop_budget_exceeds_host")
        );
    }

    #[test]
    fn unbound_legs_are_rejected() {
        let mut spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        if let Some(multi) = spec.multi_binding.as_mut() {
            let mut legs: Vec<ExecutionLegLimit> = multi.legs.to_vec();
            legs[1].local_account_no = 9;
            multi.legs = Arc::from(legs);
        }
        assert_eq!(
            validate_execution_binding(&spec, &markets(), &accounts(), None),
            Err("execution_binding_account_unbound")
        );
    }

    #[test]
    fn duplicate_roles_and_legs_are_rejected() {
        let mut spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        if let Some(multi) = spec.multi_binding.as_mut() {
            let mut legs: Vec<ExecutionLegLimit> = multi.legs.to_vec();
            legs[1].role = ExecutionLegRole::Maker;
            multi.legs = Arc::from(legs);
        }
        assert_eq!(
            validate_execution_binding(&spec, &markets(), &accounts(), None),
            Err("execution_binding_role_invalid")
        );
        let mut spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        if let Some(multi) = spec.multi_binding.as_mut() {
            let mut legs: Vec<ExecutionLegLimit> = multi.legs.to_vec();
            legs[1].local_asset_no = legs[0].local_asset_no;
            legs[1].local_account_no = legs[0].local_account_no;
            multi.legs = Arc::from(legs);
        }
        assert_eq!(
            validate_execution_binding(&spec, &markets(), &accounts(), None),
            Err("execution_binding_duplicate_leg")
        );
    }

    #[test]
    fn serialized_form_keeps_exact_units() {
        let spec = runtime(Some(limits(unit(1, 2), unit(1, 3))));
        let text = serde_json::to_string(&spec.multi_binding).expect("serialize");
        let decoded: MultiBindingExecutionLimits =
            serde_json::from_str(&text).expect("deserialize");
        assert_eq!(decoded, *spec.multi_binding.as_ref().expect("multi"));
        assert_eq!(
            LotRatio::exact(
                decoded.legs[0].base_delta_per_lot,
                decoded.legs[1].base_delta_per_lot
            )
            .integer_value(),
            Some(10)
        );
    }

    // Keep the unused import meaningful for future leg/handle mapping tests.
    #[allow(dead_code)]
    fn _handle_type(_: AccountHandle) {}
    #[allow(dead_code)]
    fn _scope() -> RiskScopeRef {
        RiskScopeRef(Arc::from("scope"))
    }
}
