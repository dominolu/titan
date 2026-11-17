//! Map bound instance definitions onto the immutable V14 object-table input.
//!
//! This is the online/offline shared adapter between "what the instance is bound to" and
//! [`crate::ObjectTable`]. It never reads the network: authoritative units come either from the
//! offline metadata snapshot or, while a venue has no snapshot, from the already validated market
//! binding. Every missing fact is an error, never a default.

use std::{collections::BTreeMap, sync::Arc};

use titan_account_service::{AccountDefinition, CurrencyId};
use titan_domain_types::DecimalUnit;
use titan_market_service::{MarketSourceDefinition, MetadataSnapshot};

use crate::context::{MarginMode, PositionMode, Valuation};
use crate::object_table::{
    AccountObjectInput, CurrencyObjectInput, FeeObjectInput, OBJECT_PAIR_UNKNOWN, ObjectTableError,
    ObjectTableInput, PairObjectInput, SymbolObjectInput,
};
use crate::{
    ExecutionLegRole, MultiBindingExecutionLimits, StrategyAccountBinding, StrategyMarketBinding,
};

pub const OBJECT_SOURCE_BINDING_MISSING: &str = "object_source_binding_missing";
pub const OBJECT_SOURCE_METADATA_MISSING: &str = "object_source_metadata_missing";

/// Everything the object table needs from one instance's frozen definitions.
pub struct ObjectTableSources<'a> {
    pub market_sources: &'a [MarketSourceDefinition],
    pub accounts: &'a [AccountDefinition],
    pub markets: &'a [StrategyMarketBinding],
    pub account_bindings: &'a [StrategyAccountBinding],
    pub multi_binding: Option<&'a MultiBindingExecutionLimits>,
    pub metadata: Option<&'a MetadataSnapshot>,
    /// Account facts owned by the runtime layer (position/margin mode); absent means provisional.
    pub account_modes: &'a [(u32, PositionMode, MarginMode)],
    pub fees: &'a [FeeObjectInput],
}

fn account_currency_name(account: &AccountDefinition, id: CurrencyId) -> Option<Arc<str>> {
    account
        .currencies
        .iter()
        .find(|currency| currency.currency_id == id)
        .map(|currency| currency.native_currency.clone())
}

fn account_declares(account: &AccountDefinition, name: &str) -> bool {
    account
        .currencies
        .iter()
        .any(|currency| currency.native_currency.as_ref() == name)
}

pub fn object_table_input(
    sources: &ObjectTableSources<'_>,
) -> Result<ObjectTableInput, ObjectTableError> {
    let ObjectTableSources {
        market_sources,
        accounts,
        markets,
        account_bindings,
        multi_binding,
        metadata,
        account_modes,
        fees,
    } = sources;

    // --- accounts first: symbols name their currencies by account-local number ---
    let mut account_inputs: Vec<AccountObjectInput> = Vec::new();
    let mut account_quotes: BTreeMap<u32, Arc<str>> = BTreeMap::new();
    let mut account_margins: BTreeMap<u32, Arc<str>> = BTreeMap::new();
    for binding in account_bindings.iter() {
        let Some(definition) = accounts
            .iter()
            .find(|account| account.account_key == binding.account_key)
        else {
            return Err(ObjectTableError::new(
                OBJECT_SOURCE_BINDING_MISSING,
                format!("account {} is not declared", binding.account_key),
            ));
        };
        // The quote/margin currency is a definition fact: an explicit linear_quote_currency on any
        // bound instrument, otherwise the quote currency of the bound market symbol.
        let quote = binding
            .tradable_assets
            .iter()
            .find_map(|asset| {
                definition
                    .instruments
                    .iter()
                    .find(|instrument| instrument.asset_id.0 == asset.asset_id)
                    .and_then(|instrument| instrument.linear_quote_currency)
                    .and_then(|id| account_currency_name(definition, id))
            })
            .or_else(|| {
                binding.tradable_assets.iter().find_map(|asset| {
                    markets
                        .iter()
                        .find(|market| market.local_asset_no == asset.local_asset_no)
                        .and_then(|market| {
                            market_sources
                                .iter()
                                .find(|source| source.source_key == market.source_key)
                                .and_then(|source| {
                                    source
                                        .instruments
                                        .iter()
                                        .find(|instrument| instrument.asset_id.0 == market.asset_id)
                                })
                        })
                        .and_then(|instrument| instrument.linear_quote_currency.clone())
                })
            })
            .ok_or_else(|| {
                ObjectTableError::new(
                    crate::object_table::OBJECT_CURRENCY_UNKNOWN,
                    format!(
                        "account {} has no provable quote currency",
                        binding.account_key
                    ),
                )
            })?;
        if !account_declares(definition, &quote) {
            return Err(ObjectTableError::new(
                crate::object_table::OBJECT_CURRENCY_UNKNOWN,
                format!(
                    "account {} does not declare quote currency {quote}",
                    binding.account_key
                ),
            ));
        }
        // Margin currency: an explicit margin asset is not modelled by V14 yet; the quote currency
        // is the documented linear-collateral default and must be declared.
        let margin = quote.clone();
        let (position_mode, margin_mode) = account_modes
            .iter()
            .find(|(account_no, _, _)| *account_no == binding.local_account_no)
            .map(|(_, position, margin)| (*position, *margin))
            .unwrap_or((PositionMode::Net, MarginMode::Unknown));
        let currencies = definition
            .currencies
            .iter()
            .map(|currency| CurrencyObjectInput {
                currency_no: currency.currency_id.0,
                native_currency: currency.native_currency.clone(),
                amount_unit: currency.amount_unit,
                quote: currency.native_currency == quote,
                margin: currency.native_currency == margin,
                settlement: currency.native_currency == margin,
            })
            .collect();
        account_quotes.insert(binding.local_account_no, quote.clone());
        account_margins.insert(binding.local_account_no, margin.clone());
        account_inputs.push(AccountObjectInput {
            local_account_no: binding.local_account_no,
            account_id: definition.account_id.0,
            quote_currency: quote,
            margin_currency: margin,
            position_mode,
            margin_mode,
            margin_domain: definition.account_id.0,
            ready: true,
            synthetic: false,
            currencies,
        });
    }

    // --- symbols: authoritative units from the snapshot when provided ---
    let mut symbols: Vec<SymbolObjectInput> = Vec::new();
    for market in markets.iter() {
        let Some(source) = market_sources
            .iter()
            .find(|source| source.source_key == market.source_key)
        else {
            return Err(ObjectTableError::new(
                OBJECT_SOURCE_BINDING_MISSING,
                format!("market source {} is not declared", market.source_key),
            ));
        };
        let Some(binding) = source
            .instruments
            .iter()
            .find(|instrument| instrument.asset_id.0 == market.asset_id)
        else {
            return Err(ObjectTableError::new(
                OBJECT_SOURCE_BINDING_MISSING,
                format!(
                    "market source {} has no instrument for asset {}",
                    source.source_key, market.asset_id
                ),
            ));
        };
        let published = match metadata {
            Some(snapshot) => Some(
                snapshot
                    .instrument_by_symbol(&binding.native_symbol, Some(&source.source_key))
                    .ok_or_else(|| {
                        ObjectTableError::new(
                            OBJECT_SOURCE_METADATA_MISSING,
                            format!(
                                "metadata snapshot has no entry for {}:{}",
                                source.source_key, binding.native_symbol
                            ),
                        )
                    })?,
            ),
            None => None,
        };
        let quote = published
            .and_then(|instrument| instrument.linear_quote_currency.clone())
            .or_else(|| binding.linear_quote_currency.clone())
            .ok_or_else(|| {
                ObjectTableError::new(
                    crate::object_table::OBJECT_CURRENCY_UNKNOWN,
                    format!(
                        "symbol {} has no provable quote currency",
                        binding.native_symbol
                    ),
                )
            })?;
        // The collateral currency is the one of the account that trades this asset; fall back to the
        // quote currency when the symbol is not tradable on a bound account.
        let margin = account_bindings
            .iter()
            .find(|account_binding| {
                account_binding
                    .tradable_assets
                    .iter()
                    .any(|asset| asset.local_asset_no == market.local_asset_no)
            })
            .and_then(|account_binding| account_margins.get(&account_binding.local_account_no))
            .cloned()
            .unwrap_or_else(|| quote.clone());
        symbols.push(SymbolObjectInput {
            local_asset_no: market.local_asset_no,
            asset_id: market.asset_id,
            native_symbol: binding.native_symbol.clone(),
            source_key: source.source_key.clone(),
            price_tick: published.map_or(binding.price_tick, |instrument| instrument.price_tick),
            quantity_lot: published
                .map_or(binding.quantity_lot, |instrument| instrument.quantity_lot),
            contract_multiplier: published.map_or(binding.contract_multiplier, |instrument| {
                instrument.contract_multiplier
            }),
            min_quantity: published
                .map_or(binding.min_quantity, |instrument| instrument.min_quantity),
            min_notional_quote: published.map_or(binding.min_notional_quote, |instrument| {
                instrument.min_notional_quote
            }),
            quote_currency: quote,
            margin_currency: margin,
            valuation: Valuation::Linear,
            tradable: true,
            synthetic: published.is_none(),
            metadata_version: 1,
        });
    }

    // --- pairs from the multi-leg envelope: roles select the legs, units derive the ratios ---
    let mut pairs: Vec<PairObjectInput> = Vec::new();
    if let Some(limits) = multi_binding {
        let maker = limits
            .legs
            .iter()
            .find(|leg| leg.role == ExecutionLegRole::Maker)
            .ok_or_else(|| {
                ObjectTableError::new(OBJECT_PAIR_UNKNOWN, "multi-binding has no maker leg")
            })?;
        let hedge = limits
            .legs
            .iter()
            .find(|leg| leg.role == ExecutionLegRole::Hedge)
            .ok_or_else(|| {
                ObjectTableError::new(OBJECT_PAIR_UNKNOWN, "multi-binding has no hedge leg")
            })?;
        for leg in [maker, hedge] {
            if !markets
                .iter()
                .any(|market| market.local_asset_no == leg.local_asset_no)
                || !account_bindings
                    .iter()
                    .any(|binding| binding.local_account_no == leg.local_account_no)
            {
                return Err(ObjectTableError::new(
                    OBJECT_PAIR_UNKNOWN,
                    "multi-binding leg references an unbound asset or account",
                ));
            }
        }
        pairs.push(PairObjectInput {
            maker_local_asset_no: maker.local_asset_no,
            hedge_local_asset_no: hedge.local_asset_no,
            maker_local_account_no: maker.local_account_no,
            hedge_local_account_no: hedge.local_account_no,
            quote_currency_no: limits.quote_currency,
            pair_revision: u32::try_from(limits.revision).map_err(|_| {
                ObjectTableError::new(OBJECT_PAIR_UNKNOWN, "pair revision does not fit u32")
            })?,
        });
    }

    Ok(ObjectTableInput {
        symbols,
        accounts: account_inputs,
        pairs,
        fees: fees.to_vec(),
        metadata_snapshot_digest: metadata.map_or([0; 32], |snapshot| snapshot.digest),
    })
}

/// Helper for the runtime layer: one symbol's derived base delta, or `None` when units are not
/// exactly representable. Exposed so callers can gate before building the table.
pub fn base_delta(
    quantity_lot: DecimalUnit,
    contract_multiplier: DecimalUnit,
) -> Option<DecimalUnit> {
    quantity_lot.checked_mul(contract_multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExecutionLegLimit, ExecutionLegRole, RescueBudget, StopBudget, StrategyDataMode,
        StrategyTradableAsset,
    };
    use titan_market_service::{AssetId, MarketInstrumentBinding, MetadataInstrument};

    fn unit(value: &str) -> DecimalUnit {
        value.parse().expect("test decimal")
    }

    fn source(
        key: &str,
        asset_id: u32,
        symbol: &str,
        lot: &str,
        quote: Option<&str>,
    ) -> MarketSourceDefinition {
        MarketSourceDefinition {
            source_key: Arc::from(key),
            connector_type: Arc::from("test-connector"),
            connector_config: Arc::from([]),
            instruments: Arc::from([MarketInstrumentBinding {
                native_symbol: Arc::from(symbol),
                asset_id: AssetId(asset_id),
                price_tick: unit("0.1"),
                quantity_lot: unit(lot),
                contract_multiplier: unit("1"),
                min_quantity: None,
                min_notional_quote: None,
                linear_quote_currency: quote.map(Arc::from),
            }]),
            enabled: true,
            definition_version: 1,
        }
    }

    fn account(key: &str, asset_id: u32, quote_currency: Option<u32>) -> AccountDefinition {
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
                quantity_lot: unit("0.01"),
                contract_multiplier: unit("1"),
                linear_quote_currency: quote_currency.map(CurrencyId),
            }]),
            currencies: Arc::from([
                titan_account_service::AccountCurrencyBinding {
                    native_currency: Arc::from("USDT"),
                    currency_id: CurrencyId(1),
                    amount_unit: unit("0.000001"),
                },
                titan_account_service::AccountCurrencyBinding {
                    native_currency: Arc::from("USDC"),
                    currency_id: CurrencyId(2),
                    amount_unit: unit("0.000001"),
                },
            ]),
            ownership: titan_account_service::OrderOwnershipPolicy::ObserveAll,
            shutdown_order_policy: titan_account_service::ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        }
    }

    fn market(
        local_market_no: u32,
        local_asset_no: u32,
        source_key: &str,
        asset_id: u32,
    ) -> StrategyMarketBinding {
        StrategyMarketBinding {
            local_market_no,
            local_asset_no,
            source_key: Arc::from(source_key),
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

    fn leg(
        local_account_no: u32,
        local_asset_no: u32,
        role: ExecutionLegRole,
        lot: &str,
    ) -> ExecutionLegLimit {
        ExecutionLegLimit {
            local_account_no,
            local_asset_no,
            role,
            base_delta_per_lot: unit(lot),
            min_quantity_lots: 1,
            min_notional_quote: unit("5"),
            max_position_lots: 100,
            max_notional_tick_lots: "1000".to_string(),
            max_market_age_ns: 1_000_000_000,
            margin_domain: local_account_no,
            margin_rate_bps: 10_000,
        }
    }

    fn multi(maker: ExecutionLegLimit, hedge: ExecutionLegLimit) -> MultiBindingExecutionLimits {
        MultiBindingExecutionLimits {
            revision: 3,
            quote_currency: 1,
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

    struct Fixture {
        sources: Vec<MarketSourceDefinition>,
        accounts: Vec<AccountDefinition>,
        markets: Vec<StrategyMarketBinding>,
        account_bindings: Vec<StrategyAccountBinding>,
        multi: Option<MultiBindingExecutionLimits>,
        metadata: Option<MetadataSnapshot>,
    }

    fn base_fixture() -> Fixture {
        Fixture {
            sources: vec![
                source("maker", 1001, "BTC-USDT", "0.01", Some("USDT")),
                source("hedge", 2001, "BTC-USDT-SWAP", "1", Some("USDT")),
            ],
            accounts: vec![
                account("maker-acc", 1001, Some(1)),
                account("hedge-acc", 2001, Some(1)),
            ],
            markets: vec![market(0, 0, "maker", 1001), market(1, 1, "hedge", 2001)],
            account_bindings: vec![
                account_binding(0, "maker-acc", 0, 1001),
                account_binding(1, "hedge-acc", 1, 2001),
            ],
            multi: Some(multi(
                leg(0, 0, ExecutionLegRole::Maker, "0.01"),
                leg(1, 1, ExecutionLegRole::Hedge, "1"),
            )),
            metadata: None,
        }
    }

    fn build(fixture: &Fixture) -> Result<ObjectTableInput, ObjectTableError> {
        object_table_input(&ObjectTableSources {
            market_sources: &fixture.sources,
            accounts: &fixture.accounts,
            markets: &fixture.markets,
            account_bindings: &fixture.account_bindings,
            multi_binding: fixture.multi.as_ref(),
            metadata: fixture.metadata.as_ref(),
            account_modes: &[],
            fees: &[],
        })
    }

    fn snapshot(instruments: Vec<MetadataInstrument>) -> MetadataSnapshot {
        let mut snapshot = MetadataSnapshot {
            schema_version: 1,
            captured_at_ns: 1,
            instruments,
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        snapshot
    }

    fn instrument(asset_no: u32, symbol: &str, source: &str, lot: &str) -> MetadataInstrument {
        MetadataInstrument {
            asset_no,
            native_symbol: Arc::from(symbol),
            source_key: Some(Arc::from(source)),
            price_tick: unit("0.1"),
            quantity_lot: unit(lot),
            contract_multiplier: unit("1"),
            min_quantity: Some(unit(lot)),
            min_notional_quote: Some(unit("5")),
            linear_quote_currency: Some(Arc::from("USDT")),
            synthetic: false,
        }
    }

    #[test]
    fn bound_definitions_produce_a_buildable_object_table() {
        let mut fixture = base_fixture();
        fixture.metadata = Some(snapshot(vec![
            instrument(0, "BTC-USDT", "maker", "1"),
            instrument(1, "BTC-USDT-SWAP", "hedge", "0.01"),
        ]));
        let input = build(&fixture).expect("fixture is complete");
        assert_eq!(input.symbols.len(), 2);
        assert_eq!(input.accounts.len(), 2);
        assert_eq!(input.pairs.len(), 1);
        assert_eq!(input.pairs[0].pair_revision, 3);
        assert_eq!(input.symbols[0].quote_currency.as_ref(), "USDT");
        assert_eq!(input.symbols[0].margin_currency.as_ref(), "USDT");
        assert!(!input.symbols[0].synthetic);
        let table = crate::ObjectTable::build(input).expect("table admits the fixture");
        let pair = table.pair(0).expect("pair exists");
        assert_eq!(pair.hedge_lots_per_maker_lot, 100);
        assert_eq!(pair.pair_revision, 3);
    }

    #[test]
    fn pairs_without_authoritative_metadata_are_refused() {
        let fixture = base_fixture();
        let input = build(&fixture).expect("bindings alone still describe the table");
        let error =
            crate::ObjectTable::build(input).expect_err("synthetic pair legs cannot be admitted");
        assert_eq!(error.code(), "object_admission_unprovable");
    }

    #[test]
    fn metadata_snapshot_is_authoritative_when_present() {
        let mut fixture = base_fixture();
        let mut instrument = MetadataInstrument {
            asset_no: 0,
            native_symbol: Arc::from("BTC-USDT"),
            source_key: Some(Arc::from("maker")),
            price_tick: unit("0.5"),
            quantity_lot: unit("0.001"),
            contract_multiplier: unit("1"),
            min_quantity: Some(unit("0.001")),
            min_notional_quote: Some(unit("5")),
            linear_quote_currency: Some(Arc::from("USDT")),
            synthetic: false,
        };
        instrument.asset_no = 0;
        let mut snapshot = MetadataSnapshot {
            schema_version: 1,
            captured_at_ns: 1,
            instruments: vec![instrument],
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        fixture.metadata = Some(snapshot);
        // The snapshot covers only one symbol; the second one must fail closed.
        assert_eq!(
            build(&fixture).unwrap_err().code(),
            OBJECT_SOURCE_METADATA_MISSING
        );
        // Add the hedge symbol and check authoritative units win over the binding.
        if let Some(snapshot) = fixture.metadata.as_mut() {
            snapshot.instruments.push(MetadataInstrument {
                asset_no: 1,
                native_symbol: Arc::from("BTC-USDT-SWAP"),
                source_key: Some(Arc::from("hedge")),
                price_tick: unit("1"),
                quantity_lot: unit("0.1"),
                contract_multiplier: unit("1"),
                min_quantity: Some(unit("0.1")),
                min_notional_quote: Some(unit("5")),
                linear_quote_currency: Some(Arc::from("USDT")),
                synthetic: false,
            });
            snapshot.digest = snapshot.compute_digest();
        }
        let input = build(&fixture).expect("snapshot covers every bound symbol");
        assert!(!input.symbols[0].synthetic);
        assert_eq!(input.symbols[0].price_tick, unit("0.5"));
        assert_eq!(input.symbols[0].min_quantity, Some(unit("0.001")));
        assert_eq!(
            input.metadata_snapshot_digest,
            fixture.metadata.as_ref().unwrap().digest
        );
    }

    #[test]
    fn missing_definitions_and_legs_fail_closed() {
        let mut fixture = base_fixture();
        fixture.sources.remove(0);
        assert_eq!(
            build(&fixture).unwrap_err().code(),
            OBJECT_SOURCE_BINDING_MISSING
        );

        let mut fixture = base_fixture();
        fixture.multi.as_mut().unwrap().legs = Arc::from([
            leg(0, 0, ExecutionLegRole::Maker, "0.01"),
            leg(1, 9, ExecutionLegRole::Hedge, "1"),
        ]);
        assert_eq!(build(&fixture).unwrap_err().code(), OBJECT_PAIR_UNKNOWN);

        let mut fixture = base_fixture();
        fixture.accounts[0].instruments = Arc::from([]);
        fixture.markets[0].source_key = Arc::from("maker");
        fixture.account_bindings[0].tradable_assets = Arc::from([StrategyTradableAsset {
            local_asset_no: 0,
            asset_id: 1001,
        }]);
        // No account instrument to declare the quote currency; the market symbol still provides it.
        let input = build(&fixture).expect("market symbol supplies the quote currency");
        assert_eq!(input.accounts[0].quote_currency.as_ref(), "USDT");
    }
}
