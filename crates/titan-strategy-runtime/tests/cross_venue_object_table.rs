//! One compiled strategy, two venue pairs (acceptance matrix item 3).
//!
//! The matrix item is "the same artifact starts against two `runtime.toml`s". This file proves the
//! part of that claim that does not need a live venue: the object table derived from each venue
//! pair is built by the same builder from the same sources, and the hedge ratio it exposes comes
//! from the venue units rather than from anything the strategy or the configuration wrote down.
//!
//! Everything here is offline: metadata comes from hand-authored snapshots marked `synthetic`, no
//! connector is constructed, and no network or credentials are involved.

use std::sync::Arc;

use titan_account_service::{
    AccountCurrencyBinding, AccountDefinition, AccountId, AccountInstrumentBinding, CurrencyId,
    OrderOwnershipPolicy, SecretRef, ShutdownOrderPolicy,
};
use titan_domain_types::DecimalUnit;
use titan_market_service::{
    AssetId, MarketInstrumentBinding, MarketSourceDefinition, MetadataInstrument, MetadataSnapshot,
};
use titan_strategy_runtime::{
    ExecutionLegLimit, ExecutionLegRole, MultiBindingExecutionLimits, ObjectTable,
    ObjectTableSources, RescueBudget, StopBudget, StrategyAccountBinding, StrategyMarketBinding,
    StrategyRuntimeSpec, StrategyTradableAsset, object_table_input,
    validate_instrument_units_with_metadata,
};

fn unit(value: &str) -> DecimalUnit {
    value.parse().expect("exact decimal")
}

fn asset_id(value: u32) -> AssetId {
    AssetId(value)
}

fn account_asset_id(value: u32) -> titan_account_service::AssetId {
    titan_account_service::AssetId(value)
}

/// One venue of a pair: the source key, the lot size it quotes and the contract multiplier.
#[derive(Clone, Copy)]
struct Venue {
    source_key: &'static str,
    asset_id: u32,
    native_symbol: &'static str,
    quantity_lot: &'static str,
    contract_multiplier: &'static str,
    /// `quantity_lot * contract_multiplier`: what the configuration's `base_delta_per_lot` must
    /// say. Written out instead of computed so the fixture pins the number independently.
    base_delta_per_lot: &'static str,
}

/// Two venue pairs with different native lot sizes but the same economic hedge ratio (10 hedge
/// lots per maker lot): a base-currency lot venue and a contract venue quoting 1 contract = 0.01
/// base units.
fn venue_pairs() -> Vec<(&'static str, Venue, Venue)> {
    vec![
        (
            "base-and-fine-lot",
            Venue {
                source_key: "maker",
                asset_id: 1101,
                native_symbol: "BTC-USDT-SWAP",
                quantity_lot: "0.01",
                contract_multiplier: "1",
                base_delta_per_lot: "0.01",
            },
            Venue {
                source_key: "hedge",
                asset_id: 1102,
                native_symbol: "BTCUSDT",
                quantity_lot: "0.001",
                contract_multiplier: "1",
                base_delta_per_lot: "0.001",
            },
        ),
        (
            "contract-and-coarse-lot",
            Venue {
                source_key: "maker",
                asset_id: 2101,
                native_symbol: "BTC-USDT-SWAP",
                quantity_lot: "1",
                contract_multiplier: "0.01",
                base_delta_per_lot: "0.01",
            },
            Venue {
                source_key: "hedge",
                asset_id: 2102,
                native_symbol: "BTCUSDT",
                quantity_lot: "1",
                contract_multiplier: "0.001",
                base_delta_per_lot: "0.001",
            },
        ),
    ]
}

fn market_sources(maker: &Venue, hedge: &Venue) -> Vec<MarketSourceDefinition> {
    [maker, hedge]
        .into_iter()
        .map(|venue| MarketSourceDefinition {
            source_key: Arc::from(venue.source_key),
            connector_type: Arc::from("trace"),
            connector_config: Arc::from(&b""[..]),
            instruments: Arc::from([MarketInstrumentBinding {
                native_symbol: Arc::from(venue.native_symbol),
                asset_id: asset_id(venue.asset_id),
                price_tick: unit("0.1"),
                quantity_lot: unit(venue.quantity_lot),
                contract_multiplier: unit(venue.contract_multiplier),
                min_quantity: Some(unit(venue.quantity_lot)),
                min_notional_quote: Some(unit("5")),
                linear_quote_currency: Some(Arc::from("USDT")),
            }]),
            enabled: true,
            definition_version: 1,
        })
        .collect()
}

fn accounts(venues: [&Venue; 2]) -> Vec<AccountDefinition> {
    venues
        .into_iter()
        .enumerate()
        .map(|(index, venue)| AccountDefinition {
            account_key: Arc::from(format!("{}-account", venue.source_key)),
            account_id: AccountId(2001 + index as u32),
            connector_type: Arc::from("trace"),
            credential_ref: SecretRef(Arc::from("secret://trace")),
            connector_config: Arc::from(&b""[..]),
            instruments: Arc::from([AccountInstrumentBinding {
                native_symbol: Arc::from(venue.native_symbol),
                asset_id: account_asset_id(venue.asset_id),
                price_tick: unit("0.1"),
                quantity_lot: unit(venue.quantity_lot),
                contract_multiplier: unit(venue.contract_multiplier),
                linear_quote_currency: Some(CurrencyId(1)),
            }]),
            currencies: Arc::from([AccountCurrencyBinding {
                native_currency: Arc::from("USDT"),
                currency_id: CurrencyId(1),
                amount_unit: unit("0.000001"),
            }]),
            ownership: OrderOwnershipPolicy::ObserveAll,
            shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        })
        .collect()
}

fn markets(venues: [&Venue; 2]) -> Vec<StrategyMarketBinding> {
    venues
        .into_iter()
        .enumerate()
        .map(|(index, venue)| StrategyMarketBinding {
            local_market_no: index as u32,
            local_asset_no: index as u32,
            source_key: Arc::from(venue.source_key),
            asset_id: venue.asset_id,
            data_mode: titan_strategy_runtime::StrategyDataMode::Tick,
        })
        .collect()
}

fn account_bindings(venues: [&Venue; 2]) -> Vec<StrategyAccountBinding> {
    venues
        .into_iter()
        .enumerate()
        .map(|(index, venue)| StrategyAccountBinding {
            local_account_no: index as u32,
            account_key: Arc::from(format!("{}-account", venue.source_key)),
            tradable_assets: Arc::from([StrategyTradableAsset {
                local_asset_no: index as u32,
                asset_id: venue.asset_id,
            }]),
            fees: None,
        })
        .collect()
}

fn multi_binding(maker: &Venue, hedge: &Venue) -> MultiBindingExecutionLimits {
    let leg = |local_account_no: u32,
               local_asset_no: u32,
               role: ExecutionLegRole,
               base_delta_per_lot: &str| ExecutionLegLimit {
        local_account_no,
        local_asset_no,
        role,
        base_delta_per_lot: unit(base_delta_per_lot),
        min_quantity_lots: 1,
        min_notional_quote: unit("5"),
        max_position_lots: 100,
        max_notional_tick_lots: "100000".into(),
        max_market_age_ns: 1_000_000_000,
        margin_domain: local_account_no,
        margin_rate_bps: 1_000,
    };
    MultiBindingExecutionLimits {
        revision: 1,
        quote_currency: 1,
        legs: Arc::from([
            leg(0, 0, ExecutionLegRole::Maker, maker.base_delta_per_lot),
            leg(1, 1, ExecutionLegRole::Hedge, hedge.base_delta_per_lot),
        ]),
        rescue: RescueBudget {
            max_quantity_lots: 10,
            max_notional_tick_lots: "10000".into(),
            max_cost_bps: 50,
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

/// The venue's own published metadata for both legs of a pair, with the caller's lot override.
fn snapshot(
    venues: [&Venue; 2],
    maker_lot: &'static str,
    hedge_lot: &'static str,
) -> MetadataSnapshot {
    let instruments = venues
        .into_iter()
        .enumerate()
        .map(|(index, venue)| MetadataInstrument {
            asset_no: index as u32,
            native_symbol: Arc::from(venue.native_symbol),
            source_key: Some(Arc::from(venue.source_key)),
            price_tick: unit("0.1"),
            quantity_lot: unit(if index == 0 { maker_lot } else { hedge_lot }),
            contract_multiplier: unit(venue.contract_multiplier),
            min_quantity: Some(unit(if index == 0 { maker_lot } else { hedge_lot })),
            min_notional_quote: Some(unit("5")),
            linear_quote_currency: Some(Arc::from("USDT")),
            synthetic: true,
        })
        .collect::<Vec<_>>();
    let mut snapshot = MetadataSnapshot {
        schema_version: 1,
        captured_at_ns: 1_700_000_000_000,
        instruments,
        digest: [0; 32],
    };
    snapshot.digest = snapshot.compute_digest();
    assert!(snapshot.digest_matches(), "snapshot digest is reproducible");
    snapshot
}

fn strategy_definition(
    markets: &[StrategyMarketBinding],
    accounts: &[StrategyAccountBinding],
    multi_binding: MultiBindingExecutionLimits,
) -> titan_strategy_runtime::StrategyDefinition {
    let value = serde_json::json!({
        "strategy_key": "cross-venue-probe",
        "runtime_strategy_no": 1,
        "package": {
            "loader_type": "native",
            "uri": "artifact://cross_venue.titan",
            "expected_digest": vec![0_u8; 32]
        },
        "markets": markets,
        "accounts": accounts,
        "risk_scope": "shared",
        "runtime": serde_json::to_value(StrategyRuntimeSpec {
            multi_binding: Some(multi_binding),
            ..StrategyRuntimeSpec::default()
        })
        .expect("runtime spec serializes"),
        "recovery": "fresh",
        "enabled": true,
        "run_config_revision": 1
    });
    serde_json::from_value(value).expect("strategy definition")
}

fn object_table(sources: &ObjectTableSources<'_>) -> ObjectTable {
    let input = object_table_input(sources).expect("object table input");
    ObjectTable::build(input).expect("object table")
}

#[test]
fn one_artifact_derives_the_same_hedge_ratio_from_two_venue_pairs() {
    let mut ratios = Vec::new();
    let mut identities = Vec::new();
    for (label, maker, hedge) in venue_pairs() {
        let venues = [&maker, &hedge];
        let sources = market_sources(&maker, &hedge);
        let accounts = accounts(venues);
        let markets = markets(venues);
        let bindings = account_bindings(venues);
        let limits = multi_binding(&maker, &hedge);
        let snapshot = snapshot(venues, maker.quantity_lot, hedge.quantity_lot);

        // The startup validator accepts both pairs against their own snapshot.
        let strategy = strategy_definition(&markets, &bindings, limits.clone());
        let report = validate_instrument_units_with_metadata(
            &sources,
            &accounts,
            std::slice::from_ref(&strategy),
            Some(&snapshot),
        );
        assert!(
            report.is_ok(),
            "{label}: {:?}",
            report
                .errors
                .iter()
                .map(|error| (error.code(), error.subject(), error.detail()))
                .collect::<Vec<_>>()
        );

        let table = object_table(&ObjectTableSources {
            market_sources: &sources,
            accounts: &accounts,
            markets: &markets,
            account_bindings: &bindings,
            multi_binding: Some(&limits),
            metadata: Some(&snapshot),
            account_modes: &[],
            fees: &[],
        });
        let pair = table.pair(0).expect("pair object");
        // 0.01 base per maker lot / 0.001 base per hedge lot = 10, derived from venue units.
        assert_eq!(pair.hedge_lots_per_maker_lot, 10, "{label}");
        assert_eq!(pair.hedge_ticks_per_maker_tick_num, 1, "{label}");
        assert_eq!(pair.hedge_ticks_per_maker_tick_den, 1, "{label}");
        assert_eq!(
            table
                .symbol(0)
                .expect("maker symbol")
                .base_delta_per_lot
                .unit(),
            Some(unit("0.01")),
            "{label}"
        );
        assert_eq!(
            table
                .symbol(1)
                .expect("hedge symbol")
                .base_delta_per_lot
                .unit(),
            Some(unit("0.001")),
            "{label}"
        );
        assert_eq!(table.metadata_snapshot_digest(), snapshot.digest, "{label}");
        ratios.push(pair.hedge_lots_per_maker_lot);
        // Different venues are different instruments, so the identities must not collapse.
        identities.push(table.identity_digest());
    }
    assert_eq!(ratios, [10, 10], "same ratio without recompiling anything");
    assert_ne!(
        identities[0], identities[1],
        "venue identity is part of the object identity, not the ratio"
    );
}

#[test]
fn a_venue_pair_with_a_non_integral_ratio_is_refused_at_startup() {
    let (_, maker, hedge) = venue_pairs()[0];
    let venues = [&maker, &hedge];
    let sources = market_sources(&maker, &hedge);
    let accounts = accounts(venues);
    let markets = markets(venues);
    let bindings = account_bindings(venues);
    // 0.01 / 0.003 = 10/3: no integer number of hedge lots reproduces one maker lot.
    let snapshot = snapshot(venues, "0.01", "0.003");

    // The configuration must also agree with the snapshot, so the reported failure is the ratio.
    let limits = multi_binding(&maker, &hedge);
    let mut legs = limits.legs.to_vec();
    legs[1].base_delta_per_lot = unit("0.003");
    let strategy = strategy_definition(
        &markets,
        &bindings,
        MultiBindingExecutionLimits {
            legs: Arc::from(legs),
            ..limits
        },
    );
    let report = validate_instrument_units_with_metadata(
        &sources,
        &accounts,
        std::slice::from_ref(&strategy),
        Some(&snapshot),
    );
    assert!(
        report
            .instruments
            .iter()
            .any(|units| units.base_delta_per_lot == unit("0.01")),
        "the report exposes the derived units"
    );
    let codes = report.error_codes();
    assert!(
        codes.contains(&"instrument_lot_ratio_not_integral"),
        "{codes:?}"
    );

    // The builder refuses the same pair on its own, with the object-level reason code.
    let input = object_table_input(&ObjectTableSources {
        market_sources: &sources,
        accounts: &accounts,
        markets: &markets,
        account_bindings: &bindings,
        multi_binding: strategy.runtime.multi_binding.as_ref(),
        metadata: Some(&snapshot),
        account_modes: &[],
        fees: &[],
    })
    .expect("sources are complete");
    let error = ObjectTable::build(input).expect_err("non-integral ratio cannot be admitted");
    assert_eq!(error.code(), "object_pair_lot_ratio", "{}", error.detail());
}
