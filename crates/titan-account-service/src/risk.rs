//! Platform risk valuation. Inputs stay in native tick/lot units until the final currency conversion.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Mutex};
use titan_core_types::EventView;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRiskOrder {
    pub client_order_id: Id128,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub filled_lots: i64,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRiskEvidence {
    pub intent_revision: u64,
    pub account_id: u32,
    pub account_generation: u64,
    pub account_epoch: u64,
    pub asset_id: u32,
    pub positions: Vec<(u8, i64)>,
    pub expected_positions: Vec<(u8, String)>,
    pub orders: Vec<ExecutionRiskOrder>,
    pub inventory_confirmed: bool,
}

/// Shared by runtime admission and account monitoring; cancellations do not release reservations.
pub fn exposure_tick_lots(
    ask: i64,
    gross_lots: i128,
    orders: impl IntoIterator<Item = (i64, i64)>,
) -> ObservationResult<(i128, i128)> {
    if ask <= 0 || gross_lots < 0 {
        return Err("risk mark or gross quantity is invalid".into());
    }
    let actual = gross_lots
        .checked_mul(i128::from(ask))
        .ok_or("risk notional overflow")?;
    let mut bound = actual;
    for (remaining, limit) in orders {
        if remaining < 0 || limit < 0 {
            return Err("risk order quantity or price is invalid".into());
        }
        bound = bound
            .checked_add(
                i128::from(remaining)
                    .checked_mul(i128::from(ask.max(limit)))
                    .ok_or("risk notional overflow")?,
            )
            .ok_or("risk notional overflow")?;
    }
    Ok((actual, bound))
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RiskQuote {
    pub stream: u32,
    pub bid: i64,
    pub ask: i64,
    pub received_ns: i64,
}
#[derive(Default)]
pub(crate) struct RiskPrices(Mutex<BTreeMap<(u32, u32), RiskQuote>>);
impl RiskPrices {
    pub fn bind(&self, account: u32, asset: u32, stream: u32) -> ObservationResult<()> {
        let mut prices = self.0.lock().map_err(|_| "risk price cache poisoned")?;
        if prices.len() >= 1024 && !prices.contains_key(&(account, asset)) {
            return Err("risk price binding capacity exceeded".into());
        }
        if prices
            .get(&(account, asset))
            .is_some_and(|old| old.stream != stream)
        {
            return Err("ambiguous risk price source".into());
        }
        prices.entry((account, asset)).or_insert(RiskQuote {
            stream,
            ..Default::default()
        });
        Ok(())
    }
    /// Latest-value coalescing only: market publication never waits for a sampler or disk writer.
    pub fn observe(&self, event: EventView<'_>) {
        if event.schema_version != 1 || event.payload.len() != 100 {
            return;
        }
        let bytes = event.payload;
        if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != 2 {
            return;
        }
        let asset = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let mut quote = RiskQuote {
            stream: event.metadata.source_id,
            received_ns: i64::from_le_bytes(bytes[44..52].try_into().unwrap()),
            ..Default::default()
        };
        for offset in [52, 76] {
            let price = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            let qty = i64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
            match bytes[offset + 16] {
                1 if qty > 0 => quote.bid = price,
                2 if qty > 0 => quote.ask = price,
                _ => return,
            }
        }
        if quote.bid <= 0 || quote.ask < quote.bid {
            return;
        }
        if let Ok(mut prices) = self.0.try_lock() {
            for ((_, id), value) in prices.iter_mut() {
                if *id == asset
                    && value.stream == quote.stream
                    && quote.received_ns >= value.received_ns
                {
                    *value = quote;
                }
            }
        }
    }
    pub fn snapshot(&self) -> ObservationResult<BTreeMap<(u32, u32), RiskQuote>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| "risk price cache poisoned")?
            .clone())
    }
}

fn notional_units(
    tick_lots: i128,
    instrument: &AccountInstrumentBinding,
    currency: &AccountCurrencyBinding,
) -> ObservationResult<i128> {
    let units = [
        instrument.price_tick,
        instrument.quantity_lot,
        instrument.contract_multiplier,
    ];
    let mut numerator = tick_lots;
    for unit in units {
        if unit.coefficient() == 0 {
            return Err("invalid risk instrument units".into());
        }
        numerator = numerator
            .checked_mul(i128::from(unit.coefficient()))
            .ok_or("risk decimal overflow")?;
    }
    if currency.amount_unit.coefficient() == 0 {
        return Err("invalid risk currency unit".into());
    }
    let scale: i32 = units.iter().map(|u| i32::from(u.scale())).sum();
    let shift = i32::from(currency.amount_unit.scale()) - scale;
    let mut denominator = i128::from(currency.amount_unit.coefficient());
    if shift >= 0 {
        numerator = numerator
            .checked_mul(
                10i128
                    .checked_pow(shift as u32)
                    .ok_or("risk decimal scale overflow")?,
            )
            .ok_or("risk decimal overflow")?;
    } else {
        denominator = denominator
            .checked_mul(
                10i128
                    .checked_pow((-shift) as u32)
                    .ok_or("risk decimal scale overflow")?,
            )
            .ok_or("risk decimal overflow")?;
    }
    // Conservative rounding at the declared currency precision, never binary floating point.
    Ok(numerator / denominator + i128::from(numerator % denominator != 0))
}

pub(crate) fn account_risk(
    definition: &AccountDefinition,
    image: &AccountRecoverySnapshot,
    prices: &RiskPrices,
    execution: &[ExecutionRiskEvidence],
    now: i64,
) -> ObservationResult<Value> {
    let prices = prices.snapshot()?;
    let mut assets = Vec::new();
    let mut totals = BTreeMap::<u32, (i128, i128)>::new();
    let mut complete = true;
    for instrument in definition.instruments.iter() {
        let asset = instrument.asset_id.0;
        let result: ObservationResult<Value> = (|| {
            let quote = prices
                .get(&(definition.account_id.0, asset))
                .ok_or("mark_price_unavailable")?;
            if quote.bid <= 0
                || quote.ask < quote.bid
                || now
                    .checked_sub(quote.received_ns)
                    .is_none_or(|age| !(0..=2_000_000_000).contains(&age))
            {
                return Err("mark_price_stale".into());
            }
            let currency = definition
                .currencies
                .iter()
                .find(|c| Some(c.currency_id) == instrument.linear_quote_currency)
                .ok_or("linear_quote_currency_unavailable")?;
            let evidence = execution.iter().find(|e| {
                e.account_id == definition.account_id.0
                    && e.asset_id == asset
                    && e.account_generation == image.orders.account.generation
                    && Some(e.account_epoch) == image.orders.committed_epoch
            });
            let mut observed = BTreeMap::<u8, i128>::new();
            for p in image
                .positions
                .items
                .iter()
                .filter(|p| p.asset_id == instrument.asset_id)
            {
                observed.insert(p.position_side, i128::from(p.quantity_lots));
            }
            let actual = observed.values().try_fold(0i128, |sum, q| {
                sum.checked_add(q.abs()).ok_or("risk quantity overflow")
            })?;
            let mut potential = observed
                .iter()
                .map(|(s, q)| (*s, q.abs()))
                .collect::<BTreeMap<_, _>>();
            let mut orders = BTreeMap::<(u8, [u8; 16]), (i64, i64, i64)>::new();
            let mut unknown = false;
            for (index, o) in image
                .orders
                .items
                .iter()
                .filter(|o| o.asset_id == instrument.asset_id && !matches!(o.status, 2 | 3 | 4 | 6))
                .enumerate()
            {
                unknown |= o.status == 255 || o.price_ticks <= 0;
                let key = if o.client_order_id.0 != [0; 16] {
                    (0, o.client_order_id.0)
                } else {
                    unknown = true;
                    let mut key = [0; 16];
                    key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    (1, key)
                };
                orders.insert(
                    key,
                    (o.quantity_lots, o.filled_quantity_lots, o.price_ticks),
                );
            }
            if let Some(e) = evidence {
                for (side, quantity) in &e.expected_positions {
                    let qty = quantity
                        .parse::<i128>()?
                        .checked_abs()
                        .ok_or("risk quantity overflow")?;
                    potential
                        .entry(*side)
                        .and_modify(|old| *old = (*old).max(qty))
                        .or_insert(qty);
                }
                for o in &e.orders {
                    let row = orders.entry((0, o.client_order_id.0)).or_insert((
                        o.quantity_lots,
                        o.filled_lots,
                        o.price_ticks,
                    ));
                    row.0 = row.0.max(o.quantity_lots);
                    row.1 = row.1.max(o.filled_lots);
                    row.2 = row.2.max(o.price_ticks);
                }
            }
            let matches = evidence.is_some_and(|e| {
                e.inventory_confirmed
                    && e.positions.iter().all(|(side, qty)| {
                        observed.get(side).copied().unwrap_or(0) == i128::from(*qty)
                    })
                    && observed.iter().all(|(side, qty)| {
                        e.positions
                            .iter()
                            .find(|(s, _)| s == side)
                            .map_or(0, |(_, q)| i128::from(*q))
                            == *qty
                    })
            });
            let gross = potential.values().try_fold(0i128, |sum, q| {
                sum.checked_add(*q).ok_or("risk quantity overflow")
            })?;
            let remaining = orders
                .values()
                .map(|(q, f, p)| {
                    q.checked_sub(*f)
                        .filter(|n| *n >= 0)
                        .map(|n| (n, *p))
                        .ok_or("invalid cumulative risk quantity")
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (_, bound) = exposure_tick_lots(quote.ask, gross, remaining)?;
            let actual_units = notional_units(
                exposure_tick_lots(quote.ask, actual, [])?.0,
                instrument,
                currency,
            )?;
            let bound_units = notional_units(bound, instrument, currency)?;
            let row = totals.entry(currency.currency_id.0).or_default();
            row.0 = row
                .0
                .checked_add(actual_units)
                .ok_or("risk currency total overflow")?;
            row.1 = row
                .1
                .checked_add(bound_units)
                .ok_or("risk currency total overflow")?;
            let mut missing = Vec::new();
            if !matches {
                missing.push("execution_inventory_or_intents_unproven");
            }
            if unknown {
                missing.push("unknown_order_state_or_price");
            }
            let exact = |units: i128| -> ObservationResult<Value> {
                Ok(
                    json!({"currency":currency.native_currency,"amount_unit":currency.amount_unit.to_string(),"amount_units":units.to_string(),"amount":crate::observation::exact(units,currency.amount_unit)?}),
                )
            };
            Ok(
                json!({"asset_id":asset.to_string(),"instrument":instrument.native_symbol,"currency":currency.native_currency,"price_source_stream":quote.stream.to_string(),"price_captured_at_ns":quote.received_ns.to_string(),"captured_at_ns":now.to_string(),"actual_notional":exact(actual_units)?,"pending_bound_notional":exact(bound_units)?,"complete":missing.is_empty(),"missing":missing}),
            )
        })();
        let value=result.unwrap_or_else(|error|json!({"asset_id":asset.to_string(),"instrument":instrument.native_symbol,"actual_notional":null,"pending_bound_notional":null,"complete":false,"missing":[error.to_string()]}));
        complete &= value["complete"] == true;
        assets.push(value);
    }
    let notional=totals.into_iter().map(|(id,(actual,bound))| -> ObservationResult<Value> {
        let c=definition.currencies.iter().find(|c|c.currency_id.0==id).ok_or("missing currency")?;
        Ok(json!({"currency":c.native_currency,"amount_unit":c.amount_unit.to_string(),"actual_amount":crate::observation::exact(actual,c.amount_unit)?,"pending_bound_amount":crate::observation::exact(bound,c.amount_unit)?}))
    }).collect::<ObservationResult<Vec<_>>>()?;
    Ok(
        json!({"scope":"account_positions_and_known_execution_intents","valuation":"fresh_best_ask_conservative","captured_at_ns":now.to_string(),"assets":assets,"notional":notional,"complete":complete && !definition.instruments.is_empty(),"maximum_method":"observed_samples_not_continuous_market_maximum"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn image<T>(items: Vec<T>) -> AccountStateSnapshot<T> {
        AccountStateSnapshot {
            account: AccountHandle {
                account_id: AccountId(1),
                generation: 1,
            },
            state: AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(7),
            captured_at: 20,
            items: items.into(),
        }
    }
    fn definition() -> AccountDefinition {
        AccountDefinition {
            account_key: "gate".into(),
            account_id: AccountId(1),
            connector_type: "test".into(),
            credential_ref: SecretRef("unused".into()),
            connector_config: Arc::from([]),
            instruments: Arc::from([AccountInstrumentBinding {
                native_symbol: "BTC_USDT".into(),
                asset_id: AssetId(1),
                price_tick: "0.1".parse().unwrap(),
                quantity_lot: "0.0001".parse().unwrap(),
                contract_multiplier: "1".parse().unwrap(),
                linear_quote_currency: Some(CurrencyId(1)),
            }]),
            currencies: Arc::from([AccountCurrencyBinding {
                native_currency: "USDT".into(),
                currency_id: CurrencyId(1),
                amount_unit: "0.000001".parse().unwrap(),
            }]),
            ownership: OrderOwnershipPolicy::ObserveAll,
            shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        }
    }
    fn snapshot(qty: i64, filled: i64) -> AccountRecoverySnapshot {
        AccountRecoverySnapshot::coherent(
            image(vec![OrderSnapshot {
                position_side: 0,
                asset_id: AssetId(1),
                side: 1,
                order_type: 0,
                time_in_force: 0,
                status: 1,
                price_ticks: 1000,
                quantity_lots: 10,
                filled_quantity_lots: filled,
                reduce_only: false,
                client_order_id: Id128([1; 16]),
                venue_order_id: Id128([2; 16]),
                command_id: Id128([3; 16]),
            }]),
            image(vec![PositionSnapshot {
                asset_id: AssetId(1),
                position_side: 0,
                margin_type: 0,
                quantity_lots: qty,
                entry_price_ticks: 1000,
                liquidation_price_ticks: 0,
                realized_pnl_units: 0,
                unrealized_pnl_units: 0,
                margin_currency_id: CurrencyId(1),
            }]),
            image(Vec::<BalanceSnapshot>::new()),
        )
        .unwrap()
    }
    #[test]
    fn monetary_risk_merges_intents_once_and_reserves_fill_position_gaps() {
        let def = definition();
        let prices = RiskPrices::default();
        prices.bind(1, 1, 3).unwrap();
        prices.0.lock().unwrap().insert(
            (1, 1),
            RiskQuote {
                stream: 3,
                bid: 999,
                ask: 1000,
                received_ns: 10,
            },
        );
        let mut evidence = ExecutionRiskEvidence {
            intent_revision: 0,
            account_id: 1,
            account_generation: 1,
            account_epoch: 1,
            asset_id: 1,
            positions: vec![(0, 3)],
            expected_positions: vec![(0, "3".into())],
            orders: vec![ExecutionRiskOrder {
                client_order_id: Id128([1; 16]),
                price_ticks: 1000,
                quantity_lots: 10,
                filled_lots: 3,
            }],
            inventory_confirmed: true,
        };
        let result = account_risk(&def, &snapshot(3, 3), &prices, &[evidence.clone()], 20).unwrap();
        assert_eq!(result["assets"][0]["actual_notional"]["amount"], "0.03");
        assert_eq!(
            result["assets"][0]["pending_bound_notional"]["amount"],
            "0.1"
        );
        assert_eq!(result["complete"], true);
        evidence.positions = vec![(0, 0)];
        evidence.inventory_confirmed = false;
        let lag = account_risk(&def, &snapshot(0, 3), &prices, &[evidence], 20).unwrap();
        assert_eq!(lag["assets"][0]["pending_bound_notional"]["amount"], "0.1");
        assert_eq!(lag["complete"], false);
        assert!(
            account_risk(&def, &snapshot(3, 3), &prices, &[], 3_000_000_000).unwrap()["assets"][0]
                ["actual_notional"]
                .is_null()
        );
        let mut unknown = def;
        Arc::make_mut(&mut unknown.instruments)[0].linear_quote_currency = None;
        assert!(account_risk(&unknown,&snapshot(3,3),&prices,&[],20).unwrap()["assets"][0]["actual_notional"].is_null());
    }
    #[test]
    fn notional_rounding_is_exact_and_never_nets_hedge_legs() {
        assert_eq!(
            exposure_tick_lots(1000, 12, [(3, 1005)]).unwrap(),
            (12000, 15015)
        );
        assert!(exposure_tick_lots(1000, i128::MAX, []).is_err());
        let mut def = definition();
        Arc::make_mut(&mut def.instruments)[0].price_tick = "0.0000001".parse().unwrap();
        assert_eq!(
            notional_units(1, &def.instruments[0], &def.currencies[0]).unwrap(),
            1
        );
    }
}
