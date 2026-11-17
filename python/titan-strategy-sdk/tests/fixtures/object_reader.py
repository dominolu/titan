"""End-to-end V14 object reader: compiled callback reads the authoritative object table."""

import numpy as np
from numba import njit

from titan_strategy import testing
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

state_dtype = record(
    price_coefficient=int64,
    price_scale=int64,
    base_delta_coefficient=int64,
    min_qty_lots=int64,
    min_notional_tick_lots=int64,
    quote_currency_no=int64,
    account_id=int64,
    amount_unit_scale=int64,
    lifecycle=int64,
    maker_fee_bps=int64,
    fee_present=int64,
)

SPEC = StrategySpec(
    strategy_id="v14_object_reader",
    strategy_version="1.0.0",
    state_schema_version=1,
    parameters=(),
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.OBJECT_CONTEXT,
)


@njit
def on_tick(ctx):
    symbol = ctx.symbol(0)
    account = ctx.account_info(0)
    currency = ctx.currency(0, 1)
    status = ctx.status()
    fee = ctx.fee(0)
    ctx.state["price_coefficient"] = np.int64(symbol["price_tick"]["coefficient"])
    ctx.state["price_scale"] = np.int64(symbol["price_tick"]["scale"])
    ctx.state["base_delta_coefficient"] = np.int64(symbol["base_delta_per_lot"]["coefficient"])
    ctx.state["min_qty_lots"] = np.int64(symbol["min_qty_lots"])
    ctx.state["min_notional_tick_lots"] = np.int64(symbol["min_notional_tick_lots"])
    ctx.state["quote_currency_no"] = np.int64(symbol["quote_currency_no"])
    ctx.state["account_id"] = np.int64(account["account_id"])
    ctx.state["amount_unit_scale"] = np.int64(currency["amount_unit"]["scale"])
    ctx.state["lifecycle"] = np.int64(status["lifecycle"])
    ctx.state["maker_fee_bps"] = np.int64(fee["maker_fee_bps"])
    ctx.state["fee_present"] = np.int64(fee["flags"])


TESTS = [{
    "name": "authoritative_units_are_visible_in_the_callback",
    "objects": testing.objects(
        symbols=[dict(
            local_asset_no=0,
            asset_id=1,
            quote_currency_no=1,
            margin_currency_no=1,
            flags=1,
            price_tick={"coefficient": 1, "scale": 1},
            quantity_lot={"coefficient": 1, "scale": 2},
            contract_multiplier={"coefficient": 1, "scale": 0},
            base_delta_per_lot={"coefficient": 1, "scale": 2},
            min_quantity={"coefficient": 1, "scale": 2},
            min_notional_quote={"coefficient": 5, "scale": 0},
            min_qty_lots=1,
            min_notional_tick_lots=5000,
        )],
        accounts=[dict(
            local_account_no=0,
            account_id=77,
            quote_currency_no=1,
            margin_currency_no=1,
            currency_start=0,
            currency_count=1,
            flags=1,
        )],
        currencies=[dict(
            local_account_no=0,
            currency_no=1,
            flags=1,
            amount_unit={"coefficient": 1, "scale": 6},
        )],
        fees=[dict(local_account_no=0, flags=1, maker_fee_bps=-3, taker_fee_bps=7)],
        status={"lifecycle": 3},
    ),
    "steps": [{
        "callback": "on_tick",
        "expect": {
            "price_coefficient": 1,
            "price_scale": 1,
            "base_delta_coefficient": 1,
            "min_qty_lots": 1,
            "min_notional_tick_lots": 5000,
            "quote_currency_no": 1,
            "account_id": 77,
            "amount_unit_scale": 6,
            "lifecycle": 3,
            "maker_fee_bps": -3,
            "fee_present": 1,
        },
    }],
}]


def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick}, tests=TESTS)
