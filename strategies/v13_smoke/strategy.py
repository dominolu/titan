"""Minimal Strategy ABI V13 artifact used by compiler/runtime smoke tests."""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.parameters import IntParam
from titan_strategy.state import array, int64, new_state, record
from titan_strategy.types import EventKind, EventQos


state_dtype = record(counter=int64, nested=record(value=int64), window=array(int64, 4))

SPEC = StrategySpec(
    strategy_id="v13_smoke",
    strategy_version="1.0.0",
    state_schema_version=1,
    parameters=(IntParam("initial_counter", minimum=0),),
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.ORDER_EXECUTION,
)


@njit
def on_tick(ctx):
    ctx.state["counter"] += 1
    ctx.state["nested"]["value"] += ctx.best_bid_ticks(0)
    ctx.state["window"][2] += 3
    ctx.state["nested"]["value"] = ctx.submit_order(0, 0, 1, 1, 2, 100, 1)


def build(parameters):
    state = new_state(state_dtype)
    state[0]["counter"] = parameters["initial_counter"]
    return StrategyDefinition(SPEC, state, {"on_tick": on_tick}, {})
