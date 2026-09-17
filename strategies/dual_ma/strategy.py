"""Strategy ABI V13 typed-state dual moving average."""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.parameters import IntParam
from titan_strategy.state import array, float64, int64, new_state, record
from titan_strategy.types import EventKind, EventQos

MAX_WINDOW = 256
state_dtype = record(
    samples=array(float64, MAX_WINDOW), cursor=int64, count=int64,
    fast=int64, slow=int64, fast_sum=float64, slow_sum=float64,
    fast_mean=float64, slow_mean=float64,
)
SPEC = StrategySpec(
    strategy_id="dual_ma", strategy_version="2.0.0", state_schema_version=1,
    parameters=(IntParam("fast", required=False, default=2, minimum=1),
                IntParam("slow", required=False, default=4, minimum=2, maximum=MAX_WINDOW)),
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),
                   EventSubscription(EventKind.BAR, "on_bar", 1, EventQos.RELIABLE_ORDERED)),
    capabilities=Capability.MARKET_DATA,
)

@njit
def update(s, price):
    cursor, count, fast, slow = s["cursor"], s["count"], s["fast"], s["slow"]
    if count >= slow:
        s["slow_sum"] -= s["samples"][cursor]
    if count >= fast:
        s["fast_sum"] -= s["samples"][(cursor + slow - fast) % slow]
    s["samples"][cursor] = price
    s["fast_sum"] += price
    s["slow_sum"] += price
    count += 1
    s["count"], s["cursor"] = count, (cursor + 1) % slow
    s["fast_mean"] = s["fast_sum"] / min(count, fast)
    s["slow_mean"] = s["slow_sum"] / min(count, slow)

@njit
def on_tick(ctx):
    for tick in ctx.ticks():
        update(ctx.state, tick["price_ticks"])

@njit
def on_bar(ctx):
    for bar in ctx.bars():
        update(ctx.state, bar["close_ticks"])

def build(parameters):
    if parameters["slow"] <= parameters["fast"]:
        raise ValueError("dual_ma requires 0 < fast < slow")
    state = new_state(state_dtype)
    state[0]["fast"], state[0]["slow"] = parameters["fast"], parameters["slow"]
    return StrategyDefinition(SPEC, state, {"on_tick": on_tick, "on_bar": on_bar})
