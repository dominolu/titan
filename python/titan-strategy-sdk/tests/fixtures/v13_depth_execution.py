"""Synthetic native order callbacks for host depth live/replay admission tests."""
from numba import njit
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

SPEC = StrategySpec("depth_execution_probe", "1.0.0", 1,
    subscriptions=(EventSubscription(EventKind.DEPTH, "on_depth", 1, EventQos.RELIABLE_ORDERED),
                   EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.RELIABLE_ORDERED)),
    capabilities=Capability.MARKET_DATA | Capability.HOST_DEPTH_100 | Capability.ORDER_EXECUTION)
state_dtype = record(callbacks=int64)

@njit
def on_depth(ctx):
    ctx.state["callbacks"] += 1

@njit
def on_tick(ctx):
    mode = ctx.ticks()[0]["price_ticks"]
    if mode == 1:
        ctx.submit_order(0, 0, 1, 1, 1, 10000, 1)
    elif mode == 2:
        ctx.cancel_order(0, 0, 7)
        ctx.submit_order(0, 0, 2, 1, 2, 10000, 1, True)
    elif mode == 3:
        ctx.submit_order(0, 0, 2, 1, 6, 10000, 1, True)

def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_depth": on_depth, "on_tick": on_tick})
