from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

state_dtype = record(counter=int64)
SPEC = StrategySpec(
    "readonly_violation", "1.0.0", 1, (),
    (EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    Capability.MARKET_DATA,
)


@njit
def on_tick(ctx):
    market = ctx.market(0)
    market["best_bid_ticks"] = 0


def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick}, {})
