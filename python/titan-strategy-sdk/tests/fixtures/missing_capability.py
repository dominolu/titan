from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos


state_dtype = record(counter=int64)
SPEC = StrategySpec(
    strategy_id="missing_capability",
    strategy_version="1.0.0",
    state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA,
)


@njit
def on_tick(ctx):
    ctx.submit_order(0, 0, 1, 1, 1, 100, 1)


def build(parameters):
    del parameters
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick})
