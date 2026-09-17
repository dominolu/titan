from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos


state_dtype = record(value=int64)
SPEC = StrategySpec(
    strategy_id="missing_view",
    strategy_version="1.0.0",
    state_schema_version=1,
    parameters=(),
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.ACCOUNT_DATA,
)


@njit
def on_tick(ctx):
    ctx.state["value"] = ctx.position(0, 0)["qty_lots"]


def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick})
