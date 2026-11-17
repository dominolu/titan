from numba import njit
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos, PositionSide

state_dtype = record(long_qty=int64, short_qty=int64, order_id=int64)
SPEC = StrategySpec(
    strategy_id="hedge_views", strategy_version="1.0.0", state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.ACCOUNT_DATA | Capability.ORDER_EXECUTION,
)

@njit
def on_tick(ctx):
    ctx.state["long_qty"] = ctx.position(0, 0, PositionSide.LONG)["qty_lots"]
    ctx.state["short_qty"] = ctx.position(0, 0, PositionSide.SHORT)["qty_lots"]
    ctx.state["order_id"] = ctx.submit_order(0, 0, 1, 1, 1, 100, 1, True, PositionSide.SHORT)

def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick})
