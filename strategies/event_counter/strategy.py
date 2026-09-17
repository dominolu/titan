"""Strategy ABI V13 typed-state event counter."""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import new_state, record, uint64
from titan_strategy.types import EventKind, EventQos

state_dtype = record(tick_items=uint64, tick_callbacks=uint64, bar_items=uint64, bar_callbacks=uint64)
SPEC = StrategySpec(
    strategy_id="event_counter", strategy_version="2.0.0", state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),
                   EventSubscription(EventKind.BAR, "on_bar", 1, EventQos.RELIABLE_ORDERED)),
    capabilities=Capability.MARKET_DATA,
)

@njit
def on_tick(ctx):
    ctx.state["tick_items"] += len(ctx.ticks())
    ctx.state["tick_callbacks"] += 1

@njit
def on_bar(ctx):
    ctx.state["bar_items"] += len(ctx.bars())
    ctx.state["bar_callbacks"] += 1

def build(_parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick, "on_bar": on_bar})
