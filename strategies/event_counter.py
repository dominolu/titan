"""Strategy ABI V13 typed-state event counter."""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import new_state, record, uint64
from titan_strategy.types import EventKind, EventQos

state_dtype = record(tick_items=uint64, tick_callbacks=uint64, bar_items=uint64, bar_callbacks=uint64)
SPEC = StrategySpec(
    strategy_type="event_counter", source_version="2.0.0", recovery_state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA,
)

@njit
def on_tick(ctx):
    ctx.state["tick_items"] += len(ctx.ticks())
    ctx.state["tick_callbacks"] += 1

def build(_parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick})


TESTS = (
    {"name": "empty_callback", "steps": [{"callback": "on_tick"}],
     "expect": {"tick_callbacks": 1, "tick_items": 0}},
    {"name": "count_items", "steps": [{"callback": "on_tick", "views": {"ticks": [{"price_ticks": 100}, {"price_ticks": 101}]}}],
     "expect": {"tick_callbacks": 1, "tick_items": 2}},
)
