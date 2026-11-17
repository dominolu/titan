"""Recovery-time command rejection fixture.

A `Recover` callback must not issue commands: the online host fails the runtime when it tries and
the offline adapter must return the same stable error. This strategy deliberately submits inside
`on_recover` so both paths can be exercised without touching a real exchange.
"""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

state_dtype = record(counter=int64)

SPEC = StrategySpec(
    strategy_id="v13_recover_submit",
    strategy_version="1.0.0",
    state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.ORDER_EXECUTION,
)


@njit
def on_tick(ctx):
    ctx.state["counter"] += 1


@njit
def on_recover(ctx):
    # Attempted command inside recovery: must be refused, never staged.
    ctx.submit_order(0, 0, 1, 1, 2, 100, 1)
    return 0


def build(parameters):
    return StrategyDefinition(
        SPEC,
        new_state(state_dtype),
        {"on_tick": on_tick, "on_recover": on_recover},
        {},
    )
