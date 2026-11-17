from numba import njit
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

SPEC = StrategySpec("host_depth_probe", "1.0.0", 1,
    subscriptions=(EventSubscription(EventKind.DEPTH, "on_depth", 1, EventQos.RELIABLE_ORDERED),),
    capabilities=Capability.MARKET_DATA | Capability.HOST_DEPTH_100)
state_dtype = record(callbacks=int64, valid=int64, reset=int64, bid100=int64, ask100=int64, epoch=int64)

@njit
def on_depth(ctx):
    info = ctx.depth_batch()[0]
    s = ctx.state
    s["callbacks"] += 1
    s["valid"] = info["valid"]
    s["reset"] = info["reset"]
    s["epoch"] = info["stream_epoch"]
    s["bid100"] = 0
    s["ask100"] = 0
    if info["valid"] != 0:
        s["bid100"] = ctx.depth()[99]["price_ticks"]
        s["ask100"] = ctx.depth()[199]["price_ticks"]

def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_depth": on_depth})
