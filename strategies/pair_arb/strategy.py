"""ABI V13 pair-arbitrage strategy implementing the Slot-based V3 contract."""

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.parameters import EnumParam, IntParam
from titan_strategy.state import array, int64, new_state, record, uint8, uint32, uint64
from titan_strategy.types import EventKind, EventQos

MAX_ORDER_REFS = 2
MAKER_TAKER, TAKER_TAKER = 1, 2
LONG_SPREAD, SHORT_SPREAD = 1, 2
CREATED, WARMING_UP, RUNNING, DRAINING = 1, 2, 3, 4
RECONCILING, PAUSED, STOPPED, ERROR = 5, 6, 7, 8
NORMAL, RESTRICTED, EMERGENCY, HALT = 0, 1, 2, 3
INITIATOR, HEDGE = 1, 2
BUY, SELL, LIMIT, IOC, POST_ONLY = 1, 2, 1, 2, 4
PENDING_STATUS, ACCEPTED_STATUS, PARTIAL_STATUS, FILLED_STATUS = 1, 2, 3, 4
CANCEL_PENDING_STATUS, CANCELED_STATUS = 5, 6
REJECTED_STATUS, EXPIRED_STATUS, UNKNOWN_STATUS = 7, 8, 255
ACCOUNT_READY = 4
READY_MARKETS, READY_ACCOUNTS, READY_POSITIONS, READY_RECONCILED = 1, 2, 4, 8
READY_ALL = READY_MARKETS | READY_ACCOUNTS | READY_POSITIONS | READY_RECONCILED

pair_dtype = record(
    status=uint8, posture=uint8, posture_latched=uint8, ready_mask=uint8,
    reconcile_required=uint8, mode=uint8, direction=uint8, drain_requested=uint8,
    left_asset_no=uint32, right_asset_no=uint32, left_account_no=uint32,
    right_account_no=uint32, hedge_ratio_num=int64, hedge_ratio_den=int64,
    spread_ticks=int64, requote_distance_ticks=int64, slot_lots=int64,
    max_position_lots=int64, dust_lots=int64, start_time_ns=int64,
    cancel_timeout_ns=int64, slot_timeout_ns=int64, max_unhedged_duration_ns=int64,
    max_unhedged_soft=int64, max_unhedged_hard=int64, max_gross_notional_ticks=int64,
    max_slippage_ticks=int64, quote_cooldown_ns=int64, market_stale_after_ns=int64,
    cancel_retry_limit=uint32, filled_gross_notional_ticks=int64,
    gross_imbalance=int64, open_order_lots=int64, unknown_order_lots=int64,
    fill_count=uint64, cancel_count=uint64, reject_count=uint64, unknown_count=uint64,
    slot_start_count=uint64, posture_change_count=uint64, last_error=uint32,
    left_market_ts_ns=int64, right_market_ts_ns=int64, next_quote_ts_ns=int64,
    left_account_epoch=uint64, right_account_epoch=uint64,
    left_account_sequence=uint64, right_account_sequence=uint64,
    expected_left_position_lots=int64, expected_right_position_lots=int64,
)
slot_dtype = record(
    id=uint64, created_ts_ns=int64, deadline_ts_ns=int64, first_imbalance_ts_ns=int64,
    completed_ts_ns=int64, target_lots=int64, initiator_filled_lots=int64,
    hedge_filled_lots=int64, initiator_order_id=uint64, hedge_order_id=uint64,
    initiator_cancel_failures=uint32, hedge_cancel_failures=uint32,
    initiator_fill_sequence=uint64, hedge_fill_sequence=uint64,
)
# V3 state keeps only business relationships that runtime public facts cannot derive.
order_ref_dtype = record(order_id=uint64, slot_id=uint64, role=uint8, reserved=array(uint8, 7))
state_dtype = record(pair=pair_dtype, slot=slot_dtype,
                     order_refs=array(order_ref_dtype, MAX_ORDER_REFS))

SPEC = StrategySpec(
    strategy_id="pair_arb", strategy_version="3.0.1", state_schema_version=3,
    parameters=(
        IntParam("left_asset_no", required=False, default=0, minimum=0),
        IntParam("right_asset_no", required=False, default=1, minimum=0),
        IntParam("left_account_no", required=False, default=0, minimum=0),
        IntParam("right_account_no", required=False, default=1, minimum=0),
        EnumParam("direction", required=False, default="LONG_SPREAD",
                  values=("LONG_SPREAD", "SHORT_SPREAD")),
        EnumParam("mode", required=False, default="MAKER_TAKER",
                  values=("MAKER_TAKER", "TAKER_TAKER")),
        IntParam("hedge_ratio_numerator", required=False, default=1, minimum=1),
        IntParam("hedge_ratio_denominator", required=False, default=1, minimum=1),
        IntParam("max_position_lots", minimum=1), IntParam("slot_lots", minimum=1),
        IntParam("spread_ticks", required=False, default=0, minimum=0),
        IntParam("requote_distance_ticks", required=False, default=1, minimum=0),
        IntParam("dust_lots", required=False, default=0, minimum=0),
        IntParam("start_time_ns", required=False, default=0, minimum=0),
        IntParam("cancel_timeout_ns", required=False, default=1_000_000_000, minimum=1),
        IntParam("slot_timeout_ns", minimum=1),
        IntParam("max_unhedged_duration_ns", required=False, default=5_000_000_000, minimum=1),
        IntParam("max_unhedged_lots_soft", required=False, default=10, minimum=1),
        IntParam("max_unhedged_lots_hard", required=False, default=20, minimum=1),
        IntParam("max_gross_notional_ticks", required=False,
                 default=9_000_000_000_000_000_000, minimum=1),
        IntParam("max_slippage_ticks", required=False, default=5, minimum=1),
        IntParam("quote_cooldown_ns", required=False, default=100_000_000, minimum=0),
        IntParam("market_stale_after_ns", required=False, default=2_000_000_000, minimum=1),
        IntParam("cancel_retry_limit", required=False, default=3, minimum=0),
    ),
    subscriptions=(
        EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),
        EventSubscription(EventKind.FILL, "on_fill", 2, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.ORDER, "on_order", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.CANCEL, "on_cancel", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.POSITION, "on_position", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.BALANCE, "on_balance", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.ACCOUNT_STATE, "on_account_state", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.TIMER, "on_timer", 1, EventQos.BEST_EFFORT),
    ),
    capabilities=(Capability.MARKET_DATA | Capability.ACCOUNT_DATA
                  | Capability.ORDER_EXECUTION | Capability.TIMER),
)

@njit
def find_ref(s, order_id):
    for i in range(MAX_ORDER_REFS):
        if s["order_refs"][i]["order_id"] == order_id:
            return i
    return -1

@njit
def bind_ref(s, order_id, role):
    for i in range(MAX_ORDER_REFS):
        ref = s["order_refs"][i]
        if ref["order_id"] == 0:
            ref["order_id"], ref["slot_id"], ref["role"] = order_id, s["slot"]["id"], role
            return True
    return False

@njit
def clear_ref(s, order_id):
    i = find_ref(s, order_id)
    if i >= 0:
        ref = s["order_refs"][i]
        ref["order_id"], ref["slot_id"], ref["role"] = 0, 0, 0

@njit
def active_index(active, order_id):
    for i in range(len(active)):
        if active[i]["order_id"] == order_id:
            return i
    return -1

@njit
def free_refs(s):
    count = 0
    for i in range(MAX_ORDER_REFS):
        if s["order_refs"][i]["order_id"] == 0:
            count += 1
    return count

@njit
def required_hedge_lots(s, initiator_lots):
    p = s["pair"]
    numerator = initiator_lots * p["hedge_ratio_num"]
    return (numerator + p["hedge_ratio_den"] - 1) // p["hedge_ratio_den"]

@njit
def hedge_gap(s):
    sl = s["slot"]
    return required_hedge_lots(s, sl["initiator_filled_lots"]) - sl["hedge_filled_lots"]

@njit
def slot_complete(s):
    p, sl = s["pair"], s["slot"]
    return (sl["id"] != 0 and sl["initiator_filled_lots"] >= sl["target_lots"]
            and abs(hedge_gap(s)) <= p["dust_lots"]
            and sl["initiator_order_id"] == 0 and sl["hedge_order_id"] == 0)

@njit
def set_posture(s, posture):
    p = s["pair"]
    if p["posture"] != posture:
        p["posture"], p["posture_change_count"] = posture, p["posture_change_count"] + 1

@njit
def fail_closed(s, code):
    p = s["pair"]
    set_posture(s, HALT)
    p["posture_latched"], p["reconcile_required"] = 1, 1
    p["last_error"], p["status"] = code, ERROR

@njit
def require_reconcile(s, code):
    p = s["pair"]
    if p["posture"] < RESTRICTED:
        set_posture(s, RESTRICTED)
    p["reconcile_required"], p["last_error"], p["status"] = 1, code, RECONCILING

@njit
def valid_book(market):
    return market["best_bid_ticks"] > 0 and market["best_ask_ticks"] > market["best_bid_ticks"]

@njit
def refresh_readiness(ctx):
    p = ctx.state["pair"]
    left_market, right_market = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    markets_ready = (valid_book(left_market) and valid_book(right_market)
                     and p["left_market_ts_ns"] > 0 and p["right_market_ts_ns"] > 0
                     and ctx.now >= p["left_market_ts_ns"] and ctx.now >= p["right_market_ts_ns"]
                     and ctx.now - p["left_market_ts_ns"] <= p["market_stale_after_ns"]
                     and ctx.now - p["right_market_ts_ns"] <= p["market_stale_after_ns"])
    p["ready_mask"] = ((p["ready_mask"] | READY_MARKETS) if markets_ready
                       else (p["ready_mask"] & 0xFE))
    left_account, right_account = ctx.account(p["left_account_no"]), ctx.account(p["right_account_no"])
    accounts_ready = (left_account["account_no"] == p["left_account_no"]
                      and right_account["account_no"] == p["right_account_no"]
                      and left_account["state"] == ACCOUNT_READY
                      and right_account["state"] == ACCOUNT_READY
                      and left_account["account_epoch"] == p["left_account_epoch"]
                      and right_account["account_epoch"] == p["right_account_epoch"])
    p["ready_mask"] = ((p["ready_mask"] | READY_ACCOUNTS) if accounts_ready
                       else (p["ready_mask"] & 0xFD))
    left_position = ctx.position(p["left_account_no"], p["left_asset_no"])
    right_position = ctx.position(p["right_account_no"], p["right_asset_no"])
    positions_ready = (left_position["account_no"] == p["left_account_no"]
                       and left_position["asset_no"] == p["left_asset_no"]
                       and right_position["account_no"] == p["right_account_no"]
                       and right_position["asset_no"] == p["right_asset_no"])
    p["ready_mask"] = ((p["ready_mask"] | READY_POSITIONS) if positions_ready
                       else (p["ready_mask"] & 0xFB))

@njit
def opportunity_valid(ctx):
    p = ctx.state["pair"]
    if p["ready_mask"] != READY_ALL or ctx.now < p["start_time_ns"]:
        return False
    left, right = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    if p["direction"] == LONG_SPREAD:
        return right["best_bid_ticks"] - left["best_ask_ticks"] >= p["spread_ticks"]
    return left["best_bid_ticks"] - right["best_ask_ticks"] >= p["spread_ticks"]

@njit
def position_capacity(ctx):
    p = ctx.state["pair"]
    left = ctx.position(p["left_account_no"], p["left_asset_no"])["qty_lots"]
    right = ctx.position(p["right_account_no"], p["right_asset_no"])["qty_lots"]
    if p["direction"] == LONG_SPREAD:
        left_capacity, right_capacity = p["max_position_lots"] - left, p["max_position_lots"] + right
    else:
        left_capacity, right_capacity = p["max_position_lots"] + left, p["max_position_lots"] - right
    if left_capacity <= 0 or right_capacity <= 0:
        return 0
    ratio_capacity = (right_capacity * p["hedge_ratio_den"]) // p["hedge_ratio_num"]
    return max(0, min(left_capacity, ratio_capacity))

@njit
def positions_caught_up(ctx):
    p, sl = ctx.state["pair"], ctx.state["slot"]
    left = ctx.position(p["left_account_no"], p["left_asset_no"])
    right = ctx.position(p["right_account_no"], p["right_asset_no"])
    return (left["account_sequence"] >= sl["initiator_fill_sequence"]
            and right["account_sequence"] >= sl["hedge_fill_sequence"]
            and left["qty_lots"] == p["expected_left_position_lots"]
            and right["qty_lots"] == p["expected_right_position_lots"])

@njit
def restored_order_valid(s, order, ref):
    p, sl = s["pair"], s["slot"]
    if (order["qty_lots"] <= 0 or order["cumulative_filled_lots"] < 0
            or order["cumulative_filled_lots"] > order["qty_lots"]
            or order["status"] not in (PENDING_STATUS, ACCEPTED_STATUS,
                                       PARTIAL_STATUS, CANCEL_PENDING_STATUS, UNKNOWN_STATUS)):
        return False
    if ref["role"] == INITIATOR:
        expected_side = BUY if p["direction"] == LONG_SPREAD else SELL
        return (sl["initiator_order_id"] == order["order_id"]
                and order["account_no"] == p["left_account_no"]
                and order["asset_no"] == p["left_asset_no"]
                and order["side"] == expected_side)
    if ref["role"] == HEDGE:
        expected_side = SELL if p["direction"] == LONG_SPREAD else BUY
        return (sl["hedge_order_id"] == order["order_id"]
                and order["account_no"] == p["right_account_no"]
                and order["asset_no"] == p["right_asset_no"]
                and order["side"] == expected_side)
    return False

@njit
def ensure_draining_hedge(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    if p["status"] != DRAINING:
        return
    if sl["id"] == 0:
        p["status"] = STOPPED
        return
    gap = hedge_gap(s)
    if gap < -p["dust_lots"]:
        fail_closed(s, 205)
    elif gap > p["dust_lots"] and sl["hedge_order_id"] == 0:
        submit_role(ctx, HEDGE, gap, 1)
    elif (abs(gap) <= p["dust_lots"] and sl["initiator_order_id"] == 0
          and sl["hedge_order_id"] == 0):
        sl["completed_ts_ns"] = ctx.now
        p["status"] = STOPPED

@njit
def risk_check(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    refresh_readiness(ctx)
    active, open_lots, unknown_lots, initiator_risk = ctx.active_orders(), 0, 0, 0
    for i in range(MAX_ORDER_REFS):
        ref = s["order_refs"][i]
        if ref["order_id"] == 0:
            continue
        index = active_index(active, ref["order_id"])
        if index < 0:
            unknown_lots += 1
            continue
        order = active[index]
        leaves = max(0, order["qty_lots"] - order["cumulative_filled_lots"])
        open_lots += leaves
        if ref["role"] == INITIATOR:
            initiator_risk += required_hedge_lots(s, leaves)
        if order["status"] == UNKNOWN_STATUS:
            unknown_lots += leaves
        if (order["status"] == CANCEL_PENDING_STATUS
                and ctx.now - order["updated_ts_ns"] > p["cancel_timeout_ns"]):
            unknown_lots += leaves
    gap = abs(hedge_gap(s)) if sl["id"] else 0
    worst_gap = gap + initiator_risk + unknown_lots
    p["gross_imbalance"], p["open_order_lots"], p["unknown_order_lots"] = worst_gap, open_lots, unknown_lots
    if gap > 0 and sl["first_imbalance_ts_ns"] == 0:
        sl["first_imbalance_ts_ns"] = ctx.now
    if gap == 0:
        sl["first_imbalance_ts_ns"] = 0
    desired = NORMAL
    if p["ready_mask"] != READY_ALL or worst_gap >= p["max_unhedged_soft"]:
        desired = RESTRICTED
    if (worst_gap >= p["max_unhedged_hard"]
            or p["filled_gross_notional_ticks"] >= p["max_gross_notional_ticks"]
            or (sl["first_imbalance_ts_ns"] > 0
                and ctx.now - sl["first_imbalance_ts_ns"] >= p["max_unhedged_duration_ns"])):
        desired = EMERGENCY
    if unknown_lots > 0:
        desired, p["reconcile_required"], p["status"] = HALT, 1, RECONCILING
        p["unknown_count"] += 1
    if p["posture_latched"]:
        desired = HALT
    set_posture(s, desired)
    return desired

@njit
def maker_price(ctx):
    p = ctx.state["pair"]
    left, right = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    if p["direction"] == LONG_SPREAD:
        return max(1, min(left["best_bid_ticks"], right["best_bid_ticks"] - p["spread_ticks"]))
    return max(left["best_ask_ticks"], right["best_ask_ticks"] + p["spread_ticks"])

@njit
def submit_role(ctx, role, qty, emergency):
    if qty <= 0 or free_refs(ctx.state) == 0:
        return 0
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    left, right = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    if role == INITIATOR:
        account, asset = p["left_account_no"], p["left_asset_no"]
        side = BUY if p["direction"] == LONG_SPREAD else SELL
        if p["mode"] == MAKER_TAKER:
            price, tif = maker_price(ctx), POST_ONLY
        else:
            price, tif = (left["best_ask_ticks"] if side == BUY else left["best_bid_ticks"]), IOC
    else:
        account, asset = p["right_account_no"], p["right_asset_no"]
        side = SELL if p["direction"] == LONG_SPREAD else BUY
        price, tif = (right["best_bid_ticks"] if side == SELL else right["best_ask_ticks"]), IOC
    if emergency:
        price = price + p["max_slippage_ticks"] if side == BUY else max(1, price - p["max_slippage_ticks"])
    order_id = ctx.submit_order(account, asset, side, LIMIT, qty, price, tif)
    if order_id == 0 or not bind_ref(s, order_id, role):
        fail_closed(s, 101)
        return 0
    if role == INITIATOR:
        sl["initiator_order_id"] = order_id
    else:
        sl["hedge_order_id"] = order_id
    return order_id

@njit
def cancel_role(ctx, role):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    order_id = sl["initiator_order_id"] if role == INITIATOR else sl["hedge_order_id"]
    if order_id == 0:
        return
    active = ctx.active_orders()
    index = active_index(active, order_id)
    if index < 0:
        require_reconcile(s, 201)
        return
    order = active[index]
    if order["status"] in (CANCEL_PENDING_STATUS, UNKNOWN_STATUS):
        return
    if ctx.cancel_order(order["account_no"], order["asset_no"], order_id) != 0:
        p["cancel_count"] += 1

@njit
def begin_slot(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    if (p["status"] != RUNNING or p["posture"] != NORMAL
            or ctx.now < p["next_quote_ts_ns"] or not opportunity_valid(ctx)):
        return
    target = min(p["slot_lots"], position_capacity(ctx))
    required_refs = 2 if p["mode"] == TAKER_TAKER else 1
    if target <= p["dust_lots"] or free_refs(s) < required_refs:
        if free_refs(s) < required_refs:
            set_posture(s, RESTRICTED)
            p["last_error"] = 102
        return
    sl["id"] += 1
    sl["created_ts_ns"], sl["deadline_ts_ns"] = ctx.now, ctx.now + p["slot_timeout_ns"]
    sl["first_imbalance_ts_ns"], sl["completed_ts_ns"], sl["target_lots"] = 0, 0, target
    sl["initiator_filled_lots"], sl["hedge_filled_lots"] = 0, 0
    sl["initiator_order_id"], sl["hedge_order_id"] = 0, 0
    sl["initiator_cancel_failures"], sl["hedge_cancel_failures"] = 0, 0
    sl["initiator_fill_sequence"], sl["hedge_fill_sequence"] = 0, 0
    p["slot_start_count"] += 1
    submit_role(ctx, INITIATOR, target, 0)
    if p["mode"] == TAKER_TAKER:
        submit_role(ctx, HEDGE, required_hedge_lots(s, target), 0)

@njit
def finish_slot(s, now):
    if slot_complete(s):
        s["slot"]["completed_ts_ns"] = now
        return True
    return False

@njit
def reconcile_quote(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    if sl["initiator_order_id"] == 0:
        return
    active = ctx.active_orders()
    index = active_index(active, sl["initiator_order_id"])
    if index < 0:
        require_reconcile(s, 202)
        return
    order = active[index]
    if order["status"] in (CANCEL_PENDING_STATUS, UNKNOWN_STATUS):
        return
    if order["status"] not in (PENDING_STATUS, ACCEPTED_STATUS, PARTIAL_STATUS):
        require_reconcile(s, 203)
        return
    right = ctx.market(p["right_asset_no"])
    edge = (right["best_bid_ticks"] - order["price_ticks"] if p["direction"] == LONG_SPREAD
            else order["price_ticks"] - right["best_ask_ticks"])
    if edge < p["spread_ticks"] or abs(edge - p["spread_ticks"]) > p["requote_distance_ticks"]:
        cancel_role(ctx, INITIATOR)

@njit
def drive(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    posture = risk_check(ctx)
    if p["status"] == WARMING_UP and p["ready_mask"] == READY_ALL:
        p["status"] = RUNNING
    if posture == HALT or p["status"] in (RECONCILING, ERROR, PAUSED, STOPPED):
        cancel_role(ctx, INITIATOR)
        return
    if sl["id"] and not slot_complete(s) and ctx.now >= sl["deadline_ts_ns"]:
        cancel_role(ctx, INITIATOR)
        if sl["hedge_order_id"]:
            cancel_role(ctx, HEDGE)
    if posture >= RESTRICTED or p["status"] != RUNNING:
        cancel_role(ctx, INITIATOR)
    if p["mode"] == MAKER_TAKER and posture == NORMAL and p["status"] == RUNNING:
        reconcile_quote(ctx)
    gap = hedge_gap(s) if sl["id"] else 0
    if gap < -p["dust_lots"]:
        fail_closed(s, 204)
        return
    emergency = posture == EMERGENCY
    if (sl["id"] and gap > p["dust_lots"] and sl["hedge_order_id"] == 0
            and (emergency or p["status"] == DRAINING
                 or sl["initiator_filled_lots"] >= sl["target_lots"])):
        submit_role(ctx, HEDGE, gap, 1 if emergency else 0)
    if slot_complete(s):
        finish_slot(s, ctx.now)
        if p["status"] == RUNNING and posture == NORMAL and positions_caught_up(ctx):
            begin_slot(ctx)
        elif p["status"] == DRAINING:
            p["status"] = STOPPED
        return
    if sl["id"] == 0:
        begin_slot(ctx)
    elif (p["status"] == RUNNING and posture == NORMAL and sl["initiator_order_id"] == 0
          and sl["initiator_filled_lots"] < sl["target_lots"] and opportunity_valid(ctx)):
        submit_role(ctx, INITIATOR, sl["target_lots"] - sl["initiator_filled_lots"], 0)

@njit
def on_start(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    left_account, right_account = ctx.account(p["left_account_no"]), ctx.account(p["right_account_no"])
    if (left_account["account_no"] != p["left_account_no"]
            or right_account["account_no"] != p["right_account_no"]
            or left_account["state"] != ACCOUNT_READY or right_account["state"] != ACCOUNT_READY):
        fail_closed(s, 301)
        return
    if ((p["left_account_epoch"] and p["left_account_epoch"] != left_account["account_epoch"])
            or (p["right_account_epoch"] and p["right_account_epoch"] != right_account["account_epoch"])):
        fail_closed(s, 302)
        return
    p["left_account_epoch"], p["right_account_epoch"] = left_account["account_epoch"], right_account["account_epoch"]
    p["left_account_sequence"] = left_account["account_sequence"]
    p["right_account_sequence"] = right_account["account_sequence"]
    left_position = ctx.position(p["left_account_no"], p["left_asset_no"])
    right_position = ctx.position(p["right_account_no"], p["right_asset_no"])
    if (left_position["account_no"] != p["left_account_no"]
            or left_position["asset_no"] != p["left_asset_no"]
            or right_position["account_no"] != p["right_account_no"]
            or right_position["asset_no"] != p["right_asset_no"]):
        fail_closed(s, 303)
        return
    active, restored = ctx.active_orders(), sl["id"] != 0
    if not restored and (left_position["qty_lots"] != 0 or right_position["qty_lots"] != 0 or len(active) != 0):
        fail_closed(s, 304)
        return
    if restored and (left_position["qty_lots"] != p["expected_left_position_lots"]
                     or right_position["qty_lots"] != p["expected_right_position_lots"]):
        fail_closed(s, 307)
        return
    for i in range(len(active)):
        ref_index = find_ref(s, active[i]["order_id"])
        if ref_index < 0 or not restored_order_valid(s, active[i], s["order_refs"][ref_index]):
            fail_closed(s, 305)
            return
    for i in range(MAX_ORDER_REFS):
        ref = s["order_refs"][i]
        if ref["order_id"] and (ref["slot_id"] != sl["id"]
                                or active_index(active, ref["order_id"]) < 0):
            fail_closed(s, 306)
            return
    p["ready_mask"] |= READY_ACCOUNTS | READY_POSITIONS | READY_RECONCILED
    p["reconcile_required"] = 0
    if p["status"] in (CREATED, WARMING_UP, RUNNING):
        p["status"] = WARMING_UP

@njit
def on_tick(ctx):
    p = ctx.state["pair"]
    for tick in ctx.ticks():
        if tick["asset_no"] == p["left_asset_no"]:
            p["left_market_ts_ns"] = tick["receive_ts_ns"]
        elif tick["asset_no"] == p["right_asset_no"]:
            p["right_market_ts_ns"] = tick["receive_ts_ns"]
    drive(ctx)

@njit
def on_timer(ctx):
    drive(ctx)

@njit
def on_fill(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    for fill in ctx.fills():
        index = find_ref(s, fill["order_id"])
        if index < 0:
            require_reconcile(s, 401)
            return
        ref = s["order_refs"][index]
        if ref["slot_id"] != sl["id"] or fill["fill_qty_lots"] <= 0:
            require_reconcile(s, 402)
            return
        role, delta = ref["role"], fill["fill_qty_lots"]
        p["filled_gross_notional_ticks"] += abs(delta * fill["fill_price_ticks"])
        p["fill_count"] += 1
        if role == INITIATOR:
            sl["initiator_filled_lots"] += delta
            p["expected_left_position_lots"] += delta if p["direction"] == LONG_SPREAD else -delta
            sl["initiator_fill_sequence"] = max(sl["initiator_fill_sequence"], fill["account_sequence"])
            if sl["initiator_filled_lots"] > sl["target_lots"]:
                fail_closed(s, 403)
                return
            if fill["final_fill"] or sl["initiator_filled_lots"] >= sl["target_lots"]:
                clear_ref(s, fill["order_id"])
                sl["initiator_order_id"] = 0
                gap = hedge_gap(s)
                if gap > p["dust_lots"] and sl["hedge_order_id"] == 0:
                    submit_role(ctx, HEDGE, gap, 0)
        elif role == HEDGE:
            sl["hedge_filled_lots"] += delta
            p["expected_right_position_lots"] += -delta if p["direction"] == LONG_SPREAD else delta
            sl["hedge_fill_sequence"] = max(sl["hedge_fill_sequence"], fill["account_sequence"])
            if hedge_gap(s) < -p["dust_lots"]:
                fail_closed(s, 404)
                return
            if fill["final_fill"] or hedge_gap(s) <= p["dust_lots"]:
                clear_ref(s, fill["order_id"])
                sl["hedge_order_id"] = 0
        else:
            fail_closed(s, 405)
            return
    finish_slot(s, ctx.now)
    ensure_draining_hedge(ctx)

@njit
def on_order(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    for event in ctx.order_events():
        index = find_ref(s, event["order_id"])
        if index < 0:
            if event["status"] in (FILLED_STATUS, CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS):
                continue
            require_reconcile(s, 501)
            return
        ref = s["order_refs"][index]
        if ref["slot_id"] != sl["id"]:
            require_reconcile(s, 502)
            return
        if event["status"] == UNKNOWN_STATUS:
            require_reconcile(s, 503)
            return
        if event["status"] == REJECTED_STATUS:
            role = ref["role"]
            p["reject_count"] += 1
            clear_ref(s, event["order_id"])
            if role == INITIATOR:
                sl["initiator_order_id"] = 0
            else:
                sl["hedge_order_id"] = 0
            p["next_quote_ts_ns"] = ctx.now + p["quote_cooldown_ns"]
    ensure_draining_hedge(ctx)

@njit
def on_cancel(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    for event in ctx.cancel_events():
        index = find_ref(s, event["order_id"])
        if index < 0:
            if event["final_status"] in (CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS):
                continue
            require_reconcile(s, 601)
            return
        ref = s["order_refs"][index]
        role = ref["role"]
        if ref["slot_id"] != sl["id"]:
            require_reconcile(s, 602)
            return
        if event["request_result"] == 1:
            if role == INITIATOR:
                sl["initiator_cancel_failures"] += 1
                failures = sl["initiator_cancel_failures"]
            else:
                sl["hedge_cancel_failures"] += 1
                failures = sl["hedge_cancel_failures"]
            if failures >= p["cancel_retry_limit"]:
                require_reconcile(s, 603)
            continue
        if event["request_result"] == 2:
            require_reconcile(s, 604)
            return
        if event["final_status"] == FILLED_STATUS:
            continue
        if event["final_status"] in (CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS):
            if event["final_status"] == REJECTED_STATUS:
                p["reject_count"] += 1
            clear_ref(s, event["order_id"])
            if role == INITIATOR:
                sl["initiator_order_id"] = 0
            else:
                sl["hedge_order_id"] = 0
            p["next_quote_ts_ns"] = ctx.now + p["quote_cooldown_ns"]
    finish_slot(s, ctx.now)
    ensure_draining_hedge(ctx)

@njit
def on_position(ctx):
    for _event in ctx.position_events():
        pass
    risk_check(ctx)

@njit
def on_balance(ctx):
    for _event in ctx.balance_events():
        pass

@njit
def on_account_state(ctx):
    s, p = ctx.state, ctx.state["pair"]
    for event in ctx.account_state_events():
        if event["account_no"] == p["left_account_no"]:
            if p["left_account_epoch"] and event["account_epoch"] != p["left_account_epoch"]:
                require_reconcile(s, 701)
                return
            p["left_account_sequence"] = event["account_sequence"]
        elif event["account_no"] == p["right_account_no"]:
            if p["right_account_epoch"] and event["account_epoch"] != p["right_account_epoch"]:
                require_reconcile(s, 702)
                return
            p["right_account_sequence"] = event["account_sequence"]
        if (event["account_no"] in (p["left_account_no"], p["right_account_no"])
                and event["state"] != ACCOUNT_READY):
            p["ready_mask"] &= 0xFD
            p["reconcile_required"], p["status"] = 1, RECONCILING
            set_posture(s, RESTRICTED)
            return
    risk_check(ctx)

@njit
def on_stop(ctx):
    p = ctx.state["pair"]
    p["drain_requested"], p["status"] = 1, DRAINING
    cancel_role(ctx, INITIATOR)
    p["ready_mask"] = 0
    ensure_draining_hedge(ctx)

def build(parameters):
    if parameters["left_asset_no"] == parameters["right_asset_no"]:
        raise ValueError("asset numbers must be distinct")
    if parameters["slot_lots"] > parameters["max_position_lots"]:
        raise ValueError("slot_lots exceeds max_position_lots")
    if parameters["max_unhedged_lots_hard"] < parameters["max_unhedged_lots_soft"]:
        raise ValueError("hard unhedged limit is below soft limit")
    state = new_state(state_dtype)
    p = state[0]["pair"]
    p["status"], p["posture"] = CREATED, NORMAL
    p["mode"] = MAKER_TAKER if parameters["mode"] == "MAKER_TAKER" else TAKER_TAKER
    p["direction"] = LONG_SPREAD if parameters["direction"] == "LONG_SPREAD" else SHORT_SPREAD
    for name in ("left_asset_no", "right_asset_no", "left_account_no", "right_account_no",
                 "spread_ticks", "requote_distance_ticks", "slot_lots", "max_position_lots",
                 "dust_lots", "start_time_ns", "cancel_timeout_ns", "slot_timeout_ns",
                 "max_unhedged_duration_ns", "cancel_retry_limit", "max_slippage_ticks",
                 "quote_cooldown_ns", "market_stale_after_ns"):
        p[name] = parameters[name]
    p["hedge_ratio_num"] = parameters["hedge_ratio_numerator"]
    p["hedge_ratio_den"] = parameters["hedge_ratio_denominator"]
    p["max_unhedged_soft"] = parameters["max_unhedged_lots_soft"]
    p["max_unhedged_hard"] = parameters["max_unhedged_lots_hard"]
    p["max_gross_notional_ticks"] = parameters["max_gross_notional_ticks"]
    handlers = {"on_start": on_start, "on_tick": on_tick, "on_fill": on_fill,
                "on_order": on_order, "on_cancel": on_cancel, "on_position": on_position,
                "on_balance": on_balance, "on_account_state": on_account_state,
                "on_timer": on_timer, "on_stop": on_stop}
    return StrategyDefinition(SPEC, state, handlers, {"description": "Slot-based pair arbitrage V3"})
