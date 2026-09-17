"""Typed-state ABI V13 pair-arbitrage state machine."""

from numba import njit
import numpy as np

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.parameters import EnumParam, IntParam
from titan_strategy.state import array, int64, new_state, record, uint8, uint32, uint64
from titan_strategy.types import EventKind, EventQos

MAX_ORDERS = 4
MAX_OBLIGATIONS = 16
HISTORY_SIZE = 64
MAKER_TAKER, TAKER_TAKER = 1, 2
LONG_SPREAD, SHORT_SPREAD = 1, 2
CREATED, WARMING_UP, QUOTING, HEDGE_ONLY = 1, 2, 3, 4
DRAINING, RECONCILING, PAUSED, STOPPED, FAULTED = 5, 6, 7, 8, 9
NORMAL, RESTRICTED, EMERGENCY, HALT = 0, 1, 2, 3
INITIATOR, HEDGE = 1, 2
EMPTY, REQUESTING, UNKNOWN, WORKING, PARTIAL, FILLED = 0, 1, 2, 3, 4, 5
CANCEL_PENDING, CANCELED, REJECTED, EXPIRED = 6, 7, 8, 9
BUY, SELL, LIMIT, MARKET, GTC, IOC, POST_ONLY = 1, 2, 1, 2, 1, 2, 4
ACCEPTED_STATUS, PARTIAL_STATUS, FILLED_STATUS = 2, 3, 4
CANCEL_PENDING_STATUS, CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS = 5, 6, 7, 8
ACCOUNT_READY = 4
READY_MARKETS, READY_ACCOUNTS, READY_POSITIONS, READY_RECONCILED = 1, 2, 4, 8
READY_ALL = READY_MARKETS | READY_ACCOUNTS | READY_POSITIONS | READY_RECONCILED
OBLIGATION_EMPTY, OBLIGATION_OPEN, OBLIGATION_DONE = 0, 1, 2

pair_dtype = record(
    status=uint8, posture=uint8, posture_latched=uint8, ready_mask=uint8,
    reconcile_required=uint8, mode=uint8, direction=uint8, drain_requested=uint8,
    left_asset_no=uint32, right_asset_no=uint32, left_account_no=uint32,
    right_account_no=uint32, hedge_ratio_num=int64, hedge_ratio_den=int64, spread_ticks=int64,
    requote_distance_ticks=int64, slot_lots=int64, max_position_lots=int64,
    dust_lots=int64, cancel_timeout_ns=int64, slot_timeout_ns=int64,
    max_unhedged_duration_ns=int64, max_unhedged_soft=int64, max_unhedged_hard=int64,
    max_gross_notional_ticks=int64, max_slippage_ticks=int64, quote_cooldown_ns=int64,
    market_stale_after_ns=int64, cancel_retry_limit=uint32, next_slot_id=uint64,
    total_initiator_filled=int64, total_hedge_filled=int64,
    completed_gross_imbalance=int64, gross_imbalance=int64, open_order_lots=int64,
    filled_gross_notional_ticks=int64, left_position_lots=int64, right_position_lots=int64,
    fill_count=uint64, cancel_count=uint64, reject_count=uint64, unknown_count=uint64,
    slot_start_count=uint64, posture_change_count=uint64, last_error=uint32,
    next_obligation_id=uint64, left_market_ts_ns=int64, right_market_ts_ns=int64,
    next_quote_ts_ns=int64, left_account_epoch=uint64, right_account_epoch=uint64,
    left_account_sequence=uint64, right_account_sequence=uint64,
)
slot_dtype = record(
    id=uint64, created_ts_ns=int64, deadline_ts_ns=int64, first_imbalance_ts_ns=int64,
    completed_ts_ns=int64, target_lots=int64, initiator_filled_lots=int64,
    hedge_filled_lots=int64, initiator_order_id=uint64, hedge_order_id=uint64,
)
order_dtype = record(
    order_id=uint64, slot_id=uint64, role=uint8, status=uint8,
    status_before_cancel=uint8, side=uint8, asset_no=uint32, account_no=uint32,
    price_ticks=int64, qty_lots=int64, filled_lots=int64, submit_ts_ns=int64,
    cancel_ts_ns=int64, last_event_ts_ns=int64, last_sequence=uint64,
    cancel_failures=uint32,
)
history_dtype = record(
    order_id=uint64, slot_id=uint64, role=uint8, status=uint8, side=uint8,
    reserved=uint8, filled_lots=int64, last_sequence=uint64,
)
obligation_dtype = record(
    id=uint64, slot_id=uint64, source_order_id=uint64, status=uint8,
    reserved=array(uint8, 7), required_lots=int64, completed_lots=int64,
    created_ts_ns=int64, updated_ts_ns=int64, retry_count=uint32,
)
state_dtype = record(
    pair=pair_dtype, slot=slot_dtype, orders=array(order_dtype, MAX_ORDERS),
    obligations=array(obligation_dtype, MAX_OBLIGATIONS),
    history=array(history_dtype, HISTORY_SIZE), history_cursor=uint32, history_count=uint32,
)

SPEC = StrategySpec(
    strategy_id="pair_arb", strategy_version="2.0.0", state_schema_version=1,
    parameters=(
        IntParam("left_asset_no", required=False, default=0, minimum=0),
        IntParam("right_asset_no", required=False, default=1, minimum=0),
        IntParam("left_account_no", required=False, default=0, minimum=0),
        IntParam("right_account_no", required=False, default=1, minimum=0),
        EnumParam("direction", required=False, default="LONG_SPREAD", values=("LONG_SPREAD", "SHORT_SPREAD")),
        EnumParam("mode", required=False, default="MAKER_TAKER", values=("MAKER_TAKER", "TAKER_TAKER")),
        IntParam("hedge_ratio_numerator", required=False, default=1, minimum=1),
        IntParam("hedge_ratio_denominator", required=False, default=1, minimum=1),
        IntParam("max_position_lots", minimum=1), IntParam("slot_lots", minimum=1),
        IntParam("spread_ticks", required=False, default=0, minimum=0),
        IntParam("requote_distance_ticks", required=False, default=1, minimum=0),
        IntParam("dust_lots", required=False, default=0, minimum=0),
        IntParam("cancel_timeout_ns", required=False, default=1_000_000_000, minimum=1),
        IntParam("slot_timeout_ns", minimum=1),
        IntParam("max_unhedged_duration_ns", required=False, default=5_000_000_000, minimum=1),
        IntParam("max_unhedged_lots_soft", required=False, default=10, minimum=1),
        IntParam("max_unhedged_lots_hard", required=False, default=20, minimum=1),
        IntParam("max_gross_notional_ticks", required=False, default=9_000_000_000_000_000_000, minimum=1),
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
    capabilities=Capability.MARKET_DATA | Capability.ACCOUNT_DATA | Capability.ORDER_EXECUTION | Capability.TIMER,
)


@njit
def find_order(s, order_id):
    for i in range(MAX_ORDERS):
        if s["orders"][i]["order_id"] == order_id:
            return i
    return -1


@njit
def history_order(s, order_id):
    for i in range(int(s["history_count"])):
        if s["history"][i]["order_id"] == order_id:
            return np.int64(i)
    return np.int64(-1)


@njit
def halt(s, code):
    p = s["pair"]
    if p["posture"] != HALT:
        p["posture_change_count"] += 1
    p["posture"], p["posture_latched"] = HALT, 1
    p["reconcile_required"], p["last_error"], p["status"] = 1, code, FAULTED


@njit
def archive(s, i, status):
    o, h = s["orders"][i], s["history"][s["history_cursor"]]
    h["order_id"], h["slot_id"], h["role"], h["status"] = o["order_id"], o["slot_id"], o["role"], status
    h["side"], h["filled_lots"], h["last_sequence"] = o["side"], o["filled_lots"], o["last_sequence"]
    s["history_cursor"] = (s["history_cursor"] + 1) % HISTORY_SIZE
    s["history_count"] = min(HISTORY_SIZE, s["history_count"] + 1)
    o["order_id"], o["status"] = 0, EMPTY


@njit
def hedge_lots_for(s, initiator_lots):
    p = s["pair"]
    numerator = initiator_lots * p["hedge_ratio_num"]
    return (numerator + p["hedge_ratio_den"] - 1) // p["hedge_ratio_den"]


@njit
def hedge_debt(s):
    debt = 0
    for i in range(MAX_OBLIGATIONS):
        item = s["obligations"][i]
        if item["status"] == OBLIGATION_OPEN:
            debt += max(0, item["required_lots"] - item["completed_lots"])
    return debt


@njit
def create_obligation(s, source_order_id, initiator_delta, now):
    required = hedge_lots_for(s, initiator_delta)
    if required <= 0:
        return True
    for i in range(MAX_OBLIGATIONS):
        item = s["obligations"][i]
        if item["status"] in (OBLIGATION_EMPTY, OBLIGATION_DONE):
            item["id"] = s["pair"]["next_obligation_id"]
            s["pair"]["next_obligation_id"] += 1
            item["slot_id"], item["source_order_id"] = s["slot"]["id"], source_order_id
            item["status"], item["required_lots"], item["completed_lots"] = OBLIGATION_OPEN, required, 0
            item["created_ts_ns"], item["updated_ts_ns"], item["retry_count"] = now, now, 0
            return True
    halt(s, 102)
    return False


@njit
def apply_hedge_fill(s, hedge_delta, now):
    remaining = hedge_delta
    for i in range(MAX_OBLIGATIONS):
        item = s["obligations"][i]
        if remaining <= 0:
            break
        if item["status"] != OBLIGATION_OPEN:
            continue
        debt = item["required_lots"] - item["completed_lots"]
        applied = min(remaining, debt)
        item["completed_lots"] += applied
        item["updated_ts_ns"] = now
        remaining -= applied
        if item["completed_lots"] >= item["required_lots"]:
            item["status"] = OBLIGATION_DONE
    if remaining > 0:
        halt(s, 404)


@njit
def imbalance(s):
    return hedge_debt(s)


@njit
def metrics(s, now):
    p, sl = s["pair"], s["slot"]
    gap = hedge_debt(s)
    p["gross_imbalance"] = p["completed_gross_imbalance"] + abs(gap)
    p["open_order_lots"] = 0
    for i in range(MAX_ORDERS):
        o = s["orders"][i]
        if o["order_id"]:
            p["open_order_lots"] += max(0, o["qty_lots"] - o["filled_lots"])
    if gap and sl["first_imbalance_ts_ns"] == 0:
        sl["first_imbalance_ts_ns"] = now
    if not gap:
        sl["first_imbalance_ts_ns"] = 0
    desired = NORMAL
    if p["gross_imbalance"] >= p["max_unhedged_soft"]:
        desired = RESTRICTED
    if (p["gross_imbalance"] >= p["max_unhedged_hard"] or
            p["filled_gross_notional_ticks"] >= p["max_gross_notional_ticks"] or
            (sl["first_imbalance_ts_ns"] and now - sl["first_imbalance_ts_ns"] >= p["max_unhedged_duration_ns"])):
        desired = EMERGENCY
    if not p["posture_latched"] and desired != p["posture"]:
        p["posture"], p["posture_change_count"] = desired, p["posture_change_count"] + 1


@njit
def bind_order(s, order_id, role, account, asset, side, price, qty, now):
    for i in range(MAX_ORDERS):
        o = s["orders"][i]
        if o["order_id"] == 0:
            o["order_id"], o["slot_id"], o["role"], o["status"] = order_id, s["slot"]["id"], role, REQUESTING
            o["account_no"], o["asset_no"], o["side"] = account, asset, side
            o["price_ticks"], o["qty_lots"], o["filled_lots"], o["submit_ts_ns"] = price, qty, 0, now
            return i
    halt(s, 101)
    return -1


@njit
def submit_leg(ctx, role, qty, emergency):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    left, right = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    if role == INITIATOR:
        account, asset = p["left_account_no"], p["left_asset_no"]
        side = BUY if p["direction"] == LONG_SPREAD else SELL
        price = (left["best_ask_ticks"] if side == BUY else left["best_bid_ticks"]) if p["mode"] == TAKER_TAKER else (left["best_bid_ticks"] if side == BUY else left["best_ask_ticks"])
        tif = IOC if p["mode"] == TAKER_TAKER else POST_ONLY
    else:
        account, asset = p["right_account_no"], p["right_asset_no"]
        side = SELL if p["direction"] == LONG_SPREAD else BUY
        price, tif = (right["best_bid_ticks"] if side == SELL else right["best_ask_ticks"]), IOC
    kind = LIMIT
    if emergency:
        if side == BUY:
            price += p["max_slippage_ticks"]
        else:
            price = max(1, price - p["max_slippage_ticks"])
    order_id = ctx.submit_order(account, asset, side, kind, qty, price, tif)
    if bind_order(s, order_id, role, account, asset, side, price, qty, ctx.now) >= 0:
        if role == INITIATOR:
            sl["initiator_order_id"] = order_id
        else:
            sl["hedge_order_id"] = order_id


@njit
def valid_market(ctx):
    p = ctx.state["pair"]
    l, r = ctx.market(p["left_asset_no"]), ctx.market(p["right_asset_no"])
    if l["best_bid_ticks"] <= 0 or l["best_ask_ticks"] <= l["best_bid_ticks"] or r["best_bid_ticks"] <= 0 or r["best_ask_ticks"] <= r["best_bid_ticks"]:
        return False
    if (ctx.now - p["left_market_ts_ns"] > p["market_stale_after_ns"] or
            ctx.now - p["right_market_ts_ns"] > p["market_stale_after_ns"]):
        return False
    return (r["best_bid_ticks"] - l["best_ask_ticks"] >= p["spread_ticks"] if p["direction"] == LONG_SPREAD else l["best_bid_ticks"] - r["best_ask_ticks"] >= p["spread_ticks"])


@njit
def finish_slot(s, now):
    sl, p = s["slot"], s["pair"]
    if (sl["id"] and sl["initiator_filled_lots"] >= sl["target_lots"] and
            not sl["initiator_order_id"] and not sl["hedge_order_id"] and
            hedge_debt(s) <= p["dust_lots"]):
        p["completed_gross_imbalance"] += hedge_debt(s)
        sl["id"], sl["completed_ts_ns"] = 0, now
        sl["initiator_filled_lots"], sl["hedge_filled_lots"] = 0, 0
        return True
    return False


@njit
def start_slot(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    capacity = p["max_position_lots"] - max(abs(p["left_position_lots"]), abs(p["right_position_lots"]))
    free = 0
    for i in range(MAX_ORDERS):
        free += 1 if not s["orders"][i]["order_id"] else 0
    required = 1
    if (sl["id"] or p["status"] != QUOTING or p["posture"] != NORMAL or
            ctx.now < p["next_quote_ts_ns"] or capacity <= p["dust_lots"] or
            free < required or not valid_market(ctx)):
        return
    target = min(p["slot_lots"], capacity)
    sl["id"], sl["created_ts_ns"] = p["next_slot_id"], ctx.now
    p["next_slot_id"], p["slot_start_count"] = p["next_slot_id"] + 1, p["slot_start_count"] + 1
    sl["deadline_ts_ns"], sl["target_lots"] = ctx.now + p["slot_timeout_ns"], target
    submit_leg(ctx, INITIATOR, target, 0)


@njit
def cancel_ref(ctx, i):
    o = ctx.state["orders"][i]
    if o["order_id"] and o["status"] not in (CANCEL_PENDING, UNKNOWN):
        o["status_before_cancel"] = o["status"]
        if ctx.cancel_order(o["account_no"], o["asset_no"], o["order_id"]):
            o["status"], o["cancel_ts_ns"] = CANCEL_PENDING, ctx.now
            ctx.state["pair"]["cancel_count"] += 1


@njit
def drive(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    if valid_market(ctx):
        p["ready_mask"] |= READY_MARKETS
    else:
        p["ready_mask"] &= ~READY_MARKETS
    fully_ready = p["ready_mask"] == READY_ALL
    if p["status"] == WARMING_UP and fully_ready:
        p["status"] = QUOTING
    for i in range(MAX_ORDERS):
        o = s["orders"][i]
        if o["order_id"] and ((o["status"] == REQUESTING and ctx.now - o["submit_ts_ns"] >= p["cancel_timeout_ns"]) or (o["status"] == CANCEL_PENDING and ctx.now - o["cancel_ts_ns"] >= p["cancel_timeout_ns"])):
            o["status"], p["unknown_count"] = UNKNOWN, p["unknown_count"] + 1
            halt(s, 201)
    if sl["id"] and ctx.now >= sl["deadline_ts_ns"]:
        for i in range(MAX_ORDERS):
            cancel_ref(ctx, i)
    metrics(s, ctx.now)
    if p["posture"] != NORMAL or not fully_ready or p["status"] != QUOTING:
        for i in range(MAX_ORDERS):
            if s["orders"][i]["role"] == INITIATOR:
                cancel_ref(ctx, i)
    if (p["mode"] == MAKER_TAKER and p["posture"] == NORMAL and fully_ready and
            p["status"] == QUOTING and
            sl["initiator_order_id"]):
        i = find_order(s, sl["initiator_order_id"])
        if i >= 0:
            o = s["orders"][i]
            if o["status"] in (WORKING, PARTIAL):
                left = ctx.market(p["left_asset_no"])
                target_price = (left["best_bid_ticks"] if o["side"] == BUY
                                else left["best_ask_ticks"])
                if abs(target_price - o["price_ticks"]) > p["requote_distance_ticks"]:
                    cancel_ref(ctx, i)
    if p["posture"] == EMERGENCY:
        p["status"] = HEDGE_ONLY
    debt = hedge_debt(s)
    if p["status"] in (HEDGE_ONLY, DRAINING) and debt > p["dust_lots"] and not sl["hedge_order_id"]:
        submit_leg(ctx, HEDGE, debt, 1)
    if p["posture"] != HALT:
        finish_slot(s, ctx.now)
        if sl["id"] and p["posture"] == NORMAL and p["status"] == QUOTING:
            gap = hedge_debt(s)
            if gap > p["dust_lots"] and not sl["hedge_order_id"]:
                submit_leg(ctx, HEDGE, gap, 0)
            elif (abs(gap) <= p["dust_lots"] and not sl["initiator_order_id"] and
                    sl["initiator_filled_lots"] < sl["target_lots"] and valid_market(ctx)):
                submit_leg(ctx, INITIATOR, sl["target_lots"] - sl["initiator_filled_lots"], 0)
        if not sl["id"] and p["posture"] == NORMAL and p["status"] == QUOTING:
            start_slot(ctx)
    if p["status"] == DRAINING:
        any_order = False
        for i in range(MAX_ORDERS):
            if s["orders"][i]["order_id"]:
                any_order = True
        if not any_order and hedge_debt(s) <= p["dust_lots"]:
            p["status"] = STOPPED


@njit
def on_start(ctx):
    s, p = ctx.state, ctx.state["pair"]
    active = ctx.active_orders()
    for i in range(len(active)):
        a = active[i]
        if a["account_no"] in (p["left_account_no"], p["right_account_no"]) and find_order(s, a["order_id"]) < 0:
            halt(s, 301)
            return
    for i in range(MAX_ORDERS):
        o = s["orders"][i]
        if not o["order_id"]:
            continue
        matches = 0
        for j in range(len(active)):
            a = active[j]
            if a["order_id"] == o["order_id"] and a["account_no"] == o["account_no"]:
                if (a["asset_no"] != o["asset_no"] or a["side"] != o["side"] or
                        a["price_ticks"] != o["price_ticks"] or a["qty_lots"] != o["qty_lots"] or
                        a["cumulative_filled_lots"] != o["filled_lots"]):
                    halt(s, 302)
                    return
                matches += 1
        if matches != 1:
            halt(s, 303)
            return
    # The runtime opens the command gate only after account/order/position snapshots are committed.
    # on_start performs the strategy-private half of that reconciliation.
    p["ready_mask"] |= READY_ACCOUNTS | READY_POSITIONS | READY_RECONCILED
    p["reconcile_required"], p["status"] = 0, WARMING_UP


@njit
def on_tick(ctx):
    p = ctx.state["pair"]
    for item in ctx.ticks():
        if item["asset_no"] == p["left_asset_no"]:
            p["left_market_ts_ns"] = item["receive_ts_ns"]
        elif item["asset_no"] == p["right_asset_no"]:
            p["right_market_ts_ns"] = item["receive_ts_ns"]
    drive(ctx)


@njit
def on_timer(ctx):
    drive(ctx)


@njit
def on_fill(ctx):
    s, p, sl = ctx.state, ctx.state["pair"], ctx.state["slot"]
    for f in ctx.fills():
        i = find_order(s, f["order_id"])
        if i < 0:
            h = history_order(s, f["order_id"])
            if h >= 0 and f["cumulative_filled_lots"] <= s["history"][h]["filled_lots"]:
                continue
            halt(s, 401); return
        o = s["orders"][i]
        if f["asset_no"] != o["asset_no"] or f["account_no"] != o["account_no"] or f["side"] != o["side"] or f["cumulative_filled_lots"] < o["filled_lots"] or f["cumulative_filled_lots"] > o["qty_lots"]:
            halt(s, 402); return
        delta = f["cumulative_filled_lots"] - o["filled_lots"]
        if delta == 0 and f["cumulative_filled_lots"] == o["filled_lots"]:
            continue
        if delta != f["fill_qty_lots"] or delta <= 0:
            halt(s, 403); return
        o["filled_lots"], o["last_sequence"] = f["cumulative_filled_lots"], f["account_sequence"]
        o["status"] = FILLED if o["filled_lots"] == o["qty_lots"] else PARTIAL
        p["filled_gross_notional_ticks"] += abs(delta * f["fill_price_ticks"]); p["fill_count"] += 1
        if o["role"] == INITIATOR:
            sl["initiator_filled_lots"] += delta; p["total_initiator_filled"] += delta
            if not create_obligation(s, o["order_id"], delta, ctx.now):
                return
        else:
            sl["hedge_filled_lots"] += delta; p["total_hedge_filled"] += delta
            apply_hedge_fill(s, delta, ctx.now)
            if p["status"] == FAULTED:
                return
        if o["filled_lots"] == o["qty_lots"] or f["final_fill"]:
            oid, role = o["order_id"], o["role"]; archive(s, i, FILLED)
            if role == INITIATOR and sl["initiator_order_id"] == oid: sl["initiator_order_id"] = 0
            if role == HEDGE and sl["hedge_order_id"] == oid: sl["hedge_order_id"] = 0
        gap = hedge_debt(s)
        if p["mode"] == MAKER_TAKER and gap > p["dust_lots"] and not sl["hedge_order_id"]:
            submit_leg(ctx, HEDGE, gap, 0)
        metrics(s, ctx.now); finish_slot(s, ctx.now)


@njit
def on_order(ctx):
    s, sl = ctx.state, ctx.state["slot"]
    for e in ctx.order_events():
        i = find_order(s, e["order_id"])
        if i < 0:
            if history_order(s, e["order_id"]) >= 0: continue
            halt(s, 501); return
        o = s["orders"][i]
        if e["asset_no"] != o["asset_no"] or e["account_no"] != o["account_no"] or e["cumulative_filled_lots"] < o["filled_lots"]:
            halt(s, 502); return
        o["last_sequence"], o["last_event_ts_ns"] = e["account_sequence"], e["event_ts_ns"]
        status = e["status"]
        if status == ACCEPTED_STATUS: o["status"] = WORKING
        elif status == PARTIAL_STATUS: o["status"] = PARTIAL
        elif status == CANCEL_PENDING_STATUS: o["status_before_cancel"], o["status"], o["cancel_ts_ns"] = o["status"], CANCEL_PENDING, ctx.now
        elif status in (CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS):
            oid, role = o["order_id"], o["role"]
            terminal = CANCELED if status == CANCELED_STATUS else (REJECTED if status == REJECTED_STATUS else EXPIRED)
            if terminal == REJECTED: s["pair"]["reject_count"] += 1
            archive(s, i, terminal)
            if role == INITIATOR and sl["initiator_order_id"] == oid: sl["initiator_order_id"] = 0
            if role == HEDGE and sl["hedge_order_id"] == oid: sl["hedge_order_id"] = 0
            s["pair"]["next_quote_ts_ns"] = ctx.now + s["pair"]["quote_cooldown_ns"]
        elif status == FILLED_STATUS:
            o["status"] = FILLED
        finish_slot(s, ctx.now)


@njit
def on_cancel(ctx):
    s, sl = ctx.state, ctx.state["slot"]
    for e in ctx.cancel_events():
        i = find_order(s, e["order_id"])
        if i < 0:
            if history_order(s, e["order_id"]) >= 0: continue
            halt(s, 601); return
        o = s["orders"][i]
        if e["asset_no"] != o["asset_no"] or e["account_no"] != o["account_no"]:
            halt(s, 602); return
        if e["request_result"]:
            o["cancel_failures"], o["status"] = o["cancel_failures"] + 1, o["status_before_cancel"]
            if o["cancel_failures"] > s["pair"]["cancel_retry_limit"]: halt(s, 603)
        elif e["final_status"] in (CANCELED_STATUS, REJECTED_STATUS, EXPIRED_STATUS):
            oid, role = o["order_id"], o["role"]; archive(s, i, CANCELED)
            if role == INITIATOR and sl["initiator_order_id"] == oid: sl["initiator_order_id"] = 0
            if role == HEDGE and sl["hedge_order_id"] == oid: sl["hedge_order_id"] = 0
        else: o["status"] = o["status_before_cancel"]
        finish_slot(s, ctx.now)


@njit
def on_position(ctx):
    p = ctx.state["pair"]
    for e in ctx.position_events():
        if e["account_no"] == p["left_account_no"] and e["asset_no"] == p["left_asset_no"]: p["left_position_lots"] = e["qty_lots"]
        elif e["account_no"] == p["right_account_no"] and e["asset_no"] == p["right_asset_no"]: p["right_position_lots"] = e["qty_lots"]
    if abs(p["left_position_lots"]) > p["max_position_lots"] or abs(p["right_position_lots"]) > p["max_position_lots"]: halt(ctx.state, 701)
    else: p["ready_mask"] |= READY_POSITIONS


@njit
def on_balance(ctx):
    # Balance facts are consumed to keep the reliable account stream complete. Account-level
    # buying-power limits remain authoritative in the execution/risk service.
    for _event in ctx.balance_events():
        pass


@njit
def on_account_state(ctx):
    p = ctx.state["pair"]
    for e in ctx.account_state_events():
        if e["account_no"] == p["left_account_no"]:
            if p["left_account_epoch"] and e["account_epoch"] != p["left_account_epoch"]:
                halt(ctx.state, 802); return
            p["left_account_epoch"], p["left_account_sequence"] = e["account_epoch"], e["account_sequence"]
        elif e["account_no"] == p["right_account_no"]:
            if p["right_account_epoch"] and e["account_epoch"] != p["right_account_epoch"]:
                halt(ctx.state, 803); return
            p["right_account_epoch"], p["right_account_sequence"] = e["account_epoch"], e["account_sequence"]
        if e["account_no"] in (p["left_account_no"], p["right_account_no"]) and e["state"] != ACCOUNT_READY:
            p["ready_mask"] &= 0xFD
            p["status"] = HEDGE_ONLY if hedge_debt(ctx.state) > p["dust_lots"] else RECONCILING
            p["reconcile_required"] = 1
            return
    p["ready_mask"] |= READY_ACCOUNTS


@njit
def on_stop(ctx):
    ctx.state["pair"]["status"] = DRAINING
    for i in range(MAX_ORDERS): cancel_ref(ctx, i)
    ctx.state["pair"]["ready_mask"] = 0


def build(parameters):
    if parameters["left_asset_no"] == parameters["right_asset_no"]: raise ValueError("asset numbers must be distinct")
    if parameters["slot_lots"] > parameters["max_position_lots"]: raise ValueError("slot_lots exceeds max_position_lots")
    if parameters["max_unhedged_lots_hard"] < parameters["max_unhedged_lots_soft"]: raise ValueError("hard unhedged limit is below soft limit")
    state, p = new_state(state_dtype), None
    p = state[0]["pair"]
    p["status"], p["posture"] = CREATED, NORMAL
    p["mode"] = MAKER_TAKER if parameters["mode"] == "MAKER_TAKER" else TAKER_TAKER
    p["direction"] = LONG_SPREAD if parameters["direction"] == "LONG_SPREAD" else SHORT_SPREAD
    for name in ("left_asset_no", "right_asset_no", "left_account_no", "right_account_no", "spread_ticks", "requote_distance_ticks", "slot_lots", "max_position_lots", "dust_lots", "cancel_timeout_ns", "slot_timeout_ns", "max_unhedged_duration_ns", "cancel_retry_limit", "max_slippage_ticks", "quote_cooldown_ns", "market_stale_after_ns"):
        p[name] = parameters[name]
    p["hedge_ratio_num"], p["hedge_ratio_den"] = parameters["hedge_ratio_numerator"], parameters["hedge_ratio_denominator"]
    p["max_unhedged_soft"], p["max_unhedged_hard"] = parameters["max_unhedged_lots_soft"], parameters["max_unhedged_lots_hard"]
    p["max_gross_notional_ticks"], p["next_slot_id"], p["next_obligation_id"] = parameters["max_gross_notional_ticks"], 1, 1
    handlers = {"on_start": on_start, "on_tick": on_tick, "on_fill": on_fill, "on_order": on_order,
                "on_cancel": on_cancel, "on_position": on_position, "on_balance": on_balance,
                "on_account_state": on_account_state,
                "on_timer": on_timer, "on_stop": on_stop}
    return StrategyDefinition(SPEC, state, handlers, {"description": "full typed-state pair arbitrage V13"})
