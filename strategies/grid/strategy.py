"""Bounded, event-driven linear-contract grid for the Titan V13 native runtime.

Prices and quantities use ABI ticks/lots; money in this module is tick * lot.
The host owns exchange facts, execution, fees, funding and recovery barriers.
"""

import numpy as np

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.parameters import EnumParam, FloatParam, IntParam
from titan_strategy.observation import BusinessObservationSpec, Equals, ObservationField
from titan_strategy.state import array, float64, int64, new_state, record, uint8, uint32, uint64
from titan_strategy.types import EventKind, EventQos

MAX_REFS = 33
MAX_RETIRED = 128
CREATED, WARMING, INITIALIZING, RUNNING, RESETTING = 1, 2, 3, 4, 5
PAUSED, STOPPING, STOPPED, ERROR, RECONCILING = 6, 7, 8, 9, 10
BUY, SELL, LIMIT, GTC, IOC, POST_ONLY = 1, 2, 1, 1, 2, 4
GRID, INITIAL, EXIT = 1, 2, 3
PENDING, ACCEPTED, PARTIAL, FILLED, CANCEL_PENDING = 1, 2, 3, 4, 5
CANCELED, REJECTED, EXPIRED, UNKNOWN = 6, 7, 8, 255
NEUTRAL, LONG, SHORT = 0, 1, 2
ARITHMETIC, GEOMETRIC, RESET_PAIR, RETAIN = 0, 1, 0, 1
FIXED_QTY, FIXED_NOTIONAL, ALLOCATED, ALLOCATED_FIXED = 0, 1, 2, 3
ACCOUNT_READY = 4
MANUAL_STOP, RISK_STOP, BOUNDARY_STOP = 1, 2, 3
SAFE_INTEGER = 2**52

config_dtype = record(
    account_no=uint32, asset_no=uint32, quote_currency_no=uint32,
    direction=uint8, neutral_mode=uint8, spacing_mode=uint8, order_policy=uint8, size_mode=uint8,
    anchor_mode=uint8, stop_action=uint8, start_mode=uint8,
    lower_price_ticks=int64, upper_price_ticks=int64, grid_levels=int64,
    buy_levels=int64, sell_levels=int64, order_qty_lots=int64,
    order_notional_tick_lots=int64, allocated_capital_tick_lots=int64,
    leverage=int64, min_order_lots=int64, min_notional_tick_lots=int64,
    max_position_lots=int64, max_notional_tick_lots=int64,
    stop_below_ticks=int64, stop_above_ticks=int64, max_loss_tick_lots=int64,
    max_slippage_ticks=int64, market_stale_after_ns=int64,
    order_timeout_ns=int64, flatten_timeout_ns=int64, retry_backoff_ns=int64, cancel_retry_limit=int64,
    max_rejections=int64, quote_units_per_tick_lot=float64,
    margin_reserve_pct=float64,
)
grid_dtype = record(
    status=uint8, initialized=uint8, initialization_done=uint8, stop_requested=uint8, stale_pause=uint8,
    stop_reason=uint8, last_error=uint32, account_epoch=uint64, fill_sequence=uint64, anchor_sequence=uint64,
    position_wait_ns=int64, ioc_no_progress=int64, flatten_started_ns=int64, retired_cursor=uint32,
    market_ts_ns=int64, next_submit_ns=int64, anchor_ticks=float64,
    initial_target_lots=int64, expected_position_lots=int64,
    frozen_order_qty_lots=int64,
    long_limit_lots=int64, short_limit_lots=int64, round_no=uint64,
    fill_count=uint64, completed_orders=uint64, completed_pairs=uint64, paired_lots=uint64,
    reject_count=uint64, consecutive_rejects=int64, cancel_count=uint64,
    grid_inventory_lots=int64, initial_inventory_lots=int64,
    grid_cost_tick_lots=float64, initial_cost_tick_lots=float64,
    grid_profit_tick_lots=float64, inventory_profit_tick_lots=float64,
    unrealized_tick_lots=float64, gross_total_tick_lots=float64,
)
# These are business intents and consumed-fill watermarks, not an exchange order book.
ref_dtype = record(
    order_id=uint64, role=uint8, side=uint8, terminal=uint8, reduce_only=uint8,
    target_price_ticks=int64, target_lots=int64, applied_lots=int64,
    terminal_lots=int64, matched_lots=int64, fill_value_tick_lots=float64, last_fill_ticks=int64,
    created_ns=int64, terminal_ns=int64, cancel_sent_ns=int64, cancel_attempts=int64,
    reported_lots=int64, reported_ns=int64, order_sequence=uint64,
    last_fill_sequence=uint64,
)
retired_dtype = record(order_id=uint64, applied_lots=int64, side=uint8)
state_dtype = record(config=config_dtype, grid=grid_dtype, refs=array(ref_dtype, MAX_REFS),
                     retired=array(retired_dtype, MAX_RETIRED))

# Only this projection depends on the private recovery layout. Public field names,
# units and meanings remain stable when the recovery structure changes.
GRID_OBSERVATION = BusinessObservationSpec(
    schema_id="titan.grid", schema_version=1,
    fields=(
        ObservationField("phase", "enum", "state", "grid.status", label="网格业务阶段",
                         enum_mapping=tuple(enumerate(("created", "warming", "initializing", "running",
                             "resetting", "paused", "stopping", "stopped", "error", "reconciling"), 1))),
        ObservationField("anchor_ticks", "decimal", "price_tick", "grid.anchor_ticks",
                         label="网格锚点", valid_when=Equals("grid.initialized", 1)),
        ObservationField("completed_pairs", "uint64", "count", "grid.completed_pairs", label="完成配对"),
        ObservationField("paired_lots", "uint64", "quantity_lot", "grid.paired_lots", label="配对数量"),
        ObservationField("paired_gross_tick_lots", "decimal", "tick_lot", "grid.grid_profit_tick_lots",
                         label="配对毛收益", description="未计手续费与资金费，不是策略净收益"),
        ObservationField("expected_inventory_lots", "int64", "quantity_lot", "grid.expected_position_lots",
                         label="策略期望库存", description="实际持仓由平台账户服务确认"),
        ObservationField("last_market_at_ns", "int64", "ns", "grid.market_ts_ns", label="最后处理行情时间"),
        ObservationField("last_error", "uint64", "code", "grid.last_error", label="业务错误码"),

    ),
)


def integer(name, default, minimum=0, maximum=SAFE_INTEGER):
    return IntParam(name, required=False, default=default, minimum=minimum, maximum=maximum)


SPEC = StrategySpec(
    strategy_type="grid", source_version="1.2.1", recovery_state_schema_version=3,
    parameters=(
        integer("account_no", 0, maximum=2**32 - 1),
        integer("asset_no", 0, maximum=2**32 - 1),
        integer("quote_currency_no", 0, maximum=2**32 - 1),
        EnumParam("direction", required=False, default="neutral", values=("neutral", "long_bias", "short_bias")),
        EnumParam("neutral_mode", required=False, default="two_sided", values=("two_sided", "inventory_only")),
        EnumParam("spacing_mode", required=False, default="arithmetic", values=("arithmetic", "geometric")),
        EnumParam("order_policy", required=False, default="reset_pair", values=("reset_pair", "retain")),
        EnumParam("size_mode", required=False, default="fixed_qty", values=("fixed_qty", "fixed_notional", "allocated", "allocated_fixed_qty")),
        EnumParam("anchor_mode", required=False, default="order_average", values=("order_average", "last_fill")),
        EnumParam("stop_action", required=False, default="flatten", values=("flatten", "cancel")),
        EnumParam("start_mode", required=False, default="strict", values=("strict", "takeover")),
        integer("lower_price_ticks", 80, 1), integer("upper_price_ticks", 120, 2),
        integer("grid_levels", 8, 1, 4096),
        integer("buy_levels", 1, 0, 16), integer("sell_levels", 1, 0, 16),
        integer("order_qty_lots", 2, 1), integer("order_notional_tick_lots", 200, 1),
        integer("allocated_capital_tick_lots", 10000, 1), integer("leverage", 3, 1, 125),
        integer("min_order_lots", 1, 1), integer("min_notional_tick_lots", 1, 1),
        integer("max_position_lots", 0), integer("max_notional_tick_lots", 10000, 1),
        integer("stop_below_ticks", 0), integer("stop_above_ticks", 0),
        integer("max_loss_tick_lots", 0), integer("max_slippage_ticks", 2),
        integer("market_stale_after_ns", 5_000_000_000, 1, 10**15),
        integer("order_timeout_ns", 10_000_000_000, 1, 10**15),
        integer("flatten_timeout_ns", 120_000_000_000, 1, 10**15),
        integer("retry_backoff_ns", 500_000_000, 1, 10**15),
        integer("cancel_retry_limit", 3, 1, 100), integer("max_rejections", 5, 1, 1000),
        FloatParam("quote_units_per_tick_lot", required=False, default=1.0, minimum=0.0, exclusive_minimum=True),
        FloatParam("margin_reserve_pct", required=False, default=5.0, minimum=0.0, maximum=100.0),
    ),
    subscriptions=tuple(
        EventSubscription(kind, name, 2 if kind == EventKind.FILL else 1, qos)
        for kind, name, qos in (
            (EventKind.BBO, "on_tick", EventQos.LATEST),
            (EventKind.FILL, "on_fill", EventQos.RELIABLE_ORDERED),
            (EventKind.ORDER, "on_order", EventQos.RELIABLE_ORDERED),
            (EventKind.CANCEL, "on_cancel", EventQos.RELIABLE_ORDERED),
            (EventKind.POSITION, "on_position", EventQos.RELIABLE_ORDERED),
            (EventKind.BALANCE, "on_balance", EventQos.RELIABLE_ORDERED),
            (EventKind.ACCOUNT_STATE, "on_account_state", EventQos.RELIABLE_ORDERED),
            (EventKind.TIMER, "on_timer", EventQos.RELIABLE_ORDERED),
        )
    ),
    capabilities=Capability.MARKET_DATA | Capability.ACCOUNT_DATA | Capability.ORDER_EXECUTION | Capability.TIMER | Capability.STOP_BARRIER | Capability.RECOVERY_ORDER_RESULTS | Capability.PAUSE_SAFE,
)


@njit
def fail(s, code, reconcile=False):
    s["grid"]["last_error"] = code
    s["grid"]["status"] = RECONCILING if reconcile else ERROR


@njit
def find_ref(s, order_id):
    for i in range(MAX_REFS):
        if s["refs"][i]["order_id"] == order_id and order_id != 0:
            return i
    return -1


@njit
def active_index(active, order_id):
    for i in range(len(active)):
        if active[i]["order_id"] == order_id:
            return i
    return -1


@njit
def ref_count(s, role=0, side=0):
    count = 0
    for r in s["refs"]:
        if r["order_id"] and (role == 0 or r["role"] == role) and (side == 0 or r["side"] == side):
            count += 1
    return count


@njit
def quantity(c, price):
    if price <= 0:
        return 0
    if c["size_mode"] == FIXED_QTY:
        return c["order_qty_lots"]
    amount = c["order_notional_tick_lots"]
    if c["size_mode"] in (ALLOCATED, ALLOCATED_FIXED):
        amount = c["allocated_capital_tick_lots"] * c["leverage"] // c["grid_levels"]
    return int(amount // price)


@njit
def order_quantity(s, price):
    if s["config"]["size_mode"] == ALLOCATED_FIXED and s["grid"]["frozen_order_qty_lots"]:
        return s["grid"]["frozen_order_qty_lots"]
    return quantity(s["config"], price)


@njit
def level_price(c, anchor, side, level):
    if c["spacing_mode"] == ARITHMETIC:
        distance = (c["upper_price_ticks"] - c["lower_price_ticks"]) / c["grid_levels"]
        raw = anchor - level * distance if side == BUY else anchor + level * distance
    else:
        ratio = (c["upper_price_ticks"] / c["lower_price_ticks"]) ** (1.0 / c["grid_levels"]) - 1.0
        raw = anchor * (1.0 - ratio) ** level if side == BUY else anchor * (1.0 + ratio) ** level
    return int(np.floor(raw + 1e-9)) if side == BUY else int(np.ceil(raw - 1e-9))


@njit
def valid_size(c, qty, price):
    return (price > 0 and qty >= c["min_order_lots"] and qty <= SAFE_INTEGER // price
            and qty * price >= c["min_notional_tick_lots"])


@njit
def retire_ref(s, index):
    r, g = s["refs"][index], s["grid"]
    retired = s["retired"][g["retired_cursor"]]
    retired["order_id"], retired["applied_lots"], retired["side"] = r["order_id"], r["applied_lots"], r["side"]
    g["retired_cursor"] = (g["retired_cursor"] + 1) % MAX_RETIRED
    r["order_id"] = 0


@njit
def retired_fill(s, order_id, side, cumulative):
    for r in s["retired"]:
        if r["order_id"] == order_id:
            return r["side"] == side and 0 < cumulative <= r["applied_lots"]
    return False


@njit
def retired_index(s, order_id):
    for i in range(MAX_RETIRED):
        if s["retired"][i]["order_id"] == order_id and order_id:
            return i
    return -1


@njit
def observe_filled(s, index, cumulative, now):
    r = s["refs"][index]
    if cumulative < r["applied_lots"] or cumulative < r["reported_lots"] or cumulative > r["target_lots"]:
        fail(s, 204, True)
        return False
    r["reported_lots"] = cumulative
    if cumulative > r["applied_lots"] and r["reported_ns"] == 0:
        r["reported_ns"] = now
    return True


@njit
def position_caught_up(ctx):
    c, g = ctx.state["config"], ctx.state["grid"]
    pos = ctx.position(c["account_no"], c["asset_no"])
    for r in ctx.state["refs"]:
        if r["order_id"] and r["reported_lots"] > r["applied_lots"]:
            return False
    if pos["account_sequence"] >= g["fill_sequence"] and pos["qty_lots"] == g["expected_position_lots"]:
        g["position_wait_ns"] = 0
        return True
    buys, sells, _ = risk_reservations(ctx.state)
    possible = g["expected_position_lots"] - sells <= pos["qty_lots"] <= g["expected_position_lots"] + buys
    if pos["account_sequence"] < g["fill_sequence"] or (ref_count(ctx.state) and possible):
        if g["position_wait_ns"] == 0:
            g["position_wait_ns"] = ctx.now
        if ctx.now - g["position_wait_ns"] >= c["order_timeout_ns"]:
            fail(ctx.state, 205, True)
        return False
    fail(ctx.state, 201, True)
    return False


@njit
def cancel_orders(ctx, grids_only=False):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    active = ctx.active_orders()
    for r in s["refs"]:
        if not r["order_id"] or r["terminal"] or (grids_only and r["role"] == EXIT):
            continue
        index = active_index(active, r["order_id"])
        if index < 0:
            continue  # A queued terminal/fill event may still be ahead of this callback.
        order = active[index]
        if order["status"] == UNKNOWN:
            fail(s, 202, True)
            continue
        if order["status"] == CANCEL_PENDING:
            continue
        if order["status"] in (FILLED, CANCELED, REJECTED, EXPIRED):
            continue
        if r["cancel_sent_ns"] and ctx.now - r["cancel_sent_ns"] < c["retry_backoff_ns"]:
            continue
        if r["cancel_attempts"] >= c["cancel_retry_limit"]:
            fail(s, 203, True)
            continue
        token = ctx.cancel_order(c["account_no"], c["asset_no"], r["order_id"])
        r["cancel_sent_ns"] = ctx.now
        r["cancel_attempts"] += 1
        if token:
            g["cancel_count"] += 1


@njit
def submit(ctx, role, side, qty, price, tif, reduce_only=False):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    index = -1
    for i in range(MAX_REFS):
        if s["refs"][i]["order_id"] == 0:
            index = i
            break
    if index < 0 or not valid_size(c, qty, price):
        fail(s, 301)
        return False
    order_id = ctx.submit_order(c["account_no"], c["asset_no"], side, LIMIT, qty, price, tif, reduce_only)
    if order_id == 0:
        fail(s, 302, True)  # Transport/host failures are not exchange rejections.
        return False
    r = s["refs"][index]
    r["order_id"], r["role"], r["side"] = order_id, role, side
    r["reduce_only"] = reduce_only
    r["target_price_ticks"], r["target_lots"] = price, qty
    r["applied_lots"], r["terminal_lots"], r["terminal"] = 0, 0, 0
    r["matched_lots"] = 0
    r["fill_value_tick_lots"], r["last_fill_ticks"] = 0.0, 0
    r["created_ns"], r["cancel_sent_ns"], r["cancel_attempts"] = ctx.now, 0, 0
    r["terminal_ns"] = 0
    r["reported_lots"], r["reported_ns"], r["order_sequence"] = 0, 0, 0
    r["last_fill_sequence"] = 0
    return True


@njit
def account_fill(s, role, side, qty, price):
    g = s["grid"]
    signed = qty if side == BUY else -qty
    remaining = qty
    matched = 0
    # Match strategy-created inventory first; initialization inventory has a separate attribution.
    for bucket in range(2):
        inventory = g["grid_inventory_lots"] if bucket == 0 else g["initial_inventory_lots"]
        value = g["grid_cost_tick_lots"] if bucket == 0 else g["initial_cost_tick_lots"]
        if inventory == 0 or (inventory > 0) == (signed > 0) or remaining == 0:
            continue
        close = min(abs(inventory), remaining)
        average = value / abs(inventory)
        profit = (price - average) * close if inventory > 0 else (average - price) * close
        if bucket == 0 and role == GRID:
            g["grid_profit_tick_lots"] += profit
            g["paired_lots"] += close
            matched += close
        else:
            g["inventory_profit_tick_lots"] += profit
        value -= average * close
        inventory += close if inventory < 0 else -close
        if bucket == 0:
            g["grid_inventory_lots"], g["grid_cost_tick_lots"] = inventory, value
        else:
            g["initial_inventory_lots"], g["initial_cost_tick_lots"] = inventory, value
        remaining -= close
    if remaining:
        delta = remaining if side == BUY else -remaining
        if role == GRID:
            g["grid_inventory_lots"] += delta
            g["grid_cost_tick_lots"] += remaining * price
        else:
            g["initial_inventory_lots"] += delta
            g["initial_cost_tick_lots"] += remaining * price
    g["expected_position_lots"] += signed
    return matched


@njit
def mark_to_market(s, mid):
    g = s["grid"]
    unrealized = 0.0
    for bucket in range(2):
        qty = g["grid_inventory_lots"] if bucket == 0 else g["initial_inventory_lots"]
        cost = g["grid_cost_tick_lots"] if bucket == 0 else g["initial_cost_tick_lots"]
        if qty:
            unrealized += mid * float(qty) - (cost if qty > 0 else -cost)
    g["unrealized_tick_lots"] = unrealized
    g["gross_total_tick_lots"] = g["grid_profit_tick_lots"] + g["inventory_profit_tick_lots"] + unrealized


@njit
def finish_ref(s, index):
    c, g, r = s["config"], s["grid"], s["refs"][index]
    if r["applied_lots"] < max(r["terminal_lots"], r["reported_lots"]):
        return  # Terminal status may precede the independent execution facts.
    if r["applied_lots"] == r["target_lots"]:
        g["completed_orders"] += 1
        g["consecutive_rejects"] = 0
        if r["role"] == GRID:
            if r["matched_lots"] == r["target_lots"]:
                g["completed_pairs"] += 1
            if r["last_fill_sequence"] >= g["anchor_sequence"]:
                if c["anchor_mode"] == 0:
                    g["anchor_ticks"] = r["fill_value_tick_lots"] / r["applied_lots"]
                else:
                    g["anchor_ticks"] = r["last_fill_ticks"]
                g["anchor_sequence"] = r["last_fill_sequence"]
            g["round_no"] += 1
            if c["order_policy"] == RESET_PAIR and g["status"] == RUNNING:
                g["status"] = RESETTING
    if r["role"] != GRID and r["applied_lots"] == 0:
        g["ioc_no_progress"] += 1
        if r["role"] == INITIAL and g["status"] != STOPPING and g["ioc_no_progress"] >= c["max_rejections"]:
            fail(s, 303)
    retire_ref(s, index)


@njit
def order_matches(c, r, order):
    return (order["account_no"] == c["account_no"] and order["asset_no"] == c["asset_no"]
            and order["position_side"] == 0 and order["side"] == r["side"]
            and order["qty_lots"] == r["target_lots"] and order["price_ticks"] == r["target_price_ticks"]
            and order["order_type"] == LIMIT
            and order["time_in_force"] == (POST_ONLY if r["role"] == GRID else IOC)
            and order["reduce_only"] == r["reduce_only"]
            and 0 <= order["cumulative_filled_lots"] <= r["target_lots"]
            and order["status"] in (PENDING, ACCEPTED, PARTIAL, FILLED, CANCEL_PENDING, CANCELED, REJECTED, EXPIRED))


@njit
def validate_active(ctx):
    s, c = ctx.state, ctx.state["config"]
    active = ctx.active_orders()
    for order in active:
        index = find_ref(s, order["order_id"])
        retired = retired_index(s, order["order_id"])
        if index < 0 and retired >= 0 and order["status"] in (FILLED, CANCELED, REJECTED, EXPIRED):
            old = s["retired"][retired]
            if old["side"] == order["side"] and old["applied_lots"] == order["cumulative_filled_lots"]:
                continue
        if index < 0:
            fail(s, 401, True)
            return False
        r = s["refs"][index]
        if not order_matches(c, r, order):
            fail(s, 402, True)
            return False
        if not observe_filled(s, index, order["cumulative_filled_lots"], ctx.now):
            return False
    return True


@njit
def risk_reservations(s):
    buys, sells, notional = 0, 0, 0
    for r in s["refs"]:
        if not r["order_id"]:
            continue
        leaves = r["target_lots"] - r["applied_lots"]
        if r["side"] == BUY:
            buys += leaves
        else:
            sells += leaves
        notional += leaves * r["target_price_ticks"]
    return buys, sells, notional


@njit
def has_capacity(ctx, side, qty, price):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    buys, sells, pending = risk_reservations(s)
    if qty <= 0 or price <= 0:
        return False
    pos = g["expected_position_lots"]
    closing = max(0, pos - sells) if side == SELL else max(0, -pos - buys)
    if qty <= closing:
        return True  # Confirmed inventory reduction does not reserve opening margin.
    high = pos + buys + (qty if side == BUY else 0)
    low = pos - sells - (qty if side == SELL else 0)
    if high > g["long_limit_lots"] or low < g["short_limit_lots"]:
        return False
    # Boundary price values every possible resulting position conservatively.
    mark = max(c["upper_price_ticks"], ctx.market(c["asset_no"])["best_ask_ticks"], price)
    worst = max(abs(high), abs(low)) * float(mark)
    if worst > c["max_notional_tick_lots"]:
        return False
    balance = ctx.balance(c["account_no"], c["quote_currency_no"])
    quote_scale = c["quote_units_per_tick_lot"]
    required = (abs(pos) * float(mark) + pending + qty * price) / c["leverage"]
    additional = (pending + qty * price) / c["leverage"]
    reserve = 1.0 + c["margin_reserve_pct"] / 100.0
    return (required * reserve <= c["allocated_capital_tick_lots"]
            and np.ceil(additional * reserve * quote_scale) <= balance["available_units"])


@njit
def initialize(ctx, mid, bid, ask):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    if not g["initialized"]:
        if not c["lower_price_ticks"] < mid < c["upper_price_ticks"]:
            fail(s, 501)
            return
        g["anchor_ticks"] = mid
        if c["size_mode"] == ALLOCATED_FIXED:
            g["frozen_order_qty_lots"] = quantity(c, mid)
        if c["spacing_mode"] == ARITHMETIC:
            k = int((mid - c["lower_price_ticks"]) * c["grid_levels"] // (c["upper_price_ticks"] - c["lower_price_ticks"]))
        else:
            k = int(np.log(mid / c["lower_price_ticks"]) / np.log(c["upper_price_ticks"] / c["lower_price_ticks"]) * c["grid_levels"])
        k = max(0, min(k, c["grid_levels"] - 1))
        target = 0
        if c["direction"] == LONG:
            target = (c["grid_levels"] - k) * order_quantity(s, mid)
        elif c["direction"] == SHORT:
            target = -k * order_quantity(s, mid)
        # Explicit takeover is valued from the authority in on_start.
        if c["direction"] == NEUTRAL and g["expected_position_lots"]:
            target = g["expected_position_lots"]
        g["initial_target_lots"], g["initialized"] = target, 1
    delta = g["initial_target_lots"] - g["expected_position_lots"]
    if delta == 0:
        g["initialization_done"] = 1
        g["status"] = RUNNING
        return
    if ref_count(s) or ctx.now < g["next_submit_ns"]:
        return
    side = BUY if delta > 0 else SELL
    price = ask + c["max_slippage_ticks"] if side == BUY else max(1, bid - c["max_slippage_ticks"])
    if not valid_size(c, abs(delta), price) or not has_capacity(ctx, side, abs(delta), price):
        fail(s, 502)
        return
    reducing = abs(delta) <= max(0, g["expected_position_lots"] if side == SELL else -g["expected_position_lots"])
    submit(ctx, INITIAL, side, abs(delta), price, IOC, reducing)


@njit
def quote(ctx, bid, ask):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    if ctx.now < g["next_submit_ns"]:
        return
    upper_edge = level_price(c, g["anchor_ticks"], SELL, 1) > c["upper_price_ticks"]
    lower_edge = level_price(c, g["anchor_ticks"], BUY, 1) < c["lower_price_ticks"]
    if ((c["direction"] == LONG and upper_edge and g["expected_position_lots"] == 0)
            or (c["direction"] == SHORT and lower_edge and g["expected_position_lots"] == 0)):
        g["stop_requested"], g["status"] = 1, STOPPING
        g["stop_reason"] = BOUNDARY_STOP
        cancel_orders(ctx, True)
        return
    # Retain orders at old anchors; search a bounded number of levels for free prices.
    for leg in range(2):
        # A non-tradable residual must be discovered before staging more opening risk.
        side = (SELL if leg == 0 else BUY) if g["expected_position_lots"] > 0 else (BUY if leg == 0 else SELL)
        if (side == BUY and c["direction"] == LONG and upper_edge) or (side == SELL and c["direction"] == SHORT and lower_edge):
            continue
        desired = c["buy_levels"] if side == BUY else c["sell_levels"]
        count = ref_count(s, GRID, side)
        for level in range(1, 65):
            if count >= desired or g["status"] != RUNNING:
                break
            price = level_price(c, g["anchor_ticks"], side, level)
            if side == SELL and c["direction"] == LONG and upper_edge:
                price = c["upper_price_ticks"]
            elif side == BUY and c["direction"] == SHORT and lower_edge:
                price = c["lower_price_ticks"]
            if price < c["lower_price_ticks"] or price > c["upper_price_ticks"]:
                break
            if (side == BUY and price >= ask) or (side == SELL and price <= bid):
                continue
            duplicate = False
            for r in s["refs"]:
                if r["order_id"]:
                    if (r["target_price_ticks"] == price
                            or (side == BUY and r["side"] == SELL and price >= r["target_price_ticks"])
                            or (side == SELL and r["side"] == BUY and price <= r["target_price_ticks"])):
                        duplicate = True
            if duplicate:
                continue
            qty = order_quantity(s, price)
            if side == SELL and c["direction"] == LONG:
                qty = min(qty, max(0, g["expected_position_lots"] - risk_reservations(s)[1]))
            elif side == SELL and c["direction"] == NEUTRAL and c["neutral_mode"] == 1:
                qty = min(qty, max(0, g["expected_position_lots"] - risk_reservations(s)[1]))
            elif side == BUY and c["direction"] == SHORT:
                qty = min(qty, max(0, -g["expected_position_lots"] - risk_reservations(s)[0]))
            if qty == 0:
                continue
            if not valid_size(c, qty, price):
                fail(s, 503)
                cancel_orders(ctx)
                break
            buys, sells, _ = risk_reservations(s)
            closing = max(0, g["expected_position_lots"] - sells) if side == SELL else max(0, -g["expected_position_lots"] - buys)
            if not has_capacity(ctx, side, qty, price):
                qty = min(qty, closing)
                if not valid_size(c, qty, price):
                    continue
            if submit(ctx, GRID, side, qty, price, POST_ONLY, qty <= closing):
                count += 1


@njit
def drive_inner(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    if g["status"] in (CREATED, STOPPED):
        return
    account = ctx.account(c["account_no"])
    if account["account_epoch"] != g["account_epoch"]:
        fail(s, 601, True)
    ready = account["state"] == ACCOUNT_READY
    validate_active(ctx)
    active = ctx.active_orders()
    for r in s["refs"]:
        if not r["order_id"]:
            continue
        index = active_index(active, r["order_id"])
        overdue = ctx.now - r["created_ns"] >= c["order_timeout_ns"]
        if r["terminal"]:
            if ctx.now - r["terminal_ns"] >= c["order_timeout_ns"] and r["applied_lots"] < r["terminal_lots"]:
                fail(s, 602, True)
        elif index < 0 and overdue:
            fail(s, 603, True)
        elif index >= 0:
            order = active[index]
            if ((order["status"] == PENDING and overdue)
                    or (r["role"] != GRID and overdue)
                    or (order["status"] == CANCEL_PENDING
                        and ctx.now - (r["cancel_sent_ns"] if r["cancel_sent_ns"] else order["updated_ts_ns"]) >= c["order_timeout_ns"])):
                fail(s, 604, True)
        if r["reported_lots"] > r["applied_lots"] and ctx.now - r["reported_ns"] >= c["order_timeout_ns"]:
            fail(s, 606, True)
    if g["position_wait_ns"] and ctx.now - g["position_wait_ns"] >= c["order_timeout_ns"]:
        if not ready or not position_caught_up(ctx):
            fail(s, 205, True)
    market = ctx.market(c["asset_no"])
    bid, ask = market["best_bid_ticks"], market["best_ask_ticks"]
    fresh = (bid > 0 and ask > bid and g["market_ts_ns"] > 0
             and 0 <= ctx.now - g["market_ts_ns"] < c["market_stale_after_ns"])
    if bid > 0 and ask > bid:
        mid = bid + (ask - bid) / 2.0
        mark_to_market(s, mid)
        if fresh and g["status"] not in (ERROR, RECONCILING) and ((c["stop_below_ticks"] and mid <= c["stop_below_ticks"])
                or (c["stop_above_ticks"] and mid >= c["stop_above_ticks"])
                or abs(g["expected_position_lots"]) * float(max(ask, c["upper_price_ticks"])) > c["max_notional_tick_lots"]
                or (c["max_loss_tick_lots"] and g["gross_total_tick_lots"] <= -c["max_loss_tick_lots"])):
            g["stop_requested"], g["status"] = 1, STOPPING
            g["stop_reason"] = RISK_STOP
    if g["status"] in (ERROR, RECONCILING):
        cancel_orders(ctx)
        return
    if g["status"] == STOPPING:
        if g["flatten_started_ns"] == 0:
            g["flatten_started_ns"] = ctx.now
        # Opening retry limits must not permanently abandon confirmed inventory in a fast
        # market. Protective exits retain price/quantity protection and a separate total deadline.
        if (g["expected_position_lots"] and (c["stop_action"] == 1 or g["stop_reason"] == RISK_STOP)
                and (ctx.now < g["flatten_started_ns"]
                     or ctx.now - g["flatten_started_ns"] >= c["flatten_timeout_ns"])):
            fail(s, 608, True)
            cancel_orders(ctx)
            return
        cancel_orders(ctx, True)
        if ref_count(s) or not ready or not position_caught_up(ctx):
            return
        if g["expected_position_lots"] and (c["stop_action"] == 1 or g["stop_reason"] == RISK_STOP):
            if not fresh or ctx.now < g["next_submit_ns"]:
                return
            pos = g["expected_position_lots"]
            side = SELL if pos > 0 else BUY
            price = max(1, bid - c["max_slippage_ticks"]) if side == SELL else ask + c["max_slippage_ticks"]
            submit(ctx, EXIT, side, abs(pos), price, IOC, True)
        else:
            g["status"] = STOPPED
        return
    if ctx.is_paused:
        cancel_orders(ctx)
        return
    if not ready or not fresh:
        if g["status"] != PAUSED:
            g["stale_pause"], g["status"] = 1, PAUSED
        cancel_orders(ctx)
        return
    if g["status"] == RESETTING:
        cancel_orders(ctx, True)
    if not position_caught_up(ctx):
        return
    if g["status"] == PAUSED:
        if ref_count(s):
            cancel_orders(ctx)
            return
        g["stale_pause"] = 0
        g["status"] = RUNNING if g["initialization_done"] else INITIALIZING
    if (g["expected_position_lots"] > g["long_limit_lots"]
            or g["expected_position_lots"] < g["short_limit_lots"]):
        fail(s, 605)
        cancel_orders(ctx)
        return
    if g["status"] in (WARMING, INITIALIZING):
        g["status"] = INITIALIZING
        initialize(ctx, bid + (ask - bid) / 2.0, bid, ask)
    if g["status"] == RESETTING:
        cancel_orders(ctx, True)
        if ref_count(s):
            return
        g["status"] = RUNNING
    if g["status"] == RUNNING:
        quote(ctx, bid, ask)


@njit
def drive(ctx):
    drive_inner(ctx)
    if ctx.state["grid"]["stop_requested"] and ctx.state["grid"]["status"] == STOPPED:
        ctx.complete_stop()


@njit
def on_start(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    account = ctx.account(c["account_no"])
    pos = ctx.position(c["account_no"], c["asset_no"])
    balance = ctx.balance(c["account_no"], c["quote_currency_no"])
    if (account["state"] != ACCOUNT_READY or pos["position_side"] != 0
            or account["account_no"] != c["account_no"]
            or pos["account_no"] != c["account_no"] or pos["asset_no"] != c["asset_no"]
            or balance["account_no"] != c["account_no"] or balance["currency_no"] != c["quote_currency_no"]):
        fail(s, 701, True)
        return -1
    qty = pos["qty_lots"]
    if (qty > g["long_limit_lots"] or qty < g["short_limit_lots"]
            or (qty and (pos["average_price_ticks"] <= 0
                         or pos["average_price_ticks"] > SAFE_INTEGER // abs(qty)))):
        fail(s, 706, True)
        return -1
    if g["status"] == CREATED:
        if ref_count(s) or len(ctx.active_orders()) or (c["start_mode"] == 0 and pos["qty_lots"]):
            fail(s, 702, True)
            return -1
        if pos["qty_lots"] and pos["average_price_ticks"] <= 0:
            fail(s, 703, True)
            return -1
        g["expected_position_lots"] = pos["qty_lots"]
        g["initial_inventory_lots"] = pos["qty_lots"]
        g["initial_cost_tick_lots"] = abs(pos["qty_lots"]) * pos["average_price_ticks"]
        g["status"] = WARMING
    elif pos["qty_lots"] != g["expected_position_lots"] or not validate_active(ctx):
        fail(s, 704, True)
        return -1
    if g["grid_inventory_lots"] + g["initial_inventory_lots"] != g["expected_position_lots"]:
        fail(s, 707, True)
        return -1
    if g["status"] in (ERROR, RECONCILING):
        return -1
    g["account_epoch"] = account["account_epoch"]
    return 0


@njit
def on_recover(ctx):
    # Host has already validated checkpoint identity, replay and account reconciliation.
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    if (g["status"] == STOPPING and g["expected_position_lots"]
            and (c["stop_action"] == 1 or g["stop_reason"] == RISK_STOP) and g["flatten_started_ns"]
            and (ctx.now < g["flatten_started_ns"]
                 or ctx.now - g["flatten_started_ns"] >= c["flatten_timeout_ns"])):
        fail(s, 710, True)
        return -1
    g["market_ts_ns"], g["fill_sequence"], g["anchor_sequence"], g["position_wait_ns"] = 0, 0, 0, 0
    if not validate_active(ctx):
        return -1
    active = ctx.active_orders()
    outcomes = ctx.recovery_orders()
    for i in range(MAX_REFS):
        r = s["refs"][i]
        if r["order_id"]:
            index = active_index(active, r["order_id"])
            proof = active_index(outcomes, r["order_id"])
            if (proof < 0 or not order_matches(c, r, outcomes[proof])
                    or outcomes[proof]["cumulative_filled_lots"] != r["applied_lots"]):
                fail(s, 705, True)
                return -1
            outcome = outcomes[proof]
            if index < 0:
                if outcome["status"] not in (CANCELED, REJECTED, EXPIRED, FILLED):
                    fail(s, 708, True)
                    return -1
                r["terminal"], r["terminal_lots"] = outcome["status"], r["applied_lots"]
                finish_ref(s, i)
                g["next_submit_ns"] = ctx.now + c["retry_backoff_ns"]
            else:
                if (active[index]["cumulative_filled_lots"] != r["applied_lots"]
                        or outcome["status"] not in (ACCEPTED, PARTIAL)):
                    fail(s, 709, True)
                    return -1
                r["cancel_sent_ns"], r["cancel_attempts"], r["created_ns"] = 0, 0, ctx.now
                r["reported_lots"], r["reported_ns"] = r["applied_lots"], 0
                r["order_sequence"] = outcome["account_sequence"]
                r["last_fill_sequence"] = 0
    if g["status"] == STOPPED and c["stop_action"] == 0 and g["stop_reason"] == MANUAL_STOP:
        g["stop_requested"], g["status"] = 0, PAUSED
        g["flatten_started_ns"] = 0
    if on_start(ctx) != 0:
        return -1
    return 0


@njit
def on_tick(ctx):
    c, g = ctx.state["config"], ctx.state["grid"]
    for tick in ctx.ticks():
        if tick["asset_no"] == c["asset_no"] and tick["receive_ts_ns"] <= ctx.now:
            g["market_ts_ns"] = max(g["market_ts_ns"], tick["receive_ts_ns"])
    drive(ctx)


@njit
def on_fill(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    for fill in ctx.fills():
        if fill["account_no"] != c["account_no"] or fill["asset_no"] != c["asset_no"]:
            continue
        cumulative = fill["cumulative_filled_lots"]
        if cumulative <= 0 or fill["fill_qty_lots"] <= 0 or fill["fill_price_ticks"] <= 0:
            fail(s, 803, True)
            break
        index = find_ref(s, fill["order_id"])
        if index < 0:
            if retired_fill(s, fill["order_id"], fill["side"], cumulative):
                continue
            fail(s, 801, True)
            break
        r = s["refs"][index]
        if fill["side"] != r["side"]:
            fail(s, 804, True)
            break
        if cumulative <= r["applied_lots"]:
            continue
        delta = cumulative - r["applied_lots"]
        if (delta != fill["fill_qty_lots"] or cumulative > r["target_lots"]
                or fill["side"] != r["side"] or fill["fill_price_ticks"] <= 0
                or fill["fill_price_ticks"] > SAFE_INTEGER // r["target_lots"]
                or (r["terminal"] and cumulative > r["terminal_lots"])
                or (fill["final_fill"] and cumulative != r["target_lots"])):
            fail(s, 802, True)
            break
        r["applied_lots"] = cumulative
        r["reported_lots"] = max(r["reported_lots"], cumulative)
        if r["reported_lots"] == cumulative:
            r["reported_ns"] = 0
        r["order_sequence"] = max(r["order_sequence"], fill["account_sequence"])
        r["last_fill_sequence"] = max(r["last_fill_sequence"], fill["account_sequence"])
        r["fill_value_tick_lots"] += delta * fill["fill_price_ticks"]
        r["last_fill_ticks"] = fill["fill_price_ticks"]
        r["matched_lots"] += account_fill(s, r["role"], r["side"], delta, fill["fill_price_ticks"])
        g["fill_sequence"] = max(g["fill_sequence"], fill["account_sequence"])
        if g["position_wait_ns"] == 0:
            g["position_wait_ns"] = ctx.now
        g["fill_count"] += 1
        g["consecutive_rejects"] = 0
        if r["role"] != GRID:
            g["ioc_no_progress"] = 0
        if r["role"] == INITIAL:
            g["anchor_ticks"] = fill["fill_price_ticks"]
        if cumulative == r["target_lots"] or (r["terminal"] and cumulative >= r["terminal_lots"]):
            finish_ref(s, index)
    drive(ctx)


@njit
def on_order(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["grid"]
    for event in ctx.order_events():
        if event["account_no"] != c["account_no"] or event["asset_no"] != c["asset_no"]:
            continue
        index = find_ref(s, event["order_id"])
        if index < 0:
            retired = retired_index(s, event["order_id"])
            if (event["status"] not in (FILLED, CANCELED, REJECTED, EXPIRED)
                    or retired < 0 or event["cumulative_filled_lots"] > s["retired"][retired]["applied_lots"]):
                fail(s, 901, True)
            continue
        r = s["refs"][index]
        if event["account_sequence"] and event["account_sequence"] < r["order_sequence"]:
            continue
        if (event["qty_lots"] != r["target_lots"] or event["price_ticks"] != r["target_price_ticks"]
                or event["cumulative_filled_lots"] > r["target_lots"] or event["position_side"] != 0):
            fail(s, 902, True)
            break
        if event["status"] == UNKNOWN:
            fail(s, 903, True)
            break
        if r["terminal"] and event["status"] not in (FILLED, CANCELED, REJECTED, EXPIRED):
            continue  # A stale acknowledgement cannot reopen a terminal lifecycle.
        if not observe_filled(s, index, event["cumulative_filled_lots"], ctx.now):
            break
        r["order_sequence"] = max(r["order_sequence"], event["account_sequence"])
        if event["status"] in (FILLED, CANCELED, REJECTED, EXPIRED):
            if r["terminal"]:
                if r["terminal"] != event["status"] or r["terminal_lots"] != event["cumulative_filled_lots"]:
                    fail(s, 906, True)
                continue
            was_rejected = r["terminal"] == REJECTED
            if r["terminal"] == 0:
                r["terminal_ns"] = ctx.now
            r["terminal"], r["terminal_lots"] = event["status"], event["cumulative_filled_lots"]
            if event["status"] == FILLED and r["terminal_lots"] != r["target_lots"]:
                fail(s, 904, True)
                break
            if event["status"] == REJECTED and not was_rejected:
                g["reject_count"] += 1
                g["consecutive_rejects"] += 1
                if g["status"] != STOPPING and r["role"] != EXIT and g["consecutive_rejects"] >= c["max_rejections"]:
                    fail(s, 905)
            g["next_submit_ns"] = ctx.now + c["retry_backoff_ns"]
            finish_ref(s, index)
    drive(ctx)


@njit
def on_cancel(ctx):
    s, c = ctx.state, ctx.state["config"]
    for event in ctx.cancel_events():
        if event["account_no"] != c["account_no"] or event["asset_no"] != c["asset_no"]:
            continue
        index = find_ref(s, event["order_id"])
        if index < 0:
            continue
        if event["request_result"] == 2:
            fail(s, 1001, True)
        elif event["request_result"] == 1:
            s["refs"][index]["cancel_sent_ns"] = ctx.now
        # Cancel events have no cumulative execution quantity. Only an order terminal
        # event (or a complete fill) can release the reservation safely.
    drive(ctx)


@njit
def on_position(ctx):
    drive(ctx)


@njit
def on_balance(ctx):
    drive(ctx)


@njit
def on_account_state(ctx):
    drive(ctx)


@njit
def on_timer(ctx):
    drive(ctx)


@njit
def on_stop(ctx):
    g = ctx.state["grid"]
    if g["status"] in (ERROR, RECONCILING):
        g["stop_requested"] = 1
        cancel_orders(ctx)
        return 0
    if g["stop_reason"] == 0:
        g["stop_reason"] = MANUAL_STOP
    g["stop_requested"], g["status"] = 1, STOPPING
    drive(ctx)
    return 0


def build(parameters):
    p = SPEC.validate_parameters(parameters)
    if p["lower_price_ticks"] >= p["upper_price_ticks"]:
        raise ValueError("lower_price_ticks must be below upper_price_ticks")
    if not p["buy_levels"] + p["sell_levels"]:
        raise ValueError("at least one active grid side is required")
    if p["upper_price_ticks"] - p["lower_price_ticks"] < p["grid_levels"]:
        raise ValueError("spacing must be at least one price tick")
    if p["spacing_mode"] == "geometric":
        ratio = (p["upper_price_ticks"] / p["lower_price_ticks"]) ** (1.0 / p["grid_levels"]) - 1.0
        if ratio >= 1.0 or p["lower_price_ticks"] * ratio < 1.0:
            raise ValueError("geometric spacing collapses ticks or has a nonpositive buy factor")
    if p["stop_below_ticks"] and p["stop_above_ticks"] and p["stop_below_ticks"] >= p["stop_above_ticks"]:
        raise ValueError("stop_below_ticks must be below stop_above_ticks")
    if p["allocated_capital_tick_lots"] * p["leverage"] > SAFE_INTEGER:
        raise ValueError("allocated leveraged capital exceeds exact arithmetic range")
    state = new_state(state_dtype)
    c, g = state[0]["config"], state[0]["grid"]
    enums = {
        "direction": ("neutral", "long_bias", "short_bias"),
        "neutral_mode": ("two_sided", "inventory_only"),
        "spacing_mode": ("arithmetic", "geometric"),
        "order_policy": ("reset_pair", "retain"),
        "size_mode": ("fixed_qty", "fixed_notional", "allocated", "allocated_fixed_qty"),
        "anchor_mode": ("order_average", "last_fill"),
        "stop_action": ("cancel", "flatten"), "start_mode": ("strict", "takeover"),
    }
    for name in c.dtype.names:
        c[name] = enums[name].index(p[name]) if name in enums else p[name]
    if p["size_mode"] == "fixed_qty":
        min_qty = max_qty = p["order_qty_lots"]
    else:
        amount = p["order_notional_tick_lots"] if p["size_mode"] == "fixed_notional" else p["allocated_capital_tick_lots"] * p["leverage"] // p["grid_levels"]
        min_qty, max_qty = amount // p["upper_price_ticks"], amount // p["lower_price_ticks"]
    # Reject configurations that would silently shrink/skip an invalid price layer.
    if min_qty < p["min_order_lots"]:
        raise ValueError("order sizing cannot satisfy minimum quantity/notional over the range")
    if p["size_mode"] in ("fixed_qty", "allocated_fixed_qty"):
        if min_qty * p["lower_price_ticks"] < p["min_notional_tick_lots"]:
            raise ValueError("order sizing cannot satisfy minimum quantity/notional over the range")
    elif amount - p["upper_price_ticks"] + 1 < p["min_notional_tick_lots"]:
        price, intervals = p["lower_price_ticks"], 0
        while price <= p["upper_price_ticks"]:
            qty = amount // price
            if qty * price < p["min_notional_tick_lots"]:
                raise ValueError("order sizing cannot satisfy minimum quantity/notional over the range")
            price = amount // qty + 1
            intervals += 1
            if intervals > 100000:
                raise ValueError("cannot prove minimum notional within the bounded validation budget")
    limit = p["max_position_lots"] or p["grid_levels"] * max_qty
    if limit < min_qty:
        raise ValueError("position limit is below the minimum configured grid order")
    if (limit * (p["upper_price_ticks"] + p["max_slippage_ticks"]) > SAFE_INTEGER
            or max_qty * p["upper_price_ticks"] * MAX_REFS > SAFE_INTEGER):
        raise ValueError("price/quantity exposure exceeds exact arithmetic range")
    c["max_position_lots"] = limit
    g["long_limit_lots"] = 0 if p["direction"] == "short_bias" else limit
    g["short_limit_lots"] = 0 if p["direction"] == "long_bias" or (p["direction"] == "neutral" and p["neutral_mode"] == "inventory_only") else -limit
    g["status"] = CREATED
    handlers = {
        "on_start": on_start, "on_recover": on_recover, "on_tick": on_tick,
        "on_fill": on_fill, "on_order": on_order, "on_cancel": on_cancel,
        "on_position": on_position, "on_balance": on_balance,
        "on_account_state": on_account_state, "on_timer": on_timer, "on_stop": on_stop,
    }
    return StrategyDefinition(SPEC, state, handlers, {"description": "Bounded linear-contract grid; platform owns net accounting"}, business_observation=GRID_OBSERVATION)
