"""Single-file, fill-triggered cross-exchange market maker (Titan native V13).

Successor to reference_mm, inspired by bulletxyz/hft-for-dummies part 2.
Defaults: reference_asset_no=0 is now ALSO the hedge market; traded_asset_no=1
is the PostOnly maker market. account_no=0 trades the maker; hedge_account_no=1
trades the hedge. Runtime bindings select exchanges, symbols and accounts.

Every consumed maker fill changes the exact hedge debt. One price-protected IOC
hedge can be in flight; new fills never cause duplicate submissions. Maker orders
are pulled while debt or a hedge is pending. Only native fills change inventory;
ACKs, missing active rows and cumulative order totals never synthesize executions.
Known terminal zero/partial IOC outcomes retry with a bounded deadline/backoff;
unknown outcomes latch failure and preserve the original order identity.

reference_tick_ratio converts a hedge price tick into maker price ticks in an
explicit common quote unit. hedge_lots_per_maker_lot is an EXACT positive integer
base-delta conversion, including contract multipliers. Every one-lot maker partial
fill must be hedgeable: choose compatible binding lots and venue minima. No
USDT/USD/USDC equivalence or automatic FX/contract conversion is assumed.

Maker prices retain the reference/basis model and inventory skew, but are also
bounded by executable hedge BBO prices, configured fees, slippage and min profit.
Only visible top-of-book size is checked; execution at the assumed price/fee is
not guaranteed. Per-venue limits remain independent. Gross PnL is in maker tick
* maker lot; actual fees/funding/net equity belong to Titan's platform ledger.

Fresh starts require both NET positions flat and both markets free of orders.
Stop/loss pulls maker orders, hedges known remaining debt, and waits for outcomes
and both positions. It RETAINS matched cross-exchange positions, not flat accounts.
Pause-safe is deliberately NOT advertised: a hedge may open a second-venue
position, while the current platform pause gate permits only reduce-only orders.
Recovery needs host-proven order outcomes; old reference_mm checkpoints cannot
migrate to this renamed type / source 2.0.2 / recovery schema 2.

Compile on titan-build (no separate parameter file):
  printf '{}' | titan strategy compile --strategy strategies/cross_exchange_mm.py \
    --parameters /dev/stdin --artifact-format bundle --output cross_exchange_mm.titan
Titan owns connectivity, order reconciliation, persistence and account facts.
Bullet still needs a connector. Controlled live execution_limits currently only
supports one market binding; do not disable that gate for this two-market strategy.
"""

import numpy as np
from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy import testing
from titan_strategy.parameters import FloatParam, IntParam
from titan_strategy.observation import BusinessObservationSpec, ObservationField
from titan_strategy.state import array, float64, int64, new_state, record, uint64
from titan_strategy.types import EventKind, EventQos

BUY, SELL, LIMIT, IOC, POST_ONLY = 1, 2, 1, 2, 4
# Declared intent carries in the submit request's first reserved byte. The platform verifies
# it against the bound leg roles once a multi-binding envelope is configured; on a legacy
# single-market instance it is declarative only and grants no admission relaxation.
MAKER_QUOTE, KNOWN_FILL_HEDGE = 1, 2
PENDING, ACCEPTED, PARTIAL, FILLED, CANCEL_PENDING = 1, 2, 3, 4, 5
CANCELED, REJECTED, EXPIRED, UNKNOWN = 6, 7, 8, 255
READY, HEDGE, MAX_RETIRED, SAFE = 4, 2, 128, 2**52

config_dtype = record(
    account_no=int64, hedge_account_no=int64, reference_asset_no=int64, traded_asset_no=int64,
    order_qty_lots=int64, max_position_lots=int64, hedge_lots_per_maker_lot=int64,
    max_hedge_position_lots=int64, max_unhedged_lots=int64, hedge_min_order_lots=int64,
    spread_bps=float64, skew_bps=float64, requote_threshold_bps=float64,
    reference_tick_ratio=float64, maker_fee_bps=float64, hedge_fee_bps=float64,
    min_profit_bps=float64, hedge_slippage_bps=float64, hedge_min_notional_tick_lots=float64,
    max_gross_loss_tick_lots=float64, max_notional_tick_lots=float64,
    max_hedge_notional_tick_lots=float64, warmup_ns=int64, basis_halflife_ns=int64,
    reference_stale_ns=int64, venue_stale_ns=int64, min_requote_ns=int64,
    order_timeout_ns=int64, hedge_timeout_ns=int64, retry_backoff_ns=int64,
    max_rejections=int64, max_hedge_attempts=int64,
)
runtime_dtype = record(
    started=int64, error=int64, stopped=int64, loss_latched=int64, risk_latched=int64,
    account_epoch=uint64, hedge_epoch=uint64, last_now_ns=int64,
    reference_ts_ns=int64, venue_ts_ns=int64, basis_start_ns=int64, basis_update_ns=int64,
    basis=float64, fair_ticks=float64, expected_position_lots=int64, hedge_position_lots=int64,
    cash_tick_lots=float64, hedge_cash_tick_lots=float64, gross_pnl_tick_lots=float64,
    position_wait_ns=int64, debt_started_ns=int64, debt_side=int64, hedge_anchor_ticks=int64,
    next_hedge_ns=int64, hedge_attempts=int64, quotes_draining=int64,
    retired_cursor=int64, fill_count=int64, hedge_fill_count=int64, rejections=int64,
)
ref_dtype = record(
    order_id=uint64, account_no=int64, asset_no=int64, side=int64, tif=int64,
    price_ticks=int64, qty_lots=int64, applied_lots=int64, reported_lots=int64,
    terminal=int64, terminal_lots=int64, created_ns=int64, missing_ns=int64,
    fill_wait_ns=int64, cancel_sent_ns=int64, next_quote_ns=int64, sequence=uint64,
)
retired_dtype = record(order_id=uint64, account_no=int64, asset_no=int64, side=int64,
    applied_lots=int64, price_ticks=int64, qty_lots=int64)
state_dtype = record(config=config_dtype, mm=runtime_dtype,
    refs=array(ref_dtype, 3), retired=array(retired_dtype, MAX_RETIRED))


def integer(name, default, minimum=1, maximum=SAFE):
    return IntParam(name, required=False, default=default, minimum=minimum, maximum=maximum)


def positive(name, default, maximum=SAFE):
    return FloatParam(name, required=False, default=default, minimum=0.0,
        exclusive_minimum=True, maximum=maximum)


MM_OBSERVATION = BusinessObservationSpec(
    schema_id="titan.cross_exchange_mm", schema_version=1,
    fields=tuple(ObservationField(name, kind, unit, "mm." + field, label=label)
        for name, kind, unit, field, label in (
            ("last_error", "int64", "code", "error", "业务错误码"),
            ("stopped", "int64", "flag", "stopped", "停止请求"),
            ("loss_latched", "int64", "flag", "loss_latched", "亏损锁存"),
            ("risk_latched", "int64", "flag", "risk_latched", "风险锁存"),
            ("maker_inventory_lots", "int64", "maker_lot", "expected_position_lots", "做市库存"),
            ("hedge_inventory_lots", "int64", "hedge_lot", "hedge_position_lots", "对冲库存"),
            ("debt_started_ns", "int64", "ns", "debt_started_ns", "原敞口开始时间"),
            ("hedge_attempts", "int64", "count", "hedge_attempts", "对冲尝试次数"),
            ("gross_pnl_tick_lots", "decimal", "maker_tick_lot", "gross_pnl_tick_lots", "两腿毛收益"),
            ("reference_at_ns", "int64", "ns", "reference_ts_ns", "对冲行情时间"),
            ("maker_at_ns", "int64", "ns", "venue_ts_ns", "做市行情时间"),
        )),
)

SPEC = StrategySpec(
    strategy_type="cross_exchange_mm", source_version="2.0.3", recovery_state_schema_version=2,
    parameters=(
        integer("account_no", 0, 0, 2**32 - 1), integer("hedge_account_no", 1, 0, 2**32 - 1),
        integer("reference_asset_no", 0, 0, 2**32 - 1), integer("traded_asset_no", 1, 0, 2**32 - 1),
        integer("order_qty_lots", 5, maximum=10**9), integer("max_position_lots", 10, maximum=10**9),
        integer("max_hedge_position_lots", 10, maximum=10**9),
        integer("max_unhedged_lots", 10, maximum=10**9),
        positive("spread_bps", 10.0, 1000.0),
        FloatParam("skew_bps", required=False, default=5.0, minimum=0.0, maximum=1000.0),
        FloatParam("min_profit_bps", required=False, default=2.0, minimum=0.0, maximum=1000.0),
        FloatParam("hedge_slippage_bps", required=False, default=5.0, minimum=0.0, maximum=1000.0),
        positive("max_gross_loss_tick_lots", 2000.0), positive("max_notional_tick_lots", 10**12),
        positive("max_hedge_notional_tick_lots", 10**12),
        integer("warmup_ns", 60_000_000_000, maximum=10**15),
        integer("basis_halflife_ns", 300_000_000_000, maximum=10**15),
        integer("reference_stale_ns", 5_000_000_000, maximum=10**15),
        integer("venue_stale_ns", 10_000_000_000, maximum=10**15),
        integer("min_requote_ns", 500_000_000, 0, 10**15),
        integer("order_timeout_ns", 10_000_000_000, maximum=10**15),
        integer("hedge_timeout_ns", 10_000_000_000, maximum=10**15),
        integer("retry_backoff_ns", 500_000_000, maximum=10**15),
        integer("max_rejections", 5, maximum=1000), integer("max_hedge_attempts", 5, maximum=1000),
    ),
    subscriptions=tuple(EventSubscription(kind, name, 2 if kind == EventKind.FILL else 1, qos)
        for kind, name, qos in (
            (EventKind.BBO, "on_tick", EventQos.LATEST),
            (EventKind.FILL, "on_fill", EventQos.RELIABLE_ORDERED),
            (EventKind.ORDER, "on_order", EventQos.RELIABLE_ORDERED),
            (EventKind.CANCEL, "on_cancel", EventQos.RELIABLE_ORDERED),
            (EventKind.POSITION, "on_position", EventQos.RELIABLE_ORDERED),
            (EventKind.ACCOUNT_STATE, "on_account_state", EventQos.RELIABLE_ORDERED),
            (EventKind.TIMER, "on_timer", EventQos.RELIABLE_ORDERED),
        )),
    capabilities=(Capability.MARKET_DATA | Capability.ACCOUNT_DATA | Capability.ORDER_EXECUTION
        | Capability.TIMER | Capability.STOP_BARRIER | Capability.RECOVERY_ORDER_RESULTS
        | Capability.BUSINESS_FAULT_DRAIN | Capability.REQUIRES_PERIODIC_TIMER
        | Capability.OBJECT_CONTEXT),
)


@njit
def fail(s, code):
    if s["mm"]["error"] == 0:
        s["mm"]["error"] = code


@njit
def business_result(s):
    g = s["mm"]
    if g["error"]:
        return int(g["error"])
    if g["loss_latched"]:
        return 302
    return 301 if g["risk_latched"] else 0


@njit
def terminal(status):
    return status in (FILLED, CANCELED, REJECTED, EXPIRED)


@njit
def find(rows, order_id):
    for i in range(len(rows)):
        if order_id and rows[i]["order_id"] == order_id:
            return i
    return -1


@njit
def relevant(c, account, asset):
    return ((account == c["account_no"] and asset == c["traded_asset_no"])
        or (account == c["hedge_account_no"] and asset == c["reference_asset_no"]))


@njit
def fresh(now, timestamp, limit):
    return timestamp > 0 and timestamp <= now and now - timestamp <= limit


@njit
def valid_book(book):
    return 0 < book["best_bid_ticks"] < book["best_ask_ticks"] <= SAFE


@njit
def reset_prices(s):
    g = s["mm"]
    g["basis_start_ns"], g["basis_update_ns"], g["fair_ticks"] = 0, 0, 0.0


@njit
def debt(s):
    return (s["mm"]["expected_position_lots"] * s["config"]["hedge_lots_per_maker_lot"]
        + s["mm"]["hedge_position_lots"])


@njit
def matches(r, order):
    return (order["account_no"] == r["account_no"] and order["asset_no"] == r["asset_no"]
        and order["position_side"] == 0 and order["side"] == r["side"]
        and order["price_ticks"] == r["price_ticks"] and order["qty_lots"] == r["qty_lots"]
        and order["order_type"] == LIMIT and order["time_in_force"] == r["tif"]
        and order["reduce_only"] == 0 and 0 <= order["cumulative_filled_lots"] <= r["qty_lots"])


@njit
def old_matches(old, account, asset, price, qty, cumulative):
    return (old["account_no"] == account and old["asset_no"] == asset
        and old["price_ticks"] == price and old["qty_lots"] == qty
        and old["applied_lots"] == cumulative)


@njit
def observe(s, r, cumulative, now):
    if cumulative < 0 or cumulative > r["qty_lots"]:
        fail(s, 104)
        return
    r["reported_lots"] = max(r["reported_lots"], cumulative)
    if r["reported_lots"] > r["applied_lots"] and r["fill_wait_ns"] == 0:
        r["fill_wait_ns"] = now


@njit
def finish(s, index, now):
    r, g = s["refs"][index], s["mm"]
    if (not r["terminal"] or r["applied_lots"] != r["terminal_lots"]
            or r["applied_lots"] < r["reported_lots"]):
        return
    old = s["retired"][g["retired_cursor"]]
    old["order_id"], old["account_no"], old["asset_no"] = r["order_id"], r["account_no"], r["asset_no"]
    old["side"], old["applied_lots"] = r["side"], r["applied_lots"]
    old["price_ticks"], old["qty_lots"] = r["price_ticks"], r["qty_lots"]
    g["retired_cursor"] = (g["retired_cursor"] + 1) % MAX_RETIRED
    if index == HEDGE:
        g["next_hedge_ns"] = now + s["config"]["retry_backoff_ns"]
    r["order_id"] = 0


@njit
def cancel(ctx, index):
    s, c, r = ctx.state, ctx.state["config"], ctx.state["refs"][index]
    if not r["order_id"] or r["terminal"] or r["cancel_sent_ns"]:
        return
    active = ctx.active_orders()
    i = find(active, r["order_id"])
    if i < 0 or active[i]["status"] in (PENDING, UNKNOWN, CANCEL_PENDING) or terminal(active[i]["status"]):
        return
    if ctx.cancel_order(r["account_no"], r["asset_no"], r["order_id"]) == 0:
        fail(s, 110)
    else:
        r["cancel_sent_ns"] = ctx.now
        r["next_quote_ns"] = max(r["next_quote_ns"], ctx.now + c["min_requote_ns"])


@njit
def pull(ctx):
    cancel(ctx, 0)
    cancel(ctx, 1)


@njit
def audit(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    active = ctx.active_orders()
    execution_ready = True
    for order in active:
        if not relevant(c, order["account_no"], order["asset_no"]):
            continue
        i = find(s["refs"], order["order_id"])
        if i < 0:
            j = find(s["retired"], order["order_id"])
            if (j >= 0 and terminal(order["status"]) and order["position_side"] == 0
                    and order["side"] == s["retired"][j]["side"]
                    and old_matches(s["retired"][j], order["account_no"], order["asset_no"],
                        order["price_ticks"], order["qty_lots"], order["cumulative_filled_lots"])):
                continue
            fail(s, 103)
            continue
        r = s["refs"][i]
        if (not matches(r, order)
                or order["status"] not in (PENDING, ACCEPTED, PARTIAL, FILLED, CANCEL_PENDING, CANCELED, REJECTED, EXPIRED)):
            fail(s, 104)
            continue
        observe(s, r, order["cumulative_filled_lots"], ctx.now)
    for r in s["refs"]:
        if not r["order_id"]:
            continue
        i = find(active, r["order_id"])
        if not r["terminal"] and (i < 0 or terminal(active[i]["status"])):
            execution_ready = False
            if r["missing_ns"] == 0:
                r["missing_ns"] = ctx.now
        else:
            r["missing_ns"] = 0
        if r["reported_lots"] > r["applied_lots"]:
            execution_ready = False
        for at in (r["missing_ns"], r["fill_wait_ns"], r["cancel_sent_ns"]):
            if at and ctx.now - at >= c["order_timeout_ns"]:
                fail(s, 105)
        if i >= 0 and active[i]["status"] == PENDING and ctx.now - r["created_ns"] >= c["order_timeout_ns"]:
            fail(s, 105)
    maker = ctx.position(c["account_no"], c["traded_asset_no"])
    hedge = ctx.position(c["hedge_account_no"], c["reference_asset_no"])
    positions_ready = (maker["qty_lots"] == g["expected_position_lots"]
        and hedge["qty_lots"] == g["hedge_position_lots"])
    if not positions_ready or not execution_ready:
        if g["position_wait_ns"] == 0:
            g["position_wait_ns"] = ctx.now
        if ctx.now - g["position_wait_ns"] >= c["order_timeout_ns"]:
            fail(s, 108)
    else:
        g["position_wait_ns"] = 0
    return positions_ready, execution_ready


@njit
def prices(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    ref, venue = ctx.market(c["reference_asset_no"]), ctx.market(c["traded_asset_no"])
    if not (fresh(ctx.now, g["reference_ts_ns"], c["reference_stale_ns"])
            and fresh(ctx.now, g["venue_ts_ns"], c["venue_stale_ns"])
            and valid_book(ref) and valid_book(venue)):
        reset_prices(s)
        return False
    ref_mid = (float(ref["best_bid_ticks"]) + ref["best_ask_ticks"]) * 0.5 * c["reference_tick_ratio"]
    venue_mid = (float(venue["best_bid_ticks"]) + venue["best_ask_ticks"]) * 0.5
    if not np.isfinite(ref_mid) or not 0 < ref_mid <= SAFE:
        fail(s, 109)
        return False
    sample = venue_mid - ref_mid
    if g["basis_start_ns"] == 0:
        g["basis"], g["basis_start_ns"], g["basis_update_ns"] = sample, ctx.now, g["venue_ts_ns"]
    elif g["venue_ts_ns"] > g["basis_update_ns"]:
        elapsed = g["venue_ts_ns"] - g["basis_update_ns"]
        age = max(elapsed, ctx.now - g["basis_start_ns"])
        weight = max(1.0 - np.exp2(-elapsed / c["basis_halflife_ns"]), elapsed / age)
        g["basis"] += min(1.0, weight) * (sample - g["basis"])
        g["basis_update_ns"] = g["venue_ts_ns"]
    g["fair_ticks"] = ref_mid + g["basis"]
    return ctx.now - g["basis_start_ns"] >= c["warmup_ns"]


@njit
def protected_price(book, side, bps):
    if side == SELL:
        return max(1, int(np.ceil(book["best_bid_ticks"] * (1.0 - bps / 10000.0))))
    return int(np.floor(book["best_ask_ticks"] * (1.0 + bps / 10000.0)))


@njit
def submit(ctx, index, side, qty, price):
    s, c, r = ctx.state, ctx.state["config"], ctx.state["refs"][index]
    account = c["hedge_account_no"] if index == HEDGE else c["account_no"]
    asset = c["reference_asset_no"] if index == HEDGE else c["traded_asset_no"]
    tif = IOC if index == HEDGE else POST_ONLY
    if qty <= 0 or price <= 0 or price > SAFE // qty:
        fail(s, 109)
        return
    purpose = KNOWN_FILL_HEDGE if index == HEDGE else MAKER_QUOTE
    oid = ctx.submit_order(account, asset, side, LIMIT, qty, price, tif, False, 0, purpose)
    if oid == 0:
        fail(s, 110)
        return
    r["order_id"], r["account_no"], r["asset_no"], r["side"], r["tif"] = oid, account, asset, side, tif
    r["price_ticks"], r["qty_lots"] = price, qty
    r["applied_lots"], r["reported_lots"], r["terminal"], r["terminal_lots"] = 0, 0, 0, 0
    r["created_ns"], r["missing_ns"], r["fill_wait_ns"], r["cancel_sent_ns"] = ctx.now, 0, 0, 0
    r["sequence"], r["next_quote_ns"] = 0, ctx.now + c["min_requote_ns"]


@njit
def drive_hedge(ctx, execution_ready):
    s, c, g, r = ctx.state, ctx.state["config"], ctx.state["mm"], ctx.state["refs"][HEDGE]
    net = debt(s)
    if net == 0 and not r["order_id"]:
        g["debt_started_ns"], g["debt_side"], g["hedge_anchor_ticks"] = 0, 0, 0
        g["hedge_attempts"], g["next_hedge_ns"] = 0, 0
        return
    g["quotes_draining"] = 1
    if g["debt_started_ns"] == 0:
        g["debt_started_ns"] = ctx.now
    if abs(net) > c["max_unhedged_lots"]:
        g["risk_latched"] = 1  # Still hedge known debt; permanently stop new maker risk.
    if ctx.now - g["debt_started_ns"] >= c["hedge_timeout_ns"]:
        fail(s, 201)
    if g["error"]:
        cancel(ctx, HEDGE)
        return
    side = SELL if net > 0 else BUY
    if r["order_id"]:
        leaves = r["qty_lots"] - r["applied_lots"]
        if net == 0 or side != r["side"] or leaves > abs(net):
            cancel(ctx, HEDGE)
        return  # Pending, cancel-pending and terminal-awaiting-fill all reserve this slot.
    if not execution_ready or ctx.is_paused or ctx.now < g["next_hedge_ns"]:
        return
    book = ctx.market(c["reference_asset_no"])
    if not valid_book(book) or not fresh(ctx.now, g["reference_ts_ns"], c["reference_stale_ns"]):
        return  # Clock/deadline keeps running while market data is stale.
    if g["hedge_attempts"] >= c["max_hedge_attempts"]:
        fail(s, 202)
        return
    if g["debt_side"] != side or g["hedge_anchor_ticks"] == 0:
        g["debt_side"] = side
        g["hedge_anchor_ticks"] = book["best_bid_ticks"] if side == SELL else book["best_ask_ticks"]
    price = protected_price(book, side, c["hedge_slippage_bps"])
    if side == SELL:
        anchor = int(np.ceil(g["hedge_anchor_ticks"] * (1.0 - c["hedge_slippage_bps"] / 10000.0)))
        price = max(price, anchor)
    else:
        anchor = int(np.floor(g["hedge_anchor_ticks"] * (1.0 + c["hedge_slippage_bps"] / 10000.0)))
        price = min(price, anchor)
    qty = abs(net)
    after = g["hedge_position_lots"] + (qty if side == BUY else -qty)
    if (qty < c["hedge_min_order_lots"] or qty * float(price) < c["hedge_min_notional_tick_lots"]
            or abs(after) > c["max_hedge_position_lots"]
            or max(abs(after), abs(g["hedge_position_lots"])) * float(max(price, book["best_ask_ticks"])) > c["max_hedge_notional_tick_lots"]):
        fail(s, 203)
        return
    submit(ctx, HEDGE, side, qty, price)
    if not g["error"]:
        g["hedge_attempts"] += 1


@njit
def quote_safe(ctx, index, price):
    """Audit live reservations as well as candidates, independent of requote throttling."""
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    hedge, venue = ctx.market(c["reference_asset_no"]), ctx.market(c["traded_asset_no"])
    side = BUY if index == 0 else SELL
    current = s["refs"][index]
    qty = current["qty_lots"] - current["applied_lots"] if current["order_id"] else c["order_qty_lots"]
    hedge_price = protected_price(hedge, SELL if side == BUY else BUY, c["hedge_slippage_bps"])
    available = hedge["best_bid_qty_lots"] if side == BUY else hedge["best_ask_qty_lots"]
    if (qty <= 0 or available < qty * c["hedge_lots_per_maker_lot"]
            or c["hedge_lots_per_maker_lot"] * float(hedge_price) < c["hedge_min_notional_tick_lots"]):
        return False
    high, low = g["expected_position_lots"], g["expected_position_lots"]
    mark = max(price, venue["best_ask_ticks"])
    for i in range(2):
        r = s["refs"][i]
        if r["order_id"]:
            leaves = r["qty_lots"] - r["applied_lots"]
            if r["side"] == BUY:
                high += leaves
            else:
                low -= leaves
            mark = max(mark, r["price_ticks"])
    if not current["order_id"]:
        if side == BUY:
            high += qty
        else:
            low -= qty
    worst = max(abs(high), abs(low))
    return (high <= c["max_position_lots"] and low >= -c["max_position_lots"]
        and worst * float(mark) <= c["max_notional_tick_lots"]
        and worst * c["hedge_lots_per_maker_lot"] <= c["max_hedge_position_lots"]
        and worst * c["hedge_lots_per_maker_lot"] * float(max(hedge_price, hedge["best_ask_ticks"]))
            <= c["max_hedge_notional_tick_lots"])


@njit
def quote_side(ctx, index, price):
    s, c, g, r = ctx.state, ctx.state["config"], ctx.state["mm"], ctx.state["refs"][index]
    side, qty = BUY if index == 0 else SELL, c["order_qty_lots"]
    hedge, venue = ctx.market(c["reference_asset_no"]), ctx.market(c["traded_asset_no"])
    if not quote_safe(ctx, index, price):
        price = 0
    hedge_side = SELL if side == BUY else BUY
    hedge_price = protected_price(hedge, hedge_side, c["hedge_slippage_bps"])
    mf, hf, edge = c["maker_fee_bps"] / 10000.0, c["hedge_fee_bps"] / 10000.0, c["min_profit_bps"] / 10000.0
    if side == BUY:
        cap = int(np.floor(hedge_price * c["reference_tick_ratio"] * (1.0 - hf) / ((1.0 + mf) * (1.0 + edge))))
        price = min(price, cap)
        profitable = not r["order_id"] or r["price_ticks"] <= cap
    else:
        cap = int(np.ceil(hedge_price * c["reference_tick_ratio"] * (1.0 + hf) * (1.0 + edge) / (1.0 - mf)))
        price = max(price, cap) if price > 0 else 0
        profitable = not r["order_id"] or r["price_ticks"] >= cap
    if r["order_id"]:
        crossing = r["price_ticks"] >= venue["best_ask_ticks"] if side == BUY else r["price_ticks"] <= venue["best_bid_ticks"]
        if price <= 0 or crossing or not profitable:
            cancel(ctx, index)
        elif (ctx.now >= r["next_quote_ns"] and not r["terminal"]
                and abs(r["price_ticks"] - price) * 10000.0 / price >= c["requote_threshold_bps"]):
            cancel(ctx, index)
        return
    if price <= 0 or ctx.now < r["next_quote_ns"]:
        return
    submit(ctx, index, side, qty, price)


@njit
def drive(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    if ctx.now <= 0 or ctx.now < g["last_now_ns"]:
        fail(s, 112)
    g["last_now_ns"] = ctx.now
    maker, hedge = ctx.account(c["account_no"]), ctx.account(c["hedge_account_no"])
    if (maker["state"] != READY or hedge["state"] != READY
            or maker["account_epoch"] != g["account_epoch"] or hedge["account_epoch"] != g["hedge_epoch"]):
        fail(s, 111)
    positions_ready, execution_ready = audit(ctx)
    if (abs(g["expected_position_lots"]) > c["max_position_lots"]
            or abs(g["hedge_position_lots"]) > c["max_hedge_position_lots"]
            or not np.isfinite(g["cash_tick_lots"]) or abs(g["cash_tick_lots"]) > SAFE
            or not np.isfinite(g["hedge_cash_tick_lots"]) or abs(g["hedge_cash_tick_lots"]) > SAFE):
        fail(s, 109)
    market_ready = prices(ctx)
    a, b = ctx.market(c["traded_asset_no"]), ctx.market(c["reference_asset_no"])
    if (valid_book(a) and valid_book(b)
            and fresh(ctx.now, g["venue_ts_ns"], c["venue_stale_ns"])
            and fresh(ctx.now, g["reference_ts_ns"], c["reference_stale_ns"])):
        mid_a = (float(a["best_bid_ticks"]) + a["best_ask_ticks"]) * 0.5
        mid_b = (float(b["best_bid_ticks"]) + b["best_ask_ticks"]) * 0.5
        g["gross_pnl_tick_lots"] = (g["cash_tick_lots"] + g["expected_position_lots"] * mid_a
            + (g["hedge_cash_tick_lots"] + g["hedge_position_lots"] * mid_b)
            * c["reference_tick_ratio"] / c["hedge_lots_per_maker_lot"])
        if g["gross_pnl_tick_lots"] <= -c["max_gross_loss_tick_lots"]:
            g["loss_latched"] = 1
        if (abs(g["expected_position_lots"]) * float(a["best_ask_ticks"]) > c["max_notional_tick_lots"]
                or abs(g["hedge_position_lots"]) * float(b["best_ask_ticks"]) > c["max_hedge_notional_tick_lots"]):
            g["risk_latched"] = 1
        for i in range(2):
            if s["refs"][i]["order_id"] and not quote_safe(ctx, i, s["refs"][i]["price_ticks"]):
                cancel(ctx, i)
    drive_hedge(ctx, execution_ready)
    if g["quotes_draining"]:
        pull(ctx)
        if not s["refs"][0]["order_id"] and not s["refs"][1]["order_id"]:
            g["quotes_draining"] = 0
    if (g["error"] or g["stopped"] or g["loss_latched"] or g["risk_latched"] or ctx.is_paused
            or debt(s) != 0 or s["refs"][HEDGE]["order_id"] or g["quotes_draining"]
            or not positions_ready or not execution_ready or not market_ready):
        pull(ctx)
        if g["error"]:
            cancel(ctx, HEDGE)
        if ((g["stopped"] or business_result(s)) and positions_ready and execution_ready
                and debt(s) == 0 and not s["refs"][HEDGE]["order_id"]
                and not s["refs"][0]["order_id"] and not s["refs"][1]["order_id"]):
            ctx.complete_stop()
        return business_result(s)
    fair = g["fair_ticks"]
    if not np.isfinite(fair) or not 0 < fair <= SAFE:
        fail(s, 109)
        pull(ctx)
        return business_result(s)
    venue = ctx.market(c["traded_asset_no"])
    lean = c["skew_bps"] * max(-1.0, min(1.0, g["expected_position_lots"] / c["max_position_lots"]))
    bid = int(np.floor(min(fair * (1.0 - (c["spread_bps"] + lean) / 10000.0), venue["best_ask_ticks"] - 1)))
    ask = int(np.ceil(max(fair * (1.0 + (c["spread_bps"] - lean) / 10000.0), venue["best_bid_ticks"] + 1)))
    quote_side(ctx, 0, bid)
    if not g["error"]:
        quote_side(ctx, 1, ask)
    return business_result(s)


@njit
def unit_float(value):
    result = float(value["coefficient"])
    for _ in range(value["scale"]):
        result = result / 10.0
    return result


@njit
def on_start(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    maker, hedge = ctx.account(c["account_no"]), ctx.account(c["hedge_account_no"])
    pos = ctx.position(c["account_no"], c["traded_asset_no"])
    hp = ctx.position(c["hedge_account_no"], c["reference_asset_no"])
    if maker["state"] != READY or hedge["state"] != READY or pos["position_side"] != 0 or hp["position_side"] != 0 or g["error"]:
        fail(s, 101)
        return -1
    if not g["started"]:
        # Exchange facts come from the object table, never from strategy parameters.
        pair = ctx.pair(0)
        hedge_symbol = ctx.symbol(c["reference_asset_no"])
        tick = unit_float(hedge_symbol["price_tick"])
        lot = unit_float(hedge_symbol["quantity_lot"])
        c["hedge_lots_per_maker_lot"] = int(pair["hedge_lots_per_maker_lot"])
        c["hedge_min_order_lots"] = int(pair["min_hedge_lots"])
        denominator = int(pair["hedge_ticks_per_maker_tick_den"])
        if denominator > 0:
            c["reference_tick_ratio"] = (float(pair["hedge_ticks_per_maker_tick_num"])
                / float(denominator))
        if tick > 0.0 and lot > 0.0:
            c["hedge_min_notional_tick_lots"] = (
                unit_float(pair["min_hedge_notional_quote"]) / (tick * lot))
        maker_fee = ctx.fee(c["account_no"])
        hedge_fee = ctx.fee(c["hedge_account_no"])
        c["maker_fee_bps"] = float(maker_fee["maker_fee_bps"]) if maker_fee["flags"] & 1 else 0.0
        c["hedge_fee_bps"] = float(hedge_fee["taker_fee_bps"]) if hedge_fee["flags"] & 1 else 0.0
        if (c["hedge_lots_per_maker_lot"] <= 0
                or c["hedge_min_order_lots"] > c["hedge_lots_per_maker_lot"]):
            fail(s, 103)
            return -1
        if c["max_position_lots"] * c["hedge_lots_per_maker_lot"] > c["max_hedge_position_lots"]:
            fail(s, 104)
            return -1
        if c["order_qty_lots"] * c["hedge_lots_per_maker_lot"] > c["max_unhedged_lots"]:
            fail(s, 105)
            return -1
        dirty = pos["qty_lots"] != 0 or hp["qty_lots"] != 0
        for order in ctx.active_orders():
            if relevant(c, order["account_no"], order["asset_no"]):
                dirty = True
        if dirty:
            fail(s, 102)
            return -1
        g["started"], g["account_epoch"], g["hedge_epoch"] = 1, maker["account_epoch"], hedge["account_epoch"]
    elif pos["qty_lots"] != g["expected_position_lots"] or hp["qty_lots"] != g["hedge_position_lots"]:
        fail(s, 108)
        return -1
    return 0


@njit
def on_tick(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    if (not fresh(ctx.now, g["reference_ts_ns"], c["reference_stale_ns"])
            or not fresh(ctx.now, g["venue_ts_ns"], c["venue_stale_ns"])):
        reset_prices(s)
    for tick in ctx.ticks():
        at = tick["receive_ts_ns"]
        if not 0 < at <= ctx.now:
            continue
        if tick["asset_no"] == c["reference_asset_no"]:
            g["reference_ts_ns"] = max(g["reference_ts_ns"], at)
        elif tick["asset_no"] == c["traded_asset_no"]:
            g["venue_ts_ns"] = max(g["venue_ts_ns"], at)
    return drive(ctx)


@njit
def on_fill(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    for f in ctx.fills():
        if not relevant(c, f["account_no"], f["asset_no"]):
            continue
        i, cumulative = find(s["refs"], f["order_id"]), f["cumulative_filled_lots"]
        if i < 0:
            j = find(s["retired"], f["order_id"])
            if (j >= 0 and f["account_no"] == s["retired"][j]["account_no"]
                    and f["asset_no"] == s["retired"][j]["asset_no"] and f["side"] == s["retired"][j]["side"]
                    and 0 < cumulative <= s["retired"][j]["applied_lots"]):
                continue
            fail(s, 107)
            continue
        r = s["refs"][i]
        if (f["account_no"] != r["account_no"] or f["asset_no"] != r["asset_no"]
                or f["side"] != r["side"] or cumulative <= 0):
            fail(s, 106)
            continue
        if cumulative <= r["applied_lots"]:
            continue
        delta = cumulative - r["applied_lots"]
        if (delta != f["fill_qty_lots"] or cumulative > r["qty_lots"]
                or not 0 < f["fill_price_ticks"] <= SAFE // r["qty_lots"]
                or (r["terminal"] and cumulative > r["terminal_lots"])
                or (f["final_fill"] and cumulative != r["qty_lots"])):
            fail(s, 106)
            continue
        signed = delta if r["side"] == BUY else -delta
        if i == HEDGE:
            g["hedge_position_lots"] += signed
            g["hedge_cash_tick_lots"] -= signed * float(f["fill_price_ticks"])
            g["hedge_fill_count"] += 1
        else:
            g["expected_position_lots"] += signed
            g["cash_tick_lots"] -= signed * float(f["fill_price_ticks"])
            g["rejections"], g["quotes_draining"] = 0, 1
        g["fill_count"] += 1
        r["applied_lots"], r["sequence"] = cumulative, max(r["sequence"], f["account_sequence"])
        observe(s, r, cumulative, ctx.now)
        if cumulative == r["reported_lots"]:
            r["fill_wait_ns"] = 0
        if cumulative == r["qty_lots"]:
            if r["terminal"] and r["terminal"] != FILLED:
                fail(s, 104)
            r["terminal"], r["terminal_lots"] = FILLED, cumulative
        finish(s, i, ctx.now)
    return drive(ctx)


@njit
def on_order(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    for event in ctx.order_events():
        if not relevant(c, event["account_no"], event["asset_no"]):
            continue
        i = find(s["refs"], event["order_id"])
        if i < 0:
            j = find(s["retired"], event["order_id"])
            if (j < 0 or not terminal(event["status"]) or event["position_side"] != 0
                    or not old_matches(s["retired"][j], event["account_no"], event["asset_no"],
                        event["price_ticks"], event["qty_lots"], event["cumulative_filled_lots"])):
                fail(s, 107)
            continue
        r = s["refs"][i]
        if event["account_sequence"] and event["account_sequence"] < r["sequence"]:
            continue
        if (event["account_no"] != r["account_no"] or event["asset_no"] != r["asset_no"]
                or event["qty_lots"] != r["qty_lots"] or event["price_ticks"] != r["price_ticks"]
                or event["position_side"] != 0 or event["status"] not in (PENDING, ACCEPTED, PARTIAL, FILLED, CANCEL_PENDING, CANCELED, REJECTED, EXPIRED)):
            fail(s, 104)
            continue
        if r["terminal"] and not terminal(event["status"]):
            continue
        observe(s, r, event["cumulative_filled_lots"], ctx.now)
        r["sequence"] = max(r["sequence"], event["account_sequence"])
        if terminal(event["status"]):
            if (event["cumulative_filled_lots"] < r["applied_lots"]
                    or (event["status"] == FILLED and event["cumulative_filled_lots"] != r["qty_lots"])
                    or (r["terminal"] and (r["terminal"] != event["status"] or r["terminal_lots"] != event["cumulative_filled_lots"]))):
                fail(s, 104)
                continue
            if not r["terminal"] and event["status"] == REJECTED and i != HEDGE:
                g["rejections"] += 1
                if g["rejections"] >= c["max_rejections"]:
                    g["risk_latched"] = 1
            r["terminal"], r["terminal_lots"], r["cancel_sent_ns"] = event["status"], event["cumulative_filled_lots"], 0
            r["next_quote_ns"] = max(r["next_quote_ns"], ctx.now + c["retry_backoff_ns"])
            finish(s, i, ctx.now)
    return drive(ctx)


@njit
def on_cancel(ctx):
    c = ctx.state["config"]
    for event in ctx.cancel_events():
        if relevant(c, event["account_no"], event["asset_no"]):
            i = find(ctx.state["refs"], event["order_id"])
            if i >= 0 and event["request_result"] == 2:
                fail(ctx.state, 110)
    return drive(ctx)


@njit
def on_position(ctx):
    return drive(ctx)


@njit
def on_account_state(ctx):
    return drive(ctx)


@njit
def on_timer(ctx):
    return drive(ctx)


@njit
def on_stop(ctx):
    ctx.state["mm"]["stopped"] = 1
    return drive(ctx)


@njit
def on_recover(ctx):
    s, c, g = ctx.state, ctx.state["config"], ctx.state["mm"]
    a, b = ctx.account(c["account_no"]), ctx.account(c["hedge_account_no"])
    pa, pb = ctx.position(c["account_no"], c["traded_asset_no"]), ctx.position(c["hedge_account_no"], c["reference_asset_no"])
    if (g["error"] or not g["started"] or ctx.now < g["last_now_ns"] or a["state"] != READY or b["state"] != READY
            or pa["position_side"] != 0 or pb["position_side"] != 0
            or pa["qty_lots"] != g["expected_position_lots"] or pb["qty_lots"] != g["hedge_position_lots"]
            or (debt(s) != 0 and g["debt_started_ns"] == 0)
            or (g["debt_started_ns"] and ctx.now - g["debt_started_ns"] >= c["hedge_timeout_ns"])):
        return -1
    outcomes, active = ctx.recovery_orders(), ctx.active_orders()
    # Validate all proof before changing business state. Recovery emits no commands.
    for r in s["refs"]:
        if not r["order_id"]:
            continue
        proof, live = find(outcomes, r["order_id"]), find(active, r["order_id"])
        if (proof < 0 or not matches(r, outcomes[proof])
                or outcomes[proof]["cumulative_filled_lots"] != r["applied_lots"]
                or r["reported_lots"] != r["applied_lots"]):
            return -1
        if live < 0:
            if not terminal(outcomes[proof]["status"]):
                return -1
            if ((outcomes[proof]["status"] == FILLED and r["applied_lots"] != r["qty_lots"])
                    or (r["terminal"] and (r["terminal"] != outcomes[proof]["status"]
                        or r["terminal_lots"] != r["applied_lots"]))):
                return -1
        elif (not matches(r, active[live]) or active[live]["status"] not in (ACCEPTED, PARTIAL)
                or outcomes[proof]["status"] != active[live]["status"]
                or active[live]["cumulative_filled_lots"] != r["applied_lots"]
                or r["cancel_sent_ns"] or r["terminal"]):
            return -1
    for order in active:
        if relevant(c, order["account_no"], order["asset_no"]) and find(s["refs"], order["order_id"]) < 0:
            return -1
    for i in range(3):
        r = s["refs"][i]
        if r["order_id"] and find(active, r["order_id"]) < 0:
            proof = find(outcomes, r["order_id"])
            r["terminal"], r["terminal_lots"] = outcomes[proof]["status"], r["applied_lots"]
            finish(s, i, ctx.now)
    g["account_epoch"], g["hedge_epoch"], g["position_wait_ns"] = a["account_epoch"], b["account_epoch"], 0
    g["reference_ts_ns"], g["venue_ts_ns"] = 0, 0
    reset_prices(s)
    return 0


def build(parameters):
    p = SPEC.validate_parameters(parameters)
    if p["reference_asset_no"] == p["traded_asset_no"] or p["account_no"] == p["hedge_account_no"]:
        raise ValueError("maker and hedge require separate market and account bindings")
    if p["order_qty_lots"] > p["max_position_lots"] or p["skew_bps"] > p["spread_bps"]:
        raise ValueError("invalid maker size or inventory skew")
    state = new_state(state_dtype)
    for name, value in p.items():
        state[0]["config"][name] = value
    return StrategyDefinition(SPEC, state, {
        "on_start": on_start, "on_tick": on_tick, "on_fill": on_fill,
        "on_order": on_order, "on_cancel": on_cancel, "on_position": on_position,
        "on_account_state": on_account_state, "on_timer": on_timer,
        "on_stop": on_stop, "on_recover": on_recover,
    }, business_observation=MM_OBSERVATION)


def _test_markets(bid=9990, ask=10010, qty=1000):
    return [{"asset_no": asset, "best_bid_ticks": bid, "best_ask_ticks": ask,
             "best_bid_qty_lots": qty, "best_ask_qty_lots": qty} for asset in (0, 1)]


def _test_ticks(now_ns):
    return [{"asset_no": asset, "exchange_ts_ns": now_ns, "receive_ts_ns": now_ns}
            for asset in (0, 1)]


_TEST_ACCOUNTS = [{"account_no": account, "account_epoch": 1, "account_sequence": 1, "state": 4}
                  for account in (0, 1)]
_TEST_POSITIONS = [
    {"account_no": 0, "asset_no": 1, "account_sequence": 1, "qty_lots": 0, "position_side": 0},
    {"account_no": 1, "asset_no": 0, "account_sequence": 1, "qty_lots": 0, "position_side": 0},
]
# Default parameters quote both sides at now=2s: bid 9980 / ask 10022, five maker lots each.
_TEST_QUOTES = [
    {"order_id": 1, "account_no": 0, "asset_no": 1, "side": 1, "price_ticks": 9980,
     "qty_lots": 5, "order_type": 1, "time_in_force": 4, "status": 2, "account_sequence": 1,
     "created_ts_ns": 2_000_000_000, "updated_ts_ns": 2_000_000_000},
    {"order_id": 2, "account_no": 0, "asset_no": 1, "side": 2, "price_ticks": 10022,
     "qty_lots": 5, "order_type": 1, "time_in_force": 4, "status": 2, "account_sequence": 1,
     "created_ts_ns": 2_000_000_000, "updated_ts_ns": 2_000_000_000},
]

def _decimal(value):
    scale = 6
    return {"coefficient": int(round(value * 10**scale)), "scale": scale}


_TEST_OBJECTS = testing.objects(
    symbols=[
        dict(local_asset_no=0, asset_id=1, quote_currency_no=1, margin_currency_no=1, flags=1,
             price_tick=_decimal(0.1), quantity_lot=_decimal(0.01),
             contract_multiplier=_decimal(1.0), base_delta_per_lot=_decimal(0.01),
             min_quantity=_decimal(0.01), min_notional_quote=_decimal(5.0),
             min_qty_lots=1, min_notional_tick_lots=1),
        dict(local_asset_no=1, asset_id=2, quote_currency_no=1, margin_currency_no=1, flags=1,
             price_tick=_decimal(0.1), quantity_lot=_decimal(0.01),
             contract_multiplier=_decimal(1.0), base_delta_per_lot=_decimal(0.01),
             min_quantity=_decimal(0.01), min_notional_quote=_decimal(5.0),
             min_qty_lots=1, min_notional_tick_lots=1),
    ],
    accounts=[
        dict(local_account_no=0, account_id=1, quote_currency_no=1, margin_currency_no=1,
             currency_start=0, currency_count=1, flags=1),
        dict(local_account_no=1, account_id=2, quote_currency_no=1, margin_currency_no=1,
             currency_start=0, currency_count=1, flags=1),
    ],
    currencies=[
        dict(local_account_no=0, currency_no=1, flags=1, amount_unit=_decimal(0.000001)),
    ],
    pairs=[dict(maker_local_asset_no=1, hedge_local_asset_no=0,
                maker_local_account_no=0, hedge_local_account_no=1,
                quote_currency_no=1, flags=1, hedge_lots_per_maker_lot=1,
                hedge_ticks_per_maker_tick_num=1, hedge_ticks_per_maker_tick_den=1,
                min_hedge_lots=1, min_hedge_notional_quote=_decimal(0.001),
                pair_revision=1)],
    fees=[dict(local_account_no=0, flags=1, maker_fee_bps=0, taker_fee_bps=0),
          dict(local_account_no=1, flags=1, maker_fee_bps=4, taker_fee_bps=4)],
    status={"lifecycle": 3},
)

TESTS = tuple({**scene, "objects": _TEST_OBJECTS} for scene in (
    {"name": "missing_account_fails_closed", "steps": [{"callback": "on_start", "status": -1}],
     # Missing public views make the zero-filled accessors set last_error_code to -2; the
     # strategy itself then latches 101 and fails closed without staging any command.
     "expect": {"commands.submit_count": 0, "commands.cancel_count": 0,
                "context.last_error_code": -2, "mm.error": 101, "state_bytes": {"le": 65536}}},
    {"name": "warmup_then_two_sided_postonly_quotes",
     "state": {"config.warmup_ns": 1_000_000_000},
     "steps": [
         {"callback": "on_start",
          "views": {"accounts": _TEST_ACCOUNTS, "positions": _TEST_POSITIONS},
          "expect": {"mm.started": 1, "mm.error": 0, "commands.submit_count": 0}},
         {"callback": "on_tick", "now_ns": 1_000_000_000,
          "views": {"ticks": _test_ticks(1_000_000_000), "markets": _test_markets()},
          "expect": {"commands.submit_count": 0}},
         {"callback": "on_tick", "now_ns": 2_000_000_000,
          "views": {"ticks": _test_ticks(2_000_000_000), "markets": _test_markets()}},
     ],
     "expect": {"commands.submit_count": 2, "commands.cancel_count": 0, "mm.error": 0}},
    {"name": "resting_quotes_pulled_inside_requote_throttle_when_hedge_budget_breached",
     "state": {"config.warmup_ns": 1_000_000_000,
               "config.max_hedge_notional_tick_lots": 50080},
     "steps": [
         {"callback": "on_start",
          "views": {"accounts": _TEST_ACCOUNTS, "positions": _TEST_POSITIONS}},
         {"callback": "on_tick", "now_ns": 1_000_000_000,
          "views": {"ticks": _test_ticks(1_000_000_000), "markets": _test_markets()}},
         {"callback": "on_tick", "now_ns": 2_000_000_000,
          "views": {"ticks": _test_ticks(2_000_000_000), "markets": _test_markets()},
          "expect": {"commands.submit_count": 2}},
         {"callback": "on_tick", "now_ns": 2_000_000_001,
          "views": {"ticks": _test_ticks(2_000_000_001),
                    "markets": _test_markets(10000, 10020),
                    "active_orders": _TEST_QUOTES}},
     ],
     "expect": {"commands.cancel_count": {"ge": 1}, "commands.submit_count": 2,
                "mm.error": 0}},
))
