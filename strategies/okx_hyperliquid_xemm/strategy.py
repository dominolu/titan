"""Fixed-memory OKX/Hyperliquid cross-exchange market-making strategy.

The strategy is deliberately self-contained and uses Titan Strategy ABI v10 views. Prices
and quantities received from the ABI are integer ticks/lots represented as ``float64``. All order
commands are converted back to exact integer ticks/lots before submission.

ABI v10 supplies typed account state, command results, depth sequence metadata and live timers.
The strategy remains fail-closed until both bound account snapshots and both market views arrive.
"""

from math import ceil, floor
from types import SimpleNamespace

import numpy as np
from numba import njit


# Event codes produced by CanonicalStrategyEventAdapter.
DEPTH_EVENT = 2
BBO_EVENT = 4
BUY_SIDE = 1
SELL_SIDE = 2

# Account order status codes produced by connector::account_plugin::api_status.
ORDER_NEW = 1
ORDER_EXPIRED = 2
ORDER_FILLED = 3
ORDER_CANCELED = 4
ORDER_PARTIALLY_FILLED = 5
ORDER_REJECTED = 6

# Strategy modes.
MODE_WARMING_UP = 1
MODE_QUOTING = 2
MODE_HEDGE_ONLY = 3
MODE_PAUSED = 4
MODE_STOPPING = 5
MODE_FAULTED = 6

# Per-leg lifecycle.
LEG_EMPTY = 0
LEG_SUBMITTING = 1
LEG_OPEN = 2
LEG_CANCELING = 3

# Fixed state layout. Keep these constants stable across compatible package versions.
F_MAKER_BID = 0
F_MAKER_ASK = 1
F_HEDGE_SELL_VWAP = 2
F_HEDGE_BUY_VWAP = 3
F_HEDGE_SELL_LIMIT = 4
F_HEDGE_BUY_LIMIT = 5
F_TARGET_BID_TICKS = 6
F_TARGET_ASK_TICKS = 7
F_TARGET_BID_LOTS = 8
F_TARGET_ASK_LOTS = 9
F_UNHEDGED_BASE = 10
F_MAKER_POSITION_ESTIMATE = 11
F_HEDGE_POSITION_ESTIMATE = 12
F_ACTIVE_BID_TICKS = 13
F_ACTIVE_ASK_TICKS = 14
F_ACTIVE_BID_LOTS = 15
F_ACTIVE_ASK_LOTS = 16
F_ESTIMATED_BID_EDGE_BPS = 17
F_ESTIMATED_ASK_EDGE_BPS = 18
F_TOTAL_MAKER_FILL_BASE = 19
F_TOTAL_HEDGE_FILL_BASE = 20
F_HEDGE_AVAILABLE_BID_BASE = 21
F_HEDGE_AVAILABLE_ASK_BASE = 22
F_LAST_HEDGE_PRICE = 23

MAX_PRICE_SAMPLES = 64
F_BID_SAMPLES = 32
F_ASK_SAMPLES = F_BID_SAMPLES + MAX_PRICE_SAMPLES
F64_STATE_LEN = F_ASK_SAMPLES + MAX_PRICE_SAMPLES

I_MODE = 0
I_BID_ORDER_ID = 1
I_ASK_ORDER_ID = 2
I_HEDGE_ORDER_ID = 3
I_BID_STATE = 4
I_ASK_STATE = 5
I_HEDGE_STATE = 6
I_NEXT_ORDER_ID = 7
I_LAST_MAKER_TS = 8
I_LAST_HEDGE_TS = 9
I_LAST_SAMPLE_TS = 10
I_SAMPLE_CURSOR = 11
I_SAMPLE_COUNT = 12
I_LAST_BID_ACTION_TS = 13
I_LAST_ASK_ACTION_TS = 14
I_HEDGE_ATTEMPT = 15
I_NEXT_HEDGE_RETRY_TS = 16
I_UNHEDGED_SINCE_TS = 17
I_DEDUPE_CURSOR = 18
I_DEDUPE_COUNT = 19
I_LAST_ERROR = 20
I_QUOTE_GENERATION = 21
I_MAKER_FILL_COUNT = 22
I_HEDGE_FILL_COUNT = 23
I_HEDGE_SUBMIT_COUNT = 24
I_CANCEL_COUNT = 25
I_REJECT_COUNT = 26
I_STALE_COUNT = 27
I_MAKER_POSITION_READY = 28
I_HEDGE_POSITION_READY = 29
I_ACCOUNT_READY_MASK = 30
I_HEDGE_DEPTH_EPOCH = 31
I_HEDGE_DEPTH_SEQUENCE = 32
I_MAKER_POSITION_SEQUENCE = 33
I_HEDGE_POSITION_SEQUENCE = 34
I_DEPTH_INVALID_COUNT = 35

MAX_FILL_DEDUPE = 128
I_DEDUPE_KEYS = 48
I64_STATE_LEN = I_DEDUPE_KEYS + MAX_FILL_DEDUPE


def _number(parameters, name, default=None):
    value = parameters.get(name, default)
    if value is None:
        raise ValueError(f"okx_hyperliquid_xemm requires parameter {name}")
    value = float(value)
    if not np.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _integer(parameters, name, default=None):
    value = parameters.get(name, default)
    if value is None:
        raise ValueError(f"okx_hyperliquid_xemm requires parameter {name}")
    return int(value)


def build(parameters):
    """Build the ABI v10 strategy and eagerly validate all cold-path parameters."""

    maker_asset_no = _integer(parameters, "maker_asset_no", 0)
    hedge_asset_no = _integer(parameters, "hedge_asset_no", 1)
    maker_account_no = _integer(parameters, "maker_account_no", 0)
    hedge_account_no = _integer(parameters, "hedge_account_no", 1)

    order_amount_base = _number(parameters, "order_amount_base")
    min_profitability = _number(parameters, "min_profitability_bps", 10.0) / 10_000.0
    cancel_profitability = _number(parameters, "cancel_profitability_bps", 0.0) / 10_000.0
    maker_fee = _number(parameters, "maker_fee_bps") / 10_000.0
    taker_fee = _number(parameters, "taker_fee_bps") / 10_000.0
    hedge_slippage = _number(parameters, "hedge_slippage_bps", 20.0) / 10_000.0
    taker_volume_factor = _number(parameters, "taker_volume_factor", 0.25)
    quote_conversion_rate = _number(parameters, "quote_conversion_rate", 1.0)

    maker_price_tick = _number(parameters, "maker_price_tick")
    maker_quantity_lot = _number(parameters, "maker_quantity_lot")
    maker_contract_base_multiplier = _number(parameters, "maker_contract_base_multiplier")
    hedge_price_tick = _number(parameters, "hedge_price_tick")
    hedge_quantity_lot = _number(parameters, "hedge_quantity_lot")
    hedge_contract_base_multiplier = _number(
        parameters, "hedge_contract_base_multiplier", 1.0
    )

    max_order_notional = _number(parameters, "max_order_notional")
    max_abs_position_base = _number(parameters, "max_abs_position_base")
    max_unhedged_base = _number(parameters, "max_unhedged_base")
    max_unhedged_notional = _number(parameters, "max_unhedged_notional")
    market_stale_ns = _integer(parameters, "market_stale_ms", 2_000) * 1_000_000
    conversion_expiry_ns = _integer(parameters, "conversion_expiry_ns")
    requote_threshold_ticks = _integer(parameters, "requote_threshold_ticks", 1)
    anti_hysteresis_ns = _integer(parameters, "anti_hysteresis_ms", 60_000) * 1_000_000
    sample_interval_ns = _integer(parameters, "price_sample_interval_ms", 5_000) * 1_000_000
    sample_window = _integer(parameters, "price_sample_window", 12)
    hedge_retry_limit = _integer(parameters, "hedge_retry_limit", 5)
    hedge_retry_backoff_ns = _integer(parameters, "hedge_retry_backoff_ms", 100) * 1_000_000
    adjust_order_enabled = bool(parameters.get("adjust_order_enabled", True))
    top_depth_tolerance = _number(parameters, "top_depth_tolerance_base", 0.0)

    if maker_asset_no < 0 or hedge_asset_no < 0 or maker_asset_no == hedge_asset_no:
        raise ValueError("maker_asset_no and hedge_asset_no must be distinct non-negative values")
    if maker_account_no < 0 or hedge_account_no < 0 or maker_account_no == hedge_account_no:
        raise ValueError(
            "maker_account_no and hedge_account_no must be distinct non-negative values"
        )
    if order_amount_base <= 0.0:
        raise ValueError("order_amount_base must be positive")
    if min_profitability <= -1.0 or cancel_profitability <= -1.0:
        raise ValueError("profitability parameters must be greater than -10000 bps")
    if cancel_profitability > min_profitability:
        raise ValueError("cancel_profitability_bps must not exceed min_profitability_bps")
    if maker_fee <= -1.0 or maker_fee >= 1.0 or taker_fee < 0.0 or taker_fee >= 1.0:
        raise ValueError("fee parameters are outside the supported range")
    if hedge_slippage < 0.0 or hedge_slippage >= 1.0:
        raise ValueError("hedge_slippage_bps must be in [0, 10000)")
    if not 0.0 < taker_volume_factor <= 1.0:
        raise ValueError("taker_volume_factor must be in (0, 1]")
    if quote_conversion_rate <= 0.0:
        raise ValueError("quote_conversion_rate must be positive")
    if min(
        maker_price_tick,
        maker_quantity_lot,
        maker_contract_base_multiplier,
        hedge_price_tick,
        hedge_quantity_lot,
        hedge_contract_base_multiplier,
    ) <= 0.0:
        raise ValueError("all tick, lot, and contract multiplier parameters must be positive")
    if min(
        max_order_notional,
        max_abs_position_base,
        max_unhedged_base,
        max_unhedged_notional,
    ) <= 0.0:
        raise ValueError("all notional and exposure limits must be positive")
    if market_stale_ns <= 0 or conversion_expiry_ns <= 0:
        raise ValueError("market_stale_ms and conversion_expiry_ns must be positive")
    if requote_threshold_ticks <= 0 or anti_hysteresis_ns <= 0 or sample_interval_ns <= 0:
        raise ValueError("requote and sampling intervals must be positive")
    if sample_window <= 0 or sample_window > MAX_PRICE_SAMPLES:
        raise ValueError(f"price_sample_window must be in [1, {MAX_PRICE_SAMPLES}]")
    if hedge_retry_limit < 0 or hedge_retry_backoff_ns <= 0:
        raise ValueError("hedge retry configuration is invalid")
    if top_depth_tolerance != 0.0:
        raise ValueError(
            "top_depth_tolerance_base requires maker full-depth binding; this strategy uses "
            "the lower-bandwidth OKX BBO binding, so the value must be 0"
        )

    maker_lot_base = maker_quantity_lot * maker_contract_base_multiplier
    hedge_lot_base = hedge_quantity_lot * hedge_contract_base_multiplier

    state = np.zeros(F64_STATE_LEN, dtype=np.float64)
    state_i64 = np.zeros(I64_STATE_LEN, dtype=np.int64)
    state_i64[I_MODE] = MODE_WARMING_UP
    state_i64[I_NEXT_ORDER_ID] = 1

    @njit
    def next_order_id(s):
        value = s.state_i64[I_NEXT_ORDER_ID]
        if value <= 0 or value >= 2_000_000_000:
            value = 1
        s.state_i64[I_NEXT_ORDER_ID] = value + 1
        return value

    @njit
    def fill_seen(s, fill):
        # account_version is reliable and ordered per account.
        side_bit = 1 if fill["side"] > 0 else 0
        key = (
            fill["sequence"] * 16
            + fill["local_account_no"] * 4
            + fill["asset_no"] * 2
            + side_bit
        )
        count = s.state_i64[I_DEDUPE_COUNT]
        for index in range(count):
            if s.state_i64[I_DEDUPE_KEYS + index] == key:
                return True
        cursor = s.state_i64[I_DEDUPE_CURSOR]
        s.state_i64[I_DEDUPE_KEYS + cursor] = key
        s.state_i64[I_DEDUPE_CURSOR] = (cursor + 1) % MAX_FILL_DEDUPE
        if count < MAX_FILL_DEDUPE:
            s.state_i64[I_DEDUPE_COUNT] = count + 1
        return False

    @njit
    def effective_maker_top(s):
        current_bid = s.state[F_MAKER_BID]
        current_ask = s.state[F_MAKER_ASK]
        bid = current_bid
        ask = current_ask
        count = s.state_i64[I_SAMPLE_COUNT]
        for index in range(count):
            sampled_bid = s.state[F_BID_SAMPLES + index]
            sampled_ask = s.state[F_ASK_SAMPLES + index]
            if sampled_bid > bid:
                bid = sampled_bid
            if sampled_ask > 0.0 and (ask <= 0.0 or sampled_ask < ask):
                ask = sampled_ask
        if ask <= bid:
            return current_bid, current_ask
        return bid, ask

    @njit
    def take_price_sample(s):
        now = s.now
        if now - s.state_i64[I_LAST_SAMPLE_TS] < sample_interval_ns:
            return
        bid = s.state[F_MAKER_BID]
        ask = s.state[F_MAKER_ASK]
        if bid <= 0.0 or ask <= bid:
            return
        cursor = s.state_i64[I_SAMPLE_CURSOR]
        s.state[F_BID_SAMPLES + cursor] = bid
        s.state[F_ASK_SAMPLES + cursor] = ask
        s.state_i64[I_SAMPLE_CURSOR] = (cursor + 1) % sample_window
        count = s.state_i64[I_SAMPLE_COUNT]
        if count < sample_window:
            s.state_i64[I_SAMPLE_COUNT] = count + 1
        s.state_i64[I_LAST_SAMPLE_TS] = now

    @njit
    def side_depth_metrics(items, required_side, target_base):
        total_base = 0.0
        for item in items:
            if item["side"] == required_side and item["action"] == 1:
                if item["price"] > 0.0 and item["qty"] > 0.0:
                    total_base += item["qty"] * hedge_lot_base

        usable_base = total_base * taker_volume_factor
        desired_base = target_base
        if desired_base > usable_base:
            desired_base = usable_base
        desired_lots = floor(desired_base / hedge_lot_base)
        desired_base = desired_lots * hedge_lot_base
        if desired_base <= 0.0:
            return 0.0, 0.0, 0.0, total_base

        remaining = desired_base
        quote = 0.0
        worst = 0.0
        previous_price = 1.7976931348623157e308 if required_side == BUY_SIDE else -1.0

        # Repeated best-level scan makes the result independent of connector item ordering and
        # avoids allocating/sorting in the callback.
        while remaining > hedge_lot_base * 0.25:
            selected = -1.0 if required_side == BUY_SIDE else 1.7976931348623157e308
            for item in items:
                price = item["price"] * hedge_price_tick
                if item["side"] != required_side or item["action"] != 1:
                    continue
                if required_side == BUY_SIDE:
                    if price > 0.0 and price < previous_price and price > selected:
                        selected = price
                else:
                    if price > previous_price and price < selected:
                        selected = price
            if selected <= 0.0 or selected >= 1.7976931348623157e308:
                break

            level_base = 0.0
            for item in items:
                price = item["price"] * hedge_price_tick
                if (
                    item["side"] == required_side
                    and item["action"] == 1
                    and price == selected
                    and item["qty"] > 0.0
                ):
                    level_base += item["qty"] * hedge_lot_base
            take = level_base if level_base < remaining else remaining
            quote += take * selected
            remaining -= take
            worst = selected
            previous_price = selected

        if remaining > hedge_lot_base * 0.25:
            return 0.0, 0.0, 0.0, total_base
        return quote / desired_base, worst, desired_base, total_base

    @njit
    def cancel_bid(s):
        if s.state_i64[I_BID_STATE] == LEG_EMPTY:
            return
        order_id = s.state_i64[I_BID_ORDER_ID]
        if order_id > 0 and s.cancel(maker_asset_no, order_id, False, maker_account_no) == 0:
            s.state_i64[I_BID_STATE] = LEG_CANCELING
            s.state_i64[I_CANCEL_COUNT] += 1

    @njit
    def cancel_ask(s):
        if s.state_i64[I_ASK_STATE] == LEG_EMPTY:
            return
        order_id = s.state_i64[I_ASK_ORDER_ID]
        if order_id > 0 and s.cancel(maker_asset_no, order_id, False, maker_account_no) == 0:
            s.state_i64[I_ASK_STATE] = LEG_CANCELING
            s.state_i64[I_CANCEL_COUNT] += 1

    @njit
    def cancel_quotes(s):
        cancel_bid(s)
        cancel_ask(s)

    @njit
    def risk_gate(s):
        now = s.now
        if s.state_i64[I_MAKER_POSITION_READY] == 0:
            return False
        if s.state_i64[I_HEDGE_POSITION_READY] == 0:
            return False
        if s.state_i64[I_ACCOUNT_READY_MASK] != 3:
            return False
        if now >= conversion_expiry_ns:
            return False
        if s.state_i64[I_LAST_MAKER_TS] <= 0 or s.state_i64[I_LAST_HEDGE_TS] <= 0:
            return False
        if now - s.state_i64[I_LAST_MAKER_TS] > market_stale_ns:
            return False
        if now - s.state_i64[I_LAST_HEDGE_TS] > market_stale_ns:
            return False
        if abs(s.state[F_UNHEDGED_BASE]) >= max_unhedged_base:
            return False
        reference = s.state[F_LAST_HEDGE_PRICE]
        if reference > 0.0 and abs(s.state[F_UNHEDGED_BASE]) * reference >= max_unhedged_notional:
            return False
        if abs(s.state[F_MAKER_POSITION_ESTIMATE] + s.state[F_HEDGE_POSITION_ESTIMATE]) >= max_abs_position_base:
            return False
        return True

    @njit
    def calculate_targets(s):
        s.state[F_TARGET_BID_TICKS] = 0.0
        s.state[F_TARGET_ASK_TICKS] = 0.0
        s.state[F_TARGET_BID_LOTS] = 0.0
        s.state[F_TARGET_ASK_LOTS] = 0.0
        bid_top, ask_top = effective_maker_top(s)
        maker_bid = s.state[F_MAKER_BID]
        maker_ask = s.state[F_MAKER_ASK]
        if bid_top <= 0.0 or ask_top <= bid_top or maker_ask <= maker_bid:
            return

        net_position = s.state[F_MAKER_POSITION_ESTIMATE] + s.state[F_HEDGE_POSITION_ESTIMATE]

        sell_vwap = s.state[F_HEDGE_SELL_VWAP]
        sell_base = s.state[F_HEDGE_AVAILABLE_BID_BASE]
        if sell_vwap > 0.0 and sell_base >= maker_lot_base:
            bid_cap = sell_vwap * quote_conversion_rate * (1.0 - taker_fee)
            bid_cap /= (1.0 + maker_fee) * (1.0 + min_profitability)
            bid_price = bid_cap
            if adjust_order_enabled:
                competitive = bid_top + maker_price_tick
                if competitive < bid_price:
                    bid_price = competitive
            post_only_cap = maker_ask - maker_price_tick
            if bid_price > post_only_cap:
                bid_price = post_only_cap
            bid_ticks = floor(bid_price / maker_price_tick + 1e-12)
            bid_price = bid_ticks * maker_price_tick
            bid_base = sell_base
            if bid_base > order_amount_base:
                bid_base = order_amount_base
            if bid_price > 0.0 and bid_base * bid_price > max_order_notional:
                bid_base = max_order_notional / bid_price
            if net_position + bid_base > max_abs_position_base:
                bid_base = max_abs_position_base - net_position
            bid_lots = floor(bid_base / maker_lot_base + 1e-12)
            actual_base = bid_lots * maker_lot_base
            if bid_ticks > 0.0 and bid_lots > 0.0 and actual_base <= sell_base:
                proceeds = sell_vwap * actual_base * quote_conversion_rate * (1.0 - taker_fee)
                cost = bid_price * actual_base * (1.0 + maker_fee)
                edge = proceeds / cost - 1.0
                if edge + 1e-12 >= min_profitability:
                    s.state[F_TARGET_BID_TICKS] = bid_ticks
                    s.state[F_TARGET_BID_LOTS] = bid_lots
                    s.state[F_ESTIMATED_BID_EDGE_BPS] = edge * 10_000.0

        buy_vwap = s.state[F_HEDGE_BUY_VWAP]
        buy_base = s.state[F_HEDGE_AVAILABLE_ASK_BASE]
        if buy_vwap > 0.0 and buy_base >= maker_lot_base:
            ask_floor = buy_vwap * quote_conversion_rate * (1.0 + taker_fee)
            ask_floor *= (1.0 + min_profitability) / (1.0 - maker_fee)
            ask_price = ask_floor
            if adjust_order_enabled:
                competitive = ask_top - maker_price_tick
                if competitive > ask_price:
                    ask_price = competitive
            post_only_floor = maker_bid + maker_price_tick
            if ask_price < post_only_floor:
                ask_price = post_only_floor
            ask_ticks = ceil(ask_price / maker_price_tick - 1e-12)
            ask_price = ask_ticks * maker_price_tick
            ask_base = buy_base
            if ask_base > order_amount_base:
                ask_base = order_amount_base
            if ask_price > 0.0 and ask_base * ask_price > max_order_notional:
                ask_base = max_order_notional / ask_price
            if net_position - ask_base < -max_abs_position_base:
                ask_base = max_abs_position_base + net_position
            ask_lots = floor(ask_base / maker_lot_base + 1e-12)
            actual_base = ask_lots * maker_lot_base
            if ask_ticks > 0.0 and ask_lots > 0.0 and actual_base <= buy_base:
                proceeds = ask_price * actual_base * (1.0 - maker_fee)
                cost = buy_vwap * actual_base * quote_conversion_rate * (1.0 + taker_fee)
                edge = proceeds / cost - 1.0
                if edge + 1e-12 >= min_profitability:
                    s.state[F_TARGET_ASK_TICKS] = ask_ticks
                    s.state[F_TARGET_ASK_LOTS] = ask_lots
                    s.state[F_ESTIMATED_ASK_EDGE_BPS] = edge * 10_000.0

    @njit
    def bid_still_profitable(s):
        if s.state_i64[I_BID_STATE] == LEG_EMPTY:
            return True
        sell_vwap = s.state[F_HEDGE_SELL_VWAP]
        if sell_vwap <= 0.0:
            return False
        cap = sell_vwap * quote_conversion_rate * (1.0 - taker_fee)
        cap /= (1.0 + maker_fee) * (1.0 + cancel_profitability)
        return s.state[F_ACTIVE_BID_TICKS] * maker_price_tick <= cap + maker_price_tick * 1e-9

    @njit
    def ask_still_profitable(s):
        if s.state_i64[I_ASK_STATE] == LEG_EMPTY:
            return True
        buy_vwap = s.state[F_HEDGE_BUY_VWAP]
        if buy_vwap <= 0.0:
            return False
        floor_price = buy_vwap * quote_conversion_rate * (1.0 + taker_fee)
        floor_price *= (1.0 + cancel_profitability) / (1.0 - maker_fee)
        return s.state[F_ACTIVE_ASK_TICKS] * maker_price_tick + maker_price_tick * 1e-9 >= floor_price

    @njit
    def submit_bid(s):
        order_id = next_order_id(s)
        result = s.submit_buy_order(
            maker_asset_no,
            order_id,
            s.state[F_TARGET_BID_TICKS],
            s.state[F_TARGET_BID_LOTS],
            1,  # GTX / Post Only
            0,  # Limit
            False,
            False,
            0,
            maker_account_no,
        )
        if result == 0:
            s.state_i64[I_BID_ORDER_ID] = order_id
            s.state_i64[I_BID_STATE] = LEG_SUBMITTING
            s.state[F_ACTIVE_BID_TICKS] = s.state[F_TARGET_BID_TICKS]
            s.state[F_ACTIVE_BID_LOTS] = s.state[F_TARGET_BID_LOTS]
            s.state_i64[I_LAST_BID_ACTION_TS] = s.now

    @njit
    def submit_ask(s):
        order_id = next_order_id(s)
        result = s.submit_sell_order(
            maker_asset_no,
            order_id,
            s.state[F_TARGET_ASK_TICKS],
            s.state[F_TARGET_ASK_LOTS],
            1,  # GTX / Post Only
            0,  # Limit
            False,
            False,
            0,
            maker_account_no,
        )
        if result == 0:
            s.state_i64[I_ASK_ORDER_ID] = order_id
            s.state_i64[I_ASK_STATE] = LEG_SUBMITTING
            s.state[F_ACTIVE_ASK_TICKS] = s.state[F_TARGET_ASK_TICKS]
            s.state[F_ACTIVE_ASK_LOTS] = s.state[F_TARGET_ASK_LOTS]
            s.state_i64[I_LAST_ASK_ACTION_TS] = s.now

    @njit
    def reconcile_quotes(s):
        if not risk_gate(s):
            if s.state_i64[I_MODE] == MODE_QUOTING:
                s.state_i64[I_STALE_COUNT] += 1
            if abs(s.state[F_UNHEDGED_BASE]) > hedge_lot_base * 0.25:
                s.state_i64[I_MODE] = MODE_HEDGE_ONLY
            else:
                s.state_i64[I_MODE] = MODE_PAUSED
            cancel_quotes(s)
            return

        s.state_i64[I_MODE] = MODE_QUOTING
        calculate_targets(s)

        if not bid_still_profitable(s) or s.state[F_TARGET_BID_LOTS] <= 0.0:
            cancel_bid(s)
        elif s.state_i64[I_BID_STATE] == LEG_EMPTY:
            submit_bid(s)
        elif (
            s.state_i64[I_BID_STATE] == LEG_OPEN
            and abs(s.state[F_TARGET_BID_TICKS] - s.state[F_ACTIVE_BID_TICKS])
            >= requote_threshold_ticks
            and s.now - s.state_i64[I_LAST_BID_ACTION_TS] >= anti_hysteresis_ns
        ):
            cancel_bid(s)

        if not ask_still_profitable(s) or s.state[F_TARGET_ASK_LOTS] <= 0.0:
            cancel_ask(s)
        elif s.state_i64[I_ASK_STATE] == LEG_EMPTY:
            submit_ask(s)
        elif (
            s.state_i64[I_ASK_STATE] == LEG_OPEN
            and abs(s.state[F_TARGET_ASK_TICKS] - s.state[F_ACTIVE_ASK_TICKS])
            >= requote_threshold_ticks
            and s.now - s.state_i64[I_LAST_ASK_ACTION_TS] >= anti_hysteresis_ns
        ):
            cancel_ask(s)

        s.state_i64[I_QUOTE_GENERATION] += 1

    @njit
    def maybe_submit_hedge(s):
        unhedged = s.state[F_UNHEDGED_BASE]
        if abs(unhedged) < hedge_lot_base * 0.75 or s.state_i64[I_HEDGE_STATE] != LEG_EMPTY:
            return
        if s.now < s.state_i64[I_NEXT_HEDGE_RETRY_TS]:
            return
        if s.now - s.state_i64[I_LAST_HEDGE_TS] > market_stale_ns:
            cancel_quotes(s)
            s.state_i64[I_MODE] = MODE_HEDGE_ONLY
            return

        lots = floor(abs(unhedged) / hedge_lot_base + 1e-12)
        if lots <= 0.0:
            return
        order_id = next_order_id(s)
        result = -1
        if unhedged > 0.0:
            limit_price = s.state[F_HEDGE_SELL_LIMIT] * (1.0 - hedge_slippage)
            price_ticks = floor(limit_price / hedge_price_tick + 1e-12)
            if price_ticks > 0.0:
                result = s.submit_sell_order(
                    hedge_asset_no,
                    order_id,
                    price_ticks,
                    lots,
                    3,  # IOC
                    0,  # Limit
                    False,
                    False,
                    0,
                    hedge_account_no,
                )
        else:
            limit_price = s.state[F_HEDGE_BUY_LIMIT] * (1.0 + hedge_slippage)
            price_ticks = ceil(limit_price / hedge_price_tick - 1e-12)
            if price_ticks > 0.0:
                result = s.submit_buy_order(
                    hedge_asset_no,
                    order_id,
                    price_ticks,
                    lots,
                    3,  # IOC
                    0,  # Limit
                    False,
                    False,
                    0,
                    hedge_account_no,
                )
        if result == 0:
            s.state_i64[I_HEDGE_ORDER_ID] = order_id
            s.state_i64[I_HEDGE_STATE] = LEG_SUBMITTING
            s.state_i64[I_HEDGE_SUBMIT_COUNT] += 1

    @njit
    def on_start(s):
        s.state_i64[I_MODE] = MODE_WARMING_UP

    @njit
    def on_tick(s):
        ticks = s.ticks()
        if len(ticks) == 0:
            return
        asset_no = ticks[0]["asset_no"]

        if asset_no == maker_asset_no:
            bid = 0.0
            ask = 0.0
            for tick in ticks:
                event = tick["event"]
                if (event["ev"] & 0xFF) != BBO_EVENT:
                    continue
                side = (event["ev"] >> 8) & 0xFF
                price = event["px"] * maker_price_tick
                if side == BUY_SIDE and price > bid:
                    bid = price
                elif side == SELL_SIDE and price > 0.0 and (ask <= 0.0 or price < ask):
                    ask = price
            if bid > 0.0 and ask > bid:
                s.state[F_MAKER_BID] = bid
                s.state[F_MAKER_ASK] = ask
                s.state_i64[I_LAST_MAKER_TS] = s.now
                take_price_sample(s)

        maybe_submit_hedge(s)
        reconcile_quotes(s)

    @njit
    def on_depth(s):
        batch = s.depth()
        if batch["asset_no"] != hedge_asset_no:
            return
        # The VWAP calculation requires a complete image. Incremental batches are rejected until
        # a fixed-memory local book implementation is explicitly selected for this strategy.
        if batch["kind"] != 1 or batch["flags"] & 1 == 0:
            s.state_i64[I_DEPTH_INVALID_COUNT] += 1
            s.state_i64[I_LAST_HEDGE_TS] = 0
            cancel_quotes(s)
            return
        epoch = batch["stream_epoch"]
        sequence = batch["last_update_sequence"]
        previous_epoch = s.state_i64[I_HEDGE_DEPTH_EPOCH]
        previous_sequence = s.state_i64[I_HEDGE_DEPTH_SEQUENCE]
        # Hyperliquid l2Book messages are complete, idempotent images and do not expose a native
        # update sequence. Dynamic connector boundaries may therefore repeat the synthesized
        # (epoch, sequence) coordinates; only a true regression is unsafe.
        if epoch < previous_epoch or (epoch == previous_epoch and sequence < previous_sequence):
            return
        if batch["first_update_sequence"] > sequence:
            s.state_i64[I_DEPTH_INVALID_COUNT] += 1
            cancel_quotes(s)
            return
        s.state_i64[I_HEDGE_DEPTH_EPOCH] = epoch
        s.state_i64[I_HEDGE_DEPTH_SEQUENCE] = sequence
        items = s.depth_items()
        sell_vwap, sell_limit, sell_base, total_bid = side_depth_metrics(
            items, BUY_SIDE, order_amount_base
        )
        buy_vwap, buy_limit, buy_base, total_ask = side_depth_metrics(
            items, SELL_SIDE, order_amount_base
        )
        if sell_vwap > 0.0 and buy_vwap > sell_vwap:
            s.state[F_HEDGE_SELL_VWAP] = sell_vwap
            s.state[F_HEDGE_BUY_VWAP] = buy_vwap
            s.state[F_HEDGE_SELL_LIMIT] = sell_limit
            s.state[F_HEDGE_BUY_LIMIT] = buy_limit
            s.state[F_HEDGE_AVAILABLE_BID_BASE] = sell_base
            s.state[F_HEDGE_AVAILABLE_ASK_BASE] = buy_base
            s.state[F_LAST_HEDGE_PRICE] = (sell_vwap + buy_vwap) * 0.5
            s.state_i64[I_LAST_HEDGE_TS] = s.now
        maybe_submit_hedge(s)
        reconcile_quotes(s)

    @njit
    def on_filled(s):
        fills = s.fills()
        for fill in fills:
            if fill_seen(s, fill):
                continue
            base = 0.0
            if fill["asset_no"] == maker_asset_no:
                base = fill["last_fill_qty"] * maker_lot_base
                if fill["side"] > 0:
                    s.state[F_UNHEDGED_BASE] += base
                    s.state[F_MAKER_POSITION_ESTIMATE] += base
                else:
                    s.state[F_UNHEDGED_BASE] -= base
                    s.state[F_MAKER_POSITION_ESTIMATE] -= base
                s.state[F_TOTAL_MAKER_FILL_BASE] += base
                s.state_i64[I_MAKER_FILL_COUNT] += 1
                if s.state_i64[I_UNHEDGED_SINCE_TS] == 0:
                    s.state_i64[I_UNHEDGED_SINCE_TS] = s.now
            elif fill["asset_no"] == hedge_asset_no:
                base = fill["last_fill_qty"] * hedge_lot_base
                if fill["side"] > 0:
                    s.state[F_UNHEDGED_BASE] += base
                    s.state[F_HEDGE_POSITION_ESTIMATE] += base
                else:
                    s.state[F_UNHEDGED_BASE] -= base
                    s.state[F_HEDGE_POSITION_ESTIMATE] -= base
                if abs(s.state[F_UNHEDGED_BASE]) < hedge_lot_base * 0.5:
                    s.state[F_UNHEDGED_BASE] = 0.0
                    s.state_i64[I_UNHEDGED_SINCE_TS] = 0
                s.state[F_TOTAL_HEDGE_FILL_BASE] += base
                s.state_i64[I_HEDGE_FILL_COUNT] += 1

        if abs(s.state[F_UNHEDGED_BASE]) >= max_unhedged_base:
            s.state_i64[I_MODE] = MODE_HEDGE_ONLY
            cancel_quotes(s)
        maybe_submit_hedge(s)

    @njit
    def on_order(s):
        orders = s.orders()
        for order in orders:
            status = order["status"]
            terminal = (
                status == ORDER_EXPIRED
                or status == ORDER_FILLED
                or status == ORDER_CANCELED
                or status == ORDER_REJECTED
            )
            if order["asset_no"] == maker_asset_no:
                if order["side"] > 0:
                    if terminal:
                        s.state_i64[I_BID_STATE] = LEG_EMPTY
                        s.state_i64[I_BID_ORDER_ID] = 0
                        s.state[F_ACTIVE_BID_TICKS] = 0.0
                        s.state[F_ACTIVE_BID_LOTS] = 0.0
                    elif status == ORDER_NEW or status == ORDER_PARTIALLY_FILLED:
                        s.state_i64[I_BID_STATE] = LEG_OPEN
                else:
                    if terminal:
                        s.state_i64[I_ASK_STATE] = LEG_EMPTY
                        s.state_i64[I_ASK_ORDER_ID] = 0
                        s.state[F_ACTIVE_ASK_TICKS] = 0.0
                        s.state[F_ACTIVE_ASK_LOTS] = 0.0
                    elif status == ORDER_NEW or status == ORDER_PARTIALLY_FILLED:
                        s.state_i64[I_ASK_STATE] = LEG_OPEN
            elif order["asset_no"] == hedge_asset_no:
                if status == ORDER_NEW or status == ORDER_PARTIALLY_FILLED:
                    s.state_i64[I_HEDGE_STATE] = LEG_OPEN
                elif terminal:
                    s.state_i64[I_HEDGE_STATE] = LEG_EMPTY
                    s.state_i64[I_HEDGE_ORDER_ID] = 0
                    if status == ORDER_FILLED and abs(s.state[F_UNHEDGED_BASE]) < hedge_lot_base * 0.5:
                        s.state_i64[I_HEDGE_ATTEMPT] = 0
                        s.state_i64[I_NEXT_HEDGE_RETRY_TS] = 0
                    elif abs(s.state[F_UNHEDGED_BASE]) >= hedge_lot_base * 0.5:
                        attempt = s.state_i64[I_HEDGE_ATTEMPT] + 1
                        s.state_i64[I_HEDGE_ATTEMPT] = attempt
                        if status == ORDER_REJECTED:
                            s.state_i64[I_REJECT_COUNT] += 1
                        if attempt > hedge_retry_limit:
                            s.state_i64[I_MODE] = MODE_FAULTED
                            s.state_i64[I_LAST_ERROR] = 1
                            cancel_quotes(s)
                        else:
                            exponent = attempt - 1
                            if exponent > 20:
                                exponent = 20
                            s.state_i64[I_NEXT_HEDGE_RETRY_TS] = (
                                s.now + hedge_retry_backoff_ns * (1 << exponent)
                            )
        maybe_submit_hedge(s)

    @njit
    def on_position(s):
        position = s.position_event()
        account_no = position["local_account_no"]
        sequence = position["sequence"]
        quantity = abs(position["quantity"])
        if position["position_side"] == 2:
            quantity = -quantity
        if account_no == maker_account_no and position["asset_no"] == maker_asset_no:
            if sequence >= s.state_i64[I_MAKER_POSITION_SEQUENCE]:
                s.state_i64[I_MAKER_POSITION_SEQUENCE] = sequence
                s.state[F_MAKER_POSITION_ESTIMATE] = quantity * maker_lot_base
                s.state_i64[I_MAKER_POSITION_READY] = 1
                s.state_i64[I_ACCOUNT_READY_MASK] |= 1
        elif account_no == hedge_account_no and position["asset_no"] == hedge_asset_no:
            if sequence >= s.state_i64[I_HEDGE_POSITION_SEQUENCE]:
                s.state_i64[I_HEDGE_POSITION_SEQUENCE] = sequence
                s.state[F_HEDGE_POSITION_ESTIMATE] = quantity * hedge_lot_base
                s.state_i64[I_HEDGE_POSITION_READY] = 1
                s.state_i64[I_ACCOUNT_READY_MASK] |= 2
        if (
            s.state_i64[I_MAKER_POSITION_READY] != 0
            and s.state_i64[I_HEDGE_POSITION_READY] != 0
        ):
            s.state[F_UNHEDGED_BASE] = (
                s.state[F_MAKER_POSITION_ESTIMATE]
                + s.state[F_HEDGE_POSITION_ESTIMATE]
            )

    @njit
    def on_balance(s):
        # Balance events participate in the Runtime readiness barrier. Strategy-level notional
        # limits remain conservative and do not expand merely because more balance is available.
        balance = s.balance_event()
        if balance["available"] < 0.0:
            s.state_i64[I_MODE] = MODE_PAUSED
            cancel_quotes(s)

    @njit
    def on_command_result(s):
        result = s.command_result()
        if result["final_result"] == 0 or result["outcome"] == 1:
            return
        order_id = result["order_id"]
        s.state_i64[I_REJECT_COUNT] += 1
        if order_id == s.state_i64[I_BID_ORDER_ID]:
            s.state_i64[I_BID_STATE] = LEG_EMPTY
            s.state_i64[I_BID_ORDER_ID] = 0
        elif order_id == s.state_i64[I_ASK_ORDER_ID]:
            s.state_i64[I_ASK_STATE] = LEG_EMPTY
            s.state_i64[I_ASK_ORDER_ID] = 0
        elif order_id == s.state_i64[I_HEDGE_ORDER_ID]:
            s.state_i64[I_HEDGE_STATE] = LEG_EMPTY
            s.state_i64[I_HEDGE_ORDER_ID] = 0
            s.state_i64[I_NEXT_HEDGE_RETRY_TS] = s.now + hedge_retry_backoff_ns

    @njit
    def on_account_state(s):
        event = s.account_state()
        account_no = event["local_account_no"]
        bit = 1 if account_no == maker_account_no else 2
        kind = event["kind"]
        if kind == 6 or kind == 9 or (kind == 8 and event["state"] != 4):
            s.state_i64[I_ACCOUNT_READY_MASK] &= ~bit
            s.state_i64[I_MODE] = MODE_PAUSED
            cancel_quotes(s)
        elif kind == 8 and event["state"] == 4:
            s.state_i64[I_ACCOUNT_READY_MASK] |= bit

    @njit
    def on_timer(s):
        maybe_submit_hedge(s)
        reconcile_quotes(s)

    @njit
    def on_error(s):
        s.state_i64[I_MODE] = MODE_FAULTED
        s.state_i64[I_LAST_ERROR] = s.last_error

    @njit
    def on_stop(s):
        s.state_i64[I_MODE] = MODE_STOPPING

    return SimpleNamespace(
        strategy_id="okx_hyperliquid_xemm",
        strategy_version="0.2.0",
        on_start=on_start,
        on_tick=on_tick,
        on_depth=on_depth,
        on_filled=on_filled,
        on_order=on_order,
        on_position=on_position,
        on_balance=on_balance,
        on_command_result=on_command_result,
        on_account_state=on_account_state,
        on_timer=on_timer,
        on_error=on_error,
        on_stop=on_stop,
        state=state,
        state_i64=state_i64,
        metadata={
            "maker": "okx",
            "hedge": "hyperliquid",
            "maker_asset_no": maker_asset_no,
            "hedge_asset_no": hedge_asset_no,
            "production_ready": False,
            "requires_testnet_validation": True,
            "required_runtime_abi": 10,
        },
    )
