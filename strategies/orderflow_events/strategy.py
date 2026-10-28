"""L0 order-flow event collector for Titan Strategy ABI V13.

The strategy subscribes BBO and depth, consumes the host-validated 100-level book, and recomputes the
L0 event dictionary (E01/E02/E03/E04/E07/E08/E11/E13/E14) on a fixed cadence that
defaults to 100 ms. Every event has its own detector and its own threshold set, so a
detector can be disabled or tightened from ``parameters.json`` without touching code.

It never submits orders; it is an event producer. Per-event counters, the latest event
and the current market context are exposed through the business observation protocol.

Platform caveats (documented, not strategy bugs)
------------------------------------------------
1. ABI V13 routes only BBO and depth batches into a strategy: there is no trade event
   kind, so ``ctx.ticks()`` cannot carry prints. ``on_tick`` already accepts trade ticks
   (``kind == KIND_TRADE``) and ``ingest_trade`` is the switch point; until the platform
   maps a trade subscription there, the flow feed is inferred from touch-level quantity
   reductions (``flow_source=depth_proxy``).
2. Liquidation orders are not collected (the connector does not subscribe
   ``forceOrder``), so E14 runs as ``proxy`` only.
3. Requires HOST_DEPTH_100: Rust reconstructs the deeper book and supplies a complete
   covered Top100 plus epoch/validity metadata. BBO never mutates that depth book.

Units: prices are exchange ticks and quantities are exchange lots. Tick ratios are scale
free, so bp thresholds need no conversion; BTC thresholds use the ``qty_lot`` parameter.
"""

import numpy as np

from numba import njit

from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.observation import BusinessObservationSpec, ObservationField
from titan_strategy.parameters import BoolParam, EnumParam, FloatParam, IntParam
from titan_strategy.state import (
    array, float64, int32, int64, new_state, record, uint8, uint32, uint64,
)
from titan_strategy.types import EventKind, EventQos

# --------------------------------------------------------------------------- layout
BOOK_LEVELS = 100
BUCKETS = 640                  # 64 s of 100 ms buckets (covers the 60 s E14 window)
SECONDS = 901                  # 15 min plus the current 1 s sample (E11 lookback, E14 base, sigma ring)
TOUCH_SLOTS = 8                # active price slots for refill / iceberg state
EVENT_TYPES = 16               # event codes are used as indices (E01..E14)
EWM_SERIES = 8

BUCKET_NS = 100_000_000
KIND_TRADE = 3
FLOW_DEPTH_PROXY, FLOW_PLATFORM_TRADES = 1, 2

E01, E02, E03, E04, E07, E08, E11, E13, E14 = 1, 2, 3, 4, 7, 8, 11, 13, 14

SIDE_BID, SIDE_ASK = 1, 2
AGGRESSOR_SELL, AGGRESSOR_BUY = -1, 1

# --------------------------------------------------------------------------- schema
cfg_dtype = record(
    asset_no=uint32,
    compute_interval_ns=int64,
    bucket_ns=int64,
    flow_source=uint8,
    cooldown_ms=int64,
    stale_after_ns=int64,
    qty_lot=float64,
    depth_levels=int64,
    sigma_floor_bp=float64,
    max_spread_bp=float64,
    min_depth_btc=float64,
    # E01 bulldozer
    e01_enabled=uint8, e01_window_ms=int64, e01_run_min=int64, e01_vol_btc=float64,
    e01_dp_bp=float64, e01_rho=float64, e01_gap_ms=int64,
    # E02 acceleration
    e02_enabled=uint8, e02_window_ms=int64, e02_slope_z=float64, e02_min_ms=int64,
    # E03 absorption
    e03_enabled=uint8, e03_window_ms=int64, e03_vol_btc=float64, e03_impact_bp=float64,
    e03_kappa_bp_per_btc=float64, e03_refill=float64,
    # E04 iceberg
    e04_enabled=uint8, e04_fills=int64, e04_refill_ms=int64, e04_window_ms=int64,
    # E07 sweep
    e07_enabled=uint8, e07_window_ms=int64, e07_levels=int64, e07_vol_btc=float64,
    e07_dp_bp=float64, e07_pre_depth_ratio=float64,
    # E08 order-flow imbalance / hidden-cancel proxy
    e08_enabled=uint8, e08_window_ms=int64, e08_z=float64, e08_h_ratio=float64,
    e08_persist=int64,
    # E11 stop run
    e11_enabled=uint8, e11_lookback_ms=int64, e11_break_bp=float64, e11_vol_btc=float64,
    e11_min_ms=int64,
    # E13 order impact
    e13_enabled=uint8, e13_window_ms=int64, e13_impact_z=float64, e13_vol_z=float64,
    e13_maxq_z=float64, e13_min_prints=int64,
    # E14 liquidation-like displacement
    e14_enabled=uint8, e14_window_ms=int64, e14_disp_z=float64, e14_vol_z=float64,
    e14_prints=int64, e14_lookback_ms=int64,
)

book_dtype = record(
    bid_price=array(int64, BOOK_LEVELS), bid_qty=array(float64, BOOK_LEVELS),
    ask_price=array(int64, BOOK_LEVELS), ask_qty=array(float64, BOOK_LEVELS),
    best_bid_ticks=int64, best_bid_qty=float64, best_ask_ticks=int64, best_ask_qty=float64,
    mid=float64, prev_mid=float64, mid_ts_ns=int64, mid_updates=int64,
    prev_bid_ticks=int64, prev_bid_qty=float64, prev_ask_ticks=int64, prev_ask_qty=float64,
    depth1_bid=float64, depth1_ask=float64, depth5_bid=float64, depth5_ask=float64,
    ready=uint8, stale=uint8, last_update_ns=int64, resets=int64,
    epoch=uint64, warm_since_ns=int64,
)

ring_dtype = record(
    bkt_seq=array(int64, BUCKETS), bkt_mid=array(float64, BUCKETS),
    bkt_buy=array(float64, BUCKETS), bkt_sell=array(float64, BUCKETS),
    bkt_maxq=array(float64, BUCKETS), bkt_buy_n=array(int32, BUCKETS),
    bkt_sell_n=array(int32, BUCKETS), bkt_ofi=array(float64, BUCKETS),
    bkt_depth=array(float64, BUCKETS),
    cur_bucket=int64, bucket_seen=uint64,
    sec_seq=array(int64, SECONDS), sec_mid=array(float64, SECONDS),
    last_sec=int64, second_seen=int64,
    sigma_var=float64, sigma_bp=float64, sigma_floor_bp=float64, prev_sec_mid=float64,
)

flow_dtype = record(
    run_dir=int64, run_len=int64, run_start_ns=int64, run_last_ns=int64,
    run_start_mid=float64, run_adverse_bp=float64,
    touch_price=array(int64, TOUCH_SLOTS), touch_side=array(uint8, TOUCH_SLOTS),
    touch_q0=array(float64, TOUCH_SLOTS), touch_qmin=array(float64, TOUCH_SLOTS),
    touch_ts=array(int64, TOUCH_SLOTS), touch_n=int64,
    ice_price=array(int64, TOUCH_SLOTS), ice_initial=array(float64, TOUCH_SLOTS),
    ice_cum=array(float64, TOUCH_SLOTS), ice_fills=array(int64, TOUCH_SLOTS),
    ice_last_ns=array(int64, TOUCH_SLOTS), ice_refill_ms=array(float64, TOUCH_SLOTS),
    ice_n=int64,
    view_bid_price=array(int64, BOOK_LEVELS),
    view_ask_price=array(int64, BOOK_LEVELS),
    view_ns=int64, view_bid_depth5=float64, view_ask_depth5=float64,
    break_dir=int64, break_ns=int64, break_px=float64, break_ext=float64, break_lots=float64,
    ext_side=int64, ext_px=float64, ext_ns=int64, ext_prints=int64, ext_done=int64,
    last_print_ns=int64, last_print_px=float64, prints_seen=int64, dropped=int64,
)

ev_dtype = record(
    last_compute_ns=int64, compute_count=uint64,
    last_emit_ns=array(int64, EVENT_TYPES), count=array(uint64, EVENT_TYPES),
    direction=array(int64, EVENT_TYPES), strength=array(float64, EVENT_TYPES),
    events_total=uint64,
    ofi_now=float64, ofi_persist=int64, h_now=float64,
    prev_depth5=float64,
    slope_prev=float64, slope_ns=int64, px_slope_prev=float64,
    slope_last=float64, px_slope_last=float64,
    ewm_mean=array(float64, EWM_SERIES), ewm_var=array(float64, EWM_SERIES),
    ewm_n=array(int64, EWM_SERIES),
)

obs_dtype = record(
    mid_ticks=float64, spread_bp=float64, sigma_bp=float64, ofi_2s=float64,
    depth1_btc=float64, depth5_btc=float64, flow_source=uint8, book_ready=uint8,
    book_valid=uint8, stream_epoch=uint64, book_resets=uint64, warmup_remaining_ms=int64,
    prints_seen=uint64, dropped=uint64, asset_no=uint64, computes=uint64,
    last_type=uint8, last_direction=int64, last_strength=float64, last_ts_ns=int64,
    events_total=uint64,
    e01_count=uint64, e01_dir=int64, e01_strength=float64,
    e02_count=uint64, e02_dir=int64, e02_strength=float64,
    e03_count=uint64, e03_dir=int64, e03_strength=float64,
    e04_count=uint64, e04_dir=int64, e04_strength=float64,
    e07_count=uint64, e07_dir=int64, e07_strength=float64,
    e08_count=uint64, e08_dir=int64, e08_strength=float64,
    e11_count=uint64, e11_dir=int64, e11_strength=float64,
    e13_count=uint64, e13_dir=int64, e13_strength=float64,
    e14_count=uint64, e14_dir=int64, e14_strength=float64,
)

state_dtype = record(cfg=cfg_dtype, book=book_dtype, ring=ring_dtype, flow=flow_dtype,
                     ev=ev_dtype, obs=obs_dtype)

OBSERVATION = BusinessObservationSpec(
    schema_id="titan.orderflow_events", schema_version=2,
    fields=(
        ObservationField("mid_ticks", "decimal", "price_tick", "obs.mid_ticks", label="中间价"),
        ObservationField("spread_bp", "decimal", "bp", "obs.spread_bp", label="点差"),
        ObservationField("sigma_bp", "decimal", "bp", "obs.sigma_bp", label="条件波动率"),
        ObservationField("ofi_2s", "decimal", "quantity_lot", "obs.ofi_2s", label="2 秒 OFI"),
        ObservationField("depth1_btc", "decimal", "btc", "obs.depth1_btc", label="一档深度"),
        ObservationField("depth5_btc", "decimal", "btc", "obs.depth5_btc", label="五档深度"),
        ObservationField("flow_source", "enum", "source", "obs.flow_source", label="流量来源",
                         enum_mapping=((FLOW_DEPTH_PROXY, "depth_proxy"),
                                       (FLOW_PLATFORM_TRADES, "platform_trades"))),
        ObservationField("book_ready", "enum", "flag", "obs.book_ready", label="盘口就绪",
                         enum_mapping=((0, "unavailable"), (1, "ready"))),
        ObservationField("book_valid", "enum", "flag", "obs.book_valid", label="深度覆盖有效",
                         enum_mapping=((0, "invalid"), (1, "valid"))),
        ObservationField("stream_epoch", "uint64", "count", "obs.stream_epoch", label="行情代次"),
        ObservationField("book_resets", "uint64", "count", "obs.book_resets", label="盘口重置次数"),
        ObservationField("warmup_remaining_ms", "int64", "ms", "obs.warmup_remaining_ms", label="剩余预热时间"),
        ObservationField("prints_seen", "uint64", "count", "obs.prints_seen", label="已处理流量笔数"),
        ObservationField("dropped", "uint64", "count", "obs.dropped", label="被丢弃的行情条目"),
        ObservationField("asset_no", "uint64", "index", "obs.asset_no", label="绑定的本地品种号"),
        ObservationField("computes", "uint64", "count", "obs.computes", label="事件计算轮次"),
        ObservationField("events_total", "uint64", "count", "obs.events_total", label="事件总数"),
        ObservationField("last_type", "enum", "event", "obs.last_type", label="最近事件",
                         enum_mapping=((0, "none"), (E01, "e01_bulldozer"),
                                       (E02, "e02_acceleration"), (E03, "e03_absorption"),
                                       (E04, "e04_iceberg"), (E07, "e07_sweep"),
                                       (E08, "e08_ofi"), (E11, "e11_stop_run"),
                                       (E13, "e13_order_impact"),
                                       (E14, "e14_liquidation_proxy"))),
        ObservationField("last_direction", "int64", "sign", "obs.last_direction", label="最近事件方向"),
        ObservationField("last_strength", "decimal", "ratio", "obs.last_strength", label="最近事件强度"),
        ObservationField("last_ts_ns", "int64", "ns", "obs.last_ts_ns", label="最近事件时间"),
        ObservationField("e01_count", "uint64", "count", "obs.e01_count", label="E01 次数"),
        ObservationField("e02_count", "uint64", "count", "obs.e02_count", label="E02 次数"),
        ObservationField("e03_count", "uint64", "count", "obs.e03_count", label="E03 次数"),
        ObservationField("e04_count", "uint64", "count", "obs.e04_count", label="E04 次数"),
        ObservationField("e07_count", "uint64", "count", "obs.e07_count", label="E07 次数"),
        ObservationField("e08_count", "uint64", "count", "obs.e08_count", label="E08 次数"),
        ObservationField("e11_count", "uint64", "count", "obs.e11_count", label="E11 次数"),
        ObservationField("e13_count", "uint64", "count", "obs.e13_count", label="E13 次数"),
        ObservationField("e14_count", "uint64", "count", "obs.e14_count", label="E14 次数"),
        ObservationField("e03_strength", "decimal", "ratio", "obs.e03_strength", label="E03 最新强度"),
        ObservationField("e13_strength", "decimal", "ratio", "obs.e13_strength", label="E13 最新强度"),
    ),
)


def _int(name, default, minimum=0, maximum=2**62):
    return IntParam(name, required=False, default=default, minimum=minimum, maximum=maximum)


def _float(name, default, minimum=0.0, maximum=1e12):
    return FloatParam(name, required=False, default=default, minimum=minimum, maximum=maximum)


SPEC = StrategySpec(
    strategy_type="orderflow_events", source_version="0.2.1", recovery_state_schema_version=2,
    parameters=(
        _int("asset_no", 0, maximum=2**32 - 1),
        _int("compute_interval_ms", 100, 100, 100),
        _float("qty_lot", 0.001, 1e-12, 1e6),
        _float("sigma_floor_bp", 0.15, 0.0, 100.0),
        EnumParam("flow_source", required=False, default="depth_proxy",
                  values=("depth_proxy", "platform_trades")),
        _int("cooldown_ms", 500, 0, 600000),
        _int("stale_after_ms", 2000, 100, 600000),
        _int("depth_levels", BOOK_LEVELS, 1, BOOK_LEVELS),
        _float("max_spread_bp", 5.0, 0.0, 1000.0),
        _float("min_depth_btc", 0.0, 0.0, 1e7),
        BoolParam("e01_enabled", required=False, default=True),
        _int("e01_window_ms", 5000, 100, 60000), _int("e01_run_min", 8, 2, 1000),
        _float("e01_vol_btc", 5.0, 0.0, 1e6), _float("e01_dp_bp", 2.0, 0.0, 1000.0),
        _float("e01_rho", 0.33, 0.0, 10.0), _int("e01_gap_ms", 200, 1, 10000),
        BoolParam("e02_enabled", required=False, default=True),
        _int("e02_window_ms", 5000, 100, 60000), _float("e02_slope_z", 2.0, 0.0, 100.0),
        _int("e02_min_ms", 1000, 0, 60000),
        BoolParam("e03_enabled", required=False, default=True),
        _int("e03_window_ms", 10000, 100, 60000), _float("e03_vol_btc", 8.0, 0.0, 1e6),
        _float("e03_impact_bp", 1.0, 0.0, 1000.0),
        _float("e03_kappa_bp_per_btc", 0.2, 0.0, 1e6), _float("e03_refill", 0.5, 0.0, 10.0),
        BoolParam("e04_enabled", required=False, default=True),
        _int("e04_fills", 3, 2, 1000), _int("e04_refill_ms", 500, 10, 60000),
        _int("e04_window_ms", 30000, 100, 300000),
        BoolParam("e07_enabled", required=False, default=True),
        _int("e07_window_ms", 1000, 100, 10000), _int("e07_levels", 3, 1, BOOK_LEVELS),
        _float("e07_vol_btc", 3.0, 0.0, 1e6), _float("e07_dp_bp", 1.5, 0.0, 1000.0),
        _float("e07_pre_depth_ratio", 1.0, 0.0, 100.0),
        BoolParam("e08_enabled", required=False, default=True),
        _int("e08_window_ms", 2000, 100, 60000), _float("e08_z", 3.0, 0.0, 100.0),
        _float("e08_h_ratio", 0.5, 0.0, 100.0), _int("e08_persist", 2, 1, 100),
        BoolParam("e11_enabled", required=False, default=True),
        _int("e11_lookback_ms", 900000, 60000, 900000), _float("e11_break_bp", 3.0, 0.0, 1000.0),
        _float("e11_vol_btc", 3.0, 0.0, 1e6), _int("e11_min_ms", 1000, 0, 3600000),
        BoolParam("e13_enabled", required=False, default=True),
        _int("e13_window_ms", 1000, 100, 10000), _float("e13_impact_z", 4.0, 0.0, 100.0),
        _float("e13_vol_z", 2.0, 0.0, 100.0), _float("e13_maxq_z", 3.0, 0.0, 100.0),
        _int("e13_min_prints", 2, 1, 1000),
        BoolParam("e14_enabled", required=False, default=True),
        _int("e14_window_ms", 60000, 1000, 70000), _float("e14_disp_z", 5.0, 0.0, 100.0),
        _float("e14_vol_z", 2.0, 0.0, 100.0), _int("e14_prints", 3, 1, 1000),
        _int("e14_lookback_ms", 300000, 60000, 900000),
    ),
    subscriptions=(
        EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),
        EventSubscription(EventKind.DEPTH, "on_depth", 1, EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.TIMER, "on_timer", 1, EventQos.RELIABLE_ORDERED),
    ),
    capabilities=Capability.MARKET_DATA | Capability.TIMER | Capability.HOST_DEPTH_100,
)


# --------------------------------------------------------------------------- book
@njit
def book_clear(bk):
    for i in range(BOOK_LEVELS):
        bk["bid_price"][i] = 0
        bk["bid_qty"][i] = 0.0
        bk["ask_price"][i] = 0
        bk["ask_qty"][i] = 0.0
    bk["best_bid_qty"] = 0.0
    bk["best_ask_qty"] = 0.0
    bk["depth1_bid"] = 0.0
    bk["depth1_ask"] = 0.0
    bk["depth5_bid"] = 0.0
    bk["depth5_ask"] = 0.0
    bk["prev_bid_ticks"] = 0
    bk["prev_bid_qty"] = 0.0
    bk["prev_ask_ticks"] = 0
    bk["prev_ask_qty"] = 0.0
    bk["best_bid_ticks"] = 0
    bk["best_ask_ticks"] = 0
    bk["mid"] = 0.0
    bk["prev_mid"] = 0.0
    bk["ready"] = 0


@njit
def bid_count(bk):
    count = 0
    for i in range(BOOK_LEVELS):
        if bk["bid_qty"][i] > 0.0:
            count += 1
    return count


@njit
def ask_count(bk):
    count = 0
    for i in range(BOOK_LEVELS):
        if bk["ask_qty"][i] > 0.0:
            count += 1
    return count


@njit
def bid_qty_at(bk, price):
    for i in range(BOOK_LEVELS):
        p = bk["bid_price"][i]
        if p <= 0:
            return 0.0
        if p == price:
            return bk["bid_qty"][i]
        if p < price:
            return 0.0
    return 0.0


@njit
def ask_qty_at(bk, price):
    for i in range(BOOK_LEVELS):
        p = bk["ask_price"][i]
        if p <= 0:
            return 0.0
        if p == price:
            return bk["ask_qty"][i]
        if p > price:
            return 0.0
    return 0.0


@njit
def apply_bid(bk, price, qty):
    count = bid_count(bk)
    index = -1
    for i in range(count):
        p = bk["bid_price"][i]
        if p == price:
            index = i
            break
        if p < price:
            break
    if qty <= 0.0:
        if index < 0:
            return
        for i in range(index, count - 1):
            bk["bid_price"][i] = bk["bid_price"][i + 1]
            bk["bid_qty"][i] = bk["bid_qty"][i + 1]
        bk["bid_price"][count - 1] = 0
        bk["bid_qty"][count - 1] = 0.0
        return
    if index >= 0:
        bk["bid_qty"][index] = qty
        return
    position = count
    for i in range(count):
        if bk["bid_price"][i] < price:
            position = i
            break
    if position >= BOOK_LEVELS:
        return
    top = count - 1
    if top > BOOK_LEVELS - 2:
        top = BOOK_LEVELS - 2
    for i in range(top, position - 1, -1):
        bk["bid_price"][i + 1] = bk["bid_price"][i]
        bk["bid_qty"][i + 1] = bk["bid_qty"][i]
    bk["bid_price"][position] = price
    bk["bid_qty"][position] = qty


@njit
def apply_ask(bk, price, qty):
    count = ask_count(bk)
    index = -1
    for i in range(count):
        p = bk["ask_price"][i]
        if p == price:
            index = i
            break
        if p > price:
            break
    if qty <= 0.0:
        if index < 0:
            return
        for i in range(index, count - 1):
            bk["ask_price"][i] = bk["ask_price"][i + 1]
            bk["ask_qty"][i] = bk["ask_qty"][i + 1]
        bk["ask_price"][count - 1] = 0
        bk["ask_qty"][count - 1] = 0.0
        return
    if index >= 0:
        bk["ask_qty"][index] = qty
        return
    position = count
    for i in range(count):
        if bk["ask_price"][i] > price:
            position = i
            break
    if position >= BOOK_LEVELS:
        return
    top = count - 1
    if top > BOOK_LEVELS - 2:
        top = BOOK_LEVELS - 2
    for i in range(top, position - 1, -1):
        bk["ask_price"][i + 1] = bk["ask_price"][i]
        bk["ask_qty"][i + 1] = bk["ask_qty"][i]
    bk["ask_price"][position] = price
    bk["ask_qty"][position] = qty


@njit
def refresh_book(bk, levels):
    n_bid = bid_count(bk)
    n_ask = ask_count(bk)
    if n_bid > 0:
        bk["best_bid_ticks"] = bk["bid_price"][0]
        bk["best_bid_qty"] = bk["bid_qty"][0]
    else:
        bk["best_bid_ticks"] = 0
        bk["best_bid_qty"] = 0.0
    if n_ask > 0:
        bk["best_ask_ticks"] = bk["ask_price"][0]
        bk["best_ask_qty"] = bk["ask_qty"][0]
    else:
        bk["best_ask_ticks"] = 0
        bk["best_ask_qty"] = 0.0
    depth5_bid = 0.0
    depth5_ask = 0.0
    for i in range(5):
        depth5_bid += bk["bid_qty"][i]
        depth5_ask += bk["ask_qty"][i]
    bk["depth1_bid"] = bk["bid_qty"][0] if n_bid > 0 else 0.0
    bk["depth1_ask"] = bk["ask_qty"][0] if n_ask > 0 else 0.0
    bk["depth5_bid"] = depth5_bid
    bk["depth5_ask"] = depth5_ask
    if bk["best_bid_ticks"] > 0 and bk["best_ask_ticks"] > bk["best_bid_ticks"]:
        bk["prev_mid"] = bk["mid"]
        bk["mid"] = 0.5 * (float(bk["best_bid_ticks"]) + float(bk["best_ask_ticks"]))
        bk["ready"] = 1
    else:
        bk["mid"] = 0.0
        bk["ready"] = 0


# --------------------------------------------------------------------------- ring
@njit
def bucket_slot(ring, ts_ns, bucket_ns):
    sequence = ts_ns // bucket_ns
    index = int(sequence % BUCKETS)
    if ring["bkt_seq"][index] != sequence:
        ring["bkt_seq"][index] = sequence
        ring["bkt_mid"][index] = 0.0
        ring["bkt_buy"][index] = 0.0
        ring["bkt_sell"][index] = 0.0
        ring["bkt_maxq"][index] = 0.0
        ring["bkt_buy_n"][index] = 0
        ring["bkt_sell_n"][index] = 0
        ring["bkt_ofi"][index] = 0.0
        ring["bkt_depth"][index] = 0.0
        ring["bucket_seen"] += 1
        ring["cur_bucket"] = sequence
    return index


@njit
def bucket_index_for(ring, sequence):
    index = int(sequence % BUCKETS)
    if ring["bkt_seq"][index] != sequence:
        return -1
    return index


@njit
def window_lots(ring, window_ms, agg_side, bucket_ns):
    if window_ms <= 0:
        return 0.0
    span = int(window_ms * 1000000 // bucket_ns)
    current = ring["cur_bucket"]
    total = 0.0
    for k in range(span):
        index = bucket_index_for(ring, current - k)
        if index < 0:
            continue
        if agg_side == 0:
            total += ring["bkt_buy"][index] - ring["bkt_sell"][index]
        elif agg_side > 0:
            total += ring["bkt_buy"][index]
        else:
            total += ring["bkt_sell"][index]
    return total


@njit
def window_prints(ring, window_ms, agg_side, bucket_ns):
    span = int(window_ms * 1000000 // bucket_ns)
    current = ring["cur_bucket"]
    total = 0
    for k in range(span):
        index = bucket_index_for(ring, current - k)
        if index < 0:
            continue
        if agg_side > 0:
            total += ring["bkt_buy_n"][index]
        elif agg_side < 0:
            total += ring["bkt_sell_n"][index]
        else:
            total += ring["bkt_buy_n"][index] + ring["bkt_sell_n"][index]
    return total


@njit
def window_maxq(ring, window_ms, bucket_ns):
    span = int(window_ms * 1000000 // bucket_ns)
    current = ring["cur_bucket"]
    best = 0.0
    for k in range(span):
        index = bucket_index_for(ring, current - k)
        if index < 0:
            continue
        if ring["bkt_maxq"][index] > best:
            best = ring["bkt_maxq"][index]
    return best


@njit
def window_ofi(ring, window_ms, bucket_ns):
    span = int(window_ms * 1000000 // bucket_ns)
    current = ring["cur_bucket"]
    total = 0.0
    for k in range(span):
        index = bucket_index_for(ring, current - k)
        if index < 0:
            continue
        total += ring["bkt_ofi"][index]
    return total


@njit
def window_depth(ring, window_ms, bucket_ns):
    span = int(window_ms * 1000000 // bucket_ns)
    limit = span + 40
    for k in range(span, limit):
        index = bucket_index_for(ring, ring["cur_bucket"] - k)
        if index < 0:
            continue
        past = ring["bkt_depth"][index]
        if past > 0.0:
            return past
    return 0.0


@njit
def dp_bp(ring, window_ms, bucket_ns, mid_now):
    span = int(window_ms * 1000000 // bucket_ns)
    if span <= 0 or mid_now <= 0.0:
        return 0.0
    # A compute can be skipped (stale/spread/depth gate), so the bucket exactly `span`
    # back may have no mid. Walk backwards a bounded number of buckets instead of
    # returning 0.0, which would silently suppress every displacement detector.
    limit = span + 40
    for k in range(span, limit):
        index = bucket_index_for(ring, ring["cur_bucket"] - k)
        if index < 0:
            continue
        past = ring["bkt_mid"][index]
        if past > 0.0:
            return (mid_now - past) / past * 10000.0
    return 0.0


# --------------------------------------------------------------------------- stats
@njit
def ewm_update(ev, series, value, alpha):
    count = ev["ewm_n"][series]
    if count == 0:
        ev["ewm_mean"][series] = value
        ev["ewm_var"][series] = 0.0
        ev["ewm_n"][series] = 1
        return
    mean = ev["ewm_mean"][series]
    var = ev["ewm_var"][series]
    delta = value - mean
    mean = mean + alpha * delta
    var = (1.0 - alpha) * (var + alpha * delta * delta)
    ev["ewm_mean"][series] = mean
    ev["ewm_var"][series] = var
    if count < 1000000000:
        ev["ewm_n"][series] = count + 1


@njit
def ewm_z(ev, series, value):
    if ev["ewm_n"][series] < 8:
        return 0.0
    var = ev["ewm_var"][series]
    if var <= 1e-18:
        return 0.0
    return (value - ev["ewm_mean"][series]) / np.sqrt(var)


@njit
def sigma_window_bp(ring, window_ms):
    sigma = ring["sigma_bp"]
    if sigma <= 0.0:
        return 0.0
    scaled = sigma * np.sqrt(float(window_ms) / 1000.0)
    if scaled < ring["sigma_floor_bp"]:
        return ring["sigma_floor_bp"]
    return scaled


@njit
def second_mean(ring, seconds):
    total = 0.0
    count = 0
    latest = ring["last_sec"]
    for k in range(seconds):
        sequence = latest - k
        index = int(sequence % SECONDS)
        if ring["sec_seq"][index] != sequence:
            continue
        value = ring["sec_mid"][index]
        if value <= 0.0:
            continue
        total += value
        count += 1
    if count == 0:
        return 0.0
    return total / float(count)


@njit
def second_extremes(ring, seconds, mid_now):
    """High/low of the previous ``seconds`` completed seconds (excludes the current one)."""
    high = 0.0
    low = 0.0
    latest = ring["last_sec"]
    for k in range(1, seconds):
        sequence = latest - k
        index = int(sequence % SECONDS)
        if ring["sec_seq"][index] != sequence:
            continue
        value = ring["sec_mid"][index]
        if value <= 0.0:
            continue
        if high <= 0.0 or value > high:
            high = value
        if low <= 0.0 or value < low:
            low = value
    if high <= 0.0:
        high = mid_now
    if low <= 0.0:
        low = mid_now
    return high, low


# --------------------------------------------------------------------------- flow
@njit
def push_print(s, ts_ns, aggressor, qty_lots, price_ticks):
    """Record one print: a platform trade, or a depth-inferred touch reduction."""
    ring = s["ring"]
    flow = s["flow"]
    cfg = s["cfg"]
    index = bucket_slot(ring, ts_ns, cfg["bucket_ns"])
    if aggressor > 0:
        ring["bkt_buy"][index] += qty_lots
        ring["bkt_buy_n"][index] += 1
    else:
        ring["bkt_sell"][index] += qty_lots
        ring["bkt_sell_n"][index] += 1
    if qty_lots > ring["bkt_maxq"][index]:
        ring["bkt_maxq"][index] = qty_lots
    flow["prints_seen"] += 1
    flow["last_print_ns"] = ts_ns
    flow["last_print_px"] = float(price_ticks)
    gap_ns = cfg["e01_gap_ms"] * 1000000
    if flow["run_dir"] == aggressor and ts_ns - flow["run_last_ns"] <= gap_ns:
        flow["run_len"] += 1
    else:
        flow["run_dir"] = aggressor
        flow["run_len"] = 1
        flow["run_start_ns"] = ts_ns
        flow["run_start_mid"] = s["book"]["mid"]
        flow["run_adverse_bp"] = 0.0
    flow["run_last_ns"] = ts_ns
    mid = s["book"]["mid"]
    if mid > 0.0 and flow["run_start_mid"] > 0.0:
        if aggressor > 0:
            adverse = (flow["run_start_mid"] - mid) / flow["run_start_mid"] * 10000.0
        else:
            adverse = (mid - flow["run_start_mid"]) / flow["run_start_mid"] * 10000.0
        if adverse > flow["run_adverse_bp"]:
            flow["run_adverse_bp"] = adverse
    if flow["break_dir"] != 0 and aggressor == flow["break_dir"]:
        flow["break_lots"] += qty_lots
    if flow["ext_side"] != 0:
        flow["ext_prints"] += 1


@njit
def ingest_trade(s, tick):
    """Platform trade tick switch point (``kind == KIND_TRADE``)."""
    qty = float(tick["qty_lots"])
    if qty <= 0.0:
        return
    ts_ns = tick["exchange_ts_ns"]
    if ts_ns <= 0:
        ts_ns = tick["receive_ts_ns"]
    aggressor = AGGRESSOR_BUY if tick["side"] == SIDE_BID else AGGRESSOR_SELL
    push_print(s, ts_ns, aggressor, qty, tick["price_ticks"])


@njit
def touch_slot_for(s, price, side):
    flow = s["flow"]
    for i in range(TOUCH_SLOTS):
        if flow["touch_side"][i] == side and flow["touch_price"][i] == price:
            return i
    slot = int(flow["touch_n"] % TOUCH_SLOTS)
    flow["touch_n"] += 1
    flow["touch_side"][slot] = side
    flow["touch_price"][slot] = price
    flow["touch_ts"][slot] = 0
    flow["touch_q0"][slot] = 0.0
    flow["touch_qmin"][slot] = 0.0
    return slot


@njit
def ice_slot_for(s, price):
    flow = s["flow"]
    for i in range(TOUCH_SLOTS):
        if flow["ice_price"][i] == price:
            return i
    slot = int(flow["ice_n"] % TOUCH_SLOTS)
    flow["ice_n"] += 1
    flow["ice_price"][slot] = price
    flow["ice_initial"][slot] = 0.0
    flow["ice_cum"][slot] = 0.0
    flow["ice_fills"][slot] = 0
    flow["ice_last_ns"][slot] = 0
    flow["ice_refill_ms"][slot] = 0.0
    return slot


@njit
def track_touch(s, ts_ns, side, price, qty_before, qty_after):
    """Track refill (E03) and iceberg refill (E04) for one level update."""
    flow = s["flow"]
    slot = touch_slot_for(s, price, side)
    if flow["touch_ts"][slot] == 0:
        flow["touch_ts"][slot] = ts_ns
        flow["touch_q0"][slot] = qty_before
        flow["touch_qmin"][slot] = qty_after
    if qty_after < flow["touch_qmin"][slot]:
        flow["touch_qmin"][slot] = qty_after
    ice = ice_slot_for(s, price)
    if flow["ice_initial"][ice] <= 0.0 and qty_before > 0.0:
        flow["ice_initial"][ice] = qty_before
    if qty_after < qty_before:
        flow["ice_cum"][ice] += qty_before - qty_after
        if qty_after <= 0.0:
            flow["ice_fills"][ice] += 1
            flow["ice_last_ns"][ice] = ts_ns
    elif qty_before <= 0.0 and qty_after > 0.0 and flow["ice_last_ns"][ice] > 0:
        elapsed_ms = float(ts_ns - flow["ice_last_ns"][ice]) / 1000000.0
        if elapsed_ms <= float(s["cfg"]["e04_refill_ms"]):
            flow["ice_refill_ms"][ice] = elapsed_ms


@njit
def touch_refill_ratio(s, window_ms, now_ns):
    flow = s["flow"]
    bk = s["book"]
    total = 0.0
    for i in range(TOUCH_SLOTS):
        price = flow["touch_price"][i]
        if price <= 0 or flow["touch_ts"][i] == 0:
            continue
        if now_ns - flow["touch_ts"][i] > window_ms * 1000000:
            continue
        side = flow["touch_side"][i]
        current = bid_qty_at(bk, price) if side == SIDE_BID else ask_qty_at(bk, price)
        base = flow["touch_q0"][i] - flow["touch_qmin"][i]
        if base <= 0.0:
            continue
        recovered = current - flow["touch_qmin"][i]
        if recovered < 0.0:
            recovered = 0.0
        ratio = recovered / base
        if ratio > 1.0:
            ratio = 1.0
        total += ratio
    return total


@njit
def levels_consumed(s, side):
    flow = s["flow"]
    bk = s["book"]
    count = 0
    if side == SIDE_BID:
        for i in range(BOOK_LEVELS):
            price = flow["view_bid_price"][i]
            if price <= 0:
                continue
            if price >= bk["bid_price"][BOOK_LEVELS - 1] and bid_qty_at(bk, price) <= 0.0:
                count += 1
    else:
        for i in range(BOOK_LEVELS):
            price = flow["view_ask_price"][i]
            if price <= 0:
                continue
            if price <= bk["ask_price"][BOOK_LEVELS - 1] and ask_qty_at(bk, price) <= 0.0:
                count += 1
    return count


# --------------------------------------------------------------------------- emit
@njit
def emit(s, code, direction, strength, ts_ns):
    ev = s["ev"]
    obs = s["obs"]
    if ev["last_emit_ns"][code] != 0 and ts_ns - ev["last_emit_ns"][code] < s["cfg"]["cooldown_ms"] * 1000000:
        return
    ev["last_emit_ns"][code] = ts_ns
    ev["count"][code] += 1
    ev["direction"][code] = direction
    ev["strength"][code] = strength
    ev["events_total"] += 1
    obs["last_type"] = code
    obs["last_direction"] = direction
    obs["last_strength"] = strength
    obs["last_ts_ns"] = ts_ns
    obs["events_total"] = ev["events_total"]
    if code == E01:
        obs["e01_count"] += 1
        obs["e01_dir"] = direction
        obs["e01_strength"] = strength
    elif code == E02:
        obs["e02_count"] += 1
        obs["e02_dir"] = direction
        obs["e02_strength"] = strength
    elif code == E03:
        obs["e03_count"] += 1
        obs["e03_dir"] = direction
        obs["e03_strength"] = strength
    elif code == E04:
        obs["e04_count"] += 1
        obs["e04_dir"] = direction
        obs["e04_strength"] = strength
    elif code == E07:
        obs["e07_count"] += 1
        obs["e07_dir"] = direction
        obs["e07_strength"] = strength
    elif code == E08:
        obs["e08_count"] += 1
        obs["e08_dir"] = direction
        obs["e08_strength"] = strength
    elif code == E11:
        obs["e11_count"] += 1
        obs["e11_dir"] = direction
        obs["e11_strength"] = strength
    elif code == E13:
        obs["e13_count"] += 1
        obs["e13_dir"] = direction
        obs["e13_strength"] = strength
    elif code == E14:
        obs["e14_count"] += 1
        obs["e14_dir"] = direction
        obs["e14_strength"] = strength


# --------------------------------------------------------------------------- detectors
@njit
def detect_e01(s, now_ns):
    cfg = s["cfg"]
    if cfg["e01_enabled"] == 0:
        return
    flow = s["flow"]
    bk = s["book"]
    ring = s["ring"]
    if flow["run_len"] < cfg["e01_run_min"]:
        return
    if now_ns - flow["run_last_ns"] > cfg["e01_gap_ms"] * 1000000:
        return
    signed_dp = dp_bp(ring, cfg["e01_window_ms"], cfg["bucket_ns"], bk["mid"])
    if abs(signed_dp) < cfg["e01_dp_bp"]:
        return
    direction = 1 if signed_dp > 0.0 else -1
    if flow["run_dir"] != direction:
        return
    lots = window_lots(ring, cfg["e01_window_ms"], direction, cfg["bucket_ns"])
    volume_btc = lots * cfg["qty_lot"]
    if volume_btc < cfg["e01_vol_btc"]:
        return
    rho = flow["run_adverse_bp"] / max(abs(signed_dp), 1e-9)
    if rho > cfg["e01_rho"]:
        return
    strength = min(min(volume_btc / max(cfg["e01_vol_btc"], 1e-9),
                       abs(signed_dp) / max(cfg["e01_dp_bp"], 1e-9)),
                   1.0 + (cfg["e01_rho"] - rho))
    emit(s, E01, direction, strength, now_ns)


@njit
def detect_e02(s, now_ns):
    cfg = s["cfg"]
    if cfg["e02_enabled"] == 0:
        return
    ev = s["ev"]
    flow = s["flow"]
    ring = s["ring"]
    bk = s["book"]
    if flow["run_len"] < cfg["e01_run_min"]:
        return
    if now_ns - flow["run_start_ns"] < cfg["e02_min_ms"] * 1000000:
        return
    z_flow = ewm_z(ev, 0, ev["slope_last"])
    z_px = ewm_z(ev, 1, ev["px_slope_last"])
    if z_flow < cfg["e02_slope_z"] or z_px < cfg["e02_slope_z"]:
        return
    signed_dp = dp_bp(ring, cfg["e02_window_ms"], cfg["bucket_ns"], bk["mid"])
    direction = 1 if signed_dp > 0.0 else -1
    if flow["run_dir"] != direction:
        return
    emit(s, E02, direction, min(z_flow, z_px) / max(cfg["e02_slope_z"], 1e-9), now_ns)


@njit
def detect_e03(s, now_ns):
    cfg = s["cfg"]
    if cfg["e03_enabled"] == 0:
        return
    ring = s["ring"]
    bk = s["book"]
    window_ms = cfg["e03_window_ms"]
    signed_lots = window_lots(ring, window_ms, 0, cfg["bucket_ns"])
    volume_btc = abs(signed_lots) * cfg["qty_lot"]
    if volume_btc < cfg["e03_vol_btc"]:
        return
    impact = abs(dp_bp(ring, window_ms, cfg["bucket_ns"], bk["mid"]))
    if impact > cfg["e03_impact_bp"]:
        return
    kappa = impact / max(volume_btc, 1e-9)
    if kappa > cfg["e03_kappa_bp_per_btc"]:
        return
    refill = touch_refill_ratio(s, window_ms, now_ns)
    if refill < cfg["e03_refill"]:
        return
    direction = -1 if signed_lots > 0.0 else 1
    strength = min(min(volume_btc / max(cfg["e03_vol_btc"], 1e-9),
                       cfg["e03_kappa_bp_per_btc"] / max(kappa, 1e-9)),
                   refill / max(cfg["e03_refill"], 1e-9))
    emit(s, E03, direction, strength, now_ns)


@njit
def detect_e04(s, now_ns):
    cfg = s["cfg"]
    if cfg["e04_enabled"] == 0:
        return
    flow = s["flow"]
    mid = s["book"]["mid"]
    window_ns = cfg["e04_window_ms"] * 1000000
    for i in range(TOUCH_SLOTS):
        price = flow["ice_price"][i]
        if price <= 0 or flow["ice_last_ns"][i] == 0:
            continue
        if now_ns - flow["ice_last_ns"][i] > window_ns:
            continue
        if flow["ice_initial"][i] <= 0.0:
            continue
        if flow["ice_fills"][i] < cfg["e04_fills"]:
            continue
        if flow["ice_refill_ms"][i] <= 0.0 or flow["ice_refill_ms"][i] > float(cfg["e04_refill_ms"]):
            continue
        direction = 1 if float(price) < mid else -1
        refill_ratio = float(cfg["e04_refill_ms"]) / max(flow["ice_refill_ms"][i], 1e-9)
        hidden_ratio = flow["ice_cum"][i] / flow["ice_initial"][i]
        strength = min(float(flow["ice_fills"][i]) / max(float(cfg["e04_fills"]), 1.0),
                       min(refill_ratio, hidden_ratio))
        emit(s, E04, direction, strength, now_ns)
        flow["ice_fills"][i] = 0
        flow["ice_refill_ms"][i] = 0.0
        return


@njit
def detect_e07(s, now_ns):
    cfg = s["cfg"]
    if cfg["e07_enabled"] == 0:
        return
    ring = s["ring"]
    bk = s["book"]
    flow = s["flow"]
    window_ms = cfg["e07_window_ms"]
    buy_btc = window_lots(ring, window_ms, 1, cfg["bucket_ns"]) * cfg["qty_lot"]
    sell_btc = window_lots(ring, window_ms, -1, cfg["bucket_ns"]) * cfg["qty_lot"]
    direction = 1 if buy_btc > sell_btc else -1
    signed_dp = dp_bp(ring, window_ms, cfg["bucket_ns"], bk["mid"])
    if abs(signed_dp) < cfg["e07_dp_bp"]:
        return
    if signed_dp * float(direction) <= 0.0:
        return
    consumed = levels_consumed(s, SIDE_ASK if direction > 0 else SIDE_BID)
    one_side_btc = buy_btc if direction > 0 else sell_btc
    if consumed < cfg["e07_levels"] and one_side_btc < cfg["e07_vol_btc"]:
        return
    if direction > 0:
        pre_depth = flow["view_ask_depth5"]
    else:
        pre_depth = flow["view_bid_depth5"]
    pre_depth_btc = pre_depth * cfg["qty_lot"]
    if pre_depth_btc * cfg["e07_pre_depth_ratio"] > one_side_btc:
        return
    strength = min(min(float(consumed) / max(float(cfg["e07_levels"]), 1.0),
                       one_side_btc / max(cfg["e07_vol_btc"], 1e-9)),
                   abs(signed_dp) / max(cfg["e07_dp_bp"], 1e-9))
    emit(s, E07, direction, strength, now_ns)


@njit
def ofi_increment(bk, side, price, qty):
    if side == SIDE_BID:
        previous_price = bk["prev_bid_ticks"]
        previous_qty = bk["prev_bid_qty"]
        if previous_price <= 0:
            bk["prev_bid_ticks"] = price
            bk["prev_bid_qty"] = qty
            return 0.0
        delta = 0.0
        if price >= previous_price:
            delta += qty
        if price <= previous_price:
            delta -= previous_qty
        bk["prev_bid_ticks"] = price
        bk["prev_bid_qty"] = qty
        return delta
    previous_price = bk["prev_ask_ticks"]
    previous_qty = bk["prev_ask_qty"]
    if previous_price <= 0:
        bk["prev_ask_ticks"] = price
        bk["prev_ask_qty"] = qty
        return 0.0
    delta = 0.0
    if price <= previous_price:
        delta += qty
    if price >= previous_price:
        delta -= previous_qty
    bk["prev_ask_ticks"] = price
    bk["prev_ask_qty"] = qty
    return -delta


@njit
def detect_e08(s, now_ns):
    cfg = s["cfg"]
    if cfg["e08_enabled"] == 0:
        return
    ev = s["ev"]
    bk = s["book"]
    ofi = ev["ofi_now"]
    ofi_z = ewm_z(ev, 6, ofi)
    depth5 = bk["depth5_bid"] + bk["depth5_ask"]
    h_ratio = 0.0
    if depth5 > 0.0 and ev["prev_depth5"] > 0.0:
        h_ratio = ev["h_now"]
    if abs(ofi_z) < cfg["e08_z"]:
        ev["ofi_persist"] = 0
        return
    if h_ratio > -cfg["e08_h_ratio"]:
        ev["ofi_persist"] = 0
        return
    ev["ofi_persist"] += 1
    if ev["ofi_persist"] < cfg["e08_persist"]:
        return
    direction = 1 if ofi_z > 0.0 else -1
    emit(s, E08, direction, abs(ofi_z) / max(cfg["e08_z"], 1e-9), now_ns)


@njit
def detect_e11(s, now_ns):
    cfg = s["cfg"]
    if cfg["e11_enabled"] == 0:
        return
    flow = s["flow"]
    ring = s["ring"]
    bk = s["book"]
    mid = bk["mid"]
    if mid <= 0.0:
        return
    seconds = int(cfg["e11_lookback_ms"] // 1000)
    if seconds >= SECONDS:
        seconds = SECONDS - 1
    high, low = second_extremes(ring, seconds, mid)
    break_bp = cfg["e11_break_bp"]
    if flow["break_dir"] == 0:
        if mid > high * (1.0 + break_bp / 10000.0):
            flow["break_dir"] = 1
            flow["break_ns"] = now_ns
            flow["break_px"] = high
            flow["break_ext"] = mid
            flow["break_lots"] = 0.0
        elif mid < low * (1.0 - break_bp / 10000.0):
            flow["break_dir"] = -1
            flow["break_ns"] = now_ns
            flow["break_px"] = low
            flow["break_ext"] = mid
            flow["break_lots"] = 0.0
        return
    if flow["break_dir"] > 0:
        if mid < high:
            flow["break_dir"] = 0
            flow["break_lots"] = 0.0
            return
        if mid > flow["break_ext"]:
            flow["break_ext"] = mid
    else:
        if mid > low:
            flow["break_dir"] = 0
            flow["break_lots"] = 0.0
            return
        if mid < flow["break_ext"]:
            flow["break_ext"] = mid
    if now_ns - flow["break_ns"] < cfg["e11_min_ms"] * 1000000:
        return
    swept_btc = flow["break_lots"] * cfg["qty_lot"]
    if swept_btc < cfg["e11_vol_btc"]:
        return
    direction = -flow["break_dir"]
    strength = min(swept_btc / max(cfg["e11_vol_btc"], 1e-9), 2.0)
    emit(s, E11, direction, strength, now_ns)
    flow["break_dir"] = 0
    flow["break_lots"] = 0.0


@njit
def detect_e13(s, now_ns):
    cfg = s["cfg"]
    if cfg["e13_enabled"] == 0:
        return
    ev = s["ev"]
    ring = s["ring"]
    bk = s["book"]
    window_ms = cfg["e13_window_ms"]
    signed_dp = dp_bp(ring, window_ms, cfg["bucket_ns"], bk["mid"])
    sigma = sigma_window_bp(ring, window_ms)
    if sigma <= 0.0:
        return
    impact_z = abs(signed_dp) / sigma
    if impact_z < cfg["e13_impact_z"]:
        return
    direction = 1 if signed_dp > 0.0 else -1
    volume_btc = abs(window_lots(ring, window_ms, 0, cfg["bucket_ns"])) * cfg["qty_lot"]
    maxq_btc = window_maxq(ring, window_ms, cfg["bucket_ns"]) * cfg["qty_lot"]
    vol_z = ewm_z(ev, 3, volume_btc)
    maxq_z = ewm_z(ev, 4, maxq_btc)
    if vol_z < cfg["e13_vol_z"] and maxq_z < cfg["e13_maxq_z"]:
        return
    prints = window_prints(ring, window_ms, direction, cfg["bucket_ns"])
    if prints < cfg["e13_min_prints"]:
        return
    strength = min(impact_z / max(cfg["e13_impact_z"], 1e-9),
                   max(vol_z / max(cfg["e13_vol_z"], 1e-9), maxq_z / max(cfg["e13_maxq_z"], 1e-9)))
    emit(s, E13, direction, strength, now_ns)


@njit
def detect_e14(s, now_ns):
    cfg = s["cfg"]
    if cfg["e14_enabled"] == 0:
        return
    ev = s["ev"]
    flow = s["flow"]
    ring = s["ring"]
    bk = s["book"]
    mid = bk["mid"]
    if mid <= 0.0:
        return
    seconds = int(cfg["e14_lookback_ms"] // 1000)
    if seconds >= SECONDS:
        seconds = SECONDS - 1
    base = second_mean(ring, seconds)
    if base <= 0.0:
        return
    sigma = sigma_window_bp(ring, cfg["e14_window_ms"])
    if sigma <= 0.0:
        return
    displacement = abs(mid - base) / base * 10000.0
    if flow["ext_done"] != 0:
        if displacement / sigma < 0.5 * cfg["e14_disp_z"]:
            flow["ext_done"] = 0
            flow["ext_side"] = 0
            flow["ext_prints"] = 0
        return
    if flow["ext_side"] == 0:
        if displacement / sigma < cfg["e14_disp_z"]:
            return
        flow["ext_side"] = 1 if mid > base else -1
        flow["ext_px"] = mid
        flow["ext_ns"] = now_ns
        flow["ext_prints"] = 0
        return
    if flow["ext_side"] > 0 and mid > flow["ext_px"]:
        flow["ext_px"] = mid
        flow["ext_ns"] = now_ns
        flow["ext_prints"] = 0
    elif flow["ext_side"] < 0 and mid < flow["ext_px"]:
        flow["ext_px"] = mid
        flow["ext_ns"] = now_ns
        flow["ext_prints"] = 0
    if flow["ext_prints"] < cfg["e14_prints"]:
        return
    volume_btc = abs(window_lots(ring, cfg["e14_window_ms"], 0, cfg["bucket_ns"])) * cfg["qty_lot"]
    vol_z = ewm_z(ev, 5, volume_btc)
    if vol_z < cfg["e14_vol_z"]:
        return
    extreme_disp = abs(flow["ext_px"] - base) / base * 10000.0
    strength = min(extreme_disp / max(cfg["e14_disp_z"] * sigma, 1e-9),
                   vol_z / max(cfg["e14_vol_z"], 1e-9))
    emit(s, E14, flow["ext_side"], strength, now_ns)
    flow["ext_done"] = 1
    flow["ext_prints"] = 0


# --------------------------------------------------------------------------- compute
@njit
def refresh_view(s, now_ns):
    flow = s["flow"]
    bk = s["book"]
    flow["view_bid_depth5"] = bk["depth5_bid"]
    flow["view_ask_depth5"] = bk["depth5_ask"]
    flow["view_ns"] = now_ns
    for i in range(BOOK_LEVELS):
        flow["view_bid_price"][i] = bk["bid_price"][i]
        flow["view_ask_price"][i] = bk["ask_price"][i]


@njit
def update_seconds(s, now_ns):
    ring = s["ring"]
    bk = s["book"]
    mid = bk["mid"]
    if mid <= 0.0:
        return
    second = now_ns // 1000000000
    if ring["last_sec"] == second:
        return
    ring["last_sec"] = second
    index = int(second % SECONDS)
    ring["sec_seq"][index] = second
    ring["sec_mid"][index] = mid
    ring["second_seen"] += 1
    previous = ring["prev_sec_mid"]
    if previous > 0.0:
        ret_bp = (mid - previous) / previous * 10000.0
        alpha = 1.0 - np.power(0.5, 1.0 / 15.0)
        ring["sigma_var"] = (1.0 - alpha) * ring["sigma_var"] + alpha * ret_bp * ret_bp
        ring["sigma_bp"] = np.sqrt(ring["sigma_var"])
    ring["prev_sec_mid"] = mid


@njit
def update_baselines(s, now_ns):
    """Update the EWMA baselines once per compute, unconditionally."""
    cfg = s["cfg"]
    ev = s["ev"]
    ring = s["ring"]
    bk = s["book"]
    qty_lot = cfg["qty_lot"]
    flow_1s = window_lots(ring, 1000, 0, cfg["bucket_ns"])
    dp_1s = abs(dp_bp(ring, 1000, cfg["bucket_ns"], bk["mid"]))
    if ev["slope_ns"] > 0:
        elapsed = float(now_ns - ev["slope_ns"]) / 1e9
        if elapsed > 0.0:
            slope_flow = (flow_1s - ev["slope_prev"]) / elapsed
            slope_px = (dp_1s - ev["px_slope_prev"]) / elapsed
            ewm_update(ev, 0, slope_flow, 0.05)
            ewm_update(ev, 1, slope_px, 0.05)
            ev["slope_last"] = slope_flow
            ev["px_slope_last"] = slope_px
    ev["slope_prev"] = flow_1s
    ev["px_slope_prev"] = dp_1s
    ev["slope_ns"] = now_ns
    volume_e13 = abs(window_lots(ring, cfg["e13_window_ms"], 0, cfg["bucket_ns"])) * qty_lot
    maxq_e13 = window_maxq(ring, cfg["e13_window_ms"], cfg["bucket_ns"]) * qty_lot
    ewm_update(ev, 3, volume_e13, 0.05)
    ewm_update(ev, 4, maxq_e13, 0.02)
    volume_e14 = abs(window_lots(ring, cfg["e14_window_ms"], 0, cfg["bucket_ns"])) * qty_lot
    ewm_update(ev, 5, volume_e14, 0.02)
    ofi = window_ofi(ring, cfg["e08_window_ms"], cfg["bucket_ns"])
    ev["ofi_now"] = ofi
    ewm_update(ev, 6, ofi, 0.05)
    depth5 = bk["depth5_bid"] + bk["depth5_ask"]
    index = bucket_slot(ring, now_ns, cfg["bucket_ns"])
    ring["bkt_depth"][index] = depth5
    depth_past = window_depth(ring, cfg["e08_window_ms"], cfg["bucket_ns"])
    if depth5 > 0.0 and depth_past > 0.0:
        flow_lots = window_lots(ring, cfg["e08_window_ms"], 0, cfg["bucket_ns"])
        hidden = -(depth5 - depth_past) + flow_lots
        ev["h_now"] = hidden / max(depth_past, 1e-9)
    else:
        ev["h_now"] = 0.0
    ev["prev_depth5"] = depth5


@njit
def compute_events(s, now_ns):
    cfg = s["cfg"]
    bk = s["book"]
    ring = s["ring"]
    ev = s["ev"]
    obs = s["obs"]
    if bk["ready"] == 0 or bk["mid"] <= 0.0:
        obs["book_ready"] = 0
        obs["book_valid"] = 0
        return
    if bk["last_update_ns"] > 0 and now_ns - bk["last_update_ns"] > cfg["stale_after_ns"]:
        bk["stale"] = 1
        obs["book_ready"] = 0
        obs["book_valid"] = 0
        return
    bk["stale"] = 0
    update_seconds(s, now_ns)
    index = bucket_slot(ring, now_ns, cfg["bucket_ns"])
    ring["bkt_mid"][index] = bk["mid"]
    mid = bk["mid"]
    spread_bp = 0.0
    if bk["best_bid_ticks"] > 0 and bk["best_ask_ticks"] > 0:
        spread_bp = (float(bk["best_ask_ticks"]) - float(bk["best_bid_ticks"])) / mid * 10000.0
    depth5_btc = (bk["depth5_bid"] + bk["depth5_ask"]) * cfg["qty_lot"]
    if cfg["max_spread_bp"] > 0.0 and spread_bp > cfg["max_spread_bp"]:
        return
    if cfg["min_depth_btc"] > 0.0 and depth5_btc < cfg["min_depth_btc"]:
        return
    if now_ns - s["flow"]["view_ns"] >= 1000000000:
        refresh_view(s, now_ns)
    update_baselines(s, now_ns)
    obs["mid_ticks"] = mid
    obs["spread_bp"] = spread_bp
    obs["sigma_bp"] = ring["sigma_bp"]
    obs["ofi_2s"] = window_ofi(ring, 2000, cfg["bucket_ns"])
    obs["depth1_btc"] = (bk["depth1_bid"] + bk["depth1_ask"]) * cfg["qty_lot"]
    obs["depth5_btc"] = depth5_btc
    obs["prints_seen"] = s["flow"]["prints_seen"]
    obs["dropped"] = s["flow"]["dropped"]
    obs["asset_no"] = cfg["asset_no"]
    obs["book_ready"] = bk["ready"]
    obs["flow_source"] = cfg["flow_source"]
    obs["book_valid"] = 1
    obs["stream_epoch"] = bk["epoch"]
    obs["book_resets"] = bk["resets"]
    # A reset discards all historical windows. Accumulate fresh observations
    # for the longest configured lookback before enabling any detector.
    warm_ms = max(cfg["e11_lookback_ms"], cfg["e14_lookback_ms"], cfg["e14_window_ms"])
    obs["warmup_remaining_ms"] = max(0, warm_ms - (now_ns - bk["warm_since_ns"]) // 1000000)
    if obs["warmup_remaining_ms"] > 0:
        obs["book_ready"] = 0
        return
    detect_e01(s, now_ns)
    detect_e02(s, now_ns)
    detect_e03(s, now_ns)
    detect_e04(s, now_ns)
    detect_e07(s, now_ns)
    detect_e08(s, now_ns)
    detect_e11(s, now_ns)
    detect_e13(s, now_ns)
    detect_e14(s, now_ns)



@njit
def reset_depth_epoch(s, now_ns):
    book_clear(s["book"])
    s["book"]["warm_since_ns"] = now_ns
    s["book"]["last_update_ns"] = 0
    s["book"]["stale"] = 0
    s["book"]["resets"] += 1
    s["obs"]["book_ready"] = 0
    s["obs"]["book_valid"] = 0
    s["obs"]["mid_ticks"] = 0.0
    s["obs"]["depth1_btc"] = 0.0
    s["obs"]["depth5_btc"] = 0.0
    for i in range(BUCKETS):
        s["ring"]["bkt_seq"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_mid"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_buy"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_sell"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_maxq"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_buy_n"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_sell_n"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_ofi"][i] = 0
    for i in range(BUCKETS):
        s["ring"]["bkt_depth"][i] = 0
    s["ring"]["cur_bucket"] = 0
    s["ring"]["bucket_seen"] = 0
    for i in range(SECONDS):
        s["ring"]["sec_seq"][i] = 0
    for i in range(SECONDS):
        s["ring"]["sec_mid"][i] = 0
    s["ring"]["last_sec"] = 0
    s["ring"]["second_seen"] = 0
    s["ring"]["sigma_var"] = 0
    s["ring"]["sigma_bp"] = 0
    s["ring"]["prev_sec_mid"] = 0
    s["flow"]["run_dir"] = 0
    s["flow"]["run_len"] = 0
    s["flow"]["run_start_ns"] = 0
    s["flow"]["run_last_ns"] = 0
    s["flow"]["run_start_mid"] = 0
    s["flow"]["run_adverse_bp"] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["touch_price"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["touch_side"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["touch_q0"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["touch_qmin"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["touch_ts"][i] = 0
    s["flow"]["touch_n"] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_price"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_initial"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_cum"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_fills"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_last_ns"][i] = 0
    for i in range(TOUCH_SLOTS):
        s["flow"]["ice_refill_ms"][i] = 0
    s["flow"]["ice_n"] = 0
    for i in range(BOOK_LEVELS):
        s["flow"]["view_bid_price"][i] = 0
    for i in range(BOOK_LEVELS):
        s["flow"]["view_ask_price"][i] = 0
    s["flow"]["view_bid_depth5"] = 0.0
    s["flow"]["view_ask_depth5"] = 0.0
    s["flow"]["view_ns"] = 0
    s["flow"]["break_dir"] = 0
    s["flow"]["break_ns"] = 0
    s["flow"]["break_px"] = 0
    s["flow"]["break_ext"] = 0
    s["flow"]["break_lots"] = 0
    s["flow"]["ext_side"] = 0
    s["flow"]["ext_px"] = 0
    s["flow"]["ext_ns"] = 0
    s["flow"]["ext_prints"] = 0
    s["flow"]["ext_done"] = 0
    s["flow"]["last_print_ns"] = 0
    s["flow"]["last_print_px"] = 0
    for i in range(EVENT_TYPES):
        s["ev"]["last_emit_ns"][i] = 0
    for i in range(EVENT_TYPES):
        s["ev"]["direction"][i] = 0
    for i in range(EVENT_TYPES):
        s["ev"]["strength"][i] = 0
    s["ev"]["ofi_now"] = 0
    s["ev"]["ofi_persist"] = 0
    s["ev"]["h_now"] = 0
    s["ev"]["prev_depth5"] = 0
    s["ev"]["slope_prev"] = 0
    s["ev"]["slope_ns"] = 0
    s["ev"]["px_slope_prev"] = 0
    s["ev"]["slope_last"] = 0
    s["ev"]["px_slope_last"] = 0
    for i in range(EWM_SERIES):
        s["ev"]["ewm_mean"][i] = 0
    for i in range(EWM_SERIES):
        s["ev"]["ewm_var"][i] = 0
    for i in range(EWM_SERIES):
        s["ev"]["ewm_n"][i] = 0


# --------------------------------------------------------------------------- handlers
@njit
def on_start(ctx):
    s = ctx.state
    bk = s["book"]
    cfg = s["cfg"]
    reset_depth_epoch(s, ctx.now)


@njit
def on_tick(ctx):
    s = ctx.state
    cfg = s["cfg"]
    bk = s["book"]
    ring = s["ring"]
    for tick in ctx.ticks():
        if tick["asset_no"] != cfg["asset_no"]:
            continue
        ts_ns = tick["exchange_ts_ns"]
        if ts_ns <= 0:
            ts_ns = tick["receive_ts_ns"]
        receive_ns = tick["receive_ts_ns"]
        if receive_ns > 0 and receive_ns > ctx.now + 1000000000:
            s["flow"]["dropped"] += 1
            continue
        if ts_ns <= 0:
            s["flow"]["dropped"] += 1
            continue
        if tick["kind"] == KIND_TRADE:
            ingest_trade(s, tick)
            continue
        if bk["ready"] == 0 or ctx.now - bk["last_update_ns"] > cfg["stale_after_ns"]:
            continue
        price = tick["price_ticks"]
        if price <= 0:
            continue
        side = tick["side"]
        qty = float(tick["qty_lots"])
        delta = ofi_increment(bk, side, price, qty)
        index = bucket_slot(ring, ts_ns, cfg["bucket_ns"])
        ring["bkt_ofi"][index] += delta


@njit
def known_deleted_level(s, side, price, ts_ns):
    bk = s["book"]
    # Absence only proves zero within the preceding projection's covered range.
    if side == SIDE_BID and price < bk["bid_price"][BOOK_LEVELS - 1]:
        return False
    if side == SIDE_ASK and price > bk["ask_price"][BOOK_LEVELS - 1]:
        return False
    flow = s["flow"]
    known = False
    for i in range(TOUCH_SLOTS):
        if flow["touch_price"][i] == price and flow["touch_side"][i] == side and flow["touch_qmin"][i] == 0.0:
            known = True
    if not known:
        return False
    window_ns = max(s["cfg"]["e03_window_ms"], s["cfg"]["e04_window_ms"]) * 1000000
    for i in range(TOUCH_SLOTS):
        if flow["ice_price"][i] == price and flow["ice_last_ns"][i] > 0:
            age = ts_ns - flow["ice_last_ns"][i]
            if age >= 0 and age <= window_ns:
                return True
    return False


@njit
def on_depth(ctx):
    s = ctx.state
    cfg = s["cfg"]
    bk = s["book"]
    batches = ctx.depth_batch()
    if len(batches) != 1:
        reset_depth_epoch(s, ctx.now)
        return
    info = batches[0]
    if info["version"] != 1 or info["asset_no"] != cfg["asset_no"]:
        return
    reset = info["reset"] != 0 or bk["epoch"] != info["stream_epoch"] or bk["ready"] == 0 or bk["stale"] != 0
    if bk["last_update_ns"] > 0 and ctx.now - bk["last_update_ns"] > cfg["stale_after_ns"]:
        reset = True
    if reset or info["valid"] == 0:
        reset_depth_epoch(s, ctx.now)
        bk["epoch"] = info["stream_epoch"]
        s["obs"]["stream_epoch"] = info["stream_epoch"]
        s["obs"]["book_resets"] = bk["resets"]
    if info["valid"] == 0:
        return
    items = ctx.depth()
    if len(items) != 2 * BOOK_LEVELS:
        reset_depth_epoch(s, ctx.now)
        return
    ts_ns = items[0]["exchange_ts_ns"]
    if ts_ns <= 0:
        ts_ns = items[0]["receive_ts_ns"]
    if ts_ns <= 0 or items[0]["receive_ts_ns"] > ctx.now + 1000000000:
        reset_depth_epoch(s, ctx.now)
        return
    previous_best_bid = bk["best_bid_ticks"]
    previous_best_ask = bk["best_ask_ticks"]
    # Compare against the old projection before replacing it. Prices entering
    # the window have unknown prior size and must not count as refills.
    if not reset:
        for item in items:
            price = item["price_ticks"]
            qty = float(item["qty_lots"])
            side = item["side"]
            before = bid_qty_at(bk, price) if side == SIDE_BID else ask_qty_at(bk, price)
            if qty != before and (before > 0.0 or known_deleted_level(s, side, price, ts_ns)):
                track_touch(s, ts_ns, side, price, before, qty)
                if qty < before and cfg["flow_source"] == FLOW_DEPTH_PROXY:
                    if side == SIDE_BID and price >= previous_best_bid:
                        push_print(s, ts_ns, AGGRESSOR_SELL, before - qty, price)
                    elif side == SIDE_ASK and price <= previous_best_ask:
                        push_print(s, ts_ns, AGGRESSOR_BUY, before - qty, price)
        # Explicitly removed levels are absent from the replacement. Only
        # infer removal within the new covered range, never on window exit.
        bid_floor = items[BOOK_LEVELS - 1]["price_ticks"]
        ask_ceiling = items[2 * BOOK_LEVELS - 1]["price_ticks"]
        for i in range(BOOK_LEVELS):
            for side in range(1, 3):
                price = bk["bid_price"][i] if side == SIDE_BID else bk["ask_price"][i]
                before = bk["bid_qty"][i] if side == SIDE_BID else bk["ask_qty"][i]
                covered = price >= bid_floor if side == SIDE_BID else price <= ask_ceiling
                present = False
                offset = 0 if side == SIDE_BID else BOOK_LEVELS
                for j in range(BOOK_LEVELS):
                    if items[offset + j]["price_ticks"] == price:
                        present = True
                        break
                if before > 0.0 and covered and not present:
                    track_touch(s, ts_ns, side, price, before, 0.0)
                    if cfg["flow_source"] == FLOW_DEPTH_PROXY:
                        if side == SIDE_BID and price >= previous_best_bid:
                            push_print(s, ts_ns, AGGRESSOR_SELL, before, price)
                        elif side == SIDE_ASK and price <= previous_best_ask:
                            push_print(s, ts_ns, AGGRESSOR_BUY, before, price)
    for i in range(BOOK_LEVELS):
        bk["bid_price"][i] = items[i]["price_ticks"]
        bk["bid_qty"][i] = float(items[i]["qty_lots"])
        bk["ask_price"][i] = items[BOOK_LEVELS + i]["price_ticks"]
        bk["ask_qty"][i] = float(items[BOOK_LEVELS + i]["qty_lots"])
    bk["last_update_ns"] = ts_ns
    refresh_book(bk, cfg["depth_levels"])


@njit
def on_timer(ctx):
    s = ctx.state
    cfg = s["cfg"]
    now_ns = ctx.now
    if now_ns - s["ev"]["last_compute_ns"] < cfg["compute_interval_ns"]:
        return
    s["ev"]["last_compute_ns"] = now_ns
    s["ev"]["compute_count"] += 1
    compute_events(s, now_ns)
    s["obs"]["computes"] = s["ev"]["compute_count"]


# --------------------------------------------------------------------------- build
_RAW_PARAM_SKIP = {
    "asset_no", "compute_interval_ms", "qty_lot", "sigma_floor_bp", "flow_source",
    "cooldown_ms", "stale_after_ms", "depth_levels", "max_spread_bp",
    "min_depth_btc",
}


def build(parameters):
    p = SPEC.validate_parameters(parameters)
    if int(p["compute_interval_ms"]) * 1000000 != BUCKET_NS:
        raise ValueError("compute_interval_ms must be 100: the bucket ring assumes a 100 ms cadence")
    for name in ("e01_window_ms", "e02_window_ms", "e03_window_ms", "e07_window_ms",
                 "e08_window_ms", "e13_window_ms", "e14_window_ms"):
        if int(p[name]) % 100 != 0:
            raise ValueError(f"{name} must be a multiple of 100 ms")
    if int(p["e13_window_ms"]) >= BUCKETS * 100:
        raise ValueError("e13_window_ms exceeds the bucket ring")
    if int(p["e14_window_ms"]) >= BUCKETS * 100 - 100:
        raise ValueError("e14_window_ms exceeds the bucket ring (63.9 s)")
    if int(p["e11_lookback_ms"]) >= SECONDS * 1000:
        raise ValueError("e11_lookback_ms exceeds the 15 min second ring")
    if int(p["e14_lookback_ms"]) >= SECONDS * 1000:
        raise ValueError("e14_lookback_ms exceeds the 15 min second ring")
    if p["flow_source"] == "platform_trades":
        # The ABI V13 runtime maps only BBO and DEPTH into a strategy today; selecting
        # this source would produce zero prints and therefore zero events.
        raise ValueError("flow_source=platform_trades is unavailable until ABI V13 exposes trade ticks")
    state = new_state(state_dtype)
    cfg = state[0]["cfg"]
    cfg["asset_no"] = p["asset_no"]
    cfg["compute_interval_ns"] = int(p["compute_interval_ms"]) * 1000000
    cfg["bucket_ns"] = BUCKET_NS
    cfg["flow_source"] = FLOW_PLATFORM_TRADES if p["flow_source"] == "platform_trades" else FLOW_DEPTH_PROXY
    cfg["cooldown_ms"] = p["cooldown_ms"]
    cfg["stale_after_ns"] = int(p["stale_after_ms"]) * 1000000
    cfg["qty_lot"] = p["qty_lot"]
    cfg["depth_levels"] = min(int(p["depth_levels"]), BOOK_LEVELS)
    cfg["sigma_floor_bp"] = p["sigma_floor_bp"]
    cfg["max_spread_bp"] = p["max_spread_bp"]
    cfg["min_depth_btc"] = p["min_depth_btc"]
    for name in cfg.dtype.names:
        if name in _RAW_PARAM_SKIP or name not in p:
            continue
        cfg[name] = p[name]
    state[0]["ring"]["sigma_floor_bp"] = p["sigma_floor_bp"]
    state[0]["obs"]["flow_source"] = cfg["flow_source"]
    handlers = {"on_start": on_start, "on_tick": on_tick, "on_depth": on_depth,
                "on_timer": on_timer}
    return StrategyDefinition(
        SPEC, state, handlers,
        {"description": "L0 order-flow event collector: BBO+depth in, per-event counters out"},
        business_observation=OBSERVATION,
    )
