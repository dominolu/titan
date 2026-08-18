===========
HftBacktest
===========

|rustc| |license|

High-Frequency Trading Backtesting Tool
=======================================

This framework is designed for developing high frequency trading and market making strategies. It focuses on accounting for both feed and order latencies, as well as the order queue position for order fill simulation. The framework aims to provide more accurate market replay-based backtesting, based on full order book and trade tick feed data.

This repository is a trimmed and actively extended fork of the upstream `hftbacktest <https://github.com/nkaz001/hftbacktest>`_ project. Non-essential assets (notebook examples, documentation sources, CI workflows, and community files) have been removed so that the codebase concentrates on the core engine, live trading, data collection, and exchange connectors.

Why Accurate Backtesting Matters — Not Just Conservative Approach
=================================================================

Trading is a highly competitive field where only the small edges usually exist, but they can still make a significant
difference. Because of this, backtesting must accurately simulate real-world conditions: it should neither rely on an
overly pessimistic approach that hides these small edges and profit opportunities, nor on an overly optimistic one that
overstates them through unrealistic simulation. Or at the very least, you should clearly understand what differs from
live trading and by how much, since sometimes fully accurate backtesting is not practical due to the time it requires.

This is not about overfitting at the start — before you even consider issues like overfitting, you need confidence that
your backtesting truly reflects real-world execution. For example, if you run a live trading strategy in January 2025,
the backtest for that exact period should produce results that closely align with the actual results. Once you’ve
validated that your backtesting can accurately reproduce live trading results, then you can proceed to deeper research,
optimization, and considerations around overfitting.

Accurate backtesting is the foundation. Without it, all further analysis — whether conservative or aggressive — becomes
unreliable.

Key Features
============

* Working in `Numba <https://numba.pydata.org/>`_ JIT function (Python).
* Complete tick-by-tick simulation with a customizable time interval or based on the feed and order receipt.
* Full order book reconstruction based on Level-2 Market-By-Price and Level-3 Market-By-Order feeds.
* Backtest accounting for both feed and order latency, using provided models or your own custom model.
* Order fill simulation that takes into account the order queue position, using provided models or your own custom model.
* Backtesting of multi-asset and multi-exchange models.
* Live trading with the same algorithm code for backtesting and live: Binance Futures, Binance Spot, and Bybit (available from both Rust and Python).

Documentation
=============

The upstream project maintains `full documentation <https://hftbacktest.readthedocs.io/>`_, which is still largely applicable. Note that the local ``docs/`` sources have been removed from this repository.

Repository Structure
====================

This is a Cargo workspace with the following members:

* ``hftbacktest/`` — Core engine: event-driven tick-by-tick backtesting, market depth implementations, latency/queue/fee models, and the live trading bot.
* ``hftbacktest-derive/`` — Procedural macros used by the core crate (``build_asset``, ``NpyDTyped``).
* ``py-hftbacktest/`` — PyO3 bindings and the Python package (``hftbacktest``), including data utilities and performance statistics.
* ``collector/`` — Historical market data collector for Binance, Bybit, and Hyperliquid.
* ``connector/`` — Live trading connector process: a unified ``Connector``/``ConnectorBuilder`` trait per exchange (currently Binance Futures, Binance Spot, and Bybit), communicating with bots over shared-memory IPC (iceoryx2).

Getting Started
===============

Build the Rust workspace:

.. code-block:: console

 cargo build --release

Install the Python package (requires Python 3.11+):

.. code-block:: console

 pip install ./py-hftbacktest

Or, during development:

.. code-block:: console

 cd py-hftbacktest
 maturin develop

On macOS (aarch64), building the Python extension additionally requires the
linker flags defined in ``.cargo/config.toml`` (``-undefined dynamic_lookup``).

Data Format
-----------

``hftbacktest`` digests a NumPy structured array of events. Each event has 8 fields in the following order:

* ``ev`` (u64): Event flags (depth/trade/snapshot/BBO, buy/sell, local/exchange, and so on).
* ``exch_ts`` (i64): Exchange timestamp — when the event occurred on the exchange.
* ``local_ts`` (i64): Local timestamp — when the event was received locally.
* ``px`` (f64): Price.
* ``qty`` (f64): Quantity.
* ``order_id`` (u64): Order ID, used only by Level-3 Market-By-Order feeds.
* ``ival`` (i64): Reserved for an additional integer value.
* ``faval`` (f64): Reserved for an additional float value.

Raw exchange feeds can be collected with ``collector/`` and then converted to
this normalized format using the utilities in
``py-hftbacktest/hftbacktest/data/``. Timestamps should use nanoseconds
consistently, since the live bot operates in nanoseconds.

A Quick Example
---------------

Get a glimpse of what backtesting with hftbacktest looks like with these code snippets:

.. code-block:: python

    @njit
    def market_making_algo(hbt):
        asset_no = 0
        tick_size = hbt.depth(asset_no).tick_size
        lot_size = hbt.depth(asset_no).lot_size

        # in nanoseconds
        while hbt.elapse(10_000_000) == 0:
            hbt.clear_inactive_orders(asset_no)

            a = 1
            b = 1
            c = 1
            hs = 1

            # Alpha, it can be a combination of several indicators.
            forecast = 0
            # In HFT, it can be various measurements of short-term market movements,
            # such as the high-low range in the last X minutes.
            volatility = 0
            # Delta risk, it can be a combination of several risks.
            position = hbt.position(asset_no)
            risk = (c + volatility) * position
            half_spread = (c + volatility) * hs

            max_notional_position = 1000
            notional_qty = 100

            depth = hbt.depth(asset_no)

            mid_price = (depth.best_bid + depth.best_ask) / 2.0

            # fair value pricing = mid_price + a * forecast
            #                      or underlying(correlated asset) + adjustment(basis + cost + etc) + a * forecast
            # risk skewing = -b * risk
            reservation_price = mid_price + a * forecast - b * risk
            new_bid = reservation_price - half_spread
            new_ask = reservation_price + half_spread

            new_bid_tick = min(np.round(new_bid / tick_size), depth.best_bid_tick)
            new_ask_tick = max(np.round(new_ask / tick_size), depth.best_ask_tick)

            order_qty = np.round(notional_qty / mid_price / lot_size) * lot_size

            # Elapses a process time.
            if not hbt.elapse(1_000_000) != 0:
                return False

            last_order_id = -1
            update_bid = True
            update_ask = True
            buy_limit_exceeded = position * mid_price > max_notional_position
            sell_limit_exceeded = position * mid_price < -max_notional_position
            orders = hbt.orders(asset_no)
            order_values = orders.values()
            while order_values.has_next():
                order = order_values.get()
                if order.side == BUY:
                    if order.price_tick == new_bid_tick or buy_limit_exceeded:
                        update_bid = False
                    if order.cancellable and (update_bid or buy_limit_exceeded):
                        hbt.cancel(asset_no, order.order_id, False)
                        last_order_id = order.order_id
                elif order.side == SELL:
                    if order.price_tick == new_ask_tick or sell_limit_exceeded:
                        update_ask = False
                    if order.cancellable and (update_ask or sell_limit_exceeded):
                        hbt.cancel(asset_no, order.order_id, False)
                        last_order_id = order.order_id

            # It can be combined with a grid trading strategy by submitting multiple orders to capture better spreads and
            # have queue position.
            # This approach requires more sophisticated logic to efficiently manage resting orders in the order book.
            if update_bid:
                # There is only one order at a given price, with new_bid_tick used as the order ID.
                order_id = new_bid_tick
                hbt.submit_buy_order(asset_no, order_id, new_bid_tick * tick_size, order_qty, GTX, LIMIT, False)
                last_order_id = order_id
            if update_ask:
                # There is only one order at a given price, with new_ask_tick used as the order ID.
                order_id = new_ask_tick
                hbt.submit_sell_order(asset_no, order_id, new_ask_tick * tick_size, order_qty, GTX, LIMIT, False)
                last_order_id = order_id

            # All order requests are considered to be requested at the same time.
            # Waits until one of the order responses is received.
            if last_order_id >= 0:
                # Waits for the order response for a maximum of 5 seconds.
                timeout = 5_000_000_000
                if not hbt.wait_order_response(asset_no, last_order_id, timeout):
                    return False

        return True

License
=======

MIT. See ``LICENSE``. The original work is by nkaz001 (upstream `hftbacktest <https://github.com/nkaz001/hftbacktest>`_).

.. |license| image:: https://img.shields.io/badge/License-MIT-green.svg
    :alt: License
    :target: https://github.com/nkaz001/hftbacktest/blob/master/LICENSE

.. |rustc| image:: https://shields.io/badge/rustc-1.91-blue
    :alt: Rust Version
    :target: https://www.rust-lang.org/
