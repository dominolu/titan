=====
Titan
=====

|rustc| |license|

一个开源的、生产级的高频交易（HFT）**回测与实盘交易框架**，专注于加密货币永续合约，
采用全 Rust 核心，并提供统一的交易所连接器。

框架基于全订单簿的行情回放进行模拟，并精确计入行情/下单延迟与订单队列位置，使回测结果
能够真实还原实际成交环境。同一套策略代码在研究和实盘之间无需任何修改即可运行——
「研究到实盘零差异」是框架的设计初衷。

本仓库是上游 `hftbacktest <https://github.com/nkaz001/hftbacktest>`_ 项目的精简版，并在
持续扩展。非核心资产（notebook 示例、文档源、CI 工作流、社区文件）已被移除，代码库聚焦于
核心引擎、实盘交易、数据采集与交易所连接器。

核心特性
========

回测引擎
--------

* 完整的逐笔（tick-by-tick）模拟，时间间隔可自定义，或基于行情/订单接收事件驱动。
* 基于 Level-2 Market-By-Price 与 Level-3 Market-By-Order 行情的全订单簿重建。
* 回测计入行情与下单延迟，支持内置模型或自定义模型。
* 订单成交模拟计入订单队列位置，支持内置模型或自定义模型。
* 支持多资产、多交易所联合回测。

实盘交易
--------

* **当前 Rust 回调实现**（``hftbacktest::strategy``）：``Strategy`` trait + 两级 ctx
  （市场 → 品种），同一份 Rust 策略可以用于回测与实盘。
* **Numba 事件策略接口**：面向策略作者的事件 API 固定为单参数
  ``@njit def on_tick(s)`` / ``@njit def on_bar(s)``；``s`` 同时提供行情上下文、状态和
  下单能力。Rust 拥有事件循环、时钟、行情状态与撮合；当前已连接 Tick、Bar 与
  Bar-signal/Tick-execution Hybrid，并支持 Timer、Funding 和显式 OHLC Bar 撮合配置。完整设计见
  `docs/bar_tick_numba_strategy.md <docs/bar_tick_numba_strategy.md>`_。
* **统一 Broker API**：``connector/src/api.rs`` 提供一套统一数据结构
  （订单/持仓/账户/行情/订单簿/资金费/成交/费率/杠杆）与 ``BrokerApi`` trait，
  覆盖所有已支持的交易所——策略切换 broker 只需改一行代码。
* **事件驱动连接器**：每个交易所实现 ``Connector``/``ConnectorBuilder`` trait，通过共享内存
  IPC（iceoryx2）与机器人通信。
* **统一资金费事件**：``LiveEvent::Funding`` 在 Binance（WS 推送）、OKX（WS 推送）、
  Hyperliquid（WS 推送）三家一致。
* **防失控安全网**：生产连接器定期刷新交易所侧倒计时全撤，并在 SIGINT/SIGTERM
  退出时等待全撤完成。

数据
----

* 历史行情采集器（WebSocket）：支持 Binance（USD-M / COIN-M / Spot）、Bybit、Hyperliquid。
* 统一的 NumPy 事件格式，回测与实盘回放共用。

快速开始
========

构建 Rust workspace：

.. code-block:: console

    cargo build --release

数据格式
--------

``hftbacktest`` 消费一个 NumPy 结构化数组，每个事件包含 8 个字段，顺序如下：

* ``ev`` (u64)：事件标志（深度/成交/快照/BBO、买/卖、本地/交易所等）。
* ``exch_ts`` (i64)：交易所时间戳——事件在交易所发生的时间。
* ``local_ts`` (i64)：本地时间戳——本地收到事件的时间。
* ``px`` (f64)：价格。
* ``qty`` (f64)：数量。
* ``order_id`` (u64)：订单 ID，仅 Level-3 Market-By-Order 行情使用。
* ``ival`` (i64)：预留整数字段。
* ``fval`` (f64)：预留浮点字段。

原始交易所行情可用 ``collector/`` 采集，转换为该归一化格式后即可回测。
时间戳应统一使用纳秒，因为实盘机器人以纳秒为单位运行。

采集器产生的 gzip JSON 日志可直接转换为 NPZ：

.. code-block:: console

    cargo run -p collector --bin normalize -- \
      --exchange binance --input data/btcusdt_20260821.gz --output data/btcusdt_20260821.npz

转换器支持 ``binance``、``bybit`` 和 ``hyperliquid``，输出 key 固定为 ``data``。

Rust 回测入口也支持 materialized Bar Parquet。Bar 文件必须包含
``ts/open/high/low/close/volume/vwap/transaction_count/source/is_final``；``ts`` 表示
Bar 开始时间。包含多个来源且时间重叠的文件必须通过 ``--bar-source`` 显式选择来源：

.. code-block:: console

    cargo run -p titan-examples --bin backtest --release -- \
      --data-kind bar \
      --data data/AAPL_1m_all_sources.parquet \
      --bar-source polygon_s3

Bar 由 Rust 直接读取并按关闭时间跳转，不会转换成伪 Tick。默认周期为 60 秒，亦可通过
``--bar-timeframe-ns`` 调整；默认只接收 ``is_final=true`` 的记录。

Numba 单参数 ``on_bar(s)`` 双均线策略示例：

.. code-block:: console

    python examples/dual_ma_bar_backtest.py \
      --data data/AAPL_1m_all_sources.parquet \
      --source polygon_s3 \
      --short-period 20 --long-period 50 --quantity 1

重复性能测试前可先转换成 Rust 可直接读取的扁平 NPY，避免每次解析 Parquet：

.. code-block:: console

    python examples/convert_bar_parquet.py \
      --input data/AAPL_1m_all_sources.parquet \
      --output data/AAPL_1m_polygon_s3.timed_bar.npy \
      --source polygon_s3

    cargo build -p titan-examples --bin backtest --release
    target/release/backtest \
      --data-kind bar \
      --data data/AAPL_1m_polygon_s3.timed_bar.npy \
      --runs 100

上游算法参考
------------

下面的 Python 片段只用于解释做市算法；仓库已经恢复 Python/Numba binding。新事件策略
接口不是这里展示的手写 ``while hbt.elapse``，而是 Numba 单参数
``on_tick(s)`` / ``on_bar(s)`` 回调，详见
`Bar/Tick 回测与实盘统一策略接口 <docs/bar_tick_numba_strategy.md>`_。当前可直接运行的
实现见下一节的 Rust 示例。

.. code-block:: python

    @njit
    def market_making_algo(hbt):
        asset_no = 0
        tick_size = hbt.depth(asset_no).tick_size
        lot_size = hbt.depth(asset_no).lot_size

        # 单位为纳秒
        while hbt.elapse(10_000_000) == 0:
            hbt.clear_inactive_orders(asset_no)

            a = 1
            b = 1
            c = 1
            hs = 1

            # Alpha，可以是多个指标的组合。
            forecast = 0
            # HFT 中通常是短期市场波动的各种度量，如最近 X 分钟的高低区间。
            volatility = 0
            # Delta 风险，可以是多个风险的组合。
            position = hbt.position(asset_no)
            risk = (c + volatility) * position
            half_spread = (c + volatility) * hs

            max_notional_position = 1000
            notional_qty = 100

            depth = hbt.depth(asset_no)
            mid_price = (depth.best_bid + depth.best_ask) / 2.0

            # 公允价值 = mid_price + a * forecast
            #          或 标的(相关资产) + 调整项(基差 + 成本 + 等) + a * forecast
            # 风险倾斜 = -b * risk
            reservation_price = mid_price + a * forecast - b * risk
            new_bid = reservation_price - half_spread
            new_ask = reservation_price + half_spread

            new_bid_tick = min(np.round(new_bid / tick_size), depth.best_bid_tick)
            new_ask_tick = max(np.round(new_ask / tick_size), depth.best_ask_tick)
            order_qty = np.round(notional_qty / mid_price / lot_size) * lot_size

            # 推进处理时间。
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

            if update_bid:
                # 同一价格只挂一个单，以 new_bid_tick 作为订单 ID。
                order_id = new_bid_tick
                hbt.submit_buy_order(asset_no, order_id, new_bid_tick * tick_size, order_qty, GTX, LIMIT, False)
                last_order_id = order_id
            if update_ask:
                order_id = new_ask_tick
                hbt.submit_sell_order(asset_no, order_id, new_ask_tick * tick_size, order_qty, GTX, LIMIT, False)
                last_order_id = order_id

            # 所有订单请求视为同一时刻发出。
            # 等待收到其中任一订单的响应。
            if last_order_id >= 0:
                # 最长等待 5 秒订单响应。
                timeout = 5_000_000_000
                if not hbt.wait_order_response(asset_no, last_order_id, timeout):
                    return False

        return True

Rust 版做市示例策略
-------------------

同一套做市逻辑的 Rust 实现位于 ``examples/`` crate
（`examples/src/market_making.rs <examples/src/market_making.rs>`_），通过
``Strategy`` trait 的 ``on_tick``/``on_bar`` 回调驱动，回测与实盘共用一份代码：

.. code-block:: console

    # 回测（无数据文件时自动使用合成 demo 数据）
    cargo run -p titan-examples --bin backtest

    # 实盘：连接器进程 + LiveBot + 同一策略
    cargo run -p connector --features okx -- --name my-okx --connector okx --config connector/examples/okx.toml
    cargo run -p titan-examples --bin live -- --connector-name my-okx --symbol BTC-USDT-SWAP

``on_tick``/``on_bar`` 的完整用法（两级 ctx、状态槽、下单接口）见
`docs/rust_strategy.md <docs/rust_strategy.md>`_。

该 Rust trait 是内部实现和迁移期兼容入口，不是默认的 Python 策略 API。默认接口是
Numba ``@njit def on_tick(s)`` / ``@njit def on_bar(s)``，并显式区分 Bar、Tick 和
Hybrid 数据源；见
`docs/bar_tick_numba_strategy.md <docs/bar_tick_numba_strategy.md>`_。

实盘交易
========

连接器
------

以独立进程运行连接器，通过共享内存 IPC（iceoryx2）向机器人发布归一化事件：

.. code-block:: console

    cargo run --release -p connector -- <名称> <连接器> <配置文件.toml>

示例：

.. code-block:: console

    connector my-bf binancefutures binancefutures.toml
    connector my-okx okx okx.toml
    connector my-hl hyperliquid hyperliquid.toml

配置模板位于 ``connector/examples/`` 中（每个文件均注释了主网/测试网地址与签名方式，
例如 OKX 模拟盘、Hyperliquid API 钱包配置）。

支持的交易所
------------

.. list-table::
   :widths: 20 30 15 35
   :header-rows: 1

   * - 连接器
     - 市场
     - 状态
     - 说明
   * - ``binancefutures``
     - Binance USD-M 永续合约
     - ✅ 生产可用
     - 测试网/主网；symbol 小写；统一 API 已接入并通过实盘冒烟
   * - ``okx``
     - OKX V5 SWAP
     - ✅ 生产可用
     - 实盘 + 模拟盘（``x-simulated-trading``）；统一 API 已接入并通过实盘冒烟
   * - ``hyperliquid``
     - Hyperliquid 永续合约
     - ✅ 生产可用
     - EIP-712 phantom-agent 签名；主网/测试网；统一 API 已接入并通过实盘冒烟
   * - ``binancespot``
     - Binance 现货
     - 🚧 开发中
     - 实盘框架已搭建；统一 API 尚未接线
   * - ``bybit``
     - Bybit 线性合约
     - 🚧 开发中
     - 实盘框架已搭建；统一 API 尚未接线

测试
====

.. code-block:: console

    cargo test --workspace --all-features

测试覆盖核心回测、策略回调、请求/响应映射、WebSocket 消息解析和 Hyperliquid EIP-712
wire 序列化。准确数量以当前测试输出为准；网络冒烟测试默认忽略。

项目固定使用 Rust 1.94.0，``rust-toolchain.toml`` 会让 rustup 自动选择对应工具链。

实盘冒烟测试（需要网络，默认跳过）：

.. code-block:: console

    # Hyperliquid 公共行情
    cargo test --all-features hyperliquid::brokerapi::tests::live -- --ignored --nocapture

    # OKX 公共行情（如需要可走代理）
    HTTPS_PROXY=127.0.0.1:7897 cargo test --all-features okx::brokerapi::tests::live -- --ignored --nocapture

    # Binance USD-M 测试网行情（trade/depth/markPrice/bookTicker）
    cargo test --all-features binancefutures::market_data_stream::tests::live_ws -- --ignored --nocapture

    # Hyperliquid userFundings WebSocket 订阅
    HYPERLIQUID_USER=0x... cargo test --all-features hyperliquid::ws::tests::live_user_fundings -- --ignored --nocapture

文档
====

* 上游项目维护着完整的 `官方文档 <https://hftbacktest.readthedocs.io/>`_，大部分仍然适用。
  注意本仓库已移除上游的 ``docs/`` 文档源，仅保留与当前代码对应的
  ``docs/rust_strategy.md``。
* Rust 策略（``Strategy`` trait）用法：`docs/rust_strategy.md <docs/rust_strategy.md>`_。
* Rust 策略实盘端到端验证记录：`docs/live_e2e_record.md <docs/live_e2e_record.md>`_。
* 连接器相关：`connector/README.md <connector/README.md>`_（架构与实现指南）、
  `API 覆盖清单 <connector/API_COVERAGE.md>`_、
  `API 差距分析 <connector/API_GAP_ANALYSIS.md>`_、
  `连接器测试模板 <connector/TESTING_TEMPLATE.md>`_。

路线图
======

* 为 ``binancespot`` 与 ``bybit`` 连接器接入统一 API。
* 基于同一套 ``Connector`` + ``BrokerApi`` 模型扩展更多交易所连接器。

License
=======

MIT。见 ``LICENSE``。原始工作来自 nkaz001（上游
`hftbacktest <https://github.com/nkaz001/hftbacktest>`_）。

.. |license| image:: https://img.shields.io/badge/License-MIT-green.svg
    :alt: License
    :target: https://github.com/nkaz001/hftbacktest/blob/master/LICENSE

.. |rustc| image:: https://shields.io/badge/rustc-1.94-blue
    :alt: Rust Version
    :target: https://www.rust-lang.org/
