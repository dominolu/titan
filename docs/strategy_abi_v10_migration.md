# Strategy ABI v10 migration

Strategy ABI v10 adds the live-state views required by multi-venue strategies while preserving
the numeric identifiers of the original v9 callback slots.

## Added typed views

- `DepthBatchEvent` and `DepthItemEvent` preserve market/source identity, snapshot flags,
  `stream_epoch`, update sequence ranges and per-level actions.
- `PositionEvent`, `BalanceEvent`, `CommandResultEvent` and `AccountStateEvent` expose canonical
  account facts without handing opaque connector payloads to Numba.
- `FillEvent` and `OrderEvent` now include `local_account_no`.

The new callback slots are `balance=10`, `command_result=11`, `account_state=12` and `depth=13`.
Existing slots `start=0` through `stop=9` have not changed.

## Runtime behavior

- `EventView` retains canonical publication metadata instead of dropping source identity and
  delivery sequencing before the strategy adapter.
- StrategyPlugin creates upstream market subscriptions after installing the EventEngine route,
  requests and waits for the initial depth snapshot, and owns unsubscribe through ResourceScope.
- Account snapshots seed the strategy before `on_start`. Account control facts continue to update
  the strategy while command admission is closed. Reconcile/stream invalidation closes the command
  gate until every bound account is READY again.
- `runtime.timer_interval` enables a lane-serialized live housekeeping timer.
- `cancel_owned_orders` now keeps the account lane alive and requires terminal order facts before
  completing stop.

All Numba strategy manifests must declare runtime ABI major version 10 and be recompiled because
the descriptor fingerprint and Fill/Order layouts changed.

## Python SDK 结构（统一 ABI 管理）

SDK 只保留一个 ABI 定义入口，策略不再接触 ABI 细节：

```text
python/titan-strategy-sdk/titan_strategy/
  intrinsic.py   # 叶子模块：Numba 指针 / 宿主调用 intrinsic
  abi_v10.py     # 唯一 ABI 定义：dtype、回调槽位、订单词表、Rust descriptor 布局校验
  callbacks.py   # 执行宿主调用、命令编码、callback bridge
  context.py     # Strategy facade：行情 / 账户 / 订单视图 + 下单接口
```

依赖方向只能向下，禁止反向引用：

```text
intrinsic.py <- abi_v10.py <- callbacks.py <- context.py <- strategies/*
```

约束（由 `tests/test_strategy_surface.py` 在 CI 中断言）：

- 任何 Rust/Python/Numba 共享的字段、offset、size、alignment 只在 `abi_v10.py` 出现；
  `context.py`、`callbacks.py`、策略包都不定义 dtype。
- `callbacks.py` 不定义 dtype，只负责宿主调用（`execution_submit` / `execution_cancel`）、
  请求结构写入、回测命令编码和 callback bridge。
- `context.py` 不再承担 ABI 定义职责：它只提供视图与下单接口，内部调用两个下层模块。
- `validate_runtime_descriptor(...)` 保留在 `abi_v10.py`，继续逐字段校验 Rust descriptor 与
  NumPy dtype 的一致性；该检查是布局漂移的唯一守卫，不允许删除或弱化。
- `ABI_VERSION` 在 Python 侧只在 `abi_v10.py` 定义一次，Rust 侧对应
  `titan_runtime_abi::STRATEGY_ABI_VERSION`；`compiler.py` 只引用不复制。

## 策略侧接口

策略只用 facade 表达交易意图，不写任何 ABI 常量或指针运算：

```python
@njit
def on_tick(s):
    for tick in s.ticks():
        price = tick["event"]["px"]
        if price < s.best_bid(0):
            s.submit_maker_bid(0, order_id, price, qty)
    s.cancel_order(order_id, asset_no)
```

可用接口：

```text
视图:  now, state, state_i64, ticks(), bars(), fills(), orders(),
       market(asset), best_bid/best_ask/best_bid_qty/best_ask_qty/tick_size/lot_size,
       position(asset), position_event, balance_event, account_state(), depth(), depth_items(),
       timer(), funding(), payload()
下单:  submit_maker_order / submit_taker_order / submit_market_order
       submit_maker_bid / submit_maker_ask / submit_taker_buy / submit_taker_sell
撤销:  cancel_order(order_id, asset_no, account_no)
停止:  stop()
```

历史签名 `submit_buy_order` / `submit_sell_order` / `cancel` 继续保留，旧的 Numba 策略包
（如 `strategies/okx_hyperliquid_xemm`）无需改动；新增策略应使用上面的意图命名接口。
