# titan-strategy-sdk（Strategy ABI V13）

该 SDK 是 Titan 唯一的策略编写与 AOT 编译入口。策略使用 typed state、只读 public views
和 callback-local command staging；生产运行时只加载 `native-v13` 制品，不嵌入 Python。

主要模块：

- `abi_v13.py`：固定宽度 ABI dtype、回调槽位、常量与 fingerprint。
- `definition.py` / `parameters.py` / `state.py`：策略声明、参数 schema、typed state schema。
- `context_v13.py`：只读行情/账户视图与 submit/cancel command staging。
- `static_compiler.py`：受限定义加载、静态校验、Numba AOT、deterministic CBOR manifest。
- `cli.py`：离线编译命令，由 Rust `titan strategy compile` 调用。

策略必须导出一个 `STRATEGY = StrategyDefinition(...)`，handler 使用 `@njit`，签名为
`handler(context)`。参数在编译期校验并写入 initial state；部署期不能覆盖参数。

在编译服务器构建：

```bash
titan strategy compile \
  --strategy strategies/pair_arb/strategy.py \
  --parameters strategies/pair_arb/parameters.json \
  --target x86_64-unknown-linux-gnu \
  --cpu-baseline x86-64-v2 \
  --artifact-format bundle \
  --output pair_arb.titan
```

在编译服务器测试：

```bash
PYTHONPATH=python/titan-strategy-sdk:. \
  uv run --project python/titan-strategy-sdk --with pytest \
  pytest -q python/titan-strategy-sdk/tests
```


## 通用恢复协议（2026-10-13）

可在 `handlers` 中声明可选的 `on_recover(ctx)` 生命周期回调。回调接收已恢复的 typed state 和宿主对账后的当前 public views，负责业务状态重建，返回 `0`/`None` 表示接受，返回负 ABI 状态表示拒绝。该回调无需 subscription。

宿主先校验快照身份、制品、绑定、state schema、私有数据和恢复基线校验和，然后检查一致版本的账户快照、已知订单结果及累计成交。恢复回调期间 submit/cancel gate 保持关闭；新 generation 的检查点确认落盘，并再次确认公共状态未发生变化后，才允许启动。没有 `on_recover` 的策略继续使用严格的公共状态摘要匹配。

支持已知订单无新增成交的状态变化、账户会话重建和余额更新。缺失订单结果、未知活动订单、持仓/累计成交变化或快照中未完成的成交交付会明确拒绝，等待完整历史事实；不会用净持仓推导成交。恢复回调不得下单或撤单，启动后的风险处置继续由正常业务回调执行。

C 结构布局不变，恢复回调使用新槽位 12（`EventKind.RECOVER`），ABI fingerprint 更新为 `6b7e11cc113413aecdaeaa27dc14549b00c6b50c8d5c5fe5e0ed599ac0269dfb`。所有制品须重新 AOT 编译；严格指纹校验保持启用。旧制品、旧状态结构及变更后的绑定不自动迁移。`pair_arb` 当前制品版本为 `3.1.0`，私有状态 schema 为 `4`。

## 持仓方向（2026-10-13）

账户快照保存交易所报告的持仓方向。单向模式保存 NET；双向模式分别保存 LONG、SHORT，包括零仓位。数量统一为有符号 lots：LONG 非负、SHORT 非正、NET 可正可负。零仓位更新只清零该侧，不删除或覆盖另一侧。重复的同方向记录、未知方向和同品种混合 NET/LONG/SHORT 的快照会拒绝初始化。

```python
from titan_strategy.types import PositionSide

long_position = ctx.position(account_no, asset_no, PositionSide.LONG)
short_position = ctx.position(account_no, asset_no, PositionSide.SHORT)
# 买入以平空；持仓方向不能由 BUY/SELL 自动推断。
order_id = ctx.submit_order(account_no, asset_no, 1, 1, qty, price, 1,
                            True, PositionSide.SHORT)
```

`ctx.position(account_no, asset_no)` 和 `submit_order` 的默认方向仍为 NET。查询不存在的方向产生稳定的缺失视图错误，不返回另一侧或合成净持仓。已有 NET 策略（包括当前 `pair_arb`）须显式适配方向才能在双向账户上交易；账户本身可以正常保存双侧持仓。宿主不会修改交易所账户模式。

PositionView、PositionEventView、OrderEventView、ActiveOrderView 和 SubmitOrderRequest 增加 `position_side`。V13 ABI fingerprint 更新为 `7864d0be8e1b83b0683747bf4fdfd4cce1f87377cb3fdb97c56a9a270c7a50ed`，所有策略制品须重新 AOT 编译；旧制品和旧快照不自动迁移。恢复逐侧比较数量和价格，两侧同步变化但净敞口相同也不能通过一致性校验。

Binance 的 NET 请求转换为 BOTH；双向平仓使用显式方向，适配层省略不允许的 reduceOnly 参数，并拒绝 reduce_only 与开仓方向冲突的请求。Hyperliquid 适配层明确拒绝 LONG/SHORT 请求，避免静默丢弃方向。
