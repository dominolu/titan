# titan-strategy-sdk（Strategy ABI V13）

该 SDK 提供 Numba 策略编写和目标文件桥接；唯一的制品编译入口是 Rust `titan strategy compile`。
策略使用 typed state、只读 public views 和 callback-local command staging；生产运行时只加载 `native-v13` 制品，不嵌入 Python。

主要模块：

- `abi_v13.py`：固定宽度 ABI dtype、回调槽位、常量与 fingerprint。
- `definition.py` / `parameters.py` / `state.py`：策略声明、参数 schema、typed state schema。
- `context_v13.py`：只读行情/账户视图与 submit/cancel command staging。
- `static_compiler.py`：受限定义加载、Python AST/Numba 类型检查、PIC 目标文件及原始符号描述。
- `cli.py`：内部 `describe` / `emit-objects` JSON 协议，由 Rust 启动，不作为用户编译入口。

Rust 拥有编译进程调度、超时/资源限制、原生链接与依赖审计、规范 CBOR manifest、制品摘要、Ed25519 签名及制品校验。
Python 不读取签名私钥，也不生成或校验 `.titan` / `.manifest.cbor`；不再依赖 `cryptography` 或提供 Python 制品 API。

策略必须导出模块常量 `SPEC`、`state_dtype` 和 `build(parameters) -> StrategyDefinition`；handler 使用 `@njit`，签名为
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
# 先在编译服务器构建 Rust CLI，再运行 SDK/Numba 回归。
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
cargo +1.94.1 build -p titan-cli --bin titan
TITAN_TEST_BINARY="$PWD/target/debug/titan" PYTHONPATH=python/titan-strategy-sdk:. \
  "$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v
# 完整 workspace / AOT / 恢复 / CLI / 报告回归：
bash scripts/verify_system_refactor.sh
```


## 通用恢复协议（2026-10-13）

可在 `handlers` 中声明可选的 `on_recover(ctx)` 生命周期回调。回调接收已恢复的 typed state 和宿主对账后的当前 public views，负责业务状态重建，返回 `0`/`None` 表示接受，返回负 ABI 状态表示拒绝。该回调无需 subscription。

宿主先校验快照身份、制品、绑定、state schema、私有数据和恢复基线校验和，然后检查一致版本的账户快照、已知订单结果及累计成交。恢复回调期间 submit/cancel gate 保持关闭；新 generation 的检查点确认落盘，并再次确认公共状态未发生变化后，才允许启动。没有 `on_recover` 的策略继续使用严格的公共状态摘要匹配。

支持已知订单无新增成交的状态变化、账户会话重建和余额更新。缺失订单结果、未知活动订单、持仓/累计成交变化或快照中未完成的成交交付会明确拒绝，等待完整历史事实；不会用净持仓推导成交。恢复回调不得下单或撤单，启动后的风险处置继续由正常业务回调执行。

C 结构布局不变，恢复回调使用新槽位 12（`EventKind.RECOVER`），ABI fingerprint 更新为 `6b7e11cc113413aecdaeaa27dc14549b00c6b50c8d5c5fe5e0ed599ac0269dfb`。所有制品须重新 AOT 编译；严格指纹校验保持启用。旧制品、旧状态结构及变更后的绑定不自动迁移。`pair_arb` 当前制品版本为 `3.1.0`，私有状态 schema 为 `4`。

## 停止完成屏障（2026-10-22）

需要等待平仓、持仓事实或其他业务确认的策略声明 `Capability.STOP_BARRIER`，并在
`on_stop` 启动停止流程。宿主保持 Stopping，继续交付账户、行情和 timer 回调；只有策略
确认业务完成后调用 `ctx.complete_stop()`，且宿主没有活动订单或待交付成交，才完成停止。
未调用完成确认时保持等待，达到宿主 stop deadline 则失败，不会误报已停止。

SDK 将完成确认写入现有 context.flags 的位 0；布局和指纹不变。制品 manifest 的
`stop_barrier` 能力提供版本门禁：旧宿主遇到未知能力会拒绝加载。未声明该能力的策略
沿用原有停止规则。离线 adapter 的 `stop()` 对该能力保持回调入口开放，调用方继续提供
订单/成交/持仓事实，通过 `is_stopped()` 判断结束。

CLI 的 V13 trace 支持显式 `{"kind":"stop","now_ns":...}`，后续事件负责交付停止阶段的
撤单、成交与持仓确认；文件结束而策略尚未完成停止时，运行失败，不写成功结果。

示例和完整用法见 [Numba 合约网格](../../strategies/grid/README.md)。

## 恢复订单结果（2026-10-22）

需要区分“已确认撤单”和“订单结果缺失”的策略声明 `Capability.RECOVERY_ORDER_RESULTS`，
在 `on_recover(ctx)` 中通过只读 `ctx.recovery_orders()` 查询宿主已经对账的订单结果。
元素使用 `active_order_dtype`，可以包含 CANCELED/EXPIRED/REJECTED/FILLED；普通
`ctx.active_orders()` 仍只表示当前活动投影。只有结果身份和已消费累计成交一致时才能
释放业务引用，不能以缺少活动订单代替结果证据。

布局和 ABI 指纹不变，Recover 的 typed payload 使用现有 `event_payload_ptr/len`：
指针指向宿主持有的连续 `TitanActiveOrderView` 数组，len 为字节数，生命周期仅覆盖本次
同步回调。Numba 接口检查 callback kind 与长度对齐并返回 readonly view。新能力写入
manifest，旧宿主拒绝加载；未声明该能力的旧策略不改变恢复行为。

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

## Rust 编译控制器（2026-10-21）

控制器固定策略源码与参数输入，再以固定 `PYTHONHASHSEED=0` 启动两个短生命周期 Python 进程：
`describe`（60 秒，CPU 30 秒）验证并描述 typed 策略，`emit-objects`（300 秒）重新加载同一输入并输出 Numba 目标文件。
两阶段元数据必须相同。进程及其子进程处于独立进程组，超时后终止并回收；临时文件随成功或失败统一清理。
Rust 根据已验证的回调集合规范化目标文件符号、生成 ABI descriptor，调度 `nm`/`objcopy`/`cc`，完成链接并禁止 Python/Numba/NRT 动态依赖。

编译只支持编译主机对应的 Linux ELF target，CPU baseline 必须显式属于受支持集合。该限制匹配当前 GNU 工具链实现，未提供交叉编译。
源码、状态 schema 与初始 state 的事实由 Python 桥接提供；制品规则由 Rust 校验并复用运行时加载器验证。
参数摘要采用 Rust 的递归排序 JSON 编码；不同参数文本空白或对象键顺序不会改变摘要。

ABI fingerprint、CBOR 字段、签名覆盖范围及 pair/bundle 读取方式保持不变；编译器 build 身份和制品摘要会随本次重构改变。
旧制品继续由现有严格加载器验证；旧检查点不会自动迁移到重新编译后摘要不同的制品。
制品先在目标文件系统暂存并验证，再发布；pair 以 manifest 最后提交，替换窗口中的不一致读取会被摘要校验拒绝，失败时回滚已有文件。
报告模块及其 Python 依赖保持原样。
