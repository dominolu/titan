# Strategy ABI v9 migration

状态：当前版本

Strategy ABI v9 在 v8 Funding 语义之上补齐账户 Fill 的双数量语义和多账户命令路由。Rust
`titan-runtime-abi` 与 Python Host 都严格要求 ABI version `9`，版本或布局指纹不一致会在 callback
执行前失败。

## Breaking changes

`FillEvent` 将旧的单一成交数量拆分为：

- `last_fill_qty`：当前规范化 Fill 事实贡献的增量数量，用于仓位、成交额、费用和 PnL；
- `cumulative_filled_qty`：应用当前 Fill 后的订单累计成交数量，用于单调性与一致性校验。

策略不能再把累计数量当作本次成交增量。重复 trade ID 不得再次应用 `last_fill_qty`，累计数量不得
回退；这些语义由 Connector 在事实进入 Strategy Runtime 前完成。

`OrderCommand` 新增 `local_account_no: u32`。该字段是在策略实例创建阶段解析并冻结的本地账户绑定，
用于将 submit/amend/cancel 显式路由到一个账户。单账户策略使用绑定表中的账户编号，不得再依赖 asset、
当前持仓或默认账户进行隐式推断。

## Migration checklist

1. 重新使用当前 Python SDK dtype 编译 Numba strategy，不复制 v8 NumPy dtype。
2. 将 Fill 处理逻辑改为使用 `last_fill_qty` 计算增量，并用 `cumulative_filled_qty` 做订单一致性检查。
3. 写入每条 `OrderCommand` 时设置有效的 `local_account_no`。
4. 重新生成并校验 Runtime ABI descriptor/layout fingerprint。
5. 用相同输入核对 Rust 与 Python 的 Fill 字段偏移、命令布局和 callback 序列。

Funding 配置字段及其同时间边界规则没有变化，参阅历史
[Strategy ABI v8 migration](strategy_abi_v8_migration.md)。
