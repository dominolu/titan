# cross_exchange_mm B5-剩余：保证金投影与进程崩溃矩阵

日期：2026-11-11（Asia/Shanghai）。承接 [B5 首轮（部分完成）](../cross-exchange-mm-b5-reservation/README.md)。
本轮补上两项：**account-service 侧的保证金投影**与**真实的进程崩溃矩阵**。

## 1. 逐腿预占模型修正（本轮发现并修复）

首轮只让 maker 报价预占，hedge 单本身不占预算。这有两个漏洞：

- maker 成交后若 maker 报价被释放，而 hedge 单在途，账面预占会掉到真实义务之下；
- 进程重启后，在途 hedge 单不会被重建，重启即欠预占。

修正为**两条腿同方向记账**：

| 订单所在腿 | 预占的 hedge 手数 |
| --- | --- |
| maker 腿（非 reduce-only） | `手数 × 整数 lot 比例` |
| hedge 腿（非 reduce-only） | 自身手数 |

成交结算只缩小 own-leg 占用，hedge 义务保持不变；hedge 单接管时新增自身预占，
所以从报价到对冲的交接过程中总预占**不低于**真实义务。恢复时按两条腿的非终态、
非 reduce-only 订单重建，重启不再欠预占。

## 2. 保证金投影（接进 account-service）

`crates/titan-account-service/src/risk.rs` 新增：

| 函数 | 语义 |
| --- | --- |
| `collateral_units(bound_tick_lots, instrument, currency, margin_rate_bps)` | 按显式初始保证金率换算最坏名义所需的抵押单位，向上取整到绑定货币精度，整数运算 |
| `MarginProjection` / `margin_projection(available_units, ...)` | 单账户视角：`available_units`、`required_units`、`headroom_units`、`covered()` |

关键约束：

- **逐账户独立投影**：一个账户的盈余从不覆盖另一个账户的缺口——不同保证金域不可互抵。
- 余额缺失、为零或为负一律拒绝（`margin balance unavailable` / `margin balance not positive`），
  不当作"需求为零"。
- 保证金率必须显式配置，缺失或为 0 直接拒绝，不猜交易所费率。
- `ExecutionLegLimit` 新增 `margin_rate_bps`（默认 10,000 = 全额名义，校验区间 1..=100,000），
  让费率进入配置而不是隐含默认。

## 3. 进程崩溃矩阵

新增跨进程测试：子进程在选定崩溃点写入"已持久化的订单事实"后阻塞，父进程 `kill` 它，
再从事实重建账本并断言预算。

| 崩溃点 | 已持久化事实 | 重建后 hedge 预占 | 断言 |
| --- | --- | --- | --- |
| `before_dispatch` | 无 | 0 | 未派发即未占用 |
| `after_first_dispatch` | 1 笔 maker 报价 | 50 | 精确覆盖 |
| `after_dispatch` | 2 笔报价 + 1 笔对冲 | 100 | 精确覆盖 |
| `after_fill` | 同上（成交仅在内存） | 100 | 成交未持久化时仍按订单占用，不欠预占 |

同时断言：重启后救援额度**只会被判定为已用尽**（存在存活订单时），绝不退款。

实现方式与仓库既有的 `sqlite_kill_boundaries` 一致（`current_exe()` + `--ignored --exact` +
`ready` 文件 + `kill`），是真实的进程终止，不是状态模拟。

## 4. 验收（构建服务器，rustc 1.94.1）

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| Clippy（两包，`-D warnings`） | 通过 |
| Rust 单测 | **143 通过**（account-service 33 + strategy-runtime 110）、0 失败、12 忽略 |
| `titan-cli` 集成测试 | 108 通过 |
| SDK 回归 | 161 通过 |
| 内嵌原生场景 | 声明 3、通过 3 |
| 配置预检单测 / 示例配置 | 6 通过 / shadow PASS |
| 产物编译 / `validate` | 通过 |

本轮新增回归：保证金投影（账户独立、缺/零/负余额拒绝、缺费率拒绝、50% 费率、不足时负 headroom）、
按货币精度向上取整、双方向预占交接不塌陷、四崩溃点跨进程矩阵。

## 5. 仍未完成

1. **运行时准入尚未调用保证金投影。** 投影函数与配置字段已就绪，但把它接进 `commit_commands`
   需要在 `StrategyRuntimeBuildContext` 增加账户服务句柄（11 处构造点）。该改动与 B8
   （管理入口双绑准入）改同一批构造点，安排在 B8 一并做，避免两次大面积改动。
   因此当前"派单前拒绝保证金不足"仍**未被代码执行**。
2. **救援额度仍不持久化**，重启按"已用尽"保守处理（方向安全，但运维上需要重新授权）。
3. `rescue` 未接到实际退出路径（B7）。
4. 离线执行器仍未接入用途校验与预占（B9）。

## 6. 产物身份

| 项目 | 值 |
| --- | --- |
| 源码版本 / 恢复 schema | `2.0.2` / 2（策略源码本轮未变） |
| `compiled_strategy_digest` | `00059a8f78d3a151ac770a30a32b93ba21ca3c25e8ca55a48f7e0c12ee4a63ad` |
| 本次产物 sha256 | `019423e89dc1dd9b8271e1285879a786746958fa54de4d16184eed70c81f1ed2` |
| 编译用 `titan` 二进制 sha256 | `f1a95610205486d9c4a38cc84621e27d27149a5667bc633914651c2f8c38240f` |

产物未签名、不可字节复现；原始日志与产物在仓库外的私有证据目录。

## 7. 复现入口

```bash
cargo fmt --all -- --check
cargo clippy -p titan-strategy-runtime -p titan-account-service --all-targets --offline --no-deps -- -D warnings
cargo test -p titan-strategy-runtime -p titan-account-service --offline
cargo test -p titan-cli --offline
python -m unittest discover -s python/titan-strategy-sdk/tests -p 'test_*.py'
titan strategy test strategies/cross_exchange_mm.py --json
```

崩溃矩阵单独运行：

```bash
cargo test -p titan-strategy-runtime reservations_survive_process_kill -- --nocapture
```
