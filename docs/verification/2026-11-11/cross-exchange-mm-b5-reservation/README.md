# cross_exchange_mm B5：逐腿预占与救援额度（部分完成）

日期：2026-11-11（Asia/Shanghai）。对应工作清单 B5。前置：[B4 验收](../cross-exchange-mm-b4-command-purpose/README.md)、
[多绑定执行设计](../../../cross_exchange_mm_multi_binding_design.md) §5。

**结论先写：本轮完成的是数量/名义敞口预占与救援额度的账本、准入与重建。**
后续轮次补上了[保证金投影与进程崩溃矩阵](../cross-exchange-mm-b5-reservation-rest/README.md)，
并把双方向预占（maker 报价 + hedge 单）修正进模型；仍未完成的是把保证金投影接进运行时准入。

## 已实现

| 项目 | 内容 |
| --- | --- |
| 账本 | 新增 `crates/titan-strategy-runtime/src/execution_reservation.rs`：`ExecutionReservations`、`MakerQuote`、`LegCaps` |
| 派单前预占 | `commit_commands` 在派发任何命令前，对每笔 `MakerQuote` 预占 `剩余手数 × 整数lot比例` 的 hedge 手数与最坏保护价名义；超额返回 `execution_reservation_unavailable`，整批不派发 |
| 撤单不释放 | `reservation_releasable(status, awaiting)`：`CANCEL_PENDING`(5) 及所有非终态继续占用；终态但有待补独立成交时也继续占用 |
| 只减不增地结算 | `settle(order_id, cumulative_filled_lots, price)` 幂等：重复回放不重复扣减；已成交部分的 hedge 义务**保持占用**，只有 hedge 单自己的终态证明能释放 |
| 加宽不使用 | `widen_mark` 只允许提高最坏保护价对应名义，降低被忽略，不会释放预算 |
| 救援硬上限 | `charge_rescue` 受 `rescue.max_quantity_lots` 与 `max_notional_tick_lots` 双重约束，超限 `execution_rescue_exhausted`，运行期内不重置 |
| 参考修订 | 账本绑定 `revision`，不同修订的预占请求被拒绝（`execution_reservation_revision`） |
| 恢复重建 | 恢复路径按 `orders_list` 中非终态、非 reduce-only 的订单重建预占（`ExecutionReservations::rebuild`），每次回调前先对账；重启不会从空预算开始 |
| 救援不低于已花 | 由于救援消耗尚未持久化，恢复时只要存在任何非终态订单，就把救援额度按**已用尽**处理（fail-closed），直到运维显式授权新修订；宁可拒绝救援，也不退款 |

## 验收（构建服务器，rustc 1.94.1）

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| Clippy（两包，`-D warnings`） | 通过 |
| Rust 单测（`titan-strategy-runtime` + `titan-account-service`） | **139 通过**、0 失败、11 忽略（新增 9 项） |
| `titan-cli` 集成测试 | 108 通过 |
| SDK 回归 | 161 通过 |
| 内嵌原生场景 | 声明 3、通过 3 |
| 产物编译 / `validate` | 通过，`valid: true` |

新增 9 项回归：派单前预占与重复预占拒绝、撤单不释放（含幂等结算与超量拒绝）、
超出 maker/hedge 腿上限的预占被拒（含名义上限）、加宽只增不减、救援双重上限与不可退款、
恢复重建等价（含缺订单导致欠预占的反例、救援已花不可退、超限拒绝）、恢复拒绝破坏绑定的订单、
修订不匹配拒绝、非终态/待补成交继续占用。

## 明确未完成（不得据此宣称 B5 通过）

1. **逐所保证金预占没有实现。** 平台当前没有可用的保证金/可用余额投影（账户服务只提供
   notional 证据），所以本轮预占的是**数量与名义敞口**，不是交易所侧保证金占用。
   "跨所盈利不得抵消亏损所保证金"这条仍未由代码证明。
2. **崩溃点矩阵没有自动化。** 验收标准要求"崩溃点矩阵通过"，本轮只做了恢复重建的单元级等价性，
   没有真正的进程终止/重启矩阵；`intent 提交前后崩溃`、`预占提交后传输前崩溃` 两类场景未覆盖。
3. **救援消耗未持久化。** 以"存在未终结订单即视为用尽"的保守替代，未实现按修订持久化的消耗水位。
4. **`rescue` 只做上限记账，没有接到退出路径。** `RescueExit` / `OwnedExit` 的实际退出执行
   属于 B7（`FlattenOwned`）。
5. 离线执行器仍未接入预占与用途校验（B9）。

## 产物身份

| 项目 | 值 |
| --- | --- |
| 源码版本 / 恢复 schema | `2.0.2` / 2（本策略本轮源码未变） |
| `compiled_strategy_digest` | `940d9ca1b136204f9c6930fae0e8728bf1da5bfa01ecd450bf33d5d3c5abb31f` |
| 本次产物 sha256 | `1ff36699a1c6810e9ebd2c64b12a6621541fd443251a0b69a44b98a4ba81553c` |
| 编译用 `titan` 二进制 sha256 | `196aeac6429b2867cb3a2e6989dd29ab1d6a34deec87e49c23911518b929f2f0` |

产物未签名、不可字节复现；原始日志与产物在仓库外的私有证据目录。

## 复现入口

```bash
cargo fmt --all -- --check
cargo clippy -p titan-strategy-runtime -p titan-account-service --all-targets --offline --no-deps -- -D warnings
cargo test -p titan-strategy-runtime -p titan-account-service --offline
cargo test -p titan-cli --offline
python -m unittest discover -s python/titan-strategy-sdk/tests -p 'test_*.py'
titan strategy test strategies/cross_exchange_mm.py --json
titan strategy compile strategies/cross_exchange_mm.py --artifact-format bundle --output cross_exchange_mm.titan
titan strategy validate cross_exchange_mm.titan --json
```
