# cross_exchange_mm B8（部分）：保证金投影接入运行时准入

日期：2026-11-11（Asia/Shanghai）。对应工作清单 B8。**本轮只完成一半**：保证金投影已接进
运行时准入；管理入口的双账户/双市场准入与执行模板**没有做**，原因见第 5 节。

## 1. 运行时准入接入保证金投影

此前 B5-剩余只交付了 `account-service` 侧的投影函数，没有任何调用者。本轮把它接进派单前准入：

| 位置 | 改动 |
| --- | --- |
| `StrategyRuntimeBuildContext` | 新增 `accounts_service: Option<Arc<dyn AccountService>>`（真实宿主注入，示例/测试为 `None`） |
| `runtime_v13::margin_legs` | 创建期按绑定腿向账户服务取 `(instrument, currency)` 单位；缺服务、缺账户、缺资产或货币一律返回 `execution_margin_unproven` 拒绝创建 |
| `runtime_v13::check_leg_margins` / `margin_outcome` | 派单前逐腿计算「已预占 own 名义 + 本次提交名义」，用该账户自己的 `available_units` 走 `margin_projection`；不覆盖则 `execution_margin_insufficient`，余额未知则 `execution_margin_unproven` |
| `reserve_staged_makers` | 每笔非 reduce-only 提交在预占账本之前先过保证金检查 |

关键性质：

- **逐账户独立**：一个账户的盈余不会被另一个账户的缺口占用。
- **失败关闭**：余额未知、为零、为负、保证金率缺失都拒绝，不当作零需求。
- **多绑定必须有账户服务**：缺服务时创建即失败，不会退化成"无抵押证明也能跑"。
- 旧单市场路径（`multi_binding` 为 `None`）完全不受影响，`margin_legs` 为空、检查直接返回。

## 2. 管理入口：明确拒绝而不是误报

`crates/titan-cli/src/management/control.rs` 增加两条前置校验：

- 双市场/双账户策略在受控管理路径下会得到明确错误：
  `managed admission supports one market and one account; multi-leg bindings require the
  multi-binding execution envelope (worklist B8)`，而不是原来含糊的 `strategy account binding mismatch`。
- 运行配置若已自带 `execution_limits` 或 `multi_binding` 会被拒绝：受控管理路径拥有执行包络，
  不允许配置文件另设一套。

这不会放开任何能力，只是把"为什么不能启动"讲清楚，并堵住配置覆盖包络的旁路。

## 3. 验收（构建服务器，rustc 1.94.1）

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| Clippy（两包，`-D warnings`） | 通过 |
| Rust 单测 | **144 通过**（account-service 33 + strategy-runtime 111）、0 失败、12 忽略 |
| `titan-cli` 集成测试 | 108 通过（含受控管理路径既有用例） |
| SDK 回归 | 161 通过 |
| 内嵌原生场景 | 声明 3、通过 3 |
| 配置预检单测 | 6 通过 |
| 产物编译 / `validate` | 通过 |

新增回归 `margin_admission_is_per_account_and_fail_closed` 覆盖：双侧充足通过；一富一穷时穷的一侧
`execution_margin_insufficient` 而富的一侧仍通过（证明不跨账户抵扣）；余额未知
`execution_margin_unproven`；已预占名义计入同一余额；降低保证金率释放容量（100% 拒绝、50% 通过）；
无预占无请求时不需要余额。

## 4. 单市场门禁的真实现状（澄清）

`commit_commands` 里的 `execution_binding_unproven` 只在配置了 `execution_limits`（旧单市场包络）
时触发。配置 `multi_binding` 时根本不进那个分支，因此**运行时层面双市场已经不再被挡**；
真正挡住"受控管理启动"的是 `control.rs` 的单腿准入，也就是本节未完成的部分。
不受管理的 `core-run` 两条路都能起，但没有管理生命周期与观测。

## 5. 为什么没做管理入口双绑模板（未完成项）

多腿绑定需要一套新的受控配置：每腿的账户、资产、行情源、角色、`base_delta_per_lot`、
最小量/最小金额、仓位与名义上限、行情年龄、保证金域、保证金率，加上救援额度与停止预算
（约 12 个新字段，每个都要校验、投影到管理资源、进入幂等键，并配套测试）。
它会改动**当前唯一可用的受控实盘路径**（单腿准入及其验收用例）。

把它和保证金接线混在同一轮草率落地，会得到一个"看似能启动、实际校验不全"的管理路径——
这正是工作清单里写明要避免的情况。因此本轮只交付可完整验证的一半，另一半单独立项。

## 6. 产物身份

| 项目 | 值 |
| --- | --- |
| 源码版本 / 恢复 schema | `2.0.2` / 2（策略源码未变） |
| `compiled_strategy_digest` | `d63fd6cf1e0627c2ca93445ec57851644481d29c5017069764dce28316e2a355` |
| 本次产物 sha256 | `0be86e55a54f482d9d887ffa8110ffa278483c60c90530660ae6751488be9044` |
| 编译用 `titan` 二进制 sha256 | `152be270044a0aaeb6af3d8ca50c5325ab5f5a530ec0941c2d8a698b5cd9fe6c` |

产物未签名、不可字节复现；原始日志与产物在仓库外的私有证据目录。

## 7. 复现入口

```bash
cargo fmt --all -- --check
cargo clippy -p titan-strategy-runtime -p titan-account-service --all-targets --offline --no-deps -- -D warnings
cargo test -p titan-strategy-runtime -p titan-account-service --offline
cargo test -p titan-cli --offline
python -m unittest discover -s python/titan-strategy-sdk/tests -p 'test_*.py'
titan strategy test strategies/cross_exchange_mm.py --json
titan strategy validate cross_exchange_mm.titan --json
```
