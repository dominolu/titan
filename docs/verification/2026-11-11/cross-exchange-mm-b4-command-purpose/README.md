# cross_exchange_mm B4：提交用途与"成交先于持仓"的合法对冲

日期：2026-11-11（Asia/Shanghai）。对应工作清单 B4。本轮只做平台侧命令用途与成交先于持仓的
准入证明，**不涉及实盘、真实账户或凭据**。前置：[阶段 A 验收](../cross-exchange-mm-phase-a/README.md)、
[多绑定执行设计](../../../cross_exchange_mm_multi_binding_design.md) §4。

## 解决的问题

此前 `InventoryProof::validate_opening` 在任何"成交已到、持仓未到"的窗口里拒绝一切
非 reduce-only 提交。跨所做市的对冲恰好发生在那个窗口：maker 成交先到，平台还没收到持仓推送，
于是唯一合法的对冲被自身风控挡掉（修复方案 §4.3 记录的缺陷）。直接删除门禁会放开全部新开风险，
所以必须引入可验证的用途声明。

## 实现

| 位置 | 改动 |
| --- | --- |
| `crates/titan-strategy-runtime/src/execution_binding.rs` | 新增 `CommandPurpose`（0=未声明、1=maker 报价、2=已知成交对冲、3=救援退出、4=自有持仓退出）、`validate_command_purpose`、`hedge_reduces_proven_exposure` |
| `crates/titan-strategy-runtime/src/runtime_v13.rs` | `commit_commands` 每次派单前校验用途；`NativeV13Core.multi_binding` 接入；`validate_opening` 改为用途感知 |
| `python/titan-strategy-sdk/titan_strategy/context_v13.py` | `submit_order(..., purpose=0)`，写入请求首个保留字节 |
| `python/titan-strategy-sdk/titan_strategy/types.py` | 新增 `CommandPurpose` 枚举（可读性用，策略侧传整数） |
| `strategies/cross_exchange_mm.py` | 每笔提交声明用途；版本 `2.0.1` → `2.0.2`，恢复 schema 仍为 2 |
| `python/titan-strategy-sdk/tests/test_cross_exchange_mm.py` | 夹具接受并记录用途；新增用途声明测试 |

ABI 布局不变：用途复用 `TitanSubmitOrderRequest.reserved[0]`，默认 0。

## 准入规则（为什么不能借用途绕过风控）

1. 未知用途字节一律拒绝（`execution_purpose_invalid`），有无多绑定包络都拒绝。
2. 未配置多绑定包络（旧单市场 `execution_limits` 路径）时，非零用途只是声明式信息，
   **不授予任何放宽**；`validate_opening` 仍是全局门禁，行为与旧版一致。
3. 配置多绑定包络后按绑定角色复核：
   - `MakerQuote` 只能来自 maker 腿，`KnownFillHedge` 只能来自 hedge 腿（`execution_purpose_role`）；
   - 未绑定的 `(account, asset)` 拒绝（`execution_purpose_unbound`）；
   - `RescueExit` / `OwnedExit` 必须 reduce-only；
   - `KnownFillHedge` 不允许声明 reduce-only。
4. `KnownFillHedge` 是唯一能在持仓未确认时放行的提交，条件是**同时**满足：
   - 没有仍缺独立成交事件的填充（`unsettled == false`，即 order 事件已报量但成交事件未到，一律拒绝）；
   - 该提交指向 hedge 腿；
   - 它**缩小**平台已用独立成交证明的未对冲敞口：
     `unhedged = maker_expected × 整数lot比例 + hedge_expected`，
     卖出只在 `unhedged > 0` 且 `qty <= unhedged` 时成立，买入对称；
   - 其余情况仍返回 `execution_inventory_unproven`。

因此用途只能覆盖"已经存在、已证明"的敞口，不能新开风险；超量、反向、未声明用途都保持拒绝。

## 验收

构建服务器隔离目录，rustc 1.94.1。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| Clippy（两包，`--all-targets --no-deps -D warnings`） | 通过 |
| Rust 单测（`titan-strategy-runtime` + `titan-account-service`） | 130 通过、0 失败、11 忽略 |
| `titan-cli` 集成测试 | 108 通过 |
| SDK 回归 | 161 通过 |
| 内嵌原生场景（`titan strategy test`） | 声明 3、通过 3 |
| 产物编译 / `validate` | 通过，`valid: true` |
| 配置预检单测 | 6 通过 |

新增回归：

- `known_fill_hedge_is_admitted_only_when_it_reduces_proven_exposure`：正常对冲放行；
  超量（51 > 50）、反向、未声明用途、仍缺独立成交、无多绑定包络、持仓已确认后普通开仓，
  六种情形分别断言。
- `command_purpose_is_verified_against_bound_roles`：角色错配、reduce-only 违规、
  未绑定腿、未知用途字节（有/无包络）、旧路径声明式接受，六类断言。
- `test_declared_submit_purposes_match_the_leg`（Python）：maker 两笔声明为 `1`，
  成交后的 IOC 对冲声明为 `2`。

## 产物身份

| 项目 | 值 |
| --- | --- |
| 源码版本 / 恢复 schema | `2.0.2` / 2（状态 8,072 字节，布局未变） |
| `source_digest` | `2a639d8b783c5f830ac2e08238c7aa101ecbd4c93710ee38b7c7b954c5223829` |
| `parameters_digest` | `ef980cd2c3441c0f8869d3afe13dc00b37817f3254d23c2c12b8a942ce1e0713` |
| 本次产物 sha256 | `a225631846fd3254d98b9f0c6259b1e2339afcba227329479a655fad8edf8dc3` |
| 编译用 `titan` 二进制 sha256 | `90ebc933a868223fae2cf9bd35c63ed2ee981fbaacc31205148e07ac27fc2b2a` |

产物未签名，且如[阶段 A 验收](../cross-exchange-mm-phase-a/README.md)所述**不可字节复现**；
源码身份以 `source_digest` + `parameters_digest` + 工具链为准。原始日志与产物在仓库外的私有证据目录。

## 边界

- 仍未解除单市场门禁：多绑定包络需要 B5（逐所预占）、B7（`FlattenOwned`）、B8（管理准入）
  与管理入口双绑模板，才能走受控实盘路径。本轮只让"配置了多绑定包络的实例"具备合法的
  fill-before-position 对冲准入。
- 离线执行器 `offline_v13.rs` 目前没有执行风控路径，用途校验尚未接入；纳入 B9。
- 逐所保证金、救援额度消耗、崩溃恢复下的用途重放都未实现（B5）。
- 没有访问真实账户、凭据、私有流或实盘服务器。

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
