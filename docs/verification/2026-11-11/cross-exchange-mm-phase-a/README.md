# cross_exchange_mm 阶段 A 修复与验收（2.0.1）

日期：2026-11-11（Asia/Shanghai）。基线：`master` `80189496` 加当时工作区未提交改动。
本文记录 `2.0.1` 的验收；该策略随后升级到 `2.0.2`（提交用途声明），见 [B4 验收](../cross-exchange-mm-b4-command-purpose/README.md)。
本轮只做阶段 A（策略报价安全、业务故障返回、公开观测、内嵌场景）与静默配置预检，
**不涉及平台双绑定执行（阶段 B）**，也不启动实盘、不连接真实账户、不读取凭据。

## 本轮修改

| 编号 | 修改 | 位置 |
| --- | --- | --- |
| A1-a | SDK 静态校验在声明 `BUSINESS_FAULT_DRAIN` 时允许处理器返回计算业务码；未声明的策略仍只允许 `0/-1..-4` | `python/titan-strategy-sdk/titan_strategy/static_compiler.py` |
| A1-b | 公开观测 5 个字段由 `uint64` 改 `int64`，与状态字段符号一致；状态布局不变，schema 仍为 2 | `strategies/cross_exchange_mm.py` |
| A1-c | 业务故障回调批次改为只丢弃非 reduce-only 新风险提交，保留同回调暂存撤单/reduce-only 并正常校验提交；在线宿主与离线执行器同规则 | `crates/titan-strategy-runtime/src/runtime_v13.rs`、`offline_v13.rs` |
| A1-d | 对冲预算测试标定由 50050 改 50080（旧值在初始报价即被新检查拒绝，测试期望不成立） | `python/titan-strategy-sdk/tests/test_cross_exchange_mm.py` |
| A1-e | 内嵌 `TESTS` 场景修正并新增：缺账户失败关闭、预热后双边 PostOnly 报价、限频窗口内 hedge 预算越界立即撤单 | `strategies/cross_exchange_mm.py` |
| A1-f | Clippy 与 rustfmt 收口（类型别名、可简化布尔表达式、格式化） | `titan-account-service`、`titan-strategy-runtime` |
| C3 | 新增双市场配置静态预检 `scripts/cross_exchange_mm_preflight.py` 与 6 项单测 | `scripts/` |

## 环境

- 构建与非实盘验证在编译服务器隔离目录执行；本地只编辑与审查。
- rustc 1.94.1、Python 3.11.14、Numba 0.63.1、NumPy 2.2.6。
- 目标三元组 `x86_64-unknown-linux-gnu`，CPU baseline `x86-64`，产物未签名。

## 结果

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy -p titan-strategy-runtime -p titan-account-service --all-targets --no-deps -- -D warnings` | 通过 |
| Rust 单测（上述两包） | 128 通过、0 失败、11 忽略 |
| SDK 回归（`python/titan-strategy-sdk/tests`） | 160 项通过 |
| 内嵌原生场景（`titan strategy test`） | 声明 3、通过 3（native_callbacks） |
| `titan-cli` 集成测试 | 108 项通过（含 `single_file_strategy` 6 项、发布 3 项） |
| 配置预检单测 | 6 项通过 |
| 示例配置预检（`--allow-shadow`） | PASS（按设计无执行风控包络，仅 shadow 允许） |
| 示例配置预检（受控实盘语义） | FAIL（`execution-limits`：双市场受控执行包络仍缺失，符合阶段 B 未完成） |
| 产物编译与校验（`strategy compile` / `validate`） | 通过，`valid: true` |

## 产物与摘要

**稳定身份（跨构建可复现）**：

| 项目 | 值 |
| --- | --- |
| 策略类型 / 源码版本 | `cross_exchange_mm` / `2.0.1` |
| 恢复 schema | 2（状态布局未变） |
| `source_digest` | `9b3d9e3e43d4a6a07853eaffe5aa91ba79669fead892b6fddda2737a8e26130e` |
| `parameters_digest` | `ef980cd2c3441c0f8869d3afe13dc00b37817f3254d23c2c12b8a942ce1e0713` |
| 内嵌场景 | 声明 3、通过 3 |
| 目标 / CPU baseline | `x86_64-unknown-linux-gnu` / `x86-64` |

**某一次构建的实际字节（不可复现，仅用于本次产物）**：

| 项目 | 值 |
| --- | --- |
| `compiled_strategy_digest` | `953b851e8fb6c5f4736cba5cf79c45e18c647b0040771162769f750f498f1311` |
| `native_library_sha256` | 见私有证据目录内的 `show` 输出 |
| 产物文件 sha256 | `852cbb0b8837562a1789f3e393387511f113c612aa5cf5bbf235c9cf59ff5e0c` |
| 编译用 `titan` 二进制 sha256 | `7ec73a502b1670bc9c1a7ab14a821230acfc314e1d0d5a78de05f493870e2a93` |

### 产物不可字节复现（本轮发现）

同一源码、同一 `titan` 二进制连续编译两次，结果字节不同：

| 编译 | bundle sha256 | `compiled_strategy_digest` | `native_library_sha256` |
| --- | --- | --- | --- |
| 第 1 次 | `ac4b3b0758641fe1…` | `3c9b75ee52445206…` | `c858293ef70f752f…` |
| 第 2 次 | `cfabb353ad0d563c…` | `3bcf0dd1adba67d0…` | `f78cd7a355d66247…` |

`source_digest` 与 `parameters_digest` 两次完全相同。结论：Numba/LLVM 生成的产物含非确定性内容，
因此**不能**用"重新编译应得到相同摘要"作为产物校验，也不能把某次构建的
`compiled_strategy_digest` 当作源码身份的等价物；源码身份以 `source_digest` +
`parameters_digest` + 工具链身份为准。发布流水线的重复构建一致性校验对这个策略不适用。

产物与原始命令日志保存在仓库外的私有证据目录（含未签名 bundle 与逐命令输出），
本页只保留脱敏摘要；产物本身不入 Git。

## 边界（不得据此宣称可实盘）

- 受控实盘 `execution_limits` 仍只接受单市场绑定，双市场双账户受控执行包络（阶段 B）不存在；
  关闭该门禁运行属于未证明路径，文档明确禁止。
- 未签名产物；受控实盘需要签名产物与受信公钥配置。
- 没有逐所保证金/救援预占、没有 `FlattenOwned`、没有逐市场行情年龄；这些是 B3-B7。
- 本轮全部为合成事实与静态配置校验，不是双所真实行情回测，也没有收益、成交率或延迟结论。
- 没有访问任何真实账户、凭据、交易所私有流或实盘服务器。

## 复现入口（通用）

在编译服务器隔离目录：

```bash
cargo fmt --all -- --check
cargo clippy -p titan-strategy-runtime -p titan-account-service --all-targets --offline --no-deps -- -D warnings
cargo test -p titan-strategy-runtime -p titan-account-service --offline
python -m unittest discover -s python/titan-strategy-sdk/tests -p 'test_*.py'
python -m unittest discover -s scripts -p 'test_cross_exchange_mm_preflight.py'
titan strategy test strategies/cross_exchange_mm.py --json
titan strategy compile strategies/cross_exchange_mm.py --artifact-format bundle --output cross_exchange_mm.titan
titan strategy validate cross_exchange_mm.titan --json
python scripts/cross_exchange_mm_preflight.py configs/cross_exchange_mm.example.toml --allow-shadow
```
