# Strategy ABI V13 实施状态

更新日期：2026-10-13

## 结论

Strategy Service 与 `pair_arb` 已完成 V13 硬切换。生产路径只接受 `native-v13` 制品；旧
in-process Python/Numba loader、f64/i64 双数组 state、V12 SDK surface、旧策略包和旧 CLI
编译入口均已删除。Python/Numba 只存在于编译服务器的离线 AOT 阶段。

## 已完成

- V13 SDK：固定宽度 ABI、typed state、参数 schema、只读 public views、callback-local command
  staging、受限 definition worker 和一次性 AOT worker。
- 确定性制品：host-target PIC native library、descriptor、动态符号审计、deterministic CBOR、
  pair/bundle 原子发布、digest/signature 校验；同一进程重复构建也由隔离 worker 保证字节稳定。
- Rust loader/runtime：ABI fingerprint、state schema/alignment、native digest、可选 Ed25519、内容寻址
  cache、实例隔离、事件解码、公开状态快照、订单 ownership 过滤、command 校验和 direct execution。
- Engine 集成：V13 manifest 驱动订阅；市场 routing key 与账户 routing key 由部署 binding 注入；
  lifecycle、timer、supervisor、activation gate、handler deadline 与 EventEngine PRIMARY lane 接通。
- `pair_arb`：V3 单 current-slot、runtime-owned active orders/OrdersList、最小私有 order refs、
  maker-taker/taker-taker、cancel-confirm-replace、账户/持仓/readiness/stale gate 与故障姿态。
- CLI：`titan strategy compile --strategy ...` 为唯一策略编译入口；Core live 不再链接或加载
  libpython。旧的 in-process backtest worker已移除，避免继续暴露非 V13 策略执行路径。
- 部署模板：`deploy/pair_arb_v13` 固定 V13 bundle 与 digest。所有主网 source/account/strategy
  默认禁用，只有获得明确实盘授权后才能逐项启用。

## 2026-09-18 编译服务器验证

- Python SDK：25 passed，另有 5 个 subtests passed。
- `titan-strategy-runtime` V13 单元测试：10 passed。
- Rust CLI 单元/配置/无 Python live validate 测试通过。
- `pair_arb` V3 AOT bundle：digest
  `a67214598f0e708aa96a8f43710ce48fd05f3b2e0210241036bbea3873332c70`。
- Rust `v13_pair_arb_smoke`：`dlopen -> on_start -> initiator -> actual-fill-driven hedge` 通过。
- `cargo check --workspace --all-targets` 通过。

## 本次未执行

未连接真实账户，未启动长期策略，未下单或撤单。部署模板保持禁用；实盘启用需另行确认账户、
品种、最小数量、价格保护和最大风险敞口。


## 2026-10-13 通用恢复

新增宿主恢复协议与可选 `on_recover` 生命周期回调。账户服务提供同一 committed epoch/version 下的订单、持仓和余额；策略运行时保存受校验和保护的事实基线，确认订单归属、订单参数、累计成交和持仓，再调用策略业务状态重建。绑定摘要还覆盖行情来源/模式/时间框架以及账户本地品种到实际资产的执行映射。恢复期间关闭命令入口；新检查点持久化失败、启动前状态漂移或运行中会话变化均保持交易关闭。

订单批次在外部执行前先持久化私有状态和完整订单意图，防止崩溃后出现快照无法归属的本策略订单。此路径增加一次检查点写入与 flush；当前实现以持久化正确性为先，未宣称实盘延迟满足 HFT SLA。

已移除 `pair_arb` 的账户 epoch/sequence 私有字段及重复会话处理，保留 Slot、角色、预期持仓和风险姿态等业务事实。制品版本 `3.1.0`，私有 state schema `4`。

恢复回调扩展更新了 ABI fingerprint，C 结构布局不变。历史制品须重新编译；旧快照不跨制品或 state schema 自动迁移。新协议中缺少恢复基线，或策略未声明恢复回调时，仍执行严格公共摘要匹配。

当前不会自动重放缺少完整历史依据的离线成交。订单终态不在保留范围、历史成交变化、未知活动订单或持仓变化时返回明确的恢复失败原因。市场状态重新采样，策略自行 warm-up。协议说明见 [SDK 恢复契约](../python/titan-strategy-sdk/README.md)。

完整验证：440 项 Rust、2 项 doctest、34 项 Python，以及全部原生/CLI 集成通过；Clippy `-D warnings` 无未处理提示。源码、限制和原始证据见 [系统重构清单](system_refactor_checklist.md)。

## 2026-10-13 双向持仓

通用账户链路以账户、资产和方向区分 NET/LONG/SHORT，允许双侧零仓位并逐侧维护 WS 时间戳。策略持仓查询和下单请求支持显式方向；订单历史、公共摘要和恢复对账均保留方向。已有 NET 策略不会自动合并双侧仓位。最新 ABI fingerprint 为 `7864d0be8e1b83b0683747bf4fdfd4cce1f87377cb3fdb97c56a9a270c7a50ed`，此次扩展改变公共视图布局；旧制品须重新编译，旧快照不自动迁移。验证证据见系统重构清单的同名章节。

## 2026-10-13 Review 修复验证

R1–R7 已修复：真实投影复合快照、定时失败关门、同步发布重入保护、延迟下单会话 fencing、宿主账户订阅、真实模式空仓 bootstrap 和 Hyperliquid 完整持仓流。最终编译服务器回归为 Rust 446、doctest 2、Python 35，通过严格 Clippy 和全部 AOT/CLI 非实盘集成。实盘测试继续保持未启用，旧 ABI/制品/状态结构不自动迁移。具体范围及原始日志见 [Review 修复报告](verification/2026-10-13/review-fixes/README.md)。
