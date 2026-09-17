# Strategy ABI V13 实施状态

更新日期：2026-09-17

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
- `pair_arb`：maker-taker/taker-taker slot、累计成交 delta、hedge obligation ledger、受保护 IOC
  对冲、cancel-confirm-replace、超时/冷却、账户/持仓/readiness/stale gate、draining 与故障姿态。
- CLI：`titan strategy compile --strategy ...` 为唯一策略编译入口；Core live 不再链接或加载
  libpython。旧的 in-process backtest worker已移除，避免继续暴露非 V13 策略执行路径。
- 部署模板：`deploy/pair_arb_v13` 固定 V13 bundle 与 digest。所有主网 source/account/strategy
  默认禁用，只有获得明确实盘授权后才能逐项启用。

## 编译服务器验证

- Python SDK：13 passed，另有 5 个 subtests passed。
- `titan-strategy-runtime` V13 单元测试：3 passed。
- Rust CLI 单元/配置/无 Python live validate 测试通过。
- `pair_arb` AOT bundle：digest
  `5ea85a7c5d62c2d5f190e33c8733ccab0620eb012367ba73f218a95657565d39`。
- Rust `v13_pair_arb_smoke`：`dlopen -> on_start -> initiator -> actual-fill-driven hedge` 通过。
- `cargo check -p titan-strategy-runtime -p titan-cli` 无 warning。

## 本次未执行

未连接真实账户，未启动长期策略，未下单或撤单。部署模板保持禁用；实盘启用需另行确认账户、
品种、最小数量、价格保护和最大风险敞口。
