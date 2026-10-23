# Gate 本地处理优化与 WS 到真实策略回调实测

2026-10-21。Gate 的借用解码与公共精确数量换算已实现，默认开启。最终版本在 `43.130.234.54` 对照测试：本机收到完整 WS 帧到原生 V13 `on_depth` 入口，P50 从 **89.924 µs 降至 55.233 µs（下降 38.58%）**；预热分配审计从每次回调约 **220.26 次降至 2 次（下降 99.09%）**。这不是网络延迟，也不是纯函数调用成本。P99 仍有明显波动，不能据此承诺尾延迟上限。

GitHub 交付时将 Gate 与必要 V13 制品构建接口单独暂存，保留其他 CLI/Python 迁移改动在本地；对实际交付快照独立重跑得到 **508 passed / 0 failed / 12 ignored**，详见[交付快照验证](../gate-crossex-publication/README.md)。本文下方 512 项记录对应此前包含 CLI 迁移的完整工作区。

## 实现与兼容性

- [Gate depth 解码](../../../../connector/src/gatecrossex/depth.rs)：直接从帧借用 symbol、价格和数量字符串，不构造 `serde_json::Value` 树，不逐档克隆 String。每侧 16 档以内使用内联 `SmallVec`；更深盘口可能产生容器分配。非 L2 消息先按轻量提示分流，再由具体解析器验证。
- [精确数量乘数](../../../../connector/src/decimal.rs)：启动会话时从已核对的行情合约元数据编译十进制比例。热路径用带溢出和对齐检查的整数运算直接得到 ABI lot，不先产生基础币数量 String，不使用浮点乘积。普通非负十进制以外的表示不在该转换器的支持范围内。
- [公共借用视图](../../../../connector/src/connector.rs)增加 `NativeDepthLevels::ScaledBorrowed`；[MarketEventBridge](../../../../connector/src/market_event.rs)同步写入 EventArena。异步队列保存编码后的 arena lease，不持有 WS 帧引用。Market ABI V1 字节布局不变；Rust 外部穷举匹配需处理新增变体。
- [MarketRuntime](../../../../connector/src/market_runtime.rs)缓存健康状态文案，仅状态转为 Running 时更新，减少所有 broker 每帧重复创建 `Arc<str>` 的开销。
- [Gate 会话](../../../../connector/src/gatecrossex/stream.rs)在 `ws.next()` 返回后、JSON 解析前记录 `receive_ts`。`borrowed_depth = false` 可退回原通用解析路径，仍保留新的正确收帧时间点。转义字符串等无法借用的合法帧走兼容解析；无效价格、数量或 ABI 对齐错误继续拒绝发布并交现有恢复流程处理。
- Binance 公共 connector 在无凭据时关闭，不再发私有撤单 REST；新增针对无凭据关闭的回归。此改动使无账户公共行情探针的退出行为明确。

REST 订单、私有账户事实和数量语义沿用此前实现。A/B 是**同一最终二进制中的通用解析与借用解析开关**，两侧共同使用本次健康状态缓存优化；它不是历史二进制与当前二进制的全量比较。

## 测量口径

[测量程序](../../../../connector/examples/market_local_latency_probe.rs)与[原生测量制品构建器](../../../../connector/examples/market_local_latency_probe/artifact.rs)使用以下真实热路径：

```text
完整 WS 帧返回，记录 receive_ts（JSON 前）
  → Gate decoder / normalizer
  → MarketServiceCore / MarketEventBridge
  → EventArena，Market ABI
  → EventEngine PRIMARY async lane
  → NativeV13Runtime 解码与策略视图准备
  → 已加载原生 V13 on_depth 的入口打点
```

终点是编译服务器生成的 C 原生测量策略入口，通过正式 V13 制品打包、加载、调度、冻结状态流程执行，实际保存每条回调的接收时间与入口时间。它没有账户或执行绑定，不代表用户 Numba 策略的业务计算耗时。C 上下文关键字段偏移由 Rust `offset_of!` 生成并由 C 静态断言校验。

两个端点均为**同一台运行主机的 CLOCK_REALTIME**，不减交易所时间戳，排除了跨机器时钟偏差及网络传输时间。起点在 WebSocket 库返回完整帧之后，因此也不包括 TLS/WS 解帧和该返回点之前的调度等待；包含 JSON、精确换算、arena 发布、队列等待/唤醒、运行时解码与视图准备。主机 chrony 测试前后为 Normal，但 CLOCK_REALTIME 仍可能受校时影响；本次没有负时间差，未过滤负值或长尾。

固定配置：market arena 128 块、每块 32768 字节；PRIMARY lane 容量 256，SpinSleep，256 次 spin，10 µs idle sleep；主事件引擎 spin=0。没有固定 CPU 或实时调度。目标机为 KVM、2 vCPU、Intel Xeon Platinum 8255C 2.50 GHz、Linux 6.8.0-117-generic。这些配置和主机调度属于测量条件，不能外推为所有部署环境的保证。

每轮先预热 10 秒，再采样 Gate 60 秒 / CrossEx Binance 30 秒。分配计数仅在预热第 5–10 秒启用，正式延迟窗口关闭计数。分配数是**整个探针进程的 Rust alloc/realloc 调用数除以同期已提交回调数**，不计 C/TLS 内部 malloc，时间窗口边界也非逐帧事务；不能据此宣称整个系统零分配。五档帧的逐档 String 已消除，接收帧及公共链路仍有分配。

## 最终版本结果

单位均为 µs；分位数用 nearest-rank，排除预热，不裁剪异常值。五档买卖共 10 档，每条回调样本都核对了档数。

| 模式 | 正式样本 | 平均 | P50 | P95 | P99 | 最大 | 审计分配/回调 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Gate 通用解析 | 600 | 113.771 | 89.924 | 137.152 | 996.583 | 1968.780 | 220.26（11013/50） |
| Gate 借用解析 | 600 | 77.030 | 55.233 | 83.702 | 965.181 | 1768.020 | 2.00（100/50） |
| CrossEx Binance 借用解析 | 292 | 59.632 | 52.331 | 70.402 | 308.994 | 709.969 | 2.00（98/49） |

三轮退出码均为 0，策略最终为 Stopped，drop/resync 均为 0。Gate 每轮包含预热共 699 次回调，CrossEx Binance 共 389 次。Gate 的本机整条链路中位延迟减少约 34.7 µs；这里无法将改善精确拆为解析、分配、排队各占多少。

此前迭代先做了同一旧测量二进制的 ABBA 对照，作为重复性参考，不能和最终版本合并统计：

| 顺序 | 模式 | 样本 | P50 | P99 | 审计分配/回调 |
| --- | --- | ---: | ---: | ---: | ---: |
| A | 通用 | 600 | 90.389 | 283.647 | 222.94 |
| B | 借用 | 600 | 53.503 | 367.817 | 2.00 |
| C | 借用 | 600 | 52.699 | 382.621 | 2.00 |
| D | 通用 | 600 | 90.881 | 681.594 | 219.36 |

该迭代尚未加入非 L2 轻量分流提示。两组聚合 P50 为 90.584→53.103 µs，下降 41.38%。P50 改善在各轮一致；P99 并非每次改善，最终轮也出现近 1 ms 的 P99。链路包含队列和操作系统调度，但本次没有细分 profiling，**没有证据将长尾归因到某一层**，也不能用本结果声称达到纳秒级收帧到策略延迟。此前十几纳秒的纯 ABI 调用测试只覆盖调用边界，和这里的完整处理区间不同。

## 是否可通用于其他 broker

结论：**共享数据表示、精确换算、生命周期和测量方法可以通用；交易所协议解析与恢复不能直接共用 Gate 实现。**

| 层 | 可复用范围 | 边界与当前验证 |
| --- | --- | --- |
| 借用视图 → arena → ABI | `Borrowed` / `ScaledBorrowed` 可供各 broker 发布盘口 | 借用必须在同步编码完成前有效；当前 C ABI 不变 |
| 精确数量换算 | 基础币数量用 ONE，固定线性合约面值用明确乘数 | 必须先核实元数据；反向合约或价格相关数量不能套用固定比例；测试覆盖 Gate、OKX 类比例和非十次幂 lot |
| 通用发布状态 | 缓存健康文案的变更已作用于所有 MarketRuntime | 仍保留原状态转移与错误处理 |
| Gate CrossEx 底层所 | 本次真实验证 GATE 合约乘数与 BINANCE 数量 1:1 | 两者共享 CrossEx 五档协议；不等于独立原生 Binance adapter 已验收，也不等于所有所都支持该频道 |
| 原生 Binance | 已有借用解码思路可沿用公共视图；无需替换为 Gate JSON schema | 本轮通用探针尝试失败：1000 买档 + 1000 卖档初始快照需要 48052 字节，超过探针 32768 字节事件块；原生 V13 市场视图另有 1024 条预算。该结果是探针容量配置不适配，不据此断言所有 Binance 生产配置失败。最新探针对该模式提前明确拒绝，避免反复恢复 |
| 原生 OKX / Hyperliquid 等 | 可采用相同借用/精确整数发布方式和同机入口探针 | 仍需分别处理消息 schema、数量单位、订阅、序号、checksum、快照/增量、恢复与容量；本轮没有逐一改造或真实 WS 验收 |

进一步适配顺序应是：核实单位及最大批次 → 在各自 decoder 生成借用视图 → 用逐字节 ABI 对照验证 → 配套 arena 字节/策略视图条数/测量状态容量 → 再做真实 WS 测量。不能只提高一个全局容量常量，或把所有所的序号、校验和及恢复统一成 Gate 的有限档快照。

本任务的 Gate 优化、实际策略入口对照、共享换算层及适配分析已完成。原生 Binance 大快照的预算协调、其他 broker 全量优化和长时间尾延迟归因是后续独立工作，当前不标记为已通过。

## 编译、回归与来源

本地仅编辑和审查。构建及非实盘验证在 `192.168.3.88` 的隔离目录 `/home/dominolu/dev/titan-gatecrossex-l2opt-20261021-39f1267b`；未覆盖旧远端工作区。基线 `39f1267b6412165fc7adc005719bc9e09dcef9a8` 加未提交修改；源码身份见 [SHA256 清单](source-files.sha256)及[服务器核验](logs/source-check.log)。清单同时覆盖保留的编译链迁移工作；本次没有提交或回滚它们。

项目目录通过 `rust-toolchain.toml` 覆盖为 Rust/Cargo **1.94.1**、`x86_64-unknown-linux-gnu`，构建机 glibc 2.35；服务器 home 默认的 Rust 1.93.0 不是本次项目工具链。复用编译服务器 fixture：pair_arb `5f532d44ed3b14725e8aa212de9c30ee0e4dbb81378bfcd202d8221cf660af63`；event_counter `e0cbe122bea8c0e3a24d932cbadb5b2945205d9b3258482b0684a40be85bdb65`。

| 检查 | 结果 | 日志 |
| --- | --- | --- |
| 全 workspace、all targets、all features | **512 passed / 0 failed / 12 ignored** | [workspace](logs/titan-crossex-l2opt-workspace.log) |
| connector 回归 | 287 passed / 0 failed / 12 ignored，包含精确换算、ABI 一致性、借用来源、非法数值、转义回退与公共关闭 | [connector](logs/titan-crossex-l2opt-tests.log) |
| workspace doctest | 2 passed / 0 failed / 1 ignored | [doc](logs/titan-crossex-l2opt-doc.log) |
| fmt check、workspace Clippy all targets/features `-D warnings` | 通过 | [fmt](logs/titan-crossex-l2opt-fmt.log)、[Clippy](logs/titan-crossex-l2opt-clippy.log) |
| gatecrossex 单 feature library 编译 | 通过；21 条既有 feature 组合下 unused/dead-code warning，非无警告门禁 | [feature](logs/titan-crossex-l2opt-feature.log) |
| release 测量 example | 构建成功，最终版本完成三轮实际 WS 测试 | [build](logs/titan-crossex-l2opt-build.log)、[最终退出码与数据](final/) |
| 构建机本地模拟 WS → 真实 V13 | 通用/借用各 30 个正式样本、零 drop/resync，均正常停止；只作集成 smoke | [通用](loopback-legacy/summary.json)、[借用](loopback-borrowed/summary.json)、[fixture](loopback.py) |
| 原始 CSV 独立重算 | 每条 delta、档数、分位数与汇总一致；失败的原生 Binance 明确保留 exit=1 | [脚本](analyze.py)、[分析](analysis.json)、[日志](analysis.log) |

生产修改完成后的全仓库回归为 512 项；随后仅测量 example 增加 CrossEx Binance 模式及原生 Binance 容量拒绝提示，又通过 fmt/Clippy/release 构建和最终公开 WS 验证。忽略的真实账户测试没有统一开启。本轮未改变 Python 应用代码。

验证环境变量：`CARGO_TARGET_DIR=/home/dominolu/dev/titan-compiler-20261021-39f1267b/target`；`TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan-compiler-20261021-39f1267b/.venv/bin/python`；`TITAN_STRATEGY_SDK` 指向本次隔离目录的 `python/titan-strategy-sdk`。

编译服务器生成的最终 stripped `probe.final` SHA256：`ba7400339b5f7a0151b0e3d911fd8a027c84b269ecd21d40f987fab8b544900f`；测量策略 `probe.titan`：`0e748b99806f933905836180a91bfbaf1ccb9609312c109e4f023ae7580b1a5e`，与实盘服务器摘要一致，见[部署摘要](final/final-binary.sha256)。首轮 ABBA 的旧探针 SHA256 为 `e0ed5ff4486debeb50ace4f5c879e88591ba350f1ba1e4e43d57829cbe2a69e8`，单独保存在 iteration1。

复现实盘测量时，在编译服务器构建 release example 并通过 `--build-artifact DIR` 生成策略，核对摘要后只部署这两个产物，在目标服务器依次执行：

```sh
./probe.final ./probe.titan gate-legacy 60 final-legacy
./probe.final ./probe.titan gate-borrowed 60 final-borrowed
./probe.final ./probe.titan crossex-binance 30 final-crossex-binance
```

正式样本与汇总位于 [final](final/)，首轮完整记录位于 [iteration1](iteration1/)。本轮仅使用公开行情，没有读取 API 密钥、账户或发订单；全部测量进程已退出。此前真实账户临时凭据文件仍不存在。
