# CrossEx L2 WebSocket 延迟实测

2026-10-21，在用户指定的 `43.130.234.54` 测量。**Gate BTC 消息时间戳至本项目 connector 回调入口的观测年龄：平均 10.02 ms，P50 7.94 ms，P95 19.85 ms，P99 27.23 ms，最大 181.89 ms。** 五档快照约每 100 ms 一次，不能把到达延迟解释为推送频率。

## 方法与口径

- 品种：`GATE_FUTURE_BTC_USDT`；项目实际 `GateCrossEx` 行情工厂订阅 Depth，默认五档。另用独立原始 WS 连接同时订阅 Gate、Binance、OKX 的 BTC USDT 合约五档快照作对照。
- 端点：`wss://api.gateio.ws/ws/crossex/public`，频道 `order_book_5`。[官方频道定义](https://www.gate.com/docs/developers/crossex/ws/en/#full-limited-level-order-book-subscription)。有限档完整快照，不是完整全深度增量订单簿。
- 运行时间为北京时间 **14:48:52–14:51:02**；先预热 10 秒，随后连续采样 **120 秒**。实际有效采样窗口为 14:49:02–14:51:02。
- 主要指标是 `本机观察 UTC 时间 − result.ts`。原始连接在 `ws.next()` 返回后、JSON 解析前打点；项目连接在 `NativeMarketBatch::Depth` 的直接发布回调入口打点，包含实际解析、数量归一化及回调前的耗时，尚未进入 MarketService 的状态应用、ABI 发布或策略调度。
- `result.ts` 按协议只是消息中的毫秒时间戳；本次没有独立证明其等同撮合引擎事件时间。因此这里是**消息时间戳至本机的观测年龄**，不能直接当作撮合引擎到策略的总延迟，也不是纯网络单向时延。
- 另外记录 `本机接收 − time_ms` 及两个服务端时间戳之差，用于观察推送分段。这些字段的时钟可能不同，不把其差值直接等同精确网关处理耗时。
- 分位数为 nearest-rank。没有去掉尖峰、负值或尾部样本；本轮各组负延迟均为 0。原始连接与项目连接独立，不能相减两组分位数来推算 connector 开销。
- 全程不读取 API 密钥，不登录私有 WS，不请求账户状态或发送交易指令。探针已正常退出，返回码 [0](exit-code.txt)。

## 与纳秒回调基准的区别

本次探针在回调入口立即取时间，**没有在回调返回时再次打点，不能据此计算回调自身的执行耗时**。两个已测指标必须分别解读：

| 指标 | 起点 → 终点 | P50 / P99 |
| --- | --- | --- |
| 消息观测年龄 | 远端 `result.ts` → 本机回调入口 | 7.94 / 27.23 ms |
| 本机归一化阶段 | JSON 解析之后的 `receive_ts` → 本机回调入口 | 20.283 / 48.166 µs |

第二段中有双边十档的数量换算、字符串复制/格式化和回调发布，尚未计入 MarketService 的 ABI 编码及策略回调，不等于一次函数调用开销。源码存在逐档字符串分配，具有优化空间；没有分段 profiling，不能把整段时间全部归因于其中某个函数。

仓库中[历史纳秒报告](../../../numba_rust_callback_benchmark_m1_pro.json)测量的是 Apple M1 Pro 上的 `single_pointer_tick_callback`（文件标记 ABI 9），Rust P50 3.375 ns、Numba P50 28.750 ns，仅为回调 ABI 基准。另有[历史 Binance 主网实盘记录](../../../market_plugin_technical_design.md#133-性能指标与实盘结果)，从 WS 帧到达、JSON 解析之前计时，到 FastLane handler 入口，Depth P50 18.219 / 19.669 / 18.211 µs。其边界、底层所和执行内容均与本次 Gate 不同，不能直接据此认定性能改善或退化；本次没有复测纯调用的纳秒开销。

## 观测结果

单位为毫秒。表中的原始连接与项目连接是不同 TCP 会话。

| 路径 | 样本数 | 平均 | P50 | P95 | P99 | 最大 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Gate → 实际 connector 直接回调** | **1200** | **10.02** | **7.94** | **19.85** | **27.23** | **181.89** |
| Gate → 原始 WS 接收 | 1200 | 10.72 | 8.86 | 20.57 | 26.63 | 49.24 |
| Binance → CrossEx 原始 WS 接收 | 1106 | 6.61 | 6.41 | 8.94 | 12.33 | 34.38 |
| OKX → CrossEx 原始 WS 接收 | 961 | 11.70 | 11.23 | 15.38 | 18.56 | 27.81 |

其他观测：

| 指标 | 平均 | P50 | P95 | P99 | 最大 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gate connector 相邻快照间隔（ms） | 100.00 | 99.99 | 102.60 | 114.21 | 273.26 |
| Binance 原始相邻快照间隔（ms） | 108.56 | 102.09 | 132.08 | 234.06 | 432.44 |
| OKX 原始相邻快照间隔（ms） | 124.90 | 100.47 | 206.09 | 401.91 | 701.79 |
| WS 协议 Ping/Pong RTT（ms，12 次） | 4.52 | 4.43 | 5.31 | 5.31 | 5.31 |
| Gate JSON 解析后 receive_ts → 直接回调（µs） | 24.57 | 20.28 | 31.93 | 48.17 | 1063.41 |

原始连接 `time_ms` 到本机的 P50 分别为 Gate 3.23 ms、Binance 3.49 ms、OKX 3.23 ms。项目 receive_ts 在 JSON 解析之后，因此上表的微秒项只覆盖其后的数量换算和发布阶段，不能代表全部 connector 开销。

Gate 实际路径稳定在同一 epoch `1`；无 connector 错误，未发现探针所检查的空盘口、档数或时间戳格式错误。四组共 **4467** 条样本，每条均为双边各五档，源时间戳没有回退或重复。有限档协议不提供连续序号，因此本次没有证明“零丢包”；OKX、Binance 的较长推送间隔也不能直接判为丢帧。

### 尖峰

最大 181.89373 ms 的 Gate 样本之前出现 **273.261501 ms** 到达间隔，下一条仅隔 **17.834 µs** 到达，下一条年龄仍有 79.91 ms，表现为停顿后成簇到达。该最大延迟样本从 connector receive_ts 到回调仅 **19.625 µs**，尖峰已经发生在这一打点之前。[最高五个样本](data/outliers.json)。

本次没有抓包或内核调度跟踪，无法进一步区分服务端排队、链路/TCP、TLS/JSON 处理或用户态调度。没有把该尖峰归因于已确定的网络故障，也没有从统计中删除。

## 时钟及运行限制

[采样前 Chrony](chrony-before.txt) 和[采样后 Chrony](chrony-after.txt) 均为 Stratum 3、Leap Normal。System time 报告约慢 **0.043 ms**；RMS offset 约 **0.070 ms**。但按 `Root delay/2 + Root dispersion` 计算的保守 NTP 误差界约 **36.5–36.6 ms**，还不包含远端行情时钟的不确定性，**不能据此宣称绝对单向时延具有亚毫秒准确度**。RTT 使用同机单调时钟，不依赖两端 UTC 时钟一致。

观察期间本机 UTC 与单调时钟总历时差约 −0.0055 ms。探针在现有应用运行中的主机上使用两个 Tokio worker，未绑定独占 CPU；37 秒处进程 CPU 约 0.5%。[进程负载](process-during.txt)、[活动连接](sockets-during.txt)、[主机](host.txt)。该 2 分钟窗口不代表其他时段、全部品种或长期尾部 SLA。

## 构建及复核证据

本地只写代码和文档。`192.168.3.88` 的隔离源码 `/home/dominolu/dev/titan-gatecrossex-live-20261021-39f1267b` 负责格式检查、Clippy 和 release 构建；实盘服务器只运行该服务器生成的产物。Rust/Cargo 1.94.1，Titan 基线 `39f1267b6412165fc7adc005719bc9e09dcef9a8` 加未提交修改。生产 connector 源码沿用上一轮验收版本，本轮新增[公共 L2 测量 example](../../../../connector/examples/gate_crossex_l2_latency_probe.rs)。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | [通过](logs/titan-crossex-l2-fmt.log) |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | [通过](logs/titan-crossex-l2-clippy.log) |
| `cargo build --locked --release -p connector --example gate_crossex_l2_latency_probe` | [通过](logs/titan-crossex-l2-build.log) |
| 347 项最终源码身份核对 | [全部一致](logs/titan-crossex-l2-source-check.log)，[清单](source-files.sha256) SHA256 `fb4e25ad54f70e3e3487ae03b614a2ffe2e981dd5a0aeb5c8391843830817378` |
| 部署制品 SHA256 | `2f5086c0aa82cd474c179ced92b7d0a933d6c0b3051338e516f5f6d8b00380f4`，[实盘摘要](binary.sha256) 与编译服务器一致 |
| 原始样本独立复算 | 在编译服务器用标准库 Python 从 CSV 重算所有时延与间隔分位数，4467 条全部一致；[脚本](recompute.py)、[复算结果](data/recomputed.json)、[日志](logs/titan-crossex-l2-recompute.log) |

首次按 package 默认 features 运行 Clippy 时触发了已有 hftbacktest 关闭 S3 后的两项 needless_return；随后使用项目统一的 workspace/all-features 门禁通过，没有修改无关生产逻辑。[首次 lint 日志](logs/titan-crossex-l2-clippy-default-features.log)。本轮未重跑上一阶段的完整单元测试，不将历史 507 项结果当作本次新证据。

原始证据：[样本 CSV](data/samples.csv)、[Rust 汇总](data/summary.json)、[首帧协议数据](data/first-frames.json)、[进度日志](stderr.log)。复跑时先在编译服务器构建以上 example，部署同一产物到目标主机，运行 `./probe 120 data`；输出目录应使用新的目录，避免覆盖历史结果。
