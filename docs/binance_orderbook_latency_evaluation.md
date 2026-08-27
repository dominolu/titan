# Titan 与 Nautilus Binance OrderBook 延迟评测报告

评测日期：2026-08-26

## 1. 评测目的

本报告汇总 Titan/HftBacktest 与 Nautilus 在 Binance USD-M Futures 实盘公开行情下，从行情接收进入本地系统到策略消费行情的延迟数据，比较两套系统当前观测到的中位延迟和尾部延迟，并说明测试口径差异。

本报告中的 Titan 数据来自服务器 `43.165.184.116` 上的优化版实测；Nautilus 数据来自本次提供的 60 秒正式测试结果。

## 2. 测试对象

### 2.1 Titan/HftBacktest

- 标的：Binance USD-M Futures `BTCUSDT`
- 初始深度：REST 快照，最多 1000 档买盘和 1000 档卖盘
- 后续更新：WebSocket 增量深度
- 同时订阅：trade、`depth@0ms`、markPrice、bookTicker
- 策略接口：Numba `@njit on_tick(s)`
- 延迟起点：Tungstenite 返回完整 WebSocket Text frame 后、JSON 解码前
- 延迟终点：Numba `on_tick` 读取对应 tick
- Tick frame：1 ms 最大等待时间；feed 到达后立即返回
- 预热：30 秒
- 正式采样：60 秒
- 有效样本：32,894
- 策略回调：118,290（包含预热阶段）
- 时钟异常：0
- 结果日志：`/home/ubuntu/titan-latency-results/ws_to_on_tick_20260826T025947Z.log`

Titan 已启用以下热路径优化：

1. 一条 WebSocket 消息形成一个 `FeedBatch`，避免逐价格档进入 MPSC 队列。
2. publisher 批量执行深度融合。
3. Iceoryx 共享内存使用 `instrument_id + Event[]` 固定布局传输，不在 feed 热路径携带 symbol 字符串，也不使用 bincode。ß
4. LiveBot 一次消费整批 feed，并在队列清空后才重新等待 IPC 通知。

### 2.2 Nautilus

- 标的：Binance USD-M Futures `BTCUSDT`
- 初始深度：REST 1000 档深度快照
- 后续更新：持续应用 WebSocket 增量，`update_speed=0`
- 正式测试时长：60 秒
- 测试时间：北京时间 2026-08-26 11:01:25–11:02:25
- 总样本：2,246
- 热身排除：前 100 个样本
- 有效样本：2,146
- 接收 → `on_tick` 平均值：392.071 µs
- 延迟主口径：行情接收 → Nautilus `on_tick`
- 回调分发口径：框架回调 → 自定义 `on_tick`
- 外部端到端口径：交易所事件时间 → `on_tick`

Nautilus 数据未提供服务器配置、接收时间戳的精确采集位置、交易所与本机时钟同步误差及 CPU 使用率。正式结论前需要核实这些条件。

## 3. 延迟结果

所有数据统一换算为微秒。

| 指标 | Titan | Nautilus | 观测差异 |
|---|---:|---:|---:|
| min | 8.930 µs | 未提供 | 不比较 |
| p50 | 53.969 µs | 310.663 µs | Titan 约快 5.76 倍，延迟低 82.6% |
| p90 | 116.337 µs | 6.988 | 不比较 |
| p99 | 656.760 µs | 2,050 µs | Titan 约快 3.12 倍，延迟低 68.0% |
| max | 2,022.963 µs | 5,214 µs | Titan 约快 2.58 倍，延迟低 61.2% |
| mean | 75.787 µs | 392.071 µs | Titan 约快 5.17 倍，延迟低 80.7% |

从本次观测结果看：

- Titan 中位延迟约为 54 µs，明显低于 Nautilus 的 311 µs。
- Titan p99 约为 0.657 ms，Nautilus p99 约为 2.050 ms。
- Nautilus 本轮未提供 p99.9，无法与 Titan 的 1.407 ms 比较。
- Titan 本轮最大值为 2.023 ms，低于 Nautilus 的 5.214 ms。
- Titan 平均值为 75.787 µs，低于 Nautilus 的 392.071 µs。
- Titan 约 99% 的样本在 1 ms 内到达 Numba `on_tick`。

### 3.1 Nautilus 延迟分解

| Nautilus 测量区间 | 结果 | 说明 |
|---|---:|---|
| 行情接收 → `on_tick` | p50 310.663 µs；p95 769.332 µs；p99 2.050 ms；max 5.214 ms | 与 Titan WS ingress → Numba `on_tick` 最接近的比较口径 |
| 框架回调 → 自定义 `on_tick` | p50 0.930 µs；p95 1.349 µs；p99 1.620 µs；max 32.190 µs | 用户自定义回调通常不是主要延迟来源 |
| 交易所事件时间 → `on_tick` | p50 5.464 ms；p90 6.988 ms；p95 7.871 ms；p99 8.946 ms；max 46.738 ms | 包含交易所发布时间、网络传输、接收处理和本地回调；还会受到跨机器时钟误差影响 |

Nautilus 的框架回调到自定义 `on_tick` 的 p99 只有 1.620 µs，说明其 310.663 µs 中位接收延迟和 2.050 ms p99 主要产生在自定义策略函数之前，而不是用户 `on_tick` 函数内部。自定义回调最大值为 32.190 µs，存在少量调度或系统离群点，但对主链路最大值 5.214 ms 的贡献仍然有限。

## 4. 样本吞吐与口径差异

| 项目 | Titan | Nautilus |
|---|---:|---:|
| 正式统计时长 | 60 秒 | 60 秒 |
| 原始采集量 | 32,894 个 WS 时间戳样本 | 2,246 个样本 |
| 约合原始采集速率 | 548.23 样本/秒 | 37.43 样本/秒 |
| 热身排除 | 30 秒 | 前 100 个样本 |
| 有效统计量 | 32,894 | 2,146 |
| Nautilus 有效样本速率 | 不适用 | 约 35.77 样本/秒 |

两者原始采集速率相差约 14.6 倍，说明当前并不是严格相同的事件计数口径：

- Titan 对 trade、增量深度、markPrice 和 bookTicker 的不同 WebSocket 推送进行统计；同一 WS frame 内的多个价格档按同一 ingress 时间去重。
- Nautilus 数据描述为 OrderBook 批次，可能进行了时间窗口聚合、消息合并或只订阅深度流。
- 两次测试并非同一时刻进行，Binance 实际行情活跃度可能不同。

因此，当前数据适合评价“各自测试配置下的端到端表现”，但不适合直接推导两套引擎的最大吞吐差异。

## 5. 综合评价

### 延迟

Titan 在本轮已有的共同指标上占优：p50 约快 5.76 倍，p99 约快 3.12 倍，平均值约快 5.17 倍，最大值约快 2.58 倍。Nautilus 本轮未提供 p99.9，因此不沿用上一轮 p99.9 数据。Titan 优化后的固定布局批量 IPC 基本消除了此前 10–400 ms 的排队型长尾。

### 稳定性

- Titan：32,894 个正式样本，时钟异常为 0，测试程序正常生成结果并退出；connector 日志没有错误或退出 panic。
- Nautilus：60 秒内采集 2,246 个样本，排除 100 个热身样本后统计 2,146 个样本；本次提供的数据未单独报告错误计数。

Titan 之前一次测试曾在策略消费者关闭后记录 MPSC `SendError` panic；本轮没有复现，connector 日志仅包含 Iceoryx 默认配置提示。退出流程仍建议纳入长期稳定性测试。

### 数据完整性

两套系统均使用 REST 1000 档快照初始化 OrderBook，再应用 WebSocket 增量。当前尚未提供以下一致性验证数据：

- 本地 best bid/ask 与交易所快照的周期性校验结果
- update ID 连续性及断线重建次数
- 丢批、重复批和乱序批统计
- 长时间运行后的 OrderBook checksum 或档位差异

因此，本报告只能确认延迟和短期测试完成状态，不能据此证明两套本地 OrderBook 在长时间运行中始终完全一致。

## 6. 当前结论

在本次各自测试配置下，Titan 的观测端到端延迟显著低于 Nautilus：

- Titan p50：53.969 µs；Nautilus p50：310.663 µs。
- Titan p99：656.760 µs；Nautilus p99：2.050 ms。
- Titan 平均值：75.787 µs；Nautilus 平均值：392.071 µs。
- Titan 最大值：2.023 ms；Nautilus 最大值：5.214 ms。
- Nautilus 本轮未提供 p99.9，不能与 Titan p99.9 直接比较。
- Titan 在高于 Nautilus 当前批次采集速率的事件输入下，仍将 p99 保持在约 1 ms。

但由于订阅内容、采样单位、测试时段和时长不同，当前结论应表述为“Titan 在现有观测数据中领先”，而不是严格同条件下已经证明 Titan 必然快于 Nautilus。

## 7. 建议的正式对照测试

为形成可用于技术选型的最终结论，建议下一轮采用完全一致的 A/B 条件：

1. 在同一台服务器、同一时段同时运行 Titan 和 Nautilus。
2. 都只订阅 `BTCUSDT` 同一种增量深度 stream，不混入 trade、markPrice 或 bookTicker。
3. 都使用 REST 1000 档初始化，并记录 snapshot 完成时间。
4. 延迟起点统一为 WebSocket frame 完整到达用户态的时刻。
5. 延迟终点统一为策略回调入口，并按每个 WS frame 只记录一个样本。
6. 统一使用 30 秒预热、至少 10 分钟正式采样，连续运行 5 轮。
7. 同时记录 p50、p90、p95、p99、p99.9、max、mean、CPU、RSS、丢批数、队列最大深度和 update ID gap。
8. 定期比较两套系统的 best bid/ask 和指定深度档位，确保低延迟不是通过漏处理行情获得。

完成上述对照后，才能同时评价延迟、吞吐、资源消耗和 OrderBook 正确性。
