# EventEngine实现任务分解与验收记录

状态：实现完成

对应方案：[EventEngine独立技术实现设计](event_engine_technical_design.md)

实现位置：[`crates/titan-event-engine`](../crates/titan-event-engine)

## 1. 任务分解

### 1.1 基础模型与容量边界

- [x] EventDescriptor、EventHeader、PublishRequest和数字事件类型注册；
- [x] SmallEvent、MarketBatch、Snapshot三个固定Pool；
- [x] generation检查、引用计数、最后引用回收和Slot退休；
- [x] 有界Critical/Market MPSC入口；
- [x] 直接复制发布和MarketBatch reserve/commit零复制发布；
- [x] 配置反序列化及启动前容量、水位、budget和affinity校验。

### 1.2 路由与Subscriber隔离

- [x] 实现PluginEngine `EventControl`契约；
- [x] RouteTransaction暂存、safe point提交、陈旧版本拒绝和订阅退休；
- [x] 按数字事件ID预编译路由索引和routing key过滤；
- [x] 每Subscriber固定容量Channel和独立回调线程；
- [x] ActivationGate打开前不执行Handler；
- [x] Subscriber异常和panic隔离。

### 1.3 EventLoop调度

- [x] Critical、pending、Market、Timer固定处理顺序；
- [x] 每类同时使用条数和墙钟时间budget；
- [x] 大扇出按`max_fanout_per_step`保存游标并分步执行；
- [x] Dedicated和SpinSleep两种IdlePolicy；
- [x] 固定容量Timer队列和有界TimerSignal；
- [x] 固定源序号表、重复抑制和sequence gap通知。

### 1.4 背压与恢复

- [x] Market不能侵占`critical_reserve`；
- [x] Latest覆盖、BestEffort丢弃和ReliableOrdered失效处理；
- [x] 每Subscriber与全局双重有界pending dispatch；
- [x] 共享pending配额及保留容量核算；
- [x] pending FIFO重试、满载和超龄终局处理；
- [x] pending跨Subscriber持久游标round-robin，避免低token长期占满重试budget；
- [x] `NORMAL/LAGGING/PENDING/RECOVERING/RESYNC_REQUIRED/FAILED/STOPPED`状态；
- [x] 缺失sequence min/max记录及`recovery_sequence`截点校验；
- [x] RuntimeHealth权威状态和有界FaultSignalRing。

### 1.5 生命周期、诊断与观测

- [x] EventEngine异常捕获、权威故障状态和Subscriber清理；
- [x] Subscriber原子admission gate、终态不可逆和失败入队竞态清理；
- [x] 恢复前旧Handler/EventLease quiescence校验及Latest/Critical逻辑FIFO屏障；
- [x] Plugin发布完整source、sequence、timestamp、routing key和flags元数据；
- [x] PluginEngine先后顺序由`TitanCoreRuntime`统一编排；
- [x] 停止时排空Ingress、pending和Subscriber Channel并验证Arena归零；
- [x] Subscriber队列深度、pending深度、未释放Handle和保守最老年龄；
- [x] 冷路径Top-K及按`pressure_scan_budget`持久游标增量诊断API，不在EventLoop执行O(N)扫描；
- [x] 发布、出队、pending、投递和Subscriber接收五阶段有界Trace Ring；
- [x] 无运行期分配的对数延迟直方图，输出P50、P99、P99.9和最大桶值；
- [x] Ingress、pending、Arena、调度、重同步、Trace和Fault指标快照。

### 1.6 测试与性能工具

- [x] 配置、Arena回收、Pool隔离、零复制和代际测试；
- [x] 多Publisher、扇出、routing key、FIFO和事务测试；
- [x] `critical_reserve`、pending满、pending超龄和恢复测试；
- [x] Critical持续负载下Market和Timer公平性测试；
- [x] Subscriber失败、queue/pending引用回收、Fault Ring满载和RuntimeHealth测试；
- [x] 退订停止新路由后排空pending，Engine shutdown排空Ingress/pending/Channel测试；
- [x] Release/Acquire发布、最后引用回收、失败入队admission、满槽竞争和pending所有权竞争的loom模型测试；
- [x] 可独立运行的吞吐及定速负载benchmark；
- [x] 全工作区回归测试。

## 2. 验收命令

```bash
cargo fmt --all -- --check
cargo test -p titan-event-engine --all-targets
DYLD_LIBRARY_PATH=/opt/homebrew/opt/python@3.11/Frameworks/Python.framework/Versions/3.11/lib \
  cargo test --workspace --all-targets
cargo bench -p titan-event-engine --bench event_engine
```

当前工具链未安装可用于Rust 1.94.0的Clippy组件，因此静态验收使用无warning的`cargo check -p titan-event-engine --all-targets`、格式检查和完整测试替代；这不影响其他测试结果。

## 3. 原型性能记录

环境：Apple M1 Pro，10逻辑CPU，Darwin arm64，Rust 1.94.0，release profile。

| 模式 | 事件数 | 目标/实际速率 | EventEngine dispatch P99桶 | Subscriber P99桶 |
|---|---:|---:|---:|---:|
| 最大吞吐 | 1,000,000 | 约1,054,907/s | 134,217,727ns | 134,217,727ns |
| 定速负载 | 500,000 | 500,000/s | 16,383ns | 65,535ns |
| 定速负载 | 500,000 | 799,975/s | 262,143ns | 524,287ns |
| 定速负载 | 500,000 | 949,951/s | 2,097,151ns | 2,097,151ns |
| 审查修复后最大吞吐 | 1,000,000 | 约912,108/s | 134,217,727ns | 134,217,727ns |
| 审查修复后定速负载 | 500,000 | 499,994/s | 32,767ns | 131,071ns |

“审查修复后”数据包含失败投递原子admission gate的正确性成本；定速测试曾受共享主机调度干扰出现更高离群值，表中记录复跑稳定值。最大吞吐测试包含主动灌满队列造成的排队时间，不能作为低延迟SLA。上述Mac共享环境结果只验证benchmark和负载拐点记录能力；正式PerformanceEnvelope仍按技术方案要求在目标Linux、NUMA、cpuset、IRQ隔离和网卡部署条件下冻结，不属于代码实现缺口。
