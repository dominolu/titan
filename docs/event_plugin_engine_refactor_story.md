# Titan 新一代实盘内核：用 EventEngine 与 PluginEngine 重建高频交易基础设施

在量化交易系统里，真正困难的往往不是“接上一个交易所”，而是让行情、账户、策略和执行在持续高负载、网络抖动与版本升级中，仍然保持低延迟、可恢复、可验证。

Titan 最近完成了一轮核心架构重构：以 EventEngine 统一事件数据面，以 PluginEngine 统一插件控制面，再由 TitanCoreRuntime 管理两者的启动、激活、替换和关闭。行情、账户、策略和执行能力由此进入同一条受约束的运行链路。

这不是一次简单的模块拆分，而是把“能运行的交易程序”升级为“可度量、可隔离、可演进的交易运行时”。

## 为什么要重构

传统交易框架常常从一条直接回调链开始：WebSocket 收到行情，调用策略；策略发出指令，再调用连接器。早期这样做足够直接，但系统扩展后会逐渐出现几个结构性问题：

- 快行情、账户事实和控制命令共享执行路径，一个慢消费者就可能扩散延迟；
- 无界队列把过载转化为内存增长，直到问题以更危险的方式出现；
- 动态加载只是“把代码装进进程”，缺少 ABI、权限、生命周期和资源回收约束；
- 重连后的快照与增量容易产生时间窗口，订单和仓位可能在恢复阶段失去一致性；
- 平均延迟看起来正常，但 P99.9 尾延迟、丢弃和积压无法被准确观察；
- 每增加一种业务能力都要修改主程序，插件故障边界与 Core 耦合在一起。

Titan 的新版本把这些问题拆成两个明确的职责域：EventEngine 负责事件如何安全、高效地流动，PluginEngine 负责能力如何被验证、装配和管理。

## EventEngine：低延迟不再以失控为代价

EventEngine 是 Titan 的统一事件数据面。它不只追求快，更强调在容量边界内给出确定行为。

### 预分配内存与明确所有权

事件首先写入固定容量的多级 EventArena。句柄带 generation 校验，事件只有在最后一个消费者释放引用后才归还池中。大型 MarketBatch 可以由获授权的插件直接 reserve/commit 到 EventArena，减少热路径复制与临时分配。

这意味着内存压力不再被隐藏：容量、引用和回收都有明确边界，Arena 耗尽也会成为可观测的运行状态，而不是悄悄演变为进程失控。

### 快路径与可靠路径统一建模

高频行情通过 FastLane 低成本投递，可选择固定成本的 inline 投影，或由有界队列驱动的异步 worker。账户、订单等关键事实则使用带可靠性语义的 Primary/Async lane。

每条 lane 都拥有独立队列、worker、健康状态、水位和恢复代际。不同消费者互不拖累，一个审计或指标消费者变慢，不会直接阻塞策略主链。

对于关键事件，EventEngine 提供有界 pending、FIFO 重试、source sequence gap 检测和 `RESYNC_REQUIRED` 截止。对于行情类事件，则可以按 `LATEST`、`RELIABLE_ORDERED` 或 `BEST_EFFORT` 等 QoS 选择符合业务目标的过载行为。快路径是一种交付方式，QoS 是可靠性策略，两者不再混为一谈。

### 原子路由与可恢复状态

动态订阅不会直接修改正在运行的路由表。EventControl 将路由变更编译为事务，并在 EventLoop safe point 提交，避免消费者看到半完成的配置。

内部 SnapshotBarrier 能在恢复期间暂存增量、接入权威快照，并按新的 generation 提交候选状态，使“先订阅还是先拉快照”的经典竞态进入正式协议，而不是依赖各业务模块自行猜测时序。

### 尾延迟成为一等指标

EventEngine 内置无分配的对数延迟直方图，持续记录 P50、P99、P99.9 和最大值，同时暴露 queue drop、最大队列深度、resync 与 Arena exhaustion。

在当前目标机上，release 模式以默认容量连续执行三轮 100 万事件、30 万 events/s 测试，吞吐达到 299,993–299,999 events/s，且 drop、resync、Arena exhaustion 均为零。当前冻结门槛为：

- dispatch P99.9 不高于 8.39 ms；
- subscriber P99.9 不高于 16.78 ms。

门槛与硬件、容量和负载契约绑定。Titan 不用一次实验结果承诺所有环境，而是把性能基线版本化，让后续优化和回归有据可查。

## PluginEngine：插件化不只是动态加载

PluginEngine 是 Titan 的统一控制面。它负责回答三个问题：一个插件是否可信，它何时可以对业务可见，以及停止后是否真的释放干净。

### 从静态耦合到动态能力装配

业务能力已从 Titan CLI 和 Core 的静态注册中移出，由独立动态插件包提供标准 Factory。Core 只依赖统一能力接口，不感知具体业务实现。

新增或替换能力不再要求修改 EventEngine、PluginEngine 或 Titan main。插件成为可独立构建、验证和发布的能力单元，Core 则保持稳定。

### 加载前完成兼容性与供应链校验

插件包进入运行时前，需要通过 package manifest、SHA-256、ABI、schema/config version、capability 和 Core Runtime API 校验。动态库内部声明与外部包清单不一致时，候选插件会在建立进程级 code lease 之前被拒绝。

这让“能被加载”与“允许被运行”成为两件不同的事，也为自动部署和第三方插件留下了清晰的安全边界。

### ActivationGate：先装配完整，再一次开放

Service Endpoint 和 EventEngine RouteTable 属于不同组件，无法依靠一次 CPU 原子操作同时切换。Titan 使用 PluginPlan、预绑定 endpoint generation、路由事务和共享 ActivationGate 实现业务层面的原子激活：

```text
校验配置与能力
  → 编译不可变 PluginPlan
  → 创建 PluginSlot 与 ResourceScope
  → 发布 gated endpoint
  → 提交候选路由
  → 启动 suspended worker
  → 打开 ActivationGate
```

Gate 打开前，Publisher、Service 和 Handler 都不能处理业务。任一步失败，系统保持 Gate 关闭，并按相反顺序撤销路由、端点和资源，因此不会暴露“服务已经可调用，但事件订阅尚未就绪”的半激活状态。

### ResourceScope 与代码租约

每个插件实例拥有独立 ResourceScope。线程、定时器、订阅、Service registration、Endpoint lease 和 Event lease 都登记在作用域中，并在失败回滚或停止时逆序释放。

动态库还受 code lease 保护：只要仍有线程、服务调用或事件引用依赖该代码，运行时就不会卸载动态库。这解决了动态插件体系中最危险的一类问题——代码已被卸载，但回调或对象仍然存活。

在热路径上，插件只持有预绑定的 ServiceHandle、EventPublisher 或路由句柄，不需要查询 PluginRegistry 或 ServiceRegistry。控制面的灵活性不会被带进逐笔事件路径。

## 两个引擎如何协同

EventEngine 与 PluginEngine 不是两个平行模块，而是一套完整的提交和生命周期协议：

```text
Source / Provider
  → Producer Plugin
  → EventEngine Primary Async lane / FastLane
  → Consumer Plugin
  → Pre-bound ServiceHandle
  → Provider
```

PluginEngine 决定哪些插件、服务和订阅可以被激活；EventEngine 保证激活后的事实按既定 QoS、容量和顺序投递。TitanCoreRuntime 先启动 EventEngine，再装配 PluginEngine；关闭时先停止插件和新命令，退休订阅并等待 handler 退出，最后排空 EventEngine，并验证 EventArena 引用归零。

结果是：数据面保持固定成本和可观测，控制面保持可替换和可回滚，两者通过 ActivationGate、RouteTransaction 和 ResourceScope 建立明确边界。

## 这次改造的重要意义

对业务开发者，新的内核把事件传递、故障恢复和生命周期从业务代码中剥离。插件只需要实现标准能力契约，不再各自维护队列、重连窗口和启动顺序。

对插件开发者，工作重点从“接进主程序”变成“实现标准 Factory 和能力契约”。Manifest 描述权限与版本，PluginEngine 管理装配和释放，EventEngine 提供一致的数据投递与可观测性。插件可以更独立地开发、验证和发布。

对运维人员，系统不再只有“进程活着”这一种健康信号。队列深度、尾延迟、丢弃、恢复代际、订阅者健康、资源释放和账户 reconcile 都成为可检查的运行事实。

对架构演进而言，最重要的变化是控制面与数据面真正解耦。事件模型和性能边界可以独立优化，插件类型和部署节奏也可以独立演进；双方只通过稳定契约协作。新能力不再持续侵入 Core，核心升级也不必迫使所有业务模块同时改造。

对故障控制而言，有界容量、独立 lane、ActivationGate 和 ResourceScope 把异常限制在明确范围内。慢消费者、插件启动失败、配置替换失败或资源回收异常，都有确定的状态变化和回滚路径，而不是依靠进程重启掩盖问题。

对性能工程而言，延迟优化从经验判断变为可重复验证。P50、P99、P99.9、最大值、队列深度和丢弃指标共同描述系统行为，使容量规划、硬件选型和版本回归建立在同一套证据上。

## 现在的 Titan

这次重构没有试图用一个更复杂的抽象包装所有问题。相反，它把关键边界变得更清楚：

- EventEngine 对事件容量、顺序、延迟和恢复负责；
- PluginEngine 对能力校验、装配、激活和资源生命周期负责；
- 业务插件只实现自身领域能力，不接管 Core 生命周期；
- 消费者只处理标准事实，并通过预绑定服务提交意图。

Titan 因此获得了一条更适合长期演进的实盘主链：插件可以扩展和替换，事件可以度量，故障可以隔离，恢复可以验证。

这正是高频交易基础设施从“能跑”走向“可信”的关键一步。

## 延伸阅读

- [Titan 项目概览](../README.md)
- [EventEngine 实现说明](../crates/titan-event-engine/README.md)
- [PluginEngine 实现说明](../crates/titan-plugin-engine/README.md)
- [Core Runtime 交互契约](core_runtime_contract.md)
