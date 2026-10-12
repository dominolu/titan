# Titan 系统重构与验证清单

基线提交：`741d2de56f652855a3e5848e523f312b014dc15f`。开始日期：2026-10-12。

本轮统一事件发布、账户事实、恢复、服务生命周期、策略执行和结果协议，减少跨模块重复状态与分支。所有构建、格式检查、静态检查、测试、AOT 制品生成和性能验证均在编译服务器 `192.168.3.88` 执行。本轮不接入真实账户，不部署或启用实盘策略。

## 执行顺序与完成条件

### 1 事件发布与路由

- [x] E1 将复制发布和 reservation commit 汇入同一提交入口。
- [x] E2 在任何 PRIMARY/FastLane 消费者可见前完成全局发布准入；拒绝的事件不得部分投递。
- [x] E3 统一各消费路径的 source sequence 检查、故障计数和发布元数据；保留可靠有序与行情 Latest 的不同 QoS。
- [x] E4 停机关闭发布准入并等待已进入的发布操作结束，防止尾部事件越过关闭边界。
- 验收要求：复制与 reservation 两条 API 均覆盖 ingress 满、重复序号、并发发布、停止竞态、内存 lease 回收；现有 PRIMARY、FastLane、可靠重试和 SnapshotBarrier 回归通过。

### 2 账户投影与快照

- [x] A1 集中管理账户订单、持仓、余额、epoch 与各投影提交版本，快照内容和版本在同一个提交边界读取。
- [x] A2 REST bootstrap 和私有流事实复用投影更新入口；消除重复 OrderChanged 构造与整数组反复复制。
- [x] A3 保留订单字段与明确的终态规则，规范累计成交去重和快照缺失订单的处理。
- [x] A4 增加余额事实入口，更新交易所私有流中的余额；保留各 venue 的字段解析和授权方式。
- 验收要求：快照与更新并发、不同投影版本、重复成交、非零初始仓位、余额更新和终态订单均有回归测试。

### 3 账户恢复与候选替换

- [x] R1 首次启动、重连和重新对账使用同一恢复流程：暂停准入、暂存增量、查询 REST、合并、提交、READY。
- [x] R2 暂存事实使用有界容量，超时或溢出保持 Invalidated，不能凭 socket 连接成功恢复 READY。
- [x] R3 处理断线期间终态订单、成交和余额变化；推进恢复 epoch，明确必须重新创建策略的恢复边界。
- [x] R4 候选账户可以在对外 publisher 关闭时完成本地 bootstrap；提交替换后统一发布快照与 READY。
- 验收要求：模拟断线遗漏事实、恢复失败、暂存溢出、候选替换失败回滚与成功切换；不得调用真实交易所。

### 4 生命周期与公共组件

- [x] L1 复用有界操作历史、deadline 等待、错误汇总与资源关闭；行情、账户、策略保留各自领域生命周期。
- [x] L2 全局停机 deadline 贯穿 Strategy、Execution、Account、Market、EventEngine；停止失败不得伪报成功。
- [x] L3 状态查询不承担停止超时推进；生命周期推进发生在执行或调度入口。
- [x] L4 限制运行中依赖组件替换，防止策略继续持有旧 generation 的市场或执行绑定。
- 验收要求：超时、重复停机、失败清理、过期 handle、操作历史容量和依赖替换覆盖完整。

### 5 V13 运行与双腿执行

- [x] V1 区分快照 generation 与新运行 generation，覆盖 Service → Factory → Instance 完整恢复。
- [x] V2 合并订单事实更新入口，活动视图与持久化历史保持一致；启动合并状态结束后释放。
- [x] V3 命令批次先校验、预留本地容量和登记，再执行；逐腿请求结果继续收敛，不能因第二腿本地拒绝遗漏第一腿事实。
- [x] V4 停机等待订单与成交事实收敛；策略风险缺口无法处理时返回 ABI 错误，最终 checkpoint 必须获得写入确认。
- [x] V5 checkpoint 使用独立周期和 dirty 标记；定义历史保留边界，限制全量复制成本。
- [x] V6 实盘与离线适配复用 callback/context/staging 入口，保持不同执行后端的撮合语义。
- [x] V7 pair_arb 容纳 Taker-Taker 双腿任意成交顺序；集中终态清理，保持 Maker-Taker、撤单确认后重挂和恢复校验。
- 验收要求：完整重启、Order→Fill 停机窗口、执行器饱和、双腿任意顺序、撤单拒绝、checkpoint 写失败、AOT 回调 smoke。

### 6 连接器转换与调度

- [x] C1 共用定点单位与完整 client namespace 的订单元数据 ID；清理拒绝请求的跟踪状态，保留交易所 wire、签名和枚举解析差异。
- [x] C2 清理发布端永远成功的 Result 兼容语义，集中 native 与 FeedBatch 的公共编码。
- [x] C3 策略 timer 和 supervisor 共用调度线程，保留独立 PRIMARY lane；统一线程取消与 join。
- [x] C4 `timer_capacity` 用于控制队列，`max_commands_per_callback` 独立设置；`checkpoint_interval` 独立于 timer 周期。账户、行情和执行 I/O runtime 继续按服务隔离，由各自资源 scope 关闭。
- 验收要求：三家 venue 的解析、wire、重连及共享调度取消测试；变更热路径时在编译服务器重测延迟基线。

### 7 验证后运行计划与监控

- [x] P1 controller 与 worker 使用同一份验证后配置；启动前核对配置摘要、artifact 摘要和策略身份。
- [x] P2 两个实盘入口复用信号处理、运行监控和停机流程。
- [x] P3 区分进程存活与组件健康，记录 Failed/Invalidated/Paused，不让 worker 存活掩盖交易链路故障。
- 验收要求：校验后配置变化、制品替换、组件故障、SIGTERM、owner token 与 worker 退出状态。

### 8 结果与报告

- [x] B1 所有后端共用 ResultBundle writer，最后提交 manifest，记录真实的制品与配置身份。
- [x] B2 Rust/Python reader 使用同一 v1 协议；returns 等缺失数据使用明确空值，不重算交易结果。
- [x] B3 CLI trace 回放及实盘停止结果均能进入 report，校验文件大小、摘要和路径。
- 验收要求：run → report 完整链路、输出损坏、未提交 manifest、空收益序列和 Python reporting 回归。

### 9 数据采集

- [x] D1 无界队列改为有界写入队列，将同步压缩移出 async 接收循环。
- [x] D2 停机先终止采集再排空写入；明确返回写盘和 gzip finish 错误。
- [x] D3 文件轮转和同日重启不覆盖已有采集数据。
- 验收要求：慢写盘、队列背压、写入失败、轮转、同日重启、退出排空和压缩文件可读性。

## 编译服务器验证门禁

- [x] 确认远端路径、版本或源码摘要、工作区状态；在独立目录验证，不覆盖已有服务器代码。
- [x] 固化本次源码摘要、工具链、测试命令与原始日志。
- [x] `cargo fmt --all -- --check`。
- [x] `cargo check --workspace --all-targets --all-features`。
- [x] `cargo clippy --workspace --all-targets --all-features`；区分既有 warning 与新增 warning。
- [x] `cargo test --workspace --all-targets --all-features`；真实账户测试保持 ignored。
- [x] Python Strategy SDK 与 reporting 完整测试。
- [x] V13 策略 AOT 构建、artifact smoke、pair_arb smoke 和 CLI 回放报告集成验证。
- [x] `scripts/check_single_runtime.sh`。
- [x] 更新验证结果和所有未完成项，不能将实现完成与验证通过混为同一状态。

## 验证记录

初始本地工作区干净。编译服务器原有 `/home/dominolu/dev/titan` 不含 `.git`，未覆盖或修改。本轮源码及验证均在 `/home/dominolu/dev/titan-refactor-20261012-741d2de5`；完整日志位于其 `verification-artifacts/final/`。

复验入口：`bash scripts/verify_system_refactor.sh`。脚本检查服务器 IP，拒绝在本地执行。验证顺序是格式 → check → clippy → Rust 测试及 doctest → SDK/reporting → 单一运行时检查 → AOT 制品 → 四个原生场景 → CLI 回放、报告、损坏文件拒绝和 SIGTERM。最终结果、工具链和源码摘要见 [验证报告](verification/2026-10-12/refactor_validation_report.md)。

## 实现边界与兼容性

- 新的 `max_commands_per_callback` 默认 64、`checkpoint_interval` 默认 1 秒；已有配置无需补字段。SDK 回调可返回 `None` 或字面量 ABI 状态码 `0/-1/-2/-3/-4`，C ABI V13 布局和 fingerprint 不变。
- 账户投影使用每个资产的有符号净持仓；同一资产返回多条 position side 时会拒绝 bootstrap，避免覆盖或遗漏敞口。本轮没有更改账户交易模式。稀疏持仓/余额增量保留已知的 REST 字段。
- 恢复仍严格验证 artifact、binding、state checksum 与 `public_state_identity`。公共事实或序列边界不一致时保持失败，不自动迁移旧私有状态；重连不会自动恢复原策略交易准入。
- 订单终态历史保留 1,024 条；策略历史保留约 4,096 条终态加活动订单，辅助 ID 表跟随保留集合回收。运行中的订单和未收敛成交不因历史清理丢弃。
- 替换候选账户在提交前禁止下单，也不执行取消旧账户订单的停机策略。绑定中的账户/行情替换须先释放策略资源。提交前失败保留旧 generation；提交后失败明确进入 Failed。
- 验证未接入真实账户。12 项实盘测试保持 ignored；实盘部署、真实账户对账和最小风险下单/撤单不属于本轮执行范围。


最终验证：Rust workspace 427 项通过，12 项真实账户测试 ignored；doctest 2 项通过、1 项 ignored；Python SDK 27 项及 reporting 4 项通过。格式、check、clippy、全部原生 smoke 和 CLI 集成均通过。Clippy 的非阻断提示保留在原始日志中，未宣称 `-D warnings` 清零。
