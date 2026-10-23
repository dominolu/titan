# Numba 网格全面复核与修复

日期：2026-10-22。对照 `/Users/dominolu/dev/gate_arb` 的实际服务、领域模型及回归测试，复核 Titan 网格的成交、撤单竞态、风控和恢复。发现的问题已直接修复。全部编译、格式检查、静态检查和测试均在 `192.168.3.88` 执行；未连接真实交易账户、部署或启动实盘。

## 结论和版本边界

策略升级为 `grid 1.1.0`，state schema `2`。增加逐订单消费水位、有限确认等待、减仓优先、停止原因和平台已对账订单结果的恢复接口。原有 `1.0.0` / schema `1` checkpoint 不做隐式迁移；旧实例应先用原制品完成停止，再按平台流程新建新版实例。同一版本恢复仍要求制品、参数、绑定及 checkpoint 身份匹配。

首版的 [77 项 SDK 验证](../grid/README.md) 保留为历史；本报告使用本次修复后的新证据。先将 18 个针对性场景放到复核前代码上运行，得到 16 个失败、2 个错误，证明原测试集未覆盖这些问题，见 [修复前结果](before.txt)。修复后 SDK 全量 105 项通过，其中新增的排列场景逐一检查 144 种消息顺序。

## 问题与修复

| 范围 | 发现的问题 | 修复后的行为及验证 |
| --- | --- | --- |
| 完全成交与撤对手单 | 等持仓推送后才发撤单，对手单继续暴露 | 完全成交立即进入重置并撤对侧；重新报价仍等待独立成交及持仓确认。原生回调也覆盖持仓滞后 |
| 成交去重 | 用账户全局序号忽略未知订单成交，可能吞掉另一订单的迟到成交 | 逐订单累计量消费；最近 128 个终结意图保留已消费量及方向。未知、超出已消费量或缓存之外的成交进入对账，不猜测归属 |
| 累计量与终态 | 回退的终态累计量可能释放尚未消费的成交和风险预占 | 订单已报告量、已消费量分别单调；终态不得重开或冲突。保留引用直至独立成交完整；较旧的有序号消息不会覆盖新水位 |
| 锚点竞态 | 较早订单的迟到成交可能把较新的锚点回退 | 用权威成交 account sequence 限定锚点推进。无权威顺序时按可靠回调交付顺序；不编造跨订单时间顺序 |
| 等待无上限 | 部分成交已报告但 Fill 缺失、持仓投影缺失、IOC 受理无终态可能永久等待 | 分别记录缺口起点；`order_timeout_ns` 到期对账。行情过期、账户未就绪也不能无限延后期限 |
| 撤单超时 | 更新订单 `updated_ts_ns` 可能不断重置撤单超时 | 从本策略发送撤单的时间计时；明确拒绝后退避重试，尝试次数有上限。撤单受理不会释放订单 |
| 保证金与减仓 | 低/负可用保证金时已确认库存的减仓也被开仓预算拦截，且普通网格减仓缺少 reduce-only | 优先处理减仓，用未被反向单预占的库存确定数量并设置 reduce-only；该部分不要求开仓保证金。中性反转预算不足时可只减库存 |
| 尾仓与保留旧单 | 发现不可交易尾仓前可能先增仓；跨锚点补单可能穿过本策略仍被预占的反向单 | 先检查减仓侧，最小规则不满足则停止新增风险；同时检查新旧锚点订单价格，阻止自交叉报价 |
| 风险停止 | 人工 `cancel` 留仓选项也可能用于止损；风险停止的 checkpoint 可能恢复报价 | 人工、风险、边界分别记录原因。风险停止始终进入受保护的 reduce-only 平仓；风险/边界完成不会按人工留仓恢复 |
| 名义敞口 | 只按区间上界估值，行情越过上界后可能低估风险 | 按上界、当前卖一及候选价的最大值估值；当前持仓超预算触发风险停止。乘法采用受限或浮点估值，避免整数溢出 |
| 接管与账本 | 超限接管可能先执行成本乘法；私有库存账本缺少净数量一致性校验 | 在成本运算前核对方向、持仓上限、价格范围及绑定；初始库存与网格库存之和必须匹配预期净仓 |
| IOC 无进展 | 连续零成交过期可能无限补单 | 初始化与退出的零进展终态累计计数，达到 `max_rejections` 失败关闭；真实成交进展才重置计数 |
| 证明式恢复 | 停机期间已确认撤单的引用因不在活动单里而被误拒；策略拿不到公共层已对账的终态证据 | 新增显式 `RECOVERY_ORDER_RESULTS` 能力与只读 `ctx.recovery_orders()`。仅接受平台已验证的身份、累计量和结果；缺证据或新增未消费成交仍拒绝。恢复回调不发命令、下单门保持关闭 |
| 原策略功能遗漏 | 中性库存约束和创建时资金换算后固定数量未被原样表达 | 增加 `neutral_mode=inventory_only`、`size_mode=allocated_fixed_qty`，后者在创建盘口按 `资金×杠杆/格数/价格` 换算后冻结；动态 allocated 仍是另一模式 |
| 最小名义金额 | 将最高价对应的最小数量乘最低价，错误拒绝有效动态金额配置 | 按实际价量关系验证；必要时遍历商值区间，有明确计算预算，不扫描巨大价格区间 |

实现入口：[策略](../../../../strategies/grid/strategy.py)、[使用说明](../../../../strategies/grid/README.md)、[源策略兼容参数](../../../../strategies/grid/parameters.gate_arb.json)、[业务回归](../../../../python/titan-strategy-sdk/tests/test_grid.py)、[原生 ABI 回归](../../../../python/titan-strategy-sdk/tests/test_grid_native.py)。

恢复能力使用既有 Recover 回调的 payload，长度单位为字节，内容为连续只读订单视图；未改 ABI 结构或放宽指纹检查。编译器要求显式能力和 Recover 回调，旧宿主拒绝未知能力。负载长度不合法、在错误回调中访问等情况有原生测试。账户事实重建和对账仍由 Rust 平台完成。

## 本次验证

| 检查 | 实际结果 | 证据 |
| --- | --- | --- |
| Rust 格式 | `cargo +1.94.1 fmt --all -- --check` 通过 | [格式](fmt.txt) |
| Clippy | 全 workspace / all-targets / all-features，`-D warnings` 通过 | [Clippy](clippy.txt) |
| Rust 回归 | 全 workspace / all-targets / all-features，512 passed、0 failed、12 ignored | [Rust](rust-tests.txt) |
| Rust 文档测试 | 2 passed、0 failed、1 ignored | [文档测试](doctests.txt) |
| SDK 回归 | 105 passed | [SDK](sdk.txt) |
| 消息排列 | 24 种单边成交/终态/撤单/持仓排列 + 120 种双边成交/终态/持仓排列；核对净仓、毛收益、计数及锚点 | SDK 中 `test_review_execution_order_position_permutations_preserve_business_facts` |
| AOT 制品 | 默认参数和源策略兼容参数均编译并通过 Rust loader 校验 | [默认编译](grid-build.txt)、[默认校验](artifact-validation.json)、[兼容编译](gate-arb-build.txt)、[兼容校验](gate-arb-validation.json) |
| 原生回调 | 真实共享库覆盖成交、撤单、低保证金减仓、恢复终态证明、非法恢复负载和停止 | SDK 中 `TestGridNative` |
| 真实宿主 | 原生运行时验证已证实撤单可恢复、缺结果拒绝、新成交拒绝；恢复时门保持关闭。停止验证撤单确认 → reduce-only 退出 → 独立成交 → 权威零持仓 | [宿主探针](host-smoke.txt) |
| CLI 完整轨迹 | COMPLETED，7 事件、3 下单意图、1 撤单意图 | [运行](cli-run.txt)、[结果](cli-result.json) |
| CLI 截断轨迹 | 停止未完成时明确失败，不生成成功结果 | SDK 中 `test_cli_trace_drains_and_rejects_incomplete_stop` |

两个 bundle 都是编译服务器生成的 Linux x86-64-v2 原生制品，示例参数且未签名；[默认 manifest](artifact-manifest.json)、[兼容 manifest](gate-arb-manifest.json)、[文件 SHA256](artifact-checksums.txt)。源兼容参数已验证能够编译和加载，其实际初始仓位不适用默认参数的固定事件轨迹；相关数量/方向语义由业务回归覆盖。

## 源码身份与复现

- 本地基线 HEAD：`2aafc598b0ac43d40a2852cae26a16aed09d9408`；保留已有未提交改动，验证对象为工作区快照，不能仅用 HEAD 代表本次源码。
- 远端新建隔离目录：`/home/dominolu/dev/titan-grid-review-20261022-2aafc598`，未覆盖服务器原工作区。复用服务器虚拟环境和构建缓存，并显式设置 `TITAN_STRATEGY_PYTHON`、`TITAN_STRATEGY_SDK`、`TITAN_TEST_BINARY` 和 `PYTHONPATH` 指向本轮目录。
- [输入源码 SHA256](source-sha256.txt)、[远端逐文件核对](source-check.txt)、[版本及工作区](source-version.txt)、[gate_arb 对照输入身份](gate-arb-input.txt)、[工具链](toolchain.txt)。本证据目录自身不计入输入摘要，避免递归；未同步源项目凭据和运行数据。
- [最终构建/SDK/制品/宿主/CLI 复验脚本](final-verify.sh)。Rust 格式、Clippy 和全量测试命令见上述表格，在同一目录执行。脚本使用新的 `TITAN_HOME`，复跑时应另选未使用的目录。

## 验收边界

这些测试验证策略状态机与执行一致性，不是收益回测或实盘验收。支持范围仍是单账户、单品种、线性合约 NET 模式。手续费、资金费、净权益、交易所实时规则和最终账户风控由平台管理，策略指标为毛收益。现货库存分配、双向仓位、Web/通知、热调参及自动设置账户杠杆未纳入此原生组件。真实部署需要目标品种单位和账户配置重新编译，再走独立的实盘授权与验证流程；12 项 ignored 测试未统一开启。
