# 策略研究工作区与策略实盘归并

日期：2026-10-26。按用户确认的方案，一级导航收敛为**运行总览、策略研究、账户风险**；参数版本、编译产物、回测、报告及策略实盘统一归入每个策略工作区。沿用[既有工作台视觉](../../../design/management-console-v0.1/README.md)及[前一批页面能力](../management-console-pages/README.md)。

## 功能与职责

| 入口 | 保留与整合的功能 |
| --- | --- |
| 策略研究目录 `/research` | 服务端授权目录、稳定策略身份、参数预设与记录数量、分页及策略实盘入口 |
| 版本与参数 | 选择当前策略预设、JSON/表单编辑、精确整数、校验、草稿保存/编辑/复制、冻结、版本差异与构建 |
| 构建记录 | 冻结版本选择、真实编译、进度/回执/日志、产物摘要/来源/下载、跳转回测或实盘；跨页保持选定版本 |
| 回测与报告 | 当前策略产物/结果、共享数据准备、历史场景、压力模型、回测、指标/曲线、原始结果、报告生成与隔离预览；从结果进入实盘固定其产物 |
| 策略实盘 | 当前策略产物与授权绑定、部署、启动风险确认/取消、部署记录与运行实例分页、嵌入同一运行详情 |
| 实盘运行详情 | 原有暂停、恢复运行、停止、checkpoint 恢复、订单/持仓、九类事实历史、权益/风险、业务状态、冻结参数/版本、日志、告警和诊断 |
| 公共功能 | 会话、权限、SSE/重同步、原键重试、全局历史与操作查询；工作区内展示关联操作，运行总览与账户风险继续保留 |

工作区路由为 `/research/:researchId?tab=parameters|artifacts|backtests|live`。同策略切换环节保留编辑状态，切换策略路径销毁上下文并丢弃旧响应。原 `/welcome`、`/artifacts`、`/backtests` 保留兼容跳转：单策略直接进入，多策略先选择，不默选另一个策略。全局 `/runs/:id` 仍可访问同一运行详情。

`ResearchWorkspace` 组合已有功能组件，`StrategyLive` 复用 `RunDetail`，没有复制生命周期、恢复、风控或交易账本实现。移除旧 `BuildArtifacts.vue` 的混合职责，编译产物归 `ResearchArtifacts`，部署/启动归 `StrategyLive`。管理状态仍由现有 Store、Operation 与 N/E 回执确认，不新增研究任务、发布状态机、数据库表或服务。

## 策略身份与数据范围

节点模板可配置：

```toml
[node.templates.grid-neutral]
research_id = "grid"
research_name = "网格策略"
source = "../strategies/grid/strategy.py"
parameters = "../strategies/grid/parameters.json"
```

多个参数预设使用同一 `research_id` 即归入同一研究策略。该配置对应工作区 ID `research-grid`，与模板别名、原生包的策略 ID、数值 runtime ID 和运行实例 ID 分别表达。显式配置最多 119 字符，最终工作区 ID 仍受 128 字符上限约束。

未配置时由节点身份和规范化源码路径生成稳定归属，同一源码的预设共享工作区，修改源码字节不改变身份；跨节点或源码路径迁移使用显式身份。产物文件继续保存在所属节点，研究工作区只保存引用和摘要。

新记录沿链传递归属：参数版本 → 编译产物 → 回测 → 报告；编译产物 → 部署 → 运行。旧记录沿原引用和可证明的源码摘要只读解析，不重写冻结记录或摘要。复用模板别名但源码不符时不重新归属；无法确认的保留在“未归属的历史记录”。

- `GET /api/v1/research` 返回授权工作区目录；既有资源查询增加 `research_id`、`resource_id` 条件，**先授权/归属过滤，后分页**。
- 页面使用服务端策略内分页和有限选择，不过滤全局近期缓存来冒充完整目录。资源选择须经服务端确认归属；无效链接先清空选择，不回退到先前或其他策略产物。
- 数据集与账户绑定仍是共享授权目录；只读账户观测不归入策略研究或策略实盘。
- 参数变更不影响已运行实例。部署、启动及恢复继续核对固定身份、权限、账户/品种、风险边界、代次、原期限与 checkpoint；受理不表示完成，普通操作未决不阻断独立停止。
- 原币种、整数精度、数据完整性、过期与缺失提示沿用原规则，界面不重算财务或风险指标。

## 验证环境

所有编译、格式、类型、静态检查和测试在以下独立目录执行，本地仅编辑、审查和核对文件摘要：

```text
192.168.3.88:/home/dominolu/dev/titan-research-fixes-20261026-0099c1fb
源码基线：0099c1fb566331a2ac3d43a8ef0f4b7ac586daba + 本轮源码清单
```

创建前核对上一轮源码清单与新目录不存在，不覆盖服务器既有源码修改。Rust 复用既有 target 与 rsproxy 离线缓存，使用原 Cargo.lock；Numba/报告使用服务器已安装的 SDK Python，前端复用同一 pnpm-lock.yaml 的依赖安装。

该目录从前一轮验收源码创建，四项 review 修复使用新的独立目录，未覆盖旧源码或验收数据。本轮重新编译并冻结、strip 服务器程序，再从该程序导出 DTO 并构建配套前端。M1/M2/M3 均使用该程序；[源码清单](checks/review-fixes/source-sha256.txt)、[服务器核对](checks/review-fixes/source-identity-check.txt)、[本地交付核对](checks/review-fixes/local-source-identity-check.txt)证明 60 个交付文件与验收源码一致。

```text
titan SHA256 = de1103944b7f65ed3536affddb69a531305a186d335d463187b761533d1dbe58
```

M1 使用既有公开 BTCUSDT 历史输入，M2 使用 E 生成的合成账户快照，M3 使用真实原生 worker 与回环模拟交易所。未访问真实交易所账户、未修改实盘服务器、未启动真实策略。

## 验证与证据

最终结果见[验收汇总](checks/review-fixes/summary.json)。以下均是四项修复之后重新执行的结果，未以先前通过记录代替本轮验收。

| 验证 | 最终结果与证据 |
| --- | --- |
| Rust CLI | [50 passed、0 failed、0 ignored](checks/review-fixes/cli-tests.txt)，包含独立研究读取不占用受理锁、完整资源返回与 ID 长度边界；[格式检查](checks/review-fixes/rust-format-check.txt)、[Clippy `-D warnings`](checks/review-fixes/clippy.txt)、[构建](checks/review-fixes/build.txt)通过 |
| 前端门禁 | [类型与格式](checks/review-fixes/web-check.txt)、[ESLint](checks/review-fixes/web-lint.txt)、[生产构建](checks/review-fixes/web-build.txt)、[缓存](checks/review-fixes/cache.txt)及[精度/展示](checks/review-fixes/run-view.txt)通过 |
| 新增 Vue/Pinia 回归 | [8 项通过](checks/review-fixes/research-view.json)：最后选择、新输入、会话/组件销毁、600 条部署事件、资源释放、容量拒绝和迟到读取 |
| 历史查询与受理并发 | [12,000 条模拟历史、每条 32 KiB 元数据](checks/review-fixes/query/results.json)通过；目录 0.347 秒、空策略资源查询 0.346 秒；历史查询期间资源读取与草稿写入最长 0.102 秒；有效工作区 ID 的 128 字符边界可查询 |
| M1 研究闭环 | [9 项流程](checks/review-fixes/m1/results.json)、[22 项浏览器检查](checks/review-fixes/m1/browser/browser.json)、[6 项故障注入](checks/review-fixes/m1/faults.json)通过 |
| M2 账户只读 | [6 项流程](checks/review-fixes/m2/results.json)、[8 项浏览器检查](checks/review-fixes/m2/browser.json)通过 |
| M3 受控生命周期 | [21 项原生模拟流程](checks/review-fixes/m3/results.json)、[9 项浏览器检查](checks/review-fixes/m3/browser/browser.json)通过，含部署、暂停/恢复、崩溃、错误身份拒绝、原期限恢复和停止安全释放 |

所有浏览器 `pageErrors` 为空。本轮仅针对 CLI 管理模块、前端及相关集成流程验证，不把历史全仓库测试数量作为本轮通过数量，也不声明真实交易所已复验。

## 四项审查问题的修复

| 问题 | 修复与回归 |
| --- | --- |
| 迟到的参数复制覆盖最后选择 | `editVersion` 校验请求代次、会话、组件存活及当前编辑输入，等待 `nextTick` 后再次检查目标；连续复制、用户继续输入、退出会话和销毁页面均验证不会被迟到响应覆盖 |
| 固定资源被其他环节覆盖 | Store 按组件所有者合并有界固定集合，`useResourcePins` 在组件销毁时释放；工作区与嵌入运行详情分别持有资源，600 条后续部署事件不能淘汰当前部署，容量超限原子拒绝 |
| 研究查询阻塞控制数据库 | 查询使用独立 WAL 只读连接及阻塞任务线程，轻量投影只保留授权和引用字段，完整资源仅为最终分页加载；无新表或独立研究状态机，原授权和旧记录解析继续通过测试 |
| 研究 ID 前缀导致超长 | 校验最终 ID；119 字符配置产生合法 128 字符身份，120 和 128 字符配置拒绝，避免登记不可查询的工作区 |

审查前的[参数竞态](review/copy-race.json)、[固定资源冲突](review/pin-ownership.json)及[查询阻塞](review/backend-review.json)记录保留用于对照。旧查询探针的并发只读等待约 0.961 秒，新探针同时验证只读和草稿写入，最长 0.102 秒；这是本次服务器与模拟数据的观测值，不是延迟 SLA。

首轮独立编译超时探针曾因未继承研究身份被正确拒绝，修正测试节点配置后重跑，未放宽身份校验。本轮第一次 Rust 原生编译测试遗漏 SDK Python 环境，补齐既有环境后全部重跑通过；未变更依赖版本。

## 实际页面

- [策略研究目录](browser/research-index.png)、[版本与参数](browser/parameters.png)、[构建记录](browser/artifacts.png)、[回测与报告](browser/backtests.png)。
- [受控部署](browser/deployment.png)、[运行详情](browser/running.png)、[暂停](browser/paused.png)、[停止](browser/stopped.png)。
