# Titan 简化命令与后台服务修改清单

状态：已完成并通过本轮非实盘验收。范围来自本轮确认方案：后台程序 `titan`，客户端 `titan_cli`；去掉日常命令的 `console` 前缀；二级命令与交互模式；本机服务自动启动；数据库配置向导；逐层 help。现有管理 API、Operation、账户事实、恢复校验和存储迁移机制继续复用。

## 1. 主入口

```text
titan serve [manager|node] --config <server.toml>
titan_cli [--profile <name>] [--json|--ndjson]
titan_cli check
titan_cli use <profile>
titan_cli strategy [ls|show|start|pause|resume|stop|logs] [target]
titan_cli account [ls|show|balance|positions|orders|risk|fills] [target]
titan_cli research [build|backtest|report] <strategy-or-result>
titan_cli config [database|show|check]
titan_cli help [strategy|account|research|config]
```

无参数且终端交互时进入 shell；无参数非交互输入显示 help 并退出，不挂起。四个分组在交互终端可直接进入，完整命令用于脚本。shell 支持 help、Tab 补全、历史、back/exit，提示符显示 profile、分组与当前目标。退出不停止后台或策略。机器输出模式需明确选择动作。

## 2. 逐项修改与验收

| 编号 | 修改 | 验收要点 | 状态 |
| --- | --- | --- | --- |
| C01 | 客户端/服务拆分 | 双二进制、直接 serve、旧工作区/worker/console 兼容 | 已完成 |
| C02 | 二级命令与完整 help | 根/分组/叶子/全局参数说明及示例；查看帮助不触发认证或启动 | 已完成 |
| C03 | 配置发现、profile 与名称 | 参数/环境/profile 优先级，use 原子保存；ID/alias/唯一名称，歧义拒绝 | 已完成 |
| C04 | 简化策略控制与等待 | 固定代次、默认有界等待、detach、中断、未知任务原身份找回、终态显式重试 | 已完成 |
| C05 | 研究流程 | 新前端真实 Describe/保存/冻结/编译，回测和报告；私有阶段回执，资源快照绑定 | 已完成 |
| C06 | 人读与机器输出 | 有界摘要/表格、控制符过滤、原金融值/缺失语义；JSON/NDJSON 信封不变 | 已完成 |
| C07 | 交互 shell | 分组/目标上下文、补全/help/历史、Ctrl-C/EOF、终端恢复、非 TTY 不阻塞 | 已完成，6 组 PTY 通过 |
| C08 | 本机自动启动 | 4 客户端只启动 1 manager；身份/版本/就绪/锁；远程不降级；不启动策略 | 已完成 |
| C09 | 数据库向导与离线配置 | SQLite 默认，PostgreSQL 仅历史，只保存引用；show 脱敏，check 有界检查 | 已完成 |
| C10 | 存储状态保护 | 候选/生效区分、immutable profile 不覆盖、固定 target、已有管理库缺失拒绝重建 | 已完成 |
| C11 | 文档与兼容迁移 | 主手册展示短命令；维护入口保留，旧验收不重写，架构经验更新 | 已完成 |
| C12 | 服务器最终门禁 | 格式/Clippy/260 项 Rust/13 组新前端/13 组流程/真实 PostgreSQL/契约导出 | 已完成，840 个输入一致且隐私通过 |

详细结果、程序身份、合成环境范围与未覆盖项见[本轮验收](verification/2026-11-03/cli-simplification/README.md)，操作说明见[客户端手册](managed_cli.md)。各矩阵存在包含关系，不简单相加。

## 3. 保持的边界

- 日常客户端访问明确的管理连接；原本机 Registry 保留显式兼容入口，不因同名 ID 自动猜控制通道。
- 可读名称解析后固定部署/运行身份。恢复继续核对原恢复点、代次、期限与准入证据；没有唯一证据时拒绝，不清空业务状态重启。
- 自动启动仅管理器，复用服务前校验身份；不自动启动 node/策略、不因远程失败改连本机。已有启动记录却丢失管理库时拒绝创建空库。
- 普通新命令会只读核对旧 journal。处理中/未知保留原意图，控制终态允许新的显式执行，研究中间成功继续原流水线；指定 journal 永不自动新建。明确拒绝还须原键确认无 Operation，空查询本身不构成未受理证明。
- 管理与恢复库保持本地 SQLite。PostgreSQL 仅历史，校验固定 target；候选与既有数据迁移分别表达，切换继续走原 prepare/校验/activate，不写远端任意路径。
- 保留原有用户变更；不提交、推送、更新外部 Issue 或执行实盘。构建、格式、静态检查和测试仅在指定服务器隔离目录进行；原始证据私有保存。
