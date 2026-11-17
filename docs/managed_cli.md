# Titan 客户端与后台服务

`titan` 是后台管理/执行程序，`titan_cli` 是日常客户端。客户端默认访问选定的管理器；策略、账户事实、恢复与 Operation 仍由原服务处理。实施清单与逐项状态见[简化命令计划](cli_simplification_plan.md)。本页新版入口的本轮验证见[简化 CLI 验收](verification/2026-11-03/cli-simplification/README.md)，旧验收记录保留原范围。

## 日常命令与 help

```sh
titan_cli --help
titan_cli help strategy
titan_cli strategy start --help
titan_cli account --help
titan_cli research --help
titan_cli config --help

titan_cli use prod
titan_cli check
titan_cli strategy ls
titan_cli strategy show grid-btc
titan_cli strategy start grid-btc
titan_cli strategy pause grid-btc
titan_cli strategy resume grid-btc
titan_cli strategy stop grid-btc
titan_cli strategy logs grid-btc -f

titan_cli account ls
titan_cli account balance gate
titan_cli account positions gate
titan_cli account orders gate

titan_cli research build grid --params /private/parameters.json
titan_cli research backtest grid --data btc-history
titan_cli research report grid
```

默认显示有界表格或摘要，缺失金额与风险不补零；脚本添加 `--json`，多帧流使用 `--ndjson`。构建、回测和生命周期命令默认有界等待，`--timeout` 设置本地观察期限，`--detach` 只返回受理；research 流水线 detach 后停在首个未完成阶段，后续通过原 journal 继续，不暗中提交后续动作。Ctrl-C 结束本地观察，不取消或停止远端任务。

目标接受准确资源 ID、profile alias 或授权范围内唯一名称。存在多个部署、运行、参数预设、版本或回测时明确要求选择，不按时间猜最新；可分别使用准确 ID 或研究命令的 `--preset/--version/--backtest`。`start` 启动固定部署；以运行 ID 启动时，只允许原服务明确声明可恢复且固定恢复证据完整的运行。缺证据不清空业务状态重新开始。

## 交互模式

直接运行 `titan_cli`，或在终端输入 `titan_cli strategy` 等分组命令，进入交互界面。提示符显示环境、分组与选中目标：

```text
titan[prod]> strategy
titan[prod]/strategy> ls
titan[prod]/strategy> use grid-btc
titan[prod]/strategy[grid-btc]> status
titan[prod]/strategy[grid-btc]> start
titan[prod]/strategy[grid-btc]> logs -f
titan[prod]/strategy[grid-btc]> back
titan[prod]/strategy> back
titan[prod]> exit
```

根层 `use` 选择 profile，分组内 `use` 选择当前目标。`help` 和 Tab 补全来自同一命令树，历史仅保存有界非敏感命令；退出不停止后台或策略。非终端无参输入只显示 help，不等待交互输入；机器输出模式必须选择实际动作。

## 连接、本机自动启动与服务

配置路径优先使用 `--client-config`、`TITAN_CLIENT_CONFIG`、兼容的 `TITAN_CONFIG`；否则使用 `XDG_CONFIG_HOME/titan/client.toml` 或用户配置目录。`use` 原子保存默认 profile；显式参数仍覆盖环境和 profile。每个 CLI 进程使用内存 Cookie/CSRF 会话，日常入口以 `check` 检查连接。

```toml
schema_version = 1
default_profile = "prod"

[profiles.prod]
url = "https://console.example"
username = "operator"
password_env = "TITAN_CONSOLE_PASSWORD"
node = "titan-live"
environment = "live"

[profiles.prod.aliases]
grid-btc = "exact-deployment-or-run-id"
```

本机自动启动需要 profile 显式声明本机 HTTP loopback URL 和已存在的服务配置，示例：

```toml
[profiles.local]
url = "http://127.0.0.1:8080"
username = "operator"
password_env = "TITAN_CONSOLE_PASSWORD"

[profiles.local.local_server]
manager_config = "/private/server.toml"
startup_seconds = 15
# program 可选；默认查找 titan_cli 同目录的 titan
```

客户端先核对 origin、监听地址和配置，复用现有服务；未运行时通过锁保证单次启动，等待认证与协议就绪。启动超过期限保留原进程身份供检查，不重启已有 worker。只自动启动管理器，不启动执行节点或策略；远程连接失败不降级到本机。已有启动记录而管理数据库缺失或不是普通文件时，自动启动拒绝创建空库；需先恢复原数据或由管理员明确初始化。`--no-auto-start` 禁止自动启动，但仍核验已声明的本机服务身份。自动启动的管理器终端输出丢弃，不生成无界客户端日志；需要持久诊断日志时由服务管理器托管。

```sh
titan serve --config /private/server.toml
titan serve manager --config /private/server.toml
titan serve node --config /private/node.toml
```

## 数据库配置

配置操作可以在后台未启动或连接不可用时执行：

```sh
titan_cli config database
titan_cli config show
titan_cli config check

# 无交互方式
titan_cli config database --history sqlite --storage-root /private/node/storage
titan_cli config database --history postgres --dsn-env TITAN_HISTORY_DSN --storage-root /private/node/storage --save-only
```

交互向导选择历史数据库、所属机器上的存储目录及环境引用。管理与恢复继续使用本地 SQLite，PostgreSQL 仅用于历史投影；不接受明文 DSN/密码参数。`show` 只显示脱敏状态，`check` 有界检查本地访问、引用、连接和 schema/权限，不修改数据库。

配置先以候选形式原子保存。新且空的目录可复用原 StorageControl 初始化；已有控制库/profile 不原位覆盖。需要投影、迁移或切换时显示 `candidate/migration_required`，继续使用[原存储维护流程](local_persistence_operations.md)准备、校验和激活。配置向导不会改写远端任意路径，也不会自动修改节点服务配置；只有所属节点实际使用该 storage_root 且对应路由已生效，才能解释为已启用。

## 过程记录与兼容

日常操作不需要操作 Operation 对象。客户端先将固定输入、幂等键及阶段回执留在用户私有 state 目录，断线/unknown 后按原身份查询，不换键自动重做；必要时使用输出的 journal ID 和 `--journal` 继续同一流程。客户端崩溃或响应丢失不能证明未受理，参数草稿的身份未知时不会仅凭相同参数猜测归属。

普通命令先核对上次流程：处理中或未知时继续观察原任务；已证明结束的控制动作允许这一次新的显式命令创建新意图。研究流程中间阶段成功则继续原流水线。明确准入拒绝还须按原键查明没有已受理任务，才允许下一次显式重试；空查询本身不构成未受理证明。显式 `--journal` 始终只核对原流程，不自动创建新任务。

`titan console ...`、原本机 `titan run/ls/show/logs/stop/report` 与存储维护入口保留兼容。它们的作用域与下述历史手册一致；新版 `titan_cli` 不因同名 ID 自动回退到本机 Registry。

---

# 兼容管理入口与历史验收

以下为原 `titan console` 完整协议入口和 2026-11-02 验收范围。新用户优先使用上方二级命令；低层参数继续用于兼容、维护与排障。原本机 `titan strategy`、`run/ls/show/logs/stop/report` 使用本机工作区和 Registry，管理命令通过授权 API v2 访问已登记资源。


## 连接与凭据

客户端配置只保存凭据环境变量的名字：

```toml
schema_version = 1
default_profile = "build"

[profiles.build]
url = "https://console.example"
username = "operator"
password_env = "TITAN_CONSOLE_PASSWORD"
node = "titan-build"
environment = "backtest"
```

通过既有秘密注入方式设置所引用环境变量，不将密码放入命令参数。调用示例：

```sh
titan console --client-config /private/client.toml --profile build login --json
titan console --client-config /private/client.toml check --json
titan console --client-config /private/client.toml node ls --json
```

所有客户端参数可放在管理子命令后。优先级为显式参数、`TITAN_*` 环境引用、选定 profile；profile 选择依次使用 `--profile`、`TITAN_PROFILE`、`default_profile`。配置路径依次来自 `--client-config`、`TITAN_CLIENT_CONFIG`、兼容的 `TITAN_CONFIG`。连接覆盖变量为 `TITAN_URL`、`TITAN_USERNAME`、`TITAN_PASSWORD_ENV`、`TITAN_NODE`、`TITAN_ENVIRONMENT`。URL 必须为不含身份、路径、查询和片段的 HTTP(S) origin。

每次进程独立登录，Cookie 和 CSRF 仅保留在内存，不写入会话文件。`login/check` 返回公共身份和协议信息；`logout` 撤销本次进程新建的会话，不表示登出其他终端。节点服务仍使用 `titan console node --config /private/console.toml`，管理器仍使用 `console manager --config ...`；服务端配置与客户端配置分别命名。

## 输出与退出码

单次 `--json` 的 stdout 是一个完整对象。持续观察使用 `--ndjson`，每行一个完整对象。当前默认人读输出为有界表格/摘要，完整字段使用机器输出读取。诊断和发送前的幂等键写入 stderr。

```json
{"schema_version":1,"api_version":2,"kind":"operation","data":{"id":"operation-id","state":"pending"}}
```

`kind` 区分连接、结果、Operation、观察帧和错误；精确字段以实际命令输出为准。错误在 stderr 中使用同一 envelope，`data` 包含稳定 `code`、安全 `message` 和可用的 `request_id`。JSON 数字/字符串、原币种、原单位和服务端的大整数原样保留，客户端不计算权益或风险准入。

| 退出码 | 含义 |
| --- | --- |
| 0 | 查询成功或写请求已受理；只有带等待的 Operation 成功才表示该操作成功 |
| 1 | 未分类的命令执行错误 |
| 2 | 输入、配置或用法错误 |
| 3 | 认证或权限拒绝 |
| 4 | 资源不存在 |
| 5 | 修订、幂等输入或代次冲突 |
| 6 | 网络、服务或协议错误 |
| 7 | 等待的 Operation 失败、取消或结果未知，查看 `data.code` 区分 |
| 8 | 本地观察期限到达，远端操作可能继续 |
| 130 | Operation 等待被 Ctrl-C 中断；watch/logs/facts 的只读观察中断正常退出 |

写请求没有自动重发。HTTP 202/退出 0 只表示受理；`unknown` 保持未知，不能生成新键重做。客户端观察期限不改变 Runtime 原有停止/恢复期限。

## Operation 闭环

```sh
titan console operation list --limit 20 --offset 0 --json
titan console operation show operation-id --json
titan console operation by-key stable-request-key --json
titan console operation wait operation-id --timeout-seconds 120 --json
titan console operation cancel operation-id --wait --timeout-seconds 120 --json
```

`list` 支持 `--idempotency-key`、`--run-id`、`--from-ns`、`--to-ns`。列表分页每页最多 200。`operation submit --request /private/request.json` 保留固定请求文件入口；所有专用 Operation 写命令接受 `--idempotency-key`、`--wait`、`--timeout-seconds`。未提供键时先生成键并向 stderr flush 后才发送。网络错误后用原键找回；同键异输入由服务端拒绝。

`cancel` 不带等待只表示取消请求的回执，已派发任务需要执行端清理并确认。等待超时或 Ctrl-C 只结束观察，不发送取消，不终止 worker，不释放占用。低层兼容命令 `console request --url ... --username ... --password-env ... --request ...` 仍可提交 v1 Operation；默认返回旧原始回执，指定机器输出时使用新 envelope。公共客户端认证和新命令使用 v2，v1 兼容读取和历史冻结字节不改写。

## 账户、绑定与节点

```sh
titan console account ls --json
titan console account show account-resource-id --json
titan console account balances account-resource-id --json
titan console account positions account-resource-id --json
titan console account orders account-resource-id --json
titan console account risk account-resource-id --json
titan console binding ls --json
titan console binding show binding-id --json
titan console node ls --json
titan console node show node-resource-id --json
```

账户档案是配置登记，不是账户事实。资料档案没有可证明的原生账户引用时不会按名称猜测关联；凭据引用与私有配置不输出。事实查询使用明确的账户资源 ID，未启动观测时显示 unavailable，缺失余额、权益、费用或风险字段不补零。账户持仓保留 NET/LONG/SHORT 身份。

账户与绑定继续通过节点管理员的声明式配置和既有校验登记，见[管理运行手册](management_console_operations.md)。本批不增加账户凭据维护、转账、手工交易、模式切换或账户热替换。

## 策略目录、参数与构建回测

```sh
titan console strategy ls --json
titan console strategy show strategy-id --json
titan console strategy presets --strategy-id strategy-id --json
titan console parameter ls --strategy-id strategy-id --json
titan console strategy versions --strategy-id strategy-id --json
```

`strategy backtests/reports/deployments/history` 同样通过稳定策略身份归集，分页使用 `--offset/--limit`；`history --kind` 支持预设、参数版本、策略版本、回测、报告、部署和运行。`strategy version <strategy-id> <version-id>` 返回公共版本元数据。`strategy_id` 是稳定业务身份，`strategy_type` 是实现类别，`source_version` 是作者版本，`runtime_strategy_no` 是运行路由编号，`run_generation` 是运行代次，不能互换。

以下变量表示从前一步实际回执中读取的 ID，不由 CLI 猜测最新资源：

```sh
titan console parameter create --parameter-preset-id "$PRESET" --parameters /private/parameters.json --json
titan console strategy describe --parameter-preset-id "$PRESET" --parameters /private/parameters.json --idempotency-key validate-v1 --wait --json
titan console parameter freeze "$PARAMETER_VERSION" --expected-revision "$REVISION" --freeze-operation-id "$DESCRIBE_OPERATION" --json
titan console strategy compile --parameter-version-id "$PARAMETER_VERSION" --idempotency-key compile-v1 --wait --json
titan console strategy prepare --dataset-id "$DATASET" --idempotency-key prepare-v1 --wait --json
titan console strategy backtest --strategy-version-id "$STRATEGY_VERSION" --dataset-id "$DATASET" --idempotency-key backtest-v1 --wait --json
titan console strategy report --backtest-id "$BACKTEST" --idempotency-key report-v1 --wait --json
titan console strategy reports --strategy-id "$STRATEGY" --json
```

参数文件直接是 JSON object，最大 1 MiB。`parameter copy <id>` 创建未冻结草稿，不沿用原冻结证据。`parameter update <id> --parameters ... --expected-revision ...` 更新草稿；冻结和更新都使用显式修订。Describe 成功与冻结输入必须匹配，冻结后的参数不可修改。参数草稿 CRUD 是原 SaveVersion 协议，没有 Operation 幂等键；回执丢失后先查询核对，避免盲目重复 create/copy。编译失败保留已有有效版本。

## 部署、启动与恢复

先读取版本、绑定与部署证据，确认目标账户、环境、品种和服务端风险预算。以下生命周期命令使用已配置的 `live` profile；该 profile 必须固定所属节点和 `environment = "live"`，避免沿用构建 profile。部署与启动分别为 Operation：

```sh
titan console --profile live deployment create --strategy-version-id "$STRATEGY_VERSION" --binding-id "$BINDING" --node "$LIVE_NODE" --environment live --idempotency-key deploy-v1 --wait --json
titan console --profile live deployment show "$DEPLOYMENT" --json
titan console --profile live run start --deployment-id "$DEPLOYMENT" --node "$LIVE_NODE" --environment live --idempotency-key start-v1 --wait --json
titan console --profile live run show "$RUN" --json
titan console --profile live run pause "$RUN" --expected-run-generation "$GENERATION" --idempotency-key pause-v1 --wait --json
titan console --profile live run resume "$RUN" --expected-run-generation "$GENERATION" --idempotency-key resume-v1 --wait --json
titan console --profile live run stop "$RUN" --expected-run-generation "$GENERATION" --idempotency-key stop-v1 --wait --json
titan console --profile live run restore "$RUN" --expected-run-generation "$GENERATION" --recovery-point-sha256 "$RECOVERY_SHA256" --idempotency-key restore-v1 --wait --json
```

这些是操作语法示例，实盘权限仍由任务授权、所属节点和原风险限制约束。没有 `live_control` 或不支持暂停的节点/策略明确拒绝。`resume` 在原代次从暂停继续；`restore` 固定旧代次与恢复点启动新代次，不清空状态、不放宽原恢复期限或身份检查。

本机 `titan stop` 通过 Registry 请求终止，只表示 STOP_REQUESTED；managed Stop 必须通过管理器，并等待 Runtime 业务停止屏障。Operation 成功、进程退出、持仓/活动订单事实、账户占用释放分别是独立证据；失联、失败或 unknown 不能当作安全释放。托管配置仍禁止绕过管理器直接启动。

## 状态、历史与持续观察

`console status` 查看当前授权快照；`console watch --ndjson` 进行有界周期轮询，`--samples` 可限制帧数。持续流不使用无界缓存，不把单次快照当作完整目录。节点连接、进程、生命周期、账户就绪、观测完整性和交易门保留服务端原字段；原生命令门不能解释为允许新增开仓。

`console logs <resource-id>` 使用固定 worker.log 的字节偏移分页，`--follow --ndjson` 跟随。`console history`、`console facts` 和账户 fills/history 使用对应资源及观察会话/代次。`--generation`（别名 `--worker-session-id`）校验当前来源，`--run-generation` 真正筛选事实的运行代次；运行资源默认筛选当前代次，账户档案不推断运行归属。事实使用 `--from-ns/--to-ns` 筛选接收时间、`--cursor` 续页和 `--newest-first` 指定方向。`facts --max-pages 10 --ndjson` 在 1–100 页预算内逐页导出；多页必须使用 NDJSON，不积累事实列表，每页给出 `export.has_more`、`next_cursor` 和是否到达页数限制。分页固定同一来源与服务端上界，过滤条件变化须重新查询；到页数上限仍有游标时不能宣称完整导出。旧 history 投影不支持时间过滤时明确拒绝，事实时间筛选使用记录接收纳秒时间。

观察中断只停止本地查询。断线后最后证据须标记 stale/unavailable，重连不能让旧修订或旧代次覆盖当前来源。业务观测仅消费公开 schema，未知协议只作有界通用展示，不解析恢复 BLOB 或私有状态。

## Issue 覆盖与验证

所有构建、fmt、Clippy、测试和真实 CLI 合成验收必须在 `titan-build` 执行。本机只编辑与人工审查。原始命令输出和清单保存于私有档案，公开目录只保存脱敏摘要。

| 需求 | 命令/入口 | 自动化场景 | 本轮状态 |
| --- | --- | --- | --- |
| #9 客户端与契约 | profile、login/check/logout、全局连接、JSON | management_cli_smoke 基础连接/权限/兼容 | 已实现；本轮非实盘验收通过 |
| #10 Operation | operation list/show/by-key/submit/wait/cancel | 幂等冲突、超时、取消、回执不确定 | 已实现；本轮非实盘验收通过 |
| #11 账户与绑定 | account、binding、facts | 无实时事实、授权、缺失与历史过滤 | 已实现；本轮非实盘验收通过 |
| #12 策略与版本 | strategy、parameter | 基础目录；--build 真实冻结/编译；--offline 历史闭环 | 已实现；本轮非实盘验收通过 |
| #13 生命周期 | deployment、run | --lifecycle 复用原生合成交易夹具，真实CLI发起 | 已实现；本轮非实盘验收通过 |
| #14 状态观测 | node、status、watch、logs/history/facts | 快照、代次、断线、Ctrl-C/日志分页 | 已实现；本轮非实盘验收通过 |
| #15 文档与整体验证 | 本手册及兼容手册 | fmt/Clippy/受影响Rust与真实CLI矩阵 | 已实现；本轮非实盘验收通过 |
| #8 总览 | 上述完整闭环 | 各子项新证据汇总 | 已实现；本轮非实盘验收通过 |

本轮完整结果见[管理 CLI 验收记录](verification/2026-11-02/managed-cli/README.md)：233 项受影响 Rust 测试通过、12 项保留忽略，真实 CLI 主矩阵 14 组及观察故障 9 项通过；主矩阵内复用的生命周期夹具 26 项、历史链路 8 项通过。上述计数有包含关系，不相加当作独立总量。watch 采用有界轮询；独立 PostgreSQL/S3 驱动、全部在途取消竞态和实盘场景未在本轮覆盖。

验证脚本为 [management_cli_smoke.py](../scripts/management_cli_smoke.py) 和 [management_cli_watch_smoke.py](../scripts/management_cli_watch_smoke.py)，依赖现有管理合成环境和编译服务器产出的真实二进制。`--lifecycle` 只连接回环模拟交易服务；`--offline` 使用已登记历史输入进行模拟回放，均不表示实盘验收。历史报告不能替代本批改动的新证据。
