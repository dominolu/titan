# 管理模块运行与验证

当前实现与验收进度见 [实施计划](management_console_implementation_plan.md)。生产启用范围以各阶段验证报告为准。新增入口在现有 `titan-cli` 中，A 和 N 分进程运行；编译、回测和报告仍由独立 CLI 进程执行。

## 构建与配套发布

以下命令只能在 `192.168.3.88` 的隔离验证目录执行。先核对版本、目录和未提交修改。Rust 产物和 Web 资源必须配套发布：生成的 TS 文件包含该 Rust 可执行文件的 SHA-256，页面登录时校验协议和程序身份。

```sh
cargo build --locked -p titan-cli --bin titan
cargo rustc --locked -p hftbacktest --example v13_grid_history -- -C opt-level=2
target/debug/titan console export-types --output web/console/src/generated
cd web/console
pnpm install --frozen-lockfile
pnpm typecheck
pnpm build
```

使用 `CARGO_TARGET_DIR` 时相应替换产物路径。构建服务器如需镜像，将注册表覆盖写入隔离目录的 Cargo 配置，保留仓库已有配置；不能在本地安装依赖或构建。PureAdmin 上游身份见 [UPSTREAM](../web/console/UPSTREAM.md)，保留 MIT 许可。

发布包包含原样的 `titan`、历史回放程序、`web/console/dist`、Python SDK/报告组件和对应依赖清单。记录文件摘要、工具链、Cargo.lock 和 pnpm-lock.yaml 身份；打包后再次修改或 strip `titan` 会使页面身份校验失败，应重新导出契约并构建页面。

## 配置与启动

以 [配置示例](../configs/management_console.example.toml) 为起点。配置文件路径均相对于该文件所在目录解析；模板源文件、参数、数据集和程序路径由节点管理员预先登记，HTTP 不接受任意 shell 或文件路径。实例目录设置为 `0700`，配置与凭据文件设置为 `0600`。

通过 `titan console password-hash` 从标准输入读取密码，生成 Argon2 哈希；不把明文密码放入参数、仓库或审计记录。节点令牌使用不少于 32 字符的随机值，通过配置中的环境变量引用提供。页面会话使用 HttpOnly Cookie，写请求同时校验 Origin 与 CSRF。用户必须显式配置节点、账户、环境和角色；空账户列表不允许访问任何真实账户。

```sh
titan console node --config /private/console.toml
titan console manager --config /private/console.toml
```

N 只绑定 loopback。跨主机访问经已认证的 SSH 隧道或 TLS 代理。开发页面只允许 loopback HTTP；生产配置使用 HTTPS origin 和 `secure_cookie=true`。管理端可以只含 `[manager]`，节点只含 `[node]`，密钥和实盘配置保留在所属节点。

CLI 经同一管理 API 提交固定请求：

```sh
titan console request --url https://console.example --username operator \
  --password-env TITAN_CONSOLE_PASSWORD --request /private/request.json
```

请求内容必须提供稳定的 `idempotency_key`、`node_id` 和带 `kind` 的 `action`。重试使用完全相同的输入和键。传输超时后先按原键查询，不能换键重做。

## 执行与恢复语义

管理数据库保存唯一 Operation、输入快照、审计、会话和事件游标；N 维护本机 Registry、不可变请求与回执。管理服务或节点重启不会主动杀死独立任务。启动新进程前先创建唯一操作目录并持久化请求；PID、启动时间或回执无法证明时进入 `unknown`，保留占用且不自动重派。

未派发任务可在 A 确认取消。已派发任务由执行进程确认，先终止其子进程组并等待清理，再发布终态；取消请求本身不能表示取消成功。节点明确返回受理前容量拒绝时，原操作继续排队。其他丢失回执不会被当成容量拒绝。

Stop 使用独立的有界受理容量和派发循环；目录同步、普通任务排队及旧 Stop 回执核对不阻塞新的 Stop 受理。E 仅缓存成功进入控制队列的 Stop 操作，队列满后可重新提交；成功受理后的重复停止保留原始操作及期限。N 对已证明死亡或 PID 被复用的任务释放进程槽位，原操作仍保持 `unknown`、禁止重派；实盘账户占用继续由独立的释放证明约束。

跨进程恢复不能只看当前活动订单。E 对 checkpoint 中非终态意图通过原账户 connector 的只读查询补齐最多 128 笔权威结果，再进入现有恢复对账；查询共享原启动期限。交易所无结果、身份冲突、累计成交变化或持仓证据不足仍拒绝，不能用“活动列表中已消失”推断撤单成功。

恢复点及运行代次现统一保存在账户/运行编号命名空间内的 `runtime.db`。恢复请求继续提交原始载荷 SHA256，节点核验并 pin 完整记录引用，冻结进 managed spec 后启动 worker；不会因 head 更新改选其他记录。旧 JSON/代次文件必须先显式导入，正常运行不再写文件或自动回退。迁移、检查及隔离备份步骤见[恢复存储维护](recovery_storage_operations.md)，本轮非实盘证据见[M1 验证](verification/2026-10-27/data-persistence/m1/README.md)。

数据准备在独立目录写入，校验源文件与缓存摘要、fsync 后才发布缓存引用。失败或取消保留旧缓存。新登记的数据集必须先执行准备操作；回测要求缓存引用与成功的原始准备回执一致，不能仅凭一个导入文件的摘要声称它对应某组 CSV。回测固定制品、数据与执行模型，收益、回撤、含在途风险和最终残仓均来自 Rust 引擎。失败时保留已有结果，页面不补零或计算替代指标；报告校验 ResultBundle 文件清单及摘要。

备份管理库和 N 的完整状态目录，包括 WAL 一致快照、原请求、回执、Registry、制品和数据引用。回滚先停接收新任务，确认未完成任务身份，再配套切换程序与静态资源。数据库 `user_version` 超过程序能力时拒绝启动；不得删除回执、账户事实或换操作 ID 来解除 `unknown`。实盘 worker 的升级、停止与恢复须使用后续 M2/M3 已验收的协议，不能因 Web 回滚而杀死或接管旧实例。

## 非实盘闭环

`scripts/management_console_smoke.py` 创建私有测试配置和真实 A/N 进程，完成权限、参数冻结、构建、缓存准备、重启核对、回测、报告和取消。`--matrix` 执行三种行情、三种方向及两种执行模型共十八组；`--browser` 启动编译服务器上的 Playwright。测试结束清理 A/N 进程，结果保存在指定目录；失败不生成整体通过记录。

浏览器可以通过 `TITAN_BROWSER_EXECUTABLE` 指定与锁定 Playwright 匹配的 Chrome for Testing。浏览器与安装包摘要也应登记。验证脚本不会连接真实账户；实盘授权和 100 USDT 风险上限不能用于扩大历史测试之外的真实仓位。

## M2 只读账户观测

只有在所属实盘节点显式设置 `node.observe_bindings` 才创建账户观测 worker。默认列表为空，编译节点不会连接真实账户。A 的 `manager.nodes.<id>.live_control` 与 N 的 `node.live_control` 默认均为 `false`，两端分别拒绝真实交易控制动作；只读页面没有交易动作能力。

```toml
[manager.nodes.live]
endpoint = "http://127.0.0.1:8042" # 跨主机时使用 SSH 隧道或 TLS
token_env = "TITAN_CONSOLE_LIVE_TOKEN"
environments = ["live"]
live_control = false

[node]
id = "live"
listen = "127.0.0.1:8042"
token_env = "TITAN_CONSOLE_LIVE_TOKEN"
work_root = "state/live"
titan_program = "bin/titan"
environments = ["live"]
observe_bindings = ["gate-btc"]
live_control = false

[node.bindings.gate-btc]
account = "gate"
instrument = "GATE_FUTURE_BTC_USDT"
runtime_config = "runtime.toml"
strategy_key = "grid"
max_exposure_quote = "100"
max_price_deviation_bps = 50
max_duration_seconds = 90
```

用户身份仍须显式授予 `nodes=["live"]`、`accounts=["gate"]`、`environments=["live"]`。`runtime.toml` 使用既有 `ApplicationConfig`，账户凭据使用所属节点的 `secret://file/` 引用。只读 worker 在适配配置前移除全部策略，仅保留预登记品种的公开行情源，并强制 `ObserveAll / LeaveOpen`，不会因关闭观测而撤销账户原有订单。观测期限包含启动对账时间，停止另保留有限的退出清理预算。

账户事实由 E 的 `AccountFactJournal` 持续记录到 `node.work_root/account-facts.sqlite3`。它保留原始 ABI 事实、当时的价格/数量/币种单位和来源水位，按无歧义的交易所成交/流水复合身份去重，不从订单累计成交构造成交；同一记录出现单位或经济字段矛盾时保留首次事实并报告冲突。队列超限、写盘错误和异常重启进入完整性诊断。CrossEx 账单按[官方协议](https://www.gate.com/docs/developers/crossex/)通过独立后台 GET 任务读取，查询时间段固定、页数有界并重叠去重；失败不推进已确认覆盖水位。私有流和交易执行不等待页面或账单查询。

费用汇总只在后台按最多 4096 条新成交逐批追赶，并按账户及来源/时间索引读取历史。尚未追赶完成时，成交总数和费用合计保持缺失，同时给出处理进度与诊断；不会把部分合计标成完整结果。该缓存可由持久化原始事实重建，重启不会重记成交或重扣费用。

金额和大整数以字符串传输。账户权益按原币展示，不混加币种。采样账户收益按权益变化扣除外部转入转出，费用和资金费已含在权益中，不重复扣减；它保留采样窗口与缺失的账户/账单共同水位证明，不能标成完整的策略收益。缺少估值、成交归属或连续性时明确保留缺失原因。

线性合约估值需在账户的 instrument 中显式配置 `linear_quote_currency`，值为 `currencies` 中对应计价币的 `currency_id`（例如 USDT 为 1 时配置 `linear_quote_currency=1`）。这同时声明该合约采用线性计价；反向合约不能填写该字段。沿用配置中的 tick、lot、contract multiplier 和币种最小单位，以检查溢出的整数计算并向上取整。公开 BBO 必须来自 symbol、asset、tick/lot 均一致的预登记绑定，且不超过两秒；缺少映射或新鲜行情时名义敞口保持缺失，不猜测币种或汇率。

E 统一计算实际敞口及含在途上界：双向持仓分别取绝对值，合并账户订单与运行时已持久化意图，撤单请求不释放占用，独立成交尚未投影到持仓时保留成交数量。监控与交易准入共用 tick/lot 敞口计算；A/N/U 只传输和展示。无法证明全部意图、成交持仓一致性或委托状态时显示不完整。约 500ms 的观测采样持续写入逐品种、原币的历史最大实际敞口和最大含在途上界，跨 worker 重启保留；该最大值只代表已记录采样，不表示连续行情最大值或完整历史覆盖。

账户事实库当前 `user_version=2`。每个记录源持有操作系统文件锁，健康的并行 writer 不产生 `RECORDING_GAP`；只有失去锁且未确认干净退出的来源才产生缺口诊断，并按其账户范围报告。历史已有且无法证明范围的全局缺口保留，不能自动抹除。备份和回滚须包含库、WAL 一致快照及 recorder 目录；旧版本不支持 v2 数据库，不能直接原地降级。

worker 的 Unix IPC 目录为 `0700`、socket 为 `0600`，请求还绑定令牌、运行 ID、代次、PID 和进程启动时间。N 重启只复用身份及配置指纹均可证明的存活 worker；失去证明时拒绝自动接管。A 每个节点独立核对，单个慢节点不会阻塞其他节点。UI 同时显示节点连接、进程、账户、策略、记录和完整性状态，失联时保留带过期标记的最后事实。

创建、停止和析构 `TradingRuntime` 的整个生命周期由同步 owner 线程持有；IPC 和网络异步任务使用各自的运行时，不能把拥有 Tokio runtime 的业务宿主放进 HTTP/CLI 的异步上下文后再析构。worker 异常退出且缺少终态回执时，N 返回 `UNKNOWN`，不从进程消失推断已完成停止。

新历史通过 `/api/v2/resources/<id>/facts` 或策略下的 run/backtest `/facts` 读取，实盘模拟和回测复用同一端口。节点固定环境/账户/运行后查询，游标绑定来源、过滤和固定上界；每页最多 200 条/约 4 MiB。旧 `/history` 保留早期记录兼容，页面提供显式开关。代次/会话不符及旧游标拒绝；来源 gap、未关闭和 received segment complete 不表示交易所完整覆盖。目录、配置和维护流程见[本地历史与结果手册](local_persistence_operations.md)。

宿主日志通过同一资源授权入口 `/log` 按字节偏移读取，每页最多 64 KiB。只发布固定的 `worker.log`，不发布 claim、运行配置或凭据。告警确认属于 A 的元数据，并与审计在同一事务提交，不修改 E 的诊断；新的来源条件需要重新确认。

`scripts/management_observation_smoke.py --browser` 在编译服务器验证模拟账户事实的端到端转发、权限、过期、告警和日志。`scripts/management_live_readonly.py` 仅用于已授权实盘节点，使用已验证二进制执行 90 秒只读验收；它以只读方式读取该节点既有账户后端，凭据及完整快照不离开节点，最终输出脱敏证据。脚本包含固定的账户 ID 1 与目标品种保护，不应挪到编译服务器执行真实账户验证。

## M3 签名部署与交易控制

实盘 N 在同一节点协议上接收 Deploy、Start、Pause、Resume、Stop 和 Recover。A 与 N 均须显式启用 `live_control=true`，用户须具有目标节点、live 环境、账户的 operator 权限。编译节点保持交易能力关闭；只读用户继续使用同一 REST/SSE 查询。

配置一个稳定的 `node.bindings.<id>.account_identity`，同一真实账户在各节点必须使用同一个规范身份；它参与 A 的账户/品种占用和订单命名空间占用。该字段须满足资源 ID 规则。N 的 `artifact_sources.<build-node>` 指向已认证的构建节点协议，填写 endpoint、token_env 和 backtest 环境；不能提供任意下载 URL。编译节点配置 `signing_key` 和 `signing_key_id`，私钥仅保留在编译服务器。实盘 runtime 的 `strategy_service.trusted_ed25519_keys` 登记公钥，并声明 `managed_by_console=true`；直接 core-run 会拒绝此配置。

部署固定签名制品、运行配置摘要和静态绑定。N 复用 E 的原生加载器校验签名、摘要、ABI、目标和通用 CPU 配置，通过暂存和原子发布落地。配置漂移后须重新部署。运行时不依赖 Python；恢复不重新编译策略。制品页显示不可变部署，启动确认显示账户、品种、敞口上限、价格保护、期限和制品中的停止方式。

本期受控配置采用单账户、单品种、local account/asset 0 的原生策略。平台按精确 tick/lot/合约乘数转换报价金额上限，E 同时约束总持仓、旧挂单剩余量和同批新增委托；对冲腿按绝对量分别计入，旧挂单按各自限价保守计价。下单须具有新鲜 BBO 和价格偏离证明；意图先持久化并 flush，再复查期限后派单。减少风险的委托仍受价格保护。账户以 ObserveAll 接收全部事实，关闭账户时使用 LeaveOpen，不代替其他所有者撤单。

Pause 需要制品显式声明 `pause_safe`。暂停仍消费订单、成交、持仓和定时器，只允许撤单及 venue reduce-only 保护动作；策略必须读取 `ctx.is_paused`，保持业务引用直至终态/成交已确认。未声明能力的旧制品不提供暂停按钮。Resume 复用同一代次和状态，不是 fresh start。Stop 调用 E 的实际 operation，等待停止屏障；页面不能覆盖制品的 cancel 或 flatten 选择。保留仓位、未知委托或失败时不释放账户占用。

Recover 必须显式引用 N 登记的 checkpoint 文件 SHA256 和旧运行代次。E 在加载前固定文件身份，继续校验制品、schema、绑定及账户事实，不允许清空状态、忽略未消费成交或重置保护性退出。节点沿用原 worker 的 boot ID 和 CLOCK_BOOTTIME 截止时间；重启 A/N、恢复进程、系统时间回退均不延长原期限，节点重启或期限耗尽会拒绝恢复。旧实例的迟到安全快照不能释放新恢复操作持有的占用。

控制回执超时先查询同一 E operation。只读回执查询不会补执行动作；缺少 E 的调用前/后证据时保留 unknown。A/N 重启通过 owner token、PID/启动身份和固定 socket 重新查询原 worker，不能重新启动它。恢复未成功时保留旧账户占用，只有明确的“从未尝试激活”证据可释放失败 Start 的占用。

编译服务器先运行 `scripts/management_control_smoke.py`（loopback 模拟交易所、实际签名制品/A/N/E）和 `--browser` 页面验收。真实账户验收脚本 `scripts/management_live_control.py` 仅在指定实盘节点运行，要求编译服务器的冻结 release 和 SHA256；内置 gate/ID 1/BTC、100 USDT、50 bps、120 秒、最小数量及前后残留核对。它不是长期策略入口。实盘部署完成后关闭本轮 A/N，不注册自动重启交易服务。

## 历史与对象后端（M3）

管理节点复用 `work_root/storage`，先在节点维护环境执行 `titan storage initialize --storage-root <work_root>/storage --profile <private.json>`，再启动节点。profile 只保存环境变量引用，变量需注入节点和其 worker；不通过管理 HTTP 请求传递密钥或任意存储路径。管理器自身的目录库不切换。

后台投影独立于策略运行及其恢复 writer。`prepare-history/prepare-objects` 产生维护计划，`activate --plan` 才提交生效修订，切换后旧历史游标返回 409，需要刷新。配置库丢失或身份不符时拒绝自动回退。本地恢复仍使用 SQLite；可信运行配置需满足已冻结的新运行预算准入。

原结果包中的 `history.db` 保持不可变，M3 为历史投影与裁剪创建 `history-source.db` 工作副本，API 继续使用原逻辑接口。准备数据、编译文件及报告缓存缺失时从已登记清单按原字节物化，不重新编译策略或重跑回测。完整配置、回切、缓存清理及故障操作见[历史与结果存储维护](local_persistence_operations.md)。

## P14 实盘准入与已验收流程

P14 已完成本轮受控实盘验收，详见[本轮记录](verification/2026-10-28/data-persistence/m4/README.md)。用户明确运维 SSH 账户并核对节点后，先在实盘节点运行 `scripts/persistence_live_preflight.py --output <私有准入报告>`；只允许 GET，核对 gate / ID 1 / BTC 的规格、模式、零风险和无其他控制器。`management_live_control.py` 现在要求 `--preflight <报告路径>`，报告须在 10 分钟内且固定风险约束完全匹配，启动前仍复查。使用编译服务器冻结包及其 expected SHA，不在实盘节点编译。脚本同时收集本地 SQLite 固定记录恢复和统一事实的证据；只有下撤单、原期限恢复与停止、占用释放、最终零残留和进程退出均通过，才标记 P14 完成。

本轮实际通过 13 项流程及独立后置核对，六笔最小委托全部撤销、零成交，固定 SQLite 记录/pin、代次递增和原期限保持；最终本轮占用、目标挂单/持仓和验收进程均为零。临时明文凭据已删除，完整账本和恢复证据留在实盘节点私有目录。后续复现应新建私有目录并重新做现场准入；不得将本轮通过当作长期运行授权、真实成交恢复或有仓位平仓的证据。


### 真实成交补验的当前状态

2026-10-28 的[成交补验](verification/2026-10-28/data-persistence/m4-filled/README.md)未通过。独立成交及费用已落盘，持仓进入原 60 s SQLite 点，但策略已因先持仓后成交的序号判断进入待对账状态，恢复候选未激活。自有进程退出后采用单独的最小 reduce-only 安全平仓，不能记为策略 Stop 通过。独立 GET 复核为零目标挂单/持仓，验收进程已退出。

用户明确选择保留占用、停止本轮复验。两条账户/命名空间占用和原 UNKNOWN、失败回执、到期预算、原恢复记录及 pin 均保留；未执行人工处置，未部署候选 04。临时明文凭据及传输文件已清理。新候选仅通过非实盘验证，不据此自动恢复实盘。

`management_live_fill_recovery.py` 只用于明确授权的有限补验。复用旧目录须验证未激活或安全 Stop 的原 E 证明；已有真实成交按本轮新增成交筛选，同时核对既有财务摘要不变，历史诊断保留。恢复前还须核对公开业务阶段与错误码，不能把公共 `pending_execution=false` 当作策略必然可恢复。

`--reconciliation-receipt` 仅验证显式人工审计回执与保留的 store ID、原记录、操作集和代次下界；它不会释放占用、制造 E 安全证明或改写旧运行。本轮没有有效人工回执，参数不能用于绕过用户的停止决定。历史复现说明不构成继续运行授权。


### 完整模拟链路的新证据

[2026-10-28 A/N/E 非实盘验收](verification/2026-10-28/data-persistence/m4-ane/README.md)使用冻结候选 04 复现跨进程重放后库存重复推进。修复候选 05 保留同一签名策略文件，恢复已消费累计量，并抑制重复成交的策略回调与计数。完整模拟部署、先持仓后成交、SQLite 跨进程恢复、成交/费用去重及原 10 s 内有仓位 Stop 通过；旧崩溃源缺口保留，新源干净封存。

复现入口为 `scripts/management_fill_recovery_smoke.py`，只允许编译服务器，所有 REST/WebSocket URL 必须为 loopback，凭据为合成值，归档环境为 simulation。指定冻结 release、程序/策略 SHA 和新证据目录；不重新编译策略替代原签名文件。

实盘仍保持停止，两条真实保留占用不动。当前可供后续审查的修复产物为编译服务器的 `evidence/m4-fill-release-05`；非实盘通过不生成 E 的真实安全释放证明，也不授权解占或启动实盘。

### 候选 05 的故障验收边界

[五场景故障补验](verification/2026-10-28/data-persistence/m4-faults/README.md)已在编译服务器完成。验收入口新增 `--scenario partial|bootstrap-replay|market-reconnect|private-disconnect|abnormal-stop`；部分成交使用同一源码的双 lot 签名参数 fixture，其余场景使用原签名文件。程序/Web 及 65 项冻结发布文件均未变化。

可信事实下，部分成交恢复、bootstrap 重放恢复及行情重连的 Stop 均在原 10 s 内平仓、退出 E 并由 A 释放模拟占用。私有断线会使旧运行失效，重新连上不恢复旧交易门；独立成交缺失的异常态也不能凭累计订单量强行结算。对应 Stop 必须保留失败回执、残仓和占用，等待 A 最终 FAILED，不能将 E 退出当作安全释放。

异常态原引擎 Stop 预算仍为 10 s，本轮客户端收到超时失败回执为 11.702 s；轮询观察等待不构成期限延长或平仓通过。私有流 `ACCOUNT_STREAM_GAP` 与旧崩溃来源缺口保留，即使 writer 清洁关闭也不能宣称交易所历史连续。模拟负例数据库只留证，不进行人工解占；实盘及原两条真实占用继续不动。

### 私有断线和缺成交后的显式恢复

[候选 07 安全恢复](verification/2026-10-28/data-persistence/m4-safe-recovery/README.md)已通过编译服务器的 5 个新增场景和 5 个故障回归。当前候选为 `evidence/m4-safe-recovery-release-07`，候选 06 的内部门标志失败和候选 05 均保留；新程序未部署至实盘。平台失败时同时关闭激活门和原生命令门，连接重新 READY 不自动恢复旧运行。

恢复必须等旧 E 退出，保留原占用，以原 run/generation 和事先确认的固定 SHA 提交显式 Restore。异常时最新 head 可能已经 pending 或含业务错误；不能改写该 BLOB，应保留失败证据并选择已有、无未决执行且可由完整原生成交解释的固定点。已知订单的非零累计量必须补查完整原生 ID/数量/费用，补查不完整、内容矛盾或与旧账本冲突时拒绝激活。新 E 在原总期限内启动新代次，原 BLOB/pin、boot ID、单调截止值和到期值不变；旧 Stop 仍失败，新 Stop 仍为独立的 10 s 操作。

当前仅验收可精确证明的净持仓增加；不借持仓镜像补造成交，不自动重试失败恢复、回退代次、解除占用或切换 namespace。两个正例新 Stop 退出/释放为 3.566 / 3.615 s，旧运行保持 FAILED 和安全证明 false。全账户历史缺口及冲突继续显示，不据局部补查恢复完整收益统计。实盘和原两条真实保留占用维持用户的停止决定。
