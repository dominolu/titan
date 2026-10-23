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

数据准备在独立目录写入，校验源文件与缓存摘要、fsync 后才发布缓存引用。失败或取消保留旧缓存。新登记的数据集必须先执行准备操作；回测要求缓存引用与成功的原始准备回执一致，不能仅凭一个导入文件的摘要声称它对应某组 CSV。回测固定制品、数据与执行模型，收益、回撤、含在途风险和最终残仓均来自 Rust 引擎。失败时保留已有结果，页面不补零或计算替代指标；报告校验 ResultBundle 文件清单及摘要。

备份管理库和 N 的完整状态目录，包括 WAL 一致快照、原请求、回执、Registry、制品和数据引用。回滚先停接收新任务，确认未完成任务身份，再配套切换程序与静态资源。数据库 `user_version` 超过程序能力时拒绝启动；不得删除回执、账户事实或换操作 ID 来解除 `unknown`。实盘 worker 的升级、停止与恢复须使用后续 M2/M3 已验收的协议，不能因 Web 回滚而杀死或接管旧实例。

## 非实盘闭环

`scripts/management_console_smoke.py` 创建私有测试配置和真实 A/N 进程，完成权限、参数冻结、构建、缓存准备、重启核对、回测、报告和取消。`--matrix` 执行三种行情、三种方向及两种执行模型共十八组；`--browser` 启动编译服务器上的 Playwright。测试结束清理 A/N 进程，结果保存在指定目录；失败不生成整体通过记录。

浏览器可以通过 `TITAN_BROWSER_EXECUTABLE` 指定与锁定 Playwright 匹配的 Chrome for Testing。浏览器与安装包摘要也应登记。验证脚本不会连接真实账户；实盘授权和 100 USDT 风险上限不能用于扩大历史测试之外的真实仓位。
