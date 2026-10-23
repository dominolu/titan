# M3 部署、控制与恢复验收

日期：2026-10-23。基线 `dcd0b801` 加 [源码 SHA256 清单](source-sha256.txt)。T14 至 T17 已完成；M1、M2、M3 均已实现并通过各自验收。执行环境遵守仓库 AGENTS.md：本地编辑，192.168.3.88 构建与非实盘验证，43.130.234.54 受控实盘。

## 实现与职责

| 工作包 | 实现与边界 | 验收 |
| --- | --- | --- |
| T14 占用与部署 | A 在同一 SQLite 事务内保留规范账户/品种及订单命名空间占用；N 从预登记构建节点流式下载、校验和原子发布；原生签名、摘要、ABI、平台检查复用 E 加载器 | 并发启动、幂等、配置漂移、篡改、保留旧版本、旧运行迟到证明、失败启动的未激活证明、受管理配置 CLI 旁路拒绝 |
| T15 生命周期 | 在 M2 同一 IPC 增加控制及回执查询；N 转发已有 StrategyAdminService operation，A/U 展示实际结果；独立 worker 跨 A/N 重启继续运行 | Pause/Resume/Stop、重复 Stop 原 deadline、回执丢失收敛、只读查询不补执行、停止预留回执容量及只读用户 |
| T16 固定 checkpoint 恢复 | N 登记文件 SHA256，E 从同一份字节验证并执行公共恢复；沿用原 boot ID/CLOCK_BOOTTIME 预算；恢复期间保留账户占用 | 错误 checkpoint、缺终态、累计成交不符、关闭交易门、旧证明不能释放新占用、代次增加及原期限保持 |
| T17 验证与发布 | 冻结服务器构建物和配套 Web；完成原生探针、实际浏览器、M1/M2 回归以及指定账户有限验收 | 下列日志、发布摘要及实盘统计 |

平台 execution envelope 使用精确整数计入持仓绝对量、旧挂单剩余量与同批新增量，旧挂单按各自限价保守计价；要求新鲜盘口、价格保护和意图先持久化再派单。策略的网格、成交、停止和恢复判断继续在 E。网格 1.2.1/schema 3 增加显式 `pause_safe`：暂停持续消费事实和定时器，host 限制开仓，保护性退出继续执行。

## 编译服务器结果

隔离目录：`/home/dominolu/dev/titan-management-20261023-5f424533`。工具链沿用 M1/M2；PureAdmin 上游仍固定 `f0ff132561ab684bb78379239adf29f9a38ac7f1`。

- 全仓库 `cargo test --workspace --all-features`：**556 passed、0 failed、13 ignored**（包含 doctest）。见 [汇总](checks/workspace-v20.json)、[完整日志](checks/workspace-v20.log)。未统一开启依赖真实账户或环境的 ignored 测试。
- 受影响 Rust 包 all-targets Clippy `-D warnings`、格式检查及构建通过，见 [Clippy](checks/clippy-v20.log)、[构建](checks/build-v20.log)。ts-rs 的属性解析提示沿用 M2，Serde 输入约束保持有效。
- Python SDK **113 项通过**，见 [日志](checks/sdk-final.log)；包含暂停期间成交/撤单确认与恢复运行。SDK 源码在最终发布期间保持一致。
- 最终 Web 类型、静态检查和生产构建通过，见 [类型](checks/web-types-v20.log)、[静态检查](checks/web-lint-v20.log)、[构建](checks/web-build-v20.log)。
- 实际原生网格探针通过暂停、成交/撤单竞态、reduce-only 退出、持仓确认、重复停止与恢复拒绝，见 [探针](checks/grid-probe-v20.log)、[制品构建](checks/grid-probe-build-v20.log)。
- 实际 A/N/E、原生签名制品和 loopback 模拟交易所的 **18 项检查通过**，见 [结果](checks/control-results.json)、[过程](checks/loopback-17.log)。包含错误 checkpoint 后显式重试、1024 个普通控制回执后的 Stop 预留名额。
- 实际浏览器验证部署、启动确认/取消、暂停、恢复运行、停止和 reader 权限通过，页面异常为零，见 [结果](browser/browser.json)、[部署页面](browser/start-confirmation.png)、[暂停页面](browser/paused.png)、[停止页面](browser/stopped.png)。
- 最终配套程序回归 M1 实际 describe/build/prepare/backtest/report、权限、失败、取消、重启及页面流程，见 [结果](checks/offline-results.json)。M2 只读事实、历史、诊断确认、失联、慢节点及 reader 页面回归通过，见 [结果](checks/observation-results.json)。十八组历史回放仍以 M1 的独立证据为准，本轮重新验证离线工作流。

编译与有限控制验收顺序执行。M3 模拟交易所只监听 loopback，编译服务器不连接真实账户。实盘候选制品另外以 [固定参数](live/parameters.json) 经正式管理构建流程编译签名，prepare-only 验证未建立账户或交易连接，见 [日志](checks/live-artifact-v2.log)。

## 发布身份

先复制、strip 程序，再从冻结程序导出 DTO 并重建 Web。冻结目录为编译服务器 `verification-artifacts/management/m3/release-v2`，实盘对应 `m3-release-v2`；见 [摘要清单](checks/release-v2-sha256.txt)。

| 产物 | SHA256 |
| --- | --- |
| `bin/titan` | `8b0ccebd2f8d981dc90fb81460000709065fb3ce865fc0b6c3e43f03fb766a84` |
| `bin/v13_grid_history` | `1e9f620aafe4bfcbb883d6df7efa42f87ee89d17c69aa4986e12dbb36d73ed73` |
| 签名实盘网格 bundle | `902befcdc0effcb70e5b6132ac51e8520e3b8813268d601e8ad349fff560076f` |
| `m3-release-v2.tar.gz` | `8201c61b0b6a688167c93c2820e5d80517f93fd964b1bcaa8418bff43c486ea0` |

发布包仅含服务器产物、Web、签名制品/公钥元数据、源码摘要及验收脚本。签名私钥留在编译服务器；账户凭据、完整账户快照和账本留在实盘节点私有目录。

## 指定实盘账户结果

用户确认的目标为 **43.130.234.54、CrossEx gate（ID 1）、GATE_FUTURE_BTC_USDT、最大敞口 100 USDT**。受控配置为新鲜盘口偏离不超过 **50 bps**、原始总期限 **120 秒**、最小数量 **0.0001 BTC**，制品停止方式为 flatten。实盘前确认账户模式 SINGLE、品种规则、BTC 无既有挂单/持仓、无旧 BTC 控制器及 Titan 进程；包和程序摘要在节点核对通过。

[实盘结果](live/results.json)：全部通过，交易验收耗时 **23.348 秒**。七个管理操作均 succeeded，覆盖签名部署、启动、暂停、恢复运行、第二次暂停、受控崩溃后的固定 checkpoint 恢复以及停止。A/N 重启保持同一 worker；恢复从代次 1 增至 2，两个代次的 `expires_at_ns` 完全相同。

[脱敏账本核对](live/audit-summary.json)：六笔最小数量 post-only 委托均已取消，18 条订单事实，累计成交量及独立成交记录均为零。订单事件实际使用独立 source ID 3。旧崩溃代次保留 UNKNOWN；新代次 Stopped，E 给出安全释放证明。A 占用行数为零，Titan 活动进程为零。

[后置检查](live/postflight.json)：**BTC 挂单 0、持仓 0**。本轮没有真实成交；成交竞态与有仓位平仓屏障由原生探针和 SDK 场景覆盖。账户绩效、资金费/资金流覆盖和策略归属继续按 E 的完整性输出展示，不把本轮结果解释为完整策略净收益或连续回撤。

## 修复记录与限制

首次 M3 模拟发现行情/账户各自按逻辑 ID 生成相同 source ID，私有订单被事件引擎按重复序号丢弃；见 [修复前记录](failure/private-fact-loss.log)。现由事件引擎统一分配有界、生命周期内不复用的流 ID，替换连接分配新身份，策略绑定读取实际流 ID；对应回归与本轮实盘订单事实均通过。

构建与控制并行时慢盘触发 checkpoint flush deadline，见 [失败关闭证据](failure/slow-disk.log)。保留 fsync 和派单前意图屏障，按实际持久化延迟配置验收 handler/checkpoint 周期，减少重复回执写盘与心跳开销，并将构建与控制验收顺序执行。E 最终事实在拆除运行时前落盘；失败和 UNKNOWN 保留占用，不清空状态换取通过。

运行、信任配置、回滚和恢复步骤见 [运行说明](../../../../management_console_operations.md)。回滚前保留 Registry、事实账本、checkpoint、原操作及回执，使用配套前后端；旧 host 会拒绝新的暂停能力位。
