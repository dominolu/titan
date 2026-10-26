# 策略参数、构建制品、历史回测与账户风险改造

日期：2026-10-26。沿用用户已确认的[工作台视觉方向](../../../design/management-console-v0.1/README.md)，承接[运行总览与策略详情](../management-console-ui/README.md)。

## 范围与功能对照

| 页面 | 实现与保留功能 |
| --- | --- |
| 策略参数 `/welcome` | 模板选择与源摘要、JSON/描述接口驱动的表单编辑、精确整数输入、参数校验、草稿创建/修改/复制、冻结版本、两版本参数比较、搜索/状态筛选与构建入口 |
| 构建制品 `/artifacts` | 制品目录、摘要与来源、搜索、制品下载、授权绑定及风险边界预览、部署、部署来源、启动前确认与取消 |
| 历史回测 `/backtests` | 制品和历史场景选择、数据准备、压力执行开关、回测、引擎全量指标与有界曲线、结果筛选/切换、失败残留提示、原始结果下载、报告生成与隔离预览 |
| 账户风险 `/accounts` | 授权来源选择、原币权益/净收益/回撤、逐品种风险与采样最大值、独立成交数、完整订单、分方向持仓与余额、九类历史、权益与费用、业务状态/checkpoint、节点诊断、告警知悉和宿主日志 |
| 公共入口 | 统一导航、会话、连接和重同步、历史查询、待确认原键重试、操作记录/取消请求/回执/日志；运行总览和策略详情继续保留 |

公共深色导航、浅色内容、青绿主动作、指标排版、表格和窄屏布局沿用首批实现。没有更换框架、组件库或锁文件，没有新增后台接口、任务系统、授权实现或交易规则。

四个页面分别位于 `StrategyParameters.vue`、`BuildArtifacts.vue`、`HistoricalBacktests.vue` 和 `AccountRisk.vue`。移除旧的多页面 `index.vue` 和旧账户观测面板；`HostLog`、`AccountSnapshot` 同时供策略详情与账户风险使用，`FactHistory`、`EquityHistoryChart`、`RiskCard`、`RunAlerts` 和 `OperationsPanel` 继续复用。`useConsoleWork` 仅处理当前页面请求忙碌、回执展示与迟到响应；真正的受理、幂等与状态仍归原 Store 和服务端。

## 接口与数据边界

- 校验、构建、部署、启动、数据准备、回测和报告统一通过原 `Action`/`Operation`。保存草稿与冻结仍使用原参数版本接口，保留 `expected_revision` 和校验操作 ID。
- 编辑器按模板身份切换，不因普通目录/SSE 刷新丢失已编辑输入。修改参数、模板或草稿身份后，冻结前需要重新校验。缺失参数与显式 `null` 在版本比较中分别表达。
- 启动确认列出部署的账户、品种、最大敞口、价格保护、期限与停止方式；确认后再次检查会话、权限、部署和绑定身份。取消确认不提交操作。
- 显式选中的模板、结果和账户来源缺失时提示重新选择，不暗中换成另一个资源。页面选择加入现有有界缓存的固定资源集合；来源、代次、会话或组件结束后的迟到历史/日志/写响应不覆盖当前页面。
- 账户原币种指标与回测 USDT 指标分别展示。账户风险和账户权益不能作为策略收益；不跨币种合计，不合并 LONG/SHORT。过期或证据不完整的当前风险保持缺失，历史最大值只称持久化采样最大值。
- 数字卡片仅格式化显示，原始精度可在标题、明细和完整结果中查看；图表使用原有有界采样，收益、回撤与风险不从图表重算。切换结果先清除旧曲线，报告继续使用后端文件白名单和无脚本 sandbox。
- 所有写按钮受原 operator 权限约束，服务端仍做最终授权；HTTP 受理、取消请求与业务完成分别表达。

## 验证环境

源码基线 `0099c1fb566331a2ac3d43a8ef0f4b7ac586daba`，另加本轮完整前端源码摘要（包含上一批页面）。服务器目录：

```text
192.168.3.88:/home/dominolu/dev/titan-console-pages-20261026-0099c1fb
```

创建前已确认目录不存在，并核对上一批服务器源码摘要；从 Git 基线导出后叠加当前源码。服务器常用 `/home/dominolu/dev/titan` 目录不带 Git 元数据，因此未将其假定为干净检出，也未覆盖该目录。依赖复用原 lockfile 的已安装目录。

本地只编辑、审查和校对源码摘要。所有格式化、类型、lint、构建及测试均在编译服务器执行；未连接真实账户、未发布到实盘服务器。Rust、DTO 与后台协议未改，沿用已验收并匹配前端 `EXPECTED_BUILD` 的程序：

```text
titan SHA256 = fee02111897d57b7919655d654b22558fd4788890afd9f517254d37d362c043e
```

前端产物在本轮编译服务器构建；M1 使用已有公开历史行情，M2 使用 E 生成的合成快照，M3 使用真实原生 worker 与回环模拟交易所。

## 验证结果

最终汇总：[summary.json](checks/summary.json)。以下结果均来自最终同一份前端构建，各浏览器测试的 `pageErrors` 均为空。

| 验证 | 结果与证据 |
| --- | --- |
| TypeScript、Vue 类型与 Prettier | 通过，[check.txt](checks/check.txt) |
| ESLint | 修改页面与路由零警告通过，[lint.txt](checks/lint.txt) |
| 缓存与展示 | 600 次资源更新、精确整数/单位、来源身份、原生状态、缺失和过期事实通过，[cache.txt](checks/cache.txt)、[presentation.txt](checks/presentation.txt) |
| 前端生产构建 | 通过，[build.txt](checks/build.txt)、[产物摘要](checks/frontend-sha256.txt) |
| M1 | 5 项流程、16 项浏览器检查、6 项故障注入通过，[流程](checks/m1/results-ui.json)、[浏览器](checks/m1/browser/browser.json)、[故障](checks/m1/faults.json)、[日志](checks/m1-accepted.txt) |
| M2 | 6 项协议/页面流程、8 项浏览器检查通过，[流程](checks/m2-accepted/results.json)、[浏览器](checks/m2-accepted/browser.json)、[日志](checks/m2-accepted.txt) |
| M3 | 21 项原生/页面流程、8 项浏览器控制检查通过；模拟交易所残留订单为 0，[流程](checks/m3-accepted/results.json)、[浏览器](checks/m3-accepted/browser/browser.json)、[日志](checks/m3-accepted.txt) |
| 交付一致性 | 35 个源码文件摘要一致，旧混合页面与账户面板已移除；[清单](checks/source-sha256.txt)、[删除清单](checks/deleted-sources.txt)、[服务器复核](checks/source-identity-check.txt)、[本地交付复核](checks/local-source-identity-check.txt) |

M1 浏览器重新执行草稿创建/修改/复制/比较、表单与 JSON 编辑、校验失效、冻结、构建、制品下载与搜索、数据准备、压力开关、回测、报告、四页窄屏与只读权限、断线重连。默认模板参数初始化和目录刷新保留输入有独立断言。M2 检查账户权益、完整订单与分方向持仓、事实历史、节点诊断、日志、告警、过期风险和迟到响应；M3 重新验证新制品页的受控部署、启动确认/取消及原策略详情暂停、恢复运行、停止和只读控制。

首轮在本次全新目录完成单组真实历史编译、回放和报告；最终轮使用 `--reuse --browser` 复用该目录内的不可变输入与准备回执，重新执行页面发起的构建、回测、报告及故障注入。此次未重跑旧的 18 组历史行情矩阵或全部 Rust 单元测试，也不将旧证据当作新页面已验证的证明。

### 实际构建截图

以下使用公开历史行情与回环合成账户，不是实盘账户：

- [策略参数](browser/parameters.png)、[构建制品](browser/artifacts.png)、[历史回测](browser/backtests.png)、[账户风险](browser/accounts.png)。
- [已选制品与授权绑定](browser/deployment.png)、[启动确认](browser/start-confirmation.png)、[只读权限](browser/readonly.png)、[账户观测过期](browser/stale-account.png)。
- 390px 窄屏：[参数](browser/mobile-welcome.png)、[制品](browser/mobile-artifacts.png)、[回测](browser/mobile-backtests.png)、[账户](browser/mobile-accounts.png)。四页根容器和文档均未产生水平溢出，完整表格在各自容器内滚动。

### 复现入口与本轮修复

[toolchain.txt](checks/toolchain.txt)记录工具版本和程序身份。前端检查在上述服务器源码目录的 `web/console` 中执行：

```sh
pnpm check
pnpm exec eslint src/views/console src/router/modules/home.ts src/views/login/index.vue --max-warnings 0
pnpm test:cache
node ../../scripts/management_run_view_test.mjs
pnpm build
```

业务入口为 `scripts/management_console_smoke.py --browser`、`scripts/management_observation_smoke.py --browser`、`scripts/management_control_smoke.py --browser`，均指定本轮 `--repo`、已验收的 `--binary` 和独立 `--output`。M1 的 `--history` 使用同一已验收目录中的 `v13_grid_history`，`--data` 为 `/home/dominolu/dev/titan-grid-history-20261022-2aafc598/data/grid-history`，`--parameters` 为该历史验收目录下的 `verification-artifacts/grid-history/final/artifacts`。浏览器使用 `TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome`。

本轮补齐默认模板首次进入时自动填入参数、普通目录刷新不重置输入、输入变更使冻结校验失效、来源固定及会话响应保护；启动确认后再次核对授权绑定和部署身份，切换回测结果先清除旧曲线。账户日志、快照与历史复用公共组件，消除旧面板的重复实现。

回归开发中曾修正临时测试参数字段、相对下载 URL，以及 Element Plus switch 隐藏 input 的定位；最终通过可见包装控件操作并断言其真实状态，未使用强制点击绕过交互。只归档截图、结果、日志和摘要白名单，未复制测试会话、签名私钥、临时配置或数据库。
