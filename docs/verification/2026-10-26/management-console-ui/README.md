# 管理后台首批页面改造

日期：2026-10-26。视觉基线为用户已确认的[高保真设计 v0.1](../../../design/management-console-v0.1/README.md)，源码基线为 `0099c1fb566331a2ac3d43a8ef0f4b7ac586daba` 加本轮源码清单。

## 改造范围

- 新增运行总览 `/runs`，支持选择观测来源、实例搜索、状态筛选、账户权益、逐品种风险和告警。
- 新增策略详情 `/runs/:id`，接入运行概览、账户订单/持仓、成交与九类历史事实、冻结参数/制品/checkpoint、操作记录、宿主日志和完整诊断。
- 使用已确认的深色导航、浅色内容与青绿色主动作；桌面保持主内容和风险侧栏，窄屏纵向排列并保留移动导航和完整表格。
- 登录后进入运行总览。策略参数、构建制品、历史回测和账户风险保持现有业务功能，共用新的导航、会话和历史查询入口。
- 移除旧运行页面的重复控制实现。会话、请求、SSE、缓存、幂等键、权限和管理操作仍复用现有接口与 Store；未引入另一套前端业务状态机。

主要入口：[布局](../../../../web/console/src/views/console/ConsoleLayout.vue)、[运行总览](../../../../web/console/src/views/console/RunsOverview.vue)、[策略详情](../../../../web/console/src/views/console/RunDetail.vue)、[展示适配](../../../../web/console/src/views/console/run-view.ts)、[历史请求](../../../../web/console/src/views/console/use-fact-history.ts)。

## 数据与控制边界

界面展示实际接口能够证明的字段，不将设计示例作为生产数据：

| 内容 | 本轮实现 |
| --- | --- |
| 权益与回撤 | 账户级、原币种；多币种不相加，回撤不转造百分比 |
| 网格配对收益 | 直接显示 E 的 `grid_profit_tick_lots`，单位为 tick·lot；不自行计算 USDT 收益 |
| 策略净收益 | 当前接口未提供可靠策略归属，显示缺失并说明原因 |
| 风险上界与最大值 | 直接使用 E 的确认金额和持久化采样最大值；过期或不完整时当前值缺失 |
| 权益曲线 | 查询 `equity_sample`，展示最近一页最多 20 条；不伪装成已有完整 24h/7d 查询。绘图可用浮点数，明细和提示保留原始值 |
| 订单与持仓 | 按账户元数据精确展示 tick/lot 数量，保留原始值，不合并 LONG/SHORT；原生账户状态码与 Python 回调枚举分别处理 |
| 生命周期控制 | 只展示后端 `allowed_actions`；只读禁用，暂停/恢复运行在观测受限时禁用，仍保留后端允许的停止和 checkpoint 恢复 |
| 确认与回执 | 确认前后核对实例、代次、会话和 checkpoint；受理不等于完成，不提前清零委托或持仓 |
| 普通操作互斥 | 同实例有在途请求、未确认受理或未结束回执时等待原操作；停止入口不被其他普通操作的忙碌状态禁用 |
| 来源切换 | 缺失实例不会回退到另一个实例；历史、日志的迟到响应不能覆盖新来源 |
| 会话切换 | 新页面及保留的账户页告警确认、操作取消均防止旧会话响应写入当前缓存 |

原始诊断、账户权益/费用、完整业务状态、余额和全部字段仍可展开查看。告警知悉与恢复成功分别表达；原期限、账户占用、停止屏障和风控规则继续由平台确认。

暂停状态明确展示“新增开仓已暂停，成交与保护性处理继续”。`command_gate_open` 表示命令通道可接收处理，不被界面解释成暂停时仍允许开仓。制品摘要只显示 runtime 确认的制品身份，不拿部署资源摘要代替。路由切换重建页面并重置内容区滚动位置。

## 验证环境与证据

验证仅在 `192.168.3.88:/home/dominolu/dev/titan-console-ui-20261026-0099c1fb` 的独立目录进行。本地只编辑和审查，未执行项目构建、代码生成、格式检查或测试。未覆盖服务器既有修改。

本次没有修改 Rust、DTO 或后台协议，使用服务器已有、与前端 `EXPECTED_BUILD` 匹配的程序：

```text
titan SHA256 = fee02111897d57b7919655d654b22558fd4788890afd9f517254d37d362c043e
```

前端生产产物由本轮编译服务器构建。所有交易连接均指向回环模拟交易所；M2 使用 E 生成的合成账户快照，M1 使用服务器已有公开历史行情。不连接真实账户，不进行实盘部署。

最终结果见 [summary.json](checks/summary.json)，各浏览器测试的 `pageErrors` 均为空：

| 验证 | 结果与证据 |
| --- | --- |
| TypeScript / Vue 类型与 Prettier | 通过，[check.log](checks/check.log) |
| ESLint | 修改页面及路由零警告通过，[lint.log](checks/lint.log) |
| 缓存与展示精度 | 600 次缓存更新通过；大整数、单位、身份、缺失/过期事实和原生订单状态通过，[cache.log](checks/cache.log)、[run-view.log](checks/run-view.log) |
| 前端生产构建 | 通过，[build.log](checks/build.log)、[产物 SHA256](checks/frontend-sha256.txt) |
| M1 | 5 项流程、8 项浏览器检查、6 项故障注入通过，[流程](checks/m1/results-ui.json)、[浏览器](checks/m1/browser/browser.json)、[故障](checks/m1/faults.json) |
| M2 | 6 项协议/页面检查通过；含历史分页、九类事实、账户诊断、知悉、过期、重连、只读、缺失实例、迟到响应和 390px 布局，[结果](checks/m2-release/results.json)、[浏览器](checks/m2-release/browser.json) |
| M3 | 21 项原生/页面检查、8 项浏览器控制检查通过，模拟交易所残留订单为 0，[结果](checks/m3-release/results.json)、[浏览器](checks/m3-release/browser/browser.json) |
| 源码一致性 | [源码清单](checks/source-sha256.txt)的 23 个文件逐项复核通过，[服务器复核](checks/source-identity-check.txt)、[本地交付复核](checks/local-source-identity-check.txt) |

M1 最终轮复用本次独立测试目录内已有的不可变输入与准备回执；浏览器重新执行参数校验、冻结、构建、回测与报告，并执行故障注入。此次没有重跑旧的 18 组策略历史行情矩阵，也没有因前端改造重跑全部 Rust 单元测试。

浏览器实际截图来自本轮构建与回环测试接口：

- [运行总览](browser/overview.png)、[运行中的策略详情](browser/running.png)。
- [已暂停](browser/paused.png)、[停止后](browser/stopped.png)、[启动确认](browser/start-confirmation.png)。
- [观测过期](browser/stale-run.png)、[移动端详情](browser/mobile-run.png)。

### 复现入口

[工具版本](checks/toolchain.txt)记录 Node、pnpm、Python、浏览器与 Rust 程序摘要。服务器依赖复用既有相同 lockfile 的安装目录，未更换组件库或引入新运行依赖。在上述独立源码目录执行的前端检查为：

```sh
cd web/console
pnpm check
pnpm exec eslint src/views/console src/views/login/index.vue src/router/modules/home.ts --max-warnings 0
pnpm test:cache
node ../../scripts/management_run_view_test.mjs
pnpm build
```

业务回归入口为 `scripts/management_console_smoke.py --browser`、`scripts/management_observation_smoke.py --browser` 和 `scripts/management_control_smoke.py --browser`。均指定本轮 `--repo` 与已验收的 `--binary`；M1 使用前述公开行情和冻结参数目录，M2 使用合成 E 快照，M3 自建回环交易所和真实原生 worker。最终日志为 [M1](checks/m1-release.log)、[M2](checks/m2-release.log)、[M3](checks/m3-release.log)。截图和结果只归档白名单文件，测试会话、签名私钥和临时配置没有复制进仓库。

### 本轮修复

- 历史请求按实例、运行代次、历史类型和会话监听；SSE 更新资源对象不重置翻页。来源从不可用恢复时可重试，旧来源或旧会话的迟到请求不能写回。
- 同实例普通控制在请求或受理结果未确认时互斥；独立停止仍可用。用延迟 Pause HTTP 回执的浏览器场景验证，不把受理成功当作业务完成。
- 订单状态使用原生账户枚举，tick/lot 数量按账户元数据精确转换；缺失单位、来源或风险证据保持缺失。
- 恢复原页面的历史完整性提示、节点连接原因、业务状态、完整事实与宿主日志入口；暂停文案区分命令通道与新增开仓能力。

首轮和第二轮 M2 页面测试分别暴露了自动化脚本对 Element Plus 下拉框的重复可访问标签、内部只读输入框遮挡问题，已改为点击控件包装层；没有绕过可见性或强制点击。首次同步的 macOS 附带元数据文件只在本轮独立目录清理，后续使用明确文件同步。
