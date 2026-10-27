# 策略命名与业务观测解耦验证

状态：实现、编译及全部非实盘验收通过（2026-10-27）。

## 代码与执行环境

- 本地基线：`638bae69324114e8a2447f648c8b7ced832196bb`，本轮修改尚未提交。
- 编译服务器：`192.168.3.88`；隔离源码目录：`/home/dominolu/dev/titan-observation-20261027-638bae69`。
- 该目录是源码快照，未将其当作 Git checkout。最终身份由基线、修改文件 SHA256、完整源码清单及删除文件清单共同确认。
- 本地仅编辑、审查及核对同步文件。编译、格式化、静态检查、类型生成和全部测试均在编译服务器执行。
- 使用原 Cargo.lock、pnpm-lock.yaml 和服务器既有依赖；服务器镜像配置单独保留，不用本地 macOS Cargo 配置覆盖。
- 本轮不读取真实账户、不连接真实交易所、不部署实盘。控制验收使用回环模拟交易所、合成凭据及真实 Rust 运行宿主。

## 实现与证据

策略为唯一策略业务主体。参数版本、策略版本、运行、部署、回测、报告及恢复点均归属于策略。公开代码/API/页面采用统一定义；历史 ID、冻结摘要、签名文件及恢复点保持原值，旧字段由兼容入口适配。

SDK 声明公开 schema 与私有投影，Rust 编译器按恢复布局验证投影并签名；运行宿主从已提交恢复点提取公开值，经 IPC、节点、管理器 REST/SSE 到页面。观测链路中的管理器和页面不解析网格内部恢复结构。完整接口、字段和数据流见[协议说明](../../../strategy_observation_protocol.md)。

| 门禁 | 本轮结果与证据 |
| --- | --- |
| Rust 格式与静态检查 | [cargo fmt](checks/core/rust-format.log)、[全工作区 Clippy，全部目标及 feature，`-D warnings`](checks/core/rust-clippy.log)通过 |
| Rust 测试 | [581 passed、0 failed、12 ignored](checks/core/rust-tests.log)；[doctest 2 passed、1 ignored](checks/core/rust-doc-tests.log)。合计 583 passed、13 ignored；真实交易所探针没有执行 |
| 编译 | [CLI](checks/core/cli-build.log)、[优化历史回测引擎](checks/core/history-build.log)通过 |
| Python | [SDK 115 项](checks/core/sdk-tests.log)、[报告 5 项](checks/core/reporting-tests.log)通过 |
| 类型与前端 | [DTO 导出](checks/core/dto-export.log)、[类型检查](checks/core/web-types.log)、[Prettier](checks/core/web-format.log)、[ESLint](checks/core/web-lint.log)、[生产静态资源构建](checks/core/web-build.log)通过 |
| 前端行为 | [缓存与 600 条增量事件](checks/core/web-cache.log)、[精度、单位、业务协议与代次](checks/core/run-view.log)、[Vue/Pinia 8 项参数编辑及固定资源回归](checks/core/strategy-view.log)通过 |
| 网格完整链路 | [24 项流程](checks/core/loopback/results.json)、[10 项浏览器检查](checks/core/loopback/browser/browser.json)通过；模拟订单 10 笔、残留挂单 0 |
| 历史闭环与页面 | 全新目录单次[9 项流程](checks/history/results.json)、[22 项浏览器检查](checks/history/browser/browser.json)、[6 项故障注入](checks/history/faults.json)通过，覆盖冻结、编译、回测、报告、跨策略选择拒绝和移动端 |
| 账户共性数据 | [6 项只读流程](checks/readonly/results.json)、[8 项浏览器检查](checks/readonly/browser.json)通过，含断线降级、迟到响应和权限 |
| 查询与受理隔离 | [12,000 条、每条 32 KiB 历史](checks/query/results.json)通过；128 字符身份可查询，并发读取及草稿写入最大约 0.044 秒，本次观测不作为 SLA |
| 旧签名策略 | [使用最终程序验证上一轮签名文件](checks/legacy-strategy-validation.json)通过，文件 SHA256 不变 |

网格验收核对了策略归属、策略版本、运行编号 `77`、运行代次、公开 schema hash、恢复结构版本、REST/SSE 和页面。它还覆盖签名/ABI/摘要、传输篡改、配置漂移、失去启动/控制确认后查询同一回执、管理器与节点重启、进程崩溃、错误恢复身份拒绝、未决订单只读补查、恢复沿用原期限及安全停止释放占用。

新增独立性回归实际改变私有字段名称、偏移和恢复 hash，确认公开 schema hash 和输出不变，并验证大整数精度、路径/类型/边界拒绝、空值条件、未知枚举及非有限值。新增兼容回归覆盖存量 SSE 事件规范化、原始恢复业务状态停止导出、原请求哈希与控制回执身份别名，歧义字段拒绝。

[源码修改清单](checks/source-sha256.txt)、[完整源码清单](checks/all-source-sha256.txt)、[删除文件清单](checks/deleted-files.txt)和[源码身份](checks/source-identity.json)记录最终交付；[本地同步核对](checks/local-source-identity.json)确认交付文件与服务器一致。[进程收尾核对](checks/final-cleanup.json)确认本轮验证进程均退出。

## 公开数据样例

- [版本公开 schema 与 hash](checks/core/loopback/business-observation-schema.json)。
- [按策略、运行查询的实际快照](checks/core/loopback/business-observation.json)：业务协议 `titan.grid/v1`，恢复结构版本 `3`，二者分别表达。
- [实际 SSE 业务更新](checks/core/loopback/business-observation-sse.json)。
- [实际业务观测页面](checks/core/loopback/browser/business-observation.png)。

公开 schema 不含内部路径、偏移、初始状态或恢复布局。恢复兼容仍校验策略编译内容、绑定、状态结构及校验和；观测协议相同不放宽恢复条件。

## 复现与构建身份

在编译服务器的隔离目录执行：

```bash
export CARGO_TARGET_DIR=/tmp/titan-deplugin-target
export CARGO_BUILD_JOBS=8
export TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome
bash scripts/verify_strategy_observation.sh evidence/new-verification
```

追加验收命令见 [历史闭环](checks/history/command.txt)、[账户只读](checks/readonly/command.txt)及[历史查询](checks/query/command.txt)。

Python、SDK 与报告路径由脚本显式设置；每个步骤的命令保存在 `checks/core/*.command.txt`。[工具链](checks/core/toolchain.txt)为 Rust/Cargo 1.94.1、Node 22.22.0、pnpm 10.32.1、Python 3.11.14。

[构建 SHA256](checks/core/build-sha256.txt)：

```text
titan = 65f8a1b1a9b326c757704ddabf7fba8780b0331a5a3eaaeab6da6ceac9512acc
v13_grid_history = 5c12b2fa76f08dccee45140fa07d7dccc2de4524a113488c3efb447c7b810218
```

管理 DTO 中的 `EXPECTED_BUILD` 对应上述 CLI SHA256，前端与服务器配套。CLI 为本轮非实盘验收的 debug 构建；本轮没有生成或部署实盘发布版本。

## 验证范围

业务观测来源为最新已提交恢复点，更新时效受提交间隔、宿主采样与管理器同步周期影响。本轮验证协议与链路正确性，不提供逐 tick 时延或生产负载 SLA。账户事实、费用、资金费和风控继续由平台提供，网格配对毛收益没有转换成策略净收益。

报告保持冻结的 ResultBundle v1 文件契约，在历史引擎中显式适配源码类型和源码版本字段。原生 ABI V13 布局及指纹没有升级。历史验证记录仅用于定位旧文件，不作为本轮通过证据。
