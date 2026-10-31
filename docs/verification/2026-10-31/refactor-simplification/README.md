# 项目精简重构验收

此为首次阶段的历史记录。后续 review 发现大版本列表响应上限回归和心跳测试调度依赖，最终合入状态以[Review 修复验收](../refactor-simplification-review-fixes/README.md)为准。

2026-10-31。对应[重构清单](../../../refactor_simplification_checklist.md) R01–R09，九项均完成。所有格式化、Clippy、构建和测试在 `192.168.3.88:/home/dominolu/dev/titan-simplify-20261031-69717434/src` 执行；本地仅编辑、审查、同步和归档。

## 输入与范围

- 基线 `69717434b6dca1c3f461d9ea451583a0654260e8`，加本轮未提交代码；没有 pull、重置或覆盖用户既有跨所策略和文档修改。
- [初始清单](source-inputs.json)含 759 个明确文件，排除历史验证归档、本地构建目录、node_modules、运行目录及秘密。已有受版本管理的策略 fixture 保留；新输入包括共享采集传输与请求回归脚本。SDK 全量测试还使用用户原有未提交的跨所策略及测试，未修改这些文件。
- [格式化后冻结输入](source-frozen.json)与[验收后输入](source-after.json)完全一致。远端格式化只涉及共享传输与 HTTP 测试两个文件，已取回本地；验收后仅更新清单、文档入口、工程记忆和本证据说明。
- 生产实现净减少 338 行：统计已跟踪实现差异，剔除新增 HTTP 测试，计入新共享模块在 `cfg(test)` 之前的实现；测试、验证脚本与文档单列，不宣称现有前端包体缩小或未经测量的性能提升。

## 实施与证据

| 项目 | 结果与保留语义 | 验证 |
| --- | --- | --- |
| R01 | 删除三个无调用的 fetch_symbol_list 及 dead_code 豁免；显式 symbols 和深度快照保持 | [全工作区 Clippy](logs/clippy.log)、[Rust](logs/rust.log) |
| R02 | 三份 Binance WS 收敛为共享传输；URL 构造一次，端点、接收时间、队列背压与 spot 5s/30s、futures 10s/300s 保持 | [Rust 内 5 项新回归](logs/rust.log)：URL 顺序/空列表、文本/心跳/关闭、心跳续期/超时、重连/接收端关闭 |
| R03 | 删除不可达 >10/>20 退避；前三次不等、第四次起 1s、连接严格 >30s 清零保持 | [退避边界回归](logs/rust.log)覆盖 1–24 次和 30s/30s+1ns |
| R04 | handler 删除 public_version，普通 JSON 共用响应中间件，SSE 继续直接调用同一过滤器；下载路径旁路不变 | [HTTP 边界与既有历史 SSE 回归](logs/rust.log)：私有恢复字段/投影去除、公有 schema/身份/摘要保留；下载 JSON 内容不被过滤。生产路由挂载与下载直接返回另经人工审查 |
| R05 | 删除 OKX cancel_all 的第二次顶层检查及 chunks 后空批次分支；第一次顶层、逐单 0/51401、20 单分批及超时保持 | [全特性 Clippy](logs/clippy.log)、[既有 connector 回归](logs/rust.log)；仅删除由前置条件证明冗余的判断，未新增交易所实盘验收 |
| R06 | HOST_DEPTH_100 只在订阅 tuple 归一化后校验；修复生成器被提前消费 | [155 项 SDK](logs/sdk.log)含生成器完整保留与非法/缺失深度拒绝，既有 LATEST 负例保留 |
| R07 | 删除四个无调用 store getter；页面继续用策略归属查询 | [页面回归](logs/web-research-view.log)、[缓存](logs/web-cache.log)、[前端检查](logs/web-check.log) |
| R08 | 删除未接入 ECharts 插件与两处入口注释；两张真实图表仍自行按需注册 | 人工核对 ResultChart/EquityHistoryChart 注册；[类型/格式](logs/web-check.log)、[构建](logs/web-build.log)，未执行浏览器绘图验收 |
| R09 | log/history/facts 共用查询身份；observe 只用 worker_session_id，其他模式只用 run_generation | [40 条生产 API 请求捕获](logs/web-api.log)：双身份、缺失、null/0/空串、大整数、路径编码、游标和分页；[运行展示](logs/web-run-view.log) |

## 门禁结果

[机器汇总](summary.json)、[退出码](logs/status.tsv)、[工具链](logs/environment.txt)。

| 门禁 | 结果 |
| --- | --- |
| rustfmt / fmt-check | 通过 |
| 全 workspace/all-targets/all-features Clippy | `-D warnings`，通过 |
| Rust 全 workspace/all-targets/all-features | 734 passed，0 failed，26 ignored |
| doctest | 2 passed，0 failed，1 ignored |
| CLI debug 构建与现有单文件发布集成 | 通过，集成测试包含在 Rust 数量中 |
| SDK | 155 passed |
| 报告 | 5 passed，功能未改 |
| 前端类型/Prettier、变更文件 ESLint、缓存、API 请求、生产构建 | 全部通过 |
| 运行展示和策略页面既有回归 | 通过，含八项选择/会话/卸载/资源固定边界 |

## 命令与诊断

主门禁入口为[本轮脚本](../../../../scripts/verify_refactor_simplification.sh)，从上述隔离源码目录运行。父目录的 `.cargo/config.toml` 显式配置 `crates-io` 使用 `sparse+https://rsproxy.cn/index/`，让 Clippy 及测试子进程继承同一缓存来源；保持 Cargo.lock，不升级依赖。Python 使用服务器既有虚拟环境，SDK/PYTHONPATH 显式指向本轮源码。前端依赖复用服务器目录。附加命令：

```bash
node scripts/management_run_view_test.mjs
node scripts/management_research_view_test.mjs
```

首次尝试因 Clippy 子进程没有继承镜像配置而找不到已缓存 phf，记录在[首次日志](logs-attempt-1/clippy.log)。修正父目录配置后，第二次尝试指出新增测试的握手回调返回大错误类型，记录在[第二次日志](logs-attempt-2/clippy.log)。删除测试中多余的握手回调，URL 仍有独立用例，回环重连测试继续保留，未添加 lint 豁免或改变产品超时。第三次完整门禁全部通过；另外一次证据采集 Python 换行转义错误已修正，不属于产品测试失败。

本轮没有真实账户读取、下撤单、实盘策略运行或部署。ignored 测试没有开启；这些结果不代表交易所实盘兼容、浏览器绘图或长期运行验收。
