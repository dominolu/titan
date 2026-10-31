# 项目精简重构清单

日期：2026-10-31。基线：`69717434b6dca1c3f461d9ea451583a0654260e8`。先审查调用关系，再逐项实施和远端验证。用户已有跨所策略、研究文档和工程记忆修改保留，不作为本轮删除对象。

## 已确认的实施项

| ID | 问题与证据 | 实施方式 | 验收 | 状态 |
| --- | --- | --- | --- | --- |
| R01 | collector 三份 `fetch_symbol_list` 只有定义，均用 dead_code 豁免；CLI 使用显式 symbols | 删除未接入的自动发现函数及豁免 | collector 编译、Clippy | 已完成并验证 |
| R02 | Binance spot/UM/CM 的连接、Ping/Pong、重连循环几乎完整复制 | 共用传输模块，各市场只声明端点及原心跳参数；URL 在重连前构造一次，去掉循环中的多余 collect/字符串复制 | 服务器回环 WS 模拟：文本、Ping/Pong、关闭、超时；全工作区检查 | 已完成并验证 |
| R03 | 三个重连循环先判断 >3，再 else-if >10/>20，后两分支不可达 | 删除不可达的 5s/10s 分支，保持当前有效的一秒退避及 30s 重置语义 | 重连边界用例及 collector 检查 | 已完成并验证 |
| R04 | v2 handler 的 public_version 与 management_response 中间件重复过滤私有元数据，规则还不一致（state_description） | handler 在序列化前投影公共资源，复用 HTTP/SSE 的同一过滤器；不维护第二份字段规则 | 65 个 64KiB 状态版本的真实路由分页/详情、SSE 与下载逐字节回归 | 已完成并验证 |
| R05 | OKX 批量撤单对不可变 resp.code 连续检查两次，chunks(20) 内空批次分支不可达 | 删除第二次顶层检查和空批次判断，保留逐单结果与 51401 原语义 | connector 全特性测试/Clippy | 已完成并验证 |
| R06 | StrategySpec 构造器与归一化后的 __post_init__ 重复 HOST_DEPTH_100 校验；前者会提前消费生成器订阅 | 只保留归一化后的一次校验 | SDK 正负例及生成器订阅回归 | 已完成并验证 |
| R07 | console store 的四个历史目录 getter 无消费者；实际页面已按策略分页查询 | 删除 parameterPresets/parameterVersions/strategyVersions/backtests getter | 前端类型及现有页面回归 | 已完成并验证 |
| R08 | ECharts 模板插件从未接入，main 仅有注释；两个真实图表直接按需注册 | 删除未使用插件和入口注释 | 前端类型与构建、两图独立注册代码审查 | 已完成并验证 |
| R09 | log/history/facts 三次复制运行/观察会话身份映射 | 提取同一查询身份 helper，保留原字段和值 | 两种模式请求参数回归及前端检查 | 已完成并验证 |

## 本轮结果

R01–R09 及 review 确认的 P2/P3 均已修复，整合 GitHub 上游 TLS 更新后，在 `192.168.3.88` 对精确待提交树完成验收：Rust 736 passed / 26 ignored，doctest 2 passed / 1 ignored，SDK 120 passed，报告 5 passed；大列表真实路由回归、采集器定向回归、10 次可控心跳、全特性 Clippy `-D warnings`、前端类型/格式/lint/缓存/构建、40 条 API 请求断言及既有页面回归全部通过。SDK 数量不含用户原有未提交跨所策略及其 35 项测试。

最终命令、日志、冻结清单与范围见[Review 修复与提交验收](verification/2026-10-31/refactor-simplification-review-fixes/README.md)；[首次验证记录](verification/2026-10-31/refactor-simplification/README.md)保留历史结果，不能代替最终合入验收。采集器新 5 项回归和 HTTP 边界回归包含在 Rust 数量中；本轮无实盘连接、下单或部署，不将忽略项标为通过。

## 审查原则与范围

- 平台能力以 Rust 为主；Python 保留 Numba 策略和必要编译桥接；报告保持现状。
- 简化重复保护层和不可达分支；账户事实、未知执行结果、恢复身份、持久化确认、授权、请求代次及数量价格准入仍按实际责任校验。
- 不以未见页面入口判断公开 API 无用；历史记录适配、冻结字节与签名语义有明确消费者，需保留。
- 不迁移用户正在修改的跨所策略，不执行实盘访问或部署。
- 本地仅编辑与审查；格式、静态检查、构建、单元和非实盘集成测试全部在 `192.168.3.88` 的新隔离目录执行。验证前记录源码摘要，结果完成后逐项更新本清单。

## 并行审查

已完成 Rust 平台、管理前端和 Python/工具链三路只读审查，确定 R01–R09。未采用 use-strategy 的异步 pin 抽象：两入口授权及解析语义不同，收益有限且会增加回调抽象。未删除历史 M3 脚本：它仍属于既有证据的复现入口，硬编码环境应另作验证工具治理；本轮不把已用验收能力归为无用功能。Python 正式 describe/emit-objects/test-native 桥接和历史管理适配仍有明确调用，继续保留。
