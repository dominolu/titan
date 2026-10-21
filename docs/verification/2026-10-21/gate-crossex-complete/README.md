# CrossEx 接口补齐与最终回归

> 本文保留该阶段历史结果；后续修复、507 项回归及真实 Gate 被动订单验收见[实盘报告](../gate-crossex-live/README.md)。

2026-10-21。**原项目 API 调用的代码迁移已补齐，四项 P1 已修复；真实账户验收尚未完成。** 能力、映射与配置见[broker 说明](../../../gate_crossex_broker.md)。此记录取代首版的完整性判断，历史复审的失败日志保留不变。

## 版本与输入身份

- Titan 基线 `39f1267b6412165fc7adc005719bc9e09dcef9a8`，保留任务开始前已有的未提交编译链等工作，没有提交或覆盖这些修改。
- gate_arb 基线 `756d2db3b20afc249b2b9a9f527a449bbe015512`；两个适配器当时是未跟踪文件，因此另记录[实际输入文件 SHA256](gate-arb-inputs.json)，不能只用该 Git 提交代表输入内容。
- 本地只编辑、审查、同步和计算校验和；所有格式化、构建、Clippy、测试及无凭据公共数据探测均在 `192.168.3.88` 完成。
- 隔离源码 `/home/dominolu/dev/titan-gatecrossex-complete-20261021-39f1267b`，由先前验证目录复制；无 `.git`，未覆盖服务器已有工作区。工具链 Rust/Cargo 1.94.1；CLI 编译测试使用服务器已有 Python 3.11.14 虚拟环境。
- [343 项源码/配置/文档清单](source-files.sha256) 与本地逐项一致；清单摘要 `bdd3de08c84965e25002e1732be24b3f885f1950f069e938aea3d0023e2dfc32`。[逐项核对日志](logs/source-verification.log)。清单不含 verification 证据目录、构建输出及二进制 fixture。最终文档仅修正原生账本示例为 `FUNDING_FEE`，之后再次核对清单；生产代码与测试通过时相同。

二进制 fixture 沿用编译服务器此前生成并验证的产物，没有上传本地构建输出：

| fixture | SHA256 |
| --- | --- |
| `deploy/pair_arb_v13/artifacts/pair_arb.titan` | `5f532d44ed3b14725e8aa212de9c30ee0e4dbb81378bfcd202d8221cf660af63` |
| `crates/titan-cli/tests/fixtures/artifacts/event_counter.titan` | `e0cbe122bea8c0e3a24d932cbadb5b2945205d9b3258482b0684a40be85bdb65` |

本地 pair_arb fixture 仍为原先的 `39bd8b286866c12fdb56c218199bd0c810bc299ab435f5053e3983c90641225e`，没有覆盖。此回归不代表该不同 fixture 也经过相同验收。

## 修复与接口覆盖

| 项目 | 交付行为与证据 |
| --- | --- |
| 新增成交价误用累计均价 | 新增成交价按累计金额差计算，必要时取数量匹配的最后成交价；订单累计均价独立发布。0.005@100 后 0.005@200 的第二笔 Fill 为 200，OrderChanged 均价为 150。 |
| 重连忽略 REST 成交基线 | 更新累计量及金额基线，防止旧快照覆盖更晚 WS；断线前 0.004、REST 0.006、WS 0.008 的新增量为 0.002。 |
| 全撤遇单笔失败中止 | 继续处理其余订单，最后查询权威挂单；测试中首单已成交拒撤，第二单仍成功撤销。 |
| 余额刷新阻塞订单推送 | 私有事实解析为同步无 I/O 函数；资产/利息在会话拥有的后台任务合并刷新，设 15 秒总超时；旧会话取消任务。HTTP 被 oneshot 保持 pending 时订单仍可交付。测试的 100ms 是依赖关系复现窗口，不是生产延迟 SLA。 |
| 原项目缺少的接口迁移 | 补齐原项目全部 18 个 REST 包装及通用 request，包括风险限额、margin positions、划转支持/执行/查询、账本；36 组原生 REST 路由、认证和原始 payload 在一项 HTTP 协议测试中逐项核对。 |
| 原生扩展 | 补充 WS 任意频道订阅/取消、私有确认、API 写请求、原始 payload/result；补充历史持仓/利息、利率/折扣、ADL、账户模式、逐仓保证金、Convert、证券和批量撤单方法。写请求不重试，断线待确认写请求标记结果未知。 |
| 账户与业务 | 统一 broker 增加 MARGIN、SPOT 库存、隔离所配置、每品种持仓模式及结算币种；余额保留原币数量，总额使用新鲜实际汇率，缺失汇率拒绝估值。原生方法保留负债等统一模型未表达的字段。 |
| 批量与账本 | 逐项结果接口保留部分成功并继续执行；账本分页后按 symbol 本地过滤，保留资金费币种及符号。 |
| 集成测试发现的额外错误 | 修正 PositionChangedV1 中币种 ID 与未实现 PnL 字节重叠。PnL 使用 88..96，币种使用 96..100；正、负、零及 i64 边界验证互不覆盖，账户集成验证结算币种单位正确。 |
| 真实公共协议差异 | 允许中文和混合大小写的原生币名；公共 subscribe/unsubscribe 仅确认发送，快照等所请求 symbol 数据收齐，不依赖不存在的确认帧。 |

原项目按 symbol 覆盖另一条对冲持仓的行为没有照搬；Titan 保留独立 LONG/SHORT。SPOT/MARGIN 的按金额市价买单使用原生 `quote_qty`，不把报价金额伪装成基础币数量。统一接口不能表达的请求、交易所没有提供的历史公共行情及倒计时撤单均有[明确能力边界](../../../gate_crossex_broker.md)。

## 最终结果

| 检查 | 结果 | 日志 |
| --- | --- | --- |
| 格式 `cargo fmt --all -- --check` | 通过 | [fmt](logs/fmt.log) |
| workspace / all targets / all features Clippy `-D warnings` | 通过 | [Clippy](logs/clippy.log) |
| 独立 gatecrossex feature，关闭默认 features | 编译通过 | [feature](logs/feature.log) |
| workspace / all targets / all features | **496 passed / 0 failed / 12 ignored** | [workspace](logs/workspace.log) |
| 其中 connector | 273 passed / 0 failed / 12 ignored；含 40 项 CrossEx 测试 | [workspace](logs/workspace.log) |
| workspace doctest | 2 passed / 0 failed / 1 ignored | [doctest](logs/doctest.log) |
| 主网公共长连接，不使用账户 | Binance BTC 的 Bbo/Depth/Trade/Mark/Funding 五类均收到 | [stream](logs/public-stream.log) |
| 主网公共查询，不使用账户 | 7824 个 FUTURE/SPOT/MARGIN 元数据解析成功；ticker 和五档盘口成功 | [query](logs/public-query.log) |

完整门禁脚本：[verify.py](verify.py)；每项命令、退出码和耗时：[results.json](logs/results.json)。脚本逐项执行，失败即停。保留所有 ignored，不启用其他 broker 的真实账户测试。相对首版的 477 项通过，新增 17 项 CrossEx 回归、1 项账户价格/币种集成回归、1 项持仓事件字节边界回归。

开发中定向测试先发现字节重叠，修复后重跑；Clippy 首次报告复杂类型，拆分类型别名后完整门禁通过。真实公共探测先暴露 Unicode/大小写 symbol 及公共无确认帧行为，修复并加回归后重新执行最终全仓库检查与两个探测。没有放宽断言或将当前行情当历史数据返回。

## 实盘状态

没有读取真实账户、发单、撤单、划转、修改账户模式、部署构建产物或启动实盘策略。本次没有指定实盘服务器、目标账户和订单风险参数；因此代码迁移及非实盘验证完成，**私有实盘验收不能标记完成**。需要按 AGENTS.md 在实盘服务器使用本次编译服务器产物，核对目标账户、环境、品种、数量、价格保护和最大敞口后，进行受控私有登录、快照、最小风险下撤改单及恢复验证。公共数据和模拟测试的通过不替代这一阶段，也不代表全部底层所、品种或长稳均已验收。
