# Cross-exchange MM 改名与增量对冲验收

日期：2026-10-31。本轮以提交 `1a7cc72083ecd9b5634c1c5319c65a034ffc1035` 为验证基线。

原 `reference_mm` 改为单文件 [cross_exchange_mm.py](../../../../strategies/cross_exchange_mm.py)，使用现有 V13 SDK 增加逐成交触发的 maker–taker 对冲；[说明及参数](../../../../strategies/cross_exchange_mm.md)。独立测试改名为 [test_cross_exchange_mm.py](../../../../python/titan-strategy-sdk/tests/test_cross_exchange_mm.py)。业务没有依赖其他策略模块，没有更改 ABI 或平台执行实现。

## 环境和隔离

- 本地仅编辑、审查、归档及核对文件身份，所有编译与测试在规定的 `192.168.3.88` 完成。
- 本次专用路径为 `/home/dominolu/dev/titan-cross-exchange-mm-20261031-1a7cc720`。
- 最初源码同步遇到自动审批无法确认 `media-nas` 目标的问题。只读核对 `ssh -G` 及远端 `SSH_CONNECTION`，确认服务器地址为 `192.168.3.88` 后，以该显式地址同步；没有绕过审批或换到其他服务器。
- 工作区存在其他任务并行修改 CLI/SDK/示例布局。首轮同步混入未完成的 CLI 修改而构建失败，保留在服务器 `logs/initial-concurrent-cli-build.txt`，没有据此修改其他任务代码。
- 最终从固定提交生成源码归档，仅包含 Cargo、源码、SDK、策略、fixture 和配置清单；SHA256 为 `ae052402efa8565457cbf743d7ce9beaf0da21d2ab0cdcc81c0ba817a98d7927`。服务器核对归档摘要后解包至 `frozen/`，再覆盖本次单文件策略和测试。
- `.cargo/config.toml` 仅在隔离目录使用服务器既有 rsproxy 离线缓存配置，Cargo.lock 不变。复用构建缓存 `/tmp/titan-deplugin-target` 后，立即把 CLI 复制并冻结为本任务 `tools/titan`，后续测试和产物编译使用该文件。见 [CLI 摘要](cli-sha256.txt)。
- Python 复用服务器虚拟环境，但 `TITAN_STRATEGY_SDK` / `PYTHONPATH` 明确指向本次 `frozen/`。工具链见 [toolchain.txt](toolchain.txt)。

该验证覆盖固定提交加本次文件，不替其他任务正在进行的工作区迁移背书。最终源码、测试、Cargo.lock 与取回产物摘要见 [source-sha256.txt](source-sha256.txt)。

## 本轮结果

| 验证 | 结果 | 证据 |
| --- | --- | --- |
| 固定基线 Rust CLI 构建 | 通过，随后冻结二进制 | [build.txt](build.txt) |
| 完整 SDK 回归 | 150 项通过 | [sdk-tests.txt](sdk-tests.txt) |
| 本策略专项测试 | 上述回归中的 30 项业务 + 5 项原生/CLI 测试通过 | 同上 |
| 正式 AOT pair 编译 | Rust 编译入口生成 `.so`，实际执行原生回调 | 同上 |
| 默认参数 bundle 编译 | 通过，参数从标准输入提供 `{}` | [bundle-build.json](bundle-build.json) |
| 产物校验 | `valid: true` | [bundle-validate.json](bundle-validate.json) |

源码类型 `cross_exchange_mm`、版本 `2.0.0`、恢复 schema 2；状态大小 8,072 字节，目标 Linux x86_64 / CPU baseline x86-64，未签名。具体内容摘要及原生库摘要见 [manifest](bundle-manifest.json)。编译服务器生成的产物已取回本地 `artifacts/cross_exchange_mm/cross_exchange_mm.titan`；该 artifacts 路径按仓库规则不入 Git。

专项覆盖：

- 两个独立 NET 账户启动准入，maker PostOnly / hedge IOC，成本利润边界和可见对冲档位数量。
- maker 买卖方向、每次部分成交立即对冲、整数 lot 倍率，以及成交先于持仓推送。
- 同一 IOC 在途期间合并欠量、重复成交去重、终态/成交/持仓 6 种排列。
- 对冲部分成交后等待原终态，再仅重试剩余欠量；撤单 ACK 不释放预占。
- 取消与成交竞态后的反向修正，未知/丢失/无结果订单不盲重发。
- 已确认拒单的退避与尝试次数、连续敞口原期限、重试不放宽原方向价格锚。
- 对冲行情过期仍计算期限；maker 行情失效不阻断已知欠量的合法对冲。
- 毛亏损触发后继续补齐已知对冲、重新预热阶段继续盯市、两腿毛收益单位换算。
- Stop 等待 maker 撤单、IOC 实际成交及双方持仓；失败不得报告停止完成。
- 恢复时证明匹配、拒绝未知/未消费成交、不发命令且不重置期限。

CLI 合成回放确认：60 秒默认预热后挂 2 笔 maker 单，maker 买入成交触发 1 笔 IOC 卖出对冲，撤掉剩余 maker 单，双方持仓 +5 / -5、净欠量为零后完成 Stop；缺少最后对冲成交的回放明确失败。该流程不是实盘撮合或收益验证。

首次专项测试的一个夹具没有交付 maker 撤单确认，先触发撤单期限而非欲单独验证的对冲期限；补齐独立事实后通过，没有放宽产品期限。较早的 `reference_mm` 148 项记录和本轮 150 项使用不同基线/源码集，不能通过数量增减推断回归。

## 行为与部署边界

- 保留 `account_no` / `traded_asset_no` / `reference_asset_no`，新增 `hedge_account_no`；参考场所现在也必须具备真实对冲账户。
- 每个原生 maker 最小成交 lot 必须精确对应整数个 hedge lot，并满足对冲最小量/金额。非整数转换、反向合约和现货借币模式不在本版范围内，不扩大数量来伪造对冲完成。
- Stop 或亏损限制撤 maker 单、补齐已知欠量，但保留两所相互匹配的仓位；并非双账户平仓。任何未知执行、超时或证据缺失均保留失败/风险事实。
- 不再声明 `PAUSE_SAFE`：对冲经常需要在另一个账户新开反向仓位，不得伪装成 reduce-only 绕过 Pause 门禁。
- 恢复 schema 和类型已变化，旧 `reference_mm` 的产物和检查点不得直接迁移。旧验证记录保留为历史。
- 当前受控实盘 `execution_limits` 仍只支持单行情绑定；本次不绕过该门禁，也没有补 Bullet connector。报价费用只是配置模型，净权益/费用/资金费与账户保证金仍由平台处理。
- 本次没有访问真实账户、实盘下撤单、部署或长期策略运行，也没有重跑未改动的 Rust workspace 全量回归；不声明实盘收益或生产 SLA。
