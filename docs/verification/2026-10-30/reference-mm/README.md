# 单文件参考价做市策略验证

历史说明：本记录对应 2026-10-30 的 `reference_mm` 1.0.0 / schema 1，下面的通过结果仅属于该版本。
后续已按用户要求改名并增加对冲，当前源码和说明见 [cross_exchange_mm](../../../../strategies/cross_exchange_mm.md)。旧源码/测试路径保留在历史叙述中，不代表新版本或对冲功能已通过本记录的验证。

日期：2026-10-30。基线：`c67acf9cdad51e9ee1108dca7311d450c1beb46c`。

当时新增业务实现仅为 `strategies/reference_mm/strategy.py`，测试位于
`python/titan-strategy-sdk/tests/test_reference_mm.py`。未改动平台、SDK 或其他策略实现；工作区中原有及其他任务的修改不属于本次变更。

参考来源：[hft-for-dummies / part 2](https://github.com/bulletxyz/hft-for-dummies/tree/2c0a433b0e87b80458d306a97e2ae92db1c6aced)，Apache-2.0。使用 Titan 公共 V13 SDK 独立实现参考价、基差均值、价差、库存偏移和双边 PostOnly 报价；不搬入上游网络或账户运行时。

## 验证环境与身份

- 本地仅编辑、审查、传输及核对文件摘要。构建、AOT 编译和全部测试在 `192.168.3.88` 执行。
- 专用目录：`/home/dominolu/dev/titan-reference-mm-20261030-c67acf9c/src`。同步前已检查服务器目录，未覆盖已有验证目录或未提交工作。
- 编译服务器已有源码目录无 `.git`；基线与新增源文件摘要联合标识此次源码。策略、测试、Cargo.lock 和取回产物的摘要与本地逐字节一致，见 [摘要清单](source-sha256.txt)。
- 工具链为 Cargo/Rust 1.94.1、Python 3.11.14，见 [工具链](toolchain.txt)。复用服务器虚拟环境，但 `TITAN_STRATEGY_SDK` / `PYTHONPATH` 显式指向本次源码。
- 使用服务器既有 rsproxy 缓存配置和离线依赖；仅隔离目录的 `.cargo/config.toml` 使用该环境配置，Cargo.lock 未修改。构建缓存为 `/tmp/titan-deplugin-target`，不进行跨版本性能比较。

## 结果

| 项目 | 本次结果 | 证据 |
| --- | --- | --- |
| Rust CLI 构建 | `cargo build -p titan-cli --bin titan` 成功 | [构建日志](build.txt) |
| 完整 SDK 回归 | 148 项通过，37.041 秒 | [测试日志](sdk-tests.txt) |
| 新策略专项回归 | 包含于上述 148 项：19 项业务、4 项原生/CLI 测试通过 | 同上 |
| 原生 pair 编译与加载 | 正式 Rust 编译入口生成 `.so`，实际调用 V13 回调 | 同上 |
| 默认参数 bundle 编译 | 单策略源文件，参数从标准输入提供 `{}` | [编译结果](bundle-build.json) |
| 产物校验 | `valid: true` | [校验结果](bundle-validate.json) |

专项测试覆盖预热、不同 tick 比例、库存偏移、PostOnly、仓位上限、启动拒绝脏账户、部分成交去重、终态/成交/持仓乱序、撤单 ACK 不释放预占、行情过期与重新预热、未知订单、撤单失败、账户代次变化、确认超时、时钟回退、价格变动触发毛亏损限制、暂停、恢复证据门及留仓停止。

CLI 回放使用合成行情和显式账户事实，确认默认 60 秒预热后提交 2 笔报价，随后撤销 2 笔并完成停止；不是历史收益或实盘撮合验证。未修改 Rust 源码，因此本次未重跑 Rust workspace 全量回归。

## 产物

- 类型 `reference_mm`，版本 `1.0.0`，ABI V13，恢复 schema 1。
- 状态大小 5,648 字节；目标 `x86_64-unknown-linux-gnu`，CPU baseline `x86-64`，未签名。见 [manifest](bundle-manifest.json)。
- 产物摘要：`c497419d03036ce88ba30cba9d04ab603276ca870f8d0e8663cab94f6c99553e`。
- `.titan` 文件 SHA256：`e34195fc24e55c0b8403b6af7b04bf64b6b8f4b5cca9a4804f83cd202e58ab80`。
- 编译服务器输出为该任务目录下的 `artifacts/reference_mm.titan`，已取回本地 `artifacts/reference_mm/reference_mm.titan`；本地 artifacts 目录按仓库规则不入 Git。

服务器复现编译（默认参数不需要独立 JSON 文件）：

```bash
printf '{}' | titan strategy compile \
  --strategy strategies/reference_mm/strategy.py \
  --parameters /dev/stdin --artifact-format bundle \
  --output reference_mm.titan
```

策略源码头部记录绑定、单位、停止行为和编译方法。默认参考行情编号 0、交易行情编号 1、账户编号 0；下单数量及仓位上限分别为 5 / 10 个交易品种 lot。参数全部在 `SPEC` 定义，可通过编译参数覆盖；运行时连接、账户、绑定及周期 timer 仍由 Titan 配置提供。

## 使用边界

- 本次没有访问真实账户、实盘部署或下撤单，也未启动长期策略。
- 当前仓库没有 Bullet connector。该文件可在离线或具备相应平台接入的环境中使用，不等于完成 Bullet 实盘接入。
- 当前受控实盘 `execution_limits` 路径要求单行情绑定；该策略使用两个行情绑定，现有受控实盘入口尚不能承载，不能通过禁用原风控绕过。
- 支持单向 NET 账户；Fresh Start 要求目标无持仓、无挂单，恢复另走平台证据门。
- 参考 tick 比例须按相同经济资产和明确的报价单位配置，不默认 USDT/USD/USDC 等价。
- 停止和毛亏损触发均撤单留仓，不提供自动平仓。策略内毛收益不含手续费和资金费，净权益保护仍由平台承担。
- 成交历史补查、网络和执行一致性、账户账本及耐久恢复均留在平台。策略只保存业务意图与消费水位，不以活动列表缺失、撤单 ACK 或持仓数量推造成交。
