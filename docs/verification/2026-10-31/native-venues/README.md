# 三家交易所接入验收（2026-10-31）

本轮完成 Lighter Core、Backpack 的 Rust 连接器和 Variational Omni 的官方公开统计客户端。**不是三家实盘交易全部验收通过。** 能力、配置、官方来源与任务分解见[接入说明](../../../exchange_onboarding.md)。

## 基线与执行环境

- 分支 `codex/depth-top100-recovery`，基线 `c29a97be08f55c905e5efb3522c17943f70b7137`，验证对象是含本轮修改的工作区快照，而非已提交发布版本。
- 开始时已有文档、reference_mm 策略和 Python 测试改动，保持原样；没有覆盖编译服务器原目录。
- 所有构建、格式化、Clippy、测试与公开网络验证均在 `192.168.3.88` 执行。本地仅修改、审查、同步源码及保存证据、核对字节摘要。
- 隔离源码 `/tmp/titan-native-venues-20261030`，Cargo target `/tmp/titan-native-venues-target`。Rust/Cargo 1.94.1，Linux x86_64；详见[环境](candidate-03/environment.txt)。
- Python 编译桥接使用服务器既有 `/home/dominolu/dev/titan/.venv/bin`，`PYTHONPATH` / `TITAN_STRATEGY_SDK` 指向隔离源码的 SDK。错误使用系统 Python 时的缺少 llvmlite 不能当成产品测试结果。
- 离线使用服务器现有 rsproxy 缓存，Cargo 使用 `--offline --locked`。没有本地构建产物、实盘账户访问、部署或长期策略运行。

## 最终候选 03

| 检查 | 结果与证据 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过；[日志](candidate-03/format.log) |
| Clippy，workspace / all-targets / all-features / `-D warnings` | 通过；[日志](candidate-03/clippy.log) |
| `cargo test --workspace` | 696 项 Rust 测试、2 项 doctest 通过；默认忽略 26 项 Rust 测试及 1 项 doctest；[日志](candidate-03/workspace.log) |
| 官方 signer 离线 ABI | 单独选择执行 1 项并通过，不运行其余 ignored 测试；[日志](candidate-03/official-signer.log) |
| 源码冻结与同步 | 检查前后 645 份输入摘要一致，本地逐份一致；[前](candidate-03/sources-before.json)、[后](candidate-03/sources-after.json)、[本地核对](local-source-identity.json) |
| Lighter 公开接口 | 通过：241 个 instrument，ETH 100×2 档、3 成交、当前资金费/OI、2 K 线、2 资金费历史；含真实公共 WS 快照和 REST；[日志](candidate-03/public-lighter.log) |
| Variational 公开接口 | 候选 03 首轮 429，间隔后同一源码复验通过，581 个 listing；[首轮](candidate-03/public-variational.log)、[复验](public-followup/variational.log) |
| Backpack 公开接口 | 未通过：markets 请求最多 3 次均为 TRANSPORT；[日志](candidate-03/public-backpack.log)。独立 Python urllib 的 status/markets 请求也被重置，见下文 |

[原始状态码](candidate-03/status.tsv)保留首轮失败，因此完整脚本退出 1；不能用复验成功掩盖原始失败，也不能把尚未通过的 Backpack 改为成功。Variational 的[复验状态](public-followup/status.tsv)单独保存，复验期间没有修改候选源码。

Connector 默认测试为 319 通过、14 忽略，已包含在 696 项中。新增验证覆盖签名原文与参数类型、原生精度、时间与费率单位、终态历史查单、订单身份耐久映射、同进程账户替换共享日志/跨进程互斥、私有就绪、深度序号与失效重连、原生成交、未知盈亏、nonce 未知冻结、最后一道提交守卫，以及限流与 PERP/SWAP 数据区分。模拟 C 库只验证参数和调用行为，不能代替官方密码学库。

### 官方 signer 产物

- 官方 lighter-go v1.0.10，提交 `9d38261d1a4cc5c7211b383ba07a4d6e41604708`，使用仓库的 `scripts/build_lighter_signer.sh` 在编译服务器构建。
- 产物 `/tmp/titan-lighter-signer-sparse-20261030/lighter-signer.so`，SHA256 `4eda0bfe101f61c7aacfeebdda83359646eb2f856d71a96ec12f5ca3d0a0cde4`；[完整构建身份](signer-build.txt)含 Go、依赖、头文件摘要及 Git clean 状态。
- 使用 Go 1.27.3 linux/amd64；工具链由官方发布元数据校验，压缩包 SHA256 `65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40`。依赖按官方 go.sum 校验，网络超时后使用 goproxy.cn 获取。
- 实际测试创建随机、未注册的 API key，仅保留内存；使用 `127.0.0.1:1` 和显式 nonce，调用 create/cancel/modify 签名。不调用 CheckClient，不发送交易，不读取实盘账户。原生签名内存按官方 Free 回收。
- 该版本 C ABI 的 market index 为 int；库按 SHA256 加载，不能任意换成新版同名文件。产物未部署。

### Backpack 连通性诊断

服务器系统解析到 `65.9.126.15 / .35 / .9 / .92`。独立于 Rust 连接器，使用 Python urllib、10 秒超时和透明 User-Agent 请求公开接口，结果如下：

```text
https://api.backpack.exchange/api/v1/status URLError <urlopen error [Errno 104] Connection reset by peer>
https://api.backpack.exchange/api/v1/markets URLError <urlopen error [Errno 104] Connection reset by peer>
```

这证明本轮失败也出现在连接器以外的网络客户端；不能据此断言交易所故障或所有网络均不可用。没有修改服务器 DNS、代理或系统配置。待该节点获得可用公开访问路径后，须重跑公开探针并继续核对真实字段。

## 失败候选与修复

- [候选 01](candidate-01/status.tsv)：构建/测试通过，但源码一致性失败，原因是初始 Cargo.lock 没有记录启用 chrono serde 后的依赖边。已审查并同步 lockfile；后续使用 `--locked`。此候选不算冻结验收通过。
- [候选 02](candidate-02/status.tsv)：源码一致性及必需检查通过，Lighter/Variational 公开接口通过；Backpack 暴露 Trade timestamp 实际整数与文档字符串示例不一致，[原始错误](candidate-02/public-backpack.log)保留。最终候选兼容 ISO 及明确不重叠的当代 Unix ms/us 区间并增加负例测试；Backpack 最终真实公开探针仍受连接重置阻塞，不能声称所有新字段均获真实响应验证。
- Lighter 公开 WS 的区域访问须使用官方 `readonly=true`；当前实现已加上。真实公开样本还确认 candles 查询秒/响应毫秒、fundings 查询与响应秒、费用 ppm 与 USDC 金额不可混用。

## 可复现入口

在编译服务器隔离源码执行，输出目录必须不存在：

```sh
export PATH=/home/dominolu/dev/titan/.venv/bin:$PATH
export PYTHONPATH=$PWD/python/titan-strategy-sdk
export TITAN_STRATEGY_SDK=$PWD/python/titan-strategy-sdk
export CARGO_TARGET_DIR=/tmp/titan-native-venues-target
export TITAN_CARGO_REGISTRY_CONFIG=/tmp/titan-venues-cargo-cache.toml
export TITAN_BASE_REVISION=c29a97be08f55c905e5efb3522c17943f70b7137
export TITAN_VENUES_SOURCE_LIST=/tmp/titan-venues-verification-files.bin
export TITAN_LIGHTER_SIGNER_TEST_PATH=/tmp/titan-lighter-signer-sparse-20261030/lighter-signer.so
export TITAN_LIGHTER_SIGNER_TEST_SHA256=4eda0bfe101f61c7aacfeebdda83359646eb2f856d71a96ec12f5ca3d0a0cde4
bash scripts/verify_native_venues.sh /tmp/new-native-venues-report
```

source list 是同步时生成的 NUL 分隔非忽略文件清单，排除 `docs/`（证据和文档独立维护），包含源码、Cargo.lock、配置和验证脚本。更新代码时必须重新生成清单并同步，不把历史摘要当作新版本验证。只读探针按步骤最多重试 3 次 TRANSPORT；协议错误、交易写请求不自动重试。Variational 返回的 retry-after 保留给调用方。

## 剩余条件

1. Backpack 在编译节点的真实公开接口验收仍未通过，需恢复可用网络路径后复验。
2. Lighter / Backpack 私有鉴权、账户快照、真实下撤单、独立成交与恢复须在实盘服务器受控验收。需要用户指定服务器入口、凭据引用、目标账户/品种及最大数量、价格保护和风险敞口。当前没有这些新账户的授权范围；不沿用其他交易所的历史账户授权。
3. [Variational 官方 Trading API](https://docs.variational.io/technical-documentation/api#trading-api)截至本次核对仍未向任何用户开放。已实现官方可用的 v2 统计，等待官方开放交易协议后继续，不能逆向网页会话或制造 BrokerApi 能力。

平台新增 `REALIZED_PNL_UNAVAILABLE` 标记，不改变账户事件长度/schema；Backpack 缺少逐笔已实现盈亏时历史展示 null，并拒绝以缺失字段证明零盈亏恢复。旧已知盈亏的事实指纹保持原样。此变更必须和观察器、恢复代码一起部署，本轮没有部署。
