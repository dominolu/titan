# 全部已公开 API 接入验收

2026-10-31，按用户追加“所有 API 接口都接入”的范围，在首次连接器实现上完成全部已公开原生接口。完整分解与调用方式见[接入说明](../../../exchange_onboarding.md)，逐操作名称/路径/官方来源见[目录](../../../exchange_api_catalog.md)。代码覆盖完成与真实业务验收分开记录。

## 实现范围

| 服务 | 原生覆盖 |
| --- | --- |
| Backpack | 75 个 HTTP 操作、20 个 WS 流模式；含现货/永续/批量、借贷、资金/钱包、RFQ/报价、Vault、股票与全部发布的行情/历史路径 |
| Lighter | 104 个 HTTP 操作（91 exchange / 13 Explorer）、25 个 WS 频道模式及两种交易 RPC、23 类 Sign* 交易、key 生成/auth/显式 client check 工具 |
| Variational | 两条已公开 HTTP 路径：v2 stats 与废弃 v1 stats。原生入口保留所有字段，不给 v1 的占位值补造 v2 语义。Trading API 尚未开放 |

原生 API 提供逐操作命名 Rust 方法和枚举，不把完整协议裁剪到统一 BrokerApi。参数分开校验位置和基本结构，原生十进制字符串和整数保留，响应可流式读取。所有 179 个 Backpack/Lighter 路由均实际经过回环 HTTP 服务器，覆盖方法、body、表单、签名头与 Explorer 不携带账户 token；两条 Variational 路径另有共享限流及完整字段测试。这些是协议测试，不是模拟出交易所真实账户权限或业务受理结果。

统一运行时仍只自动投影经专门适配的 USDC perpetual 账户/行情，不因存在原生 spot、RFQ 或账户管理接口就自动宣告对应策略/财务事实能力。显式 nonce、原生 clientId 和外部钱包 L1 补签流程由调用方在独占执行身份下使用。

## 来源与生成

- Backpack 官方 OpenAPI 从 [官方文档](https://docs.backpack.exchange/)的嵌入规范取得，全部 75 个 method/path 对应 75 个命名绑定。
- Lighter 官方 SDK 的 `openapi.json` 固定提交 `d30dc2296d27f1584faeb22a590486aded1f03b1`，与当前官方文档补集（含 Explorer）合并，去重后 104 个 method/path。官方模型页没有 paths，Explorer 重复标题按实际路由命名，不靠页面标题推测接口。
- [HTTP 清单](../../../../connector/specs/manifest.json)保存每份官方文档的 URL/SHA256 和操作来源；运行时规范只保留功能性 schema，去掉介绍和示例正文。
- signer 固定官方 [aefa968](https://github.com/elliottech/lighter-go/tree/aefa96848a105d82a6bbb55d3d05663293a71713)。C 头文件生成于 `192.168.3.88`；[ABI 清单](../../../../connector/specs/lighter-signer.json)含其摘要、23 个签名函数的参数及结构体布局。不能用旧 v1.0.10 的 int32 ABI 库替代。
- 生成器、格式化、静态检查、测试和公开网络探针全部只在 `192.168.3.88` 执行。本机仅编辑/审查/同步源码、保存证据和核对摘要。

## 最终冻结结果

基线 `c29a97be08f55c905e5efb3522c17943f70b7137`，分支 `codex/depth-top100-recovery` 的未提交工作区快照；保留既有文档/reference_mm 改动。最终证据来自服务器候选 05，恢复默认并行测试，没有设置 RUST_TEST_THREADS。隔离目录 `/tmp/titan-native-venues-20261030`，target `/tmp/titan-native-venues-target`，不覆盖服务器既有项目目录。

| 检查 | 实际结果 |
| --- | --- |
| 官方覆盖审计 | Backpack 75/75、Lighter 104/104、signer 23/23，无遗漏；[日志](final/official-coverage.log) |
| 格式 / Clippy | fmt、workspace/all-targets/all-features Clippy `-D warnings` 通过；[fmt](final/format.log)、[Clippy](final/clippy.log) |
| 整库回归 | 706 项 Rust、2 项 doctest 通过；默认忽略 26 项 Rust、1 项 doctest；[日志](final/workspace.log) |
| 连接器测试 | 329 通过、14 默认忽略，包含在 706 项中；原生 API 的 10 项新增测试覆盖全部 HTTP 路由、WS RPC/消息及宽市场 ID |
| 官方签名器 | 单独选择 1 项 ignored 测试，通过 create/cancel/modify 基础路径及全部 23 类交易、key/auth 工具、L1Sig 字段补全；[日志](final/official-signer.log) |
| 源码身份 | 660 份输入在必需检查前后、全部公网探针结束后及本地逐字节一致；[前](final/sources-before.json)、[后](final/sources-after.json)、[最终](final/sources-final.json)、[本地](local-source-identity.json) |
| Lighter 公共探针 | 原有 metadata / 100×2 book / trade / funding / OI / candles / funding history，加 systemConfig、assetDetails、feeTiers、Explorer markets 的 HTTP/JSON 读取通过；[日志](final/public-lighter.log) |
| Variational 公共探针 | v2 类型化统计和 v1 原生统计均读到 581 listing，通过；[日志](final/public-variational.log) |
| Backpack 公共探针 | 未通过，markets 请求最多三次均连接重置；[日志](final/public-backpack.log) |

[原始状态](final/status.tsv)如实保留 Backpack 失败，整段 runner 退出 1，不能称作全绿或全部 API 已获真实业务验证。公开原生探针只读取代表性公共接口；全部私有、资金操作和 operator 权限均未在真实账户验证。原有[独立网络诊断](../native-venues/README.md#backpack-连通性诊断)也显示 Python urllib 请求被重置，没有修改服务器 DNS 或代理来绕过。

### 真实官方 signer 产物

- 编译服务器产物 `/tmp/titan-lighter-native-all-signer-20261031/lighter-signer.so`。
- SHA256 `1e1a81619a006de4cc44a65901f9832521655969eed48169615c358d231f2a08`，头文件 SHA256 `fcbe90e05378e51f6cb2370db6836badc3e65f7dd87b5363ce8cf131a9fbc743`。
- [构建回执](signer-build.txt)记录固定提交、Go 1.27.3、CGO、依赖/构建身份和头文件摘要；许可证随服务器产物保留。
- 离线测试使用随机未注册 key、回环地址 `127.0.0.1:1` 和显式 nonce，不打印/保存私钥，不调用 CheckClient，不发出交易。23 类都是官方库实际生成签名，不是用 C 假库代替密码学。
- L1Sig 测试只校验外部签名字符串的格式与字段写入，不宣称合成测试签名拥有真实钱包权限。Go 官方 HTTP client 具有 30 秒总期限；显式 check_client 使用后台线程，本轮没有调用它读取真实账户。

## 核对中解决的问题

- Backpack 批量下单签名按官方向量逐笔加 `instruction=orderExecute` 后拼接，最后才追加 timestamp/window，不把整数组当成单对象签名；逐项响应原样返回。
- Lighter 部分 OpenAPI 把 integer 字段的 enum 写成字符串，客户端按数值含义核对，但线上仍发送整数；字符串数字及越界枚举均拒绝。
- 新 signer 的 market index 是 int64，原生有效范围 48 位，不能沿用旧 int32 map 或按 ID 范围推断市场类型。元数据、私有流、深度序号和签名已统一；大于 32 位的元数据与真实离线签名通过。
- OCO 验证遵守原生不同 client index、SL/TP、reduce-only 与 expiry 规则；pool/staking 使用子账户命名空间，operator 使用独立 market slot，未把范围校验失败改成成功。
- WS 单笔 tx_info 为对象，批量 tx_types/tx_infos 为 JSON 字符串，与官方 SDK 相同；超过 2^53 的整数仍精确保留。
- 规范裁剪保留名为 description 的真实业务属性，独立组件字段清单与官方输入逐项一致；不能因同名说明关键字而丢掉账户、充值或市场的字段定义。
- 单页多个 OpenAPI JSON 块按独立命名空间保留，避免模型页前面的块被后面的块覆盖；来源清单记录 block_index 和 catalog_key。
- L1 授权消息没有丢弃；外部钱包补签写入 L1Sig。自动 nonce 入口遇到必须 L1 签名的流程拒绝，保持显式流程与原执行身份。

### 失败记录与最终复验

- [候选 01](candidate-01/status.tsv)通过当时全部必需检查，Backpack 公开失败；随后加入更严格的 schema 字段审计。
- [候选 02](candidate-02/status.tsv)在字段审计中拒绝，发现单页多个 OpenAPI 模型块被覆盖。修正独立命名空间后字段审计通过。
- [候选 03](candidate-03/status.tsv)在默认并行整库测试中遇到既有停机测试失败；[独立复跑](shutdown-isolated.log)在原期限内通过。[候选 04](candidate-04/status.tsv)仅用于串行诊断（RUST_TEST_THREADS=1），不作为最终默认并行结果。
- 定位到测试夹具的非协作阻塞分支先 yield，线程晚调度时会在进入阻塞前被原 5ms Tokio timeout 取消，与测试目标不同。只修正该测试分支首次 poll，不改产品停机逻辑，也不改 5ms / 80ms / 1s 原期限。候选 05 恢复默认并行，706 项 Rust 测试及全部必需检查通过；最终 Backpack 公网失败单独保留。

## 复现

在编译服务器隔离目录，使用[环境记录](final/environment.txt)中的工具链：

```sh
python3 scripts/generate_lighter_native_signer.py \
  /tmp/titan-lighter-native-all-signer-20261031/lighter-signer.h \
  connector/src/lighter/signer_native_generated.rs
python3 scripts/generate_native_api.py /tmp/titan-native-all-api-research "$PWD"
cargo fmt --all
export PATH=/home/dominolu/dev/titan/.venv/bin:$PATH
export PYTHONPATH=$PWD/python/titan-strategy-sdk
export TITAN_STRATEGY_SDK=$PYTHONPATH
export CARGO_TARGET_DIR=/tmp/titan-native-venues-target
export TITAN_CARGO_REGISTRY_CONFIG=/tmp/titan-venues-cargo-cache.toml
export TITAN_BASE_REVISION=c29a97be08f55c905e5efb3522c17943f70b7137
export TITAN_OFFICIAL_API_RESEARCH=/tmp/titan-native-all-api-research
export TITAN_VENUES_SOURCE_LIST=/tmp/titan-native-all-api-verify-files.bin
export TITAN_LIGHTER_SIGNER_TEST_PATH=/tmp/titan-lighter-native-all-signer-20261031/lighter-signer.so
export TITAN_LIGHTER_SIGNER_TEST_SHA256=1e1a81619a006de4cc44a65901f9832521655969eed48169615c358d231f2a08
bash scripts/verify_native_venues.sh /tmp/new-all-api-report
```

清单是 NUL 分隔的非忽略文件集合，排除 docs/，包含所有源码、配置、生成代码/schema、Cargo.lock 与验证脚本。修改实现后重新同步、生成清单、冻结并验证，不沿用本轮测试作为新修改已通过的证明。

## 仍未完成的外部条件

1. Backpack 的编译节点公开访问仍被重置，需要可用网络后复验；不能把回环协议通过替代真实响应验证。
2. 所有真实私有/资金/交易/恢复验收仍需实盘服务器入口、凭据引用、目标账户和每项操作参数及风险上界。用户要求接入全部接口不等于提供了可执行提现/转账的收款地址和金额；本轮没有真实账户读取、转账提现、模式变更、实盘下撤单、部署或策略启动。
3. [Variational 官方交易 API](https://docs.variational.io/technical-documentation/api#trading-api)尚未开放，无法实现不存在的公开协议。此处覆盖的是全部已公开 API，而非逆向网站内部会话。
