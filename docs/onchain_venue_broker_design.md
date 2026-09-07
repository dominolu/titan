# 链上 Venue 接入方案：EVM 链与 Solana 双 Broker 设计

> 状态：方案 A（EVM）与方案 B（Solana，JSON-RPC 全链路）均已实现（见下方"实现状态"）。
> 范围：为 connector 新增两类链上 venue —— 方案 A（EVM：Ethereum / Arbitrum / Arbitrum Orbit 类链，含 Robinhood Chain）与方案 B（Solana）。
> 前置结论：两者共享现有 `BrokerApi` / `Connector` / 插件注册抽象，差异只在链上技术栈与数据模型适配层。

## 实现状态（方案 B：Solana，2026-09-05）

- `connector/src/solana/`（feature = `solana`，已入 default features）：`config` / `types`（vault 储备模型）/ `rpc`（JSON-RPC 封装，finalized blockhash）/ `market`（WS accountSubscribe 行情后端，BBO+快照+推演 Trade）/ `raydium`（V4 swap 指令构造 + 恒乘行情推演）/ `tx`（消息编码/ed25519 签名/WSOL wrap-unwrap/确认监视）/ `ordermanager` / `brokerapi`。
- 注册：`DynamicVenue::Solana`（"solana" / "solana-account"）、`SolanaMarketFactory`、`crates/titan-connector-solana-plugin`。
- 实盘验证（mainnet，`examples/solana_swap_probe.rs`）：WS 行情加载 → BUY 200k tokens 成交 → SELL 回卖成交 → 订单状态机与余额对账全通。签名 `yyWArCpX...` / `5HM3oTbS...`。
- 实盘踩坑（已固化在代码注释）：系统程序指令 tag 为 u32（Token/ATA/Raydium 为 u8）；ComputeBudget tag 2=CU limit、3=CU price、4=loaded accounts data size limit；消息账户必须按 [可写签名者, 只读签名者, 可写未签名者, 只读未签名者] 连续分区；公共 RPC 上 blockhash 用 finalized 档；公共节点预检偶发误报，预检被拒时降级 skipPreflight 由确认循环回报真实结果。
- gRPC：`solana-grpc.publicnode.com` 可达但服务需认证；yellowstone-grpc 接入待提供凭证（prechain 插槽已在 `PoolReserves` 中预留）。
- 配置示例：`connector/examples/solana.toml`。
- 验证：`cargo test -p connector --no-default-features --features binancefutures,okx,hyperliquid,evm,solana --lib solana::` 通过 16 个 Solana 测试；`titan-connector-solana-plugin` 构建测试通过。

## 实现状态（方案 A）

- `connector/src/evm/`（feature = `evm`，已入 default features）：
  - `config.rs` TOML 配置 + 运行期解析；`types.rs` 池配置/池状态（confirmed + prechain 双视图）；
  - `provider.rs` alloy JSON-RPC 封装（HTTP 读 + WS 订阅 + 收据轮询）；
  - `dex/uniswap_v2.rs` 常数乘积行情推演 + router swap calldata（`DexAdapter` trait，可扩展 V3/Solidly）；
  - `market.rs` RPC WS 行情后端（Sync/Swap → BBO/快照/Trade 事件，快照带单调 epoch）；
  - `feed.rs` Arbitrum sequencer feed 预链推演（`sequencer_client`，pair 直连与 router 两类 calldata）；
  - `tx.rs` EIP-1559 swap 构造/签名/nonce 管理/自动 approve + 收据确认循环（发布 `AccountPublication::Order`）；
  - `ordermanager.rs` tx hash 订单状态机（pending→filled/rejected/canceled）；
  - `brokerapi.rs` `BrokerApi` 全量 AMM 语义映射（GTC/条件单显式拒绝，cancel 幂等）。
- 注册：`DynamicVenue::Evm`（"evm" / "evm-account"）、`EvmMarketFactory`、`venue_account_factories`、`crates/titan-connector-evm-plugin`（cdylib）。
- 配置示例：`connector/examples/evm.toml`。
- 验证：`cargo test -p connector --no-default-features --features binancefutures,okx,hyperliquid,evm --lib evm::` 通过 30 个 EVM 测试；`titan-connector-evm-plugin` 构建测试通过。
- 依赖注记：本机 cargo mirror 为部分快照，alloy 版本线为 primitives/sol-types 1.7 + 其余 2.4；`arb_sequencer_consensus` 依赖 consensus 1.8，测试经 RLP 往返对齐两套类型（见 `evm/feed.rs` 测试辅助）。工具链已升至 1.94.1（alloy 2.4 的 MSRV）。

---

## 0. 总体架构

两类 venue 都按现有 hyperliquid 模式落地为独立模块，注册路径与现有 venue 完全一致：

```
connector/src/evm/        方案 A：EVM 链 venue（feature = "evm"）
connector/src/solana/     方案 B：Solana venue（feature = "solana"）
```

每个模块内部沿用标准分层，但把 CEX 的 REST/WS 层替换为链上对应物：

| 统一抽象 | CEX 实现（现状） | EVM 实现（方案 A） | Solana 实现（方案 B） |
|---|---|---|---|
| `client.rs` | reqwest REST | alloy-provider（HTTP/WS RPC） | solana-rpc-client（nonblocking） |
| `public_stream.rs` | 交易所 WS | RPC log/头订阅 + **sequencer feed 预链流**（见 §0.1） | WS 订阅；**yellowstone-grpc（Geyser）**为待接低延迟后端（见 §0.1） |
| `private_stream.rs` | user data WS | 块/收据事件驱动确认 | 签名订阅（`signatureSubscribe`）/ 块订阅 |
| `brokerapi.rs` | REST API 映射 | 合约调用 + tx 构造 | 指令（ixn）构造 + tx 构造 |
| `ordermanager.rs` | cloid → Order 状态机 | tx hash → 状态机（pending→confirmed，含 reorg） | 签名 → 状态机（含 landing 重试） |
| `signing.rs` | CEX HMAC / 手写 EIP-712 | alloy-signer-local（EIP-155/1559） | ed25519-dalek（现有依赖即可） |

新增统一接入点（两个方案共用，一次改动）：

1. `connector/src/lib.rs`：`#[cfg(feature = "evm")] pub mod evm;`、`#[cfg(feature = "solana")] pub mod solana;`
2. `connector/Cargo.toml`：新增 feature 与链上依赖（见各方案）。
3. `dynamic_plugin.rs`：`DynamicVenue` 枚举加 `Evm` / `Solana` 变体 + 四处 match 分支。
4. `market_plugin.rs` / `account_plugin.rs`：各加一个 factory。
5. 新建插件 crate：`crates/titan-connector-evm-plugin/`、`crates/titan-connector-solana-plugin/`（复制 hyperliquid-plugin boilerplate）。

### 0.1 高频行情通道（两方案统一设计，P0 必备能力）

两类 venue 的高频数据通道在抽象上等价：**订阅"进入序列化器但尚未落块"的交易流**，在出块前得到池状态变化，把行情延迟从"块间隔级"压到"网络传输级"：

| | Solana | Arbitrum（含 Orbit 链） |
|---|---|---|
| 通道 | **yellowstone-grpc**（Geyser gRPC，节点插件推送账户/交易变更） | **sequencer feed relay**（`wss://arb1.arbitrum.io/feed`，Nitro 自带，预链 L2 消息流） |
| 数据内容 | 账户数据变更（AMM pool 状态）、pending 交易 | L2 用户交易（zlib/brotli 压缩广播），经 `ParseL2Transactions` 解码为 typed tx |
| 相对 WS RPC 的优势 | 节点内存级推送，无轮询/无 WS 帧开销；可按 owner/程序过滤 | **交易在执行前可见**（FCFS 排序 = 到达即排队），比等块头早约一个块间隔 |
| Rust 接入 | `yellowstone-grpc-client` + `yellowstone-grpc-proto` | `sequencer_client` crate（处理解压与解码）；需自建 relay 或用公共端点 |
| Ethereum 主网 | —— | **无等价 feed**；替代：自有全节点 + bloXroute 类商业流。主网保持 RPC WS 慢腿定位 |

统一封装：`market.rs` 对上暴露同一个 `MarketSource` trait，下挂两个后端 —— `RpcFeedBackend`（WS 订阅，基线，任何链可用）与 `LowLatencyFeedBackend`（gRPC/feed，高频，按链启用），策略层无感知切换。Orbit 链（含 Robinhood Chain）因同属 Nitro 栈，每条链有自己的 feed 端点，接法与 Arbitrum One 相同。

> 延迟定位修正：有了预链流之后，Arbitrum venue 具备了参与亚秒级行情博弈的条件（sequencer 是 FCFS，到达即排序），不再是纯慢腿；Solana 侧 Geyser 流 + Jito 提交是标准 HFT 组合。但预链数据仍是"意图"而非"成交"，合成行情需区分 pre-chain（预链状态）与 on-chain（已确认状态）两个视图。

---

## 方案 A：EVM 链 Broker（Ethereum / Arbitrum / Orbit 链）

### A.1 目标与范围

- 首发目标链：Arbitrum One（L2 流动性最好、块间隔 ~250ms）。
- 同一套代码通过配置支持：Ethereum 主网、任意 Arbitrum Orbit 链（含 Robinhood Chain）—— Orbit 链与 Arbitrum One 共享 Nitro 技术栈，差异只有 RPC URL 与 chain id。
- 交易对象：部署在目标链上的 AMM/订单簿 DEX 合约。首期支持 Uniswap V2 类（AMM，最简）与 Uniswap V3/V4 类（集中流动性）二选一按需扩展；限价单类协议（如 CoW、链上限价协议）作为可选扩展。

### A.2 新增依赖（alloy 生态，按需引入）

```toml
[dependencies]
alloy = { version = "1", default-features = false, features = [
    "provider-http", "provider-ws", "signer-local", "consensus", "rlp", "sol-types", "contract", "eips",
] }
sequencer_client = "0.7"   # Arbitrum sequencer feed 解码（zlib/brotli 解压 + typed tx）
brotli = "*"               # feed 广播解压依赖（随 sequencer_client 传入）
```

- 现有 `k256` / `sha3` / `hex` 与 alloy-primitives 同源，不冲突；hyperliquid 的手写签名栈保持不动，新代码统一走 alloy。
- Orbit 链无需额外依赖，`alloy-chains` 之外的 chain id 直接由配置传入。

### A.3 模块布局

```
connector/src/evm/
├── mod.rs              # EvmConfig { rpc_url, ws_url, chain_id, private_key,
│                       #   router_address, quote_token, tokens: Vec<TokenConfig>, max_gas_*, ... }
├── provider.rs         # alloy Provider 封装：连接管理、重连、块头订阅
├── dex/
│   ├── mod.rs          # trait DexAdapter：quote()/encode_swap()/decode_events()
│   ├── uniswap_v2.rs   # 首期：Router/Pair 合约绑定（sol! 宏）
│   └── uniswap_v3.rs   # 二期：Quoter + Pool slot0/tick 订阅
├── market.rs           # 合成行情：Swap/Sync 事件 + 余额读取 → OrderBook 快照/增量
├── feed.rs             # 低延迟后端：Arbitrum sequencer feed（sequencer_client 解码 L2 交易，
│                       #   按 to=pool/router 过滤 → 预链池状态视图）；Orbit 链换 feed 端点即可
├── brokerapi.rs        # impl BrokerApi：见 A.5 语义映射
├── tx.rs               # tx 构造：gas 策略（Arbitrum 上 gas 价为静态，主网需 EIP-1559 策略）、nonce 管理
├── ordermanager.rs     # tx hash → Order 状态机，含 reorg 处理与超时
└── tests.rs
```

### A.4 行情方案（关键设计）

DEX 没有 order book，`BrokerApi::get_order_book` 返回的是**合成的深度快照**：

- **V2 类**：订阅 `Sync(reserve0, reserve1)` 事件维护恒定乘积池状态，`get_order_book` 时按价格区间积分生成合成深度；定期 `eth_call` 对账（防漏事件）。
- **V3 类**：订阅 `Swap` 事件 + Pool 的 `slot0`/`liquidity`/`ticks`（`eth_call`），本地维护 tick 状态生成真实深度。
- `Ticker`/`get_trades` 由 `Swap` 事件直接映射（价格、数量、tx hash 即 trade id）。
- 块间隔即行情粒度：Arbitrum ~250ms/块，主网 12s/块 —— 主网上不适合作为低延迟腿，定位为慢腿。
- **预链视图（高频路径）**：`feed.rs` 订阅 sequencer feed，对解码出的 L2 交易按 `to` 地址过滤出目标 pool/router 的 swap，**在交易执行前**本地推演池状态（对 V2 直接按常数乘积公式预演；V3 需模拟 swap），生成 pre-chain 行情视图；落块后与 RPC 订阅的 on-chain 视图对账收敛。Arbitrum 为 FCFS 排序，feed 到达时间即排序依据，这是 L2 上做延迟博弈的基础。

### A.5 BrokerApi 语义映射（CEX 模型 → 链上）

| BrokerApi 方法 | EVM 实现 | 备注 |
|---|---|---|
| `submit_order` | 构造 router swap calldata → EIP-1559 tx → `send_raw_transaction`；`OrderInfo.order_id = tx hash`，`client_order_id` 保留传入 | AMM swap 广播后不可撤回 |
| `cancel_order` / `cancel_all_orders` / `cancel_all_after` | 返回 `ApiError`（不支持）或仅作用于限价单协议 | 空实现需在上层文档化 |
| `get_order` / `get_open_orders` | 按 tx hash 查收据：pending / success / revert / not-found-but-timeout=dropped | open 状态只有 pending，语义收窄 |
| `get_fills` | 解析收据中的 `Swap`/`Transfer` log | price = 实际成交比例 |
| `get_positions` / `get_account` | `eth_call` 读取代币余额（+ permit2/vault 授权状态） | spot 无杠杆，`set_leverage` 空操作 |
| `get_instruments` | 部署时静态配置（tokens/pools 列表）+ 链上校验 | 不存在动态拉取 |

- `position_side`/reduce_only：spot 场景映射为 `Unknown`/空操作；库存对冲靠 CEX 腿。
- **资金路径**：发单前提是 hot wallet 持有代币并完成 router 授权（approve/permit2），授权管理需在 `tx.rs` 中显式处理并在配置中限额。

### A.6 延迟与风控约束

- Arbitrum：确认 ~1-2 块（250ms-1s），tick-to-trade 受限于出块，不适合抢单，定位为**对冲腿/库存再平衡腿**。
- 风控参数落地：`max_gas_price`、`max_slippage_bps`（swap 的 `amountOutMin`）、单 tx 限额、私钥零化（对齐现有 zeroize 实践）、nonce 回滚与卡单处理（同一 nonce 卡单会阻塞后续发单，需支持 replacement）。

### A.7 实施阶段

1. **P0 骨架**：provider + V2 adapter + `get_order_book`/`get_ticker` 合成行情（只读，跑通 market plugin）；**sequencer feed 接入作为 P0 的一部分**（pre-chain 视图与 RPC 视图对账）。
2. **P1 交易**：submit_order（swap）+ 收据确认 + ordermanager 状态机 + brokerapi 全量映射。
3. **P2 稳态**：nonce/gas 管理、reorg 处理、对账与掉线快照重放、V3 支持。
4. **P3 扩展**：Orbit 链配置矩阵验证（含 Robinhood Chain，逐链验证 feed 端点）、限价单协议评估。

---

## 方案 B：Solana Broker

### B.1 目标与范围

- 交易对象：主流 AMM（首期 Raydium CLMM 或 Orca Whirlpool 二选一；两者 arb-bot-rs 有可借鉴实现）。
- 定位与 EVM 相同：对冲腿 / 库存再平衡，非抢单。

### B.2 新增依赖

```toml
[dependencies]
solana-sdk = "3"
solana-rpc-client = "3"        # nonblocking 版本；solana-client 是阻塞的，不要用
spl-token = "9"
spl-token-2022 = "10"
yellowstone-grpc-client = "12" # 高频行情（Geyser gRPC），P0 必备
yellowstone-grpc-proto = "12"
# 三期上链加速（可选）：Jito bundle 提交
```

- **签名/序列化底子已在**：`ed25519-dalek`（Solana 签名即 ed25519）、`bincode`（Solana 交易序列化即 bincode）均为现有依赖；注意 Solana 侧用 bincode v1，与 workspace 现有 bincode v2 并存无冲突。

### B.3 模块布局

```
connector/src/solana/
├── mod.rs              # SolanaConfig { rpc_url, ws_url, keypair_path|private_key,
│                       #   program/pool 地址, commitment, jito_url?, ... }
├── rpc.rs              # solana-rpc-client 封装：连接、重试、blockhash 管理
├── amm/
│   ├── mod.rs          # trait AmmAdapter：quote()/build_swap_ix()/decode_event()
│   └── whirlpool.rs    # 首期 Orca Whirlpool（或 raydium_clmm.rs）
├── market.rs           # 账户状态 → 合成行情；双后端：
│                       #   - RpcFeedBackend：WS accountSubscribe（基线/降级）
│                       #   - LowLatencyFeedBackend：yellowstone-grpc 按 program/pool 账户过滤推送
├── brokerapi.rs        # impl BrokerApi
├── tx.rs               # tx 组装：blockhash 刷新、优先费策略、ATA 检查、签名提交
├── ordermanager.rs     # 签名 → 状态机：processing→landed/dropped，重试（重签+新 blockhash）
└── tests.rs
```

### B.4 与 EVM 方案的关键差异

| 维度 | EVM（方案 A） | Solana（方案 B） |
|---|---|---|
| 签名 | secp256k1（alloy） | ed25519（现有依赖） |
| 行情来源 | log 事件订阅 + **sequencer feed 预链流** | **账户变更订阅**（AMM 状态存在账户数据里，不 emit log）+ **yellowstone-grpc（Geyser）高频推送** |
| 撤单 | 不可能 | 同样不可能（已广播 tx 不可撤回） |
| 卡单处理 | 同 nonce replacement | 重签 + 新 blockhash 重发，需 landing 重试循环 |
| 确认语义 | 收据 + reorg 风险 | commitment 级别（confirmed/finalized），无 reorg 语义但可能 dropped |
| 上链加速 | 无需（L2 本身快） | Jito bundle（三期可选） |
| 计价 | gas 以 ETH/链内 token 计 | 优先费以 SOL 计，需纳入成本模型 |

### B.5 BrokerApi 语义映射

与 A.5 同构，差异点：

- `submit_order` → 构建 swap 指令 + 计算/设置优先费 + 最近 blockhash → 签名 → `sendTransaction`；`order_id = 签名（signature）`。
- `get_order` → `getSignatureStatuses` + 本地状态机（processing/landed/dropped），dropped 需主动重试。
- `get_positions` → 读取 SPL token 账户（含 Token-2022）余额与 ATA 状态。
- `get_instruments` → 静态配置 pool/mint 列表，启动时 `getAccountInfo` 校验并解码 pool 状态。

### B.6 实施阶段

1. **P0 骨架**：RPC 封装 + Whirlpool/Raydium 状态解码 + 合成行情；**yellowstone-grpc 接入作为 P0 的一部分**（Geyser 流为主，WS 为降级备份）。
2. **P1 交易**：swap tx 组装 + 签名提交 + 状态机 + brokerapi 映射。
3. **P2 稳态**：blockhash/优先费管理、landing 重试、ATA/租金处理、对账。
4. **P3 扩展**：Jito 提交、多 pool 路由。

---

## 共享工作与排期建议

- **一次性改动（两方案共用）**：lib.rs / Cargo.toml / dynamic_plugin / 两个 plugin factory / 两个插件 crate —— 约 1-2 天。
- **建议顺序**：先做方案 A（P0+P1）。理由：alloy 的抽象层更成熟、Arbitrum 块间隔短更接近现有 HFT 心智模型、且 Orbit 链复用即得（Robinhood Chain 只是换配置）；方案 B 复用同一套 `DexAdapter`/`AmmAdapter` 思想，等 EVM 路径踩平语义映射后再启动。
- **定位约束**：接入预链流（sequencer feed / Geyser）后，两类 venue 具备亚秒级行情能力，可参与跨所价差博弈；但下单确认仍受出块与 landing 不确定性约束，与 CEX 腿 组合时建议以 CEX 为快腿、链上为对手腿做库存对冲（详见 §0.1）。

## 风险清单

1. 代币授权（approve/permit2）与私钥托管风险 —— 配置限额 + 零化，建议独立热钱包。
2. 卡单/卡 nonce（EVM）与 dropped tx（Solana）会阻塞策略 —— 状态机必须显式建模这两态。
3. AMM 行情是合成快照，深度可信度低于 CEX —— 策略层消费 `OrderBook` 时需知晓来源标记（可在 `InstrumentInfo`/快照元数据中带 venue type）。
4. Orbit 新链（如 Robinhood Chain）DEX 部署与流动性尚未稳定 —— 上线前逐链验证合约地址与深度。
5. 开源参考（whack-a-mole、arb-bot-rs 等）仅作架构参考，不可直接实盘。


---

## 多池型扩展设计（DexFamily）

> 状态：设计稿（待实施）
> 目标：把"新增一个 DEX/池型"的成本从"改 venue 代码"降到"加一个适配器（或纯配置）"。

### 1. 现状耦合点

1. EVM `PoolConfig` 是 V2 形状写死（`pair_address` + 两 token），报价走常数乘积；
2. Solana Raydium V4 的账户模板/数学/vault 偏移直接焊在 `raydium.rs` 与 `tx.rs`；
3. 报价、执行编码、行情订阅三个能力散在 venue 各层，没有按 DEX 收口。

### 2. 目标架构

核心思想：venue 层用 `BrokerApi` 归一化了交易所，池子层用 `DexFamily` 归一化 AMM。
下游（market / tx / brokerapi）只消费两种类型：`PoolConfig`（配置）与 `PoolObservation`（状态）。

```rust
/// 池的归一化观测快照。
enum PoolObservation {
    ConstantProduct { reserve_base: u128, reserve_quote: u128 },     // V2 系 / Raydium V4 / CPMM
    Clmm { sqrt_price_x96: U256, liquidity: u128, tick: i32 },       // V3 / V4 / Whirlpool
}

/// 一个 DEX 家族 = 一个实现单元（每链每协议一个）。
pub trait DexFamily: Send + Sync {
    fn kind(&self) -> &'static str;
    /// 启动时校验池配置与链上账户布局（错配置早失败，杜绝 FoRGER 池偏移猜错一类问题）
    fn verify_pool(&self, pool: &PoolConfig) -> Result<(), ...>;
    /// 行情订阅目标：Solana = 账户列表；EVM = 合约地址 + 事件签名（+ topic 过滤）
    fn market_data_targets(&self, pool: &PoolConfig) -> Vec<MarketTarget>;
    /// 原始观测 → 归一化快照
    fn decode(&self, pool: &PoolConfig, raw: &RawObservation) -> Option<PoolObservation>;
    /// 报价（quote 单位；Sell=得到 / Buy=付出，沿用现有语义）
    fn quote(&self, pool: &PoolConfig, obs: &PoolObservation, side: ApiSide, base_qty: f64)
        -> Option<Quote>;
    /// 交易执行编码：EVM = calldata；Solana = 账户列表 + 指令数据
    fn encode_swap(&self, ctx: &SwapCtx, intent: &SwapIntent, obs: &PoolObservation)
        -> Result<EncodedSwap>;
}
```

`PoolConfig` 泛化为「地址 + dex 字符串 + 家族私有 spec」：

```toml
[[pools]]
symbol = "WETH/USDC"
dex = "sushi-v2"            # 家族选择
pool = "0x..."              # 池/对合约地址
# 家族私有字段（spec），由 DexFamily 定义并解析：
router = "0x..."            # sushi-v2 需要；uniswap-v2 用默认 router 可省
```

### 3. 家族清单与成本分档

| 家族 | 链 | 模型 | 成本 |
|---|---|---|---|
| `uniswap-v2` / `sushi-v2` / `pancake-v2` | EVM | ConstantProduct | **纯配置**（V2 家族参数化：router + fee） |
| `raydium-v4`（现有，迁移） | Solana | ConstantProduct | 重构搬入 trait |
| `raydium-cpmm` | Solana | ConstantProduct | 纯配置（池为 PDA，可本地推导） |
| `uniswap-v3` | EVM | Clmm | 新适配器：Swap 事件 + slot0/ticks 读取 + tick 积分报价 |
| `uniswap-v4` | EVM | Clmm | 新适配器：见 §4，执行走 Universal Router |
| `orca-whirlpool` | Solana | Clmm | 新适配器：池账户单订阅即得 sqrtPrice/liquidity；深度需加订 tick array |

CLMM 深度策略分级：MVP 用 `sqrtPrice + liquidity` 的单 tick 近似（够市价单），
深度快照标注精度级别；完整 tick 积分为适配器内部二期优化，不影响 trait 形状。

### 4. Uniswap V4 的处理（Singleton 架构的三个差异）

V4 与 V2/V3 的根本区别：**所有池共享一个 PoolManager 单例合约**，池不再是一个地址，
而是一个 id；结算走 flash accounting（unlock/callback 模式）。对架构的影响：

**(1) 池标识：poolId 本地可推导。**
`poolId = keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))`，
配置里只需要 `dex = "uniswap-v4"` + 两个 mint + fee + tickSpacing + hook 地址，
poolId 由适配器本地 keccak 计算（alloy 已有依赖），无需链上查询。
currency 排序规则沿用 V4（地址小者为 currency0，native ETH 用 address(0) 表示）。

**(2) 行情：单例上按 poolId 过滤订阅。**
所有 V4 池的 `Swap` 事件都从同一个 PoolManager 合约发出，topic1 = poolId。
MarketTarget 表达为「PoolManager 地址 + event + topic 过滤」，一条 WS 订阅可覆盖
全部 V4 池（比 V3 每池一条更省）。即时价格由 Swap 事件携带的 sqrtPriceX96 直接维护；
补齐可用 StateView 合约（Uniswap 官方部署）的 `getSlot0(poolId)` / `getLiquidity(poolId)`
eth_call 对账。

**(3) 执行：走 Universal Router，不自建 callback。**
直接调 PoolManager 需要 unlock/callback 回调合约 —— 我们不部署自有合约。
Universal Router（Uniswap 官方无许可路由，主网/Arbitrum 均有部署）以
`execute(commands, inputs, deadline)` 编码封装了 unlock/swap/settle 全流程：
`encode_swap` = V4Adapter 生成 commands 字节串（SWAP 精确输入 + WRAP/UNWRAP 可选）
+ ABI inputs。滑点保护走命令参数 `minAmountOut`，语义与现有 `min_out` 一致。
**(4) Hooks 风险控制。** hook 地址是 poolId 的一部分；带 hook 的池可能改费率、
改价格路径、拦截转账 —— 行为不可静态推断。默认策略：`hooks != address(0)` 的池
配置时直接拒绝（verify_pool 拦截），白名单放开留作每个 hook 单独评估。动态费率
（DynamicFee hook）的池同样先排除。

### 5. 迁移路径（不改外部行为的渐进重构）

1. **抽 `DexFamily` + 归一化 `PoolObservation`**：把 EVM `DexAdapter` 与 Solana
   `raydium.rs` 的能力搬进各自家族实现；venue 层改吃 `PoolObservation`。纯重构，
   现有测试全绿即通过。约半天。
2. **EVM V2 家族参数化**：`UniswapV2Adapter` → `V2Family { router, fee_bps }`；
   SushiSwap（Arbitrum）以纯配置接入，并用实盘 round-trip 验证（复用
   `evm_swap_probe` 方法论）。约 1 小时。
3. **Solana Whirlpool 适配器**：用"CPI 模板提取 + getAccountInfo 批量校验"方法论
   拿 swap 账户列表；行情先做 sqrtPrice 即时价。约 1-2 天。
4. **EVM V3 适配器**：tick 状态维护（Swap 事件流 + ticks 合约读取）+ tick 积分报价。
   约 1-2 天。
5. **EVM V4 适配器**：poolId 推导 + PoolManager Swap 订阅 + Universal Router 编码。
   在 V3 完成后做（Clmm 报价复用）。约 1 天。

每一步独立可验证，验证方式与现有两个 venue 一致：单元测试（数学/编码） +
小额实盘 round-trip。
