# 链上 Venue 接入方案：EVM 链与 Solana 双 Broker 设计

> 状态：设计稿
> 范围：为 connector 新增两类链上 venue —— 方案 A（EVM：Ethereum / Arbitrum / Arbitrum Orbit 类链，含 Robinhood Chain）与方案 B（Solana）。
> 前置结论：两者共享现有 `BrokerApi` / `Connector` / 插件注册抽象，差异只在链上技术栈与数据模型适配层。

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
| `public_stream.rs` | 交易所 WS | RPC log/头订阅 + AMM 池状态合成 | WS 订阅或 yellowstone-grpc（Geyser） |
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

1. **P0 骨架**：provider + V2 adapter + `get_order_book`/`get_ticker` 合成行情（只读，跑通 market plugin）。
2. **P1 交易**：submit_order（swap）+ 收据确认 + ordermanager 状态机 + brokerapi 全量映射。
3. **P2 稳态**：nonce/gas 管理、reorg 处理、对账与掉线快照重放、V3 支持。
4. **P3 扩展**：Orbit 链配置矩阵验证（含 Robinhood Chain）、限价单协议评估。

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
# 二期 HFT 行情：
# yellowstone-grpc-client + yellowstone-grpc-proto（Geyser gRPC）
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
├── market.rs           # 账户状态 → 合成行情；WS accountSubscribe 或 Geyser gRPC
├── brokerapi.rs        # impl BrokerApi
├── tx.rs               # tx 组装：blockhash 刷新、优先费策略、ATA 检查、签名提交
├── ordermanager.rs     # 签名 → 状态机：processing→landed/dropped，重试（重签+新 blockhash）
└── tests.rs
```

### B.4 与 EVM 方案的关键差异

| 维度 | EVM（方案 A） | Solana（方案 B） |
|---|---|---|
| 签名 | secp256k1（alloy） | ed25519（现有依赖） |
| 行情来源 | log 事件订阅 | **账户变更订阅**（AMM 状态存在账户数据里，不 emit log） |
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

1. **P0 骨架**：RPC 封装 + Whirlpool/Raydium 状态解码 + 合成行情（WS 订阅）。
2. **P1 交易**：swap tx 组装 + 签名提交 + 状态机 + brokerapi 映射。
3. **P2 稳态**：blockhash/优先费管理、landing 重试、ATA/租金处理、对账。
4. **P3 扩展**：yellowstone-grpc 行情、Jito 提交、多 pool 路由。

---

## 共享工作与排期建议

- **一次性改动（两方案共用）**：lib.rs / Cargo.toml / dynamic_plugin / 两个 plugin factory / 两个插件 crate —— 约 1-2 天。
- **建议顺序**：先做方案 A（P0+P1）。理由：alloy 的抽象层更成熟、Arbitrum 块间隔短更接近现有 HFT 心智模型、且 Orbit 链复用即得（Robinhood Chain 只是换配置）；方案 B 复用同一套 `DexAdapter`/`AmmAdapter` 思想，等 EVM 路径踩平语义映射后再启动。
- **明确定位约束**：两类 venue 均为**慢腿/对冲腿**，与 CEX 腿组成跨所库存对冲；不要用它们承担低延迟抢单角色。

## 风险清单

1. 代币授权（approve/permit2）与私钥托管风险 —— 配置限额 + 零化，建议独立热钱包。
2. 卡单/卡 nonce（EVM）与 dropped tx（Solana）会阻塞策略 —— 状态机必须显式建模这两态。
3. AMM 行情是合成快照，深度可信度低于 CEX —— 策略层消费 `OrderBook` 时需知晓来源标记（可在 `InstrumentInfo`/快照元数据中带 venue type）。
4. Orbit 新链（如 Robinhood Chain）DEX 部署与流动性尚未稳定 —— 上线前逐链验证合约地址与深度。
5. 开源参考（whack-a-mole、arb-bot-rs 等）仅作架构参考，不可直接实盘。
