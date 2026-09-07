# OKX–Hyperliquid 跨所做市策略技术设计

状态：Draft v0.1

关联需求：`docs/okx_hyperliquid_cross_exchange_market_making_requirements.md`

目标运行时：Titan Core Runtime + Market/Account/Strategy Plugin

策略包建议名：`strategies/okx_hyperliquid_xemm`

## 1. 设计结论

该策略不能仅新增一个 Numba `strategy.py` 就安全上线。Titan 已具备 OKX/Hyperliquid 行情连接器、账户
连接器、多账户命令路由和固定内存策略 ABI，但当前生产链路仍缺少 XEMM 的数个必要事实与控制能力。

实施应分为两层：

1. 先补齐运行时的深度、账户、命令结果、定时器、reduce-only、订阅编排与恢复能力；
2. 再实现固定内存、事件驱动的 XEMM 策略包。

在 P0 能力完成前，只允许离线仿真或 shadow mode，不允许真实发单。

## 2. 现状评估

### 2.1 已具备能力

- `connector/src/okx` 支持 OKX 公共/私有 WebSocket、REST 下单、GTX/Post Only、IOC 和安全撤单心跳。
- `connector/src/hyperliquid` 支持 Hyperliquid L2/trades、账户流、签名下单、ALO/Post Only、IOC 和
  `scheduleCancel` 安全心跳。
- `titan.market` 能发布带 asset id、epoch、sequence 和时间戳的 canonical depth batch。
- `titan.account` 定义了 OrderChanged、Fill v2、PositionChanged、BalanceChanged、CommandResult 与对账事件。
- Strategy ABI v9 的 `OrderCommand` 带 `local_account_no`，可把 maker 和 hedge 命令路由到不同账户。
- Strategy Plugin 支持固定容量 state、可靠有序账户事件订阅、命令 capability 和 owned-order 元数据。

### 2.2 P0 阻塞项

| 编号 | 当前限制 | 影响 | 必需改动 |
|---|---|---|---|
| G-01 | `CanonicalStrategyEventAdapter` 将 Depth/BBO 转成普通 `TickItem`，丢失 action、flags、epoch 和 sequence | 无法可靠重建增量盘口与检测 gap，不能计算目标量 VWAP | 增加 typed depth ABI view 或完整保留深度元数据 |
| G-02 | adapter 只支持 Depth/Trade/BBO/Bar/Order/Fill；Position、Balance、CommandResult 和 control facts 会被拒绝 | 无法做保证金、仓位、拒单与重连风险控制 | 为这些 canonical event 增加 typed callback view |
| G-03 | Strategy Plugin 创建 lane，但 Core 配置启动 market source 后未按策略 binding 自动调用 `MarketApi::Subscribe` | 配置可运行但没有明确的行情订阅所有者 | 增加订阅编排器，按 binding 引用计数订阅/退订 |
| G-04 | `SubmitOrderCommand` 没有 `reduce_only`，Account adapter 将其硬编码为 false | 永续减仓语义丢失 | 贯通 ABI → Strategy Gateway → Account command → connector |
| G-05 | `SCHEDULE_TIMER` 当前被 manifest 校验拒绝 | 静默行情时无法按 deadline 撤单、批量对冲或检测 stale | 实现 timer queue 与 `on_timer` live dispatch |
| G-06 | live adapter 没有向策略暴露 account id/local account no；Fill 的 `venue_no` 固定为 0 | 同 asset id 多账户时事件归属含糊 | 事件绑定必须携带 `local_account_no`，或使用实例内唯一 asset id 并强校验 |
| G-07 | Account execution 接受命令后异步失败依赖 CommandResult，但策略 adapter 不支持该事件；gateway 同步拒绝会令整个 runtime Failed | 无法安全执行对冲重试 | 引入 command 状态机；可恢复业务拒单不得直接杀死 runtime |
| G-08 | ConfigurationAdapter 强制 production strategy `recovery = fresh` | 崩溃后无法恢复 fill→hedge 事务 | 支持 checkpoint + authoritative reconciliation |
| G-09 | 当前 Numba live context 未填充 positions/markets 数组 | `s.position()` / `s.market()` 在该路径不可作为事实来源 | 由 typed events维护策略状态，或由 runtime 提供一致快照 |
| G-10 | 当前 Strategy `OrderCommand` 只支持 submit/cancel，amend 被禁用 | 不能原地改单 | v1 明确采用 cancel-confirm-replace；非阻塞项 |

### 2.3 标识约束

当前 canonical adapter 使用 `asset_id -> local_asset_no` 映射，不包含 market source 维度。即使两个场所交易
同一经济标的，也必须分配不同的 canonical asset id：

| 场所 | native symbol | asset id | local asset no | local account no |
|---|---|---:|---:|---:|
| OKX maker | `BTC-USDT-SWAP` | 1001 | 0 | 0 |
| Hyperliquid hedge | `BTC` | 1002 | 1 | 1 |

策略层再通过 contract multiplier 把 1001/1002 归一化成同一 `BTC delta`。不得让两边复用 asset id 1001，
否则行情和 fill 会在 adapter 的 map 中互相覆盖。

## 3. 目标架构

```text
OKX Public WS --------> OkxMarketConnector ---------+
                                                     |
HL Public WS ---------> HyperliquidMarketConnector --+--> EventEngine PRIMARY lane
                                                     |           |
OKX Private/REST -----> OkxAccountConnector ---------+           v
                                                     |    XemmStrategyRuntime
HL Private/REST ------> HyperliquidAccountConnector -+      | state machine
                                                            | OrderCommand
                                                            v
                                                    StrategyCommandGateway
                                                       |             |
                                                       v             v
                                                   OKX account   HL account
```

所有热路径事件经 EventEngine 的同一 strategy PRIMARY lane 串行处理。策略不得启动 Python async task、访问
网络或持有 connector 对象。冷路径 metadata、配置和恢复操作由 Core/Plugin 完成。

## 4. 组件设计

### 4.1 Market subscription orchestrator

Core 在创建 strategy 前，根据每个 `StrategyMarketBinding` 建立引用计数订阅：

- OKX asset 1001：Depth + BBO；
- Hyperliquid asset 1002：Depth + Trades；
- 订阅 route 必须在 connector subscription 前注册，避免丢失首个 snapshot；
- strategy 停止或替换时释放 lease，最后一个消费者退出后再退订；
- 重连后 connector 必须发布新 epoch/snapshot，策略在 snapshot 完成前冻结报价。

策略的 `subscriptions` 仍定义 EventEngine 路由；market subscription lease 定义上游数据生产。两者必须由
ConfigurationAdapter 做一致性校验。

### 4.2 Strategy ABI 扩展

建议升级 Strategy ABI，而不是继续在 `TickItem.event.ival/fval` 中塞非类型化字段。新增：

```rust
#[repr(C)]
pub struct DepthUpdate {
    pub local_market_no: u32,
    pub local_asset_no: u32,
    pub stream_epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub flags: u16,
    pub side: u8,
    pub action: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
}

#[repr(C)]
pub struct AccountFactHeader {
    pub local_account_no: u32,
    pub local_asset_no: u32,
    pub account_epoch: u64,
    pub account_version: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
}
```

并增加/补齐 callbacks：

- `on_depth`：批量 typed depth updates；
- `on_position`：typed position snapshot/upsert；
- `on_balance`：typed balance snapshot/upsert；
- `on_command_result`：accepted/rejected/outcome-unknown；
- `on_stream_state`：market/account healthy/degraded/invalidated；
- `on_reconcile`：reconcile started/completed；
- `on_timer`：staleness、hedge deadline、重试和采样。

Fill/Order view 同样增加 `local_account_no`，不得继续把 `venue_no` 固定为 0。

### 4.3 Order command 扩展

`reduce_only` 必须作为显式字段贯通：

```text
Strategy ABI OrderCommand
  -> StandardStrategyCommandGateway
  -> titan_account_plugin::SubmitOrderCommand
  -> UnifiedOrderRequest
  -> OKX / Hyperliquid wire request
```

同时建议在 submit command 中增加 `command_purpose`：`MAKER_QUOTE`、`HEDGE`、`RISK_FLATTEN`。该字段用于
审计和风险策略，不替代 client order id。

同步参数错误、capability 错误仍使 runtime fault；交易所拒单、限频和 outcome unknown 作为 typed
CommandResult 交给策略状态机，不应由 gateway 直接把实例置为 Failed。

### 4.4 Account cache 与风险快照

Runtime 为每个绑定账户维护单写缓存：

- open orders，按 client order id 与 venue order id 双索引；
- position，保留 quantity、entry、liquidation price、margin mode；
- balances/available margin；
- account epoch/version 与最后更新时间；
- command result 与 fill 去重集合。

每个 strategy callback 看到的账户事实必须对应不晚于当前 event sequence 的一致版本。缓存过期时策略
只允许 cancel/reconcile/hedge，不允许新增 maker 风险。

### 4.5 XEMM strategy package

建议目录：

```text
strategies/okx_hyperliquid_xemm/
├── __init__.py
├── strategy.py
├── strategy-manifest.json
└── README.md
```

Manifest capabilities 至少包含：

```text
READ_TICK | READ_DEPTH | READ_ACCOUNT |
SUBMIT_ORDER | CANCEL_ORDER | SCHEDULE_TIMER | CHECKPOINT_STATE
```

状态必须预分配，禁止 callback 内动态扩容。建议将 state 分为：

- `state_f64`：BBO、VWAP、conversion、positions、unhedged qty、目标/活动价格数量、费用与风险阈值；
- `state_i64`：状态枚举、时间戳、epoch/sequence、order id、command id、累计 fill lots、重试次数、timer id；
- 固定容量 order slots：maker bid、maker ask、当前 hedge 以及有限数量历史 tombstone；
- 固定容量 fill dedupe ring：保存稳定 hash 与过期时间。

若恢复需求无法在固定数组内安全容纳，应将 XEMM 实现为 Rust-native strategy runtime，而不是在 Numba
callback 内引入 Python 容器。

## 5. 核心数据模型

### 5.1 QuoteLeg

```text
side: BID | ASK
generation: u64
target_price_ticks: i64
target_qty_lots: i64
active_order_id: u64
active_price_ticks: i64
active_qty_lots: i64
filled_qty_lots: i64
state: EMPTY | SUBMITTING | OPEN | CANCELING | TERMINAL
last_action_ts: i64
```

### 5.2 HedgeLot

```text
source_fill_key_hash: u64
maker_order_id: u64
side: BUY | SELL
required_base_qty: f64
submitted_base_qty: f64
filled_base_qty: f64
hedge_order_id: u64
attempt: u32
state: PENDING | SUBMITTING | OPEN | RECONCILING | DONE | FAILED
first_seen_ts: i64
deadline_ts: i64
```

同方向 HedgeLot 可合并提交，但每个源 fill 的 required/covered 数量必须独立保留，以便审计和恢复。

### 5.3 NormalizedExposure

```text
okx_base_delta = okx_position_lots × okx_contract_base_multiplier
hl_base_delta  = hl_position_lots  × hyperliquid_contract_base_multiplier
net_base_delta = okx_base_delta + hl_base_delta
unhedged_base  = Σ maker_fill_delta - Σ hedge_fill_delta
```

交易所 side、position side 和符号转换只允许在 connector/account canonicalization 层做一次，策略收到的
quantity 必须统一为 signed net quantity。

## 6. 事件状态机

### 6.1 启动

```text
on_start
  close command gate for maker submits
  acquire market subscriptions
  request full reconciliation for both accounts
  wait for market snapshots + account reconcile completed + valid FX
  rebuild owned order / fill / position state
  cancel orphaned owned maker orders if generation is obsolete
  if exposure within limits -> QUOTING
  else -> HEDGE_ONLY
```

### 6.2 行情事件

```text
on_depth(batch)
  validate epoch/sequence
  apply snapshot or delta to bounded depth book
  update receive timestamp
  if both books valid:
      recompute hedge VWAP and target quote
      run risk gates
      reconcile target vs active quote legs
```

盘口至少保留能够覆盖 `order_amount / taker_volume_factor` 的深度。超过固定容量时保留最优 N 档，并在
目标数量超出已保留累计量时返回 depth insufficient，不允许外推。

### 6.3 Maker fill

```text
on_fill(fill)
  resolve local_account_no + asset
  if fill_key already seen: return
  record dedupe key
  if OKX owned maker order:
      delta = incremental_fill × signed_side × contract_multiplier
      append/update HedgeLot(opposite side, abs(delta))
      update unhedged exposure
      if hard limit reached: cancel both maker legs; state = HEDGE_ONLY
      submit_or_schedule_hedge()
  else if Hyperliquid owned hedge order:
      apply only incremental hedge fill
      reduce matching HedgeLots
      if all covered: terminalize hedge order
```

### 6.4 Hedge submit/retry

```text
submit_or_schedule_hedge
  aggregate uncovered lots of same side
  quantize down to HL lot
  if below min lot: retain dust and arm max-age timer
  compute executable VWAP and IOC protection price
  submit with unique attempt order id

on_command_result / on_order
  accepted/open       -> wait for fill/final state
  partial+terminal    -> create retry for remainder
  rejected            -> classify; refresh metadata/balance or back off
  outcome_unknown     -> RECONCILING, never immediate duplicate submit
  retry limit reached -> FAULTED/HEDGE_ONLY + critical alert
```

### 6.5 Quote reconcile

对每个 bid/ask 独立执行：

```text
if risk gate closed:
    cancel active leg
elif no target:
    cancel active leg
elif no active order:
    submit Post Only target
elif active differs and cooldown elapsed:
    cancel active; wait terminal; submit newest generation
```

报价 generation 只在输入事实或量化后的目标变化时递增。撤单确认后使用最新 generation，丢弃中间过时
目标，防止行情快速变化造成排队补挂。

## 7. 数值与量化

### 7.1 内部单位

- Canonical event 与 command 继续使用整数 ticks/lots。
- 计算经济价格时使用 `ticks × DecimalUnit`；数量使用 `lots × quantity_lot × contract_multiplier`。
- Numba 热路径可使用 `f64` 做计算，但每次发单前必须转回整数并做边界/有限值校验。
- 最终盈利校验以量化后的整数价格与数量重新计算。

### 7.2 VWAP

```text
remaining = target_base_qty
quote = 0
for level in executable_side_best_to_worst:
    take = min(remaining, level_base_qty)
    quote += take * level_price
    remaining -= take
if remaining > epsilon: DEPTH_INSUFFICIENT
vwap = quote / target_base_qty
```

Maker bid 的 hedge side 是 Hyperliquid bids（策略卖出）；maker ask 的 hedge side 是 Hyperliquid asks（策略买入）。

### 7.3 费用

费率以 bps 输入并转换为小数。Maker rebate 可为负。固定成本单独按 quote currency 计入，禁止把 funding
rate 当成立即成交费用。资金费仅用于监控或未来版本的持仓成本模型。

## 8. 配置与装配草案

以下为语义示例；落地时应使用 Titan `ApplicationConfig` 的 TOML 表达，并由构建工具生成 byte-array digest
与 connector config，避免人工维护：

```yaml
market_sources:
  - source_key: okx-btc-public
    connector_type: okx
    instruments:
      - { native_symbol: BTC-USDT-SWAP, asset_id: 1001, price_tick: "0.1", quantity_lot: "1" }
  - source_key: hl-btc-public
    connector_type: hyperliquid
    instruments:
      - { native_symbol: BTC, asset_id: 1002, price_tick: "0.1", quantity_lot: "0.00001" }

accounts:
  - account_key: okx-maker
    account_id: 2001
    connector_type: okx-account
    credential_ref: secret://titan/okx/main
    instruments: [{ native_symbol: BTC-USDT-SWAP, asset_id: 1001, ... }]
  - account_key: hl-hedge
    account_id: 2002
    connector_type: hyperliquid-account
    credential_ref: secret://titan/hyperliquid/main
    instruments: [{ native_symbol: BTC, asset_id: 1002, ... }]

strategies:
  - strategy_key: okx-hl-btc-xemm
    markets:
      - { local_market_no: 0, local_asset_no: 0, source_key: okx-btc-public, asset_id: 1001, data_mode: tick }
      - { local_market_no: 1, local_asset_no: 1, source_key: hl-btc-public, asset_id: 1002, data_mode: tick }
    accounts:
      - local_account_no: 0
        account_key: okx-maker
        tradable_assets: [{ local_asset_no: 0, asset_id: 1001 }]
      - local_account_no: 1
        account_key: hl-hedge
        tradable_assets: [{ local_asset_no: 1, asset_id: 1002 }]
```

实际 instrument tick/lot 与 contract multiplier 必须从交易所 metadata 验证，示例值不得直接用于生产。

## 9. 恢复与幂等设计

### 9.1 Client order id

```text
owner(strategy_id, generation) + role(maker_bid/maker_ask/hedge) + logical_order_id + attempt
```

Titan gateway 当前使用 128-bit owner id；扩展时必须保证原始 logical order id 可从 canonical account event
稳定映射回来，不能仅截取不明确的 8 字节。

### 9.2 Checkpoint

Checkpoint 至少保存：

- strategy state 与 quote generation；
- owned maker/hedge logical order；
- fill dedupe ring；
- HedgeLot required/filled/attempt；
- 最后处理的 account epoch/version 与 market epoch/sequence。

恢复时不直接重放 checkpoint 命令。先对账，再将 checkpoint 与 open orders/recent fills/positions 合并：

1. 交易所已成交、本地未知：补记 fill 并更新 HedgeLot；
2. 本地认为 open、交易所不存在：查询 order history 后终态化；
3. outcome unknown：按 client id 查询，确认不存在后才重试；
4. position 与 fill 推导不一致：以交易所 position 为风险事实，进入 `HEDGE_ONLY` 并告警。

## 10. 风控与故障处理

| 故障 | 动作 |
|---|---|
| OKX market stale/gap | 撤双边 maker，暂停报价；已有未对冲量继续在 HL 处理 |
| HL market stale/gap | 撤双边 maker；禁止使用旧价格对冲，短时等待 snapshot，超时人工处置 |
| OKX private stream invalidated | 撤单命令 + REST 对账；确认前禁止补挂 |
| HL private stream invalidated | 保持 HEDGE_ONLY，按 client id/position REST 对账 |
| FX stale | 撤 maker，保留既有 hedge 风险管理 |
| Post Only rejected | 刷新 OKX BBO，退一 tick，受 anti-hysteresis 控制重试 |
| IOC partial | 只重试 remainder，重新计算保护价 |
| rate limit | 指数退避；未对冲超时则 critical alert |
| callback budget violation | command gate 关闭；Runtime 必须触发 owned maker cancel safety path |
| process crash | 两所 exchange-side cancel-all-after/scheduleCancel 生效；重启后完整对账 |

## 11. 测试计划

### 11.1 纯算法测试

- bid cap / ask floor 在 fee、rebate、conversion、contract multiplier 下的公式测试；
- price/lot 定向量化与量化后盈利复核；
- VWAP 深度充分/不足；
- top depth tolerance 与 12 点采样窗口；
- inventory skew 和风险 headroom。

### 11.2 状态机属性测试

- 任意重复/乱序 fill 下，hedged quantity 永不超过 recognized maker fill（允许显式 flatten 除外）；
- 同一 side 同时最多一笔非终态 maker 单；
- cancel-confirm 前不 replace；
- outcome unknown 在 reconcile 前不生成新 attempt；
- 状态为 PAUSED/HEDGE_ONLY 时没有 maker submit command；
- unhedged hard limit 触发后 command 序列只包含 cancel/hedge/reconcile。

### 11.3 Connector contract 测试

- OKX GTX 映射为 `post_only`，HL IOC 映射为 `Ioc`；
- 两边 client id round-trip 与 fill/order correlation；
- partial fill、cancel race、reconnect replay、sequence gap；
- reduce-only 从 ABI 到 wire 的完整贯通；
- safety heartbeat 失效时 health 降级并关闭报价闸门。

### 11.4 集成与故障注入

- fake OKX + fake HL 的确定性 EventEngine 回放；
- maker fill 后在 hedge accepted、partial、final 各阶段杀进程并恢复；
- market/private stream 分别断开与重连；
- REST timeout/outcome unknown、429、5xx 与签名拒绝；
- 高行情速率下 callback budget、lane overflow 和 reliable account ordering。

### 11.5 上线门禁

1. 单元、属性和集成测试全通过；
2. shadow mode 至少连续运行 24 小时，无序列 gap、重复 hedge 或状态漂移；
3. OKX demo + Hyperliquid testnet 小额闭环通过；
4. 主网只读/零下单验证通过；
5. 主网最小数量 canary，人工监控并设置严格 notional/unhedged 限额；
6. 验证停止、崩溃和网络隔离时两边安全撤单机制。

## 12. 实施顺序

### Phase 0：运行时前置能力

- typed depth/account/control ABI；
- market subscription orchestration；
- timer live path；
- reduce-only command plumbing；
- command result 与可恢复拒单状态机；
- checkpoint + reconciliation recovery。

### Phase 1：策略最小闭环

- 单标的、固定角色；
- HL depth VWAP；
- OKX Post Only 单边/双边报价；
- maker partial fill → HL IOC hedge；
- cancel-confirm-replace；
- freshness 和 unhedged hard limits。

### Phase 2：生产增强

- price sampling/top depth tolerance；
- inventory skew；
- 动态 FX source；
- 完整指标、审计和运行手册；
- shadow/testnet/canary 门禁自动化。

## 13. 参考实现取舍

Hummingbot 实现是行为参考，不是逐行移植目标。以下机制保留：

- 按 taker 目标量 VWAP 反推 maker 报价；
- maker 每侧最多一单；
- 盈利、余额、漂移三类撤单检查；
- anti-hysteresis、top depth tolerance 和价格样本窗口；
- maker fill 与 taker hedge 的映射、迟到成交保留与未完成 hedge 重试。

以下行为不复制：

- 仅用简单价格倍率、未完整计入 CEX 两边费用的盈利判断；
- 用 Python 对象身份而非稳定值比较 fill id；
- 未先 reconcile 就直接重发失败/过期 hedge；
- 订单停止跟踪方法的自递归调用；
- 对冲任务单值/列表字段不一致；
- 将永续合约风险简化为现货 base/quote wallet balance。

## 14. 相关代码

- Titan Strategy ABI：`python/titan-strategy-sdk/titan_strategy/context.py`
- Titan canonical strategy adapter：`crates/titan-strategy-plugin/src/runtime.rs`
- Titan strategy command gateway：`crates/titan-strategy-plugin/src/gateway.rs`
- Titan core configuration：`crates/titan-cli/src/core_runtime.rs`
- Titan market ABI：`crates/titan-market-plugin/src/abi.rs`
- Titan account ABI/model：`crates/titan-account-plugin/src/abi.rs`、`crates/titan-account-plugin/src/model.rs`
- OKX connector：`connector/src/okx`
- Hyperliquid connector：`connector/src/hyperliquid`
