# Connector 测试模板

给新接入的 broker 编写测试时，按本模板的用例清单逐项覆盖。Hyperliquid 连接器
(`src/hyperliquid/`) 是完整的参考实现，每个用例都标注了对应的参考测试，直接对照修改即可。

## 1. 测试分层

| 层级 | 是否联网 | 默认运行 | 说明 |
|---|---|---|---|
| L1 单元测试 | 否 | 是 | 签名、序列化、订单状态机、配置解析、错误映射 |
| L2 集成测试 | 是（只读） | `#[ignore]` | 真实行情/账户查询，验证协议字段 |
| L3 端到端 | 是（下单） | `#[ignore]` | 测试网挂单→撤单/成交，需资金和环境变量 |

## 2. 标准用例清单

### 2.1 配置解析（Config / build_from）

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| 合法配置 | `build_from` 返回 `Ok` | `test_build_from_private_key_with_and_without_0x` |
| 必填字段缺失 | 返回配置错误 | 参照 `test_build_from_rejects_invalid_private_key` |
| 密钥格式变体 | 带/不带 `0x` 前缀等价 | `test_build_from_private_key_with_and_without_0x` |
| 非法密钥 | 长度/字符非法报错 | `test_build_from_rejects_invalid_private_key` |
| 默认值 | `td_mode`/`is_mainnet`/`pos_side` 等 | `test_build_from_derives_address_when_empty` |
| 身份派生/agent 模式 | 留空自动派生；显式填主账户不报错 | `test_build_from_accepts_agent_mode` |

### 2.2 消息模型反序列化（msg.rs）

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| 订单簿 | levels 数组结构、买/卖方向约定 | `test_deserialize_l2_book_levels` |
| 成交 | side 字段语义（主动买/卖） | `test_deserialize_order_state_camel_case` |
| 订单状态更新 | 字段命名映射（camelCase/蛇形）、默认值 | `test_deserialize_order_state_camel_case` |
| 账户事件 | 成交/仓位字段 | `test_deserialize_user_event_fill` |
| 错误响应 | untagged 枚举（成功/错误变体） | `test_deserialize_order_status_variants`、`test_deserialize_cancel_status_variants` |
| 未知字段 | serde 忽略不报错 | `test_deserialize_meta_ignores_unknown_fields` |
| WS 消息 | channel + data/subscription/error | `test_deserialize_ws_msg` |

### 2.3 签名 / 认证

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| **官方向量锚定** | 用官方 SDK 生成固定向量，逐字节对比 r/s/v / token / connectionId | `test_signature_matches_official_sdk_testnet`、`test_connection_id_matches_official_sdk` |
| 数学自洽 | 签名恢复出的身份 = 私钥推导身份 | `test_signature_recovers_derived_address` |
| 已知地址向量 | 固定私钥 → 固定地址 | `test_derive_address_known_vector` |
| 参数分支 | 主网/测试网、带 vault/不带、nonce 边界 | `test_signature_matches_official_sdk_mainnet`、`test_connection_id_with_vault_matches_official_sdk` |

> 官方向量是防回归的黄金标准：先跑官方 SDK 生成一组固定输入/输出，固化进测试，不要用"自产期望值"。

### 2.4 订单构造（wire format）

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| 价格精度 | tick size 对齐、最多 5 位有效数字、整数价格合法 | `test_build_order_wire_price_precision` |
| 数量精度 | szDecimals 对齐、去尾零 | `test_build_order_wire_size_precision` |
| 类型/有效期映射 | GTC/GTX/IOC/FOK/市价 → 线上枚举 | `test_build_order_wire_tif_mapping` |
| side 映射 | buy/sell → 布尔/枚举 | `test_build_order_wire_side_and_asset` |
| 拒绝路径 | 不支持的 TIF/类型/非法 side 返回 `InvalidArg` | `test_build_order_wire_rejects_fok_and_unsupported_tif` |
| 客户端订单 id | 前缀/格式（如 HL cloid 必须 `0x`+32hex） | `test_cloid_format`、`test_build_order_wire_injects_cloid_and_reduce_only` |
| 资产索引 | universe 下标语义（测试网顺序可能不同） | `test_build_assets_map_testnet_order` |

### 2.5 订单管理器状态机（ordermanager.rs）

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| 提交 resting | 状态 New、req 清空 | `test_update_from_ws_matches_by_cloid`（间接） |
| 提交即成交 | Filled、exec_qty/leaves 正确、查表清理 | `test_submit_filled_status` |
| 提交被拒 | Rejected/Expired、req 清空 | `test_submit_error_status` |
| **双通道顺序 ×2** | REST→WS 与 WS→REST 都能最终删除且不重复发布 | `test_dual_channel_removal`、`test_dual_channel_removal_ws_first` |
| 撤单成功 | Canceled、双确认后删除 | `test_dual_channel_removal` |
| 撤单失败 | 状态不变、req 清空 | `test_cancel_failure_keeps_status`、`test_update_cancel_fail_clears_req` |
| GC | 终态过期清理、活动订单保留、索引同步 | `test_gc_removes_stale_orders` |
| 查询过滤 | symbol/active 过滤 | `test_orders_filters_active_and_symbol` |
| 重复订单 | 同 (symbol, order_id) 拒绝 | `test_prepare_cloid_rejects_duplicate` |
| WS 匹配 | 按客户端 id 匹配、oid 回退 | `test_update_from_ws_matches_by_cloid`、`test_update_from_ws_falls_back_to_oid` |
| 时间戳乱序 | 旧事件不覆盖新状态 | 需按 broker 事件模型补充 |

### 2.6 响应解析与错误映射

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| 下单响应 | resting/filled/error 各分支 | `test_parse_order_statuses_resting_and_filled` |
| 撤单响应 | success/error/空数组 | `test_parse_cancel_statuses` |
| 业务错误提取 | `err` 响应中提取交易所错误消息 | `test_parse_order_statuses_err_response`、`test_exchange_error_message` |
| 错误枚举 | 错误码 → 连接器错误类型 | 按 broker 的 Error 枚举补充 |

### 2.7 行情 / 事件解析（WS）

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| channel 分派 | 前缀/精确匹配规则 | `test_classify_channel` |
| side → 事件 | 主动买/卖 → 对应事件常量 | `test_trade_side_is_sell` |
| 仓位增量 | 成交方向对本地仓位的加减 | `test_apply_fill` |

> 这些逻辑应提取为纯函数再测，不要直接测 async 消息循环。

### 2.8 工具函数

| 用例 | 断言要点 | HL 参考 |
|---|---|---|
| nonce 单调 | 连续调用严格递增、时钟回退安全 | `test_next_nonce_monotonic`、`test_next_nonce_clock_setback` |
| 格式化 | 去尾零、精度 | `test_trim_wire_decimals` |

### 2.9 端到端（L3，ignored）

`e2e_order_roundtrip`：环境变量注入密钥 → 测试网挂一个远离盘口的限价单 → 断言 `New` → 撤单 → 断言 `Canceled`。
参考：`testnet_config()` + `e2e_testnet_order_roundtrip`。

### 2.10 资金费（Funding）

`hftbacktest::LiveEvent::Funding` 事件携带 `funding_rate` / `next_funding_time`（纳秒）/ `exch_ts`。
每个连接器负责从自己的数据源采集（WS 推流或轮询）并发布该事件。

| 用例 | 断言要点 | 参考实现 |
|---|---|---|
| 资金费消息反序列化 | 结构字段、`fundingRate` 字符串→f64、未知字段忽略 | OKX `test_deserialize_funding_rate`、Binance `test_deserialize_mark_price_update` |
| 时间戳单位换算 | 交易所毫秒 → 事件纳秒（×1_000_000） | 各连接器 `Funding` 发布处 |
| 事件字段完整性 | `symbol`/`funding_rate`/`next_funding_time`/`exch_ts` 都正确填充 | `LiveEvent::Funding` 定义（hftbacktest/src/types.rs） |
| 数据源接入 | WS 频道订阅参数正确（如 Binance `@markPrice`、OKX `funding-rate`）；HL 用轮询 | Binance `market_data_stream.rs`、OKX `public_stream.rs`、HL `connect_funding_poller` |

> 说明：Binance/OKX 用 WS 推流（消息结构可单测）；HL 无推流频道，用 60s 轮询
> `metaAndAssetCtxs`（`get_funding_rates` 依赖网络，逻辑拆成纯函数后可测）。

## 3. 新 broker 接入步骤

1. 运行生成器，得到测试骨架：

   ```bash
   python3 connector/scripts/gen_broker_tests.py <broker> --output connector/src/<broker>/tests.rs
   ```

2. 在 `<broker>/mod.rs` 里挂载：`#[cfg(test)] mod tests;`
3. 对照 `src/hyperliquid/` 逐类填充 `todo!()`（消息结构、订单构造、状态机、错误映射）
4. 用官方 SDK 生成签名/认证向量，替换占位测试
5. 配置测试网，跑 e2e
6. 验收：`cargo test -p connector --features <broker>` 全绿；e2e 单独 `-- --ignored e2e --nocapture`

## 4. 测试文件组织建议

```
src/<broker>/
├── mod.rs          # #[cfg(test)] mod tests;
├── tests.rs        # 生成器输出，按 2.1~2.9 分节
├── msg.rs          # 消息结构（2.2 的测试对象）
├── ordermanager.rs # 订单状态机（2.5 的测试对象）
└── rest.rs / ws.rs # 客户端（L2/L3 测试对象）
```
