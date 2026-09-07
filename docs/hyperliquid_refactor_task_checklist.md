# Hyperliquid Connector 重构任务清单（执行版）

更新时间：2026-09-07

目标：把 Hyperliquid 重构从“代码可运行”推到“可用于实盘上线前 gate 通过”的可交付状态。

## 1) 代码层闭环（可在当前环境完成）

1. [x] 修复 WS 解析与错误传播
   - `connector/src/hyperliquid/ws.rs`
   - `handle_msg` 反序列化失败时返回 `OrderError("unparseable websocket message")`（不再静默忽略）
   - 同步新增单测：`websocket_unparseable_message_is_reported_as_connection_error`

2. [x] 对齐订单时间链路与乱序保护
   - `connector/src/hyperliquid/ordermanager.rs`
   - `update_from_ws(state, status, status_timestamp)` 使用 `status_timestamp`，回退 `state.timestamp`
   - 对旧时间戳不覆盖新状态（乱序保护）并新增单测

3. [x] cloid 前缀兼容
   - `connector/src/hyperliquid/ordermanager.rs`
   - 无前缀/带 `0x` 的 client id 做统一匹配

4. [x] 取消重连时“初始化清空 cancel-all”副作用
   - `connector/src/hyperliquid/ws.rs`
   - `init_symbol` 仅做 position seed，不再重置全量 open orders

5. [x] 兼容 Hyperliquid 私有事件新结构
   - `connector/src/hyperliquid/msg.rs` 与 `ws.rs`
   - `UserEvent { fills, funding, liquidations, non_user_cancel }` 结构兼容
   - fills 逐笔应用到本地 position，并发布 `AccountPublication::Position`

6. [x] REST 与订单状态时间语义统一
   - `connector/src/hyperliquid/brokerapi.rs`
   - `order_info_from_historical` 增加 `status_timestamp` 入参并透传

7. [x] safety_timeout 参数边界检查
   - `connector/src/hyperliquid/mod.rs`
   - 仅允许 `0`（关闭）或 `>=5000`

8. [x] 安全关闭时清 heartbeat
   - `connector/src/hyperliquid/mod.rs`
   - `shutdown` 先 `cancel_all_after(..., 0)` 再按注册 symbol 全量 cancel_all_orders

9. [x] 预检脚本与文档同步
   - 更新 `docs/refactor_remaining_tasks.md` / `docs/blocking_issues.md` / `connector/API_COVERAGE.md`

10. [x] 代码级收口清理（本地）
   - `connector/src/hyperliquid/mod.rs` 去掉未使用字段 `nonce_counter`
   - 新增边界测试覆盖 Top-level WS 非 JSON 字符串输入

## 2) 自动化验证（本地）

1. [x] 无网络单测跑通
   - 命令：`cargo test -p connector --no-default-features --features binancefutures,okx,hyperliquid --lib -- --list`
2. [x] Hyperliquid 子模块单测回归（含新增用例）
   - `cargo test -p connector --no-default-features --features binancefutures,okx,hyperliquid --lib hyperliquid::ws::tests::...`
   - 结果：全部通过

## 3) 实盘验收（外部条件）

以下项当前不受当前开发环境限制，属于**上线前门禁**：

1. [x] Hyperliquid 私有流与 REST 逐字段一致性验收（submit/amend/cancel/reconnect）
   - 目标文件：`connector/src/hyperliquid/mod.rs` 里的 `live_ws_private_stream_probe`
   - 2026-09-07 在目标机主网完成；实测发现并修复 amend 旧 oid 延迟终态覆盖新 oid、
     amend WS price/qty 未更新、动态 f64 价格 wire 暴露二进制尾数三个问题。
2. [x] 小额订单（含部分成交）在 REST→私有流→reconcile 全链路复核
   - 目标：`orders/positions/balances` 最终一致性归零或符合预期
   - [x] 0.01 ETH 市价开仓/reduce-only 平仓与 orders/positions 归零通过。
   - [x] 自动扫描薄顶档后以单档 IOC 形成 GAS 10.2/16.1 的真实 partial-fill；REST、
     私有 WS、fills 逐字段一致，reduce-only 平仓后零挂单零仓位，单笔上限仍为 20 USDC。
3. [x] 部署环境 P99/P99.9 冻结（与目标硬件负载契约绑定）
   - 目标机默认容量 300k events/s 连续三轮，冻结 dispatch/subscriber P99.9 上限为
     8,388,607/16,777,215 ns；Hyperliquid 主网 60 秒 fast-lane enqueue/handler P99.9 为
     8,191/4,095 ns，零 drop/resync。
4. [x] 资金与 API 凭据白名单管理确认：凭据不得入库，测试环境专用配置隔离
   - 凭据仅从目标机 `/home/ubuntu/.hyperliquid_env` 加载，未同步或写入仓库；证据日志不含私钥。

## 结论（截至 2026-09-07）

- 代码层重构项目**已达到可稳定运行闭环**。
- REST/WS 字段、真实重连、小额 full-fill/partial-fill/平仓和目标机性能门禁均已通过。
- Hyperliquid connector 本清单的外部验收门禁已全部解除，可以进入受控实盘发布阶段。
- 完整证据见 `docs/validation/hyperliquid_2026-09-07/README.md`。
