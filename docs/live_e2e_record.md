# 实盘端到端验证记录（Rust 策略 → 连接器 → LiveBot）

日期：2026-08-21
机器：macOS（aarch64），rustc 1.94.0
目标：验证「回测代码原样跑实盘」的最后一环——Rust `Strategy` trait 策略经
iceoryx 共享内存连接器进程跑通真实行情链路。

## 结论

✅ 全链路已跑通：**连接器进程 → LiveBot → Strategy on_tick/on_bar → 下单 → 订单事件回流**。

* `on_tick` 收到交易所真实行情（Binance USD-M 测试网 BTC-USDT 实时盘口与成交）；
* `on_bar` 按 1s 全局 bar 正常触发；
* 回调内下单经连接器到达交易所 REST，失败/成交事件回流到 LiveBot 本地订单表并更新持仓；
* 回测与实盘共用同一份 `MarketMaking` 策略代码，无需任何修改。

## 验证过程

### 1. 启动连接器（Binance USD-M 测试网，无 API key 也可跑公共行情）

```console
cargo run -p connector -- my-bf binancefutures connector/examples/binancefutures.toml
```

### 2. 启动 Rust 做市策略（20 秒）

```console
RUST_LOG=info cargo run -p titan-examples --bin live -- \
    --connector-name my-bf --symbol btcusdt \
    --tick-size 0.1 --lot-size 0.001 --run-seconds 20
```

### 3. 日志证据

on_tick 收到真实行情（每秒快照，mid/bid/ask 来自测试网实时盘口）：

```text
INFO titan_examples::market_making: tick snapshot asset_no=0 frame_ts=1787242672012795000 mid=72293.85 bid=72289.4 ask=72298.3 position=0.0 orders=2
INFO titan_examples::market_making: tick snapshot asset_no=0 frame_ts=1787242673027447000 mid=72293.1  bid=72289.4 ask=72296.8 position=0.0 orders=2
```

on_bar 触发（1s 全局 bar，OHLCV 来自帧内成交）：

```text
INFO titan_examples::market_making: bar closed asset_no=0 open=72299.4 high=72299.6 low=72299.4 close=72299.6 volume=0.025
```

回调下单 → 连接器提交 → 订单事件回流（本次无 API key，被交易所拒绝
`-2014 API-key format invalid`，拒绝结果以订单状态事件回到 LiveBot 本地订单表；
配好 key 后同一条路径会收到 New/PartiallyFilled/Filled）：

```text
INFO live: order update order_id=1099512350778 side=Sell req=None status=Expired price_tick=723002 qty=0.001
INFO live: order update order_id=722756     side=Buy  req=None status=Expired price_tick=722756 qty=0.001

ERROR connector::binancefutures::ordermanager: submit error error=OrderError { code: -2014, msg: "API-key format invalid." }
```

20 秒运行汇总（约 976 帧/秒，1ms 全局帧）：

```text
INFO live: live run finished frames=19527 orders_placed=112 orders_canceled=0 position=0.0
```

## 与 OKX 模拟盘 / Hyperliquid 测试网的对比

原计划的 OKX 模拟盘与 HL 测试网各有一个外部阻塞，本次用 Binance 测试网完成验证：

| 目标 | 状态 | 阻塞点 |
| --- | --- | --- |
| Binance USD-M 测试网 | ✅ 已跑通 | 无（公共行情免 key；下单需测试网 key） |
| OKX 模拟盘 | ⚠️ 可接入 | 本机直连 OKX 超时（需代理），连接器 WS 暂不支持代理；另需模拟盘 API key |
| Hyperliquid 测试网 | ⚠️ 可接入 | 测试网 WS 直连可达，但连接器要求 32 字节测试网私钥才能启动；另需 key 下单 |

## 拿到 API key 后的完整成交验证

1. Binance 测试网：在 `connector/examples/binancefutures.toml` 填入
   `api_key`/`secret`（测试网 key），重启连接器，重跑第 2 步。此时
   `order update` 应出现 `New` → `Filled`，`tick snapshot` 的 `position` 变为非零。
2. OKX 模拟盘：填好 demo key（`connector/examples/okx.toml`，`simulated = true`），
   并确保到 `wss://wspap.okx.com:8443` 的网络可达（直连或给 WS 加代理支持）。
3. Hyperliquid 测试网：在 `connector/examples/hyperliquid.toml` 填测试网私钥
   （`is_mainnet = false`），symbol 用 `BTC`。

## 复现步骤（含回测对照）

```console
# 回测（合成数据，同一份策略代码）
cargo run -p titan-examples --bin backtest

# 实盘
cargo run -p connector -- my-bf binancefutures connector/examples/binancefutures.toml
RUST_LOG=info cargo run -p titan-examples --bin live -- \
    --connector-name my-bf --symbol btcusdt --tick-size 0.1 --lot-size 0.001 --run-seconds 30
```

注意：环境变量 `RUST_LOG=warn` 会隐藏 INFO 行情日志，取证时用 `RUST_LOG=info`。
