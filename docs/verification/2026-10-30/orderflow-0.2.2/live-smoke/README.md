# orderflow_events 0.2.2 主网只读冒烟（2026-10-30）

在 `43.130.234.54`（`VM-0-33-ubuntu`，Ubuntu 24.04.4，2 vCPU，glibc 2.39，Binance 公网可达）
用编译服务器产出的 release `titan`（sha256 `a26d837b…`）运行 Binance USDⓈ-M 主网 BTCUSDT
只读行情 30 分钟。策略不下单、无账户、制品未签名（模板 `allow_unsigned_artifacts=true`）。

## 配置与制品

| 项 | 值 |
| --- | --- |
| 源码基线 | 提交 `bbebc1c6` + 冒烟修复（提交 `4733a42c`，二进制与验收源码均包含） |
| 二进制 | release `titan`，sha256 `a26d837b5144e18dd286ec5e8bd26491a12336bb34b6038caf5c7b472c11f691` |
| bundle | `orderflow_events.titan` sha256 `2405eb55…`，digest `a7be38b5…`，0.2.2 / schema 3 / 62,896 B |
| runtime.toml | sha256 `2f96505f…`（`async_lane_capacity=8192`、`native_symbol=btcusdt`） |
| 运行命令 | `titan run orderflow_events -e live -m tick -c runtime.toml --detach`，`RUST_LOG=info,titan_strategy_runtime=debug` |

## 首次启动暴露的阻塞（已修复并复跑）

1. `strategy_subscription_unsupported` —— `HOST_DEPTH_100` 自动加入的
   `titan.market.StreamInvalidated` 不在服务端白名单。修复后该订阅在冒烟中实际交付 4 次。
2. `PRIMARY async lane registration failed` —— `async_lane_capacity=16384` 超过
   `snapshot_barriers.per_barrier_staging_capacity=8192`；配置加载期已加校验，模板改为 8192。
3. `unbound symbol btcusdt` —— connector 小写发布，模板 `native_symbol` 改为 `btcusdt`。

## 结果（30 分 12 秒，`run-18e339413ecda4a5-2fb319`）

| 观察 | 结果 |
| --- | --- |
| 生命周期 | `RUNNING` → `STOP_REQUESTED` → `STOPPED`，`exit_code=0`、`error=null`、`health=HEALTHY` |
| 行情回调 | BBO **743,868**、DepthBatch **68,523**、StreamInvalidated **4** |
| 策略 callback dispatch | depth p50 **53 µs** / p95 **78 µs** / p99 **154 µs** / max 5.5 ms；BBO p50 <1 µs、p99 7 µs |
| 日志 | 115 MB / 812,409 行；`WARN=0`、`ERROR=0` |
| 进程 | RSS 144.2 MB（5 min）→ 146.7 MB（30 min）；CPU ≈ 67%（2 vCPU） |
| 失效处理 | 4 次 `StreamInvalidated` 全部交付，策略按 epoch/reset 清窗口，运行未中断 |

## 边界

- 单次 30 分钟、单标的只读冒烟；不是长期稳定性/内存泄漏结论，也不是引擎 P99 性能结论。
- 未接入账户/订单，未验证交易路径。
- 业务观测（`book_valid`/`book_ready`/`warmup_remaining_ms`/事件计数）需通过管理台观测帧读取，
  本次未启动管理台，未直接读取观测值。
- 制品未签名；正式部署需重签并配置 `trusted_ed25519_keys`。

## 证据

- `run-final.json`：最终 run 记录（STOPPED / exit 0 / 0.2.2）
- `dispatch-counts.txt`、`dispatch-samples.txt`、`latency.txt`：回调计数与耗时
- `log-summary.txt`、`worker-log-excerpt.txt`：日志规模、首尾与抽样
- `host.txt`、`artifact-checksums-live.txt`：主机与二进制/制品/配置摘要
- 修复后回归：`../rust-workspace-test-after-fix2.log`（672 passed）、`../doctest-after-fix2.log`、
  `../clippy-after-fix2.log`、`../runtime-test-after-fix.log`

## 复现

```bash
# 编译服务器（无 Binance 访问）
cargo test --workspace --all-targets --all-features
cargo build --release -p titan-cli
titan strategy compile --strategy strategies/orderflow_events/strategy.py \
  --parameters strategies/orderflow_events/parameters.json \
  --target x86_64-unknown-linux-gnu --cpu-baseline x86-64-v2 \
  --strategy-format bundle --output deploy/orderflow_events/artifacts/orderflow_events.titan --json

# 主网只读机器
titan validate orderflow_events -e live -m tick -c runtime.toml --json
titan run orderflow_events -e live -m tick -c runtime.toml --detach --json
titan show <run_id> --json   # STOPPED / HEALTHY / exit_code
titan stop <run_id>
```

> 迁移说明（2026-10-31）：策略源码与部署目录已从 titan 仓库迁至 hft_quant 项目
> （`strategies/orderflow_events/`、`deployment/orderflow_events/`）。本记录保留当时的
> 路径与命令，供追溯；之后的构建/发布按 hft_quant 的 `deployment/orderflow_events/build.sh`
> 与 `publish_smoke.sh` 执行。
