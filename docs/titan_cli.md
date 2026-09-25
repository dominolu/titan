# Titan CLI（Strategy ABI V13）

`titan` 是 V13 回测与实盘的统一入口。策略参数、订阅和 callback 只来自已编译 `.titan` manifest；运行时不导入策略 Python，也不接受 `strategy.json` 或 `module:function`。

## 编译与检查 artifact

```bash
titan strategy compile \
  --strategy strategies/pair_arb/strategy.py \
  --parameters strategies/pair_arb/parameters.json \
  --artifact-format bundle \
  --output pair_arb.titan \
  --signing-key ed25519-private.key \
  --key-id production-2026-09

titan strategy ls \
  --trusted-key production-2026-09=PUBLIC_KEY_HEX --json
titan strategy show pair_arb.titan \
  --trusted-key production-2026-09=PUBLIC_KEY_HEX --json
titan strategy validate pair_arb.titan --require-signature \
  --trusted-key production-2026-09=PUBLIC_KEY_HEX --json
```

编译是唯一启动 Python/Numba 的 CLI 路径。生产部署默认要求 Ed25519 签名；开发或 shadow 配置只有显式设置 `strategy_service.allow_unsigned_artifacts = true` 才能加载 unsigned artifact。`trusted_ed25519_keys` 支持同时保留多个 key id 以完成轮换。

回测也执行制品信任校验。签名制品在 `[backtest].trusted_ed25519_keys` 中配置 `key_id = "64位十六进制公钥"`；仅开发用 unsigned 制品必须显式设置 `allow_unsigned_artifact = true`。解析阶段和隔离 worker 会使用同一份策略重复校验，防止 RunSpec 生成后替换制品或降低信任要求。

## V13 Tick trace 回测

```bash
titan validate deploy/pair_arb_v13/artifacts/pair_arb.titan \
  -e backtest -m tick -c configs/pair_arb_v13_backtest.toml

titan run deploy/pair_arb_v13/artifacts/pair_arb.titan \
  -e backtest -m tick -c configs/pair_arb_v13_backtest.toml
```

回测配置只描述 trace 和运行边界：

```toml
schema_version = 1
history_capacity = 1024

[backtest]
data = "../fixtures/pair_arb_v13_trace.json"
command_capacity = 64
allow_unsigned_artifact = true # 仅 checked-in 开发示例
```

trace schema 为 1，包含初始 V13 public projections，以及按顺序交付的 tick/depth/account/timer 事实。CLI 使用 `OfflineV13Adapter` 加载与实盘相同的 `.titan`，返回 event、submit、cancel 计数和最终 typed-state digest。需要真实撮合、延迟、费用和队列模型时，HftBacktest 的 `backtest::strategy_v13` 使用同一 adapter，将 staged command 接到其执行模型。

当前公开 V13 profile 仅支持 Tick；Bar/Hybrid 会在 controller、SDK 和 Strategy Service 边界被明确拒绝，直到真实 producer、聚合器和契约测试完整。

## Core Live

```bash
titan validate pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb_v13/runtime.toml

titan run pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb_v13/runtime.toml
```

Live 的位置参数是部署配置中的 `strategy_key`。部署侧只配置 artifact URI/digest、market/account binding、risk scope、recovery 和 runtime policy；参数、订阅与 callback 均以 artifact manifest 为准。

## 任务管理

```bash
titan ls --active
titan show <run-id> --json
titan logs <run-id>
titan stop <run-id>
```

`run`、`validate`、`ls`、`show`、`logs`、`stop` 和 `strategy` 子命令支持 `--json`。机器模式的 stdout 只输出 JSON；错误从 stderr 返回稳定的 `error.code` 与 `error.message`。

## Result 与报告

```bash
titan report <run-id>
titan report <run-id> --renderer native --output report.html
```

Rust Runtime 是策略 state、订单、成交与账户事实的唯一权威。Python reporting 只校验并渲染 ResultBundle，不重新计算成交或收益。
