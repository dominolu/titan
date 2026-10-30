# orderflow_events 0.2.2 构建、测试与主网只读冒烟验收

2026-10-30。构建与测试在 `192.168.3.88`（`deepai`）的独立目录
`/home/dominolu/dev/titan-orderflow-20261030-c67acf9c/src` 完成，工具链 Rust/Cargo **1.94.1**、
Python **3.11.14**（见 [toolchain.log](toolchain.log)）；同日在 `43.130.234.54`
（Ubuntu 24.04.4，2 vCPU，glibc 2.39，Binance 公网可达）完成 30 分钟主网只读冒烟。

初次验收对应提交 `c67acf9c`；冒烟首次启动暴露两个平台阻塞，修复提交为 `4733a42c`，随后复跑
全部构建/测试与主网冒烟。本地只改源码与文档，格式化、编译、测试与制品生成都在编译服务器执行。

## 结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| rustfmt | 通过（修复后复跑） | [fmt.log](fmt.log) |
| Clippy（全工作区 / all-targets / all-features / `-D warnings`） | 通过（`EXIT=0`，修复后复跑） | [clippy-after-fix2.log](clippy-after-fix2.log) |
| `cargo test -p titan-cli`（含部署模板反序列化与容量校验测试） | **54 passed**（修复前 53），0 failed | [titan-cli-test.log](titan-cli-test.log)、[targeted-test-after-fix2.log](targeted-test-after-fix2.log) |
| `cargo test -p titan-strategy-runtime`（含新增失效订阅白名单测试） | **87 passed，3 ignored** | [runtime-test-after-fix.log](runtime-test-after-fix.log) |
| 全工作区 Rust 测试（all-targets / all-features） | **672 passed，0 failed，24 ignored**（修复前 670；42 个测试二进制） | [rust-workspace-test-after-fix2.log](rust-workspace-test-after-fix2.log) |
| doctest | **2 passed，0 failed，1 ignored** | [doctest-after-fix2.log](doctest-after-fix2.log) |
| SDK（含真实原生回调） | **125 passed**（`Ran 125 tests ... OK`） | [sdk-test.log](sdk-test.log) |
| orderflow_events 0.2.2 bundle | `source_version=0.2.2`、`recovery_state_schema_version=3`、`state_len=62896`、观测 schema `titan.orderflow_events/v3`（32 字段）、digest `a7be38b5…`、bundle `2405eb55…` | [orderflow-compile.json](orderflow-compile.json)、[orderflow-manifest.json](orderflow-manifest.json) |
| 部署模板校验 | 编译服务器离线与主网机均 `{"valid":true}` | [validate-live.json](validate-live.json) |
| **主网只读冒烟** | 30 分 12 秒 `RUNNING → STOPPED`，`exit_code=0`、`health=HEALTHY`、`error=null`；BBO 743,868 / DepthBatch 68,523 / StreamInvalidated 4；`WARN=0`、`ERROR=0`；RSS 146.7 MB、CPU ≈67% | [live-smoke/README.md](live-smoke/README.md) |
| 源码一致性 | 657 个文件 SHA256 记录并逐项复核通过（修复后复跑） | [source.sha256](source.sha256)、[source-check.log](source-check.log) |
| 制品摘要 | bundle `2405eb55…`、模板固定后 `runtime.toml` `2f96505f…` | [artifact-checksums.txt](artifact-checksums.txt) |

## 冒烟期间发现并修复的阻塞（提交 `4733a42c`）

1. **`strategy_subscription_unsupported`**：编译器为 `HOST_DEPTH_100` 自动加入的
   `titan.market.StreamInvalidated` 订阅不在 `supported_strategy_subscription` 白名单内，
   任何 100 档能力策略都无法通过服务定义校验。修复并加入白名单回归测试。
2. **`PRIMARY async lane registration failed`**：PRIMARY lane 的 snapshot staging 容量由
   `runtime.async_lane_capacity` 推导，模板 16384 超过
   `event_engine.snapshot_barriers.per_barrier_staging_capacity`（默认 8192）。修复为配置加载期
   显式校验并把模板改为 8192。
3. **`unbound symbol btcusdt`**：Binance connector 统一小写发布，模板 `native_symbol` 必须写
   `btcusdt` 而不是 `BTCUSDT`。模板与运行手册已更正。

## 边界

- 主网冒烟是**单次 30 分钟、单标的（BTCUSDT）、只读行情**：不构成长期稳定性、内存泄漏或引擎
  P99 性能结论；表中耗时为策略 callback dispatch 耗时（depth p99 154 µs），不是交易链路 P99。
- 没有接入账户或订单能力，未验证任何下单/撤单/风控路径。
- 业务观测（`book_valid`、`book_ready`、`warmup_remaining_ms`、事件计数）只通过管理台观测帧
  读取；本次未启动管理台，**未直接读取观测值**。可用 `titan console` 按同一模板部署补齐。
- 冒烟制品 `signed=false`；正式部署必须重签、配置 `trusted_ed25519_keys` 并关闭
  `allow_unsigned_artifacts`。
- 策略参数在编译时烘焙；修改 `parameters.json` 后必须重编并重新固定 `expected_digest`。
- 编译服务器默认工具链为 stable 1.93.0，本次按既有记录显式使用 `1.94.1`；隔离目录追加了
  rsproxy 镜像配置，未修改本地配置或 `Cargo.lock`。
- `docs/` 不在 [source.sha256](source.sha256) 清单内（与既有记录口径一致）。
