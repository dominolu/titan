# orderflow_events 0.2.2 构建与测试验收

2026-10-30。输入源码为提交 `c67acf9c`（`fix(strategy): orderflow_events 0.2.2 warmup, sweep band and
smoke template`），在 `192.168.3.88`（`deepai`）的独立目录
`/home/dominolu/dev/titan-orderflow-20261030-c67acf9c/src` 验证；工具链 Rust/Cargo **1.94.1**、
Python **3.11.14**（见 [toolchain.log](toolchain.log)）。

本地只改源码与文档；格式化、编译、测试和制品生成都在编译服务器执行（仓库 `AGENTS.md`）。

## 结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| rustfmt | 通过 | [fmt.log](fmt.log) |
| Clippy（全工作区 / all-targets / all-features / `-D warnings`） | 通过（`EXIT=0`） | [clippy.log](clippy.log) |
| `cargo test -p titan-cli`（含新部署模板反序列化测试） | 53 passed（另有 4/1/3 集成测试通过），0 failed | [titan-cli-test.log](titan-cli-test.log) |
| 全工作区 Rust 测试（all-targets / all-features） | **670 passed，0 failed，24 ignored**（42 个测试二进制） | [rust-workspace-test.log](rust-workspace-test.log) |
| doctest | **2 passed，0 failed，1 ignored** | [doctest.log](doctest.log) |
| SDK（含真实原生回调） | **125 passed**（`Ran 125 tests ... OK`） | [sdk-test.log](sdk-test.log) |
| orderflow_events 0.2.2 bundle | 编译成功：`source_version=0.2.2`、`recovery_state_schema_version=3`、`state_len=62896`、观测 schema `titan.orderflow_events/v3`（32 字段）、digest `a7be38b5…` | [orderflow-compile.json](orderflow-compile.json)、[orderflow-manifest.json](orderflow-manifest.json) |
| 部署模板校验 | `titan validate orderflow_events -e live -m tick` → `{"valid":true}`（离线，未连行情） | [validate-live.json](validate-live.json) |
| 源码一致性 | 657 个文件 SHA256 记录并逐项复核通过 | [source.sha256](source.sha256)、[source-check.log](source-check.log) |
| 制品摘要 | bundle `2405eb55…`、固定 digest 后的 `runtime.toml` `f026b59e…` | [artifact-checksums.txt](artifact-checksums.txt) |

## 边界

- **没有访问 Binance 公网**：编译服务器到 `fapi.binance.com` / `fstream.binance.com` 连接超时，
  主网只读冒烟未执行；`validate` 只校验配置、制品与信任策略，不建立行情连接。
- 未使用签名：本次 bundle 为 `signed=false` 的冒烟制品，模板使用 `allow_unsigned_artifacts=true`；
  正式部署必须重新签名并配置 `trusted_ed25519_keys`。
- 单元与合成回归不等于真实 100 档快照、重同步和长跑稳定性；实盘冒烟需在有公网的主网机器上
  按 [deploy/orderflow_events/README.md](../../../../deploy/orderflow_events/README.md) 第 4–5 步执行。
- 策略参数在编译时烘焙；修改 `parameters.json` 后必须重编并重新固定 `expected_digest`。
- 编译服务器默认工具链为 stable 1.93.0，本次按既有验收记录显式使用 `1.94.1`；隔离目录追加了
  rsproxy 镜像配置，未修改本地配置或 `Cargo.lock`。
- 本轮验收对应提交 `c67acf9c` 的源码；`docs/` 不在 [source.sha256](source.sha256) 的清单内（与既有记录口径一致）。
