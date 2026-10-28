# 深度快照、Top100 与 V13 语义验收

2026-10-28。本轮在 `192.168.3.88` 的独立目录 `/home/dominolu/dev/titan-depth-20261028-28c083bd` 验证。源码基线 `28c083bda82d875e3e4052a34d8db7ab5bd7f8b0` 加本轮修改；服务器目录没有 Git，以 [619 个源码及 fixture 文件的 SHA256](source.sha256) 和 [逐项一致结果](source-check.log) 标识实际输入。保留本地原有文档和未跟踪策略，不覆盖既有远端源码或运行目录。

本地仅修改和审查，全部格式化、Clippy、编译、代码生成及测试在编译服务器执行。没有访问真实账户、实盘服务器、生产数据库或执行部署。全部行情验收使用合成数据；没有运行 ignored 网络/实盘测试。

后续[代码审查](review.md)发现 **2 项 P1 和 1 项 P2 待修复**。下列测试通过不代表这些场景已覆盖，本提交尚不适合生产部署。

## 结果

| 项目 | 结果与证据 |
| --- | --- |
| Rust 格式检查 | [通过](fmt.log) |
| 全工作区 Clippy / all-targets / all-features / `-D warnings` | [通过](clippy.log) |
| 全工作区 Rust / all-targets / all-features | **662 passed，0 failed，24 ignored**，[日志](rust-test.log) |
| doctest | **2 passed，0 failed，1 ignored**，[日志](doctest.log) |
| SDK，包括真实原生策略回调 | **120 passed**，[日志](sdk-test.log) |
| CLI 构建 | [通过](build.log) |
| 原生完整深度链路 | [通过](depth-pipeline.log)：48,052 B / 2000 条快照经过 arena、PRIMARY、V13 解码和原生回调；删除前排补回未变化的第 101 档、未知代次失效清盘、新快照替换，以及同一离线重建器和旧输入拒绝 |
| CLI trace 输入 | [两项通过](trace-final/summary.json)：带显式元数据回放成功；缺失元数据以退出码 31 明确拒绝，[日志](trace-test.log) |
| 实际 orderflow_events bundle | [编译成功](orderflow-build.log)，版本 **0.2.0** / 恢复 schema **2**，状态 **62,904 B**，余量 **2,632 B**，[清单](orderflow-manifest.json) |

原生回调测试还断言 100 档视图中的 `depth5` 仍严格等于前五档之和；另覆盖：快照不产生伪成交/refill，重同步清空旧窗口和幽灵价位，空条目失效仍交付，BBO 不能重开失效盘口，移出 Top100 不当作消耗。Rust 测试覆盖：未变第 101 档补位、覆盖不足、旧代次/旧 publication 快照拒绝、大 diff 不误清盘、arena 容量边界、启动配置容量校验及 Binance 快照/diff 衔接。

工具链为 Rust/Cargo **1.94.1**、Python **3.11.14**，见 [版本记录](toolchain.log)。复用编译服务器已有 `/tmp/titan-deplugin-target` 缓存。隔离目录的 `.cargo/config.toml` 追加既有 rsproxy 镜像配置供 `cargo fmt` 子进程读取；本地配置与 Cargo.lock 没有更改，环境专用配置不纳入源码一致性清单。

策略编译结果摘要为 `11385c2544a16d8a71d614603f72e647ead29243e09c8103bf5c1fa3c927f47f`。CLI 和 bundle 的文件 SHA256 见 [产物清单](artifact-checksums.txt)。bundle 留在上述编译服务器的 `verification/orderflow.titan`；它是非实盘验证用未签名产物，没有改变生产签名或加载策略。

## 复验入口

在独立编译服务器目录设置以下环境，不在本机执行：

```bash
export CARGO_TARGET_DIR=/tmp/titan-deplugin-target CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=8
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
export PYTHONPATH="$TITAN_STRATEGY_SDK:$PWD"
```

正式验收使用 `cargo fmt --all -- --check`、全工作区 Clippy `-D warnings`、`cargo test --workspace --all-targets --all-features`、doctest 和 SDK unittest discover。补充入口：

- [原生链路探针](../../../../crates/titan-strategy-runtime/examples/v13_depth_pipeline_smoke.rs)，输入 [能力 fixture](../../../../python/titan-strategy-sdk/tests/fixtures/v13_host_depth.py) 的编译 bundle，fixture 参数为空对象。
- [CLI trace 验收](../../../../scripts/verify_depth_trace.py)，传 `--binary`、`--strategy`、`--output`，输出目录必须为新目录。
- [策略原生回调测试](../../../../python/titan-strategy-sdk/tests/test_orderflow_depth.py)。

## 验收边界

- 当前以 64 KiB 单批为界；更大 diff 失败关闭并恢复，不支持任意深度分片。
- 输入原生 `pu` 在 connector 校验；没有改写旧 ABI 冒充已向策略暴露它。新能力通过独立 versioned payload 提供 epoch、来源范围、水位和有效性。
- 100 档能力要求两侧各至少 100 个可信价位；缺乏覆盖时不输出不完整 Top100。默认 Binance 快照仍为 1000 档。
- 源码正确性和合成回归不等于公网长期可靠性、生产队列/延迟预算或实盘验收。arena 槽位由 8192 减至 2048，块数据总预算保持 128 MiB，积压能力须结合部署单独测量。
- 原 25 档历史不能证明 100 档事件；本轮增加显式元数据的离线入口，没有生成或宣称已有完整 100 档历史。
- 旧恢复 schema 1 不直接加载。策略恢复后清理行情窗口，默认需要重新预热完整 15 分钟。

完整职责和协议见 [深度契约](../../../depth_pipeline.md)。
