# 单文件策略与统一发布验收

2026-10-31。基线 `1a7cc72083ecd9b5634c1c5319c65a034ffc1035` 加本次工作区修改，精确输入见 [冻结清单](source-inputs-before.json)。本地只编辑、审查和归档；编译、格式检查、静态检查、单元测试、原生回调与发布流程验证全部在 `192.168.3.88` 的独立目录进行，未覆盖服务器旧工作区。

## 已实现

- Titan 示例及 hft_quant/orderflow 平铺为单个 Python 文件，可选同名 Markdown；默认参数与 TESTS 声明在源码内。
- CLI 支持位置参数、可选参数文件、类型化 `--set`、默认 bundle/输出、源码发现、统一原生测试和简洁的人类可读输出；`--json` 保留完整协议。
- 策略项目通过 `titan.toml` 和自动生成的 `titan.lock` 固定已发布的工具链。Titan 平台自身提供配置样例，继续使用 Rust/Cargo/SDK 依赖声明完成自编译验证，避免将 debug/release 自编译程序固定成自身依赖。
- 管理端 preset 可省略参数文件；冻结源码到操作目录时显式保留原策略工作区配置，锁校验仍适用。
- Rust build 冻结源码、执行原生场景、生成运行配置并固定摘要；verify-release/publish 校验完整文件、工具链程序、原生测试证据、源码和最终参数身份。通用 SSH 发布不启动策略。

## 本轮结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| rustfmt 检查 | 通过，退出码 0 | [日志](fmt-check.log)、[退出码](fmt-check.exit) |
| 全 workspace/all-targets/all-features Clippy | `-D warnings` 通过 | [日志](clippy.log) |
| Rust 全工作区/all-targets/all-features | **727 passed，0 failed，26 ignored**；44 个测试二进制 | [日志](rust-workspace-test.log) |
| doctest | **2 passed，0 failed，1 ignored** | [日志](doctest.log) |
| SDK / debug CLI | **154 passed** | [日志](sdk-test.log) |
| release 构建与 SDK/AOT | 优化构建成功，**154 passed** | [构建](release-build.log)、[回归](release-sdk-test.log) |
| orderflow 内嵌原生场景 | **9 passed**；覆盖快照/失效/重同步/穿价带/独立预热/删价回补/覆盖边界 | [原生结果](orderflow-inline.json)、[最终发布构建](release-orderflow-build.json) |
| 发布、重复发布和运行配置离线校验 | 通过；verify-release 的 Python 路径故意设为不可执行 | [发布校验](release-verify.json)、[首次发布](release-publish.json)、[重复发布](release-publish-repeat.json)、[配置校验](release-runtime-validate.json) |
| 源码一致性 | 761 项输入远端前后及本地同期一致；hft 四项源码/配置与编译输入一致 | [前](source-inputs-before.json)、[后](source-inputs-after.json)、[本地](local-source-check.json)、[hft](hft-source-check.json) |

五项 CLI 集成测试还覆盖：无辅助文件默认编译、旧 flags 兼容、类型/范围/未知参数拒绝、最终参数身份一致、原生断言失败保留旧产物、工具链漂移与显式更新、未声明场景、签名信任、重算清单仍无法伪造原生源码身份、缺失文件/错误程序/虚假测试证据拒绝，以及带空格路径的通用 SSH 流程。SSH 传输在该组测试中由离线 shell fixture 执行，未连接实盘服务器。

## 最终产物身份

| 字段 | 值 |
| --- | --- |
| release ID | `771622fbf1ba6717bd131580d6301c13d4c43dde800b32c4259f9354d365e7ee` |
| 策略 / source version | `orderflow_events / 0.2.2` |
| ABI / 恢复 schema / 状态长度 | `13 / 3 / 62896 B` |
| compiled_strategy_digest | `f7e90424f6e5d5290c631f54d2600e745fafe5b698e39f2564be7f9f5c9921f5` |
| bundle SHA256 | `2a322d72f10977c25316fe9f2a9102d6c6d97c840ca15879880abf05d191660a` |
| Titan release 程序 SHA256 | `a03715c9a100d17cf5e0a2c5038ba9400557521bdaccfd5feac5b37e0ef81323` |
| Cargo.lock SHA256 | `d9e7d02e9e5f3731979d623862020ed2054ab9e2aa0892d0f2561845fb3ea8b8` |

[最终 manifest](release-orderflow-manifest.json)、[完整汇总](summary.json)、[工具链版本](toolchain.json)。Python 3.11.14、NumPy 2.2.6、Numba 0.63.1、llvmlite 0.46.0；Rust/Cargo 1.94.1。未升级 Cargo.lock。

每次新编译都以实际 bundle 身份固定部署配置；即使源码、ABI 或 schema 相同，也不假定新编译结果摘要相同。不重写旧发布目录，不把重编译当作旧 checkpoint 的兼容证明。

## 位置与复核

本轮编译根目录：`/home/dominolu/dev/titan-single-file-20261031-1a7cc720`。

最终发布目录：`/home/dominolu/dev/titan-single-file-20261031-1a7cc720/hft_quant/build/strategies/releases/orderflow_events-771622fbf1ba6717bd131580d6301c13d4c43dde800b32c4259f9354d365e7ee`。

验证过的程序同时位于 `/tmp/titan-single-file-target/release/titan` 和上述发布目录的 `titan`。复制到其他路径不改变其内容摘要；运行已编译产物与发布校验不需要 Python。

以下命令只用于复核本次隔离验收，不作为项目默认路径：

```bash
ssh 192.168.3.88
cd /home/dominolu/dev/titan-single-file-20261031-1a7cc720/src
export PATH="/tmp/titan-single-file-target/release:$PATH"
export TITAN_STRATEGY_PYTHON="/home/dominolu/dev/titan/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
titan strategy discover
titan strategy compile strategies/grid.py
titan strategy test strategies/grid.py --json
```

执行脚本保存在 [完整回归](remote-final-check.sh)、[优化程序与发布验收](remote-release.sh)、[顺序入口](remote-last.sh)。镜像配置仅放在隔离目录的父级 `.cargo/config.toml`，没有修改仓库原 Cargo 配置或依赖锁。

## 迁移与边界

hft_quant 只需维护 `strategies/orderflow_events.py` 和可选 `orderflow_events.md`；工具链和环境配置由工作区统一管理，产物写入忽略 Git 的 `build/strategies/releases/`。旧源码、原生测试、部署脚本、原产物及说明完整归档在本地 `hft_quant/reports/migrations/titan-single-file-20261031-legacy.tar.gz`，并附迁移回执。

本轮未启动实盘策略、未访问真实账户、未发送订单、未部署至实盘服务器。`environment=live` 的 valid 结果是既有运行配置的离线结构与产物校验；不代表公网行情或账户验收。本轮只读开发产物未签名，签名正负例使用公开 RFC 测试种子；实际加载仍遵守明确的 Ed25519 信任策略。

初期验证曾遇到默认 registry 缓存缺项、三项 Clippy 警告及迁移场景的观测边界预期错误，均修正后按阶段重跑。E04 tracker 断言恢复到 timer 消费前，事件计数在消费后断言，业务实现未为通过测试而放宽。验收完成后仅补充验收说明、工程记忆日期与任务完成状态，编译输入的功能代码保持不变。
