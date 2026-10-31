# 单文件策略审查修复与提交验收

2026-10-31，在 `192.168.3.88:/home/dominolu/dev/titan-single-file-review-20261031` 独立验收。基线为 `1a7cc72083ecd9b5634c1c5319c65a034ffc1035`，输入来自待提交 Git tree，未包含其他任务未提交的跨所策略和研究文档。本地仅编辑、审查和归档，没有本地构建或测试。

## 修复

1. 启动 SDK worker 前核对已锁定的 Titan、SDK、Rust 与 ABI 身份。变更 SDK 的导入标记测试覆盖 describe 和 compile，均在执行前拒绝；worker 返回后继续核对动态版本和阶段一致性。
2. orderflow 的内嵌场景仅在各自的状态副本中设置固定测试配置，关闭 E04、修改本地品种号和组合覆盖均可编译。直接检查产物初始状态，证明测试配置没有进入实际发布状态。
3. 发布构建和发布校验对秘密目录、checkpoint 和 SQLite 路径使用临时校验投影，保留发布配置中的原路径。相对目录、尚不存在的绝对目录均可构建，不会在编译机创建目标路径；普通节点 validate 仍拒绝缺失秘密目录，非法容量和 `:memory:` 恢复配置仍拒绝。

## 结果

| 验收 | 结果 | 证据 |
| --- | --- | --- |
| CLI 定向集成 | 6 passed | [日志](targeted.log) |
| Clippy 全 workspace/all-targets/all-features | `-D warnings` 通过 | [日志](clippy.log) |
| Rust 全 workspace/all-targets/all-features | 728 passed，0 failed，26 ignored | [日志](rust.log) |
| doctest | 2 passed，0 failed，1 ignored | [日志](doctest.log) |
| SDK | 119 passed | [日志](sdk.log) |
| 优化构建、rustfmt | 通过 | [构建](release-build.log)、[格式](fmt-check.log) |
| orderflow 默认及三组参数覆盖 | 每组 9 个原生场景通过，产物初始配置保持实际参数 | [汇总](summary.json)、[状态检查](parameter-state-check.json) |
| 无 Python 发布校验及重复发布 | 通过 | [校验](release-verify.json)、[首次发布](release-publish.json)、[重复发布](release-publish-repeat.json) |
| 冻结输入 | 2437 项文件远端前后一致 | [前](inputs-frozen.json)、[后](inputs-after.json)、[检查](source-check.json) |

原验收 SDK 154 项还包含另一任务尚未提交的跨所策略回归；本轮 119 项来自本次实际待提交范围。新增功能测试与完整回归均通过后未改功能代码，仅补充文档和验收记录。

参数覆盖包括 `e04_enabled=false`、`asset_no=1`，以及同时设置二者和 `e11_lookback_ms=60000`。完整运行入口见 [脚本](verify.sh)，最终身份见 [release 清单](release-manifest.json)、[工具链锁](titan.lock) 和 [汇总](summary.json)。Cargo.lock 保持原摘要，未升级依赖。

## 最终身份与范围

- release ID：`7ad03e86a82319e91b653a60542b5bb50282568cccc6a5a0a95a7800b997cf55`
- Titan SHA256：`c5dee89ba61514985a3b32aa5f80c5a785b2d2660d2d5d3988448a94e13b783f`
- bundle SHA256：`69004f2f5462420d7288b37a087405112459204016bc51d86da95ba86ff65e17`
- 服务端发布包：`hft_quant/build/strategies/releases/orderflow_events-7ad03e86a82319e91b653a60542b5bb50282568cccc6a5a0a95a7800b997cf55`，相对于上述独立验收目录。

`hft_quant` 是仓库外的策略消费项目，目前没有 Git 仓库；其源码和本地工作区锁单独保留，不复制回 Titan。既有发布和迁移归档保持原样。`/tmp/titan-single-file-target` 是可复用构建缓存，历史版本应从各自不可变发布目录读取。

本轮没有访问实盘账户、发送订单、启动实盘策略或部署到实盘服务器。SSH 发布传输由离线 shell fixture 验收，未连接实盘节点。旧发布或 checkpoint 的兼容性不由本次重新编译推断。
