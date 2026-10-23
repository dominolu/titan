# Rust 编译控制器重构验证

日期：2026-10-21。Git 基准：`39f1267b6412165fc7adc005719bc9e09dcef9a8`。本轮迁移编译调度和制品职责，保留 Numba 策略及必要桥接；报告功能不变。

## 职责边界

| 职责 | 实现 |
| --- | --- |
| 读取并固定源码/参数、host target/CPU baseline 检查 | Rust CLI `compiler.rs` |
| 短生命周期 Python 进程、固定 hash seed、60/300 秒超时、CPU/内存限制、子进程组回收、SIGINT/SIGTERM 取消 | Rust CLI `compiler.rs` |
| 受限策略定义加载、Python AST/Numba 类型检查、typed state 与 LLVM PIC 对象生成 | Python SDK |
| 原始符号报告的协议检查、`nm`/`objcopy` 规范化、ABI descriptor 生成、`cc` 链接、动态依赖/导出审计 | Rust CLI + runtime `v13/compiler.rs` |
| canonical CBOR、manifest、摘要、Ed25519 签名、pair/bundle 发布与失败回滚 | Rust runtime `v13/compiler.rs` |
| 制品 ABI/摘要/签名/订阅契约校验 | 复用现有 Rust `NativeArtifactLoaderV13` |
| HTML/QuantStats 报告与 ResultBundle renderer | 原有 Python reporting，未修改 |

Python 不读取签名私钥，不调度编译子进程，不链接、打包或校验制品。`subprocess` 仅用于受限加载器禁止策略启动子进程的保护逻辑。SDK 生产 Python 源码相对基准净减少 573 行。SDK 移除 `CompileRequest` / `compile_package` / `verify_artifact` 制品 API、独立 Python 编译入口、`cbor.py` 和 `cryptography` 依赖；用户统一调用 `titan strategy compile`。Numba 类型/AST 校验属于桥接要求，继续保留。

## 兼容性与发布

- V13 ABI fingerprint、C 布局、CBOR 字段、签名覆盖范围保持不变；runtime loader 继续严格校验，不接受旧 fingerprint 的兼容分支。
- 两个 Python 阶段消费同一份冻结源码/参数，元数据必须相同；Rust 校验 typed schema hash、回调集合、原始符号和导出集合。
- 构建身份更新为 `titan-numba-bridge/1`；参数摘要使用 Rust 递归排序 JSON。重新构建可能产生不同制品摘要；旧检查点不自动迁移。
- 支持编译主机对应的 Linux ELF target；不提供跨 target 编译。保留 x86-64/generic、x86-64-v2 和 aarch64 generic/armv8-a 对应的校验。
- 私钥由 Rust 读取和使用，支持 32 字节原始 seed 与 64 字符 hex。原始 seed 不 trim，避免误删合法空白字节；密钥不进入 Python 请求、环境或元数据。
- 制品在目标文件系统暂存并复用 runtime loader 验证，bundle 以 rename 替换；pair 的 manifest 最后发布，失败恢复已有文件。pair 替换窗口读到混合版本时依靠摘要校验拒绝。严重 I/O 错误导致回滚失败时保留备份目录并报告路径。

## 回归覆盖

1. 真实 Numba 策略通过 Rust 生成签名 pair/bundle，摘要一致；重复构建字节一致。
2. 设置不可执行的 Python 路径后，Rust 仍可按可信公钥验证签名制品；未知签名公钥和篡改均拒绝。
3. 不合法策略、错误 target、错误 ABI/schema/callback 元数据失败；失败不覆盖旧制品，临时目录清理。
4. 二进制私钥的首尾空白字节保留；文本 key 严格 hex 校验。
5. 超时或实际 SIGTERM 中断后回收进程组，后台子进程不能继续写入标记文件；没有发布制品或遗留工作目录。
6. 保留 SDK 类型/只读视图、错误回调状态、双向持仓查询与下单方向、pair_arb 状态机和全部示例的 AOT/ctypes 测试；编译和制品检查改为调用 Rust CLI。
7. 运行原有 artifact、pair、ServiceCore/common recovery、drain、timer failure、CLI bundle/report/SIGTERM 回归。

全量回归还暴露一个既有测试同步问题：`latest_slot_blocks_later_critical_delivery_until_fifo_predecessors_drain` 在收到 handler 消息后立即断言引用为零，此时接收线程可能尚未释放 TrackedDelivery。修复仅让测试接收线程可 join，并在原断言前等待线程退出；不修改事件引擎业务逻辑，不延长盲等或放宽断言。

## 环境

仅在 `192.168.3.88` 的 `/home/dominolu/dev/titan-compiler-20261021-39f1267b` 执行格式化、构建、Clippy、测试和 AOT 编译。目录为新建隔离源码目录，不覆盖既有服务器工作区；复用服务器 `.venv` 和 Cargo target 缓存。本地只编辑、审查、计算传输校验和和记录结果。

源码按白名单同步，不复制账户凭据或本地构建产物。既有模板/golden 测试依赖两个预编译 fixture 和空 secrets 目录：空目录由仓库 `.gitkeep` 建立，两个制品从编译服务器此前已验证的目录复制，保持旧 fixture/配置不变。其 SHA256：

- `deploy/pair_arb_v13/artifacts/pair_arb.titan`：`5f532d44ed3b14725e8aa212de9c30ee0e4dbb81378bfcd202d8221cf660af63`
- `crates/titan-cli/tests/fixtures/artifacts/event_counter.titan`：`e0cbe122bea8c0e3a24d932cbadb5b2945205d9b3258482b0684a40be85bdb65`

新增可达依赖 `tempfile` 已存在于原 Cargo.lock，只新增两个 crate 的依赖引用；版本未升级。服务器缺失的 `linux-raw-sys 0.12.1` 原始源码包由既有本地缓存补齐，SHA256 `32a66949e030da00e8c7d4434b251670a91556f4144941d37452769c25d58a53` 与 Cargo.lock 一致；未使用本地编译产物。

## 最终结果

完整命令 `bash scripts/verify_system_refactor.sh` 返回 0。

| 检查 | 结果 |
| --- | --- |
| workspace/all-targets/all-features `cargo check` | 通过 |
| `cargo fmt --all -- --check` | 通过 |
| workspace/all-targets/all-features `cargo clippy -- -D warnings` | 通过 |
| Rust 单元、集成与全部 targets | 453 passed，0 failed，12 ignored |
| Rust doctest | 2 passed，0 failed，1 ignored |
| Python SDK/Numba | 30 passed |
| Python reporting | 4 passed |
| 三份 AOT 制品及原生 artifact/pair/recovery/drain/timer 集成 | 通过 |
| CLI bundle、两个报告 renderer、损坏/不完整 bundle、非实盘 SIGTERM | 通过 |
| 重构前 V13 制品、无 Python 的 Rust 校验 | 通过 |

原始证据：[完整执行](logs/full.log)、[Clippy](logs/clippy.log)、[Rust](logs/rust-test.log)、[SDK](logs/sdk-test.log)、[报告](logs/reporting-test.log)、[CLI 集成](logs/cli-integration.log)、[旧制品兼容](logs/legacy-artifact-validation.log)。新增原生编译/签名、实际 SIGTERM 和 pair/bundle 契约测试均包含在 Rust 日志。

最终 [源码清单](source-files.sha256) 覆盖 260 个同步路径，清单 SHA256：`0e8b6d2f34d67868fe204b03f683892bc3889910558f622563b833bded97c935`。本地取回的源码逐项匹配服务器清单；删除的 `python/titan-strategy-sdk/titan_strategy/cbor.py` 不再存在。新制品摘要见 [native-artifact-checksums.txt](logs/native-artifact-checksums.txt)，工具链见 [compiler-environment.log](logs/compiler-environment.log)。

Rust 1.94.1、Python 3.11.14、NumPy 2.2.6、Numba 0.67.0、llvmlite 0.49.0、GCC 11.4.0、GNU binutils 2.38。12 项真实交易所测试保持 ignored，没有使用真实账户、部署或下单/撤单。本轮尚未提交或推送 GitHub。
