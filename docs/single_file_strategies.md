# 单文件策略工作区

策略的交付单元是 `strategies/<name>.py`，可选同名 Markdown。策略文件声明 SPEC、参数默认值及边界、typed state、原生回调、build(parameters)，并可声明 TESTS 回归场景。账户、行情源、绑定与风险配置属于平台运行实例，不写入策略源码。

## 实施任务

- [x] S1：SDK 内嵌声明式 TESTS 与真实原生回调测试器；缺失视图、回调失败和断言失败均明确失败。
- [x] S2：CLI 位置参数、可选参数文件、类型化 --set、默认 bundle/输出、源码发现和统一 test。
- [x] S3：工作区 titan.toml、SDK/Python 解析与统一生成的 titan.lock；版本变化显式更新锁。
- [x] S4：通用 build、不可变发布目录、自动固定运行配置摘要、逐文件校验、publish 与 verify-release。
- [x] S5：Titan 示例平铺、引用与回归迁移；hft_quant orderflow 单文件及统一环境配置，去掉专属构建发布脚本。
- [x] S6：192.168.3.88 隔离构建、格式/静态检查、Rust/SDK/真实 AOT/发布正负例，保存来源和验证证据。

## 边界

- 本地只编辑与审查，所有构建、格式化和测试均在编译服务器执行。本次不访问实盘账户或启动实盘策略。
- 参数覆盖在编译时应用，部署不能覆盖已编译参数。显式参数文件继续兼容。
- SPEC 是策略业务身份的来源；文件名只是定位入口。新建源码只需要单个文件。
- Python 只负责策略声明、Numba 对象及测试桥接；Rust 负责产物格式、签名、摘要、流水线与发布。
- 内嵌 TESTS 使用有界、JSON 可表达的场景；编译两阶段使用同一份源及参数并比较场景和规范化参数，原生测试在产物发布前执行。
- 运行配置必须显式选择，不能从策略名推断交易所、账户或风险预算。生成配置只修改指定实例的产物引用与摘要，原配置保持可审查。
- 发布包固定源码、最终参数、产物、测试结果、构建身份与可选运行配置。摘要验证、ABI、能力、恢复身份和原生签名策略保持严格；发布不自动启动。
- workspace lock 记录解析后的工具链身份，缺失时生成；不匹配默认拒绝，显式 --update-lock 才可更新。
- 既有已验收发布产物和历史证据不重写。迁移源码后必须重新编译验收，不能声称兼容旧 checkpoint。

## 使用

以下编译和测试命令只在编译服务器执行：

```bash
export TITAN_STRATEGY_PYTHON=/path/to/python3.11
export TITAN_STRATEGY_SDK=/path/to/titan/python/titan-strategy-sdk

titan strategy discover
titan strategy describe strategies/grid.py --json
titan strategy compile strategies/grid.py
titan strategy compile strategies/grid.py --set order_qty_lots=3
titan strategy test strategies/grid.py --json
titan strategy ls --json
```

`compile` 默认使用源码参数、bundle 格式与 `build/strategies/<文件名>.titan`，并在发布产物前执行已声明 TESTS。
`test` 要求至少一组内嵌场景，只输出测试结果，临时编译结果自动清理。未声明场景时 `compile` 明确记录 `not_declared`，不冒充业务测试通过。
`discover` 只列出平铺 `.py`，不导入源码；`describe` 在有界 worker 中验证参数、状态及回调。`ls` 列出工作区生成的产物与既有 deploy 产物。
旧的 `--strategy SOURCE.py`、`--parameters overrides.json`、`--output`、显式 pair 格式及签名参数继续支持。
`--set` 覆盖参数文件中的同名项，SDK 再补全源码默认值并校验。重复覆盖、未知参数、错误类型与非法范围均拒绝。
同一组最终参数使用相同的规范化参数摘要；编译参数摘要口径由原始覆盖输入改为最终参数，这会改变旧产物身份。

## 工作区与工具链

Titan 平台仓库提供 [配置样例](../configs/strategy_workspace.example.toml)，实际配置和锁属于策略项目（如 hft_quant）。平台自身的 debug/release 构建继续使用 rust-toolchain.toml、Cargo.lock 及 SDK 依赖声明；不将自编译控制器固定成自己的策略依赖。

从源码父目录向上查找 `titan.toml`，找不到时再从当前目录查找。可以显式传 `--workspace /path/to/titan.toml`。

```toml
schema_version = 1

[strategy]
source_dir = "strategies"
build_dir = "build/strategies"
cpu_baseline = "x86-64-v2"

[toolchain]
sdk = "../titan/python/titan-strategy-sdk"
python = "/path/to/python3.11"
python_version = "3.11.14"
rust_version = "1.94.1"
```

路径相对工作区解析。Python 的裸程序名走 PATH；环境变量 `TITAN_STRATEGY_PYTHON` / `TITAN_STRATEGY_SDK` 可选择节点已安装环境，但仍需通过版本和锁身份检查。
`rust_version` 核对当前 Titan 可执行文件构建时的 Rust 版本，不在每个策略构建时重编 Titan。
解析并验证编译输入及原生场景后生成 JSON 格式 `titan.lock`，记录 Titan 可执行文件摘要、SDK 源码摘要、Python/Numba/LLVM 身份、Rust 和 ABI 指纹。
提交工作区锁可共享已确认的工具链约束；变更必须审查后使用 `compile` 或 `build --update-lock` 显式接受。
管理端冻结源码到操作目录后，会显式传递原源码工作区配置；省略参数文件的 preset 仍由同一 SPEC 补全默认值。
运行和发布校验已编译产物不需要 Python、SDK 或工作区锁。

已有锁时，启动 SDK worker 前先核对 Titan、SDK 源码、Rust 与 ABI 身份；worker 返回后再核对 Python/Numba/LLVM 版本。未接受的 SDK 改动不会先执行再拒绝。

## 内嵌回归场景

```python
TESTS = (
    {"name": "count_items",
     "steps": [{"callback": "on_tick",
                "views": {"ticks": [{"price_ticks": 100}]}}],
     "expect": {"tick_items": 1, "tick_callbacks": 1}},
)
```

每组场景从编译时的初始状态开始，步骤按顺序调用真实原生回调。步骤可以设置 `now_ns`、`flags`、`state`、ABI `views`、`depth_batch` 或 `recovery_orders`；视图字段使用 SDK ABI 名称。未重设的普通视图保持，事件 payload 在每个步骤独立提供。
`state` 是用于构造故障或恢复边界的状态字段覆盖，不会修改发布产物的初始状态。`status` 声明预期 ABI 返回码，默认 0。
每步和场景末尾都可写 `expect`，路径支持 `book.ready`、`ev.count.4`。直接值表示相等，也可用 `{"gt": 0}`、`{"sum": 30}`、`{"contains": 1000}` 等断言。
特殊路径 `commands.submit_count`、`commands.cancel_count`、`context.flags` 和 `state_bytes` 可核对命令、停止确认及状态预算。
测试命令 sink 只分配确定性的本地 ID，不连接交易所，也不自动生成订单终态或成交事实；需要这些事实的场景须显式提供。
深度测试可复用 `titan_strategy.testing.depth_book()`。SDK 对场景数量、步骤、视图大小和 JSON 总量设边界，进程继续受 Rust 超时和资源限制。
复杂策略的既有业务回归仍保留在平台统一测试目录，内嵌场景补充可独立交付的回归，不替代已有验收。

## 发布

```bash
titan strategy build strategies/grid.py --json
titan strategy build strategies/orderflow_events.py \
  --runtime configs/titan/binance-readonly.toml --strategy-key orderflow_events --json

titan strategy verify-release /path/to/release --json
titan strategy publish /path/to/release --directory /srv/titan/releases --json
```

`build --output` 指定发布根目录；默认 `build/strategies/releases/`。每个发布目录以策略身份和完整 release ID 命名，包含冻结源码、最终参数、bundle、原生测试结果、构建工具链和 Titan 可执行文件。release ID 根据清单内容生成，清单记录逐文件 SHA256；重复发布只接受相同字节。
可选 `--runtime` 必须只含一个实例，`--strategy-key` 显式核对实例。工具生成完整 `runtime.toml`，固定 bundle 路径、摘要及允许目录，复用现有运行配置校验与原生产物信任检查；输入配置不回写。
凭据、节点数据目录和账户事实不进入发布包。目标节点所需路径必须按原运行配置规定准备；校验失败不会形成发布版本。
构建和发布校验将秘密目录、checkpoint 和恢复数据库路径投影到独立临时目录进行结构校验，发布配置保留原始路径，不创建或读取目标节点目录。部署节点的普通 `titan validate` 与启动继续严格检查实际路径；构建通过不表示这些目录已部署。
签名发布仍需显式信任，例如：

```bash
titan strategy verify-release /path/to/release --require-signature --trusted-key production=PUBLIC_KEY_HEX
bash scripts/publish_strategy.sh --release /path/to/release \
  --host ubuntu@node --remote-root /srv/titan/releases --remote-titan /opt/titan/bin/titan \
  --require-signature --trusted-key production=PUBLIC_KEY_HEX
```

通用 SSH 脚本在目标节点暂存、核对传输的 release manifest 身份，然后调用目标节点既有可信 Titan 完成验证和原子发布。不会启动策略或切换当前版本。
release 清单摘要用于身份及完整性；原生策略的真实性仍由调用方要求的 Ed25519 信任策略校验。
