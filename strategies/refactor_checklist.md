# Titan 精简重构清单

> 历史审查记录：以下权限范围、工作区状态和待办勾选属于审查基线 `69717434` 时的会话，不表示当前执行环境或 master 的实施状态。后续实现以代码及[精简重构验证脚本](../scripts/verify_refactor_simplification.sh)、[最新主干回归记录](../docs/verification/2026-10-31/merge-all-branches/README.md)为准。

日期：2026-10-31。审查基线：`69717434`（Merge single-file strategy workflows and verified releases）。

## 执行状态

已阅读根目录 AGENTS.md 和完整工程记忆，完成 Rust CLI、Python SDK、策略声明的定向审查。未在本地执行编译、格式化、静态检查或测试。编译服务器 `192.168.3.88` 已通过 SSH 连通；已确认原有目录布局，不覆盖既有源码。

当前会话可写范围为 `strategies/`，SDK、Rust、项目 docs 均在范围之外。向 `docs/simplification_refactor_checklist.md` 写入被沙箱拒绝，随后提权请求超时，复查文件未生成。清单暂存本目录。以下实现及验证尚未完成，不将只读审查记为重构完成。

## 已确定的实施项（按执行顺序）

### R1 删除 SDK 策略声明的旧名兼容层

- [x] 分析调用证据。
- [ ] 迁移旧消费者并删除兼容代码。
- [ ] 编译服务器验证。

证据：`python/titan-strategy-sdk/titan_strategy/definition.py:79–106` 手写构造器接收 strategy_id/strategy_version/state_schema_version，又提供三项只读别名；仓库内正式单文件策略均已采用 strategy_type/source_version/recovery_state_schema_version。旧消费者仍存在于 SDK 的 test_v13_sdk.py、test_grid_native.py 及四个 v13 fixture，必须同时迁移。

改法：StrategySpec 恢复标准 frozen dataclass 自动构造器，删除旧关键字分派、重复参数检测和只读别名。原有 __post_init__ 继续执行类型名称、版本、参数唯一性、订阅、能力校验。更新所有仓库内旧调用和作者文档，明确源 API 不再接受旧名。原生 V13 结构中的 state_schema_version 是协议字段，保持原名。

验收：SDK 全量测试、fixture 真实 AOT 编译及原生回调；未知字段仍拒绝；合法策略声明、描述元数据与状态布局一致。该项属于明确的源 API 清理，不宣称对仓库外旧源码无影响。

### R2 删除 HOST_DEPTH_100 重复能力校验

- [x] 分析调用证据。
- [ ] 随 R1 统一至 __post_init__。
- [ ] 编译服务器验证。

证据：definition.py:93–96 在构造器校验 reliable ordered DEPTH，调用 __post_init__ 后在129–132再次校验相同条件。R1 删除手写构造器后，自然只剩归一化后的单一规则。

验收：合法 DEPTH + MARKET_DATA 通过，缺 DEPTH、错误 QoS、缺 MARKET_DATA 均拒绝。复用 test_inline_scenes.py 的深度能力回归。

### R3 合并 Numba 桥接输出的重复分支

- [x] 分析调用证据。
- [ ] 精简 cli.py 输出流程。
- [ ] 编译服务器验证。

证据：`python/titan-strategy-sdk/titan_strategy/cli.py:33–46` 的 test-native 分支单独生成 describe_strategy JSON 后 return，普通路径末尾生成相同内容。

改法：test-native 分支仅执行并保存 native-tests.json；emit-objects 分支仅执行并保存 objects.json；全部 action 走同一个 describe_strategy 与 `<action>.json` 写入点。三个 worker 仍独立执行元数据、参数与场景校验，Rust 仍比对阶段漂移。

验收：三个 action 输出文件名及 JSON 内容不变，原生场景失败仍阻止发布，失败保留旧产物；运行 strategy_compile 与 single_file_strategy CLI 集成测试。

### R4 合并一次 test-native 内的场景校验

- [x] 分析调用证据。
- [ ] 抽出内部已验证场景执行路径。
- [ ] 编译服务器验证。

证据：cli.py:29 已 validate_tests，34调用 run_native_tests 后，testing.py:132 再 validate_tests，重复 json.dumps 与容量检查。

改法：公开 run_native_tests 保持校验不可信输入，内部执行函数接收已验证 cases；CLI 只对同一进程中的同一 cases 验证一次，再写出并执行。不新增 validate=False 等公共绕过开关。

验收：保留1024场景、8MiB、10000步骤及断言字段限制；覆盖非法场景和容量边界，运行 SDK 场景测试及 CLI 原生编译回归。不得去掉 describe/emit/test-native 不同进程各自的校验。

### R5 复用加载阶段的最终参数

- [x] 分析调用证据。
- [ ] 将加载结果携带的规范化参数交给 CLI。
- [ ] 编译服务器验证。

证据：static_compiler.py:179–185 已通过模块 SPEC.validate_parameters 得到 normalized 并传 build；cli.py:25 又从原请求重建最终参数。grid.build 有直接测试调用，不能连同其参数及业务校验一起删除。

改法：内部加载结果同时包含 definition 和最终参数，现有 load_strategy_definition 继续返回 definition；CLI 写 normalized-parameters.json 使用加载阶段同一参数。无模块 SPEC 的兼容路径需要从返回 definition.spec 规范化。保留策略 build 的跨字段业务校验及 Rust 跨阶段比对。

验收：省略默认与显式相同默认得到相同参数摘要；未知字段、越界、必填缺失仍拒绝；build 实际收到的参数与 bridge 输出一致。若实现增加的接口复杂度超过收益，则保留当前两次校验，并在此记录理由，不为减少一次冷路径调用增加平行加载机制。

## 暂缓项及理由

1. CLI 三份小写 hex 与两份墙钟函数：runtime_host.rs:688、management/mod.rs:43、main.rs:884；墙钟在 runtime_host.rs:668 与 management/mod.rs:29。确有重复，但跨 binary/library 暴露新工具 API 的收益有限。先完成上述 SDK 路径，再按最终 diff 决定是否合并，不为抽象而抽象。
2. 耐久 JSON 替换：runtime_host.rs:675 与 management/node.rs:34 都 write/fsync/rename/parent fsync，但临时文件命名和冲突语义不同。暂不合并，更不能删除 fsync、pending 先落盘或错误传播。
3. event_counter 的 bar_items/bar_callbacks：只订阅 BBO，两个字段未被回调使用；删除会改变恢复布局，收益只有16字节，暂缓到正式 source/recovery schema 升级，不单独制造产物兼容变更。
4. 管理 legacy HTTP/记录适配：有实际消费者且维护历史身份，不能按名称直接认定无用。

## 明确保留的职责边界

订单 ACK/未知结果、订单终态/独立成交/持仓三水位、账户快照、恢复身份及证据、ABI/schema/摘要/签名、私有流故障、执行准入、持久化提交、受控停止期限和资源回收都有实际职责。只消除同一路径的重复规则，不通过放宽这些约束减少代码。报告功能按项目约定保留。

## 远端验证与收尾

- [x] SSH 连通指定编译服务器。
- [x] 确认原有验证目录：最近一次 `/home/dominolu/dev/titan-single-file-review-20261031/` 下源码在 src，日志在 logs，不是仓库根。
- [ ] 创建本轮隔离源码目录，固化基线、输入文件列表与 SHA256。只同步明确源码和 fixture，不同步凭据、.titan 或本地构建产物。
- [ ] Rust 变动若落地：fmt/check/Clippy -D warnings 及相关 CLI 回归；未变动的 Rust 仍运行受 SDK 影响的编译/发布集成测试。
- [ ] SDK 全量测试、内嵌场景与真实 AOT 编译。所有命令仅在192.168.3.88执行。
- [ ] 保存工具链、日志、退出码、前后源码身份，记录新增失败与环境问题。
- [ ] 将清单归入 docs，按实际结果更新工程记忆，保留用户原有未提交修改；最终审查 diff。

本轮不访问真实账户、不部署、不运行长期策略。现有跨所做市源码、测试、研究文档和工程记忆的未提交修改不纳入本轮编辑范围。
