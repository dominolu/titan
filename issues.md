# Issues

> 历史记录：下文保存 `729cbdd3` 时的失败现象与当时状态，不代表当前 master 的问题状态。后续 TLS 修复可参考提交 [`0b8fd6f6`](https://github.com/dominolu/titan/commit/0b8fd6f6)，最新主干回归见[分支合并验证](docs/verification/2026-10-31/merge-all-branches/README.md)；历史 CI 观测不作追溯改写。

记录已确认的缺陷、诊断证据和修复验收条件。状态更新以新的验证结果为准。

| ID | 问题 | 优先级 | 状态 | 记录日期 |
| --- | --- | --- | --- | --- |
| ISSUE-001 | 连接器 bootstrap 单测触发 reqwest `No provider set` panic | P2 | 待修复，根因待确认 | 2026-10-31 |

## ISSUE-001：连接器 bootstrap 单测触发 `No provider set`

GitHub 跟踪：[Issue #5](https://github.com/dominolu/titan/issues/5)（Open，`bug`）。

### 已观测现象

- CI 命令：`cargo test -p connector --features okx`。
- 失败用例：`account_runtime::tests::bootstrap_validates_populates_caches_and_publishes_ready_last`，位于 [connector/src/account_runtime.rs](connector/src/account_runtime.rs)。
- panic 位置：`reqwest-0.12.28/src/async_impl/client.rs:767:29`。
- 错误信息：`No provider set`。
- 该任务结果：333 项通过、1 项失败、14 项忽略，进程退出码为 101。
- 观测版本：`729cbdd3bc6160011fb06edd586da18a4e3618df`；首次引入版本尚未定位。

原始证据：[GitHub Actions 失败任务](https://github.com/dominolu/titan/actions/runs/41174319231/job/124458598356)。

```text
---- account_runtime::tests::bootstrap_validates_populates_caches_and_publishes_ready_last stdout ----
panicked at reqwest-0.12.28/src/async_impl/client.rs:767:29:
No provider set

test result: FAILED. 333 passed; 1 failed; 14 ignored
```

### 影响与当前判断

该问题使连接器专项 CI 任务失败。同一提交的整库、全特性、Python 与质量检查任务通过，不能据此将本问题标记为已解决。实际运行时是否受影响尚未确认。

`--features okx` 仍会启用默认 features，因此当前证据不足以将问题归因于 OKX 适配器。待排查方向是 reqwest 客户端构造前的 TLS/crypto provider 初始化，以及测试是否依赖其他用例对进程全局状态的初始化；这些尚未作为根因验证。

### 复现入口

后续复现与测试在约定的编译服务器 `192.168.3.88` 执行，先核对远端源码身份、Rust 1.94.1 和 CI 环境差异。本条记录没有新增独立复现结果。

```bash
cargo test -p connector --features okx

RUST_BACKTRACE=1 cargo test -p connector --features okx \
  account_runtime::tests::bootstrap_validates_populates_caches_and_publishes_ready_last \
  -- --exact --nocapture
```

### 修复验收条件

- [ ] 定位并记录 provider 缺失的实际原因。
- [ ] 原 CI 命令与目标单测独立运行通过，验证并行执行下的初始化行为。
- [ ] 整库默认与全特性测试通过，无相关回归。
- [ ] 更新本条状态并链接对应修复提交和 CI 结果。
