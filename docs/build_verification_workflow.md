# 构建服务器验证入口（自动依赖快照）

本页只说明**怎么跑**。执行环境约定（本地不编译、固定用 `titan-build`、实盘另走远程节点）见
[AGENTS.md](../AGENTS.md)；验证记录格式见[隐私与验证记录](repository_privacy.md)。

## 1. 一条命令

在构建服务器上：

```bash
scripts/verify.sh cargo test --workspace --all-targets --all-features
```

在开发机上，加上 `--sync` 让它先把当前工作树推上去：

```bash
TITAN_BUILD_HOST=<私有配置中的 ssh 目标> scripts/verify.sh --sync cargo test --workspace --all-targets --all-features
```

就这一条。它做四件事，且都不需要你记参数：

1. **把当前工作树同步到服务器上唯一的构建目录**（`TITAN_BUILD_DIR`，默认 `~/dev/titan`），
   包含未提交的修改。同步是精确的：工作树里没有的源文件会在服务器上被删除，只有 `target/`
   和 `.git/` 保留。**不存在“每个版本一个目录”**；版本身份记录在服务器上的 `.sync-manifest`
   和 `git status` 里。同步失败则整轮停止，不会拿服务器上的旧副本冒充本次验证。
2. **把 `CARGO_TARGET_DIR` 钉到仓库 `target/`**。SDK 测试与多个 `verify_*.sh` 按
   `<repo>/target/debug/titan` 找二进制；指到别的目录会**静默地再编一整套**（实测约 28 分钟、
   额外 28G 磁盘）。
3. **恢复依赖快照**（命中时约 1 秒）。约 340 个第三方 crate 只要 `Cargo.lock`、工具链版本和
   profile 没变就一次都不重编。快照键取的是 `rust-toolchain.toml` 里**实际选中的工具链**
   （`rustup run <channel> rustc --version`），不是 rustup proxy 自身的版本号。
4. **跑完自动保存/刷新快照**，并只保留最近 2 份（一次工作集 + 一次回滚）。

### 1.1 之前的做法为什么不可靠

- 每轮新建一个 `~/dev/titan-<日期>-<sha>` 目录，用完不删，累积到 42 个、约 300G。
- 长期使用的 `~/dev/titan` 没有 `.git`，靠人工 rsync 覆盖，于是新旧文件混在一起：一个 9 月的
  `v13.rs` 和 11 月的 `lib.rs` 并存，该目录因此永远编不过任何全工作区检查。
- 快照键用 proxy 的 `rustc --version`（恒为 rustup 自身版本），换工具链后键不变，于是每轮都
  `snapshot: missing`，白白重编整套依赖，也从未写入过快照。

## 2. 实测（titan-build，24 核，当时负载 10–70）

| 场景 | 耗时 | 重编的 crate |
| --- | --- | --- |
| 全新检出、快照命中，全量测试 | **~3.5 分钟** | 11 个自研 crate + 0 个依赖 |
| 无改动，全量测试 | **~2.3 分钟** | 0 |
| 改一个 crate 后全量测试 | **~3.5 分钟** | 11 个自研 crate |
| 快照不存在（换了 lockfile / rustc），首次全量 | ~20–25 分钟 | 全部 |
| 恢复快照本身 | **~1 秒** | — |

关键点：**改你的代码，第三方依赖永远不再重编**；只有换 `Cargo.lock` 或升 `rustc` 才会付一次全量。

## 3. 常用变体

```bash
scripts/verify.sh cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/verify.sh cargo build --workspace --all-targets
scripts/verify.sh --no-cache cargo test -p titan-cli    # 临时绕过快照（target 仍会被钉住）
scripts/dep_cache.sh status                              # 当前键 / 快照是否存在 / 体积
scripts/dep_cache.sh save                                # 手动建快照（一般不用，verify.sh 会自动存）
scripts/dep_cache.sh prune 2                             # 手动清理
```

`verify.sh` 的退出码就是被包装命令的退出码；快照恢复失败会**回退成正常编译**而不是让任务失败。

## 4. 环境变量

| 变量 | 作用 | 默认 |
| --- | --- | --- |
| `TITAN_DEP_CACHE` | 快照根目录 | `$TITAN_HOME/dep-cache`，否则 `~/.cache/titan-deps` |
| `TITAN_BUILD_PROFILE` | `debug` / `release` | `debug` |
| `TITAN_BUILD_PYTHON` | 能 `import numba, llvmlite` 的解释器 | 构建服务器已在 shell 配置里按能力自动解析；其它机器回退 `python3` |
| `TITAN_NO_DEP_CACHE=1` | 关闭快照（仍钉 target） | 未设置 |
| `TITAN_ALLOW_PYTHON_WITHOUT_NUMBA=1` | 跳过解释器预检 | 未设置 |

**`TITAN_BUILD_PYTHON` 在构建服务器上已自动解析**：shell 配置块 `TITAN_VERIFY_PYTHON` 会按顺序
探测候选解释器，取第一个能 `import numba, llvmlite` 的，所以普通 `ssh` 会话里就已可用。
`verify.sh` 在**开跑前**做这项检查，不通过直接 `exit 3`——避免在付完整轮编译之后才失败（早期
正是这样白跑过 28 分钟）。其他机器需要自行设置，或设 `TITAN_ALLOW_PYTHON_WITHOUT_NUMBA=1`
跳过该检查（此时编译策略的用例会失败）。

## 5. 已有 `verify_*.sh` 脚本

`verify_data_persistence.sh`、`verify_refactor_simplification.sh`、`verify_management_console.sh`、
`verify_strategy_observation.sh` 已改为 `source scripts/build_env.sh`，不再各自指定私有 target 目录。
它们仍按原方式直接调用（带各自的证据目录参数），共享同一份 `target/` 与依赖快照。

## 6. 实现与边界

- [`scripts/build_env.sh`](build_env.sh)：钉 target、导出 `TITAN_TEST_BINARY`/`TITAN_STRATEGY_SDK`/
  `PYTHONPATH`、检测解释器；传入其它 `CARGO_TARGET_DIR` 会**直接拒绝**（exit 2）。
- [`scripts/dep_cache.sh`](dep_cache.sh)：快照键 = `rustc 版本 + Cargo.lock 哈希 + profile`；
  `cp -al` 硬链接，几乎不占额外磁盘；**保存前剔除自研 crate 的产物**，保证恢复后你的改动一定被重编。
- 快照只覆盖依赖产物；`target/` 里的自研产物不共享，避免误用旧二进制。
