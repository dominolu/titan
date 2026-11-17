# Titan CLI（Strategy ABI V13）

日常账户、策略运行与研究操作使用 `titan_cli`，提供 `strategy/account/research/config` 二级命令、交互 help、离线数据库配置和显式本机后台自动启动，见[客户端手册](managed_cli.md)。后台服务使用 `titan serve`。下文保留 `titan` 的 V13 本机工具和兼容入口；策略参数、订阅和 callback 只来自已编译 `.titan` manifest，运行时不导入策略 Python，也不接受 `strategy.json` 或 `module:function`。

单文件源码发现、默认参数、内嵌测试、工具链锁及不可变发布参见[单文件策略工作区](single_file_strategies.md)。经管理器访问的账户、策略、Operation、生命周期与持续观测入口见[管理 CLI](managed_cli.md)。

## 编译与检查 artifact

```bash
titan strategy compile \
  --strategy strategies/pair_arb.py \
  --artifact-format bundle \
  --output pair_arb.titan

titan strategy ls --json
titan strategy show pair_arb.titan --json
titan strategy validate pair_arb.titan --json
```

源码描述、编译和原生回归测试在有界 Python/Numba worker 中执行；运行、产物检查与发布校验不依赖 Python。制品加载只做完整性校验：`native_library_sha256` 比对、`compiled_strategy_digest` 和 `target_triple`。解析阶段和隔离 worker 会使用同一份策略重复校验，防止 RunSpec 生成后替换制品。

## 历史回测

历史回测使用 `hftbacktest/examples/grid_history.rs`，以真实 BBO/成交数据驱动与实盘相同的
`.titan` 产物，撮合、延迟、费用和队列模型由 HftBacktest 提供：

```bash
cargo run -p hftbacktest --example grid_history -- prepare ...
```

CLI 的 `titan run -e backtest`（trace 回放）**已移除**。它重放手工冻结的事件轨迹并比对策略发出的
订单意图序列，没有盈亏、没有撮合，因此无法回答真实历史回测回答不了的问题，却需要为每个场景维护
一份夹具和一套 trace schema。相关配置（`configs/*_backtest.toml`）与轨迹夹具（`fixtures/*_trace.json`）
一并删除；`fixtures/grid_v13_metadata.json` 保留，仅作为 ABI 身份与元数据快照的测试来源。

底层的 `OfflineV13Adapter` 保留：历史回测正是通过它把 staged command 接到自己的执行模型。

当前公开 profile 仅支持 Tick；Bar/Hybrid 会在 controller、SDK 和 Strategy Service 边界被明确拒绝，直到真实 producer、聚合器和契约测试完整。

trace 自 `schema_version = 2` 起必须携带 `metadata` 快照（`schema_version`、`captured_at_ns`、
`instruments`、`digest`）。加载时校验快照自身 digest、覆盖 trace 初始行情的每个 `asset_no`；缺失或
digest 不符直接拒绝，不做隐式补全。快照可用 `titan instruments snapshot-digest <SNAPSHOT.json>
[--update]` 重算/刷新，用 `titan instruments verify --config … --metadata-snapshot …` 与运行配置核对。

## Core Live

```bash
titan validate pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb/runtime.toml

titan run pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb/runtime.toml
```

Live 的位置参数是部署配置中的 `strategy_key`。部署侧只配置 artifact URI/digest、market/account binding、risk scope、recovery 和 runtime policy；参数、订阅与 callback 均以 artifact manifest 为准。

## 标的单位校验

```bash
titan instruments verify --config deploy/pair_arb/runtime.toml --json
titan instruments verify --config deploy/pair_arb/runtime.toml \
  --metadata-snapshot SNAPSHOT.json --json
titan instruments snapshot-digest SNAPSHOT.json --update
```

只读、离线，不启动服务、不连接交易所。它解析运行配置后调用与 `core-run` 相同的校验器，检查：
市场/账户两侧 `price_tick`/`quantity_lot`/`contract_multiplier`/报价币是否一致、`base_delta_per_lot`
是否等于 `quantity_lot × contract_multiplier`、多绑定腿的 maker/hedge 比例是否为精确正整数、
最小 maker 成交能否整数对冲。退出码 0 表示通过，非 0 表示至少一项错误；`--json` 输出稳定的
`errors[].code`（如 `instrument_units_market_account_mismatch`、`instrument_lot_ratio_not_integral`）。

`scripts/cross_exchange_mm_preflight.py` 不再自行计算比例，而是调用本命令并原样报告原因码
（`--titan` 或 `TITAN_BIN` 指定二进制；未提供时仅 shadow 模式给 WARN，受控实盘给 FAIL）。
提供 `--metadata-snapshot` 时，报告额外核对快照 digest、资产覆盖与单位/报价一致性（原因码
`instrument_metadata_missing`/`instrument_metadata_mismatch`/`instrument_metadata_digest_mismatch`），
并在 JSON 中给出 `metadata_verified`/`metadata_digest`；不提供时明确标记“metadata 未校验”。

## 恢复存储维护

恢复点与代次统一保存在节点本地 SQLite，维护命令不启动账户 connector：

```bash
titan recovery --database /srv/titan/recovery/runtime.db inspect
```

同一入口提供 `import-legacy`、`import-generation`、`export`、`backup`、`pin`、`unpin` 和 `prune`，成功输出 JSON。旧文件须显式迁移，备份副本默认隔离，不能直接激活交易。配置与完整操作顺序见[SQLite 恢复存储维护](recovery_storage_operations.md)。

## 任务管理

统一历史与结果维护使用 `titan storage`：`initialize`、`inspect`、`facts`、`export-facts`、`register-bundle`、`register-publication`、`publish-manifest`、`project` 和 `materialize-bundle`。查询使用固定上界游标，登记重试只操作既有字节，不重跑引擎。

**外部后端已下线**：PostgreSQL 历史投影与 S3 对象存储按“聚焦回测与实盘”的决策移除，`prepare-history`/`prepare-objects`/`resume`/`activate`/`prune-history`/`evict-local-objects`/`clean-staging`/`inspect-parquet` 子命令一并删除。历史与结果固定落在本地 SQLite 与文件对象。`local_persistence_operations.md` 中关于这些命令的章节仅作历史参考。

```bash
titan ls --active
titan show <run-id> --json
titan logs <run-id>
titan stop <run-id>
```

`run`、`validate`、`ls`、`show`、`logs`、`stop` 和 `strategy` 子命令支持 `--json`。机器模式的 stdout 只输出 JSON；错误从 stderr 返回稳定的 `error.code` 与 `error.message`。

## Result 与报告

```bash
titan report <run-id>
titan report <run-id> --renderer native --output report.html
```

Rust Runtime 是策略 state、订单、成交与账户事实的唯一权威。Python reporting 只校验并渲染 ResultBundle，不重新计算成交或收益。
