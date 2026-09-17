# titan-strategy-sdk（Strategy ABI V13）

该 SDK 是 Titan 唯一的策略编写与 AOT 编译入口。策略使用 typed state、只读 public views
和 callback-local command staging；生产运行时只加载 `native-v13` 制品，不嵌入 Python。

主要模块：

- `abi_v13.py`：固定宽度 ABI dtype、回调槽位、常量与 fingerprint。
- `definition.py` / `parameters.py` / `state.py`：策略声明、参数 schema、typed state schema。
- `context_v13.py`：只读行情/账户视图与 submit/cancel command staging。
- `static_compiler.py`：受限定义加载、静态校验、Numba AOT、deterministic CBOR manifest。
- `cli.py`：离线编译命令，由 Rust `titan strategy compile` 调用。

策略必须导出一个 `STRATEGY = StrategyDefinition(...)`，handler 使用 `@njit`，签名为
`handler(context)`。参数在编译期校验并写入 initial state；部署期不能覆盖参数。

在编译服务器构建：

```bash
titan strategy compile \
  --strategy strategies/pair_arb/strategy.py \
  --parameters strategies/pair_arb/parameters.json \
  --target x86_64-unknown-linux-gnu \
  --cpu-baseline x86-64-v2 \
  --artifact-format bundle \
  --output pair_arb.titan
```

在编译服务器测试：

```bash
PYTHONPATH=python/titan-strategy-sdk:. \
  uv run --project python/titan-strategy-sdk --with pytest \
  pytest -q python/titan-strategy-sdk/tests
```
