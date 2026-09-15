# titan-strategy-sdk

Numba 策略 SDK：一个 ABI 定义入口 + 一个策略门面。策略包只依赖 `titan_strategy` 的门面，
不接触 ABI 细节（dtype、offset、指针、订单编码）。

## 分层

```text
intrinsic.py   Numba 指针 / 宿主调用 intrinsic（叶子模块）
     ^
abi_v10.py     唯一 ABI 定义：dtype、回调槽位、订单词表、Rust descriptor 布局校验
     ^
callbacks.py   执行宿主调用、请求写入、回测命令编码、callback bridge
     ^
context.py     Strategy facade：行情 / 账户 / 订单视图 + 下单接口
     ^
strategies/*   策略包
```

依赖只能向下。`abi_v10.py` 不 import 上层模块；`callbacks.py` 不定义 dtype；
`context.py` 不含指针运算和订单码表。`tests/test_strategy_surface.py` 用 AST 断言这三条规则，
并用一个只调用门面的 Numba 策略做端到端编码校验。

## 策略写法

```python
from numba import njit
import numpy as np


def build(parameters):
    state = np.zeros(8, dtype=np.float64)
    state_i64 = np.zeros(8, dtype=np.int64)

    @njit
    def on_tick(s):
        for tick in s.ticks():
            price = tick["event"]["px"]
            if price < s.best_bid(0):
                s.submit_maker_order(0, 1, price, 1.0, 1)   # side: 1 buy / -1 sell
        s.cancel_order(1, 0)

    return {
        "strategy_id": "example",
        "strategy_version": "1.0.0",
        "on_tick": on_tick,
        "state": state,
        "state_i64": state_i64,
    }
```

`build(parameters)` 由 `titan_strategy.compiler.compile_strategy` 在冷路径调用；返回对象提供
`state` / `state_i64` 两个一维 C 连续数组，以及一到多个 `@njit` 单参 handler。

完整接口清单与分层约束见
[docs/strategy_abi_v10_migration.md](../../docs/strategy_abi_v10_migration.md)。

## 运行测试

按仓库约定在编译服务器执行（需要带 numpy + numba 的解释器）：

```bash
cd ~/dev/titan && PYTHONPATH=$PWD/python/titan-strategy-sdk \
  ~/miniconda3/envs/hft/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -v
```

Rust 运行时的端到端验证（会用真实 ABI descriptor 编译策略包）：

```bash
cd ~/dev/titan && PYO3_PYTHON=$PWD/.venv/bin/python \
  PYTHONHOME=$HOME/miniconda3/envs/hft LD_LIBRARY_PATH=$HOME/miniconda3/envs/hft/lib \
  cargo test -p titan-cli --test cli_golden
```
