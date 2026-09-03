# Strategy ABI v8 migration

> 历史迁移文档：当前 Rust Runtime 与 Python Host 使用 Strategy ABI v9。新代码请参阅
> [Strategy ABI v9 migration](strategy_abi_v9_migration.md)。本文仅用于解释从 v7 到 v8 的 Funding
> 布局变化。

ABI v8 makes Funding semantics explicit and identical across Tick, Bar, Hybrid and live event
projection. Rust owns settlement ordering and account mutation; Numba still receives one
`on_funding(s)` callback and does not run an event loop.

## Breaking change

`RuntimeFunding`/`funding_dtype` adds the following configuration fields before the four event
timestamps:

- `price_source`: `0` mark, `1` index, or `0x80000000 | external_source_id`;
- `position_snapshot`: `0` before same-time settlement events, `1` after them;
- `formula`: currently `0` (`InstrumentNotional`);
- `rounding_mode`: `0` nearest, `1` toward zero, `2` floor, `3` ceil;
- `boundary`: `0` before same-time market events, `1` after them;
- `rounding_increment`: a finite positive increment.

The configured values are frozen per asset for a run. A later Funding event with a conflicting
configuration fails fast instead of silently changing accounting semantics. `FundingEvent` also
carries its declared price source, and settlement rejects a source mismatch.

In v8, `position_snapshot` and `boundary` must both be `before` or both be `after`. Mixed pairs are
rejected because the engine cannot publish a Funding event at one same-time boundary while using
a position snapshot from the other without retaining an additional authoritative account image.

## Python migration

Import `funding_dtype` from `hftbacktest.eventbot`; do not copy the old v7 NumPy dtype. Existing
callers that leave the new integer fields at zero retain mark-price, before-boundary,
instrument-notional and nearest-rounding behavior. The Python adapter normalizes a zero
`rounding_increment` to `1e-12` for this compatibility path.

ABI v8 发布时 Rust 和 Python 均要求版本 `8`；当前实现已经升级到 v9。旧版本策略仍会在 callback
运行前失败，不存在不安全的结构体尺寸回退。

Order and fill layouts introduced by ABI v7 are unchanged. See
[`strategy_abi_v7_migration.md`](strategy_abi_v7_migration.md) for those fields.
