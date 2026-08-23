# Strategy ABI v7 migration

ABI v7 makes execution callbacks lossless across Tick, Bar, Hybrid and live modes. It is a
fail-fast ABI change: a v6 NumPy dtype/context is rejected instead of being reinterpreted.

## Added callback fields

`orders` adds `venue_order_id`, `sequence`, `venue_no`, `instrument_id` and `reason`.
`fills` adds the same five fields. Existing field names and semantics are unchanged.

`venue_order_id == 0` means the request was rejected before venue acceptance. `sequence` is the
canonical execution-report sequence, not the callback batch index. `reason` uses these stable
codes:

| Code | Meaning |
| ---: | --- |
| 0 | none |
| 1 | local risk |
| 2 | exchange risk |
| 3 | invalid instrument |
| 4 | invalid price |
| 5 | invalid quantity |
| 6 | duplicate client order ID |
| 7 | position limit |
| 8 | notional limit |
| 9 | insufficient balance |
| 10 | insufficient margin |
| 11 | reduce-only violation |
| 12 | market closed |
| 13 | insufficient liquidity |
| 14 | expired |
| 15 | user canceled |

Unknown connector-specific values use `0x80000000 | connector_code`.

## Required action

Strategies importing `hftbacktest.eventbot` need no source change: the exported aligned dtypes are
already v7. Code that copied the v6 dtype must replace it with `order_event_dtype`/`fill_dtype` from
the package. Recompile cached Numba `cfunc` callbacks after upgrading. Runtime startup validates
the context size, ABI version and Rust dtype sizes before the first callback.
