# Titan ResultBundle schema v1

`titan run-worker` owns Bundle creation. A completed run directory contains:

- `result.json`: immutable Runtime facts, canonical execution/funding reports, authoritative final
  exchange/local account snapshots, optional canonical return observations, and final strategy
  state.
- `manifest.json`: committed last; contains schema version, run/strategy identity, ABI
  fingerprint, file sizes, and SHA-256 digests.

Readers must treat a directory without `manifest.json` as incomplete and must verify every digest
before rendering. Timestamps are signed nanoseconds; callback counts are indexed by the stable ABI
event ID. Arrays retain Runtime order. Python renderers may format these facts but may not recompute
execution, accounts, positions, fees, funding, PnL, or equity.

`execution_reports` is the canonical order/fill lifecycle table. `exchange_final` and
`local_delivered_final` contain the two explicit visibility boundaries; account fields include
balance, position, fees, funding, realized/unrealized PnL, and margin. `returns` is always present:
it is empty when the selected backend did not record a canonical equity series, and renderers must
not manufacture one from fills or final balances. In that case the QuantStats renderer emits a
verified no-data page instead of inventing statistics or failing the report command.

Schema evolution is additive within v1. A breaking field, ordering, unit, or valuation change
requires a new integer schema version and an explicit migration reader.
