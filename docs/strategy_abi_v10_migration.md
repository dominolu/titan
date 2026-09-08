# Strategy ABI v10 migration

Strategy ABI v10 adds the live-state views required by multi-venue strategies while preserving
the numeric identifiers of the original v9 callback slots.

## Added typed views

- `DepthBatchEvent` and `DepthItemEvent` preserve market/source identity, snapshot flags,
  `stream_epoch`, update sequence ranges and per-level actions.
- `PositionEvent`, `BalanceEvent`, `CommandResultEvent` and `AccountStateEvent` expose canonical
  account facts without handing opaque connector payloads to Numba.
- `FillEvent` and `OrderEvent` now include `local_account_no`.

The new callback slots are `balance=10`, `command_result=11`, `account_state=12` and `depth=13`.
Existing slots `start=0` through `stop=9` have not changed.

## Runtime behavior

- `EventView` retains canonical publication metadata instead of dropping source identity and
  delivery sequencing before the strategy adapter.
- StrategyPlugin creates upstream market subscriptions after installing the EventEngine route,
  requests and waits for the initial depth snapshot, and owns unsubscribe through ResourceScope.
- Account snapshots seed the strategy before `on_start`. Account control facts continue to update
  the strategy while command admission is closed. Reconcile/stream invalidation closes the command
  gate until every bound account is READY again.
- `runtime.timer_interval` enables a lane-serialized live housekeeping timer.
- `cancel_owned_orders` now keeps the account lane alive and requires terminal order facts before
  completing stop.

All Numba strategy manifests must declare runtime ABI major version 10 and be recompiled because
the descriptor fingerprint and Fill/Order layouts changed.
