# pair_arb（Strategy ABI V13）

ABI V13 Slot-based pair-arbitrage V3.0.1 strategy. It keeps one current Slot and only minimal private
`order_id/slot_id/role` relationships. Runtime-owned `active_orders` and the checkpointed lifecycle
`OrdersList` are authoritative for order facts. Maker-Taker hedges after the initiator target is
complete; Taker-Taker stages both IOC legs in one callback. Both modes enforce cancel-confirm-replace,
ratio-aware capacity, readiness/staleness gates, and fail-closed recovery.
Checkpoint recovery verifies projected positions and order-role identity. Runtime stop remains in
draining mode until cancel/fill/hedge events remove every active order or the stop deadline fails.

Build on the compilation server:

```text
titan strategy compile \
  --strategy strategies/pair_arb/strategy.py \
  --parameters strategies/pair_arb/parameters.json \
  --target x86_64-unknown-linux-gnu \
  --cpu-baseline x86-64-v2 \
  --artifact-format bundle \
  --output pair_arb.titan
```

The strategy never owns or duplicates the account order book, cumulative fills, or terminal order
history. Those facts belong to the ABI V13 runtime.
