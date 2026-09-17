# pair_arb（Strategy ABI V13）

ABI V13 typed-state pair-arbitrage strategy. It implements maker-taker and taker-taker slots,
bounded private order references and terminal history, cumulative-fill validation, hedge catch-up,
cancel/request timeouts, risk posture escalation, position/account safety checks, and restore-time
reconciliation against runtime-owned `active_orders`.

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

The strategy never owns the complete account order book. Runtime public views are authoritative;
private state only preserves references and business relationships required by the state machine.
