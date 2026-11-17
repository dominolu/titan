# cross_exchange_mm deployment template (disabled, shadow only)

This directory is a **template**. It is not a live deployment and must not be enabled
until the platform multi-binding controlled-execution work is complete
(see `docs/cross_exchange_mm_live_worklist.md`, items B1-B9) and the owner of the
live environment explicitly authorizes a controlled two-account run.

Why it cannot run live today:

- The controlled live path (`execution_limits`) still rejects two markets
  (`execution_binding_unproven` in `crates/titan-strategy-runtime/src/runtime_v13.rs`).
- Without `execution_limits` the instance would run with **no platform exposure
  envelope**, which the strategy's own documentation forbids.
- `FlattenOwned` and per-venue margin/rescue reservation do not exist yet, so a
  risk stop may retain a matched cross-venue position.

## Layout

```
deploy/cross_exchange_mm/
  runtime.toml            # generated from configs/cross_exchange_mm.example.toml, private values filled
  artifacts/strategy.titan# frozen release artifact from `titan strategy build`
  secrets/                # 0600 credential files, never committed
  checkpoints/            # recovery storage
  logs/                   # systemd stdout/stderr
```

`runtime.toml`, `artifacts/`, `secrets/`, `checkpoints/` and `logs/` are git-ignored.

## Procedure (after B1-B9 and explicit authorization)

1. On the build server, in an isolated directory of the frozen commit:
   `cargo build --release -p titan-cli` and `titan strategy build strategies/cross_exchange_mm.py
   --runtime <runtime.toml> --strategy-key cross_exchange_mm --json`, then
   `titan strategy verify-release <release> --json`.
2. Copy `configs/cross_exchange_mm.example.toml` to `deploy/cross_exchange_mm/runtime.toml`,
   fill account/market keys and the artifact digest, and keep `enabled = false`.
3. Run `python3 scripts/cross_exchange_mm_preflight.py deploy/cross_exchange_mm/runtime.toml
   --check-secrets` on the target host. Any FAIL stops the rollout.
4. Install both venue credential files under `secrets/` with mode 0600 (names only here,
   values never in Git, commands, logs or tickets).
5. Run the read-only live checks from the worklist (C4) and record the evidence privately.
6. Only then enable market sources, accounts and the strategy in one controlled step,
   with the stop budget and maximum exposure agreed in advance.

## systemd

`titan-cross-exchange-mm.service` is a user unit mirroring the pair_arb template. Its
`TimeoutStopSec` must exceed the two-leg stop budget; today the managed path caps Stop at
10 seconds, so the unit is only meaningful for a core-run shadow instance.
