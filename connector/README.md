# Titan Connector crate

`connector` is the shared Rust connector crate for the Titan live chain. It implements the three
supported venues as dynamic plugin factories:

| Venue | Market | Status |
|---|---|---|
| Binance Futures | USD-M perpetual | ✅ live (mainnet verified) |
| OKX | V5 SWAP | ✅ unified connector ready; real-account acceptance pending credentials |
| Hyperliquid | Perpetual | ✅ unified connector ready; real-account acceptance pending credentials |

There is no standalone `connector` executable and no iceoryx/IPC bridge. Live trading runs through
the `titan` CLI → `TitanCoreRuntime` → PluginEngine/EventEngine chain:

```text
MarketPlugin/AccountPlugin
  -> venue plugin package (cdylib) with ConnectorFactory
  -> concrete venue connector
  -> EventEngine Primary/Async lanes (account) or FastLane mirrors (market)
```

## Usage

Binance Futures mainnet REST→private-stream probes are under
[`connector/examples/`](examples). They use real credentials only through environment variables and
never place fillable orders or leave state behind. See the probe source and
[`docs/refactor_remaining_tasks.md`](../docs/refactor_remaining_tasks.md) for the accepted
field-semantics contract.

## Order safety

Binance Futures, OKX and Hyperliquid refresh an exchange-side scheduled-cancel heartbeat while
credentials are configured. `safety_timeout_ms` defaults to 30 seconds and may be set to zero only
for non-trading/public-data sessions. On SIGINT/SIGTERM the Core runtime waits for exchange
cancel-all responses before exiting; a failed cancellation is logged as an operational incident.
