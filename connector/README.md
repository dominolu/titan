# Titan Connector crate

`connector` is the shared Rust connector crate for the Titan live chain. It implements the three
supported venues as dynamic plugin factories:

| Venue | Market | Status |
|---|---|---|
| Binance Futures | USD-M perpetual | ✅ live (mainnet verified) |
| OKX | V5 SWAP | ✅ live (mainnet verified) |
| Hyperliquid | Perpetual | ✅ live (mainnet verified) |

There is no standalone `connector` executable and no iceoryx/IPC bridge. Live trading runs through
the `titan` CLI → `TitanCoreRuntime` → PluginEngine/EventEngine chain:

```text
MarketPlugin/AccountPlugin
  -> venue plugin package (cdylib) with ConnectorFactory
  -> concrete venue connector
  -> EventEngine Primary/Async lanes (account) or FastLane mirrors (market)
```

## Usage

Mainnet REST→private-stream probes are implemented as ignored live tests or examples and use real
credentials only through environment variables. Every order probe has bounded notional exposure,
explicit remainder cancellation and final REST reconciliation. Hyperliquid has completed public
REST/WS, private reconnect, submit/amend/cancel, full-fill and partial-fill acceptance; see the
[`Hyperliquid mainnet acceptance report`](../docs/validation/hyperliquid_2026-09-07/README.md).

## Order safety

Binance Futures, OKX and Hyperliquid refresh an exchange-side scheduled-cancel heartbeat while
credentials are configured. `safety_timeout_ms` defaults to 30 seconds and may be set to zero only
for non-trading/public-data sessions. On SIGINT/SIGTERM the Core runtime waits for exchange
cancel-all responses before exiting; a failed cancellation is logged as an operational incident.
