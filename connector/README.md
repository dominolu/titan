# Titan Connector crate

`connector` is the shared Rust connector crate for the Titan live chain. It implements the three
supported venues as statically linked factories:

| Venue | Market | Status |
|---|---|---|
| Binance Futures | USD-M perpetual | ✅ live (mainnet verified) |
| OKX | V5 SWAP | ✅ live (mainnet verified) |
| Hyperliquid | Perpetual | ✅ live (mainnet verified) |

There is no standalone `connector` executable and no iceoryx/IPC bridge. Live trading runs through
the `titan` CLI → `TradingRuntime` → EventEngine chain:

```text
TradingRuntime static ConnectorCatalog
  -> MarketService/AccountService
  -> concrete venue connector and REST API
  -> EventEngine Primary/Async lanes (account) or FastLane mirrors (market)
```

## Usage

Mainnet acceptance remains recorded as historical evidence; ordinary workspace tests never use
real credentials or submit live orders. Hyperliquid has completed public
REST/WS, private reconnect, submit/amend/cancel, full-fill and partial-fill acceptance; see the
[`Hyperliquid mainnet acceptance report`](../docs/validation/hyperliquid_2026-09-07/README.md).

## Order safety

Binance Futures, OKX and Hyperliquid refresh an exchange-side scheduled-cancel heartbeat while
credentials are configured. `safety_timeout_ms` defaults to 30 seconds and may be set to zero only
for non-trading/public-data sessions. On SIGINT/SIGTERM, `TradingRuntime` closes execution
admission, waits to its configured deadline, then applies the account connector's explicit
shutdown order policy.
