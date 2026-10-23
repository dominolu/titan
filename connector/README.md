# Titan Connector crate

`connector` is the shared Rust connector crate for the Titan live chain. It implements the
supported venues as statically linked factories:

| Venue | Market | Status |
|---|---|---|
| Binance Futures | USD-M perpetual | ✅ live (mainnet verified) |
| OKX | V5 SWAP | ✅ live (mainnet verified) |
| Hyperliquid | Perpetual | ✅ live (mainnet verified) |
| Gate CrossEx | FUTURE / SPOT / MARGIN + native extensions | Gate BTC passive-order lifecycle verified; [live scope](../docs/verification/2026-10-21/gate-crossex-live/README.md) |

There is no standalone `connector` executable and no iceoryx/IPC bridge. Live trading runs through
the `titan` CLI → `TradingRuntime` → EventEngine chain:

```text
TradingRuntime static ConnectorCatalog
  -> MarketService/AccountService
  -> concrete venue connector and REST API
  -> EventEngine Primary/Async lanes (account) or FastLane mirrors (market)
```

## Usage

Gate CrossEx uses the `gate-crossex` market factory and `gate-crossex-account` account factory.
See [capabilities and configuration](../docs/gate_crossex_broker.md) and
[`examples/gatecrossex.toml`](examples/gatecrossex.toml). It supports CROSS_EXCHANGE and explicitly scoped ISOLATED_EXCHANGE accounts, and preserves native symbols such as `OKX_FUTURE_BTC_USDT`. Use base quantities with a
contract multiplier of one. CrossEx has no scheduled-cancel endpoint; use LeaveOpen or CancelAll
as the explicit shutdown policy.

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
