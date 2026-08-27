# HftBacktest - Connector
Connector provides a single point of communication with exchanges, brokers, or data feed providers. 
It is designed to manage multiple bots, allowing each bot to connect to several different connectors simultaneously.

![architecture](https://github.com/nkaz001/hftbacktest/blob/master/docs/images/arch.png)

## Supported Exchanges
**CAUTION: Use at your own risk. Live trading features may not function correctly in all cases.
Please report any issues you encounter by submitting them to the Issues.**

Supported exchanges include:

* Binance Futures (Tested on the Testnet)
  - The symbol should be in lowercase.
* Bybit Futures (Under development)
  - The symbol should be in uppercase.
* OKX V5 SWAP
  - Enable the `okx` feature; live and simulated trading are supported.
* Hyperliquid Perpetual
  - Enable the `hyperliquid` feature; API-wallet signing is supported.

## Getting Started

1. Clone the repository:

    ```
    git clone https://github.com/nkaz001/hftbacktest.git
    ```

2. Build Connector. After building, the executable file `connector` will be generated under `target/release` directory:

    ```
    cargo build --release --package connector
    ```

3. Configure the settings file. Please see the [examples](https://github.com/nkaz001/hftbacktest/blob/master/connector/examples) directory for guidance.

4. Run Connector. You can run multiple instances of Connector for the same exchange using different names and configurations:

    **Example**
    ```
    connector --name bf --connector binancefutures --config binancefutures.toml
    ```

Note: Since Connector communicates with bots via shared memory, both Connector and the bots must run on the same machine.

## Binance WS to `on_tick` latency probe

Run the probe on the target server (not on a separate control machine), because it measures the
same-host path `WS text frame received -> connector decode/fusion -> iceoryx IPC -> Numba
on_tick(s)`. Rust owns the live event loop and calls the single-argument `@njit` callback described
in `docs/bar_tick_numba_strategy.md`; Python is used only for cold-path setup and result reporting:

```console
connector/scripts/run_binance_ws_to_on_tick_latency.sh
```

The script uses Binance USD-M **mainnet public market data only**. It supplies no API key and the
probe does not submit orders. Defaults are `btcusdt`, a 1 ms strategy frame, 5 seconds of warm-up,
and 60 seconds of measurement. Override them with environment variables:

```console
SYMBOL=ethusdt TICK_SIZE=0.01 LOT_SIZE=0.001 \
FRAME_US=1000 WARMUP_SECONDS=10 MEASURE_SECONDS=300 \
connector/scripts/run_binance_ws_to_on_tick_latency.sh
```

The final `RESULT_JSON=...` line reports `min`, `p50`, `p90`, `p99`, `p99.9`, `max`, mean and
standard deviation in microseconds. A Binance depth/BBO push can fan out into multiple TickBatch
items; items from one WS frame share one ingress timestamp and are sampled once inside
`@njit def on_tick(s)`. The preallocated `state_i64` sample buffer keeps the callback in Numba
`nopython` mode without Python lists, dictionaries or `objmode`. Raw probe and connector logs are
written under `latency_results/` unless `RESULT_DIR` is set.

## Order safety

Binance Futures, OKX and Hyperliquid refresh an exchange-side scheduled-cancel heartbeat while
credentials are configured. `safety_timeout_ms` defaults to 30 seconds and may be set to zero only
for non-trading/public-data sessions. On SIGINT or SIGTERM every connector waits for exchange
cancel-all responses before exiting. A failed shutdown cancellation is logged as an error and must
be treated as an operational incident.

Live order modification is deliberately disabled in `LiveBot`: cancel the old order, wait for its
response, and submit a replacement.

## Connector Implementation Guide
If a connector adheres to the IPC protocol, it does not have to be implemented in the same manner as Connector.
However, following this implementation makes it easier to develop additional connectors.

To implement a connector, you mainly need to implement two traits: `Connector` and `ConnectorBuilder`.

For further details, please see the documentation.
