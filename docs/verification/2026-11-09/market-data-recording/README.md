# Market data recording: REST validation, quality gate, and stress acceptance

Status: **server-side collector validation passed; live exchange sampling not performed**.

This summary covers the market-data recording changes based on design and implementation plan rev.4. The sources were tested in an isolated staging workspace on `titan-build`; the server checkout was not changed. The source baseline was `80189496` plus the worktree changes present for this verification. No raw command logs, host details, or exchange response dumps are included here.

## Implemented acceptance paths

- Binance spot and futures REST depth snapshots are aligned to the live update sequence, replayed through a bounded delta window, and compared with the current local book. REST failures and unalignable samples are persisted as stale validation events.
- Bybit spot/linear/inverse REST orderbook snapshots use the response update ID and the same shadow-delta validator. The response adapter has fixture coverage. Bybit documents the REST `u` update ID as corresponding to the websocket orderbook update ID ([Bybit V5 orderbook](https://bybit-exchange.github.io/docs/v5/market/orderbook)).
- OKX and Hyperliquid are explicitly reported as not supporting this sequence-aligned REST shadow check. Low sample counts, stale samples, mismatches, and unsupported venues have distinct quality warnings or failures.
- Replay requires a valid per-day quality report and a per-market entry. `FAIL`, missing reports, malformed reports, and absent markets are rejected; `PASS_WITH_WARNINGS` remains eligible under the design rule that WARN is not bad data. `--diagnostic` remains available for inspection without the quality gate.
- Quality now reports REST samples/mismatches/stale samples, precision-reject gap counts and ratios, exact late-event counts (including no-change slots) and ratios, latency and changes percentiles, OI maximum interval and funding last-sample age, observed auxiliary sources, event counts, and integrity/summary status. Configured `fsync_interval_blocks` is passed through the recorder to the segment and index writers.
- Abrupt process exit, incomplete tail, corrupted block, and subsequent-block recovery cases are covered by tests.

## Build-server verification

Commands ran on `titan-build` against an isolated source staging directory:

- `cargo fmt --manifest-path collector/Cargo.toml`
- `cargo test --manifest-path collector/Cargo.toml -p collector --all-targets --offline`
- `cargo clippy --manifest-path collector/Cargo.toml -p collector --all-targets --no-deps --offline -- -D warnings`
- Release performance/capacity probe: `cargo test --release --manifest-path collector/Cargo.toml -p collector --test book benchmark_segment_compression_and_sequential_read --offline -- --ignored --nocapture`

Results:

- Collector all-target tests: **47 passed, 0 failed, 1 ignored**. The ignored test is the separate acceptance probe listed below.
- Collector Clippy with dependency lints excluded: **passed with `-D warnings`**. Running Clippy over workspace dependencies also reports two `type_complexity` warnings in the separately modified account-service package; those warnings are outside this collector change.
- Abrupt-exit recovery: child process exited before destructors/sealing; recovery found the synced block and no truncated bytes.
- Release replay probe: **300,000 slots, 500 blocks, Top-100 snapshot, five depth changes per slot**. CRC/read, ZSTD decompression, payload decode, and ladder application measured **7.11M records/s**, above the design threshold of 1M records/s. Measured read bandwidth was 60.3 MB/s for the compressed fixture.
- Compressed book data was 2,544,168 bytes; trade gzip data was 2,568,867 bytes. The fixture projects to **14.7 MB per market-day**, or **0.339 GB/day for 23 markets**, at one trade and five book changes per 100 ms slot.

The capacity figure is a synthetic extrapolation, not a production guarantee. It excludes OI/funding/liquidation event volume, filesystem/index overhead beyond the measured segment, variable market activity, and venue-specific event rates. No 23-live-market soak or real exchange REST sampling was run. REST endpoint adapters are fixture-tested; end-to-end exchange behavior remains subject to a separately authorized non-account market-data run.
