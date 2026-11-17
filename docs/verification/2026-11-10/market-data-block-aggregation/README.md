# Market data block aggregation: 60 s blocks with one window snapshot

Status: **implemented and verified on the build server; live public-market pilot passed**.
This summary covers the recorder change that makes the writer match the block
aggregation described in [the v1 format design](../../market_data_recording_design.md)
§12.5. No raw logs, host details, or exchange response dumps are included.

## Change

The recorder previously wrote **one self-contained data block per 100 ms grid
slot**, so every block repeated the full Top-N window snapshot. With
`grid_ms=100`, `depth_levels=100`, `block_duration_ms=60000` this produced one
block every 100 ms instead of one block per 60 s window: about 570× more blocks
than the design, each carrying a full 100-level snapshot, and `block_duration_ms`
was only validated/stored, never used for packing.

The writer now packs closed slots into `block_duration_ms` blocks:

- the first slot of a block contributes the full window snapshot, later slots
  contribute only their net window diff record;
- a block is sealed when the next slot leaves the UTC grid window
  (`block_duration_ms / grid_ms` slots), when a tick would exceed the design
  default rebase threshold (30 000 ticks; fresh snapshot and base tick start the
  next block), at a feed gap (pending block is appended before the new-epoch gap
  control block), and at feed end;
- empty grid slots produce no record; late-only slots keep an empty diff record
  so the sparse sequence chain stays monotonic.

The on-disk format is unchanged: v1 blocks already support multi-record payloads
and multi-record blocks are decoded by `replay`, `verify`, and the quality
scanner without modification.

## Volume effect (same Top-100 public data)

| Capture | Blocks | Book bytes | Notes |
| --- | ---: | ---: | --- |
| Before, one block per slot (8 h sample) | 556,117 | 630 MB | 1,069 B/block average, 96.5% of all data |
| After, 60 s blocks (600 s pilot) | 21 | 2.64 MB | n_slots 600/597/600...; 36–178 KB per block |

The 600 s pilot ran in a high-activity window (mean 93 net changes per changed
slot versus 32 in the 8 h sample). The same-activity counterfactual for the
old writer is about 11,400 blocks and 12.9 MB of book data, i.e. ~540× fewer
blocks and ~4.9× smaller book volume; at the 8 h sample's mean change rate the
reduction is ~7–10× (roughly 0.13–0.25 GB/day for two markets instead of
1.86 GB/day). Remaining volume is real net window change: 20–150 changes per
slot in the observed samples.

## Build-server verification

Commands ran on `titan-build` against an isolated source staging directory:

- `cargo fmt --manifest-path collector/Cargo.toml`
- `cargo test --manifest-path collector/Cargo.toml -p collector --all-targets --offline`
- `cargo clippy --manifest-path collector/Cargo.toml -p collector --all-targets --no-deps --offline -- -D warnings`
- release probe: `cargo test --release --manifest-path collector/Cargo.toml -p collector --test book benchmark_segment_compression_and_sequential_read --offline -- --ignored --nocapture`

Results:

- Collector all-target tests: **55 passed, 0 failed, 1 ignored** (the release
  probe). The new tests cover multi-slot packing, full-window sealing, slot
  window crossing, rebase splitting, late-only records, and an end-to-end
  `run_market` replay with three changed slots inside one block.
- Clippy with `-D warnings` (dependency lints excluded): **passed**.
- Release replay probe: 500 blocks / 300,000 records, **7.02M records/s**
  (threshold 1M), 59.5 MB/s read, 8.48 B/slot, compression ratio 0.198,
  fixture projection 14.7 MB per market-day.

## Live public-market pilot

600 s, Binance BTCUSDT spot + USDⓈ-M perpetual, public streams only,
`grid_ms=100`, `depth_levels=100`, `block_duration_ms=60000`:

- 2 segments, 21 data blocks (n_slots: 600 ×18, 597, 351, 245; the 351-slot spot
  block is a tick-rebase split), block sizes 36–178 KB.
- Integrity: 0 corrupt, 0 truncated, 0 invalid event records,
  0 summary mismatches.
- Quality: overall `PASS_WITH_WARNINGS`; spot `PASS`;
  perpetual `PASS_WITH_WARNINGS` with only the known
  `rest_shadow_stale_sample` warning. REST shadow samples 9 per market,
  0 mismatches; 0 gaps.
- Replay: quality-gated `gap-policy=error` exits 0; diagnostic replay reads
  21 blocks / 11,872 records / 1,100,207 changes with 0 gaps and 0 corrupt
  blocks.
- Trade and auxiliary streams are unaffected.

A 24 h capture with this build was launched on the live server after the pilot;
its result is tracked separately as a private run record.
