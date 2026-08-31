# Titan EventEngine

`titan-event-engine` implements the bounded live event transport described by
[`docs/event_engine_technical_design.md`](../../docs/event_engine_technical_design.md).

Implemented boundaries:

- three fixed EventArena pools with generation-checked handles and last-reference recycling;
- bounded Critical/Market MPSC ingress and per-subscriber SPSC-compatible channels;
- numeric precompiled routes committed by `EventControl` transactions at EventLoop safe points;
- item/time/fanout scheduling budgets, bounded timers and source-sequence gap detection;
- `critical_reserve`, bounded per-subscriber/global pending dispatch, FIFO retry and
  `RESYNC_REQUIRED` recovery cutoffs;
- preallocated subscriber/runtime health and bounded fault signals;
- `EventReceiver` channels driven by PluginEngine-owned subscriber threads, with callback
  panic/error containment and no Handler ownership inside EventEngine;
- allocation-free logarithmic latency histograms for P50/P99/P99.9/max reporting;
- `TitanCoreRuntime` composition that starts EventEngine before PluginEngine and stops plugins
  before draining EventEngine.

Run correctness and concurrency-model tests:

```bash
cargo test -p titan-event-engine --all-targets
```

Run the standalone throughput benchmark:

```bash
cargo bench -p titan-event-engine --bench event_engine
```

Set `TITAN_EVENT_BENCH_EVENTS` to override the default one million events.
Set `TITAN_EVENT_BENCH_RATE` for a paced load test and `TITAN_EVENT_BENCH_CPU` to run the
EventLoop in dedicated affinity mode.
