use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use titan_event_engine::*;
use titan_plugin_engine::{
    ActivationGate, DispatchOutcome, EventControl, EventHandler, EventQos, EventView, PluginError,
    PluginIdentity, SubscriptionSpec,
};

struct Counter(Arc<AtomicUsize>);

impl EventHandler for Counter {
    fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn main() {
    let events = std::env::var("TITAN_EVENT_BENCH_EVENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let target_rate = std::env::var("TITAN_EVENT_BENCH_RATE")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    // Benchmark-tuned capacities used by the recorded M1 Pro runs. Set
    // TITAN_EVENT_BENCH_DEFAULT_CONFIG=1 to sweep with EventEngineConfig::default(), or override
    // individual pools/queues below to probe how far a capacity can shrink before drops appear.
    let use_default_config = std::env::var("TITAN_EVENT_BENCH_DEFAULT_CONFIG")
        .ok()
        .is_some_and(|value| value == "1");
    let env_usize = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
    };
    let mut config = EventEngineConfig::default();
    if !use_default_config {
        config.arena.small_event = PoolConfig {
            slots: env_usize("TITAN_EVENT_BENCH_SMALL_SLOTS").unwrap_or(131_072),
            block_bytes: env_usize("TITAN_EVENT_BENCH_SMALL_BLOCK_BYTES").unwrap_or(64),
            low_watermark: env_usize("TITAN_EVENT_BENCH_SMALL_LOW_WATERMARK").unwrap_or(8_192),
        };
        config.arena.market_batch = PoolConfig {
            slots: env_usize("TITAN_EVENT_BENCH_MARKET_SLOTS").unwrap_or(8),
            block_bytes: env_usize("TITAN_EVENT_BENCH_MARKET_BLOCK_BYTES").unwrap_or(128),
            low_watermark: env_usize("TITAN_EVENT_BENCH_MARKET_LOW_WATERMARK").unwrap_or(1),
        };
        config.arena.snapshot = PoolConfig {
            slots: env_usize("TITAN_EVENT_BENCH_SNAPSHOT_SLOTS").unwrap_or(2),
            block_bytes: env_usize("TITAN_EVENT_BENCH_SNAPSHOT_BLOCK_BYTES").unwrap_or(256),
            low_watermark: env_usize("TITAN_EVENT_BENCH_SNAPSHOT_LOW_WATERMARK").unwrap_or(1),
        };
        config.ingress.critical_capacity =
            env_usize("TITAN_EVENT_BENCH_CRITICAL_CAPACITY").unwrap_or(65_536);
        config.subscribers.default_capacity =
            env_usize("TITAN_EVENT_BENCH_SUBSCRIBER_CAPACITY").unwrap_or(65_536);
        config.subscribers.critical_reserve =
            env_usize("TITAN_EVENT_BENCH_SUBSCRIBER_CRITICAL_RESERVE").unwrap_or(4_096);
        config.pending_dispatch.global_capacity =
            env_usize("TITAN_EVENT_BENCH_PENDING_GLOBAL").unwrap_or(8_192);
        config.pending_dispatch.guaranteed_per_critical_subscriber =
            env_usize("TITAN_EVENT_BENCH_PENDING_SUBSCRIBER").unwrap_or(1_024);
        config.dispatch.critical = DrainBudgetConfig::new(1_024, 1_000_000);
        config.dispatch.pending = DrainBudgetConfig::new(256, 250_000);
        config.runtime.spin_iterations = 100_000;
    }
    config.runtime.spin_iterations =
        env_usize("TITAN_EVENT_BENCH_RUNTIME_SPIN").unwrap_or(config.runtime.spin_iterations);
    if let Some(cpu) = std::env::var("TITAN_EVENT_BENCH_CPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.runtime.mode = RuntimeMode::Dedicated;
        config.runtime.cpu_affinity = Some(cpu);
    }
    if let Some(cpu) = std::env::var("TITAN_EVENT_BENCH_SUBSCRIBER_CPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.subscribers.runtime_mode = SubscriberRuntimeMode::Dedicated;
        config.subscribers.cpu_affinity = vec![cpu];
    }
    let profile = (
        config.arena.small_event.slots,
        config.arena.market_batch.slots,
        config.arena.snapshot.slots,
        config.ingress.critical_capacity,
        config.ingress.market_capacity,
        config.subscribers.default_capacity,
        config.subscribers.critical_reserve,
        config.pending_dispatch.global_capacity,
        config.pending_dispatch.per_subscriber_capacity,
    );

    let engine = EventEngine::new(config).expect("benchmark config must be valid");
    let handle = engine.handle();
    handle
        .register_event("benchmark", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(ActivationGate::new());
    let transaction = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    handle
        .stage_subscription(
            transaction,
            &PluginIdentity::new("bench", "counter"),
            &SubscriptionSpec {
                event_type: Arc::from("benchmark"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                capacity: profile.5,
                routing_keys: Arc::from([]),
            },
        )
        .unwrap();
    let (_, mut subscriptions) = handle.commit_at_safe_point(transaction).unwrap();
    let subscription = subscriptions.pop().unwrap();
    let receiver = subscription.receiver;
    let subscriber_gate = gate.clone();
    let counter = Arc::new(Counter(count.clone()));
    let subscriber = std::thread::spawn(move || {
        if subscriber_gate.wait_until_active() != titan_plugin_engine::ActivationState::Active {
            return;
        }
        while subscriber_gate.is_active() {
            match receiver.dispatch_next(counter.as_ref(), Duration::from_micros(10)) {
                Ok(DispatchOutcome::Delivered | DispatchOutcome::Idle) => {}
                Ok(DispatchOutcome::Closed) | Err(_) => break,
            }
        }
    });
    gate.activate();

    let started = Instant::now();
    for sequence in 1..=events as u64 {
        if let Some(rate) = target_rate {
            let target_elapsed_ns = (sequence as u128 - 1)
                .saturating_mul(1_000_000_000)
                .checked_div(rate as u128)
                .unwrap_or(0);
            while started.elapsed().as_nanos() < target_elapsed_ns {
                std::hint::spin_loop();
            }
        }
        let mut request = PublishRequest::new("benchmark", 1, b"event");
        request.source_sequence = sequence;
        loop {
            match handle.try_publish(request) {
                Ok(()) => break,
                Err(PublishError::CriticalIngressFull)
                | Err(PublishError::EventArenaExhausted(_)) => std::hint::spin_loop(),
                Err(error) => panic!("unexpected publish failure: {error}"),
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while count.load(Ordering::Acquire) != events {
        assert!(Instant::now() < deadline, "benchmark delivery timed out");
        thread::yield_now();
    }
    let elapsed = started.elapsed();
    let metrics = engine.metrics().snapshot();
    println!(
        "events={events} target_rate={target_rate:?} elapsed={elapsed:?} throughput={:.0}/s dispatch_p99_bucket_ns={} subscriber_p99_bucket_ns={} drop_total={} resync_total={} arena_exhausted={:?} fast_lane_drop_total={}",
        events as f64 / elapsed.as_secs_f64(),
        metrics.dispatch_latency.p99_ns,
        metrics.subscriber_latency.p99_ns,
        metrics.drop_total,
        metrics.resync_total,
        metrics.arena_exhausted,
        metrics.fast_lane_drop_total,
    );
    println!(
        "config small_event_slots={} market_batch_slots={} snapshot_slots={} critical_capacity={} market_capacity={} subscriber_capacity={} critical_reserve={} pending_global={} pending_subscriber={}",
        profile.0,
        profile.1,
        profile.2,
        profile.3,
        profile.4,
        profile.5,
        profile.6,
        profile.7,
        profile.8,
    );
    gate.quiesce();
    subscriber.join().unwrap();
    engine.stop().unwrap();
}
