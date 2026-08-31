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
    let mut config = EventEngineConfig::default();
    config.arena.small_event = PoolConfig {
        slots: 131_072,
        block_bytes: 64,
        low_watermark: 8_192,
    };
    config.arena.market_batch = PoolConfig {
        slots: 8,
        block_bytes: 128,
        low_watermark: 1,
    };
    config.arena.snapshot = PoolConfig {
        slots: 2,
        block_bytes: 256,
        low_watermark: 1,
    };
    config.ingress.critical_capacity = 65_536;
    config.subscribers.default_capacity = 65_536;
    config.subscribers.critical_reserve = 4_096;
    config.pending_dispatch.global_capacity = 8_192;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 1_024;
    config.dispatch.critical = DrainBudgetConfig::new(1_024, 1_000_000);
    config.dispatch.pending = DrainBudgetConfig::new(256, 250_000);
    config.runtime.spin_iterations = 100_000;
    if let Some(cpu) = std::env::var("TITAN_EVENT_BENCH_CPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.runtime.mode = RuntimeMode::Dedicated;
        config.runtime.cpu_affinity = Some(cpu);
    }

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
                capacity: 65_536,
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
        "events={events} target_rate={target_rate:?} elapsed={elapsed:?} throughput={:.0}/s dispatch_p99_bucket_ns={} subscriber_p99_bucket_ns={}",
        events as f64 / elapsed.as_secs_f64(),
        metrics.dispatch_latency.p99_ns,
        metrics.subscriber_latency.p99_ns,
    );
    gate.quiesce();
    subscriber.join().unwrap();
    engine.stop().unwrap();
}
