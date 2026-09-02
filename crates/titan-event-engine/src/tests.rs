use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, bounded};
use titan_plugin_engine::{
    ActivationGate, DispatchOutcome, ErrorKind, EventControl, EventHandler, EventPublishMetadata,
    EventQos, EventReceiver, EventView, LifecycleState, PluginError, PluginIdentity,
    SubscriptionSpec, TraceContext,
};

use crate::*;

fn test_config() -> EventEngineConfig {
    let mut config = EventEngineConfig::default();
    config.arena.small_event = PoolConfig {
        slots: 64,
        block_bytes: 64,
        low_watermark: 4,
    };
    config.arena.market_batch = PoolConfig {
        slots: 64,
        block_bytes: 128,
        low_watermark: 4,
    };
    config.arena.snapshot = PoolConfig {
        slots: 8,
        block_bytes: 256,
        low_watermark: 1,
    };
    config.ingress = IngressConfig {
        critical_capacity: 64,
        market_capacity: 64,
        max_sources: 16,
    };
    config.subscribers.max_count = 16;
    config.subscribers.default_capacity = 16;
    config.subscribers.critical_reserve = 2;
    config.subscribers.spin_iterations = 0;
    config.subscribers.idle_sleep_us = 50;
    config.pending_dispatch.per_subscriber_capacity = 4;
    config.pending_dispatch.global_capacity = 32;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 1;
    config.pending_dispatch.max_age_ms = 1_000;
    config.dispatch.critical = DrainBudgetConfig::new(64, 5_000_000);
    config.dispatch.pending = DrainBudgetConfig::new(64, 5_000_000);
    config.dispatch.market = DrainBudgetConfig::new(64, 5_000_000);
    config.dispatch.timer = DrainBudgetConfig::new(64, 5_000_000);
    config.dispatch.max_fanout_per_step = 4;
    config.dispatch.timer_capacity = 16;
    config.runtime.spin_iterations = 10;
    config.runtime.sleep_us = 50;
    config
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

struct RecordingHandler(Sender<Vec<u8>>);

impl EventHandler for RecordingHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.0
            .send(event.payload.to_vec())
            .expect("test receiver remains alive");
        Ok(())
    }
}

struct BlockingHandler {
    entered: Sender<Vec<u8>>,
    release: Receiver<()>,
}

impl EventHandler for BlockingHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.entered
            .send(event.payload.to_vec())
            .expect("test receiver remains alive");
        self.release
            .recv()
            .expect("test controls handler completion");
        Ok(())
    }
}

#[test]
fn fast_lane_runs_inline_and_keeps_the_normal_route() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("fast", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();

    let (fast_tx, fast_rx) = bounded(1);
    let token = handle
        .register_fast_lane("fast", 1, vec![7], Arc::new(RecordingHandler(fast_tx)))
        .unwrap();
    let transaction = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    handle
        .stage_subscription(
            transaction,
            &PluginIdentity::new("test", "mirror"),
            &SubscriptionSpec {
                event_type: Arc::from("fast"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                capacity: 8,
                routing_keys: Arc::from([7]),
            },
        )
        .unwrap();
    let (_, committed) = handle.commit_at_safe_point(transaction).unwrap();

    let mut request = PublishRequest::new("fast", 1, b"payload");
    request.routing_key = 7;
    handle.try_publish(request).unwrap();
    assert_eq!(fast_rx.try_recv().unwrap(), b"payload");

    let (mirror_tx, mirror_rx) = bounded(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    while committed[0]
        .receiver
        .dispatch_next(&RecordingHandler(mirror_tx.clone()), Duration::ZERO)
        .unwrap()
        == DispatchOutcome::Idle
    {
        assert!(Instant::now() < deadline);
    }
    assert_eq!(mirror_rx.recv().unwrap(), b"payload");
    assert!(handle.unregister_fast_lane(token));
    engine.stop().unwrap();
}

#[test]
fn async_fast_lane_is_ordered_bounded_and_does_not_block_publishers() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("async-fast", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();

    let (entered_tx, entered_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let token = handle
        .register_async_fast_lane(
            &[("async-fast", 1)],
            vec![9],
            AsyncFastLaneConfig {
                capacity: 1,
                idle_sleep: Duration::from_millis(1),
                ..AsyncFastLaneConfig::default()
            },
            Arc::new(BlockingHandler {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();

    let mut first = PublishRequest::new("async-fast", 1, b"first");
    first.routing_key = 9;
    handle.try_publish(first).unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"first"
    );

    let mut second = PublishRequest::new("async-fast", 1, b"second");
    second.routing_key = 9;
    let started = Instant::now();
    handle.try_publish(second).unwrap();
    assert!(started.elapsed() < Duration::from_millis(50));

    let mut overflow = PublishRequest::new("async-fast", 1, b"overflow");
    overflow.routing_key = 9;
    handle.try_publish(overflow).unwrap();
    assert_eq!(engine.metrics().snapshot().fast_lane_drop_total, 1);

    release_tx.send(()).unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"second"
    );
    release_tx.send(()).unwrap();
    assert!(handle.unregister_fast_lane(token));
    let metrics = engine.metrics().snapshot();
    assert_eq!(metrics.fast_lane_enqueue_total, 2);
    assert_eq!(metrics.fast_lane_depth_max, 1);
    engine.stop().unwrap();
}

#[test]
fn async_fast_lane_contains_handler_failure() {
    struct FailingFastHandler(Arc<AtomicUsize>);
    impl EventHandler for FailingFastHandler {
        fn handle(&self, _event: EventView<'_>) -> Result<(), PluginError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(PluginError::new(
                ErrorKind::PluginFailed,
                PluginIdentity::new("test", "async-fast-failed"),
                LifecycleState::Running,
                "callback",
                "expected async FastLane failure",
            ))
        }
    }

    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("async-fail", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let token = handle
        .register_async_fast_lane(
            &[("async-fail", 1)],
            vec![],
            AsyncFastLaneConfig::default(),
            Arc::new(FailingFastHandler(calls.clone())),
        )
        .unwrap();
    handle
        .try_publish(PublishRequest::new("async-fail", 1, b"first"))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while calls.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    handle
        .try_publish(PublishRequest::new(
            "async-fail",
            1,
            b"normal-route-survives",
        ))
        .unwrap();
    thread::sleep(Duration::from_millis(10));
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(handle.unregister_fast_lane(token));
    engine.stop().unwrap();
}

#[test]
fn async_fast_lane_priority_bypasses_normal_backlog() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    for event_type in ["fast-normal", "fast-priority"] {
        handle
            .register_event(event_type, 1, EventClass::Market, PoolKind::MarketBatch)
            .unwrap();
    }
    engine.start().unwrap();
    let (entered_tx, entered_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let token = handle
        .register_async_fast_lane(
            &[("fast-normal", 1), ("fast-priority", 1)],
            vec![],
            AsyncFastLaneConfig {
                capacity: 2,
                priority_event_types: vec![Arc::from("fast-priority")],
                ..AsyncFastLaneConfig::default()
            },
            Arc::new(BlockingHandler {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();
    handle
        .try_publish(PublishRequest::new("fast-normal", 1, b"running"))
        .unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"running"
    );
    handle
        .try_publish(PublishRequest::new("fast-normal", 1, b"normal"))
        .unwrap();
    handle
        .try_publish(PublishRequest::new("fast-priority", 1, b"priority"))
        .unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"priority"
    );
    release_tx.send(()).unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"normal"
    );
    release_tx.send(()).unwrap();
    assert!(handle.unregister_fast_lane(token));
    engine.stop().unwrap();
}

fn drive_receiver(
    receiver: Arc<dyn EventReceiver>,
    gate: Arc<ActivationGate>,
    handler: Arc<dyn EventHandler>,
) {
    thread::spawn(move || {
        if gate.wait_until_active() != titan_plugin_engine::ActivationState::Active {
            return;
        }
        while gate.is_active() {
            match receiver.dispatch_next(handler.as_ref(), Duration::from_micros(50)) {
                Ok(DispatchOutcome::Delivered | DispatchOutcome::Idle) => {}
                Ok(DispatchOutcome::Closed) | Err(_) => break,
            }
        }
    });
}

fn subscribe(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    qos: EventQos,
    capacity: usize,
    handler: Arc<dyn EventHandler>,
) -> (u64, Arc<ActivationGate>) {
    let gate = Arc::new(ActivationGate::new());
    let tx = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    handle
        .stage_subscription(
            tx,
            &PluginIdentity::new("test", "subscriber"),
            &SubscriptionSpec {
                event_type: Arc::from(event_type),
                schema_version,
                qos,
                capacity,
                routing_keys: Arc::from([]),
            },
        )
        .unwrap();
    let (_, mut subscriptions) = handle.commit_at_safe_point(tx).unwrap();
    let subscription = subscriptions.pop().unwrap();
    let token = subscription.token.0;
    drive_receiver(subscription.receiver, gate.clone(), handler);
    (token, gate)
}

#[test]
fn configuration_rejects_unbounded_or_inconsistent_capacity() {
    let mut config = test_config();
    config.subscribers.critical_reserve = config.subscribers.default_capacity;
    assert_eq!(config.validate(), Err(ConfigError::CriticalReserve));

    let mut config = test_config();
    config.pending_dispatch.global_capacity = 0;
    assert_eq!(config.validate(), Err(ConfigError::PendingCapacity));

    let mut config = test_config();
    config.runtime.mode = RuntimeMode::Dedicated;
    config.runtime.cpu_affinity = None;
    assert_eq!(config.validate(), Err(ConfigError::DedicatedAffinity));

    let mut config = test_config();
    config.subscribers.runtime_mode = SubscriberRuntimeMode::Dedicated;
    config.subscribers.cpu_affinity.clear();
    assert_eq!(
        config.validate(),
        Err(ConfigError::DedicatedSubscriberAffinity)
    );
}

#[test]
fn parked_subscriber_is_actively_woken_by_publish() {
    let mut config = test_config();
    config.subscribers.runtime_mode = SubscriberRuntimeMode::Park;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("wake", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();
    let transaction = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    handle
        .stage_subscription(
            transaction,
            &PluginIdentity::new("test", "parked"),
            &SubscriptionSpec {
                event_type: Arc::from("wake"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                capacity: 8,
                routing_keys: Arc::from([]),
            },
        )
        .unwrap();
    let (_, mut subscriptions) = handle.commit_at_safe_point(transaction).unwrap();
    let receiver = subscriptions.pop().unwrap().receiver;
    let (tx, rx) = bounded(1);
    let consumer = thread::spawn(move || {
        receiver
            .dispatch_next(&RecordingHandler(tx), Duration::from_secs(1))
            .unwrap()
    });
    thread::sleep(Duration::from_millis(20));
    handle
        .try_publish(PublishRequest::new("wake", 1, b"woken"))
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(250)).unwrap(),
        b"woken"
    );
    assert_eq!(consumer.join().unwrap(), DispatchOutcome::Delivered);
    engine.stop().unwrap();
}

#[test]
fn subscriptions_from_one_owner_share_a_mailbox_until_the_last_route_retires() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    for event_type in ["shared-a", "shared-b"] {
        handle
            .register_event(event_type, 1, EventClass::Market, PoolKind::MarketBatch)
            .unwrap();
    }
    engine.start().unwrap();
    let transaction = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    let owner = PluginIdentity::new("test", "shared-mailbox");
    for event_type in ["shared-a", "shared-b"] {
        handle
            .stage_subscription_in_mailbox(
                transaction,
                &owner,
                "shared",
                &SubscriptionSpec {
                    event_type: Arc::from(event_type),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    capacity: 8,
                    routing_keys: Arc::from([]),
                },
            )
            .unwrap();
    }
    let (_, subscriptions) = handle.commit_at_safe_point(transaction).unwrap();
    assert_eq!(subscriptions.len(), 2);
    assert_eq!(subscriptions[0].mailbox_id, subscriptions[1].mailbox_id);
    let receiver = subscriptions[0].receiver.clone();
    let first_token = subscriptions[0].token;
    let (tx, rx) = bounded(3);
    let handler = RecordingHandler(tx);

    handle
        .try_publish(PublishRequest::new("shared-a", 1, b"a"))
        .unwrap();
    handle
        .try_publish(PublishRequest::new("shared-b", 1, b"b"))
        .unwrap();
    for _ in 0..2 {
        let deadline = Instant::now() + Duration::from_secs(1);
        while receiver
            .dispatch_next(&handler, Duration::from_micros(50))
            .unwrap()
            == DispatchOutcome::Idle
        {
            assert!(
                Instant::now() < deadline,
                "shared mailbox delivery timed out"
            );
        }
    }
    assert_eq!(rx.recv().unwrap(), b"a");
    assert_eq!(rx.recv().unwrap(), b"b");

    handle.retire_subscription(first_token).unwrap();
    handle
        .try_publish(PublishRequest::new("shared-b", 1, b"still-open"))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while receiver
        .dispatch_next(&handler, Duration::from_micros(50))
        .unwrap()
        == DispatchOutcome::Idle
    {
        assert!(
            Instant::now() < deadline,
            "surviving route delivery timed out"
        );
    }
    assert_eq!(rx.recv().unwrap(), b"still-open");
    engine.stop().unwrap();
}

#[test]
fn plugin_control_market_reservation_publishes_without_copy_api() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("reserved", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(1);
    let (_, gate) = subscribe(
        &handle,
        "reserved",
        1,
        EventQos::ReliableOrdered,
        8,
        Arc::new(RecordingHandler(tx)),
    );
    gate.activate();
    let mut reservation = EventControl::reserve_market_batch(
        &handle,
        "reserved",
        1,
        4,
        EventPublishMetadata::default(),
        TraceContext::default(),
    )
    .unwrap();
    reservation.payload_mut().copy_from_slice(b"zero");
    reservation.commit().unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), b"zero");
    gate.quiesce();
    engine.stop().unwrap();
}

#[test]
fn arena_is_bounded_reuses_generation_and_reclaims_last_reference() {
    let metrics = Arc::new(EngineMetrics::default());
    let mut config = ArenaConfig::default();
    config.small_event = PoolConfig {
        slots: 1,
        block_bytes: 8,
        low_watermark: 0,
    };
    let arena = EventArena::new(&config, metrics);
    let mut reservation = arena.reserve(PoolKind::SmallEvent, 4).unwrap();
    reservation.payload_mut().copy_from_slice(b"test");
    let event = reservation.commit();
    let first = event.handle();
    let clone = event.clone();
    assert_eq!(clone.payload(), b"test");
    assert!(matches!(
        arena.reserve(PoolKind::SmallEvent, 1),
        Err(PublishError::EventArenaExhausted(PoolKind::SmallEvent))
    ));
    drop(event);
    assert_eq!(arena.outstanding_blocks(), 1);
    drop(clone);
    assert_eq!(arena.outstanding_blocks(), 0);
    let second = arena.reserve(PoolKind::SmallEvent, 1).unwrap().commit();
    assert_eq!(first.block_id, second.handle().block_id);
    assert_ne!(first.generation, second.handle().generation);
}

#[test]
fn plugin_event_control_routes_off_publisher_and_event_loop_threads() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("orders", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(4);
    let (token, gate) = subscribe(
        &handle,
        "orders",
        1,
        EventQos::ReliableOrdered,
        8,
        Arc::new(RecordingHandler(tx)),
    );
    assert!(gate.activate());
    handle
        .publish("orders", 1, b"accepted", TraceContext::default())
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        b"accepted"
    );
    let trace_stages = std::iter::from_fn(|| handle.pop_trace_point())
        .map(|point| point.stage)
        .collect::<Vec<_>>();
    for stage in [
        TraceStage::Published,
        TraceStage::EventLoopDequeued,
        TraceStage::Dispatched,
        TraceStage::SubscriberReceived,
    ] {
        assert!(trace_stages.contains(&stage));
    }
    assert_eq!(
        handle.subscriber_health(token).unwrap().state,
        SubscriberState::Normal
    );
    handle
        .retire_subscription(titan_plugin_engine::SubscriptionToken(token))
        .unwrap();
    engine.stop().unwrap();
    assert_eq!(engine.arena().outstanding_blocks(), 0);
}

#[test]
fn critical_reserve_flows_to_bounded_pending_then_resync() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.pending_dispatch.per_subscriber_capacity = 1;
    config.pending_dispatch.global_capacity = 1;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("fills", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, _rx) = bounded(8);
    let (token, _closed_gate) = subscribe(
        &handle,
        "fills",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    for value in 1_u8..=6 {
        handle
            .try_publish(PublishRequest::new("fills", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::ResyncRequired)
    });
    let health = handle.subscriber_health(token).unwrap();
    assert_eq!(health.delivery_gap, Some((1, 6)));
    handle.complete_recovery(token, 6).unwrap();
    assert_eq!(
        handle.subscriber_health(token).unwrap().state,
        SubscriberState::Normal
    );
    engine.stop().unwrap();
}

#[test]
fn pending_guarantee_is_reserved_only_for_critical_routes() {
    let mut config = test_config();
    config.pending_dispatch.per_subscriber_capacity = 2;
    config.pending_dispatch.global_capacity = 2;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 2;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("market-only", 1, EventClass::Market, PoolKind::SmallEvent)
        .unwrap();
    handle
        .register_event(
            "critical-only",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();

    for _ in 0..2 {
        subscribe(
            &handle,
            "market-only",
            1,
            EventQos::Latest,
            4,
            Arc::new(RecordingHandler(bounded(1).0)),
        );
    }
    subscribe(
        &handle,
        "critical-only",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(bounded(1).0)),
    );
    engine.stop().unwrap();
}

#[test]
fn market_batch_reservation_is_zero_copy_and_latest_is_coalesced() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 2;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("market", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(8);
    let (_token, gate) = subscribe(
        &handle,
        "market",
        1,
        EventQos::Latest,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    for value in 1_u8..=5 {
        let mut reservation = handle
            .reserve_market_batch(ReserveRequest::new("market", 1, 1))
            .unwrap();
        reservation.payload_mut()[0] = value;
        reservation.commit().unwrap();
    }
    wait_until(|| engine.metrics().snapshot().dispatch_total >= 3);
    assert!(gate.activate());
    let mut values = vec![rx.recv_timeout(Duration::from_secs(2)).unwrap()[0]];
    wait_until(|| {
        while let Ok(value) = rx.try_recv() {
            values.push(value[0]);
        }
        values.contains(&5)
    });
    assert_eq!(values[0], 1);
    assert!(values.contains(&5));
    assert!(engine.metrics().snapshot().drop_total > 0);
    engine.stop().unwrap();
}

#[test]
fn source_gaps_and_timers_use_bounded_out_of_band_signals() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("ticks", 1, EventClass::Market, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    for sequence in [1, 3] {
        let mut request = PublishRequest::new("ticks", 1, b"x");
        request.source_id = 1;
        request.source_sequence = sequence;
        handle.try_publish(request).unwrap();
    }
    wait_until(|| engine.metrics().snapshot().source_sequence_gap_total == 1);
    let signal = std::iter::from_fn(|| handle.pop_fault_signal())
        .find(|signal| signal.kind == FaultKind::SourceSequenceGap)
        .unwrap();
    assert_eq!(signal.sequence, 3);

    let deadline = handle.now_ns() + 1_000_000;
    handle.schedule_timer(7, deadline).unwrap();
    wait_until(|| handle.pop_timer_signal().is_some());
    engine.stop().unwrap();
}

#[test]
fn multiple_publishers_deliver_without_loss() {
    let mut config = test_config();
    config.ingress.critical_capacity = 1_024;
    config.arena.small_event.slots = 1_024;
    config.arena.small_event.low_watermark = 16;
    config.subscribers.default_capacity = 1_024;
    config.subscribers.critical_reserve = 128;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("orders", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    struct CountingHandler(Arc<AtomicUsize>);
    impl EventHandler for CountingHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    let (_token, gate) = subscribe(
        &handle,
        "orders",
        1,
        EventQos::ReliableOrdered,
        1_024,
        Arc::new(CountingHandler(count.clone())),
    );
    gate.activate();
    let mut publishers = Vec::new();
    for producer in 0..4_u32 {
        let handle = handle.clone();
        publishers.push(thread::spawn(move || {
            for sequence in 1..=100_u64 {
                let mut request = PublishRequest::new("orders", 1, b"o");
                request.source_id = producer;
                request.source_sequence = sequence;
                loop {
                    match handle.try_publish(request) {
                        Ok(()) => break,
                        Err(PublishError::CriticalIngressFull)
                        | Err(PublishError::EventArenaExhausted(_)) => thread::yield_now(),
                        Err(error) => panic!("unexpected publish error: {error}"),
                    }
                }
            }
        }));
    }
    for publisher in publishers {
        publisher.join().unwrap();
    }
    wait_until(|| count.load(Ordering::Relaxed) == 400);
    engine.stop().unwrap();
}

#[test]
fn release_acquire_publication_and_last_release_are_model_checked() {
    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering, fence},
        };
        use loom::thread;

        let published = Arc::new(AtomicBool::new(false));
        let payload = Arc::new(AtomicUsize::new(0));
        let producer_published = published.clone();
        let producer_payload = payload.clone();
        let producer = thread::spawn(move || {
            producer_payload.store(42, Ordering::Relaxed);
            producer_published.store(true, Ordering::Release);
        });
        let consumer = thread::spawn(move || {
            if published.load(Ordering::Acquire) {
                assert_eq!(payload.load(Ordering::Relaxed), 42);
            }
        });
        producer.join().unwrap();
        consumer.join().unwrap();

        let refs = Arc::new(AtomicUsize::new(2));
        let recycled = Arc::new(AtomicUsize::new(0));
        let mut releases = Vec::new();
        for _ in 0..2 {
            let refs = refs.clone();
            let recycled = recycled.clone();
            releases.push(thread::spawn(move || {
                if refs.fetch_sub(1, Ordering::Release) == 1 {
                    fence(Ordering::Acquire);
                    recycled.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for release in releases {
            release.join().unwrap();
        }
        assert_eq!(recycled.load(Ordering::Relaxed), 1);
    });

    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use loom::thread;

        const CLOSED: usize = 1 << (usize::BITS - 1);
        const COUNT_MASK: usize = CLOSED - 1;
        let admission = Arc::new(AtomicUsize::new(0));
        let queued = Arc::new(AtomicUsize::new(0));
        let producer_admission = admission.clone();
        let producer_queued = queued.clone();
        let producer = thread::spawn(move || {
            let mut current = producer_admission.load(Ordering::Acquire);
            loop {
                if current & CLOSED != 0 {
                    break;
                }
                match producer_admission.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        producer_queued.fetch_add(1, Ordering::Relaxed);
                        producer_admission.fetch_sub(1, Ordering::Release);
                        break;
                    }
                    Err(actual) => current = actual,
                }
            }
        });
        let failure_admission = admission.clone();
        let failure_queued = queued.clone();
        let failure = thread::spawn(move || {
            failure_admission.fetch_or(CLOSED, Ordering::AcqRel);
            while failure_admission.load(Ordering::Acquire) & COUNT_MASK != 0 {
                thread::yield_now();
            }
            failure_queued.swap(0, Ordering::AcqRel);
        });
        producer.join().unwrap();
        failure.join().unwrap();
        assert_eq!(queued.load(Ordering::Acquire), 0);
    });

    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use loom::thread;

        const FREE: usize = 0;
        const PUBLISHED: usize = 1;
        let slot = Arc::new(AtomicUsize::new(FREE));
        let winners = Arc::new(AtomicUsize::new(0));
        let mut producers = Vec::new();
        for _ in 0..2 {
            let slot = slot.clone();
            let winners = winners.clone();
            producers.push(thread::spawn(move || {
                if slot
                    .compare_exchange(FREE, PUBLISHED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for producer in producers {
            producer.join().unwrap();
        }
        assert_eq!(winners.load(Ordering::Acquire), 1);
        assert_eq!(slot.swap(FREE, Ordering::AcqRel), PUBLISHED);
    });

    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use loom::thread;

        const PENDING: usize = 0;
        const CHANNEL: usize = 1;
        const RETIRED: usize = 2;
        let owner = Arc::new(AtomicUsize::new(PENDING));
        let retry_owner = owner.clone();
        let retry = thread::spawn(move || {
            let _ =
                retry_owner.compare_exchange(PENDING, CHANNEL, Ordering::AcqRel, Ordering::Acquire);
        });
        let retire_owner = owner.clone();
        let retire = thread::spawn(move || {
            let _ = retire_owner.compare_exchange(
                PENDING,
                RETIRED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
        retry.join().unwrap();
        retire.join().unwrap();
        assert!(matches!(owner.load(Ordering::Acquire), CHANNEL | RETIRED));
    });
}

#[test]
fn pending_retry_preserves_fifo_after_gate_opens() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.pending_dispatch.per_subscriber_capacity = 4;
    config.pending_dispatch.global_capacity = 4;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 4;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("fifo", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(8);
    let (_token, gate) = subscribe(
        &handle,
        "fifo",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    for value in 1_u8..=7 {
        handle
            .try_publish(PublishRequest::new("fifo", 1, &[value]))
            .unwrap();
    }
    wait_until(|| engine.metrics().snapshot().publish_total == 7);
    gate.activate();
    let values = (0..7)
        .map(|_| rx.recv_timeout(Duration::from_secs(2)).unwrap()[0])
        .collect::<Vec<_>>();
    assert_eq!(values, (1_u8..=7).collect::<Vec<_>>());
    assert!(engine.metrics().snapshot().pending_retry_success >= 3);
    engine.stop().unwrap();
}

#[test]
fn precompiled_routes_filter_keys_and_continue_large_fanout() {
    let mut config = test_config();
    config.dispatch.max_fanout_per_step = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("quotes", 1, EventClass::Market, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let mut receivers = Vec::new();
    for (index, routing_key) in [7_u64, 7, 9].into_iter().enumerate() {
        let (tx, rx) = bounded(2);
        let gate = Arc::new(ActivationGate::new());
        let transaction = handle
            .begin_route_update(handle.current_route_version())
            .unwrap();
        handle
            .stage_subscription(
                transaction,
                &PluginIdentity::new("test", format!("route-{index}")),
                &SubscriptionSpec {
                    event_type: Arc::from("quotes"),
                    schema_version: 1,
                    qos: EventQos::BestEffort,
                    capacity: 8,
                    routing_keys: Arc::from([routing_key]),
                },
            )
            .unwrap();
        let (_, mut subscriptions) = handle.commit_at_safe_point(transaction).unwrap();
        let subscription = subscriptions.pop().unwrap();
        drive_receiver(
            subscription.receiver,
            gate.clone(),
            Arc::new(RecordingHandler(tx)),
        );
        gate.activate();
        receivers.push(rx);
    }
    let mut request = PublishRequest::new("quotes", 1, b"q");
    request.routing_key = 7;
    handle.try_publish(request).unwrap();
    assert_eq!(
        receivers[0].recv_timeout(Duration::from_secs(2)).unwrap(),
        b"q"
    );
    assert_eq!(
        receivers[1].recv_timeout(Duration::from_secs(2)).unwrap(),
        b"q"
    );
    assert!(
        receivers[2]
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );
    assert!(engine.metrics().snapshot().fanout_continuation_total > 0);
    engine.stop().unwrap();
}

#[test]
fn route_transactions_reject_stale_base_without_partial_commit() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("route", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let first = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    let stale = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    for transaction in [first, stale] {
        handle
            .stage_subscription(
                transaction,
                &PluginIdentity::new("test", "route"),
                &SubscriptionSpec {
                    event_type: Arc::from("route"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    capacity: 8,
                    routing_keys: Arc::from([]),
                },
            )
            .unwrap();
    }
    handle.commit_at_safe_point(first).unwrap();
    let error = handle.commit_at_safe_point(stale).unwrap_err();
    assert_eq!(error.kind, ErrorKind::SubscriptionRejected);
    assert_eq!(handle.current_route_version().0, 1);
    engine.stop().unwrap();
}

#[test]
fn pool_exhaustion_is_isolated_and_persisted_in_runtime_health() {
    let mut config = test_config();
    config.arena.market_batch.slots = 1;
    config.arena.market_batch.low_watermark = 0;
    config.arena.small_event.slots = 2;
    config.arena.small_event.low_watermark = 0;
    config.fault_signal_ring.capacity = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("market", 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    handle
        .register_event("risk", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let held = handle
        .reserve_market_batch(ReserveRequest::new("market", 1, 1))
        .unwrap();
    assert!(matches!(
        handle.reserve_market_batch(ReserveRequest::new("market", 1, 1)),
        Err(PublishError::EventArenaExhausted(PoolKind::MarketBatch))
    ));
    assert!(matches!(
        handle.reserve_market_batch(ReserveRequest::new("market", 1, 1)),
        Err(PublishError::EventArenaExhausted(PoolKind::MarketBatch))
    ));
    assert_ne!(handle.runtime_health().arena_pressure_mask, 0);
    assert!(engine.metrics().snapshot().fault_signal_drop_total > 0);
    handle
        .try_publish(PublishRequest::new("risk", 1, b"r"))
        .unwrap();
    drop(held);
    handle.clear_runtime_health();
    assert_eq!(handle.runtime_health(), RuntimeHealthSnapshot::default());
    engine.stop().unwrap();
}

#[test]
fn callback_failure_is_contained_and_marks_only_that_subscriber_failed() {
    struct FailingHandler;
    impl EventHandler for FailingHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            Err(PluginError::new(
                ErrorKind::PluginFailed,
                PluginIdentity::new("test", "failed"),
                LifecycleState::Running,
                "callback",
                "injected failure",
            ))
        }
    }
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("failure", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (token, gate) = subscribe(
        &handle,
        "failure",
        1,
        EventQos::ReliableOrdered,
        8,
        Arc::new(FailingHandler),
    );
    gate.activate();
    handle
        .try_publish(PublishRequest::new("failure", 1, b"x"))
        .unwrap();
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::Failed)
    });
    assert!(
        std::iter::from_fn(|| handle.pop_fault_signal())
            .any(|signal| signal.kind == FaultKind::SubscriberFailed)
    );
    engine.stop().unwrap();
}

#[test]
fn failed_subscriber_records_and_releases_queued_and_pending_deliveries() {
    struct BlockingFailureHandler(crossbeam_channel::Receiver<()>);
    impl EventHandler for BlockingFailureHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            self.0.recv().expect("test releases the blocked callback");
            Err(PluginError::new(
                ErrorKind::PluginFailed,
                PluginIdentity::new("test", "blocked-failure"),
                LifecycleState::Running,
                "callback",
                "injected failure after queue saturation",
            ))
        }
    }

    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "blocked-failure",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (release_tx, release_rx) = bounded(0);
    let (token, gate) = subscribe(
        &handle,
        "blocked-failure",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(BlockingFailureHandler(release_rx)),
    );
    gate.activate();
    for value in 1_u8..=6 {
        handle
            .try_publish(PublishRequest::new("blocked-failure", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.pending_depth > 0)
    });
    release_tx.send(()).unwrap();
    wait_until(|| {
        handle.subscriber_health(token).is_some_and(|health| {
            health.state == SubscriberState::Failed
                && health.pending_depth == 0
                && health.channel_depth == 0
                && health.outstanding_handles == 0
                && health.delivery_gap.is_some()
        })
    });
    engine.stop().unwrap();
    assert_eq!(engine.arena().outstanding_blocks(), 0);
}

#[test]
fn recovery_waits_for_the_old_handler_epoch_to_quiesce() {
    struct BlockingHandler {
        started: Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    }
    impl EventHandler for BlockingHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            let _ = self.started.try_send(());
            self.release.recv().unwrap();
            Ok(())
        }
    }

    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.pending_dispatch.per_subscriber_capacity = 1;
    config.pending_dispatch.global_capacity = 1;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "recovery-epoch",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (started_tx, started_rx) = bounded(1);
    let (release_tx, release_rx) = bounded(0);
    let (token, gate) = subscribe(
        &handle,
        "recovery-epoch",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(BlockingHandler {
            started: started_tx,
            release: release_rx,
        }),
    );
    gate.activate();
    handle
        .try_publish(PublishRequest::new("recovery-epoch", 1, b"1"))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    for value in 2_u8..=7 {
        handle
            .try_publish(PublishRequest::new("recovery-epoch", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::ResyncRequired)
    });
    let recovery_sequence = handle
        .subscriber_health(token)
        .unwrap()
        .delivery_gap
        .unwrap()
        .1;
    assert!(matches!(
        handle.complete_recovery(token, recovery_sequence),
        Err(EngineError::RecoveryNotQuiescent(id)) if id == token
    ));
    release_tx.send(()).unwrap();
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.outstanding_handles == 0)
    });
    handle.complete_recovery(token, recovery_sequence).unwrap();
    assert_eq!(
        handle.subscriber_health(token).unwrap().state,
        SubscriberState::Normal
    );
    engine.stop().unwrap();
}

#[test]
fn plugin_publish_metadata_drives_routing_and_source_sequence() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("metadata", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(4);
    let gate = Arc::new(ActivationGate::new());
    let route = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();
    handle
        .stage_subscription(
            route,
            &PluginIdentity::new("test", "metadata"),
            &SubscriptionSpec {
                event_type: Arc::from("metadata"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                capacity: 8,
                routing_keys: Arc::from([42]),
            },
        )
        .unwrap();
    let (_, mut subscriptions) = handle.commit_at_safe_point(route).unwrap();
    let subscription = subscriptions.pop().unwrap();
    drive_receiver(
        subscription.receiver,
        gate.clone(),
        Arc::new(RecordingHandler(tx)),
    );
    gate.activate();
    handle
        .publish_with_metadata(
            "metadata",
            1,
            b"matched",
            EventPublishMetadata {
                source_id: 3,
                source_sequence: 7,
                routing_key: 42,
                ..EventPublishMetadata::default()
            },
            TraceContext::default(),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), b"matched");
    let mut duplicate = EventPublishMetadata {
        source_id: 3,
        source_sequence: 7,
        routing_key: 42,
        ..EventPublishMetadata::default()
    };
    handle
        .publish_with_metadata(
            "metadata",
            1,
            b"duplicate",
            duplicate,
            TraceContext::default(),
        )
        .unwrap();
    duplicate.source_sequence = 9;
    handle
        .publish_with_metadata("metadata", 1, b"gap", duplicate, TraceContext::default())
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), b"gap");
    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    wait_until(|| handle.runtime_health().last_source_gap == Some((3_u64 << 32) | 9));
    engine.stop().unwrap();
}

#[test]
fn lagging_subscriber_returns_to_normal_below_low_watermark() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.subscribers.lagging_high_watermark_ratio = 0.5;
    config.subscribers.recovery_low_watermark_ratio = 0.25;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("lagging", 1, EventClass::Market, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(8);
    let (token, gate) = subscribe(
        &handle,
        "lagging",
        1,
        EventQos::BestEffort,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    for value in 1_u8..=3 {
        handle
            .try_publish(PublishRequest::new("lagging", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::Lagging)
    });
    gate.activate();
    for _ in 0..3 {
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::Normal)
    });
    assert!(
        std::iter::from_fn(|| handle.pop_fault_signal())
            .any(|signal| signal.kind == FaultKind::SubscriberRecovered)
    );
    engine.stop().unwrap();
}

#[test]
fn pressure_diagnostics_scan_incrementally_with_configured_budget() {
    let mut config = test_config();
    config.diagnostics.pressure_scan_budget = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("pressure", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let mut tokens = Vec::new();
    for _ in 0..2 {
        tokens.push(
            subscribe(
                &handle,
                "pressure",
                1,
                EventQos::ReliableOrdered,
                8,
                Arc::new(RecordingHandler(bounded(1).0)),
            )
            .0,
        );
    }
    let first = handle.pressure_subscriber_batch();
    let second = handle.pressure_subscriber_batch();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].0, second[0].0);
    assert!(tokens.contains(&first[0].0) && tokens.contains(&second[0].0));
    engine.stop().unwrap();
}

#[test]
fn pending_retry_round_robins_across_subscribers() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.pending_dispatch.per_subscriber_capacity = 4;
    config.pending_dispatch.global_capacity = 8;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 4;
    config.dispatch.pending = DrainBudgetConfig::new(1, 5_000_000);
    config.diagnostics.trace_ring_capacity = 4_096;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "pending-fair",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let mut tokens = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..2 {
        let (token, gate) = subscribe(
            &handle,
            "pending-fair",
            1,
            EventQos::ReliableOrdered,
            4,
            Arc::new(RecordingHandler(bounded(16).0)),
        );
        tokens.push(token);
        gates.push(gate);
    }
    for value in 1_u8..=6 {
        handle
            .try_publish(PublishRequest::new("pending-fair", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        tokens.iter().all(|token| {
            handle
                .subscriber_health(*token)
                .is_some_and(|health| health.pending_depth >= 2)
        })
    });
    while handle.pop_trace_point().is_some() {}
    for gate in gates {
        gate.activate();
    }
    wait_until(|| engine.metrics().snapshot().pending_retry_success >= 2);
    let retried = std::iter::from_fn(|| handle.pop_trace_point())
        .filter(|point| point.stage == TraceStage::Dispatched)
        .map(|point| point.subscriber_id)
        .take(2)
        .collect::<Vec<_>>();
    assert_eq!(retried.len(), 2);
    assert_ne!(retried[0], retried[1]);
    engine.stop().unwrap();
}

#[test]
fn latest_slot_blocks_later_critical_delivery_until_fifo_predecessors_drain() {
    fn tracked(
        arena: &Arc<EventArena>,
        health: &Arc<SubscriberHealth>,
        clock: &EngineClock,
        descriptor: Arc<EventDescriptor>,
        sequence: u64,
    ) -> TrackedDelivery {
        let mut reservation = arena.reserve(PoolKind::SmallEvent, 1).unwrap();
        reservation.payload_mut()[0] = sequence as u8;
        TrackedDelivery::new(
            Delivery {
                descriptor,
                header: EventHeader {
                    local_sequence: sequence,
                    ..EventHeader::default()
                },
                payload: reservation.commit(),
                ingress_at_ns: clock.now_ns(),
            },
            health.clone(),
            clock.clone(),
        )
    }

    let metrics = Arc::new(EngineMetrics::default());
    let arena = EventArena::new(&test_config().arena, metrics.clone());
    let health = Arc::new(SubscriberHealth::default());
    let gate = Arc::new(ActivationGate::new());
    let (tx, rx) = bounded(8);
    let channel = SubscriberChannel::new(SubscriberChannelArgs {
        id: 1,
        owner: PluginIdentity::new("test", "fifo"),
        capacity: 4,
        critical_reserve: 1,
        high_ratio: 0.8,
        low_ratio: 0.5,
        health: health.clone(),
        runtime_mode: SubscriberRuntimeMode::SpinSleep,
        spin_iterations: 0,
        idle_sleep: Duration::from_micros(50),
        cpu_affinity: None,
        fault_signals: Arc::new(crossbeam_queue::ArrayQueue::new(8)),
        trace_ring: Arc::new(crossbeam_queue::ArrayQueue::new(32)),
        metrics,
    });
    drive_receiver(
        channel.clone(),
        gate.clone(),
        Arc::new(RecordingHandler(tx)),
    );
    let market = Arc::new(EventDescriptor {
        id: 1,
        event_type: Arc::from("market"),
        schema_version: 1,
        class: EventClass::Market,
        pool: PoolKind::SmallEvent,
    });
    let critical = Arc::new(EventDescriptor {
        id: 2,
        event_type: Arc::from("critical"),
        schema_version: 1,
        class: EventClass::Critical,
        pool: PoolKind::SmallEvent,
    });
    let clock = EngineClock::new();
    for sequence in 1..=3 {
        assert!(
            channel
                .try_push_market(tracked(&arena, &health, &clock, market.clone(), sequence,))
                .is_ok()
        );
    }
    let latest = channel
        .try_push_market(tracked(&arena, &health, &clock, market, 4))
        .unwrap_err();
    assert!(!channel.replace_latest(latest));
    let mut blocked_critical = channel
        .try_push_critical(tracked(&arena, &health, &clock, critical, 5))
        .unwrap_err();
    gate.activate();
    let mut values = Vec::new();
    for _ in 0..4 {
        values.push(rx.recv_timeout(Duration::from_secs(2)).unwrap()[0]);
    }
    loop {
        match channel.try_push_critical(blocked_critical) {
            Ok(()) => break,
            Err(returned) => {
                blocked_critical = returned;
                thread::yield_now();
            }
        }
    }
    values.push(rx.recv_timeout(Duration::from_secs(2)).unwrap()[0]);
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
    channel.stop_and_drain();
    assert_eq!(arena.outstanding_blocks(), 0);
}

#[test]
fn retirement_stops_new_routing_and_drains_existing_critical_work() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("retire", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(0);
    let (token, gate) = subscribe(
        &handle,
        "retire",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    gate.activate();
    for value in 1_u8..=6 {
        handle
            .try_publish(PublishRequest::new("retire", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.pending_depth > 0)
    });

    let retiring_handle = handle.clone();
    let retirement = thread::spawn(move || {
        retiring_handle.retire_subscription(titan_plugin_engine::SubscriptionToken(token))
    });
    let mut received = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(payload) => received.push(payload[0]),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                panic!("retired subscriber runtime did not exit")
            }
        }
    }
    retirement.join().unwrap().unwrap();
    assert!((1..=6).collect::<Vec<_>>().starts_with(&received));
    assert!(handle.subscriber_health(token).is_none());
    engine.stop().unwrap();
    assert_eq!(engine.arena().outstanding_blocks(), 0);
}

#[test]
fn engine_shutdown_drains_ingress_pending_and_subscriber_channels() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    let engine = Arc::new(EventEngine::new(config).unwrap());
    let handle = engine.handle();
    handle
        .register_event(
            "shutdown-drain",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(0);
    let (token, gate) = subscribe(
        &handle,
        "shutdown-drain",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(tx)),
    );
    gate.activate();
    for value in 1_u8..=6 {
        handle
            .try_publish(PublishRequest::new("shutdown-drain", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.pending_depth > 0)
    });

    let stopping_engine = engine.clone();
    let shutdown = thread::spawn(move || stopping_engine.stop());
    let mut received = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(payload) => received.push(payload[0]),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                panic!("subscriber runtime did not exit during shutdown")
            }
        }
    }
    shutdown.join().unwrap().unwrap();
    assert!((1..=6).collect::<Vec<_>>().starts_with(&received));
    assert!(matches!(
        handle.try_publish(PublishRequest::new("shutdown-drain", 1, b"late")),
        Err(PublishError::Stopped)
    ));
    assert_eq!(engine.arena().outstanding_blocks(), 0);
}

#[test]
fn timer_queue_and_signal_queue_remain_bounded() {
    let mut config = test_config();
    config.dispatch.timer_capacity = 2;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    engine.start().unwrap();
    let future = handle.now_ns() + 10_000_000_000;
    handle.schedule_timer(1, future).unwrap();
    handle.schedule_timer(2, future).unwrap();
    assert!(matches!(
        handle.schedule_timer(3, future),
        Err(EngineError::TimerQueueFull)
    ));
    engine.stop().unwrap();
}

#[test]
fn pending_age_expires_to_resync_without_blocking_event_loop() {
    let mut config = test_config();
    config.subscribers.default_capacity = 4;
    config.subscribers.critical_reserve = 1;
    config.pending_dispatch.per_subscriber_capacity = 4;
    config.pending_dispatch.global_capacity = 4;
    config.pending_dispatch.guaranteed_per_critical_subscriber = 4;
    config.pending_dispatch.max_age_ms = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("aged", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    let (token, _gate) = subscribe(
        &handle,
        "aged",
        1,
        EventQos::ReliableOrdered,
        4,
        Arc::new(RecordingHandler(bounded(8).0)),
    );
    for value in 1_u8..=5 {
        handle
            .try_publish(PublishRequest::new("aged", 1, &[value]))
            .unwrap();
    }
    wait_until(|| {
        handle
            .subscriber_health(token)
            .is_some_and(|health| health.state == SubscriberState::ResyncRequired)
    });
    assert!(
        std::iter::from_fn(|| handle.pop_fault_signal())
            .any(|signal| signal.kind == FaultKind::PendingExpired)
    );
    engine.stop().unwrap();
}

#[test]
fn critical_load_still_services_market_and_due_timers() {
    let mut config = test_config();
    config.dispatch.critical = DrainBudgetConfig::new(1, 5_000_000);
    config.dispatch.market = DrainBudgetConfig::new(1, 5_000_000);
    config.diagnostics.trace_ring_capacity = 1_024;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("critical", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    handle
        .register_event("market", 1, EventClass::Market, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();
    handle.schedule_timer(99, handle.now_ns()).unwrap();
    for sequence in 1..=50_u64 {
        let mut request = PublishRequest::new("critical", 1, b"c");
        request.source_sequence = sequence;
        request.trace.trace_id = 1;
        handle.try_publish(request).unwrap();
    }
    let mut market = PublishRequest::new("market", 1, b"m");
    market.trace.trace_id = 2;
    handle.try_publish(market).unwrap();
    wait_until(|| handle.pop_timer_signal().is_some());
    let mut market_dequeued = false;
    wait_until(|| {
        while let Some(point) = handle.pop_trace_point() {
            market_dequeued |=
                point.trace.trace_id == 2 && point.stage == TraceStage::EventLoopDequeued;
        }
        market_dequeued
    });
    engine.stop().unwrap();
}

#[test]
fn core_runtime_enforces_event_before_plugin_lifecycle() {
    let mut runtime =
        TitanCoreRuntime::new(test_config(), titan_plugin_engine::ApiVersion::new(1, 0)).unwrap();
    runtime.start().unwrap();
    assert!(matches!(
        runtime
            .event_handle()
            .try_publish(PublishRequest::new("unknown", 1, b"x")),
        Err(PublishError::InvalidEvent)
    ));
    runtime
        .shutdown(titan_plugin_engine::StopReason::Shutdown)
        .unwrap();
}

#[test]
fn latency_histogram_reports_required_percentiles_without_allocation_on_record() {
    let histogram = LatencyHistogram::default();
    for value in [1, 2, 4, 8, 16, 32, 64, 128] {
        histogram.record(value);
    }
    let summary = histogram.summary();
    assert_eq!(summary.count, 8);
    assert!(summary.p50_ns <= summary.p99_ns);
    assert!(summary.p99_ns <= summary.p999_ns);
    assert!(summary.p999_ns <= summary.max_ns);
}

struct IntegrationEndpoint;

impl titan_plugin_engine::ServiceEndpoint for IntegrationEndpoint {
    fn call(
        &self,
        request: titan_plugin_engine::BoxValue,
        _: TraceContext,
    ) -> Result<titan_plugin_engine::BoxValue, PluginError> {
        Ok(Box::new(*request.downcast::<u64>().unwrap() + 1))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct IntegrationHandler {
    received: Sender<(String, Vec<u8>)>,
    count: Arc<AtomicUsize>,
}

impl EventHandler for IntegrationHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.count.fetch_add(1, Ordering::Release);
        self.received
            .send((
                thread::current().name().unwrap_or("unnamed").to_owned(),
                event.payload.to_vec(),
            ))
            .unwrap();
        Ok(())
    }
}

struct IntegrationPlugin {
    identity: PluginIdentity,
    publisher: Arc<std::sync::Mutex<Option<titan_plugin_engine::EventPublisher>>>,
    delivered: Arc<AtomicUsize>,
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl titan_plugin_engine::Plugin for IntegrationPlugin {
    fn validate(&self, _: &titan_plugin_engine::ValidationContext) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("validate");
        Ok(())
    }

    fn start(
        &mut self,
        context: &mut titan_plugin_engine::PluginContext,
    ) -> Result<(), PluginError> {
        *self.publisher.lock().unwrap() = Some(context.events.clone());
        self.log.lock().unwrap().push("start");
        Ok(())
    }

    fn quiesce(&mut self, _: titan_plugin_engine::StopReason) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("quiesce");
        let before = self.delivered.load(Ordering::Acquire);
        self.publisher.lock().unwrap().as_ref().unwrap().publish(
            "integration.event",
            1,
            b"quiesce",
            TraceContext::default(),
        )?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.delivered.load(Ordering::Acquire) == before {
            if Instant::now() >= deadline {
                return Err(PluginError::new(
                    ErrorKind::PluginFailed,
                    self.identity.clone(),
                    LifecycleState::Quiescing,
                    "quiesce",
                    "subscriber did not converge while activation gate was active",
                ));
            }
            thread::yield_now();
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("stop");
        Ok(())
    }
}

struct IntegrationFactory {
    manifest: &'static titan_plugin_engine::PluginManifest,
    received: Sender<(String, Vec<u8>)>,
    delivered: Arc<AtomicUsize>,
    publisher: Arc<std::sync::Mutex<Option<titan_plugin_engine::EventPublisher>>>,
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl titan_plugin_engine::PluginFactory for IntegrationFactory {
    fn manifest(&self) -> &'static titan_plugin_engine::PluginManifest {
        self.manifest
    }

    fn create(
        &self,
        init: titan_plugin_engine::PluginInit,
    ) -> Result<titan_plugin_engine::PluginBundle, PluginError> {
        let service_key = titan_plugin_engine::ServiceKey {
            id: titan_plugin_engine::ServiceId::new("integration", "counter"),
            version: semver::Version::new(1, 0, 0),
            scope: titan_plugin_engine::ServiceScope::Global,
        };
        Ok(titan_plugin_engine::PluginBundle {
            lifecycle: Box::new(IntegrationPlugin {
                identity: init.identity,
                publisher: self.publisher.clone(),
                delivered: self.delivered.clone(),
                log: self.log.clone(),
            }),
            service_exports: vec![titan_plugin_engine::ServiceExport {
                service_key,
                endpoint: Arc::new(IntegrationEndpoint),
            }],
            subscription_bindings: vec![titan_plugin_engine::SubscriptionBinding {
                spec: SubscriptionSpec {
                    event_type: Arc::from("integration.event"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    capacity: 8,
                    routing_keys: Arc::from([]),
                },
                handler: Arc::new(IntegrationHandler {
                    received: self.received.clone(),
                    count: self.delivered.clone(),
                }),
            }],
        })
    }
}

#[test]
fn plugin_engine_uses_real_event_engine_and_direct_stop_is_safe() {
    use titan_plugin_engine::{
        ApiVersion, CallMode, ConfigSnapshot, ExecutionModel, ExecutionSpec, PluginEngine,
        PluginManifest, PluginSpec, ProvidedService, PublishedEvent, ReloadPolicy, ScopeKind,
        ServiceId, ServiceKey, ServiceScope, SubscribedEvent, SubscriptionLimits,
    };

    let events = Arc::new(EventEngine::new(test_config()).unwrap());
    let event_handle = Arc::new(events.handle());
    event_handle
        .register_event(
            "integration.event",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    events.start().unwrap();

    let (received_tx, received_rx) = bounded(8);
    let delivered = Arc::new(AtomicUsize::new(0));
    let publisher = Arc::new(std::sync::Mutex::new(None));
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let manifest = Box::leak(Box::new(PluginManifest {
        plugin_type: Arc::from("integration"),
        name: Arc::from("integration"),
        version: semver::Version::new(1, 0, 0),
        engine_api_version: titan_plugin_engine::CORE_RUNTIME_API_VERSION,
        abi_version: ApiVersion::new(1, 0),
        config_schema: Arc::new(serde_json::json!({})),
        provides: vec![ProvidedService {
            id: ServiceId::new("integration", "counter"),
            version: semver::Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        }],
        requires: vec![],
        publishes: vec![PublishedEvent {
            event_type: Arc::from("integration.event"),
            schema_version: 1,
        }],
        subscribes: vec![SubscribedEvent {
            event_type: Arc::from("integration.event"),
            schema_version: 1,
            allowed_qos: std::collections::BTreeSet::from([EventQos::ReliableOrdered]),
        }],
        supported_execution_models: std::collections::BTreeSet::from([ExecutionModel::Background]),
        reload_policy: ReloadPolicy::RestartRequired,
    }));
    let mut plugins = PluginEngine::new(event_handle.clone(), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(IntegrationFactory {
                manifest,
                received: received_tx,
                delivered: delivered.clone(),
                publisher,
                log: log.clone(),
            }),
            semver::Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugins
        .apply(&[PluginSpec {
            instance_id: Arc::from("integration-1"),
            plugin_type: Arc::from("integration"),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Background,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 8,
                allowed_qos: std::collections::BTreeSet::from([EventQos::ReliableOrdered]),
            },
            service_scopes: vec![],
            required_service_scopes: vec![],
        }])
        .unwrap();

    event_handle
        .publish("integration.event", 1, b"running", TraceContext::default())
        .unwrap();
    let (thread_name, payload) = received_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(payload, b"running");
    assert!(thread_name.starts_with("titan-subscriber-integration-1"));

    let service_key = ServiceKey {
        id: ServiceId::new("integration", "counter"),
        version: semver::Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    };
    let service = plugins.services().bind(&service_key).unwrap();
    assert_eq!(
        *service
            .call(Box::new(41_u64), TraceContext::default())
            .unwrap()
            .downcast::<u64>()
            .unwrap(),
        42
    );

    plugins.stop_all().unwrap();
    assert_eq!(
        service
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap_err()
            .kind,
        ErrorKind::ServiceUnavailable
    );
    assert_eq!(
        &*log.lock().unwrap(),
        &["validate", "start", "quiesce", "stop"]
    );
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(2)).unwrap().1,
        b"quiesce"
    );
    events.stop().unwrap();
    assert_eq!(events.arena().outstanding_blocks(), 0);
}

struct CommitConflictPlugin {
    events: Arc<EventEngineHandle>,
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl titan_plugin_engine::Plugin for CommitConflictPlugin {
    fn validate(&self, _: &titan_plugin_engine::ValidationContext) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("validate");
        Ok(())
    }

    fn start(&mut self, _: &mut titan_plugin_engine::PluginContext) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("start");
        let transaction = self
            .events
            .begin_route_update(self.events.current_route_version())?;
        self.events.commit_at_safe_point(transaction)?;
        Ok(())
    }

    fn quiesce(&mut self, _: titan_plugin_engine::StopReason) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("quiesce");
        Ok(())
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        self.log.lock().unwrap().push("stop");
        Ok(())
    }
}

struct CommitConflictFactory {
    manifest: &'static titan_plugin_engine::PluginManifest,
    events: Arc<EventEngineHandle>,
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl titan_plugin_engine::PluginFactory for CommitConflictFactory {
    fn manifest(&self) -> &'static titan_plugin_engine::PluginManifest {
        self.manifest
    }

    fn create(
        &self,
        _init: titan_plugin_engine::PluginInit,
    ) -> Result<titan_plugin_engine::PluginBundle, PluginError> {
        Ok(titan_plugin_engine::PluginBundle {
            lifecycle: Box::new(CommitConflictPlugin {
                events: self.events.clone(),
                log: self.log.clone(),
            }),
            service_exports: vec![titan_plugin_engine::ServiceExport {
                service_key: titan_plugin_engine::ServiceKey {
                    id: titan_plugin_engine::ServiceId::new("integration", "rollback"),
                    version: semver::Version::new(1, 0, 0),
                    scope: titan_plugin_engine::ServiceScope::Global,
                },
                endpoint: Arc::new(IntegrationEndpoint),
            }],
            subscription_bindings: vec![],
        })
    }
}

#[test]
fn real_route_commit_failure_rolls_back_started_plugins_and_endpoints() {
    use titan_plugin_engine::{
        ApiVersion, CallMode, ConfigSnapshot, EngineState, ExecutionModel, ExecutionSpec,
        PluginEngine, PluginManifest, PluginSpec, ProvidedService, ReloadPolicy, ScopeKind,
        ServiceId, ServiceKey, ServiceScope, SubscriptionLimits,
    };

    let events = Arc::new(EventEngine::new(test_config()).unwrap());
    let event_handle = Arc::new(events.handle());
    events.start().unwrap();
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let manifest = Box::leak(Box::new(PluginManifest {
        plugin_type: Arc::from("commit-conflict"),
        name: Arc::from("commit-conflict"),
        version: semver::Version::new(1, 0, 0),
        engine_api_version: titan_plugin_engine::CORE_RUNTIME_API_VERSION,
        abi_version: ApiVersion::new(1, 0),
        config_schema: Arc::new(serde_json::json!({})),
        provides: vec![ProvidedService {
            id: ServiceId::new("integration", "rollback"),
            version: semver::Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        }],
        requires: vec![],
        publishes: vec![],
        subscribes: vec![],
        supported_execution_models: std::collections::BTreeSet::from([ExecutionModel::Passive]),
        reload_policy: ReloadPolicy::RestartRequired,
    }));
    let mut plugins = PluginEngine::new(event_handle.clone(), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(CommitConflictFactory {
                manifest,
                events: event_handle,
                log: log.clone(),
            }),
            semver::Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    let error = plugins
        .apply(&[PluginSpec {
            instance_id: Arc::from("conflict-1"),
            plugin_type: Arc::from("commit-conflict"),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 1,
                allowed_qos: std::collections::BTreeSet::new(),
            },
            service_scopes: vec![],
            required_service_scopes: vec![],
        }])
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::SubscriptionRejected);
    assert_eq!(plugins.state(), EngineState::Failed);
    assert_eq!(
        &*log.lock().unwrap(),
        &["validate", "start", "quiesce", "stop"]
    );
    let key = ServiceKey {
        id: ServiceId::new("integration", "rollback"),
        version: semver::Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    };
    assert_eq!(
        plugins
            .services()
            .bind(&key)
            .unwrap()
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap_err()
            .kind,
        ErrorKind::ServiceUnavailable
    );
    events.stop().unwrap();
}
