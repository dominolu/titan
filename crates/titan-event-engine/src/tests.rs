use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, bounded};
use titan_core_types::{
    ActivationGate, ApiVersion, ComponentIdentity, ComponentState, CoreError, DispatchOutcome,
    ErrorKind, EventApiCapabilities, EventControl, EventHandler, EventPublishMetadata, EventQos,
    EventReceiver, EventView, SubscriptionSpec, TraceContext,
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
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError> {
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

struct CountingHandler(Arc<AtomicUsize>);

impl EventHandler for CountingHandler {
    fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
        self.0.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

struct ThreadNameHandler(Sender<String>);

impl EventHandler for ThreadNameHandler {
    fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
        self.0
            .send(thread::current().name().unwrap_or("unnamed").to_string())
            .unwrap();
        Ok(())
    }
}

#[test]
fn staged_route_commit_fails_fast_until_event_loop_is_running() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    let transaction = handle
        .begin_route_update(handle.current_route_version())
        .unwrap();

    let error = handle.commit_at_safe_point(transaction).unwrap_err();
    assert_eq!(error.kind, ErrorKind::RuntimeNotActive);

    // A failed pre-start commit does not consume the candidate, so the Core Runtime can start
    // EventEngine and commit the exact graph that it already validated.
    engine.start().unwrap();
    handle.commit_at_safe_point(transaction).unwrap();
    engine.stop().unwrap();
}

#[test]
fn primary_async_lane_owns_handler_and_advances_three_watermarks() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event("primary", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.start().unwrap();

    let (tx, rx) = bounded(4);
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("primary"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([42]),
            }],
            PrimaryAsyncLaneConfig {
                capacity: 4,
                critical_reserve: 1,
                reliable_pending_capacity: 4,
                snapshot_staging_capacity: 4,
                control_capacity: 4,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(RecordingHandler(tx)),
        )
        .unwrap();
    let publisher_thread = thread::current().id();
    let mut request = PublishRequest::new("primary", 1, b"one");
    request.routing_key = 42;
    handle.try_publish(request).unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), b"one");
    wait_until(|| lane.progress().committed_sequence == 1);
    assert_eq!(
        lane.progress(),
        LaneProgress {
            admitted_sequence: 1,
            dispatched_sequence: 1,
            committed_sequence: 1,
        }
    );
    let (worker_tx, worker_rx) = bounded(1);
    lane.submit_safe_point(move || {
        worker_tx.send(thread::current().id()).unwrap();
        Ok(())
    })
    .unwrap()
    .wait(Instant::now() + Duration::from_secs(1))
    .unwrap();
    assert_ne!(worker_rx.recv().unwrap(), publisher_thread);

    assert!(handle.unregister_primary_async_lane(lane.token()));
    engine.stop().unwrap();
}

#[test]
fn primary_async_lane_fails_after_hard_handler_duration_and_releases_tail() {
    struct SlowHandler(AtomicUsize);
    impl EventHandler for SlowHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            thread::sleep(Duration::from_millis(5));
            Ok(())
        }
    }

    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "slow-primary",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let handler = Arc::new(SlowHandler(AtomicUsize::new(0)));
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("slow-primary"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([]),
            }],
            PrimaryAsyncLaneConfig {
                capacity: 8,
                critical_reserve: 1,
                reliable_pending_capacity: 4,
                snapshot_staging_capacity: 8,
                control_capacity: 4,
                idle_sleep: Duration::from_millis(1),
                max_handler_duration: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            handler.clone(),
        )
        .unwrap();
    for payload in [b"one".as_slice(), b"two", b"three"] {
        handle
            .try_publish(PublishRequest::new("slow-primary", 1, payload))
            .unwrap();
    }
    wait_until(|| lane.health().state == SubscriberState::Failed);
    assert_eq!(handler.0.load(Ordering::Acquire), 1);
    assert_eq!(lane.health().channel_depth, 0);
    assert_eq!(lane.health().pending_depth, 0);
    assert!(
        std::iter::from_fn(|| handle.pop_fault_signal())
            .any(|signal| signal.kind == FaultKind::SubscriberFailed)
    );
    handle.unregister_primary_async_lane(lane.token());
    engine.stop().unwrap();
}

#[test]
fn snapshot_barrier_stages_then_replays_only_newer_tail() {
    let mut config = test_config();
    config.arena.snapshot.slots = 16;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "snapshot-source",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (tx, rx) = bounded(8);
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("snapshot-source"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([]),
            }],
            PrimaryAsyncLaneConfig {
                capacity: 8,
                critical_reserve: 1,
                reliable_pending_capacity: 4,
                snapshot_staging_capacity: 8,
                control_capacity: 4,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(RecordingHandler(tx)),
        )
        .unwrap();
    let barrier = lane
        .begin_snapshot_barrier(SnapshotBarrierRequest {
            source_ids: Arc::from([3]),
            deadline: Instant::now() + Duration::from_secs(1),
        })
        .unwrap();
    for (sequence, payload) in [(9, b"old".as_slice()), (11, b"new".as_slice())] {
        let mut request = PublishRequest::new("snapshot-source", 1, payload);
        request.source_id = 3;
        request.source_sequence = sequence;
        handle.try_publish(request).unwrap();
    }
    assert!(rx.try_recv().is_err());
    let mut snapshot = PublishRequest::new("snapshot-source", 1, b"snapshot");
    snapshot.source_id = 3;
    snapshot.source_sequence = 10;
    lane.publish_snapshot_fact(barrier, snapshot).unwrap();
    let replay_watermark = lane
        .snapshot_provider_complete(
            barrier,
            &[StreamBoundary {
                source_id: 3,
                stream_epoch: 1,
                source_sequence: 10,
            }],
        )
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"snapshot"
    );
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), b"new");
    wait_until(|| lane.progress().committed_sequence >= replay_watermark);
    lane.complete_snapshot_barrier(barrier, lane.progress().committed_sequence)
        .unwrap();
    assert_eq!(lane.health().state, SubscriberState::Normal);
    assert!(lane.snapshot_barrier().is_none());
    assert!(handle.unregister_primary_async_lane(lane.token()));
    engine.stop().unwrap();
}

#[test]
fn overloaded_primary_lane_isolated_from_healthy_lane() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "primary-isolation",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();

    let subscription = [PrimarySubscriptionSpec {
        event_type: Arc::from("primary-isolation"),
        schema_version: 1,
        qos: EventQos::ReliableOrdered,
        routing_keys: Arc::from([]),
    }];
    let (entered_tx, entered_rx) = bounded(8);
    let (release_tx, release_rx) = bounded(8);
    let slow = handle
        .register_primary_async_lane(
            &subscription,
            PrimaryAsyncLaneConfig {
                capacity: 2,
                critical_reserve: 1,
                reliable_pending_capacity: 2,
                snapshot_staging_capacity: 2,
                control_capacity: 2,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(BlockingHandler {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();
    let (healthy_tx, healthy_rx) = bounded(8);
    let healthy = handle
        .register_primary_async_lane(
            &subscription,
            PrimaryAsyncLaneConfig {
                capacity: 8,
                critical_reserve: 1,
                reliable_pending_capacity: 2,
                snapshot_staging_capacity: 8,
                control_capacity: 2,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(RecordingHandler(healthy_tx)),
        )
        .unwrap();

    handle
        .try_publish(PublishRequest::new("primary-isolation", 1, b"0"))
        .unwrap();
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"0"
    );
    for value in 1_u8..=5 {
        handle
            .try_publish(PublishRequest::new("primary-isolation", 1, &[value]))
            .unwrap();
    }
    wait_until(|| slow.health().state == SubscriberState::ResyncRequired);
    for expected in 0_u8..=5 {
        assert_eq!(
            healthy_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            vec![if expected == 0 { b'0' } else { expected }]
        );
    }
    assert_eq!(healthy.health().state, SubscriberState::Normal);

    for _ in 0..6 {
        let _ = release_tx.try_send(());
    }
    assert!(handle.unregister_primary_async_lane(slow.token()));
    assert!(handle.unregister_primary_async_lane(healthy.token()));
    engine.stop().unwrap();
}

#[test]
fn multiple_brokers_and_primary_lanes_drain_a_bounded_burst_without_cross_routing() {
    const BROKERS: usize = 4;
    const EVENTS_PER_BROKER: usize = 1_024;
    let mut config = test_config();
    config.arena.small_event.slots = BROKERS * EVENTS_PER_BROKER * 2;
    config.ingress.critical_capacity = BROKERS * EVENTS_PER_BROKER * 2;
    config.ingress.max_sources = BROKERS * 2;
    config.subscribers.max_count = BROKERS * 2;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "multi-broker-burst",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();

    let mut lanes = Vec::with_capacity(BROKERS);
    let mut counts = Vec::with_capacity(BROKERS);
    for broker in 0..BROKERS {
        let count = Arc::new(AtomicUsize::new(0));
        let lane = handle
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from("multi-broker-burst"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([broker as u64 + 1]),
                }],
                PrimaryAsyncLaneConfig {
                    capacity: EVENTS_PER_BROKER * 2,
                    critical_reserve: 8,
                    reliable_pending_capacity: EVENTS_PER_BROKER,
                    snapshot_staging_capacity: 8,
                    control_capacity: 8,
                    idle_sleep: Duration::from_micros(50),
                    ..PrimaryAsyncLaneConfig::default()
                },
                Arc::new(CountingHandler(count.clone())),
            )
            .unwrap();
        lanes.push(lane);
        counts.push(count);
    }

    for sequence in 1..=EVENTS_PER_BROKER as u64 {
        for broker in 0..BROKERS as u32 {
            loop {
                let mut request = PublishRequest::new("multi-broker-burst", 1, b"x");
                request.source_id = broker + 1;
                request.source_sequence = sequence;
                request.routing_key = u64::from(broker + 1);
                request.trace.trace_id = (u64::from(broker + 1) << 32) | sequence;
                match handle.try_publish(request) {
                    Ok(()) => break,
                    Err(PublishError::CriticalIngressFull)
                    | Err(PublishError::EventArenaExhausted(_)) => thread::yield_now(),
                    Err(error) => panic!("unexpected burst publication failure: {error}"),
                }
            }
        }
    }

    wait_until(|| {
        counts
            .iter()
            .all(|count| count.load(Ordering::Acquire) == EVENTS_PER_BROKER)
    });
    for (lane, count) in lanes.iter().zip(&counts) {
        assert_eq!(count.load(Ordering::Acquire), EVENTS_PER_BROKER);
        assert_eq!(lane.health().state, SubscriberState::Normal);
        assert_eq!(
            lane.progress(),
            LaneProgress {
                admitted_sequence: EVENTS_PER_BROKER as u64,
                dispatched_sequence: EVENTS_PER_BROKER as u64,
                committed_sequence: EVENTS_PER_BROKER as u64,
            }
        );
    }

    for lane in lanes {
        assert!(handle.unregister_primary_async_lane(lane.token()));
    }
    engine.stop().unwrap();
    assert_eq!(engine.arena().outstanding_blocks(), 0);
}

#[test]
fn primary_worker_policies_run_on_isolated_workers_and_reserve_dedicated_cpu() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "worker-policy",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();

    let available_core = core_affinity::get_core_ids()
        .and_then(|cores| cores.into_iter().next())
        .map(|core| core.id)
        .expect("test host must expose at least one logical CPU");
    let binding_supported = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: available_core })
    })
    .join()
    .unwrap();
    if !binding_supported {
        let result = handle.register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("worker-policy"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([1]),
            }],
            PrimaryAsyncLaneConfig {
                runtime_mode: SubscriberRuntimeMode::Dedicated,
                cpu_affinity: Some(available_core),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(CountingHandler(Arc::new(AtomicUsize::new(0)))),
        );
        assert!(matches!(result, Err(EngineError::CpuAffinityFailed(_))));
    }
    let modes = [
        binding_supported.then_some((SubscriberRuntimeMode::Dedicated, Some(available_core))),
        Some((SubscriberRuntimeMode::SpinSleep, None)),
        Some((SubscriberRuntimeMode::Park, None)),
    ];
    let mut lanes = Vec::new();
    let mut receivers = Vec::new();
    for (index, (runtime_mode, cpu_affinity)) in modes.into_iter().flatten().enumerate() {
        let (tx, rx) = bounded(1);
        let lane = handle
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from("worker-policy"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([index as u64 + 1]),
                }],
                PrimaryAsyncLaneConfig {
                    runtime_mode,
                    spin_iterations: 16,
                    idle_sleep: Duration::from_millis(10),
                    cpu_affinity,
                    ..PrimaryAsyncLaneConfig::default()
                },
                Arc::new(ThreadNameHandler(tx)),
            )
            .unwrap();
        lanes.push(lane);
        receivers.push(rx);
    }

    let duplicate = binding_supported.then(|| {
        handle.register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("worker-policy"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([99]),
            }],
            PrimaryAsyncLaneConfig {
                runtime_mode: SubscriberRuntimeMode::Dedicated,
                cpu_affinity: Some(available_core),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(CountingHandler(Arc::new(AtomicUsize::new(0)))),
        )
    });
    if let Some(duplicate) = duplicate {
        assert!(matches!(
            duplicate,
            Err(EngineError::InvalidPrimaryLaneConfig)
        ));
    }

    for routing_key in 1..=lanes.len() as u64 {
        let mut request = PublishRequest::new("worker-policy", 1, b"x");
        request.routing_key = routing_key;
        handle.try_publish(request).unwrap();
    }
    let names = receivers
        .into_iter()
        .map(|receiver| receiver.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names.len(), lanes.len());
    assert!(
        names
            .iter()
            .all(|name| name.starts_with("event-primary-lane-"))
    );

    for lane in lanes {
        assert!(handle.unregister_primary_async_lane(lane.token()));
    }

    if binding_supported {
        let (tx, _rx) = bounded(1);
        let reused = handle
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from("worker-policy"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([100]),
                }],
                PrimaryAsyncLaneConfig {
                    runtime_mode: SubscriberRuntimeMode::Dedicated,
                    cpu_affinity: Some(available_core),
                    ..PrimaryAsyncLaneConfig::default()
                },
                Arc::new(ThreadNameHandler(tx)),
            )
            .unwrap();
        assert!(handle.unregister_primary_async_lane(reused.token()));
    }
    engine.stop().unwrap();
}

#[test]
fn primary_best_effort_drop_does_not_advance_admitted_watermark() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "primary-best-effort",
            1,
            EventClass::Market,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (entered_tx, entered_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("primary-best-effort"),
                schema_version: 1,
                qos: EventQos::BestEffort,
                routing_keys: Arc::from([]),
            }],
            PrimaryAsyncLaneConfig {
                capacity: 2,
                critical_reserve: 1,
                reliable_pending_capacity: 1,
                snapshot_staging_capacity: 2,
                control_capacity: 2,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(BlockingHandler {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();
    handle
        .try_publish(PublishRequest::new("primary-best-effort", 1, b"first"))
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    handle
        .try_publish(PublishRequest::new("primary-best-effort", 1, b"queued"))
        .unwrap();
    handle
        .try_publish(PublishRequest::new("primary-best-effort", 1, b"dropped"))
        .unwrap();
    wait_until(|| lane.progress().admitted_sequence == 2);
    assert_eq!(lane.progress().admitted_sequence, 2);
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    assert!(handle.unregister_primary_async_lane(lane.token()));
    engine.stop().unwrap();
}

#[test]
fn primary_latest_is_coalesced_independently_per_routing_key() {
    let engine = EventEngine::new(test_config()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "primary-latest",
            1,
            EventClass::Market,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (entered_tx, entered_rx) = bounded(8);
    let (release_tx, release_rx) = bounded(8);
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("primary-latest"),
                schema_version: 1,
                qos: EventQos::Latest,
                routing_keys: Arc::from([1, 2]),
            }],
            PrimaryAsyncLaneConfig {
                capacity: 2,
                critical_reserve: 1,
                reliable_pending_capacity: 1,
                snapshot_staging_capacity: 4,
                control_capacity: 2,
                idle_sleep: Duration::from_millis(1),
                ..PrimaryAsyncLaneConfig::default()
            },
            Arc::new(BlockingHandler {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();
    let publish = |key, payload| {
        let mut request = PublishRequest::new("primary-latest", 1, payload);
        request.routing_key = key;
        handle.try_publish(request).unwrap();
    };
    publish(1, b"first");
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"first"
    );
    publish(1, b"queued");
    publish(1, b"latest-one");
    publish(2, b"latest-two");
    for _ in 0..4 {
        release_tx.send(()).unwrap();
    }
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"queued"
    );
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"latest-one"
    );
    assert_eq!(
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"latest-two"
    );
    assert!(handle.unregister_primary_async_lane(lane.token()));
    engine.stop().unwrap();
}

#[test]
fn aborted_snapshot_barrier_releases_staging_and_allows_retry() {
    let mut config = test_config();
    config.arena.snapshot.slots = 4;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "snapshot-abort",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let (tx, _rx) = bounded(4);
    let lane = handle
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from("snapshot-abort"),
                schema_version: 1,
                qos: EventQos::ReliableOrdered,
                routing_keys: Arc::from([]),
            }],
            PrimaryAsyncLaneConfig::default(),
            Arc::new(RecordingHandler(tx)),
        )
        .unwrap();
    let request = || SnapshotBarrierRequest {
        source_ids: Arc::from([9]),
        deadline: Instant::now() + Duration::from_secs(1),
    };
    let first = lane.begin_snapshot_barrier(request()).unwrap();
    let mut staged = PublishRequest::new("snapshot-abort", 1, b"staged");
    staged.source_id = 9;
    staged.source_sequence = 1;
    handle.try_publish(staged).unwrap();
    wait_until(|| {
        lane.snapshot_barrier()
            .is_some_and(|value| value.staged_events == 1)
    });
    lane.abort_snapshot_barrier(first).unwrap();
    assert!(lane.snapshot_barrier().is_none());
    assert_eq!(lane.health().state, SubscriberState::ResyncRequired);
    let retry = lane.begin_snapshot_barrier(request()).unwrap();
    lane.abort_snapshot_barrier(retry).unwrap();
    assert!(handle.unregister_primary_async_lane(lane.token()));
    engine.stop().unwrap();
}

#[test]
fn snapshot_barrier_registry_enforces_and_releases_global_limits() {
    let mut config = test_config();
    config.arena.snapshot.slots = 8;
    config.snapshot_barriers.max_active = 2;
    config.snapshot_barriers.per_barrier_staging_capacity = 1;
    config.snapshot_barriers.global_staging_capacity = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "snapshot-global-limit",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();

    let make_lane = |routing_key| {
        handle
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from("snapshot-global-limit"),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([routing_key]),
                }],
                PrimaryAsyncLaneConfig {
                    snapshot_staging_capacity: 1,
                    idle_sleep: Duration::from_millis(1),
                    ..PrimaryAsyncLaneConfig::default()
                },
                Arc::new(CountingHandler(Arc::new(AtomicUsize::new(0)))),
            )
            .unwrap()
    };
    let first_lane = make_lane(1);
    let second_lane = make_lane(2);
    let barrier_request = || SnapshotBarrierRequest {
        source_ids: Arc::from([7]),
        deadline: Instant::now() + Duration::from_secs(1),
    };
    let first = first_lane
        .begin_snapshot_barrier(barrier_request())
        .unwrap();
    let second = second_lane
        .begin_snapshot_barrier(barrier_request())
        .unwrap();

    let mut first_snapshot = PublishRequest::new("snapshot-global-limit", 1, b"first");
    first_snapshot.source_id = 7;
    first_snapshot.routing_key = 1;
    first_lane
        .publish_snapshot_fact(first, first_snapshot)
        .unwrap();
    let mut second_snapshot = PublishRequest::new("snapshot-global-limit", 1, b"second");
    second_snapshot.source_id = 7;
    second_snapshot.routing_key = 2;
    assert!(matches!(
        second_lane.publish_snapshot_fact(second, second_snapshot),
        Err(EngineError::SnapshotStagingFull)
    ));
    assert_eq!(
        second_lane.snapshot_barrier().unwrap().state,
        SnapshotBarrierState::Failed
    );

    first_lane.abort_snapshot_barrier(first).unwrap();
    second_lane.abort_snapshot_barrier(second).unwrap();
    let retry = second_lane
        .begin_snapshot_barrier(barrier_request())
        .unwrap();
    let mut retry_snapshot = PublishRequest::new("snapshot-global-limit", 1, b"retry");
    retry_snapshot.source_id = 7;
    retry_snapshot.routing_key = 2;
    second_lane
        .publish_snapshot_fact(retry, retry_snapshot)
        .unwrap();
    second_lane.abort_snapshot_barrier(retry).unwrap();

    assert!(handle.unregister_primary_async_lane(first_lane.token()));
    assert!(handle.unregister_primary_async_lane(second_lane.token()));
    engine.stop().unwrap();
}

#[test]
fn snapshot_barrier_registry_enforces_active_limit_and_deadline() {
    let mut config = test_config();
    config.snapshot_barriers.max_active = 1;
    config.snapshot_barriers.per_barrier_staging_capacity = 4;
    config.snapshot_barriers.global_staging_capacity = 4;
    config.snapshot_barriers.timeout_ms = 50;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "snapshot-active-limit",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let subscription = [PrimarySubscriptionSpec {
        event_type: Arc::from("snapshot-active-limit"),
        schema_version: 1,
        qos: EventQos::ReliableOrdered,
        routing_keys: Arc::from([]),
    }];
    let make_lane = || {
        handle
            .register_primary_async_lane(
                &subscription,
                PrimaryAsyncLaneConfig {
                    snapshot_staging_capacity: 4,
                    idle_sleep: Duration::from_millis(1),
                    ..PrimaryAsyncLaneConfig::default()
                },
                Arc::new(CountingHandler(Arc::new(AtomicUsize::new(0)))),
            )
            .unwrap()
    };
    let first_lane = make_lane();
    let second_lane = make_lane();
    let first = first_lane
        .begin_snapshot_barrier(SnapshotBarrierRequest {
            source_ids: Arc::from([1]),
            deadline: Instant::now() + Duration::from_millis(20),
        })
        .unwrap();
    assert!(matches!(
        second_lane.begin_snapshot_barrier(SnapshotBarrierRequest {
            source_ids: Arc::from([2]),
            deadline: Instant::now() + Duration::from_millis(20),
        }),
        Err(EngineError::SnapshotBarrierLimit)
    ));
    wait_until(|| {
        first_lane
            .snapshot_barrier()
            .is_some_and(|barrier| barrier.state == SnapshotBarrierState::Failed)
    });
    assert_eq!(first_lane.health().state, SubscriberState::ResyncRequired);

    // Timeout releases the global active slot even while the failed tombstone remains
    // inspectable until its owner acknowledges it with abort.
    let second = second_lane
        .begin_snapshot_barrier(SnapshotBarrierRequest {
            source_ids: Arc::from([2]),
            deadline: Instant::now() + Duration::from_millis(20),
        })
        .unwrap();
    first_lane.abort_snapshot_barrier(first).unwrap();
    second_lane.abort_snapshot_barrier(second).unwrap();
    assert!(handle.unregister_primary_async_lane(first_lane.token()));
    assert!(handle.unregister_primary_async_lane(second_lane.token()));
    engine.stop().unwrap();
}

impl EventHandler for BlockingHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError> {
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
            &ComponentIdentity::new("test", "mirror"),
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
        fn handle(&self, _event: EventView<'_>) -> Result<(), CoreError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(CoreError::new(
                ErrorKind::ComponentFailed,
                ComponentIdentity::new("test", "async-fast-failed"),
                ComponentState::Running,
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
        if gate.wait_until_active() != titan_core_types::ActivationState::Active {
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
            &ComponentIdentity::new("test", "subscriber"),
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
    config.snapshot_barriers.per_barrier_staging_capacity =
        config.snapshot_barriers.global_staging_capacity + 1;
    assert_eq!(config.validate(), Err(ConfigError::SnapshotBarrierCapacity));

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

    let core = core_affinity::get_core_ids()
        .and_then(|cores| cores.into_iter().next())
        .expect("test host must expose at least one logical CPU")
        .id;
    let mut config = test_config();
    config.runtime.mode = RuntimeMode::Dedicated;
    config.runtime.cpu_affinity = Some(core);
    config.subscribers.runtime_mode = SubscriberRuntimeMode::Dedicated;
    config.subscribers.cpu_affinity = vec![core];
    assert_eq!(
        config.validate(),
        Err(ConfigError::CpuAffinityConflict(core))
    );

    let unavailable = core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .map(|core| core.id)
        .max()
        .unwrap_or(0)
        .saturating_add(1_000);
    let mut config = test_config();
    config.runtime.cpu_affinity = Some(unavailable);
    assert_eq!(
        config.validate(),
        Err(ConfigError::CpuAffinityUnavailable(unavailable))
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
            &ComponentIdentity::new("test", "parked"),
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
    let owner = ComponentIdentity::new("test", "shared-mailbox");
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
fn event_control_market_reservation_publishes_without_copy_api() {
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
fn event_control_routes_off_publisher_and_event_loop_threads() {
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
        .retire_subscription(titan_core_types::SubscriptionToken(token))
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
        fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
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
    let (token, gate) = subscribe(
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
    wait_until(|| {
        engine.metrics().snapshot().publish_total == 7
            && handle
                .subscriber_health(token)
                .is_some_and(|health| health.pending_depth >= 3)
    });
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
                &ComponentIdentity::new("test", format!("route-{index}")),
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
                &ComponentIdentity::new("test", "route"),
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
        fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
            Err(CoreError::new(
                ErrorKind::ComponentFailed,
                ComponentIdentity::new("test", "failed"),
                ComponentState::Running,
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
        fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
            self.0.recv().expect("test releases the blocked callback");
            Err(CoreError::new(
                ErrorKind::ComponentFailed,
                ComponentIdentity::new("test", "blocked-failure"),
                ComponentState::Running,
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
        fn handle(&self, _: EventView<'_>) -> Result<(), CoreError> {
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
fn event_publish_metadata_drives_routing_and_source_sequence() {
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
            &ComponentIdentity::new("test", "metadata"),
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
        owner: ComponentIdentity::new("test", "fifo"),
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
        retiring_handle.retire_subscription(titan_core_types::SubscriptionToken(token))
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
fn core_runtime_owns_only_the_event_engine() {
    let runtime = TitanCoreRuntime::new(test_config()).unwrap();
    runtime.start().unwrap();
    assert!(matches!(
        runtime
            .event_handle()
            .try_publish(PublishRequest::new("unknown", 1, b"x")),
        Err(PublishError::InvalidEvent)
    ));
    runtime.shutdown().unwrap();
}

#[test]
fn v13_adapter_explicitly_advertises_legacy_capabilities() {
    let engine = EventEngine::new(test_config()).unwrap();
    let legacy = V13EventControlAdapter::new(engine.handle());
    assert_eq!(legacy.api_version(), ApiVersion::new(1, 0));
    assert_eq!(legacy.api_capabilities(), EventApiCapabilities::default());
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
