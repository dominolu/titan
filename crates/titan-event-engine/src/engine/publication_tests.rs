use super::*;

struct Counter(Arc<AtomicUsize>);

impl EventHandler for Counter {
    fn handle(&self, _: EventView<'_>) -> Result<(), titan_core_types::CoreError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// Exercise publication with ingress intentionally undrained, independent of worker timing.
fn publisher() -> (EventEngine, EventEngineHandle, Arc<AtomicUsize>) {
    let mut config = EventEngineConfig::default();
    config.ingress.critical_capacity = 1;
    let engine = EventEngine::new(config).unwrap();
    let handle = engine.handle();
    handle
        .register_event("admission", 1, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    engine.shared.running.store(true, Ordering::Release);
    engine
        .shared
        .accepting_publications
        .store(true, Ordering::Release);
    let counter = Arc::new(AtomicUsize::new(0));
    handle
        .register_fast_lane("admission", 1, vec![], Arc::new(Counter(counter.clone())))
        .unwrap();
    (engine, handle, counter)
}

#[test]
fn rejected_copied_and_reserved_events_are_never_delivered() {
    let (_engine, handle, counter) = publisher();
    handle
        .try_publish(PublishRequest::new("admission", 1, b"accepted"))
        .unwrap();
    assert_eq!(
        handle.try_publish(PublishRequest::new("admission", 1, b"rejected")),
        Err(PublishError::CriticalIngressFull)
    );
    let mut reservation = handle
        .reserve_event_payload(ReserveRequest::new("admission", 1, 1))
        .unwrap();
    reservation.payload_mut()[0] = 42;
    assert_eq!(reservation.commit(), Err(PublishError::CriticalIngressFull));
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    assert_eq!(handle.shared.arena.outstanding_blocks(), 1);
}

#[test]
fn duplicate_sequences_are_filtered_before_direct_delivery_and_rejection_is_retryable() {
    let (_engine, handle, counter) = publisher();
    let mut request = PublishRequest::new("admission", 1, b"first");
    request.source_id = 1;
    request.source_sequence = 7;
    handle.try_publish(request).unwrap();
    handle.try_publish(request).unwrap();
    request.source_sequence = 8;
    assert_eq!(
        handle.try_publish(request),
        Err(PublishError::CriticalIngressFull)
    );
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    handle.shared.critical_ingress.pop();
    handle.try_publish(request).unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 2);
    assert_eq!(
        handle.shared.metrics.snapshot().source_sequence_gap_total,
        0
    );
}

#[test]
fn reservation_created_before_stop_cannot_publish_after_stop() {
    let (engine, handle, counter) = publisher();
    let reservation = handle
        .reserve_event_payload(ReserveRequest::new("admission", 1, 1))
        .unwrap();
    assert!(matches!(
        engine.stop(),
        Err(EngineError::OutstandingBlocks(1))
    ));
    assert_eq!(reservation.commit(), Err(PublishError::Stopped));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(handle.shared.arena.outstanding_blocks(), 0);
    engine.stop().unwrap();
}

#[test]
fn recursive_sequenced_source_is_rejected_instead_of_deadlocking() {
    struct Recursive(EventEngineHandle, Arc<AtomicBool>);
    impl EventHandler for Recursive {
        fn handle(&self, _: EventView<'_>) -> Result<(), titan_core_types::CoreError> {
            let mut request = PublishRequest::new("admission", 1, b"nested");
            request.source_id = 1;
            request.source_sequence = 2;
            self.1.store(
                self.0.try_publish(request) == Err(PublishError::ReentrantSource),
                Ordering::Release,
            );
            Ok(())
        }
    }
    let (_engine, handle, _) = publisher();
    let rejected = Arc::new(AtomicBool::new(false));
    handle
        .register_fast_lane(
            "admission",
            1,
            vec![],
            Arc::new(Recursive(handle.clone(), rejected.clone())),
        )
        .unwrap();
    let mut request = PublishRequest::new("admission", 1, b"outer");
    request.source_id = 1;
    request.source_sequence = 1;
    handle.try_publish(request).unwrap();
    assert!(rejected.load(Ordering::Acquire));
}

#[test]
fn review_cross_source_inline_publication_must_not_deadlock() {
    struct CrossPublish {
        handle: EventEngineHandle,
        barrier: Arc<std::sync::Barrier>,
    }
    impl EventHandler for CrossPublish {
        fn handle(&self, event: EventView<'_>) -> Result<(), titan_core_types::CoreError> {
            self.barrier.wait();
            let mut nested = PublishRequest::new("review-cross", 1, b"nested");
            nested.source_id = if event.metadata.source_id == 1 { 2 } else { 1 };
            nested.source_sequence = 2;
            assert!(matches!(
                self.handle.try_publish(nested),
                Err(PublishError::ReentrantSource)
            ));
            Ok(())
        }
    }
    let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    let handle = engine.handle();
    handle
        .register_event(
            "review-cross",
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.shared.running.store(true, Ordering::Release);
    engine
        .shared
        .accepting_publications
        .store(true, Ordering::Release);
    handle
        .register_fast_lane(
            "review-cross",
            1,
            vec![],
            Arc::new(CrossPublish {
                handle: handle.clone(),
                barrier: Arc::new(std::sync::Barrier::new(2)),
            }),
        )
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for source in [1, 2] {
        let publisher = handle.clone();
        let tx = tx.clone();
        std::thread::spawn(move || {
            let mut request = PublishRequest::new("review-cross", 1, b"outer");
            request.source_id = source;
            request.source_sequence = 1;
            let result = publisher.try_publish(request);
            let _ = tx.send(result);
        });
    }

    let completed = (0..2)
        .filter(|_| {
            rx.recv_timeout(std::time::Duration::from_millis(300))
                .is_ok()
        })
        .count();
    println!("Concurrent cross-source inline publications completed: {completed}/2");
    if completed != 2 {
        std::mem::forget(engine);
    }
    assert_eq!(
        completed, 2,
        "cross-source lock inversion deadlocks both publisher threads"
    );
}
