
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
            let _ = self.handle.try_publish(nested);
            Ok(())
        }
    }
    let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    let handle = engine.handle();
    handle.register_event("review-cross", 1, EventClass::Critical, PoolKind::SmallEvent).unwrap();
    engine.shared.running.store(true, Ordering::Release);
    engine.shared.accepting_publications.store(true, Ordering::Release);
    handle.register_fast_lane("review-cross", 1, vec![], Arc::new(CrossPublish {
        handle: handle.clone(), barrier: Arc::new(std::sync::Barrier::new(2)),
    })).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for source in [1, 2] {
        let publisher = handle.clone(); let tx = tx.clone();
        std::thread::spawn(move || {
            let mut request = PublishRequest::new("review-cross", 1, b"outer");
            request.source_id = source; request.source_sequence = 1;
            let result = publisher.try_publish(request);
            let _ = tx.send(result);
        });
    }
    // Isolated negative probe only: skip Drop's shutdown wait on intentionally blocked workers.
    std::mem::forget(engine);
    let completed = (0..2).filter(|_| rx.recv_timeout(std::time::Duration::from_millis(300)).is_ok()).count();
    println!("Concurrent cross-source inline publications completed: {completed}/2");
    assert_eq!(completed, 2, "cross-source lock inversion deadlocks both publisher threads");
}
