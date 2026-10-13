from pathlib import Path
root=Path('/home/dominolu/dev/titan-review-20261013')
p=root/'crates/titan-strategy-runtime/examples/v13_common_recovery_smoke.rs'
s=p.read_text().replace('    fail_flush: bool,','    fail_flush: bool,\n    fail_submit_once: std::sync::atomic::AtomicBool,')
s=s.replace('        self.snapshots.lock().unwrap().push(snapshot);','''        if self.fail_submit_once.swap(false, std::sync::atomic::Ordering::AcqRel) {
            return Err(StrategyError::new(StrategyErrorKind::CheckpointFailed, "review", "injected_submit", "injected submit failure"));
        }
        self.snapshots.lock().unwrap().push(snapshot);''')
s=s.replace('    let definition = StrategyDefinition {','    let mut definition = StrategyDefinition {')
s=s.replace('    let engine = EventEngine::new','    definition.runtime.checkpoint_interval = Duration::from_nanos(1);\n    let engine = EventEngine::new',1)
s=s.replace('let (original, _) = create(', 'let (original, activation) = create(')
marker='    assert_eq!(\n        wait(\n            &original,\n            original\n                .freeze_state'
start=s.index(marker)
s=s[:start]+'''    sink.fail_submit_once.store(true, std::sync::atomic::Ordering::Release);
    original.fire_timer(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while sink.fail_submit_once.load(std::sync::atomic::Ordering::Acquire) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!sink.fail_submit_once.load(std::sync::atomic::Ordering::Acquire), "timer must run");
    std::thread::sleep(Duration::from_millis(20));
    println!("After timer checkpoint failure: lifecycle={:?}, gate_open={}", original.state().lifecycle, activation.is_open());
    assert_eq!(original.state().lifecycle, StrategyLifecycle::Failed, "timer checkpoint failure must fail closed");
    assert!(!activation.is_open());
}
'''
(root/'crates/titan-strategy-runtime/examples/v13_review_timer_failure.rs').write_text(s)
