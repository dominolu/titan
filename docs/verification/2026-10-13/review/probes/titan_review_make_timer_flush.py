from pathlib import Path
root=Path('/home/dominolu/dev/titan-review-20261013')
s=(root/'strategies/v13_smoke/strategy.py').read_text()
s=s.replace('subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),','subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),\n                   EventSubscription(EventKind.TIMER, "on_timer", 1, EventQos.BEST_EFFORT)),')
s=s.replace('Capability.MARKET_DATA | Capability.ORDER_EXECUTION,','Capability.MARKET_DATA | Capability.ORDER_EXECUTION | Capability.TIMER,')
s=s.replace('@njit\ndef on_recover', '@njit\ndef on_timer(ctx):\n    ctx.state["nested"]["value"] = ctx.submit_order(0, 0, 1, 1, 1, 100, 1)\n\n\n@njit\ndef on_recover')
s=s.replace('{"on_tick": on_tick, "on_recover": on_recover}', '{"on_tick": on_tick, "on_recover": on_recover, "on_timer": on_timer}')
(root/'strategies/v13_timer_review.py').write_text(s)
s=(root/'crates/titan-strategy-runtime/examples/v13_common_recovery_smoke.rs').read_text()
s=s.replace('AccountEventPayload', '').replace('self as account, ', 'self as account').replace('ComponentIdentity, EventView, ResourceScope', 'ComponentIdentity, ResourceScope')
s=s.replace('struct Clock;','''struct Broker;
impl account::DirectExecutionConnector for Broker {
    fn submit(&self, _: account::DirectNewOrderRequest) -> account::ExecutionFuture { panic!("intent flush failure must prevent mock dispatch") }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture { unreachable!() }
}
struct Observer;
impl account::ExecutionObserver for Observer { fn observe(&self, _: account::ObservedExecutionResult) {} }
struct Clock;''')
s=s.replace('    let activation = Arc::new(StrategyActivationGate::default());', '''    let activation = Arc::new(StrategyActivationGate::default());
    let executor = Box::leak(Box::new(account::ExecutionRuntime::new(1, 16).unwrap()));
    let execution = executor.bind(account::AccountHandle { account_id: account::AccountId(8), generation: 1 }, Arc::new(Broker), Arc::new(Observer));''')
s=s.replace('execution: Arc::from([]),', 'execution: Arc::from([StrategyExecutionBinding { local_account_no: 0, assets: BTreeMap::from([(0, 1)]), handle: execution }]),')
s=s.replace('let sink = Arc::new(Sink::default());', 'let sink = Arc::new(Sink { fail_flush: true, ..Default::default() });')
s=s.replace('let (original, _) = create(', 'let (original, activation) = create(')
marker='    assert_eq!(\n        wait(\n            &original,\n            original\n                .freeze_state'
start=s.index(marker)
s=s[:start]+'''    original.fire_timer(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while sink.snapshots.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!sink.snapshots.lock().unwrap().is_empty(), "timer must register an intent snapshot");
    std::thread::sleep(Duration::from_millis(20));
    println!("After timer command-intent flush failure: lifecycle={:?}, gate_open={}", original.state().lifecycle, activation.is_open());
    assert_eq!(original.state().lifecycle, StrategyLifecycle::Failed, "timer command-intent flush failure must fail closed");
    assert!(!activation.is_open());
}
'''
(root/'crates/titan-strategy-runtime/examples/v13_review_timer_flush.rs').write_text(s)
p=root/'crates/titan-event-engine/src/engine/publication_tests.rs';p.write_text(p.read_text().replace('PoolKind::AccountOrder','PoolKind::SmallEvent'))
