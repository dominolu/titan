//! Allocation audit and repeatable payload lifecycle microbenchmark; no network.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use titan_event_engine::{ArenaConfig, EngineMetrics, EventArena, PoolKind};
struct Allocator;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn main() {
    let mut config = ArenaConfig::default();
    for pool in [
        &mut config.small_event,
        &mut config.market_batch,
        &mut config.snapshot,
    ] {
        pool.slots = 8;
        pool.low_watermark = 1;
    }
    let arena = EventArena::new(&config, Arc::new(EngineMetrics::default()));
    for length in [532, 4852, 48052, 96052, 1_572_844] {
        if arena.reserve(PoolKind::MarketBatch, length).is_err() {
            println!("length={length} unsupported");
            continue;
        }
        let lifecycle = || {
            let mut reservation = arena.reserve(PoolKind::MarketBatch, length).unwrap();
            reservation.payload_mut().fill(37);
            let event = reservation.commit();
            let clone = event.clone();
            assert_eq!(event.payload().as_ptr(), clone.payload().as_ptr());
            black_box(clone.payload());
            drop(event);
            drop(clone);
        };
        for _ in 0..1000 {
            lifecycle();
        }
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        lifecycle();
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        let rounds = if length > 65536 { 1000 } else { 100_000 };
        let mut samples = [0u128; 9];
        for sample in &mut samples {
            let started = Instant::now();
            for _ in 0..rounds {
                lifecycle();
            }
            *sample = started.elapsed().as_nanos() / rounds;
        }
        samples.sort_unstable();
        if length <= config.market_batch.block_bytes {
            assert_eq!(allocations, 0);
        }
        assert_eq!(arena.outstanding_blocks(), 0);
        println!(
            "length={length} allocations={allocations} median_ns={} min_ns={} max_ns={}",
            samples[4], samples[0], samples[8]
        );
    }
}
