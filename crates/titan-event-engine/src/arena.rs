use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering, fence},
    },
};

use crossbeam_queue::ArrayQueue;

use crate::{ArenaConfig, EngineMetrics, PoolConfig, PoolKind, PublishError};

const SLOT_FREE: u8 = 0;
const SLOT_RESERVED: u8 = 1;
const SLOT_PUBLISHED: u8 = 2;
const SLOT_RETIRED: u8 = 3;
const MAX_REFCOUNT: usize = isize::MAX as usize;

struct BlockSlot {
    state: AtomicU8,
    generation: AtomicU32,
    refs: AtomicUsize,
    len: AtomicUsize,
    data: UnsafeCell<Box<[u8]>>,
}

// Access to data is exclusive while RESERVED and immutable while PUBLISHED. Reuse is
// synchronized by the pool free queue and the slot state Release/Acquire transition.
unsafe impl Sync for BlockSlot {}

impl BlockSlot {
    fn new(block_bytes: usize) -> Self {
        Self {
            state: AtomicU8::new(SLOT_FREE),
            generation: AtomicU32::new(1),
            refs: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
            data: UnsafeCell::new(vec![0_u8; block_bytes].into_boxed_slice()),
        }
    }
}

struct EventPool {
    kind: PoolKind,
    block_bytes: usize,
    low_watermark: usize,
    slots: Box<[BlockSlot]>,
    free: ArrayQueue<u32>,
    in_use: AtomicUsize,
    retired: AtomicUsize,
}

impl EventPool {
    fn new(kind: PoolKind, config: &PoolConfig) -> Self {
        let free = ArrayQueue::new(config.slots);
        for index in 0..config.slots {
            free.push(index as u32)
                .expect("new pool free queue has exact capacity");
        }
        let slots = (0..config.slots)
            .map(|_| BlockSlot::new(config.block_bytes))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            kind,
            block_bytes: config.block_bytes,
            low_watermark: config.low_watermark,
            slots,
            free,
            in_use: AtomicUsize::new(0),
            retired: AtomicUsize::new(0),
        }
    }

    fn slot(&self, block_id: u32) -> &BlockSlot {
        &self.slots[block_id as usize]
    }

    fn return_slot(&self, block_id: u32, slot: &BlockSlot) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        let generation = slot.generation.load(Ordering::Relaxed);
        if generation == u32::MAX {
            slot.state.store(SLOT_RETIRED, Ordering::Release);
            self.retired.fetch_add(1, Ordering::Relaxed);
            return;
        }
        slot.generation.store(generation + 1, Ordering::Relaxed);
        slot.len.store(0, Ordering::Relaxed);
        slot.state.store(SLOT_FREE, Ordering::Release);
        self.free
            .push(block_id)
            .expect("a checked-out pool slot has exactly one return");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventHandle {
    pub pool: PoolKind,
    pub block_id: u32,
    pub generation: u32,
    pub length: u32,
}

pub struct EventArena {
    pools: [EventPool; 3],
    metrics: Arc<EngineMetrics>,
}

impl EventArena {
    pub fn new(config: &ArenaConfig, metrics: Arc<EngineMetrics>) -> Arc<Self> {
        Arc::new(Self {
            pools: [
                EventPool::new(PoolKind::SmallEvent, &config.small_event),
                EventPool::new(PoolKind::MarketBatch, &config.market_batch),
                EventPool::new(PoolKind::Snapshot, &config.snapshot),
            ],
            metrics,
        })
    }

    fn pool(&self, kind: PoolKind) -> &EventPool {
        &self.pools[kind.index()]
    }

    pub fn reserve(
        self: &Arc<Self>,
        pool_kind: PoolKind,
        length: usize,
    ) -> Result<EventReservation, PublishError> {
        let pool = self.pool(pool_kind);
        if length > pool.block_bytes || length > u32::MAX as usize {
            return Err(PublishError::PayloadTooLarge {
                pool: pool_kind,
                length,
                capacity: pool.block_bytes,
            });
        }
        let Some(block_id) = pool.free.pop() else {
            self.metrics.arena_exhausted[pool_kind.index()].fetch_add(1, Ordering::Relaxed);
            return Err(PublishError::EventArenaExhausted(pool_kind));
        };
        let slot = pool.slot(block_id);
        slot.state
            .compare_exchange(
                SLOT_FREE,
                SLOT_RESERVED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .expect("free queue only contains FREE slots");
        slot.refs.store(1, Ordering::Relaxed);
        slot.len.store(0, Ordering::Relaxed);
        pool.in_use.fetch_add(1, Ordering::Relaxed);
        if pool.free.len() <= pool.low_watermark {
            self.metrics.arena_pressure[pool_kind.index()].fetch_add(1, Ordering::Relaxed);
        }
        Ok(EventReservation {
            arena: self.clone(),
            handle: EventHandle {
                pool: pool_kind,
                block_id,
                generation: slot.generation.load(Ordering::Relaxed),
                length: length as u32,
            },
            active: true,
        })
    }

    fn retain(&self, handle: EventHandle) {
        let slot = self.pool(handle.pool).slot(handle.block_id);
        assert_eq!(
            slot.generation.load(Ordering::Acquire),
            handle.generation,
            "attempted to retain a stale EventHandle"
        );
        assert_eq!(
            slot.state.load(Ordering::Acquire),
            SLOT_PUBLISHED,
            "attempted to retain an unpublished EventHandle"
        );
        let previous = slot.refs.fetch_add(1, Ordering::Relaxed);
        assert!(
            previous > 0 && previous < MAX_REFCOUNT,
            "EventBlock refcount overflow or resurrection"
        );
    }

    fn release(&self, handle: EventHandle) {
        let pool = self.pool(handle.pool);
        let slot = pool.slot(handle.block_id);
        assert_eq!(
            slot.generation.load(Ordering::Acquire),
            handle.generation,
            "stale EventHandle release"
        );
        let previous = slot.refs.fetch_sub(1, Ordering::Release);
        assert!(previous > 0, "EventBlock refcount underflow");
        if previous == 1 {
            fence(Ordering::Acquire);
            pool.return_slot(handle.block_id, slot);
        }
    }

    fn abort_reservation(&self, handle: EventHandle) {
        let pool = self.pool(handle.pool);
        let slot = pool.slot(handle.block_id);
        slot.state
            .compare_exchange(
                SLOT_RESERVED,
                SLOT_FREE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("only an active reservation can be aborted");
        slot.refs.store(0, Ordering::Relaxed);
        pool.return_slot(handle.block_id, slot);
    }

    fn payload(&self, handle: EventHandle) -> &[u8] {
        let pool = self.pool(handle.pool);
        let slot = pool.slot(handle.block_id);
        assert_eq!(
            slot.generation.load(Ordering::Acquire),
            handle.generation,
            "stale EventHandle read"
        );
        assert_eq!(
            slot.state.load(Ordering::Acquire),
            SLOT_PUBLISHED,
            "unpublished EventHandle read"
        );
        let len = slot.len.load(Ordering::Relaxed);
        // SAFETY: PUBLISHED data is immutable. The caller owns a reference and prevents reuse.
        let data = unsafe { &*slot.data.get() };
        &data[..len]
    }

    pub fn snapshot(&self) -> ArenaSnapshot {
        let pools = PoolKind::ALL.map(|kind| {
            let pool = self.pool(kind);
            PoolSnapshot {
                kind: pool.kind,
                capacity: pool.slots.len(),
                free: pool.free.len(),
                in_use: pool.in_use.load(Ordering::Relaxed),
                retired: pool.retired.load(Ordering::Relaxed),
                block_bytes: pool.block_bytes,
            }
        });
        ArenaSnapshot { pools }
    }

    pub fn outstanding_blocks(&self) -> usize {
        self.pools
            .iter()
            .map(|pool| pool.in_use.load(Ordering::Acquire))
            .sum()
    }
}

pub struct EventReservation {
    arena: Arc<EventArena>,
    handle: EventHandle,
    active: bool,
}

impl EventReservation {
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let slot = self.arena.pool(self.handle.pool).slot(self.handle.block_id);
        assert_eq!(slot.state.load(Ordering::Acquire), SLOT_RESERVED);
        // SAFETY: reservation is unique and payload_mut requires exclusive access to it.
        let data = unsafe { &mut *slot.data.get() };
        &mut data[..self.handle.length as usize]
    }

    pub fn commit(mut self) -> OwnedEvent {
        let slot = self.arena.pool(self.handle.pool).slot(self.handle.block_id);
        slot.len
            .store(self.handle.length as usize, Ordering::Relaxed);
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);
        self.active = false;
        OwnedEvent {
            arena: self.arena.clone(),
            handle: self.handle,
        }
    }
}

impl Drop for EventReservation {
    fn drop(&mut self) {
        if self.active {
            self.arena.abort_reservation(self.handle);
        }
    }
}

pub struct OwnedEvent {
    arena: Arc<EventArena>,
    handle: EventHandle,
}

impl OwnedEvent {
    pub fn handle(&self) -> EventHandle {
        self.handle
    }

    pub fn payload(&self) -> &[u8] {
        self.arena.payload(self.handle)
    }
}

impl Clone for OwnedEvent {
    fn clone(&self) -> Self {
        self.arena.retain(self.handle);
        Self {
            arena: self.arena.clone(),
            handle: self.handle,
        }
    }
}

impl Drop for OwnedEvent {
    fn drop(&mut self) {
        self.arena.release(self.handle);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolSnapshot {
    pub kind: PoolKind,
    pub capacity: usize,
    pub free: usize,
    pub in_use: usize,
    pub retired: usize,
    pub block_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArenaSnapshot {
    pub pools: [PoolSnapshot; 3],
}
