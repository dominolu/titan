use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Condvar, Mutex};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ActivationState {
    Prepared = 0,
    Active = 1,
    Quiescing = 2,
    Stopped = 3,
}

/// One-shot activation barrier shared by endpoint, publisher, and subscriber runtime.
pub struct ActivationGate {
    state: AtomicU8,
    lock: Mutex<()>,
    changed: Condvar,
    async_changed: watch::Sender<ActivationState>,
}

impl Default for ActivationGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ActivationGate {
    pub fn new() -> Self {
        let (async_changed, _) = watch::channel(ActivationState::Prepared);
        Self {
            state: AtomicU8::new(ActivationState::Prepared as u8),
            lock: Mutex::new(()),
            changed: Condvar::new(),
            async_changed,
        }
    }

    pub fn state(&self) -> ActivationState {
        match self.state.load(Ordering::Acquire) {
            0 => ActivationState::Prepared,
            1 => ActivationState::Active,
            2 => ActivationState::Quiescing,
            _ => ActivationState::Stopped,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == ActivationState::Active as u8
    }

    pub fn activate(&self) -> bool {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .state
            .compare_exchange(
                ActivationState::Prepared as u8,
                ActivationState::Active as u8,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.changed.notify_all();
            self.async_changed.send_replace(ActivationState::Active);
            true
        } else {
            false
        }
    }

    pub fn quiesce(&self) -> bool {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .state
            .compare_exchange(
                ActivationState::Active as u8,
                ActivationState::Quiescing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.changed.notify_all();
            self.async_changed.send_replace(ActivationState::Quiescing);
            true
        } else {
            false
        }
    }

    pub fn stop(&self) {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.state
            .store(ActivationState::Stopped as u8, Ordering::Release);
        self.changed.notify_all();
        self.async_changed.send_replace(ActivationState::Stopped);
    }

    pub fn wait_until_active(&self) -> ActivationState {
        let mut guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            let state = self.state();
            if state != ActivationState::Prepared {
                return state;
            }
            guard = self
                .changed
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub async fn wait_until_active_async(&self) -> ActivationState {
        let mut changed = self.async_changed.subscribe();
        loop {
            let state = self.state();
            if state != ActivationState::Prepared {
                return state;
            }
            if changed.changed().await.is_err() {
                return self.state();
            }
        }
    }
}
