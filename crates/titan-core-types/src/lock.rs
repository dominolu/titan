//! Lock helpers shared by the core services.
//!
//! A poisoned mutex means some thread panicked while holding the guard. Every Titan service treats
//! that as a recoverable state: the data is still structurally valid, and refusing to look at it
//! would turn one panic into a permanently wedged service. `lock_unpoisoned` states that policy
//! once instead of repeating `unwrap_or_else(|p| p.into_inner())` at every call site.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Acquires the guard, ignoring poisoning.
pub trait LockUnpoisoned<T: ?Sized> {
    /// Returns the guard, recovering the inner value when the mutex is poisoned.
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> LockUnpoisoned<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Acquires a shared read guard, ignoring poisoning.
pub trait ReadUnpoisoned<T: ?Sized> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
}

impl<T: ?Sized> ReadUnpoisoned<T> for RwLock<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Acquires an exclusive write guard, ignoring poisoning.
pub trait WriteUnpoisoned<T: ?Sized> {
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> WriteUnpoisoned<T> for RwLock<T> {
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn a_poisoned_mutex_still_yields_the_guard_and_keeps_the_value() {
        let shared = Arc::new(Mutex::new(7_u32));
        let poisoner = shared.clone();
        let _ = thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the mutex on purpose");
        })
        .join();
        assert!(shared.is_poisoned());
        assert_eq!(*shared.lock_unpoisoned(), 7);
    }

    #[test]
    fn a_poisoned_rwlock_still_yields_both_guards() {
        let shared = Arc::new(RwLock::new(3_u32));
        let poisoner = shared.clone();
        let _ = thread::spawn(move || {
            let _guard = poisoner.write().unwrap();
            panic!("poison the rwlock on purpose");
        })
        .join();
        assert!(shared.is_poisoned());
        assert_eq!(*shared.read_unpoisoned(), 3);
        *shared.write_unpoisoned() = 4;
        assert_eq!(*shared.read_unpoisoned(), 4);
    }
}
