use crate::{CoreError, Resource};
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

/// Completed operations have finite retention; pending operations remain queryable.
pub const OPERATION_HISTORY_LIMIT: usize = 1024;
pub struct OperationHistory<K, V> {
    values: HashMap<K, V>,
    order: VecDeque<K>,
    pending: fn(&V) -> bool,
    limit: usize,
}
impl<K: Eq + Hash + Clone, V> OperationHistory<K, V> {
    pub fn new(pending: fn(&V) -> bool) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            pending,
            limit: OPERATION_HISTORY_LIMIT,
        }
    }
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        while self
            .values
            .values()
            .filter(|value| !(self.pending)(value))
            .count()
            >= self.limit
        {
            let Some(index) = self.order.iter().position(|key| {
                self.values
                    .get(key)
                    .is_some_and(|value| !(self.pending)(value))
            }) else {
                break;
            };
            if let Some(expired) = self.order.remove(index) {
                self.values.remove(&expired);
            }
        }
        if !self.values.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.values.insert(key, value)
    }
}
impl<K, V> Deref for OperationHistory<K, V> {
    type Target = HashMap<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl<K, V> DerefMut for OperationHistory<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

#[derive(Default)]
pub struct BindingLease(Option<Box<dyn FnOnce() + Send>>);
impl Resource for BindingLease {
    fn close(&mut self) -> Result<(), CoreError> {
        if let Some(release) = self.0.take() {
            release();
        }
        Ok(())
    }
}
impl Drop for BindingLease {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub struct BindingPins<K>(Arc<Mutex<HashMap<K, usize>>>);
impl<K> Default for BindingPins<K> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
}
impl<K: Eq + Hash + Copy + Send + 'static> BindingPins<K> {
    pub fn is_pinned(&self, key: K) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&key)
    }
    pub fn pin(&self, key: K) -> BindingLease {
        *self
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key)
            .or_default() += 1;
        let pins = self.0.clone();
        BindingLease(Some(Box::new(move || {
            let mut pins = pins.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(count) = pins.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    pins.remove(&key);
                }
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn history_retains_pending_and_binding_leases_release_once() {
        let mut history = OperationHistory::new(|value: &bool| *value);
        history.insert(0, true);
        for id in 1..3000 {
            history.insert(id, false);
        }
        assert_eq!(history.len(), 1025);
        assert_eq!(history.get(&0), Some(&true));
        let pins = BindingPins::default();
        let mut lease = pins.pin(7);
        let second = pins.pin(7);
        lease.close().unwrap();
        lease.close().unwrap();
        assert!(pins.is_pinned(7));
        drop(second);
        assert!(!pins.is_pinned(7));
    }
}
