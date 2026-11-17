//! Local account writer ownership. External/cross-process ownership remains a node admission duty.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use titan_core_types::LockUnpoisoned;

type OwnerBook = BTreeMap<u32, BTreeMap<(u32, u64), bool>>;

#[derive(Default)]
pub(crate) struct ExecutionOwners {
    owners: Mutex<OwnerBook>,
}

/// Shared by the runtime and its resource scope. Unsafe teardown leaves a quarantine entry.
#[derive(Clone, Default)]
pub struct AccountExecutionLease(Option<Arc<LeaseInner>>);

struct LeaseInner {
    book: Arc<ExecutionOwners>,
    accounts: Vec<u32>,
    owner: (u32, u64),
    safe: AtomicBool,
    released: AtomicBool,
}

impl ExecutionOwners {
    pub(crate) fn claim(
        self: &Arc<Self>,
        accounts: &[u32],
        owner: (u32, u64),
        exclusive: bool,
    ) -> Result<AccountExecutionLease, &'static str> {
        let mut keys = accounts.to_vec();
        keys.sort_unstable();
        keys.dedup();
        if keys.len() != accounts.len() || keys.is_empty() || owner.0 == 0 || owner.1 == 0 {
            return Err("invalid_execution_ownership");
        }
        let mut all = self.owners.lock_unpoisoned();
        if keys.iter().any(|id| {
            all.get(id).is_some_and(|owners| {
                owners.contains_key(&owner)
                    || (exclusive && !owners.is_empty())
                    || owners.values().any(|v| *v)
            })
        }) {
            return Err("account_execution_owned");
        }
        for id in &keys {
            all.entry(*id).or_default().insert(owner, exclusive);
        }
        Ok(AccountExecutionLease(Some(Arc::new(LeaseInner {
            book: self.clone(),
            accounts: keys,
            owner,
            safe: AtomicBool::new(true),
            released: AtomicBool::new(false),
        }))))
    }
}

impl AccountExecutionLease {
    pub fn mark_exposed(&self) {
        if let Some(inner) = &self.0 {
            inner.safe.store(false, Ordering::Release);
        }
    }
    /// Call only after orders, independent fills and positions prove zero owned risk.
    pub fn release_zero_risk(&self) {
        if let Some(inner) = &self.0 {
            inner.safe.store(true, Ordering::Release);
            inner.release();
        }
    }
}
impl LeaseInner {
    fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut all = self.book.owners.lock_unpoisoned();
        for id in &self.accounts {
            if let Some(owners) = all.get_mut(id) {
                owners.remove(&self.owner);
                if owners.is_empty() {
                    all.remove(id);
                }
            }
        }
    }
}
impl Drop for LeaseInner {
    fn drop(&mut self) {
        if self.safe.load(Ordering::Acquire) {
            self.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dual_claim_is_atomic_and_unknown_risk_retains_ownership() {
        let book = Arc::new(ExecutionOwners::default());
        let shared = book.claim(&[1], (1, 1), false).unwrap();
        assert!(book.claim(&[2, 1], (2, 1), true).is_err());
        let other = book.claim(&[2], (3, 1), true).unwrap();
        drop(shared);
        drop(other);
        let dual = book.claim(&[1, 2], (2, 1), true).unwrap();
        dual.mark_exposed();
        assert!(book.claim(&[2], (3, 1), false).is_err());
        let witness = dual.clone();
        drop(dual);
        assert!(book.claim(&[1], (4, 1), true).is_err());
        witness.release_zero_risk();
        assert!(book.claim(&[1, 2], (4, 1), true).is_ok());
    }
    #[test]
    fn dropping_exposed_lease_does_not_free_account() {
        let book = Arc::new(ExecutionOwners::default());
        let lease = book.claim(&[1], (1, 1), true).unwrap();
        lease.mark_exposed();
        drop(lease);
        assert!(book.claim(&[1], (2, 1), true).is_err());
    }
}
