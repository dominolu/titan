use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use titan_plugin_engine::ResourceScope;

use crate::{
    AccountConnector, AccountConnectorFactory, AccountDefinition, AccountError, AccountErrorKind,
    AccountHandle, AccountId, AccountInstanceSnapshot, AccountLifecycle,
};

#[derive(Default)]
pub struct AccountConnectorFactoryRegistry {
    values: RwLock<HashMap<Arc<str>, Arc<dyn AccountConnectorFactory>>>,
}

impl AccountConnectorFactoryRegistry {
    pub fn register(&self, factory: Arc<dyn AccountConnectorFactory>) -> Result<(), AccountError> {
        let connector_type: Arc<str> = Arc::from(factory.connector_type());
        if connector_type.trim().is_empty() {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "empty connector type",
            ));
        }
        let mut values = self.values.write().unwrap_or_else(|p| p.into_inner());
        if values.contains_key(&connector_type) {
            return Err(AccountError::new(
                AccountErrorKind::AlreadyExists,
                "connector factory already registered",
            ));
        }
        values.insert(connector_type, factory);
        Ok(())
    }

    pub fn get(
        &self,
        connector_type: &str,
    ) -> Result<Arc<dyn AccountConnectorFactory>, AccountError> {
        self.values
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(connector_type)
            .cloned()
            .ok_or_else(|| {
                AccountError::new(
                    AccountErrorKind::FactoryNotFound,
                    "account connector factory is not registered",
                )
            })
    }

    pub fn len(&self) -> usize {
        self.values.read().unwrap_or_else(|p| p.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(crate) struct AccountEntry {
    pub handle: AccountHandle,
    pub definition: AccountDefinition,
    pub connector: Arc<dyn AccountConnector>,
    pub resources: Mutex<Option<ResourceScope>>,
    pub publisher_admission: Arc<AtomicBool>,
    pub secret_active: Arc<AtomicBool>,
    lifecycle: AtomicU8,
}

impl AccountEntry {
    pub(crate) fn new(
        handle: AccountHandle,
        definition: AccountDefinition,
        connector: Arc<dyn AccountConnector>,
        resources: ResourceScope,
        publisher_admission: Arc<AtomicBool>,
        secret_active: Arc<AtomicBool>,
    ) -> Self {
        Self {
            handle,
            definition,
            connector,
            resources: Mutex::new(Some(resources)),
            publisher_admission,
            secret_active,
            lifecycle: AtomicU8::new(AccountLifecycle::Created as u8),
        }
    }

    pub(crate) fn lifecycle(&self) -> AccountLifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            0 => AccountLifecycle::Created,
            1 => AccountLifecycle::Starting,
            2 => AccountLifecycle::Connecting,
            3 => AccountLifecycle::Reconciling,
            4 => AccountLifecycle::Ready,
            5 => AccountLifecycle::Degraded,
            6 => AccountLifecycle::Invalidated,
            7 => AccountLifecycle::Stopping,
            8 => AccountLifecycle::Stopped,
            _ => AccountLifecycle::Failed,
        }
    }
    pub(crate) fn set_lifecycle(&self, value: AccountLifecycle) {
        self.lifecycle.store(value as u8, Ordering::Release);
    }
    pub(crate) fn close_publication(&self) {
        self.publisher_admission.store(false, Ordering::Release);
    }
    pub(crate) fn close_resources(&self) -> Result<(), AccountError> {
        self.close_publication();
        self.secret_active.store(false, Ordering::Release);
        let Some(mut resources) = self
            .resources
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        else {
            return Ok(());
        };
        resources.close().map_err(|errors| {
            AccountError::new(
                AccountErrorKind::ResourceReleaseFailed,
                errors
                    .into_iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })
    }
    pub(crate) fn snapshot(&self) -> AccountInstanceSnapshot {
        AccountInstanceSnapshot {
            handle: self.handle,
            account_key: self.definition.account_key.clone(),
            connector_type: self.definition.connector_type.clone(),
            definition_version: self.definition.definition_version,
            enabled: self.definition.enabled,
            lifecycle: self.lifecycle(),
        }
    }
}

#[derive(Default)]
struct AccountRegistryState {
    by_id: HashMap<AccountId, Arc<AccountEntry>>,
    by_key: HashMap<Arc<str>, AccountHandle>,
    last_generation_by_key: HashMap<Arc<str>, u64>,
    last_generation_by_id: HashMap<AccountId, u64>,
}

#[derive(Default)]
pub struct AccountRegistry {
    state: RwLock<AccountRegistryState>,
}

impl AccountRegistry {
    pub(crate) fn next_generation(&self, account_key: &str, account_id: AccountId) -> u64 {
        let s = self.state.read().unwrap_or_else(|p| p.into_inner());
        s.last_generation_by_key
            .get(account_key)
            .copied()
            .unwrap_or(0)
            .max(
                s.last_generation_by_id
                    .get(&account_id)
                    .copied()
                    .unwrap_or(0),
            )
            .saturating_add(1)
    }

    pub(crate) fn validate_insert(
        &self,
        d: &AccountDefinition,
        max_accounts: usize,
        max_instruments: usize,
        max_currencies: usize,
        replacing: Option<AccountId>,
    ) -> Result<(), AccountError> {
        if d.account_id.0 == 0
            || d.account_key.trim().is_empty()
            || d.connector_type.trim().is_empty()
            || d.credential_ref.as_str().trim().is_empty()
            || d.instruments.is_empty()
            || d.currencies.is_empty()
            || d.definition_version == 0
        {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "account id, key, connector type, credential ref, bindings and a non-zero definition version are required",
            ));
        }
        if d.instruments.len() > max_instruments || d.currencies.len() > max_currencies {
            return Err(AccountError::new(
                AccountErrorKind::CapacityExceeded,
                "account binding capacity exceeded",
            ));
        }
        let mut symbols = HashSet::new();
        let mut assets = HashSet::new();
        for b in d.instruments.iter() {
            if b.native_symbol.trim().is_empty()
                || !symbols.insert(b.native_symbol.as_ref())
                || !assets.insert(b.asset_id)
            {
                return Err(AccountError::new(
                    AccountErrorKind::InvalidDefinition,
                    "instrument native symbols and asset ids must be non-empty and unique",
                ));
            }
        }
        let mut native = HashSet::new();
        let mut currencies = HashSet::new();
        for b in d.currencies.iter() {
            if b.native_currency.trim().is_empty()
                || !native.insert(b.native_currency.as_ref())
                || !currencies.insert(b.currency_id)
            {
                return Err(AccountError::new(
                    AccountErrorKind::InvalidDefinition,
                    "currency native names and ids must be non-empty and unique",
                ));
            }
        }
        if let crate::OrderOwnershipPolicy::ManagedOnly { client_id_prefix } = &d.ownership {
            if client_id_prefix.is_empty() {
                return Err(AccountError::new(
                    AccountErrorKind::InvalidDefinition,
                    "managed ownership requires a client id prefix",
                ));
            }
        }
        let s = self.state.read().unwrap_or_else(|p| p.into_inner());
        if replacing.is_none() && s.by_id.len() >= max_accounts {
            return Err(AccountError::new(
                AccountErrorKind::CapacityExceeded,
                "account capacity exceeded",
            ));
        }
        if s.by_id.contains_key(&d.account_id) && Some(d.account_id) != replacing {
            return Err(AccountError::new(
                AccountErrorKind::AlreadyExists,
                "account id already exists",
            ));
        }
        if s.by_key
            .get(d.account_key.as_ref())
            .is_some_and(|h| Some(h.account_id) != replacing)
        {
            return Err(AccountError::new(
                AccountErrorKind::AlreadyExists,
                "account key already exists",
            ));
        }
        Ok(())
    }

    pub(crate) fn insert(&self, e: Arc<AccountEntry>) -> Result<(), AccountError> {
        let mut s = self.state.write().unwrap_or_else(|p| p.into_inner());
        if s.by_id.contains_key(&e.handle.account_id)
            || s.by_key.contains_key(&e.definition.account_key)
        {
            return Err(AccountError::new(
                AccountErrorKind::AlreadyExists,
                "account already exists",
            ));
        }
        s.last_generation_by_key
            .insert(e.definition.account_key.clone(), e.handle.generation);
        s.last_generation_by_id
            .insert(e.handle.account_id, e.handle.generation);
        s.by_key.insert(e.definition.account_key.clone(), e.handle);
        s.by_id.insert(e.handle.account_id, e);
        Ok(())
    }

    pub(crate) fn get(&self, h: AccountHandle) -> Result<Arc<AccountEntry>, AccountError> {
        let s = self.state.read().unwrap_or_else(|p| p.into_inner());
        let e = s.by_id.get(&h.account_id).ok_or_else(|| {
            AccountError::new(AccountErrorKind::AccountNotFound, "account not found")
        })?;
        if e.handle.generation != h.generation {
            return Err(AccountError::new(
                AccountErrorKind::StaleHandle,
                "account handle generation is stale",
            ));
        }
        Ok(e.clone())
    }
    pub(crate) fn resolve(&self, key: &str) -> Result<AccountHandle, AccountError> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_key
            .get(key)
            .copied()
            .ok_or_else(|| {
                AccountError::new(AccountErrorKind::AccountNotFound, "account not found")
            })
    }
    pub(crate) fn remove(&self, h: AccountHandle) -> Result<Arc<AccountEntry>, AccountError> {
        self.get(h)?;
        let mut s = self.state.write().unwrap_or_else(|p| p.into_inner());
        let e = s.by_id.remove(&h.account_id).ok_or_else(|| {
            AccountError::new(AccountErrorKind::AccountNotFound, "account not found")
        })?;
        s.by_key.remove(&e.definition.account_key);
        Ok(e)
    }
    pub(crate) fn swap(
        &self,
        old: AccountHandle,
        new: Arc<AccountEntry>,
    ) -> Result<Arc<AccountEntry>, AccountError> {
        self.get(old)?;
        let mut s = self.state.write().unwrap_or_else(|p| p.into_inner());
        let old_e = s.by_id.get(&old.account_id).cloned().ok_or_else(|| {
            AccountError::new(AccountErrorKind::AccountNotFound, "account not found")
        })?;
        s.by_key.remove(&old_e.definition.account_key);
        s.last_generation_by_key
            .insert(new.definition.account_key.clone(), new.handle.generation);
        s.last_generation_by_id
            .insert(new.handle.account_id, new.handle.generation);
        s.by_key
            .insert(new.definition.account_key.clone(), new.handle);
        s.by_id.insert(new.handle.account_id, new);
        Ok(old_e)
    }
    pub(crate) fn list_entries(&self) -> Vec<Arc<AccountEntry>> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .values()
            .cloned()
            .collect()
    }
    pub fn list(&self) -> Arc<[AccountInstanceSnapshot]> {
        let mut v: Vec<_> = self
            .list_entries()
            .into_iter()
            .map(|e| e.snapshot())
            .collect();
        v.sort_by_key(|x| x.handle.account_id);
        v.into()
    }
    pub fn len(&self) -> usize {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
