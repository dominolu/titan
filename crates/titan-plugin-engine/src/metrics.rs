use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{ErrorKind, LifecycleState, PluginIdentity, ServiceKey};

fn state_value(state: LifecycleState) -> u64 {
    state as u64
}

#[derive(Debug, Default)]
pub(crate) struct PluginMetricSeries {
    state: AtomicU64,
    start_success: AtomicU64,
    start_failure: AtomicU64,
    stop_success: AtomicU64,
    stop_failure: AtomicU64,
    restart_total: AtomicU64,
    failure_total: AtomicU64,
    callback_total: AtomicU64,
    callback_duration_ns: AtomicU64,
    callback_duration_ns_max: AtomicU64,
    callback_budget_exceeded_total: AtomicU64,
    callback_stalled_total: AtomicU64,
    resource_release_failure_total: AtomicU64,
}

impl PluginMetricSeries {
    pub(crate) fn state(&self, state: LifecycleState) {
        self.state.store(state_value(state), Ordering::Release);
    }
    pub(crate) fn start(&self, success: bool) {
        if success {
            &self.start_success
        } else {
            &self.start_failure
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn stop(&self, success: bool) {
        if success {
            &self.stop_success
        } else {
            &self.stop_failure
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn failure(&self) {
        self.failure_total.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn callback(&self, duration_ns: u64, budget_exceeded: bool) {
        self.callback_total.fetch_add(1, Ordering::Relaxed);
        self.callback_duration_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
        self.callback_duration_ns_max
            .fetch_max(duration_ns, Ordering::Relaxed);
        if budget_exceeded {
            self.callback_budget_exceeded_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn stalled(&self) {
        self.callback_stalled_total.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn restart(&self) {
        self.restart_total.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn resource_release_failure(&self) {
        self.resource_release_failure_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, identity: PluginIdentity) -> PluginMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        PluginMetricsSnapshot {
            identity,
            lifecycle_state: load(&self.state),
            start_success_total: load(&self.start_success),
            start_failure_total: load(&self.start_failure),
            stop_success_total: load(&self.stop_success),
            stop_failure_total: load(&self.stop_failure),
            restart_total: load(&self.restart_total),
            failure_total: load(&self.failure_total),
            callback_total: load(&self.callback_total),
            callback_duration_ns_total: load(&self.callback_duration_ns),
            callback_duration_ns_max: load(&self.callback_duration_ns_max),
            callback_budget_exceeded_total: load(&self.callback_budget_exceeded_total),
            callback_stalled_total: load(&self.callback_stalled_total),
            resource_release_failure_total: load(&self.resource_release_failure_total),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ServiceMetricSeries {
    call_success: AtomicU64,
    call_failure: AtomicU64,
    unavailable: AtomicU64,
    duration_ns: AtomicU64,
    duration_ns_max: AtomicU64,
}

impl ServiceMetricSeries {
    pub(crate) fn call(&self, duration_ns: u64, success: bool) {
        if success {
            &self.call_success
        } else {
            &self.call_failure
        }
        .fetch_add(1, Ordering::Relaxed);
        self.duration_ns.fetch_add(duration_ns, Ordering::Relaxed);
        self.duration_ns_max
            .fetch_max(duration_ns, Ordering::Relaxed);
    }
    pub(crate) fn unavailable(&self) {
        self.unavailable.fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(
        &self,
        key: ServiceKey,
        provider: PluginIdentity,
        consumer: PluginIdentity,
    ) -> ServiceMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ServiceMetricsSnapshot {
            key,
            provider,
            consumer,
            call_success_total: load(&self.call_success),
            call_failure_total: load(&self.call_failure),
            unavailable_total: load(&self.unavailable),
            call_duration_ns_total: load(&self.duration_ns),
            call_duration_ns_max: load(&self.duration_ns_max),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMetricsSnapshot {
    pub identity: PluginIdentity,
    pub lifecycle_state: u64,
    pub start_success_total: u64,
    pub start_failure_total: u64,
    pub stop_success_total: u64,
    pub stop_failure_total: u64,
    pub restart_total: u64,
    pub failure_total: u64,
    pub callback_total: u64,
    pub callback_duration_ns_total: u64,
    pub callback_duration_ns_max: u64,
    pub callback_budget_exceeded_total: u64,
    pub callback_stalled_total: u64,
    pub resource_release_failure_total: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMetricsSnapshot {
    pub key: ServiceKey,
    pub provider: PluginIdentity,
    pub consumer: PluginIdentity,
    pub call_success_total: u64,
    pub call_failure_total: u64,
    pub unavailable_total: u64,
    pub call_duration_ns_total: u64,
    pub call_duration_ns_max: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginFailureMetricsSnapshot {
    pub identity: PluginIdentity,
    pub reason: ErrorKind,
    pub total: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceReleaseFailureMetricsSnapshot {
    pub identity: PluginIdentity,
    pub resource_type: Arc<str>,
    pub total: u64,
}

#[derive(Default)]
pub struct PluginEngineMetrics {
    plugins: Mutex<BTreeMap<PluginIdentity, Arc<PluginMetricSeries>>>,
    services:
        Mutex<BTreeMap<(ServiceKey, PluginIdentity, PluginIdentity), Arc<ServiceMetricSeries>>>,
    failures: Mutex<BTreeMap<(PluginIdentity, ErrorKind), u64>>,
    resource_release_failures: Mutex<BTreeMap<(PluginIdentity, Arc<str>), u64>>,
}

impl PluginEngineMetrics {
    pub(crate) fn plugin(&self, identity: &PluginIdentity) -> Arc<PluginMetricSeries> {
        self.plugins
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(identity.clone())
            .or_default()
            .clone()
    }
    pub(crate) fn service(
        &self,
        key: &ServiceKey,
        provider: &PluginIdentity,
        consumer: &PluginIdentity,
    ) -> Arc<ServiceMetricSeries> {
        self.services
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry((key.clone(), provider.clone(), consumer.clone()))
            .or_default()
            .clone()
    }
    pub fn plugin_snapshots(&self) -> Vec<PluginMetricsSnapshot> {
        self.plugins
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(identity, series)| series.snapshot(identity.clone()))
            .collect()
    }
    pub fn service_snapshots(&self) -> Vec<ServiceMetricsSnapshot> {
        self.services
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|((key, provider, consumer), series)| {
                series.snapshot(key.clone(), provider.clone(), consumer.clone())
            })
            .collect()
    }
    pub(crate) fn record_failure(&self, identity: &PluginIdentity, reason: ErrorKind) {
        *self
            .failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry((identity.clone(), reason))
            .or_default() += 1;
    }
    pub(crate) fn record_resource_release_failure(
        &self,
        identity: &PluginIdentity,
        resource_type: Arc<str>,
    ) {
        *self
            .resource_release_failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry((identity.clone(), resource_type))
            .or_default() += 1;
    }
    pub fn failure_snapshots(&self) -> Vec<PluginFailureMetricsSnapshot> {
        self.failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|((identity, reason), total)| PluginFailureMetricsSnapshot {
                identity: identity.clone(),
                reason: *reason,
                total: *total,
            })
            .collect()
    }
    pub fn resource_release_failure_snapshots(&self) -> Vec<ResourceReleaseFailureMetricsSnapshot> {
        self.resource_release_failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(
                |((identity, resource_type), total)| ResourceReleaseFailureMetricsSnapshot {
                    identity: identity.clone(),
                    resource_type: resource_type.clone(),
                    total: *total,
                },
            )
            .collect()
    }
}
