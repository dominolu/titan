use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    ActivationGate, BackgroundFlightRecorderExporter, BoundServices, ColdAsyncRuntime, ErrorKind,
    EventControl, EventHandler, EventView, ExecutionModel, ExecutionSpec, FlightRecorder,
    PluginBundle, PluginContext, PluginEngineMetrics, PluginError, PluginIdentity, PluginInit,
    PluginMetricSeries, PluginPlan, PluginRegistry, ResourceScope, RouteTransaction, ServiceKey,
    ServiceRegistry, StopReason, SubscriptionCandidate, SubscriptionExecutor, SubscriptionRuntime,
    SubscriptionSpec, ValidationContext, publication_grants, subscription_grants, trace_kind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleState {
    Discovered,
    Validated,
    Resolved,
    Starting,
    Running,
    Quiescing,
    Stopping,
    Stopped,
    Failed,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthState {
    Healthy,
    Degraded,
    Stalled,
    Failed,
}

#[derive(Clone, Debug)]
pub struct PluginDiagnostic {
    pub identity: PluginIdentity,
    pub lifecycle_state: LifecycleState,
    pub health: HealthState,
    pub config_version: u64,
    pub provided_services: Vec<ServiceKey>,
    pub required_services: Vec<ServiceKey>,
    pub subscriptions: Vec<SubscriptionSpec>,
    pub execution: ExecutionSpec,
    pub subscription_count: usize,
    pub callback_stats: Option<crate::CallbackStats>,
    pub thread_heartbeat_age: Option<std::time::Duration>,
    pub event_channel_depth: usize,
    pub subscriber_pending_depth: usize,
    pub subscriber_outstanding_handles: usize,
    pub resource_count: usize,
    pub resource_counts: BTreeMap<Arc<str>, usize>,
    pub last_error: Option<PluginError>,
    pub updated_at: SystemTime,
}

pub struct PluginSlot {
    pub identity: PluginIdentity,
    pub lifecycle_state: LifecycleState,
    pub health: HealthState,
    pub gate: Arc<ActivationGate>,
    pub resources: ResourceScope,
    pub context: PluginContext,
    pub execution: ExecutionSpec,
    pub bundle: PluginBundle,
    pub provided_services: Vec<ServiceKey>,
    pub required_services: Vec<ServiceKey>,
    subscriptions: Vec<SubscriptionRuntime>,
    pub last_error: Option<PluginError>,
    pub updated_at: SystemTime,
    metrics: Arc<PluginMetricSeries>,
}

impl PluginSlot {
    fn transition(&mut self, state: LifecycleState) {
        self.lifecycle_state = state;
        self.metrics.state(state);
        self.updated_at = SystemTime::now();
    }

    fn diagnostic(&self) -> PluginDiagnostic {
        let stalled = self
            .subscriptions
            .iter()
            .any(SubscriptionRuntime::is_stalled);
        PluginDiagnostic {
            identity: self.identity.clone(),
            lifecycle_state: if stalled {
                LifecycleState::Failed
            } else {
                self.lifecycle_state
            },
            health: if stalled {
                HealthState::Stalled
            } else {
                self.health
            },
            config_version: self.context.config.version,
            provided_services: self.provided_services.clone(),
            required_services: self.required_services.clone(),
            subscriptions: self
                .bundle
                .subscription_bindings
                .iter()
                .map(|binding| binding.spec.clone())
                .collect(),
            execution: self.execution.clone(),
            subscription_count: self.subscriptions.len(),
            callback_stats: self
                .subscriptions
                .iter()
                .find_map(SubscriptionRuntime::callback_stats),
            thread_heartbeat_age: self
                .subscriptions
                .iter()
                .map(SubscriptionRuntime::heartbeat_age)
                .max(),
            event_channel_depth: self
                .subscriptions
                .iter()
                .map(|runtime| runtime.receiver_diagnostics().channel_depth)
                .sum(),
            subscriber_pending_depth: self
                .subscriptions
                .iter()
                .map(|runtime| runtime.receiver_diagnostics().pending_depth)
                .sum(),
            subscriber_outstanding_handles: self
                .subscriptions
                .iter()
                .map(|runtime| runtime.receiver_diagnostics().outstanding_handles)
                .sum(),
            resource_count: self.resources.resource_count(),
            resource_counts: self.resources.resource_counts(),
            last_error: self.last_error.clone(),
            updated_at: self.updated_at,
        }
    }
}

pub struct PreparedRuntime {
    route_transaction: RouteTransaction,
    newly_staged_services: BTreeSet<ServiceKey>,
    staged_subscriptions: Vec<(
        Arc<str>,
        SubscriptionCandidate,
        SubscriptionSpec,
        Arc<dyn crate::EventHandler>,
    )>,
}

struct MailboxHandlers(Vec<(Arc<str>, u32, Arc<dyn EventHandler>)>);

impl EventHandler for MailboxHandlers {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        let Some((_, _, handler)) = self.0.iter().find(|(event_type, schema_version, _)| {
            event_type.as_ref() == event.event_type && *schema_version == event.schema_version
        }) else {
            return Err(crate::engine_error(
                ErrorKind::PluginFailed,
                "dispatch_shared_mailbox",
                format!(
                    "no handler for {} schema {}",
                    event.event_type, event.schema_version
                ),
            ));
        };
        handler.handle(event)
    }
}

pub struct RuntimeHost {
    slots: BTreeMap<Arc<str>, PluginSlot>,
    background_runtime: Option<Arc<ColdAsyncRuntime>>,
    recorder: Arc<FlightRecorder>,
    exporter: Option<Arc<BackgroundFlightRecorderExporter>>,
    metrics: Arc<PluginEngineMetrics>,
}

impl Default for RuntimeHost {
    fn default() -> Self {
        Self::new(
            Arc::new(FlightRecorder::new(4096)),
            Arc::new(PluginEngineMetrics::default()),
        )
    }
}

impl RuntimeHost {
    pub(crate) fn new(recorder: Arc<FlightRecorder>, metrics: Arc<PluginEngineMetrics>) -> Self {
        Self {
            slots: BTreeMap::new(),
            background_runtime: None,
            recorder,
            exporter: None,
            metrics,
        }
    }

    pub(crate) fn set_exporter(&mut self, exporter: Option<Arc<BackgroundFlightRecorderExporter>>) {
        self.exporter = exporter;
    }

    fn record_failure_to(
        recorder: &FlightRecorder,
        exporter: Option<&BackgroundFlightRecorderExporter>,
        metrics: &PluginEngineMetrics,
        error: &PluginError,
    ) {
        metrics.record_failure(&error.identity, error.kind);
        recorder.record(
            error.trace_context.unwrap_or_default(),
            trace_kind::LIFECYCLE_FAILURE,
            error.kind as u64,
            true,
        );
        if let Some(exporter) = exporter {
            let snapshot = recorder.freeze();
            let _ = exporter.try_export(snapshot);
            recorder.unfreeze();
        }
    }

    fn record_failure(&self, error: &PluginError) {
        Self::record_failure_to(
            &self.recorder,
            self.exporter.as_deref(),
            &self.metrics,
            error,
        );
    }

    pub fn prepare(
        &mut self,
        plan: &PluginPlan,
        registry: &PluginRegistry,
        services: &mut ServiceRegistry,
        events: Arc<dyn EventControl>,
    ) -> Result<PreparedRuntime, PluginError> {
        if !self.slots.is_empty() {
            return Err(crate::engine_error(
                ErrorKind::PluginFailed,
                "prepare",
                "runtime host is not empty",
            ));
        }
        let background_capacity = plan
            .entries()
            .filter(|entry| entry.spec.execution.model == ExecutionModel::Background)
            .map(|entry| {
                registry
                    .get(&entry.spec.plugin_type)
                    .map_or(0, |registered| {
                        registered.factory.manifest().subscribes.len()
                    })
            })
            .sum::<usize>();
        self.background_runtime = (background_capacity > 0)
            .then(|| ColdAsyncRuntime::new(2, background_capacity).map(Arc::new))
            .transpose()?;
        let mut newly_staged_services = BTreeSet::new();
        for entry in plan.entries() {
            for key in &entry.provides {
                let existed = services.contains(key);
                if let Err(error) = services.stage(key.clone(), entry.identity.clone()) {
                    for staged in &newly_staged_services {
                        services.remove(staged);
                    }
                    return Err(error);
                }
                if !existed {
                    newly_staged_services.insert(key.clone());
                }
            }
        }
        let transaction = events.begin_route_update(events.current_route_version())?;
        let mut staged = Vec::new();
        let result = (|| {
            for instance_id in plan.start_order() {
                let entry = plan.entry(instance_id).expect("plan order is valid");
                let registered = registry
                    .get(&entry.spec.plugin_type)
                    .expect("plan registry entry is valid");
                let manifest = registered.factory.manifest();
                let mut handles = BTreeMap::new();
                for binding in &entry.bindings {
                    if let Some(key) = &binding.key {
                        let handle =
                            services
                                .bind_for(key, entry.identity.clone())
                                .ok_or_else(|| {
                                    PluginError::new(
                                        ErrorKind::DependencyMissing,
                                        entry.identity.clone(),
                                        LifecycleState::Resolved,
                                        "prepare",
                                        format!("service {key:?} was not staged"),
                                    )
                                })?;
                        handles.insert(key.clone(), handle);
                    }
                }
                let bound = BoundServices::new(handles);
                let gate = Arc::new(ActivationGate::new());
                let resources = ResourceScope::new(entry.identity.clone());
                let resource_handle = resources.handle();
                let subscription_executor = SubscriptionExecutor {
                    model: entry.spec.execution.model,
                    cpu_affinity: entry.spec.execution.cpu_affinity,
                    background: self.background_runtime.clone(),
                    callback_budget: entry.spec.execution.callback_budget.clone(),
                    recorder: self.recorder.clone(),
                    exporter: self.exporter.clone(),
                    metrics: self.metrics.plugin(&entry.identity),
                };
                let bundle = catch_unwind(AssertUnwindSafe(|| {
                    registered.factory.create(PluginInit {
                        identity: entry.identity.clone(),
                        config: entry.spec.config.clone(),
                    })
                }))
                .map_err(|_| {
                    panic_error(
                        &entry.identity,
                        LifecycleState::Discovered,
                        "factory_create",
                    )
                })??;
                validate_bundle(entry, manifest, &bundle)?;
                let publisher = crate::EventPublisher::new(
                    entry.identity.clone(),
                    publication_grants(manifest),
                    gate.clone(),
                    events.clone(),
                );
                let router = (!manifest.subscribes.is_empty()).then(|| {
                    crate::ScopedEventRouter::new(
                        entry.identity.clone(),
                        subscription_grants(manifest),
                        entry.spec.subscription_limits.max_capacity,
                        entry.spec.subscription_limits.allowed_qos.clone(),
                        gate.clone(),
                        events.clone(),
                        resource_handle.clone(),
                        subscription_executor,
                    )
                });
                let context = PluginContext {
                    identity: entry.identity.clone(),
                    config: entry.spec.config.clone(),
                    services: bound.clone(),
                    events: publisher,
                    event_routes: router,
                    resources: resource_handle,
                };
                catch_unwind(AssertUnwindSafe(|| {
                    bundle.lifecycle.validate(&ValidationContext {
                        identity: entry.identity.clone(),
                        config: entry.spec.config.clone(),
                        services: bound,
                    })
                }))
                .map_err(|_| {
                    panic_error(&entry.identity, LifecycleState::Validated, "validate")
                })??;
                for binding in &bundle.subscription_bindings {
                    let candidate = events.stage_subscription_in_mailbox(
                        transaction,
                        &entry.identity,
                        "plugin-default",
                        &binding.spec,
                    )?;
                    staged.push((
                        entry.spec.instance_id.clone(),
                        candidate,
                        binding.spec.clone(),
                        binding.handler.clone(),
                    ));
                }
                self.slots.insert(
                    entry.spec.instance_id.clone(),
                    PluginSlot {
                        identity: entry.identity.clone(),
                        lifecycle_state: LifecycleState::Validated,
                        health: HealthState::Healthy,
                        gate,
                        resources,
                        context,
                        execution: entry.spec.execution.clone(),
                        bundle,
                        provided_services: entry.provides.clone(),
                        required_services: entry
                            .bindings
                            .iter()
                            .filter_map(|binding| binding.key.clone())
                            .collect(),
                        subscriptions: Vec::new(),
                        last_error: None,
                        updated_at: SystemTime::now(),
                        metrics: self.metrics.plugin(&entry.identity),
                    },
                );
            }
            Ok(())
        })();
        if let Err(error) = result {
            events.abort(transaction);
            self.rollback_prepared(services, &newly_staged_services);
            return Err(error);
        }
        for slot in self.slots.values_mut() {
            slot.transition(LifecycleState::Resolved);
        }
        Ok(PreparedRuntime {
            route_transaction: transaction,
            newly_staged_services,
            staged_subscriptions: staged,
        })
    }

    pub fn start_and_commit(
        &mut self,
        plan: &PluginPlan,
        prepared: PreparedRuntime,
        services: &mut ServiceRegistry,
        events: Arc<dyn EventControl>,
    ) -> Result<(), PluginError> {
        let recorder = self.recorder.clone();
        let exporter = self.exporter.clone();
        let metrics = self.metrics.clone();
        let PreparedRuntime {
            route_transaction,
            newly_staged_services,
            staged_subscriptions,
        } = prepared;
        let mut started = Vec::new();
        for instance_id in plan.start_order() {
            let slot = self
                .slots
                .get_mut(instance_id)
                .expect("prepared slot exists");
            slot.transition(LifecycleState::Starting);
            let started_result = catch_unwind(AssertUnwindSafe(|| {
                slot.bundle.lifecycle.start(&mut slot.context)
            }))
            .map_err(|_| panic_error(&slot.identity, LifecycleState::Starting, "start"))
            .and_then(|result| result);
            if let Err(error) = started_result {
                slot.metrics.start(false);
                Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                slot.last_error = Some(error.clone());
                slot.health = HealthState::Failed;
                slot.transition(LifecycleState::Failed);
                self.rollback_start(
                    &started,
                    services,
                    &events,
                    route_transaction,
                    &newly_staged_services,
                );
                return Err(error);
            }
            slot.metrics.start(true);
            for export in &slot.bundle.service_exports {
                if let Err(error) = services.publish(
                    &export.service_key,
                    export.endpoint.clone(),
                    slot.gate.clone(),
                ) {
                    Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                    self.rollback_start(
                        &started,
                        services,
                        &events,
                        route_transaction,
                        &newly_staged_services,
                    );
                    return Err(error);
                }
            }
            started.push(instance_id.clone());
        }
        let (_, committed) = match events.commit_at_safe_point(route_transaction) {
            Ok(committed) => committed,
            Err(error) => {
                Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                self.rollback_start(
                    &started,
                    services,
                    &events,
                    route_transaction,
                    &newly_staged_services,
                );
                return Err(error);
            }
        };
        if committed.len() != staged_subscriptions.len() {
            for subscription in committed {
                let _ = events.retire_subscription(subscription.token);
            }
            self.rollback_start(
                &started,
                services,
                &events,
                route_transaction,
                &newly_staged_services,
            );
            return Err(crate::engine_error(
                ErrorKind::SubscriptionRejected,
                "commit",
                "event engine returned an invalid token set",
            ));
        }
        let mut mailboxes = BTreeMap::<
            u64,
            (
                Arc<str>,
                Arc<dyn crate::EventReceiver>,
                Vec<crate::SubscriptionToken>,
                Vec<(Arc<str>, u32, Arc<dyn EventHandler>)>,
            ),
        >::new();
        for ((instance_id, _, spec, handler), subscription) in
            staged_subscriptions.into_iter().zip(committed)
        {
            let mailbox = mailboxes.entry(subscription.mailbox_id).or_insert_with(|| {
                (
                    instance_id.clone(),
                    subscription.receiver.clone(),
                    Vec::new(),
                    Vec::new(),
                )
            });
            mailbox.2.push(subscription.token);
            mailbox
                .3
                .push((spec.event_type, spec.schema_version, handler));
        }
        let committed_tokens = mailboxes
            .values()
            .flat_map(|(_, _, tokens, _)| tokens.iter().copied())
            .collect::<Vec<_>>();
        for (_, (instance_id, receiver, tokens, handlers)) in mailboxes {
            let slot = self
                .slots
                .get_mut(&instance_id)
                .expect("subscription owner exists");
            let runtime = match SubscriptionRuntime::start_group(
                slot.identity.clone(),
                slot.gate.clone(),
                receiver,
                Arc::new(MailboxHandlers(handlers)),
                events.clone(),
                tokens.clone(),
                SubscriptionExecutor {
                    model: slot.execution.model,
                    cpu_affinity: slot.execution.cpu_affinity,
                    background: self.background_runtime.clone(),
                    callback_budget: slot.execution.callback_budget.clone(),
                    recorder: self.recorder.clone(),
                    exporter: self.exporter.clone(),
                    metrics: slot.metrics.clone(),
                },
            ) {
                Ok(runtime) => runtime,
                Err(error) => {
                    Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                    for token in &committed_tokens {
                        let _ = events.retire_subscription(*token);
                    }
                    self.rollback_start(
                        &started,
                        services,
                        &events,
                        route_transaction,
                        &newly_staged_services,
                    );
                    return Err(error);
                }
            };
            slot.subscriptions.push(runtime);
        }
        for instance_id in plan.start_order() {
            let slot = self.slots.get_mut(instance_id).expect("slot exists");
            if !slot.gate.activate() {
                let error = PluginError::new(
                    ErrorKind::RuntimeStartFailed,
                    slot.identity.clone(),
                    slot.lifecycle_state,
                    "activate",
                    "activation gate was not prepared",
                );
                Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                self.rollback_start(
                    &started,
                    services,
                    &events,
                    route_transaction,
                    &newly_staged_services,
                );
                return Err(error);
            }
            slot.transition(LifecycleState::Running);
        }
        Ok(())
    }

    pub fn abort_prepared(
        &mut self,
        prepared: PreparedRuntime,
        services: &mut ServiceRegistry,
        events: &Arc<dyn EventControl>,
    ) {
        events.abort(prepared.route_transaction);
        self.rollback_prepared(services, &prepared.newly_staged_services);
    }

    pub fn quiesce_all(
        &mut self,
        plan: &PluginPlan,
        services: &ServiceRegistry,
        reason: StopReason,
    ) -> Result<(), Vec<PluginError>> {
        let mut errors = Vec::new();
        let recorder = self.recorder.clone();
        let exporter = self.exporter.clone();
        let metrics = self.metrics.clone();
        for instance_id in plan.stop_order() {
            let Some(slot) = self.slots.get_mut(instance_id) else {
                continue;
            };
            slot.transition(LifecycleState::Quiescing);
            if reason == StopReason::Restart {
                slot.metrics.restart();
            }
            for key in &slot.provided_services {
                services.make_unavailable(key);
            }
            let result = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.quiesce(reason)))
                .map_err(|_| panic_error(&slot.identity, LifecycleState::Quiescing, "quiesce"))
                .and_then(|result| result);
            if let Err(error) = result {
                slot.metrics.failure();
                Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                slot.last_error = Some(error.clone());
                slot.health = HealthState::Degraded;
                errors.push(error);
            } else {
                slot.gate.quiesce();
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn stop_all(
        &mut self,
        plan: &PluginPlan,
        services: &ServiceRegistry,
    ) -> Result<(), Vec<PluginError>> {
        let mut errors = Vec::new();
        let recorder = self.recorder.clone();
        let exporter = self.exporter.clone();
        let metrics = self.metrics.clone();
        for instance_id in plan.stop_order() {
            let Some(slot) = self.slots.get_mut(instance_id) else {
                continue;
            };
            slot.transition(LifecycleState::Stopping);
            for key in &slot.provided_services {
                services.make_unavailable(key);
            }
            slot.gate.quiesce();
            for mut subscription in slot.subscriptions.drain(..) {
                if let Err(error) = crate::Resource::close(&mut subscription) {
                    slot.metrics.resource_release_failure();
                    metrics.record_resource_release_failure(
                        &slot.identity,
                        Arc::from("dynamic-subscription"),
                    );
                    Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                    errors.push(error);
                }
            }
            let result = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.stop()))
                .map_err(|_| panic_error(&slot.identity, LifecycleState::Stopping, "stop"))
                .and_then(|result| result);
            if let Err(error) = result {
                slot.metrics.stop(false);
                Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, &error);
                errors.push(error);
            } else {
                slot.metrics.stop(true);
            }
            if let Err(mut resource_errors) = slot.resources.close() {
                for _ in &resource_errors {
                    slot.metrics.resource_release_failure();
                }
                for error in &resource_errors {
                    let resource_type = error
                        .cause_chain
                        .iter()
                        .find_map(|cause| cause.strip_prefix("resource="))
                        .map_or_else(|| Arc::from("unknown"), Arc::from);
                    metrics.record_resource_release_failure(&slot.identity, resource_type);
                    Self::record_failure_to(&recorder, exporter.as_deref(), &metrics, error);
                }
                errors.append(&mut resource_errors);
            }
            slot.gate.stop();
            slot.transition(LifecycleState::Stopped);
        }
        self.background_runtime = None;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn clear_stopped_for_plan(&mut self, services: &mut ServiceRegistry, next: &PluginPlan) {
        let retained = next
            .entries()
            .flat_map(|entry| {
                entry
                    .provides
                    .iter()
                    .cloned()
                    .map(|key| (key, entry.identity.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        for slot in self.slots.values() {
            for key in &slot.provided_services {
                if retained.get(key) != Some(&slot.identity) {
                    services.remove(key);
                }
            }
        }
        self.slots.clear();
        self.background_runtime = None;
    }

    pub fn diagnostics(&self) -> Vec<PluginDiagnostic> {
        self.slots.values().map(PluginSlot::diagnostic).collect()
    }
    pub fn slot(&self, instance_id: &str) -> Option<&PluginSlot> {
        self.slots.get(instance_id)
    }

    pub fn report_failure(
        &mut self,
        instance_id: &str,
        error: PluginError,
        services: &ServiceRegistry,
        stalled: bool,
    ) -> Result<(), PluginError> {
        self.record_failure(&error);
        let slot = self.slots.get_mut(instance_id).ok_or_else(|| {
            crate::engine_error(
                ErrorKind::PluginFailed,
                "report_failure",
                format!("unknown plugin instance {instance_id}"),
            )
        })?;
        for key in &slot.provided_services {
            services.make_unavailable(key);
        }
        slot.gate.quiesce();
        slot.last_error = Some(error);
        slot.metrics.failure();
        slot.health = if stalled {
            HealthState::Stalled
        } else {
            HealthState::Failed
        };
        slot.transition(LifecycleState::Failed);
        Ok(())
    }

    fn rollback_prepared(
        &mut self,
        services: &mut ServiceRegistry,
        newly_staged_services: &BTreeSet<ServiceKey>,
    ) {
        for (_, mut slot) in std::mem::take(&mut self.slots) {
            for key in &slot.provided_services {
                if newly_staged_services.contains(key) {
                    services.remove(key);
                } else {
                    services.make_unavailable(key);
                }
            }
            slot.gate.stop();
            let _ = slot.resources.close();
        }
        self.background_runtime = None;
    }

    fn rollback_start(
        &mut self,
        started: &[Arc<str>],
        services: &mut ServiceRegistry,
        events: &Arc<dyn EventControl>,
        transaction: RouteTransaction,
        newly_staged_services: &BTreeSet<ServiceKey>,
    ) {
        events.abort(transaction);
        let started_set: BTreeSet<_> = started.iter().cloned().collect();
        for id in started.iter().rev() {
            if let Some(slot) = self.slots.get_mut(id) {
                for key in &slot.provided_services {
                    services.make_unavailable(key);
                }
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    slot.bundle.lifecycle.quiesce(StopReason::Failure)
                }));
                let _ = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.stop()));
                slot.gate.stop();
                for mut subscription in slot.subscriptions.drain(..) {
                    let _ = crate::Resource::close(&mut subscription);
                }
                let _ = slot.resources.close();
                slot.transition(LifecycleState::Stopped);
            }
        }
        for (id, slot) in &mut self.slots {
            for key in &slot.provided_services {
                services.make_unavailable(key);
            }
            if !started_set.contains(id) {
                if matches!(
                    slot.lifecycle_state,
                    LifecycleState::Starting | LifecycleState::Failed
                ) {
                    let _ = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.stop()));
                }
                slot.gate.stop();
                for mut subscription in slot.subscriptions.drain(..) {
                    let _ = crate::Resource::close(&mut subscription);
                }
                let _ = slot.resources.close();
                if slot.lifecycle_state != LifecycleState::Failed {
                    slot.transition(LifecycleState::Stopped);
                }
            }
        }
        for key in newly_staged_services {
            services.remove(key);
        }
        self.background_runtime = None;
    }
}

fn panic_error(identity: &PluginIdentity, state: LifecycleState, operation: &str) -> PluginError {
    PluginError::new(
        ErrorKind::PluginFailed,
        identity.clone(),
        state,
        operation,
        "plugin panicked; panic was contained at the host boundary",
    )
}

fn validate_bundle(
    entry: &crate::PluginPlanEntry,
    manifest: &crate::PluginManifest,
    bundle: &PluginBundle,
) -> Result<(), PluginError> {
    let expected: BTreeSet<_> = entry.provides.iter().cloned().collect();
    let actual: BTreeSet<_> = bundle
        .service_exports
        .iter()
        .map(|export| export.service_key.clone())
        .collect();
    if expected != actual || actual.len() != bundle.service_exports.len() {
        return Err(PluginError::new(
            ErrorKind::ManifestInvalid,
            entry.identity.clone(),
            LifecycleState::Discovered,
            "validate_bundle",
            "service exports do not exactly match the compiled plan",
        ));
    }
    let grants = subscription_grants(manifest);
    for binding in &bundle.subscription_bindings {
        if binding.spec.capacity == 0
            || binding.spec.capacity > entry.spec.subscription_limits.max_capacity
            || !entry
                .spec
                .subscription_limits
                .allowed_qos
                .contains(&binding.spec.qos)
            || !grants
                .get(&(binding.spec.event_type.clone(), binding.spec.schema_version))
                .is_some_and(|qos| qos.contains(&binding.spec.qos))
        {
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                entry.identity.clone(),
                LifecycleState::Discovered,
                "validate_bundle",
                "subscription exceeds manifest authorization",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod rollback_tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, Mutex},
    };

    use semver::Version;

    use super::*;
    use crate::{
        ApiVersion, BoxValue, CORE_RUNTIME_API_VERSION, CallMode, CommittedSubscription,
        ConfigSnapshot, EventApiCapabilities, EventQos, ExecutionModel, ExecutionSpec, Plugin,
        PluginFactory, PluginInit, PluginManifest, PluginSpec, ProvidedService, ReloadPolicy,
        RouteVersion, ScopeKind, ServiceEndpoint, ServiceId, ServiceScope, SubscriptionLimits,
        SubscriptionToken, TraceContext,
    };

    #[derive(Default)]
    struct NoopEvents;

    impl EventControl for NoopEvents {
        fn api_version(&self) -> ApiVersion {
            CORE_RUNTIME_API_VERSION
        }
        fn api_capabilities(&self) -> EventApiCapabilities {
            EventApiCapabilities::V2_REQUIRED
        }
        fn current_route_version(&self) -> RouteVersion {
            RouteVersion(1)
        }
        fn begin_route_update(&self, _: RouteVersion) -> Result<RouteTransaction, PluginError> {
            Ok(RouteTransaction(1))
        }
        fn stage_subscription(
            &self,
            _: RouteTransaction,
            _: &PluginIdentity,
            _: &SubscriptionSpec,
        ) -> Result<SubscriptionCandidate, PluginError> {
            unreachable!("the fixture has no subscriptions")
        }
        fn commit_at_safe_point(
            &self,
            _: RouteTransaction,
        ) -> Result<(RouteVersion, Vec<CommittedSubscription>), PluginError> {
            Ok((RouteVersion(2), vec![]))
        }
        fn abort(&self, _: RouteTransaction) {}
        fn retire_subscription(&self, _: SubscriptionToken) -> Result<(), PluginError> {
            Ok(())
        }
        fn publish(&self, _: &str, _: u32, _: &[u8], _: TraceContext) -> Result<(), PluginError> {
            Ok(())
        }
    }

    struct Endpoint;
    impl ServiceEndpoint for Endpoint {
        fn call(&self, request: BoxValue, _: TraceContext) -> Result<BoxValue, PluginError> {
            Ok(request)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct Lifecycle {
        log: Arc<Mutex<Vec<&'static str>>>,
        fail_start: bool,
    }
    impl Plugin for Lifecycle {
        fn validate(&self, _: &ValidationContext) -> Result<(), PluginError> {
            self.log.lock().unwrap().push("validate");
            Ok(())
        }
        fn start(&mut self, _: &mut PluginContext) -> Result<(), PluginError> {
            self.log.lock().unwrap().push("start");
            if self.fail_start {
                Err(crate::engine_error(
                    ErrorKind::RuntimeStartFailed,
                    "fixture_start",
                    "injected package start failure",
                ))
            } else {
                Ok(())
            }
        }
        fn quiesce(&mut self, _: StopReason) -> Result<(), PluginError> {
            self.log.lock().unwrap().push("quiesce");
            Ok(())
        }
        fn stop(&mut self) -> Result<(), PluginError> {
            self.log.lock().unwrap().push("stop");
            Ok(())
        }
    }

    struct Factory {
        manifest: &'static PluginManifest,
        log: Arc<Mutex<Vec<&'static str>>>,
        key: ServiceKey,
        fail_start: bool,
    }
    impl PluginFactory for Factory {
        fn manifest(&self) -> &'static PluginManifest {
            self.manifest
        }
        fn create(&self, _: PluginInit) -> Result<PluginBundle, PluginError> {
            Ok(PluginBundle {
                lifecycle: Box::new(Lifecycle {
                    log: self.log.clone(),
                    fail_start: self.fail_start,
                }),
                service_exports: vec![crate::ServiceExport {
                    service_key: self.key.clone(),
                    endpoint: Arc::new(Endpoint),
                }],
                subscription_bindings: vec![],
            })
        }
    }

    #[test]
    fn endpoint_activation_failure_rolls_back_endpoint_lifecycle_and_resources() {
        let key = ServiceKey {
            id: ServiceId::new("fixture", "endpoint"),
            version: Version::new(1, 0, 0),
            scope: ServiceScope::Global,
        };
        let manifest: &'static PluginManifest = Box::leak(Box::new(PluginManifest {
            plugin_type: Arc::from("activation-failure"),
            name: Arc::from("activation-failure"),
            version: Version::new(1, 0, 0),
            engine_api_version: CORE_RUNTIME_API_VERSION,
            abi_version: ApiVersion::new(1, 0),
            config_schema_version: 1,
            config_schema: Arc::new(serde_json::json!({})),
            provides: vec![ProvidedService {
                id: key.id.clone(),
                version: key.version.clone(),
                scope_kind: ScopeKind::Global,
                call_mode: CallMode::Inline,
            }],
            requires: vec![],
            publishes: vec![],
            subscribes: vec![],
            supported_execution_models: BTreeSet::from([ExecutionModel::Passive]),
            reload_policy: ReloadPolicy::RestartRequired,
        }));
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = PluginRegistry::default();
        registry
            .register(
                Arc::new(Factory {
                    manifest,
                    log: log.clone(),
                    key: key.clone(),
                    fail_start: false,
                }),
                Version::new(1, 0, 0),
                "fixture",
                CORE_RUNTIME_API_VERSION,
                ApiVersion::new(1, 0),
            )
            .unwrap();
        let spec = PluginSpec {
            instance_id: Arc::from("one"),
            plugin_type: Arc::from("activation-failure"),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 1,
                allowed_qos: BTreeSet::<EventQos>::new(),
            },
            service_scopes: vec![],
            required_service_scopes: vec![],
        };
        let plan = crate::compile_plugin_plan(&[spec], &registry, 1).unwrap();
        let events: Arc<dyn EventControl> = Arc::new(NoopEvents);
        let mut services = ServiceRegistry::default();
        let mut host = RuntimeHost::default();
        let prepared = host
            .prepare(&plan, &registry, &mut services, events.clone())
            .unwrap();
        let handle = services.bind(&key).unwrap();

        // Simulates an endpoint/runtime activation failure after prepare but before gates open.
        host.slots.get("one").unwrap().gate.stop();
        let error = host
            .start_and_commit(&plan, prepared, &mut services, events)
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::RuntimeStartFailed);
        assert_eq!(
            handle
                .call(Box::new(1_u64), TraceContext::default())
                .unwrap_err()
                .kind,
            ErrorKind::ServiceUnavailable
        );
        assert_eq!(
            &*log.lock().unwrap(),
            &["validate", "start", "quiesce", "stop"]
        );
        let slot = host.slot("one").unwrap();
        assert_eq!(slot.lifecycle_state, LifecycleState::Stopped);
        assert_eq!(slot.resources.resource_count(), 0);
    }

    #[test]
    fn package_replacement_changes_endpoint_generation_and_restores_previous_on_failure() {
        let key = ServiceKey {
            id: ServiceId::new("fixture", "replaceable"),
            version: Version::new(1, 0, 0),
            scope: ServiceScope::Global,
        };
        let manifest: &'static PluginManifest = Box::leak(Box::new(PluginManifest {
            plugin_type: Arc::from("replaceable"),
            name: Arc::from("replaceable"),
            version: Version::new(1, 0, 0),
            engine_api_version: CORE_RUNTIME_API_VERSION,
            abi_version: ApiVersion::new(1, 0),
            config_schema_version: 1,
            config_schema: Arc::new(serde_json::json!({})),
            provides: vec![ProvidedService {
                id: key.id.clone(),
                version: key.version.clone(),
                scope_kind: ScopeKind::Global,
                call_mode: CallMode::Inline,
            }],
            requires: vec![],
            publishes: vec![],
            subscribes: vec![],
            supported_execution_models: BTreeSet::from([ExecutionModel::Passive]),
            reload_policy: ReloadPolicy::RestartRequired,
        }));
        let mut spec = PluginSpec {
            instance_id: Arc::from("replaceable-1"),
            plugin_type: Arc::from("replaceable"),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 1,
                allowed_qos: BTreeSet::new(),
            },
            service_scopes: vec![],
            required_service_scopes: vec![],
        };
        let events: Arc<dyn EventControl> = Arc::new(NoopEvents);
        let log = Arc::new(Mutex::new(Vec::new()));
        let make_factory = |fail_start| {
            Arc::new(Factory {
                manifest,
                log: log.clone(),
                key: key.clone(),
                fail_start,
            }) as Arc<dyn PluginFactory>
        };
        let mut engine = crate::PluginEngine::new(events, ApiVersion::new(1, 0)).unwrap();
        engine
            .register(make_factory(false), Version::new(1, 0, 0), "package-v1")
            .unwrap();
        engine.apply(std::slice::from_ref(&spec)).unwrap();
        let handle = engine.services().bind(&key).unwrap();
        let first_generation = handle.generation().unwrap();

        spec.config = Arc::new(ConfigSnapshot::new(2, serde_json::json!({"revision": 2})));
        assert_eq!(
            engine
                .change_plan(std::slice::from_ref(&spec))
                .unwrap()
                .changes[0]
                .kind,
            crate::ChangeKind::RestartPlugin
        );
        engine.replace(std::slice::from_ref(&spec)).unwrap();
        assert!(handle.generation().unwrap() > first_generation);
        let config_generation = handle.generation().unwrap();

        let change = engine
            .replace_package(
                make_factory(false),
                Version::new(2, 0, 0),
                "package-v2",
                std::slice::from_ref(&spec),
            )
            .unwrap();
        assert_eq!(change.changes[0].kind, crate::ChangeKind::RestartPlugin);
        assert!(handle.generation().unwrap() > config_generation);
        handle
            .call(Box::new(7_u64), TraceContext::default())
            .unwrap();
        let second_generation = handle.generation().unwrap();

        assert!(
            engine
                .replace_package(
                    make_factory(true),
                    Version::new(3, 0, 0),
                    "package-v3",
                    std::slice::from_ref(&spec),
                )
                .is_err()
        );
        assert_eq!(engine.state(), crate::EngineState::Running);
        assert!(handle.generation().unwrap() > second_generation);
        handle
            .call(Box::new(9_u64), TraceContext::default())
            .unwrap();
        engine.shutdown(StopReason::Shutdown).unwrap();
    }
}
