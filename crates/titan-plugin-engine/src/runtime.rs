use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    ActivationGate, BoundServices, ErrorKind, EventControl, EventHandler, EventView, PluginBundle,
    PluginContext, PluginError, PluginIdentity, PluginInit, PluginPlan, PluginRegistry,
    ResourceScope, RouteTransaction, ServiceKey, ServiceRegistry, StopReason,
    SubscriptionCandidate, SubscriptionRuntime, SubscriptionSpec, ValidationContext,
    publication_grants, subscription_grants,
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
    pub subscription_count: usize,
    pub resource_count: usize,
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
    pub bundle: PluginBundle,
    pub provided_services: Vec<ServiceKey>,
    subscriptions: Vec<SubscriptionRuntime>,
    pub last_error: Option<PluginError>,
    pub updated_at: SystemTime,
}

impl PluginSlot {
    fn transition(&mut self, state: LifecycleState) {
        self.lifecycle_state = state;
        self.updated_at = SystemTime::now();
    }

    fn diagnostic(&self) -> PluginDiagnostic {
        PluginDiagnostic {
            identity: self.identity.clone(),
            lifecycle_state: self.lifecycle_state,
            health: self.health,
            config_version: self.context.config.version,
            provided_services: self.provided_services.clone(),
            subscription_count: self.subscriptions.len(),
            resource_count: self.resources.resource_count(),
            last_error: self.last_error.clone(),
            updated_at: self.updated_at,
        }
    }
}

pub struct PreparedRuntime {
    route_transaction: RouteTransaction,
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

#[derive(Default)]
pub struct RuntimeHost {
    slots: BTreeMap<Arc<str>, PluginSlot>,
}

impl RuntimeHost {
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
        for entry in plan.entries() {
            for key in &entry.provides {
                services.stage(key.clone(), entry.identity.clone())?;
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
                        let handle = services.bind(key).ok_or_else(|| {
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
                        bundle,
                        provided_services: entry.provides.clone(),
                        subscriptions: Vec::new(),
                        last_error: None,
                        updated_at: SystemTime::now(),
                    },
                );
            }
            Ok(())
        })();
        if let Err(error) = result {
            events.abort(transaction);
            self.rollback_prepared(services);
            return Err(error);
        }
        for slot in self.slots.values_mut() {
            slot.transition(LifecycleState::Resolved);
        }
        Ok(PreparedRuntime {
            route_transaction: transaction,
            staged_subscriptions: staged,
        })
    }

    pub fn start_and_commit(
        &mut self,
        plan: &PluginPlan,
        prepared: PreparedRuntime,
        services: &ServiceRegistry,
        events: Arc<dyn EventControl>,
    ) -> Result<(), PluginError> {
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
                slot.last_error = Some(error.clone());
                slot.health = HealthState::Failed;
                slot.transition(LifecycleState::Failed);
                self.rollback_start(&started, services, &events, prepared.route_transaction);
                return Err(error);
            }
            for export in &slot.bundle.service_exports {
                if let Err(error) = services.publish(
                    &export.service_key,
                    export.endpoint.clone(),
                    slot.gate.clone(),
                ) {
                    self.rollback_start(&started, services, &events, prepared.route_transaction);
                    return Err(error);
                }
            }
            started.push(instance_id.clone());
        }
        let (_, committed) = match events.commit_at_safe_point(prepared.route_transaction) {
            Ok(committed) => committed,
            Err(error) => {
                self.rollback_start(&started, services, &events, prepared.route_transaction);
                return Err(error);
            }
        };
        if committed.len() != prepared.staged_subscriptions.len() {
            for subscription in committed {
                let _ = events.retire_subscription(subscription.token);
            }
            self.rollback_start(&started, services, &events, prepared.route_transaction);
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
            prepared.staged_subscriptions.into_iter().zip(committed)
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
            ) {
                Ok(runtime) => runtime,
                Err(error) => {
                    for token in tokens {
                        let _ = events.retire_subscription(token);
                    }
                    self.rollback_start(&started, services, &events, prepared.route_transaction);
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
                self.rollback_start(&started, services, &events, prepared.route_transaction);
                return Err(error);
            }
            slot.transition(LifecycleState::Running);
        }
        Ok(())
    }

    pub fn quiesce_all(
        &mut self,
        plan: &PluginPlan,
        services: &ServiceRegistry,
        reason: StopReason,
    ) -> Result<(), Vec<PluginError>> {
        let mut errors = Vec::new();
        for instance_id in plan.stop_order() {
            let Some(slot) = self.slots.get_mut(instance_id) else {
                continue;
            };
            slot.transition(LifecycleState::Quiescing);
            for key in &slot.provided_services {
                services.make_unavailable(key);
            }
            let result = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.quiesce(reason)))
                .map_err(|_| panic_error(&slot.identity, LifecycleState::Quiescing, "quiesce"))
                .and_then(|result| result);
            if let Err(error) = result {
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
                    errors.push(error);
                }
            }
            let result = catch_unwind(AssertUnwindSafe(|| slot.bundle.lifecycle.stop()))
                .map_err(|_| panic_error(&slot.identity, LifecycleState::Stopping, "stop"))
                .and_then(|result| result);
            if let Err(error) = result {
                errors.push(error);
            }
            if let Err(mut resource_errors) = slot.resources.close() {
                errors.append(&mut resource_errors);
            }
            slot.gate.stop();
            slot.transition(LifecycleState::Stopped);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn clear_stopped(&mut self, services: &mut ServiceRegistry) {
        for slot in self.slots.values() {
            for key in &slot.provided_services {
                services.remove(key);
            }
        }
        self.slots.clear();
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
        slot.health = if stalled {
            HealthState::Stalled
        } else {
            HealthState::Failed
        };
        slot.transition(LifecycleState::Failed);
        Ok(())
    }

    fn rollback_prepared(&mut self, services: &mut ServiceRegistry) {
        for (_, mut slot) in std::mem::take(&mut self.slots) {
            for key in &slot.provided_services {
                services.remove(key);
            }
            slot.gate.stop();
            let _ = slot.resources.close();
        }
    }

    fn rollback_start(
        &mut self,
        started: &[Arc<str>],
        services: &ServiceRegistry,
        events: &Arc<dyn EventControl>,
        transaction: RouteTransaction,
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
