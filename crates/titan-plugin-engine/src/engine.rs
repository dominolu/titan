use std::sync::Arc;

use crate::{
    ApiVersion, BackgroundFlightRecorderExporter, ErrorKind, EventApiCapabilities, EventControl,
    FlightRecorder, PluginDiagnostic, PluginError, PluginFactory, PluginPlan, PluginRegistry,
    PluginSpec, RuntimeHost, ServiceRegistry, StopReason, compile_plugin_plan,
};

pub const PLUGIN_RUNTIME_FAILED_EVENT: &str = "titan.core.PluginRuntimeFailed";
pub const PLUGIN_CALLBACK_STALLED_EVENT: &str = "titan.core.PluginCallbackStalled";
pub const PLUGIN_HEALTH_CHANGED_EVENT: &str = "titan.core.PluginHealthChanged";
pub const PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION: u32 = 1;
pub const PLUGIN_RUNTIME_EVENT_TYPES: [&str; 3] = [
    PLUGIN_RUNTIME_FAILED_EVENT,
    PLUGIN_CALLBACK_STALLED_EVENT,
    PLUGIN_HEALTH_CHANGED_EVENT,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineState {
    Empty,
    Prepared,
    Running,
    Quiescing,
    Stopped,
    Failed,
}

struct PendingAssembly {
    plan: PluginPlan,
    runtime: crate::PreparedRuntime,
}

pub struct PluginEngine {
    registry: PluginRegistry,
    services: ServiceRegistry,
    runtimes: RuntimeHost,
    event_control: Arc<dyn EventControl>,
    host_api: ApiVersion,
    host_abi: ApiVersion,
    plan: Option<PluginPlan>,
    pending: Option<PendingAssembly>,
    next_generation: u64,
    state: EngineState,
    recorder: Arc<FlightRecorder>,
    exporter: Option<Arc<BackgroundFlightRecorderExporter>>,
    metrics: Arc<crate::PluginEngineMetrics>,
}

#[derive(Clone, Debug)]
pub struct PluginEngineObservabilitySnapshot {
    pub engine_state: EngineState,
    pub plugin_profile_version: Option<u64>,
    pub route_table_version: u64,
    pub plugins: Vec<PluginDiagnostic>,
    pub plugin_metrics: Vec<crate::PluginMetricsSnapshot>,
    pub service_metrics: Vec<crate::ServiceMetricsSnapshot>,
    pub failure_metrics: Vec<crate::PluginFailureMetricsSnapshot>,
    pub resource_release_failure_metrics: Vec<crate::ResourceReleaseFailureMetricsSnapshot>,
}

impl PluginEngine {
    pub fn new(
        event_control: Arc<dyn EventControl>,
        host_abi: ApiVersion,
    ) -> Result<Self, PluginError> {
        let event_api = event_control.api_version();
        if !event_api.supports(crate::CORE_RUNTIME_API_VERSION) {
            return Err(crate::engine_error(
                ErrorKind::ApiVersionMismatch,
                "create_engine",
                format!(
                    "EventEngine API {}.{} is incompatible",
                    event_api.major, event_api.minor
                ),
            ));
        }
        let capabilities = event_control.api_capabilities();
        if !capabilities.contains(EventApiCapabilities::V2_REQUIRED) {
            return Err(crate::engine_error(
                ErrorKind::ApiVersionMismatch,
                "create_engine",
                format!(
                    "EventEngine API is missing required capabilities: required={:#x}, actual={:#x}",
                    EventApiCapabilities::V2_REQUIRED.0,
                    capabilities.0
                ),
            ));
        }
        let recorder = Arc::new(FlightRecorder::new(4096));
        let metrics = Arc::new(crate::PluginEngineMetrics::default());
        Ok(Self {
            registry: PluginRegistry::default(),
            services: ServiceRegistry::new(recorder.clone(), metrics.clone()),
            runtimes: RuntimeHost::new(recorder.clone(), metrics.clone()),
            event_control,
            host_api: crate::CORE_RUNTIME_API_VERSION,
            host_abi,
            plan: None,
            pending: None,
            next_generation: 1,
            state: EngineState::Empty,
            recorder,
            exporter: None,
            metrics,
        })
    }

    pub fn register(
        &mut self,
        factory: Arc<dyn PluginFactory>,
        package_version: semver::Version,
        source: impl Into<Arc<str>>,
    ) -> Result<(), PluginError> {
        if self.state != EngineState::Empty && self.state != EngineState::Stopped {
            return Err(crate::engine_error(
                ErrorKind::PluginFailed,
                "register",
                "registry is immutable while plugins are assembled",
            ));
        }
        self.registry.register(
            factory,
            package_version,
            source,
            self.host_api,
            self.host_abi,
        )
    }

    /// Registers a dynamic plugin that uses only lifecycle callbacks and event publication.
    /// Connector/service ABIs are registered by their domain adapter instead.
    pub fn register_dynamic_lifecycle_package(
        &mut self,
        package: crate::LoadedDynamicPackage,
    ) -> Result<(), PluginError> {
        let factory = crate::DynamicLifecyclePluginFactory::from_package(package)?;
        let package_version = factory.package_version().clone();
        let source = factory.source().clone();
        self.register(Arc::new(factory), package_version, source)
    }

    pub fn compile(&self, specs: &[PluginSpec]) -> Result<PluginPlan, PluginError> {
        compile_plugin_plan(specs, &self.registry, self.next_generation)
    }

    pub fn change_plan(&self, specs: &[PluginSpec]) -> Result<crate::ChangePlan, PluginError> {
        let next = self.compile(specs)?;
        match &self.plan {
            Some(current) => crate::compile_change_plan(current, &next, &self.registry),
            None => Ok(crate::ChangePlan {
                changes: next
                    .entries()
                    .map(|entry| crate::PluginChange {
                        instance_id: entry.spec.instance_id.clone(),
                        kind: crate::ChangeKind::Added,
                    })
                    .collect(),
            }),
        }
    }

    /// Applies a validated configuration replacement. The new graph is compiled before the old
    /// graph is quiesced, so invalid updates never disturb a running assembly.
    pub fn replace(&mut self, specs: &[PluginSpec]) -> Result<(), Vec<PluginError>> {
        let next = self.compile(specs).map_err(|error| vec![error])?;
        if let Some(current) = &self.plan {
            crate::compile_change_plan(current, &next, &self.registry)
                .map_err(|error| vec![error])?;
        }
        let previous: Vec<_> = self
            .plan
            .as_ref()
            .map(|plan| plan.entries().map(|entry| entry.spec.clone()).collect())
            .unwrap_or_default();
        self.shutdown(StopReason::Restart)?;
        match self.apply(specs) {
            Ok(()) => Ok(()),
            Err(update_error) => {
                let mut errors = vec![update_error];
                if !previous.is_empty()
                    && let Err(recovery_error) = self.apply(&previous)
                {
                    errors.push(recovery_error);
                }
                Err(errors)
            }
        }
    }

    /// Replaces one registered package and applies the supplied complete profile as one control
    /// transaction. The candidate package is validated and compiled before the running graph is
    /// disturbed. If candidate startup fails, the previous registry entry and previous profile are
    /// restored.
    pub fn replace_package(
        &mut self,
        factory: Arc<dyn PluginFactory>,
        package_version: semver::Version,
        source: impl Into<Arc<str>>,
        specs: &[PluginSpec],
    ) -> Result<crate::ChangePlan, Vec<PluginError>> {
        if !matches!(self.state, EngineState::Running | EngineState::Stopped) {
            return Err(vec![crate::engine_error(
                ErrorKind::PluginFailed,
                "replace_package",
                "package replacement requires a running or stopped engine",
            )]);
        }
        let plugin_type = factory.manifest().plugin_type.clone();
        let previous_specs: Vec<_> = self
            .plan
            .as_ref()
            .map(|plan| plan.entries().map(|entry| entry.spec.clone()).collect())
            .unwrap_or_default();
        let previous_package = self
            .registry
            .replace(
                factory,
                package_version,
                source,
                self.host_api,
                self.host_abi,
            )
            .map_err(|error| vec![error])?;
        let candidate = match self.compile(specs) {
            Ok(plan) => plan,
            Err(error) => {
                self.registry.restore(plugin_type, previous_package);
                return Err(vec![error]);
            }
        };
        let changes = match &self.plan {
            Some(current) => {
                match crate::compile_change_plan(current, &candidate, &self.registry) {
                    Ok(changes) => changes,
                    Err(error) => {
                        self.registry.restore(plugin_type, previous_package);
                        return Err(vec![error]);
                    }
                }
            }
            None => crate::ChangePlan {
                changes: candidate
                    .entries()
                    .map(|entry| crate::PluginChange {
                        instance_id: entry.spec.instance_id.clone(),
                        kind: crate::ChangeKind::Added,
                    })
                    .collect(),
            },
        };

        let mut errors = Vec::new();
        if let Err(mut shutdown_errors) = self.shutdown(StopReason::Restart) {
            errors.append(&mut shutdown_errors);
        }
        if errors.is_empty() {
            if let Err(error) = self.apply(specs) {
                errors.push(error);
            } else {
                return Ok(changes);
            }
        }

        self.registry.restore(plugin_type, previous_package);
        if !previous_specs.is_empty()
            && let Err(error) = self.apply(&previous_specs)
        {
            errors.push(error);
        }
        Err(errors)
    }

    pub fn apply(&mut self, specs: &[PluginSpec]) -> Result<(), PluginError> {
        self.prepare(specs)?;
        self.start_prepared()
    }

    /// Builds and validates plugin bundles, stages endpoints and routes, but does not start
    /// plugins or make any business capability visible.
    pub fn prepare(&mut self, specs: &[PluginSpec]) -> Result<(), PluginError> {
        if matches!(
            self.state,
            EngineState::Prepared | EngineState::Running | EngineState::Quiescing
        ) {
            return Err(crate::engine_error(
                ErrorKind::PluginFailed,
                "apply",
                "active or prepared plan must be stopped before replacement",
            ));
        }
        let plan = self.compile(specs)?;
        self.runtimes
            .clear_stopped_for_plan(&mut self.services, &plan);
        let prepared = match self.runtimes.prepare(
            &plan,
            &self.registry,
            &mut self.services,
            self.event_control.clone(),
        ) {
            Ok(value) => value,
            Err(error) => {
                self.state = EngineState::Failed;
                return Err(error);
            }
        };
        self.pending = Some(PendingAssembly {
            plan,
            runtime: prepared,
        });
        self.state = EngineState::Prepared;
        Ok(())
    }

    /// Starts a previously prepared assembly and commits routes/endpoints at the EventEngine safe
    /// point. The EventEngine must be running before this call.
    pub fn start_prepared(&mut self) -> Result<(), PluginError> {
        let pending = self.pending.take().ok_or_else(|| {
            crate::engine_error(
                ErrorKind::PluginFailed,
                "start_prepared",
                "no prepared plugin assembly",
            )
        })?;
        if let Err(error) = self.runtimes.start_and_commit(
            &pending.plan,
            pending.runtime,
            &mut self.services,
            self.event_control.clone(),
        ) {
            self.state = EngineState::Failed;
            return Err(error);
        }
        self.next_generation += 1;
        self.plan = Some(pending.plan);
        self.state = EngineState::Running;
        Ok(())
    }

    /// Aborts an assembly that has not been committed. Used when EventEngine startup fails after
    /// plugin validation.
    pub fn abort_prepared(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.runtimes
                .abort_prepared(pending.runtime, &mut self.services, &self.event_control);
        }
        self.state = EngineState::Stopped;
    }

    pub fn quiesce_all(&mut self, reason: StopReason) -> Result<(), Vec<PluginError>> {
        let Some(plan) = &self.plan else {
            return Ok(());
        };
        self.state = EngineState::Quiescing;
        self.runtimes.quiesce_all(plan, &self.services, reason)
    }

    pub fn stop_all(&mut self) -> Result<(), Vec<PluginError>> {
        if self.pending.is_some() {
            self.abort_prepared();
            return Ok(());
        }
        let Some(plan) = &self.plan else {
            self.state = EngineState::Stopped;
            return Ok(());
        };
        let mut errors = Vec::new();
        if self.state == EngineState::Running
            && let Err(mut values) =
                self.runtimes
                    .quiesce_all(plan, &self.services, StopReason::Shutdown)
        {
            errors.append(&mut values);
        }
        if let Err(mut values) = self.runtimes.stop_all(plan, &self.services) {
            errors.append(&mut values);
        }
        let result = if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        };
        self.state = if result.is_ok() {
            EngineState::Stopped
        } else {
            EngineState::Failed
        };
        result
    }

    pub fn shutdown(&mut self, reason: StopReason) -> Result<(), Vec<PluginError>> {
        if self.pending.is_some() {
            self.abort_prepared();
            return Ok(());
        }
        let mut errors = Vec::new();
        if let Err(mut values) = self.quiesce_all(reason) {
            errors.append(&mut values);
        }
        if let Err(mut values) = self.stop_all() {
            errors.append(&mut values);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn state(&self) -> EngineState {
        self.state
    }
    pub fn plan(&self) -> Option<&PluginPlan> {
        self.plan.as_ref()
    }
    pub fn diagnostics(&self) -> Vec<PluginDiagnostic> {
        self.runtimes.diagnostics()
    }
    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }
    pub fn registry(&self) -> &PluginRegistry {
        &self.registry
    }

    pub fn flight_recorder(&self) -> Arc<FlightRecorder> {
        self.recorder.clone()
    }

    pub fn metrics(&self) -> Arc<crate::PluginEngineMetrics> {
        self.metrics.clone()
    }

    /// Cold-path, internally consistent-enough diagnostic view. Counters are monotonic atomics;
    /// lifecycle/configuration fields are read by the PluginEngine control owner.
    pub fn observability_snapshot(&self) -> PluginEngineObservabilitySnapshot {
        PluginEngineObservabilitySnapshot {
            engine_state: self.state,
            plugin_profile_version: self.plan.as_ref().map(|plan| plan.generation),
            route_table_version: self.event_control.current_route_version().0,
            plugins: self.runtimes.diagnostics(),
            plugin_metrics: self.metrics.plugin_snapshots(),
            service_metrics: self.metrics.service_snapshots(),
            failure_metrics: self.metrics.failure_snapshots(),
            resource_release_failure_metrics: self.metrics.resource_release_failure_snapshots(),
        }
    }

    /// Installs or removes the cold-path exporter used for forced fault snapshots.
    pub fn set_flight_recorder_exporter(
        &mut self,
        exporter: Option<Arc<BackgroundFlightRecorderExporter>>,
    ) {
        self.runtimes.set_exporter(exporter.clone());
        self.exporter = exporter;
    }

    pub fn report_runtime_failure(
        &mut self,
        instance_id: &str,
        error: PluginError,
        stalled: bool,
    ) -> Result<(), PluginError> {
        let trace = error.trace_context.unwrap_or_default();
        let plugin_type = bounded_diagnostic_text(&error.identity.plugin_type, 256);
        let event_instance_id = bounded_diagnostic_text(&error.identity.instance_id, 256);
        let operation = bounded_diagnostic_text(&error.operation, 256);
        let message = bounded_diagnostic_text(&error.message, 16 * 1024);
        let causes = error
            .cause_chain
            .iter()
            .take(16)
            .map(|cause| bounded_diagnostic_text(cause, 4 * 1024))
            .collect::<Vec<_>>();
        let occurred_at_ns = error
            .occurred_at
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let payload = serde_json::to_vec(&serde_json::json!({
            "schema_version": PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
            "plugin_type": plugin_type,
            "instance_id": event_instance_id,
            "lifecycle_state": format!("{:?}", error.lifecycle_state),
            "health": if stalled { "STALLED" } else { "FAILED" },
            "error_kind": format!("{:?}", error.kind),
            "operation": operation,
            "message": message,
            "cause_chain": causes,
            "occurred_at_ns": occurred_at_ns,
            "recoverable": error.recoverable,
            "request_id": error.request_id,
        }))
        .map_err(|serialization_error| {
            crate::engine_error(
                ErrorKind::PluginFailed,
                "report_runtime_failure",
                serialization_error.to_string(),
            )
        })?;
        self.runtimes
            .report_failure(instance_id, error, &self.services, stalled)?;
        let event_type = if stalled {
            PLUGIN_CALLBACK_STALLED_EVENT
        } else {
            PLUGIN_RUNTIME_FAILED_EVENT
        };
        let failure_result = self.event_control.publish(
            event_type,
            PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
            &payload,
            trace,
        );
        let health_result = self.event_control.publish(
            PLUGIN_HEALTH_CHANGED_EVENT,
            PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
            &payload,
            trace,
        );
        failure_result.and(health_result)
    }
}

fn bounded_diagnostic_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
