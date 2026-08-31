use std::sync::Arc;

use crate::{
    ApiVersion, ErrorKind, EventControl, PluginDiagnostic, PluginError, PluginFactory, PluginPlan,
    PluginRegistry, PluginSpec, RuntimeHost, ServiceRegistry, StopReason, compile_plugin_plan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineState {
    Empty,
    Running,
    Quiescing,
    Stopped,
    Failed,
}

pub struct PluginEngine {
    registry: PluginRegistry,
    services: ServiceRegistry,
    runtimes: RuntimeHost,
    event_control: Arc<dyn EventControl>,
    host_api: ApiVersion,
    host_abi: ApiVersion,
    plan: Option<PluginPlan>,
    next_generation: u64,
    state: EngineState,
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
        Ok(Self {
            registry: PluginRegistry::default(),
            services: ServiceRegistry::default(),
            runtimes: RuntimeHost::default(),
            event_control,
            host_api: crate::CORE_RUNTIME_API_VERSION,
            host_abi,
            plan: None,
            next_generation: 1,
            state: EngineState::Empty,
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

    pub fn apply(&mut self, specs: &[PluginSpec]) -> Result<(), PluginError> {
        if self.state == EngineState::Running || self.state == EngineState::Quiescing {
            return Err(crate::engine_error(
                ErrorKind::PluginFailed,
                "apply",
                "running plan must be stopped before replacement",
            ));
        }
        self.runtimes.clear_stopped(&mut self.services);
        let plan = self.compile(specs)?;
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
        if let Err(error) = self.runtimes.start_and_commit(
            &plan,
            prepared,
            &self.services,
            self.event_control.clone(),
        ) {
            self.state = EngineState::Failed;
            return Err(error);
        }
        self.next_generation += 1;
        self.plan = Some(plan);
        self.state = EngineState::Running;
        Ok(())
    }

    pub fn quiesce_all(&mut self, reason: StopReason) -> Result<(), Vec<PluginError>> {
        let Some(plan) = &self.plan else {
            return Ok(());
        };
        self.state = EngineState::Quiescing;
        self.runtimes.quiesce_all(plan, &self.services, reason)
    }

    pub fn stop_all(&mut self) -> Result<(), Vec<PluginError>> {
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

    pub fn report_runtime_failure(
        &mut self,
        instance_id: &str,
        error: PluginError,
        stalled: bool,
    ) -> Result<(), PluginError> {
        self.runtimes
            .report_failure(instance_id, error, &self.services, stalled)?;
        let event_type = if stalled {
            "titan.core.PluginCallbackStalled"
        } else {
            "titan.core.PluginRuntimeFailed"
        };
        self.event_control.publish(
            event_type,
            1,
            instance_id.as_bytes(),
            crate::TraceContext::default(),
        )
    }
}
