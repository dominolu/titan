use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use crate::{
    CallMode, ErrorKind, ExecutionModel, PluginError, PluginIdentity, PluginRegistry, PluginSpec,
    ServiceId, ServiceKey, ServiceScope, engine_error,
};

#[derive(Clone, Debug)]
pub struct ServiceBindingPlan {
    pub consumer: PluginIdentity,
    pub requirement_index: usize,
    pub provider: Option<PluginIdentity>,
    pub key: Option<ServiceKey>,
    pub direct_inline: bool,
}

#[derive(Clone, Debug)]
pub struct PluginPlanEntry {
    pub identity: PluginIdentity,
    pub spec: PluginSpec,
    pub provides: Vec<ServiceKey>,
    pub bindings: Vec<ServiceBindingPlan>,
}

#[derive(Clone, Debug)]
pub struct PluginPlan {
    pub generation: u64,
    entries: BTreeMap<Arc<str>, PluginPlanEntry>,
    start_order: Arc<[Arc<str>]>,
    stop_order: Arc<[Arc<str>]>,
}

impl PluginPlan {
    pub fn entry(&self, instance_id: &str) -> Option<&PluginPlanEntry> {
        self.entries.get(instance_id)
    }

    pub fn entries(&self) -> impl Iterator<Item = &PluginPlanEntry> {
        self.entries.values()
    }

    pub fn start_order(&self) -> &[Arc<str>] {
        &self.start_order
    }

    pub fn stop_order(&self) -> &[Arc<str>] {
        &self.stop_order
    }
}

pub fn compile_plugin_plan(
    specs: &[PluginSpec],
    registry: &PluginRegistry,
    generation: u64,
) -> Result<PluginPlan, PluginError> {
    let enabled: Vec<_> = specs.iter().filter(|spec| spec.enabled).cloned().collect();
    let mut ids = BTreeSet::new();
    for spec in &enabled {
        if !ids.insert(spec.instance_id.clone()) {
            return Err(engine_error(
                ErrorKind::ConfigInvalid,
                "compile_plugin_plan",
                format!("duplicate instance id {}", spec.instance_id),
            ));
        }
        let registered = registry.get(&spec.plugin_type).ok_or_else(|| {
            PluginError::new(
                ErrorKind::ManifestInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                format!("unknown plugin type {}", spec.plugin_type),
            )
        })?;
        let manifest = registered.factory.manifest();
        let validator = jsonschema::validator_for(&manifest.config_schema).map_err(|error| {
            PluginError::new(
                ErrorKind::ManifestInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                format!("invalid config schema: {error}"),
            )
        })?;
        if let Err(error) = validator.validate(&spec.config.value) {
            return Err(PluginError::new(
                ErrorKind::ConfigInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                format!("configuration does not match schema: {error}"),
            ));
        }
        if !registered
            .factory
            .manifest()
            .supported_execution_models
            .contains(&spec.execution.model)
        {
            return Err(PluginError::new(
                ErrorKind::ConfigInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                "unsupported execution model",
            ));
        }
        if let Some(budget) = &spec.execution.callback_budget
            && (budget.soft_budget_us == 0
                || budget.stall_threshold_us < budget.soft_budget_us
                || budget.max_consecutive_violations == 0)
        {
            return Err(PluginError::new(
                ErrorKind::ConfigInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                "invalid callback budget",
            ));
        }
        if spec.subscription_limits.max_capacity == 0 {
            return Err(PluginError::new(
                ErrorKind::ConfigInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                "subscription capacity limit must be positive",
            ));
        }
        if spec.execution.model == ExecutionModel::Passive && !manifest.subscribes.is_empty() {
            return Err(PluginError::new(
                ErrorKind::ConfigInvalid,
                spec.identity(),
                crate::LifecycleState::Discovered,
                "compile_plugin_plan",
                "passive plugins cannot declare event subscriptions",
            ));
        }
    }

    let mut providers: BTreeMap<
        (ServiceId, crate::ServiceScope),
        (PluginIdentity, ServiceKey, CallMode, ExecutionModel),
    > = BTreeMap::new();
    let mut entries = BTreeMap::new();
    for spec in &enabled {
        let manifest = registry
            .get(&spec.plugin_type)
            .expect("checked")
            .factory
            .manifest();
        let configured_scopes: BTreeMap<_, _> = spec.service_scopes.iter().cloned().collect();
        for configured in configured_scopes.keys() {
            if !manifest
                .provides
                .iter()
                .any(|provided| &provided.id == configured)
            {
                return Err(PluginError::new(
                    ErrorKind::ConfigInvalid,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!("scope configured for undeclared service {configured}"),
                ));
            }
        }
        let mut provides = Vec::with_capacity(manifest.provides.len());
        for provided in &manifest.provides {
            let scope = if let Some(scope) = configured_scopes.get(&provided.id).cloned() {
                scope
            } else {
                match provided.scope_kind {
                    crate::ScopeKind::Global => crate::ServiceScope::Global,
                    crate::ScopeKind::PluginInstance => {
                        crate::ServiceScope::PluginInstance(spec.instance_id.clone())
                    }
                    crate::ScopeKind::Custom => {
                        return Err(PluginError::new(
                            ErrorKind::ConfigInvalid,
                            spec.identity(),
                            crate::LifecycleState::Discovered,
                            "compile_plugin_plan",
                            format!("custom scope for {} must be explicit", provided.id),
                        ));
                    }
                }
            };
            if scope.kind() != provided.scope_kind {
                return Err(PluginError::new(
                    ErrorKind::ConfigInvalid,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!("scope kind mismatch for {}", provided.id),
                ));
            }
            let key = ServiceKey {
                id: provided.id.clone(),
                version: provided.version.clone(),
                scope: scope.clone(),
            };
            if let Some((owner, ..)) = providers.insert(
                (provided.id.clone(), scope),
                (
                    spec.identity(),
                    key.clone(),
                    provided.call_mode,
                    spec.execution.model,
                ),
            ) {
                return Err(PluginError::new(
                    ErrorKind::ServiceConflict,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!("service {} conflicts with provider {}", provided.id, owner),
                ));
            }
            provides.push(key);
        }
        entries.insert(
            spec.instance_id.clone(),
            PluginPlanEntry {
                identity: spec.identity(),
                spec: spec.clone(),
                provides,
                bindings: Vec::new(),
            },
        );
    }

    let mut edges: BTreeMap<Arc<str>, BTreeSet<Arc<str>>> = enabled
        .iter()
        .map(|s| (s.instance_id.clone(), BTreeSet::new()))
        .collect();
    let mut indegree: BTreeMap<Arc<str>, usize> =
        enabled.iter().map(|s| (s.instance_id.clone(), 0)).collect();
    for spec in &enabled {
        let manifest = registry
            .get(&spec.plugin_type)
            .expect("checked")
            .factory
            .manifest();
        let mut bindings = Vec::new();
        let required_scopes: BTreeMap<_, _> =
            spec.required_service_scopes.iter().cloned().collect();
        for configured in required_scopes.keys() {
            if !manifest
                .requires
                .iter()
                .any(|required| &required.id == configured)
            {
                return Err(PluginError::new(
                    ErrorKind::ConfigInvalid,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!("selector configured for undeclared dependency {configured}"),
                ));
            }
        }
        for (index, requirement) in manifest.requires.iter().enumerate() {
            let scope = required_scopes
                .get(&requirement.id)
                .cloned()
                .unwrap_or(ServiceScope::Global);
            if scope.kind() != requirement.scope_kind
                || (requirement.scope_kind != crate::ScopeKind::Global
                    && !required_scopes.contains_key(&requirement.id))
            {
                return Err(PluginError::new(
                    ErrorKind::ConfigInvalid,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!(
                        "dependency {} requires an explicit {:?} selector",
                        requirement.id, requirement.scope_kind
                    ),
                ));
            }
            let found = providers.get(&(requirement.id.clone(), scope.clone()));
            let (provider, key, mode, provider_model) = if let Some(value) = found {
                value
            } else if requirement.required {
                return Err(PluginError::new(
                    ErrorKind::DependencyMissing,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!(
                        "missing required service {} in scope {:?}",
                        requirement.id, scope
                    ),
                ));
            } else {
                bindings.push(ServiceBindingPlan {
                    consumer: spec.identity(),
                    requirement_index: index,
                    provider: None,
                    key: None,
                    direct_inline: false,
                });
                continue;
            };
            if !requirement.version.matches(&key.version) {
                return Err(PluginError::new(
                    ErrorKind::DependencyMissing,
                    spec.identity(),
                    crate::LifecycleState::Discovered,
                    "compile_plugin_plan",
                    format!(
                        "service {} version {} does not match {}",
                        requirement.id, key.version, requirement.version
                    ),
                ));
            }
            let direct_inline = *mode == CallMode::Inline
                && execution_domain_compatible(
                    spec.execution.model,
                    *provider_model,
                    &spec.instance_id,
                    &provider.instance_id,
                );
            if *mode == CallMode::Inline
                && provider.instance_id != spec.instance_id
                && edges
                    .get_mut(&provider.instance_id)
                    .expect("provider exists")
                    .insert(spec.instance_id.clone())
            {
                *indegree
                    .get_mut(&spec.instance_id)
                    .expect("consumer exists") += 1;
            }
            bindings.push(ServiceBindingPlan {
                consumer: spec.identity(),
                requirement_index: index,
                provider: Some(provider.clone()),
                key: Some(key.clone()),
                direct_inline,
            });
        }
        entries
            .get_mut(&spec.instance_id)
            .expect("entry exists")
            .bindings = bindings;
    }

    let mut ready: VecDeque<_> = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
        .collect();
    let mut start_order = Vec::with_capacity(enabled.len());
    while let Some(id) = ready.pop_front() {
        start_order.push(id.clone());
        for consumer in &edges[&id] {
            let degree = indegree.get_mut(consumer).expect("consumer exists");
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(consumer.clone());
            }
        }
    }
    if start_order.len() != enabled.len() {
        return Err(engine_error(
            ErrorKind::DependencyCycle,
            "compile_plugin_plan",
            "synchronous service dependency cycle",
        ));
    }
    let stop_order: Vec<_> = start_order.iter().rev().cloned().collect();
    Ok(PluginPlan {
        generation,
        entries,
        start_order: start_order.into(),
        stop_order: stop_order.into(),
    })
}

fn execution_domain_compatible(
    consumer: ExecutionModel,
    provider: ExecutionModel,
    consumer_id: &str,
    provider_id: &str,
) -> bool {
    consumer_id == provider_id
        || (consumer == ExecutionModel::Passive && provider == ExecutionModel::Passive)
}
