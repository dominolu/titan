use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{ErrorKind, PluginError, PluginIdentity, PluginPlan, PluginRegistry, ReloadPolicy};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ChangeKind {
    Added,
    Removed,
    Live,
    RestartPlugin,
    Unchanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginChange {
    pub instance_id: Arc<str>,
    pub kind: ChangeKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChangePlan {
    pub changes: Vec<PluginChange>,
}

impl ChangePlan {
    pub fn requires_restart(&self) -> bool {
        self.changes.iter().any(|change| {
            matches!(
                change.kind,
                ChangeKind::Added | ChangeKind::Removed | ChangeKind::RestartPlugin
            )
        })
    }
    pub fn is_noop(&self) -> bool {
        self.changes
            .iter()
            .all(|change| change.kind == ChangeKind::Unchanged)
    }
}

pub fn compile_change_plan(
    current: &PluginPlan,
    next: &PluginPlan,
    registry: &PluginRegistry,
) -> Result<ChangePlan, PluginError> {
    let current_entries: BTreeMap<_, _> = current
        .entries()
        .map(|entry| (entry.spec.instance_id.clone(), entry))
        .collect();
    let next_entries: BTreeMap<_, _> = next
        .entries()
        .map(|entry| (entry.spec.instance_id.clone(), entry))
        .collect();
    let ids: BTreeSet<_> = current_entries
        .keys()
        .chain(next_entries.keys())
        .cloned()
        .collect();
    let mut changes = Vec::new();
    for id in ids {
        let kind = match (current_entries.get(&id), next_entries.get(&id)) {
            (None, Some(_)) => ChangeKind::Added,
            (Some(_), None) => ChangeKind::Removed,
            (Some(old), Some(new))
                if old.spec.plugin_type != new.spec.plugin_type
                    || old.spec.execution != new.spec.execution
                    || old.package_version != new.package_version
                    || old.package_source != new.package_source =>
            {
                ChangeKind::RestartPlugin
            }
            (Some(old), Some(new))
                if old.spec.config.hash != new.spec.config.hash
                    || old.spec.config.value != new.spec.config.value =>
            {
                let policy = registry
                    .get(&new.spec.plugin_type)
                    .expect("compiled plan type exists")
                    .factory
                    .manifest()
                    .reload_policy;
                match policy {
                    ReloadPolicy::Never => {
                        return Err(PluginError::new(
                            ErrorKind::ConfigInvalid,
                            PluginIdentity::new(new.spec.plugin_type.clone(), id.clone()),
                            crate::LifecycleState::Running,
                            "compile_change_plan",
                            "manifest forbids reload",
                        ));
                    }
                    ReloadPolicy::Live => ChangeKind::Live,
                    ReloadPolicy::WhenQuiescent | ReloadPolicy::RestartRequired => {
                        ChangeKind::RestartPlugin
                    }
                }
            }
            (Some(_), Some(_)) => ChangeKind::Unchanged,
            (None, None) => unreachable!(),
        };
        changes.push(PluginChange {
            instance_id: id,
            kind,
        });
    }
    Ok(ChangePlan { changes })
}
