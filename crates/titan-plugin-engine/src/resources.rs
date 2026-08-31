use std::sync::{Arc, Mutex};

use crate::{ErrorKind, LifecycleState, PluginError, PluginIdentity};

pub trait Resource: Send + 'static {
    fn close(&mut self) -> Result<(), PluginError>;
}

struct ResourceEntry {
    name: Arc<str>,
    resource: Box<dyn Resource>,
}

#[derive(Default)]
struct ScopeState {
    closed: bool,
    entries: Vec<ResourceEntry>,
}

/// Sole owner of a plugin instance's revocable resources.
pub struct ResourceScope {
    identity: PluginIdentity,
    state: Arc<Mutex<ScopeState>>,
}

#[derive(Clone)]
pub struct ResourceScopeHandle {
    identity: PluginIdentity,
    state: Arc<Mutex<ScopeState>>,
}

impl ResourceScope {
    pub fn new(identity: PluginIdentity) -> Self {
        Self {
            identity,
            state: Arc::new(Mutex::new(ScopeState::default())),
        }
    }

    pub fn handle(&self) -> ResourceScopeHandle {
        ResourceScopeHandle {
            identity: self.identity.clone(),
            state: self.state.clone(),
        }
    }

    pub fn resource_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .len()
    }

    pub fn close(&mut self) -> Result<(), Vec<PluginError>> {
        let mut entries = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            std::mem::take(&mut state.entries)
        };
        let mut errors = Vec::new();
        while let Some(mut entry) = entries.pop() {
            if let Err(mut error) = entry.resource.close() {
                error.kind = ErrorKind::ResourceReleaseFailed;
                error
                    .cause_chain
                    .push(Arc::from(format!("resource={}", entry.name)));
                errors.push(error);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Drop for ResourceScope {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl ResourceScopeHandle {
    pub fn register(
        &self,
        name: impl Into<Arc<str>>,
        mut resource: impl Resource,
    ) -> Result<(), PluginError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed {
            drop(state);
            let mut error = PluginError::new(
                ErrorKind::ResourceReleaseFailed,
                self.identity.clone(),
                LifecycleState::Stopping,
                "register_resource",
                "resource scope is closed",
            );
            if let Err(close_error) = resource.close() {
                error.cause_chain.push(Arc::from(close_error.to_string()));
            }
            return Err(error);
        }
        state.entries.push(ResourceEntry {
            name: name.into(),
            resource: Box::new(resource),
        });
        Ok(())
    }

    pub fn child(&self, name: impl Into<Arc<str>>) -> Result<ResourceScopeHandle, PluginError> {
        let scope = ResourceScope::new(self.identity.clone());
        let handle = scope.handle();
        self.register(name, ChildScope(Some(scope)))?;
        Ok(handle)
    }
}

struct ChildScope(Option<ResourceScope>);

impl Resource for ChildScope {
    fn close(&mut self) -> Result<(), PluginError> {
        let Some(mut scope) = self.0.take() else {
            return Ok(());
        };
        scope.close().map_err(|errors| {
            errors.into_iter().next().unwrap_or_else(|| {
                PluginError::new(
                    ErrorKind::ResourceReleaseFailed,
                    scope.identity.clone(),
                    LifecycleState::Stopping,
                    "close_child_scope",
                    "child scope failed",
                )
            })
        })
    }
}

pub struct ClosureResource<F: FnMut() -> Result<(), PluginError> + Send + 'static>(pub Option<F>);

impl<F: FnMut() -> Result<(), PluginError> + Send + 'static> Resource for ClosureResource<F> {
    fn close(&mut self) -> Result<(), PluginError> {
        self.0.as_mut().map_or(Ok(()), |close| close())
    }
}
