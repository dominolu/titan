use std::{future::Future, sync::Arc, thread::JoinHandle, time::Duration};

use tokio::sync::Semaphore;

use crate::{
    ActivationGate, ActivationState, ErrorKind, LifecycleState, PluginError, PluginIdentity,
    Resource, ResourceScopeHandle,
};

pub struct ColdAsyncRuntime {
    runtime: Option<tokio::runtime::Runtime>,
    admission: Arc<Semaphore>,
}

impl ColdAsyncRuntime {
    pub fn new(worker_threads: usize, admission_capacity: usize) -> Result<Self, PluginError> {
        if worker_threads == 0 || admission_capacity == 0 {
            return Err(crate::engine_error(
                ErrorKind::ConfigInvalid,
                "create_cold_async",
                "worker and admission capacities must be positive",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("titan-cold-async")
            .enable_time()
            .build()
            .map_err(|error| {
                crate::engine_error(
                    ErrorKind::PluginFailed,
                    "create_cold_async",
                    error.to_string(),
                )
            })?;
        Ok(Self {
            runtime: Some(runtime),
            admission: Arc::new(Semaphore::new(admission_capacity)),
        })
    }

    pub fn try_spawn<F>(&self, future: F) -> Result<tokio::task::JoinHandle<F::Output>, PluginError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            crate::engine_error(
                ErrorKind::ControlQueueFull,
                "cold_async_spawn",
                "cold async admission is full",
            )
            .recoverable(true)
        })?;
        Ok(self
            .runtime
            .as_ref()
            .expect("runtime is live")
            .spawn(async move {
                let _permit = permit;
                future.await
            }))
    }
}

impl Drop for ColdAsyncRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

pub struct BlockingExecutor {
    sender: Option<crossbeam_channel::Sender<Box<dyn FnOnce() + Send>>>,
    workers: Vec<JoinHandle<()>>,
}

impl BlockingExecutor {
    pub fn new(worker_threads: usize, queue_capacity: usize) -> Result<Self, PluginError> {
        if worker_threads == 0 || queue_capacity == 0 {
            return Err(crate::engine_error(
                ErrorKind::ConfigInvalid,
                "create_blocking_executor",
                "worker and queue capacities must be positive",
            ));
        }
        let (sender, receiver) =
            crossbeam_channel::bounded::<Box<dyn FnOnce() + Send>>(queue_capacity);
        let mut workers = Vec::new();
        for index in 0..worker_threads {
            let receiver = receiver.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("titan-blocking-{index}"))
                    .spawn(move || {
                        while let Ok(task) = receiver.recv() {
                            task();
                        }
                    })
                    .map_err(|error| {
                        crate::engine_error(
                            ErrorKind::PluginFailed,
                            "create_blocking_executor",
                            error.to_string(),
                        )
                    })?,
            );
        }
        Ok(Self {
            sender: Some(sender),
            workers,
        })
    }
    pub fn try_submit(&self, task: impl FnOnce() + Send + 'static) -> Result<(), PluginError> {
        self.sender
            .as_ref()
            .expect("executor is live")
            .try_send(Box::new(task))
            .map_err(|error| match error {
                crossbeam_channel::TrySendError::Full(_) => crate::engine_error(
                    ErrorKind::ControlQueueFull,
                    "blocking_submit",
                    "blocking executor queue is full",
                )
                .recoverable(true),
                crossbeam_channel::TrySendError::Disconnected(_) => crate::engine_error(
                    ErrorKind::PluginFailed,
                    "blocking_submit",
                    "blocking executor stopped",
                ),
            })
    }
}

impl Drop for BlockingExecutor {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub fn spawn_dedicated(
    identity: PluginIdentity,
    resources: &ResourceScopeHandle,
    name: impl Into<String>,
    cpu_affinity: Option<usize>,
    gate: Arc<ActivationGate>,
    task: impl FnOnce() + Send + 'static,
) -> Result<(), PluginError> {
    let name = name.into();
    if let Some(cpu) = cpu_affinity
        && !core_affinity::get_core_ids()
            .is_some_and(|cores| cores.iter().any(|core| core.id == cpu))
    {
        return Err(PluginError::new(
            ErrorKind::ConfigInvalid,
            identity,
            LifecycleState::Starting,
            "spawn_dedicated",
            format!("CPU {cpu} is unavailable"),
        ));
    }
    let error_identity = identity.clone();
    let thread = std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            if let Some(cpu) = cpu_affinity
                && let Some(core) = core_affinity::get_core_ids()
                    .and_then(|cores| cores.into_iter().find(|core| core.id == cpu))
            {
                let _ = core_affinity::set_for_current(core);
            }
            if gate.wait_until_active() == ActivationState::Active {
                task();
            }
        })
        .map_err(|error| {
            PluginError::new(
                ErrorKind::RuntimeStartFailed,
                error_identity,
                LifecycleState::Starting,
                "spawn_dedicated",
                error.to_string(),
            )
        })?;
    resources.register("dedicated-thread", JoinThread(Some(thread), identity))
}

struct JoinThread(Option<JoinHandle<()>>, PluginIdentity);
impl Resource for JoinThread {
    fn close(&mut self) -> Result<(), PluginError> {
        self.0.take().map_or(Ok(()), |thread| {
            thread.join().map_err(|_| {
                PluginError::new(
                    ErrorKind::PluginFailed,
                    self.1.clone(),
                    LifecycleState::Stopping,
                    "join_thread",
                    "dedicated thread panicked",
                )
            })
        })
    }
}
