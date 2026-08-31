use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, TrySendError};

use crate::{ErrorKind, PluginError, engine_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlOperation {
    QuiesceAll,
    StopAll,
    RestartAll,
    StartInstance(Arc<str>),
    QuiesceInstance(Arc<str>),
    StopInstance(Arc<str>),
    RestartInstance(Arc<str>),
}

#[derive(Clone, Debug)]
pub struct ControlCommand {
    pub idempotency_key: Arc<str>,
    pub deadline: Instant,
    pub operation: ControlOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlTicket {
    pub request_id: u64,
}

#[derive(Clone, Debug)]
pub enum ControlOperationState {
    Queued,
    Running,
    Succeeded,
    Failed(PluginError),
}

impl ControlOperationState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed(_))
    }
}

pub trait ControlHandler: Send + Sync + 'static {
    fn execute(&self, operation: &ControlOperation) -> Result<(), PluginError>;
}

struct QueuedCommand {
    request_id: u64,
    command: ControlCommand,
}

#[derive(Default)]
struct ControlState {
    operations: BTreeMap<u64, ControlOperationState>,
    idempotency: BTreeMap<Arc<str>, u64>,
}

struct SharedState {
    inner: Mutex<ControlState>,
    changed: Condvar,
}

/// Bounded, single-writer lifecycle command processor with queryable timeout semantics.
pub struct PluginControl {
    sender: Option<Sender<QueuedCommand>>,
    shared: Arc<SharedState>,
    next_request_id: AtomicU64,
    worker: Option<JoinHandle<()>>,
}

impl PluginControl {
    pub fn new(capacity: usize, handler: Arc<dyn ControlHandler>) -> Result<Self, PluginError> {
        if capacity == 0 {
            return Err(engine_error(
                ErrorKind::ConfigInvalid,
                "create_control",
                "control queue capacity must be positive",
            ));
        }
        let (sender, receiver) = crossbeam_channel::bounded(capacity);
        let shared = Arc::new(SharedState {
            inner: Mutex::new(ControlState::default()),
            changed: Condvar::new(),
        });
        let worker_shared = shared.clone();
        let worker = std::thread::Builder::new()
            .name("titan-plugin-control".into())
            .spawn(move || control_loop(receiver, worker_shared, handler))
            .map_err(|error| {
                engine_error(ErrorKind::PluginFailed, "create_control", error.to_string())
            })?;
        Ok(Self {
            sender: Some(sender),
            shared,
            next_request_id: AtomicU64::new(1),
            worker: Some(worker),
        })
    }

    pub fn try_submit(&self, command: ControlCommand) -> Result<ControlTicket, PluginError> {
        if command.deadline <= Instant::now() {
            return Err(engine_error(
                ErrorKind::ControlDeadlineExceeded,
                "try_submit",
                "command deadline has elapsed",
            ));
        }
        let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(request_id) = state.idempotency.get(&command.idempotency_key).copied() {
            return Ok(ControlTicket { request_id });
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        state
            .idempotency
            .insert(command.idempotency_key.clone(), request_id);
        state
            .operations
            .insert(request_id, ControlOperationState::Queued);
        match self
            .sender
            .as_ref()
            .expect("control sender is live")
            .try_send(QueuedCommand {
                request_id,
                command: command.clone(),
            }) {
            Ok(()) => {
                self.shared.changed.notify_all();
                Ok(ControlTicket { request_id })
            }
            Err(TrySendError::Full(_)) => {
                state.idempotency.remove(&command.idempotency_key);
                state.operations.remove(&request_id);
                Err(engine_error(
                    ErrorKind::ControlQueueFull,
                    "try_submit",
                    "control command queue is full",
                )
                .recoverable(true))
            }
            Err(TrySendError::Disconnected(_)) => Err(engine_error(
                ErrorKind::PluginFailed,
                "try_submit",
                "control thread is not running",
            )),
        }
    }

    pub fn submit_and_wait(
        &self,
        command: ControlCommand,
    ) -> Result<ControlOperationState, PluginError> {
        let deadline = command.deadline;
        let ticket = self.try_submit(command)?;
        let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if let Some(result) = state.operations.get(&ticket.request_id)
                && result.is_terminal()
            {
                return Ok(result.clone());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(engine_error(
                    ErrorKind::ControlDeadlineExceeded,
                    "submit_and_wait",
                    format!(
                        "request {} may still execute; query it by request_id",
                        ticket.request_id
                    ),
                )
                .recoverable(true));
            }
            let wait = deadline.saturating_duration_since(now);
            let (next, _) = self
                .shared
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(|p| p.into_inner());
            state = next;
        }
    }

    pub fn query(&self, request_id: u64) -> Option<ControlOperationState> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .operations
            .get(&request_id)
            .cloned()
    }
}

impl Drop for PluginControl {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn control_loop(
    receiver: Receiver<QueuedCommand>,
    shared: Arc<SharedState>,
    handler: Arc<dyn ControlHandler>,
) {
    while let Ok(queued) = receiver.recv() {
        {
            let mut state = shared.inner.lock().unwrap_or_else(|p| p.into_inner());
            state
                .operations
                .insert(queued.request_id, ControlOperationState::Running);
            shared.changed.notify_all();
        }
        let result = if queued.command.deadline <= Instant::now() {
            Err(engine_error(
                ErrorKind::ControlDeadlineExceeded,
                "execute_control",
                "deadline elapsed before execution",
            ))
        } else {
            handler.execute(&queued.command.operation)
        };
        let mut state = shared.inner.lock().unwrap_or_else(|p| p.into_inner());
        state.operations.insert(
            queued.request_id,
            match result {
                Ok(()) => ControlOperationState::Succeeded,
                Err(error) => ControlOperationState::Failed(error),
            },
        );
        shared.changed.notify_all();
    }
}

pub fn deadline_after(duration: Duration) -> Instant {
    Instant::now() + duration
}
