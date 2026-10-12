//! Shared housekeeping; strategy callbacks still execute on their own PRIMARY lanes.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use titan_core_types::{CoreError, Resource};

struct Task {
    active: bool,
    interval: Duration,
    next: Instant,
    callback: Box<dyn FnMut() + Send>,
}
#[derive(Default)]
struct State {
    stop: AtomicBool,
    next_id: AtomicU64,
    tasks: Mutex<BTreeMap<u64, Arc<Mutex<Task>>>>,
}
pub(crate) struct Scheduler {
    state: Arc<State>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}
pub(crate) struct ScheduledTask {
    state: Arc<State>,
    id: u64,
    task: Arc<Mutex<Task>>,
}

impl Scheduler {
    pub fn new() -> std::io::Result<Self> {
        let state = Arc::new(State::default());
        let worker = state.clone();
        let join = std::thread::Builder::new()
            .name("strategy-housekeeping".into())
            .spawn(move || {
                while !worker.stop.load(Ordering::Acquire) {
                    let tasks: Vec<_> = worker
                        .tasks
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .values()
                        .cloned()
                        .collect();
                    for task in tasks {
                        let mut task = task.lock().unwrap_or_else(|p| p.into_inner());
                        if task.active && Instant::now() >= task.next {
                            task.next = Instant::now() + task.interval;
                            (task.callback)();
                        }
                    }
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            })?;
        Ok(Self {
            state,
            join: Mutex::new(Some(join)),
        })
    }
    pub fn schedule(
        &self,
        interval: Duration,
        callback: impl FnMut() + Send + 'static,
    ) -> ScheduledTask {
        let mut tasks = self.state.tasks.lock().unwrap_or_else(|p| p.into_inner());
        let id = self.state.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let task = Arc::new(Mutex::new(Task {
            active: true,
            interval,
            next: Instant::now() + interval,
            callback: Box::new(callback),
        }));
        tasks.insert(id, task.clone());
        ScheduledTask {
            state: self.state.clone(),
            id,
            task,
        }
    }
}
impl Resource for ScheduledTask {
    fn close(&mut self) -> Result<(), CoreError> {
        self.task.lock().unwrap_or_else(|p| p.into_inner()).active = false;
        self.state
            .tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
        Ok(())
    }
}
impl Drop for ScheduledTask {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        if let Some(join) = self
            .join
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            join.thread().unpark();
            let _ = join.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancel_waits_for_current_callback_and_removes_task() {
        let scheduler = Scheduler::new().unwrap();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = count.clone();
        let mut task = scheduler.schedule(Duration::from_millis(1), move || {
            observed.fetch_add(1, Ordering::AcqRel);
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while count.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(count.load(Ordering::Acquire) > 0);
        task.close().unwrap();
        let previous = count.load(Ordering::Acquire);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(count.load(Ordering::Acquire), previous);
        assert!(scheduler.state.tasks.lock().unwrap().is_empty());
    }
}
