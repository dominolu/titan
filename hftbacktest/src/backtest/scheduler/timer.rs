use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimerId {
    pub owner_id: u64,
    pub timer_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DuplicateTimerPolicy {
    Replace,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerEvent {
    pub deadline_ts: i64,
    pub id: TimerId,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TimerError {
    #[error("timer is already registered")]
    Duplicate,
}

/// Deterministic first-class timer source. The scheduler can compare `next_timestamp` with market
/// sources, so timers advance the runtime even when no market data is available.
pub struct TimerQueue {
    policy: DuplicateTimerPolicy,
    timers: BTreeMap<(i64, u64, u64, u64), TimerEvent>,
    by_id: HashMap<TimerId, (i64, u64, u64, u64)>,
    next_sequence: u64,
}

impl TimerQueue {
    pub fn new(policy: DuplicateTimerPolicy) -> Self {
        Self {
            policy,
            timers: BTreeMap::new(),
            by_id: HashMap::new(),
            next_sequence: 0,
        }
    }

    pub fn schedule(&mut self, deadline_ts: i64, id: TimerId) -> Result<TimerEvent, TimerError> {
        if let Some(old_key) = self.by_id.remove(&id) {
            if self.policy == DuplicateTimerPolicy::Reject {
                self.by_id.insert(id, old_key);
                return Err(TimerError::Duplicate);
            }
            self.timers.remove(&old_key);
        }
        let event = TimerEvent {
            deadline_ts,
            id,
            sequence: self.next_sequence,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("timer sequence overflow");
        let key = (deadline_ts, id.owner_id, id.timer_id, event.sequence);
        self.timers.insert(key, event);
        self.by_id.insert(id, key);
        Ok(event)
    }

    pub fn cancel(&mut self, id: TimerId) -> bool {
        self.by_id
            .remove(&id)
            .and_then(|key| self.timers.remove(&key))
            .is_some()
    }

    pub fn next_timestamp(&self) -> Option<i64> {
        self.timers.first_key_value().map(|(key, _)| key.0)
    }

    pub fn drain_due(&mut self, now: i64, out: &mut Vec<TimerEvent>) {
        while self
            .timers
            .first_key_value()
            .is_some_and(|(key, _)| key.0 <= now)
        {
            let (_, event) = self.timers.pop_first().unwrap();
            self.by_id.remove(&event.id);
            out.push(event);
        }
    }

    pub fn reset(&mut self) {
        self.timers.clear();
        self.by_id.clear();
        self.next_sequence = 0;
    }
}

impl Default for TimerQueue {
    fn default() -> Self {
        Self::new(DuplicateTimerPolicy::Replace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timers_replace_and_drain_without_market_data() {
        let mut queue = TimerQueue::default();
        let a = TimerId {
            owner_id: 2,
            timer_id: 1,
        };
        let b = TimerId {
            owner_id: 1,
            timer_id: 5,
        };
        queue.schedule(20, a).unwrap();
        queue.schedule(10, a).unwrap();
        queue.schedule(10, b).unwrap();
        assert_eq!(queue.next_timestamp(), Some(10));
        let mut due = Vec::new();
        queue.drain_due(10, &mut due);
        assert_eq!(due.iter().map(|event| event.id).collect::<Vec<_>>(), [b, a]);
        assert_eq!(queue.next_timestamp(), None);
        queue.reset();
        assert_eq!(queue.schedule(1, a).unwrap().sequence, 0);
    }
}
