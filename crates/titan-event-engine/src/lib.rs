//! Bounded, preallocated event transport for Titan's live runtime.
//!
//! Publishers only reserve/copy payloads and enqueue handles. A single EventLoop assigns
//! local sequence numbers, routes immutable blocks, retries bounded pending deliveries,
//! and updates preallocated health state. Normal subscriber callbacks are driven through
//! [`titan_plugin_engine::EventReceiver`] on caller-owned runtime threads. Explicit FastLane
//! routes can either run synchronously on the publisher or enqueue an arena lease to one bounded,
//! ordered worker. In both modes the event continues through the normal EventLoop route as an
//! audit/mirror copy.

mod arena;
mod channel;
mod config;
mod control;
mod core;
mod engine;
mod error;
mod health;
mod metrics;
mod model;

pub use arena::*;
pub use channel::*;
pub use config::*;
pub use core::*;
pub use engine::*;
pub use error::*;
pub use health::*;
pub use metrics::*;
pub use model::*;

pub use titan_plugin_engine::CORE_RUNTIME_API_VERSION;

#[cfg(test)]
mod tests;
