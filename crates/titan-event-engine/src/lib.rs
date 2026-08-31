//! Bounded, preallocated event transport for Titan's live runtime.
//!
//! Publishers only reserve/copy payloads and enqueue handles. A single EventLoop assigns
//! local sequence numbers, routes immutable blocks, retries bounded pending deliveries,
//! and updates preallocated health state. Subscriber callbacks are driven through
//! [`titan_plugin_engine::EventReceiver`] on caller-owned runtime threads and never run on
//! publisher or EventLoop threads.

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
