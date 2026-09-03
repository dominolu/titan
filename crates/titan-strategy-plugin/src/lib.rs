//! Dynamic strategy package and runtime management for Titan.
//!
//! Business events bypass the registry: EventEngine owns each PRIMARY lane and invokes the
//! runtime's opaque `EventHandler` on its isolated worker.

mod artifact;
mod error;
mod gateway;
mod model;
mod plugin;
mod runtime;
mod service;
mod service_adapters;

pub use artifact::*;
pub use error::*;
pub use gateway::*;
pub use model::*;
pub use plugin::*;
pub use runtime::*;
pub use service::*;
pub use service_adapters::*;

#[cfg(test)]
mod tests;
