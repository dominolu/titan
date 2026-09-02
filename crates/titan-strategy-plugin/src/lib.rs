//! Dynamic strategy package and runtime management for Titan.
//!
//! Business events bypass the registry: EventEngine owns each PRIMARY lane and invokes the
//! runtime's opaque `EventHandler` on its isolated worker.

mod artifact;
mod checkpoint;
mod error;
mod gateway;
mod model;
mod plugin;
mod runtime;
mod service;

pub use artifact::*;
pub use checkpoint::*;
pub use error::*;
pub use gateway::*;
pub use model::*;
pub use plugin::*;
pub use runtime::*;
pub use service::*;

#[cfg(test)]
mod tests;
