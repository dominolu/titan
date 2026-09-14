//! Strategy package and runtime management for Titan.
//!
//! Business events bypass the registry: EventEngine owns each PRIMARY lane and invokes the
//! runtime's opaque `EventHandler` on its isolated worker.

mod artifact;
mod error;
mod model;
mod runtime;
mod service;
mod service_core;

pub use artifact::*;
pub use error::*;
pub use model::*;
pub use runtime::*;
pub use service::*;
pub use service_core::*;

#[cfg(test)]
mod tests;
