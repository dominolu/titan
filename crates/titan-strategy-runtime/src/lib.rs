//! Strategy package and runtime management for Titan.
//!
//! Business events bypass the registry: EventEngine owns each PRIMARY lane and invokes the
//! runtime's opaque `EventHandler` on its isolated worker.

mod compiled_strategy;
mod error;
mod model;
mod observation;
mod offline_v13;
mod runtime;
mod runtime_v13;
mod service;
mod service_core;
mod v13;

pub use compiled_strategy::*;
pub use error::*;
pub use model::*;
pub use observation::*;
pub use offline_v13::*;
pub use runtime::*;
pub use runtime_v13::*;
pub use service::*;
pub use service_core::*;
pub use v13::*;

mod scheduler;

mod recovery;
pub use recovery::*;
