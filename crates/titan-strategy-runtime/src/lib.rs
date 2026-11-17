//! Strategy package and runtime management for Titan.
//!
//! Business events bypass the registry: EventEngine owns each PRIMARY lane and invokes the
//! runtime's opaque `EventHandler` on its isolated worker.

mod compiled_strategy;
mod context;
mod depth_book;
mod depth_execution;
mod error;
mod execution_binding;
mod execution_reservation;
mod instrument_validation;
mod metadata_provider;
mod model;
mod native_runtime;
mod object_gate;
mod object_sources;
mod object_table;
mod observation;
mod offline;
mod recovery_storage;
mod runtime;
mod service;
mod service_core;
mod strategy;
#[cfg(test)]
mod test_allocations;

pub use compiled_strategy::*;
pub use context::*;
pub use depth_book::DepthBatchInfoV1;
pub use error::*;
pub use execution_binding::*;
pub use execution_reservation::*;
pub use instrument_validation::*;
pub use metadata_provider::*;
pub use model::*;
pub use native_runtime::*;
pub use object_gate::*;
pub use object_sources::*;
pub use object_table::*;
pub use observation::*;
pub use offline::*;
pub use recovery_storage::*;
pub use runtime::*;
pub use service::*;
pub use service_core::*;
pub use strategy::*;

mod scheduler;

mod recovery;
pub use recovery::*;
