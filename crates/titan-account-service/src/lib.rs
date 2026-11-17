//! Account connector lifecycle, registry, direct execution binding and query service for Titan.
//!
//! Each connector receives a restricted publisher and publishes private-stream facts directly
//! into EventEngine. REST requests are dispatched through a pre-bound `ExecutionHandle`.

mod abi;
mod core;
mod error;
mod execution;
mod execution_ownership;
mod model;
mod observation;
mod registry;
mod risk;
mod service;

pub use abi::*;
pub use core::*;
pub use error::*;
pub use execution::*;
pub use execution_ownership::AccountExecutionLease;
pub use model::*;
pub use observation::*;
pub use registry::*;
pub use risk::*;
pub use service::*;

#[cfg(test)]
mod tests;
