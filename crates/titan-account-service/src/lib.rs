//! Account connector lifecycle, registry, direct execution binding and query service for Titan.
//!
//! Each connector receives a restricted publisher and publishes private-stream facts directly
//! into EventEngine. REST requests are dispatched through a pre-bound `ExecutionHandle`.

mod abi;
mod core;
mod error;
mod execution;
mod model;
mod registry;
mod service;

pub use abi::*;
pub use core::*;
pub use error::*;
pub use execution::*;
pub use model::*;
pub use registry::*;
pub use service::*;

#[cfg(test)]
mod tests;
