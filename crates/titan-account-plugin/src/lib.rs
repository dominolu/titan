//! Account connector lifecycle, registry, restricted capabilities and service facade for Titan.
//!
//! Account facts bypass this control plane: each connector receives a restricted publisher and
//! publishes directly into EventEngine. Network protocols and reconciliation remain connector
//! responsibilities.

mod abi;
mod error;
mod model;
mod plugin;
mod registry;
mod service;

pub use abi::*;
pub use error::*;
pub use model::*;
pub use plugin::*;
pub use registry::*;
pub use service::*;

#[cfg(test)]
mod tests;
