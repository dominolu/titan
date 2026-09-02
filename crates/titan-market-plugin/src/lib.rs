//! Market connector lifecycle, registry and service facade for Titan.
//!
//! Market payloads never pass through this crate: connectors receive a restricted publisher and
//! publish directly to EventEngine. This crate only implements the control plane.

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
