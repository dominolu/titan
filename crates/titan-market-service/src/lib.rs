//! Market connector lifecycle, registry and service facade for Titan.
//!
//! Market payloads never pass through this crate: connectors receive a restricted publisher and
//! publish directly to EventEngine.

mod abi;
mod core;
mod error;
mod model;
mod registry;
mod service;

pub use abi::*;
pub use core::*;
pub use error::*;
pub use model::*;
pub use registry::*;
pub use service::*;

#[cfg(test)]
mod tests;
