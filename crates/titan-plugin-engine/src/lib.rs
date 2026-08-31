//! Titan's plugin assembly and lifecycle kernel.
//!
//! The crate deliberately keeps registries and lifecycle operations on the control path.
//! Runtime service calls use pre-bound [`ServiceHandle`] values and event delivery is delegated
//! to an [`EventControl`] implementation, so neither hot path re-enters [`PluginEngine`].

mod activation;
mod callback;
mod change;
mod control;
mod dynamic_abi;
mod engine;
mod error;
mod event;
mod executor;
mod model;
mod plan;
mod plugin;
mod registry;
mod resources;
mod runtime;
mod service;

pub use activation::*;
pub use callback::*;
pub use change::*;
pub use control::*;
pub use dynamic_abi::*;
pub use engine::*;
pub use error::*;
pub use event::*;
pub use executor::*;
pub use model::*;
pub use plan::*;
pub use plugin::*;
pub use registry::*;
pub use resources::*;
pub use runtime::*;
pub use service::*;

/// Version of the EventEngine/PluginEngine interaction contract implemented here.
pub const CORE_RUNTIME_API_VERSION: ApiVersion = ApiVersion::new(1, 0);

#[cfg(test)]
mod tests;
