//! Small contracts shared by Titan's statically assembled core services.

// CoreError intentionally carries complete component and operation context across service
// boundaries. Boxing it would complicate the stable service traits for no measurable hot-path
// benefit.
#![allow(clippy::result_large_err)]

mod activation;
mod error;
mod event;
mod model;
mod resources;

pub use activation::*;
pub use error::*;
pub use event::*;
pub use model::*;
pub use resources::*;

/// Version of the EventEngine/core-service interaction contract implemented here.
pub const CORE_RUNTIME_API_VERSION: ApiVersion = ApiVersion::new(2, 0);
pub const CORE_RUNTIME_V1_COMPAT_VERSION: ApiVersion = ApiVersion::new(1, 0);
