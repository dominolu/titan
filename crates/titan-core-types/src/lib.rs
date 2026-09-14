//! Small contracts shared by Titan's statically assembled core services.

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
