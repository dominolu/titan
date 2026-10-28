//! Platform persistence ports and local adapters, shared by account and simulation hosts.
//! No strategy code, execution decisions or financial recomputation belongs here.
pub mod columnar;
mod facts;
mod objects;
mod postgres_projection;
mod profile;
mod routing;
mod run;
mod s3;
mod snapshot;
pub use facts::*;
pub use objects::*;
pub use postgres_projection::*;
pub use profile::*;
pub use routing::*;
pub use run::*;
pub use s3::*;
pub use snapshot::*;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub fn failure(code: &str) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, code.to_owned()).into()
}
pub fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}
