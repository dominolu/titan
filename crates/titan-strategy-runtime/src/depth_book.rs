//! Depth book re-exports.
//!
//! The reconstruction itself lives in `titan-l2-book`, shared with the market data recorder; this
//! module keeps the runtime's own path to it so the live code does not change with the move.
pub(crate) use titan_l2_book::HostDepthBook;
pub use titan_l2_book::DepthBatchInfoV1;
