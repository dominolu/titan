//! The single strategy Runtime used by Titan workers.

mod runtime;
#[cfg(feature = "backtest")]
mod tick;

pub use runtime::*;
#[cfg(feature = "backtest")]
pub use tick::*;
pub use titan_runtime_abi::{Bar, Event};
