//! Runnable example strategies for the Titan HFT framework.
//!
//! The [`market_making`] module implements a Rust-native market-making strategy
//! (mirroring the Python example in the root README) that runs unchanged on both
//! the backtest engine and the live bot — "研究到实盘零差异".
//!
//! * `cargo run -p titan-examples --bin backtest` — backtest on synthetic or npz data.
//! * `cargo run -p titan-examples --bin live` — live trading through a running connector.

pub mod market_making;
