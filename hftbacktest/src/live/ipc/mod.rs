use std::time::Duration;

use crate::{
    live::{BotError, Instrument},
    prelude::BuildError,
    types::{LiveEvent, LiveRequest},
};

mod config;
pub mod iceoryx;

pub use config::MAX_FEED_BATCH_EVENTS;

pub const TO_ALL: u64 = 0;

/// Returns the stable connector-local identifier used by the feed hot path.
///
/// Registration still carries the symbol for control-plane compatibility. Feed batches carry
/// only this identifier, avoiding symbol allocation, encoding and lookup on every market event.
#[inline]
pub fn instrument_id(symbol: &str) -> u64 {
    // FNV-1a is deliberately implemented here instead of using `Hash`, whose default hasher is
    // randomized per process. A zero value is reserved by several IPC paths.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in symbol.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    if hash == 0 { 1 } else { hash }
}

#[cfg(test)]
mod tests {
    use super::instrument_id;

    #[test]
    fn instrument_id_is_stable_and_symbol_specific() {
        assert_eq!(instrument_id("btcusdt"), instrument_id("btcusdt"));
        assert_ne!(instrument_id("btcusdt"), instrument_id("ethusdt"));
        assert_ne!(instrument_id("btcusdt"), 0);
    }
}

/// Provides the IPC communication methods.
pub trait Channel {
    /// Builds a [`Channel`] based on a list of [`Instrument`].
    fn build<MD>(instruments: &[Instrument<MD>]) -> Result<Self, BuildError>
    where
        Self: Sized;

    /// Attempts to receive a [`LiveEvent`] from all registered connectors until the specified
    /// `timeout` duration is reached.
    /// If the ID of the received message does not match the provided ID, the message will be
    /// ignored and this will attempt to receive a [`LiveEvent`] again until the timeout is reached.
    ///
    /// `(instrument_no, LiveEvent)` will be returned if the message is received.
    fn recv_timeout(&mut self, id: u64, timeout: Duration) -> Result<(usize, LiveEvent), BotError>;

    /// Sends a [`LiveRequest`] to the connector corresponding to the `inst_no`.
    fn send(&mut self, id: u64, inst_no: usize, request: LiveRequest) -> Result<(), BotError>;
}
