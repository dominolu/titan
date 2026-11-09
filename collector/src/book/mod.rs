//! Version 1 market depth recording format.

mod format;
mod recorder;
mod replay;
mod segment;

pub use format::{
    BlockHeader, BlockInput, BlockKind, Change, DataPayload, FeedMode, GapReason, Level,
    MarketMeta, SegmentHeader, Side, SlotRecord, decode_block, decode_compressed_block,
    decode_data_payload, decode_segment_header, encode_block, encode_compressed_block,
    encode_data_payload, encode_segment_header,
};
pub use recorder::{
    ClosedSlot, DepthMessage, Feed, FeedMessage, GapMessage, InstrumentType, MarketConfig,
    MarketRecorder, RecorderConfig, RecorderSettings, RestShadowReport, RestShadowStatus,
    RestShadowValidator, SnapshotMessage, build_data_block, decimal_steps, run_market,
};
pub use replay::Ladder;
pub use segment::{BookWriter, RecoveryReport, SegmentReader, SegmentWriter, recover_segment};

use thiserror::Error;

pub const SEGMENT_HEADER_LEN: usize = 128;
pub const BLOCK_HEADER_LEN: usize = 128;
pub const INDEX_ENTRY_LEN: usize = 64;
pub const TICK_BIAS: i64 = 32768;
pub const SEQ_UNAVAILABLE: u32 = u32::MAX;

#[derive(Debug, Error)]
pub enum BookError {
    #[error("invalid market data format: {0}")]
    InvalidFormat(&'static str),
    #[error("unsupported market data format version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid market data field: {0}")]
    InvalidField(&'static str),
    #[error("market data block is corrupt")]
    CorruptBlock,
    #[error("market data block payload is corrupt: {0}")]
    CorruptPayload(&'static str),
    #[error("market data segment is corrupt")]
    CorruptSegment,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, BookError>;
