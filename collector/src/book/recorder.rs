#![allow(async_fn_in_trait)]

use std::collections::BTreeMap;

use titan_l2_book::HostDepthBook;

use crate::file::{AuxRecord, MarketEventWriter, TradeRecord};
use serde::Deserialize;

use super::segment::BookWriter;
use super::{
    BLOCK_HEADER_LEN, BlockHeader, BlockInput, BookError, Change, DataPayload, FeedMode, GapReason,
    Level, MarketMeta, SEQ_UNAVAILABLE, Side, SlotRecord, encode_data_payload,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepthMessage {
    pub exch_ts_ns: i64,
    pub local_ts_ns: i64,
    pub first_prev_seq: Option<u64>,
    /// First native update sequence covered by this message, when the venue
    /// exposes a range such as Binance's [U, u].
    pub first_seq: Option<u64>,
    pub last_seq: u64,
    pub changes: Vec<Change>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMessage {
    pub exch_ts_ns: i64,
    pub local_ts_ns: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub last_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapMessage {
    pub start_local_ns: i64,
    pub start_exch_ns: i64,
    pub end_local_ns: i64,
    pub end_exch_ns: i64,
    pub reason: GapReason,
    pub last_seq: u64,
    pub next_seq: u64,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedMessage {
    Depth(DepthMessage),
    Snapshot(SnapshotMessage),
    Trade(TradeRecord),
    Aux(AuxRecord),
    Gap(GapMessage),
}

/// A statically dispatched asynchronous source of normalized market updates.
#[allow(async_fn_in_trait)]
pub trait Feed {
    async fn next(&mut self) -> Option<FeedMessage>;
}

#[derive(Clone, Debug, Deserialize)]
pub struct RecorderConfig {
    pub recorder: RecorderSettings,
    pub markets: Vec<MarketConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RecorderSettings {
    pub root: std::path::PathBuf,
    pub grid_ms: u16,
    pub depth_levels: u16,
    pub block_duration_ms: u16,
    #[serde(default = "default_segment_duration_ms")]
    pub segment_duration_ms: u32,
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_fsync_interval")]
    pub fsync_interval_blocks: u16,
    #[serde(default = "default_oi_poll_ms")]
    pub oi_poll_ms: u32,
    #[serde(default = "default_validation_interval_ms")]
    pub validation_interval_ms: u32,
    /// Package the previous UTC date into a single zip after the day rolls over.
    #[serde(default)]
    pub daily_archive: bool,
    /// Delete archived data files after a verified daily archive.
    #[serde(default)]
    pub daily_archive_prune: bool,
    /// Grace period after UTC midnight before archiving (10-3600 seconds).
    #[serde(default = "default_archive_grace_secs")]
    pub daily_archive_grace_secs: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub market_id: u32,
    pub venue_id: u16,
    pub venue: String,
    pub instrument: InstrumentType,
    pub symbol: String,
    pub feed_mode: FeedMode,
    pub tick_units: u64,
    pub price_decimals: u8,
    pub qty_step_units: u64,
    pub qty_decimals: u8,
    pub depth_available: u16,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentType {
    Spot,
    Perpetual,
    Future,
}

/// Converts a non-negative decimal string into integer tick/quantity steps.
/// No floating-point conversion is used, and the supplied precision must match
/// the market's declared scale exactly.
pub fn decimal_steps(value: &str, decimals: u8, step_units: u64) -> Result<u64, BookError> {
    if step_units == 0 || value.is_empty() || value.starts_with('-') || value.starts_with('+') {
        return Err(BookError::InvalidField("decimal value"));
    }
    let mut parts = value.split('.');
    let whole = parts
        .next()
        .ok_or(BookError::InvalidField("decimal value"))?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > decimals as usize
    {
        return Err(BookError::InvalidField("decimal value"));
    }
    let scale = 10u64
        .checked_pow(decimals as u32)
        .ok_or(BookError::InvalidField("decimal scale"))?;
    let whole_units = whole
        .parse::<u64>()
        .map_err(|_| BookError::InvalidField("decimal range"))?
        .checked_mul(scale)
        .ok_or(BookError::InvalidField("decimal range"))?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u64>()
            .map_err(|_| BookError::InvalidField("decimal value"))?
    };
    let fraction_scale = 10u64
        .checked_pow(decimals as u32 - fraction.len() as u32)
        .ok_or(BookError::InvalidField("decimal scale"))?;
    let units = whole_units
        .checked_add(
            fraction_value
                .checked_mul(fraction_scale)
                .ok_or(BookError::InvalidField("decimal range"))?,
        )
        .ok_or(BookError::InvalidField("decimal range"))?;
    if units % step_units != 0 {
        return Err(BookError::InvalidField("decimal step alignment"));
    }
    Ok(units / step_units)
}

impl RecorderConfig {
    pub fn parse(text: &str) -> Result<Self, BookError> {
        // Read the values before defaults fill in the absent ones: a validation that only sees the
        // filled configuration cannot reject a value the operator left at a default that is itself
        // unusable (`segment_duration_ms` below).
        let explicit: toml::Value =
            toml::from_str(text).map_err(|_| BookError::InvalidField("recorder config"))?;
        let segment_duration_ms: u32 = explicit
            .get("recorder")
            .and_then(|recorder| recorder.get("segment_duration_ms"))
            .and_then(toml::Value::as_integer)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_else(default_segment_duration_ms);
        // Segments rotate on multiples of this length since the epoch while their `date` directory
        // follows the segment's start, so a length that does not divide a day puts two UTC dates
        // inside one hour-of-day boundary and the day's archive would lose one of them.
        if !divides_a_day(segment_duration_ms) {
            return Err(BookError::InvalidField("segment duration"));
        }
        let config: Self =
            toml::from_str(text).map_err(|_| BookError::InvalidField("recorder config"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), BookError> {
        let r = &self.recorder;
        if !(10..=60_000).contains(&r.grid_ms)
            || !(1..=500).contains(&r.depth_levels)
            || !(5_000..=65_000).contains(&r.block_duration_ms)
            || !(60_000..=86_400_000).contains(&r.segment_duration_ms)
            || !r.block_duration_ms.is_multiple_of(r.grid_ms)
            || !(1..=19).contains(&r.compression_level)
            || !matches!(r.compression.as_str(), "zstd" | "none")
            || !(1..=60).contains(&r.fsync_interval_blocks)
            || !(10_000..=3_600_000).contains(&r.oi_poll_ms)
            || (r.validation_interval_ms != 0 && r.validation_interval_ms < 1_000)
            || (r.daily_archive && !(10..=3_600).contains(&r.daily_archive_grace_secs))
            || self.markets.is_empty()
        {
            return Err(BookError::InvalidField("recorder config values"));
        }
        let mut ids = std::collections::BTreeSet::new();
        for m in &self.markets {
            if m.market_id == 0
                || !ids.insert(m.market_id)
                || m.venue.is_empty()
                || m.symbol.is_empty()
                || m.tick_units == 0
                || m.qty_step_units == 0
                || m.price_decimals > 19
                || m.qty_decimals > 19
                || m.depth_available == 0
                || m.depth_available > 1000
            {
                return Err(BookError::InvalidField("market config values"));
            }
            if m.feed_mode == FeedMode::Snapshot && m.depth_available > 20 {
                return Err(BookError::InvalidField("snapshot feed depth"));
            }
        }
        Ok(())
    }

    pub fn market_meta(&self, market: &MarketConfig) -> MarketMeta {
        MarketMeta {
            market_id: market.market_id,
            venue_id: market.venue_id,
            symbol: market.symbol.clone(),
            feed_mode: market.feed_mode,
            tick_units: market.tick_units,
            price_decimals: market.price_decimals,
            qty_step_units: market.qty_step_units,
            qty_decimals: market.qty_decimals,
            grid_ms: self.recorder.grid_ms,
            block_duration_ms: self.recorder.block_duration_ms,
            depth_requested: self.recorder.depth_levels,
            depth_available: market.depth_available,
            session_epoch: 0,
        }
    }
}

fn default_compression_level() -> i32 {
    3
}
fn default_compression() -> String {
    "zstd".into()
}
fn default_fsync_interval() -> u16 {
    1
}
/// Whether a segment length splits the UTC day evenly: 1 h, 2 h, 6 h and so on.
///
/// The lengths a recorder is configured with are the ones listed here, each a whole number of
/// minutes that divides a day; anything else rotates its segments off the day boundary, which is
/// what the check refuses.
fn divides_a_day(segment_ms: u32) -> bool {
    matches!(
        segment_ms,
        60_000
            | 120_000
            | 240_000
            | 300_000
            | 480_000
            | 600_000
            | 720_000
            | 900_000
            | 960_000
            | 1_200_000
            | 1_440_000
            | 1_800_000
            | 2_400_000
            | 2_880_000
            | 3_600_000
            | 4_800_000
            | 7_200_000
            | 9_600_000
            | 10_800_000
            | 14_400_000
            | 19_200_000
            | 21_600_000
            | 28_800_000
            | 43_200_000
            | 86_400_000
    )
}

fn default_segment_duration_ms() -> u32 {
    3_600_000
}
fn default_oi_poll_ms() -> u32 {
    60_000
}
fn default_validation_interval_ms() -> u32 {
    60_000
}
fn default_archive_grace_secs() -> u64 {
    120
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosedSlot {
    pub slot: i64,
    pub exch_ts_ns: i64,
    pub local_ts_ns: i64,
    pub seq_first_prev: Option<u64>,
    pub seq_last: u64,
    pub n_msgs: u16,
    pub n_msgs_saturated: bool,
    pub changes: Vec<Change>,
    pub snapshot_bids: Vec<Level>,
    pub snapshot_asks: Vec<Level>,
}

/// Packs one UTC-aligned run of closed grid slots into a self-contained v1 data
/// block. The caller supplies the window at block open and the sequence watermarks
/// captured at block open/close.
#[allow(clippy::too_many_arguments)]
pub fn build_data_block(
    meta: &MarketMeta,
    start_local_ns: i64,
    end_local_ns: i64,
    start_exch_ns: i64,
    base_tick: i64,
    seq_base: u64,
    seq_last_processed: u64,
    n_slots: u16,
    bids: Vec<Level>,
    asks: Vec<Level>,
    closed_slots: &[ClosedSlot],
) -> Result<BlockInput, BookError> {
    let grid_ns = i64::from(meta.grid_ms)
        .checked_mul(1_000_000)
        .ok_or(BookError::InvalidField("grid duration"))?;
    if meta.grid_ms == 0
        || meta.block_duration_ms == 0
        || !meta.block_duration_ms.is_multiple_of(meta.grid_ms)
        || n_slots == 0
        || start_exch_ns.rem_euclid(grid_ns) != 0
        || n_slots as u32 > u32::from(meta.block_duration_ms / meta.grid_ms)
    {
        return Err(BookError::InvalidField("block grid"));
    }
    let end_exch_ns = start_exch_ns
        .checked_add(i64::from(n_slots) * grid_ns)
        .ok_or(BookError::InvalidField("block end"))?;
    let mut records = Vec::with_capacity(closed_slots.len());
    let mut previous_delta_ms = None;
    let start_slot = start_exch_ns.div_euclid(grid_ns);
    for slot in closed_slots {
        if slot.slot < start_slot
            || slot.slot >= start_slot + i64::from(n_slots)
            || slot.exch_ts_ns < start_exch_ns
            || slot.exch_ts_ns >= end_exch_ns
        {
            return Err(BookError::InvalidField("slot outside block"));
        }
        let delta_ms = u16::try_from((slot.exch_ts_ns - start_exch_ns) / 1_000_000)
            .map_err(|_| BookError::InvalidField("exchange time delta"))?;
        if previous_delta_ms.is_some_and(|previous| delta_ms <= previous) {
            return Err(BookError::InvalidField("slot time order"));
        }
        if i64::from(delta_ms / meta.grid_ms) != slot.slot - start_slot {
            return Err(BookError::InvalidField("slot timestamp mismatch"));
        }
        previous_delta_ms = Some(delta_ms);
        let lag_us =
            i32::try_from((i128::from(slot.local_ts_ns) - i128::from(slot.exch_ts_ns)) / 1_000)
                .map_err(|_| BookError::InvalidField("local lag"))?;
        let (last_delta, first_prev_delta) = if meta.feed_mode == FeedMode::Snapshot {
            (0, SEQ_UNAVAILABLE)
        } else {
            let last = slot
                .seq_last
                .checked_sub(seq_base)
                .ok_or(BookError::InvalidField("sequence before base"))?;
            let first_prev = slot
                .seq_first_prev
                .ok_or(BookError::InvalidField("missing predecessor sequence"))?
                .checked_sub(seq_base)
                .ok_or(BookError::InvalidField("predecessor before base"))?;
            let last =
                u32::try_from(last).map_err(|_| BookError::InvalidField("sequence delta"))?;
            let first_prev = u32::try_from(first_prev)
                .map_err(|_| BookError::InvalidField("predecessor delta"))?;
            if last == SEQ_UNAVAILABLE || first_prev == SEQ_UNAVAILABLE {
                return Err(BookError::InvalidField("sequence sentinel"));
            }
            (last, first_prev)
        };
        records.push(SlotRecord {
            local_lag_us: lag_us,
            seq_last_delta: last_delta,
            seq_first_prev_delta: first_prev_delta,
            exch_delta_ms: delta_ms,
            n_msgs: slot.n_msgs,
            changes: slot.changes.clone(),
        });
    }
    let last_processed_delta = if meta.feed_mode == FeedMode::Snapshot {
        0
    } else {
        let delta = seq_last_processed
            .checked_sub(seq_base)
            .ok_or(BookError::InvalidField("sequence watermark before base"))?;
        let delta = u32::try_from(delta)
            .map_err(|_| BookError::InvalidField("sequence watermark delta"))?;
        if delta == SEQ_UNAVAILABLE {
            return Err(BookError::InvalidField("sequence watermark sentinel"));
        }
        delta
    };
    let mut header = BlockHeader::data(meta.market_id, start_local_ns, start_exch_ns, 0)?;
    header.block_end_local_ns = end_local_ns;
    header.block_end_exch_ns = end_exch_ns;
    header.n_slots = n_slots;
    if closed_slots.iter().any(|slot| slot.n_msgs_saturated) {
        header.block_flags |= 1 << 4;
    }
    header.seq_base = if meta.feed_mode == FeedMode::Snapshot {
        0
    } else {
        seq_base
    };
    header.seq_last_processed_delta = last_processed_delta;
    let payload = DataPayload {
        bids,
        asks,
        records,
    };
    let block = encode_data_payload(header, base_tick, &payload)?;
    if block.header.block_len < BLOCK_HEADER_LEN as u32 {
        return Err(BookError::InvalidField("block length"));
    }
    Ok(block)
}

/// Runs one normalized market feed to EOF and packs the net diff of every
/// changed closed slot into `block_duration_ms` (default 60 s) self-contained
/// blocks: the first slot of a block contributes the full window snapshot and
/// the following slots only their net window diff. Feed adapters must yield a
/// snapshot before diff messages and must end the feed when a reconnect
/// requires resynchronizing.
pub async fn run_market<F: Feed>(
    mut meta: MarketMeta,
    root: &std::path::Path,
    venue: &str,
    instrument: &str,
    settings: &RecorderSettings,
    mut feed: F,
) -> Result<(), BookError> {
    if settings.grid_ms != meta.grid_ms
        || settings.block_duration_ms != meta.block_duration_ms
        || !(60_000..=86_400_000).contains(&settings.segment_duration_ms)
        || !(1..=19).contains(&settings.compression_level)
        || !matches!(settings.compression.as_str(), "zstd" | "none")
    {
        return Err(BookError::InvalidField("recorder settings mismatch"));
    }
    let mut writer = BookWriter::new_with_options(
        root,
        venue,
        instrument,
        meta.clone(),
        settings.segment_duration_ms,
        settings.compression_level,
        settings.compression == "zstd",
        settings.fsync_interval_blocks,
    )?;
    let mut recorder = MarketRecorder::new(meta.clone())?;
    let mut aggregator = SlotAggregator::new(&meta)?;
    let mut event_writer = MarketEventWriter::new();
    let mut initialized = false;
    let mut last_times = (0, 0);
    let idle_duration = std::time::Duration::from_millis(u64::from(meta.grid_ms));
    let mut idle =
        tokio::time::interval_at(tokio::time::Instant::now() + idle_duration, idle_duration);
    let mut next = Box::pin(feed.next());
    loop {
        let outcome = tokio::select! {
            result = &mut next => Some(result),
            _ = idle.tick() => None,
        };
        let message = match outcome {
            Some(result) => {
                drop(next);
                next = Box::pin(feed.next());
                match result {
                    Some(message) => message,
                    None => break,
                }
            }
            None => {
                if let Some(slot) = recorder.flush_open_slot()? {
                    write_closed_slot(&meta, &mut writer, slot, &mut aggregator, &mut last_times)?;
                }
                continue;
            }
        };
        let closed = match message {
            FeedMessage::Gap(gap) => {
                if let Some(block) = aggregator.flush(&meta)? {
                    append_block(&mut writer, block, &mut last_times)?;
                }
                writer.append_gap(
                    gap.start_local_ns,
                    gap.start_exch_ns,
                    gap.end_local_ns,
                    gap.end_exch_ns,
                    gap.reason,
                    gap.last_seq,
                    gap.next_seq,
                    gap.detail.as_bytes(),
                )?;
                meta.session_epoch = meta
                    .session_epoch
                    .checked_add(1)
                    .ok_or(BookError::InvalidField("session epoch overflow"))?;
                recorder = MarketRecorder::new(meta.clone())?;
                initialized = false;
                last_times = (gap.end_local_ns, gap.end_exch_ns);
                Vec::new()
            }
            FeedMessage::Trade(trade) => {
                event_writer.write_trade(root, venue, instrument, &meta.symbol, &trade)?;
                Vec::new()
            }
            FeedMessage::Aux(aux) => {
                if instrument == "spot" {
                    return Err(BookError::InvalidField("aux event for spot market"));
                }
                event_writer.write_aux(root, venue, instrument, &meta.symbol, &aux)?;
                Vec::new()
            }
            FeedMessage::Snapshot(snapshot) if !initialized && meta.feed_mode == FeedMode::Diff => {
                recorder.install_snapshot(&snapshot.bids, &snapshot.asks, snapshot.last_seq)?;
                initialized = true;
                Vec::new()
            }
            FeedMessage::Snapshot(snapshot) if meta.feed_mode == FeedMode::Snapshot => {
                initialized = true;
                recorder.apply_snapshot_message(snapshot)?
            }
            FeedMessage::Snapshot(_) => {
                return Err(BookError::InvalidField("unexpected resync snapshot"));
            }
            FeedMessage::Depth(depth) => {
                initialized = true;
                recorder.apply(depth)?
            }
        };
        for slot in closed {
            write_closed_slot(&meta, &mut writer, slot, &mut aggregator, &mut last_times)?;
        }
    }
    if let Some(slot) = recorder.flush_open_slot()? {
        write_closed_slot(&meta, &mut writer, slot, &mut aggregator, &mut last_times)?;
    }
    if let Some(block) = aggregator.flush(&meta)? {
        append_block(&mut writer, block, &mut last_times)?;
    }
    writer.finish(last_times.0, last_times.1)?;
    event_writer.finish()?;
    Ok(())
}

/// Maximum tick span inside one block, measured from the block-start snapshot
/// through every recorded change. Mirrors the design's default
/// `rebase_threshold_ticks` (30000): the slot that would exceed it seals the
/// pending block first, so the next block starts with a fresh snapshot and a
/// fresh base tick.
const REBASE_THRESHOLD_TICKS: i64 = 30_000;

fn grid_ns(meta: &MarketMeta) -> Result<i64, BookError> {
    i64::from(meta.grid_ms)
        .checked_mul(1_000_000)
        .ok_or(BookError::InvalidField("grid duration"))
}

fn tick_bounds(
    snapshot_bids: &[Level],
    snapshot_asks: &[Level],
    changes: &[Change],
) -> Result<(i64, i64), BookError> {
    let mut bounds: Option<(i64, i64)> = None;
    let mut include = |tick: i64| {
        bounds = Some(match bounds {
            Some((min, max)) => (min.min(tick), max.max(tick)),
            None => (tick, tick),
        });
    };
    for level in snapshot_bids.iter().chain(snapshot_asks) {
        include(level.tick);
    }
    for change in changes {
        include(change.tick);
    }
    bounds.ok_or(BookError::InvalidField("empty slot window"))
}

/// One run of closed grid slots that is being packed into a self-contained
/// block. The block-start window snapshot is captured from the first slot; the
/// later slots only contribute their net window diff, so a block stores one
/// snapshot per `block_duration_ms` instead of one per 100 ms grid slot.
struct PendingBlock {
    start_slot: i64,
    start_local_ns: i64,
    start_exch_ns: i64,
    snapshot_bids: Vec<Level>,
    snapshot_asks: Vec<Level>,
    seq_base: u64,
    records: Vec<ClosedSlot>,
    min_tick: i64,
    max_tick: i64,
    last_slot: i64,
    end_local_ns: i64,
}

impl PendingBlock {
    fn open(meta: &MarketMeta, mut slot: ClosedSlot) -> Result<Self, BookError> {
        let grid_ns = grid_ns(meta)?;
        let start_exch_ns = slot
            .slot
            .checked_mul(grid_ns)
            .ok_or(BookError::InvalidField("slot timestamp"))?;
        let seq_base = if meta.feed_mode == FeedMode::Snapshot {
            0
        } else {
            slot.seq_first_prev
                .ok_or(BookError::InvalidField("missing slot predecessor"))?
        };
        let (min_tick, max_tick) =
            tick_bounds(&slot.snapshot_bids, &slot.snapshot_asks, &slot.changes)?;
        let snapshot_bids = std::mem::take(&mut slot.snapshot_bids);
        let snapshot_asks = std::mem::take(&mut slot.snapshot_asks);
        let start_local_ns = slot.local_ts_ns;
        let last_slot = slot.slot;
        Ok(Self {
            start_slot: slot.slot,
            start_local_ns,
            start_exch_ns,
            snapshot_bids,
            snapshot_asks,
            seq_base,
            records: vec![slot],
            min_tick,
            max_tick,
            last_slot,
            end_local_ns: start_local_ns,
        })
    }

    fn coverage(&self) -> i64 {
        self.last_slot - self.start_slot + 1
    }

    fn span_with(&self, changes: &[Change]) -> i64 {
        let mut min_tick = self.min_tick;
        let mut max_tick = self.max_tick;
        for change in changes {
            min_tick = min_tick.min(change.tick);
            max_tick = max_tick.max(change.tick);
        }
        max_tick - min_tick
    }

    fn add(&mut self, mut slot: ClosedSlot) -> Result<(), BookError> {
        if slot.slot <= self.last_slot {
            return Err(BookError::InvalidField("slot order"));
        }
        let (min_tick, max_tick) =
            tick_bounds(&slot.snapshot_bids, &slot.snapshot_asks, &slot.changes)?;
        self.min_tick = self.min_tick.min(min_tick);
        self.max_tick = self.max_tick.max(max_tick);
        // Later slots are net diff records relative to the previous window, so
        // their per-slot snapshot copies are not needed once bounds are counted.
        slot.snapshot_bids = Vec::new();
        slot.snapshot_asks = Vec::new();
        self.last_slot = slot.slot;
        self.end_local_ns = slot.local_ts_ns;
        self.records.push(slot);
        Ok(())
    }

    fn into_block(self, meta: &MarketMeta, n_slots: i64) -> Result<BlockInput, BookError> {
        let n_slots =
            u16::try_from(n_slots).map_err(|_| BookError::InvalidField("block slot count"))?;
        let base_tick = i64::try_from((i128::from(self.min_tick) + i128::from(self.max_tick)) / 2)
            .map_err(|_| BookError::InvalidField("base tick"))?;
        let seq_last_processed = self
            .records
            .last()
            .map(|slot| slot.seq_last)
            .unwrap_or(self.seq_base);
        build_data_block(
            meta,
            self.start_local_ns,
            self.end_local_ns,
            self.start_exch_ns,
            base_tick,
            self.seq_base,
            seq_last_processed,
            n_slots,
            self.snapshot_bids,
            self.snapshot_asks,
            &self.records,
        )
    }
}

/// Packs closed slots into self-contained blocks whose window is bounded by
/// `block_duration_ms`. A block is sealed when the next slot falls outside its
/// grid window, when a tick would exceed the rebase threshold, at a gap, or at
/// the end of the feed.
struct SlotAggregator {
    slots_per_block: i64,
    pending: Option<PendingBlock>,
}

impl SlotAggregator {
    fn new(meta: &MarketMeta) -> Result<Self, BookError> {
        if meta.grid_ms == 0 || !meta.block_duration_ms.is_multiple_of(meta.grid_ms) {
            return Err(BookError::InvalidField("block aggregation window"));
        }
        Ok(Self {
            slots_per_block: i64::from(meta.block_duration_ms / meta.grid_ms),
            pending: None,
        })
    }

    /// Accepts the next closed slot and returns a sealed block when one must be
    /// appended before the slot can be buffered.
    fn push(
        &mut self,
        meta: &MarketMeta,
        slot: ClosedSlot,
    ) -> Result<Option<BlockInput>, BookError> {
        let mut sealed = None;
        let seal = match self.pending.as_ref() {
            Some(pending) => {
                if slot.slot < pending.last_slot {
                    return Err(BookError::InvalidField("slot order"));
                }
                let crossed = slot.slot - pending.start_slot >= self.slots_per_block;
                let rebase = pending.span_with(&slot.changes) > REBASE_THRESHOLD_TICKS;
                if crossed {
                    Some((true, 0))
                } else if rebase {
                    Some((false, pending.coverage()))
                } else {
                    None
                }
            }
            None => None,
        };
        if let Some((crossed, coverage)) = seal {
            let pending = self
                .pending
                .take()
                .ok_or(BookError::InvalidField("pending block state"))?;
            let n_slots = if crossed {
                self.slots_per_block
            } else {
                coverage
            };
            sealed = Some(pending.into_block(meta, n_slots)?);
        }
        let pending = match self.pending.take() {
            Some(mut pending) => {
                pending.add(slot)?;
                pending
            }
            None => PendingBlock::open(meta, slot)?,
        };
        if pending.coverage() >= self.slots_per_block && sealed.is_none() {
            let n_slots = pending.coverage();
            sealed = Some(pending.into_block(meta, n_slots)?);
        } else {
            // A block sealed at the start of this push may already fill the
            // window (only when block_duration_ms == grid_ms); keep this slot
            // buffered so the next push or the final flush seals it.
            self.pending = Some(pending);
        }
        Ok(sealed)
    }

    fn flush(&mut self, meta: &MarketMeta) -> Result<Option<BlockInput>, BookError> {
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        let n_slots = pending.coverage();
        Ok(Some(pending.into_block(meta, n_slots)?))
    }
}

fn write_closed_slot(
    meta: &MarketMeta,
    writer: &mut BookWriter,
    slot: ClosedSlot,
    aggregator: &mut SlotAggregator,
    last_times: &mut (i64, i64),
) -> Result<(), BookError> {
    if let Some(block) = aggregator.push(meta, slot)? {
        append_block(writer, block, last_times)?;
    }
    Ok(())
}

fn append_block(
    writer: &mut BookWriter,
    block: BlockInput,
    last_times: &mut (i64, i64),
) -> Result<(), BookError> {
    writer.append(&block)?;
    *last_times = (
        block.header.block_end_local_ns,
        block.header.block_end_exch_ns,
    );
    Ok(())
}

/// Applies normalized deltas in feed order and produces one net window diff
/// whenever a grid slot closes. Full-depth state is retained so levels entering
/// or leaving the Top-N window are represented as ordinary net changes.
pub struct MarketRecorder {
    meta: MarketMeta,
    /// The window's levels, maintained by the reconstruction the live runtime also uses: merging,
    /// sequence order and the snapshot's coverage rule are one implementation, not two.
    book: HostDepthBook,
    previous_window: Option<(Vec<Level>, Vec<Level>)>,
    current_slot: Option<i64>,
    last_closed_slot: Option<i64>,
    slot_exch_ts: i64,
    slot_local_ts: i64,
    slot_first_prev: Option<u64>,
    slot_last_seq: u64,
    slot_n_msgs: u64,
    last_clamped_exch_ts: Option<i64>,
    last_seq: Option<u64>,
    /// The stream epoch the shared book is applying: a snapshot starts a new one.
    book_epoch: u64,
}

impl MarketRecorder {
    pub fn new(meta: MarketMeta) -> Result<Self, BookError> {
        if meta.grid_ms == 0
            || meta.depth_requested == 0
            || meta.depth_available == 0
            || meta.block_duration_ms == 0
            || !meta.block_duration_ms.is_multiple_of(meta.grid_ms)
            || meta.tick_units == 0
            || meta.qty_step_units == 0
        {
            return Err(BookError::InvalidField("market metadata"));
        }
        Ok(Self {
            book: HostDepthBook::with_levels(
                usize::from(meta.depth_requested.min(meta.depth_available)).max(1),
                true,
            ),
            meta,
            previous_window: None,
            current_slot: None,
            last_closed_slot: None,
            slot_exch_ts: 0,
            slot_local_ts: 0,
            slot_first_prev: None,
            slot_last_seq: 0,
            slot_n_msgs: 0,
            last_clamped_exch_ts: None,
            last_seq: None,
            book_epoch: 0,
        })
    }

    pub fn install_snapshot(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        seq: Option<u64>,
    ) -> Result<(), BookError> {
        validate_levels(bids, Side::Bid)?;
        validate_levels(asks, Side::Ask)?;
        // The epoch follows the message's stream: a snapshot starts a new one.
        self.book_epoch = self.book_epoch.saturating_add(1);
        let bids: Vec<(i64, i64)> = bids.iter().map(|l| (l.tick, l.qty as i64)).collect();
        let asks: Vec<(i64, i64)> = asks.iter().map(|l| (l.tick, l.qty as i64)).collect();
        self.book
            .apply_snapshot(self.book_epoch, seq.unwrap_or_default(), &bids, &asks)
            .map_err(BookError::InvalidField)?;
        self.previous_window = Some(self.window());
        self.last_seq = seq;
        Ok(())
    }

    pub fn apply(&mut self, message: DepthMessage) -> Result<Vec<ClosedSlot>, BookError> {
        if self.meta.feed_mode != FeedMode::Diff {
            return Err(BookError::InvalidField("diff message for snapshot feed"));
        }
        if self.previous_window.is_none() {
            return Err(BookError::InvalidField("depth snapshot required"));
        }
        // A delta that names no predecessor asks not to be sequence-checked. That is the shape a
        // feed takes when something upstream already guarantees continuity — the connector checks
        // `pu` and publishes only a delta it accepted — so the recorder applies the change and
        // leaves the chain to that owner rather than repeating a weaker copy of it.
        if let Some(previous) = self.last_seq.filter(|_| message.first_prev_seq.is_some()) {
            // A message that ends at or before the last one applied carries nothing the book does
            // not have: whether it names that message's predecessor or an older one, it is a replay
            // of a state the book has moved past. Applying it would put an outdated change on top
            // of newer ones, so it is counted and dropped.
            if message.last_seq <= previous {
                return Ok(Vec::new());
            }
            if message.first_prev_seq != Some(previous) {
                // The two sequences are the whole diagnosis: the book's position and the message's
                // predecessor decide whether this is a skip or a stale reader.
                eprintln!(
                    "sequence gap: book at {previous}, message [{}..{}] predecessor {:?}",
                    message.first_seq.unwrap_or_default(),
                    message.last_seq,
                    message.first_prev_seq
                );
                return Err(BookError::InvalidField("sequence gap"));
            }
        }
        let grid_ns = i64::from(self.meta.grid_ms) * 1_000_000;
        let clamped = self
            .last_clamped_exch_ts
            .map_or(message.exch_ts_ns, |last| last.max(message.exch_ts_ns));
        let raw_slot = clamped.div_euclid(grid_ns);
        let minimum_slot = self
            .current_slot
            .or_else(|| self.last_closed_slot.map(|last| last.saturating_add(1)));
        let slot = minimum_slot.map_or(raw_slot, |minimum| raw_slot.max(minimum));
        // A message stamped behind the slot the book is in still carries a change the book has yet
        // to apply. Dropping it would leave the book behind every message after it, which is a
        // disagreement with the venue rather than a timestamp, so it goes into the open slot.
        let mut closed = Vec::new();
        if let Some(current) = self.current_slot
            && slot > current
            && let Some(record) = self.close_slot(current)?
        {
            closed.push(record);
        }
        if self.current_slot.is_some_and(|current| slot > current) {
            self.last_closed_slot = self.current_slot;
        }
        if self.current_slot != Some(slot) {
            self.current_slot = Some(slot);
            self.slot_n_msgs = 0;
            self.slot_first_prev = None;
        }
        let prev_seq = message.first_prev_seq.or(self.last_seq);
        if self.slot_first_prev.is_none() {
            self.slot_first_prev = prev_seq;
        }
        self.apply_changes(&message.changes)?;
        self.slot_exch_ts = clamped.max(slot.saturating_mul(grid_ns));
        self.slot_local_ts = message.local_ts_ns;
        self.slot_last_seq = message.last_seq;
        self.slot_n_msgs = self.slot_n_msgs.saturating_add(1);
        self.last_seq = Some(message.last_seq);
        self.last_clamped_exch_ts = Some(self.slot_exch_ts);
        Ok(closed)
    }

    pub fn apply_snapshot_message(
        &mut self,
        message: SnapshotMessage,
    ) -> Result<Vec<ClosedSlot>, BookError> {
        if self.meta.feed_mode != FeedMode::Snapshot {
            return Err(BookError::InvalidField("snapshot message for diff feed"));
        }
        validate_levels(&message.bids, Side::Bid)?;
        validate_levels(&message.asks, Side::Ask)?;
        let grid_ns = i64::from(self.meta.grid_ms) * 1_000_000;
        let clamped = self
            .last_clamped_exch_ts
            .map_or(message.exch_ts_ns, |last| last.max(message.exch_ts_ns));
        let raw_slot = clamped.div_euclid(grid_ns);
        let minimum_slot = self
            .current_slot
            .or_else(|| self.last_closed_slot.map(|last| last.saturating_add(1)));
        let slot = minimum_slot.map_or(raw_slot, |minimum| raw_slot.max(minimum));
        let mut closed = Vec::new();
        if let Some(current) = self.current_slot
            && slot > current
            && let Some(record) = self.close_slot(current)?
        {
            closed.push(record);
        }
        if self.current_slot.is_some_and(|current| slot > current) {
            self.last_closed_slot = self.current_slot;
        }
        if self.current_slot != Some(slot) {
            self.current_slot = Some(slot);
            self.slot_n_msgs = 0;
        }
        // A snapshot feed replaces the book outright, as its own stream epoch.
        self.book_epoch = self.book_epoch.saturating_add(1);
        let bids: Vec<(i64, i64)> = message
            .bids
            .iter()
            .map(|l| (l.tick, l.qty as i64))
            .collect();
        let asks: Vec<(i64, i64)> = message
            .asks
            .iter()
            .map(|l| (l.tick, l.qty as i64))
            .collect();
        self.book
            .apply_snapshot(
                self.book_epoch,
                message.last_seq.unwrap_or_default(),
                &bids,
                &asks,
            )
            .map_err(BookError::InvalidField)?;
        if self.previous_window.is_none() {
            self.previous_window = Some(self.window());
        }
        self.slot_exch_ts = clamped.max(slot.saturating_mul(grid_ns));
        self.slot_local_ts = message.local_ts_ns;
        self.slot_n_msgs = self.slot_n_msgs.saturating_add(1);
        self.last_clamped_exch_ts = Some(self.slot_exch_ts);
        Ok(closed)
    }

    pub fn flush_open_slot(&mut self) -> Result<Option<ClosedSlot>, BookError> {
        let Some(slot) = self.current_slot else {
            return Ok(None);
        };
        let closed = self.close_slot(slot)?;
        self.last_closed_slot = Some(slot);
        self.current_slot = None;
        self.slot_n_msgs = 0;
        self.slot_first_prev = None;
        Ok(closed)
    }

    pub fn window(&self) -> (Vec<Level>, Vec<Level>) {
        let n = self.meta.depth_requested.min(self.meta.depth_available) as usize;
        let (all_bids, all_asks) = self.book.retained();
        let bids = all_bids
            .iter()
            .rev()
            .take(n)
            .map(|(&tick, &qty)| Level {
                tick,
                qty: qty as u64,
            })
            .collect();
        let asks = all_asks
            .iter()
            .take(n)
            .map(|(&tick, &qty)| Level {
                tick,
                qty: qty as u64,
            })
            .collect();
        (bids, asks)
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    fn close_slot(&mut self, slot: i64) -> Result<Option<ClosedSlot>, BookError> {
        let current = self.window();
        let previous = self
            .previous_window
            .as_ref()
            .ok_or(BookError::InvalidField("depth snapshot required"))?;
        let mut changes = diff_side(&previous.0, &current.0, Side::Bid);
        changes.extend(diff_side(&previous.1, &current.1, Side::Ask));
        let snapshot_bids = previous.0.clone();
        let snapshot_asks = previous.1.clone();
        self.previous_window = Some(current);
        if changes.is_empty() {
            return Ok(None);
        }
        Ok(Some(ClosedSlot {
            slot,
            exch_ts_ns: self.slot_exch_ts,
            local_ts_ns: self.slot_local_ts,
            seq_first_prev: self.slot_first_prev,
            seq_last: self.slot_last_seq,
            n_msgs: self.slot_n_msgs.min(u16::MAX as u64) as u16,
            n_msgs_saturated: self.slot_n_msgs > u64::from(u16::MAX),
            changes,
            snapshot_bids,
            snapshot_asks,
        }))
    }

    fn apply_changes(&mut self, changes: &[Change]) -> Result<(), BookError> {
        let items: Vec<(u8, i64, i64)> = changes
            .iter()
            .map(|change| {
                let side = match change.side {
                    Side::Bid => 1,
                    Side::Ask => 2,
                };
                (side, change.tick, change.qty as i64)
            })
            .collect();
        let last = self.slot_last_seq;
        self.book
            .apply_changes(self.book_epoch, last, last, &items)
            .map(|_| ())
            .map_err(BookError::InvalidField)
    }
}

fn validate_levels(levels: &[Level], side: Side) -> Result<(), BookError> {
    if levels.iter().any(|l| l.qty == 0) {
        return Err(BookError::InvalidField("snapshot quantity"));
    }
    let ordered = levels.windows(2).all(|pair| match side {
        Side::Bid => pair[0].tick > pair[1].tick,
        Side::Ask => pair[0].tick < pair[1].tick,
    });
    if !ordered {
        return Err(BookError::InvalidField("snapshot ordering"));
    }
    Ok(())
}

fn diff_side(previous: &[Level], current: &[Level], side: Side) -> Vec<Change> {
    let before: BTreeMap<_, _> = previous.iter().map(|l| (l.tick, l.qty)).collect();
    let after: BTreeMap<_, _> = current.iter().map(|l| (l.tick, l.qty)).collect();
    before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|tick| match (before.get(&tick), after.get(&tick)) {
            (Some(_), None) => Some(Change { side, tick, qty: 0 }),
            (None, Some(qty)) => Some(Change {
                side,
                tick,
                qty: *qty,
            }),
            (Some(a), Some(b)) if a != b => Some(Change {
                side,
                tick,
                qty: *b,
            }),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{Ladder, SegmentReader, decode_data_payload};

    /// A feed that yields the messages it was given and then ends.
    struct Scripted {
        messages: std::collections::VecDeque<FeedMessage>,
    }

    impl Feed for Scripted {
        async fn next(&mut self) -> Option<FeedMessage> {
            self.messages.pop_front()
        }
    }

    /// Reads back the records of every data block the recorder wrote.
    fn recorded_records(root: &std::path::Path) -> Vec<(SlotRecord, crate::book::BlockHeader)> {
        let mut records = Vec::new();
        let mut directories = vec![root.join("book")];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    directories.push(path);
                } else if path.extension().is_some_and(|ext| ext == "tnmd") {
                    let reader = SegmentReader::open(&path).unwrap();
                    for index in 0..reader.block_count() {
                        let block = reader.read_block(index).unwrap();
                        if block.header.kind == crate::book::BlockKind::Data {
                            records.extend(
                                decode_data_payload(&block)
                                    .unwrap()
                                    .records
                                    .into_iter()
                                    .map(|record| (record, block.header.clone())),
                            );
                        }
                    }
                }
            }
        }
        records
    }

    fn settings(root: &std::path::Path) -> RecorderSettings {
        RecorderSettings {
            root: root.to_path_buf(),
            grid_ms: 100,
            depth_levels: 2,
            block_duration_ms: 1000,
            segment_duration_ms: 60_000,
            compression_level: 1,
            compression: "none".into(),
            fsync_interval_blocks: 1,
            validation_interval_ms: 60_000,
            daily_archive: false,
            daily_archive_prune: false,
            daily_archive_grace_secs: 10,
            oi_poll_ms: 60_000,
        }
    }

    fn delta(exch_ts_ns: i64, first_prev: u64, last: u64, tick: i64, qty: u64) -> FeedMessage {
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns,
            local_ts_ns: exch_ts_ns,
            first_prev_seq: Some(first_prev),
            first_seq: Some(first_prev + 1),
            last_seq: last,
            changes: vec![Change {
                side: Side::Bid,
                tick,
                qty,
            }],
        })
    }

    #[tokio::test]
    async fn trades_the_adapter_produces_reach_the_trade_directory() {
        // A venue that publishes through `FeedBatch` (OKX, Hyperliquid) has its trades converted by
        // the adapter into `FeedMessage::Trade`. The recorder is what decides whether that record
        // is stored at all: `data/trade/` is the only place the quality report counts trades from,
        // so a trade that does not land there is a day reported as having no trades.
        let root = std::env::temp_dir().join(format!("titan-trades-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let trade = |exch_ts_ns: i64, side, price: &str, qty: &str| {
            FeedMessage::Trade(crate::file::TradeRecord {
                exch_ts_ns,
                local_ts_ns: exch_ts_ns,
                side,
                price: price.to_owned(),
                qty: qty.to_owned(),
                trade_id: None,
            })
        };
        let feed = Scripted {
            messages: std::collections::VecDeque::from(vec![
                FeedMessage::Snapshot(SnapshotMessage {
                    exch_ts_ns: 1_000_000_000,
                    local_ts_ns: 1_000_000_000,
                    bids: vec![Level { tick: 100, qty: 1 }],
                    asks: vec![Level { tick: 101, qty: 1 }],
                    last_seq: Some(10),
                }),
                trade(
                    1_000_000_000,
                    crate::file::TradeSide::Buy,
                    "64757.6",
                    "213.28",
                ),
                trade(1_000_100_000, crate::file::TradeSide::Sell, "64758", "0.5"),
            ]),
        };
        run_market(meta(), &root, "okx", "perpetual", &settings(&root), feed)
            .await
            .unwrap();
        let directory = root
            .join("trade/venue=okx/instrument=perpetual/symbol=BTCUSDT")
            .join("date=1970-01-01");
        let mut files: Vec<_> = std::fs::read_dir(&directory)
            .expect("the trade directory")
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.to_string_lossy().ends_with(".ndjson.gz"))
            .collect();
        files.sort();
        assert_eq!(files.len(), 1, "{files:?}");
        let mut text = String::new();
        std::io::Read::read_to_string(
            &mut flate2::read::MultiGzDecoder::new(std::fs::File::open(&files[0]).unwrap()),
            &mut text,
        )
        .unwrap();
        let records: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["side"], "buy");
        assert_eq!(records[0]["price"], "64757.6");
        assert_eq!(records[0]["qty"], "213.28");
        assert_eq!(records[1]["side"], "sell");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn a_late_frame_is_written_and_the_recorded_book_stays_with_the_stream() {
        // The transport hands frames over as they arrive, so a frame the venue stamped for an
        // earlier slot can follow one stamped later while the sequence chain still follows. Here
        // the late frame carries one further bid quantity at the same price: a reader replaying the
        // net diffs ends on whichever quantity the last record wrote, which is why order matters.
        let root = std::env::temp_dir().join(format!("titan-late-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        // Slot 10 fills at 1.0 s. The frame for slot 11 (1.1 s) arrives after the one for slot 12
        // (1.3 s), so the book has already moved on when it is read. It follows the frame that
        // overtook it, as the venue's sequence says; only its stamp is behind.
        let late = DepthMessage {
            exch_ts_ns: 1_100_000_000,
            local_ts_ns: 1_400_000_000,
            first_prev_seq: Some(12),
            first_seq: Some(13),
            last_seq: 13,
            changes: vec![Change {
                side: Side::Bid,
                tick: 100,
                qty: 3,
            }],
        };
        let feed = Scripted {
            messages: std::collections::VecDeque::from(vec![
                FeedMessage::Snapshot(SnapshotMessage {
                    exch_ts_ns: 1_000_000_000,
                    local_ts_ns: 1_000_000_000,
                    bids: vec![Level { tick: 100, qty: 1 }],
                    asks: vec![Level { tick: 101, qty: 1 }],
                    last_seq: Some(10),
                }),
                delta(1_000_000_000, 10, 11, 100, 2),
                delta(1_300_000_000, 11, 12, 100, 4),
                FeedMessage::Depth(late),
            ]),
        };
        let mut market = meta();
        market.depth_requested = 2;
        market.depth_available = 2;
        run_market(market, &root, "venue", "spot", &settings(&root), feed)
            .await
            .unwrap();
        let records = recorded_records(&root);
        let quantities: Vec<u64> = records
            .iter()
            .flat_map(|(record, _)| record.changes.iter())
            .filter(|change| change.side == Side::Bid)
            .map(|change| change.qty)
            .collect();
        // Every message reached the file once, sequenced as the stream carried them, so a reader
        // replaying the diffs ends on the state the stream ended with. The late one was applied
        // after the one stamped ahead of it, which is the only order that keeps the book and the
        // venue's last update in step.
        assert_eq!(quantities, vec![2, 3], "each message is written once");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_replayed_update_is_counted_late_and_leaves_the_book_alone() {
        let mut recorder = MarketRecorder::new(meta()).unwrap();
        recorder
            .install_snapshot(
                &[Level { tick: 100, qty: 1 }],
                &[Level { tick: 101, qty: 1 }],
                Some(10),
            )
            .unwrap();
        recorder
            .apply(DepthMessage {
                exch_ts_ns: 100_000_000,
                local_ts_ns: 100_000_000,
                first_prev_seq: Some(10),
                first_seq: Some(11),
                last_seq: 12,
                changes: vec![Change {
                    side: Side::Bid,
                    tick: 100,
                    qty: 2,
                }],
            })
            .unwrap();
        // The same update again, now that the book stands at 12: its predecessor is still the 10 it
        // named, so it is a replay, and an outdated change must not be applied on top of a newer
        // one.
        let nothing = recorder
            .apply(DepthMessage {
                exch_ts_ns: 100_000_000,
                local_ts_ns: 100_000_000,
                first_prev_seq: Some(10),
                first_seq: Some(11),
                last_seq: 12,
                changes: vec![Change {
                    side: Side::Bid,
                    tick: 100,
                    qty: 9,
                }],
            })
            .unwrap();
        assert!(nothing.is_empty());
        assert_eq!(recorder.window().0, vec![Level { tick: 100, qty: 2 }]);
        recorder.flush_open_slot().unwrap();
    }

    fn meta() -> MarketMeta {
        MarketMeta {
            market_id: 1,
            venue_id: 1,
            symbol: "BTCUSDT".into(),
            feed_mode: FeedMode::Diff,
            tick_units: 1,
            price_decimals: 0,
            qty_step_units: 1,
            qty_decimals: 0,
            grid_ms: 100,
            block_duration_ms: 1000,
            depth_requested: 2,
            depth_available: 2,
            session_epoch: 1,
        }
    }

    /// The stored form is a block snapshot plus one net diff per closed slot. This replays that
    /// form independently of the recorder's own book and checks every slot: what a reader
    /// reconstructs has to be exactly the window the recorder held when the slot closed.
    #[test]
    fn snapshot_plus_slot_diffs_rebuild_every_window() {
        let mut market = meta();
        market.block_duration_ms = 1000;
        market.depth_requested = 5;
        market.depth_available = 5;
        let mut recorder = MarketRecorder::new(market.clone()).unwrap();
        let snapshot_bids = vec![
            Level { tick: 100, qty: 10 },
            Level { tick: 99, qty: 20 },
            Level { tick: 98, qty: 30 },
        ];
        let snapshot_asks = vec![Level { tick: 101, qty: 11 }, Level { tick: 102, qty: 21 }];
        recorder
            .install_snapshot(&snapshot_bids, &snapshot_asks, Some(1))
            .unwrap();
        // Windows the recorder holds after each slot closes, collected as slots are produced.
        let mut closed: Vec<ClosedSlot> = Vec::new();
        let mut expected: Vec<(i64, Vec<Level>, Vec<Level>)> = Vec::new();
        let mut previous = 1u64;
        // Ten slots of traffic: adds, changes and removals on both sides.
        type SlotScript = (i64, Vec<(Side, i64, u64)>);
        let script: Vec<SlotScript> = vec![
            (0, vec![(Side::Bid, 100, 12)]),
            (1, vec![(Side::Ask, 101, 13), (Side::Ask, 103, 33)]),
            (2, vec![(Side::Bid, 97, 40)]),
            (3, vec![(Side::Ask, 103, 0)]),
            (4, vec![(Side::Bid, 98, 0), (Side::Bid, 96, 50)]),
            (5, vec![(Side::Bid, 100, 15)]),
            (6, vec![(Side::Ask, 104, 60)]),
            (7, vec![(Side::Bid, 95, 70), (Side::Bid, 97, 0)]),
            (8, vec![(Side::Ask, 102, 25)]),
            (9, vec![(Side::Ask, 105, 80)]),
        ];
        for (local, (slot_index, changes)) in script.iter().enumerate() {
            let message = DepthMessage {
                exch_ts_ns: (slot_index + 1) * 100_000_000,
                local_ts_ns: local as i64,
                first_prev_seq: Some(previous),
                first_seq: Some(previous + 1),
                last_seq: previous + 1,
                changes: changes
                    .iter()
                    .map(|(side, tick, qty)| Change {
                        side: *side,
                        tick: *tick,
                        qty: *qty,
                    })
                    .collect(),
            };
            previous = message.last_seq;
            let produced = recorder.apply(message).unwrap();
            for slot in produced {
                expected.push((
                    slot.slot,
                    slot.snapshot_bids.clone(),
                    slot.snapshot_asks.clone(),
                ));
                closed.push(slot);
            }
        }
        if let Some(slot) = recorder.flush_open_slot().unwrap() {
            expected.push((
                slot.slot,
                slot.snapshot_bids.clone(),
                slot.snapshot_asks.clone(),
            ));
            closed.push(slot);
        }
        assert_eq!(closed.len(), expected.len());

        // `snapshot_bids`/`snapshot_asks` of a closed slot are the window as it stood *before* that
        // slot's changes: that is the block-start snapshot the writer stores. Applying a slot's diff
        // therefore has to arrive at the next slot's window, and the last diff at the final state.
        let mut bids: BTreeMap<i64, u64> = closed[0]
            .snapshot_bids
            .iter()
            .map(|l| (l.tick, l.qty))
            .collect();
        let mut asks: BTreeMap<i64, u64> = closed[0]
            .snapshot_asks
            .iter()
            .map(|l| (l.tick, l.qty))
            .collect();
        let final_window = recorder.window();
        for (index, slot) in closed.iter().enumerate() {
            for change in &slot.changes {
                let book = if change.side == Side::Bid {
                    &mut bids
                } else {
                    &mut asks
                };
                if change.qty == 0 {
                    book.remove(&change.tick);
                } else {
                    book.insert(change.tick, change.qty);
                }
            }
            let rebuilt_bids: Vec<Level> = bids
                .iter()
                .rev()
                .take(5)
                .map(|(tick, qty)| Level {
                    tick: *tick,
                    qty: *qty,
                })
                .collect();
            let rebuilt_asks: Vec<Level> = asks
                .iter()
                .take(5)
                .map(|(tick, qty)| Level {
                    tick: *tick,
                    qty: *qty,
                })
                .collect();
            // After this slot's diff: the next slot's starting window, or the final one.
            let (want_bids, want_asks) = match closed.get(index + 1) {
                Some(next) => (next.snapshot_bids.clone(), next.snapshot_asks.clone()),
                None => (final_window.0.clone(), final_window.1.clone()),
            };
            assert_eq!(
                rebuilt_bids, want_bids,
                "bids after slot {index} do not rebuild the next window"
            );
            assert_eq!(
                rebuilt_asks, want_asks,
                "asks after slot {index} do not rebuild the next window"
            );
        }
    }

    #[test]
    fn slot_diff_is_net_window_change() {
        let mut r = MarketRecorder::new(meta()).unwrap();
        r.install_snapshot(
            &[Level { tick: 100, qty: 1 }, Level { tick: 99, qty: 1 }],
            &[Level { tick: 101, qty: 1 }],
            Some(10),
        )
        .unwrap();
        assert!(
            r.apply(DepthMessage {
                exch_ts_ns: 10_000_000,
                local_ts_ns: 10_000_100,
                first_prev_seq: Some(10),
                first_seq: Some(11),
                last_seq: 11,
                changes: vec![
                    Change {
                        side: Side::Bid,
                        tick: 98,
                        qty: 3
                    },
                    Change {
                        side: Side::Bid,
                        tick: 98,
                        qty: 0
                    }
                ]
            })
            .unwrap()
            .is_empty()
        );
        assert!(
            r.apply(DepthMessage {
                exch_ts_ns: 110_000_000,
                local_ts_ns: 110_000_100,
                first_prev_seq: Some(11),
                first_seq: Some(12),
                last_seq: 12,
                changes: vec![
                    Change {
                        side: Side::Bid,
                        tick: 99,
                        qty: 0
                    },
                    Change {
                        side: Side::Ask,
                        tick: 101,
                        qty: 2
                    }
                ]
            })
            .unwrap()
            .is_empty()
        );
        let closed = r
            .apply(DepthMessage {
                exch_ts_ns: 210_000_000,
                local_ts_ns: 210_000_100,
                first_prev_seq: Some(12),
                first_seq: Some(13),
                last_seq: 13,
                changes: vec![],
            })
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(
            closed[0].changes,
            vec![
                Change {
                    side: Side::Bid,
                    tick: 99,
                    qty: 0
                },
                Change {
                    side: Side::Ask,
                    tick: 101,
                    qty: 2
                }
            ]
        );
    }

    #[test]
    fn non_monotone_feed_sequence_fails_closed() {
        let mut r = MarketRecorder::new(meta()).unwrap();
        r.install_snapshot(
            &[Level { tick: 100, qty: 1 }],
            &[Level { tick: 101, qty: 1 }],
            Some(4),
        )
        .unwrap();
        let msg = DepthMessage {
            exch_ts_ns: 1,
            local_ts_ns: 1,
            first_prev_seq: Some(3),
            first_seq: Some(4),
            last_seq: 5,
            changes: vec![],
        };
        assert!(r.apply(msg).is_err());
    }

    #[test]
    fn unknown_delete_outside_partial_exchange_snapshot_is_ignored() {
        let mut recorder = MarketRecorder::new(meta()).unwrap();
        recorder
            .install_snapshot(
                &[Level { tick: 100, qty: 1 }],
                &[Level { tick: 101, qty: 1 }],
                Some(10),
            )
            .unwrap();

        let closed = recorder
            .apply(DepthMessage {
                exch_ts_ns: 10_000_000,
                local_ts_ns: 10_001_000,
                first_prev_seq: Some(10),
                first_seq: Some(11),
                last_seq: 11,
                changes: vec![Change {
                    side: Side::Bid,
                    tick: 99,
                    qty: 0,
                }],
            })
            .unwrap();

        assert!(closed.is_empty());
        assert_eq!(recorder.window().0, vec![Level { tick: 100, qty: 1 }]);
        assert_eq!(recorder.window().1, vec![Level { tick: 101, qty: 1 }]);
        assert_eq!(recorder.last_seq, Some(11));
    }

    #[test]
    fn a_delta_without_a_predecessor_is_applied_untouched() {
        // A feed that names no predecessor owns continuity itself (the connector checks `pu`), so
        // the recorder applies the change instead of repeating the check with less information.
        let mut recorder = MarketRecorder::new(meta()).unwrap();
        recorder
            .install_snapshot(
                &[Level { tick: 10, qty: 1 }],
                &[Level { tick: 11, qty: 1 }],
                Some(7),
            )
            .unwrap();
        let applied = recorder
            .apply(DepthMessage {
                exch_ts_ns: 100_000_000,
                local_ts_ns: 200_000_000,
                first_prev_seq: None,
                first_seq: Some(8),
                last_seq: 8,
                changes: vec![Change {
                    side: Side::Bid,
                    tick: 12,
                    qty: 2,
                }],
            })
            .expect("a delta without a predecessor must be applied");
        assert!(applied.is_empty(), "a new level still opens the same slot");
    }

    #[test]
    fn a_named_predecessor_that_does_not_match_is_still_a_gap() {
        // The check is not gone: a feed that does claim continuity is held to it.
        let mut recorder = MarketRecorder::new(meta()).unwrap();
        recorder
            .install_snapshot(
                &[Level { tick: 10, qty: 1 }],
                &[Level { tick: 11, qty: 1 }],
                Some(7),
            )
            .unwrap();
        let err = recorder.apply(DepthMessage {
            exch_ts_ns: 1,
            local_ts_ns: 2,
            first_prev_seq: Some(99),
            first_seq: None,
            last_seq: 100,
            changes: vec![],
        });
        assert!(matches!(err, Err(BookError::InvalidField("sequence gap"))));
    }

    #[test]
    fn sequence_gap_does_not_partially_mutate_the_book() {
        let mut r = MarketRecorder::new(meta()).unwrap();
        r.install_snapshot(
            &[Level { tick: 100, qty: 1 }],
            &[Level { tick: 101, qty: 1 }],
            Some(4),
        )
        .unwrap();
        let msg = DepthMessage {
            exch_ts_ns: 1,
            local_ts_ns: 1,
            first_prev_seq: Some(3),
            first_seq: Some(5),
            last_seq: 5,
            changes: vec![
                Change {
                    side: Side::Bid,
                    tick: 100,
                    qty: 9,
                },
                Change {
                    side: Side::Bid,
                    tick: 99,
                    qty: 0,
                },
            ],
        };
        assert!(r.apply(msg).is_err());
        assert_eq!(r.window().0, vec![Level { tick: 100, qty: 1 }]);
    }

    #[test]
    fn config_requires_unique_market_ids_and_valid_grid() {
        let text = r#"
[recorder]
root = "/workspace/market"
grid_ms = 100
depth_levels = 100
block_duration_ms = 60000

[[markets]]
market_id = 1
venue_id = 1
venue = "binancefuturesum"
instrument = "perpetual"
symbol = "BTCUSDT"
feed_mode = "diff"
tick_units = 1
price_decimals = 2
qty_step_units = 1
qty_decimals = 3
depth_available = 1000
"#;
        let config = RecorderConfig::parse(text).unwrap();
        assert_eq!(config.market_meta(&config.markets[0]).grid_ms, 100);
        let invalid = text.replace("block_duration_ms = 60000", "block_duration_ms = 59999");
        assert!(RecorderConfig::parse(&invalid).is_err());
        // A segment must divide the UTC day: 90 minutes rotates at 01:30, where a segment's start
        // crosses into the next date.
        let misaligned = text.replace(
            "block_duration_ms = 60000",
            "block_duration_ms = 60000\nsegment_duration_ms = 5400000",
        );
        assert!(RecorderConfig::parse(&misaligned).is_err());
        let aligned = text.replace(
            "block_duration_ms = 60000",
            "block_duration_ms = 60000\nsegment_duration_ms = 21600000",
        );
        assert!(RecorderConfig::parse(&aligned).is_ok());
    }

    #[test]
    fn decimal_steps_are_exact_and_reject_misaligned_values() {
        assert_eq!(decimal_steps("123.450", 3, 5).unwrap(), 24_690);
        assert!(decimal_steps("123.451", 3, 5).is_err());
        assert!(decimal_steps("1e-3", 3, 1).is_err());
    }

    #[test]
    fn closed_slots_pack_into_a_block_with_relative_sequences() {
        let mut market = meta();
        market.block_duration_ms = 1000;
        let closed = ClosedSlot {
            slot: 1,
            exch_ts_ns: 150_000_000,
            local_ts_ns: 150_010_000,
            seq_first_prev: Some(100),
            seq_last: 101,
            n_msgs: 3,
            n_msgs_saturated: false,
            changes: vec![Change {
                side: Side::Bid,
                tick: 99,
                qty: 2,
            }],
            snapshot_bids: vec![Level { tick: 100, qty: 1 }],
            snapshot_asks: vec![Level { tick: 101, qty: 1 }],
        };
        let block = build_data_block(
            &market,
            100_000_000,
            200_000_000,
            100_000_000,
            100,
            100,
            102,
            10,
            vec![Level { tick: 100, qty: 1 }],
            vec![Level { tick: 101, qty: 1 }],
            &[closed],
        )
        .unwrap();
        let encoded = super::super::encode_block(&block).unwrap();
        let decoded = super::super::decode_block(&encoded).unwrap();
        let payload = super::super::decode_data_payload(&decoded).unwrap();
        assert_eq!(payload.records[0].seq_last_delta, 1);
        assert_eq!(payload.records[0].seq_first_prev_delta, 0);
        assert_eq!(decoded.header.seq_last_processed_delta, 2);
    }

    #[test]
    fn saturated_message_count_sets_the_block_flag() {
        let mut market = meta();
        market.block_duration_ms = 1000;
        let closed = ClosedSlot {
            slot: 1,
            exch_ts_ns: 150_000_000,
            local_ts_ns: 150_010_000,
            seq_first_prev: Some(100),
            seq_last: 101,
            n_msgs: u16::MAX,
            n_msgs_saturated: true,
            changes: vec![Change {
                side: Side::Bid,
                tick: 99,
                qty: 2,
            }],
            snapshot_bids: vec![Level { tick: 100, qty: 1 }],
            snapshot_asks: vec![Level { tick: 101, qty: 1 }],
        };
        let block = build_data_block(
            &market,
            100_000_000,
            200_000_000,
            100_000_000,
            100,
            100,
            101,
            10,
            closed.snapshot_bids.clone(),
            closed.snapshot_asks.clone(),
            &[closed],
        )
        .unwrap();
        assert_ne!(block.header.block_flags & (1 << 4), 0);
    }

    fn slot_window(
        slot: i64,
        seq: u64,
        changes: Vec<Change>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> ClosedSlot {
        ClosedSlot {
            slot,
            exch_ts_ns: slot * 100_000_000 + 1_000_000,
            local_ts_ns: slot * 100_000_000 + 2_000_000,
            seq_first_prev: Some(seq),
            seq_last: seq + 1,
            n_msgs: 1,
            n_msgs_saturated: false,
            changes,
            snapshot_bids: bids,
            snapshot_asks: asks,
        }
    }

    fn change(side: Side, tick: i64, qty: u64) -> Change {
        Change { side, tick, qty }
    }

    fn decode(block: &BlockInput) -> DataPayload {
        let encoded = super::super::encode_block(block).unwrap();
        let decoded = super::super::decode_block(&encoded).unwrap();
        super::super::decode_data_payload(&decoded).unwrap()
    }

    #[test]
    fn aggregator_packs_several_slots_into_one_block() {
        let market = meta();
        let mut aggregator = SlotAggregator::new(&market).unwrap();
        let bids = vec![Level { tick: 100, qty: 1 }];
        let asks = vec![Level { tick: 101, qty: 1 }];
        assert!(
            aggregator
                .push(
                    &market,
                    slot_window(
                        1000,
                        10,
                        vec![change(Side::Bid, 99, 2)],
                        bids.clone(),
                        asks.clone(),
                    ),
                )
                .unwrap()
                .is_none()
        );
        assert!(
            aggregator
                .push(
                    &market,
                    slot_window(
                        1001,
                        11,
                        vec![change(Side::Bid, 98, 3)],
                        bids.clone(),
                        asks.clone(),
                    ),
                )
                .unwrap()
                .is_none()
        );
        assert!(
            aggregator
                .push(
                    &market,
                    slot_window(
                        1003,
                        12,
                        vec![change(Side::Ask, 102, 5)],
                        bids.clone(),
                        asks.clone(),
                    ),
                )
                .unwrap()
                .is_none()
        );
        let block = aggregator.flush(&market).unwrap().unwrap();
        assert_eq!(block.header.n_slots, 4);
        assert_eq!(block.header.block_start_exch_ns, 1000 * 100_000_000);
        let payload = decode(&block);
        assert_eq!(payload.bids, bids);
        assert_eq!(payload.asks, asks);
        let changes = payload
            .records
            .iter()
            .map(|record| record.changes.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            changes,
            vec![
                vec![change(Side::Bid, 99, 2)],
                vec![change(Side::Bid, 98, 3)],
                vec![change(Side::Ask, 102, 5)],
            ]
        );
        assert_eq!(payload.records[0].seq_first_prev_delta, 0);
        assert_eq!(payload.records[0].seq_last_delta, 1);
        assert_eq!(payload.records[2].seq_first_prev_delta, 2);
        assert_eq!(block.header.seq_last_processed_delta, 3);
        let mut ladder = Ladder::from_snapshot(&payload.bids, &payload.asks).unwrap();
        for record in &payload.records {
            ladder.apply(&record.changes).unwrap();
        }
        assert_eq!(
            ladder.top_n(Side::Bid, 10),
            vec![
                Level { tick: 100, qty: 1 },
                Level { tick: 99, qty: 2 },
                Level { tick: 98, qty: 3 },
            ]
        );
        assert_eq!(
            ladder.top_n(Side::Ask, 10),
            vec![Level { tick: 101, qty: 1 }, Level { tick: 102, qty: 5 }]
        );
    }

    #[test]
    fn aggregator_seals_after_a_full_window() {
        let market = meta();
        let mut aggregator = SlotAggregator::new(&market).unwrap();
        let bids = vec![Level { tick: 100, qty: 1 }];
        let asks = vec![Level { tick: 101, qty: 1 }];
        for offset in 0..10i64 {
            let sealed = aggregator
                .push(
                    &market,
                    slot_window(
                        1000 + offset,
                        10 + offset as u64,
                        vec![change(Side::Bid, 99, 2 + offset as u64)],
                        bids.clone(),
                        asks.clone(),
                    ),
                )
                .unwrap();
            if offset < 9 {
                assert!(sealed.is_none());
            } else {
                let block = sealed.unwrap();
                assert_eq!(block.header.n_slots, 10);
                let payload = decode(&block);
                assert_eq!(payload.records.len(), 10);
            }
        }
        assert!(aggregator.flush(&market).unwrap().is_none());
    }

    #[test]
    fn aggregator_seals_before_a_slot_outside_the_window() {
        let market = meta();
        let mut aggregator = SlotAggregator::new(&market).unwrap();
        let bids = vec![Level { tick: 100, qty: 1 }];
        let asks = vec![Level { tick: 101, qty: 1 }];
        assert!(
            aggregator
                .push(
                    &market,
                    slot_window(
                        1000,
                        10,
                        vec![change(Side::Bid, 99, 2)],
                        bids.clone(),
                        asks.clone(),
                    ),
                )
                .unwrap()
                .is_none()
        );
        let block = aggregator
            .push(
                &market,
                slot_window(
                    1601,
                    11,
                    vec![change(Side::Bid, 99, 3)],
                    bids.clone(),
                    asks.clone(),
                ),
            )
            .unwrap()
            .unwrap();
        assert_eq!(block.header.n_slots, 10);
        assert_eq!(block.header.block_start_exch_ns, 1000 * 100_000_000);
        assert_eq!(decode(&block).records.len(), 1);
        let tail = aggregator.flush(&market).unwrap().unwrap();
        assert_eq!(tail.header.n_slots, 1);
        assert_eq!(tail.header.block_start_exch_ns, 1601 * 100_000_000);
    }

    #[test]
    fn aggregator_seals_when_a_tick_exceeds_the_rebase_window() {
        let market = meta();
        let mut aggregator = SlotAggregator::new(&market).unwrap();
        assert!(
            aggregator
                .push(
                    &market,
                    slot_window(
                        1000,
                        10,
                        vec![change(Side::Bid, 99, 2)],
                        vec![Level { tick: 100, qty: 1 }],
                        vec![Level { tick: 101, qty: 1 }],
                    ),
                )
                .unwrap()
                .is_none()
        );
        let block = aggregator
            .push(
                &market,
                slot_window(
                    1001,
                    11,
                    vec![change(Side::Bid, 40_000, 2)],
                    vec![Level { tick: 100, qty: 1 }, Level { tick: 99, qty: 2 }],
                    vec![Level { tick: 101, qty: 1 }],
                ),
            )
            .unwrap()
            .unwrap();
        assert_eq!(block.header.n_slots, 1);
        assert_eq!(decode(&block).records.len(), 1);
        let tail = aggregator.flush(&market).unwrap().unwrap();
        assert_eq!(tail.header.n_slots, 1);
        let payload = decode(&tail);
        assert!(
            payload.records[0]
                .changes
                .iter()
                .any(|change| change.tick == 40_000)
        );
    }

    #[test]
    fn aggregator_keeps_an_empty_slot_in_the_block() {
        let market = meta();
        let mut aggregator = SlotAggregator::new(&market).unwrap();
        let slot = slot_window(
            1000,
            10,
            vec![],
            vec![Level { tick: 100, qty: 1 }],
            vec![Level { tick: 101, qty: 1 }],
        );
        assert!(aggregator.push(&market, slot).unwrap().is_none());
        let block = aggregator.flush(&market).unwrap().unwrap();
        let payload = decode(&block);
        assert_eq!(payload.records.len(), 1);
        assert!(payload.records[0].changes.is_empty());
    }
}
