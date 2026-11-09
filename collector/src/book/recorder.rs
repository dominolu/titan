#![allow(async_fn_in_trait)]

use std::collections::BTreeMap;

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
    RestSnapshot(SnapshotMessage),
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
    #[serde(default = "default_rest_log_capacity")]
    pub rest_validation_log_messages: u32,
    #[serde(default = "default_rest_min_samples")]
    pub rest_validation_min_samples: u32,
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
            || (r.rest_validation_log_messages != 0
                && !(256..=65_536).contains(&r.rest_validation_log_messages))
            || r.rest_validation_min_samples > 1_000
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
fn default_segment_duration_ms() -> u32 {
    3_600_000
}
fn default_oi_poll_ms() -> u32 {
    60_000
}
fn default_validation_interval_ms() -> u32 {
    60_000
}
fn default_rest_log_capacity() -> u32 {
    2_048
}
fn default_rest_min_samples() -> u32 {
    5
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
    pub late_events: u32,
    pub changes: Vec<Change>,
    pub snapshot_bids: Vec<Level>,
    pub snapshot_asks: Vec<Level>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestShadowStatus {
    Match,
    Mismatch,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RestShadowReport {
    pub status: RestShadowStatus,
    pub differing_levels: usize,
    pub snapshot_seq: u64,
    pub current_seq: u64,
    pub session_epoch: u32,
}

/// Bounded normalized-delta log used to align an asynchronous REST snapshot
/// with the current websocket book without mutating the recorder's main book.
pub struct RestShadowValidator {
    capacity: usize,
    session_epoch: u32,
    last_seq: Option<u64>,
    deltas: std::collections::VecDeque<DepthMessage>,
}

impl RestShadowValidator {
    pub fn new(capacity: usize, session_epoch: u32) -> Result<Self, BookError> {
        if capacity == 0 {
            return Err(BookError::InvalidField("REST shadow capacity"));
        }
        Ok(Self {
            capacity,
            session_epoch,
            last_seq: None,
            deltas: Default::default(),
        })
    }

    pub fn push(&mut self, session_epoch: u32, message: DepthMessage) -> Result<(), BookError> {
        if session_epoch != self.session_epoch {
            self.session_epoch = session_epoch;
            self.last_seq = None;
            self.deltas.clear();
        }
        if let Some(last) = self.last_seq
            && message.first_prev_seq != Some(last)
        {
            return Err(BookError::InvalidField("REST shadow sequence gap"));
        }
        self.last_seq = Some(message.last_seq);
        self.deltas.push_back(message);
        while self.deltas.len() > self.capacity {
            self.deltas.pop_front();
        }
        Ok(())
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    pub fn compare(
        &self,
        session_epoch: u32,
        snapshot_seq: u64,
        snapshot: (&[Level], &[Level]),
        current: (&[Level], &[Level]),
        depth: usize,
    ) -> Result<RestShadowReport, BookError> {
        if depth == 0 {
            return Err(BookError::InvalidField("REST shadow depth"));
        }
        validate_levels(snapshot.0, Side::Bid)?;
        validate_levels(snapshot.1, Side::Ask)?;
        validate_levels(current.0, Side::Bid)?;
        validate_levels(current.1, Side::Ask)?;
        let current_seq = self.last_seq.unwrap_or(snapshot_seq);
        let stale = || RestShadowReport {
            status: RestShadowStatus::Stale,
            differing_levels: 0,
            snapshot_seq,
            current_seq,
            session_epoch,
        };
        if session_epoch != self.session_epoch || snapshot_seq > current_seq {
            return Ok(stale());
        }
        let mut bids: BTreeMap<_, _> = snapshot.0.iter().map(|l| (l.tick, l.qty)).collect();
        let mut asks: BTreeMap<_, _> = snapshot.1.iter().map(|l| (l.tick, l.qty)).collect();
        let mut cursor = snapshot_seq;
        let mut started = false;
        for delta in self.deltas.iter().filter(|d| d.last_seq > snapshot_seq) {
            if !started {
                // Binance's REST lastUpdateId may fall inside a diff event's
                // [U, u] range. That event contains the state transition from
                // snapshot_seq + 1 through u, so apply it as the bridge.
                let next_seq = snapshot_seq.saturating_add(1);
                let covered = delta.first_seq.is_some_and(|first| first <= next_seq)
                    && delta.last_seq >= next_seq;
                if !covered {
                    return Ok(stale());
                }
                started = true;
            } else if delta.first_prev_seq != Some(cursor) {
                return Ok(stale());
            }
            for change in &delta.changes {
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
            cursor = delta.last_seq;
        }
        if cursor != current_seq || (snapshot_seq < current_seq && !started) {
            return Ok(stale());
        }
        let projected_bids = top_levels(&bids, Side::Bid, depth);
        let projected_asks = top_levels(&asks, Side::Ask, depth);
        let expected_bids = current.0.iter().copied().take(depth).collect::<Vec<_>>();
        let expected_asks = current.1.iter().copied().take(depth).collect::<Vec<_>>();
        let differing_levels = level_diff_count(&projected_bids, &expected_bids)
            + level_diff_count(&projected_asks, &expected_asks);
        Ok(RestShadowReport {
            status: if differing_levels == 0 {
                RestShadowStatus::Match
            } else {
                RestShadowStatus::Mismatch
            },
            differing_levels,
            snapshot_seq,
            current_seq,
            session_epoch,
        })
    }
}

fn top_levels(book: &BTreeMap<i64, u64>, side: Side, depth: usize) -> Vec<Level> {
    match side {
        Side::Bid => book
            .iter()
            .rev()
            .take(depth)
            .map(|(&tick, &qty)| Level { tick, qty })
            .collect(),
        Side::Ask => book
            .iter()
            .take(depth)
            .map(|(&tick, &qty)| Level { tick, qty })
            .collect(),
    }
}

fn level_diff_count(left: &[Level], right: &[Level]) -> usize {
    let common = left.len().min(right.len());
    left.iter()
        .take(common)
        .zip(right)
        .filter(|(a, b)| a != b)
        .count()
        + left.len().abs_diff(right.len())
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
    if closed_slots.iter().any(|slot| slot.late_events > 0) {
        header.block_flags |= 1 << 3;
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

/// Runs one normalized market feed to EOF and writes each changed closed slot
/// as a self-contained data block. Feed adapters must yield a snapshot before
/// diff messages and must end the feed when a reconnect requires resynchronizing.
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
    let mut event_writer = MarketEventWriter::new();
    let mut rest_shadow = (settings.rest_validation_log_messages > 0)
        .then(|| {
            RestShadowValidator::new(
                settings.rest_validation_log_messages as usize,
                meta.session_epoch,
            )
        })
        .transpose()?;
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
                    write_closed_slot(
                        &meta,
                        &mut writer,
                        &mut event_writer,
                        root,
                        venue,
                        instrument,
                        &slot,
                        &mut last_times,
                    )?;
                }
                continue;
            }
        };
        let closed = match message {
            FeedMessage::Gap(gap) => {
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
                rest_shadow = (settings.rest_validation_log_messages > 0)
                    .then(|| {
                        RestShadowValidator::new(
                            settings.rest_validation_log_messages as usize,
                            meta.session_epoch,
                        )
                    })
                    .transpose()?;
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
            FeedMessage::RestSnapshot(snapshot) => {
                let status_event = if let (Some(validator), Some(snapshot_seq)) =
                    (rest_shadow.as_ref(), snapshot.last_seq)
                {
                    let (bids, asks) = recorder.window();
                    let report = validator.compare(
                        meta.session_epoch,
                        snapshot_seq,
                        (&snapshot.bids, &snapshot.asks),
                        (&bids, &asks),
                        usize::from(meta.depth_requested.min(meta.depth_available)),
                    )?;
                    let status = match report.status {
                        RestShadowStatus::Match => "match",
                        RestShadowStatus::Mismatch => "mismatch",
                        RestShadowStatus::Stale => "stale",
                    };
                    serde_json::json!({"exch_ts_ns": snapshot.exch_ts_ns, "local_ts_ns": snapshot.local_ts_ns, "status": status, "differing_levels": report.differing_levels, "snapshot_seq": report.snapshot_seq, "current_seq": report.current_seq, "session_epoch": report.session_epoch})
                } else {
                    serde_json::json!({"exch_ts_ns": snapshot.exch_ts_ns, "local_ts_ns": snapshot.local_ts_ns, "status": "stale", "differing_levels": 0, "snapshot_seq": snapshot.last_seq, "current_seq": rest_shadow.as_ref().and_then(RestShadowValidator::last_seq), "session_epoch": meta.session_epoch})
                };
                if rest_shadow.is_some() {
                    event_writer.write_rest_validation(
                        root,
                        venue,
                        instrument,
                        &meta.symbol,
                        snapshot.local_ts_ns,
                        &status_event,
                    )?;
                }
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
                if let Some(validator) = rest_shadow.as_mut() {
                    validator.push(meta.session_epoch, depth.clone())?;
                }
                initialized = true;
                recorder.apply(depth)?
            }
        };
        for slot in closed {
            write_closed_slot(
                &meta,
                &mut writer,
                &mut event_writer,
                root,
                venue,
                instrument,
                &slot,
                &mut last_times,
            )?;
        }
    }
    if let Some(slot) = recorder.flush_open_slot()? {
        write_closed_slot(
            &meta,
            &mut writer,
            &mut event_writer,
            root,
            venue,
            instrument,
            &slot,
            &mut last_times,
        )?;
    }
    writer.finish(last_times.0, last_times.1)?;
    event_writer.finish()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_closed_slot(
    meta: &MarketMeta,
    writer: &mut BookWriter,
    event_writer: &mut MarketEventWriter,
    root: &std::path::Path,
    venue: &str,
    instrument: &str,
    slot: &ClosedSlot,
    last_times: &mut (i64, i64),
) -> Result<(), BookError> {
    if slot.late_events > 0 {
        let event = serde_json::json!({"exch_ts_ns": slot.exch_ts_ns, "local_ts_ns": slot.local_ts_ns, "status": "late", "late_events": slot.late_events});
        event_writer.write_rest_validation(
            root,
            venue,
            instrument,
            &meta.symbol,
            slot.local_ts_ns,
            &event,
        )?;
    }
    if slot.changes.is_empty() {
        return Ok(());
    }
    let block = single_slot_block(meta, slot)?;
    writer.append(&block)?;
    *last_times = (
        block.header.block_end_local_ns,
        block.header.block_end_exch_ns,
    );
    Ok(())
}

fn single_slot_block(meta: &MarketMeta, slot: &ClosedSlot) -> Result<BlockInput, BookError> {
    let grid_ns = i64::from(meta.grid_ms)
        .checked_mul(1_000_000)
        .ok_or(BookError::InvalidField("grid duration"))?;
    let start_exch_ns = slot
        .slot
        .checked_mul(grid_ns)
        .ok_or(BookError::InvalidField("slot timestamp"))?;
    let min_tick = slot
        .snapshot_bids
        .iter()
        .chain(&slot.snapshot_asks)
        .map(|level| level.tick)
        .chain(slot.changes.iter().map(|change| change.tick))
        .min()
        .ok_or(BookError::InvalidField("empty slot window"))?;
    let max_tick = slot
        .snapshot_bids
        .iter()
        .chain(&slot.snapshot_asks)
        .map(|level| level.tick)
        .chain(slot.changes.iter().map(|change| change.tick))
        .max()
        .ok_or(BookError::InvalidField("empty slot window"))?;
    let base_tick = i64::try_from((i128::from(min_tick) + i128::from(max_tick)) / 2)
        .map_err(|_| BookError::InvalidField("base tick"))?;
    let seq_base = if meta.feed_mode == FeedMode::Snapshot {
        0
    } else {
        slot.seq_first_prev
            .ok_or(BookError::InvalidField("missing slot predecessor"))?
    };
    build_data_block(
        meta,
        slot.local_ts_ns,
        slot.local_ts_ns,
        start_exch_ns,
        base_tick,
        seq_base,
        slot.seq_last,
        1,
        slot.snapshot_bids.clone(),
        slot.snapshot_asks.clone(),
        std::slice::from_ref(slot),
    )
}

/// Applies normalized deltas in feed order and produces one net window diff
/// whenever a grid slot closes. Full-depth state is retained so levels entering
/// or leaving the Top-N window are represented as ordinary net changes.
pub struct MarketRecorder {
    meta: MarketMeta,
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    previous_window: Option<(Vec<Level>, Vec<Level>)>,
    current_slot: Option<i64>,
    last_closed_slot: Option<i64>,
    slot_exch_ts: i64,
    slot_local_ts: i64,
    slot_first_prev: Option<u64>,
    slot_last_seq: u64,
    slot_n_msgs: u64,
    slot_late_events: u32,
    last_clamped_exch_ts: Option<i64>,
    last_seq: Option<u64>,
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
            meta,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            previous_window: None,
            current_slot: None,
            last_closed_slot: None,
            slot_exch_ts: 0,
            slot_local_ts: 0,
            slot_first_prev: None,
            slot_last_seq: 0,
            slot_n_msgs: 0,
            slot_late_events: 0,
            last_clamped_exch_ts: None,
            last_seq: None,
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
        self.bids = bids.iter().map(|l| (l.tick, l.qty)).collect();
        self.asks = asks.iter().map(|l| (l.tick, l.qty)).collect();
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
        if let Some(previous) = self.last_seq {
            if message.first_prev_seq != Some(previous) {
                return Err(BookError::InvalidField("sequence gap"));
            }
            if message.last_seq <= previous {
                return Err(BookError::InvalidField("sequence order"));
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
        let late = slot != raw_slot || clamped != message.exch_ts_ns;
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
            self.slot_late_events = 0;
        }
        if late {
            self.slot_late_events = self.slot_late_events.saturating_add(1);
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
        let late = slot != raw_slot || clamped != message.exch_ts_ns;
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
            self.slot_late_events = 0;
        }
        if late {
            self.slot_late_events = self.slot_late_events.saturating_add(1);
        }
        self.bids = message.bids.iter().map(|l| (l.tick, l.qty)).collect();
        self.asks = message.asks.iter().map(|l| (l.tick, l.qty)).collect();
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
        self.slot_late_events = 0;
        self.slot_first_prev = None;
        Ok(closed)
    }

    pub fn window(&self) -> (Vec<Level>, Vec<Level>) {
        let n = self.meta.depth_requested.min(self.meta.depth_available) as usize;
        let bids = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(&tick, &qty)| Level { tick, qty })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(n)
            .map(|(&tick, &qty)| Level { tick, qty })
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
        if changes.is_empty() && self.slot_late_events == 0 {
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
            late_events: self.slot_late_events,
            changes,
            snapshot_bids,
            snapshot_asks,
        }))
    }

    fn apply_changes(&mut self, changes: &[Change]) -> Result<(), BookError> {
        let mut staged: BTreeMap<(bool, i64), Option<u64>> = BTreeMap::new();
        for change in changes {
            let key = (change.side == Side::Ask, change.tick);
            let current = staged
                .get(&key)
                .copied()
                .unwrap_or_else(|| match change.side {
                    Side::Bid => self.bids.get(&change.tick).copied(),
                    Side::Ask => self.asks.get(&change.tick).copied(),
                });
            if change.qty == 0 && current.is_none() {
                return Err(BookError::InvalidField("delete missing level"));
            }
            staged.insert(key, (change.qty != 0).then_some(change.qty));
        }
        for ((ask, tick), qty) in staged {
            let book = if ask { &mut self.asks } else { &mut self.bids };
            if let Some(qty) = qty {
                book.insert(tick, qty);
            } else {
                book.remove(&tick);
            }
        }
        Ok(())
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
    fn missing_predecessor_sequence_fails_closed() {
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
            first_prev_seq: None,
            first_seq: None,
            last_seq: 8,
            changes: vec![],
        });
        assert!(matches!(err, Err(BookError::InvalidField("sequence gap"))));
    }

    #[test]
    fn late_messages_are_counted_even_when_the_slot_has_no_book_changes() {
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
                exch_ts_ns: 210_000_000,
                local_ts_ns: 210_001_000,
                first_prev_seq: Some(10),
                first_seq: Some(11),
                last_seq: 11,
                changes: vec![],
            })
            .unwrap();
        recorder
            .apply(DepthMessage {
                exch_ts_ns: 150_000_000,
                local_ts_ns: 211_001_000,
                first_prev_seq: Some(11),
                first_seq: Some(12),
                last_seq: 12,
                changes: vec![],
            })
            .unwrap();
        let slot = recorder.flush_open_slot().unwrap().unwrap();
        assert!(slot.changes.is_empty());
        assert_eq!(slot.late_events, 1);
    }

    #[test]
    fn invalid_delta_does_not_partially_mutate_the_book() {
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
            first_prev_seq: Some(4),
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
    }

    #[test]
    fn decimal_steps_are_exact_and_reject_misaligned_values() {
        assert_eq!(decimal_steps("123.450", 3, 5).unwrap(), 24_690);
        assert!(decimal_steps("123.451", 3, 5).is_err());
        assert!(decimal_steps("1e-3", 3, 1).is_err());
    }

    #[test]
    fn rest_shadow_replays_deltas_and_marks_unaligned_snapshots_stale() {
        let mut shadow = RestShadowValidator::new(8, 2).unwrap();
        shadow
            .push(
                2,
                DepthMessage {
                    exch_ts_ns: 1,
                    local_ts_ns: 2,
                    first_prev_seq: Some(10),
                    first_seq: Some(11),
                    last_seq: 11,
                    changes: vec![Change {
                        side: Side::Bid,
                        tick: 100,
                        qty: 2,
                    }],
                },
            )
            .unwrap();
        shadow
            .push(
                2,
                DepthMessage {
                    exch_ts_ns: 3,
                    local_ts_ns: 4,
                    first_prev_seq: Some(11),
                    first_seq: Some(12),
                    last_seq: 12,
                    changes: vec![Change {
                        side: Side::Ask,
                        tick: 101,
                        qty: 3,
                    }],
                },
            )
            .unwrap();
        let report = shadow
            .compare(
                2,
                10,
                (
                    &[Level { tick: 100, qty: 1 }],
                    &[Level { tick: 101, qty: 1 }],
                ),
                (
                    &[Level { tick: 100, qty: 2 }],
                    &[Level { tick: 101, qty: 3 }],
                ),
                1,
            )
            .unwrap();
        assert_eq!(report.status, RestShadowStatus::Match);
        let stale = shadow
            .compare(
                2,
                9,
                (
                    &[Level { tick: 100, qty: 1 }],
                    &[Level { tick: 101, qty: 1 }],
                ),
                (
                    &[Level { tick: 100, qty: 2 }],
                    &[Level { tick: 101, qty: 3 }],
                ),
                1,
            )
            .unwrap();
        assert_eq!(stale.status, RestShadowStatus::Stale);
        let mismatch = shadow
            .compare(
                2,
                10,
                (
                    &[Level { tick: 100, qty: 1 }],
                    &[Level { tick: 101, qty: 1 }],
                ),
                (
                    &[Level { tick: 100, qty: 9 }],
                    &[Level { tick: 101, qty: 3 }],
                ),
                1,
            )
            .unwrap();
        assert_eq!(mismatch.status, RestShadowStatus::Mismatch);
        assert_eq!(mismatch.differing_levels, 1);
    }

    #[test]
    fn rest_shadow_accepts_snapshot_inside_first_delta_sequence_range() {
        let mut shadow = RestShadowValidator::new(8, 2).unwrap();
        shadow
            .push(
                2,
                DepthMessage {
                    exch_ts_ns: 1,
                    local_ts_ns: 2,
                    first_prev_seq: Some(10),
                    first_seq: Some(11),
                    last_seq: 13,
                    changes: vec![Change {
                        side: Side::Bid,
                        tick: 100,
                        qty: 2,
                    }],
                },
            )
            .unwrap();
        shadow
            .push(
                2,
                DepthMessage {
                    exch_ts_ns: 3,
                    local_ts_ns: 4,
                    first_prev_seq: Some(13),
                    first_seq: Some(14),
                    last_seq: 14,
                    changes: vec![Change {
                        side: Side::Ask,
                        tick: 101,
                        qty: 3,
                    }],
                },
            )
            .unwrap();

        let report = shadow
            .compare(
                2,
                12,
                (
                    &[Level { tick: 100, qty: 1 }],
                    &[Level { tick: 101, qty: 1 }],
                ),
                (
                    &[Level { tick: 100, qty: 2 }],
                    &[Level { tick: 101, qty: 3 }],
                ),
                1,
            )
            .unwrap();

        assert_eq!(report.status, RestShadowStatus::Match);
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
            late_events: 0,
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
            late_events: 0,
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
}
