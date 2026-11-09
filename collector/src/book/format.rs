use super::{BLOCK_HEADER_LEN, BookError, Result, SEGMENT_HEADER_LEN, TICK_BIAS};

const SEGMENT_MAGIC: &[u8; 4] = b"TMR1";
const BLOCK_MAGIC: &[u8; 4] = b"TMB1";
const FORMAT_VERSION: u16 = 1;
pub const BLOCK_FLAG_ZSTD: u8 = 1 << 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    pub tick: i64,
    pub qty: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub side: Side,
    pub tick: i64,
    /// Zero deletes a level; positive values are quantity steps.
    pub qty: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum FeedMode {
    Diff = 0,
    Snapshot = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketMeta {
    pub market_id: u32,
    pub venue_id: u16,
    pub symbol: String,
    pub feed_mode: FeedMode,
    pub tick_units: u64,
    pub price_decimals: u8,
    pub qty_step_units: u64,
    pub qty_decimals: u8,
    pub grid_ms: u16,
    pub block_duration_ms: u16,
    pub depth_requested: u16,
    pub depth_available: u16,
    pub session_epoch: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub market_id: u32,
    pub flags: u32,
    pub segment_start_local_ns: i64,
    pub segment_start_exch_ns: i64,
    pub segment_end_local_ns: i64,
    pub segment_end_exch_ns: i64,
    pub tick_units: u64,
    pub price_decimals: u8,
    pub qty_decimals: u8,
    pub grid_ms: u16,
    pub block_duration_ms: u16,
    pub depth_requested: u16,
    pub depth_available: u16,
    pub venue_id: u16,
    pub session_epoch: u32,
    pub qty_step_units: u64,
    pub encoding_flags: u32,
    pub feed_mode: FeedMode,
    pub symbol: String,
}

impl SegmentHeader {
    pub fn from_meta(meta: &MarketMeta, start_local_ns: i64, start_exch_ns: i64) -> Self {
        Self {
            market_id: meta.market_id,
            flags: 0,
            segment_start_local_ns: start_local_ns,
            segment_start_exch_ns: start_exch_ns,
            segment_end_local_ns: 0,
            segment_end_exch_ns: 0,
            tick_units: meta.tick_units,
            price_decimals: meta.price_decimals,
            qty_decimals: meta.qty_decimals,
            grid_ms: meta.grid_ms,
            block_duration_ms: meta.block_duration_ms,
            depth_requested: meta.depth_requested,
            depth_available: meta.depth_available,
            venue_id: meta.venue_id,
            session_epoch: meta.session_epoch,
            qty_step_units: meta.qty_step_units,
            encoding_flags: 1,
            feed_mode: meta.feed_mode,
            symbol: meta.symbol.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockKind {
    Data = 0,
    Gap = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum GapReason {
    Disconnect = 1,
    SequenceBreak = 2,
    Resync = 3,
    WindowFailure = 4,
    Precision = 5,
    TruncatedTail = 6,
    WriterRestart = 7,
    CorruptBlock = 8,
    InvalidSnapshot = 9,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub kind: BlockKind,
    pub block_flags: u8,
    pub block_len: u32,
    pub market_id: u32,
    pub block_start_local_ns: i64,
    pub block_start_exch_ns: i64,
    pub block_end_local_ns: i64,
    pub block_end_exch_ns: i64,
    pub base_tick: i64,
    pub seq_base: u64,
    pub n_slots: u16,
    pub n_records: u16,
    pub n_changes: u32,
    pub n_escapes: u32,
    pub n_bid: u16,
    pub n_ask: u16,
    pub snapshot_off: u32,
    pub records_off: u32,
    pub changes_off: u32,
    pub escapes_off: u32,
    pub seq_last_processed_delta: u32,
    pub control_reason: u16,
    pub control_len: u16,
    pub control_last_seq: u64,
    pub control_next_seq: u64,
}

impl BlockHeader {
    pub fn data(
        market_id: u32,
        start_local_ns: i64,
        start_exch_ns: i64,
        payload_len: usize,
    ) -> Result<Self> {
        let block_len = align8(
            BLOCK_HEADER_LEN
                .checked_add(payload_len)
                .ok_or(BookError::InvalidField("block_len"))?,
        );
        Ok(Self {
            kind: BlockKind::Data,
            block_flags: 1,
            block_len: u32::try_from(block_len)
                .map_err(|_| BookError::InvalidField("block_len"))?,
            market_id,
            block_start_local_ns: start_local_ns,
            block_start_exch_ns: start_exch_ns,
            block_end_local_ns: start_local_ns,
            block_end_exch_ns: start_exch_ns,
            base_tick: 0,
            seq_base: 0,
            n_slots: 1,
            n_records: 0,
            n_changes: 0,
            n_escapes: 0,
            n_bid: 0,
            n_ask: 0,
            snapshot_off: 128,
            records_off: 128,
            changes_off: 128,
            escapes_off: 128,
            seq_last_processed_delta: 0,
            control_reason: 0,
            control_len: 0,
            control_last_seq: 0,
            control_next_seq: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn gap(
        market_id: u32,
        start_local_ns: i64,
        start_exch_ns: i64,
        end_local_ns: i64,
        end_exch_ns: i64,
        reason: GapReason,
        last_seq: u64,
        next_seq: u64,
        detail: &[u8],
    ) -> Result<Self> {
        let control_len = u16::try_from(detail.len())
            .map_err(|_| BookError::InvalidField("gap detail length"))?;
        let block_len = align8(
            BLOCK_HEADER_LEN
                .checked_add(detail.len())
                .ok_or(BookError::InvalidField("block length"))?,
        );
        Ok(Self {
            kind: BlockKind::Gap,
            block_flags: 0,
            block_len: u32::try_from(block_len)
                .map_err(|_| BookError::InvalidField("block length"))?,
            market_id,
            block_start_local_ns: start_local_ns,
            block_start_exch_ns: start_exch_ns,
            block_end_local_ns: end_local_ns,
            block_end_exch_ns: end_exch_ns,
            base_tick: 0,
            seq_base: 0,
            n_slots: 0,
            n_records: 0,
            n_changes: 0,
            n_escapes: 0,
            n_bid: 0,
            n_ask: 0,
            snapshot_off: BLOCK_HEADER_LEN as u32,
            records_off: BLOCK_HEADER_LEN as u32,
            changes_off: BLOCK_HEADER_LEN as u32,
            escapes_off: BLOCK_HEADER_LEN as u32,
            seq_last_processed_delta: 0,
            control_reason: reason as u16,
            control_len,
            control_last_seq: last_seq,
            control_next_seq: next_seq,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockInput {
    pub header: BlockHeader,
    /// The section bytes following the fixed block header. Detailed column codecs
    /// are layered on this framing in the recorder implementation.
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotRecord {
    pub local_lag_us: i32,
    pub seq_last_delta: u32,
    pub seq_first_prev_delta: u32,
    pub exch_delta_ms: u16,
    pub n_msgs: u16,
    pub changes: Vec<Change>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataPayload {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub records: Vec<SlotRecord>,
}

/// Encodes the v1 snapshot and CSR columns into the payload of a data block.
/// The segment's `encoding_flags.bit0` selects u16 escape quantities; v1 emits
/// that canonical encoding and rejects values outside its u32 escape range.
pub fn encode_data_payload(
    mut header: BlockHeader,
    base_tick: i64,
    snapshot: &DataPayload,
) -> Result<BlockInput> {
    if header.kind != BlockKind::Data
        || header.n_slots == 0
        || snapshot.records.len() > header.n_slots as usize
    {
        return Err(BookError::InvalidField("data payload counts"));
    }
    if snapshot.bids.len() > u16::MAX as usize
        || snapshot.asks.len() > u16::MAX as usize
        || snapshot.records.len() > u16::MAX as usize
    {
        return Err(BookError::InvalidField("data payload counts"));
    }
    if !snapshot.bids.windows(2).all(|w| w[0].tick > w[1].tick)
        || !snapshot.asks.windows(2).all(|w| w[0].tick < w[1].tick)
    {
        return Err(BookError::InvalidField("snapshot order"));
    }
    let mut payload = Vec::new();
    header.base_tick = base_tick;
    header.n_bid = snapshot.bids.len() as u16;
    header.n_ask = snapshot.asks.len() as u16;
    header.n_records = snapshot.records.len() as u16;
    if snapshot
        .records
        .iter()
        .any(|record| record.n_msgs == u16::MAX)
    {
        header.block_flags |= 1 << 4;
    }
    header.snapshot_off = BLOCK_HEADER_LEN as u32;
    for level in &snapshot.bids {
        if level.qty == 0 {
            return Err(BookError::InvalidField("snapshot quantity"));
        }
        put_u16_vec(&mut payload, tick_offset(level.tick, base_tick)?);
    }
    pad8(&mut payload);
    for level in &snapshot.asks {
        put_u16_vec(&mut payload, tick_offset(level.tick, base_tick)?);
    }
    pad8(&mut payload);
    for level in &snapshot.bids {
        if level.qty == 0 {
            return Err(BookError::InvalidField("snapshot quantity"));
        }
        put_u32_vec(
            &mut payload,
            u32::try_from(level.qty).map_err(|_| BookError::InvalidField("snapshot quantity"))?,
        );
    }
    pad8(&mut payload);
    for level in &snapshot.asks {
        if level.qty == 0 {
            return Err(BookError::InvalidField("snapshot quantity"));
        }
        put_u32_vec(
            &mut payload,
            u32::try_from(level.qty).map_err(|_| BookError::InvalidField("snapshot quantity"))?,
        );
    }
    pad8(&mut payload);
    header.records_off = (BLOCK_HEADER_LEN + payload.len()) as u32;

    let mut row_ptr = Vec::with_capacity(snapshot.records.len() + 1);
    let mut changes = Vec::new();
    row_ptr.push(0u32);
    let mut previous_time = None;
    for record in &snapshot.records {
        if previous_time.is_some_and(|previous| record.exch_delta_ms <= previous) {
            return Err(BookError::InvalidField("record time order"));
        }
        previous_time = Some(record.exch_delta_ms);
        let mut sorted = record.changes.clone();
        sorted.sort_by_key(|c| (matches!(c.side, Side::Ask), c.tick));
        if sorted != record.changes
            || sorted
                .windows(2)
                .any(|w| w[0].side == w[1].side && w[0].tick == w[1].tick)
        {
            return Err(BookError::InvalidField("change order"));
        }
        for change in &record.changes {
            tick_offset(change.tick, base_tick)?;
            if change.qty > u32::MAX as u64 {
                return Err(BookError::InvalidField("change quantity"));
            }
            changes.push(change);
        }
        row_ptr.push(
            u32::try_from(changes.len()).map_err(|_| BookError::InvalidField("change count"))?,
        );
    }
    for value in row_ptr {
        put_u32_vec(&mut payload, value);
    }
    for record in &snapshot.records {
        payload.extend_from_slice(&record.local_lag_us.to_le_bytes());
    }
    for record in &snapshot.records {
        put_u32_vec(&mut payload, record.seq_last_delta);
    }
    for record in &snapshot.records {
        put_u32_vec(&mut payload, record.seq_first_prev_delta);
    }
    for record in &snapshot.records {
        put_u16_vec(&mut payload, record.exch_delta_ms);
    }
    for record in &snapshot.records {
        put_u16_vec(&mut payload, record.n_msgs);
    }
    pad8(&mut payload);
    header.changes_off = (BLOCK_HEADER_LEN + payload.len()) as u32;

    let mut escapes = Vec::new();
    for change in &changes {
        put_u16_vec(&mut payload, tick_offset(change.tick, base_tick)?);
    }
    for change in &changes {
        match change.qty {
            0 => put_u16_vec(&mut payload, 0),
            1..=65_534 => put_u16_vec(&mut payload, change.qty as u16),
            qty => {
                put_u16_vec(&mut payload, u16::MAX);
                escapes.push(qty as u32);
            }
        }
    }
    for (index, change) in changes.iter().enumerate() {
        if index % 8 == 0 {
            payload.push(0);
        }
        if change.side == Side::Ask {
            let byte = payload.len() - 1;
            payload[byte] |= 1 << (index % 8);
        }
    }
    pad8(&mut payload);
    header.escapes_off = (BLOCK_HEADER_LEN + payload.len()) as u32;
    for qty in &escapes {
        put_u32_vec(&mut payload, *qty);
    }
    pad8(&mut payload);
    header.n_changes =
        u32::try_from(changes.len()).map_err(|_| BookError::InvalidField("change count"))?;
    header.n_escapes = escapes.len() as u32;
    header.block_len = u32::try_from(BLOCK_HEADER_LEN + payload.len())
        .map_err(|_| BookError::InvalidField("block_len"))?;
    Ok(BlockInput { header, payload })
}

pub fn decode_data_payload(input: &BlockInput) -> Result<DataPayload> {
    let h = &input.header;
    if h.kind != BlockKind::Data {
        return Err(BookError::InvalidField("not data block"));
    }
    let p = &input.payload;
    let rel = |absolute: u32| -> Result<usize> {
        let offset = absolute
            .checked_sub(BLOCK_HEADER_LEN as u32)
            .ok_or(BookError::CorruptPayload("section offset"))? as usize;
        if offset > p.len() {
            return Err(BookError::CorruptPayload("section bounds"));
        }
        Ok(offset)
    };
    let snap = rel(h.snapshot_off)?;
    let records = rel(h.records_off)?;
    let changes_off = rel(h.changes_off)?;
    let escapes_off = rel(h.escapes_off)?;
    if !(snap <= records && records <= changes_off && changes_off <= escapes_off) {
        return Err(BookError::CorruptPayload("section order"));
    }
    let nb = h.n_bid as usize;
    let na = h.n_ask as usize;
    let bid_off_start = snap;
    let ask_off_start = align8(bid_off_start + nb * 2);
    let bid_qty_start = align8(ask_off_start + na * 2);
    let ask_qty_start = align8(bid_qty_start + nb * 4);
    let qty_end = ask_qty_start
        .checked_add(
            na.checked_mul(4)
                .ok_or(BookError::CorruptPayload("snapshot size"))?,
        )
        .ok_or(BookError::CorruptPayload("snapshot size"))?;
    if qty_end > records {
        return Err(BookError::CorruptPayload("snapshot bounds"));
    }
    let mut bids = Vec::with_capacity(nb);
    let mut asks = Vec::with_capacity(na);
    for i in 0..nb {
        let off = get_u16(p, bid_off_start + i * 2) as i64;
        let qty = get_u32(p, bid_qty_start + i * 4) as u64;
        if qty == 0 {
            return Err(BookError::CorruptPayload("zero snapshot quantity"));
        }
        bids.push(Level {
            tick: h.base_tick + off - TICK_BIAS,
            qty,
        });
    }
    for i in 0..na {
        let off = get_u16(p, ask_off_start + i * 2) as i64;
        let qty = get_u32(p, ask_qty_start + i * 4) as u64;
        if qty == 0 {
            return Err(BookError::CorruptPayload("zero snapshot quantity"));
        }
        asks.push(Level {
            tick: h.base_tick + off - TICK_BIAS,
            qty,
        });
    }
    if !bids.windows(2).all(|w| w[0].tick > w[1].tick)
        || !asks.windows(2).all(|w| w[0].tick < w[1].tick)
    {
        return Err(BookError::CorruptPayload("snapshot order"));
    }
    let n = h.n_records as usize;
    let ptr_len = (n + 1)
        .checked_mul(4)
        .ok_or(BookError::CorruptPayload("record columns"))?;
    let row_end = records
        .checked_add(ptr_len)
        .ok_or(BookError::CorruptPayload("record columns"))?;
    let local_start = row_end;
    let seq_last_start = local_start + n * 4;
    let seq_first_start = seq_last_start + n * 4;
    let time_start = seq_first_start + n * 4;
    let msgs_start = time_start + n * 2;
    if align8(msgs_start + n * 2) > changes_off {
        return Err(BookError::CorruptPayload("record column bounds"));
    }
    let mut ptrs = Vec::with_capacity(n + 1);
    for i in 0..=n {
        ptrs.push(get_u32(p, records + i * 4) as usize);
    }
    if ptrs.first() != Some(&0)
        || ptrs.last() != Some(&(h.n_changes as usize))
        || ptrs.windows(2).any(|w| w[0] > w[1])
    {
        return Err(BookError::CorruptPayload("row pointers"));
    }
    let change_count = h.n_changes as usize;
    let col_start = changes_off;
    let val_start = col_start + change_count * 2;
    let side_start = val_start + change_count * 2;
    let side_len = change_count.div_ceil(8);
    let escape_start = align8(side_start + side_len);
    if escape_start != escapes_off || escape_start + h.n_escapes as usize * 4 > p.len() {
        return Err(BookError::CorruptPayload("escape bounds"));
    }
    let mut escape_i = 0usize;
    let mut all_changes = Vec::with_capacity(change_count);
    for i in 0..change_count {
        let off = get_u16(p, col_start + i * 2) as i64;
        let encoded = get_u16(p, val_start + i * 2);
        let qty = if encoded == u16::MAX {
            if escape_i >= h.n_escapes as usize {
                return Err(BookError::CorruptPayload("escape count"));
            }
            let v = get_u32(p, escape_start + escape_i * 4) as u64;
            escape_i += 1;
            if v < u16::MAX as u64 {
                return Err(BookError::CorruptPayload("invalid quantity escape"));
            }
            v
        } else {
            encoded as u64
        };
        let side = if p[side_start + i / 8] & (1 << (i % 8)) != 0 {
            Side::Ask
        } else {
            Side::Bid
        };
        all_changes.push(Change {
            side,
            tick: h.base_tick + off - TICK_BIAS,
            qty,
        });
    }
    if escape_i != h.n_escapes as usize {
        return Err(BookError::CorruptPayload("escape count"));
    }
    let mut decoded_records = Vec::with_capacity(n);
    let mut previous_time = None;
    for i in 0..n {
        let time = get_u16(p, time_start + i * 2);
        if previous_time.is_some_and(|prev| time <= prev) {
            return Err(BookError::CorruptPayload("record time order"));
        }
        previous_time = Some(time);
        let row = &all_changes[ptrs[i]..ptrs[i + 1]];
        if row
            .windows(2)
            .any(|w| (w[0].side == Side::Ask, w[0].tick) > (w[1].side == Side::Ask, w[1].tick))
        {
            return Err(BookError::CorruptPayload("change order"));
        }
        decoded_records.push(SlotRecord {
            local_lag_us: i32::from_le_bytes(
                p[local_start + i * 4..local_start + i * 4 + 4]
                    .try_into()
                    .unwrap(),
            ),
            seq_last_delta: get_u32(p, seq_last_start + i * 4),
            seq_first_prev_delta: get_u32(p, seq_first_start + i * 4),
            exch_delta_ms: time,
            n_msgs: get_u16(p, msgs_start + i * 2),
            changes: row.to_vec(),
        });
    }
    if decoded_records
        .iter()
        .any(|record| record.n_msgs == u16::MAX)
        != (h.block_flags & (1 << 4) != 0)
    {
        return Err(BookError::CorruptPayload("saturated message flag"));
    }
    Ok(DataPayload {
        bids,
        asks,
        records: decoded_records,
    })
}

pub fn encode_segment_header(h: &SegmentHeader) -> Result<[u8; SEGMENT_HEADER_LEN]> {
    if h.symbol.len() > 32 || h.grid_ms == 0 || h.block_duration_ms == 0 {
        return Err(BookError::InvalidField("segment header"));
    }
    let mut b = [0; SEGMENT_HEADER_LEN];
    b[0..4].copy_from_slice(SEGMENT_MAGIC);
    put_u16(&mut b, 4, FORMAT_VERSION);
    put_u16(&mut b, 6, SEGMENT_HEADER_LEN as u16);
    put_u32(&mut b, 8, h.market_id);
    put_u32(&mut b, 12, h.flags);
    put_i64(&mut b, 16, h.segment_start_local_ns);
    put_i64(&mut b, 24, h.segment_start_exch_ns);
    put_i64(&mut b, 32, h.segment_end_local_ns);
    put_i64(&mut b, 40, h.segment_end_exch_ns);
    put_u64(&mut b, 48, h.tick_units);
    b[56] = h.price_decimals;
    b[57] = h.qty_decimals;
    put_u16(&mut b, 58, h.grid_ms);
    put_u16(&mut b, 60, h.block_duration_ms);
    put_u16(&mut b, 62, h.depth_requested);
    put_u16(&mut b, 64, h.depth_available);
    put_u16(&mut b, 66, h.venue_id);
    put_u32(&mut b, 68, h.session_epoch);
    put_u64(&mut b, 72, h.qty_step_units);
    put_u32(&mut b, 80, h.encoding_flags);
    b[84] = h.feed_mode as u8;
    b[88..88 + h.symbol.len()].copy_from_slice(h.symbol.as_bytes());
    let header_crc = crc32c(&b[..120]);
    put_u32(&mut b, 120, header_crc);
    Ok(b)
}

pub fn decode_segment_header(b: &[u8]) -> Result<SegmentHeader> {
    if b.len() < SEGMENT_HEADER_LEN || &b[0..4] != SEGMENT_MAGIC {
        return Err(BookError::InvalidFormat("segment header"));
    }
    if get_u16(b, 4) != FORMAT_VERSION {
        return Err(BookError::UnsupportedVersion(get_u16(b, 4)));
    }
    if get_u16(b, 6) as usize != SEGMENT_HEADER_LEN || crc32c(&b[..120]) != get_u32(b, 120) {
        return Err(BookError::CorruptSegment);
    }
    let end = b[88..120].iter().position(|c| *c == 0).unwrap_or(32);
    let symbol = std::str::from_utf8(&b[88..88 + end])
        .map_err(|_| BookError::InvalidField("symbol utf8"))?
        .to_owned();
    let feed_mode = match b[84] {
        0 => FeedMode::Diff,
        1 => FeedMode::Snapshot,
        _ => return Err(BookError::InvalidField("feed_mode")),
    };
    Ok(SegmentHeader {
        market_id: get_u32(b, 8),
        flags: get_u32(b, 12),
        segment_start_local_ns: get_i64(b, 16),
        segment_start_exch_ns: get_i64(b, 24),
        segment_end_local_ns: get_i64(b, 32),
        segment_end_exch_ns: get_i64(b, 40),
        tick_units: get_u64(b, 48),
        price_decimals: b[56],
        qty_decimals: b[57],
        grid_ms: get_u16(b, 58),
        block_duration_ms: get_u16(b, 60),
        depth_requested: get_u16(b, 62),
        depth_available: get_u16(b, 64),
        venue_id: get_u16(b, 66),
        session_epoch: get_u32(b, 68),
        qty_step_units: get_u64(b, 72),
        encoding_flags: get_u32(b, 80),
        feed_mode,
        symbol,
    })
}

pub fn encode_block(input: &BlockInput) -> Result<Vec<u8>> {
    let h = &input.header;
    let expected = align8(
        BLOCK_HEADER_LEN
            .checked_add(input.payload.len())
            .ok_or(BookError::InvalidField("block length"))?,
    );
    if h.kind == BlockKind::Data && h.n_slots == 0 {
        return Err(BookError::InvalidField("data block n_slots"));
    }
    if h.kind == BlockKind::Gap && (h.n_slots != 0 || h.n_records != 0 || h.n_changes != 0) {
        return Err(BookError::InvalidField("gap block counts"));
    }
    if usize::try_from(h.block_len).ok() != Some(expected) {
        return Err(BookError::InvalidField("block_len"));
    }
    let mut b = vec![0; expected];
    b[BLOCK_HEADER_LEN..BLOCK_HEADER_LEN + input.payload.len()].copy_from_slice(&input.payload);
    b[0..4].copy_from_slice(BLOCK_MAGIC);
    b[4] = h.kind as u8;
    b[5] = h.block_flags;
    put_u32(&mut b, 8, h.block_len);
    put_u32(&mut b, 12, h.market_id);
    put_i64(&mut b, 16, h.block_start_local_ns);
    put_i64(&mut b, 24, h.block_start_exch_ns);
    put_i64(&mut b, 32, h.block_end_local_ns);
    put_i64(&mut b, 40, h.block_end_exch_ns);
    put_i64(&mut b, 48, h.base_tick);
    put_u64(&mut b, 56, h.seq_base);
    put_u16(&mut b, 64, h.n_slots);
    put_u16(&mut b, 66, h.n_records);
    put_u32(&mut b, 68, h.n_changes);
    put_u32(&mut b, 72, h.n_escapes);
    put_u16(&mut b, 76, h.n_bid);
    put_u16(&mut b, 78, h.n_ask);
    put_u32(&mut b, 80, h.snapshot_off);
    put_u32(&mut b, 84, h.records_off);
    put_u32(&mut b, 88, h.changes_off);
    put_u32(&mut b, 92, h.escapes_off);
    let payload_crc = crc32c(&b[128..]);
    put_u32(&mut b, 96, payload_crc);
    put_u32(&mut b, 100, h.seq_last_processed_delta);
    put_u16(&mut b, 104, h.control_reason);
    put_u16(&mut b, 106, h.control_len);
    put_u64(&mut b, 108, h.control_last_seq);
    put_u64(&mut b, 116, h.control_next_seq);
    let header_crc = crc32c(&b[..124]);
    put_u32(&mut b, 124, header_crc);
    Ok(b)
}

pub fn decode_block(b: &[u8]) -> Result<BlockInput> {
    if b.len() < BLOCK_HEADER_LEN || &b[..4] != BLOCK_MAGIC {
        return Err(BookError::InvalidFormat("block header"));
    }
    let kind = match b[4] {
        0 => BlockKind::Data,
        1 => BlockKind::Gap,
        _ => return Err(BookError::InvalidField("block kind")),
    };
    let len = get_u32(b, 8) as usize;
    if len < BLOCK_HEADER_LEN || !len.is_multiple_of(8) || len > b.len() {
        return Err(BookError::CorruptBlock);
    }
    if crc32c(&b[..124]) != get_u32(b, 124) || crc32c(&b[128..len]) != get_u32(b, 96) {
        return Err(BookError::CorruptBlock);
    }
    let header = BlockHeader {
        kind,
        block_flags: b[5],
        block_len: len as u32,
        market_id: get_u32(b, 12),
        block_start_local_ns: get_i64(b, 16),
        block_start_exch_ns: get_i64(b, 24),
        block_end_local_ns: get_i64(b, 32),
        block_end_exch_ns: get_i64(b, 40),
        base_tick: get_i64(b, 48),
        seq_base: get_u64(b, 56),
        n_slots: get_u16(b, 64),
        n_records: get_u16(b, 66),
        n_changes: get_u32(b, 68),
        n_escapes: get_u32(b, 72),
        n_bid: get_u16(b, 76),
        n_ask: get_u16(b, 78),
        snapshot_off: get_u32(b, 80),
        records_off: get_u32(b, 84),
        changes_off: get_u32(b, 88),
        escapes_off: get_u32(b, 92),
        seq_last_processed_delta: get_u32(b, 100),
        control_reason: get_u16(b, 104),
        control_len: get_u16(b, 106),
        control_last_seq: get_u64(b, 108),
        control_next_seq: get_u64(b, 116),
    };
    if kind == BlockKind::Data && header.n_slots == 0 {
        return Err(BookError::CorruptBlock);
    }
    if kind == BlockKind::Gap
        && (header.n_slots != 0 || header.n_records != 0 || header.n_changes != 0)
    {
        return Err(BookError::CorruptBlock);
    }
    Ok(BlockInput {
        header,
        payload: b[128..len].to_vec(),
    })
}

/// Stores the payload as a single Zstandard frame while keeping section offsets
/// relative to the uncompressed block layout. The block header marks this frame
/// with `BLOCK_FLAG_ZSTD`.
pub fn encode_compressed_block(input: &BlockInput, level: i32) -> Result<Vec<u8>> {
    if !(1..=19).contains(&level) {
        return Err(BookError::InvalidField("compression level"));
    }
    let mut payload = zstd::encode_all(input.payload.as_slice(), level)?;
    while !payload.len().is_multiple_of(8) {
        payload.push(0);
    }
    let mut header = input.header.clone();
    header.block_flags |= BLOCK_FLAG_ZSTD;
    header.block_len = u32::try_from(BLOCK_HEADER_LEN + payload.len())
        .map_err(|_| BookError::InvalidField("block_len"))?;
    encode_block(&BlockInput { header, payload })
}

pub fn decode_compressed_block(b: &[u8]) -> Result<BlockInput> {
    use std::io::Read;

    let encoded = decode_block(b)?;
    let mut decoder =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(encoded.payload))?.single_frame();
    let mut payload = Vec::new();
    decoder.read_to_end(&mut payload)?;
    if encoded.header.kind == BlockKind::Gap
        && usize::from(encoded.header.control_len) != payload.len()
    {
        return Err(BookError::CorruptPayload("gap control length"));
    }
    Ok(BlockInput {
        header: encoded.header,
        payload,
    })
}

const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}
pub(crate) fn tick_offset(tick: i64, base_tick: i64) -> Result<u16> {
    let delta = tick
        .checked_sub(base_tick)
        .ok_or(BookError::InvalidField("tick range"))?;
    if !(-TICK_BIAS..TICK_BIAS).contains(&delta) {
        return Err(BookError::InvalidField("tick offset"));
    }
    Ok((delta + TICK_BIAS) as u16)
}
pub(crate) fn align8(n: usize) -> usize {
    (n + 7) & !7
}
fn put_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes())
}
fn put_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes())
}
fn put_u64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes())
}
fn put_i64(b: &mut [u8], o: usize, v: i64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes())
}
fn put_u16_vec(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_le_bytes())
}
fn put_u32_vec(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes())
}
fn pad8(b: &mut Vec<u8>) {
    while !b.len().is_multiple_of(8) {
        b.push(0);
    }
}
fn get_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn get_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn get_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn get_i64(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> SegmentHeader {
        SegmentHeader {
            market_id: 7,
            flags: 0,
            segment_start_local_ns: 10,
            segment_start_exch_ns: 20,
            segment_end_local_ns: 0,
            segment_end_exch_ns: 0,
            tick_units: 1,
            price_decimals: 2,
            qty_decimals: 3,
            grid_ms: 100,
            block_duration_ms: 60_000,
            depth_requested: 100,
            depth_available: 200,
            venue_id: 1,
            session_epoch: 4,
            qty_step_units: 1,
            encoding_flags: 1,
            feed_mode: FeedMode::Diff,
            symbol: "BTCUSDT".into(),
        }
    }

    #[test]
    fn segment_header_roundtrip_and_crc() {
        let encoded = encode_segment_header(&header()).unwrap();
        assert_eq!(decode_segment_header(&encoded).unwrap(), header());
        let mut damaged = encoded;
        damaged[48] ^= 1;
        assert!(decode_segment_header(&damaged).is_err());
    }

    #[test]
    fn block_roundtrip_and_crc_rejection() {
        let block = BlockInput {
            header: BlockHeader::data(7, 10, 20, 3).unwrap(),
            payload: vec![1, 2, 3],
        };
        let encoded = encode_block(&block).unwrap();
        let decoded = decode_block(&encoded).unwrap();
        assert_eq!(decoded.header, block.header);
        assert_eq!(&decoded.payload[..3], &[1, 2, 3]);
        let mut damaged = encoded;
        damaged[128] ^= 1;
        assert!(decode_block(&damaged).is_err());
    }

    #[test]
    fn tick_offset_rejects_values_outside_u16_window() {
        assert_eq!(tick_offset(100, 100).unwrap(), 32768);
        assert_eq!(tick_offset(100 + 32767, 100).unwrap(), u16::MAX);
        assert!(tick_offset(100 - 32769, 100).is_err());
    }

    #[test]
    fn snapshot_and_csr_columns_roundtrip() {
        let input = DataPayload {
            bids: vec![Level { tick: 99, qty: 8 }, Level { tick: 98, qty: 7 }],
            asks: vec![Level { tick: 101, qty: 6 }],
            records: vec![SlotRecord {
                local_lag_us: 12,
                seq_last_delta: 3,
                seq_first_prev_delta: 2,
                exch_delta_ms: 100,
                n_msgs: u16::MAX,
                changes: vec![
                    Change {
                        side: Side::Bid,
                        tick: 96,
                        qty: 65_535,
                    },
                    Change {
                        side: Side::Bid,
                        tick: 97,
                        qty: 65_534,
                    },
                    Change {
                        side: Side::Bid,
                        tick: 98,
                        qty: 0,
                    },
                    Change {
                        side: Side::Ask,
                        tick: 102,
                        qty: 70_000,
                    },
                ],
            }],
        };
        let header = BlockHeader::data(7, 0, 0, 0).unwrap();
        let encoded = encode_block(&encode_data_payload(header, 100, &input).unwrap()).unwrap();
        let decoded = decode_block(&encoded).unwrap();
        assert_eq!(decode_data_payload(&decoded).unwrap(), input);
        assert_ne!(decoded.header.block_flags & (1 << 4), 0);
    }

    #[test]
    fn compressed_block_roundtrip_preserves_uncompressed_sections() {
        let block = BlockInput {
            header: BlockHeader::data(7, 10, 20, 256).unwrap(),
            payload: vec![0x5a; 256],
        };
        let encoded = encode_compressed_block(&block, 3).unwrap();
        let decoded = decode_compressed_block(&encoded).unwrap();
        assert_eq!(decoded.payload, block.payload);
        assert!(encoded.len() < block.header.block_len as usize);
    }
}
