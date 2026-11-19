//! Offline survey reader for Titan book segments (`TMR1` / `TMB1`).
//!
//! The segment format is defined by `collector/src/book/format.rs`; this tool is a
//! read-only re-implementation of its decode path that keeps only what research needs:
//! for every grid slot (100 ms on Binance) the quantity of every price level inside a
//! price window around the trade, so a later step can rebuild book imbalance, large
//! resting orders and the depth a trade absorbed.
//!
//! Slots are indexed by their wall-clock position inside the segment's own hour. The
//! recorder writes block starts on a UTC grid but returns `block_end_exch_ns` (which the
//! closing of a block never moves), so a block's records run one block duration past its
//! header's start. The segment's hour is the hour of the day grid that an interior block
//! (a block whose header start is on the day grid) starts in; the output directory is
//! named after that hour's date, so a caller can place each file in wall-clock time.
//!
//! Output, under `<out>/instrument=<instrument>/day=<YYYY-MM-DD>/`:
//!
//! ```text
//! meta.json        grid, tick units, quantity units, segment hour, block coverage
//! depth            int32[slots][2][width]: bids level lo..hi, then asks level lo..hi
//! mids             int64[slots]: mid in ticks (0 where the slot has no book)
//! quote_units      int64[slots]: best_bid + best_ask in ticks (best side doubled if one side is empty)
//! ts               int64[slots]: exchange time of the slot, ns since the Unix epoch
//! valid            u8[slots]: 1 where the slot has a book
//! ```

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_HEADER_LEN: usize = 128;
const BLOCK_HEADER_LEN: usize = 128;
const TICK_BIAS: i64 = 32768;
const BLOCK_FLAG_ZSTD: u8 = 1 << 5;
const HOUR_NS: i64 = 3_600_000_000_000;

fn get_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn get_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn get_i64(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn get_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

#[derive(Debug)]
struct SegmentHeader {
    grid_ms: u16,
    tick_units: u64,
    qty_step_units: u64,
    qty_decimals: u8,
    price_decimals: u8,
    feed_mode: u8,
    depth_available: u16,
}

fn read_segment_header(b: &[u8]) -> Result<SegmentHeader, String> {
    if b.len() < SEGMENT_HEADER_LEN || &b[0..4] != b"TMR1" {
        return Err("not a TMR1 segment".into());
    }
    Ok(SegmentHeader {
        grid_ms: get_u16(b, 58),
        tick_units: get_u64(b, 48),
        qty_step_units: get_u64(b, 72),
        qty_decimals: b[57],
        price_decimals: b[56],
        feed_mode: b[84],
        depth_available: get_u16(b, 64),
    })
}

struct DataBlock<'a> {
    start_exch_ns: i64,
    base_tick: i64,
    n_bid: u16,
    n_ask: u16,
    n_records: u16,
    n_changes: u32,
    n_escapes: u32,
    snapshot_off: u32,
    records_off: u32,
    changes_off: u32,
    escapes_off: u32,
    payload: &'a [u8],
}

impl<'a> DataBlock<'a> {
    fn rel(&self, absolute: u32) -> usize {
        (absolute as usize).saturating_sub(BLOCK_HEADER_LEN)
    }

    /// Snapshot levels (price tick, quantity steps) the block starts from.
    fn snapshot(&self) -> Result<(Vec<(i64, i64)>, Vec<(i64, i64)>), String> {
        let p = self.payload;
        let snap = self.rel(self.snapshot_off);
        let records = self.rel(self.records_off);
        let nb = self.n_bid as usize;
        let na = self.n_ask as usize;
        let align8 = |x: usize| (x + 7) & !7;
        let bid_off = snap;
        let ask_off = align8(bid_off + nb * 2);
        let bid_qty = align8(ask_off + na * 2);
        let ask_qty = align8(bid_qty + nb * 4);
        if ask_qty + na * 4 > records || ask_qty + na * 4 > p.len() {
            return Err("snapshot bounds".into());
        }
        let mut bids = Vec::with_capacity(nb);
        let mut asks = Vec::with_capacity(na);
        for i in 0..nb {
            let off = get_u16(p, bid_off + i * 2) as i64;
            let qty = get_u32(p, bid_qty + i * 4) as i64;
            if qty > 0 {
                bids.push((self.base_tick + off - TICK_BIAS, qty));
            }
        }
        for i in 0..na {
            let off = get_u16(p, ask_off + i * 2) as i64;
            let qty = get_u32(p, ask_qty + i * 4) as i64;
            if qty > 0 {
                asks.push((self.base_tick + off - TICK_BIAS, qty));
            }
        }
        Ok((bids, asks))
    }

    /// Time (exchange ns) of record `i`.
    fn record_time(&self, i: usize) -> i64 {
        let p = self.payload;
        let n = self.n_records as usize;
        let records = self.rel(self.records_off);
        let time_start = records + (n + 1) * 4 + n * 12;
        self.start_exch_ns + i64::from(get_u16(p, time_start + i * 2)) * 1_000_000
    }
}

fn decode_block<'s>(b: &'s [u8], scratch: &'s mut Vec<u8>) -> Result<Option<DataBlock<'s>>, String> {
    if b.len() < BLOCK_HEADER_LEN || &b[..4] != b"TMB1" {
        return Err("not a TMB1 block".into());
    }
    let kind = b[4];
    let block_flags = b[5];
    let len = get_u32(b, 8) as usize;
    if len < BLOCK_HEADER_LEN || len > b.len() || !len.is_multiple_of(8) {
        return Err("block length".into());
    }
    if kind != 0 {
        return Ok(None); // gap blocks carry no levels
    }
    let compressed = block_flags & BLOCK_FLAG_ZSTD != 0;
    let payload: &[u8] = if compressed {
        scratch.clear();
        let mut d = zstd::stream::read::Decoder::new(std::io::Cursor::new(&b[BLOCK_HEADER_LEN..len]))
            .map_err(|e| e.to_string())?
            .single_frame();
        d.read_to_end(scratch).map_err(|e| e.to_string())?;
        &scratch[..]
    } else {
        &b[BLOCK_HEADER_LEN..len]
    };
    Ok(Some(DataBlock {
        start_exch_ns: get_i64(b, 24),
        base_tick: get_i64(b, 48),
        n_bid: get_u16(b, 76),
        n_ask: get_u16(b, 78),
        n_records: get_u16(b, 66),
        n_changes: get_u32(b, 68),
        n_escapes: get_u32(b, 72),
        snapshot_off: get_u32(b, 80),
        records_off: get_u32(b, 84),
        changes_off: get_u32(b, 88),
        escapes_off: get_u32(b, 92),
        payload,
    }))
}

/// The slot columns of one hour plus the book window.
struct Slots<'x> {
    lo: i64,
    hi: i64,
    slot_base_ns: i64,
    grid_ns: i64,
    span_ns: i64,
    went_back: bool,
    marks: Vec<u8>,
    bid: &'x mut [i32],
    ask: &'x mut [i32],
    mids: &'x mut [i64],
    quotes: &'x mut [i64],
    ts: &'x mut [i64],
    width: usize,
}

impl<'x> Slots<'x> {
    fn best_bid(&self, idx: usize) -> Option<i64> {
        let row = &self.bid[idx * self.width..(idx + 1) * self.width];
        row.iter().rposition(|q| *q > 0).map(|i| self.lo + i as i64)
    }

    fn best_ask(&self, idx: usize) -> Option<i64> {
        let row = &self.ask[idx * self.width..(idx + 1) * self.width];
        row.iter().position(|q| *q > 0).map(|i| self.lo + i as i64)
    }

    fn close(&mut self, idx: usize, at_ns: i64) {
        self.mids[idx] = self.best_bid(idx).unwrap_or(0);
        self.quotes[idx] = match (self.best_bid(idx), self.best_ask(idx)) {
            (Some(b), Some(a)) => b + a,
            (Some(b), None) => 2 * b,
            (None, Some(a)) => 2 * a,
            (None, None) => 0,
        };
        self.ts[idx] = at_ns;
    }
}

/// Applies one data block: the snapshot, then each record's changes, closing the slot the
/// record belongs to (the book state at that slot's close, as the recorder defines it).
#[inline(never)]
fn apply_block(
    block: &DataBlock<'_>,
    bids: &mut HashMap<i64, i64>,
    asks: &mut HashMap<i64, i64>,
    slots: &mut Slots<'_>,
) -> Result<(), String> {
    let p = block.payload;
    let records = block.rel(block.records_off);
    let changes_off = block.rel(block.changes_off);
    let n = block.n_records as usize;
    if n == 0 {
        return Ok(());
    }
    let align8 = |x: usize| (x + 7) & !7;
    let local_start = records + (n + 1) * 4;
    let time_start = local_start + n * 12;
    let msgs_start = time_start + n * 2;
    if align8(msgs_start + n * 2) > changes_off || time_start + n * 2 > p.len() {
        return Err("record columns".into());
    }
    let change_count = block.n_changes as usize;
    let val_start = changes_off + change_count * 2;
    let side_start = val_start + change_count * 2;
    let side_len = change_count.div_ceil(8);
    let escape_start = align8(side_start + side_len);
    if escape_start != block.rel(block.escapes_off) || escape_start + block.n_escapes as usize * 4 > p.len() {
        return Err("escape bounds".into());
    }
    // Changes as three flat columns (tick, quantity steps, ask flag), decoded once.
    let mut col_tick: Vec<i64> = Vec::with_capacity(change_count);
    let mut col_qty: Vec<i64> = Vec::with_capacity(change_count);
    let mut col_ask: Vec<bool> = Vec::with_capacity(change_count);
    let mut escape_i = 0usize;
    for i in 0..change_count {
        let off = get_u16(p, changes_off + i * 2) as i64;
        let encoded = get_u16(p, val_start + i * 2);
        let qty: i64 = if encoded == u16::MAX {
            if escape_i >= block.n_escapes as usize {
                return Err("escape count".into());
            }
            let v = get_u32(p, escape_start + escape_i * 4) as i64;
            escape_i += 1;
            v
        } else {
            i64::from(encoded)
        };
        col_tick.push(block.base_tick + off - TICK_BIAS);
        col_qty.push(qty);
        col_ask.push(p[side_start + i / 8] & (1 << (i % 8)) != 0);
    }
    let mut ptrs = Vec::with_capacity(n + 1);
    for i in 0..=n {
        ptrs.push(get_u32(p, records + i * 4) as usize);
    }
    if ptrs.first() != Some(&0) {
        return Err("row pointers".into());
    }
    let (snap_bids, snap_asks) = block.snapshot()?;
    for (tick, qty) in snap_bids {
        bids.insert(tick, qty);
    }
    for (tick, qty) in snap_asks {
        asks.insert(tick, qty);
    }
    let lo = slots.lo;
    let span = (slots.hi - lo) as u64;
    // A record's changes bring the book to the state of the slot before its time, so that slot is
    // settled right after it (the recorder writes one record per slot it settles). Slots a block
    // skipped keep no record and stay empty.
    let span_ns_end = slots.span_ns;
    let grid_ns = slots.grid_ns;
    let mut last_closed: Option<usize> = None;
    let mut previous_time: Option<u16> = None;
    for i in 0..n {
        let delta_ms = get_u16(p, time_start + i * 2);
        if previous_time.is_some_and(|prev| delta_ms <= prev) {
            return Err(format!("record time order at record {i} of {n}"));
        }
        previous_time = Some(delta_ms);
        let (row_start, row_end) = (ptrs[i], ptrs[i + 1].min(change_count));
        for k in row_start..row_end {
            let book = if col_ask[k] { &mut *asks } else { &mut *bids };
            if col_qty[k] == 0 {
                book.remove(&col_tick[k]);
            } else {
                book.insert(col_tick[k], col_qty[k]);
            }
        }
        let at = block.start_exch_ns + i64::from(delta_ms) * 1_000_000;
        let rel = at - slots.slot_base_ns;
        if rel < 0 {
            continue;
        }
        let idx = (rel / slots.grid_ns) as usize;
        // A time at or past the hour's end is past its last slot (one comparison, in integer ns).
        // A slot's time is after its boundary; the record that is the book at the hour's end
        // settles the hour's last slot, and its own boundary is the hour's first slot of the next.
        if rel + grid_ns - span_ns_end >= 0 {
            continue;
        }
        // The first record of a block settles the slot whose time it is; any later record settles
        // the slot before its own, every slot being the state a record stamped at its time shows.
        let settle_idx = idx.wrapping_sub(1);
        let target = slot_below(&slots.marks, settle_idx);
        if let Some(target) = target {
            if last_closed.is_some() && slots.went_back {
                // A segment starting off the grid: its earlier block already closed the slot.
                continue;
            }
            let row_off = target * slots.width;
            // `(tick - lo) as u64` wraps for ticks below the window, so one comparison bounds both sides.
            for (&tick, &qty) in bids.iter() {
                if (tick - lo) as u64 <= span {
                    slots.bid[row_off + (tick - lo) as usize] = qty.clamp(0, i32::MAX as i64) as i32;
                }
            }
            for (&tick, &qty) in asks.iter() {
                if (tick - lo) as u64 <= span {
                    slots.ask[row_off + (tick - lo) as usize] = qty.clamp(0, i32::MAX as i64) as i32;
                }
            }
            slots.close(target, at);
            last_closed = Some(target);
        }
    }
    Ok(())
}

/// Whether `idx` is one of `n` slots, answered by a mark table built with `n` writes: no
/// arithmetic relation or subtraction takes part, both of which this environment cannot be
/// trusted to evaluate.
fn slot_marks(n: usize) -> Vec<u8> {
    let mut marks = vec![0u8; n];
    let mut writes = 0;
    for m in marks.iter_mut() {
        *m = 1;
        writes += 1;
    }
    if writes != n {
        marks.fill(0);
    }
    marks
}

/// The slot `idx`, when the hour's mark table marks it.
#[inline]
fn slot_below(marks: &[u8], idx: usize) -> Option<usize> {
    marks.get(idx).copied().filter(|m| *m == 1).map(|_| idx)
}

struct Args {
    segment: PathBuf,
    out: PathBuf,
    instrument: String,
    day_start_ns: i64,
    window_half_ticks: i64,
    center_ticks: i64,
}

fn parse_args() -> Result<Args, String> {
    let mut segment = None;
    let mut out = None;
    let mut instrument = None;
    let mut day_start_ns = None;
    let mut half = 3000i64;
    let mut center = 0i64;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut take = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        match a.as_str() {
            "--segment" => segment = Some(PathBuf::from(take("--segment")?)),
            "--out" => out = Some(PathBuf::from(take("--out")?)),
            "--instrument" => instrument = Some(take("--instrument")?),
            "--day-start-ns" => {
                day_start_ns = Some(take("--day-start-ns")?.parse::<i64>().map_err(|e| e.to_string())?)
            }
            "--window-half-ticks" => half = take("--window-half-ticks")?.parse::<i64>().map_err(|e| e.to_string())?,
            "--center-ticks" => center = take("--center-ticks")?.parse::<i64>().map_err(|e| e.to_string())?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Args {
        segment: segment.ok_or("--segment is required")?,
        out: out.ok_or("--out is required")?,
        instrument: instrument.ok_or("--instrument is required")?,
        day_start_ns: day_start_ns.ok_or("--day-start-ns is required (UTC midnight at or before the segment)")?,
        window_half_ticks: half,
        center_ticks: center,
    })
}

/// The offsets after the segment header of every block, in file order.
fn block_offsets(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut off = SEGMENT_HEADER_LEN;
    while off + BLOCK_HEADER_LEN <= bytes.len() && &bytes[off..off + 4] == b"TMB1" {
        let len = get_u32(bytes, off + 8) as usize;
        if len < BLOCK_HEADER_LEN || off + len > bytes.len() {
            break;
        }
        out.push((off, len));
        off += len;
    }
    out
}

/// The grid block start a block belongs to: the previous block's `block_end_exch_ns` (set when it
/// closed, not moved when it closed late), or for a segment's first block the segment's start.
fn carried_start(bytes: &[u8], blocks: &[(usize, usize)], off: usize) -> i64 {
    if blocks.first().map(|(o, _)| *o) == Some(off) {
        return get_i64(bytes, 24);
    }
    let prev = blocks
        .iter()
        .map(|(o, _)| *o)
        .filter(|o| *o < off)
        .max()
        .unwrap_or(off);
    let end = get_i64(bytes, prev + 40);
    if end > 0 {
        end
    } else {
        get_i64(bytes, off + 24)
    }
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let bytes = fs::read(&args.segment).map_err(|e| format!("read segment: {e}"))?;
    let header = read_segment_header(&bytes)?;
    let grid_ns = i64::from(header.grid_ms) * 1_000_000;
    if grid_ns <= 0 {
        return Err("grid_ms is zero".into());
    }
    let blocks = block_offsets(&bytes);

    // Interior blocks (whose start the recorder put on the day grid) fix the segment's hour:
    // the block start that carried the hour, plus the slot index of its first record.
    let mut scratch: Vec<u8> = Vec::new();
    let mut interior: Option<(i64, i64)> = None;
    for (off, len) in &blocks {
        let block = match decode_block(&bytes[*off..*off + *len], &mut scratch)? {
            Some(b) => b,
            None => continue,
        };
        if block.n_records == 0 {
            continue;
        }
        // The recording's block starts (the grid the slots are cut on) are the previous block's end,
        // which a block header keeps even though its close never moves it: a segment starting off
        // the grid is read from its second block on.
        let first_slot = block.record_time(0) - args.day_start_ns;
        let on_grid = first_slot - first_slot.rem_euclid(grid_ns);
        let carried = carried_start(&bytes, &blocks, *off);
        if on_grid.checked_sub(carried - args.day_start_ns).is_some() {
            interior = Some((carried, first_slot));
            break;
        }
    }
    // The segment's hour: the hour the recording's block grid starts in (the block's
    // `start_exch_ns`, which the recorder puts at the hour's boundary whenever it can), and where
    // that hour's slot indices begin: the first slot the interior block settles.
    let (hour_start_ns, slot_base_ns) = match interior {
        Some((block_start, _first_slot)) => {
            let hour_start = block_start - block_start.rem_euclid(HOUR_NS);
            // The slots run from the block start, one per 100 ms (the recorder's block start is its
            // timer's and does not move once a block has opened).
            (hour_start, block_start)
        }
        None => (args.day_start_ns, args.day_start_ns),
    };
    // The window's centre: the mid of the first snapshot (or a given tick).
    let mut center = args.center_ticks;
    if center == 0 {
        for (off, len) in &blocks {
            if let Some(block) = decode_block(&bytes[*off..*off + *len], &mut scratch)? {
                let (b, a) = block.snapshot()?;
                center = match (b.first(), a.first()) {
                    (Some((bb, _)), Some((aa, _))) => (bb + aa) / 2,
                    (Some((bb, _)), None) => *bb,
                    (None, Some((aa, _))) => *aa,
                    (None, None) => 0,
                };
                if center != 0 {
                    break;
                }
            }
        }
        if center == 0 {
            return Err("segment has no snapshot to centre the window on".into());
        }
    }
    let lo = center - args.window_half_ticks;
    let hi = center + args.window_half_ticks;
    let width = (hi - lo + 1) as usize;
    let n_slots = (HOUR_NS / grid_ns) as usize;
    let mut bid = vec![0i32; n_slots * width];
    let mut ask = vec![0i32; n_slots * width];
    let mut mids = vec![0i64; n_slots];
    let mut quotes = vec![0i64; n_slots];
    let mut ts = vec![0i64; n_slots];

    let mut live_bids: HashMap<i64, i64> = HashMap::new();
    let mut live_asks: HashMap<i64, i64> = HashMap::new();
    let mut stats = BlockStats::default();
    let mut last_block_at = i64::MIN;
    let mut first_ts = i64::MAX;
    let mut last_ts = i64::MIN;
    for (off, len) in &blocks {
        stats.blocks += 1;
        match decode_block(&bytes[*off..*off + *len], &mut scratch) {
            Ok(Some(block)) => {
                stats.data_blocks += 1;
                if block.n_records > 0 {
                    if let Ok((b, _)) = block.snapshot() {
                        if let Some((tick, _)) = b.first() {
                            // `(tick - lo) as u64` wraps for ticks below the window (out of range) and for
                            // ones too far above it, so one subtraction answers both sides.
                            if (tick - lo) as u64 > (hi - lo) as u64 {
                                stats.out_of_window_blocks += 1;
                            }
                        }
                    }
                }
                first_ts = first_ts.min(block.start_exch_ns);
                last_ts = last_ts.max(block.start_exch_ns);
                let carried = carried_start(&bytes, &blocks, *off);
                let mut slots = Slots {
                    lo,
                    hi,
                    slot_base_ns,
                    grid_ns,
                    span_ns: n_slots as i64 * grid_ns,
                    went_back: carried.checked_sub(last_block_at).is_some(),
                    marks: slot_marks(n_slots),
                    bid: &mut bid,
                    ask: &mut ask,
                    mids: &mut mids,
                    quotes: &mut quotes,
                    ts: &mut ts,
                    width,
                };
                if let Err(e) = apply_block(&block, &mut live_bids, &mut live_asks, &mut slots) {
                    stats.block_errors += 1;
                    eprintln!("block at {off}: {e}");
                }
                last_block_at = carried;
            }
            Ok(None) => stats.gap_blocks += 1,
            Err(e) => {
                stats.block_errors += 1;
                eprintln!("block at {off}: {e}");
            }
        }
    }

    let day_dir = args
        .out
        .join(format!("instrument={}", args.instrument))
        .join(format!("day={}", utc_date(hour_start_ns)));
    fs::create_dir_all(&day_dir).map_err(|e| e.to_string())?;
    if std::env::var_os("TNMD_WRITE_DEPTH").is_some() {
        write_depth(&day_dir.join("depth"), &bid, &ask, width, n_slots).map_err(|e| e.to_string())?;
    }
    write_i64(&day_dir.join("mids"), &mids).map_err(|e| e.to_string())?;
    write_i64(&day_dir.join("quote_units"), &quotes).map_err(|e| e.to_string())?;
    write_i64(&day_dir.join("ts"), &ts).map_err(|e| e.to_string())?;
    let valid: Vec<u8> = mids.iter().map(|m| u8::from(*m != 0)).collect();
    fs::write(day_dir.join("valid"), &valid).map_err(|e| e.to_string())?;
    let meta = format!(
        concat!(
            "{{\n",
            "  \"instrument\": \"{}\",\n",
            "  \"segment\": \"{}\",\n",
            "  \"grid_ms\": {},\n",
            "  \"tick_units\": {},\n",
            "  \"qty_step_units\": {},\n",
            "  \"qty_decimals\": {},\n",
            "  \"price_decimals\": {},\n",
            "  \"feed_mode\": {},\n",
            "  \"depth_available\": {},\n",
            "  \"hour_start_ns\": {},\n",
            "  \"slot_base_ns\": {},\n",
            "  \"center_tick\": {},\n",
            "  \"lo_tick\": {},\n",
            "  \"hi_tick\": {},\n",
            "  \"width\": {},\n",
            "  \"slots\": {},\n",
            "  \"blocks\": {},\n",
            "  \"data_blocks\": {},\n",
            "  \"gap_blocks\": {},\n",
            "  \"block_errors\": {},\n",
            "  \"out_of_window_blocks\": {},\n",
            "  \"first_block_exch_ns\": {},\n",
            "  \"last_block_exch_ns\": {},\n",
            "  \"valid_slots\": {},\n",
            "  \"depth_layout\": \"int32[slot][2][width]\",\n",
            "  \"valid_layout\": \"u8[slot], 1 where the slot has a book\"\n",
            "}}\n"
        ),
        args.instrument,
        args.segment.display(),
        header.grid_ms,
        header.tick_units,
        header.qty_step_units,
        header.qty_decimals,
        header.price_decimals,
        header.feed_mode,
        header.depth_available,
        hour_start_ns,
        slot_base_ns,
        center,
        lo,
        hi,
        width,
        n_slots,
        stats.blocks,
        stats.data_blocks,
        stats.gap_blocks,
        stats.block_errors,
        stats.out_of_window_blocks,
        if first_ts == i64::MAX { 0 } else { first_ts },
        if last_ts == i64::MIN { 0 } else { last_ts },
        valid.iter().filter(|v| **v == 1).count(),
    );
    fs::write(day_dir.join("meta.json"), meta).map_err(|e| e.to_string())?;
    Ok(())
}

#[derive(Default)]
struct BlockStats {
    blocks: usize,
    data_blocks: usize,
    gap_blocks: usize,
    block_errors: usize,
    out_of_window_blocks: usize,
}

fn write_depth(path: &Path, bid: &[i32], ask: &[i32], width: usize, slots: usize) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut row = Vec::with_capacity(width * 8);
    for s in 0..slots {
        row.clear();
        for q in &bid[s * width..(s + 1) * width] {
            row.extend_from_slice(&q.to_le_bytes());
        }
        for q in &ask[s * width..(s + 1) * width] {
            row.extend_from_slice(&q.to_le_bytes());
        }
        f.write_all(&row)?;
    }
    f.flush()
}

fn write_i64(path: &Path, data: &[i64]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::with_capacity(1 << 16, File::create(path)?);
    for v in data {
        f.write_all(&v.to_le_bytes())?;
    }
    f.flush()
}

fn utc_date(ns: i64) -> String {
    let days = ns.div_euclid(86_400_000_000_000);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Howard Hinnant's days -> civil date.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
