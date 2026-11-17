use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};

use super::{
    BLOCK_HEADER_LEN, BookError, GapReason, INDEX_ENTRY_LEN, Result, SEGMENT_HEADER_LEN,
    format::{
        BlockInput, MarketMeta, SegmentHeader, crc32c, decode_block, decode_compressed_block,
        decode_segment_header, encode_compressed_block, encode_segment_header,
    },
};

const FOOTER_MAGIC: &[u8; 4] = b"TMID";
const SIDECAR_MAGIC: &[u8; 4] = b"TMDX";

pub struct BookWriter {
    root: PathBuf,
    venue: String,
    instrument: String,
    segment_duration_ns: i64,
    meta: MarketMeta,
    compression_level: i32,
    compressed: bool,
    fsync_interval_blocks: u16,
    current: Option<SegmentWriter>,
    segment_start_exch_ns: Option<i64>,
    segment_epoch: Option<u32>,
    segment_serial: u32,
    last_end_local_ns: i64,
    last_end_exch_ns: i64,
}

impl BookWriter {
    pub fn new(
        root: &Path,
        venue: &str,
        instrument: &str,
        meta: MarketMeta,
        segment_duration_ms: u32,
        compression_level: i32,
    ) -> Result<Self> {
        Self::new_with_options(
            root,
            venue,
            instrument,
            meta,
            segment_duration_ms,
            compression_level,
            true,
            1,
        )
    }

    pub fn new_with_compression(
        root: &Path,
        venue: &str,
        instrument: &str,
        meta: MarketMeta,
        segment_duration_ms: u32,
        compression_level: i32,
        compressed: bool,
    ) -> Result<Self> {
        Self::new_with_options(
            root,
            venue,
            instrument,
            meta,
            segment_duration_ms,
            compression_level,
            compressed,
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_options(
        root: &Path,
        venue: &str,
        instrument: &str,
        meta: MarketMeta,
        segment_duration_ms: u32,
        compression_level: i32,
        compressed: bool,
        fsync_interval_blocks: u16,
    ) -> Result<Self> {
        if venue.is_empty()
            || instrument.is_empty()
            || !(60_000..=86_400_000).contains(&segment_duration_ms)
            || !(1..=19).contains(&compression_level)
            || !(1..=60).contains(&fsync_interval_blocks)
            || meta.grid_ms == 0
        {
            return Err(BookError::InvalidField("book writer configuration"));
        }
        validate_path_component(venue)?;
        validate_path_component(instrument)?;
        validate_path_component(&meta.symbol)?;
        Ok(Self {
            root: root.to_path_buf(),
            venue: venue.to_owned(),
            instrument: instrument.to_owned(),
            segment_duration_ns: i64::from(segment_duration_ms) * 1_000_000,
            meta,
            compression_level,
            compressed,
            fsync_interval_blocks,
            current: None,
            segment_start_exch_ns: None,
            segment_epoch: None,
            segment_serial: 0,
            last_end_local_ns: 0,
            last_end_exch_ns: 0,
        })
    }

    pub fn append(&mut self, block: &BlockInput) -> Result<()> {
        if block.header.kind == super::BlockKind::Gap {
            return Err(BookError::InvalidField(
                "use append_gap for epoch boundaries",
            ));
        }
        if block.header.market_id != self.meta.market_id {
            return Err(BookError::InvalidField("market id mismatch"));
        }
        let rotate = self.current.is_some()
            && (self.segment_epoch != Some(self.meta.session_epoch)
                || self.segment_start_exch_ns.is_some_and(|start| {
                    block.header.block_start_exch_ns
                        >= start.saturating_add(self.segment_duration_ns)
                }));
        if rotate {
            self.finish(self.last_end_local_ns, self.last_end_exch_ns)?;
        }
        if self.current.is_none() {
            self.open_segment(
                block.header.block_start_local_ns,
                block.header.block_start_exch_ns,
            )?;
        }
        let encoded = if self.compressed {
            encode_compressed_block(block, self.compression_level)?
        } else {
            super::format::encode_block(block)?
        };
        self.current
            .as_mut()
            .ok_or(BookError::CorruptSegment)?
            .append(&encoded)?;
        self.last_end_local_ns = block.header.block_end_local_ns;
        self.last_end_exch_ns = block.header.block_end_exch_ns;
        Ok(())
    }

    /// Starts a new epoch and puts the gap control block at the head of its
    /// segment, so readers can never encounter a gap in the middle of a file.
    #[allow(clippy::too_many_arguments)]
    pub fn append_gap(
        &mut self,
        start_local_ns: i64,
        start_exch_ns: i64,
        end_local_ns: i64,
        end_exch_ns: i64,
        reason: GapReason,
        last_seq: u64,
        next_seq: u64,
        detail: &[u8],
    ) -> Result<()> {
        if self.current.is_some() {
            self.finish(self.last_end_local_ns, self.last_end_exch_ns)?;
        }
        self.meta.session_epoch = self
            .meta
            .session_epoch
            .checked_add(1)
            .ok_or(BookError::InvalidField("session epoch overflow"))?;
        self.open_segment(start_local_ns, start_exch_ns)?;
        let header = super::BlockHeader::gap(
            self.meta.market_id,
            start_local_ns,
            start_exch_ns,
            end_local_ns,
            end_exch_ns,
            reason,
            last_seq,
            next_seq,
            detail,
        )?;
        let input = BlockInput {
            header,
            payload: detail.to_vec(),
        };
        let encoded = if self.compressed {
            encode_compressed_block(&input, self.compression_level)?
        } else {
            super::format::encode_block(&input)?
        };
        self.current
            .as_mut()
            .ok_or(BookError::CorruptSegment)?
            .append(&encoded)?;
        self.last_end_local_ns = end_local_ns;
        self.last_end_exch_ns = end_exch_ns;
        Ok(())
    }

    pub fn finish(&mut self, end_local_ns: i64, end_exch_ns: i64) -> Result<()> {
        let Some(writer) = self.current.take() else {
            return Ok(());
        };
        writer.seal(end_local_ns, end_exch_ns)?;
        self.segment_start_exch_ns = None;
        self.segment_epoch = None;
        Ok(())
    }

    fn open_segment(&mut self, start_local_ns: i64, start_exch_ns: i64) -> Result<()> {
        let datetime = DateTime::<Utc>::from_timestamp_nanos(start_exch_ns);
        let date = datetime.format("%Y-%m-%d").to_string();
        let file_stamp = datetime.format("%Y%m%dT%H%M%SZ");
        let directory = self
            .root
            .join("book")
            .join(format!("venue={}", self.venue))
            .join(format!("instrument={}", self.instrument))
            .join(format!("symbol={}", self.meta.symbol))
            .join(format!("date={date}"));
        fs::create_dir_all(&directory)?;
        let epoch = self.meta.session_epoch;
        let base = format!("segment-{file_stamp}-e{epoch}");
        let mut path = directory.join(format!("{base}.tnmd"));
        while path.exists() || PathBuf::from(format!("{}.tmp", path.display())).exists() {
            self.segment_serial = self
                .segment_serial
                .checked_add(1)
                .ok_or(BookError::InvalidField("segment serial overflow"))?;
            path = directory.join(format!("{base}-r{}.tnmd", self.segment_serial));
        }
        let header = SegmentHeader::from_meta(&self.meta, start_local_ns, start_exch_ns);
        self.current = Some(SegmentWriter::create_with_sync_interval(
            &path,
            &header,
            self.fsync_interval_blocks,
        )?);
        self.segment_start_exch_ns = Some(start_exch_ns);
        self.segment_epoch = Some(self.meta.session_epoch);
        Ok(())
    }
}

fn validate_path_component(value: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(BookError::InvalidField("path component"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryReport {
    pub blocks_found: u32,
    pub corrupt_blocks: u32,
    pub truncated_bytes: u64,
    pub rebuilt_index: bool,
}

#[derive(Clone, Debug)]
struct IndexEntry {
    block_off: u64,
    block_len: u32,
    payload_crc32c: u32,
    start_local_ns: i64,
    end_local_ns: i64,
    start_exch_ns: i64,
    first_seq: u64,
    n_records: u16,
    flags: u16,
    n_changes: u32,
    block_kind: u8,
}

impl IndexEntry {
    fn encode(&self) -> [u8; INDEX_ENTRY_LEN] {
        let mut b = [0; INDEX_ENTRY_LEN];
        b[0..8].copy_from_slice(&self.block_off.to_le_bytes());
        b[8..12].copy_from_slice(&self.block_len.to_le_bytes());
        b[12..16].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        b[16..24].copy_from_slice(&self.start_local_ns.to_le_bytes());
        b[24..32].copy_from_slice(&self.end_local_ns.to_le_bytes());
        b[32..40].copy_from_slice(&self.start_exch_ns.to_le_bytes());
        b[40..48].copy_from_slice(&self.first_seq.to_le_bytes());
        b[48..50].copy_from_slice(&self.n_records.to_le_bytes());
        b[50..52].copy_from_slice(&self.flags.to_le_bytes());
        b[52..56].copy_from_slice(&self.n_changes.to_le_bytes());
        b[56] = self.block_kind;
        b
    }
}

pub struct SegmentWriter {
    file: File,
    temp_path: PathBuf,
    final_path: PathBuf,
    header: SegmentHeader,
    entries: Vec<IndexEntry>,
    fsync_interval_blocks: u16,
}

impl SegmentWriter {
    pub fn create(path: &Path, header: &SegmentHeader) -> Result<Self> {
        Self::create_with_sync_interval(path, header, 1)
    }

    pub fn create_with_sync_interval(
        path: &Path,
        header: &SegmentHeader,
        fsync_interval_blocks: u16,
    ) -> Result<Self> {
        if !(1..=60).contains(&fsync_interval_blocks) {
            return Err(BookError::InvalidField("fsync interval"));
        }
        let final_path = path.to_path_buf();
        let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(&encode_segment_header(header)?)?;
        file.sync_all()?;
        Ok(Self {
            file,
            temp_path,
            final_path,
            header: header.clone(),
            entries: Vec::new(),
            fsync_interval_blocks,
        })
    }

    pub fn append(&mut self, block: &[u8]) -> Result<()> {
        let encoded = decode_block(block)?;
        let decoded = if encoded.header.block_flags & super::format::BLOCK_FLAG_ZSTD != 0 {
            decode_compressed_block(block)?
        } else {
            encoded
        };
        validate_control_block(&decoded)?;
        if decoded.header.market_id != self.header.market_id {
            return Err(BookError::InvalidField("market_id mismatch"));
        }
        let offset = self.file.seek(SeekFrom::End(0))?;
        if offset < SEGMENT_HEADER_LEN as u64 {
            return Err(BookError::CorruptSegment);
        }
        self.file.write_all(block)?;
        let entry = index_entry(offset, block, &decoded);
        let sidecar = PathBuf::from(format!("{}.idx", self.temp_path.display()));
        append_sidecar(&sidecar, &entry)?;
        self.entries.push(entry);
        if self
            .entries
            .len()
            .is_multiple_of(usize::from(self.fsync_interval_blocks))
        {
            self.file.sync_data()?;
            File::open(sidecar)?.sync_data()?;
        }
        Ok(())
    }

    pub fn seal(mut self, end_local_ns: i64, end_exch_ns: i64) -> Result<PathBuf> {
        self.header.flags |= 1;
        self.header.segment_end_local_ns = end_local_ns;
        self.header.segment_end_exch_ns = end_exch_ns;
        let index_off = self.file.seek(SeekFrom::End(0))?;
        let mut index = Vec::with_capacity(8 + self.entries.len() * INDEX_ENTRY_LEN);
        index.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        let entries_bytes: Vec<u8> = self.entries.iter().flat_map(|e| e.encode()).collect();
        index.extend_from_slice(&crc32c(&entries_bytes).to_le_bytes());
        index.extend_from_slice(&entries_bytes);
        self.file.write_all(&index)?;
        let mut footer = [0; 32];
        footer[0..4].copy_from_slice(&crc32c(&index).to_le_bytes());
        footer[4..12].copy_from_slice(&index_off.to_le_bytes());
        footer[12..16].copy_from_slice(&(index.len() as u32).to_le_bytes());
        footer[16..20].copy_from_slice(FOOTER_MAGIC);
        self.file.write_all(&footer)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&encode_segment_header(&self.header)?)?;
        self.file.sync_all()?;
        drop(self.file);
        fs::rename(&self.temp_path, &self.final_path)?;
        if let Some(parent) = self
            .final_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            File::open(parent)?.sync_all()?;
        }
        let temp_idx = PathBuf::from(format!("{}.idx", self.temp_path.display()));
        let final_idx = PathBuf::from(format!("{}.idx", self.final_path.display()));
        if temp_idx.exists() {
            fs::rename(temp_idx, final_idx)?;
        }
        Ok(self.final_path)
    }
}

pub struct SegmentReader {
    file: File,
    pub header: SegmentHeader,
    blocks: Vec<ReaderBlock>,
    truncated_tail: bool,
}

#[derive(Clone, Copy, Debug)]
struct ReaderBlock {
    offset: u64,
    len: u32,
    valid: bool,
    start_exch_ns: i64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mut header_bytes = [0; SEGMENT_HEADER_LEN];
        read_exact_at(&file, &mut header_bytes, 0)?;
        let header = decode_segment_header(&header_bytes)?;
        let (blocks, truncated_tail) = scan_blocks(&file)?;
        Ok(Self {
            file,
            header,
            blocks,
            truncated_tail,
        })
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn truncated_tail(&self) -> bool {
        self.truncated_tail
    }

    pub fn read_block(&self, index: usize) -> Result<BlockInput> {
        let block = *self
            .blocks
            .get(index)
            .ok_or(BookError::InvalidField("block index"))?;
        if !block.valid {
            return Err(BookError::CorruptBlock);
        }
        let mut bytes = vec![0; block.len as usize];
        read_exact_at(&self.file, &mut bytes, block.offset)?;
        let encoded = decode_block(&bytes)?;
        let decoded = if encoded.header.block_flags & super::format::BLOCK_FLAG_ZSTD != 0 {
            decode_compressed_block(&bytes)?
        } else {
            encoded
        };
        validate_control_block(&decoded)?;
        Ok(decoded)
    }

    pub fn find_block(&self, exch_ts_ns: i64) -> Option<usize> {
        let end = self
            .blocks
            .partition_point(|block| block.start_exch_ns <= exch_ts_ns);
        self.blocks[..end].iter().rposition(|block| block.valid)
    }
}

pub fn recover_segment(path: &Path) -> Result<RecoveryReport> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut header = [0; SEGMENT_HEADER_LEN];
    read_exact_at(&file, &mut header, 0)?;
    decode_segment_header(&header)?;
    let size = file.metadata()?.len();
    let scan_end = sealed_data_end(&file, size)?.unwrap_or(size);
    let mut offset = SEGMENT_HEADER_LEN as u64;
    let mut blocks = Vec::new();
    let mut corrupt_blocks = 0;
    let mut truncated_bytes = 0;
    while offset + BLOCK_HEADER_LEN as u64 <= scan_end {
        let mut bh = [0; BLOCK_HEADER_LEN];
        read_exact_at(&file, &mut bh, offset)?;
        if &bh[..4] != b"TMB1"
            || crc32c(&bh[..124]) != u32::from_le_bytes(bh[124..128].try_into().unwrap())
        {
            if let Some(next) = find_next_block(&file, offset + 8, scan_end)? {
                corrupt_blocks += 1;
                offset = next;
                continue;
            }
            if scan_end == size {
                truncated_bytes = size - offset;
                file.set_len(offset)?;
            }
            break;
        }
        let len = u32::from_le_bytes(bh[8..12].try_into().unwrap()) as u64;
        if len < BLOCK_HEADER_LEN as u64 || !len.is_multiple_of(8) || offset + len > scan_end {
            if scan_end == size {
                truncated_bytes = size - offset;
                file.set_len(offset)?;
            }
            break;
        }
        let mut bytes = vec![0; len as usize];
        read_exact_at(&file, &mut bytes, offset)?;
        let decoded = decode_block(&bytes).and_then(|encoded| {
            if encoded.header.block_flags & super::format::BLOCK_FLAG_ZSTD != 0 {
                decode_compressed_block(&bytes)
            } else {
                Ok(encoded)
            }
        });
        match decoded {
            Ok(block) if validate_control_block(&block).is_ok() => {
                blocks.push(index_entry(offset, &bytes, &block))
            }
            _ => {
                corrupt_blocks += 1;
                blocks.push(index_entry_from_header(offset, &bytes, true));
            }
        }
        offset += len;
    }
    if scan_end == size && offset < size && truncated_bytes == 0 {
        truncated_bytes = size - offset;
        file.set_len(offset)?;
    }
    file.sync_all()?;
    write_index(path, &blocks)?;
    Ok(RecoveryReport {
        blocks_found: blocks.len() as u32,
        corrupt_blocks,
        truncated_bytes,
        rebuilt_index: true,
    })
}

fn scan_blocks(file: &File) -> Result<(Vec<ReaderBlock>, bool)> {
    let size = file.metadata()?.len();
    let scan_end = sealed_data_end(file, size)?.unwrap_or(size);
    let mut offset = SEGMENT_HEADER_LEN as u64;
    let mut out = Vec::new();
    let mut last_start_exch_ns = i64::MIN;
    while offset + BLOCK_HEADER_LEN as u64 <= scan_end {
        let mut h = [0; BLOCK_HEADER_LEN];
        read_exact_at(file, &mut h, offset)?;
        if &h[..4] != b"TMB1"
            || crc32c(&h[..124]) != u32::from_le_bytes(h[124..128].try_into().unwrap())
        {
            if let Some(next) = find_next_block(file, offset + 8, scan_end)? {
                out.push(ReaderBlock {
                    offset,
                    len: u32::try_from(next - offset).map_err(|_| BookError::CorruptSegment)?,
                    valid: false,
                    start_exch_ns: last_start_exch_ns,
                });
                offset = next;
                continue;
            }
            break;
        }
        let len = u32::from_le_bytes(h[8..12].try_into().unwrap());
        if len < BLOCK_HEADER_LEN as u32 || !len.is_multiple_of(8) || offset + len as u64 > scan_end
        {
            if let Some(next) = find_next_block(file, offset + 8, scan_end)? {
                out.push(ReaderBlock {
                    offset,
                    len: u32::try_from(next - offset).map_err(|_| BookError::CorruptSegment)?,
                    valid: false,
                    start_exch_ns: last_start_exch_ns,
                });
                offset = next;
                continue;
            }
            break;
        }
        let mut b = vec![0; len as usize];
        read_exact_at(file, &mut b, offset)?;
        let start_exch_ns = i64::from_le_bytes(h[24..32].try_into().unwrap());
        last_start_exch_ns = start_exch_ns;
        let decoded = decode_block(&b).and_then(|encoded| {
            if encoded.header.block_flags & super::format::BLOCK_FLAG_ZSTD != 0 {
                decode_compressed_block(&b)
            } else {
                Ok(encoded)
            }
        });
        let valid = decoded.is_ok_and(|block| validate_control_block(&block).is_ok());
        out.push(ReaderBlock {
            offset,
            len,
            valid,
            start_exch_ns,
        });
        offset += len as u64;
    }
    Ok((out, offset < scan_end))
}

fn index_entry(offset: u64, bytes: &[u8], block: &BlockInput) -> IndexEntry {
    IndexEntry {
        block_off: offset,
        block_len: bytes.len() as u32,
        payload_crc32c: u32::from_le_bytes(bytes[96..100].try_into().unwrap()),
        start_local_ns: block.header.block_start_local_ns,
        end_local_ns: block.header.block_end_local_ns,
        start_exch_ns: block.header.block_start_exch_ns,
        first_seq: block.header.seq_base,
        n_records: block.header.n_records,
        flags: 0,
        n_changes: block.header.n_changes,
        block_kind: block.header.kind as u8,
    }
}

fn validate_control_block(block: &BlockInput) -> Result<()> {
    if block.header.kind == super::BlockKind::Gap
        && usize::from(block.header.control_len) != block.payload.len()
    {
        return Err(BookError::CorruptPayload("gap control length"));
    }
    Ok(())
}

fn index_entry_from_header(offset: u64, bytes: &[u8], corrupt: bool) -> IndexEntry {
    IndexEntry {
        block_off: offset,
        block_len: bytes.len() as u32,
        payload_crc32c: u32::from_le_bytes(bytes[96..100].try_into().unwrap()),
        start_local_ns: i64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        end_local_ns: i64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        start_exch_ns: i64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        first_seq: u64::from_le_bytes(bytes[56..64].try_into().unwrap()),
        n_records: u16::from_le_bytes(bytes[66..68].try_into().unwrap()),
        flags: if corrupt { 1 } else { 0 },
        n_changes: u32::from_le_bytes(bytes[68..72].try_into().unwrap()),
        block_kind: bytes[4],
    }
}

fn sealed_data_end(file: &File, size: u64) -> Result<Option<u64>> {
    if size < 32 {
        return Ok(None);
    }
    let mut footer = [0; 32];
    read_exact_at(file, &mut footer, size - 32)?;
    if &footer[16..20] != FOOTER_MAGIC {
        return Ok(None);
    }
    let index_off = u64::from_le_bytes(footer[4..12].try_into().unwrap());
    let index_len = u32::from_le_bytes(footer[12..16].try_into().unwrap()) as u64;
    if index_off < SEGMENT_HEADER_LEN as u64
        || index_off
            .checked_add(index_len)
            .and_then(|n| n.checked_add(32))
            != Some(size)
    {
        return Err(BookError::CorruptSegment);
    }
    Ok(Some(index_off))
}

fn append_sidecar(path: &Path, entry: &IndexEntry) -> Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    if f.metadata()?.len() == 0 {
        let mut h = [0; 32];
        h[..4].copy_from_slice(SIDECAR_MAGIC);
        h[4..6].copy_from_slice(&1u16.to_le_bytes());
        h[6..8].copy_from_slice(&32u16.to_le_bytes());
        f.write_all(&h)?;
    }
    f.seek(SeekFrom::End(0))?;
    f.write_all(&entry.encode())?;
    let count = ((f.metadata()?.len() - 32) / INDEX_ENTRY_LEN as u64) as u32;
    use std::os::unix::fs::FileExt;
    f.write_at(&count.to_le_bytes(), 8)?;
    let mut h = [0; 16];
    f.read_at(&mut h, 0)?;
    let c = crc32c(&h);
    f.write_at(&c.to_le_bytes(), 16)?;
    Ok(())
}

fn write_index(path: &Path, entries: &[IndexEntry]) -> Result<()> {
    let idx = PathBuf::from(format!("{}.idx", path.display()));
    let tmp = PathBuf::from(format!("{}.tmp", idx.display()));
    let data: Vec<u8> = entries.iter().flat_map(|e| e.encode()).collect();
    let mut h = [0; 32];
    h[..4].copy_from_slice(SIDECAR_MAGIC);
    h[4..6].copy_from_slice(&1u16.to_le_bytes());
    h[6..8].copy_from_slice(&32u16.to_le_bytes());
    h[8..12].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    let header_crc = crc32c(&h[..16]);
    h[16..20].copy_from_slice(&header_crc.to_le_bytes());
    let mut f = File::create(&tmp)?;
    f.write_all(&h)?;
    f.write_all(&data)?;
    f.sync_all()?;
    fs::rename(tmp, idx)?;
    Ok(())
}

fn find_next_block(file: &File, from: u64, size: u64) -> Result<Option<u64>> {
    let mut at = (from + 7) & !7;
    let mut magic = [0; 4];
    while at + BLOCK_HEADER_LEN as u64 <= size {
        read_exact_at(file, &mut magic, at)?;
        if &magic == b"TMB1" {
            let mut h = [0; BLOCK_HEADER_LEN];
            read_exact_at(file, &mut h, at)?;
            let len = u32::from_le_bytes(h[8..12].try_into().unwrap()) as u64;
            if crc32c(&h[..124]) == u32::from_le_bytes(h[124..128].try_into().unwrap())
                && len >= 128
                && len.is_multiple_of(8)
                && at + len <= size
            {
                return Ok(Some(at));
            }
        }
        at += 8;
    }
    Ok(None)
}

fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            return Err(BookError::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        offset += n as u64;
        buf = &mut buf[n..];
    }
    Ok(())
}
