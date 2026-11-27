use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    fs::File,
    io,
    io::{BufRead, BufReader, Read as _, Seek as _, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::info;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TradeRecord {
    pub exch_ts_ns: i64,
    pub local_ts_ns: i64,
    pub side: TradeSide,
    pub price: String,
    pub qty: String,
    pub trade_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuxSource {
    Stream,
    Rest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenInterestUnit {
    Contracts,
    Base,
    Quote,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuxRecord {
    Oi {
        exch_ts_ns: i64,
        local_ts_ns: i64,
        value: String,
        unit: OpenInterestUnit,
        source: AuxSource,
    },
    Funding {
        exch_ts_ns: i64,
        local_ts_ns: i64,
        rate: String,
        next_funding_ts_ns: Option<i64>,
        mark_px: Option<String>,
        index_px: Option<String>,
        source: AuxSource,
    },
    Liquidation {
        exch_ts_ns: i64,
        local_ts_ns: i64,
        side: Option<TradeSide>,
        price: Option<String>,
        qty: Option<String>,
        notional: Option<String>,
        order_id: Option<String>,
        status: Option<String>,
        source: AuxSource,
    },
}

#[derive(Deserialize, Serialize)]
struct EventSummary {
    records: u64,
    first_exch_ts_ns: i64,
    last_exch_ts_ns: i64,
    truncated_tail: bool,
    sha256: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EventIntegrityReport {
    pub files: usize,
    pub records: u64,
    pub truncated_files: usize,
    pub invalid_records: u64,
    pub missing_summaries: usize,
    pub mismatched_summaries: usize,
    pub markets: BTreeMap<String, MarketEventIntegrity>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketEventIntegrity {
    pub trade_count: u64,
    pub trade_truncated_tail: bool,
    pub aux_oi_samples: u64,
    pub aux_funding_samples: u64,
    pub aux_liquidation_count: u64,
    pub aux_stream_samples: u64,
    pub aux_rest_samples: u64,
    pub last_oi_exch_ts_ns: Option<i64>,
    #[serde(skip)]
    pub oi_timestamps_ns: Vec<i64>,
    pub oi_latest_ts_ns: Option<i64>,
    pub funding_timestamps_ns: Vec<i64>,
    pub funding_latest_ts_ns: Option<i64>,
    pub last_funding_exch_ts_ns: Option<i64>,
}

/// Scans trade and auxiliary gzip JSONL files without modifying them.
pub fn inspect_market_events(root: &Path, date: Option<&str>) -> io::Result<EventIntegrityReport> {
    let mut report = EventIntegrityReport::default();
    for kind in ["trade", "aux"] {
        let mut pending = vec![root.join(kind)];
        while let Some(directory) = pending.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for entry in entries {
                let path = entry?.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".ndjson.gz"))
                    && date
                        .is_none_or(|day| path.to_string_lossy().contains(&format!("date={day}")))
                {
                    report.files += 1;
                    let market = event_market_key(root, kind, &path);
                    inspect_event_file(&path, &mut report, &market, kind)?;
                }
            }
        }
    }
    Ok(report)
}

fn event_market_key(root: &Path, kind: &str, path: &Path) -> String {
    path.parent()
        .and_then(|directory| directory.strip_prefix(root.join(kind)).ok())
        .map(|relative| {
            relative
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .filter(|component| !component.starts_with("date="))
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_default()
}

fn inspect_event_file(
    path: &Path,
    report: &mut EventIntegrityReport,
    market_key: &str,
    kind: &str,
) -> io::Result<()> {
    let mut source = File::open(path)?;
    let mut hash = Sha256::new();
    io::copy(&mut source, &mut DigestWriter(&mut hash))?;
    let digest = format!("{:x}", hash.finalize());
    let mut decoder = BufReader::new(MultiGzDecoder::new(File::open(path)?));
    let mut valid_records = 0u64;
    let mut invalid_records = 0u64;
    let mut first_ts = None;
    let mut last_ts = None;
    let mut truncated = false;
    let mut line = Vec::new();
    loop {
        line.clear();
        match decoder.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) if line.last() != Some(&b'\n') => {
                truncated = true;
                break;
            }
            Ok(_) => match serde_json::from_slice::<serde_json::Value>(&line) {
                Ok(value) => match value.get("exch_ts_ns").and_then(serde_json::Value::as_i64) {
                    Some(ts) => {
                        first_ts.get_or_insert(ts);
                        last_ts = Some(ts);
                        valid_records += 1;
                        let market = report.markets.entry(market_key.to_owned()).or_default();
                        if kind == "trade" {
                            market.trade_count += 1;
                        } else if kind == "aux" {
                            match value.get("source").and_then(serde_json::Value::as_str) {
                                Some("stream") => market.aux_stream_samples += 1,
                                Some("rest") => market.aux_rest_samples += 1,
                                _ => invalid_records += 1,
                            }
                            match value.get("kind").and_then(serde_json::Value::as_str) {
                                Some("oi") => {
                                    market.aux_oi_samples += 1;
                                    market.last_oi_exch_ts_ns = Some(ts);
                                    market.oi_timestamps_ns.push(ts);
                                    market.oi_latest_ts_ns = Some(
                                        market.oi_latest_ts_ns.map_or(ts, |latest| latest.max(ts)),
                                    );
                                }
                                Some("funding") => {
                                    market.aux_funding_samples += 1;
                                    market.last_funding_exch_ts_ns = Some(ts);
                                    market.funding_timestamps_ns.push(ts);
                                    market.funding_latest_ts_ns = Some(
                                        market
                                            .funding_latest_ts_ns
                                            .map_or(ts, |latest| latest.max(ts)),
                                    );
                                }
                                Some("liquidation") => market.aux_liquidation_count += 1,
                                _ => invalid_records += 1,
                            }
                        }
                    }
                    None => invalid_records += 1,
                },
                Err(_) => invalid_records += 1,
            },
            Err(_) => {
                // A gzip member that is only a header carries no data. A writer that closes its
                // previous member and then opens the next one leaves exactly that on disk until
                // the next record arrives, so a file read at that instant ends with a bare header
                // and nothing was lost. Only a tail with data behind it is a truncation.
                truncated = !ends_with_a_bare_gzip_header(path)?;
                break;
            }
        }
    }
    report.records += valid_records;
    report.invalid_records += invalid_records;
    if truncated {
        report.truncated_files += 1;
        if kind == "trade" {
            report
                .markets
                .entry(market_key.to_owned())
                .or_default()
                .trade_truncated_tail = true;
        }
    }
    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let summary_name = filename.strip_suffix(".ndjson.gz").unwrap_or(&filename);
    let summary_path = path.with_file_name(format!("{summary_name}.summary.json"));
    match std::fs::read(summary_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => report.missing_summaries += 1,
        Err(error) => return Err(error),
        Ok(bytes) => {
            let summary: EventSummary = match serde_json::from_slice(&bytes) {
                Ok(summary) => summary,
                Err(_) => {
                    report.mismatched_summaries += 1;
                    return Ok(());
                }
            };
            if summary.records != valid_records
                || summary.first_exch_ts_ns != first_ts.unwrap_or_default()
                || summary.last_exch_ts_ns != last_ts.unwrap_or_default()
                || summary.sha256 != digest
                || summary.truncated_tail != truncated
            {
                report.mismatched_summaries += 1;
            }
        }
    }
    Ok(())
}

struct EventSegment {
    hour: String,
    path: PathBuf,
    summary_path: PathBuf,
    encoder: Option<GzEncoder<File>>,
    records: u64,
    first_ts: i64,
    last_ts: i64,
    truncated_tail: bool,
}

impl EventSegment {
    fn open(prefix: &Path, kind: &str, ts_ns: i64) -> io::Result<Self> {
        let datetime = timestamp(ts_ns)?;
        let date = datetime.format("%Y-%m-%d").to_string();
        let hour = datetime.format("%Y%m%dT%H").to_string();
        let directory = prefix.join(format!("date={date}"));
        std::fs::create_dir_all(&directory)?;
        let stem = match kind {
            "trade" => "trades",
            "aux" => "aux",
            _ => "validation",
        };
        let path = directory.join(format!("{stem}-{hour}.ndjson.gz"));
        let summary_path = directory.join(format!("{stem}-{hour}.summary.json"));
        let (records, first_ts, last_ts, truncated_tail) = if path.exists() {
            scan_existing(&path)?
        } else {
            (0, ts_ns, ts_ns, false)
        };
        // A file this process did not close cleanly must not be appended to. Its last gzip member
        // has no end-of-stream marker, and appending a new member after one leaves a stream that no
        // reader can decode — `MultiGzDecoder` fails on the truncated member and never reaches the
        // new one, which is how one restart turned a whole hour of trades unreadable for every
        // venue. The interrupted file keeps its own summary (`truncated_tail` records the cut), and
        // the rest of the hour continues in a new file beside it.
        let resume = path.exists() && !truncated_tail;
        let (path, summary_path) = if resume {
            (path, summary_path)
        } else if path.exists() {
            let (path, summary) = next_free_segment_path(&directory, stem, &hour);
            (path, summary)
        } else {
            (path, summary_path)
        };
        let (records, first_ts, last_ts, truncated_tail) = if resume {
            (records, first_ts, last_ts, truncated_tail)
        } else {
            (0, ts_ns, ts_ns, false)
        };
        let file = File::options().create(true).append(true).open(&path)?;
        Ok(Self {
            hour,
            path,
            summary_path,
            encoder: Some(GzEncoder::new(file, Compression::default())),
            records,
            first_ts,
            last_ts,
            truncated_tail,
        })
    }

    fn write<T: Serialize>(&mut self, ts_ns: i64, value: &T) -> io::Result<()> {
        let encoder = self
            .encoder
            .as_mut()
            .ok_or_else(|| io::Error::other("event segment closed"))?;
        serde_json::to_writer(&mut *encoder, value).map_err(io::Error::other)?;
        encoder.write_all(b"\n")?;
        if self.records == 0 {
            self.first_ts = ts_ns;
        }
        self.last_ts = ts_ns;
        self.records += 1;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        let Some(encoder) = self.encoder.take() else {
            return Ok(());
        };
        encoder.finish()?.sync_all()?;
        let mut file = File::open(&self.path)?;
        let mut digest = Sha256::new();
        io::copy(&mut file, &mut DigestWriter(&mut digest))?;
        let summary = EventSummary {
            records: self.records,
            first_exch_ts_ns: self.first_ts,
            last_exch_ts_ns: self.last_ts,
            truncated_tail: self.truncated_tail,
            sha256: format!("{:x}", digest.finalize()),
        };
        let tmp_path = PathBuf::from(format!("{}.tmp", self.summary_path.display()));
        let mut out = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut out, &summary).map_err(io::Error::other)?;
        out.write_all(b"\n")?;
        out.sync_all()?;
        std::fs::rename(tmp_path, &self.summary_path)?;
        if let Some(parent) = self.summary_path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

struct DigestWriter<'a>(&'a mut Sha256);
impl Write for DigestWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct MarketEventWriter {
    segments: HashMap<PathBuf, EventSegment>,
}

impl MarketEventWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write_trade(
        &mut self,
        root: &Path,
        venue: &str,
        instrument: &str,
        symbol: &str,
        record: &TradeRecord,
    ) -> io::Result<()> {
        self.write(
            root,
            "trade",
            (venue, instrument, symbol),
            record.exch_ts_ns,
            record,
        )
    }

    pub fn write_aux(
        &mut self,
        root: &Path,
        venue: &str,
        instrument: &str,
        symbol: &str,
        record: &AuxRecord,
    ) -> io::Result<()> {
        let ts_ns = match record {
            AuxRecord::Oi { exch_ts_ns, .. }
            | AuxRecord::Funding { exch_ts_ns, .. }
            | AuxRecord::Liquidation { exch_ts_ns, .. } => *exch_ts_ns,
        };
        self.write(root, "aux", (venue, instrument, symbol), ts_ns, record)
    }

    pub fn write_validation<T: Serialize>(
        &mut self,
        root: &Path,
        venue: &str,
        instrument: &str,
        symbol: &str,
        ts_ns: i64,
        record: &T,
    ) -> io::Result<()> {
        self.write(
            root,
            "validation",
            (venue, instrument, symbol),
            ts_ns,
            record,
        )
    }

    pub fn finish(&mut self) -> io::Result<()> {
        let mut first_error = None;
        for segment in self.segments.values_mut() {
            if let Err(error) = segment.finish() {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn write<T: Serialize>(
        &mut self,
        root: &Path,
        kind: &str,
        market: (&str, &str, &str),
        ts_ns: i64,
        value: &T,
    ) -> io::Result<()> {
        let (venue, instrument, symbol) = market;
        for component in [venue, instrument, symbol] {
            if !valid_component(component) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid market path component",
                ));
            }
        }
        let prefix = root
            .join(kind)
            .join(format!("venue={venue}"))
            .join(format!("instrument={instrument}"))
            .join(format!("symbol={symbol}"));
        let hour = timestamp(ts_ns)?.format("%Y%m%dT%H").to_string();
        if self
            .segments
            .get(&prefix)
            .is_some_and(|segment| segment.hour != hour)
        {
            self.segments
                .get_mut(&prefix)
                .expect("checked event segment")
                .finish()?;
            self.segments.remove(&prefix);
        }
        if !self.segments.contains_key(&prefix) {
            self.segments
                .insert(prefix.clone(), EventSegment::open(&prefix, kind, ts_ns)?);
        }
        self.segments
            .get_mut(&prefix)
            .expect("event segment inserted")
            .write(ts_ns, value)
    }
}

impl Drop for MarketEventWriter {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn timestamp(ts_ns: i64) -> io::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(
        ts_ns.div_euclid(1_000_000_000),
        ts_ns.rem_euclid(1_000_000_000) as u32,
    )
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "timestamp out of range"))
}

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && value != "."
        && value != ".."
}

/// The first free `<stem>-<hour>-r<n>` file in `directory`, with its summary path.
///
/// Used when the hour's file cannot be continued: a restarted writer starts a new file rather than
/// appending to a gzip member that was cut short.
fn next_free_segment_path(directory: &Path, stem: &str, hour: &str) -> (PathBuf, PathBuf) {
    for serial in 1u32.. {
        let path = directory.join(format!("{stem}-{hour}-r{serial}.ndjson.gz"));
        if !path.exists() {
            let summary = directory.join(format!("{stem}-{hour}-r{serial}.summary.json"));
            return (path, summary);
        }
    }
    unreachable!("the serial space is larger than the files an hour can hold")
}

/// Whether a file ends with a gzip header and nothing after it.
///
/// A header with no member behind it holds no records, so a file that ends this way is complete
/// as far as its readers are concerned: the writer simply had not written the member's first byte
/// yet. A tail that is anything else — a partial deflate block, a header with data — is a real
/// truncation and is reported as one.
fn ends_with_a_bare_gzip_header(path: &Path) -> io::Result<bool> {
    const HEADER_LEN: u64 = 10;
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len < HEADER_LEN {
        return Ok(false);
    }
    let mut header = [0u8; HEADER_LEN as usize];
    file.seek(io::SeekFrom::Start(len - HEADER_LEN))?;
    file.read_exact(&mut header)?;
    // The fixed part of a gzip header: magic, deflate, no flags, no mtime, no extra.
    Ok(header[..3] == [0x1f, 0x8b, 0x08]
        && header[3] == 0
        && header[4..8] == [0, 0, 0, 0]
        && header[8] == 0
        && header[9] == 0xff)
}

fn scan_existing(path: &Path) -> io::Result<(u64, i64, i64, bool)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(MultiGzDecoder::new(file));
    let mut records = 0;
    let mut first_ts = 0;
    let mut last_ts = 0;
    let mut truncated = false;
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) if line.last() != Some(&b'\n') => {
                truncated = true;
                break;
            }
            Ok(_) => {
                let value: serde_json::Value = match serde_json::from_slice(&line) {
                    Ok(value) => value,
                    Err(_) => {
                        truncated = true;
                        break;
                    }
                };
                let Some(ts) = value.get("exch_ts_ns").and_then(serde_json::Value::as_i64) else {
                    truncated = true;
                    break;
                };
                if records == 0 {
                    first_ts = ts;
                }
                last_ts = ts;
                records += 1;
            }
            Err(_) => {
                truncated = true;
                break;
            }
        }
    }
    Ok((records, first_ts, last_ts, truncated))
}

pub struct RotatingFile {
    date: NaiveDate,
    path: String,
    file: Option<GzEncoder<File>>,
}

impl RotatingFile {
    fn create(datetime: DateTime<Utc>, path: &str) -> Result<GzEncoder<File>, io::Error> {
        let date = datetime.date_naive().format("%Y%m%d");
        let file = File::options()
            .create(true)
            .append(true)
            .open(format!("{path}_{date}.gz"))?;
        Ok(GzEncoder::new(file, Compression::default()))
    }

    pub fn new(datetime: DateTime<Utc>, path: String) -> Result<Self, io::Error> {
        Ok(Self {
            date: datetime.date_naive(),
            file: Some(Self::create(datetime, &path)?),
            path,
        })
    }

    pub fn write(&mut self, datetime: DateTime<Utc>, data: String) -> Result<(), io::Error> {
        let date = datetime.date_naive();
        if date != self.date {
            let file = self.file.take().unwrap();
            file.finish()?.sync_all()?;
            self.file = Some(Self::create(datetime, &self.path)?);
            self.date = date;
            info!(%date, %self.path, "date is changed");
        }
        let timestamp = datetime.timestamp_nanos_opt().unwrap();
        self.file
            .as_mut()
            .unwrap()
            .write_all(format!("{timestamp} {data}\n").as_bytes())
    }
}

impl RotatingFile {
    fn finish(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            file.finish()?.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

pub struct Writer {
    path: String,
    file: HashMap<String, RotatingFile>,
}

impl Writer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            file: Default::default(),
        }
    }

    pub fn write(
        &mut self,
        recv_time: DateTime<Utc>,
        symbol: String,
        data: String,
    ) -> Result<(), anyhow::Error> {
        match self.file.entry(symbol.to_lowercase()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().write(recv_time, data)?;
            }
            Entry::Vacant(entry) => {
                let symbol = entry.key().clone();
                let path = self.path.as_str();
                entry
                    .insert(RotatingFile::new(recv_time, format!("{path}/{symbol}"))?)
                    .write(recv_time, data)?;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        let mut failure = None;
        for file in self.file.values_mut() {
            if let Err(error) = file.finish() {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod market_event_tests {
    use super::*;
    use std::{
        io::{Read, Write},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn root() -> PathBuf {
        // A per-call counter as well as the clock: tests run in parallel threads, and two starting
        // in the same nanosecond would otherwise share one directory and overwrite each other's
        // files — which is exactly how "passes alone, fails in the suite" looks.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "titan-events-{}-{nonce}-{serial}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn trade(ts: i64) -> TradeRecord {
        TradeRecord {
            exch_ts_ns: ts,
            local_ts_ns: ts + 10,
            side: TradeSide::Buy,
            price: "1.25".into(),
            qty: "0.5".into(),
            trade_id: None,
        }
    }

    #[test]
    fn a_restart_after_an_interrupted_hour_does_not_poison_the_file() {
        // The recorder was killed while a trade file was open. Its last gzip member has no
        // end-of-stream marker, and appending a new member after it leaves a stream no reader can
        // decode: `MultiGzDecoder` fails on the truncated member and never reaches the new one.
        // On the live node one restart made a whole hour of trades unreadable for every venue, so
        // the restarted writer must start a new file instead of appending.
        let root = root();
        let dir = root.join("trade/venue=binance/instrument=spot/symbol=BTCUSDT/date=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();
        // An interrupted file: one record, then a member cut off mid-stream. This is what the
        // operating system leaves behind when the process is killed between the record it had
        // written and the trailer that would close the gzip member.
        let interrupted = dir.join("trades-19700101T00.ndjson.gz");
        let mut encoder =
            GzEncoder::new(File::create(&interrupted).unwrap(), Compression::default());
        serde_json::to_writer(&mut encoder, &trade(0)).unwrap();
        encoder.write_all(b"\n").unwrap();
        encoder.finish().unwrap();
        let bytes = std::fs::read(&interrupted).unwrap();
        std::fs::write(&interrupted, &bytes[..bytes.len() - 8]).unwrap(); // drop CRC and size

        let mut writer = MarketEventWriter::new();
        writer
            .write_trade(&root, "binance", "spot", "BTCUSDT", &trade(1_000))
            .unwrap();
        writer.finish().unwrap();

        // The new record is readable from a new file, and the interrupted file is left as it was.
        let continuation = dir.join("trades-19700101T00-r1.ndjson.gz");
        let mut decoded = String::new();
        MultiGzDecoder::new(File::open(&continuation).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(decoded.trim()).unwrap();
        assert_eq!(value["exch_ts_ns"], 1_000);
        let intact: Vec<u8> = std::fs::read(&interrupted).unwrap();
        assert!(
            MultiGzDecoder::new(intact.as_slice())
                .read_to_string(&mut String::new())
                .is_err(),
            "the interrupted prefix must not be reported as readable"
        );
    }

    #[test]
    fn a_cleanly_closed_hour_continues_in_the_same_file() {
        // The opposite case: nothing was cut, so the hour keeps one file rather than sprouting a
        // `-r1` beside it on every ordinary restart.
        let root = root();
        let mut first = MarketEventWriter::new();
        first
            .write_trade(&root, "binance", "spot", "BTCUSDT", &trade(0))
            .unwrap();
        first.finish().unwrap();
        let mut second = MarketEventWriter::new();
        second
            .write_trade(&root, "binance", "spot", "BTCUSDT", &trade(1_000))
            .unwrap();
        second.finish().unwrap();
        let dir = root.join("trade/venue=binance/instrument=spot/symbol=BTCUSDT/date=1970-01-01");
        let mut decoded = String::new();
        MultiGzDecoder::new(File::open(dir.join("trades-19700101T00.ndjson.gz")).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        assert_eq!(decoded.lines().count(), 2);
        assert!(!dir.join("trades-19700101T00-r1.ndjson.gz").exists());
    }

    #[test]
    fn writes_trade_jsonl_gzip_and_hash_summary() {
        let root = root();
        let mut writer = MarketEventWriter::new();
        writer
            .write_trade(&root, "binance", "spot", "BTCUSDT", &trade(0))
            .unwrap();
        writer.finish().unwrap();
        let dir = root.join("trade/venue=binance/instrument=spot/symbol=BTCUSDT/date=1970-01-01");
        let data_path = dir.join("trades-19700101T00.ndjson.gz");
        let mut decoded = String::new();
        MultiGzDecoder::new(File::open(&data_path).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(decoded.trim()).unwrap();
        assert_eq!(value["side"], "buy");
        assert_eq!(value["price"], "1.25");
        assert!(value["trade_id"].is_null());
        let summary: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.join("trades-19700101T00.summary.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(summary["records"], 1);
        assert_eq!(summary["first_exch_ts_ns"], 0);
        let digest = format!("{:x}", Sha256::digest(std::fs::read(data_path).unwrap()));
        assert_eq!(summary["sha256"], digest);
        let report = inspect_market_events(&root, Some("1970-01-01")).unwrap();
        assert_eq!(report.files, 1);
        assert_eq!(report.records, 1);
        assert_eq!(report.truncated_files, 0);
        assert_eq!(report.mismatched_summaries, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_appends_a_gzip_member_and_rebuilds_summary_counts() {
        let root = root();
        let mut first = MarketEventWriter::new();
        first
            .write_trade(&root, "bybit", "perpetual", "ETHUSDT", &trade(1))
            .unwrap();
        first.finish().unwrap();
        let mut second = MarketEventWriter::new();
        second
            .write_trade(&root, "bybit", "perpetual", "ETHUSDT", &trade(2))
            .unwrap();
        second.finish().unwrap();
        let dir =
            root.join("trade/venue=bybit/instrument=perpetual/symbol=ETHUSDT/date=1970-01-01");
        let summary: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.join("trades-19700101T00.summary.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(summary["records"], 2);
        let mut decoded = String::new();
        MultiGzDecoder::new(File::open(dir.join("trades-19700101T00.ndjson.gz")).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        assert_eq!(decoded.lines().count(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn liquidation_records_preserve_missing_fields_as_null() {
        let root = root();
        let mut writer = MarketEventWriter::new();
        writer
            .write_aux(
                &root,
                "binance",
                "perpetual",
                "BTCUSDT",
                &AuxRecord::Liquidation {
                    exch_ts_ns: 0,
                    local_ts_ns: 1,
                    side: None,
                    price: None,
                    qty: None,
                    notional: None,
                    order_id: None,
                    status: None,
                    source: AuxSource::Stream,
                },
            )
            .unwrap();
        writer.finish().unwrap();
        let path = root.join("aux/venue=binance/instrument=perpetual/symbol=BTCUSDT/date=1970-01-01/aux-19700101T00.ndjson.gz");
        let mut decoded = String::new();
        MultiGzDecoder::new(File::open(path).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(decoded.trim()).unwrap();
        assert_eq!(value["kind"], "liquidation");
        assert!(value["order_id"].is_null());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_file_ending_in_a_bare_gzip_header_is_complete_not_truncated() {
        // What the live recorder leaves when it closes one member and opens the next: every
        // record present, followed by the header of a member with no body yet. It must read as
        // complete; reporting it as truncated marked good hours as damaged.
        let root = root();
        let dir =
            root.join("aux/venue=okx/instrument=perpetual/symbol=BTC-USDT-SWAP/date=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("aux-19700101T00.ndjson.gz");
        let mut bytes = Vec::new();
        for n in 0..2i64 {
            let record = AuxRecord::Oi {
                exch_ts_ns: n,
                local_ts_ns: n,
                value: "1".to_owned(),
                unit: OpenInterestUnit::Contracts,
                source: AuxSource::Rest,
            };
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            serde_json::to_writer(&mut encoder, &record).unwrap();
            encoder.write_all(b"\n").unwrap();
            bytes.extend_from_slice(&encoder.finish().unwrap());
        }
        bytes.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);
        std::fs::write(&path, &bytes).unwrap();

        assert!(ends_with_a_bare_gzip_header(&path).unwrap());
        let report = inspect_market_events(&root, None).unwrap();
        assert_eq!(report.truncated_files, 0, "no record was lost");
        assert_eq!(report.records, 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_tail_that_is_not_only_a_header_is_still_a_truncation() {
        // The counterpart: a member whose header has data behind it lost records, and saying so
        // is what keeps a real cut visible.
        let root = root();
        let dir =
            root.join("aux/venue=okx/instrument=perpetual/symbol=BTC-USDT-SWAP/date=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("aux-19700101T00.ndjson.gz");
        let record = AuxRecord::Oi {
            exch_ts_ns: 0,
            local_ts_ns: 0,
            value: "1".to_owned(),
            unit: OpenInterestUnit::Contracts,
            source: AuxSource::Rest,
        };
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        serde_json::to_writer(&mut encoder, &record).unwrap();
        encoder.write_all(b"\n").unwrap();
        let mut bytes = encoder.finish().unwrap();
        bytes.push(0x1f); // a torn write: header started, cut off
        std::fs::write(&path, &bytes).unwrap();

        assert!(!ends_with_a_bare_gzip_header(&path).unwrap());
        let report = inspect_market_events(&root, None).unwrap();
        assert_eq!(report.truncated_files, 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn event_integrity_scan_detects_a_truncated_gzip_tail_and_missing_summary() {
        let root = root();
        let mut writer = MarketEventWriter::new();
        writer
            .write_trade(&root, "binance", "spot", "BTCUSDT", &trade(0))
            .unwrap();
        writer.finish().unwrap();
        let dir = root.join("trade/venue=binance/instrument=spot/symbol=BTCUSDT/date=1970-01-01");
        let data = dir.join("trades-19700101T00.ndjson.gz");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&data)
            .unwrap()
            .write_all(b"broken-tail")
            .unwrap();
        let report = inspect_market_events(&root, None).unwrap();
        assert_eq!(report.truncated_files, 1);
        assert_eq!(report.mismatched_summaries, 1);
        std::fs::remove_file(dir.join("trades-19700101T00.summary.json")).unwrap();
        let report = inspect_market_events(&root, None).unwrap();
        assert_eq!(report.missing_summaries, 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
