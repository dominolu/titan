//! Daily packaging of one UTC date of recorded market data.
//!
//! The main layout groups files by market: `<archive_dir>/<date>/<venue>_<instrument>_<symbol>.zip`,
//! each zip holding that market's `book/`, `trade/`, `aux/` and `validation/`
//! entries plus a `manifest.json` with sizes and SHA-256 digests. Quality
//! reports are copied to `<archive_dir>/<date>/quality.json` once the market
//! archives of that date exist (so the report covers its own date only).
//!
//! `group_json` in the recorder configuration produces
//! `<archive_dir>/<date>.zip` holding every file of the date plus
//! `manifest.json`.
//!
//! Every archive is written to a temp file, verified (entry count and
//! per-file digest), and atomically renamed. Failures never leave a partial
//! archive behind. Source files are kept unless `prune` is requested.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

const ARCHIVE_FORMAT: &str = "titan-market-data-archive-v1";
const MANIFEST_NAME: &str = "manifest.json";
const BUFFER: usize = 64 * 1024;

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
    #[error("archive: {0}")]
    Format(String),
    #[error("archive: no files for {0}")]
    Empty(String),
    #[error("archive already exists: {0}")]
    Exists(PathBuf),
    #[error("archive verification failed: {0}")]
    Verify(&'static str),
    #[error("archive manifest is invalid")]
    Manifest,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArchiveReport {
    pub date: String,
    pub label: String,
    pub archive_path: PathBuf,
    pub archive_bytes: u64,
    pub archive_sha256: String,
    pub file_count: usize,
    pub source_bytes: u64,
    pub pruned_files: usize,
}

/// One source file of a recorded day.
#[derive(Clone, Debug)]
struct DayFile {
    path: PathBuf,
    /// Archive label, for example `binance_spot_spot_BTCUSDT` or `quality`.
    label: String,
    is_quality: bool,
}

/// Manifest inside one archive.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub format: String,
    pub date: String,
    pub label: String,
    pub created_at_utc: String,
    pub source_bytes: u64,
    pub files: Vec<ManifestFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

fn relative_name(root: &Path, path: &Path) -> Result<String, ArchiveError> {
    path.strip_prefix(root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .map_err(|_| ArchiveError::Format("path outside root".to_owned()))
}

fn market_label(root: &Path, path: &Path) -> Result<String, ArchiveError> {
    // <kind>/venue=<v>/instrument=<i>/symbol=<s>/date=<d>/...
    let relative = path
        .strip_prefix(root)
        .map_err(|_| ArchiveError::Format("path outside root".to_owned()))?;
    let mut components = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned());
    let _kind = components.next();
    let mut fields = BTreeMap::new();
    for component in components {
        if let Some(value) = component.strip_prefix("venue=") {
            fields.insert("venue", value.to_owned());
        } else if let Some(value) = component.strip_prefix("instrument=") {
            fields.insert("instrument", value.to_owned());
        } else if let Some(value) = component.strip_prefix("symbol=") {
            fields.insert("symbol", value.to_owned());
        } else if component.starts_with("date=") {
            break;
        }
    }
    match (fields.get("venue"), fields.get("instrument"), fields.get("symbol")) {
        (Some(venue), Some(instrument), Some(symbol)) => {
            Ok(format!("{venue}_{instrument}_{symbol}"))
        }
        _ => Err(ArchiveError::Format("market path".to_owned())),
    }
}

fn collect_day_files(root: &Path, day: NaiveDate) -> Result<Vec<DayFile>, ArchiveError> {
    let date_component = format!("date={day}");
    let mut paths = Vec::new();
    for directory in ["book", "trade", "aux", "validation", "quality"] {
        let base = root.join(directory);
        if base.is_dir() {
            walk(&base, &date_component, &mut paths)?;
        }
    }
    paths.sort();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let is_quality = relative_name(root, &path)?.starts_with("quality/");
        let label = if is_quality {
            "quality".to_owned()
        } else {
            market_label(root, &path)?
        };
        files.push(DayFile {
            path,
            label,
            is_quality,
        });
    }
    Ok(files)
}

fn walk(directory: &Path, date_component: &str, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            walk(&path, date_component, files)?;
        } else if file_type.is_file()
            && path
                .components()
                .any(|component| component.as_os_str().to_string_lossy() == date_component)
            && !entry.file_name().to_string_lossy().contains(".tmp")
        {
            files.push(path);
        }
    }
    Ok(())
}

/// Archives one UTC date under `root`.
///
/// With `group_json` every file goes to a single `<archive_dir>/<date>.zip`;
/// otherwise each market gets `<archive_dir>/<date>/<venue>_<instrument>_<symbol>.zip`
/// and the quality report is copied to `<archive_dir>/<date>/quality.json`.
pub fn archive_daily(
    root: &Path,
    date: &str,
    archive_dir: Option<&Path>,
    prune: bool,
    overwrite: bool,
    group_json: bool,
) -> Result<Vec<ArchiveReport>, ArchiveError> {
    let day = NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|_| ArchiveError::Format("date".to_owned()))?;
    if !root.is_dir() {
        return Err(ArchiveError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "recording root",
        )));
    }
    let files = collect_day_files(root, day)?;
    if !files.iter().any(|file| !file.is_quality) {
        return Err(ArchiveError::Empty(date.to_owned()));
    }
    let archive_dir = archive_dir.map_or_else(|| root.join("archives"), Path::to_path_buf);

    if group_json {
        let mut batch = Vec::with_capacity(files.len());
        batch.extend(files.iter().cloned());
        let report = write_batch(root, date, &archive_dir, &batch, overwrite)?;
        if prune {
            let paths = batch
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>();
            let pruned = prune_files(root, &paths)?;
            return Ok(vec![ArchiveReport { pruned_files: pruned, ..report }]);
        }
        return Ok(vec![report]);
    }

    let mut grouped: BTreeMap<String, Vec<DayFile>> = BTreeMap::new();
    let mut quality = Vec::new();
    for file in files {
        if file.is_quality {
            quality.push(file);
        } else {
            grouped.entry(file.label.clone()).or_default().push(file);
        }
    }
    let mut reports = Vec::with_capacity(grouped.len());
    for (label, batch) in grouped {
        let dir = archive_dir.join(day.to_string());
        let mut report = write_batch(root, date, &dir, &batch, overwrite)?;
        debug_assert_eq!(report.label, label);
        if prune {
            let paths = batch
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>();
            report.pruned_files = prune_files(root, &paths)?;
        }
        reports.push(report);
    }
    if !quality.is_empty() {
        copy_quality(&archive_dir, date, &quality, overwrite)?;
    }
    Ok(reports)
}

fn write_batch(
    root: &Path,
    date: &str,
    directory: &Path,
    batch: &[DayFile],
    overwrite: bool,
) -> Result<ArchiveReport, ArchiveError> {
    let first = batch
        .first()
        .ok_or_else(|| ArchiveError::Empty(date.to_owned()))?;
    let label = first.label.clone();
    // Grouped archives are written directly under the archive directory and
    // named after the date; per-market archives live in `<date>/` and use the
    // `<venue>_<instrument>_<symbol>` label.
    let grouped = directory
        .file_name()
        .is_none_or(|name| name.to_string_lossy() != *date);
    let file_name = if grouped && label == "quality" && batch.len() == 1 {
        "quality.zip".to_owned()
    } else if grouped {
        format!("{date}.zip")
    } else {
        format!("{label}.zip")
    };
    fs::create_dir_all(directory)?;
    let archive_path = directory.join(&file_name);
    if archive_path.exists() && !overwrite {
        return Err(ArchiveError::Exists(archive_path));
    }
    let temp_path = directory.join(format!(".{file_name}.tmp"));
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }
    let (manifest, source_bytes) = write_archive(&temp_path, root, date, &label, batch)?;
    let archive_bytes = fs::metadata(&temp_path)?.len();
    let archive_sha256 = sha256_file(&temp_path)?;
    verify_archive(&temp_path, date, &label, &manifest)?;
    fs::rename(&temp_path, &archive_path)?;
    sync_directory(directory)?;
    Ok(ArchiveReport {
        date: date.to_owned(),
        label,
        archive_path,
        archive_bytes,
        archive_sha256,
        file_count: batch.len(),
        source_bytes,
        pruned_files: 0,
    })
}

fn copy_quality(
    archive_dir: &Path,
    date: &str,
    quality: &[DayFile],
    overwrite: bool,
) -> Result<(), ArchiveError> {
    let directory = archive_dir.join(date);
    fs::create_dir_all(&directory)?;
    for file in quality {
        let name = file
            .path
            .file_name()
            .ok_or_else(|| ArchiveError::Format("quality path".to_owned()))?;
        let target = directory.join(name);
        if target.exists() && !overwrite {
            continue;
        }
        let temp = directory.join(format!(".{}.tmp", name.to_string_lossy()));
        {
            let mut source = File::open(&file.path)?;
            let mut output = File::create(&temp)?;
            std::io::copy(&mut source, &mut output)?;
            output.sync_all()?;
        }
        fs::rename(&temp, &target)?;
    }
    sync_directory(&directory)?;
    Ok(())
}

fn write_archive(
    temp_path: &Path,
    root: &Path,
    date: &str,
    label: &str,
    batch: &[DayFile],
) -> Result<(ArchiveManifest, u64), ArchiveError> {
    let mut manifest_files = Vec::with_capacity(batch.len());
    let mut source_bytes = 0u64;
    let file = File::create(temp_path)?;
    let mut zip = ZipWriter::new(file);
    // Data files are already compressed (.tnmd blocks / .gz streams); the
    // archive only packages them, so entries are stored without a second
    // compression pass.
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o644);
    for entry in batch {
        let name = relative_name(root, &entry.path)?;
        zip.start_file(name.clone(), options)?;
        let mut source = File::open(&entry.path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; BUFFER];
        let mut bytes = 0u64;
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            zip.write_all(&buffer[..read])?;
            bytes = bytes.saturating_add(read as u64);
        }
        source_bytes = source_bytes.saturating_add(bytes);
        manifest_files.push(ManifestFile {
            path: name,
            bytes,
            sha256: format!("{:x}", hasher.finalize()),
        });
    }
    let manifest = ArchiveManifest {
        format: ARCHIVE_FORMAT.to_owned(),
        date: date.to_owned(),
        label: label.to_owned(),
        created_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        source_bytes,
        files: manifest_files,
    };
    let json = serde_json::to_vec_pretty(&manifest).map_err(|_| ArchiveError::Manifest)?;
    zip.start_file(MANIFEST_NAME, options)?;
    zip.write_all(&json)?;
    let file = zip.finish()?;
    file.sync_all()?;
    Ok((manifest, source_bytes))
}

fn verify_archive(
    path: &Path,
    date: &str,
    label: &str,
    expected: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    let file = File::open(path)?;
    let mut zip = ZipArchive::new(file)?;
    let manifest: ArchiveManifest = {
        let mut entry = zip.by_name(MANIFEST_NAME)?;
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        serde_json::from_slice(&data).map_err(|_| ArchiveError::Manifest)?
    };
    if manifest.format != ARCHIVE_FORMAT
        || manifest.date != date
        || manifest.label != label
        || manifest.files.len() != expected.files.len()
    {
        return Err(ArchiveError::Verify("manifest header"));
    }
    if zip.len() != manifest.files.len() + 1 {
        return Err(ArchiveError::Verify("entry count"));
    }
    let mut seen = BTreeSet::new();
    let mut total = 0u64;
    for expected_file in &manifest.files {
        if !seen.insert(expected_file.path.clone()) {
            return Err(ArchiveError::Verify("duplicate entry"));
        }
        let mut entry = zip.by_name(&expected_file.path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; BUFFER];
        let mut bytes = 0u64;
        loop {
            let read = entry.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            bytes = bytes.saturating_add(read as u64);
        }
        if bytes != expected_file.bytes
            || format!("{:x}", hasher.finalize()) != expected_file.sha256
        {
            return Err(ArchiveError::Verify("content digest"));
        }
        total = total.saturating_add(bytes);
    }
    if total != manifest.source_bytes {
        return Err(ArchiveError::Verify("source size"));
    }
    Ok(())
}

fn prune_files(root: &Path, files: &[PathBuf]) -> Result<usize, ArchiveError> {
    let mut pruned = 0;
    let mut parents = BTreeSet::new();
    for path in files {
        if relative_name(root, path)?.starts_with("quality/") {
            continue;
        }
        if path.exists() {
            fs::remove_file(path)?;
            pruned += 1;
        }
        if let Some(parent) = path.parent() {
            parents.insert(parent.to_path_buf());
        }
    }
    let mut parents: Vec<PathBuf> = parents.into_iter().collect();
    parents.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for parent in parents {
        remove_empty_parents(&parent, root)?;
    }
    Ok(pruned)
}

fn remove_empty_parents(directory: &Path, root: &Path) -> std::io::Result<()> {
    let mut current = directory.to_path_buf();
    while current != root && current.starts_with(root) {
        // Keep the top-level book/trade/aux/validation/quality directories.
        if current.parent() == Some(root) {
            break;
        }
        if fs::remove_dir(&current).is_err() {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, ArchiveError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; BUFFER];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}

/// The next UTC midnight plus grace period, and the date to archive then.
pub fn next_daily_archive(now: DateTime<Utc>, grace_seconds: u64) -> (DateTime<Utc>, NaiveDate) {
    let today = now.date_naive();
    let tomorrow = today.succ_opt().unwrap_or(today);
    let midnight = tomorrow
        .and_hms_opt(0, 0, 0)
        .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        .unwrap_or(now);
    let wake = midnight + chrono::Duration::seconds(grace_seconds as i64);
    (wake, today)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_root() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("titan-archive-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_file(root: &Path, relative: &str, data: &[u8]) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, data).unwrap();
    }

    fn fixture(root: &Path) {
        let day = "date=2026-11-11";
        write_file(
            root,
            &format!("book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/{day}/segment-1.tnmd"),
            b"spot-book",
        );
        write_file(
            root,
            &format!(
                "book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/{day}/segment-1.tnmd.idx"
            ),
            b"idx",
        );
        write_file(
            root,
            &format!(
                "book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/{day}/segment-2.tnmd.tmp"
            ),
            b"open",
        );
        write_file(
            root,
            &format!(
                "trade/venue=binance_spot/instrument=spot/symbol=BTCUSDT/{day}/trades-01.ndjson.gz"
            ),
            b"spot-trades",
        );
        write_file(
            root,
            &format!(
                "book/venue=binance_futures/instrument=perpetual/symbol=XAUUSDT/{day}/segment-1.tnmd"
            ),
            b"xau-book",
        );
        write_file(
            root,
            &format!(
                "aux/venue=binance_futures/instrument=perpetual/symbol=XAUUSDT/{day}/aux-01.ndjson.gz"
            ),
            b"xau-aux",
        );
        write_file(
            root,
            "quality/date=2026-11-11/quality.json",
            b"{\"status\":\"PASS\"}",
        );
        write_file(
            root,
            "book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-10/segment-old.tnmd",
            b"old",
        );
    }

    fn zip_names(path: &Path) -> Vec<String> {
        let file = File::open(path).unwrap();
        let mut zip = ZipArchive::new(file).unwrap();
        assert_eq!(
            zip.by_index(0).unwrap().compression(),
            zip::CompressionMethod::Stored
        );
        (0..zip.len())
            .map(|index| zip.by_index(index).unwrap().name().to_owned())
            .collect()
    }

    #[test]
    fn archives_one_market_per_zip_and_copies_quality() {
        let root = test_root();
        fixture(&root);
        let reports = archive_daily(&root, "2026-11-11", None, false, false, false).unwrap();
        let labels = reports
            .iter()
            .map(|report| report.label.clone())
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["binance_futures_perpetual_XAUUSDT", "binance_spot_spot_BTCUSDT"]);
        let day_dir = root.join("archives/2026-11-11");
        let spot = day_dir.join("binance_spot_spot_BTCUSDT.zip");
        let xau = day_dir.join("binance_futures_perpetual_XAUUSDT.zip");
        assert!(spot.exists() && xau.exists());
        assert!(day_dir.join("quality.json").exists());

        let spot_names = zip_names(&spot);
        assert!(spot_names.iter().any(|name| name.ends_with("segment-1.tnmd")));
        assert!(spot_names.iter().any(|name| name.ends_with("segment-1.tnmd.idx")));
        assert!(spot_names.iter().any(|name| name.contains("trades-01")));
        assert!(!spot_names.iter().any(|name| name.contains(".tmp")));
        assert!(!spot_names.iter().any(|name| name.contains("XAUUSDT")));
        assert!(!spot_names.iter().any(|name| name.contains("segment-old")));
        assert!(spot_names.iter().any(|name| name == MANIFEST_NAME));

        let xau_names = zip_names(&xau);
        assert!(xau_names.iter().any(|name| name.contains("XAUUSDT")));
        assert!(!xau_names.iter().any(|name| name.contains("BTCUSDT")));

        let spot_report = reports
            .iter()
            .find(|report| report.label == "binance_spot_spot_BTCUSDT")
            .unwrap();
        assert_eq!(spot_report.file_count, 3);
        assert_eq!(spot_report.archive_sha256.len(), 64);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn group_json_writes_single_archive() {
        let root = test_root();
        fixture(&root);
        let reports = archive_daily(&root, "2026-11-11", None, false, false, true).unwrap();
        assert_eq!(reports.len(), 1);
        let path = root.join("archives/2026-11-11.zip");
        assert_eq!(reports[0].archive_path, path);
        let names = zip_names(&path);
        assert_eq!(names.len(), 7);
        assert!(names.iter().any(|name| name.starts_with("quality/")));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_to_overwrite_by_default() {
        let root = test_root();
        fixture(&root);
        archive_daily(&root, "2026-11-11", None, false, false, false).unwrap();
        assert!(matches!(
            archive_daily(&root, "2026-11-11", None, false, false, false),
            Err(ArchiveError::Exists(_))
        ));
        archive_daily(&root, "2026-11-11", None, false, true, false).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prune_removes_data_but_keeps_quality() {
        let root = test_root();
        fixture(&root);
        let reports = archive_daily(&root, "2026-11-11", None, true, false, false).unwrap();
        let pruned: usize = reports.iter().map(|report| report.pruned_files).sum();
        assert!(pruned >= 4);
        let day_dir = root.join("archives/2026-11-11");
        assert!(day_dir.join("binance_spot_spot_BTCUSDT.zip").exists());
        assert!(day_dir.join("binance_futures_perpetual_XAUUSDT.zip").exists());
        assert!(day_dir.join("quality.json").exists());
        assert!(!root
            .join("book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11/segment-1.tnmd")
            .exists());
        // Open segment temp files are never archived nor pruned.
        assert!(root
            .join("book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11/segment-2.tnmd.tmp")
            .exists());
        assert!(root.join("quality/date=2026-11-11/quality.json").exists());
        assert!(root
            .join("book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-10/segment-old.tnmd")
            .exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn empty_date_is_rejected() {
        let root = test_root();
        fixture(&root);
        assert!(matches!(
            archive_daily(&root, "2026-11-12", None, false, false, false),
            Err(ArchiveError::Empty(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_date_is_rejected() {
        let root = test_root();
        assert!(matches!(
            archive_daily(&root, "2026-13-01", None, false, false, false),
            Err(ArchiveError::Format(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn next_archive_boundary_is_next_utc_midnight_plus_grace() {
        let now = DateTime::parse_from_rfc3339("2026-11-11T23:59:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (wake, date) = next_daily_archive(now, 120);
        assert_eq!(wake.to_rfc3339(), "2026-11-12T00:02:00+00:00");
        assert_eq!(date.to_string(), "2026-11-11");
    }
}
