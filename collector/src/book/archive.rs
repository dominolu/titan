//! Daily packaging of one UTC date of recorded market data into a single zip.
//!
//! The archive contains every file whose path carries `date=<YYYY-MM-DD>`
//! (book segments, trade/aux/validation hour files and the quality report) plus
//! a `manifest.json` with sizes and SHA-256 digests. It is written to a temp
//! file, verified by re-reading it, and atomically renamed. Source files are
//! kept unless `prune` is requested.

use std::{
    collections::BTreeSet,
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
    Format(&'static str),
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
    pub archive_path: PathBuf,
    pub archive_bytes: u64,
    pub archive_sha256: String,
    pub file_count: usize,
    pub source_bytes: u64,
    pub pruned_files: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    format: String,
    date: String,
    created_at_utc: String,
    source_bytes: u64,
    files: Vec<ManifestFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestFile {
    path: String,
    bytes: u64,
    sha256: String,
}

/// Archives one UTC date under `root` into `<archive_dir>/<date>.zip`.
///
/// `archive_dir` defaults to `<root>/archives`. When `prune` is set the
/// archived data files are deleted after a successful verification; quality
/// reports are always kept (they are tiny and useful for review).
pub fn archive_daily(
    root: &Path,
    date: &str,
    archive_dir: Option<&Path>,
    prune: bool,
    overwrite: bool,
) -> Result<ArchiveReport, ArchiveError> {
    let day =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| ArchiveError::Format("date"))?;
    if !root.is_dir() {
        return Err(ArchiveError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "recording root",
        )));
    }
    let files = collect_day_files(root, day)?;
    if files.is_empty() {
        return Err(ArchiveError::Empty(date.to_owned()));
    }
    let archive_dir = archive_dir.map_or_else(|| root.join("archives"), Path::to_path_buf);
    fs::create_dir_all(&archive_dir)?;
    let archive_path = archive_dir.join(format!("{date}.zip"));
    if archive_path.exists() && !overwrite {
        return Err(ArchiveError::Exists(archive_path));
    }
    let temp_path = archive_dir.join(format!(".{date}.zip.tmp"));
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    let (manifest, source_bytes) = write_archive(&temp_path, root, date, &files)?;
    let archive_bytes = fs::metadata(&temp_path)?.len();
    let archive_sha256 = sha256_file(&temp_path)?;
    verify_archive(&temp_path, date, &manifest)?;
    fs::rename(&temp_path, &archive_path)?;
    sync_directory(&archive_dir)?;

    let mut pruned_files = 0;
    if prune {
        pruned_files = prune_files(root, &files)?;
    }
    Ok(ArchiveReport {
        date: date.to_owned(),
        archive_path,
        archive_bytes,
        archive_sha256,
        file_count: files.len(),
        source_bytes,
        pruned_files,
    })
}

fn write_archive(
    temp_path: &Path,
    root: &Path,
    date: &str,
    files: &[PathBuf],
) -> Result<(Manifest, u64), ArchiveError> {
    let mut manifest_files = Vec::with_capacity(files.len());
    let mut source_bytes = 0u64;
    let file = File::create(temp_path)?;
    let mut zip = ZipWriter::new(file);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o644);
    for path in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| ArchiveError::Format("path outside root"))?;
        let name = relative.to_string_lossy().replace('\\', "/");
        zip.start_file(name.clone(), options)?;
        let mut source = File::open(path)?;
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
    let manifest = Manifest {
        format: ARCHIVE_FORMAT.to_owned(),
        date: date.to_owned(),
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

fn verify_archive(path: &Path, date: &str, expected: &Manifest) -> Result<(), ArchiveError> {
    let file = File::open(path)?;
    let mut zip = ZipArchive::new(file)?;
    let manifest: Manifest = {
        let mut entry = zip.by_name(MANIFEST_NAME)?;
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        serde_json::from_slice(&data).map_err(|_| ArchiveError::Manifest)?
    };
    if manifest.format != ARCHIVE_FORMAT
        || manifest.date != date
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

fn collect_day_files(root: &Path, day: NaiveDate) -> Result<Vec<PathBuf>, ArchiveError> {
    let date_component = format!("date={day}");
    let mut files = Vec::new();
    for directory in ["book", "trade", "aux", "validation", "quality"] {
        let base = root.join(directory);
        if base.is_dir() {
            walk(&base, &date_component, &mut files)?;
        }
    }
    files.sort();
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
        {
            files.push(path);
        }
    }
    Ok(())
}

fn prune_files(root: &Path, files: &[PathBuf]) -> Result<usize, ArchiveError> {
    let mut pruned = 0;
    let mut parents = BTreeSet::new();
    for path in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| ArchiveError::Format("path outside root"))?;
        if relative.starts_with("quality") {
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
        write_file(
            root,
            "book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11/segment-1.tnmd",
            b"book-a",
        );
        write_file(
            root,
            "book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11/segment-1.tnmd.idx",
            b"idx",
        );
        write_file(
            root,
            "trade/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11/trades-01.ndjson.gz",
            b"trades",
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

    #[test]
    fn archives_one_date_and_keeps_sources() {
        let root = test_root();
        fixture(&root);
        let report = archive_daily(&root, "2026-11-11", None, false, false).unwrap();
        assert_eq!(report.file_count, 4);
        assert_eq!(report.pruned_files, 0);
        assert!(report.archive_path.exists());
        assert!(report.archive_bytes > 0);
        assert_eq!(report.archive_sha256.len(), 64);
        let file = File::open(&report.archive_path).unwrap();
        let mut zip = ZipArchive::new(file).unwrap();
        assert_eq!(zip.len(), 5);
        let mut manifest = String::new();
        zip.by_name(MANIFEST_NAME)
            .unwrap()
            .read_to_string(&mut manifest)
            .unwrap();
        assert!(manifest.contains("2026-11-11"));
        assert!(manifest.contains("book/venue=binance_spot"));
        assert!(!manifest.contains("segment-old"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_to_overwrite_by_default() {
        let root = test_root();
        fixture(&root);
        archive_daily(&root, "2026-11-11", None, false, false).unwrap();
        assert!(matches!(
            archive_daily(&root, "2026-11-11", None, false, false),
            Err(ArchiveError::Exists(_))
        ));
        archive_daily(&root, "2026-11-11", None, false, true).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prune_removes_data_but_keeps_quality() {
        let root = test_root();
        fixture(&root);
        let report = archive_daily(&root, "2026-11-11", None, true, false).unwrap();
        assert_eq!(report.file_count, 4);
        assert_eq!(report.pruned_files, 3);
        assert!(
            !root
                .join("book/venue=binance_spot/instrument=spot/symbol=BTCUSDT/date=2026-11-11")
                .exists()
        );
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
            archive_daily(&root, "2026-11-12", None, false, false),
            Err(ArchiveError::Empty(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_date_is_rejected() {
        let root = test_root();
        assert!(matches!(
            archive_daily(&root, "2026-13-01", None, false, false),
            Err(ArchiveError::Format("date"))
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
