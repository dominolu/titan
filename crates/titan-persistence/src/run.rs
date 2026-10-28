use crate::*;
use serde_json::{Value, json};
use std::path::Path;

/// Owned by the execution host. A strategy never receives this object or its configuration.
pub struct RunRecorder {
    scope: FactScope,
    recording: String,
    sequence: u64,
    stream: FactStream,
    parquet: Option<columnar::FactParquetWriter>,
    chunk: Vec<FactEnvelope>,
    chunk_bytes: usize,
    failed: bool,
}
impl RunRecorder {
    pub fn open(
        directory: &Path,
        scope: FactScope,
        recording: String,
        metadata: Value,
    ) -> Result<Self> {
        if directory.join("history.db").exists() {
            return Err(failure("run_history_already_exists"));
        }
        let archive = SqliteFactArchive::open(directory.join("history.db"))?;
        archive.begin_recording(&recording, &scope)?;
        let parquet = columnar::FactParquetWriter::create(&directory.join("facts.parquet"))?;
        let stream = FactStream::new(Box::new(archive), recording.clone(), 64)?;
        let mut recorder = Self {
            scope,
            recording,
            sequence: 0,
            stream,
            parquet: Some(parquet),
            chunk: Vec::with_capacity(64),
            chunk_bytes: 0,
            failed: false,
        };
        recorder.record("run_started", 0, metadata)?;
        Ok(recorder)
    }
    pub fn record(&mut self, kind: &str, timestamp: i64, payload: Value) -> Result<()> {
        if self.failed || self.parquet.is_none() {
            return Err(failure("run_recorder_closed"));
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| failure("run_sequence_overflow"))?;
        let mut fact = FactEnvelope::new(
            self.scope.clone(),
            &self.recording,
            "execution",
            sequence,
            kind,
            timestamp,
            payload,
        );
        fact.sampled = matches!(
            kind,
            "equity_sample" | "risk_sample" | "business_observation"
        );
        let size = serde_json::to_vec(&fact)?.len();
        if size > MAX_FACT_BYTES {
            self.failed = true;
            return Err(failure("fact_payload_too_large"));
        }
        let result = (|| {
            if self.chunk.len() == 64 || self.chunk_bytes + size > 4 * MAX_FACT_BYTES {
                self.parquet
                    .as_mut()
                    .ok_or_else(|| failure("parquet_closed"))?
                    .write_chunk(&self.chunk)?;
                self.chunk.clear();
                self.chunk_bytes = 0;
            }
            self.stream.record(fact.clone())?;
            self.chunk.push(fact);
            self.chunk_bytes += size;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        } else {
            self.sequence = sequence;
        }
        result
    }
    pub fn finish(mut self, timestamp: i64, outcome: Value) -> Result<()> {
        self.record("run_finished", timestamp, outcome)?;
        self.stream.finish(true)?;
        let mut parquet = self
            .parquet
            .take()
            .ok_or_else(|| failure("parquet_closed"))?;
        parquet.write_chunk(&self.chunk)?;
        parquet.finish()?;
        Ok(())
    }
    pub fn buffered_len(&self) -> usize {
        self.chunk.len()
    }
}

/// Store original v1 bytes without changing the report reader's input or financial semantics.
/// The local v1 manifest is published last; registration can be retried from immutable objects.
pub fn commit_result_bundle(
    directory: &Path,
    storage_root: &Path,
    manifest_bytes: &[u8],
) -> Result<ObjectRef> {
    let manifest: Value = serde_json::from_slice(manifest_bytes)?;
    if manifest["schema_version"] != 1 {
        return Err(failure("bundle_schema_unsupported"));
    }
    let id = manifest["run_id"]
        .as_str()
        .ok_or_else(|| failure("bundle_identity_missing"))?;
    let entries = manifest["files"]
        .as_array()
        .ok_or_else(|| failure("bundle_files_missing"))?;
    if entries.is_empty()
        || entries.len() > 4096
        || !entries.iter().any(|v| v["path"] == "result.json")
    {
        return Err(failure("bundle_file_budget_or_result_missing"));
    }
    let root = std::fs::canonicalize(directory)?;
    let store = object_store(storage_root)?;
    let mut files = Vec::new();
    for entry in entries {
        let name = entry["path"]
            .as_str()
            .ok_or_else(|| failure("bundle_path_missing"))?;
        let path = std::path::Path::new(name);
        if path.is_absolute()
            || !path
                .components()
                .all(|p| matches!(p, std::path::Component::Normal(_)))
        {
            return Err(failure("bundle_path_escape"));
        }
        let path = std::fs::canonicalize(root.join(path))?;
        if !path.starts_with(&root) || !path.is_file() {
            return Err(failure("bundle_path_escape"));
        }
        std::fs::File::open(&path)?.sync_all()?;
        let object = store.put_file(&path)?;
        if entry["bytes"].as_u64() != Some(object.bytes)
            || entry["sha256"].as_str() != Some(object.sha256.as_str())
        {
            return Err(failure("bundle_digest_changed"));
        }
        files.push(PublishedFile {
            path: name.into(),
            object,
        });
    }
    let original = store.put_bytes(manifest_bytes)?;
    // Retain the exact publication proof if catalog registration or final publication fails.
    publish_file(&directory.join("manifest.pending.json"), manifest_bytes)?;
    let publication = PublishedManifest {
        schema_version: 1,
        logical_id: id.into(),
        kind: "result_bundle_v1".into(),
        files,
        metadata: json!({"original_manifest":original,"strategy_id":manifest["strategy_id"],"strategy_version":manifest["strategy_version"]}),
    };
    let catalog = ResultCatalog::open(&storage_root.join("catalog.db"))?;
    let reference = publication.publish(store.as_ref(), &catalog)?;
    if StorageControl::exists(storage_root) && directory.join("history.db").is_file() {
        StorageControl::open(storage_root)?.register_source(&directory.join("history.db"))?;
    }
    publish_file(&directory.join("manifest.json"), manifest_bytes)?;
    Ok(reference)
}

/// Restore verified original bytes into a fresh directory. Never re-executes a run or renderer.
pub fn materialize_result_bundle(
    storage_root: &Path,
    reference: &ObjectRef,
    output: &Path,
) -> Result<()> {
    use std::io::Read;
    let store = object_store(storage_root)?;
    if reference.bytes > 4 * MAX_FACT_BYTES as u64 {
        return Err(failure("publication_manifest_too_large"));
    }
    let mut bytes = Vec::new();
    store.open_verified(reference)?.read_to_end(&mut bytes)?;
    let publication: PublishedManifest = serde_json::from_slice(&bytes)?;
    publication.validate()?;
    if publication.kind != "result_bundle_v1" {
        return Err(failure("publication_kind_unsupported"));
    }
    let original: ObjectRef =
        serde_json::from_value(publication.metadata["original_manifest"].clone())?;
    if original.bytes > 4 * MAX_FACT_BYTES as u64 {
        return Err(failure("bundle_manifest_too_large"));
    }
    let mut manifest_bytes = Vec::new();
    store
        .open_verified(&original)?
        .read_to_end(&mut manifest_bytes)?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
    if manifest["schema_version"] != 1
        || manifest["run_id"].as_str() != Some(publication.logical_id.as_str())
    {
        return Err(failure("publication_manifest_changed"));
    }
    let files = manifest["files"]
        .as_array()
        .ok_or_else(|| failure("bundle_files_missing"))?;
    if files.len() != publication.files.len() {
        return Err(failure("publication_files_changed"));
    }
    for f in &publication.files {
        if !files.iter().any(|v| {
            v["path"] == f.path
                && v["sha256"] == f.object.sha256
                && v["bytes"].as_u64() == Some(f.object.bytes)
        }) {
            return Err(failure("publication_files_changed"));
        }
        store.open_verified(&f.object)?;
    }
    std::fs::create_dir(output)?;
    for f in &publication.files {
        store.materialize(&f.object, &output.join(&f.path))?;
    }
    publish_file(&output.join("manifest.json"), &manifest_bytes)?;
    Ok(())
}

/// Fill only missing v1 cache files. Existing bytes are validated by the original report reader.
pub fn rehydrate_result_bundle(storage_root: &Path, id: &str, output: &Path) -> Result<()> {
    use std::io::Read;
    let reference = ResultCatalog::lookup(&storage_root.join("catalog.db"), id)?;
    let store = object_store(storage_root)?;
    let _guard = store.publication_guard()?;
    let manifest = PublishedManifest::load(store.as_ref(), &reference)?;
    if manifest.logical_id != id || manifest.kind != "result_bundle_v1" {
        return Err(failure("publication_kind_unsupported"));
    }
    manifest.verify_objects(store.as_ref())?;
    for file in &manifest.files {
        let path = output.join(&file.path);
        if !path.exists() {
            store.materialize(&file.object, &path)?;
        }
    }
    let original: ObjectRef =
        serde_json::from_value(manifest.metadata["original_manifest"].clone())?;
    let mut bytes = Vec::new();
    store.open_verified(&original)?.read_to_end(&mut bytes)?;
    publish_file(&output.join("manifest.json"), &bytes)
}
