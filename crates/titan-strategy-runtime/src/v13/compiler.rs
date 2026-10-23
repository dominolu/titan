//! Rust-owned artifact contracts, native descriptor generation and publication.
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NumbaMetadataV13 {
    pub protocol_version: u32,
    pub abi_version: u32,
    pub abi_fingerprint: [u8; 32],
    pub strategy_id: String,
    pub strategy_version: String,
    pub callbacks: Vec<String>,
    pub callback_mask: u64,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_len: usize,
    pub state_alignment: u32,
    pub initial_state: Vec<u8>,
    pub state_schema: serde_json::Value,
    pub parameter_schema: serde_json::Value,
    pub subscriptions: Vec<NumbaSubscriptionV13>,
    pub capabilities: Vec<String>,
    pub build: serde_json::Value,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NumbaSubscriptionV13 {
    pub event: String,
    pub handler: String,
    pub schema_version: u32,
    pub qos: String,
}

pub fn compiler_host_target_v13() -> &'static str {
    host_target_triple()
}

pub fn validate_compile_target_v13(target: &str, cpu: &str) -> Result<(), V13LoadError> {
    // The Numba PIC emitter and linker currently implement Linux ELF only.
    if target != host_target_triple() || !target.ends_with("-linux-gnu") {
        return Err(V13LoadError::Contract(
            "compiler requires its native Linux ELF target",
        ));
    }
    let valid = if target.starts_with("x86_64-") {
        matches!(cpu, "generic" | "x86-64" | "x86-64-v2")
    } else {
        matches!(cpu, "generic" | "armv8-a")
    };
    if !valid {
        return Err(V13LoadError::Contract("unsupported compiler CPU baseline"));
    }
    Ok(())
}

/// Stable JSON for source-schema and parameter identity, independent of map insertion order.
pub fn compiler_json_v13(value: &serde_json::Value) -> Vec<u8> {
    fn sorted(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted(value)))
                    .collect(),
            ),
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(sorted).collect())
            }
            other => other.clone(),
        }
    }
    // serde_json::Value contains no non-finite numbers or unsupported values.
    serde_json::to_vec(&sorted(value)).expect("JSON value serializes")
}
fn json_cbor(value: &serde_json::Value) -> Result<CborValue, V13LoadError> {
    Ok(match value {
        serde_json::Value::Null => CborValue::Null,
        serde_json::Value::Bool(value) => CborValue::Bool(*value),
        serde_json::Value::String(value) => CborValue::Text(value.clone()),
        serde_json::Value::Array(values) => {
            CborValue::Array(values.iter().map(json_cbor).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(values) => CborValue::Map(
            values
                .iter()
                .map(|(key, value)| Ok((CborValue::Text(key.clone()), json_cbor(value)?)))
                .collect::<Result<_, V13LoadError>>()?,
        ),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                CborValue::Integer(value.into())
            } else if let Some(value) = value.as_i64() {
                CborValue::Integer(value.into())
            } else {
                CborValue::Float(
                    value
                        .as_f64()
                        .ok_or(V13LoadError::Contract("invalid JSON number"))?,
                )
            }
        }
    })
}
fn cbor_map(values: impl IntoIterator<Item = (&'static str, CborValue)>) -> CborValue {
    CborValue::Map(
        values
            .into_iter()
            .map(|(key, value)| (CborValue::Text(key.into()), value))
            .collect(),
    )
}

impl NumbaMetadataV13 {
    pub fn validate(&self) -> Result<(), V13LoadError> {
        if self.protocol_version != 1
            || self.abi_version != 13
            || self.abi_fingerprint != V13_ABI_FINGERPRINT
        {
            return Err(V13LoadError::Contract("Numba bridge ABI/protocol mismatch"));
        }
        let id = self.strategy_id.as_bytes();
        if id.is_empty()
            || id.len() > 64
            || !id[0].is_ascii_lowercase()
            || !id
                .iter()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
            || semver::Version::parse(&self.strategy_version).is_err()
        {
            return Err(V13LoadError::Contract("invalid strategy identity"));
        }
        if self.state_schema_version == 0
            || self.state_len == 0
            || self.state_len > 65_536
            || self.initial_state.len() != self.state_len
            || !self.state_alignment.is_power_of_two()
            || self.state_alignment > 8
            || !self.state_len.is_multiple_of(self.state_alignment as usize)
        {
            return Err(V13LoadError::Contract("invalid compiler state layout"));
        }
        let mut schema = self.state_schema.clone();
        let object = schema
            .as_object_mut()
            .ok_or(V13LoadError::Contract("invalid compiler state schema"))?;
        if object.get("itemsize").and_then(|v| v.as_u64()) != Some(self.state_len as u64)
            || object.get("alignment").and_then(|v| v.as_u64()) != Some(self.state_alignment.into())
        {
            return Err(V13LoadError::Contract("state schema layout mismatch"));
        }
        object.insert("schema_version".into(), self.state_schema_version.into());
        if <[u8; 32]>::from(Sha256::digest(compiler_json_v13(&schema))) != self.state_schema_hash {
            return Err(V13LoadError::Contract(
                "compiler state schema hash mismatch",
            ));
        }
        let mut mask = 0;
        for callback in &self.callbacks {
            let index = CALLBACK_NAMES
                .iter()
                .position(|name| *name == callback)
                .ok_or(V13LoadError::Contract("unknown bridge callback"))?;
            if mask & (1 << index) != 0 {
                return Err(V13LoadError::Contract("duplicate bridge callback"));
            }
            mask |= 1 << index;
        }
        if mask == 0 || mask != self.callback_mask {
            return Err(V13LoadError::Contract("bridge callback mask mismatch"));
        }
        for sub in &self.subscriptions {
            let event = V13EventKind::from_manifest_name(&sub.event)?;
            if matches!(
                event,
                V13EventKind::Start | V13EventKind::Stop | V13EventKind::Recover
            ) || mask & (1 << event.index()) == 0
            {
                return Err(V13LoadError::Contract("subscription lacks native callback"));
            }
        }
        for (index, name) in CALLBACK_NAMES.iter().enumerate() {
            if mask & (1 << index) != 0
                && !matches!(*name, "on_start" | "on_stop" | "on_recover")
                && !self.subscriptions.iter().any(|s| s.handler == *name)
            {
                return Err(V13LoadError::Contract("callback lacks subscription"));
            }
        }
        Ok(())
    }

    pub fn descriptor_source(&self) -> Result<String, V13LoadError> {
        self.validate()?;
        let bytes = |values: &[u8]| {
            values
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        let callbacks=self.callbacks.iter().map(|name|format!(
            "extern int32_t titan_numba_impl_{name}(int32_t *, void **, void *);\nTITAN_EXPORT int32_t titan_strategy_{name}(void *context) {{\n  int32_t result = -1; void *exception = 0;\n  int32_t status = titan_numba_impl_{name}(&result, &exception, context);\n  return status == 0 ? result : -1;\n}}"
        )).collect::<Vec<_>>().join("\n");
        Ok(format!(
            r#"
#include <stdint.h>
#define TITAN_EXPORT __attribute__((visibility("default")))
struct NativeStrategyDescriptor {{
  uint32_t struct_size; uint32_t abi_version; uint8_t abi_fingerprint[32];
  uint32_t state_schema_version; uint32_t state_alignment; uint64_t state_len;
  uint8_t state_schema_hash[32]; uint64_t callback_mask;
}};
static const struct NativeStrategyDescriptor DESCRIPTOR = {{
  sizeof(struct NativeStrategyDescriptor), 13, {{{fingerprint}}},
  {version}, {alignment}, {len}, {{{hash}}}, {mask}ULL
}};
TITAN_EXPORT uint32_t titan_strategy_abi_version(void) {{ return 13; }}
TITAN_EXPORT const struct NativeStrategyDescriptor *titan_strategy_descriptor(void) {{ return &DESCRIPTOR; }}
{callbacks}
"#,
            fingerprint = bytes(&V13_ABI_FINGERPRINT),
            version = self.state_schema_version,
            alignment = self.state_alignment,
            len = self.state_len,
            hash = bytes(&self.state_schema_hash),
            mask = self.callback_mask
        ))
    }
}

pub struct ArtifactBuildV13<'a> {
    pub metadata: &'a NumbaMetadataV13,
    pub library: &'a [u8],
    pub source_digest: [u8; 32],
    pub parameters_digest: [u8; 32],
    pub target_triple: &'a str,
    pub cpu_baseline: &'a str,
    pub bundle: bool,
    pub output: &'a Path,
    pub signing: Option<(&'a str, &'a [u8; 32])>,
}
pub struct CompiledArtifactV13 {
    pub paths: Vec<PathBuf>,
    pub manifest: ArtifactManifestV13,
}

/// Publication is staged on the destination filesystem. A pair's manifest is committed last;
/// failure rolls back prior files. Bundle replacement is one rename.
pub fn write_artifact_v13(
    input: ArtifactBuildV13<'_>,
) -> Result<CompiledArtifactV13, V13LoadError> {
    let meta = input.metadata;
    meta.validate()?;
    validate_compile_target_v13(input.target_triple, input.cpu_baseline)?;
    let base = input.output;
    let stem = if matches!(
        base.extension().and_then(|v| v.to_str()),
        Some("so" | "titan" | "cbor")
    ) {
        base.file_stem()
    } else {
        base.file_name()
    }
    .and_then(|v| v.to_str())
    .ok_or(V13LoadError::Contract("invalid artifact output name"))?;
    if stem.is_empty() || stem == "." || stem == ".." || stem.contains(['/', '\\']) {
        return Err(V13LoadError::Contract("unsafe artifact output name"));
    }
    let library_name = format!("{stem}.so");
    let manifest_name = format!("{stem}.manifest.cbor");
    let native_digest: [u8; 32] = Sha256::digest(input.library).into();
    let mut value = cbor_map([
        ("artifact_format_version", CborValue::Integer(1)),
        ("strategy_id", CborValue::Text(meta.strategy_id.clone())),
        (
            "strategy_version",
            CborValue::Text(meta.strategy_version.clone()),
        ),
        ("abi_version", CborValue::Integer(13)),
        (
            "abi_fingerprint",
            CborValue::Bytes(V13_ABI_FINGERPRINT.to_vec()),
        ),
        ("target_triple", CborValue::Text(input.target_triple.into())),
        ("cpu_baseline", CborValue::Text(input.cpu_baseline.into())),
        ("native_library", CborValue::Text(library_name.clone())),
        (
            "callback_mask",
            CborValue::Integer(meta.callback_mask.into()),
        ),
        (
            "state_schema_version",
            CborValue::Integer(meta.state_schema_version.into()),
        ),
        (
            "state_schema_hash",
            CborValue::Bytes(meta.state_schema_hash.to_vec()),
        ),
        ("state_len", CborValue::Integer(meta.state_len as i128)),
        (
            "state_alignment",
            CborValue::Integer(meta.state_alignment.into()),
        ),
        (
            "initial_state",
            CborValue::Bytes(meta.initial_state.clone()),
        ),
        ("state_schema", json_cbor(&meta.state_schema)?),
        ("parameter_schema", json_cbor(&meta.parameter_schema)?),
        (
            "subscriptions",
            json_cbor(
                &serde_json::to_value(&meta.subscriptions).expect("subscriptions serialize"),
            )?,
        ),
        (
            "capabilities",
            json_cbor(&serde_json::to_value(&meta.capabilities).expect("capabilities serialize"))?,
        ),
        (
            "source_digest",
            CborValue::Bytes(input.source_digest.to_vec()),
        ),
        (
            "parameters_digest",
            CborValue::Bytes(input.parameters_digest.to_vec()),
        ),
        ("build", json_cbor(&meta.build)?),
        ("native_digest", CborValue::Bytes(native_digest.to_vec())),
    ]);
    let unsigned = canonical_cbor(&value)?;
    let artifact_digest = Sha256::digest(&unsigned).into();
    let mut trust = V13TrustPolicy::default();
    let signature = if let Some((id, seed)) = input.signing {
        if id.trim().is_empty() {
            return Err(V13LoadError::Contract("empty signing key id"));
        }
        let key = SigningKey::from_bytes(seed);
        trust.require_signature = true;
        trust
            .ed25519_keys
            .insert(Arc::from(id), key.verifying_key().to_bytes());
        Some(ArtifactSignatureV13 {
            algorithm: Arc::from("ed25519"),
            key_id: Arc::from(id),
            value: key.sign(&unsigned).to_bytes().to_vec().into(),
        })
    } else {
        None
    };
    // Reuse the runtime's contract checks before any destination is touched.
    let parsed = parse_manifest(&value, signature.clone(), artifact_digest)?;
    let signature_value = signature.as_ref().map_or(CborValue::Null, |s| {
        cbor_map([
            ("algorithm", CborValue::Text(s.algorithm.to_string())),
            ("key_id", CborValue::Text(s.key_id.to_string())),
            ("value", CborValue::Bytes(s.value.to_vec())),
        ])
    });
    if let CborValue::Map(fields) = &mut value {
        fields.push((CborValue::Text("signature".into()), signature_value));
    }
    let parent = base
        .parent()
        .filter(|v| !v.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let staging = tempfile::Builder::new()
        .prefix(".titan-artifact-")
        .tempdir_in(parent)?;
    let manifest_bytes = canonical_cbor(&value)?;
    let names = if input.bundle {
        let name = base.with_extension("titan").file_name().unwrap().to_owned();
        let file = fs::File::create(staging.path().join(&name))?;
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .last_modified_time(zip::DateTime::default())
            .unix_permissions(0o444);
        let mut entries = [
            (library_name.as_str(), input.library),
            (manifest_name.as_str(), manifest_bytes.as_slice()),
        ];
        entries.sort_by_key(|entry| entry.0);
        for (name, data) in entries {
            archive
                .start_file(name, options)
                .map_err(|_| V13LoadError::Bundle("cannot write bundle entry"))?;
            archive.write_all(data)?;
        }
        archive
            .finish()
            .map_err(|_| V13LoadError::Bundle("cannot finish bundle"))?
            .sync_all()?;
        vec![PathBuf::from(name)]
    } else {
        fs::write(staging.path().join(&library_name), input.library)?;
        fs::write(staging.path().join(&manifest_name), manifest_bytes)?;
        vec![PathBuf::from(&library_name), PathBuf::from(&manifest_name)]
    };
    let inspect_path = staging
        .path()
        .join(if input.bundle { &names[0] } else { &names[1] });
    NativeArtifactLoaderV13::new(staging.path().join("cache"), trust).inspect(&inspect_path)?;
    // Refuse directories and symlinks before moving either member of a pair.
    for name in &names {
        match fs::symlink_metadata(parent.join(name)) {
            Ok(info) if !info.file_type().is_file() => {
                return Err(V13LoadError::Contract(
                    "artifact destination is not a regular file",
                ));
            }
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        fs::File::open(staging.path().join(name))?.sync_all()?;
    }
    let mut backups = Vec::new();
    let mut published = Vec::new();
    let publication = (|| -> std::io::Result<()> {
        for (index, name) in names.iter().enumerate() {
            let destination = parent.join(name);
            if destination.exists() {
                let backup = staging.path().join(format!("backup-{index}"));
                fs::hard_link(&destination, &backup)?;
                backups.push((backup, destination.clone()));
            }
            fs::rename(staging.path().join(name), &destination)?;
            published.push(destination);
        }
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = publication {
        for path in published.iter().rev() {
            let _ = fs::remove_file(path);
        }
        let mut rollback_error = None;
        for (backup, path) in backups.iter().rev() {
            if let Err(restore) = fs::rename(backup, path) {
                rollback_error = Some(restore);
            }
        }
        if let Some(restore) = rollback_error {
            let recovery = staging.keep();
            return Err(std::io::Error::other(format!("artifact publication failed: {error}; rollback failed: {restore}; retained backups in {}", recovery.display())).into());
        }
        return Err(error.into());
    }
    Ok(CompiledArtifactV13 {
        paths: published,
        manifest: parsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata() -> NumbaMetadataV13 {
        let schema = serde_json::json!({"itemsize":8,"alignment":8,"endian":"little","fields":[]});
        let mut hashed = schema.clone();
        hashed["schema_version"] = 1.into();
        NumbaMetadataV13 {
            protocol_version: 1,
            abi_version: 13,
            abi_fingerprint: V13_ABI_FINGERPRINT,
            strategy_id: "sample".into(),
            strategy_version: "1.0.0".into(),
            callbacks: vec!["on_tick".into()],
            callback_mask: 2,
            state_schema_version: 1,
            state_schema_hash: Sha256::digest(compiler_json_v13(&hashed)).into(),
            state_len: 8,
            state_alignment: 8,
            initial_state: vec![0; 8],
            state_schema: schema,
            parameter_schema: serde_json::json!({"type":"object"}),
            subscriptions: vec![NumbaSubscriptionV13 {
                event: "bbo".into(),
                handler: "on_tick".into(),
                schema_version: 1,
                qos: "latest".into(),
            }],
            capabilities: vec!["market_data".into()],
            build: serde_json::json!({"compiler":"unit-test"}),
        }
    }
    fn build<'a>(
        metadata: &'a NumbaMetadataV13,
        output: &'a Path,
        bundle: bool,
    ) -> ArtifactBuildV13<'a> {
        ArtifactBuildV13 {
            metadata,
            library: b"unit-test native bytes",
            source_digest: [1; 32],
            parameters_digest: [2; 32],
            target_triple: host_target_triple(),
            cpu_baseline: "generic",
            bundle,
            output,
            signing: None,
        }
    }
    #[test]
    fn pair_bundle_signatures_and_reproducible_overwrite_share_runtime_validation() {
        let root = tempfile::tempdir().unwrap();
        let meta = metadata();
        let pair = root.path().join("pair/sample");
        let bundle = root.path().join("bundle/sample.titan");
        let seed = [9; 32];
        let mut pair_input = build(&meta, &pair, false);
        pair_input.signing = Some(("test-key", &seed));
        let pair = write_artifact_v13(pair_input).unwrap();
        let mut bundle_input = build(&meta, &bundle, true);
        bundle_input.signing = Some(("test-key", &seed));
        let first = write_artifact_v13(bundle_input).unwrap();
        assert_eq!(
            pair.manifest.artifact_digest,
            first.manifest.artifact_digest
        );
        let bytes = fs::read(&bundle).unwrap();
        let mut again = build(&meta, &bundle, true);
        again.signing = Some(("test-key", &seed));
        write_artifact_v13(again).unwrap();
        assert_eq!(fs::read(&bundle).unwrap(), bytes);
        let mut trust = V13TrustPolicy {
            require_signature: true,
            ..Default::default()
        };
        trust.ed25519_keys.insert(
            Arc::from("test-key"),
            SigningKey::from_bytes(&seed).verifying_key().to_bytes(),
        );
        let loader = NativeArtifactLoaderV13::new(root.path().join("cache"), trust);
        loader.inspect(&bundle).unwrap();
        loader.inspect(&pair.paths[1]).unwrap();
        assert!(
            NativeArtifactLoaderV13::new(root.path().join("cache"), V13TrustPolicy::default())
                .inspect(&bundle)
                .is_err()
        );
        // Modifying a signed manifest is rejected even when its native bytes are intact.
        let mut value = parse_cbor(&fs::read(&pair.paths[1]).unwrap()).unwrap();
        if let CborValue::Map(values) = &mut value {
            for (key, value) in values {
                if key.as_text() == Some("cpu_baseline") {
                    *value = CborValue::Text("x86-64-v2".into());
                }
            }
        }
        fs::write(&pair.paths[1], canonical_cbor(&value).unwrap()).unwrap();
        assert!(matches!(
            loader.inspect(&pair.paths[1]),
            Err(V13LoadError::Signature)
        ));
    }
    #[test]
    fn malformed_bridge_and_failed_pair_publication_preserve_previous_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let mut meta = metadata();
        let output = root.path().join("sample");
        let old_library = root.path().join("sample.so");
        fs::write(&old_library, b"previous").unwrap();
        fs::create_dir(root.path().join("sample.manifest.cbor")).unwrap();
        assert!(write_artifact_v13(build(&meta, &output, false)).is_err());
        assert_eq!(fs::read(old_library).unwrap(), b"previous");
        meta.callback_mask |= 1 << 63;
        assert!(meta.validate().is_err());
        meta = metadata();
        meta.state_schema_hash[0] ^= 1;
        assert!(meta.validate().is_err());
        meta = metadata();
        meta.callbacks = vec!["../../injection".into()];
        assert!(meta.descriptor_source().is_err());
        meta = metadata();
        meta.abi_fingerprint[0] ^= 1;
        assert!(meta.validate().is_err());
    }
    #[test]
    fn native_digest_and_required_signature_are_checked_by_rust() {
        let root = tempfile::tempdir().unwrap();
        let meta = metadata();
        let output = root.path().join("sample");
        let artifact = write_artifact_v13(build(&meta, &output, false)).unwrap();
        let required = NativeArtifactLoaderV13::new(
            root.path().join("cache"),
            V13TrustPolicy {
                require_signature: true,
                ..Default::default()
            },
        );
        assert!(required.inspect(&artifact.paths[1]).is_err());
        fs::write(&artifact.paths[0], b"corrupt").unwrap();
        assert!(
            NativeArtifactLoaderV13::new(root.path().join("cache"), V13TrustPolicy::default())
                .inspect(&artifact.paths[1])
                .is_err()
        );
        assert!(validate_compile_target_v13("wrong-target", "generic").is_err());
        assert!(validate_compile_target_v13(host_target_triple(), "native").is_err());
    }
}
